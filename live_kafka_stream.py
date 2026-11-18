"""Live End-to-End Kafka Consumer -> 33 KiB Sketches + Read-Only RF 23D -> MongoDB 8.0 & UI.

Connects `producer.py` -> `Apache Kafka 4.1.0 KRaft` (`3` partitions) -> Dual-Sink Watermark (30m),
`32.0 KiB` Bit-Packed Bloom Filter (`k=4`), `1.0 KiB` HyperLogLog++ (`p=10`), bounded `EWMA`
($\\alpha=0.15$), Read-Only Frozen `Random Forest (23D)` scoring (`STREAM_PROVISIONAL v1`), and
idempotent `MongoDB 8.0` ESR-indexed upserts (`bda501.live_stream_scores`) + live UI telemetry.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import time
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from starter_stream import (
    BLOOM_NUM_BITS,
    HLL_NUM_REGISTERS,
    HLL_P_BITS,
    bloom_check_and_add,
    create_bitpacked_bloom,
    hll_add,
    hll_estimate,
    update_bounded_decay_state,
    validate_stream_batch_ordered,
)

LOS_LABELS = ["A", "B", "C", "D", "E", "F"]
ICT_TZ = timezone(timedelta(hours=7))
DEFAULT_TELEMETRY_PATH = Path("outputs/live_kafka_telemetry.json")
DEFAULT_SCORES_JSON_PATH = Path("outputs/live_kafka_scores.json")
DEFAULT_RESET_SIGNAL_PATH = Path("outputs/live_reset_signal.json")


def _derive_los_from_ratio(ratio: float) -> str:
    if math.isnan(ratio) or ratio < 0.20:
        return "F"
    for thresh, label in ((0.80, "A"), (0.65, "B"), (0.50, "C"), (0.35, "D"), (0.20, "E")):
        if ratio >= thresh:
            return label
    return "F"


def load_readonly_scorer_context(
    model_path: Path = Path("outputs/model.joblib"),
    static_path: Path = Path("data/lakehouse/segment_static_features.parquet"),
) -> dict[str, Any]:
    """Load frozen Phase A Random Forest (23D) and static road features in read-only mode."""
    bundle = joblib.load(model_path)
    eff_static = static_path if static_path.is_file() else Path("data/demo_sample/sample_static_features.parquet")
    static_map: dict[int, dict[str, Any]] = {}
    if eff_static.is_file():
        s_df = pd.read_parquet(eff_static)
        static_map = {int(r["segment_id"]): r for r in s_df.to_dict(orient="records")}
    return {
        "rf_model": bundle["rf_multiclass"],
        "imputer": bundle["ml_imputer"],
        "features": list(bundle["full_features"]),
        "static_map": static_map,
    }


def _build_feature_row(row: dict[str, Any], static_info: dict[str, Any], feature_names: list[str]) -> dict[str, float]:
    """Construct leak-free 23D feature vector at event origin t using payload + frozen static priors."""
    vel = float(row.get("mean_velocity", row.get("velocity", 25.0)))
    vmax = float(row.get("max_velocity") or static_info.get("max_velocity") or 40.0)
    vmax = max(vmax, 5.0)
    v_ratio = float(row.get("velocity_ratio", vel / vmax))
    los_str = str(row.get("LOS") or _derive_los_from_ratio(v_ratio))
    los_ord = float(
        row.get(
            "current_los_ordinal", row.get("los_ordinal", LOS_LABELS.index(los_str) if los_str in LOS_LABELS else 0)
        )
    )

    defaults: dict[str, float] = {
        "current_los_ordinal": los_ord,
        "lag1_los_ordinal": float(row.get("lag1_los_ordinal", los_ord)),
        "lag2_los_ordinal": float(row.get("lag2_los_ordinal", los_ord)),
        "trailing_severe_rate": float(row.get("trailing_severe_rate", 1.0 if los_ord >= 4 else 0.0)),
        "mean_velocity": vel,
        "velocity_ratio": v_ratio,
        "obs_count": float(row.get("obs_count", 1.0)),
        "length_m": float(row.get("length_m") or static_info.get("length_m") or 50.0),
        "max_velocity": vmax,
        "street_level": float(row.get("street_level") or static_info.get("street_level") or 3.0),
        "weekday": float(row.get("weekday", 1.0)),
        "period_sin": float(row.get("period_sin", 0.0)),
        "period_cos": float(row.get("period_cos", 1.0)),
        "gap_minutes": float(row.get("gap_minutes", 30.0)),
        "pagerank_score": float(row.get("pagerank_score") or static_info.get("pagerank_score") or 1.0),
        "in_degree": float(row.get("in_degree") or static_info.get("in_degree") or 1.0),
        "out_degree": float(row.get("out_degree") or static_info.get("out_degree") or 1.0),
        "neighbor_los_ordinal": float(row.get("neighbor_los_ordinal", los_ord)),
        "cluster_id": float(
            row.get("cluster_id") if row.get("cluster_id") is not None else static_info.get("cluster_id", 0.0)
        ),
        "segment_train_prior_los": float(
            row.get("segment_train_prior_los") or static_info.get("segment_train_prior_los") or los_ord
        ),
        "segment_train_severe_prior": float(
            row.get("segment_train_severe_prior") or static_info.get("segment_train_severe_prior") or 0.1
        ),
        "street_causal_prior_los": float(row.get("street_causal_prior_los", los_ord)),
        "street_causal_prior_sev": float(row.get("street_causal_prior_sev", 0.1)),
    }
    return {col: defaults.get(col, 0.0) for col in feature_names}


def _score_single_event(
    row: dict[str, Any], scorer_ctx: dict[str, Any], ewma_score: float, partition: int
) -> dict[str, Any]:
    """Score one in-watermark event with frozen 23D Random Forest (`status_version=1`)."""
    seg_id = int(row["segment_id"])
    static_info = scorer_ctx["static_map"].get(seg_id, {})
    feat_names = scorer_ctx["features"]
    feat_dict = _build_feature_row(row, static_info, feat_names)
    x_mat = scorer_ctx["imputer"].transform(pd.DataFrame([feat_dict])[feat_names])
    probs_6 = scorer_ctx["rf_model"].predict_proba(x_mat)[0]
    pred_ord = int(np.argmax(probs_6))
    cur_ord = int(round(feat_dict["current_los_ordinal"]))
    cur_ord = min(max(cur_ord, 0), 5)
    severe_prob = float(probs_6[4:].sum())
    change_prob = float(1.0 - probs_6[cur_ord])

    now_ict = datetime.now(tz=ICT_TZ).strftime("%H:%M:%S")
    street_name = str(row.get("street_name") or static_info.get("street_name") or f"Segment #{seg_id}")
    return {
        "event_id": str(row.get("event_id") or f"{seg_id}_{row.get('date', 'live')}_{row.get('period', 'p0')}"),
        "raw_event_id": str(row.get("raw_event_id") or ""),
        "segment_id": seg_id,
        "street_name": street_name,
        "street_level": int(round(feat_dict["street_level"])),
        "date": str(row.get("date", "2020-08-03")),
        "period": str(row.get("period", "period_0_00")),
        "mean_velocity": round(float(feat_dict["mean_velocity"]), 2),
        "max_velocity": round(float(feat_dict["max_velocity"]), 1),
        "current_los": LOS_LABELS[cur_ord],
        "predicted_next_los": LOS_LABELS[pred_ord],
        "severe_prob": round(severe_prob, 4),
        "change_prob": round(change_prob, 4),
        "ewma_hotspot_score": round(float(ewma_score), 4),
        "pagerank_score": round(float(feat_dict["pagerank_score"]), 4),
        "kafka_partition": int(partition),
        "lifecycle_status": "STREAM_PROVISIONAL",
        "status_version": 1,
        "received_at_ict": now_ict,
    }


def init_live_state(topic: str = "hcmc-traffic-events", reset_epoch: int = 0) -> dict[str, Any]:
    """Initialize bounded 33.0 KiB streaming state and counters for live Kafka consumption."""
    return {
        "topic": topic,
        "reset_epoch": int(reset_epoch),
        "batches_processed": 0,
        "messages_consumed": 0,
        "partition_counts": {"0": 0, "1": 0, "2": 0},
        "quarantined_count": 0,
        "sink1_bronze_total": 0,
        "duplicates_caught_bloom": 0,
        "late_dropped_for_tier2": 0,
        "sink2_scored_v1": 0,
        "severe_alerts_v1_count": 0,
        "v2_guard_protected_count": 0,
        "bloom_bytes": create_bitpacked_bloom(BLOOM_NUM_BITS),
        "hll_registers": np.zeros(HLL_NUM_REGISTERS, dtype=np.uint8),
        "exact_segments_seen": set(),
        "decay_scores": {},
        "recent_scored_docs": [],
    }


def _read_current_reset_epoch(
    mongo_client: Any | None,
    reset_signal_path: Path = DEFAULT_RESET_SIGNAL_PATH,
) -> int:
    """Read latest UI reset epoch from MongoDB control doc or shared JSON signal."""
    if mongo_client is not None:
        try:
            doc = mongo_client["bda501"]["live_stream_control"].find_one({"_id": "reset_signal"})
            if doc and "reset_epoch" in doc:
                return int(doc["reset_epoch"])
        except Exception:
            pass
    if reset_signal_path.is_file():
        try:
            data = json.loads(reset_signal_path.read_text(encoding="utf-8"))
            return int(data.get("reset_epoch", 0))
        except Exception:
            pass
    return 0


def _open_mongo_live(mongo_uri: str) -> Any | None:
    if not mongo_uri:
        return None
    try:
        import pymongo  # type: ignore[import-not-found]

        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=1200)
        client.admin.command("ping")
        db = client["bda501"]
        live_coll = db["live_stream_scores"]
        live_coll.create_index([("event_id", 1)], unique=True)
        live_coll.create_index([("street_level", 1), ("severe_prob", -1), ("segment_id", 1)])
        return client
    except Exception:
        return None


def reset_live_stream_state(
    mongo_uri: str = "mongodb://127.0.0.1:27017",
    telemetry_path: Path = DEFAULT_TELEMETRY_PATH,
    scores_json_path: Path = DEFAULT_SCORES_JSON_PATH,
    reset_signal_path: Path = DEFAULT_RESET_SIGNAL_PATH,
) -> dict[str, Any]:
    """Reset live demo collections, JSON snapshots & notify running `live-consumer` via `reset_epoch`."""
    new_epoch = int(time.time_ns())
    empty_snap = {
        "status": "READY_WAITING_FOR_KAFKA",
        "topic": "hcmc-traffic-events",
        "reset_epoch": new_epoch,
        "batches_processed": 0,
        "messages_consumed": 0,
        "partition_counts": {"0": 0, "1": 0, "2": 0},
        "quarantined_count": 0,
        "sink1_bronze_total": 0,
        "duplicates_caught_bloom": 0,
        "late_dropped_for_tier2": 0,
        "sink2_scored_v1": 0,
        "severe_alerts_v1_count": 0,
        "v2_guard_protected_count": 0,
        "bloom_memory_kb": 32.0,
        "hll_memory_kb": 1.0,
        "hll_estimated_segments": 0.0,
        "exact_distinct_segments": 0,
        "mongo_connected": False,
        "last_updated_ict": datetime.now(tz=ICT_TZ).strftime("%H:%M:%S"),
    }
    client = _open_mongo_live(mongo_uri)
    if client is not None:
        db = client["bda501"]
        db["live_stream_scores"].delete_many({})
        db["live_stream_control"].replace_one(
            {"_id": "reset_signal"},
            {"_id": "reset_signal", "reset_epoch": new_epoch},
            upsert=True,
        )
        empty_snap["mongo_connected"] = True
        db["live_stream_telemetry"].replace_one({"_id": "current"}, {"_id": "current", **empty_snap}, upsert=True)
        client.close()
    telemetry_path.parent.mkdir(parents=True, exist_ok=True)
    reset_signal_path.write_text(json.dumps({"reset_epoch": new_epoch}), encoding="utf-8")
    telemetry_path.write_text(json.dumps(empty_snap, ensure_ascii=False, indent=2), encoding="utf-8")
    scores_json_path.write_text("[]", encoding="utf-8")
    return empty_snap


def _persist_batch_to_mongo_and_files(
    scored_docs: list[dict[str, Any]],
    state: dict[str, Any],
    mongo_client: Any | None,
    telemetry_path: Path,
    scores_json_path: Path,
) -> None:
    """Upsert provisional v1 scores to MongoDB 8.0, check v2 version guard, and flush telemetry."""
    mongo_ok = mongo_client is not None
    if mongo_client is not None and scored_docs:
        import pymongo  # type: ignore[import-not-found]

        db = mongo_client["bda501"]
        eids = [d["event_id"] for d in scored_docs]
        v2_hits = db["traffic_scores"].count_documents({"event_id": {"$in": eids}, "status_version": {"$gte": 2}})
        state["v2_guard_protected_count"] += int(v2_hits)
        ops = [
            pymongo.UpdateOne(
                {"event_id": d["event_id"]},
                {"$set": d},
                upsert=True,
            )
            for d in scored_docs
        ]
        db["live_stream_scores"].bulk_write(ops, ordered=False)

    hll_est = round(hll_estimate(state["hll_registers"]), 1) if state["exact_segments_seen"] else 0.0
    snap = {
        "status": "STREAMING_LIVE",
        "topic": state["topic"],
        "batches_processed": state["batches_processed"],
        "messages_consumed": state["messages_consumed"],
        "partition_counts": dict(state["partition_counts"]),
        "quarantined_count": state["quarantined_count"],
        "sink1_bronze_total": state["sink1_bronze_total"],
        "duplicates_caught_bloom": state["duplicates_caught_bloom"],
        "late_dropped_for_tier2": state["late_dropped_for_tier2"],
        "sink2_scored_v1": state["sink2_scored_v1"],
        "severe_alerts_v1_count": state["severe_alerts_v1_count"],
        "v2_guard_protected_count": state["v2_guard_protected_count"],
        "bloom_memory_kb": 32.0,
        "hll_memory_kb": 1.0,
        "hll_estimated_segments": hll_est,
        "exact_distinct_segments": len(state["exact_segments_seen"]),
        "mongo_connected": mongo_ok,
        "last_updated_ict": datetime.now(tz=ICT_TZ).strftime("%H:%M:%S"),
    }
    if mongo_client is not None:
        mongo_client["bda501"]["live_stream_telemetry"].replace_one(
            {"_id": "current"},
            {"_id": "current", **snap},
            upsert=True,
        )
    telemetry_path.parent.mkdir(parents=True, exist_ok=True)
    telemetry_path.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    scores_json_path.write_text(
        json.dumps(state["recent_scored_docs"][:50], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def process_kafka_records_batch(
    records: list[tuple[int, dict[str, Any]]],
    state: dict[str, Any],
    scorer_ctx: dict[str, Any],
    mongo_client: Any | None = None,
    telemetry_path: Path = DEFAULT_TELEMETRY_PATH,
    scores_json_path: Path = DEFAULT_SCORES_JSON_PATH,
) -> list[dict[str, Any]]:
    """Process one micro-batch of `(partition, payload)` events through Dual-Sink + 33 KiB Sketches + RF."""
    if not records:
        return []
    state["batches_processed"] += 1
    state["messages_consumed"] += len(records)
    rows_with_part = [{**payload, "_kpart": int(part)} for part, payload in records]
    accepted_df, rejected_df = validate_stream_batch_ordered(pd.DataFrame(rows_with_part))
    state["quarantined_count"] += int(len(rejected_df))

    scored_docs: list[dict[str, Any]] = []
    for row in accepted_df.to_dict(orient="records"):
        part_key = str(int(row.get("_kpart", row.get("kafka_partition", 0))) % 3)
        state["partition_counts"][part_key] = state["partition_counts"].get(part_key, 0) + 1
        state["sink1_bronze_total"] += 1

        raw_eid = str(row.get("raw_event_id") or row.get("event_id"))
        if bloom_check_and_add(state["bloom_bytes"], raw_eid):
            state["duplicates_caught_bloom"] += 1
            continue

        delay_sec = float(row.get("arrival_delay_seconds", 0.0))
        if delay_sec > 1800.0 or str(row.get("lateness_bucket", "")) == "LATE_OUT_OF_WATERMARK":
            state["late_dropped_for_tier2"] += 1
            continue

        seg_id = int(row["segment_id"])
        state["exact_segments_seen"].add(seg_id)
        hll_add(state["hll_registers"], str(seg_id), p_bits=HLL_P_BITS)
        vel = float(row.get("mean_velocity", row.get("velocity", 25.0)))
        is_sev = int(row.get("is_severe", 1 if vel < 15.0 else 0))
        update_bounded_decay_state(state["decay_scores"], seg_id, is_sev)

        doc = _score_single_event(row, scorer_ctx, state["decay_scores"].get(seg_id, 0.0), int(part_key))
        state["sink2_scored_v1"] += 1
        doc["seq_id"] = int(state["sink2_scored_v1"])
        if doc["severe_prob"] >= 0.50 or doc["predicted_next_los"] in ("E", "F"):
            state["severe_alerts_v1_count"] += 1
        scored_docs.append(doc)

    if scored_docs:
        state["recent_scored_docs"] = (scored_docs[::-1] + state["recent_scored_docs"])[:60]
    _persist_batch_to_mongo_and_files(scored_docs, state, mongo_client, telemetry_path, scores_json_path)
    return scored_docs


def _maybe_apply_reset_signal(state: dict[str, Any], mongo_client: Any | None, consumer: Any) -> dict[str, Any]:
    """Detect UI reset signal and clear in-memory 33 KiB sketches + advance Kafka offsets cleanly."""
    latest_epoch = _read_current_reset_epoch(mongo_client)
    if latest_epoch == int(state.get("reset_epoch", 0)):
        return state
    assigned = consumer.assignment()
    if assigned:
        consumer.seek_to_end(*assigned)
    print(f"[LiveReset] Cleared in-memory Bloom/HLL++ sketches (epoch={latest_epoch})", flush=True)
    return init_live_state(topic=state["topic"], reset_epoch=latest_epoch)


def _extract_polled_batch(polled: dict[Any, list[Any]]) -> list[tuple[int, dict[str, Any]]]:
    batch: list[tuple[int, dict[str, Any]]] = []
    for tp, msgs in polled.items():
        for msg in msgs:
            if isinstance(msg.value, dict):
                batch.append((int(tp.partition), msg.value))
    return batch


def run_live_kafka_consumer(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "hcmc-traffic-events",
    group_id: str = "hcmc-toc-live-consumer",
    mongo_uri: str = "mongodb://127.0.0.1:27017",
    max_messages: int = 0,
    idle_timeout_sec: float = 12.0,
    continuous: bool = False,
    reset_live: bool = False,
    auto_offset_reset: str = "earliest",
) -> dict[str, Any]:
    """Consume live events from Kafka 4.1 KRaft, score with frozen 23D RF, and sink to MongoDB 8.0."""
    from kafka import KafkaConsumer  # type: ignore[import-not-found]

    if reset_live:
        reset_live_stream_state(mongo_uri=mongo_uri)

    scorer_ctx = load_readonly_scorer_context()
    mongo_client = _open_mongo_live(mongo_uri)
    init_epoch = _read_current_reset_epoch(mongo_client)
    state = init_live_state(topic=topic, reset_epoch=init_epoch)
    brokers = [b.strip().replace("localhost:", "127.0.0.1:") for b in bootstrap_servers.split(",") if b.strip()]
    eff_group = f"{group_id}-{int(time.time())}" if reset_live else group_id

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=eff_group,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    last_msg_ts = time.monotonic()
    try:
        while True:
            state = _maybe_apply_reset_signal(state, mongo_client, consumer)
            batch = _extract_polled_batch(consumer.poll(timeout_ms=700, max_records=25))
            if batch:
                last_msg_ts = time.monotonic()
                process_kafka_records_batch(batch, state, scorer_ctx, mongo_client)
                p_str = ",".join(f"P{k}={v}" for k, v in sorted(state["partition_counts"].items()))
                print(
                    f"[LiveBatch #{state['batches_processed']}] +{len(batch)} msgs "
                    f"(Total={state['messages_consumed']} | {p_str}) | "
                    f"BloomDup={state['duplicates_caught_bloom']} | Late>30m={state['late_dropped_for_tier2']} | "
                    f"Scored_v1={state['sink2_scored_v1']} -> Mongo={mongo_client is not None}",
                    flush=True,
                )
                if 0 < max_messages <= state["messages_consumed"]:
                    break
            elif not continuous and (time.monotonic() - last_msg_ts) >= idle_timeout_sec:
                break
    finally:
        consumer.close()
        if mongo_client is not None:
            mongo_client.close()
    return json.loads(DEFAULT_TELEMETRY_PATH.read_text(encoding="utf-8")) if DEFAULT_TELEMETRY_PATH.is_file() else {}
