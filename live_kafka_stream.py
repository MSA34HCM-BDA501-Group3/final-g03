"""Live End-to-End Kafka Consumer -> 33 KiB Sketches + Read-Only RF 23D -> MongoDB 8.0 & UI.

Connects `producer.py` -> `Apache Kafka 4.1.0 KRaft` (`3` partitions) -> Dual-Sink Watermark (30m),
`32.0 KiB` Rotating Bloom Filter (`2 x 16.0 KiB`), `1.0 KiB` 64-bit HyperLogLog (`p=10`, `uint8[1024]`),
time-aware bounded `EWMA` ($\\alpha=0.15$), Read-Only Frozen `Random Forest (23D)` scoring
(`STREAM_PROVISIONAL v1`), and atomic `MongoDB 8.0` version-guarded upserts (`bda501.live_stream_scores`)
+ append-only Bronze WAL + live UI telemetry.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import os
import time
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from starter_stream import (
    HLL_NUM_REGISTERS,
    HLL_P_BITS,
    create_rotating_bloom,
    evict_stale_decay_segments,
    hll_add,
    hll_estimate,
    rotating_bloom_check_and_add,
    update_bounded_decay_state,
    validate_stream_batch_ordered,
)

LOS_LABELS = ["A", "B", "C", "D", "E", "F"]
ICT_TZ = timezone(timedelta(hours=7))
DEFAULT_TELEMETRY_PATH = Path("outputs/live_kafka_telemetry.json")
DEFAULT_SCORES_JSON_PATH = Path("outputs/live_kafka_scores.json")
DEFAULT_RESET_SIGNAL_PATH = Path("outputs/live_reset_signal.json")
DEFAULT_BRONZE_WAL_PATH = Path("outputs/live_bronze_wal.jsonl")


def _derive_los_from_ratio(ratio: float) -> str:
    if math.isnan(ratio) or ratio < 0.20:
        return "F"
    for thresh, label in ((0.80, "A"), (0.65, "B"), (0.50, "C"), (0.35, "D"), (0.20, "E")):
        if ratio >= thresh:
            return label
    return "F"


def _is_valid_value(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, str) and not val.strip():
        return False
    try:
        if pd.isna(val):
            return False
    except Exception:
        pass
    if isinstance(val, (float, np.floating)):
        fval = float(val)
        if math.isnan(fval) or math.isinf(fval):
            return False
    return True


def _pick_valid(*candidates: Any, default: Any = 0.0) -> Any:
    """Return the first candidate that is neither None, empty string, NaN, nor inf (never drops 0 or 0.0)."""
    for val in candidates:
        if _is_valid_value(val):
            return val
    return default


def _parse_date_period_utc(date_val: Any, period_val: Any) -> pd.Timestamp | None:
    if date_val is None or period_val is None:
        return None
    p_str = str(period_val).strip()
    if not p_str.startswith("period_"):
        return None
    parts = p_str.removeprefix("period_").split("_")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return None
    hr, mn = int(parts[0]), int(parts[1])
    if not (0 <= hr <= 23 and mn in (0, 30)):
        return None
    base_dt = pd.to_datetime(str(date_val), utc=True, errors="coerce")
    return None if pd.isna(base_dt) else base_dt + timedelta(minutes=(hr * 2 + mn // 30) * 30)


def _parse_slot_utc(row: dict[str, Any]) -> pd.Timestamp | None:
    slot_ts = _parse_date_period_utc(
        _pick_valid(row.get("date"), default=None),
        _pick_valid(row.get("period"), default=None),
    )
    if slot_ts is not None:
        return slot_ts
    raw_ts = _pick_valid(row.get("event_ts"), row.get("updated_at"), row.get("slot_ts"), default=None)
    if raw_ts is None:
        return None
    parsed = pd.to_datetime(raw_ts, utc=True, errors="coerce")
    return None if pd.isna(parsed) else parsed


def _compute_ict_calendar_features(row: dict[str, Any]) -> dict[str, Any]:
    slot_utc = _parse_slot_utc(row)
    if slot_utc is None:
        return {"weekday": 1.0, "period_sin": 0.0, "period_cos": 1.0, "ict_date": "2020-08-03", "vn_hour": 7.0}
    vn_ts = slot_utc + timedelta(hours=7)
    ict_p_idx = vn_ts.hour * 2 + (vn_ts.minute // 30)
    angle = 2.0 * math.pi * ict_p_idx / 48.0
    return {
        "weekday": float(vn_ts.weekday()),
        "period_sin": float(math.sin(angle)),
        "period_cos": float(math.cos(angle)),
        "ict_date": vn_ts.strftime("%Y-%m-%d"),
        "vn_hour": float(vn_ts.hour + vn_ts.minute / 60.0),
    }


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
    change_threshold = float(
        _pick_valid(
            bundle.get("change_threshold"),
            bundle.get("val_selected_change_threshold"),
            default=0.30,
        )
    )
    return {
        "rf_model": bundle["rf_multiclass"],
        "imputer": bundle["ml_imputer"],
        "features": list(bundle["full_features"]),
        "static_map": static_map,
        "change_threshold": change_threshold,
    }


def _resolve_los_ordinal(row: dict[str, Any], v_ratio: float) -> float:
    raw_los = _pick_valid(row.get("LOS"), default=None)
    los_str = str(raw_los) if raw_los is not None and str(raw_los) in LOS_LABELS else _derive_los_from_ratio(v_ratio)
    fallback_ord = float(LOS_LABELS.index(los_str)) if los_str in LOS_LABELS else 0.0
    return float(_pick_valid(row.get("current_los_ordinal"), row.get("los_ordinal"), default=fallback_ord))


def _build_feature_row(row: dict[str, Any], static_info: dict[str, Any], feature_names: list[str]) -> dict[str, float]:
    """Construct leak-free 23D feature vector at event origin t using payload + frozen static priors."""
    vel = float(
        _pick_valid(row.get("mean_velocity"), row.get("velocity"), static_info.get("mean_velocity"), default=25.0)
    )
    vmax = max(float(_pick_valid(row.get("max_velocity"), static_info.get("max_velocity"), default=40.0)), 5.0)
    raw_ratio = float(_pick_valid(row.get("velocity_ratio"), vel / vmax, default=vel / vmax))
    v_ratio = float(np.clip(raw_ratio, 0.0, 1.5))
    los_ord = _resolve_los_ordinal(row, v_ratio)
    ict_cal = _compute_ict_calendar_features(row)

    defaults: dict[str, float] = {
        "current_los_ordinal": los_ord,
        "lag1_los_ordinal": float(_pick_valid(row.get("lag1_los_ordinal"), default=los_ord)),
        "lag2_los_ordinal": float(_pick_valid(row.get("lag2_los_ordinal"), default=los_ord)),
        "trailing_severe_rate": float(
            _pick_valid(row.get("trailing_severe_rate"), default=(1.0 if los_ord >= 4 else 0.0))
        ),
        "mean_velocity": vel,
        "velocity_ratio": v_ratio,
        "obs_count": float(_pick_valid(row.get("obs_count"), default=1.0)),
        "length_m": float(_pick_valid(row.get("length_m"), static_info.get("length_m"), default=50.0)),
        "max_velocity": vmax,
        "street_level": float(_pick_valid(row.get("street_level"), static_info.get("street_level"), default=3.0)),
        "weekday": float(_pick_valid(row.get("weekday"), default=ict_cal["weekday"])),
        "period_sin": float(_pick_valid(row.get("period_sin"), default=ict_cal["period_sin"])),
        "period_cos": float(_pick_valid(row.get("period_cos"), default=ict_cal["period_cos"])),
        "gap_minutes": float(_pick_valid(row.get("gap_minutes"), default=30.0)),
        "pagerank_score": float(_pick_valid(row.get("pagerank_score"), static_info.get("pagerank_score"), default=1.0)),
        "in_degree": float(_pick_valid(row.get("in_degree"), static_info.get("in_degree"), default=1.0)),
        "out_degree": float(_pick_valid(row.get("out_degree"), static_info.get("out_degree"), default=1.0)),
        "neighbor_los_ordinal": float(_pick_valid(row.get("neighbor_los_ordinal"), default=los_ord)),
        "cluster_id": float(_pick_valid(row.get("cluster_id"), static_info.get("cluster_id"), default=0.0)),
        "segment_train_prior_los": float(
            _pick_valid(row.get("segment_train_prior_los"), static_info.get("segment_train_prior_los"), default=los_ord)
        ),
        "segment_train_severe_prior": float(
            _pick_valid(
                row.get("segment_train_severe_prior"), static_info.get("segment_train_severe_prior"), default=0.1
            )
        ),
        "street_causal_prior_los": float(
            _pick_valid(row.get("street_causal_prior_los"), static_info.get("street_causal_prior_los"), default=los_ord)
        ),
        "street_causal_prior_sev": float(
            _pick_valid(row.get("street_causal_prior_sev"), static_info.get("street_causal_prior_sev"), default=0.1)
        ),
    }
    return {col: defaults.get(col, 0.0) for col in feature_names}


def _score_single_event(
    row: dict[str, Any], scorer_ctx: dict[str, Any], ewma_score: float, partition: int
) -> dict[str, Any]:
    """Score one in-watermark event with frozen 23D Random Forest (`status_version=1`)."""
    seg_id = int(float(str(row["segment_id"])))
    static_info = scorer_ctx["static_map"].get(seg_id, {})
    feat_names = scorer_ctx["features"]
    feat_dict = _build_feature_row(row, static_info, feat_names)
    x_mat = scorer_ctx["imputer"].transform(pd.DataFrame([feat_dict])[feat_names])
    probs_6 = scorer_ctx["rf_model"].predict_proba(x_mat)[0]
    pred_ord = int(np.argmax(probs_6))
    cur_ord = min(max(int(round(feat_dict["current_los_ordinal"])), 0), 5)
    severe_prob = float(probs_6[4:].sum())
    change_prob = float(1.0 - probs_6[cur_ord])
    alt_probs = probs_6.copy()
    alt_probs[cur_ord] = -1.0
    change_threshold = float(scorer_ctx.get("change_threshold", 0.30))
    cal_ord = int(np.argmax(alt_probs)) if change_prob >= change_threshold else cur_ord

    ict_cal = _compute_ict_calendar_features(row)
    date_str = str(_pick_valid(row.get("date"), default="2020-08-03"))
    period_str = str(_pick_valid(row.get("period"), default="period_0_00"))
    ict_date = str(_pick_valid(row.get("ict_date"), default=ict_cal["ict_date"]))
    vn_hour = float(_pick_valid(row.get("vn_hour"), default=ict_cal["vn_hour"]))
    street_name = str(_pick_valid(row.get("street_name"), static_info.get("street_name"), default=f"Segment #{seg_id}"))
    event_id = str(_pick_valid(row.get("event_id"), default=f"{seg_id}_{date_str}_{period_str}"))
    raw_event_id = str(_pick_valid(row.get("raw_event_id"), default=""))
    now_ict = datetime.now(tz=ICT_TZ).strftime("%H:%M:%S")

    return {
        "event_id": event_id,
        "raw_event_id": raw_event_id,
        "segment_id": seg_id,
        "street_name": street_name,
        "street_level": int(round(feat_dict["street_level"])),
        "date": date_str,
        "period": period_str,
        "ict_date": ict_date,
        "vn_hour": vn_hour,
        "mean_velocity": round(float(feat_dict["mean_velocity"]), 2),
        "max_velocity": round(float(feat_dict["max_velocity"]), 1),
        "current_los": LOS_LABELS[cur_ord],
        "predicted_next_los": LOS_LABELS[pred_ord],
        "cal_predicted_los": LOS_LABELS[cal_ord],
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
        "pending_seek_to_end": False,
        "stale_reset_detected": False,
        "batches_processed": 0,
        "messages_consumed": 0,
        "partition_counts": {"0": 0, "1": 0, "2": 0},
        "quarantined_count": 0,
        "sink1_bronze_total": 0,
        "duplicates_caught_bloom": 0,
        "true_duplicates_caught": 0,
        "bloom_false_positives_eval": 0,
        "late_dropped_for_tier2": 0,
        "sink2_scored_v1": 0,
        "severe_alerts_v1_count": 0,
        "v2_guard_protected_count": 0,
        "max_event_ts": None,
        "rotating_bloom": create_rotating_bloom(),
        "hll_registers": np.zeros(HLL_NUM_REGISTERS, dtype=np.uint8),
        "unique_seen_flag": False,
        "decay_scores": {},
        "decay_last_ts": {},
        "recent_scored_docs": [],
        "story_cursor": None,
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


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, path)


def _append_bronze_wal(bronze_wal_path: Path, raw_rows: list[dict[str, Any]]) -> None:
    if not raw_rows:
        return
    bronze_wal_path.parent.mkdir(parents=True, exist_ok=True)
    with bronze_wal_path.open("a", encoding="utf-8") as fh:
        for rec in raw_rows:
            fh.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        fh.flush()


def reset_live_stream_state(
    mongo_uri: str = "mongodb://127.0.0.1:27017",
    telemetry_path: Path = DEFAULT_TELEMETRY_PATH,
    scores_json_path: Path = DEFAULT_SCORES_JSON_PATH,
    reset_signal_path: Path = DEFAULT_RESET_SIGNAL_PATH,
    bronze_wal_path: Path | None = None,
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
        "story_cursor": None,
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
    _atomic_write_text(reset_signal_path, json.dumps({"reset_epoch": new_epoch}))
    _atomic_write_text(telemetry_path, json.dumps(empty_snap, ensure_ascii=False, indent=2))
    _atomic_write_text(scores_json_path, "[]")
    eff_bronze = bronze_wal_path or (
        DEFAULT_BRONZE_WAL_PATH
        if telemetry_path == DEFAULT_TELEMETRY_PATH
        else telemetry_path.parent / "live_bronze_wal.jsonl"
    )
    _atomic_write_text(eff_bronze, "")
    return empty_snap


def _build_mongo_live_guard_ops(scored_docs: list[dict[str, Any]]) -> list[Any]:
    import pymongo  # type: ignore[import-not-found]

    return [
        pymongo.UpdateOne(
            {"event_id": d["event_id"]},
            [
                {
                    "$replaceRoot": {
                        "newRoot": {
                            "$cond": {
                                "if": {
                                    "$lte": [
                                        {"$ifNull": ["$status_version", 0]},
                                        int(d.get("status_version", 1)),
                                    ]
                                },
                                "then": {"$mergeObjects": ["$$ROOT", {"$literal": d}]},
                                "else": "$$ROOT",
                            }
                        }
                    }
                }
            ],
            upsert=True,
        )
        for d in scored_docs
    ]


def _build_mongo_traffic_scores_ops(scored_docs: list[dict[str, Any]]) -> list[Any]:
    import pymongo  # type: ignore[import-not-found]

    return [
        pymongo.UpdateOne(
            {"event_id": d["event_id"], "status_version": {"$lte": 1}},
            {"$set": d},
            upsert=False,
        )
        for d in scored_docs
    ]


def _count_v2_protected(db: Any, eids: list[str]) -> int:
    v2_eids: set[str] = set()
    for coll_name in ("traffic_scores", "live_stream_scores"):
        v2_eids.update(
            str(doc["event_id"])
            for doc in db[coll_name].find(
                {"event_id": {"$in": eids}, "status_version": {"$gte": 2}},
                {"_id": 0, "event_id": 1},
            )
        )
    return len(v2_eids)


def _persist_batch_to_mongo_and_files(
    scored_docs: list[dict[str, Any]],
    state: dict[str, Any],
    mongo_client: Any | None,
    telemetry_path: Path,
    scores_json_path: Path,
    reset_signal_path: Path,
) -> bool:
    """Atomically verify reset_epoch, apply MongoDB version-guarded writes, and flush telemetry."""
    latest_epoch = _read_current_reset_epoch(mongo_client, reset_signal_path=reset_signal_path)
    if latest_epoch != int(state.get("reset_epoch", 0)):
        state["stale_reset_detected"] = True
        return False

    mongo_ok = mongo_client is not None
    if mongo_client is not None and scored_docs:
        db = mongo_client["bda501"]
        eids = [d["event_id"] for d in scored_docs]
        state["v2_guard_protected_count"] += _count_v2_protected(db, eids)
        db["live_stream_scores"].bulk_write(_build_mongo_live_guard_ops(scored_docs), ordered=False)
        db["traffic_scores"].bulk_write(_build_mongo_traffic_scores_ops(scored_docs), ordered=False)

    has_hll_items = bool(state.get("unique_seen_flag", False) or np.any(state["hll_registers"] > 0))
    hll_est = round(hll_estimate(state["hll_registers"]), 1) if has_hll_items else 0.0
    max_ts = state.get("max_event_ts")
    wm_iso = (pd.Timestamp(max_ts) - timedelta(minutes=30)).isoformat() if max_ts is not None else None
    snap = {
        "status": "STREAMING_LIVE",
        "topic": state["topic"],
        "reset_epoch": int(state.get("reset_epoch", 0)),
        "batches_processed": state["batches_processed"],
        "messages_consumed": state["messages_consumed"],
        "partition_counts": dict(state["partition_counts"]),
        "quarantined_count": state["quarantined_count"],
        "sink1_bronze_total": state["sink1_bronze_total"],
        "duplicates_caught_bloom": state["duplicates_caught_bloom"],
        "bloom_false_positives_eval": state["bloom_false_positives_eval"],
        "late_dropped_for_tier2": state["late_dropped_for_tier2"],
        "sink2_scored_v1": state["sink2_scored_v1"],
        "severe_alerts_v1_count": state["severe_alerts_v1_count"],
        "v2_guard_protected_count": state["v2_guard_protected_count"],
        "current_watermark_utc": wm_iso,
        "bloom_memory_kb": round(state["rotating_bloom"].nbytes / 1024.0, 1),
        "hll_memory_kb": round(state["hll_registers"].nbytes / 1024.0, 1),
        "hll_estimated_segments": hll_est,
        "active_decay_segments_in_ram": len(state["decay_scores"]),
        "story_cursor": state.get("story_cursor"),
        "mongo_connected": mongo_ok,
        "last_updated_ict": datetime.now(tz=ICT_TZ).strftime("%H:%M:%S"),
    }
    if mongo_client is not None:
        mongo_client["bda501"]["live_stream_telemetry"].replace_one(
            {"_id": "current"},
            {"_id": "current", **snap},
            upsert=True,
        )
    _atomic_write_text(telemetry_path, json.dumps(snap, ensure_ascii=False, indent=2))
    _atomic_write_text(scores_json_path, json.dumps(state["recent_scored_docs"][:50], ensure_ascii=False, indent=2))
    return True


def _update_story_cursor(state: dict[str, Any], row: dict[str, Any]) -> None:
    scen = _pick_valid(row.get("story_scenario"), default=None)
    if scen is None:
        return
    ict_cal = _compute_ict_calendar_features(row)
    state["story_cursor"] = {
        "scenario": str(scen),
        "frame_idx": int(_pick_valid(row.get("story_frame_idx"), default=1)),
        "total_frames": int(_pick_valid(row.get("story_total_frames"), default=1)),
        "date": str(_pick_valid(row.get("date"), default="")),
        "period": str(_pick_valid(row.get("period"), default="")),
        "ict_date": str(_pick_valid(row.get("ict_date"), default=ict_cal["ict_date"])),
        "vn_hour": float(_pick_valid(row.get("vn_hour"), default=ict_cal["vn_hour"])),
        "spotlight_segment_id": int(_pick_valid(row.get("spotlight_segment_id"), row.get("segment_id"), default=21262)),
    }


def _record_live_bloom_audit(row: dict[str, Any], state: dict[str, Any]) -> None:
    dup_val = row.get("is_duplicate")
    if not _is_valid_value(dup_val):
        return
    is_dup = dup_val.strip().lower() == "true" if isinstance(dup_val, str) else bool(dup_val)
    if is_dup:
        state["true_duplicates_caught"] += 1
    else:
        state["bloom_false_positives_eval"] += 1


def _process_live_accepted_row(
    row: dict[str, Any], state: dict[str, Any], scorer_ctx: dict[str, Any]
) -> dict[str, Any] | None:
    _update_story_cursor(state, row)
    part_key = str(int(_pick_valid(row.get("_kpart"), row.get("kafka_partition"), default=0)) % 3)
    state["partition_counts"][part_key] = state["partition_counts"].get(part_key, 0) + 1
    state["sink1_bronze_total"] += 1

    ev_ts = _parse_slot_utc(row)
    if ev_ts is not None and (state["max_event_ts"] is None or ev_ts > state["max_event_ts"]):
        state["max_event_ts"] = ev_ts

    raw_eid = str(_pick_valid(row.get("raw_event_id"), row.get("event_id"), default=f"{row['segment_id']}_{ev_ts}"))
    if rotating_bloom_check_and_add(state["rotating_bloom"], raw_eid):
        state["duplicates_caught_bloom"] += 1
        _record_live_bloom_audit(row, state)
        return None

    max_ts = state["max_event_ts"]
    if max_ts is not None and ev_ts is not None and ev_ts < (max_ts - timedelta(minutes=30)):
        state["late_dropped_for_tier2"] += 1
        return None

    seg_id = int(float(str(row["segment_id"])))
    hll_add(state["hll_registers"], str(seg_id), p_bits=HLL_P_BITS)
    state["unique_seen_flag"] = True

    vel = float(_pick_valid(row.get("mean_velocity"), row.get("velocity"), default=25.0))
    is_sev = int(_pick_valid(row.get("is_severe"), default=(1 if vel < 15.0 else 0)))
    update_bounded_decay_state(
        state["decay_scores"],
        seg_id,
        is_sev,
        event_ts=ev_ts,
        last_ts_map=state["decay_last_ts"],
    )

    doc = _score_single_event(row, scorer_ctx, state["decay_scores"].get(seg_id, 0.0), int(part_key))
    state["sink2_scored_v1"] += 1
    doc["seq_id"] = int(state["sink2_scored_v1"])
    if doc["severe_prob"] >= 0.50 or doc["predicted_next_los"] in ("E", "F"):
        state["severe_alerts_v1_count"] += 1
    return doc


def process_kafka_records_batch(
    records: list[tuple[int, dict[str, Any]]],
    state: dict[str, Any],
    scorer_ctx: dict[str, Any],
    mongo_client: Any | None = None,
    telemetry_path: Path = DEFAULT_TELEMETRY_PATH,
    scores_json_path: Path = DEFAULT_SCORES_JSON_PATH,
    bronze_wal_path: Path | None = None,
    reset_signal_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Process one micro-batch of `(partition, payload)` events through Dual-Sink + 33 KiB Sketches + RF."""
    if not records:
        return []
    eff_bronze_wal = bronze_wal_path or (
        DEFAULT_BRONZE_WAL_PATH
        if telemetry_path == DEFAULT_TELEMETRY_PATH
        else telemetry_path.parent / "live_bronze_wal.jsonl"
    )
    eff_reset_signal = reset_signal_path or (
        DEFAULT_RESET_SIGNAL_PATH
        if telemetry_path == DEFAULT_TELEMETRY_PATH
        else telemetry_path.parent / "live_reset_signal.json"
    )
    rows_with_part = [{**payload, "_kpart": int(part)} for part, payload in records]
    _append_bronze_wal(eff_bronze_wal, rows_with_part)

    state["batches_processed"] += 1
    state["messages_consumed"] += len(records)
    accepted_df, rejected_df = validate_stream_batch_ordered(pd.DataFrame(rows_with_part))
    state["quarantined_count"] += int(len(rejected_df))

    scored_docs: list[dict[str, Any]] = []
    for row in accepted_df.to_dict(orient="records"):
        doc = _process_live_accepted_row(row, state, scorer_ctx)
        if doc is not None:
            scored_docs.append(doc)

    evict_stale_decay_segments(state["decay_scores"], state["decay_last_ts"], state["max_event_ts"])
    if scored_docs:
        state["recent_scored_docs"] = (scored_docs[::-1] + state["recent_scored_docs"])[:60]
    persisted = _persist_batch_to_mongo_and_files(
        scored_docs, state, mongo_client, telemetry_path, scores_json_path, eff_reset_signal
    )
    return scored_docs if persisted else []


def _maybe_apply_reset_signal(state: dict[str, Any], mongo_client: Any | None, consumer: Any) -> dict[str, Any]:
    """Detect UI reset signal and clear in-memory 33 KiB sketches + advance Kafka offsets cleanly."""
    assigned = consumer.assignment()
    if state.get("pending_seek_to_end") and assigned:
        consumer.seek_to_end(*assigned)
        state["pending_seek_to_end"] = False

    latest_epoch = _read_current_reset_epoch(mongo_client)
    if latest_epoch == int(state.get("reset_epoch", 0)) and not state.get("stale_reset_detected"):
        return state

    new_state = init_live_state(topic=state["topic"], reset_epoch=latest_epoch)
    if assigned:
        consumer.seek_to_end(*assigned)
        new_state["pending_seek_to_end"] = False
    else:
        new_state["pending_seek_to_end"] = True
    print(f"[LiveReset] Cleared in-memory Bloom/64-bit HLL sketches (epoch={latest_epoch})", flush=True)
    return new_state


def _extract_polled_batch(polled: dict[Any, list[Any]]) -> list[tuple[int, dict[str, Any]]]:
    batch: list[tuple[int, dict[str, Any]]] = []
    for tp, msgs in polled.items():
        for msg in msgs:
            if isinstance(msg.value, dict):
                batch.append((int(tp.partition), msg.value))
    return batch


def _log_live_batch(state: dict[str, Any], batch_len: int, mongo_connected: bool) -> None:
    p_str = ",".join(f"P{k}={v}" for k, v in sorted(state["partition_counts"].items()))
    print(
        f"[LiveBatch #{state['batches_processed']}] +{batch_len} msgs "
        f"(Total={state['messages_consumed']} | {p_str}) | "
        f"BloomDup={state['duplicates_caught_bloom']} | Late>30m={state['late_dropped_for_tier2']} | "
        f"Scored_v1={state['sink2_scored_v1']} -> Mongo={mongo_connected}",
        flush=True,
    )


def _run_consumer_poll_step(
    state: dict[str, Any],
    mongo_client: Any | None,
    consumer: Any,
    scorer_ctx: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    state = _maybe_apply_reset_signal(state, mongo_client, consumer)
    was_pending_seek = bool(state.get("pending_seek_to_end", False))
    polled = consumer.poll(timeout_ms=700, max_records=25)
    if was_pending_seek and consumer.assignment():
        consumer.seek_to_end(*consumer.assignment())
        state["pending_seek_to_end"] = False
        return state, False
    batch = _extract_polled_batch(polled)
    if not batch:
        return state, False
    process_kafka_records_batch(batch, state, scorer_ctx, mongo_client)
    if state.get("stale_reset_detected"):
        return _maybe_apply_reset_signal(state, mongo_client, consumer), True
    consumer.commit()
    _log_live_batch(state, len(batch), mongo_client is not None)
    return state, True


def _create_live_consumer(
    bootstrap_servers: str, topic: str, group_id: str, reset_live: bool, auto_offset_reset: str
) -> Any:
    from kafka import KafkaConsumer  # type: ignore[import-not-found]

    brokers = [b.strip().replace("localhost:", "127.0.0.1:") for b in bootstrap_servers.split(",") if b.strip()]
    return KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=f"{group_id}-{int(time.time())}" if reset_live else group_id,
        auto_offset_reset="latest" if reset_live else auto_offset_reset,
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


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
    if reset_live:
        reset_live_stream_state(mongo_uri=mongo_uri)

    scorer_ctx = load_readonly_scorer_context()
    mongo_client = _open_mongo_live(mongo_uri)
    state = init_live_state(topic=topic, reset_epoch=_read_current_reset_epoch(mongo_client))
    consumer = _create_live_consumer(bootstrap_servers, topic, group_id, reset_live, auto_offset_reset)
    last_msg_ts = time.monotonic()
    try:
        while True:
            state, had_batch = _run_consumer_poll_step(state, mongo_client, consumer, scorer_ctx)
            if had_batch:
                last_msg_ts = time.monotonic()
                if 0 < max_messages <= state["messages_consumed"]:
                    break
            elif not continuous and (time.monotonic() - last_msg_ts) >= idle_timeout_sec:
                break
    finally:
        consumer.close()
        if mongo_client is not None:
            mongo_client.close()
    return json.loads(DEFAULT_TELEMETRY_PATH.read_text(encoding="utf-8")) if DEFAULT_TELEMETRY_PATH.is_file() else {}
