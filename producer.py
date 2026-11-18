"""[C1] GPS Probe Fleet & Event-Time Replay Producer -> [C4] Apache Kafka 4.1 KRaft.

Implements G01, G02, and M02:
- Canonical event schema distinguishing raw_event_id (per GPS ping) from event_id (30m window ID)
  so two valid pings in the same segment/slot are never deduplicated away, while injected duplicate
  retries keep the exact same raw_event_id.
- Supports both raw ping replay (90,938 pings from segment_status.csv) and window-level replay
  (33,441 windows) with explicit denominator documentation and deterministic RNG (seed=42),
  without artificial window hand-picking.
- Computes real event_ts (UTC) and ingest_ts = event_ts + arrival_delay_seconds.
- Publishes JSON payloads to Kafka 3-partition topic (key=segment_id) when broker is reachable,
  reporting broker delivery status honestly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from baseline import compute_raw_event_id

DEFAULT_TOPIC = "hcmc-traffic-events"
NUM_KAFKA_PARTITIONS = 3
WATERMARK_SECONDS = 1800


def compute_event_id(segment_id: int, date_str: str, period_str: str) -> str:
    """Compute 30-minute window identifier (window_id / event_id)."""
    payload = f"{segment_id}|{date_str}|{period_str}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def compute_kafka_partition(segment_id: int, num_partitions: int = NUM_KAFKA_PARTITIONS) -> int:
    digest = hashlib.sha256(str(segment_id).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % num_partitions


def _assign_delays_and_duplicates(
    total_valid: int,
    seed: int = 42,
    dup_rate: float = 0.01,
    in_wm_rate: float = 0.035,
    out_wm_rate: float = 0.015,
    late_seconds: int = WATERMARK_SECONDS,
) -> tuple[np.ndarray, set[int], set[int], np.ndarray]:
    if total_valid <= 0:
        return np.array([], dtype=int), set(), set(), np.array([], dtype=int)
    rng = np.random.default_rng(seed)
    dup_count = min(total_valid, max(1, int(dup_rate * total_valid)))
    late_in_wm_count = int(in_wm_rate * total_valid)
    late_out_wm_count = int(out_wm_rate * total_valid)

    dup_indices = rng.choice(total_valid, size=dup_count, replace=False)
    out_wm_indices = rng.choice(total_valid, size=late_out_wm_count, replace=False)
    out_wm_set = set(out_wm_indices.tolist())

    remaining_pool = np.array([idx for idx in range(total_valid) if idx not in out_wm_set], dtype=int)
    in_wm_indices = rng.choice(
        remaining_pool,
        size=min(late_in_wm_count, len(remaining_pool)),
        replace=False,
    )
    in_wm_set = set(in_wm_indices.tolist())

    delays = rng.integers(0, 15, size=total_valid).astype(int)
    if in_wm_set:
        in_list = sorted(in_wm_set)
        delays[in_list] = rng.integers(60, late_seconds + 1, size=len(in_list))
    if out_wm_set:
        out_list = sorted(out_wm_set)
        delays[out_list] = rng.integers(late_seconds + 60, late_seconds * 3 + 1, size=len(out_list))
    return dup_indices, in_wm_set, out_wm_set, delays


def _append_and_sort_duplicates(
    base: pd.DataFrame,
    dup_indices: np.ndarray,
    seed: int,
    ts_fmt: str,
) -> pd.DataFrame:
    """Assign realistic retry arrival timestamps (+5..30s) to duplicates and sort stream by ingest_ts."""
    if base.empty:
        return base.copy()
    if len(dup_indices) == 0:
        return base.sort_values(["ingest_ts", "segment_id"], kind="mergesort").reset_index(drop=True)

    dups = base.iloc[dup_indices].copy()
    dups["is_duplicate"] = True
    rng_dup = np.random.default_rng(seed + 1)
    retry_jitter = rng_dup.integers(5, 31, size=len(dups)).astype(int)
    dup_ingest_dt = pd.to_datetime(dups["ingest_ts"], utc=True) + pd.to_timedelta(retry_jitter, unit="s")
    dups["ingest_ts"] = dup_ingest_dt.dt.strftime(ts_fmt)
    dups["arrival_delay_seconds"] = dups["arrival_delay_seconds"].astype(int) + retry_jitter

    replay_df = pd.concat([base, dups], ignore_index=True)
    sort_cols = ["ingest_ts", "is_duplicate", "segment_id"]
    return replay_df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)


def generate_raw_ping_replay(
    status_df: pd.DataFrame,
    seed: int = 42,
    late_every: int = 20,
    late_seconds: int = WATERMARK_SECONDS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """[G01/G02/M02] Replay raw GPS pings (segment_status.csv) ordered by event_ts UTC."""
    working = status_df.copy()
    working["event_ts_dt"] = pd.to_datetime(working["updated_at"], utc=True, errors="coerce")
    working = working.dropna(subset=["event_ts_dt", "segment_id", "velocity"]).copy()
    working["segment_id"] = working["segment_id"].astype(int)
    working["velocity"] = working["velocity"].astype(float)
    working = working[(working["velocity"] >= 0.0) & (working["velocity"] <= 200.0)].copy()
    working = working.sort_values(["event_ts_dt", "segment_id"]).reset_index(drop=True)

    total_valid = len(working)
    total_late_rate = 1.0 / max(late_every, 1)
    in_wm_rate = round(total_late_rate * 0.70, 4)
    out_wm_rate = round(total_late_rate * 0.30, 4)
    dup_indices, in_wm_set, out_wm_set, delays = _assign_delays_and_duplicates(
        total_valid,
        seed=seed,
        dup_rate=0.01,
        in_wm_rate=in_wm_rate,
        out_wm_rate=out_wm_rate,
        late_seconds=late_seconds,
    )

    working["date"] = working["event_ts_dt"].dt.strftime("%Y-%m-%d")
    minute_bucket = np.where(working["event_ts_dt"].dt.minute < 30, "00", "30")
    working["period"] = "period_" + working["event_ts_dt"].dt.hour.astype(str) + "_" + minute_bucket
    raw_ids = working["_id"] if "_id" in working.columns else np.arange(total_valid)
    working["raw_event_id"] = [
        compute_raw_event_id(rid, seg_id, str(uat), vel)
        for rid, seg_id, uat, vel in zip(raw_ids, working["segment_id"], working["updated_at"], working["velocity"])
    ]
    working["event_id"] = [
        compute_event_id(int(seg_id), str(d_str), str(p_str))
        for seg_id, d_str, p_str in zip(working["segment_id"], working["date"], working["period"])
    ]
    working["event_ts"] = working["event_ts_dt"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    working["arrival_delay_seconds"] = delays
    ingest_dt = working["event_ts_dt"] + pd.to_timedelta(delays, unit="s")
    working["ingest_ts"] = ingest_dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    working["kafka_partition"] = [compute_kafka_partition(int(seg_id)) for seg_id in working["segment_id"]]
    working["is_duplicate"] = False
    working["lateness_bucket"] = [
        "LATE_OUT_OF_WATERMARK" if idx in out_wm_set else ("LATE_IN_WATERMARK" if idx in in_wm_set else "ON_TIME")
        for idx in range(total_valid)
    ]

    export_cols = [
        "raw_event_id",
        "event_id",
        "segment_id",
        "velocity",
        "updated_at",
        "event_ts",
        "ingest_ts",
        "arrival_delay_seconds",
        "date",
        "period",
        "kafka_partition",
        "is_duplicate",
        "lateness_bucket",
    ]
    base = working[export_cols].copy()
    replay_df = _append_and_sort_duplicates(base, dup_indices, seed=seed, ts_fmt="%Y-%m-%dT%H:%M:%S.%fZ")

    affected_seg_windows = int(base.loc[base["lateness_bucket"] == "LATE_OUT_OF_WATERMARK", "event_id"].nunique())
    affected_slot_windows = int(
        base.loc[base["lateness_bucket"] == "LATE_OUT_OF_WATERMARK"].groupby(["date", "period"]).ngroups
    )
    part_counts = {str(int(k)): int(v) for k, v in replay_df["kafka_partition"].value_counts().sort_index().items()}
    summary = {
        "replay_mode": "raw_gps_pings",
        "seed": seed,
        "late_every": late_every,
        "watermark_seconds": late_seconds,
        "total_raw_events_emitted": int(len(replay_df)),
        "valid_unique_raw_pings": int(total_valid),
        "unique_valid_events": int(total_valid),
        "injected_duplicate_raw_pings": int(len(dup_indices)),
        "injected_late_in_watermark_raw_pings": int(len(in_wm_set)),
        "injected_late_out_of_watermark_raw_pings": int(len(out_wm_set)),
        "affected_segment_windows_by_late_pings": affected_seg_windows,
        "affected_time_slots_by_late_pings": affected_slot_windows,
        "partition_distribution": part_counts,
    }
    return replay_df, summary


def generate_replay_Batch(
    frame: pd.DataFrame,
    seed: int = 42,
    late_seconds: int = WATERMARK_SECONDS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """[G01/G02] Window-level replay stream with explicit event_ts, ingest_ts, and raw_event_id."""
    total_valid = len(frame)
    dup_indices, in_wm_set, out_wm_set, delays = _assign_delays_and_duplicates(
        total_valid,
        seed=seed,
        dup_rate=0.01,
        in_wm_rate=0.035,
        out_wm_rate=0.015,
        late_seconds=late_seconds,
    )

    extra_cols = (
        "street_level",
        "max_velocity",
        "LOS",
        "ict_date",
        "vn_hour",
        "long_snode",
        "lat_snode",
        "long_enode",
        "lat_enode",
        "current_los_ordinal",
        "lag1_los_ordinal",
        "lag2_los_ordinal",
        "trailing_severe_rate",
        "velocity_ratio",
        "obs_count",
        "length_m",
        "weekday",
        "period_sin",
        "period_cos",
        "gap_minutes",
        "pagerank_score",
        "in_degree",
        "out_degree",
        "neighbor_los_ordinal",
        "cluster_id",
        "segment_train_prior_los",
        "segment_train_severe_prior",
        "street_causal_prior_los",
        "street_causal_prior_sev",
    )
    base_cols = [
        col
        for col in (
            "event_id",
            "segment_id",
            "street_name",
            "date",
            "period",
            "slot_ts",
            "mean_velocity",
            "los_ordinal",
            "is_severe",
            *extra_cols,
        )
        if col in frame.columns
    ]
    base = frame[base_cols].copy().reset_index(drop=True)
    if "event_id" not in base.columns:
        base["event_id"] = [
            compute_event_id(int(s), str(d), str(p))
            for s, d, p in zip(base["segment_id"], base["date"], base["period"])
        ]

    if "slot_ts" in base.columns:
        slot_dt = pd.to_datetime(base["slot_ts"], utc=True)
    else:
        parts = base["period"].str.replace("period_", "", regex=False).str.split("_", expand=True)
        p_idx = parts[0].astype(int) * 2 + (parts[1].astype(int) // 30)
        slot_dt = pd.to_datetime(base["date"], utc=True) + pd.to_timedelta(p_idx * 30, unit="min")

    base["event_ts"] = slot_dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    base["arrival_delay_seconds"] = delays
    ingest_dt = slot_dt + pd.to_timedelta(delays, unit="s")
    base["ingest_ts"] = ingest_dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    base["raw_event_id"] = [
        compute_raw_event_id(idx, seg_id, ets, vel)
        for idx, seg_id, ets, vel in zip(
            range(total_valid),
            base["segment_id"],
            base["event_ts"],
            base.get("mean_velocity", np.zeros(total_valid)),
        )
    ]
    base["kafka_partition"] = [compute_kafka_partition(int(seg_id)) for seg_id in base["segment_id"]]
    base["is_duplicate"] = False
    base["lateness_bucket"] = [
        "LATE_OUT_OF_WATERMARK" if idx in out_wm_set else ("LATE_IN_WATERMARK" if idx in in_wm_set else "ON_TIME")
        for idx in range(total_valid)
    ]

    replay_df = _append_and_sort_duplicates(base, dup_indices, seed=seed, ts_fmt="%Y-%m-%dT%H:%M:%SZ")

    affected_slot_windows = int(
        base.loc[base["lateness_bucket"] == "LATE_OUT_OF_WATERMARK"].groupby(["date", "period"]).ngroups
    )
    affected_segment_windows = int(len(out_wm_set))
    part_counts = {str(int(k)): int(v) for k, v in replay_df["kafka_partition"].value_counts().sort_index().items()}
    summary = {
        "component": "[C1] GPS Probe Fleet & Replay Producer -> [C4] Kafka 4.1.0 KRaft",
        "replay_mode": "window_aggregated_30m",
        "denominator_note": (
            "G02 Contract: Window-level replay operates on 33,441 aggregated 30m windows "
            "(1% = 334 duplicates, 3.5% = 1,170 late in-WM, 1.5% = 501 late out-of-WM). "
            "Raw ping replay operates on 90,938 raw segment_status pings."
        ),
        "seed": seed,
        "topic": DEFAULT_TOPIC,
        "num_partitions": NUM_KAFKA_PARTITIONS,
        "partition_key": "segment_id",
        "partition_distribution": part_counts,
        "total_events_emitted": int(len(replay_df)),
        "valid_unique_events": int(total_valid),
        "injected_duplicate_events": int(len(dup_indices)),
        "injected_late_in_watermark_events": int(len(in_wm_set)),
        "injected_late_out_of_watermark_events": int(len(out_wm_set)),
        "affected_late_windows": affected_slot_windows,
        "affected_late_segment_windows": affected_segment_windows,
    }
    return replay_df, summary


def _ensure_kafka_topic(brokers: list[str], topic: str, num_partitions: int = NUM_KAFKA_PARTITIONS) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore[import-not-found]
    from kafka.errors import TopicAlreadyExistsError  # type: ignore[import-not-found]

    admin = KafkaAdminClient(bootstrap_servers=brokers, request_timeout_ms=5000)
    try:
        existing = set(admin.list_topics())
        if topic not in existing:
            admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def _select_broker_demo_subset(replay_df: pd.DataFrame, max_messages: int) -> pd.DataFrame:
    """Select `max_messages` preserving deterministic duplicate retries even on short live demos."""
    if max_messages <= 0 or max_messages >= len(replay_df):
        return replay_df
    head_slice = replay_df.head(max_messages)
    existing_dups = int(head_slice["is_duplicate"].sum()) if "is_duplicate" in head_slice.columns else 0
    if max_messages < 10 or existing_dups >= 2:
        return head_slice
    dup_n = max(2, max_messages // 50)
    base_n = max_messages - dup_n
    if "is_duplicate" in replay_df.columns:
        non_dups = replay_df[~replay_df["is_duplicate"]]
        front = non_dups.head(base_n).copy() if len(non_dups) >= base_n else replay_df.head(base_n).copy()
    else:
        front = replay_df.head(base_n).copy()
    retries = front.iloc[:dup_n].copy()
    retries["is_duplicate"] = True
    if "ingest_ts" in retries.columns and not retries.empty:
        retries["ingest_ts"] = (
            pd.to_datetime(retries["ingest_ts"], utc=True) + pd.to_timedelta(10, unit="s")
        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return pd.concat([front, retries], ignore_index=True)


def _resolve_input_paths(dataset_path: Path, raw_dir: Path) -> tuple[Path, Path, bool]:
    """Resolve full dataset/raw paths, falling back cleanly to committed `data/demo_sample/`."""
    sample_dir = Path("data/demo_sample")
    win_path = dataset_path if dataset_path.is_file() else sample_dir / "sample_windows_23d.csv"
    raw_status = raw_dir / "segment_status.csv"
    raw_path = raw_status if raw_status.is_file() else sample_dir / "sample_raw_status.csv"
    used_sample = (win_path != dataset_path) or (raw_path != raw_status)
    return win_path, raw_path, used_sample


def _read_broker_partition_offsets(brokers: list[str], topic: str) -> dict[str, int]:
    from kafka import KafkaConsumer, TopicPartition  # type: ignore[import-not-found]

    consumer = KafkaConsumer(bootstrap_servers=brokers, request_timeout_ms=5000)
    try:
        parts = consumer.partitions_for_topic(topic) or {0, 1, 2}
        tps = [TopicPartition(topic, int(p)) for p in sorted(parts)]
        end_offsets = consumer.end_offsets(tps)
        return {str(tp.partition): int(offset) for tp, offset in end_offsets.items()}
    finally:
        consumer.close()


def _send_records_verified(
    producer: Any,
    records: list[dict[str, Any]],
    topic: str,
    part_sent: dict[str, int],
    sleep_sec: float = 0.0,
) -> int:
    """Send records to Kafka and verify every Future via fut.get(timeout=10) after flush."""
    import time

    futures = []
    for record in records:
        part = int(record.get("kafka_partition", 0)) % NUM_KAFKA_PARTITIONS
        fut = producer.send(topic, key=record["segment_id"], value=record, partition=part)
        futures.append(fut)
        part_sent[str(part)] = part_sent.get(str(part), 0) + 1
        if sleep_sec > 0.0:
            time.sleep(sleep_sec)
    producer.flush(timeout=10)
    for fut in futures:
        fut.get(timeout=10)
    return len(futures)


def publish_to_kafka_broker(
    replay_df: pd.DataFrame,
    bootstrap_servers: str | None,
    topic: str = DEFAULT_TOPIC,
    max_messages: int = 5000,
    replay_speed: float = 0.0,
) -> dict[str, Any]:
    """Attempt live delivery to Kafka broker when bootstrap_servers is provided (M02)."""
    if not bootstrap_servers:
        return {
            "status": "not_verified",
            "broker_connected": False,
            "reason": "No --bootstrap-servers specified (offline/file replay mode).",
            "messages_sent": 0,
        }
    producer = None
    sent = 0
    try:
        from kafka import KafkaProducer  # type: ignore[import-not-found]

        brokers = [b.strip().replace("localhost:", "127.0.0.1:") for b in bootstrap_servers.split(",") if b.strip()]
        _ensure_kafka_topic(brokers, topic, num_partitions=NUM_KAFKA_PARTITIONS)
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
            request_timeout_ms=5000,
        )
        subset = _select_broker_demo_subset(replay_df, max_messages)
        records = subset.to_dict(orient="records")
        part_sent: dict[str, int] = {"0": 0, "1": 0, "2": 0}
        sleep_sec = min(0.01 / max(replay_speed, 1e-3), 0.05) if replay_speed > 0.0 else 0.0
        batch_size = 500
        for start in range(0, len(records), batch_size):
            sent += _send_records_verified(producer, records[start : start + batch_size], topic, part_sent, sleep_sec)
        producer.close()
        producer = None
        end_offsets = _read_broker_partition_offsets(brokers, topic)
        return {
            "status": "verified",
            "broker_connected": True,
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "num_partitions": len(end_offsets),
            "messages_sent": sent,
            "partition_messages_sent": part_sent,
            "partition_end_offsets": end_offsets,
        }
    except Exception as exc:
        return {
            "status": "not_verified",
            "broker_connected": False,
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "reason": f"{type(exc).__name__}: {exc}",
            "messages_sent": sent,
        }
    finally:
        if producer is not None:
            try:
                producer.close()
            except Exception:
                pass


_FALLBACK_STORY_SCENARIO_FILTERS: dict[str, tuple[str, float, float, int]] = {
    "noon_transition": ("2021-01-05", 10.5, 15.0, 21262),
    "morning_rush_val": ("2020-12-24", 6.5, 10.5, 37420),
    "cong_truong_quoc_te": ("2021-04-22", 8.5, 12.0, 21064),
    "mega_city_rush": ("2020-08-03", 6.5, 7.0, 47407),
}


def _load_story_scenario_filters() -> dict[str, tuple[str, float, float, int]]:
    try:
        from toc_backend.story_scenarios import CURATED_SCENARIOS

        loaded = {
            str(item["id"]): (
                str(item["ict_date"]),
                float(item["hour_min"]),
                float(item["hour_max"]),
                int(item["spotlight_segment_id"]),
            )
            for item in CURATED_SCENARIOS
        }
        return loaded or dict(_FALLBACK_STORY_SCENARIO_FILTERS)
    except Exception:
        return dict(_FALLBACK_STORY_SCENARIO_FILTERS)


STORY_SCENARIO_FILTERS: dict[str, tuple[str, float, float, int]] = _load_story_scenario_filters()


def _validate_story_scenario(scenario: str) -> tuple[str, float, float, int]:
    if scenario not in STORY_SCENARIO_FILTERS:
        valid = ", ".join(sorted(STORY_SCENARIO_FILTERS))
        raise ValueError(f"Unknown story scenario {scenario!r}. Valid scenarios: {valid}")
    return STORY_SCENARIO_FILTERS[scenario]


def build_story_replay_frames(
    frame: pd.DataFrame,
    scenario: str = "noon_transition",
    max_per_slot: int = 45,
) -> tuple[list[pd.DataFrame], dict[str, Any]]:
    """Build ordered 30-minute chronological frames for Story-Driven Kafka Replay."""
    ict_date, h_min, h_max, spotlight_seg = _validate_story_scenario(scenario)
    sub = frame[(frame["ict_date"] == ict_date) & (frame["vn_hour"].between(h_min, h_max))].copy()
    if sub.empty:
        sub = frame.head(120).copy()
    sub = sub.sort_values(["slot_ts", "street_level", "segment_id"]).reset_index(drop=True)
    replay_all, _ = generate_replay_Batch(sub, seed=42)
    base_only = replay_all[~replay_all["is_duplicate"]].copy()

    slots = sorted(base_only["event_ts"].unique().tolist())
    frames: list[pd.DataFrame] = []
    for f_idx, ev_ts in enumerate(slots):
        slot_rows = base_only[base_only["event_ts"] == ev_ts].copy()
        must = slot_rows[slot_rows["segment_id"].isin([spotlight_seg, 21263, 22684, 36232, 63481, 47407])]
        rest = slot_rows[~slot_rows.index.isin(must.index)].head(max(5, max_per_slot - len(must)))
        picked = pd.concat([must, rest], ignore_index=True)
        picked["story_scenario"] = scenario
        picked["story_frame_idx"] = f_idx + 1
        picked["story_total_frames"] = len(slots)
        picked["spotlight_segment_id"] = spotlight_seg
        if f_idx == 1 and len(picked) >= 2:
            dup_row = picked.iloc[[0]].copy()
            dup_row["is_duplicate"] = True
            picked = pd.concat([picked, dup_row], ignore_index=True)
        frames.append(picked)
    meta = {
        "scenario": scenario,
        "ict_date": ict_date,
        "spotlight_segment_id": spotlight_seg,
        "frames_count": len(frames),
        "total_story_messages": int(sum(len(x) for x in frames)),
    }
    return frames, meta


def publish_story_frames_to_kafka(
    frames: list[pd.DataFrame],
    bootstrap_servers: str | None,
    topic: str = DEFAULT_TOPIC,
    frame_pause: float = 1.6,
) -> dict[str, Any]:
    """Stream chronological 30m frames step-by-step through Kafka 3 partitions."""
    if not bootstrap_servers:
        return {"status": "not_verified", "broker_connected": False, "messages_sent": 0}
    producer = None
    sent = 0
    try:
        import time
        from kafka import KafkaProducer  # type: ignore[import-not-found]

        brokers = [b.strip().replace("localhost:", "127.0.0.1:") for b in bootstrap_servers.split(",") if b.strip()]
        _ensure_kafka_topic(brokers, topic, num_partitions=NUM_KAFKA_PARTITIONS)
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
            request_timeout_ms=5000,
        )
        part_sent: dict[str, int] = {"0": 0, "1": 0, "2": 0}
        for idx, f_df in enumerate(frames):
            sent += _send_records_verified(producer, f_df.to_dict(orient="records"), topic, part_sent)
            if idx + 1 < len(frames) and frame_pause > 0.0:
                time.sleep(min(frame_pause, 5.0))
        producer.close()
        producer = None
        end_offsets = _read_broker_partition_offsets(brokers, topic)
        return {
            "status": "verified",
            "broker_connected": True,
            "mode": "story_chronological_frames",
            "frames_streamed": len(frames),
            "messages_sent": sent,
            "partition_messages_sent": part_sent,
            "partition_end_offsets": end_offsets,
        }
    except Exception as exc:
        return {
            "status": "not_verified",
            "broker_connected": False,
            "messages_sent": sent,
            "reason": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if producer is not None:
            try:
                producer.close()
            except Exception:
                pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HCMC Smart Traffic Event-Time Replay Producer ([C1]).")
    parser.add_argument("--raw-dir", type=Path, default=Path("raw_data"))
    parser.add_argument("--dataset-path", type=Path, default=Path("data/dataset.csv"))
    parser.add_argument("--output-summary", type=Path, default=Path("outputs/replay_stream_summary.json"))
    parser.add_argument("--mode", choices=["window", "raw", "both", "story"], default="both")
    parser.add_argument("--scenario", type=str, default="noon_transition")
    parser.add_argument("--frame-pause", type=float, default=1.6)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--late-every", type=int, default=20)
    parser.add_argument("--late-seconds", type=int, default=WATERMARK_SECONDS)
    parser.add_argument("--bootstrap-servers", type=str, default="")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC)
    parser.add_argument("--max-messages", type=int, default=5000)
    parser.add_argument("--replay-speed", type=float, default=0.0)
    parser.add_argument("--limit", type=int, default=0)
    return parser


def _load_capped_csv(path: Path, limit: int) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {path} (see docs/DATA_SETUP.md)")
    frame = pd.read_csv(path)
    return frame.head(limit).copy() if limit > 0 else frame


def _run_story_cli(args: argparse.Namespace, win_path: Path, used_sample: bool) -> dict[str, Any]:
    _validate_story_scenario(args.scenario)
    frame = _load_capped_csv(win_path, 0)
    story_frames, story_meta = build_story_replay_frames(frame, scenario=args.scenario)
    delivery = publish_story_frames_to_kafka(
        story_frames,
        args.bootstrap_servers or None,
        topic=args.topic,
        frame_pause=args.frame_pause,
    )
    return {
        "component": "[C1] Story-Driven Rush-Hour Replay Producer -> [C4] Kafka 4.1.0 KRaft",
        "replay_mode": "story_chronological_corridor",
        "data_source_mode": "demo_sample_fallback" if used_sample else "full_project_data",
        **story_meta,
        "kafka_broker_delivery": delivery,
    }


def main() -> None:
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = _build_parser().parse_args()
    win_path, raw_status_path, used_sample = _resolve_input_paths(args.dataset_path, args.raw_dir)

    if args.mode == "story":
        summary = _run_story_cli(args, win_path, used_sample)
        out_story = Path("outputs/live_producer_summary.json")
        out_story.parent.mkdir(parents=True, exist_ok=True)
        out_story.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    replay_df: pd.DataFrame | None = None
    summary: dict[str, Any] = {}
    if args.mode in ("window", "both"):
        frame = _load_capped_csv(win_path, args.limit)
        replay_df, summary = generate_replay_Batch(frame, seed=args.seed, late_seconds=args.late_seconds)

    raw_replay_df: pd.DataFrame | None = None
    if args.mode in ("raw", "both"):
        raw_df = _load_capped_csv(raw_status_path, args.limit)
        raw_replay_df, raw_summary = generate_raw_ping_replay(
            raw_df,
            seed=args.seed,
            late_every=args.late_every,
            late_seconds=args.late_seconds,
        )
        if args.mode == "raw":
            summary = dict(raw_summary)
        else:
            summary["raw_ping_replay_summary"] = raw_summary

    summary["data_source_mode"] = "demo_sample_fallback" if used_sample else "full_project_data"
    broker_df = raw_replay_df if args.mode == "raw" and raw_replay_df is not None else replay_df
    assert broker_df is not None
    summary["kafka_broker_delivery"] = publish_to_kafka_broker(
        broker_df,
        args.bootstrap_servers or None,
        topic=args.topic,
        max_messages=args.max_messages,
        replay_speed=args.replay_speed,
    )
    out_path = args.output_summary
    if out_path == Path("outputs/replay_stream_summary.json") and (
        used_sample or (0 < args.max_messages < 5000) or args.topic != DEFAULT_TOPIC
    ):
        out_path = Path("outputs/live_producer_summary.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
