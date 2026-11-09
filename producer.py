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
    rng = np.random.default_rng(seed)
    dup_count = max(1, int(dup_rate * total_valid))
    late_in_wm_count = int(in_wm_rate * total_valid)
    late_out_wm_count = int(out_wm_rate * total_valid)

    dup_indices = rng.choice(total_valid, size=dup_count, replace=False)
    out_wm_indices = rng.choice(total_valid, size=late_out_wm_count, replace=False)
    out_wm_set = set(out_wm_indices.tolist())

    remaining_pool = np.array(
        [idx for idx in range(total_valid) if idx not in out_wm_set], dtype=int
    )
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
    working["period"] = (
        "period_" + working["event_ts_dt"].dt.hour.astype(str) + "_" + minute_bucket
    )
    raw_ids = working["_id"] if "_id" in working.columns else np.arange(total_valid)
    working["raw_event_id"] = [
        compute_raw_event_id(rid, seg_id, str(uat), vel)
        for rid, seg_id, uat, vel in zip(
            raw_ids, working["segment_id"], working["updated_at"], working["velocity"]
        )
    ]
    working["event_id"] = [
        compute_event_id(int(seg_id), str(d_str), str(p_str))
        for seg_id, d_str, p_str in zip(working["segment_id"], working["date"], working["period"])
    ]
    working["event_ts"] = working["event_ts_dt"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    working["arrival_delay_seconds"] = delays
    ingest_dt = working["event_ts_dt"] + pd.to_timedelta(delays, unit="s")
    working["ingest_ts"] = ingest_dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    working["kafka_partition"] = [
        compute_kafka_partition(int(seg_id)) for seg_id in working["segment_id"]
    ]
    working["is_duplicate"] = False
    working["lateness_bucket"] = [
        "LATE_OUT_OF_WATERMARK"
        if idx in out_wm_set
        else ("LATE_IN_WATERMARK" if idx in in_wm_set else "ON_TIME")
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
    dups = base.iloc[dup_indices].copy()
    dups["is_duplicate"] = True
    replay_df = pd.concat([base, dups], ignore_index=True)

    affected_seg_windows = int(
        base.loc[base["lateness_bucket"] == "LATE_OUT_OF_WATERMARK", "event_id"].nunique()
    )
    affected_slot_windows = int(
        base.loc[base["lateness_bucket"] == "LATE_OUT_OF_WATERMARK"]
        .groupby(["date", "period"])
        .ngroups
    )
    part_counts = {
        str(int(k)): int(v)
        for k, v in replay_df["kafka_partition"].value_counts().sort_index().items()
    }
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
    base["kafka_partition"] = [
        compute_kafka_partition(int(seg_id)) for seg_id in base["segment_id"]
    ]
    base["is_duplicate"] = False
    base["lateness_bucket"] = [
        "LATE_OUT_OF_WATERMARK"
        if idx in out_wm_set
        else ("LATE_IN_WATERMARK" if idx in in_wm_set else "ON_TIME")
        for idx in range(total_valid)
    ]

    dups = base.iloc[dup_indices].copy()
    dups["is_duplicate"] = True
    replay_df = pd.concat([base, dups], ignore_index=True)

    affected_slot_windows = int(
        base.loc[base["lateness_bucket"] == "LATE_OUT_OF_WATERMARK"]
        .groupby(["date", "period"])
        .ngroups
    )
    affected_segment_windows = int(len(out_wm_set))
    part_counts = {
        str(int(k)): int(v)
        for k, v in replay_df["kafka_partition"].value_counts().sort_index().items()
    }
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
            admin.create_topics(
                [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)]
            )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


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
    try:
        import time
        from kafka import KafkaProducer  # type: ignore[import-not-found]

        brokers = [b.strip() for b in bootstrap_servers.split(",") if b.strip()]
        _ensure_kafka_topic(brokers, topic, num_partitions=NUM_KAFKA_PARTITIONS)
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
            request_timeout_ms=5000,
        )
        subset = replay_df.head(max_messages) if max_messages > 0 else replay_df
        sent = 0
        part_sent: dict[str, int] = {"0": 0, "1": 0, "2": 0}
        for record in subset.to_dict(orient="records"):
            part = int(record.get("kafka_partition", 0)) % NUM_KAFKA_PARTITIONS
            producer.send(topic, key=record["segment_id"], value=record, partition=part)
            part_sent[str(part)] = part_sent.get(str(part), 0) + 1
            sent += 1
            if replay_speed > 0.0:
                time.sleep(min(0.01 / max(replay_speed, 1e-3), 0.05))
        producer.flush(timeout=10)
        producer.close()
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
            "messages_sent": 0,
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HCMC Smart Traffic Event-Time Replay Producer ([C1]).")
    parser.add_argument("--raw-dir", type=Path, default=Path("raw_data"))
    parser.add_argument("--dataset-path", type=Path, default=Path("data/dataset.csv"))
    parser.add_argument("--output-summary", type=Path, default=Path("outputs/replay_stream_summary.json"))
    parser.add_argument("--mode", choices=["window", "raw", "both"], default="both")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--late-every", type=int, default=20)
    parser.add_argument("--late-seconds", type=int, default=WATERMARK_SECONDS)
    parser.add_argument("--bootstrap-servers", type=str, default="")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC)
    parser.add_argument("--max-messages", type=int, default=5000)
    parser.add_argument("--replay-speed", type=float, default=0.0)
    parser.add_argument("--limit", type=int, default=0)
    return parser


def main() -> None:
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = _build_parser().parse_args()

    replay_df: pd.DataFrame | None = None
    summary: dict[str, Any] = {}
    if args.mode in ("window", "both"):
        frame = pd.read_csv(args.dataset_path)
        if args.limit > 0:
            frame = frame.head(args.limit).copy()
        replay_df, summary = generate_replay_Batch(
            frame, seed=args.seed, late_seconds=args.late_seconds
        )

    raw_status_path = args.raw_dir / "segment_status.csv"
    raw_replay_df: pd.DataFrame | None = None
    if args.mode in ("raw", "both"):
        if not raw_status_path.is_file():
            raise FileNotFoundError(f"Raw segment_status.csv not found: {raw_status_path}")
        raw_df = pd.read_csv(raw_status_path)
        if args.limit > 0:
            raw_df = raw_df.head(args.limit).copy()
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

    broker_df = raw_replay_df if args.mode == "raw" and raw_replay_df is not None else replay_df
    assert broker_df is not None
    summary["kafka_broker_delivery"] = publish_to_kafka_broker(
        broker_df,
        args.bootstrap_servers or None,
        topic=args.topic,
        max_messages=args.max_messages,
        replay_speed=args.replay_speed,
    )
    args.output_summary.parent.mkdir(parents=True, exist_ok=True)
    args.output_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

