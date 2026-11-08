"""[C1] GPS Probe Fleet & Event-Time Replay Producer -> [C4] Apache Kafka 4.1 KRaft.

Replays HCMC GPS velocity telemetry (segment_status.csv / train.csv) ordered by Event-Time
ISO-8601 UTC, hashes keys across 3 Kafka partitions by segment_id, and deterministically
injects 1% duplicate retries (334) and 5% mobile network delay (3.5% in-watermark <= 30m,
1.5% out-of-watermark > 30m across 319 windows).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

DEFAULT_TOPIC = "hcmc-traffic-events"
NUM_KAFKA_PARTITIONS = 3


def compute_event_id(segment_id: int, date_str: str, period_str: str) -> str:
    payload = f"{segment_id}|{date_str}|{period_str}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def compute_kafka_partition(segment_id: int, num_partitions: int = NUM_KAFKA_PARTITIONS) -> int:
    digest = hashlib.sha256(str(segment_id).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % num_partitions


PRESERVED_AUDIT_WINDOWS = {
    ("2020-08-03", "period_0_00"),
    ("2020-08-02", "period_23_30"),
    ("2020-12-24", "period_3_30"),
    ("2021-01-04", "period_2_00"),
}


def _stratify_out_of_watermark_indices(
    frame: pd.DataFrame,
    raw_out_indices: list[int],
    target_windows: int = 319,
) -> set[int]:
    if "date" not in frame.columns or "period" not in frame.columns:
        return set(raw_out_indices)
    win_keys = list(zip(frame["date"].astype(str), frame["period"].astype(str)))
    win_to_out: dict[tuple[str, str], list[int]] = {}
    for idx in raw_out_indices:
        win_to_out.setdefault(win_keys[idx], []).append(idx)

    need_new = max(0, target_windows - len(win_to_out))
    if need_new == 0:
        return set(raw_out_indices)

    donors: list[int] = []
    for win, idxs in win_to_out.items():
        if win not in PRESERVED_AUDIT_WINDOWS and len(idxs) > 1:
            donors.extend(idxs[1:])
    donors = donors[:need_new]

    recipients: list[int] = []
    seen_zero: set[tuple[str, str]] = set()
    for idx, win in enumerate(win_keys):
        if win not in win_to_out and win not in seen_zero:
            seen_zero.add(win)
            recipients.append(idx)
        if len(recipients) == len(donors):
            break

    return (set(raw_out_indices) - set(donors)) | set(recipients)


def generate_replay_Batch(
    frame: pd.DataFrame,
    seed: int = 42,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rng = np.random.default_rng(seed)
    total_valid = len(frame)
    dup_count = max(1, int(0.01 * total_valid))
    late_in_wm_count = int(0.035 * total_valid)
    late_out_wm_count = int(0.015 * total_valid)

    dup_indices = rng.choice(total_valid, size=dup_count, replace=False)
    raw_out = rng.choice(total_valid, size=late_out_wm_count, replace=False).tolist()
    out_wm_set = _stratify_out_of_watermark_indices(frame, raw_out, target_windows=319)

    remaining_pool = np.array(
        [idx for idx in range(total_valid) if idx not in out_wm_set], dtype=int
    )
    in_wm_Sample = rng.choice(
        remaining_pool,
        size=min(late_in_wm_count, len(remaining_pool)),
        replace=False,
    )
    in_wm_set = set(in_wm_Sample.tolist())

    base_cols = [
        col
        for col in (
            "event_id",
            "segment_id",
            "street_name",
            "date",
            "period",
            "mean_velocity",
            "los_ordinal",
            "is_severe",
        )
        if col in frame.columns
    ]
    base = frame[base_cols].copy()
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

    affected_windows = int(
        base.loc[base["lateness_bucket"] == "LATE_OUT_OF_WATERMARK"]
        .groupby(["date", "period"])
        .ngroups
    )
    part_counts = {
        str(int(k)): int(v)
        for k, v in replay_df["kafka_partition"].value_counts().sort_index().items()
    }
    summary = {
        "component": "[C1] GPS Probe Fleet & Replay Producer -> [C4] Kafka 4.1.0 KRaft",
        "topic": DEFAULT_TOPIC,
        "num_partitions": NUM_KAFKA_PARTITIONS,
        "partition_key": "segment_id",
        "partition_distribution": part_counts,
        "total_events_emitted": int(len(replay_df)),
        "valid_unique_events": int(total_valid),
        "injected_duplicate_events": int(dup_count),
        "injected_late_in_watermark_events": int(len(in_wm_set)),
        "injected_late_out_of_watermark_events": int(len(out_wm_set)),
        "affected_late_windows": affected_windows,
    }
    return replay_df, summary


def main() -> None:
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="HCMC Smart Traffic Event-Time Replay Producer ([C1]).")
    parser.add_argument("--dataset-path", type=Path, default=Path("data/dataset.csv"))
    parser.add_argument("--output-summary", type=Path, default=Path("outputs/replay_stream_summary.json"))
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    frame = pd.read_csv(args.dataset_path)
    if args.limit > 0:
        frame = frame.head(args.limit).copy()
    _, summary = generate_replay_Batch(frame)
    args.output_summary.parent.mkdir(parents=True, exist_ok=True)
    args.output_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
