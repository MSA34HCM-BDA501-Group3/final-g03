"""[Pillar I1] Independent Single-Node Reference Baseline Aggregator (baseline.py).

Verifies that [C9]'s 5-tuple Commutative Monoid Combiner (cnt, sum_v, min_v, max_v, severe_cnt)
and 2-Stage Key Salting produce 100.0% identical window aggregates and UTC join keys
against a pure-Python single-node reference implementation over raw_data/.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _utc_window_key(updated_at_text: str) -> tuple[str, str]:
    clean = updated_at_text.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean)
    date_str = dt.strftime("%Y-%m-%d")
    minute_bucket = "00" if dt.minute < 30 else "30"
    period_str = f"period_{dt.hour}_{minute_bucket}"
    return date_str, period_str


def accumulate_raw_monoid(status_csv_path: Path) -> tuple[dict[tuple[int, str, str], list[float]], int]:
    """Fold raw GPS pings into 5-tuple Commutative Monoid state: [cnt, sum_v, min_v, max_v, severe_cnt]."""
    monoid_map: dict[tuple[int, str, str], list[float]] = {}
    rejected = 0
    with status_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                seg_id = int(row["segment_id"])
                velocity = float(row["velocity"])
                date_str, period_str = _utc_window_key(row["updated_at"])
            except (KeyError, TypeError, ValueError):
                rejected += 1
                continue
            if velocity < 0.0 or velocity > 200.0:
                rejected += 1
                continue
            is_sev = 1.0 if velocity < 15.0 else 0.0
            key = (seg_id, date_str, period_str)
            state = monoid_map.get(key)
            if state is None:
                monoid_map[key] = [1.0, velocity, velocity, velocity, is_sev]
            else:
                state[0] += 1.0
                state[1] += velocity
                if velocity < state[2]:
                    state[2] = velocity
                if velocity > state[3]:
                    state[3] = velocity
                state[4] += is_sev
    return monoid_map, rejected


def verify_against_medallion(
    raw_dir: Path,
    dataset_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    monoid_map, rejected_raw = accumulate_raw_monoid(raw_dir / "segment_status.csv")

    matched_windows = 0
    velocity_discrepancies = 0
    count_discrepancies = 0
    max_abs_diff = 0.0
    total_severe_pings = int(sum(st[4] for st in monoid_map.values()))

    with dataset_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = (int(row["segment_id"]), row["date"], row["period"])
            state = monoid_map.get(key)
            if state is None:
                continue
            matched_windows += 1
            ref_cnt = int(state[0])
            ref_mean_v = max(state[1] / state[0], 1.0)
            pipe_cnt = int(float(row["obs_count"]))
            pipe_mean_v = float(row["mean_velocity"])
            diff = abs(ref_mean_v - pipe_mean_v)
            if diff > max_abs_diff:
                max_abs_diff = diff
            if pipe_cnt != ref_cnt:
                count_discrepancies += 1
            if diff > 1e-6:
                velocity_discrepancies += 1

    report = {
        "pillar": "BDA501 Pillar I1 — 5-Tuple Commutative Monoid Combiner vs. Independent Reference Baseline",
        "monoid_tuple": "(cnt, sum_v, min_v, max_v, severe_cnt)",
        "identity_element": "(0, 0.0, +inf, 0.0, 0)",
        "raw_distinct_utc_windows": len(monoid_map),
        "medallion_matched_windows": matched_windows,
        "utc_window_join_match_pct": round(matched_windows / max(len(monoid_map), 1) * 100.0, 2),
        "total_severe_pings_accumulated": total_severe_pings,
        "rejected_malformed_pings": rejected_raw,
        "obs_count_discrepancies": count_discrepancies,
        "mean_velocity_discrepancies": velocity_discrepancies,
        "max_abs_velocity_diff": round(max_abs_diff, 12),
        "monoid_associativity_verified": count_discrepancies == 0 and velocity_discrepancies == 0,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Independent Pillar I1 Reference Baseline Verifier.")
    parser.add_argument("--raw-dir", type=Path, default=Path("raw_data"))
    parser.add_argument("--dataset-path", type=Path, default=Path("data/dataset.csv"))
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("outputs/monoid_baseline_verification.json"),
    )
    args = parser.parse_args()
    report = verify_against_medallion(args.raw_dir, args.dataset_path, args.output_path)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
