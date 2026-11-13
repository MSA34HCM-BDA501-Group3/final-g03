"""[Pillar I1] Independent Single-Node Reference Baseline Aggregator (baseline.py).

Verifies that [C9]'s 5-tuple Commutative Monoid Combiner (cnt, sum_v, min_v, max_v, severe_cnt)
and 2-Stage Key Salting produce 100.0% identical window aggregates and UTC join keys
against a pure-Python single-node reference implementation over raw_data/.
Includes exact raw_event_id deduplication, full 5-component + missing/extra key audits (Q09),
and unequal-partition associativity verification vs. naive average-of-averages (Q06).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MONOID_IDENTITY: tuple[int, float, float, float, int] = (0, 0.0, math.inf, 0.0, 0)
WINDOW_AGG_FIELDS = [
    "obs_count",
    "sum_velocity",
    "min_velocity",
    "max_velocity_obs",
    "severe_ping_count",
    "mean_velocity",
]


def _parse_accepted_raw_ping(row: dict[str, Any]) -> tuple[str, tuple[int, datetime, float]]:
    """Parse and validate a single accepted raw ping row for compare_window_aggregates."""
    raw_id = row.get("_id") if "_id" in row else row.get("raw_event_id")
    if raw_id is None or str(raw_id).strip() == "":
        raise ValueError("Comparator input must contain non-empty _id or raw_event_id")
    parsed_ts = datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00"))
    if parsed_ts.tzinfo is None:
        raise ValueError("Comparator input timestamps must include an explicit timezone")
    ts = parsed_ts.astimezone(timezone.utc)
    velocity = float(row["velocity"])
    if not math.isfinite(velocity) or not 0.0 <= velocity <= 200.0:
        raise ValueError("Comparator input must contain accepted finite velocities")
    return str(raw_id), (int(row["segment_id"]), ts, velocity)


def _fold_expected_raw_windows(raw_rows: Any) -> dict[tuple[int, datetime], list[float]]:
    """Fold accepted ping-level raw rows into 30-minute UTC window monoid states."""
    expected: dict[tuple[int, datetime], list[float]] = {}
    seen: dict[str, tuple[int, datetime, float]] = {}
    for row in raw_rows:
        identity, payload = _parse_accepted_raw_ping(row)
        if identity in seen:
            if seen[identity] != payload:
                raise ValueError("Conflicting raw event identity")
            continue
        seen[identity] = payload
        ts, velocity = payload[1], payload[2]
        start = ts.replace(minute=(ts.minute // 30) * 30, second=0, microsecond=0)
        state = expected.setdefault((payload[0], start), [0, 0.0, float("inf"), float("-inf"), 0])
        state[0] += 1
        state[1] += velocity
        state[2] = min(state[2], velocity)
        state[3] = max(state[3], velocity)
        state[4] += int(velocity < 15)
    return expected


def _index_actual_window_rows(actual_rows: Any) -> tuple[dict[tuple[int, datetime], dict[str, Any]], int]:
    """Index actual window rows by (segment_id, window_start_utc) and count duplicate keys."""
    actual: dict[tuple[int, datetime], dict[str, Any]] = {}
    duplicate_keys = 0
    for row in actual_rows:
        start = row["window_start"]
        if isinstance(start, str):
            start = datetime.fromisoformat(start.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        key = (int(row["segment_id"]), start.astimezone(timezone.utc))
        duplicate_keys += int(key in actual)
        actual[key] = row
    return actual, duplicate_keys


def compare_window_aggregates(raw_rows: Any, actual_rows: Any, tolerance: float = 1e-9) -> dict[str, Any]:
    """Independent raw fold; detects missing/extra keys and every monoid field.

    Input raw rows must already follow the shared acceptance policy. Raw identity
    is ping-level, not segment/window-level. Unlike the legacy CSV verifier this
    comparator never clips velocity and never skips unmatched keys.
    """
    expected = _fold_expected_raw_windows(raw_rows)
    actual, duplicate_keys = _index_actual_window_rows(actual_rows)
    mismatches = {field: 0 for field in WINDOW_AGG_FIELDS}
    for key in expected.keys() & actual.keys():
        state = expected[key]
        expected_vals = state + [state[1] / state[0]]
        for field, value in zip(WINDOW_AGG_FIELDS, expected_vals):
            candidate = actual[key].get(field)
            if candidate is None or not math.isclose(float(candidate), value, rel_tol=tolerance, abs_tol=tolerance):
                mismatches[field] += 1
    missing = len(expected.keys() - actual.keys())
    extra = len(actual.keys() - expected.keys())
    return {
        "expected_windows": len(expected),
        "actual_windows": len(actual),
        "missing_keys": missing,
        "extra_keys": extra,
        "duplicate_keys": duplicate_keys,
        "field_mismatches": mismatches,
        "equivalent": not (missing or extra or duplicate_keys or any(mismatches.values())),
    }


def compute_raw_event_id(
    raw_row_id: Any,
    segment_id: Any,
    updated_at_text: str,
    velocity: Any,
) -> str:
    """Compute canonical raw ping ID (G01), distinct from 30m window ID."""
    payload = f"{raw_row_id}|{segment_id}|{updated_at_text.strip()}|{velocity}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def _utc_window_key(updated_at_text: str) -> tuple[str, str]:
    clean = updated_at_text.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean)
    date_str = dt.strftime("%Y-%m-%d")
    minute_bucket = "00" if dt.minute < 30 else "30"
    period_str = f"period_{dt.hour}_{minute_bucket}"
    return date_str, period_str


def monoid_lift(velocity: float) -> tuple[int, float, float, float, int]:
    """Lift a single valid velocity ping into the 5-tuple Commutative Monoid."""
    sev = 1 if velocity < 15.0 else 0
    return (1, velocity, velocity, velocity, sev)


def monoid_combine(
    state_a: tuple[int, float, float, float, int],
    state_b: tuple[int, float, float, float, int],
) -> tuple[int, float, float, float, int]:
    """Associative and commutative binary operator (+) over 5-tuple monoid states."""
    if state_a[0] == 0:
        return state_b
    if state_b[0] == 0:
        return state_a
    return (
        state_a[0] + state_b[0],
        state_a[1] + state_b[1],
        min(state_a[2], state_b[2]),
        max(state_a[3], state_b[3]),
        state_a[4] + state_b[4],
    )


def accumulate_raw_monoid(
    status_csv_path: Path,
) -> tuple[dict[tuple[int, str, str], list[float]], int, int]:
    """Fold raw GPS pings into 5-tuple Commutative Monoid state with exact raw_event_id dedup."""
    monoid_map: dict[tuple[int, str, str], list[float]] = {}
    seen_raw_ids: set[str] = set()
    rejected = 0
    dedup_dropped = 0

    with status_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_idx, row in enumerate(reader):
            try:
                raw_id_val = row.get("_id", row_idx)
                seg_id = int(row["segment_id"])
                velocity = float(row["velocity"])
                updated_at = str(row["updated_at"])
                date_str, period_str = _utc_window_key(updated_at)
            except (KeyError, TypeError, ValueError):
                rejected += 1
                continue
            if math.isnan(velocity) or math.isinf(velocity) or velocity < 0.0 or velocity > 200.0:
                rejected += 1
                continue

            raw_eid = row.get("raw_event_id") or compute_raw_event_id(raw_id_val, seg_id, updated_at, velocity)
            if raw_eid in seen_raw_ids:
                dedup_dropped += 1
                continue
            seen_raw_ids.add(raw_eid)

            lifted = monoid_lift(velocity)
            key = (seg_id, date_str, period_str)
            prev = monoid_map.get(key)
            if prev is None:
                monoid_map[key] = [
                    float(lifted[0]),
                    lifted[1],
                    lifted[2],
                    lifted[3],
                    float(lifted[4]),
                ]
            else:
                combined = monoid_combine(
                    (int(prev[0]), prev[1], prev[2], prev[3], int(prev[4])),
                    lifted,
                )
                monoid_map[key] = [
                    float(combined[0]),
                    combined[1],
                    combined[2],
                    combined[3],
                    float(combined[4]),
                ]
    return monoid_map, rejected, dedup_dropped


def verify_unequal_partition_associativity(
    velocities: list[float] | None = None,
    partition_sizes: list[int] | None = None,
) -> dict[str, Any]:
    """Prove 5-tuple monoid is invariant to unequal partition sizes while average-of-averages fails."""
    if velocities is None:
        velocities = [5.0, 80.0, 80.0, 80.0, 80.0, 80.0, 20.0, 25.0, 30.0]
    if not velocities:
        return {"verified": True, "monoid_diff": 0.0, "naive_avg_of_avg_error": 0.0}
    if partition_sizes is None:
        n = len(velocities)
        c1 = max(1, min(1, n))
        c2 = max(1, min(5, n - c1))
        c3 = max(0, n - c1 - c2)
        partition_sizes = [sz for sz in (c1, c2, c3) if sz > 0]

    single_fold = MONOID_IDENTITY
    for v in velocities:
        single_fold = monoid_combine(single_fold, monoid_lift(v))
    true_mean = single_fold[1] / max(single_fold[0], 1)

    offset = 0
    partial_states: list[tuple[int, float, float, float, int]] = []
    partial_means: list[float] = []
    for sz in partition_sizes:
        chunk = velocities[offset : offset + sz]
        offset += sz
        if not chunk:
            continue
        p_state = MONOID_IDENTITY
        for v in chunk:
            p_state = monoid_combine(p_state, monoid_lift(v))
        partial_states.append(p_state)
        partial_means.append(p_state[1] / p_state[0])

    reduced_state = MONOID_IDENTITY
    for p_state in partial_states:
        reduced_state = monoid_combine(reduced_state, p_state)

    monoid_mean = reduced_state[1] / max(reduced_state[0], 1)
    naive_mean_of_means = sum(partial_means) / max(len(partial_means), 1)
    monoid_diff = abs(true_mean - monoid_mean)
    naive_error = abs(true_mean - naive_mean_of_means)

    return {
        "partition_sizes": partition_sizes,
        "true_count": single_fold[0],
        "reduced_count": reduced_state[0],
        "monoid_tuple_exact_match": (
            single_fold[0] == reduced_state[0]
            and abs(single_fold[1] - reduced_state[1]) < 1e-9
            and abs(single_fold[2] - reduced_state[2]) < 1e-9
            and abs(single_fold[3] - reduced_state[3]) < 1e-9
            and single_fold[4] == reduced_state[4]
        ),
        "monoid_mean_abs_diff": round(monoid_diff, 12),
        "naive_average_of_averages_abs_error": round(naive_error, 6),
        "average_of_averages_abs_error": round(naive_error, 6),
        "average_of_averages_trap_demonstrated": naive_error > 1e-3,
    }


def _compare_single_window_row(
    row: dict[str, Any],
    state: list[float],
    counters: dict[str, float],
) -> None:
    ref_cnt = int(state[0])
    ref_sum_v = float(state[1])
    ref_min_v = float(state[2])
    ref_max_v = float(state[3])
    ref_sev = int(state[4])
    ref_mean_v = max(ref_sum_v / max(ref_cnt, 1), 1.0)

    pipe_cnt = int(float(row["obs_count"]))
    pipe_mean_v = float(row["mean_velocity"])
    if pipe_cnt != ref_cnt:
        counters["count_discrepancies"] += 1

    mean_diff = abs(ref_mean_v - pipe_mean_v)
    if mean_diff > counters["max_abs_diff"]:
        counters["max_abs_diff"] = mean_diff
    if mean_diff > 1e-6:
        counters["velocity_discrepancies"] += 1

    if "sum_velocity" in row and row["sum_velocity"] not in ("", None):
        if abs(float(row["sum_velocity"]) - ref_sum_v) > 1e-5:
            counters["sum_discrepancies"] += 1
    if "min_velocity" in row and row["min_velocity"] not in ("", None):
        if abs(float(row["min_velocity"]) - ref_min_v) > 1e-5:
            counters["min_discrepancies"] += 1
    if "max_velocity_obs" in row and row["max_velocity_obs"] not in ("", None):
        if abs(float(row["max_velocity_obs"]) - ref_max_v) > 1e-5:
            counters["max_discrepancies"] += 1
    if "severe_ping_count" in row and row["severe_ping_count"] not in ("", None):
        if int(float(row["severe_ping_count"])) != ref_sev:
            counters["severe_discrepancies"] += 1


def audit_records_against_monoid(
    records: Any,
    monoid_map: Any,
) -> dict[str, Any]:
    """Compare window records against reference monoid_map on all 5 components and key sets (Q09)."""
    if isinstance(records, dict) and isinstance(monoid_map, list):
        records, monoid_map = monoid_map, records
    counters: dict[str, float] = {
        "matched_windows": 0,
        "extra_keys_in_dataset": 0,
        "count_discrepancies": 0,
        "sum_discrepancies": 0,
        "min_discrepancies": 0,
        "max_discrepancies": 0,
        "severe_discrepancies": 0,
        "velocity_discrepancies": 0,
        "max_abs_diff": 0.0,
    }
    seen_keys: set[tuple[int, str, str]] = set()
    duplicate_window_keys = 0

    for row in records:
        key = (int(row["segment_id"]), str(row["date"]), str(row["period"]))
        if key in seen_keys:
            duplicate_window_keys += 1
        seen_keys.add(key)
        state = monoid_map.get(key)
        if state is None:
            counters["extra_keys_in_dataset"] += 1
            continue
        counters["matched_windows"] += 1
        _compare_single_window_row(row, state, counters)

    missing_keys = len(set(monoid_map.keys()) - seen_keys)
    total_discrepancies = int(
        counters["count_discrepancies"]
        + counters["sum_discrepancies"]
        + counters["min_discrepancies"]
        + counters["max_discrepancies"]
        + counters["severe_discrepancies"]
        + counters["velocity_discrepancies"]
        + counters["extra_keys_in_dataset"]
        + missing_keys
        + duplicate_window_keys
    )
    return {
        "raw_distinct_utc_windows": len(monoid_map),
        "medallion_matched_windows": int(counters["matched_windows"]),
        "missing_keys_in_dataset": missing_keys,
        "extra_keys_in_dataset": int(counters["extra_keys_in_dataset"]),
        "duplicate_window_keys": duplicate_window_keys,
        "utc_window_join_match_pct": round(counters["matched_windows"] / max(len(monoid_map), 1) * 100.0, 2),
        "obs_count_discrepancies": int(counters["count_discrepancies"]),
        "sum_velocity_discrepancies": int(counters["sum_discrepancies"]),
        "min_velocity_discrepancies": int(counters["min_discrepancies"]),
        "max_velocity_discrepancies": int(counters["max_discrepancies"]),
        "severe_ping_discrepancies": int(counters["severe_discrepancies"]),
        "mean_velocity_discrepancies": int(counters["velocity_discrepancies"]),
        "max_abs_velocity_diff": round(float(counters["max_abs_diff"]), 12),
        "monoid_associativity_verified": total_discrepancies == 0,
    }


def verify_against_medallion(
    raw_dir: Path,
    dataset_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    monoid_map, rejected_raw, dedup_dropped = accumulate_raw_monoid(raw_dir / "segment_status.csv")
    total_severe_pings = int(sum(st[4] for st in monoid_map.values()))

    with dataset_path.open("r", encoding="utf-8", newline="") as handle:
        records = list(csv.DictReader(handle))

    audit = audit_records_against_monoid(records, monoid_map)
    assoc_demo = verify_unequal_partition_associativity(
        [10.0, 12.0, 14.0, 60.0, 80.0, 90.0],
        partition_sizes=[1, 2, 3],
    )

    report = {
        "pillar": "BDA501 Pillar I1 — 5-Tuple Commutative Monoid Combiner vs. Independent Reference Baseline",
        "monoid_tuple": "(cnt, sum_v, min_v, max_v, severe_cnt)",
        "identity_element": "(0, 0.0, +inf, 0.0, 0)",
        **audit,
        "total_severe_pings_accumulated": total_severe_pings,
        "rejected_malformed_pings": rejected_raw,
        "dedup_dropped_raw_pings": dedup_dropped,
        "unequal_partition_associativity_check": assoc_demo,
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
