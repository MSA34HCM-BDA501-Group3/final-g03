"""[C6]-[C8] & [C15] Structured Streaming Router, O(1) Sketches, Read-Only Scorer & WAL Recovery.

Implements M03, M04, M05, M06, M11, and M12:
- Shared quality rules isolating malformed stream events to quarantine.
- Dual-Sink architecture:
  * Sink 1: Atomically persists 100% of accepted raw events to Bronze Parquet on disk
    (`stream_sink1_bronze/batch_XXXX.parquet`) before WAL commit and watermark filtering.
  * Sink 2: Real event-time watermarked (30m) micro-batch processor with Rotating Bloom Filter
    (2 x 16.0 KiB = 32.0 KiB fixed), 1.0 KiB 64-bit HyperLogLog (p=10, uint8[1024]), and
    time-aware bounded decaying hotspot state C_s(t).
- Online Read-Only Scorer ([C8]) scoring post-T_train windows with frozen [C13a]/[C13b].
- Version/ordering guard (M11) preventing stale STREAM_PROVISIONAL retries from overwriting
  BATCH_RECONCILED records.
- Compact WAL checkpoint persistence and mid-stream kill/restart recovery verification (M12).
"""

from __future__ import annotations

import argparse
from datetime import timedelta
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BLOOM_NUM_BITS = 262_144
BLOOM_NUM_BYTES = BLOOM_NUM_BITS // 8  # Exact 32,768 bytes = 32.0 KiB
BLOOM_NUM_HASHES = 4
ROTATING_BLOOM_GEN_BITS = BLOOM_NUM_BITS // 2  # 131,072 bits = 16,384 bytes (16.0 KiB per generation)
ROTATING_BLOOM_CAPACITY = 8_000
ROTATING_BLOOM_NUM_HASHES = 10
HLL_P_BITS = 10
HLL_NUM_REGISTERS = 1 << HLL_P_BITS  # Exact 1,024 bytes = 1.0 KiB (64-bit HyperLogLog, p=10)
DECAY_EVICTION_EPSILON = 1e-4
DECAY_SLOT_MINUTES = 30.0
DECAY_STALE_SLOTS = 60.0

LIFECYCLE_VERSION_PRIORITY = {
    "PHASE_A_HISTORICAL_TRAIN": 1,
    "STREAM_PROVISIONAL": 1,
    "BATCH_VERIFIED": 2,
    "BATCH_RECONCILED": 2,
}


def bloom_hash_positions(
    item: str,
    num_bits: int = BLOOM_NUM_BITS,
    num_hashes: int = BLOOM_NUM_HASHES,
) -> list[int]:
    digest = hashlib.sha256(item.encode("utf-8")).digest()
    return [int.from_bytes(digest[idx * 4 : (idx + 1) * 4], "big") % num_bits for idx in range(num_hashes)]


def create_bitpacked_bloom(num_bits: int = BLOOM_NUM_BITS) -> np.ndarray:
    """Allocate a bit-packed Bloom filter array of exact size num_bits // 8 bytes (32 KiB)."""
    num_bytes = (num_bits + 7) // 8
    return np.zeros(num_bytes, dtype=np.uint8)


def bloom_check_and_add(
    bloom_bytes: np.ndarray,
    item: str,
    num_bits: int = BLOOM_NUM_BITS,
    num_hashes: int = BLOOM_NUM_HASHES,
) -> bool:
    """Return True if item was already present in bit-packed Bloom filter, else set bits & return False."""
    positions = bloom_hash_positions(item, num_bits=num_bits, num_hashes=num_hashes)
    already_present = True
    for pos in positions:
        byte_idx = pos >> 3
        bit_mask = 1 << (pos & 7)
        if (int(bloom_bytes[byte_idx]) & bit_mask) == 0:
            already_present = False
            break
    if not already_present:
        for pos in positions:
            byte_idx = pos >> 3
            bit_mask = np.uint8(1 << (pos & 7))
            bloom_bytes[byte_idx] |= bit_mask
    return already_present


class RotatingBloomFilter:
    """Two-generation bit-packed Bloom filter (2 x 16.0 KiB = 32.0 KiB fixed memory)."""

    def __init__(
        self,
        bits_per_gen: int = ROTATING_BLOOM_GEN_BITS,
        num_hashes: int = ROTATING_BLOOM_NUM_HASHES,
        capacity: int = ROTATING_BLOOM_CAPACITY,
    ) -> None:
        self.bits_per_gen = int(bits_per_gen)
        self.num_hashes = int(num_hashes)
        self.capacity = int(capacity)
        self.active = create_bitpacked_bloom(self.bits_per_gen)
        self.previous = create_bitpacked_bloom(self.bits_per_gen)
        self.active_count = 0

    @property
    def nbytes(self) -> int:
        return int(self.active.nbytes + self.previous.nbytes)

    def _positions(self, item: str) -> list[int]:
        digest = hashlib.sha256(item.encode("utf-8")).digest()
        h1 = int.from_bytes(digest[:16], "big")
        h2 = int.from_bytes(digest[16:32], "big") | 1
        return [(h1 + idx * h2) % self.bits_per_gen for idx in range(self.num_hashes)]

    @staticmethod
    def _contains(buf: np.ndarray, positions: list[int]) -> bool:
        for pos in positions:
            if (int(buf[pos >> 3]) & (1 << (pos & 7))) == 0:
                return False
        return True

    def check_and_add(self, item: str) -> bool:
        """Return True if item is in active/previous generation; else insert (rotating at capacity)."""
        positions = self._positions(item)
        if self._contains(self.active, positions) or self._contains(self.previous, positions):
            return True
        if self.active_count >= self.capacity:
            self.previous = self.active
            self.active = create_bitpacked_bloom(self.bits_per_gen)
            self.active_count = 0
        for pos in positions:
            self.active[pos >> 3] |= np.uint8(1 << (pos & 7))
        self.active_count += 1
        return False

    def to_bytes(self) -> bytes:
        return self.active.tobytes() + self.previous.tobytes()

    def meta_dict(self) -> dict[str, int]:
        return {
            "bits_per_gen": self.bits_per_gen,
            "num_hashes": self.num_hashes,
            "capacity": self.capacity,
            "active_count": self.active_count,
        }

    @classmethod
    def from_bytes_and_meta(cls, raw_bytes: bytes, meta: dict[str, Any] | None = None) -> RotatingBloomFilter:
        info = meta or {}
        bits_per_gen = int(info.get("bits_per_gen", ROTATING_BLOOM_GEN_BITS))
        gen_bytes = (bits_per_gen + 7) // 8
        obj = cls(
            bits_per_gen=bits_per_gen,
            num_hashes=int(info.get("num_hashes", ROTATING_BLOOM_NUM_HASHES)),
            capacity=int(info.get("capacity", ROTATING_BLOOM_CAPACITY)),
        )
        obj.active_count = int(info.get("active_count", 0))
        if len(raw_bytes) >= 2 * gen_bytes:
            obj.active = np.frombuffer(raw_bytes[:gen_bytes], dtype=np.uint8).copy()
            obj.previous = np.frombuffer(raw_bytes[gen_bytes : 2 * gen_bytes], dtype=np.uint8).copy()
        return obj


def create_rotating_bloom(
    bits_per_gen: int = ROTATING_BLOOM_GEN_BITS,
    num_hashes: int = ROTATING_BLOOM_NUM_HASHES,
    capacity: int = ROTATING_BLOOM_CAPACITY,
) -> RotatingBloomFilter:
    """Allocate a 2-generation RotatingBloomFilter (2 x 16.0 KiB = 32.0 KiB fixed total)."""
    return RotatingBloomFilter(bits_per_gen=bits_per_gen, num_hashes=num_hashes, capacity=capacity)


def rotating_bloom_check_and_add(bloom: RotatingBloomFilter, item: str) -> bool:
    """Check item across active/previous generations and insert if absent."""
    return bloom.check_and_add(item)


def hll_add(register: np.ndarray, item: str, p_bits: int = HLL_P_BITS) -> None:
    """Insert item into 64-bit HyperLogLog (p=10, 1.0 KiB uint8[1024])."""
    val = int.from_bytes(hashlib.sha256(item.encode("utf-8")).digest()[:8], "big")
    bucket = val & ((1 << p_bits) - 1)
    w = val >> p_bits
    rho = int(1 + (w & -w).bit_length() - 1) if w > 0 else (64 - p_bits + 1)
    rho = min(rho, 64 - p_bits + 1)
    if rho > register[bucket]:
        register[bucket] = rho


def hll_estimate(register: np.ndarray) -> float:
    """Estimate cardinality from 64-bit HyperLogLog (p=10, 1.0 KiB uint8[1024]) registers."""
    m = len(register)
    alpha_m = 0.7213 / (1.0 + 1.079 / m)
    harmonic = float(np.sum(2.0 ** (-register.astype(float))))
    estimate = alpha_m * (m * m) / harmonic
    zeros = int(np.count_nonzero(register == 0))
    if estimate <= 2.5 * m and zeros > 0:
        return float(m * math.log(m / zeros))
    return float(estimate)


def update_bounded_decay_state(
    decay_scores: dict[int, float],
    seg_id: int,
    is_severe: int,
    decay_alpha: float = 0.15,
    eviction_threshold: float = DECAY_EVICTION_EPSILON,
    epsilon: float | None = None,
    delta_slots: float = 1.0,
    event_ts: pd.Timestamp | None = None,
    last_ts_map: dict[int, pd.Timestamp] | None = None,
) -> None:
    """Update segment decaying score O(1)/segment over elapsed slots and evict below threshold (M05)."""
    thresh = epsilon if epsilon is not None else eviction_threshold
    eff_slots = max(float(delta_slots), 1.0)
    if event_ts is not None and last_ts_map is not None and pd.notna(event_ts):
        prev_ts = last_ts_map.get(seg_id)
        if prev_ts is not None and pd.notna(prev_ts):
            eff_slots = max((event_ts - prev_ts).total_seconds() / (DECAY_SLOT_MINUTES * 60.0), 1.0)
        last_ts_map[seg_id] = event_ts

    prev = decay_scores.get(seg_id, 0.0)
    updated = prev * ((1.0 - decay_alpha) ** eff_slots) + float(is_severe)
    if updated < thresh:
        decay_scores.pop(seg_id, None)
        if last_ts_map is not None:
            last_ts_map.pop(seg_id, None)
    else:
        decay_scores[seg_id] = updated


def evict_stale_decay_segments(
    decay_scores: dict[int, float],
    last_ts_map: dict[int, pd.Timestamp],
    current_ts: pd.Timestamp | None,
    decay_alpha: float = 0.15,
    eviction_threshold: float = DECAY_EVICTION_EPSILON,
    max_stale_slots: float = DECAY_STALE_SLOTS,
) -> int:
    """Evict segments silent for > max_stale_slots (60 slots) or decayed below threshold."""
    if current_ts is None or pd.isna(current_ts) or not last_ts_map:
        return 0
    removed = 0
    for seg_id in list(decay_scores.keys()):
        last_ts = last_ts_map.get(seg_id)
        if last_ts is None or pd.isna(last_ts):
            continue
        elapsed_slots = max((current_ts - last_ts).total_seconds() / (DECAY_SLOT_MINUTES * 60.0), 0.0)
        decayed = decay_scores[seg_id] * ((1.0 - decay_alpha) ** elapsed_slots)
        if elapsed_slots > max_stale_slots or decayed < eviction_threshold:
            decay_scores.pop(seg_id, None)
            last_ts_map.pop(seg_id, None)
            removed += 1
    return removed


def _pick_first_present(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key not in row:
            continue
        val = row[key]
        if val is None or val == "":
            continue
        try:
            if pd.isna(val):
                continue
        except Exception:
            pass
        return val
    return None


def _classify_stream_row(row: dict[str, Any], valid_segment_ids: set[int] | None) -> str | None:
    seg_raw = _pick_first_present(row, ("segment_id",))
    vel_raw = _pick_first_present(row, ("velocity", "mean_velocity"))
    ts_raw = _pick_first_present(row, ("event_ts", "updated_at", "slot_ts"))
    if seg_raw is None or vel_raw is None or ts_raw is None:
        return "R0_MISSING_OR_MALFORMED_FIELD"
    try:
        seg_id = int(float(str(seg_raw)))
        vel = float(str(vel_raw))
        parsed_ts = pd.to_datetime(str(ts_raw), utc=True, errors="raise")
        if pd.isna(parsed_ts) or math.isnan(vel) or math.isinf(vel):
            return "R0_MISSING_OR_MALFORMED_FIELD"
    except Exception:
        return "R0_MISSING_OR_MALFORMED_FIELD"
    if seg_id < 0 or (valid_segment_ids is not None and seg_id not in valid_segment_ids):
        return "R1_UNKNOWN_OR_INVALID_SEGMENT"
    if vel < 0.0 or vel > 200.0:
        return "R2_VELOCITY_OUT_OF_RANGE"
    return None


def validate_stream_batch_ordered(
    raw_batch: pd.DataFrame,
    valid_segment_ids: set[int] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply shared ordered quality rules (R1..R4) to streaming events (Q02/Q11/M03)."""
    if raw_batch.empty:
        return raw_batch.copy(), pd.DataFrame()
    rules = [_classify_stream_row(row, valid_segment_ids) for row in raw_batch.to_dict(orient="records")]
    rule_series = pd.Series(rules, index=raw_batch.index, dtype=object)
    valid_mask = rule_series.isna()
    accepted = raw_batch.loc[valid_mask].copy()
    rejected = raw_batch.loc[~valid_mask].copy()
    if not rejected.empty:
        rejected["first_failing_rule"] = rule_series.loc[~valid_mask].astype(str)
    return accepted, rejected


def upsert_serving_records_with_version_guard(
    serving_store: dict[str, dict[str, Any]],
    incoming_records: list[dict[str, Any]],
) -> dict[str, int]:
    """[M11] Idempotent upsert with version ordering guard so provisional never overwrites reconciled."""
    inserted = 0
    upgraded = 0
    stale_rejected = 0
    idempotent_noop = 0

    for doc in incoming_records:
        eid = str(doc["event_id"])
        new_status = str(doc.get("lifecycle_status", "STREAM_PROVISIONAL"))
        new_ver = int(doc.get("status_version", LIFECYCLE_VERSION_PRIORITY.get(new_status, 1)))
        doc_copy = {**doc, "status_version": new_ver}

        existing = serving_store.get(eid)
        if existing is None:
            serving_store[eid] = doc_copy
            inserted += 1
            continue

        old_status = str(existing.get("lifecycle_status", "STREAM_PROVISIONAL"))
        old_ver = int(existing.get("status_version", LIFECYCLE_VERSION_PRIORITY.get(old_status, 1)))
        if new_ver < old_ver:
            stale_rejected += 1
        elif new_ver == old_ver and new_status == old_status:
            serving_store[eid] = doc_copy
            idempotent_noop += 1
        else:
            doc_copy["previous_lifecycle_status"] = old_status
            serving_store[eid] = doc_copy
            upgraded += 1

    return {
        "inserted_new": inserted,
        "upgraded_to_reconciled": upgraded,
        "idempotent_replays": idempotent_noop,
        "stale_provisional_rejected": stale_rejected,
        "total_store_documents": len(serving_store),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


SAMPLE_STATIC_STORE_PATH = Path("data/demo_sample/sample_static_features.parquet")
DEFAULT_STATIC_STORE_PATH = Path("data/lakehouse/segment_static_features.parquet")


def _resolve_static_store_target(
    manifest_path: Path,
    static_store_path: Path,
) -> tuple[dict[str, Any], Path, str, bool]:
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Frozen registry artifact missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if static_store_path.is_file():
        return manifest, static_store_path, "static_feature_store_sha256", False
    if (
        static_store_path == DEFAULT_STATIC_STORE_PATH
        and SAMPLE_STATIC_STORE_PATH.is_file()
        and "sample_static_feature_store_sha256" in manifest
    ):
        return manifest, SAMPLE_STATIC_STORE_PATH, "sample_static_feature_store_sha256", True
    raise FileNotFoundError(f"Frozen registry artifact missing: {static_store_path}")


def verify_frozen_registry_integrity(
    manifest_path: Path = Path("outputs/model_registry_manifest.json"),
    model_path: Path = Path("outputs/model.joblib"),
    vmax_lookup_path: Path = Path("outputs/vmax_lookup.json"),
    static_store_path: Path = Path("data/lakehouse/segment_static_features.parquet"),
) -> dict[str, Any]:
    """[M06/M-C02/M-C07] Verify SHA-256 hashes of frozen Phase A registry artifacts."""
    for required in (manifest_path, model_path, vmax_lookup_path):
        if not required.is_file():
            raise FileNotFoundError(f"Frozen registry artifact missing: {required}")

    manifest, eff_static_path, static_sha_key, used_sample_static = _resolve_static_store_target(
        manifest_path, static_store_path
    )
    model_sha = _sha256_file(model_path)
    vmax_sha = _sha256_file(vmax_lookup_path)
    static_sha = _sha256_file(eff_static_path)
    manifest_sha = _sha256_file(manifest_path)

    expected_model_sha = manifest.get("model_sha256") or manifest.get("sha256_digest")
    read_only_flag = bool(
        manifest.get("read_only_in_phase_b_and_c", manifest.get("phase_b_c_read_only_verified", False))
    )
    mismatches: list[str] = []
    if model_sha != expected_model_sha:
        mismatches.append("model.joblib SHA-256 mismatch")
    if vmax_sha != manifest.get("vmax_lookup_sha256"):
        mismatches.append("vmax_lookup.json SHA-256 mismatch")
    if static_sha != manifest.get(static_sha_key):
        mismatches.append(f"{eff_static_path.name} SHA-256 mismatch")
    if not read_only_flag:
        mismatches.append("read_only_in_phase_b_and_c flag is not True")
    if mismatches:
        raise RuntimeError(f"Frozen registry integrity violation: {'; '.join(mismatches)}")

    return {
        "verified": True,
        "model_version": str(manifest.get("model_version", manifest.get("selected_model", "rf_full_23d_v3"))),
        "model_sha256": model_sha,
        "vmax_lookup_sha256": vmax_sha,
        "static_feature_store_sha256": static_sha,
        "static_store_mode": "demo_sample" if used_sample_static else "full",
        "manifest_sha256": manifest_sha,
        "read_only_enforced": True,
    }


def _verify_read_only_online_scoring(
    manifest_path: Path = Path("outputs/model_registry_manifest.json"),
    model_path: Path = Path("outputs/model.joblib"),
    vmax_lookup_path: Path = Path("outputs/vmax_lookup.json"),
    static_store_path: Path = Path("data/lakehouse/segment_static_features.parquet"),
) -> dict[str, Any]:
    """Verify frozen registry hashes before and after read-only .predict_proba() online scoring."""
    if not manifest_path.is_file() or not model_path.is_file():
        return {"verified": False, "reason": "registry_not_initialized_yet"}
    import joblib

    pre_check = verify_frozen_registry_integrity(
        manifest_path=manifest_path,
        model_path=model_path,
        vmax_lookup_path=vmax_lookup_path,
        static_store_path=static_store_path,
    )
    bundle = joblib.load(model_path)
    feature_cols = list(bundle.get("full_features") or bundle.get("feature_columns", []))
    model = bundle.get("rf_multiclass") or bundle.get("multiclass_model")
    x_probe = np.zeros((1, len(feature_cols)), dtype=float)
    probs = model.predict_proba(x_probe)
    post_check = verify_frozen_registry_integrity(
        manifest_path=manifest_path,
        model_path=model_path,
        vmax_lookup_path=vmax_lookup_path,
        static_store_path=static_store_path,
    )
    if pre_check["model_sha256"] != post_check["model_sha256"] or (
        pre_check["manifest_sha256"] != post_check["manifest_sha256"]
    ):
        raise RuntimeError("Frozen registry mutated during online read-only scoring!")
    return {
        **post_check,
        "pre_post_sha256_identical": True,
        "probe_classes_count": int(probs.shape[1]),
    }


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, path)


def _atomic_write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    with tmp_path.open("wb") as fh:
        df.to_parquet(fh, index=False)
    os.replace(tmp_path, path)


def _sink1_bronze_dir(checkpoint_dir: Path) -> Path:
    return checkpoint_dir.parent / "stream_sink1_bronze"


def _count_sink1_bronze_on_disk(checkpoint_dir: Path, batch_ids: set[int] | None = None) -> int:
    sink1_dir = _sink1_bronze_dir(checkpoint_dir)
    if not sink1_dir.is_dir():
        return 0
    if batch_ids is not None:
        paths = [sink1_dir / f"batch_{bid:04d}.parquet" for bid in sorted(batch_ids)]
        return sum(int(len(pd.read_parquet(p))) for p in paths if p.is_file())
    return sum(int(len(pd.read_parquet(p))) for p in sorted(sink1_dir.glob("batch_*.parquet")))


def _parse_event_timestamp(row: dict[str, Any]) -> pd.Timestamp | None:
    raw_ts = _pick_first_present(row, ("event_ts", "updated_at", "slot_ts"))
    if raw_ts is None:
        date_val = _pick_first_present(row, ("date",))
        raw_ts = f"{date_val if date_val is not None else '2020-07-03'}T00:00:00Z"
    parsed = pd.to_datetime(raw_ts, utc=True, errors="coerce")
    return None if pd.isna(parsed) else parsed


def _record_bloom_audit(row: dict[str, Any], state: dict[str, Any]) -> None:
    """Audit-only counter executed strictly AFTER Bloom filter already dropped the event."""
    dup_val = row.get("is_duplicate")
    if dup_val is None:
        return
    try:
        if pd.isna(dup_val):
            return
    except Exception:
        pass
    is_dup = dup_val.strip().lower() == "true" if isinstance(dup_val, str) else bool(dup_val)
    if is_dup:
        state["true_duplicates_caught"] += 1
    else:
        state["bloom_false_positives_eval"] += 1


def _append_sample_window_id(state: dict[str, Any], row: dict[str, Any], raw_eid: str) -> None:
    win_id = _pick_first_present(row, ("event_id",))
    eid_str = str(win_id) if win_id is not None else raw_eid
    sample_ids: list[str] = state["sample_window_ids"]
    if eid_str in sample_ids:
        return
    sample_ids.append(eid_str)
    if len(sample_ids) > 25:
        del sample_ids[0]


def _process_accepted_row_sink2(row: dict[str, Any], state: dict[str, Any]) -> None:
    part = str(int(row.get("kafka_partition", 0)))
    state["committed_offsets"][part] = state["committed_offsets"].get(part, 0) + 1

    ev_ts = _parse_event_timestamp(row)
    if ev_ts is not None and (state["max_event_ts"] is None or ev_ts > state["max_event_ts"]):
        state["max_event_ts"] = ev_ts

    eid_raw = _pick_first_present(row, ("raw_event_id", "event_id"))
    raw_eid = str(eid_raw) if eid_raw is not None else f"{row['segment_id']}_{ev_ts}"
    if rotating_bloom_check_and_add(state["rotating_bloom"], raw_eid):
        state["bloom_duplicates_flagged"] += 1
        _record_bloom_audit(row, state)
        return

    max_ts = state["max_event_ts"]
    if max_ts is not None and ev_ts is not None and ev_ts < (max_ts - timedelta(minutes=30)):
        state["dropped_by_watermark_sink2"] += 1
        return

    seg_id = int(float(str(row["segment_id"])))
    hll_add(state["hll_registers"], str(seg_id), p_bits=HLL_P_BITS)
    vel_raw = _pick_first_present(row, ("mean_velocity", "velocity"))
    vel_val = float(vel_raw) if vel_raw is not None else 20.0
    sev_raw = _pick_first_present(row, ("is_severe",))
    is_sev = int(sev_raw) if sev_raw is not None else (1 if vel_val < 15.0 else 0)
    update_bounded_decay_state(
        state["decay_scores"],
        seg_id,
        is_sev,
        event_ts=ev_ts,
        last_ts_map=state["decay_last_ts"],
    )
    _append_sample_window_id(state, row, raw_eid)
    state["sink2_in_wm_events_count"] += 1


def _process_single_microbatch(
    batch_id: int,
    batch_df: pd.DataFrame,
    state: dict[str, Any],
    valid_segment_ids: set[int] | None = None,
    checkpoint_dir: Path | None = None,
) -> dict[str, Any]:
    if batch_id in state["committed_batch_ids"]:
        return {"batch_id": batch_id, "status": "SKIPPED_ALREADY_COMMITTED_IN_WAL"}

    accepted, rejected = validate_stream_batch_ordered(batch_df, valid_segment_ids)
    state["quarantined_count"] += len(rejected)

    eff_ckpt_dir = checkpoint_dir or (Path(state["checkpoint_dir"]) if state.get("checkpoint_dir") else None)
    if eff_ckpt_dir is not None:
        batch_parquet = _sink1_bronze_dir(eff_ckpt_dir) / f"batch_{batch_id:04d}.parquet"
        _atomic_write_parquet(batch_parquet, accepted)
        state["sink1_batch_counts"][int(batch_id)] = int(len(accepted))
        state["sink1_bronze_total"] = sum(state["sink1_batch_counts"].values())
    else:
        state["sink1_bronze_total"] += int(len(accepted))

    for row in accepted.to_dict(orient="records"):
        _process_accepted_row_sink2(row, state)

    evict_stale_decay_segments(state["decay_scores"], state["decay_last_ts"], state["max_event_ts"])
    state["committed_batch_ids"].add(batch_id)
    watermark_iso = (
        (pd.Timestamp(state["max_event_ts"]) - pd.Timedelta(30, unit="min")).isoformat()
        if state["max_event_ts"] is not None
        else None
    )
    return {
        "batch_id": batch_id,
        "status": "COMMITTED",
        "accepted_rows": int(len(accepted)),
        "quarantined_rows": int(len(rejected)),
        "current_watermark_utc": watermark_iso,
        "sink1_bronze_total": int(state["sink1_bronze_total"]),
        "sink2_in_wm_total": int(state["sink2_in_wm_events_count"]),
    }


def _save_wal_checkpoint(state: dict[str, Any], checkpoint_dir: Path) -> None:
    raw_dir = checkpoint_dir / "raw"
    agg_dir = checkpoint_dir / "agg"
    max_ts = state["max_event_ts"]
    max_ts_iso = pd.Timestamp(max_ts).isoformat() if max_ts is not None else None
    wm_iso = (pd.Timestamp(max_ts) - pd.Timedelta(30, unit="min")).isoformat() if max_ts is not None else None
    rot_bloom: RotatingBloomFilter = state["rotating_bloom"]

    offset_payload = {
        "sink": "Sink 1 (append -> stream_sink1_bronze/)",
        "topic": "hcmc-traffic-events",
        "committed_batch_ids": sorted(state["committed_batch_ids"]),
        "committed_offsets": state["committed_offsets"],
        "sink1_bronze_events_count": int(state["sink1_bronze_total"]),
        "status": "COMMITTED",
    }
    _atomic_write_text(raw_dir / "offset_log.json", json.dumps(offset_payload, indent=2))

    wal_payload = {
        "sink": "Sink 2 (update -> foreachBatch Sketches & Online Scorer)",
        "watermark": "30 minutes",
        "max_event_ts_iso": max_ts_iso,
        "current_watermark_utc": wm_iso,
        "committed_batch_ids": sorted(state["committed_batch_ids"]),
        "bloom_bits": BLOOM_NUM_BITS,
        "bloom_memory_bytes": int(rot_bloom.nbytes),
        "rotating_bloom_meta": rot_bloom.meta_dict(),
        "hll_registers": HLL_NUM_REGISTERS,
        "hll_memory_bytes": int(state["hll_registers"].nbytes),
        "hll_estimated_active_segments": round(hll_estimate(state["hll_registers"]), 2),
        "active_decay_segments_in_ram": len(state["decay_scores"]),
        "sink2_in_wm_events_count": int(state["sink2_in_wm_events_count"]),
        "dropped_by_watermark_sink2": int(state["dropped_by_watermark_sink2"]),
        "dropped_by_event_time_watermark": int(state["dropped_by_watermark_sink2"]),
        "bloom_duplicates_flagged": int(state["bloom_duplicates_flagged"]),
        "true_duplicates_caught": int(state["true_duplicates_caught"]),
        "bloom_false_positives_eval": int(state["bloom_false_positives_eval"]),
        "quarantined_count": int(state["quarantined_count"]),
        "sample_window_ids": list(state["sample_window_ids"])[-25:],
        "bloom_hex": rot_bloom.to_bytes().hex(),
        "hll_hex": state["hll_registers"].tobytes().hex(),
        "decay_scores": {str(k): round(float(v), 6) for k, v in state["decay_scores"].items()},
        "decay_last_ts_epoch": {
            str(k): round(float(pd.Timestamp(ts).timestamp()), 3)
            for k, ts in state["decay_last_ts"].items()
            if k in state["decay_scores"] and pd.notna(ts)
        },
        "status": "COMMITTED",
    }
    _atomic_write_text(agg_dir / "state_wal.json", json.dumps(wal_payload, indent=2))


def _restore_wal_into_state(state: dict[str, Any], raw_data: dict[str, Any], agg_data: dict[str, Any]) -> None:
    state["committed_batch_ids"] = set(int(x) for x in agg_data.get("committed_batch_ids", []))
    state["committed_offsets"] = {str(k): int(v) for k, v in raw_data.get("committed_offsets", {}).items()}
    state["sink2_in_wm_events_count"] = int(agg_data.get("sink2_in_wm_events_count", 0))
    state["dropped_by_watermark_sink2"] = int(agg_data.get("dropped_by_watermark_sink2", 0))
    state["bloom_duplicates_flagged"] = int(agg_data.get("bloom_duplicates_flagged", 0))
    state["true_duplicates_caught"] = int(agg_data.get("true_duplicates_caught", 0))
    state["bloom_false_positives_eval"] = int(agg_data.get("bloom_false_positives_eval", 0))
    state["quarantined_count"] = int(agg_data.get("quarantined_count", 0))
    state["sample_window_ids"] = list(agg_data.get("sample_window_ids", []))[-25:]

    max_iso = agg_data.get("max_event_ts_iso")
    if max_iso:
        parsed = pd.to_datetime(max_iso, utc=True, errors="coerce")
        state["max_event_ts"] = None if pd.isna(parsed) else parsed
    if "bloom_hex" in agg_data:
        state["rotating_bloom"] = RotatingBloomFilter.from_bytes_and_meta(
            bytes.fromhex(agg_data["bloom_hex"]),
            agg_data.get("rotating_bloom_meta"),
        )
    if "hll_hex" in agg_data:
        state["hll_registers"] = np.frombuffer(bytes.fromhex(agg_data["hll_hex"]), dtype=np.uint8).copy()
    if "decay_scores" in agg_data:
        state["decay_scores"] = {int(k): float(v) for k, v in agg_data["decay_scores"].items()}
    if "decay_last_ts_epoch" in agg_data:
        state["decay_last_ts"] = {
            int(k): pd.to_datetime(float(v), unit="s", utc=True)
            for k, v in agg_data["decay_last_ts_epoch"].items()
        }


def _init_or_restore_stream_state(checkpoint_dir: Path | None) -> dict[str, Any]:
    state: dict[str, Any] = {
        "checkpoint_dir": str(checkpoint_dir) if checkpoint_dir is not None else None,
        "committed_batch_ids": set(),
        "committed_offsets": {"0": 0, "1": 0, "2": 0},
        "max_event_ts": None,
        "rotating_bloom": create_rotating_bloom(),
        "hll_registers": np.zeros(HLL_NUM_REGISTERS, dtype=np.uint8),
        "decay_scores": {},
        "decay_last_ts": {},
        "sink1_batch_counts": {},
        "sink1_bronze_total": 0,
        "sample_window_ids": [],
        "sink2_in_wm_events_count": 0,
        "dropped_by_watermark_sink2": 0,
        "bloom_duplicates_flagged": 0,
        "true_duplicates_caught": 0,
        "bloom_false_positives_eval": 0,
        "quarantined_count": 0,
    }
    if checkpoint_dir is None:
        return state
    raw_log = checkpoint_dir / "raw" / "offset_log.json"
    agg_log = checkpoint_dir / "agg" / "state_wal.json"
    if raw_log.is_file() and agg_log.is_file():
        raw_data = json.loads(raw_log.read_text(encoding="utf-8"))
        agg_data = json.loads(agg_log.read_text(encoding="utf-8"))
        _restore_wal_into_state(state, raw_data, agg_data)
        sink1_dir = _sink1_bronze_dir(checkpoint_dir)
        for bid in sorted(state["committed_batch_ids"]):
            pq_path = sink1_dir / f"batch_{bid:04d}.parquet"
            if pq_path.is_file():
                state["sink1_batch_counts"][bid] = int(len(pd.read_parquet(pq_path)))
        state["sink1_bronze_total"] = sum(state["sink1_batch_counts"].values())
    return state


def _cleanup_previous_stream_run(checkpoint_dir: Path) -> None:
    for log_path in (checkpoint_dir / "raw" / "offset_log.json", checkpoint_dir / "agg" / "state_wal.json"):
        if log_path.is_file():
            log_path.unlink()
    sink1_dir = _sink1_bronze_dir(checkpoint_dir)
    if sink1_dir.is_dir():
        for pq_file in sink1_dir.glob("batch_*.parquet"):
            pq_file.unlink()


def _run_version_guard_audit(sample_ids: list[str]) -> dict[str, int]:
    guard_store: dict[str, dict[str, Any]] = {}
    b_res = upsert_serving_records_with_version_guard(
        guard_store,
        [{"event_id": eid, "lifecycle_status": "STREAM_PROVISIONAL", "status_version": 1} for eid in sample_ids],
    )
    c_res = upsert_serving_records_with_version_guard(
        guard_store,
        [{"event_id": eid, "lifecycle_status": "BATCH_RECONCILED", "status_version": 2} for eid in sample_ids],
    )
    stale_res = upsert_serving_records_with_version_guard(
        guard_store,
        [{"event_id": eid, "lifecycle_status": "STREAM_PROVISIONAL", "status_version": 1} for eid in sample_ids],
    )
    return {
        "phase_b_initial_upserts": b_res["inserted_new"],
        "phase_c_upgrades": c_res["upgraded_to_reconciled"],
        "phase_c_late_inserts": c_res["inserted_new"],
        "stale_provisional_retries_rejected": stale_res["stale_provisional_rejected"],
    }


def run_stream_with_restart_recovery(
    events_df: pd.DataFrame,
    checkpoint_dir: Path,
    num_microbatches: int = 4,
    kill_after_batch: int = 1,
    valid_segment_ids: set[int] | None = None,
) -> dict[str, Any]:
    """[M12/T04] Execute micro-batches with simulated container kill & WAL checkpoint restart."""
    _cleanup_previous_stream_run(checkpoint_dir)

    chunks = [
        events_df.iloc[idxs].copy()
        for idxs in np.array_split(np.arange(len(events_df)), max(num_microbatches, 1))
        if len(idxs) > 0
    ]
    progress_log: list[dict[str, Any]] = []

    state_before_kill = _init_or_restore_stream_state(checkpoint_dir)
    for b_id in range(min(kill_after_batch + 1, len(chunks))):
        prog = _process_single_microbatch(
            b_id, chunks[b_id], state_before_kill, valid_segment_ids, checkpoint_dir=checkpoint_dir
        )
        progress_log.append({"phase": "PRE_KILL", **prog})
        _save_wal_checkpoint(state_before_kill, checkpoint_dir)

    pre_kill_in_wm = state_before_kill["sink2_in_wm_events_count"]

    state_after_restart = _init_or_restore_stream_state(checkpoint_dir)
    skipped_by_wal = True
    if kill_after_batch < len(chunks):
        replayed_batch_prog = _process_single_microbatch(
            kill_after_batch, chunks[kill_after_batch], state_after_restart, valid_segment_ids, checkpoint_dir
        )
        progress_log.append({"phase": "POST_RESTART_REPLAY_CHECK", **replayed_batch_prog})
        skipped_by_wal = replayed_batch_prog.get("status") == "SKIPPED_ALREADY_COMMITTED_IN_WAL"

    for b_id in range(kill_after_batch + 1, len(chunks)):
        prog = _process_single_microbatch(
            b_id, chunks[b_id], state_after_restart, valid_segment_ids, checkpoint_dir=checkpoint_dir
        )
        progress_log.append({"phase": "POST_RESTART", **prog})
        _save_wal_checkpoint(state_after_restart, checkpoint_dir)

    total_bronze_recovered = _count_sink1_bronze_on_disk(checkpoint_dir)
    wal_path = checkpoint_dir / "agg" / "state_wal.json"
    wal_size_bytes = int(wal_path.stat().st_size) if wal_path.is_file() else 0

    return {
        "stream_query": "hcmc_traffic_dual_sink_v3",
        "watermark_delay": "30 minutes",
        "trigger_interval": "5 seconds",
        "microbatches_total": len(chunks),
        "simulated_kill_after_batch": kill_after_batch,
        "replayed_batch_skipped_by_wal": skipped_by_wal,
        "restart_recovery_verified": skipped_by_wal,
        "duplicate_side_effects_on_restart": 0 if skipped_by_wal else 1,
        "pre_kill_sink2_in_wm_events": pre_kill_in_wm,
        "post_restart_sink2_in_wm_events": state_after_restart["sink2_in_wm_events_count"],
        "post_restart_dropped_by_watermark": state_after_restart["dropped_by_watermark_sink2"],
        "dropped_by_event_time_watermark": state_after_restart["dropped_by_watermark_sink2"],
        "bloom_false_positives_eval": state_after_restart["bloom_false_positives_eval"],
        "total_bronze_sink1_events_landed": total_bronze_recovered,
        "sink1_bronze_parquet_dir": str(_sink1_bronze_dir(checkpoint_dir)),
        "wal_state_bytes": wal_size_bytes,
        "quarantined_malformed_events": state_after_restart["quarantined_count"],
        "bloom_memory_bytes": int(state_after_restart["rotating_bloom"].nbytes),
        "hll_memory_bytes": int(state_after_restart["hll_registers"].nbytes),
        "hll_estimated_segments": round(hll_estimate(state_after_restart["hll_registers"]), 2),
        "active_decay_segments_in_ram": len(state_after_restart["decay_scores"]),
        "committed_offsets": state_after_restart["committed_offsets"],
        "frozen_registry_integrity": _verify_read_only_online_scoring(),
        "version_guard_audit": _run_version_guard_audit(list(state_after_restart["sample_window_ids"])[:25]),
        "progress_history": progress_log,
    }


def _read_json_safe(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _enrich_progress_with_reconciliation(
    report: dict[str, Any],
    progress_path: Path,
    metrics_path: Path = Path("outputs/metrics.json"),
) -> dict[str, Any]:
    """Preserve Phase B/C reconciliation telemetry when starter_stream.py runs standalone."""
    preserved_keys = (
        "Phase_B_provisional_scored",
        "Phase_B_in_flight_provisional_retained",
        "Phase_C_reconciled_backfilled",
        "healed_downstream_rescored_windows",
        "rescore_prob_changed_windows_count",
        "total_serving_documents",
        "post_train_online_windows_scored",
    )
    existing = _read_json_safe(progress_path)
    for key in preserved_keys:
        if key in existing and key not in report:
            report[key] = existing[key]

    recon = _read_json_safe(metrics_path).get("streaming_mmds", {}).get("reconciliation", {})
    recon_key_map = {
        "healed_downstream_rescored_windows": "healed_downstream_rescored_windows",
        "rescore_prob_changed_windows_count": "rescore_prob_changed_windows_count",
        "Phase_B_in_flight_provisional_retained": "Phase_B_in_flight_provisional_retained",
        "Phase_C_reconciled_backfilled": "affected_late_segment_windows_recomputed",
    }
    for dst_key, src_key in recon_key_map.items():
        if src_key in recon and recon[src_key] is not None:
            report.setdefault(dst_key, int(recon[src_key]))
    return report


def _build_stream_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="HCMC Smart Traffic Dual-Sink Structured Streaming Engine ([C6]-[C8])."
    )
    parser.add_argument(
        "--bronze-path",
        type=Path,
        default=Path("data/lakehouse/bronze_raw_events/events.parquet"),
    )
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("outputs/checkpoints"))
    parser.add_argument("--progress-path", type=Path, default=Path("outputs/query_progress.json"))
    parser.add_argument("--num-microbatches", type=int, default=4)
    parser.add_argument("--kill-after-batch", type=int, default=1)
    parser.add_argument("--from-kafka", action="store_true", help="Consume live events from Kafka broker -> MongoDB")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--topic", type=str, default="hcmc-traffic-events")
    parser.add_argument("--group-id", type=str, default="hcmc-toc-live-consumer")
    parser.add_argument("--mongo-uri", type=str, default="mongodb://127.0.0.1:27017")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--idle-timeout-sec", type=float, default=12.0)
    parser.add_argument("--continuous", action="store_true", help="Keep polling Kafka indefinitely")
    parser.add_argument("--reset-live", action="store_true", help="Clear live demo telemetry/scores before consuming")
    return parser


def main() -> None:
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = _build_stream_parser().parse_args()

    if args.from_kafka:
        from live_kafka_stream import run_live_kafka_consumer

        live_rep = run_live_kafka_consumer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            mongo_uri=args.mongo_uri,
            max_messages=args.max_messages,
            idle_timeout_sec=args.idle_timeout_sec,
            continuous=args.continuous,
            reset_live=args.reset_live,
        )
        print(json.dumps(live_rep, ensure_ascii=False, indent=2))
        return

    if args.bronze_path.is_file():
        events_df = pd.read_parquet(args.bronze_path)
    elif Path("data/demo_sample/sample_windows_23d.csv").is_file():
        events_df = pd.read_csv("data/demo_sample/sample_windows_23d.csv")
    else:
        raise FileNotFoundError(f"Bronze replay stream not found: {args.bronze_path}")
    report = run_stream_with_restart_recovery(
        events_df,
        args.checkpoint_dir,
        num_microbatches=args.num_microbatches,
        kill_after_batch=args.kill_after_batch,
    )
    report = _enrich_progress_with_reconciliation(report, args.progress_path)
    _atomic_write_text(args.progress_path, json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
