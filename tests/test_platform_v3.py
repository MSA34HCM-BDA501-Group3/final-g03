"""Comprehensive Automated Verification Suite for HCMC Smart Traffic Platform (V3).

Covers:
- Sub-Problems P1, P2, P3, P4
- Operational Use Cases UC-1 through UC-6
- Empirical Edge Cases EC-1 through EC-10
- Checklist Requirements Q01-Q11, D01-D12, M01-M14, T01-T05
- Zero-Conflict 3-Phase MLOps Contract ([C13a] & [C13b])
- Product Lifecycle Governance Tree (product/ 00-proposal .. 09-growth)
"""

from __future__ import annotations

import hashlib
import json
import sys
import unicodedata
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import _find_similar_segments, _predict_single_window, _select_forecast_candidates  # noqa: E402
from baseline import (  # noqa: E402
    audit_records_against_monoid,
    monoid_combine,
    monoid_lift,
    verify_unequal_partition_associativity,
)
from pipeline import (  # noqa: E402
    LEVEL_VELOCITY_PRIORS,
    _bloom_hashes,
    _classify_segment_first_failing_rule,
    _hll_add,
    _hll_estimate,
    _is_valid_street_entity,
    _normalize_street_name,
    _parse_period_index,
    _verify_mongodb_ixscan,
    add_temporal_and_neighbor_lags,
    aggregate_status_events,
    align_utc_to_ict,
    apply_vmax_lookup,
    build_medallion_frame,
    compute_chronological_split_boundaries,
    compute_road_pagerank,
    derive_los,
    find_peer_segments,
    fit_train_only_clusters_and_priors,
    fit_train_vmax_lookup,
    inspect_raw_tables,
    mine_co_congestion_rules,
    mine_lagged_street_rules,
    score_feature_snapshot,
    validate_raw_records_ordered,
    verify_two_tier_reconciliation,
)
from producer import generate_raw_ping_replay, generate_replay_Batch  # noqa: E402
from starter_stream import (  # noqa: E402
    create_bitpacked_bloom,
    run_stream_with_restart_recovery,
    update_bounded_decay_state,
    upsert_serving_records_with_version_guard,
)

DATA_DIR = ROOT / "data"
LAKEHOUSE_DIR = DATA_DIR / "lakehouse"
OUTPUTS_DIR = ROOT / "outputs"
PRODUCT_DIR = ROOT / "product"


def _load_metrics() -> dict:
    return json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))


def test_p1_ec1_ec2_ec4_quality_funnel_and_imputation() -> None:
    """[Q02, Q03, Q04, Q05] Verify ordered quality funnel, 3-tier TRAIN-only imputation, and ICT alignment."""
    assert LEVEL_VELOCITY_PRIORS == {1: 80.0, 2: 60.0, 3: 50.0, 4: 40.0}

    metrics = _load_metrics()
    qf = metrics["quality_funnel_c5"]
    assert qf["zero_length_segments"] == 154
    assert qf["non_road_osm_segments"] == 427
    assert qf["street_entities_merged_by_nfc"] > 0
    assert qf["status_accounting_balanced"] is True
    assert (
        qf["status_input_pings"]
        == qf["status_dedup_dropped_pings"] + qf["status_accepted_pings"] + qf["status_rejected_pings"]
    )

    q_df = pd.read_csv(OUTPUTS_DIR / "quarantine_summary.csv")
    assert len(q_df) == 6
    assert (LAKEHOUSE_DIR / "quarantine_events.parquet").is_file()

    df = pd.read_csv(DATA_DIR / "dataset.csv")
    assert len(df) == 33441
    assert int(df["max_velocity"].isna().sum()) == 0
    assert bool((df["length_m"] > 0).all())
    assert bool(((df["vn_hour"] >= 0.0) & (df["vn_hour"] < 24.0)).all())
    trig_norm = np.square(df["period_sin"]) + np.square(df["period_cos"])
    assert np.allclose(trig_norm, 1.0, atol=1e-6)

    tier_counts = df["vmax_impute_tier"].value_counts().to_dict()
    assert tier_counts["TIER_1_SEGMENT_OBSERVED"] == 4946
    assert tier_counts["TIER_1_SEGMENT_TRAIN_P85"] == 8164
    assert sum(tier_counts.values()) == 33441
    assert qf["vmax_imputation_tiers_windows"] == tier_counts


def test_q01_q02_q03_q04_q05_unit_and_dirty_fixtures() -> None:
    """[Q01-Q05] Test dirty record first-failing-rule funnel, LOS boundaries, ICT rollover, and VAL invariance."""
    # Q02/Q03: Dirty segment violating both R1 (length<=0) and R2 (invalid street_type) -> only R1 recorded
    dirty_seg = {"_id": 99, "length": 0.0, "street_type": "invalid_osm_tag"}
    assert _classify_segment_first_failing_rule(dirty_seg) == "R1_NONPOSITIVE_SEGMENT_LENGTH"
    malformed_seg = {"_id": "bad", "length": -5.0, "street_type": "invalid_osm_tag"}
    assert _classify_segment_first_failing_rule(malformed_seg) == "R0_MISSING_OR_MALFORMED_FIELD"

    # Q02/Q03: Dirty raw status records with duplicates, malformed, unknown segment, and out-of-range speed
    dirty_status = pd.DataFrame(
        [
            {"_id": 1, "segment_id": 25, "velocity": 30.0, "updated_at": "2020-08-02 07:00:00+00"},
            {"_id": 1, "segment_id": 25, "velocity": 30.0, "updated_at": "2020-08-02 07:00:00+00"},
            {"_id": 2, "segment_id": -1, "velocity": "not_a_float", "updated_at": "2020-08-02 07:00:00+00"},
            {"_id": 3, "segment_id": 999999, "velocity": 0.0, "updated_at": "2020-08-02 07:00:00+00"},
            {"_id": 4, "segment_id": 25, "velocity": 220.0, "updated_at": "2020-08-02 07:00:00+00"},
        ]
    )
    funnel_res = validate_raw_records_ordered(dirty_status, valid_segment_ids={25})
    assert funnel_res["input_total"] == 5
    assert funnel_res["dedup_dropped"] == 1
    assert funnel_res["accepted_count"] == 1
    assert funnel_res["rejected_count"] == 3
    assert funnel_res["accounting_balanced"] is True
    q_rules = funnel_res["quarantine_df"]["first_failing_rule"].tolist()
    assert q_rules == [
        "R0_MISSING_OR_MALFORMED_FIELD",
        "R1_UNKNOWN_OR_INVALID_SEGMENT",
        "R2_VELOCITY_OUT_OF_RANGE",
    ]

    # Q05: Exact LOS boundary derivation and v=0 / NaN
    assert derive_los(0.80) == "A" and derive_los(0.7999) == "B"
    assert derive_los(0.65) == "B" and derive_los(0.6499) == "C"
    assert derive_los(0.50) == "C" and derive_los(0.4999) == "D"
    assert derive_los(0.35) == "D" and derive_los(0.3499) == "E"
    assert derive_los(0.20) == "E" and derive_los(0.1999) == "F"
    assert derive_los(0.0) == "F" and derive_los(np.nan) == "F"

    # Q05: UTC 17:00 -> ICT 00:00 next day rollover + unseen period handling
    utc_df = pd.DataFrame(
        {
            "date": ["2020-08-02", "2020-08-02"],
            "period": ["period_17_00", "period_99_99"],
        }
    )
    ict_df = align_utc_to_ict(utc_df)
    assert ict_df.loc[0, "ict_date"] == "2020-08-03"
    assert ict_df.loc[0, "vn_hour"] == 0.0
    assert ict_df.loc[0, "ict_period"] == "period_0_00"
    assert int(ict_df.loc[1, "is_unknown_period"]) == 1
    assert float(ict_df.loc[1, "period_sin"]) == 0.0 and float(ict_df.loc[1, "period_cos"]) == 0.0

    # Q04: Perturbing VAL/TEST windows does not alter fit_train_vmax_lookup
    toy_win = pd.DataFrame(
        {
            "segment_id": [1, 2, 3],
            "street_level": [1, 2, 4],
            "street_type": ["primary", "secondary", "residential"],
            "max_velocity": [60.0, np.nan, np.nan],
            "mean_velocity": [45.0, 35.0, 20.0],
        }
    )
    lookup_a = fit_train_vmax_lookup(toy_win.iloc[:2])
    toy_win_perturbed = toy_win.copy()
    toy_win_perturbed.loc[2, "max_velocity"] = 120.0
    lookup_b = fit_train_vmax_lookup(toy_win_perturbed.iloc[:2])
    assert lookup_a == lookup_b
    applied = apply_vmax_lookup(toy_win, lookup_a)
    assert float(applied.loc[2, "max_velocity"]) == 40.0
    assert applied.loc[2, "vmax_impute_tier"] == "TIER_3_HIERARCHICAL_LEVEL_PRIOR"


def test_p1_ec8_monoid_salting_and_medallion_lakehouse() -> None:
    """[Q06, Q07, Q08, Q09, Q10] Verify Monoid baseline, unequal partition associativity, salting & Parquet."""
    monoid_verify = json.loads(
        (OUTPUTS_DIR / "monoid_baseline_verification.json").read_text(encoding="utf-8")
    )
    assert monoid_verify["raw_distinct_utc_windows"] == 33441
    assert monoid_verify["medallion_matched_windows"] == 33441
    assert monoid_verify["missing_keys_in_dataset"] == 0
    assert monoid_verify["extra_keys_in_dataset"] == 0
    assert monoid_verify["utc_window_join_match_pct"] == 100.0
    assert monoid_verify["obs_count_discrepancies"] == 0
    assert monoid_verify["min_velocity_discrepancies"] == 0
    assert monoid_verify["max_velocity_discrepancies"] == 0
    assert monoid_verify["severe_ping_discrepancies"] == 0
    assert monoid_verify["mean_velocity_discrepancies"] == 0
    assert monoid_verify["total_severe_pings_accumulated"] == 42170
    assert monoid_verify["monoid_associativity_verified"] is True

    assoc = verify_unequal_partition_associativity()
    assert assoc["monoid_tuple_exact_match"] is True
    assert assoc["average_of_averages_trap_demonstrated"] is True

    # Q09 Failure Injection: Mutating min_velocity or mean_velocity must fail audit_records_against_monoid
    m_state = {(10, "2020-08-02", "period_7_00"): monoid_combine(monoid_lift(20.0), monoid_lift(40.0))}
    good_records = [
        {
            "segment_id": 10,
            "date": "2020-08-02",
            "period": "period_7_00",
            "obs_count": 2,
            "sum_velocity": 60.0,
            "min_velocity": 20.0,
            "max_velocity_obs": 40.0,
            "severe_ping_count": 0,
            "mean_velocity": 30.0,
        }
    ]
    assert audit_records_against_monoid(m_state, good_records)["monoid_associativity_verified"] is True
    bad_records = [{**good_records[0], "min_velocity": 25.0}]
    bad_audit = audit_records_against_monoid(m_state, bad_records)
    assert bad_audit["min_velocity_discrepancies"] == 1
    assert bad_audit["monoid_associativity_verified"] is False

    join_plan = json.loads((OUTPUTS_DIR / "join_plan_evidence.json").read_text(encoding="utf-8"))
    salting = join_plan["pillar_i1_monoid_and_salting"]
    assert salting["exact_monoid_equivalence"] is True
    assert salting["raw_input_pings"] == 90938
    assert salting["stage1_partial_salted_groups"] == 59895
    assert salting["stage2_reduced_windows"] == 33441
    assert salting["salt_bucket_cv"] < 0.02
    assert 1.0 <= salting["salted_partition_skew_ratio"] <= 1.05
    assert join_plan["pillar_i2_catalyst_join_optimization"]["partition_count"] == 10

    assert (LAKEHOUSE_DIR / "bronze_raw_events" / "events.parquet").is_file()
    assert (LAKEHOUSE_DIR / "silver_cleaned_windows" / "windows.parquet").is_file()
    gold_parts = list((LAKEHOUSE_DIR / "curated_gold").glob("year_month=*"))
    assert len(gold_parts) == 10


def test_p2_uc3_ec7_orphan_pruning_and_line_graph_pagerank() -> None:
    """[D05] Verify orphan node pruning, real road line-graph PageRank, and toy graph mass conservation."""
    metrics = _load_metrics()
    ds = metrics["dataset"]
    assert ds["nodes_count"] == 577967
    assert ds["active_intersection_nodes_count"] == 52425
    assert ds["pruned_orphan_nodes_count"] == 525542
    assert ds["segments_count"] == 84633
    assert ds["line_graph_transition_edges"] == 151929
    assert ds["dangling_nodes_count"] == 48

    pr_df = pd.read_csv(OUTPUTS_DIR / "road_pagerank_top.csv")
    assert len(pr_df) == 50
    assert int(pr_df.iloc[0]["segment_id"]) == 61781
    assert pr_df.iloc[0]["street_name"] == "Phú Định"
    assert abs(float(pr_df.iloc[0]["pagerank_score"]) - 3.7515) < 1e-3

    # D05: Toy directed line-graph with a dangling node (seg 3 has no outgoing segment)
    toy_segments = pd.DataFrame(
        {
            "_id": [1, 2, 3],
            "s_node_id": [10, 20, 30],
            "e_node_id": [20, 30, 40],
            "street_id": [1, 1, 2],
            "street_name": ["A", "A", "B"],
            "street_level": [2, 2, 3],
            "street_type": ["primary", "primary", "secondary"],
            "length": [50.0, 60.0, 70.0],
            "max_velocity": [60.0, 60.0, 50.0],
        }
    )
    toy_pr = compute_road_pagerank(toy_segments, damping=0.85, iterations=30)
    assert abs(float(toy_pr["pagerank_score"].sum()) - 3.0) < 1e-9
    assert int(toy_pr["dangling_nodes_total"].iloc[0]) == 1
    assert float(toy_pr.loc[toy_pr["segment_id"] == 3, "pagerank_score"].iloc[0]) > float(
        toy_pr.loc[toy_pr["segment_id"] == 1, "pagerank_score"].iloc[0]
    )


def test_p2_uc4_ec3_nfc_street_hygiene_and_fpgrowth_rules() -> None:
    """[D06] Verify NFC hygiene, untruncated 2-Pass SON bidirectional rules, and lagged transition rules."""
    nfd_str = unicodedata.normalize("NFD", "Đường Phạm Văn Đồng")
    nfc_str = _normalize_street_name("Ðường Phạm Văn Đồng")
    assert _normalize_street_name(nfd_str) == "Phạm Văn Đồng"
    assert nfc_str == "Phạm Văn Đồng"
    assert _normalize_street_name("Đường số 20") == "Đường số 20"
    assert _normalize_street_name("Xa lộ Hà Nội") == "Xa Lộ Hà Nội"
    assert _normalize_street_name(None) == ""
    assert not _is_valid_street_entity("Phường Hiệp Bình Chánh")
    assert not _is_valid_street_entity("Quận Gò Vấp")
    assert _is_valid_street_entity("Lý Chính Thắng")

    rules_df = pd.read_csv(OUTPUTS_DIR / "co_congestion_rules.csv")
    assert len(rules_df) == 30
    assert bool(
        (rules_df["antecedent_street"].str.lower() != rules_df["consequent_street"].str.lower()).all()
    )
    # D06: Verify both directions (A => B and B => A) are generated
    pairs_seen = set(zip(rules_df["antecedent_street"], rules_df["consequent_street"]))
    assert ("Lý Chính Thắng", "Nguyễn Thông") in pairs_seen
    assert ("Nguyễn Thông", "Lý Chính Thắng") in pairs_seen

    lagged_df = pd.read_csv(OUTPUTS_DIR / "lagged_congestion_rules.csv")
    assert len(lagged_df) > 0

    # D06: Toy basket with 30 streets (> 25) to prove no basket[:25] truncation occurs
    many_streets = [f"Tuyến Đường {i:02d}" for i in range(30)]
    toy_rows = [
        {"date": f"2020-08-{d:02d}", "period": "period_8_00", "street_name": s, "is_severe": 1}
        for d in range(1, 6)
        for s in many_streets
    ]
    toy_rules = mine_co_congestion_rules(pd.DataFrame(toy_rows), min_support_count=5, top_k=2000)
    assert "Tuyến Đường 29" in set(toy_rules["antecedent_street"])
    assert "Tuyến Đường 29" in set(toy_rules["consequent_street"])


def test_p2_uc5_ec5_ec6_7d_kmeans_and_cold_start_feature_store() -> None:
    """[D04, D07] Verify 7D K-Means clustering, 84,633-segment Static Store, and Cold-Start peer lookup."""
    metrics = _load_metrics()
    model_m = metrics["model"]
    assert model_m["clusters_populated"] == 6
    assert model_m["segments_clustered"] > 6000
    assert float(model_m["silhouette_score"]) > 0.20

    static_df = pd.read_parquet(LAKEHOUSE_DIR / "segment_static_features.parquet")
    assert len(static_df) == 84633
    assert int((~static_df["is_cold_start"]).sum()) == 10027
    assert int(static_df["is_cold_start"].sum()) == 74606
    assert int(static_df["max_velocity"].isna().sum()) == 0
    assert abs(float(static_df["pagerank_score"].sum()) - 84633.0) < 1e-3

    # D07: Cold-Start segment peer lookup must return Top-K peers in the same 7D K-Means cluster
    cold_seg_id = int(static_df[static_df["is_cold_start"]].iloc[0]["segment_id"])
    cold_cluster = int(static_df[static_df["segment_id"] == cold_seg_id].iloc[0]["cluster_id"])
    peers = find_peer_segments(static_df, cold_seg_id, limit=8)
    assert len(peers) == 8
    assert bool((peers["cluster_id"] == cold_cluster).all())
    assert cold_seg_id not in set(peers["segment_id"])

    df = pd.read_csv(DATA_DIR / "dataset.csv")
    ui_peers = _find_similar_segments(df, cold_seg_id, limit=5, static_store_df=static_df)
    assert len(ui_peers) == 5


def test_d01_d02_d03_d04_exact_t30m_horizon_purge_and_zero_leakage() -> None:
    """[D01, D02, D03, D04] Test exact +30m horizon (rejecting 60m/9h gaps), cutoff purge, and causal invariance."""
    # D01: Gap of 30m -> valid target; gap of 60m and 540m (9h) -> target must be NaN!
    gap_df = pd.DataFrame(
        {
            "segment_id": [101, 101, 101, 101],
            "street_id": [10, 10, 10, 10],
            "date": ["2020-08-01", "2020-08-01", "2020-08-01", "2020-08-01"],
            "period": ["period_1_00", "period_1_30", "period_2_30", "period_11_30"],
            "slot_ts": pd.to_datetime(
                [
                    "2020-08-01T01:00:00Z",
                    "2020-08-01T01:30:00Z",
                    "2020-08-01T02:30:00Z",
                    "2020-08-01T11:30:00Z",
                ]
            ),
            "los_ordinal": [1, 4, 2, 5],
            "is_severe": [0, 1, 0, 1],
            "mean_velocity": [40.0, 12.0, 35.0, 8.0],
            "velocity_ratio": [0.8, 0.24, 0.7, 0.16],
            "length_m": [50.0, 50.0, 50.0, 50.0],
            "max_velocity": [50.0, 50.0, 50.0, 50.0],
            "pagerank_score": [1.0, 1.0, 1.0, 1.0],
            "upstream_segment_id": [101, 101, 101, 101],
        }
    )
    lagged = add_temporal_and_neighbor_lags(gap_df)
    # Row 0 (01:00 -> 01:30 = +30m): has exact +30m target = 4
    assert float(lagged.loc[0, "target_t30m_los_ordinal"]) == 4.0
    assert float(lagged.loc[0, "target_next_los_ordinal"]) == 4.0
    # Row 1 (01:30 -> 02:30 = +60m gap): must NOT have +30m target!
    assert pd.isna(lagged.loc[1, "target_t30m_los_ordinal"])
    assert float(lagged.loc[1, "next_obs_los_ordinal"]) == 2.0
    assert int(lagged.loc[1, "is_contiguous_30m"]) == 1
    # Row 2 (02:30 -> 11:30 = +9h gap): prev_gap is 60m -> is_contiguous_30m == 0, and +30m target is NaN
    assert int(lagged.loc[2, "is_contiguous_30m"]) == 0
    assert pd.isna(lagged.loc[2, "target_t30m_los_ordinal"])

    # D03 & D04: Causal expanding priors and cluster_id must be 100% invariant to future perturbations
    timestamps = pd.date_range("2020-08-01T00:00:00Z", periods=10, freq="30min")
    rows = []
    for tidx, ts in enumerate(timestamps):
        for sid in (101, 102, 201, 202):
            rows.append(
                {
                    "segment_id": sid,
                    "street_id": 10 if sid < 200 else 20,
                    "date": ts.strftime("%Y-%m-%d"),
                    "period": f"period_{ts.hour}_{ts.minute:02d}",
                    "slot_ts": ts,
                    "los_ordinal": (sid + tidx) % 6,
                    "is_severe": int(((sid + tidx) % 6) >= 4),
                    "mean_velocity": 30.0 - tidx,
                    "velocity_ratio": 0.6 - 0.02 * tidx,
                    "length_m": 40.0 + sid * 0.1,
                    "max_velocity": 50.0,
                    "pagerank_score": 1.0 + (sid % 3) * 0.2,
                    "upstream_segment_id": sid,
                }
            )
    synth_df = add_temporal_and_neighbor_lags(pd.DataFrame(rows))
    split_a = compute_chronological_split_boundaries(synth_df)
    assert bool((synth_df.loc[split_a["sup_train_mask"], "target_ts"] <= split_a["train_cutoff_ts"]).all())
    assert bool((synth_df.loc[split_a["sup_val_mask"], "target_ts"] <= split_a["val_cutoff_ts"]).all())

    clustered_a, _, _ = fit_train_only_clusters_and_priors(
        synth_df, split_a["frame_train_mask"], n_clusters=2, warmup_mask=split_a["frame_warmup_mask"]
    )

    # Perturb rows at the last 4 timestamps (including late intra-TRAIN and VAL/TEST)
    raw_perturbed = pd.DataFrame(rows)
    late_cutoff = timestamps[5]
    raw_perturbed.loc[raw_perturbed["slot_ts"] > late_cutoff, "los_ordinal"] = 5
    raw_perturbed.loc[raw_perturbed["slot_ts"] > late_cutoff, "is_severe"] = 1
    raw_perturbed.loc[raw_perturbed["slot_ts"] > late_cutoff, "mean_velocity"] = 5.0
    synth_b = add_temporal_and_neighbor_lags(raw_perturbed)
    clustered_b, _, _ = fit_train_only_clusters_and_priors(
        synth_b, split_a["frame_train_mask"], n_clusters=2, warmup_mask=split_a["frame_warmup_mask"]
    )

    early_a = clustered_a[clustered_a["slot_ts"] <= late_cutoff]
    early_b = clustered_b[clustered_b["slot_ts"] <= late_cutoff]
    for col in (
        "segment_train_prior_los",
        "segment_train_severe_prior",
        "street_causal_prior_los",
        "street_causal_prior_sev",
        "cluster_id",
    ):
        assert np.allclose(early_a[col].to_numpy(), early_b[col].to_numpy(), atol=1e-12)


def test_p3_ml_ablation_and_zero_conflict_registry() -> None:
    """[D08, D09, D10, D11, D12] Verify +30m evaluation, VAL gate, honest GBT binary table & 3-artifact hashes."""
    metrics = _load_metrics()
    ev = metrics["evaluation"]
    assert ev["train_target_cutoff_purge_verified"] is True
    assert ev["val_target_cutoff_purge_verified"] is True
    assert ev["train_samples"] == 5494
    assert ev["val_samples"] == 1167
    assert ev["test_samples"] == 1182
    assert ev["roc_auc"] > 0.90
    assert ev["average_precision"] > 0.85
    assert "validation_gate_selection" in ev
    assert len(ev["validation_gate_selection"]["candidates"]) == 6

    # D10: GBT binary rows must have task_type == BINARY_SEVERE_EF_ONLY and null 6-class metrics
    ablation_df = pd.read_csv(OUTPUTS_DIR / "ablation_results.csv")
    gbt_rows = ablation_df[ablation_df["model"].str.startswith("GBT")]
    assert len(gbt_rows) == 2
    assert bool((gbt_rows["task_type"] == "BINARY_SEVERE_EF_ONLY").all())
    assert bool(gbt_rows["accuracy_6class"].isna().all())

    # D11: All 3 Phase A artifacts must match their frozen SHA-256 digests
    manifest = json.loads((OUTPUTS_DIR / "model_registry_manifest.json").read_text(encoding="utf-8"))
    model_sha = hashlib.sha256((OUTPUTS_DIR / "model.joblib").read_bytes()).hexdigest()
    static_sha = hashlib.sha256((LAKEHOUSE_DIR / "segment_static_features.parquet").read_bytes()).hexdigest()
    vmax_sha = hashlib.sha256((OUTPUTS_DIR / "vmax_lookup.json").read_bytes()).hexdigest()
    assert manifest["phase_a_fit_only"] is True
    assert manifest["phase_b_c_read_only_verified"] is True
    assert manifest["all_three_artifacts_immutable_verified"] is True
    assert manifest["sha256_digest"] == model_sha
    assert manifest["static_feature_store_sha256"] == static_sha
    assert manifest["vmax_lookup_sha256"] == vmax_sha

    # D12: Shared scorer parity against golden_predictions.parquet
    golden = pd.read_parquet(OUTPUTS_DIR / "golden_predictions.parquet")
    silver = pd.read_parquet(LAKEHOUSE_DIR / "silver_cleaned_windows" / "windows.parquet").head(200)
    artifact = joblib.load(OUTPUTS_DIR / "model.joblib")
    rescored = score_feature_snapshot(silver, artifact)
    assert np.allclose(golden["severe_prob"].to_numpy(), rescored["severe_prob"].to_numpy(), atol=1e-9)
    assert (golden["predicted_next_los"] == rescored["predicted_next_los"]).all()


def test_p4_uc1_uc6_ec8_ec9_stream_sketches_and_reconciliation() -> None:
    """[M05, M10, M11, M12, T04] Verify 32 KiB bit-packed Bloom, Tier 1/2 Recon, Version Guard & WAL Recovery."""
    # M05: Bit-packed Bloom filter must be exactly 32,768 bytes (32.0 KiB)
    bloom_arr = create_bitpacked_bloom(262144)
    assert bloom_arr.nbytes == 32768

    # M05: Bounded decay state evicts tiny scores below epsilon
    decay_state: dict[int, float] = {}
    update_bounded_decay_state(decay_state, 101, 1, decay_alpha=0.5, epsilon=0.2)
    assert 101 in decay_state
    for _ in range(5):
        update_bounded_decay_state(decay_state, 101, 0, decay_alpha=0.5, epsilon=0.2)
    assert 101 not in decay_state

    metrics = _load_metrics()
    sm = metrics["streaming_mmds"]
    assert sm["total_stream_events_processed"] == 33775
    assert sm["valid_unique_events"] == 33441
    assert sm["injected_duplicate_events"] == 334
    assert sm["duplicates_removed_exact"] == 334
    assert sm["injected_late_in_watermark"] == 1170
    assert sm["injected_late_dropped_by_watermark"] == 501
    assert sm["bloom_filter"]["bit_packed_uint8_bytes"] == 32768
    assert sm["bloom_filter"]["memory_kb"] == 32.0
    assert sm["hyperloglog"]["memory_kb"] == 1.0
    assert sm["hyperloglog"]["relative_error_pct"] < 5.0

    recon = sm["reconciliation"]
    assert recon["in_watermark_like_for_like_match_pct"] == 100.0
    assert recon["tier1_exceptAll_diff_count"] == 0
    assert recon["tier1_verified"] is True
    assert recon["pre_reconciliation_completeness_pct"] == 98.5
    assert recon["post_reconciliation_completeness_pct"] == 100.0

    # M10 Failure Injection: Mutating 1 in-watermark row must cause verify_two_tier_reconciliation to fail!
    toy_stream = pd.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "segment_id": [1, 2],
            "date": ["2020-08-02", "2020-08-02"],
            "period": ["period_7_00", "period_7_00"],
            "los_ordinal": [2, 4],
            "mean_velocity": [30.0, 12.0],
        }
    )
    toy_batch = toy_stream.copy()
    assert verify_two_tier_reconciliation(toy_stream, toy_batch, toy_batch)["tier1_verified"] is True
    toy_stream_mutated = toy_stream.copy()
    toy_stream_mutated.loc[0, "los_ordinal"] = 5
    mut_audit = verify_two_tier_reconciliation(toy_stream_mutated, toy_batch, toy_batch)
    assert mut_audit["tier1_verified"] is False
    assert mut_audit["tier1_exceptAll_diff_count"] > 0
    assert mut_audit["in_watermark_like_for_like_match_pct"] < 100.0

    # M11: Version guard must prevent stale STREAM_PROVISIONAL (v1) from overwriting BATCH_RECONCILED (v2)
    store: dict[str, dict] = {}
    upsert_serving_records_with_version_guard(
        store, [{"event_id": "ev1", "lifecycle_status": "BATCH_RECONCILED", "status_version": 2, "los": "E"}]
    )
    guard_res = upsert_serving_records_with_version_guard(
        store, [{"event_id": "ev1", "lifecycle_status": "STREAM_PROVISIONAL", "status_version": 1, "los": "A"}]
    )
    assert guard_res["stale_provisional_rejected"] == 1
    assert store["ev1"]["lifecycle_status"] == "BATCH_RECONCILED"
    assert store["ev1"]["los"] == "E"

    # M12: WAL Checkpoint kill & restart recovery verification + Tier 2 lag healing rescore
    progress = json.loads((OUTPUTS_DIR / "query_progress.json").read_text(encoding="utf-8"))
    assert progress["restart_recovery_verified"] is True
    assert progress["duplicate_side_effects_on_restart"] == 0
    assert progress["version_guard_audit"]["stale_provisional_retries_rejected"] == 25
    assert progress["healed_downstream_rescored_windows"] == 377
    assert progress["rescore_prob_changed_windows_count"] == 318


def test_p4_uc2_ec10_mongodb_ixscan_and_honest_fallback(tmp_path: Path) -> None:
    """[M07, M08, M09, T04] Verify Mongo IXSCAN report AND honest 'not_verified' when Mongo is disabled."""
    mongo_verify = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    assert mongo_verify["write_partitions"] == 4
    assert mongo_verify["batch_size"] == 500
    if mongo_verify.get("status") == "verified":
        assert mongo_verify["total_rows_upserted"] == 33441
        ix = mongo_verify["ixscan_explain_verification"]
        assert ix["index_used"] == "level_prob_segment_idx"
        assert "IXSCAN" in ix["winning_stages"]
        assert ix["in_memory_sort"] is False

    # M08 / T04 Failure Injection: When Mongo container is disabled/unreachable, status MUST be 'not_verified'
    serving_sample = pd.read_parquet(LAKEHOUSE_DIR / "serving_traffic_scores.parquet").head(20)
    unverified_path = tmp_path / "mongo_unverified.json"
    unverified = _verify_mongodb_ixscan(
        serving_sample, unverified_path, "dummy_sha", mongo_container=""
    )
    assert unverified["status"] == "not_verified"
    assert unverified["ixscan_explain_verification"]["status"] == "not_verified"
    assert unverified["ixscan_explain_verification"]["verified_against_mongo_8_0"] is False
    assert unverified["ixscan_explain_verification"]["index_used"] is None


def test_sketch_and_period_unit_edge_cases() -> None:
    positions = _bloom_hashes("edge_case_event_001", num_bits=262144, num_hashes=4)
    assert len(positions) == 4
    assert all(0 <= p < 262144 for p in positions)

    reg = np.zeros(1024, dtype=np.uint8)
    for idx in range(200):
        _hll_add(reg, f"seg_{idx}", p_bits=10)
    est = _hll_estimate(reg)
    assert 170.0 <= est <= 230.0

    periods = pd.Series(["period_0_00", "period_7_30", "period_23_30", "invalid_period"])
    parsed = _parse_period_index(periods).tolist()
    assert parsed == [0, 15, 47, -1]

    df = pd.read_csv(DATA_DIR / "dataset.csv")
    sparse_candidates = _select_forecast_candidates(df, "Tất cả tuyến đường", True)
    assert len(sparse_candidates) > 0
    assert int(sparse_candidates["target_next_los_ordinal"].isna().sum()) > 0
    single_row = sparse_candidates[sparse_candidates["target_next_los_ordinal"].isna()].iloc[0]
    artifact = joblib.load(OUTPUTS_DIR / "model.joblib")
    pred = _predict_single_window(single_row, artifact)
    assert pred["rf_los"] in {"A", "B", "C", "D", "E", "F"}
    assert "N/A" in pred["actual_next_los"]


def test_cold_start_scoring_and_cli_contracts(tmp_path: Path) -> None:
    """[D12, M01, M02, M06, M12] Test Cold-Start score_feature_snapshot, starter_stream CLI & producer raw mode."""
    import subprocess

    static_df = pd.read_parquet(LAKEHOUSE_DIR / "segment_static_features.parquet")
    cold_rows = static_df[static_df["is_cold_start"]].head(25).copy()
    assert len(cold_rows) == 25
    artifact = joblib.load(OUTPUTS_DIR / "model.joblib")
    cold_scored = score_feature_snapshot(cold_rows, artifact)
    assert len(cold_scored) == 25
    assert set(cold_scored["predicted_next_los"]).issubset({"A", "B", "C", "D", "E", "F"})
    assert bool(((cold_scored["severe_prob"] >= 0.0) & (cold_scored["severe_prob"] <= 1.0)).all())

    # Test producer.py --mode raw without dataset.csv
    raw_out = tmp_path / "raw_only_summary.json"
    proc_prod = subprocess.run(
        [
            sys.executable,
            str(ROOT / "producer.py"),
            "--mode",
            "raw",
            "--dataset-path",
            str(tmp_path / "nonexistent_dataset.csv"),
            "--limit",
            "200",
            "--output-summary",
            str(raw_out),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc_prod.returncode == 0
    raw_summary = json.loads(raw_out.read_text(encoding="utf-8"))
    assert raw_summary["replay_mode"] == "raw_gps_pings"
    assert raw_summary["unique_valid_events"] == 200

    # Test starter_stream.py --kill-after-batch 1 CLI contract (matches docker-compose.yml)
    prog_out = tmp_path / "stream_progress.json"
    ckpt_dir = tmp_path / "stream_ckpt"
    proc_stream = subprocess.run(
        [
            sys.executable,
            str(ROOT / "starter_stream.py"),
            "--kill-after-batch",
            "1",
            "--num-microbatches",
            "4",
            "--checkpoint-dir",
            str(ckpt_dir),
            "--progress-path",
            str(prog_out),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc_stream.returncode == 0
    stream_rep = json.loads(prog_out.read_text(encoding="utf-8"))
    assert stream_rep["restart_recovery_verified"] is True
    assert stream_rep["duplicate_side_effects_on_restart"] == 0


def test_t01_end_to_end_mini_fixture_without_train_csv(tmp_path: Path) -> None:
    """[Q01, T01] Run end-to-end window building, PageRank, lags & stream recovery without train.csv."""
    raw_dir = tmp_path / "raw_mini"
    raw_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {"_id": [1, 2, 3], "long": [106.6, 106.7, 106.8], "lat": [10.7, 10.8, 10.9]}
    ).to_csv(raw_dir / "nodes.csv", index=False)
    pd.DataFrame(
        {"_id": [10, 20], "name": ["Đường Lê Lợi", "Nguyễn Huệ"], "level": [1, 2], "type": ["primary", "secondary"]}
    ).to_csv(raw_dir / "streets.csv", index=False)
    pd.DataFrame(
        {
            "_id": [100, 200],
            "s_node_id": [1, 2],
            "e_node_id": [2, 3],
            "length": [120.0, 95.0],
            "street_id": [10, 20],
            "max_velocity": [60.0, np.nan],
            "street_level": [1, 2],
            "street_name": ["Đường Lê Lợi", "Nguyễn Huệ"],
            "street_type": ["primary", "secondary"],
        }
    ).to_csv(raw_dir / "segments.csv", index=False)
    pd.DataFrame(
        {
            "_id": [1, 2, 3, 4],
            "segment_id": [100, 100, 200, 200],
            "velocity": [48.0, 12.0, 35.0, 10.0],
            "updated_at": [
                "2020-08-02 07:05:00+00",
                "2020-08-02 07:35:00+00",
                "2020-08-02 07:10:00+00",
                "2020-08-02 07:40:00+00",
            ],
        }
    ).to_csv(raw_dir / "segment_status.csv", index=False)

    tables = inspect_raw_tables(raw_dir, tmp_path / "inv.csv", require_train=False)
    assert "train" not in tables
    status_agg = aggregate_status_events(tables["segment_status"])
    graph_df = compute_road_pagerank(tables["segments"])
    medallion = build_medallion_frame(
        None,
        status_agg,
        graph_df,
        nodes_df=tables["nodes"],
        streets_df=tables["streets"],
        segments_df=tables["segments"],
    )
    medallion = add_temporal_and_neighbor_lags(medallion)
    assert len(medallion) == 4
    assert int(medallion["target_t30m_los_ordinal"].notna().sum()) == 2
    lag_rules = mine_lagged_street_rules(medallion, min_support_count=1)
    assert isinstance(lag_rules, pd.DataFrame)

    raw_replay, raw_sum = generate_raw_ping_replay(tables["segment_status"])
    assert raw_sum["unique_valid_events"] == 4
    win_replay, _ = generate_replay_Batch(medallion)
    wal_res = run_stream_with_restart_recovery(
        win_replay, tmp_path / "ckpt", num_microbatches=2, kill_after_batch=0
    )
    assert wal_res["restart_recovery_verified"] is True


def test_product_lifecycle_governance_tree() -> None:
    expected_files = [
        PRODUCT_DIR / "00-proposal" / "project-proposal.md",
        PRODUCT_DIR / "00-proposal" / "product-vision.md",
        PRODUCT_DIR / "01-discovery" / "validation-report.md",
        PRODUCT_DIR / "02-strategy" / "product-strategy.md",
        PRODUCT_DIR / "03-requirements" / "prd.md",
        PRODUCT_DIR / "04-design" / "wireframes" / "dashboard-5tabs-wireframe.md",
        PRODUCT_DIR / "05-architecture" / "system-design.md",
        PRODUCT_DIR / "05-architecture" / "architecture-decisions" / "adr-001-zero-conflict-mlops.md",
        PRODUCT_DIR / "05-architecture" / "architecture-decisions" / "adr-002-two-tier-reconciliation.md",
        PRODUCT_DIR / "05-architecture" / "architecture-decisions" / "adr-003-causal-priors-7d-kmeans.md",
        PRODUCT_DIR / "06-engineering" / "sprint-plans" / "sprint-plan-v3.md",
        PRODUCT_DIR / "07-quality" / "release-checklist.md",
        PRODUCT_DIR / "07-quality" / "test-cases" / "test-matrix-p1-p4-uc1-uc6-ec1-ec10.md",
        PRODUCT_DIR / "08-launch" / "operations-runbook.md",
        PRODUCT_DIR / "09-growth" / "growth-roadmap.md",
    ]
    for path in expected_files:
        assert path.is_file(), f"Missing product governance document: {path}"

