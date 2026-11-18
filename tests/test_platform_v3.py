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
import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from toc_backend.service_helpers import (  # noqa: E402
    SPARK_VERIFY_PATH,
    _fetch_live_mongo_status,
    _find_similar_segments,
    _predict_single_window,
    _select_forecast_candidates,
)
from baseline import (  # noqa: E402
    audit_records_against_monoid,
    monoid_combine,
    monoid_lift,
    verify_unequal_partition_associativity,
)
from pipeline import (  # noqa: E402
    LEVEL_VELOCITY_PRIORS,
    _attach_causal_priors_and_neighbors,
    _bloom_hashes,
    _classify_segment_first_failing_rule,
    _hll_add,
    _hll_estimate,
    _is_valid_street_entity,
    _normalize_street_name,
    _parse_period_index,
    _safe_binary_ranking_metrics,
    _verify_mongodb_ixscan,
    add_temporal_and_neighbor_lags,
    aggregate_status_events,
    align_utc_to_ict,
    apply_vmax_lookup,
    build_medallion_frame,
    compute_chronological_split_boundaries,
    compute_road_pagerank,
    derive_los,
    derive_urban_los,
    derive_urban_los_series,
    evaluate_transition_regimes,
    filter_clean_raw_tables,
    find_peer_segments,
    fit_train_only_clusters_and_priors,
    fit_train_vmax_lookup,
    inspect_raw_tables,
    mine_co_congestion_rules,
    mine_lagged_street_rules,
    persist_medallion_lakehouse,
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
    verify_frozen_registry_integrity,
)

DATA_DIR = ROOT / "data"
LAKEHOUSE_DIR = DATA_DIR / "lakehouse"
OUTPUTS_DIR = ROOT / "outputs"
PRODUCT_DIR = ROOT / "product"

HAS_FULL_DATASET = (DATA_DIR / "dataset.csv").is_file()
HAS_FULL_LAKEHOUSE = (
    (LAKEHOUSE_DIR / "segment_static_features.parquet").is_file()
    and (LAKEHOUSE_DIR / "silver_cleaned_windows" / "windows.parquet").is_file()
    and (LAKEHOUSE_DIR / "serving_traffic_scores.parquet").is_file()
    and (LAKEHOUSE_DIR / "bronze_raw_events" / "events.parquet").is_file()
    and (LAKEHOUSE_DIR / "quarantine_events.parquet").is_file()
    and (LAKEHOUSE_DIR / "curated_gold").is_dir()
)
HAS_RAW_DATA = (ROOT / "raw_data" / "segment_status.csv").is_file()


def _load_metrics() -> dict:
    return json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))


def test_p1_ec1_ec2_ec4_quality_funnel_and_imputation() -> None:
    """[Q02, Q03, Q04, Q05] Verify ordered quality funnel, 3-tier TRAIN-only imputation, and ICT alignment."""
    assert LEVEL_VELOCITY_PRIORS == {1: 80.0, 2: 60.0, 3: 50.0, 4: 40.0}

    metrics = _load_metrics()
    qf = metrics["quality_funnel_c5"]
    assert qf["clean_motorized_segments"] == 84206
    assert qf["zero_length_segments"] == 154
    assert qf["non_road_osm_segments"] == 427
    assert qf["street_entities_merged_by_nfc"] == 54
    assert qf["status_input_pings"] == 90938
    assert qf["status_accepted_pings"] == 90787
    assert qf["status_rejected_pings"] == 151
    assert qf["status_accounting_balanced"] is True
    assert (
        qf["status_input_pings"]
        == qf["status_dedup_dropped_pings"] + qf["status_accepted_pings"] + qf["status_rejected_pings"]
    )
    assert sum(qf["vmax_imputation_tiers_windows"].values()) == 33394
    assert qf["vmax_imputation_tiers_windows"]["TIER_1_SEGMENT_OBSERVED"] == 4946
    assert qf["vmax_imputation_tiers_windows"]["TIER_1_SEGMENT_TRAIN_P85"] == 7650
    assert qf["vmax_imputation_tiers_segments"]["TIER_1_SEGMENT_TRAIN_P85"] == 472

    q_df = pd.read_csv(OUTPUTS_DIR / "quarantine_summary.csv")
    assert len(q_df) == 6
    r4_row = q_df[q_df["rule_id"] == "R4_UNICODE_NFD_AND_PREFIX_DRIFT"].iloc[0]
    assert int(r4_row["train_windows_affected"]) == 2247


@pytest.mark.skipif(
    not (HAS_FULL_DATASET and HAS_FULL_LAKEHOUSE),
    reason="Full dataset.csv and data/lakehouse/ are gitignored in clean clones",
)
def test_p1_full_dataset_and_quarantine_lakehouse() -> None:
    """Verify full Wave 2 dataset.csv (33,394 x 60) and quarantine_events.parquet when generated."""
    metrics = _load_metrics()
    qf = metrics["quality_funnel_c5"]
    assert (LAKEHOUSE_DIR / "quarantine_events.parquet").is_file()

    df = pd.read_csv(DATA_DIR / "dataset.csv")
    assert len(df) == 33394
    assert len(df.columns) == 60
    assert {
        "slot_ts",
        "target_ts",
        "split_set",
        "target_t30m_los_ordinal",
        "target_t30m_severe",
    }.issubset(df.columns)
    assert int(df["max_velocity"].isna().sum()) == 0
    assert bool((df["length_m"] > 0).all())
    assert bool(((df["vn_hour"] >= 0.0) & (df["vn_hour"] < 24.0)).all())
    trig_norm = np.square(df["period_sin"]) + np.square(df["period_cos"])
    assert np.allclose(trig_norm, 1.0, atol=1e-6)

    tier_counts = df["vmax_impute_tier"].value_counts().to_dict()
    assert tier_counts["TIER_1_SEGMENT_OBSERVED"] == 4946
    assert tier_counts["TIER_1_SEGMENT_TRAIN_P85"] == 7650
    assert sum(tier_counts.values()) == 33394
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
    assert int(ict_df.loc[0, "is_weekend"]) == 0
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
    """[Q06, Q07, Q08, Q09, Q10] Verify Monoid baseline, unequal partition associativity, salting & Spark."""
    monoid_verify = json.loads((OUTPUTS_DIR / "monoid_baseline_verification.json").read_text(encoding="utf-8"))
    assert monoid_verify["raw_distinct_utc_windows"] == 33394
    assert monoid_verify["medallion_matched_windows"] == 33394
    assert monoid_verify["missing_keys_in_dataset"] == 0
    assert monoid_verify["extra_keys_in_dataset"] == 0
    assert monoid_verify["utc_window_join_match_pct"] == 100.0
    assert monoid_verify["obs_count_discrepancies"] == 0
    assert monoid_verify["min_velocity_discrepancies"] == 0
    assert monoid_verify["max_velocity_discrepancies"] == 0
    assert monoid_verify["severe_ping_discrepancies"] == 0
    assert monoid_verify["mean_velocity_discrepancies"] == 0
    assert monoid_verify["total_severe_pings_accumulated"] == 42124
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
    assert salting["raw_input_pings"] == 90787
    assert salting["stage1_partial_salted_groups"] == 59655
    assert salting["stage2_reduced_windows"] == 33394
    assert salting["salt_bucket_cv"] < 0.02
    assert 1.0 <= salting["salted_partition_skew_ratio"] <= 1.05
    assert join_plan["pillar_i2_catalyst_join_optimization"]["partition_count"] == 10
    spark_rt_file = json.loads((OUTPUTS_DIR / "spark_runtime_evidence.json").read_text(encoding="utf-8"))
    spark_rt = join_plan.get("spark_runtime_verification", spark_rt_file)
    assert spark_rt_file["status"] == "verified"
    assert spark_rt["status"] == "verified"
    assert spark_rt["spark_kmeans_train_only_verified"] is True
    assert spark_rt["spark_kmeans_standard_scaler_verified"] is True
    assert spark_rt["spark_kmeans_train_segments"] == 3609
    assert spark_rt["spark_fpgrowth_rules_count"] >= 10
    assert spark_rt["spark_rf_train_rows"] == 1885
    assert spark_rt["spark_rf_numeric_features_count"] == 23
    assert spark_rt["spark_sink1_output_path"] == "outputs/spark_bronze_stream_sink"


@pytest.mark.skipif(
    not HAS_FULL_LAKEHOUSE,
    reason="Full Medallion data/lakehouse/ Parquets are gitignored in clean clones",
)
def test_p1_full_medallion_lakehouse_parquets() -> None:
    """Verify Bronze, Silver, and Partitioned Gold Lakehouse Parquets read cleanly without year_month conflict."""
    assert (LAKEHOUSE_DIR / "bronze_raw_events" / "events.parquet").is_file()
    assert (LAKEHOUSE_DIR / "silver_cleaned_windows" / "windows.parquet").is_file()
    gold_parts = list((LAKEHOUSE_DIR / "curated_gold").glob("year_month=*"))
    assert len(gold_parts) == 10
    gold_df = pd.read_parquet(LAKEHOUSE_DIR / "curated_gold")
    assert len(gold_df) == 33394
    assert list(gold_df.columns).count("year_month") == 1


def test_p2_uc3_ec7_orphan_pruning_and_line_graph_pagerank() -> None:
    """[D05] Verify orphan node pruning, real road line-graph PageRank, and toy graph mass conservation."""
    metrics = _load_metrics()
    ds = metrics["dataset"]
    assert ds["nodes_count"] == 577967
    assert ds["active_intersection_nodes_count"] == 52181
    assert ds["pruned_orphan_nodes_count"] == 525786
    assert ds["raw_segments_count"] == 84633
    assert ds["segments_count"] == 84206
    assert ds["line_graph_transition_edges"] == 92453
    assert ds["dangling_nodes_count"] == 314

    pr_df = pd.read_csv(OUTPUTS_DIR / "road_pagerank_top.csv")
    assert len(pr_df) == 50
    assert int(pr_df.iloc[0]["segment_id"]) == 21969
    assert pr_df.iloc[0]["street_name"] == "Nguyễn Kiệm"
    assert abs(float(pr_df.iloc[0]["pagerank_score"]) - 2.7799) < 1e-3

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
    assert _normalize_street_name("Đường Nguyễn Văn Linh, Phường Tân Phong, Quận 7") == "Nguyễn Văn Linh"
    assert _normalize_street_name(None) == ""
    assert not _is_valid_street_entity("Phường Hiệp Bình Chánh")
    assert not _is_valid_street_entity("Quận Gò Vấp")
    assert _is_valid_street_entity("Lý Chính Thắng")

    rules_df = pd.read_csv(OUTPUTS_DIR / "co_congestion_rules.csv")
    assert len(rules_df) == 12
    assert bool((rules_df["antecedent_street"].str.lower() != rules_df["consequent_street"].str.lower()).all())
    # D06: Verify both directions (A => B and B => A) are generated
    pairs_seen = set(zip(rules_df["antecedent_street"], rules_df["consequent_street"]))
    assert all((b, a) in pairs_seen for (a, b) in pairs_seen)
    assert ("Nguyễn Thị Minh Khai", "Cống Quỳnh") in pairs_seen
    assert ("Cống Quỳnh", "Nguyễn Thị Minh Khai") in pairs_seen
    assert ("Tô Hiến Thành", "Lý Thường Kiệt") in pairs_seen
    assert ("Lý Thường Kiệt", "Tô Hiến Thành") in pairs_seen

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
    """[D04, D07] Verify 7D K-Means clustering KPIs and Cold-Start peer lookup (demo_sample-safe)."""
    metrics = _load_metrics()
    model_m = metrics["model"]
    assert model_m["clusters_populated"] == 6
    assert model_m["segments_clustered"] == 3609
    assert float(model_m["silhouette_score"]) > 0.20

    store_m = metrics["static_feature_store_c13b"]
    assert store_m["total_segments_indexed"] == 84206
    assert store_m["observed_active_segments"] == 9995
    assert store_m["cold_start_blind_spot_segments"] == 74211
    assert store_m["vmax_imputation_tiers"]["TIER_1_SEGMENT_TRAIN_P85"] == 472

    sample_static = pd.read_parquet(DATA_DIR / "demo_sample" / "sample_static_features.parquet")
    sample_win = pd.read_csv(DATA_DIR / "demo_sample" / "sample_windows_23d.csv")
    cold_seg_id = int(sample_static[sample_static["is_cold_start"]].iloc[0]["segment_id"])
    cold_cluster = int(sample_static[sample_static["segment_id"] == cold_seg_id].iloc[0]["cluster_id"])
    peers = find_peer_segments(sample_static, cold_seg_id, limit=5)
    assert len(peers) == 5
    assert bool((peers["cluster_id"] == cold_cluster).all())
    assert cold_seg_id not in set(peers["segment_id"])
    ui_peers = _find_similar_segments(sample_win, cold_seg_id, limit=5, static_store_df=sample_static)
    assert len(ui_peers) == 5


@pytest.mark.skipif(
    not (HAS_FULL_DATASET and HAS_FULL_LAKEHOUSE),
    reason="Full segment_static_features.parquet and dataset.csv are gitignored in clean clones",
)
def test_p2_full_cold_start_feature_store_lakehouse() -> None:
    """Verify full 84,206-segment Static Feature Store and Cold-Start peer lookup on full dataset."""
    static_df = pd.read_parquet(LAKEHOUSE_DIR / "segment_static_features.parquet")
    assert len(static_df) == 84206
    assert int((~static_df["is_cold_start"]).sum()) == 9995
    assert int(static_df["is_cold_start"].sum()) == 74211
    assert int(static_df["max_velocity"].isna().sum()) == 0
    assert abs(float(static_df["pagerank_score"].sum()) - 84206.0) < 1e-3

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
    assert ev["train_samples"] == 1885
    assert ev["val_samples"] == 1167
    assert ev["test_samples"] == 1182
    assert ev["roc_auc"] > 0.90
    assert ev["average_precision"] > 0.90
    assert ev["accuracy_6class"] > 0.90
    assert ev["severe_f1"] > 0.90
    assert ev["macro_f1_6class"] > 0.40
    cm = ev["confusion_matrix_6class"]
    assert len(cm) == 6
    assert all(cm[i][i] > 0 for i in range(6))
    assert "validation_gate_selection" in ev
    assert len(ev["validation_gate_selection"]["candidates"]) == 6
    assert (
        ev["validation_gate_selection"]["selected_multiclass_model"]
        == "Random Forest (+Graph PageRank, Causal Priors & K-Means)"
    )

    # D10: GBT binary rows must have task_type == BINARY_SEVERE_EF_ONLY and null 6-class metrics; RF > DT
    ablation_df = pd.read_csv(OUTPUTS_DIR / "ablation_results.csv")
    gbt_rows = ablation_df[ablation_df["model"].str.startswith("GBT")]
    assert len(gbt_rows) == 2
    assert bool((gbt_rows["task_type"] == "BINARY_SEVERE_EF_ONLY").all())
    assert bool(gbt_rows["accuracy_6class"].isna().all())
    rf_23_row = ablation_df[ablation_df["model"].str.startswith("Random Forest (+Graph")].iloc[0]
    dt_23_row = ablation_df[ablation_df["model"].str.startswith("Decision Tree (+Graph")].iloc[0]
    assert float(rf_23_row["severe_roc_auc"]) > float(dt_23_row["severe_roc_auc"])
    assert float(rf_23_row["severe_pr_auc"]) > float(dt_23_row["severe_pr_auc"])
    assert float(rf_23_row["accuracy_6class"]) > float(dt_23_row["accuracy_6class"])

    # D11: Frozen SHA-256 registry integrity (auto-falls back to sample_static_features.parquet in clean clones)
    manifest = json.loads((OUTPUTS_DIR / "model_registry_manifest.json").read_text(encoding="utf-8"))
    model_sha = hashlib.sha256((OUTPUTS_DIR / "model.joblib").read_bytes()).hexdigest()
    vmax_sha = hashlib.sha256((OUTPUTS_DIR / "vmax_lookup.json").read_bytes()).hexdigest()
    assert manifest["phase_a_fit_only"] is True
    assert manifest["phase_b_c_read_only_verified"] is True
    assert manifest["all_three_artifacts_immutable_verified"] is True
    assert manifest["sha256_digest"] == model_sha
    assert manifest["vmax_lookup_sha256"] == vmax_sha
    reg_audit = verify_frozen_registry_integrity(OUTPUTS_DIR / "model_registry_manifest.json")
    assert reg_audit["verified"] is True


@pytest.mark.skipif(
    not HAS_FULL_LAKEHOUSE,
    reason="Full segment_static_features.parquet and silver windows are gitignored in clean clones",
)
def test_p3_full_lakehouse_registry_and_golden_parity() -> None:
    """[D11, D12] Verify full Static Store SHA-256 and scorer parity against golden_predictions.parquet."""
    manifest = json.loads((OUTPUTS_DIR / "model_registry_manifest.json").read_text(encoding="utf-8"))
    static_sha = hashlib.sha256((LAKEHOUSE_DIR / "segment_static_features.parquet").read_bytes()).hexdigest()
    assert manifest["static_feature_store_sha256"] == static_sha

    golden = pd.read_parquet(OUTPUTS_DIR / "golden_predictions.parquet")
    silver = pd.read_parquet(LAKEHOUSE_DIR / "silver_cleaned_windows" / "windows.parquet")
    assert len(golden) == 200
    assert golden["event_id"].isin(silver["event_id"]).all()
    matched_silver = silver.set_index("event_id").loc[golden["event_id"]].reset_index()
    artifact = joblib.load(OUTPUTS_DIR / "model.joblib")
    rescored = score_feature_snapshot(matched_silver, artifact)
    assert np.allclose(golden["severe_prob"].to_numpy(), rescored["severe_prob"].to_numpy(), atol=1e-9)
    assert (golden["predicted_next_los"].to_numpy() == rescored["predicted_next_los"].to_numpy()).all()


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
    assert sm["total_stream_events_processed"] == 33727
    assert sm["valid_unique_events"] == 33394
    assert sm["injected_duplicate_events"] == 333
    assert sm["duplicates_removed_exact"] == 333
    assert sm["injected_late_in_watermark"] == 1168
    assert sm["injected_late_dropped_by_watermark"] == 500
    assert sm["bloom_filter"]["bit_packed_uint8_bytes"] == 32768
    assert sm["bloom_filter"]["memory_bytes"] == 32768
    assert sm["bloom_filter"]["memory_kb"] == 32.0
    assert sm["bloom_filter"]["duplicates_flagged"] == 333
    assert sm["bloom_filter"]["empirical_fp_rate"] < 0.005
    assert sm["hyperloglog"]["memory_kb"] == 1.0
    assert sm["hyperloglog"]["relative_error_pct"] < 5.0

    recon = sm["reconciliation"]
    assert recon["in_watermark_like_for_like_match_pct"] == 100.0
    assert recon["tier1_exceptAll_diff_count"] == 0
    assert recon["tier1_verified"] is True
    assert recon["healed_downstream_lag_windows"] >= 500
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

    # M12 & M14: WAL Checkpoint kill & restart recovery + Tier 2 lag healing rescore + in-flight STREAM_PROVISIONAL
    progress = json.loads((OUTPUTS_DIR / "query_progress.json").read_text(encoding="utf-8"))
    assert progress["restart_recovery_verified"] is True
    assert progress["duplicate_side_effects_on_restart"] == 0
    assert progress["version_guard_audit"]["stale_provisional_retries_rejected"] == 25
    assert progress["healed_downstream_rescored_windows"] == 384
    assert progress["rescore_prob_changed_windows_count"] == 213
    assert progress["Phase_B_in_flight_provisional_retained"] == 489
    assert progress["frozen_registry_integrity"]["verified"] is True


@pytest.mark.skipif(
    not HAS_FULL_LAKEHOUSE,
    reason="Full serving_traffic_scores.parquet is gitignored in clean clones",
)
def test_p4_full_serving_lakehouse_lifecycle_counts() -> None:
    """Verify full serving_traffic_scores.parquet lifecycle counts across 33,394 windows."""
    serving_df = pd.read_parquet(LAKEHOUSE_DIR / "serving_traffic_scores.parquet")
    status_counts = serving_df["lifecycle_status"].value_counts().to_dict()
    assert status_counts.get("STREAM_PROVISIONAL") == 489
    assert status_counts.get("BATCH_RECONCILED") == 500
    assert status_counts.get("BATCH_VERIFIED") == 32405


def test_p4_uc2_ec10_mongodb_ixscan_and_honest_fallback(tmp_path: Path) -> None:
    """[M07, M08, M09, T04] Verify Mongo IXSCAN report AND honest 'not_verified' when Mongo is disabled."""
    mongo_verify = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    assert mongo_verify["write_partitions"] == 4
    assert mongo_verify["batch_size"] == 500
    if mongo_verify.get("status") == "verified":
        assert mongo_verify["total_rows_upserted"] == 33394
        ix = mongo_verify["ixscan_explain_verification"]
        assert ix["index_used"] == "level_prob_segment_idx"
        assert "IXSCAN" in ix["winning_stages"]
        assert ix["in_memory_sort"] is False

    # M08 / T04 Failure Injection: When Mongo container is disabled/unreachable, status MUST be 'not_verified'
    serving_path = LAKEHOUSE_DIR / "serving_traffic_scores.parquet"
    if serving_path.is_file():
        serving_sample = pd.read_parquet(serving_path).head(20)
    else:
        serving_sample = pd.DataFrame(
            [
                {"segment_id": 1, "street_level": 1, "severe_prob": 0.90, "write_partition": 0},
                {"segment_id": 2, "street_level": 2, "severe_prob": 0.80, "write_partition": 1},
                {"segment_id": 3, "street_level": 3, "severe_prob": 0.25, "write_partition": 2},
                {"segment_id": 4, "street_level": 4, "severe_prob": 0.10, "write_partition": 3},
            ]
        )
    unverified_path = tmp_path / "mongo_unverified.json"
    unverified = _verify_mongodb_ixscan(serving_sample, unverified_path, "dummy_sha", mongo_container="")
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

    ds_path = DATA_DIR / "dataset.csv"
    win_path = ds_path if ds_path.is_file() else DATA_DIR / "demo_sample" / "sample_windows_23d.csv"
    df = pd.read_csv(win_path)
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

    static_path = LAKEHOUSE_DIR / "segment_static_features.parquet"
    eff_static = static_path if static_path.is_file() else DATA_DIR / "demo_sample" / "sample_static_features.parquet"
    static_df = pd.read_parquet(eff_static)
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
    assert stream_rep["frozen_registry_integrity"]["verified"] is True


def test_t01_end_to_end_mini_fixture_without_train_csv(tmp_path: Path) -> None:
    """[Q01, T01] Run end-to-end window building, PageRank, lags & stream recovery without train.csv."""
    raw_dir = tmp_path / "raw_mini"
    raw_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"_id": [1, 2, 3], "long": [106.6, 106.7, 106.8], "lat": [10.7, 10.8, 10.9]}).to_csv(
        raw_dir / "nodes.csv", index=False
    )
    pd.DataFrame(
        {
            "_id": [10, 20],
            "name": ["Đường Lê Lợi", "Nguyễn Huệ"],
            "level": [1, 2],
            "type": ["primary", "secondary"],
        }
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
    wal_res = run_stream_with_restart_recovery(win_replay, tmp_path / "ckpt", num_microbatches=2, kill_after_batch=0)
    assert wal_res["restart_recovery_verified"] is True


def test_product_lifecycle_governance_tree() -> None:
    assert SPARK_VERIFY_PATH.name == "spark_runtime_evidence.json"
    assert SPARK_VERIFY_PATH.is_file()
    assert callable(_fetch_live_mongo_status)
    expected_files = [
        PRODUCT_DIR / "README.md",
        PRODUCT_DIR / "00-proposal" / "project-proposal.md",
        PRODUCT_DIR / "00-proposal" / "product-vision.md",
        PRODUCT_DIR / "01-discovery" / "competitor-analysis.md",
        PRODUCT_DIR / "01-discovery" / "validation-report.md",
        PRODUCT_DIR / "02-strategy" / "business-model.md",
        PRODUCT_DIR / "02-strategy" / "product-strategy.md",
        PRODUCT_DIR / "03-requirements" / "prd.md",
        PRODUCT_DIR / "04-design" / "wireframes" / "dashboard-5tabs-wireframe.md",
        PRODUCT_DIR / "05-architecture" / "system-design.md",
        PRODUCT_DIR / "05-architecture" / "architecture-decisions" / "adr-001-zero-conflict-mlops.md",
        PRODUCT_DIR / "05-architecture" / "architecture-decisions" / "adr-002-two-tier-reconciliation.md",
        PRODUCT_DIR / "05-architecture" / "architecture-decisions" / "adr-003-causal-priors-7d-kmeans.md",
        PRODUCT_DIR / "06-engineering" / "development-guidelines.md",
        PRODUCT_DIR / "06-engineering" / "release-plan.md",
        PRODUCT_DIR / "06-engineering" / "sprint-plans" / "sprint-plan-v3.md",
        PRODUCT_DIR / "07-quality" / "release-checklist.md",
        PRODUCT_DIR / "07-quality" / "test-strategy.md",
        PRODUCT_DIR / "07-quality" / "test-cases" / "test-matrix-p1-p4-uc1-uc6-ec1-ec10.md",
        PRODUCT_DIR / "08-launch" / "operations-runbook.md",
        PRODUCT_DIR / "08-launch" / "pilot-report.md",
        PRODUCT_DIR / "09-growth" / "experiment-log.md",
        PRODUCT_DIR / "09-growth" / "growth-roadmap.md",
        PRODUCT_DIR / "09-growth" / "product-analytics.md",
    ]
    for path in expected_files:
        assert path.is_file(), f"Missing product governance document: {path}"
        doc_txt = path.read_text(encoding="utf-8")
        for bad_term in ("Orphan", "Transition Regime"):
            assert bad_term not in doc_txt, f"Found misleading jargon {bad_term!r} in {path.name}"


def _extract_pptx_slides_text(pptx_path: Path) -> tuple[int, int, str]:
    import xml.etree.ElementTree as ET
    import zipfile

    with zipfile.ZipFile(pptx_path) as zf:
        slide_names = sorted(
            [n for n in zf.namelist() if n.startswith("ppt/slides/slide") and n.endswith(".xml")],
            key=lambda s: int(s.removeprefix("ppt/slides/slide").removesuffix(".xml")),
        )
        texts = [
            el.text.strip()
            for name in slide_names
            for el in ET.fromstring(zf.read(name)).iter()
            if el.tag.endswith("}t") and el.text and el.text.strip()
        ]
    return len(slide_names), len(texts), "\n".join(texts)


def _extract_pdf_pages_and_text(pdf_path: Path) -> tuple[int, str | None]:
    import importlib.util
    import re

    raw = pdf_path.read_bytes()
    assert raw.startswith(b"%PDF-"), f"Invalid PDF header: {pdf_path}"
    if importlib.util.find_spec("pypdf") is not None:
        from pypdf import PdfReader

        reader = PdfReader(str(pdf_path))
        full_txt = "\n".join(page.extract_text() or "" for page in reader.pages)
        return len(reader.pages), full_txt
    pages = len(re.findall(rb"/Type\s*/Page\b", raw))
    return pages, None


def _assert_deliverable_tokens(name: str, text: str, required: tuple[str, ...], forbidden: tuple[str, ...]) -> None:
    import re

    for req in required:
        assert req in text, f"Missing required token {req!r} in {name}"
    for bad in forbidden:
        if re.fullmatch(r"[GDQMT]\d{2}", bad):
            assert not re.search(rf"\b{bad}\b", text), f"Found forbidden audit code {bad!r} in {name}"
        else:
            assert bad not in text, f"Found forbidden or stale token {bad!r} in {name}"


FORBIDDEN_STALE_AND_INTERNAL_TOKENS: tuple[str, ...] = (
    "stage_b1_ingestion",
    "2021-03-21",
    "0.6670",
    "0.7568",
    "0.8458",
    "7,549",
    "0.3158",
    "0.465",
    "0.584",
    "0.638",
    "0.767 ms",
    "60/50/40/30",
    "0.9247",
    "0.9634",
    "e4474d61",
    "2ccdf94d",
    "59f813bf",
    "3,864",
    "los_Index",
    "Hồng Bàng (t)",
    "Đinh Bộ Lĩnh =>",
    "34399",
    "Proposal V3",
    "Architecture V3",
    "Platform V3",
    "(V3)",
    "— V3",
    "V3.1",
    "Sơ đồ Kiến trúc V4",
    "system_architecture_v4",
    "/workflow",
    "Stage-Gate",
    "Gate 1",
    "Gate 5",
    "8 Workstreams",
    "Audit Sign-Off",
    "28/28",
    "50/50",
    "Pillar I1",
    "Pillar I2",
    "Pillar I3",
    "Pillar S7",
    "Pillars I1",
    "(I2)",
    "Khắc phục 6 rủi ro",
    "Orphan Nodes",
    "Orphan Node",
    "Orphan",
    "đỉnh mồ côi",
    "Đỉnh mồ côi",
    "mồ côi",
    "Mồ côi",
    "Specialized ML",
    "Steady-State Inertia Illusion",
    "Transition Regime",
    "Discontinuous Sensor Gap",
    "Next-Observed Horizon",
    "VAL-Calibrated Acc",
    "directed segments",
    "muối hóa",
    "Muối hóa",
    "Muối Hóa",
    "quảng bá bảng chiều",
    "Quảng bá bảng chiều",
    "Quảng Bá Bảng Chiều",
    "ghi lũy đẳng",
    "Ghi lũy đẳng",
    "hiện vật",
    "Hiện vật",
    "Hiện Vật",
    "ma trận nhầm lẫn",
    "Ma trận nhầm lẫn",
    "Ma trận Nhầm lẫn",
    "tiên nghiệm nhân quả",
    "Tiên Nghiệm Nhân Quả",
    *(f"G0{i}" for i in range(1, 6)),
    *(f"Q{i:02d}" for i in range(1, 12)),
    *(f"D{i:02d}" for i in range(1, 13)),
    *(f"M{i:02d}" for i in range(1, 15)),
    *(f"T0{i}" for i in range(1, 9)),
)


def _assert_human_storytelling_and_honesty(name: str, text: str) -> None:
    """Verify Chapter 1 opens with concrete real-world GPS storytelling (no [C1]/SHA-256) and Ch.5 is honest."""
    assert "1.1." in text and "1.2." in text and "Chương 2." in text, f"Missing Ch.1 boundaries in {name}"
    ch1_intro = text.split("Chương 2.", 1)[0]
    ch1_opening = text.split("1.1.", 1)[1].split("1.2.", 1)[0]
    for required_story_token in ("Lý Thường Kiệt", "47407", "4 km/h", "Bước 1", "Bước 2", "Bước 3", "Bước 4"):
        assert (
            required_story_token in ch1_intro
        ), f"Missing concrete Chapter 1 story token {required_story_token!r} in {name}"
    for premature_jargon in ("[C1]", "[C17]", "SHA-256", "Monoid"):
        assert (
            premature_jargon not in ch1_opening
        ), f"Found premature technical jargon {premature_jargon!r} in Section 1.1 opening of {name}"
    honest_groups = (
        ("10 / 61", "10/61"),
        ("14 / 61", "14/61"),
        ("0.001953", "0.0020"),
        ("8 / 993", "8/993"),
        ("31.36x",),
    )
    for grp in honest_groups:
        assert any(tok in text for tok in grp), f"Missing honest Chapter 5 evaluation disclosure {grp!r} in {name}"


def _assert_identical_file_sha256(copy_path: Path, primary_path: Path) -> None:
    """Compare large deliverable files by SHA-256 so pytest never hangs rendering binary diffs."""
    assert copy_path.is_file(), f"Missing sync target: {copy_path}"
    copy_sha = hashlib.sha256(copy_path.read_bytes()).hexdigest()
    primary_sha = hashlib.sha256(primary_path.read_bytes()).hexdigest()
    assert copy_sha == primary_sha, f"Out-of-sync: {copy_path} ({copy_sha[:12]} != {primary_sha[:12]})"


def _assert_synced_deliverable_copies(proposal_pdf: Path, slides_pptx: Path, slides_pdf: Path) -> None:
    ws = ROOT.parent
    if not (ws / "HCMC_Smart_Traffic_Final_Report.pdf").is_file():
        return
    for copy_path, primary_path in (
        (ws / "HCMC_Smart_Traffic_Final_Report.pdf", proposal_pdf),
        (ws / "HCMC_Smart_Traffic_Final_Presentation.pptx", slides_pptx),
        (ws / "HCMC_Smart_Traffic_Final_Slides.pdf", slides_pdf),
    ):
        _assert_identical_file_sha256(copy_path, primary_path)


def _check_slide_title_shape_fit(s_idx: int, shp: object, segoe_bold: Path) -> None:
    from PIL import ImageFont

    if not getattr(shp, "has_text_frame", False) or len(shp.text_frame.paragraphs) != 1:
        return
    h_in = shp.height / 914400.0
    p = shp.text_frame.paragraphs[0]
    r0 = p.runs[0] if p.runs else None
    if not (0.26 <= h_in <= 0.36 and r0 and r0.font.bold and p.text.strip()):
        return
    sz = r0.font.size.pt if r0.font.size else 10.2
    fnt = ImageFont.truetype(str(segoe_bold), max(8, int(round(sz * 96.0 / 72.0))))
    bb = fnt.getbbox(p.text)
    tw_in = (bb[2] - bb[0]) / 96.0
    tf = shp.text_frame
    box_w_in = (shp.width - (tf.margin_left or 0) - (tf.margin_right or 0)) / 914400.0
    assert tw_in <= box_w_in * 1.01, f"Slide {s_idx} title {p.text!r} ({tw_in:.2f}in) > box ({box_w_in:.2f}in)"


def _assert_pptx_single_line_titles_fit(pptx_path: Path) -> None:
    from pptx import Presentation

    segoe_bold = Path(r"C:\Windows\Fonts\segoeuib.ttf")
    if not segoe_bold.is_file():
        return
    prs = Presentation(str(pptx_path))
    assert prs.core_properties.title and "BDA501" in prs.core_properties.title
    assert "25MS23280" in prs.core_properties.author
    for s_idx, slide in enumerate(prs.slides, 1):
        for shp in slide.shapes:
            _check_slide_title_shape_fit(s_idx, shp, segoe_bold)


def _assert_pptx_card_shapes_and_nav_dots(pptx_path: Path) -> None:
    from pptx import Presentation
    from pptx.enum.shapes import MSO_SHAPE, MSO_SHAPE_TYPE

    prs = Presentation(str(pptx_path))
    oval_dots = 0
    for s_idx, slide in enumerate(prs.slides, 1):
        for shp in slide.shapes:
            if shp.shape_type != MSO_SHAPE_TYPE.AUTO_SHAPE:
                continue
            if shp.auto_shape_type == MSO_SHAPE.OVAL:
                oval_dots += 1
            elif shp.auto_shape_type == MSO_SHAPE.SNIP_1_RECTANGLE:
                min_dim_in = min(shp.width, shp.height) / 914400.0
                snip_in = shp.adjustments[0] * min_dim_in
                assert snip_in <= 0.25, f"Slide {s_idx} card corner snip too wide ({snip_in:.2f}in > 0.25in)"
    assert oval_dots == 16, f"Expected 16 active navbar indicator dots across Slides 02..17, got {oval_dots}"


def _slide_combined_text(sl: object) -> str:
    text_parts = [sh.text_frame.text for sh in sl.shapes if sh.has_text_frame]
    for sh in sl.shapes:
        if sh.has_table:
            text_parts.extend(c.text for row in sh.table.rows for c in row.cells)
    return " ".join(text_parts)


def _assert_no_member_tokens_on_slides(slides: object, start_idx: int, end_idx: int, label: str = "Slide") -> None:
    for idx in range(start_idx, end_idx):
        sl_txt = _slide_combined_text(slides[idx])
        for member_tok in ("Minh Quyền", "Thành Đạt", "Công Minh", "25MS23"):
            assert member_tok not in sl_txt, f"{label} {idx + 1:02d} leaked member token '{member_tok}'"


def _assert_pptx_concise_slide_word_budget(pptx_path: Path) -> None:
    from pptx import Presentation

    prs = Presentation(str(pptx_path))
    per_slide = [len(_slide_combined_text(sl).split()) for sl in prs.slides]
    avg_words = sum(per_slide) / max(len(per_slide), 1)
    assert avg_words <= 350.0, f"Slide deck average word count too dense ({avg_words:.1f} > 350)"
    assert max(per_slide) <= 480, f"Single slide word count too dense ({max(per_slide)} > 480)"
    _assert_no_member_tokens_on_slides(prs.slides, 1, 15, label="Slide")


def test_proposal_pdf_and_pptx_deliverables_synchronized() -> None:
    """Verify Final Report PDF, 17-slide PPTX/PDF, Wave B docs, and toc_backend/service_helpers.py consistency."""
    proposal_pdf = ROOT / "HCMC_Smart_Traffic_Final_Report.pdf"
    slides_pdf = ROOT / "HCMC_Smart_Traffic_Final_Slides.pdf"
    slides_pptx = ROOT / "HCMC_Smart_Traffic_Final_Presentation.pptx"
    arch_png = OUTPUTS_DIR / "system_architecture.png"
    grid_png = OUTPUTS_DIR / "slides_overview_grid.png"
    chart_dir = OUTPUTS_DIR / "slide_charts"
    assert proposal_pdf.is_file() and proposal_pdf.stat().st_size > 500_000
    assert slides_pdf.is_file() and slides_pdf.stat().st_size > 500_000
    assert slides_pptx.is_file() and slides_pptx.stat().st_size > 400_000
    assert arch_png.is_file() and arch_png.stat().st_size > 200_000
    assert grid_png.is_file() and grid_png.stat().st_size > 500_000
    for visual_asset in (
        "arch_detailed_reference.png",
        "map_hcmc_problem_context.png",
        "map_hcmc_hotspots_coldstart.png",
        "map_hcmc_clusters_7d.png",
        "visual_methods_imputation_monoid.png",
        "visual_methods_graph_coldstart_sketches.png",
        "flow_batch_components.png",
        "flow_stream_components.png",
        "chart_transition_analysis.png",
        "chart_ablation_t30m.png",
        "chart_eda_anomalies.png",
        "chart_spark_skew_catalyst.png",
        "chart_streaming_serving.png",
        "ui_tab1_overview_hotspots.png",
        "ui_tab2_live_forecast_mongo.png",
        "ui_tab4_kmeans_coldstart.png",
        "eq_vmax_los.png",
        "eq_monoid.png",
        "eq_pagerank.png",
        "eq_prior.png",
        "eq_sketches.png",
    ):
        assert (chart_dir / visual_asset).is_file(), f"Missing visual asset: {visual_asset}"
    _assert_synced_deliverable_copies(proposal_pdf, slides_pptx, slides_pdf)
    _assert_pptx_single_line_titles_fit(slides_pptx)
    _assert_pptx_card_shapes_and_nav_dots(slides_pptx)
    _assert_pptx_concise_slide_word_budget(slides_pptx)

    prop_pages, prop_text = _extract_pdf_pages_and_text(proposal_pdf)
    slides_pages, slides_text = _extract_pdf_pages_and_text(slides_pdf)
    pptx_slides, pptx_text_nodes, pptx_text = _extract_pptx_slides_text(slides_pptx)
    assert prop_pages >= 15
    assert slides_pages == 17
    assert pptx_slides == 17
    assert pptx_text_nodes >= 280

    manifest = json.loads((ROOT / "outputs" / "model_registry_manifest.json").read_text(encoding="utf-8"))
    sha_prefixes = (
        manifest["sha256_digest"][:12],
        manifest["static_feature_store_sha256"][:12],
        manifest["vmax_lookup_sha256"][:12],
    )
    wave2_metrics = (
        "84,206",
        "33,394",
        "7,885",
        "1,885",
        "1,167",
        "1,182",
        "2020-12-12T08:00:00Z",
        "2020-12-25T03:30:00Z",
        "0.9255",
        "0.9894",
        "0.9543",
        "0.9362",
        "0.4253",
        "32.0 KiB",
        "500",
        "384",
        "609",
        "0.603 ms",
        "31.36x",
        "Proof-of-Scale",
        "25MS23280",
        "25MS23306",
        "25MS23310",
    ) + sha_prefixes
    slide_metrics = (
        "84,206",
        "33,394",
        "7,885",
        "1,885",
        "1,167",
        "1,182",
        "0.9255",
        "0.9894",
        "0.9543",
        "0.9362",
        "0.4253",
        "32.0 KiB",
        "500",
        "384",
        "609",
        "0.603 ms",
        "Proof-of-Scale",
        "31.36x",
        "25MS23280",
        "25MS23306",
        "25MS23310",
        "Đặng Trần Khánh",
    )
    chapters_and_sections = tuple(f"Chương {idx}." for idx in range(1, 8)) + (
        "1.4.",
        "2.4.",
        "3.4.",
        "5.4.",
        "7.2.",
        "7.3.",
        "7.4.",
        "Hình 1.1",
        "Hình 1.2",
        "Hình 2.1",
        "Hình 3.1",
        "Hình 3.2",
        "Hình 3.3",
        "Hình 4.1",
        "Bảng 5.4",
        "Bảng 5.5",
        "Hình 5.1",
        "Hình 5.2",
        "Hình 6.1",
        "Hình 6.2",
        "Hình 6.3",
        "Hình 6.4",
    )
    forbidden_report_slang = FORBIDDEN_STALE_AND_INTERNAL_TOKENS + (
        "Báo Cáo Đề Xuất",
        "Project Proposal",
        "win trễ",
        "377 win",
        "3,953 lag1",
        "p ≥ 0.03",
    )
    wave2_doc_forbidden = tuple(
        t
        for t in forbidden_report_slang
        if t not in {"0.7568", "mồ côi", "Ma trận nhầm lẫn", "ma trận nhầm lẫn", "hiện vật"}
    )
    forbidden_raw_pdf_math = ("sum_{s=1}", "m1 (+) m2", "+inf", "prior_s(t)")
    forbidden_slide_dumps = (
        FORBIDDEN_STALE_AND_INTERNAL_TOKENS
        + sha_prefixes
        + (
            "Báo Cáo Đề Xuất",
            "Project Proposal",
            "ruff check",
            "docker compose",
            "spark_engine.py",
            "compose.quyen.yml",
            "[C1]->",
            "IXSCAN, 0 SORT",
            "win trễ",
            "377 win",
            "v_max",
            "LIVE_MONGODB_8.0",
        )
    )

    artifact = joblib.load(OUTPUTS_DIR / "model.joblib")
    rf_imp = dict(zip(artifact["full_features"], artifact["rf_multiclass"].feature_importances_))
    live_imp_tokens = (
        f"{rf_imp['current_los_ordinal']:.4f}",
        f"{rf_imp['street_causal_prior_sev']:.4f}",
        f"{rf_imp['neighbor_los_ordinal']:.4f}",
        f"{rf_imp['lag2_los_ordinal']:.4f}",
    )

    proposal_md = (PRODUCT_DIR / "00-proposal" / "project-proposal.md").read_text(encoding="utf-8")
    tech_note_md = (ROOT / "TECHNICAL_NOTE.md").read_text(encoding="utf-8")
    readme_md = (ROOT / "README.md").read_text(encoding="utf-8")
    service_helpers_txt = (ROOT / "toc_backend" / "service_helpers.py").read_text(encoding="utf-8")
    wave2_md_sections = tuple(
        tok for tok in chapters_and_sections if tok not in {"Bảng 5.5", "Hình 6.2", "Hình 6.3", "Hình 6.4"}
    )
    _assert_deliverable_tokens(
        "project-proposal.md",
        proposal_md,
        wave2_metrics + wave2_md_sections + live_imp_tokens,
        wave2_doc_forbidden,
    )
    _assert_human_storytelling_and_honesty("project-proposal.md", proposal_md)
    _assert_deliverable_tokens(
        "TECHNICAL_NOTE.md",
        tech_note_md,
        wave2_metrics + tuple(f"Chương {idx}." for idx in range(1, 8)),
        wave2_doc_forbidden,
    )
    _assert_deliverable_tokens(
        "README.md",
        readme_md,
        (
            "25MS23280",
            "25MS23306",
            "25MS23310",
            "PRESENTATION_SCRIPT.md",
            "Proof-of-Scale",
            "84,206",
            "33,394",
            "0.9894",
            "0.9543",
            sha_prefixes[0],
        ),
        wave2_doc_forbidden + ("Báo cáo Đề xuất",),
    )
    _assert_deliverable_tokens(
        "toc_backend/service_helpers.py",
        service_helpers_txt,
        ("27017", "LIVE_MONGODB_8.0"),
        FORBIDDEN_STALE_AND_INTERNAL_TOKENS,
    )
    _assert_deliverable_tokens("Presentation PPTX", pptx_text, slide_metrics, forbidden_slide_dumps)
    if slides_text is not None:
        assert len(slides_text) > 8_000, "Slides PDF must have a searchable vector text layer"
        _assert_deliverable_tokens("Slides PDF", slides_text, slide_metrics, forbidden_slide_dumps)
    if prop_text is not None:
        assert "&&" not in prop_text, "Report PDF CLI Runbook must not use && (breaks Windows PowerShell 5.1)"
        _assert_deliverable_tokens(
            "Report PDF",
            prop_text,
            wave2_metrics + chapters_and_sections,
            wave2_doc_forbidden + forbidden_raw_pdf_math,
        )
        _assert_human_storytelling_and_honesty("Report PDF", prop_text)


def test_presentation_script_completeness_and_sync() -> None:
    """Verify PRESENTATION_SCRIPT.md covers all 17 slides, 3 speakers, live demo, Q&A, and workspace sync."""
    script_path = ROOT / "PRESENTATION_SCRIPT.md"
    ws_script_path = ROOT.parent / "PRESENTATION_SCRIPT.md"
    assert script_path.is_file() and script_path.stat().st_size > 15_000
    script_text = script_path.read_text(encoding="utf-8")
    if ws_script_path.parent.exists() and ws_script_path.is_file():
        assert ws_script_path.read_text(encoding="utf-8") == script_text

    slide_markers = tuple(f"#### Slide {idx:02d}:" for idx in range(1, 18))
    qa_markers = tuple(f"### Câu {idx}:" for idx in range(1, 11))
    speaker_and_demo_tokens = (
        "Nguyễn Tạ Minh Quyền",
        "25MS23280",
        "Nguyễn Thành Đạt",
        "25MS23306",
        "Phạm Công Minh",
        "25MS23310",
        "Đặng Trần Khánh",
        "Live Demo",
        "localhost:8000",
        "Tra cứu nhanh Báo cáo:",
        "Proof-of-Scale",
        "0.9894",
        "0.9543",
        "0.9255",
        "0.9362",
        "0.4253",
        "0.603 ms",
        "31.36x",
        "32.0 KiB",
    )
    _assert_deliverable_tokens(
        "PRESENTATION_SCRIPT.md",
        script_text,
        slide_markers + qa_markers + speaker_and_demo_tokens,
        FORBIDDEN_STALE_AND_INTERNAL_TOKENS + ("localhost:8501", "streamlit run app.py"),
    )


def test_transition_regime_evaluation_resolves_persistence_paradox() -> None:
    """Verify Transition-Case ML evaluation metrics, McNemar < 0.05, RF > DT, and VAL gate parity."""
    assert _safe_binary_ranking_metrics(np.array([], dtype=int), np.array([], dtype=float)) == (None, None, 0.0)
    roc_all_zero, pr_all_zero, brier_clip = _safe_binary_ranking_metrics(
        np.array([0, 0, 0], dtype=int),
        np.array([-0.05, 0.50, 1.0000000002], dtype=float),
    )
    assert roc_all_zero is None and pr_all_zero is None and 0.0 < brier_clip < 1.0

    trans_csv = OUTPUTS_DIR / "transition_evaluation.csv"
    ablation_df = pd.read_csv(OUTPUTS_DIR / "ablation_results.csv")
    assert trans_csv.is_file()
    trans_csv_txt = trans_csv.read_text(encoding="utf-8")
    assert "Nhóm đổi trạng thái" in trans_csv_txt and "Cặp tín hiệu GPS ngắt quãng" in trans_csv_txt
    assert "Transition Regime" not in trans_csv_txt and "Discontinuous Sensor Gap" not in trans_csv_txt
    assert "Tập Chuyển Pha" not in trans_csv_txt and "Hậu nghiệm" not in trans_csv_txt

    metrics = _load_metrics()
    ev = metrics["evaluation"]
    summary = ev["transition_analysis"]
    trans_df = pd.read_csv(trans_csv)
    assert len(trans_df) == 4 and len(trans_df.columns) == 10
    rf_mask = ablation_df["model"].str.startswith("Random Forest (+Graph")
    ablation_rf_pr = float(ablation_df.loc[rf_mask, "severe_pr_auc"].iloc[0])
    assert abs(float(trans_df.iloc[0]["rf_23d_severe_pr_auc"]) - ablation_rf_pr) < 1e-4
    assert summary["test_samples"] == 1182
    assert summary["steady_state_samples"] == 1121
    assert summary["transition_samples"] == 61
    assert 0.15 <= summary["val_selected_change_threshold"] <= 0.50
    assert "rf_23d_early_warning_recall_at_003" not in summary
    assert summary["persistence_transition_6class_acc"] == 0.0
    assert summary["rf_23d_transition_detect_roc_auc"] > 0.85
    assert summary["rf_23d_transition_detect_roc_auc"] > summary["dt_23d_transition_detect_roc_auc"]
    assert summary["rf_23d_val_calibrated_transition_6class_acc"] > 0.14
    assert summary["rf_23d_conditional_new_state_top1_acc"] > 0.25
    assert summary["rf_23d_transition_pr_auc"] > summary["persistence_transition_pr_auc"]
    assert summary["rf_23d_transition_brier"] < summary["persistence_transition_brier"]
    assert summary["dt_23d_transition_6class_acc"] > 0.10
    assert summary["gbt_23d_transition_pr_auc"] > 0.60
    assert summary["rf_23d_onset_risk_lift_ratio"] > 2.5
    assert summary["statistical_significance_test"]["mcnemar_transition_p_value"] < 0.05
    gap = summary["next_observed_gap_regime"]
    assert gap["rf_23d_severe_pr_auc"] > gap["persistence_severe_pr_auc"]
    assert summary["cold_start_coverage"]["total_network_segments"] == 84206
    assert summary["cold_start_coverage"]["rf_23d_network_coverage_pct"] == 100.0
    assert summary["cold_start_coverage"]["persistence_cold_start_coverage_pct"] == 0.0


@pytest.mark.skipif(
    not (HAS_FULL_DATASET and HAS_FULL_LAKEHOUSE),
    reason="Full dataset.csv and segment_static_features.parquet are gitignored in clean clones",
)
def test_transition_regime_live_dataset_execution() -> None:
    """Re-execute evaluate_transition_regimes and single-window UI scoring against full dataset.csv."""
    static_df = pd.read_parquet(LAKEHOUSE_DIR / "segment_static_features.parquet")
    peers_61781 = find_peer_segments(static_df, 61781, limit=8, prefer_observed_peers=True)
    assert len(peers_61781) == 8
    assert not peers_61781["street_name"].fillna("").str.match(r"^(Phường|Quận)\b", case=False).any()

    df = pd.read_csv(DATA_DIR / "dataset.csv")
    df["slot_ts"] = pd.to_datetime(df["slot_ts"], utc=True)
    df["target_ts"] = pd.to_datetime(df["target_ts"], utc=True)
    df = df.sort_values(["segment_id", "slot_ts"]).reset_index(drop=True)
    df["next_gap_minutes"] = (df.groupby("segment_id")["slot_ts"].shift(-1) - df["slot_ts"]).dt.total_seconds() / 60.0
    test_df = df[df["split_set"] == "TEST"].copy()
    artifact = joblib.load(OUTPUTS_DIR / "model.joblib")
    empty_df, empty_summary = evaluate_transition_regimes(
        test_df[test_df["target_next_los_ordinal"].isna()], df, artifact
    )
    assert empty_df.empty and empty_summary == {"test_samples": 0}
    trans_df, summary = evaluate_transition_regimes(
        test_df,
        df,
        artifact,
        total_segments=len(static_df),
        cold_start_segments=int(static_df["is_cold_start"].sum()),
    )
    assert len(trans_df) == 4 and len(trans_df.columns) == 10
    assert summary["test_samples"] == 1182
    assert summary["transition_samples"] == 61
    assert 0.15 <= summary["val_selected_change_threshold"] <= 0.50
    assert summary["rf_23d_transition_detect_roc_auc"] > 0.85
    assert summary["rf_23d_transition_detect_roc_auc"] > summary["dt_23d_transition_detect_roc_auc"]
    assert summary["statistical_significance_test"]["mcnemar_transition_p_value"] < 0.05
    assert summary["next_observed_gap_regime"]["rf_23d_severe_pr_auc"] > (
        summary["next_observed_gap_regime"]["persistence_severe_pr_auc"]
    )
    assert summary["cold_start_coverage"]["total_network_segments"] == 84206

    trans_row = df[(df["segment_id"] == 50026) & (df["date"] == "2021-01-04") & (df["period"] == "period_0_00")].iloc[0]
    pred_ui = _predict_single_window(trans_row, artifact)
    assert trans_row["LOS"] == "E"
    assert pred_ui["persistence_los"] == "E"
    assert pred_ui["rf_los"] == "F"
    assert pred_ui["rf_calibrated_los"] == "F"
    assert pred_ui["rf_change_prob"] > 0.70
    assert pred_ui["rf_severe_prob"] > 0.50

    story_row = df[(df["segment_id"] == 21064) & (df["date"] == "2021-04-22") & (df["period"] == "period_4_30")].iloc[0]
    pred_story = _predict_single_window(story_row, artifact)
    assert story_row["LOS"] == "E"
    assert pred_story["actual_next_los"] == "F"
    assert pred_story["rf_change_prob"] >= 0.40
    assert pred_story["rf_severe_prob"] > 0.60


def test_pre_experiment_milestone_slide_copy_calibrated_and_synced() -> None:
    """Verify the 13-slide Pre-Experiment copy stops at Section 8 + Risk + Next Plan and has zero runtime leaks."""
    from pptx import Presentation

    pre_pptx = ROOT / "HCMC_Smart_Traffic_PreExperiment_Presentation.pptx"
    pre_pdf = ROOT / "HCMC_Smart_Traffic_PreExperiment_Slides.pdf"
    assert pre_pptx.is_file() and pre_pptx.stat().st_size > 400_000
    assert pre_pdf.is_file() and pre_pdf.stat().st_size > 400_000

    ws = ROOT.parent
    if (ws / "HCMC_Smart_Traffic_Final_Presentation.pptx").is_file():
        _assert_identical_file_sha256(ws / pre_pptx.name, pre_pptx)
        _assert_identical_file_sha256(ws / pre_pdf.name, pre_pdf)

    _assert_pptx_single_line_titles_fit(pre_pptx)
    prs = Presentation(str(pre_pptx))
    assert len(prs.slides) == 13
    _assert_no_member_tokens_on_slides(prs.slides, 1, 12, label="Pre-exp Slide")

    _, _, pre_pptx_text = _extract_pptx_slides_text(pre_pptx)
    pre_pdf_pages, pre_pdf_text = _extract_pdf_pages_and_text(pre_pdf)
    assert pre_pdf_pages == 13

    required_pre_tokens = (
        "DESIGN & METHODOLOGY REPORT",
        "09–10. Risk & Next Plan",
        "8. Stream Processing",
        "9. Limitations & Risks",
        "10. Plan & Next Actions",
        "Next Actions",
        "25MS23280",
        "25MS23306",
        "25MS23310",
        "Đặng Trần Khánh",
    )
    forbidden_empirical_leaks = (
        "0.9882",
        "0.9900",
        "0.9894",
        "0.9593",
        "0.9595",
        "0.9543",
        "0.9471",
        "0.9450",
        "0.9289",
        "0.9255",
        "0.9362",
        "0.4253",
        "0.5543",
        "79.82%",
        "50.51%",
        "2/61",
        "9/61",
        "7/61",
        "10/61",
        "14/61",
        "0.001953",
        "9.35x",
        "23.55x",
        "31.36x",
        "0.636 ms",
        "0.692 ms",
        "0.603 ms",
        "0.530 ms",
        "687.43x",
        "1.023x",
        "7,294",
        "3.7515",
        "2.7799",
        "0.2853",
        "0.2197",
        "0.2241",
        "0.616%",
        "3.98%",
        "501 cửa sổ",
        "3,953",
        "377",
        "100% Hoàn thành",
        "localhost:8501",
        "Streamlit",
    )
    _assert_deliverable_tokens("Pre-Experiment PPTX", pre_pptx_text, required_pre_tokens, forbidden_empirical_leaks)
    if pre_pdf_text is not None:
        _assert_deliverable_tokens("Pre-Experiment PDF", pre_pdf_text, required_pre_tokens, forbidden_empirical_leaks)


def test_pre_experiment_presentation_script_completeness_and_sync() -> None:
    """Verify PRESENTATION_SCRIPT_PRE_EXPERIMENT.md covers all 13 slides, 10 Q&As, and zero runtime leaks."""
    pre_script = ROOT / "PRESENTATION_SCRIPT_PRE_EXPERIMENT.md"
    ws_pre_script = ROOT.parent / "PRESENTATION_SCRIPT_PRE_EXPERIMENT.md"
    assert pre_script.is_file() and pre_script.stat().st_size > 15_000
    pre_script_txt = pre_script.read_text(encoding="utf-8")
    if ws_pre_script.parent.exists() and ws_pre_script.is_file():
        assert ws_pre_script.read_text(encoding="utf-8") == pre_script_txt

    slide_markers = tuple(f"#### Slide {idx:02d}:" for idx in range(1, 14))
    qa_markers = tuple(f"### Câu {idx}:" for idx in range(1, 11))
    required_script_tokens = (
        "Nguyễn Tạ Minh Quyền",
        "25MS23280",
        "Nguyễn Thành Đạt",
        "25MS23306",
        "Phạm Công Minh",
        "25MS23310",
        "Đặng Trần Khánh",
        "HCMC_Smart_Traffic_PreExperiment_Presentation.pptx",
        "HyperLogLog++",
        "EWMA",
        "Proof-of-Scale",
        "Next Actions",
    )
    forbidden_post_run_tokens = FORBIDDEN_STALE_AND_INTERNAL_TOKENS + (
        "#### Slide 14:",
        "0.9882",
        "0.9900",
        "0.9894",
        "0.9593",
        "0.9595",
        "0.9543",
        "0.9471",
        "0.9450",
        "0.9289",
        "0.9255",
        "0.9362",
        "0.4253",
        "0.5543",
        "79.82%",
        "50.51%",
        "2/61",
        "9/61",
        "7/61",
        "10/61",
        "14/61",
        "0.001953",
        "9.35x",
        "23.55x",
        "31.36x",
        "0.636 ms",
        "0.692 ms",
        "0.603 ms",
        "0.530 ms",
        "687.43x",
        "1.023x",
        "7,294",
        "3.7515",
        "0.2853",
        "0.2241",
        "0.616%",
        "3.98%",
        "501 cửa sổ",
        "3,953",
        "377",
        "100% Hoàn thành",
        "localhost:8501",
        "Streamlit",
    )
    _assert_deliverable_tokens(
        "PRESENTATION_SCRIPT_PRE_EXPERIMENT.md",
        pre_script_txt,
        slide_markers + qa_markers + required_script_tokens,
        forbidden_post_run_tokens,
    )
    expected_speakers = {
        1: ("Nguyễn Thành Đạt (`25MS23306`)", "Thành Đạt"),
        2: ("Nguyễn Thành Đạt (`25MS23306`)", "Thành Đạt"),
        3: ("Nguyễn Thành Đạt (`25MS23306`)", "Thành Đạt"),
        4: ("Nguyễn Thành Đạt (`25MS23306`)", "Thành Đạt"),
        5: ("Nguyễn Tạ Minh Quyền (`25MS23280`)", "Minh Quyền"),
        6: ("Nguyễn Tạ Minh Quyền (`25MS23280`)", "Minh Quyền"),
        7: ("Phạm Công Minh (`25MS23310`)", "Công Minh"),
        8: ("Phạm Công Minh (`25MS23310`)", "Công Minh"),
        9: ("Phạm Công Minh (`25MS23310`)", "Công Minh"),
        10: ("Nguyễn Thành Đạt (`25MS23306`)", "Thành Đạt"),
        11: ("Nguyễn Thành Đạt (`25MS23306`)", "Thành Đạt"),
        12: ("Nguyễn Tạ Minh Quyền (`25MS23280`)", "Minh Quyền"),
        13: ("Nguyễn Tạ Minh Quyền (`25MS23280`)", "Minh Quyền"),
    }
    for idx, (speaker, short_name) in expected_speakers.items():
        block = pre_script_txt.split(f"#### Slide {idx:02d}:", 1)[1].split("\n---", 1)[0]
        assert f"- **Người trình bày:** **{speaker}**" in block, f"Wrong speaker on Pre-Exp Slide {idx:02d}"
        assert f"- **Lời thoại thuyết trình ({short_name}):**" in block, f"Missing monologue header on Slide {idx:02d}"


def test_spark_container_verification_uses_compose_network_and_mongo_service() -> None:
    """Regression test for Spark 3.5.7 / MongoDB 8.0 / Live-Consumer Docker integration config."""
    compose_txt = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    pipeline_txt = (ROOT / "pipeline.py").read_text(encoding="utf-8")
    spark_eng_txt = (ROOT / "spark_engine.py").read_text(encoding="utf-8")
    assert (ROOT / "Dockerfile.spark35").is_file()
    assert (ROOT / "requirements-spark-runtime.txt").is_file()
    for tok in ("Dockerfile.spark35", "bda501-net", "127.0.0.1:9092:9092", "live-consumer", "--from-kafka"):
        assert tok in compose_txt, f"Missing '{tok}' in docker-compose.yml"
    assert "bda501-net" in pipeline_txt and "mongodb://mongo:27017" in pipeline_txt
    assert "mongodb://mongo:27017" in spark_eng_txt


def test_live_kafka_stream_processor_and_demo_sample_fallback(tmp_path: Path) -> None:
    """Verify data/demo_sample/, producer subset selection, and live_kafka_stream batch scoring."""
    from live_kafka_stream import (
        init_live_state,
        load_readonly_scorer_context,
        process_kafka_records_batch,
    )
    from producer import _select_broker_demo_subset, generate_replay_Batch
    from toc_backend import service_helpers as dashboard_app

    sample_dir = ROOT / "data" / "demo_sample"
    for fname in (
        "sample_raw_status.csv",
        "sample_segments.csv",
        "sample_static_features.parquet",
        "sample_windows_23d.csv",
        "README.md",
    ):
        assert (sample_dir / fname).is_file(), f"Missing committed demo sample file: {fname}"

    sample_win_df = pd.read_csv(sample_dir / "sample_windows_23d.csv")
    assert len(sample_win_df) >= 200
    replay_df, _ = generate_replay_Batch(sample_win_df, seed=42)
    subset_df = _select_broker_demo_subset(replay_df, max_messages=60)
    assert len(subset_df) == 60
    assert int(subset_df["is_duplicate"].sum()) >= 2

    scorer_ctx = load_readonly_scorer_context()
    state = init_live_state()
    tel_file = tmp_path / "live_tel.json"
    scores_file = tmp_path / "live_scores.json"
    recs = subset_df.tail(25).to_dict(orient="records")
    batch_pairs = [(idx % 3, rec) for idx, rec in enumerate(recs)]
    scored_docs = process_kafka_records_batch(
        batch_pairs,
        state,
        scorer_ctx,
        mongo_client=None,
        telemetry_path=tel_file,
        scores_json_path=scores_file,
    )
    tel = json.loads(tel_file.read_text(encoding="utf-8"))
    assert tel["messages_consumed"] == 25
    assert tel["sink1_bronze_total"] == 25
    assert len(scored_docs) == tel["sink2_scored_v1"]
    assert tel["sink2_scored_v1"] + tel["duplicates_caught_bloom"] + tel["late_dropped_for_tier2"] == 25
    assert tel["hll_estimated_segments"] > 0.0

    mon_tel, mon_df = dashboard_app.query_live_kafka_stream_monitor(limit=10)
    assert isinstance(mon_tel, dict) and isinstance(mon_df, pd.DataFrame)


def test_session9_evidence_and_card_b_raw_vs_data_audit() -> None:
    """Verify ReactJS+FastAPI TOC UI (replacing legacy app.py & ui-demo.html), DATA_SETUP.md, & Session 9 evidence."""
    assert not (ROOT / "app.py").exists(), "Legacy Streamlit app.py must be retired"
    assert not (ROOT / "ui-demo.html").exists(), "Legacy ui-demo.html should be retired in favor of ReactJS+FastAPI"
    assert (ROOT / "toc_frontend" / "dist" / "index.html").is_file()
    assert (ROOT / "docs" / "DATA_SETUP.md").is_file()
    ev_dir = ROOT / "evidence" / "session9"
    required_ev = (
        "README.md",
        "A_architecture.png",
        "A_architecture.md",
        "B_benchmark.csv",
        "B_benchmark.png",
        "B_benchmark.md",
        "C_query_plan.txt",
        "C_query_plan.md",
        "D_data_validation.csv",
        "D_data_validation.md",
        "E_model_validation.csv",
        "E_model_validation.md",
        "F_serving_stream.json",
        "F_serving_stream.md",
        "G_deployment.md",
        "H_project_card.csv",
        "H_project_card.png",
        "H_project_card.md",
        "H_B1_point_in_time_matrix.csv",
        "H_B2_graph_zone_falsification.csv",
        "H_B3_missingness_sketches.csv",
        "I_readiness.md",
        "I_session9_summary_slide.png",
    )
    for fname in required_ev:
        assert (ev_dir / fname).is_file(), f"Missing Session 9 evidence file: {fname}"

    for csv_name in (
        "H_B1_point_in_time_matrix.csv",
        "H_B2_graph_zone_falsification.csv",
        "H_B3_missingness_sketches.csv",
    ):
        df_check = pd.read_csv(ev_dir / csv_name)
        assert "raw_vs_data_crosscheck" in df_check.columns
        assert df_check["raw_vs_data_crosscheck"].str.contains("VERIFIED").all()

    d_md = (ev_dir / "D_data_validation.md").read_text(encoding="utf-8")
    assert "47487" not in d_md and "47407" in d_md and "21260" in d_md

    from fastapi.testclient import TestClient
    from toc_backend.main import app as toc_app

    client = TestClient(toc_app)
    root_res = client.get("/")
    assert root_res.status_code == 200 and '<div id="root">' in root_res.text

    gr_res = client.get("/api/graph-rules")
    assert gr_res.status_code == 200
    pr_top = gr_res.json()["pagerank_top"]
    assert int(pr_top[0]["segment_id"]) == 21969 and "Nguyễn Kiệm" in str(pr_top[0]["street_name"])
    assert int(pr_top[1]["segment_id"]) == 37345
    assert all(int(row["segment_id"]) != 61781 for row in pr_top[:5])

    infra_res = client.get("/api/infrastructure-audit")
    assert infra_res.status_code == 200
    spark_ev = json.loads((OUTPUTS_DIR / "spark_runtime_evidence.json").read_text(encoding="utf-8"))
    cat = spark_ev["catalyst_join_spark"]
    infra_cat = infra_res.json()["monoid_and_catalyst"]["spark_runtime_verification"]["catalyst_join_spark"]
    assert abs(float(infra_cat["baseline_median_ms"]) - float(cat["baseline_median_ms"])) < 1e-6
    assert abs(float(infra_cat["optimized_median_ms"]) - float(cat["optimized_median_ms"])) < 1e-6
    assert abs(float(infra_cat["join_speedup_pct"]) - float(cat["join_speedup_pct"])) < 1e-6


def test_toc_fastapi_command_center_and_story_replay() -> None:
    """Verify React+FastAPI TOC Command Center (:8000), 6 alert categories, raw GPS timeline & story frames."""
    from producer import build_story_replay_frames
    from toc_backend.main import (
        api_city_frame,
        api_clusters_coldstart,
        api_graph_rules,
        api_health,
        api_infrastructure_audit,
        api_scenarios,
        api_segment_timeline,
    )

    valid_alert_types = {
        "ONSET_A_D_TO_SEVERE",
        "SEVERE_WORSENING_E_TO_F",
        "WORSENING_TRANSITION",
        "RECOVERING_TRANSITION",
        "PERSISTENT_SEVERE",
        "STABLE_FLOW",
    }
    early_warning_types = {
        "ONSET_A_D_TO_SEVERE",
        "SEVERE_WORSENING_E_TO_F",
        "WORSENING_TRANSITION",
        "RECOVERING_TRANSITION",
    }

    assert (ROOT / "toc_frontend" / "dist" / "index.html").is_file()
    health = api_health()
    assert health["status"] == "ok" and health["windows_loaded"] >= 300

    scens = api_scenarios()["scenarios"]
    assert len(scens) >= 4
    assert scens[0]["id"] == "noon_transition" and len(scens[0]["frames"]) >= 2

    frame = api_city_frame(ict_date="2021-01-05", vn_hour=12.0, alert_filter="early_warning")
    assert "[12:00 – 12:30)" in frame["window_semantics"]["collect_window"]
    assert "[12:30 – 13:00)" in frame["window_semantics"]["forecast_window"]
    assert len(frame["alerts"]) >= 2
    assert all(a["alert_type"] in valid_alert_types for a in frame["alerts"])
    assert frame["alerts"][0]["alert_type"] in early_warning_types
    assert all(a["alert_type"] != "EARLY_WARNING_TO_SEVERE" for a in frame["alerts"])

    morning_frame = api_city_frame(ict_date="2020-12-24", vn_hour=7.0, alert_filter="all")
    assert len(morning_frame["alerts"]) >= 5
    assert {a["alert_type"] for a in morning_frame["alerts"]}.issubset(valid_alert_types)

    tl = api_segment_timeline(21064, ict_date="2021-04-22", vn_hour=11.5)
    assert tl["forecast_comparison"]["current_los"] == "E"
    assert tl["forecast_comparison"]["actual_next_los"] == "F"
    assert tl["forecast_comparison"]["severe_prob"] > 0.60
    assert tl["forecast_comparison"]["change_prob"] >= 0.40
    if HAS_RAW_DATA:
        assert len(tl["raw_pings_past"]) >= 1
    tl_trans = api_segment_timeline(21063, ict_date="2021-04-22", vn_hour=11.5)
    assert tl_trans["forecast_comparison"]["current_los"] == "F"
    assert tl_trans["forecast_comparison"]["effective_forecast_los"] == "E"
    assert tl_trans["forecast_comparison"]["actual_next_los"] == "E"
    tl_sample = api_segment_timeline(47407, ict_date="2020-08-03", vn_hour=7.0)
    assert len(tl_sample["raw_pings_past"]) >= 1

    gr = api_graph_rules()
    assert len(gr["pagerank_top"]) >= 10 and len(gr["co_congestion_rules"]) >= 10

    cs = api_clusters_coldstart(segment_id=61781, limit=5)
    assert cs["selected_segment"]["badge"] == "COLD_START_STRUCTURAL_PRIOR"
    assert len(cs["peers"]) == 5

    infra = api_infrastructure_audit()
    assert len(infra["ablation_results"]) >= 6
    assert len(infra["quarantine_rules"]) >= 6
    assert len(infra["card_b1_pit_matrix"]) >= 20 and len(infra["card_b3_sketches"]) >= 8

    sample_win = pd.read_csv(ROOT / "data" / "demo_sample" / "sample_windows_23d.csv")
    story_frames, story_meta = build_story_replay_frames(sample_win, scenario="noon_transition")
    assert story_meta["frames_count"] >= 2 and len(story_frames) == story_meta["frames_count"]


def test_regression_normalize_street_name_wave2() -> None:
    """[Wave 2 Regression 1] Preserve numbered/corridor prefixes while stripping standard Đường prefix."""
    assert _normalize_street_name("Đường số 20") == "Đường số 20"
    assert _normalize_street_name("Đường D1") == "Đường D1"
    assert (
        _normalize_street_name("Đường Cao tốc TP. Hồ Chí Minh - Long Thành - Dầu Giây")
        == "Đường Cao tốc TP. Hồ Chí Minh - Long Thành - Dầu Giây"
    )
    assert _normalize_street_name("Đường Quang Trung") == "Quang Trung"
    assert _normalize_street_name("Ðường Quang Trung") == "Quang Trung"
    assert _normalize_street_name("Đường 3 Tháng 2") == "3 Tháng 2"


def test_regression_derive_urban_los_and_series() -> None:
    """[Wave 2 Regression 2] Verify BkTraffic 5 km/h urban LOS bands and high-speed corridor shifts."""
    assert derive_urban_los(35.0) == "A"
    assert derive_urban_los(34.99) == "B"
    assert derive_urban_los(30.0) == "B"
    assert derive_urban_los(25.0) == "C"
    assert derive_urban_los(20.0) == "D"
    assert derive_urban_los(15.0) == "E"
    assert derive_urban_los(14.99) == "F"
    assert derive_urban_los(0.0) == "F"
    assert derive_urban_los(np.nan) == "F"

    assert derive_urban_los(39.0, raw_max_velocity=60.0) == "B"
    assert derive_urban_los(40.0, raw_max_velocity=60.0) == "A"
    assert derive_urban_los(44.0, raw_max_velocity=80.0) == "B"
    assert derive_urban_los(45.0, raw_max_velocity=80.0) == "A"
    assert derive_urban_los(24.9, raw_max_velocity=80.0) == "F"

    speeds = pd.Series([36.0, 31.0, 26.0, 21.0, 16.0, 10.0, 40.0, 44.0, np.nan])
    vmaxs = pd.Series([40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 60.0, 80.0, 50.0])
    expected = [derive_urban_los(v, m) for v, m in zip(speeds, vmaxs)]
    actual = derive_urban_los_series(speeds, vmaxs).tolist()
    assert actual == expected
    assert derive_urban_los_series(pd.Series([35.0, 20.0, 10.0])).tolist() == ["A", "D", "F"]


def test_regression_compute_road_pagerank_drops_uturns() -> None:
    """[Wave 2 Regression 3] Exclude immediate bidirectional U-turn edges (dst.e_node_id == src.s_node_id)."""
    bi_only = pd.DataFrame(
        {
            "_id": [1, 2],
            "s_node_id": [10, 20],
            "e_node_id": [20, 10],
            "street_id": [1, 1],
            "street_name": ["Đường A", "Đường A"],
            "street_level": [2, 2],
            "street_type": ["primary", "primary"],
            "length": [100.0, 100.0],
            "max_velocity": [50.0, 50.0],
        }
    )
    pr_bi = compute_road_pagerank(bi_only, damping=0.85, iterations=30)
    assert int(pr_bi["line_graph_edges_total"].iloc[0]) == 0
    assert int(pr_bi["dangling_nodes_total"].iloc[0]) == 2
    assert pr_bi.attrs["edges_df"].empty

    with_forward = pd.DataFrame(
        {
            "_id": [1, 2, 3],
            "s_node_id": [10, 20, 20],
            "e_node_id": [20, 10, 30],
            "street_id": [1, 1, 1],
            "street_name": ["Đường A", "Đường A", "Đường A"],
            "street_level": [2, 2, 2],
            "street_type": ["primary", "primary", "primary"],
            "length": [100.0, 100.0, 100.0],
            "max_velocity": [50.0, 50.0, 50.0],
        }
    )
    pr_fwd = compute_road_pagerank(with_forward, damping=0.85, iterations=30)
    edges = pr_fwd.attrs["edges_df"]
    edge_pairs = set(zip(edges["src_seg"].astype(int), edges["dst_seg"].astype(int)))
    assert edge_pairs == {(1, 3)}
    assert (1, 2) not in edge_pairs and (2, 1) not in edge_pairs
    assert int(pr_fwd.loc[pr_fwd["segment_id"] == 1, "out_degree"].iloc[0]) == 1
    assert abs(float(pr_fwd["pagerank_score"].sum()) - 3.0) < 1e-9


def test_regression_filter_clean_raw_tables_removes_non_road() -> None:
    """[Wave 2 Regression 4] filter_clean_raw_tables drops non-road OSM tags (park, playground) & their pings."""
    tables = {
        "segments": pd.DataFrame(
            {
                "_id": [1, 2, 3, 4],
                "s_node_id": [10, 20, 30, 40],
                "e_node_id": [20, 30, 40, 50],
                "street_id": [100, 101, 102, 103],
                "street_name": ["Lê Lợi", "Nguyễn Huệ", "Công viên", "Sân chơi"],
                "street_level": [1, 3, 4, 4],
                "street_type": ["primary", "residential", "park", "playground"],
                "length": [80.0, 60.0, 40.0, 30.0],
                "max_velocity": [50.0, 40.0, np.nan, np.nan],
            }
        ),
        "segment_status": pd.DataFrame(
            {
                "_id": [1001, 1002, 1003, 1004],
                "segment_id": [1, 2, 3, 4],
                "velocity": [32.0, 25.0, 5.0, 4.0],
                "updated_at": [
                    "2020-08-01T07:00:00Z",
                    "2020-08-01T07:05:00Z",
                    "2020-08-01T07:10:00Z",
                    "2020-08-01T07:15:00Z",
                ],
            }
        ),
    }
    clean_segs, clean_status = filter_clean_raw_tables(tables)
    assert clean_segs["_id"].astype(int).tolist() == [1, 2]
    assert not clean_segs["street_type"].isin(["park", "playground"]).any()
    assert clean_status["segment_id"].astype(int).tolist() == [1, 2]


def test_regression_attach_causal_priors_strict_tau_lt_t_and_cutoff_freeze() -> None:
    """[Wave 2 Regression 5] Verify strict tau < t isolation (even at equal slot_ts) & train_cutoff_ts freeze."""
    timestamps = pd.date_range("2020-08-01T00:00:00Z", periods=6, freq="30min")
    rows = []
    for tidx, ts in enumerate(timestamps):
        for seg_id, street_id in ((101, 10), (102, 10), (201, 20)):
            rows.append(
                {
                    "segment_id": seg_id,
                    "street_id": street_id,
                    "date": ts.strftime("%Y-%m-%d"),
                    "period": f"period_{ts.hour}_{ts.minute:02d}",
                    "slot_ts": ts,
                    "los_ordinal": (tidx + seg_id) % 5,
                    "current_los_ordinal": (tidx + seg_id) % 5,
                    "is_severe": int(((tidx + seg_id) % 5) >= 4),
                    "mean_velocity": 32.0 - 2.0 * tidx,
                    "velocity_ratio": 0.7 - 0.05 * tidx,
                    "max_velocity": 50.0,
                    "upstream_segment_id": seg_id,
                    "is_synthetic_burst": 0,
                }
            )
    base_df = pd.DataFrame(rows)
    t2 = timestamps[2]
    train_cutoff = timestamps[3]

    out_base = _attach_causal_priors_and_neighbors(base_df, train_cutoff_ts=train_cutoff)

    # Perturb all observations at slot_ts >= t2 (including equal-timestamp peer at t2 and future t3..t5)
    pert_t2_df = base_df.copy()
    mask_ge_t2 = pert_t2_df["slot_ts"] >= t2
    pert_t2_df.loc[mask_ge_t2, "los_ordinal"] = 5
    pert_t2_df.loc[mask_ge_t2, "current_los_ordinal"] = 5
    pert_t2_df.loc[mask_ge_t2, "is_severe"] = 1
    pert_t2_df.loc[mask_ge_t2, "mean_velocity"] = 2.0
    pert_t2_df.loc[mask_ge_t2, "velocity_ratio"] = 0.04
    out_pert_t2 = _attach_causal_priors_and_neighbors(pert_t2_df, train_cutoff_ts=train_cutoff)

    prior_cols = (
        "causal_global_los",
        "causal_global_sev",
        "street_causal_prior_los",
        "street_causal_prior_sev",
        "segment_train_prior_los",
        "segment_train_severe_prior",
        "causal_mean_velocity",
        "causal_velocity_ratio",
    )
    le_t2_base = out_base[out_base["slot_ts"] <= t2]
    le_t2_pert = out_pert_t2[out_pert_t2["slot_ts"] <= t2]
    for col in prior_cols:
        assert np.allclose(le_t2_base[col].to_numpy(), le_t2_pert[col].to_numpy(), atol=1e-12)

    # Perturb post-cutoff observations at t4 (> train_cutoff = t3); priors at t5 must remain frozen!
    pert_post_df = base_df.copy()
    mask_t4 = pert_post_df["slot_ts"] == timestamps[4]
    pert_post_df.loc[mask_t4, "los_ordinal"] = 5
    pert_post_df.loc[mask_t4, "is_severe"] = 1
    pert_post_df.loc[mask_t4, "mean_velocity"] = 1.0
    out_pert_post = _attach_causal_priors_and_neighbors(pert_post_df, train_cutoff_ts=train_cutoff)

    t5_base = out_base[out_base["slot_ts"] == timestamps[5]]
    t5_post = out_pert_post[out_pert_post["slot_ts"] == timestamps[5]]
    for col in prior_cols:
        assert np.allclose(t5_base[col].to_numpy(), t5_post[col].to_numpy(), atol=1e-12)


def test_regression_gold_parquet_lakehouse_no_duplicate_year_month(tmp_path: Path) -> None:
    """[Wave 2 Regression 6] persist_medallion_lakehouse writes Gold partitions readable without year_month clash."""
    medallion_df = pd.DataFrame(
        {
            "event_id": ["e1", "e2", "e3"],
            "segment_id": [101, 102, 101],
            "date": ["2020-07-31", "2020-08-01", "2020-08-02"],
            "year_month": ["2020-07", "2020-08", "2020-08"],
            "period": ["period_23_30", "period_0_00", "period_0_30"],
            "mean_velocity": [35.0, 22.0, 12.0],
            "LOS": ["A", "D", "F"],
        }
    )
    replay_df = medallion_df[["event_id", "segment_id", "date", "period", "mean_velocity"]].copy()
    lh_dir = tmp_path / "lakehouse"
    info = persist_medallion_lakehouse(medallion_df, replay_df, lh_dir)
    assert info["year_month_partitions"] == {"2020-07": 1, "2020-08": 2}

    gold_dir = lh_dir / "curated_gold"
    assert (gold_dir / "year_month=2020-07" / "part-0.parquet").is_file()
    assert (gold_dir / "year_month=2020-08" / "part-0.parquet").is_file()

    gold_read = pd.read_parquet(gold_dir)
    assert len(gold_read) == 3
    assert list(gold_read.columns).count("year_month") == 1
    assert set(gold_read["year_month"].astype(str)) == {"2020-07", "2020-08"}


def test_regression_vmax_lookup_train_cutoff_guard_and_zero_leakage() -> None:
    """[Wave B Regression 1] Guard fit_train_vmax_lookup against post-cutoff leakage and verify 472 P85 segs."""
    cutoff = pd.Timestamp("2020-12-12T08:00:00Z")
    leaked_df = pd.DataFrame(
        {
            "segment_id": [101, 101],
            "street_level": [2, 2],
            "street_type": ["primary", "primary"],
            "max_velocity": [np.nan, np.nan],
            "mean_velocity": [35.0, 42.0],
            "slot_ts": [pd.Timestamp("2020-12-12T07:30:00Z"), pd.Timestamp("2020-12-12T08:30:00Z")],
        }
    )
    with pytest.raises(AssertionError, match="leakage guard"):
        fit_train_vmax_lookup(leaked_df, train_cutoff_ts=cutoff)

    clean_lookup = fit_train_vmax_lookup(leaked_df.iloc[:1], train_cutoff_ts=cutoff)
    assert clean_lookup["fitted_on"] == "ORGANIC_TRAIN_ONLY"

    if HAS_FULL_DATASET:
        vmax_json = json.loads((OUTPUTS_DIR / "vmax_lookup.json").read_text(encoding="utf-8"))
        assert len(vmax_json["tier1_segment_train_p85"]) == 472


def test_regression_toc_api_ping_id_b1_23d_no_nan() -> None:
    """[Wave B Regression 2] Verify TOC API 23D B1 matrix, Mongo speedup, timeline ping_id > 0, & NaN street clean."""
    from fastapi.testclient import TestClient
    from toc_backend.data_store import _clean_street_name
    from toc_backend.main import app as toc_app

    client = TestClient(toc_app)
    infra_res = client.get("/api/infrastructure-audit")
    assert infra_res.status_code == 200
    infra = infra_res.json()

    b1_rows = infra["card_b1_pit_matrix"]
    assert len(b1_rows) == 23
    assert [int(r["feature_idx"]) for r in b1_rows] == list(range(1, 24))

    lat = infra["mongo_verify"]["uc2_arterial_query"]["latency_benchmark_ms"]
    assert float(lat["speedup_vs_collscan_x"]) > 1.0

    tl_res = client.get("/api/segment/47407/timeline", params={"ict_date": "2020-08-03", "vn_hour": 7.0})
    assert tl_res.status_code == 200
    pings = tl_res.json()["raw_pings_past"]
    if pings:
        assert all(int(p["ping_id"]) > 0 for p in pings)

    assert _clean_street_name(float("nan"), 84535) == "Segment #84535"
    assert _clean_street_name("  ", 84535) == "Segment #84535"
    assert _clean_street_name("Lý Thường Kiệt", 47407) == "Lý Thường Kiệt"


def test_regression_live_kafka_exact_segments_and_val_threshold(tmp_path: Path) -> None:
    """[Wave B Regression 3] Verify live_kafka_stream urban LOS, selected_val_threshold, & exact_distinct_segments."""
    import live_kafka_stream as lks

    assert lks.derive_urban_los(18.0, 40.0) == "E"
    assert lks.derive_urban_los(18.0, 60.0) == "F"
    assert lks.derive_urban_los(45.0, 70.0) == "A"

    scorer_ctx = lks.load_readonly_scorer_context()
    manifest = json.loads((OUTPUTS_DIR / "model_registry_manifest.json").read_text(encoding="utf-8"))
    assert "selected_val_threshold" in scorer_ctx
    assert abs(float(scorer_ctx["selected_val_threshold"]) - float(manifest["selected_val_threshold"])) < 1e-9

    state = lks.init_live_state()
    tel_file = tmp_path / "reg_tel.json"
    scores_file = tmp_path / "reg_scores.json"
    batch_pairs = [
        (0, {"raw_event_id": "r1", "segment_id": 47407, "velocity": 18.0, "updated_at": "2020-08-03T00:05:00Z"}),
        (1, {"raw_event_id": "r2", "segment_id": 21064, "velocity": 36.0, "updated_at": "2020-08-03T00:10:00Z"}),
        (2, {"raw_event_id": "r3", "segment_id": 47407, "velocity": 14.0, "updated_at": "2020-08-03T00:15:00Z"}),
    ]
    lks.process_kafka_records_batch(
        batch_pairs,
        state,
        scorer_ctx,
        mongo_client=None,
        telemetry_path=tel_file,
        scores_json_path=scores_file,
    )
    tel = json.loads(tel_file.read_text(encoding="utf-8"))
    assert "exact_distinct_segments" in tel
    assert tel["exact_distinct_segments"] == 2


def test_regression_baseline_utc_offset_normalization() -> None:
    """[Wave B Regression 4] Verify baseline._utc_window_key normalizes UTC+7 offsets to UTC windows."""
    import baseline

    ict_key = baseline._utc_window_key("2020-08-03T07:15:00+07:00")
    utc_key = baseline._utc_window_key("2020-08-03T00:15:00Z")
    assert ict_key == ("2020-08-03", "period_0_00")
    assert ict_key == utc_key
    assert baseline._utc_window_key("2020-08-03T00:15:00+07:00") == ("2020-08-02", "period_17_00")
