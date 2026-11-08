"""Comprehensive Automated Verification Suite for HCMC Smart Traffic Platform (V3).

Covers:
- Sub-Problems P1, P2, P3, P4
- Operational Use Cases UC-1 through UC-6
- Empirical Edge Cases EC-1 through EC-10
- Zero-Conflict 3-Phase MLOps Contract ([C13a] & [C13b])
- Product Lifecycle Governance Tree (product/ 00-proposal .. 09-growth)
"""

from __future__ import annotations

import hashlib
import json
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import (  # noqa: E402
    _attach_causal_priors_and_neighbors,
    _bloom_hashes,
    _hll_add,
    _hll_estimate,
    _is_valid_street_entity,
    _normalize_street_name,
    _parse_period_index,
)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
LAKEHOUSE_DIR = DATA_DIR / "lakehouse"
OUTPUTS_DIR = ROOT / "outputs"
PRODUCT_DIR = ROOT / "product"


def _load_metrics() -> dict:
    return json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))


def test_p1_ec1_ec2_ec4_quality_funnel_and_imputation() -> None:
    metrics = _load_metrics()
    qf = metrics["quality_funnel_c5"]
    assert qf["zero_length_segments"] == 154
    assert qf["non_road_osm_segments"] == 427
    assert qf["street_entities_merged_by_nfc"] > 0

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
    assert tier_counts == {
        "TIER_2_LEVEL_ROADTYPE_MEDIAN": 28446,
        "TIER_1_SEGMENT_OBSERVED": 4946,
        "TIER_3_HIERARCHICAL_LEVEL_PRIOR": 49,
    }
    assert qf["vmax_imputation_tiers_windows"] == {
        "TIER_1_SEGMENT_OBSERVED": 4946,
        "TIER_2_LEVEL_ROADTYPE_MEDIAN": 28446,
        "TIER_3_HIERARCHICAL_LEVEL_PRIOR": 49,
    }


def test_p1_ec8_monoid_salting_and_medallion_lakehouse() -> None:
    monoid_verify = json.loads(
        (OUTPUTS_DIR / "monoid_baseline_verification.json").read_text(encoding="utf-8")
    )
    assert monoid_verify["raw_distinct_utc_windows"] == 33441
    assert monoid_verify["medallion_matched_windows"] == 33441
    assert monoid_verify["utc_window_join_match_pct"] == 100.0
    assert monoid_verify["obs_count_discrepancies"] == 0
    assert monoid_verify["mean_velocity_discrepancies"] == 0
    assert monoid_verify["total_severe_pings_accumulated"] == 42170
    assert monoid_verify["monoid_tuple"] == "(cnt, sum_v, min_v, max_v, severe_cnt)"
    assert monoid_verify["identity_element"] == "(0, 0.0, +inf, 0.0, 0)"
    assert monoid_verify["monoid_associativity_verified"] is True

    join_plan = json.loads((OUTPUTS_DIR / "join_plan_evidence.json").read_text(encoding="utf-8"))
    salting = join_plan["pillar_i1_monoid_and_salting"]
    assert salting["exact_monoid_equivalence"] is True
    assert salting["raw_input_pings"] == 90938
    assert salting["stage1_partial_salted_groups"] == 59895
    assert salting["stage2_reduced_windows"] == 33441
    assert salting["salt_bucket_cv"] < 0.02
    assert 1.0 <= salting["salted_partition_skew_ratio"] <= 1.05
    assert join_plan["pillar_i2_catalyst_join_optimization"]["baseline_exchange_stages"] == 7
    assert join_plan["pillar_i2_catalyst_join_optimization"]["optimized_exchange_stages"] == 3
    assert join_plan["pillar_i2_catalyst_join_optimization"]["dataframe_vs_sql_exceptAll_diff"] == 0
    assert join_plan["pillar_i2_catalyst_join_optimization"]["partition_count"] == 10

    assert (LAKEHOUSE_DIR / "bronze_raw_events" / "events.parquet").is_file()
    assert (LAKEHOUSE_DIR / "silver_cleaned_windows" / "windows.parquet").is_file()
    gold_parts = list((LAKEHOUSE_DIR / "curated_gold").glob("year_month=*"))
    assert len(gold_parts) == 10


def test_p2_uc3_ec7_orphan_pruning_and_line_graph_pagerank() -> None:
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


def test_p2_uc4_ec3_nfc_street_hygiene_and_fpgrowth_rules() -> None:
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
    assert rules_df.iloc[0]["antecedent_street"] == "Lý Chính Thắng"
    assert rules_df.iloc[0]["consequent_street"] == "Nguyễn Thông"
    assert abs(float(rules_df.iloc[0]["lift"]) - 28.5833) < 1e-3


def test_p2_uc5_ec5_ec6_7d_kmeans_and_cold_start_feature_store() -> None:
    metrics = _load_metrics()
    model_m = metrics["model"]
    assert model_m["clusters_populated"] == 6
    assert model_m["segments_clustered"] == 7549
    assert abs(float(model_m["silhouette_score"]) - 0.3158) < 1e-3
    assert abs(float(model_m["davies_bouldin_index"]) - 1.1462) < 1e-3

    static_df = pd.read_parquet(LAKEHOUSE_DIR / "segment_static_features.parquet")
    assert len(static_df) == 84633
    assert int((~static_df["is_cold_start"]).sum()) == 10027
    assert int(static_df["is_cold_start"].sum()) == 74606
    assert int(static_df["max_velocity"].isna().sum()) == 0
    assert abs(float(static_df["pagerank_score"].sum()) - 84633.0) < 1e-3
    seg_tiers = static_df["vmax_impute_tier"].value_counts().to_dict()
    assert seg_tiers == {
        "TIER_2_LEVEL_ROADTYPE_MEDIAN": 74309,
        "TIER_1_SEGMENT_OBSERVED": 9871,
        "TIER_3_HIERARCHICAL_LEVEL_PRIOR": 453,
    }


def test_p3_ec5_causal_expanding_priors_zero_future_leakage() -> None:
    synthetic = pd.DataFrame(
        {
            "segment_id": [101, 101, 101, 202, 202],
            "street_id": [10, 10, 10, 20, 20],
            "date": ["2020-08-01", "2020-08-01", "2020-08-01", "2020-08-01", "2020-08-01"],
            "period": ["period_1_00", "period_1_30", "period_2_00", "period_1_00", "period_1_30"],
            "slot_ts": pd.to_datetime(
                [
                    "2020-08-01T01:00:00Z",
                    "2020-08-01T01:30:00Z",
                    "2020-08-01T02:00:00Z",
                    "2020-08-01T01:00:00Z",
                    "2020-08-01T01:30:00Z",
                ]
            ),
            "los_ordinal": [0, 1, 5, 2, 3],
            "is_severe": [0, 0, 1, 0, 0],
            "current_los_ordinal": [0, 1, 5, 2, 3],
            "upstream_segment_id": [202, 202, 202, 101, 101],
        }
    ).sort_values(["slot_ts", "segment_id"]).reset_index(drop=True)

    res_a = _attach_causal_priors_and_neighbors(synthetic.copy())
    perturbed = synthetic.copy()
    perturbed.loc[perturbed["period"] == "period_2_00", "los_ordinal"] = 0
    perturbed.loc[perturbed["period"] == "period_2_00", "is_severe"] = 0
    res_b = _attach_causal_priors_and_neighbors(perturbed)

    early_a = res_a[res_a["period"] != "period_2_00"]["segment_train_prior_los"].to_numpy()
    early_b = res_b[res_b["period"] != "period_2_00"]["segment_train_prior_los"].to_numpy()
    assert np.allclose(early_a, early_b, atol=1e-12)


def test_p3_ml_ablation_and_zero_conflict_registry() -> None:
    metrics = _load_metrics()
    ev = metrics["evaluation"]
    assert ev["train_samples"] == 16389
    assert ev["val_samples"] == 3512
    assert ev["test_samples"] == 3513
    assert ev["positive_severe_test_samples"] == 1177
    assert abs(float(ev["severe_f1"]) - 0.6670) < 1e-3
    assert abs(float(ev["average_precision"]) - 0.7568) < 1e-3
    assert abs(float(ev["roc_auc"]) - 0.8458) < 1e-3

    manifest = json.loads((OUTPUTS_DIR / "model_registry_manifest.json").read_text(encoding="utf-8"))
    actual_sha = hashlib.sha256((OUTPUTS_DIR / "model.joblib").read_bytes()).hexdigest()
    assert manifest["phase_a_fit_only"] is True
    assert manifest["phase_b_c_read_only_verified"] is True
    assert manifest["sha256_digest"] == actual_sha


def test_p4_uc1_uc6_ec8_ec9_stream_sketches_and_reconciliation() -> None:
    metrics = _load_metrics()
    sm = metrics["streaming_mmds"]
    assert sm["total_stream_events_processed"] == 33775
    assert sm["valid_unique_events"] == 33441
    assert sm["injected_duplicate_events"] == 334
    assert sm["duplicates_removed_exact"] == 334
    assert sm["injected_late_in_watermark"] == 1170
    assert sm["injected_late_dropped_by_watermark"] == 501
    assert sm["decay_alpha"] == 0.15
    assert sm["bloom_filter"]["memory_kb"] == 32.0
    assert sm["hyperloglog"]["memory_kb"] == 1.0
    assert sm["hyperloglog"]["relative_error_pct"] < 5.0

    recon = sm["reconciliation"]
    assert recon["in_watermark_like_for_like_match_pct"] == 100.0
    assert recon["tier1_exceptAll_diff_count"] == 0
    assert recon["affected_late_windows_recomputed"] == 319
    assert recon["pre_reconciliation_completeness_pct"] == 98.5
    assert recon["post_reconciliation_completeness_pct"] == 100.0

    bronze_df = pd.read_parquet(LAKEHOUSE_DIR / "bronze_raw_events" / "events.parquet")
    serving_df = pd.read_parquet(LAKEHOUSE_DIR / "serving_traffic_scores.parquet")
    bronze_late_ids = set(
        bronze_df[bronze_df["lateness_bucket"] == "LATE_OUT_OF_WATERMARK"]["event_id"].unique()
    )
    serving_reconciled_ids = set(
        serving_df[serving_df["lifecycle_status"] == "BATCH_RECONCILED"]["event_id"].unique()
    )
    assert len(bronze_late_ids) == 501
    assert bronze_late_ids == serving_reconciled_ids

    hotspots = pd.read_csv(OUTPUTS_DIR / "hotspot_alerts.csv")
    assert len(hotspots) == 25
    assert set(hotspots["latest_los"].unique()).issubset({"E", "F"})
    assert bool((hotspots["alert_severity"] == "CRITICAL (E/F)").all())
    assert (OUTPUTS_DIR / "checkpoints" / "raw" / "offset_log.json").is_file()
    assert (OUTPUTS_DIR / "checkpoints" / "agg" / "state_wal.json").is_file()
    assert (OUTPUTS_DIR / "query_progress.json").is_file()


def test_p4_uc2_ec10_mongodb_ixscan_verification() -> None:
    mongo_verify = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    assert mongo_verify["write_partitions"] == 4
    assert mongo_verify["total_rows_upserted"] == 33441
    assert mongo_verify["batch_size"] == 500
    ix = mongo_verify["ixscan_explain_verification"]
    assert ix["index_used"] == "level_prob_segment_idx"
    assert "IXSCAN" in ix["winning_stages"]
    assert ix["in_memory_sort"] is False


def test_sketch_and_period_unit_edge_cases() -> None:
    import joblib
    from app import _predict_single_window, _select_forecast_candidates

    positions = _bloom_hashes("edge_case_event_001", num_bits=262144, num_hashes=4)
    assert len(positions) == 4
    assert all(0 <= p < 262144 for p in positions)

    reg = np.zeros(1024, dtype=np.uint8)
    for idx in range(200):
        _hll_add(reg, f"seg_{idx}", p_bits=10)
    est = _hll_estimate(reg)
    assert 170.0 <= est <= 230.0

    periods = pd.Series(["period_0_00", "period_7_30", "period_23_30"])
    parsed = _parse_period_index(periods).tolist()
    assert parsed == [0, 15, 47]

    df = pd.read_csv(DATA_DIR / "dataset.csv")
    sparse_candidates = _select_forecast_candidates(df, "Tất cả tuyến đường", True)
    assert len(sparse_candidates) > 0
    assert int(sparse_candidates["target_next_los_ordinal"].isna().sum()) > 0
    single_row = sparse_candidates[sparse_candidates["target_next_los_ordinal"].isna()].iloc[0]
    artifact = joblib.load(OUTPUTS_DIR / "model.joblib")
    pred = _predict_single_window(single_row, artifact)
    assert pred["actual_next_los"] == "N/A (Cuối chuỗi)"
    assert pred["rf_los"] in {"A", "B", "C", "D", "E", "F"}


def test_workflow_product_documentation_structure() -> None:
    expected_dirs = [
        "00-proposal",
        "01-discovery",
        "02-strategy",
        "03-requirements",
        "04-design",
        "05-architecture",
        "06-engineering",
        "07-quality",
        "08-launch",
        "09-growth",
    ]
    for folder in expected_dirs:
        dir_path = PRODUCT_DIR / folder
        assert dir_path.is_dir(), f"Missing workflow folder: {dir_path}"
        md_files = list(dir_path.rglob("*.md"))
        assert len(md_files) >= 2, f"Expected markdown deliverables in {dir_path}"
