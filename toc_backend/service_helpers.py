"""Pure-Python Analytical & Operational Serving Helpers for the HCMC Smart Traffic TOC Platform."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pipeline import find_peer_segments
from toc_backend.story_scenarios import build_window_semantics

ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = ROOT / "data" / "dataset.csv"
DATASET_SAMPLE_PATH = ROOT / "data" / "demo_sample" / "sample_windows_23d.csv"
STATIC_STORE_PATH = ROOT / "data" / "lakehouse" / "segment_static_features.parquet"
STATIC_SAMPLE_PATH = ROOT / "data" / "demo_sample" / "sample_static_features.parquet"
SERVING_PARQUET_PATH = ROOT / "data" / "lakehouse" / "serving_traffic_scores.parquet"
METRICS_PATH = ROOT / "outputs" / "metrics.json"
MODEL_PATH = ROOT / "outputs" / "model.joblib"
MANIFEST_PATH = ROOT / "outputs" / "model_registry_manifest.json"
ROC_PATH = ROOT / "outputs" / "roc_curve.png"
ARCH_IMG_PATH = ROOT / "outputs" / "system_architecture.png"
ABLATION_PATH = ROOT / "outputs" / "ablation_results.csv"
PAGERANK_PATH = ROOT / "outputs" / "road_pagerank_top.csv"
RULES_PATH = ROOT / "outputs" / "co_congestion_rules.csv"
LAGGED_RULES_PATH = ROOT / "outputs" / "lagged_congestion_rules.csv"
HOTSPOTS_PATH = ROOT / "outputs" / "hotspot_alerts.csv"
RECON_PATH = ROOT / "outputs" / "reconciliation_audit.csv"
QUARANTINE_PATH = ROOT / "outputs" / "quarantine_summary.csv"
MONGO_VERIFY_PATH = ROOT / "outputs" / "mongo_verify.json"
JOIN_PLAN_PATH = ROOT / "outputs" / "join_plan_evidence.json"
MONOID_VERIFY_PATH = ROOT / "outputs" / "monoid_baseline_verification.json"
SPARK_VERIFY_PATH = ROOT / "outputs" / "spark_runtime_evidence.json"
QUERY_PROGRESS_PATH = ROOT / "outputs" / "query_progress.json"
INVENTORY_PATH = ROOT / "outputs" / "file_inventory.csv"
TRANSITION_CSV_PATH = ROOT / "outputs" / "transition_evaluation.csv"
LIVE_TELEMETRY_PATH = ROOT / "outputs" / "live_kafka_telemetry.json"
LIVE_SCORES_PATH = ROOT / "outputs" / "live_kafka_scores.json"

LOS_ORDER = ["A", "B", "C", "D", "E", "F"]
ORDINAL_TO_LOS = {index: label for index, label in enumerate(LOS_ORDER)}


def _read_optional_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.is_file() else pd.DataFrame()


def _read_optional_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def _format_vn_time(vn_hour_val: Any) -> str:
    if pd.isna(vn_hour_val):
        return "N/A"
    total_minutes = int(round(float(vn_hour_val) * 60)) % (24 * 60)
    hh = total_minutes // 60
    mm = total_minutes % 60
    return f"{hh:02d}:{mm:02d} ICT"


def _utc_to_ict_date(date_val: Any, period_val: Any) -> str:
    d_str = str(date_val or "")
    p_str = str(period_val or "")
    if not d_str or not p_str.startswith("period_"):
        return d_str
    try:
        parts = p_str.replace("period_", "", 1).split("_")
        utc_dt = datetime.strptime(d_str, "%Y-%m-%d") + timedelta(hours=int(parts[0]), minutes=int(parts[1]))
        return (utc_dt + timedelta(hours=7)).strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        return d_str


def _derive_ict_date_series(df: pd.DataFrame) -> pd.Series:
    if "ict_date" in df.columns and bool(df["ict_date"].notna().all()):
        return df["ict_date"].astype(str)
    if "date" in df.columns and "period" in df.columns:
        return pd.Series(
            [_utc_to_ict_date(d, p) for d, p in zip(df["date"], df["period"])],
            index=df.index,
        )
    return df["date"].astype(str) if "date" in df.columns else pd.Series("", index=df.index)


def _predict_single_window(row: pd.Series, artifact: dict[str, Any]) -> dict[str, Any]:
    """Score one 30m window row with Persistence, DT, RF raw, VAL-calibrated RF, and GBT binary."""
    features = artifact["full_features"]
    frame_row = pd.DataFrame([{col: row.get(col, np.nan) for col in features}])
    if pd.isna(frame_row["current_los_ordinal"].iloc[0]):
        frame_row["current_los_ordinal"] = row.get("los_ordinal", 0)
    matrix = artifact["ml_imputer"].transform(frame_row[features])

    probs_6 = artifact["rf_multiclass"].predict_proba(matrix)[0]
    rf_pred_ord = int(artifact["rf_multiclass"].predict(matrix)[0])
    dt_pred_ord = int(artifact["dt_multiclass"].predict(matrix)[0])
    rf_severe_prob = float(probs_6[4:].sum())
    gbt_severe_prob = float(artifact["gbt_severe"].predict_proba(matrix)[0, 1])
    persistence_ord = int(row.get("los_ordinal", 0))
    valid_ord = 0 <= persistence_ord < 6
    rf_change_prob = float(1.0 - probs_6[persistence_ord]) if valid_ord else 0.0
    alt_probs = probs_6.copy()
    if valid_ord:
        alt_probs[persistence_ord] = -1.0
    change_threshold = float(artifact.get("val_selected_change_threshold", 0.30))
    cal_ord = int(alt_probs.argmax()) if rf_change_prob >= change_threshold else persistence_ord

    target_val = row.get("target_next_los_ordinal")
    actual_next_los = (
        ORDINAL_TO_LOS.get(int(target_val), "N/A")
        if pd.notna(target_val)
        else "N/A (Không có cặp +30m kế tiếp)"
    )

    return {
        "persistence_los": ORDINAL_TO_LOS.get(persistence_ord, "A"),
        "dt_los": ORDINAL_TO_LOS.get(dt_pred_ord, "A"),
        "rf_los": ORDINAL_TO_LOS.get(rf_pred_ord, "A"),
        "rf_calibrated_los": ORDINAL_TO_LOS.get(cal_ord, "A"),
        "rf_change_prob": rf_change_prob,
        "rf_severe_prob": rf_severe_prob,
        "gbt_severe_prob": gbt_severe_prob,
        "actual_next_los": actual_next_los,
    }


def _format_lifecycle_summary(breakdown: dict[str, int]) -> str:
    if not breakdown:
        return "Đang đồng bộ"
    verified = breakdown.get("BATCH_VERIFIED", 0)
    reconciled = breakdown.get("BATCH_RECONCILED", 0)
    provisional = breakdown.get("STREAM_PROVISIONAL", 0)
    return (
        f"{verified:,} khớp đúng hạn (BATCH_VERIFIED) · "
        f"{reconciled:,} đã bù trễ (BATCH_RECONCILED) · "
        f"{provisional:,} tạm thời (STREAM_PROVISIONAL)"
    )


def _format_hotspots_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "segment_id" not in df.columns:
        return df
    return pd.DataFrame(
        {
            "Mã đoạn": df["segment_id"].astype(int),
            "Tuyến đường": df["street_name"].fillna("N/A"),
            "Điểm EWMA": df["decaying_hotspot_score"].astype(float).round(2),
            "Vận tốc (km/h)": df["latest_velocity_kmh"].astype(float).round(1),
            "Mức LOS": df["latest_los"],
        }
    )


def _format_alerts_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "segment_id" not in df.columns:
        return df
    vn_times = df["vn_hour"].map(_format_vn_time) if "vn_hour" in df.columns else df.get("period", "")
    ict_dates = _derive_ict_date_series(df)
    return pd.DataFrame(
        {
            "Mã đoạn": df["segment_id"].astype(int),
            "Tuyến đường": df["street_name"].fillna("N/A"),
            "Cấp": df["street_level"].astype(int),
            "Ngày (ICT)": ict_dates,
            "Giờ (VN)": vn_times,
            "LOS hiện tại": df["current_los"],
            "Dự báo (+30p)": df["predicted_next_los"],
            "Nguy cơ E/F (%)": (df["severe_prob"].astype(float) * 100.0).round(1),
            "PageRank": df["pagerank_score"].astype(float).round(2),
        }
    )


def _format_transition_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "regime" not in df.columns:
        return df
    return pd.DataFrame(
        {
            "Tình huống kiểm thử": df["regime"],
            "Số mẫu": df["sample_count"].astype(int),
            "Đoán đúng 6 mức (Baseline)": df["persistence_6class_acc"],
            "Đoán đúng 6 mức (RF 23 biến)": df["rf_23d_6class_acc"],
            "PR-AUC Kẹt cứng (Baseline)": df["persistence_severe_pr_auc"],
            "PR-AUC Kẹt cứng (RF 23 biến)": df["rf_23d_severe_pr_auc"],
            "Sai số Brier (Baseline)": df["persistence_brier_error"],
            "Sai số Brier (RF 23 biến)": df["rf_23d_brier_error"],
        }
    )


def _format_peers_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "segment_id" not in df.columns:
        return df
    sim_col = "Điểm tương đồng" if "Điểm tương đồng" in df.columns else "similarity_score"
    return pd.DataFrame(
        {
            "Mã đoạn": df["segment_id"].astype(int),
            "Tuyến đường": df["street_name"].fillna("N/A"),
            "Cấp đường": df["street_level"].astype(int),
            "Loại đường": df["street_type"] if "street_type" in df.columns else "N/A",
            "Chiều dài (m)": df["length_m"].astype(float).round(1) if "length_m" in df.columns else 0.0,
            "Vận tốc TB (km/h)": df["mean_velocity"].astype(float).round(1),
            "Tốc độ tối đa (km/h)": df["max_velocity"].astype(float).round(0),
            "PageRank": df["pagerank_score"].astype(float).round(3),
            "Điểm tương đồng": df[sim_col].astype(float).round(4) if sim_col in df.columns else 1.0,
        }
    )


def _format_live_stream_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "segment_id" not in df.columns:
        return df
    seq_col = df["seq_id"].astype(int) if "seq_id" in df.columns else pd.Series(range(1, len(df) + 1), index=df.index)
    part_col = df["kafka_partition"].astype(int) if "kafka_partition" in df.columns else 0
    return pd.DataFrame(
        {
            "STT": seq_col,
            "Giờ nhận": df["received_at_ict"] if "received_at_ict" in df.columns else "",
            "Partition": part_col,
            "Mã đoạn": df["segment_id"].astype(int),
            "Tuyến đường": df["street_name"].fillna("N/A"),
            "Cấp": df["street_level"].astype(int),
            "Vận tốc (km/h)": df["mean_velocity"].astype(float).round(1),
            "LOS hiện tại": df["current_los"],
            "Dự báo +30p (RF 23D)": df["predicted_next_los"],
            "Nguy cơ E/F (%)": (df["severe_prob"].astype(float) * 100.0).round(1),
            "Điểm EWMA": df["ewma_hotspot_score"].astype(float).round(2),
            "Trạng thái": df["lifecycle_status"],
        }
    )


def _find_similar_segments(
    data: pd.DataFrame,
    query_row: pd.Series | int,
    limit: int = 10,
    static_store_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Find Top-K peers in 7D K-Means space across all motorized segments (including Cold-Start)."""
    query_seg_id = int(query_row) if isinstance(query_row, (int, np.integer)) else int(query_row["segment_id"])
    if static_store_df is None and STATIC_STORE_PATH.is_file():
        static_store_df = pd.read_parquet(STATIC_STORE_PATH)
    if static_store_df is not None and not static_store_df.empty:
        peers = find_peer_segments(static_store_df, query_seg_id, limit=limit, prefer_observed_peers=True)
        if not peers.empty:
            return peers

    if isinstance(query_row, (int, np.integer)):
        return pd.DataFrame()
    seg_summary = data.groupby("segment_id", as_index=False).agg(
        street_name=("street_name", "first"),
        street_type=("street_type", "first"),
        street_level=("street_level", "first"),
        mean_velocity=("mean_velocity", "mean"),
        max_velocity=("max_velocity", "first"),
        pagerank_score=("pagerank_score", "first"),
        severe_rate=("is_severe", "mean"),
        cluster_id=("cluster_id", "first"),
    )
    same_cluster = seg_summary[
        (seg_summary["cluster_id"] == int(query_row["cluster_id"])) & (seg_summary["segment_id"] != query_seg_id)
    ].copy()
    if same_cluster.empty:
        return same_cluster

    q_vec = np.array(
        [
            float(query_row["mean_velocity"]),
            float(query_row["pagerank_score"]),
            float(query_row["segment_train_severe_prior"]),
        ]
    )
    c_mat = same_cluster[["mean_velocity", "pagerank_score", "severe_rate"]].to_numpy(dtype=float)
    scales = np.maximum(np.std(c_mat, axis=0), 1e-3)
    dists = np.linalg.norm((c_mat - q_vec) / scales, axis=1)
    same_cluster["similarity_score"] = np.round(1.0 / (1.0 + dists), 4)
    return same_cluster.sort_values("similarity_score", ascending=False).head(limit)


def _select_forecast_candidates(
    data: pd.DataFrame,
    street_filter: str = "Tất cả tuyến đường",
    include_single_obs: bool = True,
    max_rows: int = 1500,
) -> pd.DataFrame:
    subset = data
    if street_filter and street_filter != "Tất cả tuyến đường":
        subset = subset[subset["street_name"].fillna("") == street_filter]
    if subset.empty:
        return subset.copy()
    if not include_single_obs:
        with_next = subset[subset["target_next_los_ordinal"].notna()]
        if not with_next.empty:
            subset = with_next
    return subset.head(max_rows).copy()


def _try_query_live_mongo(mongo_uri: str, limit: int) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    try:
        import pymongo  # type: ignore[import-not-found]

        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=800)
        coll = client["bda501"]["traffic_scores"]
        total_docs = int(coll.count_documents({}))
        if total_docs > 0:
            cursor = (
                coll.find(
                    {"street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}},
                    {"_id": 0},
                )
                .sort([("severe_prob", -1), ("segment_id", 1)])
                .limit(limit)
            )
            docs = list(cursor)
            status_counts = {
                str(r["_id"]): int(r["cnt"])
                for r in coll.aggregate([{"$group": {"_id": "$lifecycle_status", "cnt": {"$sum": 1}}}])
            }
            client.close()
            return pd.DataFrame(docs), {
                "serving_mode": "LIVE_MONGODB_8.0",
                "mongo_uri": mongo_uri,
                "total_serving_docs": total_docs,
                "lifecycle_breakdown": status_counts,
            }
        client.close()
    except Exception:
        pass
    return None


def query_operational_serving_store(limit: int = 25) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Query live MongoDB 8.0 `bda501.traffic_scores` if available; otherwise explicit offline fallback."""
    env_uri = os.environ.get("MONGO_URI", "").strip()
    candidate_uris = [env_uri] if env_uri else ["mongodb://127.0.0.1:27017", "mongodb://mongo:27017"]
    for uri in candidate_uris:
        live_res = _try_query_live_mongo(uri, limit)
        if live_res is not None:
            return live_res
    fallback_uri = candidate_uris[0]

    if SERVING_PARQUET_PATH.is_file():
        s_df = pd.read_parquet(SERVING_PARQUET_PATH)
        hits = (
            s_df[(s_df["street_level"].isin([1, 2])) & (s_df["severe_prob"] >= 0.70)]
            .sort_values(["severe_prob", "segment_id"], ascending=[False, True])
            .head(limit)
        )
        status_counts = {str(k): int(v) for k, v in s_df["lifecycle_status"].value_counts().items()}
        return hits, {
            "serving_mode": "OFFLINE_PARQUET_FALLBACK",
            "mongo_uri": fallback_uri,
            "total_serving_docs": int(len(s_df)),
            "lifecycle_breakdown": status_counts,
        }

    mongo_verify = _read_optional_json(MONGO_VERIFY_PATH)
    alerts = mongo_verify.get("uc2_arterial_query", {}).get("top_5_arterial_alerts", [])
    return pd.DataFrame(alerts), {
        "serving_mode": "OFFLINE_JSON_FALLBACK",
        "mongo_uri": fallback_uri,
        "total_serving_docs": int(mongo_verify.get("total_rows_upserted", 0)),
        "lifecycle_breakdown": {},
    }


_fetch_live_mongo_status = query_operational_serving_store


def _try_query_live_kafka_mongo(mongo_uri: str, limit: int) -> tuple[dict[str, Any], pd.DataFrame] | None:
    try:
        import pymongo  # type: ignore[import-not-found]

        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=800)
        db = client["bda501"]
        tel = db["live_stream_telemetry"].find_one({"_id": "current"}, {"_id": 0})
        if tel is not None:
            docs = list(
                db["live_stream_scores"].find({}, {"_id": 0}).sort([("seq_id", -1), ("severe_prob", -1)]).limit(limit)
            )
            client.close()
            if not docs and int(tel.get("sink2_scored_v1", 0)) > 0 and LIVE_SCORES_PATH.is_file():
                docs = json.loads(LIVE_SCORES_PATH.read_text(encoding="utf-8"))[:limit]
            return dict(tel), pd.DataFrame(docs)
        client.close()
    except Exception:
        pass
    return None


def query_live_kafka_stream_monitor(limit: int = 15) -> tuple[dict[str, Any], pd.DataFrame]:
    """Read live Kafka consumer telemetry & provisional v1 rows from MongoDB 8.0 or file fallback."""
    env_uri = os.environ.get("MONGO_URI", "").strip()
    uris = [env_uri] if env_uri else ["mongodb://127.0.0.1:27017", "mongodb://mongo:27017"]
    for uri in uris:
        res = _try_query_live_kafka_mongo(uri, limit)
        if res is not None:
            return res
    tel = _read_optional_json(LIVE_TELEMETRY_PATH)
    rows: list[dict[str, Any]] = []
    if LIVE_SCORES_PATH.is_file():
        try:
            rows = json.loads(LIVE_SCORES_PATH.read_text(encoding="utf-8"))[:limit]
        except Exception:
            rows = []
    return tel, pd.DataFrame(rows)


__all__ = [
    "ABLATION_PATH",
    "ARCH_IMG_PATH",
    "DATASET_PATH",
    "DATASET_SAMPLE_PATH",
    "HOTSPOTS_PATH",
    "INVENTORY_PATH",
    "JOIN_PLAN_PATH",
    "LAGGED_RULES_PATH",
    "LIVE_SCORES_PATH",
    "LIVE_TELEMETRY_PATH",
    "LOS_ORDER",
    "MANIFEST_PATH",
    "METRICS_PATH",
    "MODEL_PATH",
    "MONGO_VERIFY_PATH",
    "MONOID_VERIFY_PATH",
    "ORDINAL_TO_LOS",
    "PAGERANK_PATH",
    "QUARANTINE_PATH",
    "QUERY_PROGRESS_PATH",
    "RECON_PATH",
    "ROC_PATH",
    "ROOT",
    "RULES_PATH",
    "SERVING_PARQUET_PATH",
    "SPARK_VERIFY_PATH",
    "STATIC_SAMPLE_PATH",
    "STATIC_STORE_PATH",
    "TRANSITION_CSV_PATH",
    "_derive_ict_date_series",
    "_fetch_live_mongo_status",
    "_find_similar_segments",
    "_format_alerts_display",
    "_format_hotspots_display",
    "_format_lifecycle_summary",
    "_format_live_stream_display",
    "_format_peers_display",
    "_format_transition_display",
    "_format_vn_time",
    "_predict_single_window",
    "_read_optional_csv",
    "_read_optional_json",
    "_select_forecast_candidates",
    "_utc_to_ict_date",
    "build_window_semantics",
    "query_live_kafka_stream_monitor",
    "query_operational_serving_store",
]
