from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, TypeVar

import joblib
import numpy as np
import pandas as pd

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover
    st = None  # type: ignore[assignment]

from pipeline import find_peer_segments
from toc_backend.story_scenarios import build_window_semantics

_F = TypeVar("_F", bound=Callable[..., Any])


def _cache_data(ttl: int | None = None) -> Callable[[_F], _F]:
    if st is not None:
        return st.cache_data(ttl=ttl) if ttl is not None else st.cache_data  # type: ignore[return-value]
    return lambda fn: fn


def _cache_resource() -> Callable[[_F], _F]:
    if st is not None:
        return st.cache_resource  # type: ignore[return-value]
    return lambda fn: fn


_REPLAY_LOCK = threading.Lock()
_ACTIVE_REPLAY_PROC: subprocess.Popen[bytes] | None = None

ROOT = Path(__file__).resolve().parent
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
MAP_HOTSPOTS_PATH = ROOT / "outputs" / "slide_charts" / "map_hcmc_hotspots_coldstart.png"
MAP_CLUSTERS_PATH = ROOT / "outputs" / "slide_charts" / "map_hcmc_clusters_7d.png"
TRANSITION_CHART_PATH = ROOT / "outputs" / "slide_charts" / "chart_transition_analysis.png"
LIVE_TELEMETRY_PATH = ROOT / "outputs" / "live_kafka_telemetry.json"
LIVE_SCORES_PATH = ROOT / "outputs" / "live_kafka_scores.json"

LOS_ORDER = ["A", "B", "C", "D", "E", "F"]
ORDINAL_TO_LOS = {index: label for index, label in enumerate(LOS_ORDER)}


@_cache_data(ttl=30)
def _load_csv_cached(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    return pd.read_csv(path) if path.is_file() else pd.DataFrame()


@_cache_data(ttl=30)
def _load_json_cached(path_str: str) -> dict[str, Any]:
    path = Path(path_str)
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


@_cache_data(ttl=300)
def _load_dataset_cached(path_str: str) -> pd.DataFrame:
    return pd.read_csv(path_str)


@_cache_data(ttl=300)
def _load_static_parquet_cached(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    return pd.read_parquet(path) if path.is_file() else pd.DataFrame()


@_cache_resource()
def _load_model_artifact(model_path_str: str) -> dict[str, Any]:
    return joblib.load(model_path_str)


@_cache_resource()
def _get_toc_store(root_str: str) -> Any:
    from toc_backend.data_store import TOCDataStore

    return TOCDataStore(root=Path(root_str))


def _read_optional_csv(path: Path) -> pd.DataFrame:
    return _load_csv_cached(str(path))


def _read_optional_json(path: Path) -> dict[str, Any]:
    return _load_json_cached(str(path))


def _format_vn_time(vn_hour_val: Any) -> str:
    if pd.isna(vn_hour_val):
        return "N/A"
    total_minutes = int(round(float(vn_hour_val) * 60)) % (24 * 60)
    hh = total_minutes // 60
    mm = total_minutes % 60
    return f"{hh:02d}:{mm:02d} ICT"


def _predict_single_window(row: pd.Series, artifact: dict[str, Any]) -> dict[str, Any]:
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
    rf_change_prob = float(1.0 - probs_6[persistence_ord]) if 0 <= persistence_ord < 6 else 0.0
    alt_probs = probs_6.copy()
    if 0 <= persistence_ord < 6:
        alt_probs[persistence_ord] = -1.0
    change_threshold = float(artifact.get("val_selected_change_threshold", 0.30))
    cal_ord = int(alt_probs.argmax()) if rf_change_prob >= change_threshold else persistence_ord

    return {
        "persistence_los": ORDINAL_TO_LOS.get(persistence_ord, "A"),
        "dt_los": ORDINAL_TO_LOS.get(dt_pred_ord, "A"),
        "rf_los": ORDINAL_TO_LOS.get(rf_pred_ord, "A"),
        "rf_calibrated_los": ORDINAL_TO_LOS.get(cal_ord, "A"),
        "rf_change_prob": rf_change_prob,
        "rf_severe_prob": rf_severe_prob,
        "gbt_severe_prob": gbt_severe_prob,
        "actual_next_los": (
            ORDINAL_TO_LOS.get(int(row["target_next_los_ordinal"]), "N/A")
            if pd.notna(row.get("target_next_los_ordinal"))
            else "N/A (Không có cặp +30m kế tiếp)"
        ),
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


def _render_dataframe(df: pd.DataFrame) -> None:
    try:
        st.dataframe(df, width="stretch", hide_index=True)
    except TypeError:  # pragma: no cover
        st.dataframe(df, use_container_width=True, hide_index=True)


def _find_similar_segments(
    data: pd.DataFrame,
    query_row: pd.Series | int,
    limit: int = 10,
    static_store_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Find Top-K peers in 7D K-Means space across all 84,206 motorized segments (including Cold-Start)."""
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


def _stop_active_replay_locked() -> None:
    global _ACTIVE_REPLAY_PROC
    proc = _ACTIVE_REPLAY_PROC
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
    _ACTIVE_REPLAY_PROC = None


def _trigger_live_kafka_replay_async(mongo_uri: str) -> None:
    """Stop active replay, reset Bloom/HLL++ state, and launch 200-msg Kafka producer safely under lock."""
    global _ACTIVE_REPLAY_PROC
    from live_kafka_stream import reset_live_stream_state

    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    with _REPLAY_LOCK:
        _stop_active_replay_locked()
        reset_live_stream_state(
            mongo_uri=mongo_uri,
            telemetry_path=LIVE_TELEMETRY_PATH,
            scores_json_path=LIVE_SCORES_PATH,
        )
        _ACTIVE_REPLAY_PROC = subprocess.Popen(
            [
                sys.executable,
                "producer.py",
                "--mode",
                "window",
                "--bootstrap-servers",
                brokers,
                "--topic",
                "hcmc-traffic-events",
                "--max-messages",
                "200",
                "--replay-speed",
                "0.25",
            ],
            cwd=str(Path(__file__).resolve().parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def render_live_kafka_monitor_section() -> None:
    """Render interactive Live Kafka -> 33 KiB Sketches + RF 23D -> MongoDB 8.0 monitor."""
    import time
    from live_kafka_stream import reset_live_stream_state

    st.subheader("2B. 📡 Live Kafka Stream Monitor (Kafka 4.1 → 33 KiB Sketches + RF 23D → MongoDB 8.0)")
    c_poll, c_play, c_reset, c_info = st.columns([1.15, 1.35, 1.1, 1.8])
    if "live_auto_poll" not in st.session_state:
        st.session_state["live_auto_poll"] = False
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
    if c_play.button("▶️ Phát 200 tin Kafka"):
        _trigger_live_kafka_replay_async(mongo_uri)
        st.session_state["live_auto_poll"] = True
        st.rerun()
    if c_reset.button("🧹 Reset bộ đếm về 0"):
        with _REPLAY_LOCK:
            _stop_active_replay_locked()
            reset_live_stream_state(
                mongo_uri=mongo_uri, telemetry_path=LIVE_TELEMETRY_PATH, scores_json_path=LIVE_SCORES_PATH
            )
        st.rerun()
    auto_poll = c_poll.checkbox("⚡ Tự động cập nhật (2s)", key="live_auto_poll")

    tel, live_df = query_live_kafka_stream_monitor(limit=15)
    status_lbl = tel.get("status", "CHƯA CHẠY LUỒNG LIVE")
    parts = tel.get("partition_counts", {"0": 0, "1": 0, "2": 0})
    c_info.caption(
        f"**Trạng thái:** `{status_lbl}` · **Topic:** `{tel.get('topic', 'hcmc-traffic-events')}` · "
        f"**Cập nhật:** `{tel.get('last_updated_ict', 'N/A')} ICT`"
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(
        "Tin nhận từ Kafka (3 Partitions)",
        f"{int(tel.get('messages_consumed', 0)):,}",
        f"P0:{parts.get('0', 0)} · P1:{parts.get('1', 0)} · P2:{parts.get('2', 0)}",
    )
    k2.metric(
        "Bloom 32 KiB Chặn trùng",
        f"{int(tel.get('duplicates_caught_bloom', 0)):,} tin trùng",
        f"Trễ >30p (chờ Tier-2): {int(tel.get('late_dropped_for_tier2', 0)):,}",
    )
    k3.metric(
        "HLL++ 1 KiB Đếm đoạn đường",
        f"{float(tel.get('hll_estimated_segments', 0.0)):.0f} đoạn",
        f"Thực tế: {int(tel.get('exact_distinct_segments', 0)):,} đoạn",
    )
    k4.metric(
        "Cảnh báo v1 ghi MongoDB",
        f"{int(tel.get('sink2_scored_v1', 0)):,} bản ghi",
        (
            f"E/F: {int(tel.get('severe_alerts_v1_count', 0)):,} · "
            f"Khóa v2: {int(tel.get('v2_guard_protected_count', 0)):,}"
        ),
    )

    if not live_df.empty:
        st.caption(
            f"🟢 **Bảng dữ liệu thời gian thực từ `MongoDB 8.0 (bda501.live_stream_scores)`** — "
            f"Hiển thị **{len(live_df)}** bản ghi dự báo `+30 phút` mới nhất vừa đi qua `Kafka` & `33 KiB Sketches`:"
        )
        _render_dataframe(_format_live_stream_display(live_df))
    else:
        st.info(
            "Bộ đếm đang sẵn sàng ở `0`. Bấm nút **▶️ Phát ngay 200 tin qua Kafka** ở trên "
            "(hoặc chạy lệnh `python producer.py ...` trong Terminal) để xem luồng dự báo nhảy trực tiếp!"
        )

    with st.expander(
        "📋 Lệnh chạy nhanh Live Demo Real-Time (Producer → Kafka 3 Partitions → Consumer → MongoDB → UI)"
    ):
        st.code(
            "# Bước 1: Khởi động Kafka + MongoDB + Live Consumer + Web UI\n"
            "docker compose up -d kafka mongo app toc-web live-consumer\n\n"
            "# Bước 2: Phát 200 sự kiện từ dữ liệu dự án vào Kafka (tốc độ trực quan ~8 giây)\n"
            "python producer.py --mode window --bootstrap-servers localhost:9092 "
            "--topic hcmc-traffic-events --max-messages 200 --replay-speed 0.2",
            language="powershell",
        )
    if auto_poll:
        time.sleep(2.0)
        st.rerun()


def render_overview_tab(data: pd.DataFrame, metrics: dict[str, Any]) -> None:
    """Tab 1: HCMC Map, Hotspots, Quality Funnel & Architecture Overview."""
    st.info(
        "**Thang đo mức phục vụ giao thông (LOS A–F):** "
        "**LOS A–B** (≥ 65% tốc độ tối đa): Thông thoáng tự do · "
        "**LOS C–D** (35%–65%): Đông đúc ổn định · "
        "**LOS E–F** (< 35%): Ùn ứ nghiêm trọng đến kẹt cứng."
    )
    m_left, m_right = st.columns([1.22, 1.0])
    total_segs = int(metrics.get("dataset", {}).get("segments_count", 0))
    with m_left:
        st.subheader(f"1. Bản đồ Điểm nóng & Vùng thiếu GPS TP.HCM ({total_segs:,} Đoạn đường)")
        if MAP_HOTSPOTS_PATH.is_file():
            st.image(str(MAP_HOTSPOTS_PATH))
    with m_right:
        st.subheader("2. Các Đoạn Đường Đang Kẹt Dai Dẳng Nhất (EWMA α = 0.15)")
        hotspots_df = _read_optional_csv(HOTSPOTS_PATH)
        _render_dataframe(_format_hotspots_display(hotspots_df.head(14)))

    st.subheader("3. Thống kê Làm sạch Dữ liệu & Lưu vết Bản ghi Lỗi")
    q_df = _read_optional_csv(QUARANTINE_PATH)
    if not q_df.empty:
        _render_dataframe(q_df)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("4. Phân bố 6 Mức Giao thông (LOS A–F)")
        los_dist = (
            data.groupby("LOS")
            .size()
            .reindex(LOS_ORDER, fill_value=0)
            .rename("Số khung quan sát (30 phút)")
            .reset_index()
        )
        st.bar_chart(los_dist, x="LOS", y="Số khung quan sát (30 phút)")
    with c2:
        st.subheader("5. Kiểm kê 5 Bảng Dữ liệu Gốc TP.HCM")
        inv_df = _read_optional_csv(INVENTORY_PATH)
        _render_dataframe(inv_df)

    if ARCH_IMG_PATH.is_file():
        st.subheader("6. Sơ đồ Kiến trúc Tổng thể 4 Bước Vận hành")
        st.image(str(ARCH_IMG_PATH), caption="Kiến trúc Hợp nhất Xử lý Lô & Luồng Trực tuyến (10.5 GB RAM)")


def _find_default_case_position(sample_rows: pd.DataFrame, idx_list: list[Any]) -> int:
    """Locate Case Study #21064 on 2021-04-22 (11:30 ICT) by column values instead of hardcoded index."""
    date_col = "ict_date" if "ict_date" in sample_rows.columns else "date"
    case_mask = (sample_rows["segment_id"].astype(int) == 21064) & (
        sample_rows[date_col].astype(str) == "2021-04-22"
    )
    if "vn_hour" in sample_rows.columns:
        case_mask = case_mask & np.isclose(sample_rows["vn_hour"].fillna(-1).astype(float), 11.5, atol=0.05)
    matches = sample_rows.index[case_mask]
    if len(matches) == 0:
        matches = sample_rows.index[sample_rows["segment_id"].astype(int) == 21064]
    return idx_list.index(matches[0]) if len(matches) > 0 else 0


def _format_dropdown_option(sample_rows: pd.DataFrame, idx: Any) -> str:
    r = sample_rows.loc[idx]
    vn_h = float(r.get("vn_hour", 7.0)) if pd.notna(r.get("vn_hour")) else 7.0
    ict_d = str(r.get("ict_date") or _utc_to_ict_date(r.get("date"), r.get("period")))
    sem = build_window_semantics(ict_d, vn_h)
    return (
        f"Đoạn {int(r['segment_id'])} · {r['street_name'] or 'N/A'} · "
        f"{sem['collect_window']} ({ict_d} ➔ {sem['issue_time_ict']}) · Hiện tại: LOS {r['LOS']}"
    )


def _render_time_locked_alerts(row: pd.Series) -> None:
    """Render time-locked Early-Warning alerts at exact `(ict_date, vn_hour)` without 10-month mixing."""
    store = _get_toc_store(str(ROOT))
    ict_d = str(row.get("ict_date") or _utc_to_ict_date(row.get("date"), row.get("period")) or "2021-01-05")
    vn_h = float(row.get("vn_hour", 12.0)) if pd.notna(row.get("vn_hour")) else 12.0
    frame = store.get_city_frame(ict_date=ict_d, vn_hour=vn_h, alert_filter="early_warning")
    sem = frame["window_semantics"]
    worsening_n = frame["summary"]["early_warning_worsening_count"]
    st.caption(
        f"⏱️ **Đang khóa đúng thời điểm điều hành:** Thu thập `{sem['collect_window']}` (`{sem['ict_date']}`) ➔ "
        f"Chốt **`t = {sem['issue_time_ict']}`** ➔ "
        f"Dự báo cho `{sem['forecast_window']}` (**{worsening_n}** cảnh báo sắp xấu đi)"
    )
    if frame["alerts"]:
        disp = pd.DataFrame(
            [
                {
                    "Mã đoạn": a["segment_id"],
                    "Tuyến đường": a["street_name"],
                    "Vận tốc (km/h)": f"{a['mean_velocity']} / {int(a['max_velocity'])}",
                    "Hiện tại": a["current_los"],
                    "RF (Argmax)": a["rf_pred_los"],
                    "RF (Hiệu chỉnh)": a.get("effective_forecast_los", a["rf_cal_los"]),
                    "Thực tế +30p": a["actual_next_los"] or "—",
                    "Đổi pha (%)": round(a["change_prob"] * 100.0, 1),
                    "Nguy cơ E/F (%)": round(a["severe_prob"] * 100.0, 1),
                    "Phân loại": a["alert_type"],
                    "Nguồn": a["lifecycle_status"],
                }
                for a in frame["alerts"][:12]
            ]
        )
        _render_dataframe(disp)


def _render_forecast_header_card(row: pd.Series, pred: dict[str, Any]) -> None:
    """Render the 6-column KPI header and ICT window summary card for Tab 2."""
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Hiện tại (t)", str(row["LOS"]))
    m2.metric("Đoán theo Hiện tại", pred["persistence_los"])
    m3.metric("RF (Argmax mặc định)", pred["rf_los"])
    m4.metric(
        "RF (Hiệu chỉnh chuyển pha)",
        pred["rf_calibrated_los"],
        f"Đổi pha: {pred['rf_change_prob'] * 100:.1f}%",
    )
    m5.metric("Xác suất Kẹt cứng (E/F)", f"{pred['rf_severe_prob'] * 100:.1f}%")
    m6.metric("Thực tế (t + 30 phút)", pred["actual_next_los"])

    vn_h_num = float(row.get("vn_hour", 7.0)) if pd.notna(row.get("vn_hour")) else 7.0
    ict_d = str(row.get("ict_date") or _utc_to_ict_date(row.get("date"), row.get("period")))
    sem = build_window_semantics(ict_d, vn_h_num)
    st.info(
        f"**Tuyến đường:** {row['street_name']} (mã đoạn `{row['segment_id']}`, cấp {int(row['street_level'])}) · "
        f"**Ngày (ICT):** `{sem['ict_date']}` · **Khung thu thập:** `{sem['collect_window']}` ➔ "
        f"**Chốt `t = {sem['issue_time_ict']}`** ➔ **Dự báo cho `{sem['forecast_window']}`** · "
        f"**Vận tốc:** {row['mean_velocity']:.1f} / {row['max_velocity']:.0f} km/h · "
        f"**Đổi pha:** {pred['rf_change_prob'] * 100:.1f}% · **PageRank:** {row['pagerank_score']:.3f}"
    )


def _render_forecast_alerts_panel(row: pd.Series, metrics: dict[str, Any]) -> None:
    """Render Section 2 time-locked corridor alerts or MongoDB 8.0 IXSCAN serving table."""
    mv = _read_optional_json(MONGO_VERIFY_PATH)
    ds_total = int(metrics.get("dataset", {}).get("labeled_windows_count", mv.get("total_rows_upserted", 0)))
    p95_ms = mv.get("uc2_arterial_query", {}).get("latency_benchmark_ms", {}).get("warm_p95_ms")
    p95_str = f" · p95 = {p95_ms} ms" if p95_ms is not None else ""

    st.subheader("2. Cảnh báo Trục chính Nguy cơ Kẹt cao < 1 ms (Khóa Thời Điểm t & MongoDB 8.0)")
    alert_view_mode = st.radio(
        "Chế độ xem bảng cảnh báo:",
        [
            "🎯 Khóa đúng khung giờ t đang chọn & ưu tiên đường sắp đổi trạng thái (Phòng điều hành TOC)",
            f"⚡ Truy vấn Benchmark MongoDB 8.0 IXSCAN toàn cục ({ds_total:,} bản ghi{p95_str})",
        ],
        horizontal=True,
    )
    if alert_view_mode.startswith("🎯"):
        _render_time_locked_alerts(row)
        return
    r_col1, r_col2 = st.columns([1, 3])
    if r_col1.button("🔄 Làm mới dữ liệu trực tuyến (5s)"):
        st.rerun()
    alerts_df, serving_meta = query_operational_serving_store(limit=25)
    lifecycle_txt = _format_lifecycle_summary(serving_meta.get("lifecycle_breakdown", {}))
    r_col2.caption(
        f"**Nguồn phục vụ:** `{serving_meta['serving_mode']}` · "
        f"**Tổng bản ghi:** `{serving_meta['total_serving_docs']:,}` · "
        f"**Trạng thái đối soát:** {lifecycle_txt}"
    )
    if not alerts_df.empty:
        _render_dataframe(_format_alerts_display(alerts_df.head(10)))


def render_forecast_tab(data: pd.DataFrame, artifact: dict[str, Any], metrics: dict[str, Any]) -> None:
    """Tab 2: Live Congestion Forecast (t+30m), MongoDB 8.0 Alerts & State-Change Evaluation."""
    st.caption(
        "💡 **Giao diện Phòng Điều Hành TOC thế hệ mới (ReactJS + FastAPI + Bản đồ động & Biểu đồ GPS thô ➔ t+30p):** "
        "Mở trực tiếp tại <http://localhost:8000>"
    )
    st.subheader("1. Dự báo Trạng thái Giao thông Trước 30 Phút (t + 30 phút)")
    streets = ["Tất cả tuyến đường"] + sorted(data["street_name"].dropna().astype(str).unique().tolist())
    f1, f2 = st.columns([2, 2])
    street_choice = f1.selectbox("Lọc theo Tuyến đường", streets)
    include_sparse = f2.checkbox(
        "Bao gồm khung cuối chuỗi (chưa có quan sát kế tiếp đúng +30 phút)",
        value=False,
    )
    sample_rows = _select_forecast_candidates(
        data, street_filter=street_choice, include_single_obs=include_sparse, max_rows=35000
    )
    if sample_rows.empty:
        st.warning("Không tìm thấy khung quan sát phù hợp với bộ lọc hiện tại.")
        return

    idx_list = sample_rows.index.tolist()
    default_pos = _find_default_case_position(sample_rows, idx_list)
    selected_idx = st.selectbox(
        "Chọn đoạn đường & khung 30 phút",
        idx_list,
        index=default_pos,
        format_func=lambda i: _format_dropdown_option(sample_rows, i),
    )
    row = sample_rows.loc[selected_idx]
    pred = _predict_single_window(row, artifact)
    _render_forecast_header_card(row, pred)
    _render_forecast_alerts_panel(row, metrics)

    render_live_kafka_monitor_section()

    st.subheader("3. Bảng Thực nghiệm Mô hình & Kiểm định Khi Giao thông Đổi Trạng Thái")
    ablation_df = _read_optional_csv(ABLATION_PATH)
    _render_dataframe(ablation_df)
    trans_df = _read_optional_csv(TRANSITION_CSV_PATH)
    if not trans_df.empty:
        _render_dataframe(_format_transition_display(trans_df))
    col_left, col_right = st.columns(2)
    with col_left:
        if TRANSITION_CHART_PATH.is_file():
            st.image(str(TRANSITION_CHART_PATH))
    with col_right:
        if ROC_PATH.is_file():
            st.image(str(ROC_PATH), caption="Đường cong ROC dự báo ùn tắc nghiêm trọng (LOS E/F đúng t+30 phút)")


def render_graph_and_rules_tab() -> None:
    """Tab 3: Road Line-Graph PageRank & SON / FP-Growth Co-Congestion + Lagged Rules."""
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("1. Top Nút cổ chai Giao lộ Toàn Đô thị (PageRank)")
        st.caption(
            "Xếp hạng 84,206 đoạn đường cơ giới (92,453 hướng rẽ có hướng không quay đầu, bù 314 đoạn cụt, "
            "đã lọc bỏ 525,786 điểm uốn cong trung gian trên bản đồ)."
        )
        pr_df = _read_optional_csv(PAGERANK_PATH)
        _render_dataframe(pr_df.head(25))
    with c2:
        st.subheader("2. Các Cặp Tuyến Đường Hay Kẹt Cùng Lúc (SON / FPGrowth)")
        st.caption(
            "Khai phá trên toàn bộ giỏ tuyến đường chuẩn hóa dấu tiếng Việt NFC đang kẹt cứng (LOS E/F) mỗi 30 phút."
        )
        rules_df = _read_optional_csv(RULES_PATH)
        _render_dataframe(rules_df.head(25))

    lagged_df = _read_optional_csv(LAGGED_RULES_PATH)
    if not lagged_df.empty:
        st.subheader("3. Giả thuyết Lan truyền Kẹt xe Sau 30 Phút (Lưu ý cỡ mẫu nhỏ: 5–8 khung)")
        _render_dataframe(lagged_df)


def render_kmeans_and_cold_start_tab(data: pd.DataFrame, metrics: dict[str, Any]) -> None:
    """Tab 4: Train-Only 7D K-Means & Peer Search across all network segments."""
    st_path = STATIC_STORE_PATH if STATIC_STORE_PATH.is_file() else STATIC_SAMPLE_PATH
    static_df = _load_static_parquet_cached(str(st_path))
    total_segs = int(metrics.get("dataset", {}).get("segments_count", len(static_df)))
    k_left, k_right = st.columns([1.22, 1.0])
    with k_left:
        st.subheader(f"1. Bản đồ 6 Nhóm Hành vi Phủ Kín {total_segs:,} Đoạn Đường")
        if MAP_CLUSTERS_PATH.is_file():
            st.image(str(MAP_CLUSTERS_PATH))
    with k_right:
        st.subheader("2. Thống kê 6 Nhóm Đường Phố Toàn Mạng Lưới")
        if not static_df.empty:
            cluster_counts = (
                static_df.groupby("cluster_id")
                .agg(
                    total_segments=("segment_id", "count"),
                    gps_segments=("is_cold_start", lambda s: int((~s).sum())),
                    cold_start_segments=("is_cold_start", "sum"),
                    avg_velocity_kmh=("mean_velocity", "mean"),
                    pagerank_score=("pagerank_score", "mean"),
                )
                .round({"avg_velocity_kmh": 1, "pagerank_score": 2})
                .reset_index()
                .rename(
                    columns={
                        "cluster_id": "Nhóm",
                        "total_segments": "Tổng đoạn",
                        "gps_segments": "Có GPS",
                        "cold_start_segments": "Thiếu GPS",
                        "avg_velocity_kmh": "Vận tốc (km/h)",
                        "pagerank_score": "PageRank",
                    }
                )
            )
            _render_dataframe(cluster_counts)

    if static_df.empty:
        return
    obs_cnt = int((~static_df["is_cold_start"]).sum())
    cold_cnt = int(static_df["is_cold_start"].sum())
    total_static = max(len(static_df), 1)
    obs_pct = obs_cnt / total_static * 100.0
    cold_pct = cold_cnt / total_static * 100.0

    st.subheader(f"3. Tra cứu Đoạn Đường Tương Đồng cho Vùng Thiếu GPS ({cold_cnt:,} Đoạn)")
    st.caption(
        f"Tổng cộng: {len(static_df):,} đoạn đường · Đã có GPS: {obs_cnt:,} đoạn ({obs_pct:.2f}%) · "
        f"Thiếu GPS nhưng đã có điểm PageRank & nhóm hành vi K-Means: {cold_cnt:,} đoạn ({cold_pct:.2f}%)"
    )
    s_col1, s_col2, s_col3 = st.columns([2, 2, 1])
    only_cold = s_col1.checkbox("Chỉ chọn đoạn đường chưa từng có dữ liệu GPS", value=True)
    candidate_pool = static_df[static_df["is_cold_start"]] if only_cold else static_df
    candidate_pool = candidate_pool.sort_values("pagerank_score", ascending=False).head(500).copy()
    candidate_pool["label"] = [
        f"Đoạn {int(r.segment_id)} · {r.street_name or 'N/A'} (Cấp {int(r.street_level)}, "
        f"PageRank={r.pagerank_score:.2f}, Nhóm #{int(r.cluster_id)})"
        for r in candidate_pool.itertuples()
    ]
    chosen_seg_id = s_col2.selectbox(
        "Chọn đoạn đường cần tham chiếu hành vi",
        candidate_pool["segment_id"].astype(int).tolist(),
        format_func=lambda sid: candidate_pool.loc[candidate_pool["segment_id"] == sid, "label"].iloc[0],
    )
    peer_limit = s_col3.slider("Số đoạn tương đồng", 5, 25, 8)
    peers_df = _find_similar_segments(data, int(chosen_seg_id), limit=peer_limit, static_store_df=static_df)
    _render_dataframe(_format_peers_display(peers_df))


def render_streaming_and_explain_tab(metrics: dict[str, Any]) -> None:
    """Tab 5: Stream Sketches (32 KiB Bloom, 1 KiB HLL++), Two-Tier Recon & Runtime Explain."""
    s_metrics = metrics["streaming_mmds"]
    bf = s_metrics["bloom_filter"]
    hll = s_metrics["hyperloglog"]
    recon = s_metrics["reconciliation"]

    st.subheader("1. Bộ Đếm Luồng Gọn Nhẹ 33.0 KiB (Bloom 32.0 KiB & HyperLogLog++ 1.0 KiB)")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Tổng sự kiện luồng xử lý", f"{s_metrics['total_stream_events_processed']:,}")
    k2.metric(
        "Bộ nhớ lọc trùng Bloom",
        f"{bf['memory_kb']:.1f} KiB ({bf.get('bit_packed_uint8_bytes', 32768):,} B)",
        f"Đã chặn trùng: {s_metrics['duplicates_removed_exact']:,}",
    )
    k3.metric(
        "Tỷ lệ báo trùng nhầm (FPR)",
        f"{bf['empirical_fp_rate']:.5f}",
        f"Lý thuyết: {bf['theoretical_fp_rate']:.5f}",
    )
    k4.metric(
        "Ước lượng HyperLogLog++",
        f"{hll['hll_estimated_segments']:.0f} đoạn",
        f"Sai số: {hll['relative_error_pct']:.2f}% (Thực tế: {hll['exact_distinct_segments']:,})",
    )

    st.subheader("2. Đối Soát Cuối Ngày & Bù Dữ Liệu Đến Trễ")
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Khớp đúng hạn (≤ 30 phút)", f"{recon['in_watermark_like_for_like_match_pct']:.1f}%")
    late_win_cnt = recon.get("affected_late_segment_windows_recomputed", 501)
    healed_lag_cnt = recon.get("healed_downstream_lag_windows", 0)
    r2.metric("Khung trễ bù / Cập nhật sau", f"{late_win_cnt:,} / {healed_lag_cnt:,}")
    r3.metric("Độ đầy đủ trước đối soát", f"{recon['pre_reconciliation_completeness_pct']:.2f}%")
    r4.metric("Độ đầy đủ sau đối soát", f"{recon['post_reconciliation_completeness_pct']:.1f}%")

    recon_df = _read_optional_csv(RECON_PATH)
    _render_dataframe(recon_df)

    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("**Bằng chứng thực thi phân tán (`Spark 3.5.7` & `BroadcastHashJoin`):**")
        st.json(
            {
                "join_plan_evidence": _read_optional_json(JOIN_PLAN_PATH),
                "spark_runtime_evidence": _read_optional_json(SPARK_VERIFY_PATH),
            }
        )
    with c_right:
        st.markdown("**Bằng chứng phục vụ trực tuyến (`MongoDB 8.0 IXSCAN` & `Nhật ký WAL`):**")
        st.json(
            {
                "mongo_ixscan": _read_optional_json(MONGO_VERIFY_PATH).get("ixscan_explain_verification", {}),
                "wal_recovery": _read_optional_json(QUERY_PROGRESS_PATH),
            }
        )


def main() -> None:
    st.set_page_config(
        page_title="HCMC Smart Traffic Platform — BDA501",
        page_icon="🚦",
        layout="wide",
    )
    st.title("🚦 Hệ thống Giám sát Hiện trạng & Dự báo Ùn tắc Giao thông TP.HCM")

    ds_active_path = DATASET_PATH if DATASET_PATH.is_file() else DATASET_SAMPLE_PATH
    if not all(path.is_file() for path in (ds_active_path, METRICS_PATH, MODEL_PATH)):
        st.error(
            "Chưa tìm thấy dữ liệu/mô hình đã xử lý. Hãy chạy `python pipeline.py` "
            "hoặc `docker compose run --rm pipeline`, rồi tải lại trang."
        )
        st.stop()

    data = _load_dataset_cached(str(ds_active_path))
    metrics = _load_json_cached(str(METRICS_PATH))
    artifact = _load_model_artifact(str(MODEL_PATH))

    cold_pct_val = metrics.get("static_feature_store_c13b", {}).get("cold_start_ratio_pct")
    cold_pct_txt = f"{float(cold_pct_val):.2f}% " if cold_pct_val is not None else ""
    st.caption(
        "Luồng 4 bước khép kín: Làm sạch GPS & gom khung 30 phút (Spark 3.5.7) · "
        f"Lấp vùng mù {cold_pct_txt}thiếu GPS (PageRank & K-Means) · "
        "Dự báo trước 30 phút (Random Forest 23 biến) · "
        "Cảnh báo trực tuyến < 1 ms & tự động bù tin trễ (Kafka 4.1 · MongoDB 8.0)"
    )

    ds_info = metrics["dataset"]
    eval_info = metrics["evaluation"]
    top1, top2, top3, top4, top5 = st.columns(5)
    active_nodes = ds_info.get("active_intersection_nodes_count", ds_info["nodes_count"])
    exact_t30m_cnt = ds_info.get("exact_t30m_supervised_windows_count", 7898)
    top1.metric(
        "Giao lộ thực (Pruned)",
        f"{active_nodes:,}",
        f"Lọc từ {ds_info['nodes_count']:,} điểm",
    )
    top2.metric("Đoạn đường một chiều", f"{ds_info['segments_count']:,}")
    top3.metric("Bản tin vận tốc GPS", f"{ds_info['segment_status_events']:,}")
    top4.metric("Khung 30p / Cặp +30p", f"{ds_info['labeled_windows_count']:,} / {exact_t30m_cnt:,}")
    top5.metric("PR-AUC / F1 (+30 phút)", f"{eval_info['average_precision']:.4f} / {eval_info['severe_f1']:.4f}")

    tabs = st.tabs(
        [
            "🗺️ Tab 1: Bản đồ Điểm nóng & Dữ liệu",
            "🚦 Tab 2: Dự báo +30 Phút & MongoDB",
            "🕸️ Tab 3: Nút cổ chai & Cặp đường hay kẹt",
            "🧭 Tab 4: 6 Nhóm Đường & Vùng thiếu GPS",
            "⚡ Tab 5: Luồng 33 KiB & Đối soát tin trễ",
        ]
    )
    with tabs[0]:
        render_overview_tab(data, metrics)
    with tabs[1]:
        render_forecast_tab(data, artifact, metrics)
    with tabs[2]:
        render_graph_and_rules_tab()
    with tabs[3]:
        render_kmeans_and_cold_start_tab(data, metrics)
    with tabs[4]:
        render_streaming_and_explain_tab(metrics)


if __name__ == "__main__":
    main()
