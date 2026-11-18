from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover
    st = None  # type: ignore[assignment]

from pipeline import find_peer_segments

ROOT = Path(__file__).resolve().parent
DATASET_PATH = ROOT / "data" / "dataset.csv"
STATIC_STORE_PATH = ROOT / "data" / "lakehouse" / "segment_static_features.parquet"
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

LOS_ORDER = ["A", "B", "C", "D", "E", "F"]
ORDINAL_TO_LOS = {index: label for index, label in enumerate(LOS_ORDER)}


def _read_optional_csv(path: Path) -> pd.DataFrame:
    if path.is_file():
        return pd.read_csv(path)
    return pd.DataFrame()


def _read_optional_json(path: Path) -> dict[str, Any]:
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


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
    cal_ord = int(alt_probs.argmax()) if rf_change_prob >= 0.30 else persistence_ord

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
    if "PROVISIONAL" in breakdown or "RECONCILED" in breakdown:
        return ", ".join(f"{key}: {count}" for key, count in sorted(breakdown.items()))
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
    if "feature_version" in df.columns:
        return df[
            [c for c in ("segment_id", "street_name", "street_level", "predicted_next_los", "severe_prob") if c in df]
        ]
    vn_times = df["vn_hour"].map(_format_vn_time) if "vn_hour" in df.columns else df.get("period", "")
    return pd.DataFrame(
        {
            "Mã đoạn": df["segment_id"].astype(int),
            "Tuyến đường": df["street_name"].fillna("N/A"),
            "Cấp": df["street_level"].astype(int),
            "Ngày": df["date"],
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
    """Find Top-K peers in 7D K-Means space across all 84,633 segments (including Cold-Start)."""
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
        database = os.getenv("TRAFFIC_DATABASE", "bda501")
        coll = client[database]["traffic_scores"]
        run_id = os.getenv("TRAFFIC_RUN_ID", "").strip()
        scope = {"run_id": run_id} if run_id else {}
        total_docs = int(coll.count_documents(scope))
        if total_docs > 0:
            cursor = (
                coll.find(
                    {**scope, "street_level": {"$in": [1, 2]}, "severe_prob": {"$gte": 0.70}},
                    {"_id": 0},
                )
                .sort([("severe_prob", -1), ("segment_id", 1)])
                .limit(limit)
            )
            docs = list(cursor)
            status_counts = {
                str(r["_id"]): int(r["cnt"])
                for r in coll.aggregate(
                    [{"$match": scope}, {"$group": {"_id": "$lifecycle_status", "cnt": {"$sum": 1}}}]
                )
            }
            recent = coll.find_one(
                scope,
                {
                    "_id": 0,
                    "event_id": 1,
                    "run_id": 1,
                    "revision": 1,
                    "issued_at": 1,
                    "target_start": 1,
                    "target_end": 1,
                    "completeness": 1,
                },
                sort=[("issued_at", -1)],
            )
            client.close()
            return pd.DataFrame(docs), {
                "serving_mode": "LIVE_MONGODB_8.0",
                "mongo_uri": mongo_uri,
                "total_serving_docs": total_docs,
                "lifecycle_breakdown": status_counts,
                "database": database,
                "run_id": run_id or "legacy snapshot",
                "source": "historical replay in live MongoDB",
                "latest_replay_score": recent,
            }
        client.close()
    except Exception:
        pass
    return None


def query_operational_serving_store(limit: int = 25) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Query live MongoDB 8.0 `bda501.traffic_scores` if available; otherwise explicit offline fallback."""
    env_uri = os.environ.get("MONGO_URI", "").strip()
    candidate_uris = [env_uri] if env_uri else ["mongodb://127.0.0.1:27017", "mongodb://127.0.0.1:27019"]
    for uri in candidate_uris:
        live_res = _try_query_live_mongo(uri, limit)
        if live_res is not None:
            return live_res
    fallback_uri = candidate_uris[0]
    requested_run = os.getenv("TRAFFIC_RUN_ID", "").strip()
    if requested_run:
        return pd.DataFrame(), {
            "serving_mode": "REQUESTED_REPLAY_RUN_UNAVAILABLE",
            "run_id": requested_run,
            "mongo_uri": fallback_uri,
            "total_serving_docs": 0,
            "lifecycle_breakdown": {},
        }

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


def render_overview_tab(data: pd.DataFrame, metrics: dict[str, Any]) -> None:
    """Tab 1: HCMC Map, Hotspots, Quality Funnel & Architecture Overview."""
    st.info(
        "**Thang đo mức phục vụ giao thông (LOS A–F):** "
        "**LOS A–B** (≥ 65% tốc độ tối đa): Thông thoáng tự do · "
        "**LOS C–D** (35%–65%): Đông đúc ổn định · "
        "**LOS E–F** (< 35%): Ùn ứ nghiêm trọng đến kẹt cứng."
    )
    m_left, m_right = st.columns([1.22, 1.0])
    with m_left:
        st.subheader("1. Bản đồ Điểm nóng & Vùng thiếu GPS TP.HCM (84,633 Đoạn đường)")
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


def render_forecast_tab(data: pd.DataFrame, artifact: dict[str, Any], metrics: dict[str, Any]) -> None:
    """Tab 2: Live Congestion Forecast (t+30m), MongoDB 8.0 Alerts & State-Change Evaluation."""
    st.subheader("1. Dự báo Trạng thái Ở Cửa sổ Kế tiếp (snapshot batch)")
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
    default_pos = idx_list.index(33410) if 33410 in sample_rows.index else 0
    selected_idx = st.selectbox(
        "Chọn đoạn đường & khung 30 phút",
        idx_list,
        index=default_pos,
        format_func=lambda i: (
            f"Đoạn {int(sample_rows.loc[i, 'segment_id'])} · {sample_rows.loc[i, 'street_name'] or 'N/A'} · "
            f"{_format_vn_time(sample_rows.loc[i].get('vn_hour', np.nan))} ({sample_rows.loc[i, 'date']}) · "
            f"Hiện tại: LOS {sample_rows.loc[i, 'LOS']}"
        ),
    )
    row = sample_rows.loc[selected_idx]
    pred = _predict_single_window(row, artifact)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Hiện tại (t)", str(row["LOS"]))
    m2.metric("Đoán theo Hiện tại", pred["persistence_los"])
    m3.metric("Random Forest (23 biến)", pred["rf_los"])
    m4.metric("Xác suất Kẹt cứng (E/F)", f"{pred['rf_severe_prob'] * 100:.1f}%")
    m5.metric("Thực tế (t + 30 phút)", pred["actual_next_los"])

    vn_time_str = _format_vn_time(row.get("vn_hour", np.nan))
    st.info(
        f"**Tuyến đường:** {row['street_name']} (mã đoạn `{row['segment_id']}`, cấp {int(row['street_level'])}) · "
        f"**Giờ Việt Nam:** {vn_time_str} (`{row.get('ict_date', row['date'])}`) · "
        f"**Vận tốc:** {row['mean_velocity']:.1f} / {row['max_velocity']:.0f} km/h · "
        f"**Xác suất đổi trạng thái:** {pred['rf_change_prob'] * 100:.1f}% · "
        f"**PageRank:** {row['pagerank_score']:.3f} · **Nhóm hành vi:** #{int(row['cluster_id'])}"
    )

    st.subheader("2. Cảnh báo Trục chính Nguy cơ Kẹt cao (MongoDB)")
    r_col1, r_col2 = st.columns([1, 3])
    if r_col1.button("🔄 Làm mới dữ liệu"):
        st.rerun()
    alerts_df, serving_meta = query_operational_serving_store(limit=25)
    lifecycle_txt = _format_lifecycle_summary(serving_meta.get("lifecycle_breakdown", {}))
    r_col2.caption(
        f"**Nguồn phục vụ:** `{serving_meta['serving_mode']}` · "
        f"**Tổng bản ghi:** `{serving_meta['total_serving_docs']:,}` · "
        f"**Trạng thái đối soát:** {lifecycle_txt}"
    )
    st.caption(
        f"Run: {serving_meta.get('run_id', 'snapshot')} · "
        "Target là cửa sổ kế tiếp; thời gian báo trước phụ thuộc issued_at thực tế."
    )
    recent = serving_meta.get("latest_replay_score")
    if recent and recent.get("run_id"):
        _render_dataframe(pd.DataFrame([recent]))
        age = (pd.Timestamp.now(tz="UTC") - pd.to_datetime(recent["issued_at"], utc=True)).total_seconds()
        st.caption(f"Freshness của score mới nhất: {age:.1f}s (historical replay)")
    if not alerts_df.empty:
        online_cols = [
            c
            for c in (
                "event_id",
                "run_id",
                "revision",
                "issued_at",
                "input_cutoff",
                "target_start",
                "target_end",
                "completeness",
                "model_version",
            )
            if c in alerts_df
        ]
        if online_cols:
            _render_dataframe(alerts_df[online_cols].head(10))
        if "issued_at" in alerts_df:
            newest = pd.to_datetime(alerts_df["issued_at"], utc=True).max()
            st.caption(f"Freshness từ lần phát gần nhất: {(pd.Timestamp.now(tz='UTC') - newest).total_seconds():.1f}s")
        _render_dataframe(_format_alerts_display(alerts_df.head(10)))

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
            "Xếp hạng 84,633 đoạn đường một chiều (151,929 hướng rẽ hợp lệ, bù 48 đoạn cụt, "
            "đã lọc bỏ 525,542 điểm uốn cong trung gian trên bản đồ)."
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
    """Tab 4: Train-Only 7D K-Means & Peer Search across all 84,633 segments."""
    static_df = pd.read_parquet(STATIC_STORE_PATH) if STATIC_STORE_PATH.is_file() else pd.DataFrame()
    k_left, k_right = st.columns([1.22, 1.0])
    with k_left:
        st.subheader("1. Bản đồ 6 Nhóm Hành vi Phủ Kín 84,633 Đoạn Đường")
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

    st.subheader("3. Tra cứu Đoạn Đường Tương Đồng cho Vùng Thiếu GPS (74,606 Đoạn)")
    st.caption(
        f"Tổng cộng: {len(static_df):,} đoạn đường · Đã có GPS: {obs_cnt:,} đoạn (11.85%) · "
        f"Thiếu GPS nhưng đã có điểm PageRank & nhóm hành vi K-Means: {cold_cnt:,} đoạn (88.15%)"
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
    st.caption(
        "Luồng 4 bước khép kín: Làm sạch GPS & gom khung 30 phút (Spark 3.5.7) · "
        "Lấp vùng mù 88.15% thiếu GPS (PageRank & K-Means) · "
        "Dự báo trước 30 phút (Random Forest 23 biến) · "
        "Historical replay qua Kafka và MongoDB; thời gian báo trước phụ thuộc lúc phát dự báo"
    )
    st.info(
        "Số liệu mô hình, bản đồ và ví dụ batch là snapshot lịch sử. Bảng vận hành trong Tab 2 "
        "đọc đúng replay run được chọn từ MongoDB. Slot kế tiếp không bảo đảm 30 phút cảnh báo trước."
    )

    if not all(path.is_file() for path in (DATASET_PATH, METRICS_PATH, MODEL_PATH)):
        st.error(
            "Chưa tìm thấy dữ liệu/mô hình đã xử lý. Hãy chạy `python pipeline.py` "
            "hoặc `docker compose run --rm pipeline`, rồi tải lại trang."
        )
        st.stop()

    data = pd.read_csv(DATASET_PATH)
    metrics = json.loads(METRICS_PATH.read_text(encoding="utf-8"))
    artifact = joblib.load(MODEL_PATH)

    ds_info = metrics["dataset"]
    eval_info = metrics["evaluation"]
    top1, top2, top3, top4, top5 = st.columns(5)
    active_nodes = ds_info.get("active_intersection_nodes_count", ds_info["nodes_count"])
    exact_t30m_cnt = ds_info.get("exact_t30m_supervised_windows_count", 7898)
    top1.metric("Giao lộ thực / Tổng điểm", f"{active_nodes:,} / {ds_info['nodes_count']:,}")
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
            "⚡ Tab 5: Sketch 33 KiB & Replay lịch sử",
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
