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
ARCH_IMG_PATH = ROOT / "outputs" / "system_architecture_v4.png"
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
SPARK_VERIFY_PATH = ROOT / "outputs" / "spark_verification.json"
QUERY_PROGRESS_PATH = ROOT / "outputs" / "query_progress.json"
INVENTORY_PATH = ROOT / "outputs" / "file_inventory.csv"

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
    frame_row = pd.DataFrame([{col: row.get(col, 0.0) for col in features}])
    if "current_los_ordinal" not in frame_row or pd.isna(frame_row["current_los_ordinal"].iloc[0]):
        frame_row["current_los_ordinal"] = row.get("los_ordinal", 0)
    matrix = artifact["ml_imputer"].transform(frame_row[features])

    rf_pred_ord = int(artifact["rf_multiclass"].predict(matrix)[0])
    dt_pred_ord = int(artifact["dt_multiclass"].predict(matrix)[0])
    rf_severe_prob = float(artifact["rf_multiclass"].predict_proba(matrix)[0, 4:].sum())
    gbt_severe_prob = float(artifact["gbt_severe"].predict_proba(matrix)[0, 1])
    persistence_ord = int(row.get("los_ordinal", 0))

    return {
        "persistence_los": ORDINAL_TO_LOS.get(persistence_ord, "A"),
        "dt_los": ORDINAL_TO_LOS.get(dt_pred_ord, "A"),
        "rf_los": ORDINAL_TO_LOS.get(rf_pred_ord, "A"),
        "rf_severe_prob": rf_severe_prob,
        "gbt_severe_prob": gbt_severe_prob,
        "actual_next_los": (
            ORDINAL_TO_LOS.get(int(row["target_next_los_ordinal"]), "N/A")
            if pd.notna(row.get("target_next_los_ordinal"))
            else "N/A (Không có cặp +30m kế tiếp)"
        ),
    }


def _find_similar_segments(
    data: pd.DataFrame,
    query_row: pd.Series | int,
    limit: int = 10,
    static_store_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """[D07/M14] Find Top-K peers in 7D K-Means space across all 84,633 segments (incl. Cold-Start)."""
    query_seg_id = int(query_row) if isinstance(query_row, (int, np.integer)) else int(query_row["segment_id"])
    if static_store_df is None and STATIC_STORE_PATH.is_file():
        static_store_df = pd.read_parquet(STATIC_STORE_PATH)
    if static_store_df is not None and not static_store_df.empty:
        peers = find_peer_segments(static_store_df, query_seg_id, limit=limit, prefer_observed_peers=True)
        if not peers.empty:
            return peers

    if isinstance(query_row, (int, np.integer)):
        return pd.DataFrame()
    seg_summary = (
        data.groupby("segment_id", as_index=False)
        .agg(
            street_name=("street_name", "first"),
            street_type=("street_type", "first"),
            street_level=("street_level", "first"),
            mean_velocity=("mean_velocity", "mean"),
            max_velocity=("max_velocity", "first"),
            pagerank_score=("pagerank_score", "first"),
            severe_rate=("is_severe", "mean"),
            cluster_id=("cluster_id", "first"),
        )
    )
    same_cluster = seg_summary[
        (seg_summary["cluster_id"] == int(query_row["cluster_id"]))
        & (seg_summary["segment_id"] != query_seg_id)
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
    same_cluster["Điểm tương đồng"] = np.round(1.0 / (1.0 + dists), 4)
    return same_cluster.sort_values("Điểm tương đồng", ascending=False).head(limit)


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


def query_operational_serving_store(limit: int = 25) -> tuple[pd.DataFrame, dict[str, Any]]:
    """[M13] Query live MongoDB 8.0 `bda501.traffic_scores` if available; otherwise explicit offline fallback."""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27019")
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

    if SERVING_PARQUET_PATH.is_file():
        s_df = pd.read_parquet(SERVING_PARQUET_PATH)
        hits = (
            s_df[(s_df["street_level"].isin([1, 2])) & (s_df["severe_prob"] >= 0.70)]
            .sort_values(["severe_prob", "segment_id"], ascending=[False, True])
            .head(limit)
        )
        status_counts = {
            str(k): int(v) for k, v in s_df["lifecycle_status"].value_counts().items()
        }
        return hits, {
            "serving_mode": "OFFLINE_PARQUET_FALLBACK",
            "mongo_uri": mongo_uri,
            "total_serving_docs": int(len(s_df)),
            "lifecycle_breakdown": status_counts,
        }

    mongo_verify = _read_optional_json(MONGO_VERIFY_PATH)
    alerts = mongo_verify.get("uc2_arterial_query", {}).get("top_5_arterial_alerts", [])
    return pd.DataFrame(alerts), {
        "serving_mode": "OFFLINE_JSON_FALLBACK",
        "mongo_uri": mongo_uri,
        "total_serving_docs": int(mongo_verify.get("total_rows_upserted", 0)),
        "lifecycle_breakdown": {},
    }


def render_overview_tab(data: pd.DataFrame, metrics: dict[str, Any]) -> None:
    """Tab 1: Overview, Quality Funnel [C5]/[C15] & Exponentially Decaying Hotspots [C7]."""
    st.subheader("1. Phễu Kiểm định Chất lượng [C5] & Kho Cách ly [C15] Quarantine (Ordered First-Failing Rule)")
    q_df = _read_optional_csv(QUARANTINE_PATH)
    if not q_df.empty:
        st.dataframe(q_df, width="stretch", hide_index=True)

    st.subheader("2. Cảnh báo Điểm nóng Ùn tắc Kéo dài (UC-1 — Exponentially Decaying Windows, α = 0.15)")
    hotspots_df = _read_optional_csv(HOTSPOTS_PATH)
    st.dataframe(hotspots_df, width="stretch", hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("3. Phân bố Nhãn Mức độ Phục vụ (Reference LOS vs. Derived LOS)")
        los_dist = (
            data.groupby("LOS")
            .size()
            .reindex(LOS_ORDER, fill_value=0)
            .rename("Số khung quan sát (30 phút)")
            .reset_index()
        )
        st.bar_chart(los_dist, x="LOS", y="Số khung quan sát (30 phút)")
    with c2:
        st.subheader("4. Kiểm kê 5 Bảng Dữ liệu Giao thông TP.HCM (Kaggle Primary Source)")
        inv_df = _read_optional_csv(INVENTORY_PATH)
        st.dataframe(inv_df, width="stretch", hide_index=True)

    if ARCH_IMG_PATH.is_file():
        st.subheader("5. Kiến trúc Giải pháp Tổng thể V3 (Zones A–F · 17 Thành phần [C1]–[C17])")
        st.image(str(ARCH_IMG_PATH), caption="HCMC Smart Traffic Platform — V3 Reference Architecture")


def render_forecast_tab(data: pd.DataFrame, artifact: dict[str, Any], metrics: dict[str, Any]) -> None:
    """Tab 2: Live Congestion Forecast (t+30m), Validation-Gate Ablation & MongoDB 8.0 IXSCAN (<10ms)."""
    st.subheader("1. Dự báo LOS Khung kế tiếp Đúng +30 Phút (t + 30m) & Validation Gate (UC-2)")
    streets = ["Tất cả tuyến đường"] + sorted(
        data["street_name"].dropna().astype(str).unique().tolist()
    )
    f1, f2 = st.columns([2, 2])
    street_choice = f1.selectbox("Lọc theo Tuyến đường (bao gồm tuyến cực thưa 1 quan sát)", streets)
    include_sparse = f2.checkbox(
        "Bao gồm cửa sổ cuối chuỗi / không có cặp liên tiếp +30m (EC-5)",
        value=True,
    )
    sample_rows = _select_forecast_candidates(
        data, street_filter=street_choice, include_single_obs=include_sparse
    )
    if sample_rows.empty:
        st.warning("Không tìm thấy cửa sổ quan sát phù hợp với bộ lọc hiện tại.")
        return

    sample_rows["label"] = [
        f"Segment {r.segment_id} · {r.street_name or 'N/A'} · {r.date} {r.period} "
        f"({_format_vn_time(getattr(r, 'vn_hour', np.nan))} | LOS hiện tại: {r.LOS})"
        for r in sample_rows.itertuples()
    ]
    selected_idx = st.selectbox(
        "Chọn quan sát đoạn đường (Segment & Khung 30 phút)",
        sample_rows.index.tolist(),
        format_func=lambda idx: sample_rows.loc[idx, "label"],
    )
    row = sample_rows.loc[selected_idx]
    pred = _predict_single_window(row, artifact)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("LOS Hiện tại (t)", str(row["LOS"]))
    m2.metric("Persistence (t+30m)", pred["persistence_los"])
    m3.metric("Random Forest (+Graph)", pred["rf_los"])
    m4.metric("Xác suất Kẹt nặng (E/F)", f"{pred['rf_severe_prob'] * 100:.1f}%")
    m5.metric("LOS Thực tế (t+30m)", pred["actual_next_los"])

    vn_time_str = _format_vn_time(row.get("vn_hour", np.nan))
    st.info(
        f"**Tuyến đường:** {row['street_name']} (`segment_id={row['segment_id']}`, cấp {int(row['street_level'])}) · "
        f"**Giờ địa phương:** {vn_time_str} (`ict_date={row.get('ict_date', row['date'])}`) · "
        f"**Vận tốc:** {row['mean_velocity']:.1f}/{row['max_velocity']:.0f} km/h · "
        f"**Line-Graph PageRank:** {row['pagerank_score']:.3f} · **Cụm 7D K-Means:** #{int(row['cluster_id'])}"
    )

    st.subheader("2. Truy vấn Trục đường chính Kẹt nặng < 10ms (UC-2 — MongoDB 8.0 Compound Index IXSCAN)")
    r_col1, r_col2 = st.columns([1, 3])
    if r_col1.button("🔄 Làm mới Serving Store (5s Micro-Batch)"):
        st.rerun()
    alerts_df, serving_meta = query_operational_serving_store(limit=25)
    r_col2.caption(
        f"**Chế độ kết nối:** `{serving_meta['serving_mode']}` · "
        f"**Tổng số bản ghi Serving:** `{serving_meta['total_serving_docs']:,}` · "
        f"**Phân bố vòng đời (`lifecycle_status`):** `{serving_meta['lifecycle_breakdown']}`"
    )
    if not alerts_df.empty:
        st.dataframe(alerts_df, width="stretch", hide_index=True)

    st.subheader("3. Bảng Đối chứng Ablation Study & Validation-Gate Selection ([C11] 70/15/15 Split)")
    ablation_df = _read_optional_csv(ABLATION_PATH)
    st.dataframe(ablation_df, width="stretch", hide_index=True)
    col_left, col_right = st.columns(2)
    with col_left:
        if ROC_PATH.is_file():
            st.image(str(ROC_PATH), caption="Đường cong ROC dự báo ùn tắc nghiêm trọng (LOS E/F đúng t+30m)")
    with col_right:
        st.json(_read_optional_json(MANIFEST_PATH))


def render_graph_and_rules_tab() -> None:
    """Tab 3: Directed Line-Graph PageRank & SON / FP-Growth Co-Congestion + Lagged Rules."""
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("1. Top Điểm nghẽn Cấu trúc (UC-3 — Directed Line-Graph PageRank, MMDS Ch. 5)")
        st.caption(
            "Xếp hạng 84,633 đoạn đường (151,929 cạnh chuyển tiếp, bù 48 Dangling Nodes, "
            "cắt tỉa 525,542 đỉnh OSM mồ côi)."
        )
        pr_df = _read_optional_csv(PAGERANK_PATH)
        st.dataframe(pr_df.head(25), width="stretch", hide_index=True)
    with c2:
        st.subheader("2. Luật Đồng Kẹt xe Có hướng A ⇒ B & B ⇒ A (UC-4 — 2-Pass SON / FP-Growth)")
        st.caption(
            "Giỏ hàng (date, period) đầy đủ không cắt ngọn (untruncated), sinh luật có hướng 2 chiều "
            "trên các tuyến đường chuẩn hóa Unicode NFC đang kẹt nặng LOS ∈ {E, F}."
        )
        rules_df = _read_optional_csv(RULES_PATH)
        st.dataframe(rules_df.head(25), width="stretch", hide_index=True)

    lagged_df = _read_optional_csv(LAGGED_RULES_PATH)
    if not lagged_df.empty:
        st.subheader("3. Luật Lan truyền Kẹt xe Trễ Thời gian: Street_A(t) ⇒ Street_B(t + 30m)")
        st.dataframe(lagged_df, width="stretch", hide_index=True)


def render_kmeans_and_cold_start_tab(data: pd.DataFrame, metrics: dict[str, Any]) -> None:
    """Tab 4: Train-Only 7D K-Means & Cold-Start Peer Search across all 84,633 segments."""
    st.subheader("1. Phân cụm Hành vi Tuyến đường 7 Chiều (UC-5 — Train-Only 7D K-Means, MMDS Ch. 7)")
    cluster_counts = (
        data.groupby("cluster_id")
        .agg(
            observations=("event_id", "count"),
            unique_segments=("segment_id", "nunique"),
            severe_ef_rate=("is_severe", "mean"),
            avg_velocity_kmh=("mean_velocity", "mean"),
        )
        .reset_index()
    )
    st.dataframe(cluster_counts, width="stretch", hide_index=True)

    if not STATIC_STORE_PATH.is_file():
        return
    static_df = pd.read_parquet(STATIC_STORE_PATH)
    obs_cnt = int((~static_df["is_cold_start"]).sum())
    cold_cnt = int(static_df["is_cold_start"].sum())

    st.subheader("2. Tìm Đoạn đường Tương đồng trong Không gian 7D K-Means (Hỗ trợ cả Cold-Start 84,633 Segments)")
    st.caption(
        f"Tổng số đoạn đường: {len(static_df):,} · Đã có GPS: {obs_cnt:,} (11.85%) · "
        f"Vùng mù Cold-Start được phủ bởi Line-Graph PageRank & 7D K-Means: {cold_cnt:,} (88.15%)"
    )
    s_col1, s_col2, s_col3 = st.columns([2, 2, 1])
    only_cold = s_col1.checkbox("Chỉ chọn đoạn đường Vùng mù Cảm biến (Cold-Start: obs_windows = 0)", value=True)
    candidate_pool = static_df[static_df["is_cold_start"]] if only_cold else static_df
    candidate_pool = candidate_pool.sort_values("pagerank_score", ascending=False).head(500).copy()
    candidate_pool["label"] = [
        f"Segment {int(r.segment_id)} · {r.street_name or 'N/A'} (Cấp {int(r.street_level)}, "
        f"PageRank={r.pagerank_score:.2f}, Cụm #{int(r.cluster_id)}, ColdStart={bool(r.is_cold_start)})"
        for r in candidate_pool.itertuples()
    ]
    chosen_seg_id = s_col2.selectbox(
        "Chọn đoạn đường cần tìm Peer (Cold-Start hoặc Active)",
        candidate_pool["segment_id"].astype(int).tolist(),
        format_func=lambda sid: candidate_pool.loc[candidate_pool["segment_id"] == sid, "label"].iloc[0],
    )
    peer_limit = s_col3.slider("Top-K Peers", 5, 25, 10)
    peers_df = _find_similar_segments(data, int(chosen_seg_id), limit=peer_limit, static_store_df=static_df)
    st.dataframe(peers_df, width="stretch", hide_index=True)
    st.json(metrics["model"])


def render_streaming_and_explain_tab(metrics: dict[str, Any]) -> None:
    """Tab 5: Stream Sketches (32 KiB Bit-Packed Bloom, 1 KiB HLL++), Two-Tier Recon & Runtime Explain."""
    s_metrics = metrics["streaming_mmds"]
    bf = s_metrics["bloom_filter"]
    hll = s_metrics["hyperloglog"]
    recon = s_metrics["reconciliation"]

    st.subheader("1. Xử lý Luồng Bộ nhớ Giới hạn O(1) ([C7] 32.0 KiB Bit-Packed Bloom & 1.0 KiB HLL++)")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sự kiện luồng xử lý", f"{s_metrics['total_stream_events_processed']:,}")
    k2.metric(
        "Bộ nhớ Bit-Packed Bloom",
        f"{bf['memory_kb']:.1f} KiB ({bf.get('bit_packed_uint8_bytes', 32768):,} B)",
        f"Trùng lặp chặn: {s_metrics['duplicates_removed_exact']:,}",
    )
    k3.metric(
        "Bloom Filter FP Rate",
        f"{bf['empirical_fp_rate']:.5f}",
        f"Lý thuyết: {bf['theoretical_fp_rate']:.5f}",
    )
    k4.metric(
        "HyperLogLog Distinct",
        f"{hll['hll_estimated_segments']:.0f} tuyến",
        f"Sai số: {hll['relative_error_pct']:.2f}% (Thực tế: {hll['exact_distinct_segments']:,})",
    )

    st.subheader("2. Đối soát Hai Tầng Batch – Streaming ([C12] Two-Tier Reconciliation & Version Guard)")
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Tier 1: Khớp trong Watermark", f"{recon['in_watermark_like_for_like_match_pct']:.1f}%")
    late_win_cnt = recon.get("affected_late_segment_windows_recomputed", 501)
    healed_lag_cnt = recon.get("healed_downstream_lag_windows", 0)
    r2.metric("Bản tin trễ / Cửa sổ Lag được bù", f"{late_win_cnt:,} / {healed_lag_cnt:,}")
    r3.metric("Độ đầy đủ Pre-Recon", f"{recon['pre_reconciliation_completeness_pct']:.2f}%")
    r4.metric("Độ đầy đủ Post-Recon", f"{recon['post_reconciliation_completeness_pct']:.1f}%")

    recon_df = _read_optional_csv(RECON_PATH)
    st.dataframe(recon_df, width="stretch", hide_index=True)

    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("**Bằng chứng Pillar I1 & I2 (`Commutative Monoid`, `BroadcastHashJoin` & `Spark Plan`):**")
        st.json(_read_optional_json(JOIN_PLAN_PATH))
    with c_right:
        st.markdown("**Bằng chứng Pillar I3 & S7 (`MongoDB 8.0 IXSCAN` & `WAL Recovery`):**")
        st.json(
            {
                "mongo_ixscan": _read_optional_json(MONGO_VERIFY_PATH).get("ixscan_explain_verification", {}),
                "wal_recovery": _read_optional_json(QUERY_PROGRESS_PATH),
            }
        )


def main() -> None:
    st.set_page_config(
        page_title="HCMC Smart Traffic Analytics & Congestion Prediction (V3)",
        page_icon="🚦",
        layout="wide",
    )
    st.title("🚦 HCMC Smart Traffic Analytics & Congestion Prediction Platform (V3)")
    st.caption(
        "Zones A–F ([C1]–[C17]) · Zero-Conflict 3-Phase MLOps · Medallion Parquet Lakehouse · "
        "MMDS Stream Sketching (32 KiB Bit-Packed Bloom, 1 KiB HLL++, Decaying) · Line-Graph PageRank · "
        "2-Pass SON/FP-Growth · Train-Only 7D K-Means · Leak-Free Causal +30m ML · MongoDB 8.0 IXSCAN"
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
    top1.metric("Nút giao Hoạt động / Thô", f"{active_nodes:,} / {ds_info['nodes_count']:,}")
    top2.metric("Đoạn đường (Segments)", f"{ds_info['segments_count']:,}")
    top3.metric("Bản tin Vận tốc GPS", f"{ds_info['segment_status_events']:,}")
    top4.metric("Khung 30 phút / Cặp +30m", f"{ds_info['labeled_windows_count']:,} / {exact_t30m_cnt:,}")
    top5.metric("Severe PR-AUC / F1 (+30m)", f"{eval_info['average_precision']:.4f} / {eval_info['severe_f1']:.4f}")

    tabs = st.tabs(
        [
            "🗂️ Tab 1: Overview, Quality Funnel & Hotspots",
            "🚦 Tab 2: Live Forecast (+30m), Ablation & Mongo IXSCAN",
            "🕸️ Tab 3: Line-Graph PageRank & SON/FP-Growth Rules",
            "🧭 Tab 4: 7D K-Means & Cold-Start Peer Search",
            "⚡ Tab 5: Stream Sketches, Two-Tier Recon & Explain Plans",
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
