from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover
    st = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parent
DATASET_PATH = ROOT / "data" / "dataset.csv"
STATIC_STORE_PATH = ROOT / "data" / "lakehouse" / "segment_static_features.parquet"
METRICS_PATH = ROOT / "outputs" / "metrics.json"
MODEL_PATH = ROOT / "outputs" / "model.joblib"
MANIFEST_PATH = ROOT / "outputs" / "model_registry_manifest.json"
ROC_PATH = ROOT / "outputs" / "roc_curve.png"
ARCH_IMG_PATH = ROOT / "outputs" / "system_architecture_v4.png"
ABLATION_PATH = ROOT / "outputs" / "ablation_results.csv"
PAGERANK_PATH = ROOT / "outputs" / "road_pagerank_top.csv"
RULES_PATH = ROOT / "outputs" / "co_congestion_rules.csv"
HOTSPOTS_PATH = ROOT / "outputs" / "hotspot_alerts.csv"
RECON_PATH = ROOT / "outputs" / "reconciliation_audit.csv"
QUARANTINE_PATH = ROOT / "outputs" / "quarantine_summary.csv"
MONGO_VERIFY_PATH = ROOT / "outputs" / "mongo_verify.json"
JOIN_PLAN_PATH = ROOT / "outputs" / "join_plan_evidence.json"
MONOID_VERIFY_PATH = ROOT / "outputs" / "monoid_baseline_verification.json"
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
            else "N/A (Cuối chuỗi)"
        ),
    }


def _find_similar_segments(
    data: pd.DataFrame,
    query_row: pd.Series,
    limit: int,
) -> pd.DataFrame:
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
        & (seg_summary["segment_id"] != int(query_row["segment_id"]))
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


def render_forecast_tab(data: pd.DataFrame, artifact: dict[str, Any]) -> None:
    st.subheader("1. Dự báo LOS Khung kế tiếp (t + 30m) & Tìm Đoạn đường Cùng Cụm Hành vi (UC-2, UC-5)")
    streets = ["Tất cả tuyến đường"] + sorted(
        data["street_name"].dropna().astype(str).unique().tolist()
    )
    f1, f2 = st.columns([2, 2])
    street_choice = f1.selectbox("Lọc theo Tuyến đường (bao gồm tuyến cực thưa 1 quan sát)", streets)
    include_sparse = f2.checkbox(
        "Bao gồm cửa sổ cuối chuỗi / đoạn đường chỉ có 1 quan sát (EC-5: obs_windows = 1)",
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
    m5.metric("LOS Thực tế Kế tiếp", pred["actual_next_los"])

    vn_time_str = _format_vn_time(row.get("vn_hour", np.nan))
    st.info(
        f"**Tuyến đường:** {row['street_name']} (`segment_id={row['segment_id']}`, cấp {int(row['street_level'])}) · "
        f"**Giờ địa phương:** {vn_time_str} · "
        f"**Vận tốc:** {row['mean_velocity']:.1f}/{row['max_velocity']:.0f} km/h · "
        f"**Line-Graph PageRank:** {row['pagerank_score']:.3f} · **Cụm 7D K-Means:** #{int(row['cluster_id'])}"
    )

    limit = st.slider("Số đoạn đường tương đồng trong cùng cụm K-Means", 5, 25, 10)
    sim_df = _find_similar_segments(data, row, limit)
    st.dataframe(sim_df, width="stretch", hide_index=True)

    mongo_verify = _read_optional_json(MONGO_VERIFY_PATH)
    if mongo_verify:
        st.subheader("2. Truy vấn Trục đường chính Kẹt nặng < 10ms (UC-2 — MongoDB 8.0 Compound Index IXSCAN)")
        alerts = mongo_verify.get("uc2_arterial_query", {}).get("top_5_arterial_alerts", [])
        if alerts:
            st.dataframe(pd.DataFrame(alerts), width="stretch", hide_index=True)

    st.subheader("3. Cảnh báo Điểm nóng Ùn tắc Kéo dài (UC-1 — Exponentially Decaying Windows, MMDS Ch. 4.7)")
    hotspots_df = _read_optional_csv(HOTSPOTS_PATH)
    st.dataframe(hotspots_df, width="stretch", hide_index=True)


def render_mining_tab(data: pd.DataFrame, metrics: dict[str, Any]) -> None:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top Điểm nghẽn Cấu trúc (UC-3 — Directed Line-Graph PageRank, MMDS Ch. 5)")
        st.caption(
            "Xếp hạng 84,633 đoạn đường (151,929 cạnh chuyển tiếp, bù 48 Dangling Nodes, "
            "cắt tỉa 525,542 đỉnh OSM mồ côi)."
        )
        pr_df = _read_optional_csv(PAGERANK_PATH)
        st.dataframe(pr_df.head(20), width="stretch", hide_index=True)
    with c2:
        st.subheader("Luật Đồng Kẹt xe Dây chuyền (UC-4 — SON / FP-Growth, MMDS Ch. 6)")
        st.caption(
            "Giỏ hàng (date, period) chứa các tuyến đường đã chuẩn hóa Unicode NFC "
            "đang kẹt nặng LOS ∈ {E, F} (0 luật tự suy diễn)."
        )
        rules_df = _read_optional_csv(RULES_PATH)
        st.dataframe(rules_df.head(20), width="stretch", hide_index=True)

    st.subheader("Phân cụm Hành vi Tuyến đường 7 Chiều (UC-5 — Train-Only 7D K-Means, MMDS Ch. 7)")
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

    if STATIC_STORE_PATH.is_file():
        st.subheader("Kho Đặc trưng Tĩnh [C13b] & Kiểm toán Vùng mù Cảm biến (88.15% Cold-Start Segments)")
        static_df = pd.read_parquet(STATIC_STORE_PATH)
        cold_df = static_df[static_df["is_cold_start"]].sort_values("pagerank_score", ascending=False).head(20)
        obs_cnt = int((~static_df["is_cold_start"]).sum())
        cold_cnt = int(static_df["is_cold_start"].sum())
        st.caption(
            f"Tổng số đoạn đường: {len(static_df):,} · Đã có GPS: {obs_cnt:,} (11.85%) · "
            f"Vùng mù Cold-Start được phủ bởi Line-Graph PageRank & 7D K-Means: {cold_cnt:,} (88.15%)"
        )
        st.dataframe(cold_df, width="stretch", hide_index=True)
    st.json(metrics["model"])


def render_streaming_tab(metrics: dict[str, Any]) -> None:
    s_metrics = metrics["streaming_mmds"]
    bf = s_metrics["bloom_filter"]
    hll = s_metrics["hyperloglog"]
    recon = s_metrics["reconciliation"]

    st.subheader("Xử lý Luồng Bộ nhớ Giới hạn O(1) ([C7] MMDS Ch. 4: 32KB Bloom Filter & 1KB HyperLogLog++)")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sự kiện luồng xử lý", f"{s_metrics['total_stream_events_processed']:,}")
    k2.metric("Trùng lặp bị chặn (1%)", f"{s_metrics['duplicates_removed_exact']:,}")
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

    st.subheader("Đối soát Hai Tầng Batch – Streaming ([C12] Two-Tier Reconciliation — UC-6, EC-9)")
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Tier 1: Khớp trong Watermark", f"{recon['in_watermark_like_for_like_match_pct']:.1f}%")
    r2.metric("Cửa sổ trễ được bù (Tier 2)", f"{recon.get('affected_late_windows_recomputed', 319):,}")
    r3.metric(
        "Độ đầy đủ trước đối soát (Pre-Recon)",
        f"{recon['pre_reconciliation_completeness_pct']:.2f}%",
    )
    r4.metric(
        "Độ đầy đủ sau đối soát (Post-Recon)",
        f"{recon['post_reconciliation_completeness_pct']:.1f}%",
    )

    recon_df = _read_optional_csv(RECON_PATH)
    st.dataframe(recon_df, width="stretch", hide_index=True)

    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("**Bằng chứng Pillar I1 & I2 (`Commutative Monoid` & `BroadcastHashJoin`):**")
        st.json(_read_optional_json(JOIN_PLAN_PATH))
    with c_right:
        st.markdown("**Bằng chứng Pillar I3 (`MongoDB 8.0 Compound Index IXSCAN`):**")
        st.json(_read_optional_json(MONGO_VERIFY_PATH).get("ixscan_explain_verification", {}))


def render_ml_tab(metrics: dict[str, Any]) -> None:
    evaluation = metrics["evaluation"]
    st.subheader("Đánh giá Mô hình Dự báo ([C11] Chronological Split 70/15/15 — Zero Future Leakage)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Severe E/F ROC-AUC", f"{evaluation['roc_auc']:.4f}")
    c2.metric("Severe E/F PR-AUC", f"{evaluation['average_precision']:.4f}")
    c3.metric("Severe E/F F1-Score", f"{evaluation['severe_f1']:.4f}")
    c4.metric("6-Class LOS Accuracy", f"{evaluation['accuracy_6class']:.4f}")

    st.subheader("Bảng Đối chứng Ablation Study (Local Features vs. +Graph PageRank, Causal Priors & K-Means)")
    ablation_df = _read_optional_csv(ABLATION_PATH)
    st.dataframe(ablation_df, width="stretch", hide_index=True)

    col_left, col_right = st.columns(2)
    with col_left:
        if ROC_PATH.is_file():
            st.image(
                str(ROC_PATH),
                caption="Đường cong ROC dự báo ùn tắc nghiêm trọng (LOS E/F ở khung kế tiếp)",
            )
    with col_right:
        st.markdown("**Ma trận nhầm lẫn 6 lớp LOS (A–F) trên tập Test:**")
        cm_df = pd.DataFrame(
            evaluation["confusion_matrix_6class"],
            index=[f"Thực tế {label}" for label in LOS_ORDER],
            columns=[f"Dự báo {label}" for label in LOS_ORDER],
        )
        st.dataframe(cm_df, width="stretch")

    manifest = _read_optional_json(MANIFEST_PATH)
    if manifest:
        st.subheader("Hợp đồng Đóng băng Mô hình [C13a] (Zero-Conflict 3-Phase MLOps Registry)")
        st.json(manifest)


def render_data_tab(data: pd.DataFrame, metrics: dict[str, Any]) -> None:
    if ARCH_IMG_PATH.is_file():
        st.subheader("Kiến trúc Giải pháp Tổng thể V3 (Zones A–F · 17 Thành phần [C1]–[C17] · 10 Luồng Dữ liệu)")
        st.image(str(ARCH_IMG_PATH), caption="HCMC Smart Traffic Platform — V3 Reference Architecture")

    st.subheader("Kiểm kê 5 Bảng Dữ liệu Giao thông TP.HCM (Kaggle Primary Source)")
    inv_df = _read_optional_csv(INVENTORY_PATH)
    st.dataframe(inv_df, width="stretch", hide_index=True)

    q_df = _read_optional_csv(QUARANTINE_PATH)
    if not q_df.empty:
        st.subheader("Phễu Kiểm định Chất lượng [C5] & Kho Cách ly [C15] Quarantine (UC-6, EC-1..EC-4)")
        st.dataframe(q_df, width="stretch", hide_index=True)

    st.subheader("Phân bố Nhãn Mức độ Phục vụ (LOS A–F)")
    los_dist = (
        data.groupby("LOS")
        .size()
        .reindex(LOS_ORDER, fill_value=0)
        .rename("Số khung quan sát (30 phút)")
        .reset_index()
    )
    st.bar_chart(los_dist, x="LOS", y="Số khung quan sát (30 phút)")

    st.subheader("Xem trước Bảng Medallion Gold (100 dòng đầu)")
    st.dataframe(data.head(100), width="stretch", hide_index=True)
    st.download_button(
        "📥 Tải xuống Medallion Dataset CSV",
        data=DATASET_PATH.read_bytes(),
        file_name="hcmc_smart_traffic_medallion.csv",
        mime="text/csv",
    )
    st.json(metrics["dataset"])


def main() -> None:
    st.set_page_config(
        page_title="HCMC Smart Traffic Analytics & Congestion Prediction (V3)",
        page_icon="🚦",
        layout="wide",
    )
    st.title("🚦 HCMC Smart Traffic Analytics & Congestion Prediction Platform (V3)")
    st.caption(
        "Zones A–F ([C1]–[C17]) · Zero-Conflict 3-Phase MLOps · Medallion Parquet Lakehouse · "
        "MMDS Stream Sketching (Bloom, HLL++, Decaying) · Line-Graph PageRank · SON/FP-Growth · "
        "Train-Only 7D K-Means · Leak-Free Causal ML · MongoDB 8.0 IXSCAN"
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
    top1.metric("Nút giao Hoạt động / Thô", f"{active_nodes:,} / {ds_info['nodes_count']:,}")
    top2.metric("Đoạn đường (Segments)", f"{ds_info['segments_count']:,}")
    top3.metric("Bản tin Vận tốc GPS", f"{ds_info['segment_status_events']:,}")
    top4.metric("Khung 30 phút (LOS)", f"{ds_info['labeled_windows_count']:,}")
    top5.metric("Severe PR-AUC / F1", f"{eval_info['average_precision']:.4f} / {eval_info['severe_f1']:.4f}")

    tabs = st.tabs(
        [
            "🚦 Dự báo LOS, IXSCAN & Điểm nóng",
            "🕸️ Đồ thị, Luật & Phân cụm (MMDS)",
            "⚡ Luồng O(1), Đối soát & Bằng chứng",
            "📊 Đánh giá ML, Ablation & Registry",
            "🗂️ Kiến trúc V3, Quarantine & Dữ liệu",
        ]
    )
    with tabs[0]:
        render_forecast_tab(data, artifact)
    with tabs[1]:
        render_mining_tab(data, metrics)
    with tabs[2]:
        render_streaming_tab(metrics)
    with tabs[3]:
        render_ml_tab(metrics)
    with tabs[4]:
        render_data_tab(data, metrics)


if __name__ == "__main__":
    main()
