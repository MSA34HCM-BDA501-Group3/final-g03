"""Render cartographic HCMC maps, direct Baseline-vs-RF transition charts, and equation strips."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D
from matplotlib.patches import FancyBboxPatch, Polygon, Rectangle
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, roc_auc_score

plt.rcParams["font.sans-serif"] = ["Segoe UI", "DejaVu Sans", "Liberation Sans", "Arial"]
plt.rcParams["axes.unicode_minus"] = False

REPO_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_DIR / "raw_data"
DATA_DIR = REPO_DIR / "data"
OUTPUTS_DIR = REPO_DIR / "outputs"
CHART_DIR = OUTPUTS_DIR / "slide_charts"

SAIGON_RIVER_PTS = [
    (106.693, 10.893),
    (106.708, 10.878),
    (106.717, 10.861),
    (106.713, 10.848),
    (106.709, 10.835),
    (106.708, 10.824),
    (106.713, 10.820),
    (106.719, 10.821),
    (106.726, 10.825),
    (106.734, 10.831),
    (106.742, 10.835),
    (106.746, 10.831),
    (106.742, 10.825),
    (106.735, 10.820),
    (106.724, 10.814),
    (106.722, 10.807),
    (106.726, 10.799),
    (106.723, 10.790),
    (106.716, 10.784),
    (106.709, 10.776),
    (106.708, 10.768),
    (106.716, 10.760),
    (106.727, 10.758),
    (106.735, 10.763),
    (106.743, 10.753),
    (106.744, 10.742),
    (106.745, 10.722),
    (106.752, 10.690),
]

KENH_THANH_DA_PTS = [
    (106.713, 10.820),
    (106.716, 10.815),
    (106.722, 10.807),
]

KENH_TAU_HU_PTS = [
    (106.708, 10.768),
    (106.696, 10.761),
    (106.680, 10.752),
    (106.662, 10.749),
    (106.642, 10.739),
    (106.622, 10.725),
    (106.600, 10.713),
]

KENH_TE_PTS = [
    (106.688, 10.753),
    (106.707, 10.752),
    (106.727, 10.758),
]

KENH_NHIEU_LOC_PTS = [
    (106.659, 10.789),
    (106.674, 10.785),
    (106.685, 10.782),
    (106.696, 10.789),
    (106.711, 10.786),
]

DISTRICT_LABELS: list[tuple[float, float, str]] = [
    (106.702, 10.776, "QUẬN 1"),
    (106.684, 10.779, "QUẬN 3"),
    (106.665, 10.754, "QUẬN 5"),
    (106.718, 10.733, "QUẬN 7"),
    (106.652, 10.734, "QUẬN 8"),
    (106.665, 10.771, "QUẬN 10"),
    (106.646, 10.801, "TÂN BÌNH"),
    (106.683, 10.800, "PHÚ NHUẬN"),
    (106.708, 10.807, "BÌNH THẠNH"),
    (106.766, 10.848, "THỦ ĐỨC"),
    (106.620, 10.858, "QUẬN 12"),
    (106.664, 10.838, "GÒ VẤP"),
    (106.598, 10.756, "BÌNH TÂN"),
]


def _smooth_polyline(pts: list[tuple[float, float]], subdivisions: int = 8) -> tuple[np.ndarray, np.ndarray]:
    arr = np.asarray(pts, dtype=float)
    t_orig = np.linspace(0.0, 1.0, len(arr))
    t_fine = np.linspace(0.0, 1.0, max(len(arr) * subdivisions, 20))
    xs = np.interp(t_fine, t_orig, arr[:, 0])
    ys = np.interp(t_fine, t_orig, arr[:, 1])
    kernel = np.array([0.15, 0.25, 0.20, 0.25, 0.15])
    xs_s = np.convolve(np.pad(xs, 2, mode="edge"), kernel, mode="valid")
    ys_s = np.convolve(np.pad(ys, 2, mode="edge"), kernel, mode="valid")
    return xs_s, ys_s


def _draw_hcmc_cartographic_base(ax) -> None:
    """Draw Saigon River, urban canals, district orientation labels, north arrow & 5 km scale bar."""
    ax.set_facecolor("#F4F7FB")

    # Saigon River main channel
    rx, ry = _smooth_polyline(SAIGON_RIVER_PTS, subdivisions=10)
    ax.plot(rx, ry, color="#7DD3FC", lw=7.8, solid_capstyle="round", alpha=0.76, zorder=0.5)
    ax.plot(rx, ry, color="#BAE6FD", lw=5.4, solid_capstyle="round", alpha=0.94, zorder=0.6)

    # Major urban canals (Thanh Đa, Tàu Hủ - Bến Nghé, Kênh Tẻ, Nhiêu Lộc - Thị Nghè)
    for canal_pts, width in (
        (KENH_THANH_DA_PTS, 2.5),
        (KENH_TAU_HU_PTS, 3.2),
        (KENH_TE_PTS, 2.8),
        (KENH_NHIEU_LOC_PTS, 2.2),
    ):
        cx, cy = _smooth_polyline(canal_pts, subdivisions=8)
        ax.plot(cx, cy, color="#93C5FD", lw=width, solid_capstyle="round", alpha=0.82, zorder=0.6)

    # River label along wide Saigon River channel (clear of bridges and callouts)
    ax.text(
        106.735,
        10.822,
        "Sông Sài Gòn",
        fontsize=8.2,
        fontstyle="italic",
        fontweight="bold",
        color="#0369A1",
        rotation=33,
        ha="center",
        va="center",
        path_effects=[pe.withStroke(linewidth=2.4, foreground="#F4F7FB")],
        zorder=7,
    )

    # Subtle District Orientation Labels
    halo = [pe.withStroke(linewidth=2.6, foreground="#F8FAFC")]
    for dx, dy, dname in DISTRICT_LABELS:
        ax.text(
            dx,
            dy,
            dname,
            fontsize=7.3,
            fontweight="bold",
            color="#475569",
            alpha=0.86,
            ha="center",
            va="center",
            path_effects=halo,
            zorder=6,
        )


def _draw_north_arrow_and_scalebar(ax) -> None:
    """Add cartographic North arrow (top-right) and 5 km metric scale bar (bottom-left) without degree ticks."""
    # Clean border frame without raw lat/lon ticks
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color("#94A3B8")
        spine.set_linewidth(1.1)

    # North Arrow badge (top-right)
    nx, ny = 106.817, 10.871
    ax.add_patch(
        FancyBboxPatch(
            (nx - 0.011, ny - 0.013),
            0.022,
            0.026,
            boxstyle="round,pad=0.002,rounding_size=0.004",
            facecolor="white",
            edgecolor="#CBD5E1",
            lw=1.0,
            alpha=0.94,
            zorder=18,
        )
    )
    ax.add_patch(
        Polygon(
            [(nx, ny + 0.010), (nx - 0.0048, ny - 0.003), (nx, ny - 0.0005), (nx + 0.0048, ny - 0.003)],
            closed=True,
            facecolor="#0F172A",
            edgecolor="#0F172A",
            zorder=19,
        )
    )
    ax.text(
        nx, ny - 0.0082, "BẮC", fontsize=6.8, fontweight="bold", color="#0F172A", ha="center", va="center", zorder=19
    )

    # 5 km Scale Bar (bottom-left: 5 km at 10.78°N = 0.0457° longitude)
    sx0, sy0 = 106.558, 10.700
    deg_5km = 0.0457
    ax.add_patch(
        FancyBboxPatch(
            (sx0 - 0.004, sy0 - 0.005),
            deg_5km + 0.022,
            0.014,
            boxstyle="round,pad=0.002,rounding_size=0.003",
            facecolor="white",
            edgecolor="#CBD5E1",
            lw=0.9,
            alpha=0.94,
            zorder=18,
        )
    )
    ax.plot([sx0, sx0 + deg_5km], [sy0, sy0], color="#0F172A", lw=2.4, zorder=19)
    ax.plot([sx0, sx0], [sy0 - 0.0022, sy0 + 0.0022], color="#0F172A", lw=1.8, zorder=19)
    ax.plot([sx0 + deg_5km, sx0 + deg_5km], [sy0 - 0.0022, sy0 + 0.0022], color="#0F172A", lw=1.8, zorder=19)
    ax.text(
        sx0 + deg_5km / 2,
        sy0 + 0.0048,
        "5 km",
        fontsize=7.4,
        fontweight="bold",
        color="#0F172A",
        ha="center",
        va="center",
        zorder=19,
    )


def _load_segment_geometries() -> pd.DataFrame:
    nodes = pd.read_csv(RAW_DIR / "nodes.csv", usecols=["_id", "long", "lat"])
    segs = pd.read_csv(RAW_DIR / "segments.csv", usecols=["_id", "s_node_id", "e_node_id", "street_level"])
    segs = segs.rename(columns={"_id": "segment_id"})
    s_xy = nodes.rename(columns={"_id": "s_node_id", "long": "x1", "lat": "y1"})
    e_xy = nodes.rename(columns={"_id": "e_node_id", "long": "x2", "lat": "y2"})
    merged = segs.merge(s_xy, on="s_node_id", how="inner").merge(e_xy, on="e_node_id", how="inner")
    return merged.dropna(subset=["x1", "y1", "x2", "y2"])


def _segments_to_lines(df: pd.DataFrame) -> np.ndarray:
    if df.empty:
        return np.zeros((0, 2, 2), dtype=float)
    p1 = df[["x1", "y1"]].to_numpy(dtype=float)
    p2 = df[["x2", "y2"]].to_numpy(dtype=float)
    return np.stack([p1, p2], axis=1)


def _annotate_landmarks(ax, geom_indexed: pd.DataFrame, items: list[tuple[int, str, tuple[float, float], str]]) -> None:
    for seg_id, label, (tx, ty), color in items:
        if seg_id not in geom_indexed.index:
            continue
        row = geom_indexed.loc[seg_id]
        px = float((row["x1"] + row["x2"]) / 2.0)
        py = float((row["y1"] + row["y2"]) / 2.0)
        ax.scatter([px], [py], s=62, c=color, edgecolors="white", linewidths=1.5, zorder=14)
        ax.annotate(
            label,
            xy=(px, py),
            xytext=(tx, ty),
            fontsize=8.3,
            fontweight="bold",
            color="#0F172A",
            bbox=dict(boxstyle="round,pad=0.26", facecolor="white", edgecolor=color, lw=1.35, alpha=0.97),
            arrowprops=dict(arrowstyle="->", color=color, lw=1.35, shrinkA=2, shrinkB=3),
            zorder=15,
        )


def render_hcmc_problem_context_map(geom: pd.DataFrame, out_path: Path) -> Path:
    """Domain-level HCMC urban traffic context map for Slide 02 (no solution scores or algorithm metrics)."""
    ds = pd.read_csv(DATA_DIR / "dataset.csv", usecols=["segment_id", "is_severe"])
    seg_obs = ds.groupby("segment_id", as_index=False).agg(severe_rate=("is_severe", "mean"))
    m = geom.merge(seg_obs, on="segment_id", how="left")
    unmonitored = m[m["severe_rate"].isna()]
    smooth_obs = m[m["severe_rate"].notna() & (m["severe_rate"] < 0.15)]
    busy_obs = m[(m["severe_rate"] >= 0.15) & (m["severe_rate"] < 0.50)]
    congested_obs = m[m["severe_rate"] >= 0.50]

    fig, ax = plt.subplots(figsize=(8.3, 6.5), dpi=240, facecolor="white")
    _draw_hcmc_cartographic_base(ax)

    ax.add_collection(
        LineCollection(_segments_to_lines(unmonitored), colors="#CBD5E1", linewidths=0.30, alpha=0.76, zorder=1)
    )
    ax.add_collection(
        LineCollection(_segments_to_lines(smooth_obs), colors="#0284C7", linewidths=0.78, alpha=0.88, zorder=3)
    )
    ax.add_collection(
        LineCollection(_segments_to_lines(busy_obs), colors="#F59E0B", linewidths=1.00, alpha=0.92, zorder=4)
    )
    ax.add_collection(
        LineCollection(_segments_to_lines(congested_obs), colors="#DC2626", linewidths=1.35, alpha=0.96, zorder=5)
    )

    corridors = [
        (47407, "Trục Lý Thường Kiệt (Q.10)\nHành lang xuyên tâm nội đô", (106.554, 10.787), "#DC2626"),
        (50017, "Lê Lai – Bến Thành (Q.1)\nĐầu mối giao thông trung tâm", (106.732, 10.756), "#B91C1C"),
        (61781, "Khu vực Phú Định (Q.8)\nĐường nhánh thiếu thiết bị đo", (106.556, 10.718), "#7C3AED"),
        (21969, "Nguyễn Kiệm – Phú Nhuận\nCửa ngõ sân bay Tân Sơn Nhất", (106.558, 10.828), "#D97706"),
        (34723, "Quốc lộ 1 (Q.12)\nVành đai vận tải hàng hóa", (106.572, 10.873), "#0F766E"),
        (77224, "Kha Vạn Cân – Thủ Đức\nCửa ngõ kết nối Đông Bắc", (106.726, 10.869), "#1D4ED8"),
    ]
    _annotate_landmarks(ax, geom.set_index("segment_id"), corridors)

    ax.set_xlim(106.548, 106.835)
    ax.set_ylim(10.692, 10.890)
    ax.set_aspect(1.0 / np.cos(np.deg2rad(10.78)))
    _draw_north_arrow_and_scalebar(ax)

    ax.set_title(
        "Bối Cảnh Giao Thông TP.HCM: Điểm Nghẽn Trục Chính & Vùng Mù Quan Trắc Đô Thị",
        fontsize=10.3,
        fontweight="bold",
        color="#0F172A",
        pad=8,
    )

    legend_handles = [
        Line2D([0], [0], color="#CBD5E1", lw=2.3, label="Đường nhánh & khu dân cư thiếu cảm biến (Vùng mù quan trắc)"),
        Line2D([0], [0], color="#0284C7", lw=2.3, label="Trục đường có dữ liệu hành trình — Lưu thông ổn định"),
        Line2D([0], [0], color="#F59E0B", lw=2.5, label="Hành lang đông đúc vào khung giờ cao điểm"),
        Line2D([0], [0], color="#DC2626", lw=2.9, label="Trọng điểm thường xuyên ùn tắc trên trục chính & cửa ngõ"),
    ]
    leg = ax.legend(handles=legend_handles, loc="lower right", fontsize=7.8, framealpha=0.97, edgecolor="#CBD5E1")
    leg.set_zorder(20)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def render_hcmc_hotspots_map(geom: pd.DataFrame, out_path: Path) -> Path:
    m_json = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    st_df = pd.read_parquet(
        DATA_DIR / "lakehouse" / "segment_static_features.parquet",
        columns=["segment_id", "pagerank_score", "is_cold_start"],
    )
    ds = pd.read_csv(DATA_DIR / "dataset.csv", usecols=["segment_id", "is_severe", "los_ordinal"])
    seg_obs = ds.groupby("segment_id", as_index=False).agg(
        severe_rate=("is_severe", "mean"),
        mean_los=("los_ordinal", "mean"),
    )
    m = geom.merge(st_df[["segment_id", "pagerank_score"]], on="segment_id", how="inner").merge(
        seg_obs, on="segment_id", how="left"
    )
    cold = m[m["severe_rate"].isna()]
    free_obs = m[m["severe_rate"].notna() & (m["severe_rate"] < 0.15)]
    mod_obs = m[(m["severe_rate"] >= 0.15) & (m["severe_rate"] < 0.50)]
    sev_obs = m[m["severe_rate"] >= 0.50]

    fig, ax = plt.subplots(figsize=(8.3, 6.5), dpi=240, facecolor="white")
    _draw_hcmc_cartographic_base(ax)

    ax.add_collection(LineCollection(_segments_to_lines(cold), colors="#CBD5E1", linewidths=0.30, alpha=0.76, zorder=1))
    ax.add_collection(
        LineCollection(_segments_to_lines(free_obs), colors="#0284C7", linewidths=0.78, alpha=0.88, zorder=3)
    )
    ax.add_collection(
        LineCollection(_segments_to_lines(mod_obs), colors="#F59E0B", linewidths=1.00, alpha=0.92, zorder=4)
    )
    ax.add_collection(
        LineCollection(_segments_to_lines(sev_obs), colors="#DC2626", linewidths=1.35, alpha=0.96, zorder=5)
    )

    hot_df = pd.read_csv(OUTPUTS_DIR / "hotspot_alerts.csv").set_index("segment_id")
    top_ewma = float(hot_df.loc[47407, "decaying_hotspot_score"]) if 47407 in hot_df.index else 6.67
    pr_map = st_df.set_index("segment_id")["pagerank_score"].to_dict()
    landmarks = [
        (47407, f"Lý Thường Kiệt (Q.10)\nĐiểm nóng #1: {top_ewma:.2f}", (106.554, 10.787), "#DC2626"),
        (
            15011,
            f"Phan Đăng Lưu (Bình Thạnh)\nNút cổ chai #7: PR {pr_map.get(15011, 2.62):.2f}",
            (106.726, 10.765),
            "#B91C1C",
        ),
        (61781, f"Phú Định (Q.8)\nVùng mù Cold-Start: PR {pr_map.get(61781, 2.21):.2f}", (106.556, 10.718), "#7C3AED"),
        (
            21969,
            f"Nguyễn Kiệm – Phú Nhuận\nNút cổ chai #1: PR {pr_map.get(21969, 2.78):.2f}",
            (106.558, 10.828),
            "#D97706",
        ),
        (34723, f"Quốc lộ 1 (Q.12)\nNút cổ chai #3: PR {pr_map.get(34723, 2.69):.2f}", (106.572, 10.873), "#0F766E"),
        (
            81815,
            f"Song Hành XLHN (Thủ Đức)\nNút cổ chai #5: PR {pr_map.get(81815, 2.65):.2f}",
            (106.726, 10.869),
            "#1D4ED8",
        ),
    ]
    _annotate_landmarks(ax, geom.set_index("segment_id"), landmarks)

    ax.set_xlim(106.548, 106.835)
    ax.set_ylim(10.692, 10.890)
    ax.set_aspect(1.0 / np.cos(np.deg2rad(10.78)))
    _draw_north_arrow_and_scalebar(ax)

    total_segs = m_json["dataset"]["segments_count"]
    cold_cnt = m_json["dataset"]["cold_start_segments_count"]
    cold_pct = m_json["static_feature_store_c13b"]["cold_start_ratio_pct"]
    ax.set_title(
        f"Bản đồ Giao thông TP.HCM ({total_segs:,} Đoạn cơ giới): Vùng Thiếu GPS ({cold_pct:.2f}%) & Điểm Nóng Kẹt Xe",
        fontsize=10.0,
        fontweight="bold",
        color="#0F172A",
        pad=8,
    )

    legend_handles = [
        Line2D([0], [0], color="#CBD5E1", lw=2.3, label=f"Đoạn đường thiếu GPS ({cold_cnt:,} đoạn · {cold_pct:.2f}%)"),
        Line2D([0], [0], color="#0284C7", lw=2.3, label="Đường thông thoáng ổn định (LOS A–B)"),
        Line2D([0], [0], color="#F59E0B", lw=2.5, label="Đường đông đúc giờ cao điểm (LOS C–D)"),
        Line2D([0], [0], color="#DC2626", lw=2.9, label="Điểm nóng thường xuyên kẹt cứng (LOS E–F)"),
    ]
    leg1 = ax.legend(handles=legend_handles, loc="lower right", fontsize=7.9, framealpha=0.97, edgecolor="#CBD5E1")
    leg1.set_zorder(20)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def render_hcmc_clusters_map(geom: pd.DataFrame, out_path: Path) -> Path:
    static_df = pd.read_parquet(
        DATA_DIR / "lakehouse" / "segment_static_features.parquet",
        columns=["segment_id", "cluster_id", "is_cold_start"],
    )
    m = geom.merge(static_df, on="segment_id", how="inner")
    counts = static_df["cluster_id"].value_counts().to_dict()

    palette = {
        0: ("#F59E0B", 0.56, f"Nhóm 0: Đường nội đô Vmax ~43 km/h ({counts.get(0, 0):,} đoạn)"),
        1: ("#DC2626", 0.90, f"Nhóm 1: Trục nội đô hay kẹt 43.3% ({counts.get(1, 0):,} đoạn)"),
        2: ("#94A3B8", 0.30, f"Nhóm 2: Đường nhánh & hành lang thoáng ({counts.get(2, 0):,} đoạn)"),
        3: ("#2563EB", 0.95, f"Nhóm 3: Tuyến tốc độ cao / cửa ngõ ({counts.get(3, 0):,} đoạn)"),
        4: ("#8B5CF6", 0.82, f"Nhóm 4: Trục chính & nút cổ chai PR cao ({counts.get(4, 0):,} đoạn)"),
        5: ("#10B981", 0.70, f"Nhóm 5: Đoạn liên kết dài ~234m ({counts.get(5, 0):,} đoạn)"),
    }

    fig, ax = plt.subplots(figsize=(8.3, 6.5), dpi=240, facecolor="white")
    _draw_hcmc_cartographic_base(ax)

    for z_idx, cid in enumerate([2, 0, 5, 4, 3, 1], start=1):
        color, lw, _ = palette[cid]
        sub = m[m["cluster_id"] == cid]
        coll = LineCollection(_segments_to_lines(sub), colors=color, linewidths=lw, alpha=0.86, zorder=z_idx)
        ax.add_collection(coll)
    handles = [Line2D([0], [0], color=palette[cid][0], lw=2.5, label=palette[cid][2]) for cid in range(6)]

    ax.set_xlim(106.548, 106.835)
    ax.set_ylim(10.692, 10.890)
    ax.set_aspect(1.0 / np.cos(np.deg2rad(10.78)))
    _draw_north_arrow_and_scalebar(ax)

    ax.set_title(
        f"Bản đồ 6 Nhóm Hành Vi Đường Phố (K-Means, k = 6) Phủ Kín {len(static_df):,} Đoạn Cơ Giới TP.HCM",
        fontsize=10.0,
        fontweight="bold",
        color="#0F172A",
        pad=8,
    )
    leg2 = ax.legend(handles=handles, loc="lower right", fontsize=7.6, framealpha=0.97, edgecolor="#CBD5E1")
    leg2.set_zorder(20)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def render_transition_analysis_chart(out_path: Path) -> Path:
    """Direct 1-on-1 comparison: Baseline (copy current LOS) vs. Random Forest (23 features)."""
    m_json = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    ev = m_json["evaluation"]
    tr = ev["transition_analysis"]
    gap = tr["next_observed_gap_regime"]
    cov = tr["cold_start_coverage"]
    dist = tr["test_class_distribution"]
    pers_acc = m_json["evaluation"]["ablation_table"][0]["accuracy_6class"] * 100.0

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6.9, 6.2), dpi=220)

    cats = [
        f"Phát hiện đổi trạng thái\nROC-AUC (n = {tr['transition_samples']} ca)",
        f"Đoán trúng mức mới\nNgưỡng VAL τ={tr['val_selected_change_threshold']:.2f}",
        "Cảnh báo khi GPS thưa\nPR-AUC (> 30 phút)",
        f"Độ phủ đường thiếu GPS\n({cov['cold_start_segments']:,} đoạn)",
    ]
    pers_vals = [
        tr["persistence_transition_detect_roc_auc"] * 100.0,
        tr["persistence_transition_6class_acc"] * 100.0,
        gap["persistence_severe_pr_auc"] * 100.0,
        cov["persistence_cold_start_coverage_pct"],
    ]
    rf_vals = [
        tr["rf_23d_transition_detect_roc_auc"] * 100.0,
        tr["rf_23d_val_calibrated_transition_6class_acc"] * 100.0,
        gap["rf_23d_severe_pr_auc"] * 100.0,
        cov["rf_23d_network_coverage_pct"],
    ]

    x = np.arange(len(cats))
    w = 0.32
    b1 = ax1.bar(x - w / 2, pers_vals, w, label="Baseline (Giữ nguyên trạng thái hiện tại)", color="#94A3B8")
    b2 = ax1.bar(x + w / 2, rf_vals, w, label="Random Forest (23 biến đầu vào)", color="#0284C7")
    ax1.set_xticks(x, cats, fontsize=8.0, fontweight="bold", color="#0F172A")
    ax1.set_ylim(0, 118)
    ax1.set_ylabel("Hiệu năng / Độ phủ (%)", fontsize=8.5, fontweight="bold")
    ax1.set_title(
        "So sánh Trực diện Baseline và Random Forest ở Các Tình Huống Khó Nhất",
        fontsize=9.5,
        fontweight="bold",
        color="#0F172A",
    )
    ax1.legend(fontsize=8.0, loc="upper left", frameon=True)
    ax1.grid(axis="y", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    for bars, col in ((b1, "#475569"), (b2, "#0F172A")):
        for bar in bars:
            v = bar.get_height()
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                v + 2.0,
                f"{v:.1f}%",
                ha="center",
                fontsize=7.8,
                fontweight="bold",
                color=col,
            )

    los_labels = ["LOS A\n(Thông thoáng)", "LOS B", "LOS C", "LOS D", "LOS E\n(Ùn ứ)", "LOS F\n(Kẹt cứng)"]
    los_counts = [int(dist[k]) for k in ("A", "B", "C", "D", "E", "F")]
    bar_cols = ["#0EA5E9", "#38BDF8", "#FBBF24", "#F59E0B", "#EF4444", "#B91C1C"]
    xb = np.arange(len(los_labels))
    bars_b = ax2.bar(xb, los_counts, width=0.52, color=bar_cols)
    ax2.set_xticks(xb, los_labels, fontsize=8.0, fontweight="bold", color="#0F172A")
    ax2.set_ylim(0, 1120)
    ax2.set_ylabel("Số mẫu kiểm thử", fontsize=8.5, fontweight="bold")
    a_pct = los_counts[0] / max(sum(los_counts), 1) * 100.0
    ax2.set_title(
        f"Vì sao Baseline đạt {pers_acc:.2f}% tổng thể? {a_pct:.1f}% mẫu là LOS A & "
        f"{tr['steady_state_ratio_pct']:.1f}% đứng yên",
        fontsize=9.0,
        fontweight="bold",
        color="#0F172A",
    )
    ax2.grid(axis="y", linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    for bar, cnt in zip(bars_b, los_counts):
        pct = cnt / max(sum(los_counts), 1) * 100.0
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            cnt + 22,
            f"{cnt} ({pct:.1f}%)",
            ha="center",
            fontsize=7.7,
            fontweight="bold",
            color="#0F172A",
        )

    plt.tight_layout(pad=1.15)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _render_single_equation(tex_lines: list[str], out_path: Path, fig_h: float = 0.78, fontsize: float = 10.6) -> Path:
    bg_hex = "#F8FAFC"
    with plt.rc_context({"mathtext.fontset": "stix"}):
        fig = plt.figure(figsize=(7.35, fig_h), dpi=300, facecolor=bg_hex)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor(bg_hex)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        n = len(tex_lines)
        top_y = 0.80 if n >= 3 else (0.72 if n == 2 else 0.50)
        bot_y = 0.20 if n >= 3 else (0.28 if n == 2 else 0.50)
        for idx, line in enumerate(tex_lines):
            y_pos = 0.50 if n == 1 else (top_y - idx * ((top_y - bot_y) / max(n - 1, 1)))
            ax.text(0.50, y_pos, line, ha="center", va="center", fontsize=fontsize, color="#0F172A")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out_path, dpi=300, bbox_inches="tight", pad_inches=0.05, facecolor=bg_hex)
        plt.close(fig)
    return out_path


def render_pdf_equations() -> dict[str, Path]:
    eqs: dict[str, tuple[list[str], float, float]] = {
        "eq_vmax_los": (
            [
                r"$N_{\mathrm{in}} = N_{\mathrm{dedup}} + N_{\mathrm{valid}} + N_{\mathrm{quarantine}}"
                r"\qquad r_{s,w} = \frac{\bar{v}_{s,w}}{\hat{v}_{\max}(s)},\;\; "
                r"\hat{v}_{\max}(s) \in \left\{v_{\max}^{\mathrm{raw}},\; P_{85}^{\mathrm{train}}(s),\; "
                r"\mathrm{Med}_{\mathrm{train}}(\ell, \tau),\; v_{\mathrm{prior}}(\ell)\right\}$",
                r"$\mathrm{LOS}_{\mathrm{ratio}}(r_{s,w}):\; A\,(\geq 0.80)\;\cdot\; B\,[0.65,0.80)\;\cdot\; C\,[0.50,0.65)"
                r"\;\cdot\; D\,[0.35,0.50)\;\cdot\; E\,[0.20,0.35)\;\cdot\; F\,(<0.20)$",
                r"$\mathrm{LOS}_{\mathrm{urban}}(\bar{v}_{s,w}):\; A\,(\geq 35)\;\cdot\; B\,[30,35)\;\cdot\; C\,[25,30)"
                r"\;\cdot\; D\,[20,25)\;\cdot\; E\,[15,20)\;\cdot\; F\,(<15\;\mathrm{km/h})\;\; "
                r"[89.19\%\;\mathrm{BkTraffic}]$",
            ],
            1.28,
            10.0,
        ),
        "eq_monoid": (
            [
                r"$\eta(v) = \left(1,\; v,\; v,\; v,\; \mathbb{I}(v < 15.0\;\mathrm{km/h})\right),"
                r"\qquad \mathbf{e} = (0,\; 0.0,\; +\infty,\; -\infty,\; 0)$",
                r"$\mathbf{m}_1 \oplus \mathbf{m}_2 = \left(\mathrm{cnt}_1 + \mathrm{cnt}_2,\;"
                r"\sum v_1 + \sum v_2,\; \min(v_{\min,1}, v_{\min,2}),\;"
                r"\max(v_{\max,1}, v_{\max,2}),\; \mathrm{sev}_1 + \mathrm{sev}_2\right)$",
            ],
            0.94,
            10.6,
        ),
        "eq_pagerank": (
            [
                r"$r_s^{(k+1)} = (1 - \beta) + \beta \left("
                r"\sum_{u \in \mathrm{In}(s)} \frac{r_u^{(k)}}{d_{\mathrm{out}}(u)} + "
                r"\frac{1}{N} \sum_{d \in \mathcal{D}} r_d^{(k)} \right),"
                r"\qquad \sum_{s=1}^{84,206} r_s^{(30)} = 84,206.0 \quad (\beta = 0.85)$",
            ],
            0.76,
            10.4,
        ),
        "eq_prior": (
            [
                r"$\mathrm{Prior}_s(t) = \frac{\sum_{\tau < t} y_{s,\tau} + "
                r"m_{\mathrm{seg}} \cdot \bar{y}_{\mathrm{global}}(\tau < t)}"
                r"{\mathrm{count}_s(\tau < t) + m_{\mathrm{seg}}}"
                r"\qquad (m_{\mathrm{seg}} = 5,\; m_{\mathrm{street}} = 10,\; \forall \tau < t)$",
            ],
            0.76,
            10.6,
        ),
        "eq_sketches": (
            [
                r"$P_{\mathrm{fp}} \approx \left(1 - e^{-kn/m}\right)^k \;\; "
                r"(2 \times 131,072\;\mathrm{bits},\; k = 10)"
                r"\qquad \hat{E}_{\mathrm{HLL}} = \alpha_m m^2 \left(\sum_{j=0}^{m-1} 2^{-M[j]}\right)^{-1}$",
                r"$C_s(t) = (1 - \alpha)\,C_s(t - 1) + \mathbb{I}\left(\mathrm{LOS}_{s,t} \in \{E, F\}\right)"
                r"\qquad (\alpha = 0.15,\; \mathrm{prune}\; C_s(t) < \varepsilon = 10^{-4})$",
            ],
            0.94,
            10.4,
        ),
    }
    out_map: dict[str, Path] = {}
    for name, (lines, height, fsize) in eqs.items():
        out_map[name] = _render_single_equation(lines, CHART_DIR / f"{name}.png", fig_h=height, fontsize=fsize)
    return out_map


def render_ablation_t30m_chart(out_path: Path) -> Path:
    ab_df = pd.read_csv(OUTPUTS_DIR / "ablation_results.csv").set_index("model")
    order_top = [
        ("Decision Tree (Local Only)", "DT (14 biến)"),
        ("Random Forest (Local Only)", "RF (14 biến)"),
        ("GBT Binary Alerting (Local Only)", "GBT (14 biến)"),
        ("GBT Binary Alerting (+Graph PageRank, Causal Priors & K-Means)", "GBT (23 biến)"),
        ("Random Forest (+Graph PageRank, Causal Priors & K-Means)", "RF (23 biến) [Chọn]"),
    ]
    models_top = [lbl for _, lbl in order_top]
    sev_f1 = [float(ab_df.loc[k, "severe_ef_f1"]) for k, _ in order_top]
    sev_pr = [float(ab_df.loc[k, "severe_pr_auc"]) for k, _ in order_top]
    sev_roc = [float(ab_df.loc[k, "severe_roc_auc"]) for k, _ in order_top]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7.0, 6.2), dpi=220)
    x1 = np.arange(len(models_top))
    w1 = 0.25
    ax1.bar(x1 - w1, sev_f1, w1, label="F1 kẹt cứng", color="#38BDF8")
    ax1.bar(x1, sev_pr, w1, label="PR-AUC kẹt cứng", color="#0284C7")
    b_roc = ax1.bar(x1 + w1, sev_roc, w1, label="ROC-AUC kẹt cứng", color="#0F172A")
    ax1.set_xticks(x1, models_top, fontsize=8.0, fontweight="bold", color="#0F172A")
    ax1.set_ylim(0.72, 1.055)
    ax1.set_ylabel("Điểm trên tập TEST (1,182 mẫu)", fontsize=8.4, fontweight="bold")
    ax1.set_title(
        "Cảnh báo Kẹt cứng (LOS E–F) Trước 30 Phút",
        fontsize=9.6,
        fontweight="bold",
        color="#0F172A",
    )
    ax1.legend(ncol=3, fontsize=7.8, loc="upper left", frameon=True)
    ax1.grid(axis="y", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    for bar, val in zip(b_roc, sev_roc):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            val + 0.006,
            f"{val:.4f}",
            ha="center",
            fontsize=7.6,
            fontweight="bold",
            color="#0F172A",
        )

    order_bot = [
        ("Decision Tree (Local Only)", "DT (14 biến)"),
        ("Decision Tree (+Graph PageRank, Causal Priors & K-Means)", "DT (23 biến)"),
        ("Random Forest (Local Only)", "RF (14 biến)"),
        ("Random Forest (+Graph PageRank, Causal Priors & K-Means)", "RF (23 biến) [Chọn]"),
    ]
    models_bot = [lbl for _, lbl in order_bot]
    macro_f1 = [float(ab_df.loc[k, "macro_f1_6class"]) for k, _ in order_bot]
    acc_6 = [float(ab_df.loc[k, "accuracy_6class"]) for k, _ in order_bot]
    x2 = np.arange(len(models_bot))
    w2 = 0.30
    b_mf1 = ax2.bar(x2 - w2 / 2, macro_f1, w2, label="Macro-F1 (6 mức)", color="#0D9488")
    b_acc = ax2.bar(x2 + w2 / 2, acc_6, w2, label="Độ chính xác 6 mức", color="#2563EB")
    ax2.set_xticks(x2, models_bot, fontsize=8.2, fontweight="bold", color="#0F172A")
    ax2.set_ylim(0.25, 1.08)
    ax2.set_ylabel("Điểm 6 mức (LOS A–F)", fontsize=8.4, fontweight="bold")
    gain_pct = (macro_f1[-1] - macro_f1[1]) * 100.0
    wf1 = float(ab_df.loc["Random Forest (+Graph PageRank, Causal Priors & K-Means)", "weighted_f1_6class"])
    ax2.set_title(
        f"Phân loại 6 Mức LOS A–F (+{gain_pct:.2f}% Macro-F1 so với DT 23D; Weighted-F1 {wf1:.4f})",
        fontsize=9.3,
        fontweight="bold",
        color="#0F172A",
    )
    ax2.legend(ncol=2, fontsize=7.8, loc="upper left", frameon=True)
    ax2.grid(axis="y", linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    for bar, val, col in [(b, v, "#0F766E") for b, v in zip(b_mf1, macro_f1)] + [
        (b, v, "#1E3A8A") for b, v in zip(b_acc, acc_6)
    ]:
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            val + 0.015,
            f"{val:.4f}",
            ha="center",
            fontsize=7.7,
            fontweight="bold",
            color=col,
        )

    plt.tight_layout(pad=1.15)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


_GRAPH_KMEANS_FEATS = frozenset({"pagerank_score", "in_degree", "out_degree", "cluster_id"})
_PAST_PRIOR_FEATS = frozenset(
    {
        "neighbor_los_ordinal",
        "segment_train_prior_los",
        "segment_train_severe_prior",
        "street_causal_prior_los",
        "street_causal_prior_sev",
    }
)
_FEATURE_DISPLAY_LABELS: dict[str, str] = {
    "mean_velocity": "mean_velocity (Vận tốc TB cửa sổ)",
    "current_los_ordinal": "current_los_ordinal (Mức LOS tại t)",
    "velocity_ratio": "velocity_ratio (Tỷ lệ v / Vmax)",
    "period_sin": "period_sin (Chu kỳ giờ ICT · sin)",
    "pagerank_score": "pagerank_score (Độ tụ Line-Graph)",
    "neighbor_los_ordinal": "neighbor_los_ordinal (LOS liền kề ≤ t)",
    "street_causal_prior_los": "street_causal_prior_los (Tiên nghiệm τ<t)",
    "period_cos": "period_cos (Chu kỳ giờ ICT · cos)",
    "max_velocity": "max_velocity (Vmax nội suy 3 tầng)",
    "gap_minutes": "gap_minutes (Khoảng cách từ lag1)",
    "obs_count": "obs_count (Số ping GPS cửa sổ)",
    "lag1_los_ordinal": "lag1_los_ordinal (LOS cửa sổ t−30p)",
}


def _compute_test_confusion_bundle() -> dict[str, object]:
    bundle = joblib.load(OUTPUTS_DIR / "model.joblib")
    feats = list(bundle["full_features"])
    rf = bundle["rf_multiclass"]
    imp = bundle["ml_imputer_full"]
    tau = float(bundle.get("selected_val_threshold", 0.25))

    df = pd.read_csv(DATA_DIR / "dataset.csv")
    df["current_los_ordinal"] = df["los_ordinal"]
    test_sup = df[(df["split_set"] == "TEST") & df["target_next_los_ordinal"].notna()].copy()
    x_te = imp.transform(test_sup[feats])
    y_true = test_sup["target_next_los_ordinal"].astype(int).to_numpy()
    y_pred = rf.predict(x_te)
    probs = rf.predict_proba(x_te)
    p_sev = probs[:, 4] + probs[:, 5]

    cm = confusion_matrix(y_true, y_pred, labels=[0, 1, 2, 3, 4, 5])
    return {
        "cm": cm,
        "n": int(len(y_true)),
        "acc": float(accuracy_score(y_true, y_pred)),
        "sev_f1": float(f1_score((y_true >= 4).astype(int), (p_sev >= tau).astype(int))),
        "roc_auc": float(roc_auc_score((y_true >= 4).astype(int), p_sev)),
        "tau": tau,
    }


def render_confusion_matrix_chart(out_path: Path) -> Path:
    """Render 6x6 LOS A..F Confusion Matrix of deployed RF 23D on TEST (n=1,182)."""
    res = _compute_test_confusion_bundle()
    cm: np.ndarray = res["cm"]  # type: ignore[assignment]
    row_totals = cm.sum(axis=1)
    row_pct = cm / np.maximum(row_totals[:, None], 1) * 100.0

    fig, ax = plt.subplots(figsize=(7.6, 6.3), dpi=240, facecolor="white")
    im = ax.imshow(row_pct, cmap="Blues", vmin=0.0, vmax=100.0, aspect="auto")

    los_codes = ["A", "B", "C", "D", "E", "F"]
    y_labels = [f"LOS {c}\n(n={int(t)})" for c, t in zip(los_codes, row_totals)]
    x_labels = [f"LOS {c}" for c in los_codes]
    ax.set_xticks(np.arange(6), x_labels, fontsize=9.0, fontweight="bold", color="#0F172A")
    ax.set_yticks(np.arange(6), y_labels, fontsize=8.6, fontweight="bold", color="#0F172A")
    ax.set_xlabel("Nhãn Dự Báo RF 23D (t + 30 phút)", fontsize=9.4, fontweight="bold", color="#0F172A", labelpad=8)
    ax.set_ylabel("Nhãn Thực Tế Tập TEST (t + 30 phút)", fontsize=9.4, fontweight="bold", color="#0F172A", labelpad=8)

    for i in range(6):
        for j in range(6):
            cnt = int(cm[i, j])
            pct = float(row_pct[i, j])
            is_diag = i == j
            txt = f"{cnt}\n({pct:.1f}%)" if cnt > 0 else "0"
            col = "white" if pct >= 52.0 else ("#0F172A" if cnt > 0 else "#94A3B8")
            ax.text(
                j,
                i,
                txt,
                ha="center",
                va="center",
                fontsize=8.3 if cnt > 0 else 7.8,
                fontweight="bold" if (is_diag or cnt >= 10) else "normal",
                color=col,
            )

    # Highlight 2x2 Severe Congestion Block (LOS E-F)
    ax.add_patch(
        Rectangle(
            (3.5, 3.5),
            2.0,
            2.0,
            fill=False,
            edgecolor="#DC2626",
            linewidth=2.4,
            linestyle="--",
            zorder=6,
        )
    )
    sev_block_hits = int(cm[4:, 4:].sum())
    sev_block_tot = int(cm[4:, :].sum())
    sev_block_pct = sev_block_hits / max(sev_block_tot, 1) * 100.0
    ax.text(
        4.5,
        3.38,
        f"Khối LOS E–F: {sev_block_hits}/{sev_block_tot} ({sev_block_pct:.1f}%)",
        ha="center",
        va="bottom",
        fontsize=7.7,
        fontweight="bold",
        color="#B91C1C",
        bbox={"boxstyle": "round,pad=0.22", "facecolor": "#FEF2F2", "edgecolor": "#DC2626", "lw": 1.0},
        zorder=7,
    )

    cbar = fig.colorbar(im, ax=ax, fraction=0.044, pad=0.03)
    cbar.set_label("Tỷ lệ nhận diện theo hàng — Row Recall (%)", fontsize=8.4, fontweight="bold", color="#334155")
    cbar.ax.tick_params(labelsize=8.0)

    ax.set_xticks(np.arange(-0.5, 6, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, 6, 1), minor=True)
    ax.grid(which="minor", color="#E2E8F0", linestyle="-", linewidth=0.9)
    ax.tick_params(which="minor", bottom=False, left=False)

    ax.set_title(
        "Ma Trận Nhầm Lẫn 6 Mức LOS (A–F) — Deployed RF 23D Trên Tập TEST\n"
        f"RF 23D TEST (n={res['n']:,}): Acc {res['acc'] * 100:.2f}% · "
        f"Sev-F1 {res['sev_f1'] * 100:.2f}% (τ={res['tau']:.2f}) · ROC-AUC {res['roc_auc']:.4f}",
        fontsize=9.8,
        fontweight="bold",
        color="#0F172A",
        pad=11,
    )
    plt.tight_layout(pad=0.9)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _classify_feature_group(feat_name: str, local_set: set[str]) -> str:
    if feat_name in local_set:
        return "local"
    if feat_name in _GRAPH_KMEANS_FEATS:
        return "graph_km"
    return "priors"


def render_feature_23d_chart(out_path: Path) -> Path:
    """Render Top-12 Feature Importances + 3-Group Shares for deployed 23D Random Forest."""
    bundle = joblib.load(OUTPUTS_DIR / "model.joblib")
    feats = list(bundle["full_features"])
    local_set = set(bundle["local_features"])
    importances = np.asarray(bundle["rf_multiclass"].feature_importances_, dtype=float)

    group_meta = {
        "local": ("#0284C7", "14 Local Telemetry / Time"),
        "priors": ("#7C3AED", "5 Leak-Free Past Priors & Neighbor (τ < t)"),
        "graph_km": ("#0D9488", "4 Graph & 7D K-Means"),
    }
    group_shares = {"local": 0.0, "priors": 0.0, "graph_km": 0.0}
    rows: list[tuple[str, float, str]] = []
    for fname, imp_val in zip(feats, importances):
        grp = _classify_feature_group(fname, local_set)
        group_shares[grp] += float(imp_val) * 100.0
        rows.append((fname, float(imp_val) * 100.0, grp))
    rows.sort(key=lambda item: item[1], reverse=True)
    top12 = rows[:12]

    fig = plt.figure(figsize=(8.6, 6.25), dpi=240, facecolor="white", layout="constrained")
    gs = fig.add_gridspec(2, 1, height_ratios=[4.35, 1.25], hspace=0.18)
    ax_top = fig.add_subplot(gs[0])
    ax_bot = fig.add_subplot(gs[1])

    y_pos = np.arange(len(top12))[::-1]
    vals = [r[1] for r in top12]
    cols = [group_meta[r[2]][0] for r in top12]
    labels = [_FEATURE_DISPLAY_LABELS.get(r[0], r[0]) for r in top12]

    bars = ax_top.barh(y_pos, vals, color=cols, height=0.68, edgecolor="white", linewidth=0.6)
    ax_top.set_yticks(y_pos, labels, fontsize=8.2, fontweight="bold", color="#0F172A")
    ax_top.set_xlim(0, max(vals) * 1.18)
    ax_top.set_xlabel("Độ Quan Trọng Đặc Trưng — Gini Importance (%)", fontsize=8.8, fontweight="bold", color="#0F172A")
    ax_top.grid(axis="x", linestyle="--", alpha=0.38)
    ax_top.spines["top"].set_visible(False)
    ax_top.spines["right"].set_visible(False)

    for bar, val in zip(bars, vals):
        ax_top.text(
            val + 0.6,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.2f}%",
            va="center",
            ha="left",
            fontsize=8.0,
            fontweight="bold",
            color="#0F172A",
        )

    legend_handles = [
        Rectangle((0, 0), 1, 1, facecolor=group_meta[k][0], label=f"{group_meta[k][1]} ({group_shares[k]:.2f}%)")
        for k in ("local", "priors", "graph_km")
    ]
    ax_top.legend(handles=legend_handles, loc="lower right", fontsize=7.9, framealpha=0.96, edgecolor="#CBD5E1")
    top12_sum = sum(vals)
    ax_top.set_title(
        f"Độ Quan Trọng 23 Đặc Trưng (RF 23D) — Top 12 Chiếm {top12_sum:.2f}% Tổng Sức Mạnh Dự Báo",
        fontsize=10.1,
        fontweight="bold",
        color="#0F172A",
        pad=8,
    )

    # Bottom 100%-stacked horizontal bar of 3 groups
    left = 0.0
    for grp_key, short_lbl in (
        ("local", "14 Local Telemetry/Time"),
        ("priors", "5 Past Priors"),
        ("graph_km", "4 Graph/KM"),
    ):
        share = group_shares[grp_key]
        color = group_meta[grp_key][0]
        ax_bot.barh([0], [share], left=[left], color=color, height=0.52, edgecolor="white", linewidth=1.2)
        if share >= 12.0:
            ax_bot.text(
                left + share / 2,
                0,
                f"{short_lbl}: {share:.2f}%",
                ha="center",
                va="center",
                fontsize=8.3,
                fontweight="bold",
                color="white",
            )
        else:
            ax_bot.text(
                left + share / 2,
                0.35,
                f"{share:.2f}%",
                ha="center",
                va="bottom",
                fontsize=7.6,
                fontweight="bold",
                color=color,
            )
        left += share
    ax_bot.set_xlim(0, 100)
    ax_bot.set_ylim(-0.48, 0.72)
    ax_bot.set_yticks([])
    ax_bot.set_xlabel(
        f"Tỷ trọng 3 nhóm (Tổng 23 biến = 100%): Local {group_shares['local']:.2f}%  ·  "
        f"Past Priors & Neighbor {group_shares['priors']:.2f}%  ·  Graph & 7D K-Means {group_shares['graph_km']:.2f}% "
        "(9 biến Đồ thị/Cụm/Priors = 10.50%, quyết định dự báo vùng mù Cold-Start)",
        fontsize=7.6,
        fontweight="bold",
        color="#334155",
    )
    ax_bot.set_title("Phân Bổ Tỷ Trọng Theo 3 Nhóm Đặc Trưng Đầu Vào (23D)", fontsize=8.9, fontweight="bold", pad=4)
    for spine in ("top", "right", "left"):
        ax_bot.spines[spine].set_visible(False)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def main(skip_slide_helpers: bool = False) -> None:
    geom = _load_segment_geometries()
    m0 = render_hcmc_problem_context_map(geom, CHART_DIR / "map_hcmc_problem_context.png")
    m1 = render_hcmc_hotspots_map(geom, CHART_DIR / "map_hcmc_hotspots_coldstart.png")
    m2 = render_hcmc_clusters_map(geom, CHART_DIR / "map_hcmc_clusters_7d.png")
    c_tr = render_transition_analysis_chart(CHART_DIR / "chart_transition_analysis.png")
    c_ab = render_ablation_t30m_chart(CHART_DIR / "chart_ablation_t30m.png")
    c_cm = render_confusion_matrix_chart(CHART_DIR / "chart_confusion_matrix_test.png")
    c_fi = render_feature_23d_chart(CHART_DIR / "chart_feature_23d_importance.png")
    if not skip_slide_helpers:
        import sys

        script_dir = str(Path(__file__).resolve().parent)
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)
        from build_slides import render_eda_chart, render_spark_chart, render_streaming_serving_chart

        render_eda_chart(CHART_DIR / "chart_eda_anomalies.png")
        render_spark_chart(CHART_DIR / "chart_spark_skew_catalyst.png")
        render_streaming_serving_chart(CHART_DIR / "chart_streaming_serving.png")
    eqs = render_pdf_equations()
    print(
        f"Rendered maps ({m0.name}, {m1.name}, {m2.name}), {c_tr.name}, {c_ab.name}, "
        f"{c_cm.name}, {c_fi.name}, and {len(eqs)} equations."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Render shared cartographic, ML, and equation visual assets.")
    parser.add_argument(
        "--skip-slide-helpers",
        action="store_true",
        help="Skip importing chart helpers from build_slides.py.",
    )
    args = parser.parse_args()
    main(skip_slide_helpers=args.skip_slide_helpers)
