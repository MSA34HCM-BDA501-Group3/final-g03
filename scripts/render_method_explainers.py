"""Render intuitive visual explainer infographics for core data/graph/stream methods (Slides 06 & 07)."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, FancyArrowPatch, FancyBboxPatch, Rectangle

plt.rcParams["font.sans-serif"] = ["Segoe UI", "DejaVu Sans", "Arial"]
plt.rcParams["axes.unicode_minus"] = False

REPO_DIR = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = REPO_DIR / "outputs"
CHART_DIR = OUTPUTS_DIR / "slide_charts"


def _load_method_stats() -> dict[str, str]:
    m = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    ds = m["dataset"]
    qf = m["quality_funnel_c5"]
    vt = qf["vmax_imputation_tiers_windows"]
    total_w = max(ds["labeled_windows_count"], 1)
    p1 = m["monoid_and_catalyst_c9"]["pillar_i1_monoid_and_salting"]
    hll = m["streaming_mmds"]["hyperloglog"]
    gp = m["graph_and_patterns"]
    pruned_pct = ds["pruned_orphan_nodes_count"] / ds["nodes_count"] * 100.0
    t0 = vt["TIER_1_SEGMENT_OBSERVED"]
    t1 = vt["TIER_1_SEGMENT_TRAIN_P85"]
    t2 = vt["TIER_2_LEVEL_ROADTYPE_MEDIAN"]
    t3 = vt["TIER_3_HIERARCHICAL_LEVEL_PRIOR"]
    return {
        "miss_pct": f"{qf['missing_max_velocity_train_pct']:.2f}%",
        "t0": f"{t0 / total_w * 100:.1f}% · {t0:,}",
        "t1": f"{t1 / total_w * 100:.1f}% · {t1:,}",
        "t2": f"{t2 / total_w * 100:.1f}% · {t2:,}",
        "t3": f"{t3 / total_w * 100:.1f}% · {t3:,}",
        "skew_raw": f"{p1['temporal_slot_peak_to_median_ratio']:.2f}x",
        "skew_salt": f"{p1['peak_slot_salted_bucket_skew_ratio']:.3f}x",
        "pruned_pct": f"{pruned_pct:.2f}%",
        "top_pr": f"{gp['top_bottleneck_pagerank']:.2f}",
        "top_street": str(gp["top_bottleneck_street"]),
        "segs": f"{ds['segments_count']:,}",
        "cold_segs": f"{ds['cold_start_segments_count']:,}",
        "cold_pct": f"{m['static_feature_store_c13b']['cold_start_ratio_pct']:.2f}%",
        "active_segs": f"{ds['distinct_active_segments']:,}",
        "hll_err": f"{hll['relative_error_pct']:.2f}%",
    }


def _rbox(
    ax: plt.Axes,
    x: float,
    y: float,
    w: float,
    h: float,
    fc: str,
    ec: str,
    lw: float = 1.5,
    radius: float = 0.12,
    zorder: int = 2,
) -> None:
    ax.add_patch(
        FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle=f"round,pad=0.0,rounding_size={radius}",
            facecolor=fc,
            edgecolor=ec,
            lw=lw,
            zorder=zorder,
        )
    )


def _panel_header(ax: plt.Axes, x: float, y: float, w: float, h: float, title: str, badge: str, color: str) -> None:
    _rbox(ax, x, y, w, h, "#F8FAFC", "#CBD5E1", lw=1.4, radius=0.14, zorder=1)
    _rbox(ax, x, y + h - 0.58, w, 0.58, color, color, lw=1.0, radius=0.14, zorder=2)
    ax.add_patch(Rectangle((x, y + h - 0.58), w, 0.22, facecolor=color, edgecolor="none", zorder=2))
    ax.text(
        x + 0.22,
        y + h - 0.29,
        title,
        fontsize=10.8,
        fontweight="bold",
        color="white",
        ha="left",
        va="center",
        zorder=3,
    )
    bw = max(1.35, len(badge) * 0.095 + 0.26)
    _rbox(ax, x + w - bw - 0.18, y + h - 0.46, bw, 0.34, "white", "white", lw=0.8, radius=0.15, zorder=3)
    ax.text(
        x + w - bw / 2 - 0.18,
        y + h - 0.29,
        badge,
        fontsize=8.3,
        fontweight="bold",
        color=color,
        ha="center",
        va="center",
        zorder=4,
    )


def _arrow(
    ax: plt.Axes,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    color: str = "#0284C7",
    lw: float = 2.0,
    zorder: int = 5,
) -> None:
    ax.add_patch(
        FancyArrowPatch(
            (x1, y1),
            (x2, y2),
            arrowstyle="-|>,head_length=6,head_width=4",
            color=color,
            lw=lw,
            zorder=zorder,
        )
    )


def _draw_imputation_panel(ax: plt.Axes) -> None:
    ms = _load_method_stats()
    _panel_header(
        ax,
        0.18,
        0.15,
        8.65,
        5.10,
        "1. Nội Suy Tốc Độ Tối Đa 3 Tầng (Train-Only 3-Tier Vmax)",
        f"Giải EC-1 ({ms['miss_pct']} thiếu Vmax)",
        "#0284C7",
    )
    # Train-only lock banner
    _rbox(ax, 0.42, 4.10, 8.17, 0.44, "#E0F2FE", "#0284C7", lw=1.2, radius=0.09, zorder=2)
    ax.text(
        4.50,
        4.32,
        "KHÓA CHỐNG RÒ RỈ (TRAIN-ONLY): Chỉ học bảng tra cứu từ dữ liệu quá khứ (TRAIN), áp cố định cho VAL & TEST",
        fontsize=8.5,
        fontweight="bold",
        color="#0F172A",
        ha="center",
        va="center",
        zorder=3,
    )

    tiers = [
        (
            3.34,
            8.17,
            "#DCFCE7",
            "#15803D",
            f"Gốc hợp lệ ({ms['t0']} cửa sổ)",
            "Đường có sẵn biển tốc độ hợp lệ → Giữ nguyên (vd: Điện Biên Phủ = 60 km/h)",
        ),
        (
            2.58,
            7.65,
            "#E0F2FE",
            "#0284C7",
            f"Tầng 1 — P85 Lịch sử Đoạn ({ms['t1']})",
            "Lấy phân vị 85% vận tốc lúc đường thoáng của chính đoạn đó (Lý Thường Kiệt → 40 km/h)",
        ),
        (
            1.82,
            7.13,
            "#FEF3C7",
            "#D97706",
            f"Tầng 2 — Trung vị Cấp × Loại ({ms['t2']})",
            "Đoạn ít lịch sử → Mượn tốc độ trung vị các đường cùng cấp & loại đường trên TRAIN",
        ),
        (
            1.06,
            6.61,
            "#F3E8FF",
            "#7C3AED",
            f"Tầng 3 — Quy chuẩn Đô thị ({ms['t3']})",
            "Dự phòng cuối theo cấp đường 1..4: gán chuẩn đô thị 80 / 60 / 50 / 40 km/h",
        ),
    ]
    for idx, (ty, tw, fc, ec, head, desc) in enumerate(tiers):
        tx = 0.42 + (8.17 - tw) / 2
        _rbox(ax, tx, ty, tw, 0.60, fc, ec, lw=1.4, radius=0.09, zorder=3)
        ax.text(tx + 0.16, ty + 0.41, head, fontsize=8.6, fontweight="bold", color="#0F172A", va="center", zorder=4)
        ax.text(tx + 0.16, ty + 0.16, desc, fontsize=8.0, color="#334155", va="center", zorder=4)
        if idx < len(tiers) - 1:
            _arrow(ax, 4.50, ty - 0.01, 4.50, ty - 0.16, color="#64748B", lw=1.6)

    _rbox(ax, 0.42, 0.30, 8.17, 0.56, "#0F172A", "#0F172A", lw=1.2, radius=0.10, zorder=3)
    ax.text(
        4.50,
        0.58,
        "KẾT QUẢ: 100% cửa sổ có Vmax → Tính tỷ lệ r = vận tốc thực / Vmax → Chuẩn hóa 6 mức LOS A–F",
        fontsize=8.6,
        fontweight="bold",
        color="#38BDF8",
        ha="center",
        va="center",
        zorder=4,
    )


def _draw_monoid_panel(ax: plt.Axes) -> None:
    _panel_header(
        ax,
        9.12,
        0.15,
        8.80,
        5.10,
        "2. Bộ Gộp 5-Tuple Monoid & Chia Giỏ Key Salting",
        "Giải EC-8 (Sai số = 0.0)",
        "#0F172A",
    )
    # Input toy example banner
    _rbox(ax, 9.34, 4.10, 8.36, 0.44, "#F1F5F9", "#64748B", lw=1.2, radius=0.09, zorder=2)
    ax.text(
        13.52,
        4.32,
        "VÍ DỤ MINH HỌA: 4 xe trên cùng đoạn đường nằm ở 2 máy — "
        "Máy A: [10 km/h] · Máy B: [40, 40, 40 km/h] (Chuẩn = 32.5)",
        fontsize=8.2,
        fontweight="bold",
        color="#0F172A",
        ha="center",
        va="center",
        zorder=3,
    )

    # Left sub-box: WRONG naive partial average
    _rbox(ax, 9.34, 1.12, 3.86, 2.82, "#FEF2F2", "#DC2626", lw=1.6, radius=0.11, zorder=2)
    ax.text(
        11.27,
        3.68,
        "CÁCH NGÂY THƠ: GỘP AVG() [SAI]",
        fontsize=8.8,
        fontweight="bold",
        color="#B91C1C",
        ha="center",
        va="center",
        zorder=3,
    )
    _rbox(ax, 9.52, 2.86, 1.66, 0.56, "white", "#EF4444", lw=1.1, radius=0.08, zorder=3)
    ax.text(10.35, 3.14, "Máy A (1 xe)\navg = 10.0", fontsize=8.0, ha="center", va="center", color="#0F172A", zorder=4)
    _rbox(ax, 11.36, 2.86, 1.66, 0.56, "white", "#EF4444", lw=1.1, radius=0.08, zorder=3)
    ax.text(12.19, 3.14, "Máy B (3 xe)\navg = 40.0", fontsize=8.0, ha="center", va="center", color="#0F172A", zorder=4)
    _arrow(ax, 10.35, 2.84, 11.05, 2.34, color="#DC2626", lw=1.6)
    _arrow(ax, 12.19, 2.84, 11.49, 2.34, color="#DC2626", lw=1.6)
    _rbox(ax, 9.52, 1.30, 3.50, 1.00, "#FEE2E2", "#DC2626", lw=1.3, radius=0.09, zorder=3)
    ax.text(
        11.27,
        1.80,
        "Lấy trung bình của trung bình:\n(10.0 + 40.0) / 2 = 25.0 km/h\n→ LỆCH 7.5 km/h! (Thực tế lệch 11.11)",
        fontsize=8.0,
        fontweight="bold",
        color="#991B1B",
        ha="center",
        va="center",
        zorder=4,
    )

    # Right sub-box: RIGHT 5-Tuple Monoid + Key Salting
    _rbox(ax, 13.42, 1.12, 4.28, 2.82, "#ECFDF5", "#0D9488", lw=1.6, radius=0.11, zorder=2)
    ax.text(
        15.56,
        3.68,
        "5-TUPLE MONOID + SALTING [ĐÚNG]",
        fontsize=8.8,
        fontweight="bold",
        color="#0F766E",
        ha="center",
        va="center",
        zorder=3,
    )
    _rbox(ax, 13.58, 2.86, 1.90, 0.56, "white", "#0D9488", lw=1.1, radius=0.08, zorder=3)
    ax.text(
        14.53,
        3.14,
        "Giỏ A: (cnt=1, sum=10,\nmin=10, max=10, sev=1)",
        fontsize=7.6,
        ha="center",
        va="center",
        color="#0F172A",
        zorder=4,
    )
    _rbox(ax, 15.62, 2.86, 1.92, 0.56, "white", "#0D9488", lw=1.1, radius=0.08, zorder=3)
    ax.text(
        16.58,
        3.14,
        "Giỏ B: (cnt=3, sum=120,\nmin=40, max=40, sev=0)",
        fontsize=7.6,
        ha="center",
        va="center",
        color="#0F172A",
        zorder=4,
    )
    _arrow(ax, 14.53, 2.84, 15.32, 2.34, color="#0D9488", lw=1.6)
    _arrow(ax, 16.58, 2.84, 15.80, 2.34, color="#0D9488", lw=1.6)
    _rbox(ax, 13.58, 1.30, 3.96, 1.00, "#CCFBF1", "#0F766E", lw=1.3, radius=0.09, zorder=3)
    ax.text(
        15.56,
        1.80,
        "Cộng gộp bộ 5 số: (cnt=4, sum=130, ...)\n"
        "Bước cuối mới chia: 130 / 4 = 32.5 km/h\n"
        "→ SAI SỐ = 0.0 DÙ CHIA N GIỎ BẤT KỲ!",
        fontsize=8.0,
        fontweight="bold",
        color="#065F46",
        ha="center",
        va="center",
        zorder=4,
    )

    ms = _load_method_stats()
    _rbox(ax, 9.34, 0.30, 8.36, 0.64, "#0F172A", "#0F172A", lw=1.2, radius=0.10, zorder=3)
    ax.text(
        13.52,
        0.62,
        "2-STAGE KEY SALTING: Gắn đuôi ngẫu nhiên 0..7 chia đều giờ cao điểm ra 8 giỏ "
        f"(giảm lệch tải {ms['skew_raw']} → {ms['skew_salt']}),\n"
        "sau đó cộng gộp các bộ 5-Tuple Monoid để vừa chống treo máy vừa giữ kết quả chính xác tuyệt đối!",
        fontsize=8.0,
        fontweight="bold",
        color="#E2E8F0",
        ha="center",
        va="center",
        zorder=4,
    )


def render_methods_imputation_monoid(out_path: Path) -> Path:
    """Render Visual Explainer Slide 1: 3-Tier Vmax Imputation & 5-Tuple Monoid + Key Salting."""
    fig, ax = plt.subplots(figsize=(18.1, 5.40), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.1)
    ax.set_ylim(0.0, 5.40)
    ax.axis("off")
    _draw_imputation_panel(ax)
    _draw_monoid_panel(ax)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _draw_pagerank_col(ax: plt.Axes) -> None:
    ms = _load_method_stats()
    _panel_header(
        ax,
        0.16,
        0.15,
        5.80,
        5.10,
        "3. Cắt Điểm Uốn & Line-Graph PageRank",
        "Tìm nút cổ chai",
        "#0D9488",
    )
    # Step A: Shape point pruning
    _rbox(ax, 0.34, 3.38, 5.44, 1.12, "white", "#CBD5E1", lw=1.2, radius=0.10, zorder=2)
    ax.text(
        0.50,
        4.28,
        f"Bước A — Cắt tỉa {ms['pruned_pct']} điểm uốn cong bản đồ OSM:",
        fontsize=8.3,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    # Curved polyline with shape points vs pruned edge
    pts_x = [0.65, 1.10, 1.55, 2.00, 2.45]
    pts_y = [3.78, 3.98, 3.66, 3.94, 3.78]
    ax.plot(pts_x, pts_y, color="#94A3B8", lw=2.2, zorder=3)
    ax.scatter([pts_x[0], pts_x[-1]], [pts_y[0], pts_y[-1]], s=65, color="#0F172A", zorder=5)
    ax.scatter(pts_x[1:-1], pts_y[1:-1], s=42, color="#EF4444", zorder=5)
    ax.text(1.55, 3.49, "3 chấm đỏ chỉ vẽ độ cong", fontsize=7.4, color="#B91C1C", ha="center", zorder=4)
    _arrow(ax, 2.65, 3.80, 3.25, 3.80, color="#0D9488", lw=2.0)
    ax.plot([3.45, 5.35], [3.80, 3.80], color="#0D9488", lw=3.0, zorder=3)
    ax.scatter([3.45, 5.35], [3.80, 3.80], s=70, color="#0F172A", zorder=5)
    ax.text(4.40, 3.50, "1 đoạn đường nối 2 ngã tư thật", fontsize=7.5, fontweight="bold", color="#0F766E", ha="center")

    # Step B: Directed Line-Graph PageRank
    _rbox(ax, 0.34, 1.06, 5.44, 2.16, "#F0FDFA", "#0D9488", lw=1.3, radius=0.10, zorder=2)
    ax.text(
        0.50,
        2.98,
        "Bước B — Đảo sang Đồ thị Đoạn đường (Line-Graph):",
        fontsize=8.3,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    feeders = [(0.55, 2.38, "Nhánh A"), (0.55, 1.82, "Nhánh B"), (0.55, 1.26, "Nhánh C")]
    for fx, fy, flbl in feeders:
        _rbox(ax, fx, fy, 1.35, 0.42, "white", "#0284C7", lw=1.2, radius=0.08, zorder=3)
        ax.text(fx + 0.67, fy + 0.21, flbl, fontsize=8.0, fontweight="bold", color="#0369A1", ha="center", va="center")
        _arrow(ax, fx + 1.38, fy + 0.21, 2.52, 2.03, color="#0284C7", lw=1.7)

    _rbox(ax, 2.56, 1.62, 1.82, 0.82, "#FEE2E2", "#DC2626", lw=1.8, radius=0.10, zorder=4)
    ax.text(
        3.47,
        2.03,
        f"Đoạn Cổ Chai D\nPageRank = {ms['top_pr']}\n(Dòng xe dồn tụ!)",
        fontsize=8.0,
        fontweight="bold",
        color="#991B1B",
        ha="center",
        va="center",
        zorder=5,
    )
    _arrow(ax, 4.41, 2.03, 4.82, 2.03, color="#DC2626", lw=1.9)
    _rbox(ax, 4.85, 1.80, 0.78, 0.46, "white", "#64748B", lw=1.1, radius=0.08, zorder=3)
    ax.text(5.24, 2.03, "Lối ra\nhẹp", fontsize=7.5, color="#334155", ha="center", va="center")

    _rbox(ax, 0.34, 0.30, 5.44, 0.62, "#0F172A", "#0F172A", lw=1.1, radius=0.09, zorder=3)
    ax.text(
        3.06,
        0.61,
        "Ý NGHĨA: Mỗi đoạn đường là 1 nút (loại U-turn). Nhiều đường đổ vào\n"
        f"1 lối hẹp → PageRank cao vọt ({ms['top_street']}, Gò Dưa) dù thiếu GPS!",
        fontsize=7.7,
        fontweight="bold",
        color="#5EEAD4",
        ha="center",
        va="center",
        zorder=4,
    )


def _draw_coldstart_col(ax: plt.Axes) -> None:
    ms = _load_method_stats()
    _panel_header(
        ax,
        6.14,
        0.15,
        5.82,
        5.10,
        "4. Phân Cụm 7D K-Means Cho Vùng Mù",
        f"{ms['cold_pct']} Cold-Start",
        "#2563EB",
    )
    _rbox(ax, 6.32, 3.38, 5.46, 1.12, "white", "#CBD5E1", lw=1.2, radius=0.10, zorder=2)
    ax.text(
        6.48,
        4.28,
        f"Thách thức: {ms['cold_segs']} / {ms['segs']} đoạn cơ giới có 0 bản tin GPS",
        fontsize=8.3,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    _rbox(ax, 6.50, 3.52, 2.35, 0.56, "#E2E8F0", "#64748B", lw=1.3, radius=0.08, zorder=3)
    ax.text(
        7.67,
        3.80,
        "Đoạn X (Vùng mù: 0 GPS)\nTrạng thái = ???",
        fontsize=7.9,
        fontweight="bold",
        color="#334155",
        ha="center",
        va="center",
    )
    _arrow(ax, 8.90, 3.80, 9.35, 3.80, color="#2563EB", lw=1.8)
    _rbox(ax, 9.40, 3.52, 2.20, 0.56, "#DBEAFE", "#2563EB", lw=1.3, radius=0.08, zorder=3)
    ax.text(
        10.50,
        3.80,
        "Đo 7 đặc trưng bản đồ\n(PageRank, bậc, cấp, Vmax...)",
        fontsize=7.7,
        fontweight="bold",
        color="#1E3A8A",
        ha="center",
        va="center",
    )

    # Clustering visual box
    _rbox(ax, 6.32, 1.06, 5.46, 2.16, "#EFF6FF", "#2563EB", lw=1.3, radius=0.10, zorder=2)
    ax.text(
        6.48,
        3.00,
        f"Gom {ms['segs']} đoạn thành 6 nhóm hành vi đường phố (k = 6):",
        fontsize=8.3,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    ax.text(
        9.05,
        2.70,
        "Chấm tròn = Đường có GPS   ·   Ô vuông đen = Đường vùng mù ghép cùng cụm",
        fontsize=7.4,
        fontweight="bold",
        color="#1E40AF",
        ha="center",
        va="center",
    )
    # Cluster circles
    clusters = [
        (7.42, 1.96, "#FEE2E2", "#DC2626", "Cụm 1: Trục nội đô\nhay kẹt (E/F 43.3%)"),
        (9.12, 1.96, "#DBEAFE", "#2563EB", "Cụm 4: Nút cổ chai\nPageRank cao nhất"),
        (10.80, 1.96, "#F1F5F9", "#64748B", "Cụm 2: Đường nhánh\nthông thoáng (53.3k)"),
    ]
    for cx, cy, fc, ec, lbl in clusters:
        ax.add_patch(Circle((cx, cy + 0.08), 0.45, facecolor=fc, edgecolor=ec, lw=1.5, zorder=3))
        ax.scatter([cx - 0.16, cx + 0.15], [cy + 0.20, cy + 0.16], s=34, color=ec, zorder=4)
        ax.scatter([cx], [cy - 0.08], s=48, color="#0F172A", marker="s", zorder=5)
        ax.text(cx, cy - 0.56, lbl, fontsize=7.4, fontweight="bold", color="#0F172A", ha="center", va="center")

    _rbox(ax, 6.32, 0.30, 5.46, 0.62, "#0F172A", "#0F172A", lw=1.1, radius=0.09, zorder=3)
    ax.text(
        9.05,
        0.61,
        "Ý NGHĨA: Đường thiếu GPS mượn đặc trưng nhóm đường tương đồng\n"
        f"→ Giúp Random Forest dự báo phủ kín 100% mạng lưới ({ms['segs']} đoạn)!",
        fontsize=7.8,
        fontweight="bold",
        color="#93C5FD",
        ha="center",
        va="center",
        zorder=4,
    )


def _draw_sketches_col(ax: plt.Axes) -> None:
    ms = _load_method_stats()
    _panel_header(
        ax,
        12.14,
        0.15,
        5.80,
        5.10,
        "5. Bộ Đếm Luồng O(1) Sketches (33 KiB)",
        "Không tràn RAM",
        "#7C3AED",
    )
    # 1. Bloom Filter visual bit array
    _rbox(ax, 12.32, 3.42, 5.44, 1.08, "#FAF5FF", "#7C3AED", lw=1.3, radius=0.10, zorder=2)
    ax.text(
        12.48,
        4.28,
        "RotatingBloom (32.0 KiB · 2×16 KiB) — Lọc tin trùng:",
        fontsize=8.2,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    bits = [0, 1, 0, 0, 1, 0, 1, 0, 0, 1]
    for b_idx, b_val in enumerate(bits):
        bx = 12.50 + b_idx * 0.33
        fc = "#7C3AED" if b_val == 1 else "white"
        tc = "white" if b_val == 1 else "#475569"
        _rbox(ax, bx, 3.60, 0.28, 0.42, fc, "#6D28D9", lw=1.0, radius=0.04, zorder=3)
        ax.text(bx + 0.14, 3.81, str(b_val), fontsize=8.2, fontweight="bold", color=tc, ha="center", va="center")
    ax.text(
        15.92,
        3.81,
        "Cả k=10 ô = 1\n→ CHẶN TIN TRÙNG!\n(Cố định 32 KiB)",
        fontsize=7.6,
        fontweight="bold",
        color="#5B21B6",
        ha="left",
        va="center",
    )

    # 2. HLL++ & EWMA vs RF
    _rbox(ax, 12.32, 2.24, 5.44, 1.04, "white", "#CBD5E1", lw=1.2, radius=0.10, zorder=2)
    ax.text(
        12.48,
        3.04,
        "HyperLogLog++ (1.0 KiB) — Đếm nhanh số đường hoạt động:",
        fontsize=8.2,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    ax.text(
        12.48,
        2.60,
        "Quan sát chuỗi số 0 đầu mã băm (0001... hiếm = nhiều phần tử)\n"
        f"→ Đếm ~{ms['active_segs']} đoạn đường độc lập chỉ với 1 KiB RAM (sai số {ms['hll_err']}).",
        fontsize=7.7,
        color="#334155",
        va="center",
        zorder=3,
    )

    _rbox(ax, 12.32, 1.06, 5.44, 1.04, "#FEF3C7", "#D97706", lw=1.3, radius=0.10, zorder=2)
    ax.text(
        12.48,
        1.86,
        "EWMA Hotspot (α = 0.15) bổ trợ 2 chiều cùng Random Forest:",
        fontsize=8.2,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    ax.text(
        12.48,
        1.42,
        "• EWMA cộng dồn có phai mờ → Bắt điểm ĐANG kẹt lì nhiều giờ.\n"
        "• Random Forest (23 biến) → Dự báo điểm SẮP kẹt sau +30 phút!",
        fontsize=7.7,
        fontweight="bold",
        color="#92400E",
        va="center",
        zorder=3,
    )

    _rbox(ax, 12.32, 0.30, 5.44, 0.62, "#0F172A", "#0F172A", lw=1.1, radius=0.09, zorder=3)
    ax.text(
        15.04,
        0.61,
        "Ý NGHĨA: Dù mở rộng lên 8–17 tỷ bản tin/ngày, bộ nhớ luồng\n"
        "vẫn giữ cố định 33.0 KiB O(1), tuyệt đối không tràn RAM!",
        fontsize=7.8,
        fontweight="bold",
        color="#DDD6FE",
        ha="center",
        va="center",
        zorder=4,
    )


def render_methods_graph_coldstart_sketches(out_path: Path) -> Path:
    """Render Visual Explainer Slide 2: Line-Graph PageRank, 7D K-Means Cold-Start & O(1) Sketches."""
    fig, ax = plt.subplots(figsize=(18.1, 5.40), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.1)
    ax.set_ylim(0.0, 5.40)
    ax.axis("off")
    _draw_pagerank_col(ax)
    _draw_coldstart_col(ax)
    _draw_sketches_col(ax)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def main() -> None:
    p1 = render_methods_imputation_monoid(CHART_DIR / "visual_methods_imputation_monoid.png")
    p2 = render_methods_graph_coldstart_sketches(CHART_DIR / "visual_methods_graph_coldstart_sketches.png")
    print(f"Rendered method explainers: {p1.name} ({p1.stat().st_size:,} B), {p2.name} ({p2.stat().st_size:,} B)")


if __name__ == "__main__":
    main()
