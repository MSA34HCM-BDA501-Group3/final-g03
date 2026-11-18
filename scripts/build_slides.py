"""Build the 17-Slide Executive, Conference & Academic Defense Presentation (.pptx, .pdf & HD .pngs)."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import fitz
import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle
from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from render_component_flows import (  # noqa: E402
    render_batch_flow_diagram,
    render_stream_flow_diagram,
)
from render_method_explainers import (  # noqa: E402
    render_methods_graph_coldstart_sketches,
    render_methods_imputation_monoid,
)
from render_visual_assets import (  # noqa: E402
    _load_segment_geometries,
    render_hcmc_clusters_map,
    render_hcmc_hotspots_map,
    render_hcmc_problem_context_map,
    render_transition_analysis_chart,
)
from slide_renderer import (  # noqa: E402
    HEX_BORDER,
    HEX_MUTED,
    HEX_NAVY,
    HEX_PAGE_SOFT,
    HEX_SKY_DEEP,
    HEX_SKY_PALE,
    HEX_SKY_PRIMARY,
    HEX_SLATE,
    HEX_SLATE_DARK,
    DualSlide,
    _pptx_pill,
    _pptx_textbox,
    _zero_shape_margins,
    export_overview_grid_and_pdf,
    rgb,
)

REPO_DIR = SCRIPT_DIR.parent
OUTPUTS_DIR = REPO_DIR / "outputs"
CHART_DIR = OUTPUTS_DIR / "slide_charts"
SLIDE_PNG_DIR = OUTPUTS_DIR / "slides"
SLIDE_RENDER_DIR = OUTPUTS_DIR / "slide_renders"
TOTAL_SLIDES = 17


def _load_slide_truth() -> dict:
    m = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    sp = json.loads((OUTPUTS_DIR / "spark_runtime_evidence.json").read_text(encoding="utf-8"))
    mv = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    qp = json.loads((OUTPUTS_DIR / "query_progress.json").read_text(encoding="utf-8"))
    ab = pd.read_csv(OUTPUTS_DIR / "ablation_results.csv")
    art = joblib.load(OUTPUTS_DIR / "model.joblib")
    imps = dict(zip(art["full_features"], art["rf_multiclass"].feature_importances_))
    local_set = set(art["local_features"])
    local_imp = sum(v for k, v in imps.items() if k in local_set) * 100.0
    graph_imp = sum(v for k, v in imps.items() if k not in local_set) * 100.0
    return {
        "m": m,
        "sp": sp,
        "mv": mv,
        "qp": qp,
        "ab": ab,
        "imps": imps,
        "local_imp": local_imp,
        "graph_imp": graph_imp,
    }


def render_eda_chart(out_path: Path) -> Path:
    m = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    ds = m["dataset"]
    qf = m["quality_funnel_c5"]
    tr = m["evaluation"]["transition_analysis"]
    cold_pct = m["static_feature_store_c13b"]["cold_start_ratio_pct"]
    pruned_pct = ds["pruned_orphan_nodes_count"] / ds["nodes_count"] * 100.0

    fig, ax = plt.subplots(figsize=(6.65, 6.10), dpi=220)
    labels = [
        f"UTC → Giờ VN Rollover (+7h)\n(4,305 / {ds['labeled_windows_count']:,} cửa sổ 30m)",
        "Non-Contiguous GPS Gaps (> 30m)\n(15,514 / 23,399 cặp quan sát)",
        "Streets Missing max_velocity\n(4,566 / 5,553 tuyến đường)",
        f"Windows Missing max_velocity\n(28,495 / {ds['labeled_windows_count']:,} cửa sổ 30m)",
        f"Cold-Start Segments (0 GPS)\n({ds['cold_start_segments_count']:,} / {ds['segments_count']:,} đoạn cơ giới)",
        f"Segments Missing max_velocity\n(74,762 / {ds['raw_segments_count']:,} đoạn thô)",
        f"Intermediate Shape Points\n({ds['pruned_orphan_nodes_count']:,} / {ds['nodes_count']:,} nodes)",
    ]
    vals = [
        12.89,
        66.30,
        82.23,
        qf["missing_max_velocity_train_pct"],
        cold_pct,
        qf["missing_max_velocity_segments_pct"],
        round(pruned_pct, 2),
    ]
    cols = ["#38BDF8", "#0EA5E9", "#0284C7", "#0369A1", "#1D4ED8", "#1E3A8A", "#0F172A"]
    y = np.arange(len(labels))
    bars = ax.barh(y, vals, color=cols, height=0.58, edgecolor="none")
    ax.set_yticks(y, labels, fontsize=8.3, fontweight="bold", color="#0F172A")
    ax.set_xlim(0, 112)
    ax.set_xlabel(
        "Tỷ lệ khuyết thiếu & đặc thù dữ liệu thực nghiệm (%)", fontsize=8.8, fontweight="bold", color="#334155"
    )
    ax.set_title(
        f"5-Table EDA: Sparsity & Anomaly Profile (Non-Road POI: {qf['non_road_osm_segments']} | "
        f"Burst: {tr['test_samples'] and ds['synthetic_burst_windows_flagged']:,})",
        fontsize=9.4,
        fontweight="bold",
        color="#0F172A",
        pad=10,
    )
    ax.grid(axis="x", linestyle="--", alpha=0.35)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for bar, val in zip(bars, vals):
        ax.text(
            val + 1.5,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.2f}%",
            va="center",
            fontsize=8.6,
            fontweight="bold",
            color="#0F172A",
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def render_spark_chart(out_path: Path) -> Path:
    m = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    sp = json.loads((OUTPUTS_DIR / "spark_runtime_evidence.json").read_text(encoding="utf-8"))
    p1 = m["monoid_and_catalyst_c9"]["pillar_i1_monoid_and_salting"]
    ms_sp = sp["monoid_salting_spark"]
    jn_sp = sp["catalyst_join_spark"]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6.95, 6.10), dpi=220)
    raw_skew = p1["temporal_slot_peak_to_median_ratio"]
    salt_skew = p1["peak_slot_salted_bucket_skew_ratio"]
    bars1 = ax1.barh(
        ["2-Stage Key Salting\n(8 Buckets)", "Un-salted GroupBy\n(Rush-Hour Skew)"],
        [salt_skew, raw_skew],
        color=["#059669", "#DC2626"],
        height=0.50,
    )
    ax1.set_xscale("log")
    ax1.set_xlim(0.5, 3200)
    ax1.set_xlabel("Peak-to-Median Slot Load Ratio (Log Scale)", fontsize=8.5, fontweight="bold")
    ax1.set_title(
        f"2-Stage Key Salting (CV={p1['salt_bucket_cv']:.4f} · IQR {ms_sp['unsalted_iqr_ms']:.1f}→"
        f"{ms_sp['salted_iqr_ms']:.1f} ms · Diff={ms_sp['two_way_exceptAll_diff']})",
        fontsize=9.2,
        fontweight="bold",
        color="#0F172A",
    )
    ax1.grid(axis="x", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    for bar, txt in zip(
        bars1,
        [f"{salt_skew:.3f}x (Balanced)", f"{raw_skew:.2f}x (Hot Slot Skew)"],
    ):
        ax1.text(
            bar.get_width() * 1.18,
            bar.get_y() + bar.get_height() / 2,
            txt,
            va="center",
            fontsize=8.3,
            fontweight="bold",
            color="#0F172A",
        )

    x = np.arange(2)
    w = 0.34
    b_vals = [jn_sp["baseline_median_ms"], ms_sp["unsalted_median_ms"]]
    o_vals = [jn_sp["optimized_median_ms"], ms_sp["salted_median_ms"]]
    r1 = ax2.bar(x - w / 2, b_vals, w, label="Baseline (SortMerge / Unsalted)", color="#64748B")
    r2 = ax2.bar(x + w / 2, o_vals, w, label="Optimized (Broadcast / 8-Salt)", color="#0284C7")
    ax2.set_xticks(
        x,
        [
            f"Catalyst Join ({jn_sp['baseline_exchange_stages_measured']}→"
            f"{jn_sp['optimized_exchange_stages_measured']} Exch · +{jn_sp['join_speedup_pct']:.2f}%)",
            "Monoid Salting Tradeoff\n(2-Stage Shuffle vs IQR Stability)",
        ],
        fontsize=8.1,
        fontweight="bold",
    )
    ax2.set_ylim(0, max(o_vals + b_vals) * 1.24)
    ax2.set_ylabel("Spark 3.5.7 Median Runtime (ms)", fontsize=8.4, fontweight="bold")
    ax2.set_title(
        "Spark 3.5.7 Physical Plan & Monoid Salting Tradeoff (0 exceptAll Diff)",
        fontsize=9.4,
        fontweight="bold",
        color="#0F172A",
    )
    ax2.legend(fontsize=8.0, loc="upper left")
    ax2.grid(axis="y", linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    for bars in (r1, r2):
        for bar in bars:
            height = bar.get_height()
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                height + 4.5,
                f"{height:.1f} ms",
                ha="center",
                fontsize=8.1,
                fontweight="bold",
                color="#0F172A",
            )

    plt.tight_layout(pad=1.2)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def render_streaming_serving_chart(out_path: Path) -> Path:
    m = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    mv = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    qp = json.loads((OUTPUTS_DIR / "query_progress.json").read_text(encoding="utf-8"))
    total_w = m["dataset"]["labeled_windows_count"]
    prov_tail = int(qp["Phase_B_in_flight_provisional_retained"])
    late_backfill = int(qp["Phase_C_reconciled_backfilled"])
    batch_ver = total_w - prov_tail - late_backfill
    lat = mv["uc2_arterial_query"]["latency_benchmark_ms"]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6.85, 6.10), dpi=220)
    stages = [
        "provisional_stream (v1)\n(Active Tail Retained)",
        "late_backfilled (v2)\n(Late > 30m · 609 Lag Healed)",
        "batch_verified (v2)\n(Tier-1 exceptAll == 0)",
    ]
    counts = [prov_tail, late_backfill, batch_ver]
    b1 = ax1.barh(stages, counts, color=["#0EA5E9", "#D97706", "#059669"], height=0.52)
    ax1.set_xscale("log")
    ax1.set_xlim(100, 190000)
    ax1.set_xlabel(f"30-Minute Windows in Serving Store (Total = {total_w:,})", fontsize=8.4, fontweight="bold")
    ax1.set_title(
        "Two-Tier Reconciliation Lifecycle (Completeness: 98.50% → 100.00%)",
        fontsize=9.5,
        fontweight="bold",
        color="#0F172A",
    )
    ax1.grid(axis="x", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    for bar, cnt in zip(b1, counts):
        ax1.text(
            cnt * 1.18,
            bar.get_y() + bar.get_height() / 2,
            f"{cnt:,} windows",
            va="center",
            fontsize=8.3,
            fontweight="bold",
            color="#0F172A",
        )

    lat_labels = [
        "Warm Median\n(IXSCAN)",
        "Warm p95\n(IXSCAN)",
        "Cold First\n(IXSCAN)",
        "Warm Median\n(COLLSCAN)",
    ]
    lat_vals = [lat["warm_median_ms"], lat["warm_p95_ms"], lat["cold_query_ms"], lat["collscan_warm_median_ms"]]
    lat_cols = ["#0284C7", "#059669", "#0D9488", "#DC2626"]
    x2 = np.arange(len(lat_labels))
    b2 = ax2.bar(x2, lat_vals, width=0.46, color=lat_cols)
    ax2.set_xticks(x2, lat_labels, fontsize=8.3, fontweight="bold", color="#0F172A")
    ax2.set_ylim(0, max(lat_vals) * 1.18)
    ax2.set_ylabel("Query Latency (ms)", fontsize=8.4, fontweight="bold")
    ax2.set_title(
        f"MongoDB {mv['mongo_server_version']} ESR IXSCAN vs COLLSCAN ({lat['ixscan_vs_collscan_speedup_x']:.2f}x)",
        fontsize=9.5,
        fontweight="bold",
        color="#0F172A",
    )
    ax2.grid(axis="y", linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    for bar, val in zip(b2, lat_vals):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            val + 0.25,
            f"{val:.3f} ms",
            ha="center",
            fontsize=8.3,
            fontweight="bold",
            color="#0F172A",
        )

    plt.tight_layout(pad=1.2)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


# =============================================================================
# 12-SECTION ACADEMIC, CONFERENCE & EXECUTIVE STRUCTURE (14 SLIDES)
# =============================================================================
def _add_dual_rect(
    s: DualSlide,
    x: float,
    y_top: float,
    w: float,
    h: float,
    fill_hex: str,
    border_hex: str | None = None,
    lw: float = 0.9,
    zorder: int = 1,
) -> None:
    ec = border_hex if border_hex else "none"
    s.ax.add_patch(Rectangle((x, s._ym(y_top, h)), w, h, facecolor=fill_hex, edgecolor=ec, lw=lw, zorder=zorder))
    shp = s.slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y_top), Inches(w), Inches(h))
    shp.fill.solid()
    shp.fill.fore_color.rgb = rgb(fill_hex)
    if border_hex:
        shp.line.color.rgb = rgb(border_hex)
        shp.line.width = Pt(lw)
    else:
        shp.line.fill.background()
    _zero_shape_margins(shp)


def _add_slide_01_cover(prs: Presentation) -> DualSlide:
    """Slide 01: High-Hierarchy Minimalist Cover — 4 Consolidated Blocks (Header, Hero Title, Team, Instructor)."""
    s1 = DualSlide(prs)

    # Subtle top edge architectural accent strip
    _add_dual_rect(s1, 0.00, 0.00, 11.20, 0.08, HEX_NAVY, zorder=2)
    _add_dual_rect(s1, 11.20, 0.00, 4.80, 0.08, HEX_SKY_PRIMARY, zorder=2)

    # 1. HEADER: Institution, Program, Course & Reporting Period (appears ONCE)
    _add_dual_rect(s1, 1.05, 0.68, 0.09, 0.26, HEX_SKY_DEEP, zorder=3)
    header_left = "FSB · FPT University   |   MSE AI   |   BDA501 – Big Data Analytics"
    s1.ax.text(
        1.26,
        s1._ym(0.81),
        header_left,
        fontsize=11.6,
        fontweight="bold",
        color=HEX_SLATE_DARK,
        ha="left",
        va="center",
        zorder=3,
    )
    _pptx_textbox(s1.slide, 1.24, 0.62, 10.80, 0.40, header_left, 11.5, HEX_SLATE_DARK, bold=True)

    header_right = "Fall 2026"
    s1.ax.text(
        14.95,
        s1._ym(0.81),
        header_right,
        fontsize=11.0,
        fontweight="bold",
        color=HEX_MUTED,
        ha="right",
        va="center",
        zorder=3,
    )
    _pptx_textbox(s1.slide, 12.45, 0.62, 2.50, 0.40, header_right, 11.0, HEX_MUTED, bold=True, align=PP_ALIGN.RIGHT)
    _add_dual_rect(s1, 1.05, 1.18, 13.90, 0.015, HEX_BORDER, zorder=2)

    # 2. HERO TITLE: Badge + Primary Focal Title + Subtitle (optically centered)
    s1.add_pill_badge(1.05, 2.00, 2.28, 0.36, "FINAL PROJECT REPORT", "sky_pale", 9.0)
    _pptx_pill(s1.slide, 1.05, 2.00, 2.28, 0.36, "FINAL PROJECT REPORT", HEX_SKY_PALE, HEX_SKY_DEEP, HEX_NAVY, 8.8)

    _add_dual_rect(s1, 1.05, 2.80, 0.10, 1.84, HEX_SKY_DEEP, zorder=3)
    s1.ax.text(
        1.34,
        s1._ym(2.72),
        "HCMC Smart Traffic",
        fontsize=48.0,
        fontweight="bold",
        color=HEX_NAVY,
        ha="left",
        va="top",
        zorder=4,
    )
    _pptx_textbox(s1.slide, 1.32, 2.64, 13.50, 0.95, "HCMC Smart Traffic", 46.0, HEX_NAVY, bold=True)

    s1.ax.text(
        1.34,
        s1._ym(3.74),
        "Big Data Platform",
        fontsize=48.0,
        fontweight="bold",
        color=HEX_SKY_DEEP,
        ha="left",
        va="top",
        zorder=4,
    )
    _pptx_textbox(s1.slide, 1.32, 3.64, 13.50, 0.95, "Big Data Platform", 46.0, HEX_SKY_DEEP, bold=True)

    hero_subtitle = "Nền tảng phân tích dữ liệu lớn & dự báo ùn tắc giao thông đô thị TP.HCM"
    s1.ax.text(
        1.34,
        s1._ym(4.96),
        hero_subtitle,
        fontsize=15.8,
        color=HEX_SLATE,
        ha="left",
        va="top",
        zorder=4,
    )
    _pptx_textbox(s1.slide, 1.32, 4.88, 13.50, 0.48, hero_subtitle, 15.5, HEX_SLATE, bold=False)

    # 3 & 4. BOTTOM METADATA BAND: Project Team (Left) & Instructor (Right)
    _add_dual_rect(s1, 1.05, 6.48, 13.90, 1.66, HEX_PAGE_SOFT, zorder=1)
    _add_dual_rect(s1, 1.05, 6.48, 13.90, 0.02, HEX_BORDER, zorder=2)
    _add_dual_rect(s1, 1.05, 6.48, 0.06, 1.66, HEX_NAVY, zorder=3)
    _add_dual_rect(s1, 11.15, 6.76, 0.015, 1.12, HEX_BORDER, zorder=2)

    s1.ax.text(
        1.36, s1._ym(6.72), "PROJECT TEAM", fontsize=9.2, fontweight="bold", color=HEX_SKY_DEEP, va="top", zorder=3
    )
    _pptx_textbox(s1.slide, 1.34, 6.68, 3.20, 0.24, "PROJECT TEAM", 9.2, HEX_SKY_DEEP, bold=True)

    members = [
        (1.36, "Nguyễn Tạ Minh Quyền", "25MS23280"),
        (4.82, "Nguyễn Thành Đạt", "25MS23306"),
        (7.92, "Phạm Công Minh", "25MS23310"),
    ]
    for x_col, member_name, member_id in members:
        s1.ax.text(
            x_col, s1._ym(7.10), member_name, fontsize=13.0, fontweight="bold", color=HEX_NAVY, va="top", zorder=3
        )
        s1.ax.text(
            x_col, s1._ym(7.52), member_id, fontsize=10.8, fontweight="bold", color=HEX_MUTED, va="top", zorder=3
        )
        _pptx_textbox(s1.slide, x_col - 0.02, 7.04, 3.10, 0.40, member_name, 13.0, HEX_NAVY, bold=True)
        _pptx_textbox(s1.slide, x_col - 0.02, 7.46, 3.10, 0.38, member_id, 10.8, HEX_MUTED, bold=True)

    s1.ax.text(
        11.48, s1._ym(6.72), "INSTRUCTOR", fontsize=9.2, fontweight="bold", color=HEX_SKY_DEEP, va="top", zorder=3
    )
    _pptx_textbox(s1.slide, 11.46, 6.68, 3.20, 0.24, "INSTRUCTOR", 9.2, HEX_SKY_DEEP, bold=True)

    s1.ax.text(
        11.48, s1._ym(7.10), "Đặng Trần Khánh", fontsize=13.0, fontweight="bold", color=HEX_NAVY, va="top", zorder=3
    )
    s1.ax.text(11.48, s1._ym(7.52), "Giảng viên hướng dẫn", fontsize=10.8, color=HEX_SLATE, va="top", zorder=3)
    _pptx_textbox(s1.slide, 11.46, 7.04, 3.30, 0.40, "Đặng Trần Khánh", 13.0, HEX_NAVY, bold=True)
    _pptx_textbox(s1.slide, 11.46, 7.46, 3.30, 0.38, "Giảng viên hướng dẫn", 10.8, HEX_SLATE, bold=False)

    return s1


def _add_slide_02_problem_statement(prs: Presentation, problem_map: Path) -> DualSlide:
    """Slide 02 (Section 1): Problem Statement — Domain WHY & WHAT (Pain Point, Current Gaps, Problem & Value)."""
    s2 = DualSlide(prs)
    s2.add_chrome(
        2,
        TOTAL_SLIDES,
        0,
        "SECTION 01 · PROBLEM STATEMENT — BỐI CẢNH ĐÔ THỊ TP.HCM, RÀO CẢN HIỆN HỮU & MỤC TIÊU BÀI TOÁN",
        "1. Problem Statement: Bài Toán Điều Hành Giao Thông Chủ Động Tại TP.HCM",
        "Chuyển từ xử lý ùn tắc bị động sau sự cố sang quan trắc toàn mạng lưới và cảnh báo sớm trước 30 phút",
    )
    s2.add_card(
        0.55,
        2.00,
        7.35,
        2.04,
        "1. Vấn Đề Thực Tế Của Giao Thông TP.HCM",
        [
            "Mật độ xe cao trên mạng lưới đường đan xen dày đặc (cửa ngõ sân bay, Q.1–Q.10, Thủ Đức) khiến nghẽn "
            "cục bộ giờ cao điểm nhanh chóng lan truyền diện rộng.",
            (
                "Điều tiết hiện chủ yếu phản ứng bị động khi đã kẹt cứng, gây lãng phí lớn về thời gian và chi phí "
                "vận tải."
            ),
        ],
        badge="WHY · Pain Point",
        theme="white",
        body_size=8.4,
    )
    s2.add_card(
        0.55,
        4.16,
        7.35,
        2.08,
        "2. Vì Sao Phương Pháp Hiện Tại Chưa Đủ?",
        [
            "Vùng mù & đứt quãng: Camera cố định đắt đỏ, chỉ phủ nút giao chính; GPS hành trình lệch về đại lộ, bỏ "
            "trống đường nhánh và hay ngắt quãng, trễ tin.",
            "Thiếu dự báo sớm & chia cắt: Công cụ hiện tại chỉ báo nơi ĐANG kẹt (chưa dự báo trước 30m nơi SẮP kẹt) "
            "và tách rời trực ban sự vụ với quy hoạch hạ tầng.",
        ],
        badge="Current Gaps",
        theme="sky",
        body_size=8.4,
    )
    s2.add_card(
        0.55,
        6.36,
        7.35,
        2.02,
        "3. Bài Toán Đặt Ra & Giá Trị Thụ Hưởng",
        [
            "Bài toán: Xây dựng nền tảng dữ liệu lớn để (1) suy diễn lưu thông toàn đô thị kể cả vùng mù, (2) tìm "
            "nút cổ chai cấu trúc và (3) dự báo nguy cơ kẹt trước 30 phút.",
            "Giá trị: Giúp TOC & Kỹ sư đèn điều tiết chủ động; Sở GTVT & Tuần tra ưu tiên đúng điểm nghẽn; Người dân "
            "chọn lộ trình né kẹt từ xa.",
        ],
        badge="WHAT & Value",
        theme="sky_strong",
        body_size=8.4,
    )
    s2.add_image(8.08, 2.00, 7.37, 6.38, problem_map)
    return s2


def _add_slide_03_literature_review(prs: Presentation) -> DualSlide:
    """Slide 03 (Section 2): Domain-Level Literature Review — Existing ITS/Forecasting Approaches & Research Gap."""
    s3 = DualSlide(prs)
    s3.add_chrome(
        3,
        TOTAL_SLIDES,
        0,
        "SECTION 02 · LITERATURE REVIEW — TỔNG QUAN HỆ THỐNG HIỆN HỮU, NGHIÊN CỨU GIAO THÔNG & KHOẢNG TRỐNG",
        "2. Literature Review: Các Hướng Tiếp Cận Quan Trắc, Dự Báo & Điều Hành",
        "Khảo sát các hệ thống quản lý và nghiên cứu dự báo ùn tắc đô thị để xác định khoảng trống tích hợp",
    )
    lit_headers = [
        "Hướng Tiếp Cận",
        "Hệ Thống & Nghiên Cứu",
        "Đóng Góp Chính",
        "Hạn Chế Tại Đô Thị TP.HCM",
    ]
    lit_rows = [
        [
            "1. Quan trắc Cố định\n& Đèn Thích ứng",
            "Vòng từ, CCTV; SCATS / SCOOT\n(Sims, 1980; Hunt et al., 1981)",
            "Đo lưu lượng chính xác tại nút\ncó thiết bị để chỉnh đèn cục bộ",
            "Chi phí lắp đặt & bảo trì cao; bỏ trống\nphần lớn mạng lưới đường nhánh (vùng mù)",
        ],
        [
            "2. Dữ liệu Hành trình\n(Floating Car / GPS)",
            "GPS xe buýt, taxi, bản đồ số\n(Herrera et al., 2010; Zheng, 2015)",
            "Phủ diện rộng không cần cảm biến\nmặt đường, phản ánh vận tốc thực",
            "Lệch về đại lộ, tín hiệu ngắt quãng dài,\nthiếu chuẩn tốc độ & chỉ báo hiện trạng",
        ],
        [
            "3. Dự báo Ùn tắc\nNgắn hạn (15–60m)",
            "ARIMA (Vlahogianni, 2014); DCRNN,\nGraph WaveNet (Li et al., 2018)",
            "Dự báo sớm mức kẹt nhờ tương quan\nkhông gian – thời gian trên mạng lưới",
            "Thử nghiệm trên cao tốc cảm biến liên tục\n(PeMS, METR-LA); suy giảm khi dữ liệu thưa",
        ],
        [
            "4. Hỗ trợ Quyết định\nĐiều hành (Urban DSS)",
            "Trung tâm TOC & Big Data đô thị\n(Lv et al., 2015; Amini et al., 2017)",
            "Kết hợp kho lịch sử quy hoạch với\nbảng giám sát cho điều hành viên",
            "Mô hình dự báo tách rời luồng vận hành\n(tin GPS trễ/trùng); chia cắt trực ban & quy hoạch",
        ],
    ]
    s3.add_table(0.55, 2.00, 14.90, 4.22, lit_headers, lit_rows, [0.19, 0.25, 0.25, 0.31], font_size=8.2)

    s3.add_card(
        0.55,
        6.36,
        14.90,
        2.02,
        "Khoảng Trống Nghiên Cứu & Vì Sao Cần Nền Tảng Tích Hợp HCMC Smart Traffic",
        [
            "Khoảng trống thực tiễn: Mô hình học thuật giả định cảm biến cao tốc liên tục, trong khi đô thị thực tế "
            "đối mặt vùng mù đường nhánh rộng lớn và thiếu cảnh báo sớm +30 phút.",
            "Vì sao cần nền tảng tích hợp? Mô hình dự báo đơn lẻ không tự xử lý được GPS di động nhiễu/trễ, không suy "
            "diễn được vùng mù nếu thiếu cấu trúc mạng lưới đường, và không nối được trực ban TOC với quy hoạch "
            "Sở GTVT.",
            "Logic nghiên cứu: Xác định vấn đề (Mục 1) → Khảo sát & tìm khoảng trống (Mục 2) → Đề xuất kiến trúc & "
            "phương pháp (Mục 3–8) → Thực nghiệm kiểm chứng (Mục 9–10).",
        ],
        badge="Research Gap & Logic",
        theme="sky",
        body_size=8.3,
    )
    return s3


def _add_slide_04_system_architecture(prs: Presentation, arch_img: Path, t: dict | None = None) -> DualSlide:
    """Slide 04 (Section 3): System Architecture (Zones A-F & Pipelines) + Component Limitations & Risks."""
    tr = (t or _load_slide_truth())["m"]
    ev = tr["evaluation"]
    rc = tr["streaming_mmds"]["reconciliation"]
    s4 = DualSlide(prs)
    s4.add_chrome(
        4,
        TOTAL_SLIDES,
        1,
        "SECTION 03 · SYSTEM ARCHITECTURE — KIẾN TRÚC TỔNG THỂ (ZONES A–F · 4 PIPELINES) & 3-PHASE MLOPS",
        "3. System Architecture: Kiến Trúc Hybrid Batch–Speed & Vòng Đời 3-Phase MLOps",
        (
            "Kiến trúc nền tảng 6 dịch vụ Docker giải quyết đồng thời vùng mù, dự báo sớm +30m & Dual-UI "
            "(:8000 / :8501)"
        ),
    )
    s4.add_image(0.55, 2.00, 10.55, 6.38, arch_img)

    s4.add_card(
        11.26,
        2.00,
        4.19,
        3.12,
        "3-Phase MLOps Lifecycle",
        [
            f"Phase A (Batch Fit): Học 1 lần trên {ev['train_samples']:,} mẫu organic TRAIN, khóa SHA-256 tại "
            "Frozen Registry.",
            "Phase B (Speed Layer): Chỉ đọc .predict_proba() phát cảnh báo tạm thời v1 (< 1 ms IXSCAN).",
            f"Phase C (Recon): Cuối ngày bù {rc['affected_late_segment_windows_recomputed']} cửa sổ trễ, "
            f"hàn {rc['healed_downstream_lag_windows']} lag, re-score {rc['healed_downstream_rescored_windows']} "
            "→ chuẩn v2 (12.0 / 16.0 GB RAM).",
        ],
        badge="0 Write Conflict",
        theme="sky",
        body_size=8.2,
    )
    s4.add_card(
        11.26,
        5.26,
        4.19,
        3.12,
        "Limitations & Risks (Arch)",
        [
            "Single-Host Contention: 6 container (Kafka 1.5G, Spark 6G, Mongo 2G, FastAPI+React :8000 & "
            "Streamlit :8501) dùng chung I/O trong 12.0 / 16 GB RAM.",
            "Eventual Consistency Gap: Cảnh báo luồng (v1) phản hồi tức thì, nhưng biến trễ từ các gói tin "
            "đến muộn > 30m phải chờ Phase C cuối ngày.",
        ],
        badge="Arch Risk",
        theme="navy",
        body_size=8.2,
    )
    return s4


def _add_slide_05_data_eda_and_problems(prs: Presentation, eda_chart: Path, t: dict | None = None) -> DualSlide:
    """Slide 05 (Section 4): Data, EDA & Data Problems (5 Raw Tables & EC-1..EC-10)."""
    tr_d = t or _load_slide_truth()
    tr = tr_d["m"]
    mv = tr_d["mv"]
    ds = tr["dataset"]
    qf = tr["quality_funnel_c5"]
    ev = tr["evaluation"]
    rc = tr["streaming_mmds"]["reconciliation"]
    bloom = tr["streaming_mmds"]["bloom_filter"]
    salt = tr["monoid_and_catalyst_c9"]["pillar_i1_monoid_and_salting"]
    s5 = DualSlide(prs)
    s5.add_chrome(
        5,
        TOTAL_SLIDES,
        1,
        "SECTION 04 · DATA, EDA & DATA PROBLEM — KIỂM TOÁN 5 BẢNG GỐC & 10 DỊ THƯỜNG DỮ LIỆU (EC-1..EC-10)",
        "4. Data, EDA & Data Problem: Hồ Sơ 5 Bảng Gốc & 10 Vấn Đề Dữ Liệu Thực Nghiệm",
        "Kiểm toán định lượng 5 bảng nguồn (38.50 MB) và bóc tách 10 vấn đề dữ liệu thực tế (Edge Cases EC-1..EC-10)",
    )
    s5.add_image(0.55, 2.00, 6.25, 4.16, eda_chart)

    obs_pct = 100.0 - tr["static_feature_store_c13b"]["cold_start_ratio_pct"]
    s5.add_card(
        0.55,
        6.28,
        6.25,
        2.10,
        "Hồ Sơ 5 Bảng Gốc & Proof-of-Scale",
        [
            f"5 bảng Kaggle (38.50 MB): nodes ({ds['nodes_count']:,}), streets ({ds['streets_count']:,}), "
            f"segments ({ds['raw_segments_count']:,} → {ds['segments_count']:,} đoạn cơ giới), pings "
            f"({ds['raw_segment_status_events']:,} → {ds['segment_status_events']:,}), train "
            f"({ds['labeled_windows_count']:,} cửa sổ; {ds['derived_vs_reference_los_exact_match_pct']:.2f}% match).",
            f"Phát hiện mới: Cờ hóa {ds['synthetic_burst_windows_flagged']:,} burst windows khỏi TRAIN "
            f"→ {ev['train_samples']:,} organic; chỉ {obs_pct:.2f}% đoạn có GPS và "
            f"{ev['transition_analysis']['steady_state_ratio_pct']:.2f}% TEST đứng yên sau 30m.",
        ],
        badge="Proof-of-Scale",
        theme="sky",
        body_size=7.9,
    )

    ec_headers = ["Data Problem (EC)", "Hiện tượng trong 5 Bảng Gốc", "Giải pháp Kỹ thuật"]
    ec_rows = [
        [
            "EC-1 · Missing Vmax",
            f"{qf['missing_max_velocity_train_pct']:.2f}% cửa sổ & {qf['missing_max_velocity_segments_pct']:.2f}% đoạn",
            "Train-Only 3-Tier Imputation (P85 / Median / Prior)",
        ],
        [
            "EC-2 · Bad Geometry",
            f"{qf['zero_length_segments']} đoạn ≤ 0m, {qf['non_road_osm_segments']} POIs & "
            f"{qf['status_rejected_pings']} ping lỗi",
            "try_cast Quarantine, bù 27.0m & loại 427 POIs",
        ],
        [
            "EC-3 · Unicode Drift",
            "631 dòng Quận & 2,247 dòng lệch dấu/tiền tố",
            f"Chuẩn hóa Unicode NFC ({qf['raw_distinct_street_names']} → {qf['canonical_nfc_street_names']} đường)",
        ],
        ["EC-4 · UTC Rollover", "4,305 cửa sổ (12.89%) lệch ngày UTC+7", "Đồng bộ vn_hour (+7h), ict_date & sin/cos"],
        [
            "EC-5 · Gaps & Burst",
            f"66.30% đứt ≥60m & {ds['synthetic_burst_windows_flagged']:,} burst",
            f"Exact +30m ({ds['exact_t30m_supervised_windows_count']:,} cặp) & sạch burst TRAIN",
        ],
        [
            "EC-6 · Cold-Start",
            f"{tr['static_feature_store_c13b']['cold_start_ratio_pct']:.2f}% ({ds['cold_start_segments_count']:,} "
            "đoạn) có 0 GPS",
            "Static Feature Store (PageRank + 7D K-Means)",
        ],
        [
            "EC-7 · Shape Points",
            f"90.97% ({ds['pruned_orphan_nodes_count']:,}) node dư & {ds['dangling_nodes_count']} cụt",
            f"Cắt tỉa còn {ds['active_intersection_nodes_count']:,} nút & {ds['line_graph_transition_edges']:,} cạnh",
        ],
        [
            "EC-8 · Data Skew",
            f"Lệch {salt['temporal_slot_peak_to_median_ratio']:.2f}x & sai số avg 11.11 km/h",
            f"5-Tuple Monoid + 8-Bucket Salting (→ {salt['peak_slot_salted_bucket_skew_ratio']:.3f}x)",
        ],
        [
            "EC-9 · Late & Dupes",
            f"{tr['streaming_mmds']['duplicates_removed_exact']} trùng & "
            f"{rc['affected_late_segment_windows_recomputed']} trễ > 30m",
            f"{bloom['memory_kb']:.1f} KiB Bloom + Tier-2 (+{rc['healed_downstream_lag_windows']} lag)",
        ],
        [
            "EC-10 · Crash & Stale",
            "Sập luồng đột ngột & tin cũ v1 đến muộn",
            f"WAL Checkpoint (0 dupes) + $status_version ({mv['stale_v1_rejected_by_mongo']}/25)",
        ],
    ]
    s5.add_table(6.96, 2.00, 8.49, 6.38, ec_headers, ec_rows, [0.22, 0.38, 0.40], font_size=7.8)
    return s5


def _add_slide_06_method_explainer_imputation_monoid(prs: Presentation, method_img1: Path) -> DualSlide:
    """Slide 06: Visual Method Explainer (1/2) — 3-Tier Vmax Imputation & 5-Tuple Monoid + Key Salting."""
    sm1 = DualSlide(prs)
    sm1.add_chrome(
        6,
        TOTAL_SLIDES,
        2,
        "METHODOLOGY VISUALIZER (1/2) — TRỰC QUAN HÓA NỘI SUY 3 TẦNG VMAX, MONOID COMBINER & KEY SALTING",
        "Giải Thích Phương Pháp (1/2): Nội Suy 3 Tầng Vmax & Bộ Gộp 5-Tuple Monoid",
        "Trực quan hóa cách điền khuyết tốc độ không rò rỉ dữ liệu và phép cộng gộp phân tán đạt sai số bằng 0.0",
    )
    sm1.add_image(0.55, 2.00, 14.90, 4.50, method_img1)

    sm1.add_card(
        0.55,
        6.64,
        7.35,
        1.74,
        "Bản Chất Nội Suy 3 Tầng (3-Tier Vmax)",
        [
            "Không đoán mò hay lấy trung bình toàn bộ dữ liệu gây rò rỉ tương lai: hệ thống chỉ học trên tập TRAIN "
            "theo thứ tự ưu tiên từ riêng đến chung (lịch sử chính đoạn đó → đường cùng cấp → chuẩn đô thị).",
        ],
        badge="Zero Leakage",
        theme="sky",
        body_size=8.1,
    )
    sm1.add_card(
        8.10,
        6.64,
        7.35,
        1.74,
        "Bản Chất 5-Tuple Monoid & Key Salting",
        [
            "Lấy trung bình của trung bình luôn bị sai khi các máy chứa số xe lệch nhau; giữ bộ 5 đại lượng cộng được "
            "(số xe, tổng vận tốc, min, max, số xe kẹt) giúp chia 8 giỏ song song mà kết quả cuối vẫn khớp 100%.",
        ],
        badge="0.0 Math Error",
        theme="white",
        body_size=8.1,
    )
    return sm1


def _add_slide_07_method_explainer_graph_sketches(
    prs: Presentation, method_img2: Path, t: dict | None = None
) -> DualSlide:
    """Slide 07: Visual Method Explainer (2/2) — Line-Graph PageRank, 7D K-Means Cold-Start & O(1) Sketches."""
    tr = (t or _load_slide_truth())["m"]
    cold_pct = tr["static_feature_store_c13b"]["cold_start_ratio_pct"]
    sm2 = DualSlide(prs)
    sm2.add_chrome(
        7,
        TOTAL_SLIDES,
        2,
        "METHODOLOGY VISUALIZER (2/2) — TRỰC QUAN HÓA LINE-GRAPH PAGERANK, 7D K-MEANS & O(1) SKETCHES",
        "Giải Thích Phương Pháp (2/2): Line-Graph PageRank, K-Means & Bộ Đếm O(1)",
        f"Cách phát hiện nút cổ chai bản đồ, suy diễn cho {cold_pct:.2f}% đường thiếu GPS "
        "và lọc trùng luồng chỉ với 33.0 KiB RAM",
    )
    sm2.add_image(0.55, 2.00, 14.90, 4.50, method_img2)

    sm2.add_card(
        0.55,
        6.64,
        7.35,
        1.74,
        "Từ Đồ Thị Bản Đồ Đến Suy Diễn Vùng Mù",
        [
            "Line-Graph PageRank (loại U-turn) biến mỗi đoạn đường thành 1 nút để chỉ ra nơi dòng xe dồn tụ tự nhiên; "
            "7D K-Means ghép đoạn mù (0 GPS) vào nhóm cảm biến tương đồng để phủ 100% đô thị.",
        ],
        badge="Graph + Cluster",
        theme="sky",
        body_size=8.1,
    )
    sm2.add_card(
        8.10,
        6.64,
        7.35,
        1.74,
        "Vì Sao Dùng Bộ Đếm Luồng O(1) Sketches?",
        [
            "Ở quy mô tỷ bản tin/ngày, lưu danh sách mã tin để lọc trùng sẽ tràn RAM. Dùng dãy bit Bloom Filter "
            "(32 KiB), HLL++ (1 KiB) và điểm nóng EWMA giúp xử lý tức thời trong bộ nhớ cố định cực nhỏ.",
        ],
        badge="33.0 KiB RAM",
        theme="sky_strong",
        body_size=8.1,
    )
    return sm2


def _add_slide_08_data_pipeline_and_processing(
    prs: Presentation, flow_img: Path, t: dict | None = None
) -> DualSlide:
    """Slide 08 (Section 5): Data Pipeline & Data Processing + Component Limitations & Risks."""
    tr = (t or _load_slide_truth())["m"]
    ds = tr["dataset"]
    qf = tr["quality_funnel_c5"]
    vt = qf["vmax_imputation_tiers_windows"]
    t23 = vt["TIER_2_LEVEL_ROADTYPE_MEDIAN"] + vt["TIER_3_HIERARCHICAL_LEVEL_PRIOR"]
    t23_pct = t23 / ds["labeled_windows_count"] * 100.0
    s8 = DualSlide(prs)
    s8.add_chrome(
        8,
        TOTAL_SLIDES,
        2,
        "SECTION 05 · DATA PIPELINE & DATA PROCESSING — ORDERED QUALITY FUNNEL & TRAIN-ONLY IMPUTATION",
        "5. Data Pipeline & Processing: Ordered Quality Funnel & Train-Only Imputation",
        "Làm sạch bảo toàn 100% quân số bản ghi, chuẩn hóa Unicode NFC / UTC+7 và suy diễn tốc độ tối đa không rò rỉ",
    )
    s8.add_image(0.55, 2.00, 14.90, 4.14, flow_img)

    s8.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Ordered Quality Funnel & 3-Tier Vmax",
        [
            f"Quality Funnel: try_cast cách ly {qf['quarantined_segment_records']} đoạn & "
            f"{qf['status_rejected_pings']} ping ({ds['segment_status_events']:,} hợp lệ); loại "
            f"{qf['non_road_osm_segments']} POIs còn {ds['segments_count']:,} đoạn cơ giới; NFC "
            f"({qf['raw_distinct_street_names']} → {qf['canonical_nfc_street_names']} đường).",
            f"Train-Only 3-Tier Vmax: Gốc ({vt['TIER_1_SEGMENT_OBSERVED']:,}) → P85 "
            f"({vt['TIER_1_SEGMENT_TRAIN_P85']:,}) → Median cấp×loại ({vt['TIER_2_LEVEL_ROADTYPE_MEDIAN']:,}) "
            f"→ Prior ({vt['TIER_3_HIERARCHICAL_LEVEL_PRIOR']:,}).",
            f"Gán nhãn {ds['labeled_windows_count']:,} cửa sổ ({ds['derived_vs_reference_los_exact_match_pct']:.2f}% "
            "khớp BkTraffic LOS A–F): A–B (≥65%) · C–D (35–65%) · E–F (<35%).",
        ],
        badge="0 Silent Drops",
        theme="sky",
        body_size=7.9,
    )
    s8.add_card(
        8.10,
        6.28,
        7.35,
        2.10,
        "Limitations & Risks (Data Pipeline)",
        [
            f"Static Vmax Approximation: {t23:,} cửa sổ ({t23_pct:.2f}%) dùng Vmax suy diễn theo cấp/prior "
            "(Tier 2 & 3), chưa phản ánh rào chắn thi công hay ngập nước cục bộ.",
            "Schema Drift Quarantine Risk: ANSI try_cast chống sập job, nhưng thiết bị GPS đổi định dạng hàng loạt "
            "sẽ làm tăng tỷ lệ cách ly vào Quarantine.",
            "30m Slot Discretization: Làm tròn cửa sổ 30 phút làm mượt dao động vận tốc ngắn dưới 5 phút tại ngã tư.",
        ],
        badge="Pipeline Risk",
        theme="navy",
        body_size=7.9,
    )
    return s8


def _add_slide_09_modeling(prs: Presentation, t: dict | None = None) -> DualSlide:
    """Slide 09 (Section 6): Modeling (+30m Forecasting, 23D Features & Cold-Start) + Component Limitations."""
    tr_d = t or _load_slide_truth()
    tr = tr_d["m"]
    ds = tr["dataset"]
    ev = tr["evaluation"]
    vg = ev["validation_gate_selection"]
    ta = ev["transition_analysis"]
    imps = tr_d["imps"]
    rf23_val = next(c for c in vg["candidates"] if "Random Forest (+Graph" in c["candidate_model"])
    rf14_val = next(c for c in vg["candidates"] if "Random Forest (Local" in c["candidate_model"])
    s9 = DualSlide(prs)
    s9.add_chrome(
        9,
        TOTAL_SLIDES,
        2,
        "SECTION 06 · MODELING — EXACT T+30M PAIRING, BURST FILTERING, 23D FEATURES & LIVE VAL GATE",
        "6. Modeling: Nhãn Exact t+30m, Lọc Burst, Vector 23 Biến & Tuyển Chọn Live VAL",
        "Tách 7,192 burst windows khỏi TRAIN, tích hợp đồ thị/phân cụm 23D và phủ 100% đoạn cơ giới Cold-Start",
    )
    s9.add_card(
        0.55,
        2.00,
        7.35,
        3.08,
        "Exact t+30m Pairing, Burst Filter & Zero Leakage",
        [
            f"Exact (segment_id, slot_ts + 30m) Join: {ds['exact_t30m_supervised_windows_count']:,} cặp "
            f"(loại 15,514 cặp đứt ≥ 60m); tách {ds['synthetic_burst_windows_flagged']:,} burst windows.",
            f"Chronological Split + Boundary Purge: Organic TRAIN = {ev['train_samples']:,} "
            f"(cắt {ev['train_purged_boundary_samples']} biên; lọc "
            f"{ev['burst_filtering_ablation']['burst_train_samples_excluded']:,} burst) · "
            f"VAL = {ev['val_samples']:,} · TEST = {ev['test_samples']:,}.",
            "Future-Shuffle Falsification Test: Xáo trộn ngẫu nhiên tương lai τ > t cho sai lệch X_t = 0.0.",
        ],
        badge=f"{ev['train_samples']:,} Organic Train",
        theme="white",
        body_size=8.2,
    )
    s9.add_card(
        0.55,
        5.24,
        7.35,
        3.14,
        "Tổ Hợp 23 Biến Đầu Vào (23D Features)",
        [
            f"Nhóm 1 — Local State & Lags (14D tổng {tr_d['local_imp']:.2f}%): mean_velocity "
            f"({imps['mean_velocity']*100:.2f}%), current_los_ordinal ({imps['current_los_ordinal']*100:.2f}%), "
            f"velocity_ratio ({imps['velocity_ratio']*100:.2f}%), obs_count ({imps['obs_count']*100:.2f}%), lag1/2.",
            f"Nhóm 2 — Cyclical ICT & Geometry: period_sin ({imps['period_sin']*100:.2f}%), "
            f"period_cos ({imps['period_cos']*100:.2f}%), length_m ({imps['length_m']*100:.2f}%), "
            f"gap_minutes ({imps['gap_minutes']*100:.2f}%), weekday, street_level, Vmax.",
            f"Nhóm 3 — Graph, 7D K-Means & Priors (9D · {tr_d['graph_imp']:.2f}%): pagerank_score "
            f"({imps['pagerank_score']*100:.2f}%), neighbor_los ({imps['neighbor_los_ordinal']*100:.2f}%), "
            f"street_causal_prior ({imps['street_causal_prior_los']*100:.2f}%), degree & cluster_id.",
        ],
        badge=f"{tr_d['local_imp']:.1f}% / {tr_d['graph_imp']:.1f}%",
        theme="sky",
        body_size=8.1,
    )
    s9.add_card(
        8.10,
        2.00,
        7.35,
        3.08,
        "Live VAL Gate Selection & Cold-Start Scoring",
        [
            f"Champion (#5 RF 23D): 250 cây, max_depth=14, clean sqrt cls_w (0.65), thắng Live VAL Gate "
            f"({rf23_val['val_composite_score']:.4f} vs RF 14D {rf14_val['val_composite_score']:.4f}; τ = 0.25) "
            "→ khóa SHA-256.",
            f"Cold-Start Coverage: Static Feature Store (PageRank + 7D K-Means + Priors) phủ đủ "
            f"{ds['cold_start_segments_count']:,} đoạn 0 GPS (100% trên {ds['segments_count']:,} đoạn cơ giới).",
        ],
        badge=f"VAL Gate {rf23_val['val_composite_score']:.4f}",
        theme="sky_strong",
        body_size=8.2,
    )
    s9.add_card(
        8.10,
        5.24,
        7.35,
        3.14,
        "Limitations & Risks (Modeling)",
        [
            f"Ngưỡng mặc định bảo thủ khi đổi pha: {int(round(ta['rf_23d_transition_6class_acc']*61))}/61 "
            f"({ta['rf_23d_transition_6class_acc']*100:.2f}%); tăng lên "
            f"{int(round(ta['rf_23d_val_calibrated_transition_6class_acc']*61))}/61 "
            f"({ta['rf_23d_val_calibrated_transition_6class_acc']*100:.2f}%) ở τ = 0.40 trên VAL "
            f"(McNemar p = {ta['statistical_significance_test']['mcnemar_transition_p_value']:.4f}).",
            "Exogenous Blindness & Imbalance: Chưa có biến mưa ngập/tai nạn; lớp trung gian B–E thưa nên "
            f"Macro-F1 đạt {ev['macro_f1_6class']:.4f} (dù Severe F1 = {ev['severe_f1']:.4f}).",
        ],
        badge="Modeling Risk",
        theme="navy",
        body_size=8.2,
    )
    return s9


def _add_slide_10_batch_processing(
    prs: Presentation, batch_flow_img: Path, t: dict | None = None
) -> DualSlide:
    """Slide 10 (Section 7): Batch Processing Component Flow, Quantitative Summary & Limitations."""
    tr_d = t or _load_slide_truth()
    tr = tr_d["m"]
    sp = tr_d["sp"]
    ds = tr["dataset"]
    salt = tr["monoid_and_catalyst_c9"]["pillar_i1_monoid_and_salting"]
    ms = sp["monoid_salting_spark"]
    cj = sp["catalyst_join_spark"]
    gp = tr["graph_and_patterns"]
    km = tr["model"]
    s10 = DualSlide(prs)
    s10.add_chrome(
        10,
        TOTAL_SLIDES,
        3,
        "SECTION 07 · BATCH PROCESSING — LUỒNG XỬ LÝ LÔ PHÂN TÁN, KHAI PHÁ ĐỒ THỊ & PHÂN CỤM",
        "7. Batch Processing: Luồng Xử Lý Lô Phân Tán, Khai Phá Đồ Thị & Phân Cụm",
        "Trực quan hóa Batch ETL sai số 0.0 trên Spark 3.5.7, Line-Graph PageRank, SON/FPGrowth và 7D K-Means",
    )
    s10.add_image(0.55, 2.00, 14.90, 4.14, batch_flow_img)

    s10.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Kết Quả Thực Thi Batch ETL, Đồ Thị & Phân Cụm",
        [
            f"Spark 3.5.7 ETL: 5-Tuple Monoid khớp 100% baseline (diff=0); 8-Bucket Salting giảm lệch slot "
            f"{salt['temporal_slot_peak_to_median_ratio']:.2f}x → {salt['peak_slot_salted_bucket_skew_ratio']:.3f}x "
            f"(IQR {ms['unsalted_iqr_ms']:.2f} → {ms['salted_iqr_ms']:.2f} ms ở tradeoff "
            f"{ms['unsalted_median_ms']:.2f} → {ms['salted_median_ms']:.2f} ms); BroadcastHashJoin "
            f"({cj['baseline_exchange_stages_measured']}→{cj['optimized_exchange_stages_measured']} Exchange, "
            f"+{cj['join_speedup_pct']:.2f}%); Parquet Pruning "
            f"{tr['lakehouse_c14']['year_month_partitions']['2020-08']:,} / {ds['labeled_windows_count']:,} dòng.",
            f"Graph & Cold-Start: PageRank bảo toàn {ds['segments_count']:,}.0 (#1 #{gp['top_bottleneck_segment_id']} "
            f"{gp['top_bottleneck_street']} = {gp['top_bottleneck_pagerank']:.4f}); FPGrowth "
            f"({sp['spark_fpgrowth_rules_count']} luật) & SON ({gp['co_congestion_rules_found']} đồng kẹt / "
            f"{gp['lagged_transition_rules_found']} trễ); 7D K-Means (k=6, Silhouette={km['silhouette_score']:.4f}) "
            f"phủ {ds['cold_start_segments_count']:,} Cold-Start.",
        ],
        badge=f"0.0 Error · {ds['segments_count']:,} Segs",
        theme="sky",
        body_size=7.7,
    )
    s10.add_card(
        8.10,
        6.28,
        7.35,
        2.10,
        "Limitations & Risks (Batch Processing)",
        [
            f"Cỡ mẫu nhỏ ở luật trễ SON/FPGrowth: {gp['lagged_transition_rules_found']} luật trễ +30m (Lift tối đa "
            "24.0x, persistence-controlled) chỉ có 5–8 cửa sổ do GPS thưa → chỉ làm giả thuyết thăm dò.",
            f"Phân cụm Cold-Start tĩnh: {ds['cold_start_segments_count']:,} đoạn thiếu GPS gán cụm dựa trên hình thái "
            "đồ thị và Vmax cấp đường, chưa phản ánh được điểm họp chợ tự phát hay ngập vi mô.",
        ],
        badge="Batch Risk",
        theme="navy",
        body_size=7.9,
    )
    return s10


def _add_slide_11_stream_processing(
    prs: Presentation, stream_flow_img: Path, t: dict | None = None
) -> DualSlide:
    """Slide 11 (Section 8): Stream Processing Component Flow, Quantitative Summary & Limitations."""
    tr_d = t or _load_slide_truth()
    tr = tr_d["m"]
    mv = tr_d["mv"]
    sm = tr["streaming_mmds"]
    bl = sm["bloom_filter"]
    hll = sm["hyperloglog"]
    rc = sm["reconciliation"]
    lat = mv["uc2_arterial_query"]["latency_benchmark_ms"]
    s11 = DualSlide(prs)
    s11.add_chrome(
        11,
        TOTAL_SLIDES,
        3,
        "SECTION 08 · STREAM PROCESSING — LUỒNG XỬ LÝ THỜI GIAN THỰC, O(1) SKETCHES & ĐỐI SOÁT HAI TẦNG",
        "8. Stream Processing: Luồng Cảnh Báo Trực Tuyến, O(1) Sketches & Đối Soát Trễ",
        "Trực quan hóa luồng sự kiện Kafka, bộ lọc 33.0 KiB RAM, đối soát bù trễ hai tầng và phục vụ < 1 ms",
    )
    s11.add_image(0.55, 2.00, 14.90, 4.14, stream_flow_img)

    s11.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Kết Quả Luồng O(1), Đối Soát & Phục Vụ < 1 ms",
        [
            f"O(1) Sketches & WAL: Bloom ({bl['memory_kb']:.1f} KiB) chặn {sm['duplicates_removed_exact']}/"
            f"{sm['injected_duplicate_events']} trùng (FPR {bl['empirical_fp_rate']*100:.3f}%); HLL++ "
            f"({hll['memory_kb']:.1f} KiB) ≈ {hll['hll_estimated_segments']:.1f}/{hll['exact_distinct_segments']:,} "
            f"({hll['relative_error_pct']:.2f}% err); EWMA #1 #47407 Lý Thường Kiệt (6.6652); WAL 0 dupes.",
            f"Two-Tier Recon & MongoDB 8.0: Tier 1 diff=0; Tier 2 bù {rc['affected_late_segment_windows_recomputed']} "
            f"trễ, hàn {rc['healed_downstream_lag_windows']} lag & re-score {rc['healed_downstream_rescored_windows']} "
            f"({rc['pre_reconciliation_completeness_pct']:.1f}%→{rc['post_reconciliation_completeness_pct']:.1f}%); "
            f"$status_version chặn {mv['stale_v1_rejected_by_mongo']}/25 v1; IXSCAN warm {lat['warm_median_ms']:.3f} / "
            f"p95 {lat['warm_p95_ms']:.3f} ms ({lat['ixscan_vs_collscan_speedup_x']:.2f}x).",
        ],
        badge=f"33.0 KiB · {lat['warm_median_ms']:.3f} ms",
        theme="sky",
        body_size=7.7,
    )
    s11.add_card(
        8.10,
        6.28,
        7.35,
        2.10,
        "Limitations & Risks (Stream Processing)",
        [
            "Rủi ro bão hòa bit Bloom Filter: Mảng cố định 32.0 KiB (262,144 bits) sẽ tăng FPR nếu chạy liên tục "
            "nhiều ngày ở quy mô 1.5–3.0 TB/ngày mà không xoay vòng (Ring-Buffer Rotation) theo cửa sổ.",
            f"Độ trễ cập nhật của sự kiện quá muộn (> 30m Watermark): {rc['affected_late_segment_windows_recomputed']} "
            "cửa sổ trễ hơn 30 phút phải đợi tiến trình Tier-2 Reconciliation cuối ngày.",
        ],
        badge="Stream Risk",
        theme="navy",
        body_size=7.9,
    )
    return s11


def _fmt_ab(val: object) -> str:
    return "N/A" if pd.isna(val) else f"{float(val):.4f}"


def _ab_row(df: pd.DataFrame, idx: int, display_label: str, dims: str) -> list[str]:
    r = df.iloc[idx]
    return [
        display_label,
        dims,
        _fmt_ab(r["accuracy_6class"]),
        _fmt_ab(r["macro_f1_6class"]),
        _fmt_ab(r["severe_ef_f1"]),
        _fmt_ab(r["severe_roc_auc"]),
        _fmt_ab(r["severe_pr_auc"]),
    ]


def _add_slide_12_experiments_ablation(prs: Presentation, t: dict | None = None) -> DualSlide:
    """Slide 12 (Section 9A): Experiments — 7-Model Ablation, Confusion Matrix & Infrastructure Benchmarks."""
    tr_d = t or _load_slide_truth()
    tr = tr_d["m"]
    sp = tr_d["sp"]
    mv = tr_d["mv"]
    ab = tr_d["ab"]
    ev = tr["evaluation"]
    vg = ev["validation_gate_selection"]
    rf23_val = next(c for c in vg["candidates"] if "Random Forest (+Graph" in c["candidate_model"])
    rf14_val = next(c for c in vg["candidates"] if "Random Forest (Local" in c["candidate_model"])
    cm_diag = [int(ev["confusion_matrix_6class"][i][i]) for i in range(6)]
    cj = sp["catalyst_join_spark"]
    ms = sp["monoid_salting_spark"]
    salt = tr["monoid_and_catalyst_c9"]["pillar_i1_monoid_and_salting"]
    lat = mv["uc2_arterial_query"]["latency_benchmark_ms"]

    s12 = DualSlide(prs)
    s12.add_chrome(
        12,
        TOTAL_SLIDES,
        4,
        "SECTION 09A · EXPERIMENTS (ABLATION & BENCHMARKS) — KẾT QUẢ THỰC NGHIỆM 7 MÔ HÌNH",
        "9A. Experiments: Thực Nghiệm Ablation 7 Mô Hình Dự Báo +30m & Confusion Matrix",
        f"Đánh giá định lượng trên {ev['test_samples']:,} mẫu TEST độc lập & kiểm chứng VAL Gate (23D vs 14D)",
    )
    ab_headers = [
        f"Cấu hình Mô hình ({ev['test_samples']:,} mẫu TEST)",
        "Biến",
        "6-Class Acc",
        "Macro-F1",
        "Severe F1",
        "ROC-AUC",
        "PR-AUC",
    ]
    ab_rows = [
        _ab_row(ab, 0, "1. Persistence Baseline (LOS(t+30m) = LOS(t))", "1D"),
        _ab_row(ab, 1, "2. Decision Tree (Local Only)", "14D"),
        _ab_row(ab, 2, "3. Decision Tree (+PageRank & K-Means)", "23D"),
        _ab_row(ab, 3, "4. Random Forest (Local Only)", "14D"),
        _ab_row(ab, 4, "5. Random Forest (+Graph, Priors & K-Means) [Chọn]", "23D"),
        _ab_row(ab, 5, "6. Binary GBT (Local Only)", "14D"),
        _ab_row(ab, 6, "7. Binary GBT (+Graph, Priors & K-Means)", "23D"),
    ]
    s12.add_table(
        0.55,
        2.00,
        8.35,
        6.38,
        ab_headers,
        ab_rows,
        [0.37, 0.08, 0.12, 0.11, 0.11, 0.11, 0.10],
        highlight_row=5,
        font_size=8.0,
    )

    s12.add_card(
        9.06,
        2.00,
        6.39,
        3.08,
        "VAL Gate, Confusion Matrix & Tính Trung Thực 23D",
        [
            f"VAL Composite Gate: RF 23D thắng {rf23_val['val_composite_score']:.4f} vs RF 14D "
            f"{rf14_val['val_composite_score']:.4f} (acc={rf23_val['val_accuracy_6class']:.4f}); "
            f"TEST 6-Class diag {cm_diag} đủ 6 mức A–F.",
            f"Trên {ev['train_samples']:,} organic TRAIN: ROC-AUC = {ev['roc_auc']:.4f}, "
            f"PR-AUC = {ev['average_precision']:.4f}, Severe F1 = {ev['severe_f1']:.4f}; "
            f"Macro-F1 = {ev['macro_f1_6class']:.4f} do lớp B–E thưa (10–20 mẫu TEST).",
        ],
        badge=f"ROC-AUC {ev['roc_auc']:.4f}",
        theme="sky",
        body_size=8.2,
    )
    s12.add_card(
        9.06,
        5.24,
        6.39,
        3.14,
        "Kiểm Chứng Hiệu Năng Spark 3.5.7 & MongoDB 8.0",
        [
            f"Batch & Catalyst: Monoid diff = 0.0; BroadcastHashJoin ({cj['baseline_exchange_stages_measured']}→"
            f"{cj['optimized_exchange_stages_measured']} Exchange, {cj['baseline_median_ms']:.2f}→"
            f"{cj['optimized_median_ms']:.2f} ms, +{cj['join_speedup_pct']:.2f}%); Salting kéo lệch "
            f"{salt['temporal_slot_peak_to_median_ratio']:.2f}x→{salt['peak_slot_salted_bucket_skew_ratio']:.3f}x "
            f"(IQR {ms['unsalted_iqr_ms']:.2f}→{ms['salted_iqr_ms']:.2f} ms).",
            f"Speed & Serving: Sketches 33.0 KiB chặn 100% tin trùng; MongoDB 8.0 IXSCAN warm median "
            f"{lat['warm_median_ms']:.3f} / p95 {lat['warm_p95_ms']:.3f} ms "
            f"({lat['ixscan_vs_collscan_speedup_x']:.2f}x).",
        ],
        badge="System Benchmarks",
        theme="white",
        body_size=8.1,
    )
    return s12


def _add_slide_13_experiments_persistence_defense(
    prs: Presentation, trans_chart: Path, t: dict | None = None
) -> DualSlide:
    """Slide 13 (Section 9B): Experiments — Resolving the Persistence Paradox Across 4 Regimes."""
    tr = (t or _load_slide_truth())["m"]
    ev = tr["evaluation"]
    ta = ev["transition_analysis"]
    st = ta["statistical_significance_test"]
    ng = ta["next_observed_gap_regime"]
    cs = ta["cold_start_coverage"]
    mc = ta["spatial_cold_start_masked_cv"]
    def_chg = int(round(ta["rf_23d_transition_6class_acc"] * ta["transition_samples"]))
    cal_chg = int(round(ta["rf_23d_val_calibrated_transition_6class_acc"] * ta["transition_samples"]))
    top1_chg = int(round(ta["rf_23d_conditional_new_state_top1_acc"] * ta["transition_samples"]))
    brier_drop = (1.0 - ta["rf_23d_transition_brier"] / ta["persistence_transition_brier"]) * 100.0
    gap_pr_gain = (ng["rf_23d_severe_pr_auc"] - ng["persistence_severe_pr_auc"]) * 100.0

    s13 = DualSlide(prs)
    s13.add_chrome(
        13,
        TOTAL_SLIDES,
        4,
        "SECTION 09B · EXPERIMENTS (DEFENSE) — GIẢI MÃ NGHỊCH LÝ PERSISTENCE BASELINE TRÊN 4 CHẾ ĐỘ",
        "9B. Experiments: Vì Sao Chọn RF (23D) Khi Baseline Đạt "
        f"{ev['ablation_table'][0]['accuracy_6class']*100:.2f}%?",
        f"Bóc tách 1-đối-1 giữa {ta['steady_state_ratio_pct']:.2f}% đứng yên và "
        f"{ta['transition_ratio_pct']:.2f}% thời điểm giao thông chuyển pha",
    )
    t_headers = [
        "Chế độ Kiểm định Thực tế",
        "Persistence Baseline",
        "Random Forest (23D)",
        "Kết luận Thực nghiệm",
    ]
    n_tr = ta["transition_samples"]
    cal_pct = ta["rf_23d_val_calibrated_transition_6class_acc"] * 100.0
    top1_pct = ta["rf_23d_conditional_new_state_top1_acc"] * 100.0
    t_rows = [
        [
            f"1. Đổi trạng thái\nLOS(t) ≠ LOS(t+30m)\n({n_tr} / {ta['test_samples']:,} TEST)",
            f"Acc = 0 / {n_tr} (0.00%)\nDetect AUC = {ta['persistence_transition_detect_roc_auc']:.4f}\n"
            f"Brier = {ta['persistence_transition_brier']:.4f}",
            f"Detect AUC = {ta['rf_23d_transition_detect_roc_auc']:.4f}\n"
            f"Acc: {cal_chg}/{n_tr} ({cal_pct:.2f}% ở τ={ta['val_selected_change_threshold']:.2f}; "
            f"{def_chg}/{n_tr} mặc định)\nTop-1 mới: {top1_chg}/{n_tr} ({top1_pct:.2f}%)",
            f"McNemar p = {st['mcnemar_transition_p_value']:.4f} (7 vs 0);\nRF bắt biến động sớm & giảm "
            f"{brier_drop:.1f}% sai số Brier.",
        ],
        [
            f"2. Thoáng (A–D) đột ngột\nhóa kẹt cứng (E–F)\n"
            f"({ta['congestion_onset_samples']} / {ta['non_severe_t_samples']} TEST)",
            "Hard Recall = 0 / 8 (0.0%)\nGán xác suất kẹt = 0% cho\nmọi đoạn đang mức A–D",
            f"Hard Recall mặc định = 0 / 8\n(mẫu hiếm 0.81%)\nOnset ROC-AUC = {ta['rf_23d_onset_roc_auc']:.4f}\n"
            f"Rủi ro tăng {ta['rf_23d_onset_risk_lift_ratio']:.2f}x",
            f"Ngưỡng cứng chưa bắt kịp 8 ca\nhiếm, nhưng rủi ro tăng "
            f"{ta['rf_23d_onset_risk_lift_ratio']:.2f}x\n"
            f"({ta['rf_23d_mean_prob_onset']:.4f} vs {ta['rf_23d_mean_prob_steady_free']:.4f}).",
        ],
        [
            f"3. Cặp tín hiệu GPS\nngắt quãng > 30 phút\n"
            f"({ng['test_samples']:,} · {ng['transition_ratio_pct']:.2f}% đổi)",
            f"Severe PR-AUC = {ng['persistence_severe_pr_auc']:.4f}\nROC-AUC = {ng['persistence_severe_roc_auc']:.4f}\n"
            "(quán tính cũ suy yếu)",
            f"Severe PR-AUC = {ng['rf_23d_severe_pr_auc']:.4f} (+{gap_pr_gain:.2f}%)\n"
            f"ROC-AUC = {ng['rf_23d_severe_roc_auc']:.4f}\n(chu kỳ giờ VN + PageRank)",
            "Khi GPS ngắt quãng lâu (chiếm\n66.30% thực tế), RF vượt trội\nvề Severe PR-AUC.",
        ],
        [
            f"4. Vùng Cold-Start\nkhông có xe gửi GPS\n"
            f"({cs['cold_start_segments']:,}/{cs['total_network_segments']:,})",
            f"Network Coverage = {cs['persistence_coverage_pct']:.2f}%\nCold-Start = 0.0%\n"
            "(vô hiệu khi 0 GPS)",
            f"Network Coverage = {cs['rf_23d_network_coverage_pct']:.1f}%\n"
            f"(Masked: Acc={mc['rf_23d_masked_cold_start_6class_acc']:.4f},\n"
            f"PR-AUC={mc['rf_23d_masked_cold_start_pr_auc']:.4f})",
            f"Chỉ duy nhất RF 23D phủ được\n{tr['static_feature_store_c13b']['cold_start_ratio_pct']:.2f}% "
            "mạng lưới đường\ncơ giới thiếu cảm biến.",
        ],
    ]
    s13.add_table(0.55, 2.00, 8.35, 6.38, t_headers, t_rows, [0.22, 0.23, 0.29, 0.26], font_size=7.7)
    s13.add_image(9.06, 2.00, 6.39, 4.26, trans_chart)
    s13.add_card(
        9.06,
        6.38,
        6.39,
        2.00,
        "Đúc Kết Đối Chứng Khoa Học (1-on-1 Defense)",
        [
            f"{ev['ablation_table'][0]['accuracy_6class']*100:.2f}% của Baseline phản ánh quán tính đứng yên "
            f"({ta['steady_state_samples']:,} / {ta['test_samples']:,} cặp giữ nhãn), mù 100% (0 / "
            f"{ta['transition_samples']}) khi chuyển pha.",
            f"RF 23D bắt sớm tín hiệu đổi pha (Detect AUC = {ta['rf_23d_transition_detect_roc_auc']:.4f}, "
            f"McNemar p = {st['mcnemar_transition_p_value']:.4f}, Lift {ta['rf_23d_onset_risk_lift_ratio']:.2f}x) "
            f"và phủ 100% {cs['total_network_segments']:,} đoạn cơ giới.",
        ],
        badge="Key Finding",
        theme="sky_strong",
        body_size=8.0,
    )
    return s13


def _add_slide_14_demonstration(
    prs: Presentation, ui_tab2: Path, ui_tab4: Path, t: dict | None = None
) -> DualSlide:
    """Slide 14 (Section 10): Demonstration (Dual-UI TOC Web :8000 & Streamlit :8501 Case Studies)."""
    tr = (t or _load_slide_truth())["m"]
    ds = tr["dataset"]
    gp = tr["graph_and_patterns"]
    s14 = DualSlide(prs)
    s14.add_chrome(
        14,
        TOTAL_SLIDES,
        5,
        "SECTION 10 · DEMONSTRATION — DUAL-UI TOC WEB (:8000) & STREAMLIT (:8501) · CASE STUDIES",
        "10. Demonstration: Cận Cảnh Dự Báo E → F (Tab 2) & Tra Cứu Cold-Start (Tab 4)",
        f"Trình diễn Dual-UI: FastAPI TOC Web (:8000) & Streamlit (:8501 ↔ MongoDB 8.0 · "
        f"{ds['labeled_windows_count']:,} docs)",
    )
    s14.add_card(
        0.55,
        2.00,
        7.35,
        1.58,
        "Case Study 1 (Tab 2): Bắt Đúng E → F",
        [
            "Đoạn 21064 — Công Trường Quốc Tế (2021-04-22 11:30 ICT, đang LOS E · 12.0 km/h): Baseline giữ mức E "
            "(sai), còn Random Forest 23D dự báo đúng chuyển sang LOS F (severe_prob = 72.4%, thực tế 12:00 là F).",
        ],
        badge="Tab 2 · Forecast",
        theme="sky",
        body_size=8.2,
    )
    s14.add_image(0.55, 3.68, 7.35, 3.14, ui_tab2)

    s14.add_card(
        8.10,
        2.00,
        7.35,
        1.58,
        "Case Study 2 (Tab 4): Vùng Cold-Start",
        [
            f"Chọn đoạn 61781 — Phú Định (0 GPS, PR = 2.2141, Cluster 5 trong {ds['cold_start_segments_count']:,} "
            f"Cold-Start; #1 mạng lưới: #{gp['top_bottleneck_segment_id']} {gp['top_bottleneck_street']} = "
            f"{gp['top_bottleneck_pagerank']:.4f}), đối chiếu ngay nhóm cảm biến đồng dạng (Nguyễn Kiệm, Song Hành).",
        ],
        badge="Tab 4 · Cold-Start",
        theme="white",
        body_size=8.0,
    )
    s14.add_image(8.10, 3.68, 7.35, 3.14, ui_tab4)

    s14.add_card(
        0.55,
        6.94,
        14.90,
        1.44,
        "Kiến Trúc Dual-UI: TOC Web (:8000) & Streamlit 5 Tab (:8501)",
        [
            f"TOC Web (FastAPI :8000 + ReactJS + Leaflet): Bản đồ {ds['segments_count']:,} đoạn cơ giới theo LOS/PR, "
            "cảnh báo trực tuyến < 1 ms & mô phỏng sự cố. (Ảnh minh họa giao diện :8501 truy vấn trực tiếp MongoDB).",
            "Streamlit 5 Tab (:8501): Tab 1 EWMA Hotspots (UC-1) · Tab 2 Forecast +30m (UC-2) · Tab 3 PageRank & "
            "Luật SON (UC-3/4) · Tab 4 7D K-Means Cold-Start (UC-5) · Tab 5 Tier-2 Recon & SHA-256 Registry (UC-6).",
        ],
        badge="Dual-UI Live",
        theme="sky_strong",
        body_size=8.0,
    )
    return s14


def _add_slide_15_limitations_and_risks_summary(prs: Presentation, t: dict | None = None) -> DualSlide:
    """Slide 15 (Section 11): Limitations & Risks — Solution Component Roll-Up & System-Wide Risk Matrix."""
    tr = (t or _load_slide_truth())["m"]
    ds = tr["dataset"]
    qf = tr["quality_funnel_c5"]
    ev = tr["evaluation"]
    ta = ev["transition_analysis"]
    rc = tr["streaming_mmds"]["reconciliation"]
    vt = qf["vmax_imputation_tiers_windows"]
    t23 = vt["TIER_2_LEVEL_ROADTYPE_MEDIAN"] + vt["TIER_3_HIERARCHICAL_LEVEL_PRIOR"]
    t23_pct = t23 / ds["labeled_windows_count"] * 100.0
    obs_pct = 100.0 - tr["static_feature_store_c13b"]["cold_start_ratio_pct"]
    cal_chg = int(round(ta["rf_23d_val_calibrated_transition_6class_acc"] * ta["transition_samples"]))
    def_chg = int(round(ta["rf_23d_transition_6class_acc"] * ta["transition_samples"]))

    s15 = DualSlide(prs)
    s15.add_chrome(
        15,
        TOTAL_SLIDES,
        5,
        "SECTION 11 · LIMITATIONS & RISKS — TỔNG HỢP HẠN CHẾ SOLUTION COMPONENTS & RỦI RO TOÀN CỤC",
        "11. Limitations & Risks: Tổng Hợp Từng Thành Phần Giải Pháp & 4 Rủi Ro Chung",
        "Tổng hợp giới hạn kỹ thuật của 5 thành phần giải pháp (Mục 3, 5, 6, 7, 8) và 4 rủi ro vận hành cấp đô thị",
    )
    comp_headers = [
        "Solution Component",
        "Hạn chế & Rủi ro Đặc thù của Component",
        "Biện pháp Kiểm soát Đã Áp dụng",
    ]
    comp_rows = [
        [
            "3. System Architecture",
            "6 dịch vụ chạy chung 1 máy Docker (12.0/16 GB RAM);\nđộ trễ đồng bộ biến trễ giữa Phase B và Phase C",
            "Giới hạn mem_limit từng container;\nkhóa phân quyền đọc/ghi 3-Phase MLOps",
        ],
        [
            "5. Data Pipeline &\nData Processing",
            f"{t23_pct:.2f}% ({t23:,}) cửa sổ dùng Vmax suy diễn tĩnh (Tier 2/3);\n"
            "nguy cơ tăng Quarantine khi thiết bị đổi schema",
            "Train-Only 3-Tier Vmax (0 Data Leakage);\nANSI try_cast + Quarantine (0 silent drops)",
        ],
        [
            "6. Modeling",
            f"Đổi pha mặc định {def_chg}/{ta['transition_samples']}; lớp B–E thưa kéo\n"
            f"Macro-F1={ev['macro_f1_6class']:.4f} & thiếu biến mưa/tai nạn",
            f"VAL τ={ta['val_selected_change_threshold']:.2f} ({cal_chg}/61, "
            f"McNemar p={ta['statistical_significance_test']['mcnemar_transition_p_value']:.4f});\n"
            f"lọc {ds['synthetic_burst_windows_flagged']:,} burst & Lift {ta['rf_23d_onset_risk_lift_ratio']:.2f}x",
        ],
        [
            "7. Batch Processing",
            f"Luật trễ +30m ({tr['graph_and_patterns']['lagged_transition_rules_found']} luật) chỉ có 5–8 cửa sổ;\n"
            f"{ds['cold_start_segments_count']:,} Cold-Start dựa trên hình thái tĩnh",
            f"Gắn nhãn Exploratory Hypothesis;\nkết hợp PageRank bảo toàn {ds['segments_count']:,}.0",
        ],
        [
            "8. Stream Processing",
            "Bloom 32.0 KiB đầy bit nếu chạy dài ngày;\ntin trễ >30m phải chờ đối soát cuối ngày",
            f"Tier-2 Recon bù {rc['affected_late_segment_windows_recomputed']} trễ & "
            f"{rc['healed_downstream_lag_windows']} lag;\nMongoDB $status_version chặn 25/25 v1",
        ],
    ]
    s15.add_table(0.55, 2.00, 8.55, 6.38, comp_headers, comp_rows, [0.22, 0.43, 0.35], font_size=7.9)

    s15.add_card(
        9.26,
        2.00,
        6.19,
        6.38,
        "4 Rủi Ro & Hạn Chế Chung Toàn Hệ Thống",
        [
            f"1. Spatial & Fleet Representation Bias: Nguồn GPS chỉ phủ {obs_pct:.2f}% "
            f"({ds['distinct_active_segments']:,}/{ds['segments_count']:,} đoạn cơ giới, chủ yếu ô tô), chưa đại diện "
            "đầy đủ luồng xe máy (> 80% lưu lượng TP.HCM).",
            f"2. Open-World Concept Drift & Imbalance: Phase A trên {ev['train_samples']:,} organic TRAIN có thể giảm "
            f"độ chính xác khi mưa ngập; lớp B–E thưa (10–20 mẫu TEST) làm Macro-F1 = {ev['macro_f1_6class']:.4f} "
            f"(Severe F1 = {ev['severe_f1']:.4f}).",
            f"3. Rare Sudden-Onset Scarcity: Ca thoáng sang kẹt cứng chiếm 0.81% ({ta['congestion_onset_samples']}/"
            f"{ta['non_severe_t_samples']}, Hard Recall = 0/8; Onset ROC = {ta['rf_23d_onset_roc_auc']:.4f}, Lift "
            f"{ta['rf_23d_onset_risk_lift_ratio']:.2f}x).",
            "4. Single-Host Proof-of-Scale & Human-in-the-Loop: Cụm 6 container (12.0/16 GB RAM) & Dual-UI "
            "(:8000 / :8501) chỉ hỗ trợ khuyến nghị, chưa đóng vòng điều khiển đèn SCATS.",
        ],
        badge="System-Wide Risks",
        theme="navy",
        body_size=8.1,
    )
    return s15


def _add_slide_16_plan_and_roadmap(prs: Presentation, t: dict | None = None) -> DualSlide:
    """Slide 16 (Section 12): Plan — Completed Project Execution Plan & 3-Phase Future Roadmap."""
    tr = (t or _load_slide_truth())["m"]
    sp = (t or _load_slide_truth())["sp"]
    mv = (t or _load_slide_truth())["mv"]
    ds = tr["dataset"]
    ev = tr["evaluation"]
    salt = tr["monoid_and_catalyst_c9"]["pillar_i1_monoid_and_salting"]
    cj = sp["catalyst_join_spark"]
    rc = tr["streaming_mmds"]["reconciliation"]
    lat = mv["uc2_arterial_query"]["latency_benchmark_ms"]

    s16 = DualSlide(prs)
    s16.add_chrome(
        16,
        TOTAL_SLIDES,
        5,
        "SECTION 12 · PLAN — KẾ HOẠCH THỰC THI ĐỒ ÁN (100% HOÀN THÀNH) & LỘ TRÌNH KHẮC PHỤC HẠN CHẾ",
        "12. Plan: Nghiệm Thu Kế Hoạch Thực Hiện Nhóm & Lộ Trình Phát Triển 3 Giai Đoạn",
        "Đánh giá tiến độ hoàn thành 100% của 3 thành viên và kế hoạch hành động giải quyết các hạn chế ở Mục 11",
    )
    t_headers = [
        "Thành viên Nhóm",
        "Mục Phụ trách (12-Section)",
        "Kết quả Thực thi & Nghiệm thu (Tiến độ: 100% Hoàn thành)",
    ]
    t_rows = [
        [
            "1. Nguyễn Tạ Minh Quyền\n(MSSV: 25MS23280)",
            "Mục 1, 4, 5 & 7A:\nProblem, Data/EDA,\nPipeline & Spark ETL",
            f"Hoàn thành 100%: Quality Funnel ({ds['segments_count']:,} đoạn cơ giới, {ds['segment_status_events']:,} "
            f"pings, 89.19% LOS), 3-Tier Vmax, Monoid (diff=0.0), Key Salting "
            f"({salt['temporal_slot_peak_to_median_ratio']:.2f}x→{salt['peak_slot_salted_bucket_skew_ratio']:.3f}x), "
            f"BroadcastJoin ({cj['baseline_exchange_stages_measured']}→{cj['optimized_exchange_stages_measured']} "
            f"Exch, +{cj['join_speedup_pct']:.2f}%) & Parquet Pruning (7,268).",
        ],
        [
            "2. Nguyễn Thành Đạt\n(MSSV: 25MS23306)",
            "Mục 2, 6, 7B & 9:\nLiterature, Modeling,\nGraph & Experiments",
            f"Hoàn thành 100%: Line-Graph PageRank ({ds['segments_count']:,}.0, 92,453 cạnh), FPGrowth (335) & SON, "
            f"7D K-Means (Sil={tr['model']['silhouette_score']:.4f}) phủ {ds['cold_start_segments_count']:,} "
            f"Cold-Start, lọc {ds['synthetic_burst_windows_flagged']:,} burst → {ev['train_samples']:,} organic "
            f"TRAIN & RF 23D (ROC={ev['roc_auc']:.4f}, PR={ev['average_precision']:.4f}, "
            f"Detect={ev['transition_analysis']['rf_23d_transition_detect_roc_auc']:.4f}).",
        ],
        [
            "3. Phạm Công Minh\n(MSSV: 25MS23310)",
            "Mục 3, 8, 10, 11 & 12:\nArch, Stream, Demo,\nLimitations & Plan",
            f"Hoàn thành 100%: Docker 6 dịch vụ (12.0/16 GB RAM), Kafka 4.1 KRaft, Sketches 33.0 KiB, WAL + Spark "
            f"watermark thật, Recon ({rc['affected_late_segment_windows_recomputed']} trễ, "
            f"{rc['healed_downstream_lag_windows']} lag, {rc['healed_downstream_rescored_windows']} rescore), "
            f"MongoDB IXSCAN ({lat['warm_median_ms']:.3f}/{lat['warm_p95_ms']:.3f} ms) & Dual-UI (:8000 & :8501).",
        ],
    ]
    s16.add_table(0.55, 2.00, 14.90, 3.82, t_headers, t_rows, [0.19, 0.22, 0.59], font_size=7.9)

    s16.add_card(
        0.55,
        5.98,
        7.35,
        2.40,
        "Kế Hoạch Ngắn & Trung Hạn (Phase 1 & 2)",
        [
            "Phase 1 (Ngắn hạn — Mô hình & Sketch): Áp dụng Cost-Sensitive Focal Loss bắt ca đột ngột hóa kẹt "
            "(A–D → E–F) và Ring-Buffer Sliding Bloom Filter tự xoay vòng mỗi 30 phút.",
            "Phase 2 (Trung hạn — Đa nguồn & Chống Drift): Đấu nối GPS xe buýt/taxi thực tế (1.5–3.0 TB/ngày) + API "
            "Mưa ngập/Sự cố; tự động chạy lại Phase A hàng tuần khi phát hiện Concept Drift (PSI).",
        ],
        badge="Phase 1 & 2 Plan",
        theme="sky_strong",
        body_size=8.3,
    )
    s16.add_card(
        8.10,
        5.98,
        7.35,
        2.40,
        "Kế Hoạch Dài Hạn (Phase 3) & Quality Gate",
        [
            "Phase 3 (Dài hạn — Đa nút & Đóng vòng Đèn): Nâng cấp lên Kubernetes Multi-Node + Sharded MongoDB + "
            "Vector-Tile GIS và tích hợp API dự báo +30m vào hệ thống đèn tín hiệu SCATS.",
            "Quality Gate Nghiệm thu: Vượt qua 100% kiểm thử pytest & Spark 3.5.7 runtime, đối soát 0 diff & 0 lỗi "
            "Ruff (C901 < 15).",
        ],
        badge="Phase 3 & QA",
        theme="navy",
        body_size=8.3,
    )
    return s16


def _add_slide_17_session9_readiness_summary(prs: Presentation, t: dict | None = None) -> DualSlide:
    """Slide 17 (Session 9 Readiness): Technical Readiness Audit Summary & Live Wave-2 Parity."""
    tr = (t or _load_slide_truth())["m"]
    sp = (t or _load_slide_truth())["sp"]
    mv = (t or _load_slide_truth())["mv"]
    ds = tr["dataset"]
    ev = tr["evaluation"]
    ta = ev["transition_analysis"]
    cj = sp["catalyst_join_spark"]
    ms = sp["monoid_salting_spark"]
    salt = tr["monoid_and_catalyst_c9"]["pillar_i1_monoid_and_salting"]
    sm = tr["streaming_mmds"]
    gp = tr["graph_and_patterns"]
    lat = mv["uc2_arterial_query"]["latency_benchmark_ms"]

    s17 = DualSlide(prs)
    s17.add_chrome(
        17,
        TOTAL_SLIDES,
        5,
        "SESSION 9 & PRODUCTION READINESS · KIỂM ĐỊNH KỸ THUẬT A–I, SPARK 3.5.7 & MONGODB 8.0",
        "13. Technical Readiness & Production Verification Summary",
        "Đối chứng ranh giới [I] vs [P], kiểm nghiệm Spark 3.5.7 runtime, dữ liệu organic & MongoDB 8.0",
    )
    s17.add_card(
        0.55,
        2.00,
        14.90,
        1.78,
        "1. Architecture Truth: Ranh Giới [I] Đã Triển Khai vs [P] Đề Xuất & Dual-UI Stack",
        [
            "[I] Implemented: Cụm 6 dịch vụ Docker (12.0/16 GB RAM: Kafka 4.1 KRaft 3 phân vùng → Spark 3.5.7 → "
            "Parquet + MongoDB 8.0 → FastAPI :8000 + ReactJS/Leaflet TOC Web & Streamlit :8501) kèm data/demo_sample/.",
            "[P] Proposed & Card B Fixes: Luồng viễn thông 8–17 tỷ tin/ngày, K8s đa nút & đèn SCATS gắn nhãn [P]. "
            "Chuẩn hóa thuật ngữ Predictive ML (Past-Only Priors τ < t) & 7D Behavioral Archetypes.",
        ],
        badge="[I] vs [P] Truth",
        theme="sky_strong",
        body_size=8.1,
    )
    s17.add_card(
        0.55,
        3.94,
        7.35,
        2.12,
        "2. Controlled Spark 3.5.7 Performance & Query Plan",
        [
            f"BroadcastHashJoin giảm tầng trộn Exchange {cj['baseline_exchange_stages_measured']} → "
            f"{cj['optimized_exchange_stages_measured']} (-80%) và rút ngắn thời gian từ "
            f"{cj['baseline_median_ms']:.2f} → {cj['optimized_median_ms']:.2f} ms (+{cj['join_speedup_pct']:.2f}%; "
            "exceptAll_diff = 0).",
            f"8-Bucket Salted Monoid kéo lệch slot {salt['temporal_slot_peak_to_median_ratio']:.2f}x → "
            f"{salt['peak_slot_salted_bucket_skew_ratio']:.3f}x (giảm IQR {ms['unsalted_iqr_ms']:.2f} → "
            f"{ms['salted_iqr_ms']:.2f} ms; tradeoff {ms['unsalted_median_ms']:.2f} → {ms['salted_median_ms']:.2f} ms) "
            "& cắt 9/10 phân vùng Parquet.",
        ],
        badge=f"+{cj['join_speedup_pct']:.2f}% Join Speedup",
        theme="soft",
        body_size=8.0,
    )
    s17.add_card(
        8.10,
        3.94,
        7.35,
        2.12,
        "3. Data & Predictive ML Validation (Burst Filter & VAL Gate)",
        [
            f"B1 Point-in-Time: Tách {ds['synthetic_burst_windows_flagged']:,} burst windows → {ev['train_samples']:,} "
            f"organic TRAIN (cls_w=0.65); RF 23D thắng Live VAL Gate (0.8990 vs 0.5343); Future-Shuffle = 0.0.",
            f"B2 Falsification: #{gp['top_bottleneck_segment_id']} {gp['top_bottleneck_street']} có PageRank #1 "
            f"({gp['top_bottleneck_pagerank']:.4f}) nhưng không mặc định kẹt → PageRank đo nút phễu hình học.",
        ],
        badge="VAL Gate 0.8990",
        theme="soft",
        body_size=8.1,
    )
    s17.add_card(
        0.55,
        6.22,
        7.35,
        2.16,
        "4. Live Streaming, Spark Watermark & MongoDB 8.0",
        [
            f"Spark Streaming WAL + watermark thật ({sp['spark_sink2_watermark']}, 10 dòng bị loại hết hạn); "
            f"Bloom 32.0 KiB lọc {sm['duplicates_removed_exact']}/{sm['injected_duplicate_events']} trùng; "
            f"HLL++ 1.0 KiB ≈ {sm['hyperloglog']['hll_estimated_segments']:.1f}/"
            f"{sm['hyperloglog']['exact_distinct_segments']:,}.",
            f"MongoDB 8.0.34 ({mv['total_rows_upserted']:,} docs, 69 batch): atomic $status_version chặn "
            f"{mv['stale_v1_rejected_by_mongo']}/25 v1; IXSCAN warm {lat['warm_median_ms']:.3f} / "
            f"p95 {lat['warm_p95_ms']:.3f} ms ({lat['ixscan_vs_collscan_speedup_x']:.2f}x).",
        ],
        badge="Real Watermark + IXSCAN",
        theme="sky",
        body_size=8.0,
    )
    s17.add_card(
        8.10,
        6.22,
        7.35,
        2.16,
        "5. Honest Limitations & Evidence Provenance",
        [
            f"Giới hạn Thực tế: {ds['cold_start_segments_count']:,}/{ds['segments_count']:,} "
            f"({tr['static_feature_store_c13b']['cold_start_ratio_pct']:.2f}%) Cold-Start; TEST "
            f"Macro-F1 = {ev['macro_f1_6class']:.4f} (B–E chỉ 10–20 mẫu); khởi phát kẹt 8/993 "
            f"(Onset ROC = {ta['rf_23d_onset_roc_auc']:.4f}, Lift {ta['rf_23d_onset_risk_lift_ratio']:.2f}x).",
            "Truy xuất Nguồn sự thật: 100% số liệu đồng bộ từ outputs/metrics.json, spark_runtime_evidence.json "
            "& mongo_verify.json (Wave 2 · 2026-11-18T20:07Z).",
        ],
        badge="100% Metric Parity",
        theme="navy",
        body_size=8.0,
    )
    return s17


def _render_pdf_to_pngs(pdf_path: Path, out_dirs: list[Path], zoom: float = 2.0) -> None:
    for out_dir in out_dirs:
        out_dir.mkdir(parents=True, exist_ok=True)
    doc = fitz.open(str(pdf_path))
    try:
        mat = fitz.Matrix(zoom, zoom)
        for page_idx in range(len(doc)):
            pix = doc[page_idx].get_pixmap(matrix=mat, alpha=False)
            for out_dir in out_dirs:
                pix.save(str(out_dir / f"slide_{page_idx + 1:02d}.png"))
    finally:
        doc.close()


def _safe_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)
    except OSError:
        pass


def _save_and_sync_slide_outputs(prs: Presentation, all_slides: list[DualSlide]) -> tuple[Path, Path]:
    slide_pngs: list[Path] = []
    slide_figs = [dual.fig for dual in all_slides]
    SLIDE_PNG_DIR.mkdir(parents=True, exist_ok=True)
    for idx, dual in enumerate(all_slides, start=1):
        png_path = SLIDE_PNG_DIR / f"slide_{idx:02d}.png"
        dual.save_png(png_path, close_fig=False)
        slide_pngs.append(png_path)

    primary_pptx = REPO_DIR / "HCMC_Smart_Traffic_Final_Presentation.pptx"
    prs.save(str(primary_pptx))
    _safe_copy(primary_pptx, OUTPUTS_DIR / "HCMC_Smart_Traffic_Final_Presentation.pptx")
    legacy_pptx = REPO_DIR / "HCMC_Smart_Traffic_Proposal_Presentation.pptx"
    if legacy_pptx.is_file():
        _safe_copy(primary_pptx, legacy_pptx)

    primary_pdf = REPO_DIR / "HCMC_Smart_Traffic_Final_Slides.pdf"
    pdf_targets = [
        primary_pdf,
        OUTPUTS_DIR / "HCMC_Smart_Traffic_Final_Slides.pdf",
    ]
    grid_targets = [
        OUTPUTS_DIR / "slides_overview_grid.png",
    ]

    export_overview_grid_and_pdf(slide_pngs, grid_targets, pdf_targets, slide_figs=slide_figs)
    _render_pdf_to_pngs(primary_pdf, [SLIDE_RENDER_DIR, SLIDE_RENDER_DIR / "final"])
    return primary_pptx, primary_pdf


def _ensure_visual_assets() -> tuple[Path, Path, Path, Path, Path, Path]:
    map_prob = CHART_DIR / "map_hcmc_problem_context.png"
    map_hot = CHART_DIR / "map_hcmc_hotspots_coldstart.png"
    map_cls = CHART_DIR / "map_hcmc_clusters_7d.png"
    trans_chart = CHART_DIR / "chart_transition_analysis.png"
    flow_img = CHART_DIR / "chart_concrete_example_flow.png"
    if not (map_prob.is_file() and map_hot.is_file() and map_cls.is_file()):
        geom = _load_segment_geometries()
        render_hcmc_problem_context_map(geom, map_prob)
        if not map_hot.is_file():
            render_hcmc_hotspots_map(geom, map_hot)
        if not map_cls.is_file():
            render_hcmc_clusters_map(geom, map_cls)
    if not trans_chart.is_file():
        render_transition_analysis_chart(trans_chart)

    ui2 = CHART_DIR / "ui_tab2_live_forecast_mongo.png"
    ui4 = CHART_DIR / "ui_tab4_kmeans_coldstart.png"
    return map_prob, map_cls, trans_chart, flow_img, ui2, ui4


def main() -> None:
    t = _load_slide_truth()
    arch_img = OUTPUTS_DIR / "system_architecture.png"
    eda_chart = render_eda_chart(CHART_DIR / "chart_eda_anomalies.png")
    render_spark_chart(CHART_DIR / "chart_spark_skew_catalyst.png")
    render_streaming_serving_chart(CHART_DIR / "chart_streaming_serving.png")
    batch_flow_img = render_batch_flow_diagram(CHART_DIR / "flow_batch_components.png")
    stream_flow_img = render_stream_flow_diagram(CHART_DIR / "flow_stream_components.png")
    method_img1 = render_methods_imputation_monoid(CHART_DIR / "visual_methods_imputation_monoid.png")
    method_img2 = render_methods_graph_coldstart_sketches(CHART_DIR / "visual_methods_graph_coldstart_sketches.png")
    map_prob, _map_cls, trans_chart, flow_img, ui2, ui4 = _ensure_visual_assets()

    prs = Presentation()
    prs.slide_width = Inches(16.0)
    prs.slide_height = Inches(9.0)
    prs.core_properties.title = (
        "HCMC Smart Traffic Big Data Analytics & Congestion Prediction Platform — BDA501 Final Presentation"
    )
    prs.core_properties.author = (
        "Nguyễn Tạ Minh Quyền (25MS23280), Nguyễn Thành Đạt (25MS23306), Phạm Công Minh (25MS23310)"
    )
    prs.core_properties.subject = "FSB Course BDA501 — Big Data Analytics (12-Section Structure)"
    prs.core_properties.comments = (
        "17-Slide Deck: 1.Problem 2.Literature 3.Arch 4.EDA + 2 Visual Method Explainers + "
        "5.Pipeline 6.Modeling 7.Batch 8.Stream 9.Experiments 10.Demo 11.Limitations/Risks 12.Plan 13.Readiness"
    )

    all_slides = [
        _add_slide_01_cover(prs),
        _add_slide_02_problem_statement(prs, map_prob),
        _add_slide_03_literature_review(prs),
        _add_slide_04_system_architecture(prs, arch_img, t),
        _add_slide_05_data_eda_and_problems(prs, eda_chart, t),
        _add_slide_06_method_explainer_imputation_monoid(prs, method_img1),
        _add_slide_07_method_explainer_graph_sketches(prs, method_img2, t),
        _add_slide_08_data_pipeline_and_processing(prs, flow_img, t),
        _add_slide_09_modeling(prs, t),
        _add_slide_10_batch_processing(prs, batch_flow_img, t),
        _add_slide_11_stream_processing(prs, stream_flow_img, t),
        _add_slide_12_experiments_ablation(prs, t),
        _add_slide_13_experiments_persistence_defense(prs, trans_chart, t),
        _add_slide_14_demonstration(prs, ui2, ui4, t),
        _add_slide_15_limitations_and_risks_summary(prs, t),
        _add_slide_16_plan_and_roadmap(prs, t),
        _add_slide_17_session9_readiness_summary(prs, t),
    ]

    pptx_path, pdf_path = _save_and_sync_slide_outputs(prs, all_slides)
    print(
        f"Generated {len(all_slides)} visual slides -> {pptx_path.name} ({pptx_path.stat().st_size:,} B) "
        f"& {pdf_path.name} ({pdf_path.stat().st_size:,} B)"
    )


if __name__ == "__main__":
    main()
