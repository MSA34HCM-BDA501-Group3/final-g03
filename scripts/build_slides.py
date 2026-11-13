"""Build the 16-Slide Executive, Conference & Academic Defense Presentation (.pptx, .pdf & HD .pngs)."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
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
WORKSPACE_DIR = REPO_DIR.parent
ARTIFACT_DIR = Path(r"C:\Users\PC\.gemini\antigravity\brain\f813022d-2858-4604-88b6-b1934862999e")
CHART_DIR = REPO_DIR / "outputs" / "slide_charts"
SLIDE_PNG_DIR = REPO_DIR / "outputs" / "slides"
TOTAL_SLIDES = 16


def render_eda_chart(out_path: Path) -> Path:
    fig, ax = plt.subplots(figsize=(6.65, 6.10), dpi=220)
    labels = [
        "UTC → Giờ VN Rollover (+7h)\n(4,320 / 33,441 cửa sổ 30m)",
        "Non-Contiguous GPS Gaps (> 30m)\n(15,516 / 23,414 cặp quan sát)",
        "Streets Missing max_velocity\n(4,566 / 5,553 tuyến đường)",
        "Windows Missing max_velocity\n(28,495 / 33,441 cửa sổ 30m)",
        "Cold-Start Segments (0 GPS)\n(74,606 / 84,633 đoạn đường)",
        "Segments Missing max_velocity\n(74,762 / 84,633 đoạn đường)",
        "Intermediate Shape Points\n(525,542 / 577,967 nodes)",
    ]
    vals = [12.92, 66.27, 82.23, 85.21, 88.15, 88.34, 90.93]
    cols = ["#38BDF8", "#0EA5E9", "#0284C7", "#0369A1", "#1D4ED8", "#1E3A8A", "#0F172A"]
    y = np.arange(len(labels))
    bars = ax.barh(y, vals, color=cols, height=0.58, edgecolor="none")
    ax.set_yticks(y, labels, fontsize=8.3, fontweight="bold", color="#0F172A")
    ax.set_xlim(0, 112)
    ax.set_xlabel(
        "Tỷ lệ khuyết thiếu & đặc thù dữ liệu thực nghiệm (%)", fontsize=8.8, fontweight="bold", color="#334155"
    )
    ax.set_title(
        "5-Table EDA: Empirical Data Sparsity & Anomaly Profile",
        fontsize=10.1,
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
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6.95, 6.10), dpi=220)
    bars1 = ax1.barh(
        ["2-Stage Key Salting\n(8 Salt Buckets)", "Un-salted GroupBy\n(Rush-Hour Skew)"],
        [1.023, 687.43],
        color=["#059669", "#DC2626"],
        height=0.50,
    )
    ax1.set_xscale("log")
    ax1.set_xlim(0.5, 2500)
    ax1.set_xlabel("Peak-to-Median Partition Load Ratio (Log Scale)", fontsize=8.5, fontweight="bold")
    ax1.set_title(
        "2-Stage Key Salting (8 Buckets · CV = 0.0057)",
        fontsize=9.6,
        fontweight="bold",
        color="#0F172A",
    )
    ax1.grid(axis="x", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    for bar, txt in zip(bars1, ["1.023x (Balanced)", "687.43x (Hot Partition Skew)"]):
        ax1.text(
            bar.get_width() * 1.18,
            bar.get_y() + bar.get_height() / 2,
            txt,
            va="center",
            fontsize=8.4,
            fontweight="bold",
            color="#0F172A",
        )

    x = np.arange(2)
    w = 0.32
    r1 = ax2.bar(x - w / 2, [10, 5], w, label="SortMergeJoin (Baseline)", color="#64748B")
    r2 = ax2.bar(x + w / 2, [2, 0], w, label="BroadcastHashJoin (Optimized)", color="#0284C7")
    ax2.set_xticks(
        x,
        [
            "Full Window Pipeline\n(0.946s → 0.506s)",
            "Static Topology Join\n(0.932s → 0.666s)",
        ],
        fontsize=8.4,
        fontweight="bold",
    )
    ax2.set_ylim(0, 12.2)
    ax2.set_ylabel("Shuffle Exchanges", fontsize=8.5, fontweight="bold")
    ax2.set_title(
        "Catalyst Physical Plan: BroadcastHashJoin vs. SortMergeJoin",
        fontsize=9.6,
        fontweight="bold",
        color="#0F172A",
    )
    ax2.legend(fontsize=8.1, loc="upper right")
    ax2.grid(axis="y", linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    for bars in (r1, r2):
        for bar in bars:
            height = bar.get_height()
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                height + 0.3,
                f"{int(height)}",
                ha="center",
                fontsize=8.6,
                fontweight="bold",
                color="#0F172A",
            )

    plt.tight_layout(pad=1.2)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def render_streaming_serving_chart(out_path: Path) -> Path:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6.85, 6.10), dpi=220)
    stages = [
        "provisional_stream (v1)\n(Active Tail Windows)",
        "late_backfilled_by_batch (v2)\n(Late > 30m Watermark)",
        "batch_verified (v2)\n(100% On-Time Parity)",
    ]
    counts = [488, 501, 32452]
    b1 = ax1.barh(stages, counts, color=["#0EA5E9", "#D97706", "#059669"], height=0.52)
    ax1.set_xscale("log")
    ax1.set_xlim(100, 190000)
    ax1.set_xlabel("30-Minute Windows in Serving Store (Total = 33,441)", fontsize=8.4, fontweight="bold")
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
        "Target SLA\n(< 10 ms)",
    ]
    lat_vals = [0.530, 0.636, 1.113, 10.0]
    lat_cols = ["#0284C7", "#059669", "#0D9488", "#94A3B8"]
    x2 = np.arange(len(lat_labels))
    b2 = ax2.bar(x2, lat_vals, width=0.46, color=lat_cols)
    ax2.set_xticks(x2, lat_labels, fontsize=8.3, fontweight="bold", color="#0F172A")
    ax2.set_ylim(0, 11.8)
    ax2.set_ylabel("Query Latency (ms)", fontsize=8.4, fontweight="bold")
    ax2.set_title(
        "MongoDB 8.0.34 Compound Index IXSCAN Latency (< 1 ms)",
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
            val + 0.30,
            f"{val:.3f} ms" if val < 5 else f"{val:.1f} ms",
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


def _add_slide_04_system_architecture(prs: Presentation, arch_img: Path) -> DualSlide:
    """Slide 04 (Section 3): System Architecture (Zones A-F & Pipelines) + Component Limitations & Risks."""
    s4 = DualSlide(prs)
    s4.add_chrome(
        4,
        TOTAL_SLIDES,
        1,
        "SECTION 03 · SYSTEM ARCHITECTURE — KIẾN TRÚC TỔNG THỂ (ZONES A–F · 4 PIPELINES) & 3-PHASE MLOPS",
        "3. System Architecture: Kiến Trúc Hybrid Batch–Speed & Vòng Đời 3-Phase MLOps",
        (
            "Kiến trúc nền tảng tích hợp giải quyết đồng thời vùng mù quan trắc, dự báo sớm +30 phút và cảnh báo "
            "trực tuyến"
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
            "Phase A (Batch Fit): Học 1 lần trên 5,494 mẫu TRAIN, khóa SHA-256 tại Frozen Registry.",
            "Phase B (Speed Layer): Chỉ đọc .predict_proba() phát cảnh báo tạm thời v1 (< 1 s).",
            "Phase C (Recon): Cuối ngày bù 501 cửa sổ trễ, re-score 377 cửa sổ → chuẩn v2 trong 10.5 / 16 GB RAM.",
        ],
        badge="0 Write Conflict",
        theme="sky",
        body_size=8.3,
    )
    s4.add_card(
        11.26,
        5.26,
        4.19,
        3.12,
        "Limitations & Risks (Arch)",
        [
            "Single-Host Contention: Chạy chung Kafka (1.5G), Spark (6G), Mongo (2G), UI (1G) trên 1 máy tiềm ẩn "
            "tranh chấp I/O đĩa khi Shuffle lớn.",
            "Eventual Consistency Gap: Cảnh báo luồng (v1) phản hồi tức thì, nhưng cập nhật biến trễ từ các gói tin "
            "đến muộn > 30m phải chờ Phase C cuối ngày.",
        ],
        badge="Arch Risk",
        theme="navy",
        body_size=8.3,
    )
    return s4


def _add_slide_05_data_eda_and_problems(prs: Presentation, eda_chart: Path) -> DualSlide:
    """Slide 05 (Section 4): Data, EDA & Data Problems (5 Raw Tables & EC-1..EC-10)."""
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

    s5.add_card(
        0.55,
        6.28,
        6.25,
        2.10,
        "Hồ Sơ 5 Bảng Gốc & Proof-of-Scale",
        [
            "5 bảng gốc Kaggle (38.50 MB): nodes (577,967 điểm), streets (5,553 đường), segments (84,633 đoạn), "
            "segment_status (90,938 pings; vd: 07:08 Lý Thường Kiệt 4 km/h) & train (33,441 cửa sổ đối chứng).",
            "Mẫu chuẩn Proof-of-Scale kiểm chứng sai số 0.0 cho quy mô toàn TP.HCM (8–17 tỷ tin/ngày, "
            "1.5–3.0 TB/ngày); chỉ 11.85% đoạn có GPS và 94.84% mẫu TEST giữ nguyên mức cũ sau 30 phút.",
        ],
        badge="Proof-of-Scale",
        theme="sky",
        body_size=8.0,
    )

    ec_headers = ["Data Problem (EC)", "Hiện tượng trong 5 Bảng Gốc", "Giải pháp Kỹ thuật"]
    ec_rows = [
        ["EC-1 · Missing Vmax", "85.21% cửa sổ (28,495) thiếu Vmax", "Train-Only 3-Tier Imputation (P85/Median/Prior)"],
        ["EC-2 · Bad Geometry", "154 đoạn ≤ 0m & 427 lối đi bộ", "ANSI try_cast Quarantine + trung vị 27.0m"],
        ["EC-3 · Unicode Drift", "631 dòng Quận & 2,071 dòng lệch dấu", "Chuẩn hóa Unicode NFC (695 → 649 tên đường)"],
        ["EC-4 · UTC Rollover", "4,320 cửa sổ (12.92%) lệch ngày UTC+7", "Đồng bộ vn_hour (+7h), ict_date & sin/cos"],
        [
            "EC-5 · GPS Gaps >30m",
            "66.27% (15,516 cặp) ngắt quãng ≥ 60m",
            "Exact (segment_id, slot_ts + 30m) → 7,898 cặp",
        ],
        [
            "EC-6 · Cold-Start",
            "88.15% (74,606 đoạn) có 0 bản tin GPS",
            "Static Feature Store (PageRank + K-Means)",
        ],
        ["EC-7 · Shape Points", "90.93% (525,542 nodes) vẽ cong & 48 cụt", "Cắt tỉa shape points + bù Dangling Sinks"],
        ["EC-8 · Data Skew", "Lệch tải 687.43x & sai số avg 11.11 km/h", "5-Tuple Monoid + 2-Stage Salting (8 giỏ)"],
        ["EC-9 · Late & Dupes", "334 tin trùng (1%) & 501 cửa sổ trễ > 30m", "32.0 KiB Bloom Filter + Tier-2 Backfill"],
        [
            "EC-10 · Crash & Stale",
            "Sập luồng đột ngột & tin cũ v1 đến muộn",
            "WAL Checkpoint (0 dupes) + version_ts Guard",
        ],
    ]
    s5.add_table(6.96, 2.00, 8.49, 6.38, ec_headers, ec_rows, [0.22, 0.38, 0.40], font_size=7.9)
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


def _add_slide_07_method_explainer_graph_sketches(prs: Presentation, method_img2: Path) -> DualSlide:
    """Slide 07: Visual Method Explainer (2/2) — Line-Graph PageRank, 7D K-Means Cold-Start & O(1) Sketches."""
    sm2 = DualSlide(prs)
    sm2.add_chrome(
        7,
        TOTAL_SLIDES,
        2,
        "METHODOLOGY VISUALIZER (2/2) — TRỰC QUAN HÓA LINE-GRAPH PAGERANK, 7D K-MEANS & O(1) SKETCHES",
        "Giải Thích Phương Pháp (2/2): Line-Graph PageRank, K-Means & Bộ Đếm O(1)",
        "Cách phát hiện nút cổ chai bản đồ, suy diễn cho 88.15% đường thiếu GPS "
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
            "Line-Graph PageRank biến mỗi đoạn đường thành 1 nút để chỉ ra nơi dòng xe dồn tụ tự nhiên; sau đó 7D "
            "K-Means ghép các đoạn mù (0 GPS) vào nhóm đường có cảm biến tương đồng để phủ kín 100% đô thị.",
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


def _add_slide_08_data_pipeline_and_processing(prs: Presentation, flow_img: Path) -> DualSlide:
    """Slide 08 (Section 5): Data Pipeline & Data Processing + Component Limitations & Risks."""
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
            "5-Step Quality Funnel: ANSI try_cast tách bản ghi lỗi sang Quarantine (0 sập job, 0 rơi ngầm); lọc 427 "
            "lối đi bộ; chuẩn hóa NFC (695 → 649 đường) & UTC+7 (4,320 cửa sổ).",
            "Train-Only 3-Tier Vmax (vmax_lookup.json): Giữ gốc (4,946) → P85 đoạn (8,164) → Median cấp×loại (20,124) "
            "→ Chuẩn đô thị 80/60/50/40 km/h (207 cửa sổ).",
            "Gán nhãn 33,441 cửa sổ 30m (vận tốc / Vmax) sang LOS A–F: A–B (≥65%) · C–D (35–65%) · E–F (<35%).",
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
            "Static Vmax Approximation: 20,331 cửa sổ (60.79%) dùng Vmax suy diễn theo cấp đường (Tier 2 & 3), chưa "
            "phản ánh rào chắn thi công hay ngập nước cục bộ.",
            "Schema Drift Quarantine Risk: ANSI try_cast chống sập job, nhưng thiết bị GPS đổi định dạng hàng loạt sẽ "
            "làm tăng tỷ lệ cách ly vào Quarantine.",
            "30m Slot Discretization: Làm tròn cửa sổ 30 phút làm mượt dao động vận tốc ngắn dưới 5 phút tại ngã tư.",
        ],
        badge="Pipeline Risk",
        theme="navy",
        body_size=7.9,
    )
    return s8


def _add_slide_09_modeling(prs: Presentation) -> DualSlide:
    """Slide 09 (Section 6): Modeling (+30m Forecasting, 23D Features & Cold-Start) + Component Limitations."""
    s9 = DualSlide(prs)
    s9.add_chrome(
        9,
        TOTAL_SLIDES,
        2,
        "SECTION 06 · MODELING — EXACT T+30M PAIRING, 23D FEATURES & FROZEN RANDOM FOREST",
        "6. Modeling: Thiết Kế Nhãn Exact t+30m, Vector 23 Biến & Mô Hình Random Forest",
        "Đảm bảo 0.0 rò rỉ tương lai, tích hợp đặc trưng đồ thị/phân cụm vào dự báo +30m và chấm điểm Cold-Start",
    )
    s9.add_card(
        0.55,
        2.00,
        7.35,
        3.08,
        "Exact t+30m Pairing & Zero Leakage",
        [
            "Exact (segment_id, slot_ts + 30m) Join: Thu được 7,898 cặp chuẩn xác "
            "(loại bỏ 15,516 cặp đứt quãng ≥ 60m).",
            "Chronological Split 70/15/15 + Boundary Purge: TRAIN = 5,494 mẫu (≤ 2020-12-12T08:00Z, cắt 50 mẫu biên) · "
            "VAL = 1,167 mẫu (≤ 2020-12-25T03:30Z, cắt 5 mẫu biên) · TEST = 1,182 mẫu.",
            "Future-Shuffle Falsification Test: Xáo trộn ngẫu nhiên tương lai τ > t cho sai lệch ma trận X_t = 0.0.",
        ],
        badge="7,898 Exact Pairs",
        theme="white",
        body_size=8.4,
    )
    s9.add_card(
        0.55,
        5.24,
        7.35,
        3.14,
        "Tổ Hợp 23 Biến Đầu Vào (23D Features)",
        [
            "Nhóm 1 — Local State & Lags (7D · 79.82% Importance): current_los_ordinal (50.51%), mean_velocity "
            "(22.36%), trailing_severe_rate (6.23%), lag1/lag2_los_ordinal, velocity_ratio, obs_count.",
            "Nhóm 2 — Cyclical ICT Time & Geometry (7D · 12.18%): period_sin (10.18%), period_cos, length_m, weekday, "
            "street_level, max_velocity, gap_minutes.",
            "Nhóm 3 — Static Graph, 7D K-Means & Priors (9D · 8.00%): segment_train_severe_prior (4.92%), "
            "pagerank_score, street_causal_prior, neighbor_los, in/out_degree, cluster_id.",
        ],
        badge="23D Features",
        theme="sky",
        body_size=8.3,
    )
    s9.add_card(
        8.10,
        2.00,
        7.35,
        3.08,
        "Kiến Trúc Random Forest & Cold-Start",
        [
            "Champion Model (#5 Random Forest 23D): 250 cây, max_depth=14, class_weight='balanced_subsample', tuyển "
            "chọn trên VAL (ROC-AUC = 0.9593) và khóa SHA-256 tại outputs/model.joblib.",
            "Cold-Start Scoring: Kết hợp Static Feature Store (PageRank, cluster_id, priors) với chu kỳ giờ VN "
            "để dự báo cho cả 74,606 đoạn chưa có cảm biến (phủ 100% 84,633 đoạn).",
        ],
        badge="Model & Cold-Start",
        theme="sky_strong",
        body_size=8.4,
    )
    s9.add_card(
        8.10,
        5.24,
        7.35,
        3.14,
        "Limitations & Risks (Modeling)",
        [
            "Ngưỡng mặc định bảo thủ khi đổi pha: Do 94.84% mẫu đứng yên, ngưỡng argmax mặc định bị thận trọng (đạt "
            "2/61 ở mặc định; tăng lên 9/61 khi hiệu chỉnh ngưỡng τ = 0.30 trên VAL).",
            "Exogenous Blindness: Chưa có biến mưa ngập, tai nạn hay sự kiện đột xuất nên chỉ phát hiện kẹt sau khi "
            "vận tốc GPS bắt đầu giảm.",
        ],
        badge="Modeling Risk",
        theme="navy",
        body_size=8.4,
    )
    return s9


def _add_slide_10_batch_processing(prs: Presentation, batch_flow_img: Path) -> DualSlide:
    """Slide 10 (Section 7): Batch Processing Component Flow, Quantitative Summary & Limitations."""
    s10 = DualSlide(prs)
    s10.add_chrome(
        10,
        TOTAL_SLIDES,
        3,
        "SECTION 07 · BATCH PROCESSING — LUỒNG XỬ LÝ LÔ PHÂN TÁN, KHAI PHÁ ĐỒ THỊ & PHÂN CỤM",
        "7. Batch Processing: Luồng Xử Lý Lô Phân Tán, Khai Phá Đồ Thị & Phân Cụm",
        "Trực quan hóa các thành phần Batch ETL sai số 0.0, Line-Graph PageRank, SON/FPGrowth và 7D K-Means",
    )
    s10.add_image(0.55, 2.00, 14.90, 4.14, batch_flow_img)

    s10.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Kết Quả Thực Thi Batch ETL, Đồ Thị & Phân Cụm",
        [
            "Distributed ETL: 5-Tuple Monoid khớp 100% baseline.py (sai số = 0.0); 2-Stage Key Salting (8 giỏ) giảm "
            "lệch tải 687.43x → 1.023x; BroadcastHashJoin (10 → 2 Shuffle Stages); Parquet Pruning đọc 7,294 / 33,441 "
            "dòng tháng 2020-08.",
            "Graph & Cold-Start: Line-Graph PageRank bảo toàn khối lượng 84,633.0 (#1 Phú Định = 3.7515, #2 Lê Lai = "
            "3.4650); FPGrowth (335 luật) & 2-Pass SON (Lift 28.58x); Train-Only 7D K-Means (k=6, Silhouette = 0.2853) "
            "phủ đủ 74,606 đoạn Cold-Start.",
        ],
        badge="0.0 Error · 84,633 Segs",
        theme="sky",
        body_size=7.9,
    )
    s10.add_card(
        8.10,
        6.28,
        7.35,
        2.10,
        "Limitations & Risks (Batch Processing)",
        [
            "Cỡ mẫu nhỏ ở luật lan truyền trễ SON/FPGrowth: Các luật trễ +30m (Lift 32.92x) chỉ xuất hiện 5–8 cửa sổ "
            "(2.5–4.0 giờ quan sát) do GPS thưa — chỉ dùng làm giả thuyết thăm dò (Exploratory Hypothesis).",
            "Phân cụm Cold-Start tĩnh: 74,606 đoạn thiếu GPS được gán cụm dựa trên hình thái đồ thị và Vmax cấp đường, "
            "chưa phản ánh được các điểm họp chợ tự phát hay nút cổ chai vi mô trong hẻm.",
        ],
        badge="Batch Risk",
        theme="navy",
        body_size=7.9,
    )
    return s10


def _add_slide_11_stream_processing(prs: Presentation, stream_flow_img: Path) -> DualSlide:
    """Slide 11 (Section 8): Stream Processing Component Flow, Quantitative Summary & Limitations."""
    s11 = DualSlide(prs)
    s11.add_chrome(
        11,
        TOTAL_SLIDES,
        3,
        "SECTION 08 · STREAM PROCESSING — LUỒNG XỬ LÝ THỜI GIAN THỰC, O(1) SKETCHES & ĐỐI SOÁT HAI TẦNG",
        "8. Stream Processing: Luồng Cảnh Báo Trực Tuyến, O(1) Sketches & Đối Soát Trễ",
        "Trực quan hóa luồng sự kiện Kafka, bộ lọc 33.0 KiB RAM, đối soát bù trễ hai tầng và phục vụ dưới 1 ms",
    )
    s11.add_image(0.55, 2.00, 14.90, 4.14, stream_flow_img)

    s11.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Kết Quả Luồng O(1), Đối Soát & Phục Vụ < 1 ms",
        [
            "O(1) Sketches & WAL: BitPackedBloomFilter (32.0 KiB) chặn 334/334 tin trùng (FPR 0.616%); HyperLogLog++ "
            "(1.0 KiB) sai số 3.98%; EWMA Hotspot (α=0.15, #1 Lý Thường Kiệt = 6.67) bổ trợ Random Forest (+30m); WAL "
            "hồi phục lỗi 0 bản trùng.",
            "Two-Tier Recon & MongoDB: Tier 1 khớp 100% (32,940 cửa sổ); Tier 2 bù 501 cửa sổ trễ (> 30m), cập nhật "
            "3,953 cửa sổ lag & re-score 377 cửa sổ (98.50% → 100%); MongoDB chặn 25/25 tin cũ v1, Compound Index "
            "IXSCAN đạt 0.636 ms (warm_p95).",
        ],
        badge="33.0 KiB · 0.636 ms",
        theme="sky",
        body_size=7.9,
    )
    s11.add_card(
        8.10,
        6.28,
        7.35,
        2.10,
        "Limitations & Risks (Stream Processing)",
        [
            "Rủi ro bão hòa bit Bloom Filter: Mảng cố định 32.0 KiB (262,144 bits) sẽ tăng FPR nếu chạy liên tục nhiều "
            "ngày ở quy mô 1.5–3.0 TB/ngày mà không xoay vòng (Ring-Buffer Rotation) theo cửa sổ.",
            "Độ trễ cập nhật của sự kiện quá muộn (> 30m Watermark): Bản tin trễ hơn 30 phút do mất sóng di động phải "
            "đợi tiến trình Tier-2 Reconciliation cuối ngày.",
        ],
        badge="Stream Risk",
        theme="navy",
        body_size=7.9,
    )
    return s11


def _add_slide_12_experiments_ablation(prs: Presentation) -> DualSlide:
    """Slide 12 (Section 9A): Experiments — 7-Model Ablation, Confusion Matrix & Infrastructure Benchmarks."""
    s12 = DualSlide(prs)
    s12.add_chrome(
        12,
        TOTAL_SLIDES,
        4,
        "SECTION 09A · EXPERIMENTS (ABLATION & BENCHMARKS) — KẾT QUẢ THỰC NGHIỆM 7 MÔ HÌNH",
        "9A. Experiments: Thực Nghiệm Ablation 7 Mô Hình Dự Báo +30m & Confusion Matrix",
        "Đánh giá định lượng trên 1,182 mẫu TEST độc lập và kiểm chứng lợi ích của các đặc trưng đồ thị/phân cụm",
    )
    ab_headers = [
        "Cấu hình Mô hình (1,182 mẫu TEST)",
        "Biến",
        "6-Class Acc",
        "Macro-F1",
        "Severe F1",
        "ROC-AUC",
        "PR-AUC",
    ]
    ab_rows = [
        ["1. Persistence Baseline (LOS(t+30m) = LOS(t))", "1D", "0.9484", "0.5654", "0.9465", "0.9867", "0.9367"],
        ["2. Decision Tree (Local Only)", "14D", "0.9425", "0.3794", "0.9437", "0.9856", "0.9482"],
        ["3. Decision Tree (+PageRank & K-Means)", "23D", "0.9425", "0.3772", "0.9437", "0.9844", "0.9327"],
        ["4. Random Forest (Local Only)", "14D", "0.9425", "0.5455", "0.9434", "0.9866", "0.9630"],
        ["5. Random Forest (+Graph, Priors & K-Means) [Chọn]", "23D", "0.9450", "0.5543", "0.9409", "0.9882", "0.9471"],
        ["6. Binary GBT (Local Only)", "14D", "N/A", "N/A", "0.7840", "0.9812", "0.9328"],
        ["7. Binary GBT (+Graph, Priors & K-Means)", "23D", "N/A", "N/A", "0.9465", "0.9843", "0.9419"],
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
        "Confusion Matrix & Giá Trị Biến Đồ Thị",
        [
            "Đường chéo Confusion Matrix 6 lớp trên TEST đạt [944, 4, 3, 7, 6, 153] — phân biệt đủ cả 6 mức LOS A–F.",
            "Bổ sung 9 biến đồ thị & phân cụm (14D → 23D) nâng Macro-F1 từ 0.5455 lên 0.5543 (ở GBT nâng Severe F1 từ "
            "0.7840 lên 0.9465) và đạt ROC-AUC = 0.9882 cao nhất toàn cục.",
        ],
        badge="ROC-AUC 0.9882",
        theme="sky",
        body_size=8.4,
    )
    s12.add_card(
        9.06,
        5.24,
        6.39,
        3.14,
        "Kiểm Chứng Hiệu Năng Hạ Tầng Đầu–Cuối",
        [
            "Batch & Lakehouse: Sai số 5-Tuple Monoid = 0.0; 2-Stage Salting giảm lệch tải 687.43x → 1.023x; "
            "Parquet Pruning chỉ đọc 7,294 / 33,441 dòng tháng 2020-08.",
            "Speed & Serving: O(1) Sketches gói gọn trong 33.0 KiB RAM (chặn 100% tin trùng); MongoDB 8.0 IXSCAN "
            "phản hồi p95 = 0.636 ms (< 1 ms SLA).",
        ],
        badge="System Benchmarks",
        theme="white",
        body_size=8.4,
    )
    return s12


def _add_slide_13_experiments_persistence_defense(prs: Presentation, trans_chart: Path) -> DualSlide:
    """Slide 13 (Section 9B): Experiments — Resolving the Persistence Paradox Across 4 Regimes."""
    s13 = DualSlide(prs)
    s13.add_chrome(
        13,
        TOTAL_SLIDES,
        4,
        "SECTION 09B · EXPERIMENTS (DEFENSE) — GIẢI MÃ NGHỊCH LÝ PERSISTENCE BASELINE TRÊN 4 CHẾ ĐỘ",
        "9B. Experiments: Vì Sao Chọn Random Forest (23D) Khi Baseline Đạt 94.84%?",
        "Bóc tách trực diện 1-đối-1 giữa 94.84% thời gian đứng yên và 5.16% thời điểm giao thông chuyển trạng thái",
    )
    t_headers = [
        "Chế độ Kiểm định Thực tế",
        "Persistence Baseline",
        "Random Forest (23D)",
        "Kết luận Thực nghiệm",
    ]
    t_rows = [
        [
            "1. Đổi trạng thái\nLOS(t) ≠ LOS(t+30m)\n(61 / 1,182 mẫu TEST)",
            "Acc = 0 / 61 (0.00%)\nDetect AUC = 0.5000\nBrier = 0.3279",
            "Detect AUC = 0.7664\nAcc: 9 / 61 (14.75% ở τ=0.30;\nmặc định 2 / 61 = 3.28%)\nTop-1 hướng mới: 24 / 61",
            "Baseline mù 100% khi đổi pha;\nRF phát hiện sớm biến động &\ngiảm 10.1% sai số Brier.",
        ],
        [
            "2. Thoáng (A–D) đột ngột\nhóa kẹt cứng (E–F)\n(8 / 993 mẫu TEST)",
            "Hard Recall = 0 / 8 (0.0%)\nGán xác suất kẹt = 0% cho\nmọi đoạn đang mức A–D",
            "Hard Recall mặc định = 0 / 8\n(mẫu cực hiếm 0.81%)\nRanking ROC-AUC = 0.8136\nXác suất rủi ro tăng 9.35x",
            "Ngưỡng cứng chưa bắt kịp 8 ca\nhiếm, nhưng xác suất RF tăng\nvọt gấp 9.35x.",
        ],
        [
            "3. Cặp tín hiệu GPS\nngắt quãng > 30 phút\n(3,513 mẫu · 47.45% đổi)",
            "Severe PR-AUC = 0.6369\nROC-AUC = 0.7850\n(quán tính cũ hết tác dụng)",
            "Severe PR-AUC = 0.7082 (+7.13%)\nROC-AUC = 0.8427 (+5.77%)\n(nhờ chu kỳ giờ VN + PageRank)",
            "Khi GPS ngắt quãng lâu (chiếm\n66.27% thực tế), RF vượt xa\ncách đoán quán tính.",
        ],
        [
            "4. Vùng Cold-Start\nkhông có xe gửi GPS\n(74,606 / 84,633 đoạn)",
            "Network Coverage = 0.0%\n(bất lực vì không có LOS(t)\nhiện tại trên đường)",
            "Network Coverage = 100.0%\n(chấm điểm đủ 84,633 đoạn\nnhờ Static Feature Store)",
            "Chỉ duy nhất RF 23D mới phủ\nđược 88.15% mạng lưới đường\nnhánh thiếu cảm biến.",
        ],
    ]
    s13.add_table(0.55, 2.00, 8.35, 6.38, t_headers, t_rows, [0.22, 0.23, 0.29, 0.26], font_size=7.9)
    s13.add_image(9.06, 2.00, 6.39, 4.26, trans_chart)
    s13.add_card(
        9.06,
        6.38,
        6.39,
        2.00,
        "Đúc Kết Đối Chứng Khoa Học (1-on-1 Defense)",
        [
            "94.84% Accuracy của Baseline chỉ phản ánh quán tính đứng yên (1,121 / 1,182 cặp không đổi nhãn), hoàn "
            "toàn mù (0 / 61) ngay khi giao thông chuyển trạng thái.",
            "Random Forest (23D) phát hiện sớm tín hiệu đổi pha (Detect AUC = 0.7664, đẩy xác suất rủi ro gấp 9.35x) "
            "và mở rộng độ phủ dự báo từ 0% lên 100% mạng lưới đô thị.",
        ],
        badge="Key Finding",
        theme="sky_strong",
        body_size=8.1,
    )
    return s13


def _add_slide_14_demonstration(prs: Presentation, ui_tab2: Path, ui_tab4: Path) -> DualSlide:
    """Slide 14 (Section 10): Demonstration (:8501 Live Case Studies & 5-Tab Operational Overview)."""
    s14 = DualSlide(prs)
    s14.add_chrome(
        14,
        TOTAL_SLIDES,
        5,
        "SECTION 10 · DEMONSTRATION — CỔNG ĐIỀU HÀNH STREAMLIT 5 TAB (:8501) & 2 CASE STUDIES",
        "10. Demonstration: Cận Cảnh Dự Báo E → F (Tab 2) & Tra Cứu Cold-Start (Tab 4)",
        "Trình diễn trực tiếp trên ứng dụng Streamlit kết nối kho MongoDB 8.0 (33,441 bản ghi, làm mới 5 giây)",
    )
    s14.add_card(
        0.55,
        2.00,
        7.35,
        1.58,
        "Case Study 1 (Tab 2): Bắt Đúng E → F",
        [
            "Đoạn 21064 — Công Trường Quốc Tế (11:30 trưa, đang LOS E 12 km/h): Baseline giữ nguyên mức E (sai), "
            "còn Random Forest 23D dự báo đúng chuyển sang LOS F (xác suất kẹt 59.6%, đổi pha 73.0%).",
        ],
        badge="Tab 2 · Forecast",
        theme="sky",
        body_size=8.3,
    )
    s14.add_image(0.55, 3.68, 7.35, 3.14, ui_tab2)

    s14.add_card(
        8.10,
        2.00,
        7.35,
        1.58,
        "Case Study 2 (Tab 4): Vùng Cold-Start",
        [
            "Khi chọn đoạn 61781 — Phú Định (0 bản tin GPS, PageRank #1 = 3.7515), hệ thống đối chiếu tức thì các "
            "đoạn có cảm biến tương đồng nhất trong cùng Cluster (Châu Văn Liêm, Nguyễn Kiệm).",
        ],
        badge="Tab 4 · Cold-Start",
        theme="white",
        body_size=8.3,
    )
    s14.add_image(8.10, 3.68, 7.35, 3.14, ui_tab4)

    s14.add_card(
        0.55,
        6.94,
        14.90,
        1.44,
        "Toàn Cảnh 5 Tab Nghiệp Vụ Trên Cổng Điều Hành (:8501 ↔ MongoDB 8.0)",
        [
            "Tab 1: Bản đồ tổng quan & Top-25 EWMA Hotspots (UC-1) · Tab 2: Dự báo trực tiếp +30m dưới 1 ms (UC-2) · "
            "Tab 3: Xếp hạng nút cổ chai PageRank & luật đồng kẹt SON (UC-3, UC-4).",
            "Tab 4: Phân cụm 7D K-Means & đối chiếu vùng mù Cold-Start (UC-5) · Tab 5: Kiểm toán đối soát Tier-2 "
            "Backfill & chữ ký SHA-256 Frozen Registry (UC-6).",
        ],
        badge="5 Live Tabs",
        theme="sky_strong",
        body_size=8.2,
    )
    return s14


def _add_slide_15_limitations_and_risks_summary(prs: Presentation) -> DualSlide:
    """Slide 15 (Section 11): Limitations & Risks — Solution Component Roll-Up & System-Wide Risk Matrix."""
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
            "4 dịch vụ chạy chung 1 máy Docker (10.5/16 GB RAM);\nđộ trễ đồng bộ biến trễ giữa Phase B và Phase C",
            "Giới hạn mem_limit từng container;\nkhóa phân quyền đọc/ghi 3-Phase MLOps",
        ],
        [
            "5. Data Pipeline &\nData Processing",
            "60.79% cửa sổ dùng Vmax suy diễn tĩnh (Tier 2/3);\nnguy cơ tăng cách ly Quarantine khi đổi schema",
            "Train-Only 3-Tier Vmax (0 Data Leakage);\nANSI try_cast + Quarantine (0 silent drops)",
        ],
        [
            "6. Modeling",
            "Ngưỡng mặc định bảo thủ khi đổi pha (2/61);\nthiếu biến ngoại sinh trực tiếp (mưa ngập, tai nạn)",
            "Hiệu chỉnh ngưỡng τ=0.30 trên VAL (9/61);\ncảnh báo sớm theo xác suất (Lift 9.35x)",
        ],
        [
            "7. Batch Processing",
            "Luật trễ +30m SON/FPGrowth chỉ có 5–8 cửa sổ;\nphân cụm Cold-Start dựa trên hình thái tĩnh",
            "Gắn nhãn Exploratory Hypothesis;\nkết hợp Line-Graph PageRank bảo toàn khối lượng",
        ],
        [
            "8. Stream Processing",
            "Bloom 32.0 KiB đầy bit nếu chạy dài ngày;\ntin trễ >30m phải chờ đối soát cuối ngày",
            "Tier-2 Reconciliation bù 100% (501 cửa sổ trễ);\nMongoDB version_ts chặn 25/25 tin cũ v1",
        ],
    ]
    s15.add_table(0.55, 2.00, 8.55, 6.38, comp_headers, comp_rows, [0.22, 0.43, 0.35], font_size=8.0)

    s15.add_card(
        9.26,
        2.00,
        6.19,
        6.38,
        "4 Rủi Ro & Hạn Chế Chung Toàn Hệ Thống",
        [
            "1. Spatial & Fleet Representation Bias: Nguồn GPS cộng đồng chỉ phủ 11.85% đoạn đường (chủ yếu ô tô trục "
            "chính), chưa đại diện đầy đủ cho luồng xe máy (> 80% lưu lượng TP.HCM).",
            "2. Open-World Concept Drift & Seasonality: Mô hình khóa ở Phase A và tập TEST mùa khô (12/2020–03/2021) "
            "sẽ suy giảm khi gặp mùa mưa ngập lịch sử hoặc lô cốt thi công mới.",
            "3. Rare Sudden-Onset Scarcity: Ca chuyển từ thoáng sang kẹt cứng chỉ chiếm 0.81% (8 / 993 ca TEST, Hard "
            "Recall = 0/8), làm hạn chế biên quyết định cứng nếu thiếu hàm phạt chi phí.",
            "4. Proof-of-Scale & Human-in-the-Loop Ceiling: Cụm Docker đơn (mẫu Proof-of-Scale 38.5 MB) và UI :8501 "
            "hiện cung cấp khuyến nghị điều hành, chưa nối tự động vòng kín vào đèn SCATS.",
        ],
        badge="System-Wide Risks",
        theme="navy",
        body_size=8.3,
    )
    return s15


def _add_slide_16_plan_and_roadmap(prs: Presentation) -> DualSlide:
    """Slide 16 (Section 12): Plan — Completed Project Execution Plan & 3-Phase Future Roadmap."""
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
            "Hoàn thành 100%: Ordered Quality Funnel (ANSI try_cast, NFC, UTC+7), Train-Only Vmax Imputation, "
            "5-Tuple Monoid (sai số = 0.0), 2-Stage Key Salting (687.43x → 1.023x), BroadcastHashJoin & "
            "Medallion Parquet Pruning (7,294 dòng tháng 2020-08).",
        ],
        [
            "2. Nguyễn Thành Đạt\n(MSSV: 25MS23306)",
            "Mục 2, 6, 7B & 9:\nLiterature, Modeling,\nGraph & Experiments",
            "Hoàn thành 100%: Literature Review, Line-Graph PageRank (84,633 đoạn), FPGrowth (335 luật) & SON, "
            "7D K-Means (k=6, Silhouette 0.2853) phủ 74,606 đoạn Cold-Start, Random Forest 23D trên 7,898 cặp "
            "exact t+30m (ROC-AUC 0.9882, PR-AUC 0.9471) & Transition Defense.",
        ],
        [
            "3. Phạm Công Minh\n(MSSV: 25MS23310)",
            "Mục 3, 8, 10, 11 & 12:\nArch, Stream, Demo,\nLimitations & Plan",
            "Hoàn thành 100%: Docker 10.5/16 GB RAM, Kafka 4.1 KRaft, O(1) Sketches 33.0 KiB (32.0 KiB Bloom + "
            "1.0 KiB HLL++ + EWMA), WAL Recovery (0 dupes), Two-Tier Recon (bù 501 cửa sổ trễ, 3,953 lags, "
            "re-score 377 cửa sổ), MongoDB IXSCAN (0.636 ms) & UI :8501.",
        ],
    ]
    s16.add_table(0.55, 2.00, 14.90, 3.82, t_headers, t_rows, [0.19, 0.22, 0.59], font_size=8.2)

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
            "Quality Gate Nghiệm thu: Vượt qua 100% kiểm thử tự động (22/22 pytest & Spark unittest; 0 lỗi Ruff).",
        ],
        badge="Phase 3 & QA",
        theme="navy",
        body_size=8.3,
    )
    return s16


def _safe_copy(src: Path, dst: Path) -> None:
    if dst.parent.exists():
        try:
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
        _safe_copy(png_path, ARTIFACT_DIR / f"slide_{idx:02d}.png")

    primary_pptx = REPO_DIR / "HCMC_Smart_Traffic_Final_Presentation.pptx"
    prs.save(str(primary_pptx))
    _safe_copy(primary_pptx, WORKSPACE_DIR / "HCMC_Smart_Traffic_Final_Presentation.pptx")
    legacy_pptx = REPO_DIR / "HCMC_Smart_Traffic_Proposal_Presentation.pptx"
    if legacy_pptx.is_file():
        _safe_copy(primary_pptx, legacy_pptx)

    primary_pdf = REPO_DIR / "HCMC_Smart_Traffic_Final_Slides.pdf"
    pdf_targets = [
        primary_pdf,
        WORKSPACE_DIR / "HCMC_Smart_Traffic_Final_Slides.pdf",
    ]
    grid_targets = [
        REPO_DIR / "outputs" / "slides_overview_grid.png",
        ARTIFACT_DIR / "slides_overview_grid.png",
    ]

    export_overview_grid_and_pdf(slide_pngs, grid_targets, pdf_targets, slide_figs=slide_figs)
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
    arch_img = REPO_DIR / "outputs" / "system_architecture.png"
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
        "16-Slide Deck: 1.Problem 2.Literature 3.Arch 4.EDA + 2 Visual Method Explainers + "
        "5.Pipeline 6.Modeling 7.Batch 8.Stream 9.Experiments 10.Demo 11.Limitations/Risks 12.Plan"
    )

    all_slides = [
        _add_slide_01_cover(prs),
        _add_slide_02_problem_statement(prs, map_prob),
        _add_slide_03_literature_review(prs),
        _add_slide_04_system_architecture(prs, arch_img),
        _add_slide_05_data_eda_and_problems(prs, eda_chart),
        _add_slide_06_method_explainer_imputation_monoid(prs, method_img1),
        _add_slide_07_method_explainer_graph_sketches(prs, method_img2),
        _add_slide_08_data_pipeline_and_processing(prs, flow_img),
        _add_slide_09_modeling(prs),
        _add_slide_10_batch_processing(prs, batch_flow_img),
        _add_slide_11_stream_processing(prs, stream_flow_img),
        _add_slide_12_experiments_ablation(prs),
        _add_slide_13_experiments_persistence_defense(prs, trans_chart),
        _add_slide_14_demonstration(prs, ui2, ui4),
        _add_slide_15_limitations_and_risks_summary(prs),
        _add_slide_16_plan_and_roadmap(prs),
    ]

    pptx_path, pdf_path = _save_and_sync_slide_outputs(prs, all_slides)
    print(
        f"Generated {len(all_slides)} visual slides -> {pptx_path.name} ({pptx_path.stat().st_size:,} B) "
        f"& {pdf_path.name} ({pdf_path.stat().st_size:,} B)"
    )


if __name__ == "__main__":
    main()
