"""Build the 13-Slide Pre-Experiment Copy Presentation (Sections 1–8 + Limitations & Next-Action Plan)."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from pptx import Presentation
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_slides import _add_dual_rect, _render_pdf_to_pngs, _resolve_arch_img, render_eda_chart  # noqa: E402
from render_pre_experiment_visuals import (  # noqa: E402
    render_pre_exp_batch_flow,
    render_pre_exp_graph_sketches,
    render_pre_exp_imputation_monoid,
    render_pre_exp_pipeline_walkthrough,
    render_pre_exp_stream_flow,
)
from render_visual_assets import _load_segment_geometries, render_hcmc_problem_context_map  # noqa: E402
from slide_renderer import (  # noqa: E402
    HEX_BORDER,
    HEX_MUTED,
    HEX_NAVY,
    HEX_PAGE_SOFT,
    HEX_SKY_DEEP,
    HEX_SKY_PRIMARY,
    HEX_SLATE,
    HEX_SLATE_DARK,
    DualSlide,
    _pptx_textbox,
    export_overview_grid_and_pdf,
)

REPO_DIR = SCRIPT_DIR.parent
OUTPUTS_DIR = REPO_DIR / "outputs"
CHART_DIR = OUTPUTS_DIR / "slide_charts"
PRE_EXP_SLIDE_DIR = OUTPUTS_DIR / "slides_pre_experiment"
SLIDE_RENDER_DIR = OUTPUTS_DIR / "slide_renders"
TOTAL_PRE_SLIDES = 13

MILESTONE_NAV_TABS = [
    "01–02. Problem & Lit",
    "03–04. Arch & EDA",
    "05–06. Pipe & Model",
    "07–08. Batch & Stream",
    "09–10. Risk & Next Plan",
]
MILESTONE_FOOTER = "BDA501 · HCMC Smart Traffic Big Data Platform · Design & Methodology Milestone (Pre-Experiment)"


def _new_ms_slide(prs: Presentation) -> DualSlide:
    return DualSlide(prs, nav_tabs=MILESTONE_NAV_TABS, footer_left=MILESTONE_FOOTER)


def _add_ms_slide_01_cover(prs: Presentation) -> DualSlide:
    """Slide 01: High-Hierarchy Cover tailored for the Design & Methodology Milestone."""
    s1 = _new_ms_slide(prs)

    _add_dual_rect(s1, 0.00, 0.00, 11.20, 0.08, HEX_NAVY, zorder=2)
    _add_dual_rect(s1, 11.20, 0.00, 4.80, 0.08, HEX_SKY_PRIMARY, zorder=2)

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

    badge_label = "DESIGN & METHODOLOGY REPORT"
    s1.add_pill_badge(1.05, 2.00, 2.86, 0.36, badge_label, "sky_pale", 8.8)

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


def _add_ms_slide_02_problem_statement(prs: Presentation, problem_map: Path) -> DualSlide:
    """Slide 02 (Section 1): Problem Statement — Domain WHY, Current Gaps, WHAT & Value."""
    s2 = _new_ms_slide(prs)
    s2.add_chrome(
        2,
        TOTAL_PRE_SLIDES,
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
            "Mật độ xe cao trên mạng lưới đan xen (sân bay, Q.1–Q.10, Thủ Đức) khiến nghẽn giờ cao điểm nhanh "
            "chóng lan truyền diện rộng.",
            "Điều tiết hiện chủ yếu phản ứng bị động khi đã kẹt cứng, gây lãng phí lớn về thời gian và chi phí "
            "vận tải đô thị.",
        ],
        badge="WHY · Pain Point",
        theme="white",
        body_size=10.0,
    )
    s2.add_card(
        0.55,
        4.16,
        7.35,
        2.08,
        "2. Vì Sao Phương Pháp Hiện Tại Chưa Đủ?",
        [
            "Vùng mù & đứt quãng: Camera cố định đắt đỏ, chỉ phủ nút chính; GPS hành trình lệch về đại lộ, bỏ "
            "trống đường nhánh và hay trễ tin.",
            "Thiếu dự báo sớm & chia cắt: Công cụ hiện nay chỉ báo nơi ĐANG kẹt (chưa dự báo +30p nơi SẮP kẹt) "
            "và tách rời trực ban với quy hoạch.",
        ],
        badge="Current Gaps",
        theme="sky",
        body_size=10.0,
    )
    s2.add_card(
        0.55,
        6.36,
        7.35,
        2.02,
        "3. Bài Toán Đặt Ra & Giá Trị Thụ Hưởng",
        [
            "Bài toán: Thiết kế nền tảng dữ liệu lớn để (1) suy diễn toàn đô thị kể cả vùng mù, (2) tìm nút cổ "
            "chai cấu trúc và (3) dự báo kẹt trước 30 phút.",
            "Giá trị: TOC & Kỹ sư đèn điều tiết chủ động; Sở GTVT ưu tiên đúng điểm nghẽn; Người dân chọn lộ "
            "trình né kẹt từ xa.",
        ],
        badge="WHAT & Value",
        theme="sky_strong",
        body_size=10.0,
    )
    s2.add_image(8.08, 2.00, 7.37, 6.38, problem_map)
    return s2


def _add_ms_slide_03_literature_review(prs: Presentation) -> DualSlide:
    """Slide 03 (Section 2): Literature Review — Existing ITS/Forecasting Approaches & Research Gap."""
    s3 = _new_ms_slide(prs)
    s3.add_chrome(
        3,
        TOTAL_PRE_SLIDES,
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
            "1. Quan trắc Cố định & Đèn Thích ứng",
            "Vòng từ, CCTV; SCATS / SCOOT (Sims, 1980; Hunt et al., 1981)",
            "Đo lưu lượng chính xác tại nút có thiết bị để chỉnh đèn cục bộ",
            "Chi phí lắp đặt & bảo trì cao; bỏ trống phần lớn mạng đường nhánh (vùng mù)",
        ],
        [
            "2. Dữ liệu Hành trình (Floating Car / GPS)",
            "GPS xe buýt, taxi, bản đồ số (Herrera et al., 2010; Zheng, 2015)",
            "Phủ diện rộng không cần cảm biến mặt đường, phản ánh vận tốc thực",
            "Lệch về đại lộ, tín hiệu ngắt quãng dài, thiếu chuẩn tốc độ & chỉ báo hiện trạng",
        ],
        [
            "3. Dự báo Ùn tắc Ngắn hạn (15–60p)",
            "ARIMA (Vlahogianni, 2014); DCRNN, Graph WaveNet (Li et al., 2018)",
            "Dự báo sớm mức kẹt nhờ tương quan không gian – thời gian trên mạng lưới",
            "Thử nghiệm trên cao tốc cảm biến liên tục (PeMS, METR-LA); suy giảm khi GPS thưa",
        ],
        [
            "4. Hỗ trợ Quyết định Điều hành (Urban DSS)",
            "Trung tâm TOC & Big Data đô thị (Lv et al., 2015; Amini et al., 2017)",
            "Kết hợp kho lịch sử quy hoạch với bảng giám sát cho điều hành viên",
            "Mô hình dự báo tách rời luồng vận hành (GPS trễ/trùng); chia cắt trực ban & quy hoạch",
        ],
    ]
    s3.add_table(0.55, 2.00, 14.90, 4.18, lit_headers, lit_rows, [0.19, 0.25, 0.25, 0.31], font_size=9.0)

    s3.add_card(
        0.55,
        6.32,
        14.90,
        2.06,
        "Khoảng Trống Nghiên Cứu & Định Hướng Thiết Kế Nền Tảng Tích Hợp",
        [
            "Khoảng trống thực tiễn: Mô hình học thuật giả định cảm biến cao tốc liên tục, còn đô thị thực tế "
            "đối mặt vùng mù đường nhánh rộng lớn và thiếu cảnh báo sớm +30 phút.",
            "Vì sao cần nền tảng tích hợp? Một mô hình dự báo đơn lẻ không tự làm sạch GPS nhiễu/trễ, không suy "
            "diễn vùng mù thiếu cấu trúc đồ thị, và không nối trực ban TOC với quy hoạch Sở GTVT.",
            "Quy trình nghiên cứu (Mục 1–10): Vấn đề (Mục 1) → Khảo sát (Mục 2) → Thiết kế kiến trúc, mô hình & "
            "luồng xử lý (Mục 3–8) → Nhận diện rủi ro & kế hoạch thực nghiệm (Mục 9–10).",
        ],
        badge="Research Gap & Logic",
        theme="sky",
        body_size=9.6,
    )
    return s3


def _add_ms_slide_04_system_architecture(prs: Presentation, arch_img: Path) -> DualSlide:
    """Slide 04 (Section 3): System Architecture Design (no post-run empirical numbers)."""
    s4 = _new_ms_slide(prs)
    s4.add_chrome(
        4,
        TOTAL_PRE_SLIDES,
        1,
        "SECTION 03 · SYSTEM ARCHITECTURE — KIẾN TRÚC TỔNG THỂ (ZONES A–F · 4 PIPELINES) & 3-PHASE MLOPS",
        "3. System Architecture: Thiết Kế Kiến Trúc Hybrid Batch–Speed & 3-Phase MLOps",
        "Thiết kế nền tảng 5 dịch vụ Docker (Zones A–F) kết nối Predictive ML +30p (Zero-Leakage) & TOC (:8000)",
    )
    s4.add_image(0.55, 2.00, 10.35, 6.38, arch_img)

    s4.add_card(
        11.06,
        2.00,
        4.39,
        3.24,
        "3-Phase MLOps Lifecycle",
        [
            "4 Pipelines (Zones A–F): Batch ETL, Graph/K-Means, Predictive ML +30p & Speed Streaming.",
            "Phase A (Batch Fit): Huấn luyện Predictive ML & đặc trưng tĩnh 1 lần trên TRAIN, khóa SHA-256 Registry.",
            "Phase B (Speed): Chỉ đọc .predict_proba() phát cảnh báo v1 (Zero-Leakage, không xung đột ghi).",
            "Phase C (Recon): Cuối ngày gom tin trễ > 30p chuẩn hóa v2 trong ngân sách 11.0 / 16.0 GB.",
        ],
        badge="0 Write Conflict",
        theme="sky",
        body_size=10.1,
    )
    s4.add_card(
        11.06,
        5.36,
        4.39,
        3.02,
        "Limitations & Risks (Arch)",
        [
            "Single-Host Contention: 5 dịch vụ mặc định (Kafka 1.5G, Spark 6G, Mongo 2G, TOC :8000 1G, "
            "live-consumer 0.5G) trên 1 máy tiềm ẩn tranh chấp I/O khi Shuffle lớn.",
            "Eventual Consistency: Cảnh báo luồng v1 ưu tiên độ trễ thấp, còn biến trễ từ tin muộn > 30p chờ "
            "chu kỳ Phase C cuối ngày.",
            "Single-Node HA Scope: Chưa triển khai replication đa broker cho vận hành toàn đô thị.",
        ],
        badge="Arch Risk",
        theme="navy",
        body_size=10.1,
    )
    return s4


def _add_ms_slide_05_data_eda_and_problems(prs: Presentation, eda_chart: Path) -> DualSlide:
    """Slide 05 (Section 4): Raw 5-Table EDA & 10 Data Problems (pure EDA + designed remedies, no runtime outputs)."""
    s5 = _new_ms_slide(prs)
    s5.add_chrome(
        5,
        TOTAL_PRE_SLIDES,
        1,
        "SECTION 04 · DATA, EDA & DATA PROBLEM — KAGGLE V6 (CC0), 5 BẢNG GỐC & 10 THÁCH THỨC (EC-1..EC-10)",
        "4. Data, EDA & Data Problem: Hồ Sơ 5 Bảng Gốc & 10 Vấn Đề Dữ Liệu Thực Nghiệm",
        "Khám phá Kaggle v6 (CC0 · BkTraffic HCMUT · 38.50 MB · 46/48 active periods · UTC+7) & giải pháp EC-1..10",
    )
    s5.add_image(0.55, 2.00, 6.25, 4.02, eda_chart)

    s5.add_card(
        0.55,
        6.14,
        6.25,
        2.24,
        "Kaggle v6 (CC0) & Proof-of-Scale",
        [
            "Nguồn Kaggle v6 (CC0: Public Domain · BkTraffic HCMUT): 5 CSVs · 41 cols · 38.50 MB, 46/48 active "
            "periods (2 khung đêm trống), UTC+7 ICT; 84,633 → 84,206 đoạn cơ giới (11.87% có GPS), 33,441 → 33,394 "
            "cửa sổ cơ giới.",
            "Proof-of-Scale: Mẫu đối chứng toàn TP.HCM (ước tính 8–17 tỷ bản ghi/ngày, 1.5–3.0 TB/ngày); EDA ghi "
            "nhận Burst tổng hợp 03/08/2020 (36,096 pings = 39.7% cả 10 tháng, 7,192 cửa sổ) sẽ tách khỏi TRAIN.",
        ],
        badge="Proof-of-Scale",
        theme="sky",
        body_size=9.2,
    )

    ec_headers = ["Data Problem (EC)", "Hiện tượng từ EDA & Phân tán", "Thiết kế Kỹ thuật Đề xuất"]
    ec_rows = [
        [
            "EC-1 · Missing Vmax",
            "85.21% raw (28,495/33,441) · 85.19% cơ giới (28,448/33,394)",
            "Train-Only 3-Tier Imputation (P85 / Median / Prior)",
        ],
        [
            "EC-2 · Bad Geometry",
            "154 đoạn ≤ 0m, 427 POI (địa điểm/công trình) & 151 ping lỗi",
            "ANSI try_cast Quarantine, trung vị 27.0m & loại POI",
        ],
        ["EC-3 · Unicode Drift", "631 dòng Quận & 2,247 dòng lệch dấu", "Chuẩn hóa Unicode NFC đồng nhất tên đường"],
        ["EC-4 · UTC Rollover", "4,305 cửa sổ (12.89%) lệch ngày UTC+7", "Quy đổi vn_hour (+7h), ict_date & sin/cos"],
        [
            "EC-5 · Gaps & Burst",
            "66.30% (15,514) đứt ≥ 60m & Burst 03/08/2020 (36,096 pings · 39.7%)",
            "Exact +30m (7,885 cặp) & tách burst khỏi TRAIN",
        ],
        [
            "EC-6 · Cold-Start",
            "88.13% (74,211 đoạn) có 0 bản tin GPS",
            "Static Feature Store (PageRank + 7D K-Means)",
        ],
        [
            "EC-7 · Shape Points",
            "90.97% (525,786 nodes) vẽ cong & 314 cụt",
            "Cắt tỉa điểm uốn, loại U-turn & bù nút cụt",
        ],
        ["EC-8 · Data Skew", "Tập trung tin giờ cao điểm & lệch avg()", "5-Tuple Monoid + 2-Stage Salting (8 giỏ)"],
        ["EC-9 · Late & Dupes", "Nguy cơ gửi lặp gói tin & trễ mạng > 30m", "32.0 KiB Bloom Filter + Tier-2 Backfill"],
        [
            "EC-10 · Crash & Stale",
            "Rủi ro sập luồng & tin cũ v1 đến muộn",
            "WAL Checkpoint + khóa chống ghi đè $status_version",
        ],
    ]
    s5.add_table(6.96, 2.00, 8.49, 6.38, ec_headers, ec_rows, [0.22, 0.39, 0.39], font_size=8.6)
    return s5


def _add_ms_slide_06_method_explainer_1(prs: Presentation, method_img1: Path) -> DualSlide:
    """Slide 06: Pre-Experiment Method Explainer (1/2) — 3-Tier Vmax & 5-Tuple Monoid + Key Salting."""
    sm1 = _new_ms_slide(prs)
    sm1.add_chrome(
        6,
        TOTAL_PRE_SLIDES,
        2,
        "METHODOLOGY VISUALIZER (1/2) — TRỰC QUAN HÓA NỘI SUY 3 TẦNG VMAX, MONOID COMBINER & KEY SALTING",
        "Giải Thích Phương Pháp (1/2): Nội Suy 3 Tầng Vmax & Bộ Gộp 5-Tuple Monoid",
        "Trực quan hóa cơ chế điền khuyết tốc độ không rò rỉ dữ liệu và phép cộng gộp phân tán bảo toàn toán học",
    )
    sm1.add_image(0.55, 2.00, 14.90, 4.46, method_img1)

    sm1.add_card(
        0.55,
        6.58,
        7.35,
        1.80,
        "Nguyên Lý Nội Suy 3 Tầng (3-Tier Vmax)",
        [
            "Chỉ học bảng tra trên tập TRAIN theo thứ tự ưu tiên từ riêng đến chung (lịch sử chính đoạn đường → "
            "trung vị đường cùng cấp → quy chuẩn đô thị 80/60/50/40 km/h), triệt tiêu rò rỉ tương lai.",
        ],
        badge="Zero Leakage",
        theme="sky",
        body_size=9.8,
    )
    sm1.add_card(
        8.10,
        6.58,
        7.35,
        1.80,
        "Nguyên Lý 5-Tuple Monoid & Key Salting",
        [
            "Thay vì lấy trung bình của trung bình gây lệch khi số xe giữa các máy không đều, giữ 5 đại lượng cộng "
            "được (cnt, sum, min, max, sev) giúp chia 8 giỏ song song mà kết quả cuối vẫn chính xác tuyệt đối.",
        ],
        badge="Exact Algebra",
        theme="white",
        body_size=9.8,
    )
    return sm1


def _add_ms_slide_07_method_explainer_2(prs: Presentation, method_img2: Path) -> DualSlide:
    """Slide 07: Pre-Experiment Method Explainer (2/2) — Line-Graph PageRank, 7D K-Means & O(1) Sketches."""
    sm2 = _new_ms_slide(prs)
    sm2.add_chrome(
        7,
        TOTAL_PRE_SLIDES,
        2,
        "METHODOLOGY VISUALIZER (2/2) — TRỰC QUAN HÓA LINE-GRAPH PAGERANK, 7D K-MEANS & O(1) SKETCHES",
        "Giải Thích Phương Pháp (2/2): Line-Graph PageRank, K-Means & Bộ Đếm O(1)",
        "Cơ chế phát hiện nút cổ chai bản đồ, suy diễn cho 88.13% đường thiếu GPS và lọc trùng trong 33.0 KiB RAM",
    )
    sm2.add_image(0.55, 2.00, 14.90, 4.46, method_img2)

    sm2.add_card(
        0.55,
        6.58,
        7.35,
        1.80,
        "Từ Đồ Thị Bản Đồ Đến Suy Diễn Vùng Mù",
        [
            "Line-Graph PageRank biến mỗi đoạn đường thành 1 nút để chỉ ra nút phễu hình học; sau đó 7D K-Means "
            "ghép đoạn mù (0 GPS) vào nhóm cảm biến tương đồng để phủ kín 100% đô thị.",
        ],
        badge="Graph + Cluster",
        theme="sky",
        body_size=9.8,
    )
    sm2.add_card(
        8.10,
        6.58,
        7.35,
        1.80,
        "Vì Sao Thiết Kế Bộ Đếm O(1) Sketches?",
        [
            "Ở quy mô tỷ bản tin/ngày, lưu danh sách mã tin để lọc trùng sẽ tràn RAM. RotatingBloomFilter "
            "(32.0 KiB · 2×16 KiB), HLL++ (1.0 KiB) và EWMA xử lý tức thời trong ngân sách cố định 33.0 KiB.",
        ],
        badge="33.0 KiB Budget",
        theme="sky_strong",
        body_size=9.8,
    )
    return sm2


def _add_ms_slide_08_data_pipeline(prs: Presentation, flow_img: Path) -> DualSlide:
    """Slide 08 (Section 5): Data Pipeline & Processing Design (no post-run empirical counts)."""
    s8 = _new_ms_slide(prs)
    s8.add_chrome(
        8,
        TOTAL_PRE_SLIDES,
        2,
        "SECTION 05 · DATA PIPELINE & DATA PROCESSING — 6 QUY TẮC R1–R6, 3-TIER VMAX & DERIVE_URBAN_LOS",
        "5. Data Pipeline & Processing: Thiết Kế 6 Quy Tắc R1–R6 & Nội Suy Train-Only",
        "Thiết kế Quality Funnel 6 quy tắc R1–R6 bảo toàn 100% quân số, "
        "Unicode NFC / UTC+7 & chuẩn hóa derive_urban_los",
    )
    s8.add_image(0.55, 2.00, 14.90, 4.02, flow_img)

    s8.add_card(
        0.55,
        6.14,
        7.35,
        2.24,
        "Thiết Kế 6 Quy Tắc R1–R6 & derive_urban_los",
        [
            "6 quy tắc R1–R6 (Quality Funnel): ANSI try_cast tách lỗi sang Quarantine (0 silent drops); loại POI phi "
            "cơ giới; chuẩn hóa dấu Unicode NFC & đồng bộ múi giờ UTC+7.",
            "Train-Only 3-Tier Vmax (khóa vmax_lookup.json trên TRAIN): Tốc độ gốc → Tier 1 P85 đoạn → Tier 2 "
            "Median cấp×loại → Tier 3 quy chuẩn đô thị 80/60/50/40 km/h.",
            "Thiết kế derive_urban_los: quy đổi tỷ lệ vận tốc thực / Vmax thành 6 mức LOS A–F đối chiếu BkTraffic "
            "(A–B ≥65% · C–D 35–65% · E–F <35% kẹt cứng).",
        ],
        badge="6 Rules · 0 Drops",
        theme="sky",
        body_size=9.4,
    )
    s8.add_card(
        8.10,
        6.14,
        7.35,
        2.24,
        "Limitations & Risks (Data Pipeline)",
        [
            "Static Vmax Approximation: Các tuyến khuyết biển báo dùng Vmax suy diễn tĩnh theo cấp đường (Tier 2 & "
            "3), chưa tự động phản ánh rào chắn thi công hay ngập nước cục bộ.",
            "Schema Drift Quarantine Risk: ANSI try_cast chống sập job, nhưng thiết bị GPS đổi định dạng hàng loạt "
            "sẽ làm tăng tỷ lệ bản ghi cách ly vào Quarantine.",
            "30m Slot Discretization: Khung gom 30 phút làm mượt các dao động vận tốc ngắn dưới 5 phút tại ngã tư.",
        ],
        badge="Pipeline Risk",
        theme="navy",
        body_size=9.4,
    )
    return s8


def _add_ms_slide_09_modeling(prs: Presentation) -> DualSlide:
    """Slide 09 (Section 6): Model & Feature Research Design (balanced 2-column + 23D feature table, zero leaks)."""
    s9 = _new_ms_slide(prs)
    s9.add_chrome(
        9,
        TOTAL_PRE_SLIDES,
        2,
        "SECTION 06 · MODELING — THIẾT KẾ PREDICTIVE ML +30P (ZERO-LEAKAGE), 23D FEATURES & VAL GATE",
        "6. Modeling: Thiết Kế Dự Báo +30p (Zero-Leakage), Vector 23 Biến & VAL Gate",
        "Thiết kế ghép cặp exact t+30m chống rò rỉ, cấu trúc 23 biến Predictive ML và cơ chế phủ vùng Cold-Start",
    )
    s9.add_card(
        0.55,
        2.00,
        7.55,
        2.96,
        "Thiết Kế Nhãn Exact t+30m & Zero-Leakage",
        [
            "Exact (segment_id, slot_ts + 30m) Join: Chỉ ghép nhãn đúng khung kế tiếp t + 30p (7,885 cặp hợp lệ từ "
            "EDA; loại bỏ 15,514 cặp đứt quãng ≥ 60m).",
            "Chronological 70/15/15 + Burst Separation & Purge: Chia TRAIN / VAL / TEST theo trục thời gian, tách "
            "7,192 burst windows khỏi TRAIN và cắt mẫu sát biên.",
            "Future-Shuffle Falsification: Xáo trộn tương lai τ > t để kiểm chứng ma trận đầu vào X_t bất biến.",
        ],
        badge="Zero Leakage",
        theme="white",
        body_size=10.2,
    )
    feat_headers = ["Nhóm 23D Predictive ML", "Chiều", "Đại diện & Vai trò Dự báo (τ < t)"]
    feat_rows = [
        [
            "1. Local State & Lags",
            "7D",
            "current_los_ordinal, mean_velocity, velocity_ratio, obs_count, lag1/lag2_los, trailing_severe_rate",
        ],
        [
            "2. ICT Time & Geometry",
            "7D",
            "period_sin, period_cos, weekday, gap_minutes, length_m, street_level, max_velocity (chu kỳ giờ VN)",
        ],
        [
            "3. Graph, Cluster & Past Priors",
            "9D",
            "pagerank_score, in/out_degree, cluster_id, neighbor_los, segment/street Past-Only Priors (τ < t)",
        ],
    ]
    s9.add_table(0.55, 5.10, 7.55, 3.28, feat_headers, feat_rows, [0.27, 0.10, 0.63], font_size=9.2)

    s9.add_card(
        8.28,
        2.00,
        7.17,
        3.26,
        "Kiến Trúc Mô Hình & Tuyển Chọn VAL Gate",
        [
            "Mô hình Đề xuất — Random Forest (23D): Thiết kế 250 cây, max_depth=14, tuyển chọn bằng VAL Composite "
            "Gate — chọn trên VAL, chống nhìn trộm TEST: (val_acc + val_pr + val_roc) / 3.",
            "Hệ đối chứng 7 cấu hình Ablation: So sánh 14D vs 23D cạnh Persistence Baseline (quy tắc 0-ML: đoán giữ "
            "nguyên LOS(t)), Decision Tree & Binary GBT; Static Feature Store phủ đủ 74,211 đoạn Cold-Start.",
            "Hiệu chỉnh & Đóng băng: Tối ưu ngưỡng τ_sev, τ_chg độc lập trên tập VAL và khóa chữ ký SHA-256.",
        ],
        badge="Model Design",
        theme="sky_strong",
        body_size=10.1,
    )
    s9.add_card(
        8.28,
        5.38,
        7.17,
        3.00,
        "Limitations & Risks (Modeling)",
        [
            "Rủi ro bảo thủ khi chuyển pha: Quán tính giữ nguyên trạng thái giữa 2 khung 30p làm argmax mặc định "
            "thiên lệch về trạng thái tĩnh — cần hiệu chỉnh ngưỡng τ trên tập VAL.",
            "Thiếu biến ngoại sinh (mưa ngập, tai nạn) & mất cân bằng lớp: Vector 23D chưa có tín hiệu thời tiết/"
            "sự cố trực tiếp; lớp B–E thưa đòi hỏi đánh giá song song PR-AUC và Macro-F1.",
            "Cơ chế bù đắp vận hành: Hiển thị đồng thời xác suất liên tục P(E/F) & P(chg) thay vì một nhãn cứng.",
        ],
        badge="Modeling Risk",
        theme="navy",
        body_size=10.2,
    )
    return s9


def _add_ms_slide_10_batch_processing(prs: Presentation, batch_flow_img: Path) -> DualSlide:
    """Slide 10 (Section 7): Batch Processing Component Flow & Design (no runtime measurements)."""
    s10 = _new_ms_slide(prs)
    s10.add_chrome(
        10,
        TOTAL_PRE_SLIDES,
        3,
        "SECTION 07 · BATCH PROCESSING — THIẾT KẾ LUỒNG XỬ LÝ LÔ PHÂN TÁN, KHAI PHÁ ĐỒ THỊ & PHÂN CỤM",
        "7. Batch Processing: Thiết Kế Luồng Xử Lý Lô, Khai Phá Đồ Thị & Phân Cụm",
        "Trực quan hóa kiến trúc Batch ETL bảo toàn toán học, Line-Graph PageRank, SON/FPGrowth và 7D K-Means",
    )
    s10.add_image(0.55, 2.00, 14.90, 4.02, batch_flow_img)

    s10.add_card(
        0.55,
        6.14,
        7.35,
        2.24,
        "Thiết Kế Batch ETL, Đồ Thị & Phân Cụm",
        [
            "Distributed Spark ETL: BroadcastHashJoin ghim danh mục đường vào RAM; 2-Stage Key Salting (8 giỏ) kèm "
            "5-Tuple Monoid (cnt, sum, min, max, sev) chống lệch tải giờ cao điểm; lưu Gold Parquet theo year_month.",
            "Graph & Cold-Start Mining: Cắt tỉa 90.97% điểm uốn dựng Directed Line-Graph PageRank (β=0.85, loại "
            "U-turn, bù cụt); SON (2 lượt phân vùng) / FPGrowth tìm tập đường đồng kẹt & 7D K-Means phủ 74,211 "
            "Cold-Start.",
        ],
        badge="Batch Design",
        theme="sky",
        body_size=9.4,
    )
    s10.add_card(
        8.10,
        6.14,
        7.35,
        2.24,
        "Limitations & Risks (Batch Processing)",
        [
            "Nguy cơ cỡ mẫu thưa ở luật lan truyền đồng kẹt: Do GPS chỉ phủ 11.87% đoạn cơ giới và hay ngắt quãng, "
            "các luật lan truyền trễ +30p dễ có Support thấp → dùng làm giả thuyết thăm dò (Exploratory).",
            "Giới hạn phân cụm Cold-Start tĩnh: 74,211 đoạn thiếu GPS gán cụm theo hình thái đồ thị và Vmax cấp "
            "đường, chưa phản ánh được điểm họp chợ tự phát hay ngập vi mô trong hẻm.",
        ],
        badge="Batch Risk",
        theme="navy",
        body_size=9.4,
    )
    return s10


def _add_ms_slide_11_stream_processing(prs: Presentation, stream_flow_img: Path) -> DualSlide:
    """Slide 11 (Section 8): Stream Processing Component Flow & Design (no runtime measurements)."""
    s11 = _new_ms_slide(prs)
    s11.add_chrome(
        11,
        TOTAL_PRE_SLIDES,
        3,
        "SECTION 08 · STREAM PROCESSING — THIẾT KẾ LUỒNG THỜI GIAN THỰC, O(1) SKETCHES & ĐỐI SOÁT HAI TẦNG",
        "8. Stream Processing: Thiết Kế Luồng Cảnh Báo, O(1) Sketches & Đối Soát Trễ",
        "Trực quan hóa luồng sự kiện Kafka, bộ lọc 33.0 KiB RAM cố định, đối soát hai tầng và kho phục vụ MongoDB",
    )
    s11.add_image(0.55, 2.00, 14.90, 4.02, stream_flow_img)

    s11.add_card(
        0.55,
        6.14,
        7.35,
        2.24,
        "Thiết Kế Luồng O(1), Đối Soát & Serving",
        [
            "O(1) Sketches & WAL (nhật ký ghi trước): Phân vùng Kafka KRaft theo segment_id; bộ nhớ 33.0 KiB gồm "
            "RotatingBloomFilter (32.0 KiB · 2×16 KiB, k=10) & HLL++ (1.0 KiB); EWMA (trung bình trượt hàm mũ, "
            "α=0.15) bổ trợ RF (+30p).",
            "Two-Tier Recon & MongoDB: Tier-1 kiểm chuẩn đúng hạn & Tier-2 gom bù tin trễ > 30p, tính lại lag & "
            "re-score v2; Bulk Upsert khóa $status_version (v1 tạm → v2 chuẩn) & IXSCAN (quét chỉ mục, mục tiêu "
            "< 10 ms) cho TOC (:8000).",
        ],
        badge="33.0 KiB · SLA <10ms",
        theme="sky",
        body_size=9.1,
    )
    s11.add_card(
        8.10,
        6.14,
        7.35,
        2.24,
        "Limitations & Risks (Stream Processing)",
        [
            "Giới hạn cửa sổ xoay vòng RotatingBloomFilter (2×16 KiB): 2 thế hệ tránh bão hòa bit dài ngày, nhưng "
            "bản tin trùng quá muộn sau 2 thế hệ sẽ dựa vào chỉ mục event_id duy nhất trên MongoDB.",
            "Độ trễ sự kiện quá muộn (> 30m Watermark): Bản tin trễ hơn 30 phút do mất sóng di động phải đợi chu kỳ "
            "Tier-2 Reconciliation cuối ngày mới cập nhật vào bản chuẩn v2.",
        ],
        badge="Stream Risk",
        theme="navy",
        body_size=9.4,
    )
    return s11


def _add_ms_slide_12_limitations_and_risks(prs: Presentation) -> DualSlide:
    """Slide 12 (Section 9): Consolidated Design Limitations & System-Wide Risks (rebalanced, zero dark void)."""
    s12 = _new_ms_slide(prs)
    s12.add_chrome(
        12,
        TOTAL_PRE_SLIDES,
        4,
        "SECTION 09 · LIMITATIONS & RISKS — TỔNG HỢP GIỚI HẠN THIẾT KẾ SOLUTION COMPONENTS & RỦI RO TOÀN CỤC",
        "9. Limitations & Risks: Tổng Hợp Từng Thành Phần Thiết Kế & 4 Rủi Ro Chung",
        "Nhận diện sớm giới hạn kỹ thuật của 5 thành phần giải pháp (Mục 3, 5, 6, 7, 8) và cơ chế kiểm soát rủi ro",
    )
    comp_headers = [
        "Solution Component",
        "Giới hạn Thiết kế & Rủi ro Nhận diện Sớm",
        "Cơ chế Kiểm soát Trong Kiến trúc Đề xuất",
    ]
    comp_rows = [
        [
            "3. System Architecture",
            "5 dịch vụ mặc định chia sẻ tài nguyên trên 1 máy Docker (11.0/16 GB); "
            "độ trễ đồng bộ biến trễ giữa Phase B và Phase C",
            "Giới hạn cứng mem_limit từng container; phân tách đọc/ghi 3-Phase MLOps",
        ],
        [
            "5. Data Pipeline & Processing",
            "Các tuyến khuyết biển báo dùng Vmax suy diễn tĩnh (Tier 2/3); "
            "nguy cơ tăng cách ly Quarantine khi thiết bị đổi schema",
            "6 quy tắc R1–R6 + Train-Only 3-Tier Vmax chống rò rỉ; ANSI try_cast + Quarantine (0 silent drops)",
        ],
        [
            "6. Modeling",
            "Nguy cơ ngưỡng mặc định bảo thủ & lớp B–E thưa; thiếu biến ngoại sinh trực tiếp (mưa ngập, tai nạn)",
            "Lọc burst TRAIN, VAL Composite Gate & hiệu chỉnh τ; cảnh báo sớm theo xác suất rủi ro liên tục",
        ],
        [
            "7. Batch Processing",
            "Luật đồng kẹt trễ +30m dễ có Support thấp do GPS thưa; phân cụm Cold-Start chỉ dựa trên hình thái tĩnh",
            "Gắn nhãn Exploratory Hypothesis; kết hợp Line-Graph PageRank bảo toàn khối lượng mạng lưới",
        ],
        [
            "8. Stream Processing",
            "RotatingBloom 32.0 KiB giải phóng sau 2 thế hệ; tin trễ > 30m phải chờ phiên đối soát cuối ngày",
            "MongoDB event_id unique + Two-Tier Recon bù trễ; khóa $status_version chặn tin cũ v1 ghi đè v2",
        ],
    ]
    s12.add_table(0.55, 2.00, 8.85, 6.38, comp_headers, comp_rows, [0.20, 0.42, 0.38], font_size=8.8)

    s12.add_card(
        9.56,
        2.00,
        5.89,
        3.62,
        "4 Rủi Ro Chung Cấp Hệ Thống",
        [
            "1. Spatial & Fleet Bias: GPS cộng đồng chỉ phủ 11.87% đoạn cơ giới (thiên ô tô), chưa đại diện đầy đủ "
            "luồng xe máy (> 80% lưu lượng TP.HCM).",
            "2. Open-World Concept Drift: Mô hình đóng băng ở Phase A có nguy cơ suy giảm độ chính xác khi chuyển "
            "mùa mưa ngập lịch sử hoặc phát sinh rào chắn thi công mới.",
            "3. Rare Sudden-Onset & Imbalance: Pha đổi trạng thái đột ngột và lớp trung gian B–E chiếm tỷ trọng nhỏ, "
            "đòi hỏi lọc burst TRAIN, đánh giá PR-AUC & hiệu chỉnh ngưỡng τ.",
            "4. Proof-of-Scale & Scope: Kiểm chứng trên tập mẫu chuẩn 38.50 MB hỗ trợ khuyến nghị TOC, chưa đóng "
            "vòng tự động vào đèn tín hiệu SCATS.",
        ],
        badge="System-Wide Risks",
        theme="navy",
        body_size=10.2,
    )
    s12.add_card(
        9.56,
        5.74,
        5.89,
        2.64,
        "Nguyên Tắc Kiểm Soát Thực Nghiệm",
        [
            "Zero-Leakage Protocol: Tách chronological, tách burst khỏi TRAIN, khóa Future-Shuffle và tuyển chọn "
            "khách quan qua VAL Composite Gate trước khi chạm tập TEST.",
            "Human-in-the-Loop TOC: Hiển thị minh bạch xác suất P(E/F), P(chg) & nhãn v1 tạm thời vs v2 đối soát.",
            "Observability & Fail-Safe: Cách ly Quarantine 0 silent drops, nhật ký WAL & tự động kiểm thử pytest.",
        ],
        badge="Safe Design",
        theme="sky_strong",
        body_size=10.2,
    )
    return s12


def _add_ms_slide_13_plan_next_actions(prs: Presentation) -> DualSlide:
    """Slide 13 (Section 10): Research Progress (Sections 1–8), 3-Block Relay & Concrete Next Actions Plan."""
    s13 = _new_ms_slide(prs)
    s13.add_chrome(
        13,
        TOTAL_PRE_SLIDES,
        4,
        "SECTION 10 · PLAN & NEXT ACTIONS — TIẾN ĐỘ THIẾT KẾ (3-BLOCK RELAY) & KẾ HOẠCH THỰC NGHIỆM TIẾP THEO",
        "10. Plan & Next Actions: Kế Hoạch Thực Nghiệm Mô Hình, Đo Kiểm Runtime & Demo",
        "Phân công kỹ thuật + điều phối trình bày 3 chặng (Slide 01–13) và lộ trình chạy thực nghiệm giai đoạn kế tiếp",
    )
    t_headers = [
        "Thành viên Nhóm",
        "Kỹ thuật & Trình bày (3-Block)",
        "Next Actions (Thực nghiệm Mô hình, Đo kiểm Runtime & Demo)",
    ]
    t_rows = [
        [
            "1. Nguyễn Tạ Minh Quyền (25MS23280)",
            "Kỹ thuật: Mục 1, 4, 5 & 7A · Trình bày: Slide 01–05",
            "Chạy thực nghiệm Spark Batch trên dữ liệu thực: kiểm chứng sai số 5-Tuple Monoid so với mã tham chiếu "
            "đơn máy (baseline.py), đo hệ số giảm lệch tải của 8 giỏ Salting, số Shuffle Stages (BroadcastJoin) & "
            "hiệu quả cắt tỉa Parquet.",
        ],
        [
            "2. Nguyễn Thành Đạt (25MS23306)",
            "Kỹ thuật: Mục 2, 6 & 7B · Trình bày: Slide 06–09",
            "Huấn luyện & chạy Ablation 7 cấu hình (14D vs 23D) trên dữ liệu đã tách burst; tuyển chọn bằng VAL "
            "Composite Gate; kiểm định Future-Shuffle, McNemar & hiệu chỉnh ngưỡng chuyển pha τ trên VAL.",
        ],
        [
            "3. Phạm Công Minh (25MS23310)",
            "Kỹ thuật: Mục 3, 8, 9 & 10 · Trình bày: Slide 10–13",
            "Đo kiểm Runtime luồng Kafka → Speed Layer (watermark thật) → MongoDB ($status_version & p50/p95 "
            "IXSCAN); kiểm thử WAL Recovery, Tier-2 Recon & dựng ReactJS + FastAPI TOC 5-Tab (:8000).",
        ],
    ]
    s13.add_table(0.55, 2.00, 14.90, 3.84, t_headers, t_rows, [0.20, 0.24, 0.56], font_size=9.0)

    s13.add_card(
        0.55,
        5.98,
        7.35,
        2.40,
        "Lộ Trình Thực Thi Kế Tiếp (Sprint 1 & Sprint 2)",
        [
            "Sprint 1 — Thực nghiệm Đồ thị & Predictive ML +30p: Sinh Static Feature Store (84,206 đoạn cơ giới), "
            "tách burst khỏi TRAIN, khóa Frozen Registry, chạy Ablation 7 mô hình & đo phủ Cold-Start.",
            "Sprint 2 — Benchmark Runtime Hạ tầng & Web TOC: Khởi chạy cụm 5 container mặc định Docker (Kafka, "
            "Spark, MongoDB, live-consumer & ReactJS + FastAPI TOC :8000), đo IXSCAN & WAL.",
        ],
        badge="Immediate Next Actions",
        theme="sky_strong",
        body_size=10.3,
    )
    s13.add_card(
        8.10,
        5.98,
        7.35,
        2.40,
        "Tiêu Chí Nghiệm Thu Thực Nghiệm (Target KPIs)",
        [
            "Chuẩn xác toán học & Chống rò rỉ: Sai số gộp Monoid = 0.0 so với chuẩn đơn luồng; sai lệch kiểm thử "
            "Future-Shuffle = 0.0; vượt trội Persistence Baseline trên các ca giao thông chuyển trạng thái.",
            "Độ tin cậy vận hành & Phục vụ: Độ đầy đủ cửa sổ sau đối soát Tier-2 đạt 100%; 0 bản ghi trùng khi "
            "hồi phục WAL; truy vấn MongoDB Compound Index IXSCAN đạt SLA < 10 ms và pass 100% pytest.",
        ],
        badge="Acceptance Targets",
        theme="navy",
        body_size=10.3,
    )
    return s13


def _safe_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)
    except OSError:
        pass


def _save_pre_experiment_outputs(prs: Presentation, all_slides: list[DualSlide]) -> tuple[Path, Path]:
    slide_pngs: list[Path] = []
    slide_figs = [dual.fig for dual in all_slides]
    PRE_EXP_SLIDE_DIR.mkdir(parents=True, exist_ok=True)
    for idx, dual in enumerate(all_slides, start=1):
        png_path = PRE_EXP_SLIDE_DIR / f"slide_{idx:02d}.png"
        dual.save_png(png_path, close_fig=False)
        slide_pngs.append(png_path)

    pptx_name = "HCMC_Smart_Traffic_PreExperiment_Presentation.pptx"
    pdf_name = "HCMC_Smart_Traffic_PreExperiment_Slides.pdf"

    primary_pptx = REPO_DIR / pptx_name
    prs.save(str(primary_pptx))
    _safe_copy(primary_pptx, OUTPUTS_DIR / pptx_name)

    primary_pdf = REPO_DIR / pdf_name
    pdf_targets = [primary_pdf, OUTPUTS_DIR / pdf_name]
    grid_targets = [OUTPUTS_DIR / "pre_exp_slides_overview_grid.png"]
    export_overview_grid_and_pdf(
        slide_pngs,
        grid_targets,
        pdf_targets,
        slide_figs=slide_figs,
        pdf_title="HCMC Smart Traffic Big Data Platform — Design & Methodology Presentation (Pre-Experiment · BDA501)",
    )
    _render_pdf_to_pngs(primary_pdf, [SLIDE_RENDER_DIR / "pre_experiment"])
    return primary_pptx, primary_pdf


def main() -> None:
    arch_img = _resolve_arch_img()
    eda_chart = render_eda_chart(CHART_DIR / "chart_eda_anomalies.png")
    map_prob = CHART_DIR / "map_hcmc_problem_context.png"
    if not map_prob.is_file():
        geom = _load_segment_geometries()
        render_hcmc_problem_context_map(geom, map_prob)

    method_img1 = render_pre_exp_imputation_monoid(CHART_DIR / "pre_exp_methods_imputation_monoid.png")
    method_img2 = render_pre_exp_graph_sketches(CHART_DIR / "pre_exp_methods_graph_coldstart_sketches.png")
    flow_img = render_pre_exp_pipeline_walkthrough(CHART_DIR / "pre_exp_concrete_example_flow.png")
    batch_flow_img = render_pre_exp_batch_flow(CHART_DIR / "pre_exp_flow_batch_components.png")
    stream_flow_img = render_pre_exp_stream_flow(CHART_DIR / "pre_exp_flow_stream_components.png")

    prs = Presentation()
    prs.slide_width = Inches(16.0)
    prs.slide_height = Inches(9.0)
    prs.core_properties.title = (
        "HCMC Smart Traffic Big Data Platform — Design & Methodology Milestone (Pre-Experiment · BDA501)"
    )
    prs.core_properties.author = (
        "Nguyễn Tạ Minh Quyền (25MS23280), Nguyễn Thành Đạt (25MS23306), Phạm Công Minh (25MS23310)"
    )
    prs.core_properties.subject = "FSB Course BDA501 — Big Data Analytics (Sections 1–8 + Limitations & Next Plan)"
    prs.core_properties.comments = (
        "13-Slide Pre-Experiment Deck: 1.Problem 2.Literature 3.Arch 4.EDA + 2 Method Explainers + "
        "5.Pipeline 6.Modeling 7.Batch 8.Stream 9.Limitations/Risks 10.Plan & Next Actions"
    )

    all_slides = [
        _add_ms_slide_01_cover(prs),
        _add_ms_slide_02_problem_statement(prs, map_prob),
        _add_ms_slide_03_literature_review(prs),
        _add_ms_slide_04_system_architecture(prs, arch_img),
        _add_ms_slide_05_data_eda_and_problems(prs, eda_chart),
        _add_ms_slide_06_method_explainer_1(prs, method_img1),
        _add_ms_slide_07_method_explainer_2(prs, method_img2),
        _add_ms_slide_08_data_pipeline(prs, flow_img),
        _add_ms_slide_09_modeling(prs),
        _add_ms_slide_10_batch_processing(prs, batch_flow_img),
        _add_ms_slide_11_stream_processing(prs, stream_flow_img),
        _add_ms_slide_12_limitations_and_risks(prs),
        _add_ms_slide_13_plan_next_actions(prs),
    ]

    pptx_path, pdf_path = _save_pre_experiment_outputs(prs, all_slides)
    print(
        f"Generated {len(all_slides)} pre-experiment slides -> {pptx_path.name} ({pptx_path.stat().st_size:,} B) "
        f"& {pdf_path.name} ({pdf_path.stat().st_size:,} B)"
    )


if __name__ == "__main__":
    main()
