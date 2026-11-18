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

from build_slides import _add_dual_rect, _render_pdf_to_pngs, render_eda_chart  # noqa: E402
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
    HEX_SKY_PALE,
    HEX_SKY_PRIMARY,
    HEX_SLATE,
    HEX_SLATE_DARK,
    DualSlide,
    _pptx_pill,
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
    _pptx_pill(s1.slide, 1.05, 2.00, 2.86, 0.36, badge_label, HEX_SKY_PALE, HEX_SKY_DEEP, HEX_NAVY, 8.6)

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
            "Mật độ xe cao trên mạng lưới đường đan xen dày đặc (cửa ngõ sân bay, Q.1–Q.10, Thủ Đức) khiến nghẽn "
            "cục bộ giờ cao điểm nhanh chóng lan truyền diện rộng.",
            "Điều tiết hiện chủ yếu phản ứng bị động khi đã kẹt cứng, gây lãng phí lớn về thời gian và chi phí "
            "vận tải.",
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
            "Bài toán: Thiết kế nền tảng dữ liệu lớn để (1) suy diễn lưu thông toàn đô thị kể cả vùng mù, (2) tìm "
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
        "Khoảng Trống Nghiên Cứu & Định Hướng Thiết Kế Nền Tảng Tích Hợp",
        [
            "Khoảng trống thực tiễn: Mô hình học thuật giả định cảm biến cao tốc liên tục, trong khi đô thị thực tế "
            "đối mặt vùng mù đường nhánh rộng lớn và thiếu cảnh báo sớm +30 phút.",
            "Vì sao cần nền tảng tích hợp? Mô hình dự báo đơn lẻ không tự xử lý được GPS di động nhiễu/trễ, không suy "
            "diễn được vùng mù nếu thiếu cấu trúc mạng lưới đường, và không nối được trực ban TOC với quy hoạch "
            "Sở GTVT.",
            "Quy trình nghiên cứu: Xác định vấn đề (Mục 1) → Tìm khoảng trống (Mục 2) → Nghiên cứu mô hình & thiết kế "
            "luồng xử lý (Mục 3–8) → Kế hoạch thực nghiệm & đo kiểm (Mục 9–10).",
        ],
        badge="Research Gap & Logic",
        theme="sky",
        body_size=8.3,
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
        (
            "Kiến trúc nền tảng tích hợp giải quyết đồng thời vùng mù quan trắc, "
            "dự báo sớm +30 phút và cảnh báo trực tuyến"
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
            "Phase A (Offline Batch Fit): Huấn luyện mô hình & trích xuất đặc trưng tĩnh một lần trên tập TRAIN, khóa "
            "chữ ký SHA-256 tại Frozen Registry.",
            "Phase B (Online Speed Layer): Luồng trực tuyến chỉ đọc (.predict_proba) phát cảnh báo tạm thời (v1), "
            "tuyệt đối không xung đột ghi.",
            "Phase C (Late Recon): Cuối ngày gom tin trễ > 30m cập nhật bản chuẩn (v2) "
            "trong ngân sách 11.0 / 16.0 GB RAM.",
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
            "Single-Host Contention: Thiết kế 5 dịch vụ mặc định Kafka (1.5G), Spark (6G), Mongo (2G), ReactJS+FastAPI "
            "TOC (:8000, 1G) & live-consumer (0.5G) trên 1 máy tiềm ẩn tranh chấp I/O đĩa khi Shuffle lớn.",
            "Eventual Consistency Gap: Cảnh báo luồng (v1) ưu tiên độ trễ thấp, nên việc cập nhật biến trễ từ các gói "
            "tin muộn > 30m có độ trễ đến chu kỳ Phase C cuối ngày.",
        ],
        badge="Arch Risk",
        theme="navy",
        body_size=8.2,
    )
    return s4


def _add_ms_slide_05_data_eda_and_problems(prs: Presentation, eda_chart: Path) -> DualSlide:
    """Slide 05 (Section 4): Raw 5-Table EDA & 10 Data Problems (pure EDA + designed remedies, no runtime outputs)."""
    s5 = _new_ms_slide(prs)
    s5.add_chrome(
        5,
        TOTAL_PRE_SLIDES,
        1,
        "SECTION 04 · DATA, EDA & DATA PROBLEM — KIỂM TOÁN 5 BẢNG GỐC & 10 THÁCH THỨC DỮ LIỆU (EC-1..EC-10)",
        "4. Data, EDA & Data Problem: Hồ Sơ 5 Bảng Gốc & 10 Vấn Đề Dữ Liệu Thực Nghiệm",
        "Phân tích khám phá 5 bảng dữ liệu thô (38.50 MB) và định hình giải pháp thiết kế cho 10 dị thường dữ liệu",
    )
    s5.add_image(0.55, 2.00, 6.25, 4.16, eda_chart)

    s5.add_card(
        0.55,
        6.28,
        6.25,
        2.10,
        "Hồ Sơ 5 Bảng Gốc & Mục Tiêu Quy Mô",
        [
            "5 bảng gốc Kaggle (38.50 MB): nodes (577,967 điểm), streets (5,553 đường), segments (84,633 thô → "
            "84,206 cơ giới), segment_status (90,938 pings) & train (33,441 → 33,394 cửa sổ cơ giới).",
            "Đóng vai trò mẫu đối chứng thiết kế (Proof-of-Scale) cho toàn TP.HCM (ước tính 8–17 tỷ tin/ngày, "
            "1.5–3.0 TB/ngày); dữ liệu thô chỉ phủ GPS trên 11.87% đoạn cơ giới.",
        ],
        badge="Proof-of-Scale",
        theme="sky",
        body_size=7.9,
    )

    ec_headers = ["Data Problem (EC)", "Hiện tượng từ EDA & Phân tán", "Thiết kế Kỹ thuật Đề xuất"]
    ec_rows = [
        ["EC-1 · Missing Vmax", "85.21% cửa sổ (28,495) thiếu Vmax", "Train-Only 3-Tier Imputation (P85/Median/Prior)"],
        ["EC-2 · Bad Geometry", "154 đoạn ≤ 0m, 427 POIs & 151 ping lỗi", "ANSI try_cast Quarantine + trung vị 27.0m"],
        ["EC-3 · Unicode Drift", "631 dòng Quận & 2,247 dòng lệch dấu", "Chuẩn hóa Unicode NFC đồng nhất tên đường"],
        ["EC-4 · UTC Rollover", "4,305 cửa sổ (12.89%) lệch ngày UTC+7", "Quy đổi vn_hour (+7h), ict_date & sin/cos"],
        [
            "EC-5 · Gaps & Burst",
            "66.30% (15,514) đứt ≥ 60m & 7,192 burst",
            "Exact +30m (7,885 cặp) & sạch burst TRAIN",
        ],
        [
            "EC-6 · Cold-Start",
            "88.13% (74,211 đoạn) có 0 bản tin GPS",
            "Static Feature Store (PageRank + 7D K-Means)",
        ],
        [
            "EC-7 · Shape Points",
            "90.97% (525,786 nodes) vẽ cong & 314 cụt",
            "Cắt tỉa shape points, loại U-turn & bù cụt",
        ],
        ["EC-8 · Data Skew", "Tập trung tin giờ cao điểm & lệch avg()", "5-Tuple Monoid + 2-Stage Salting (8 giỏ)"],
        ["EC-9 · Late & Dupes", "Nguy cơ gửi lặp gói tin & trễ mạng > 30m", "32.0 KiB Bloom Filter + Tier-2 Backfill"],
        [
            "EC-10 · Crash & Stale",
            "Rủi ro sập luồng & tin cũ v1 đến muộn",
            "WAL Checkpoint + khóa chống ghi đè $status_version",
        ],
    ]
    s5.add_table(6.96, 2.00, 8.49, 6.38, ec_headers, ec_rows, [0.22, 0.38, 0.40], font_size=7.9)
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
    sm1.add_image(0.55, 2.00, 14.90, 4.50, method_img1)

    sm1.add_card(
        0.55,
        6.64,
        7.35,
        1.74,
        "Nguyên Lý Nội Suy 3 Tầng (3-Tier Vmax)",
        [
            "Không đoán mò hay lấy trung bình toàn bộ dữ liệu gây rò rỉ tương lai: hệ thống chỉ học bảng tra trên "
            "tập TRAIN theo thứ tự ưu tiên từ riêng đến chung (lịch sử chính đoạn đó → đường cùng cấp → chuẩn đô thị).",
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
        "Nguyên Lý 5-Tuple Monoid & Key Salting",
        [
            "Lấy trung bình của trung bình luôn bị lệch khi các máy chứa số xe khác nhau; giữ bộ 5 đại lượng cộng được "
            "(số xe, tổng vận tốc, min, max, số xe kẹt) giúp chia 8 giỏ song song "
            "mà kết quả cuối vẫn chính xác tuyệt đối.",
        ],
        badge="Exact Algebra",
        theme="white",
        body_size=8.1,
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
        "Vì Sao Thiết Kế Bộ Đếm O(1) Sketches?",
        [
            "Ở quy mô tỷ bản tin/ngày, lưu danh sách mã tin để lọc trùng sẽ tràn RAM. Thiết kế RotatingBloomFilter "
            "(32 KiB · 2×16 KiB), HLL++ (1 KiB) và điểm nóng EWMA giúp xử lý tức thời trong ngân sách bộ nhớ cực nhỏ.",
        ],
        badge="33.0 KiB Budget",
        theme="sky_strong",
        body_size=8.1,
    )
    return sm2


def _add_ms_slide_08_data_pipeline(prs: Presentation, flow_img: Path) -> DualSlide:
    """Slide 08 (Section 5): Data Pipeline & Processing Design (no post-run empirical counts)."""
    s8 = _new_ms_slide(prs)
    s8.add_chrome(
        8,
        TOTAL_PRE_SLIDES,
        2,
        "SECTION 05 · DATA PIPELINE & DATA PROCESSING — ORDERED QUALITY FUNNEL & TRAIN-ONLY IMPUTATION",
        "5. Data Pipeline & Processing: Thiết Kế Quality Funnel & Nội Suy Train-Only",
        "Thiết kế phễu làm sạch bảo toàn 100% quân số bản ghi, chuẩn hóa Unicode NFC / UTC+7 và chuẩn hóa nhãn LOS A–F",
    )
    s8.add_image(0.55, 2.00, 14.90, 4.14, flow_img)

    s8.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Thiết Kế Ordered Quality Funnel & 3-Tier Vmax",
        [
            "5-Step Quality Funnel: Dùng ANSI try_cast tách bản ghi lỗi sang Quarantine (không làm sập job, không rơi "
            "bản ghi ngầm); loại bỏ lối đi bộ phi cơ giới; chuẩn hóa dấu Unicode NFC & múi giờ UTC+7.",
            "Train-Only 3-Tier Vmax (khóa vmax_lookup.json trên TRAIN): Giữ tốc độ gốc hợp lệ → Tier 1 P85 lịch sử "
            "đoạn → Tier 2 Median theo cấp×loại đường → Tier 3 quy chuẩn đô thị 80/60/50/40 km/h.",
            "Chuẩn hóa trạng thái cửa sổ 30 phút (vận tốc thực / Vmax) thành 6 mức LOS A–F: A–B (≥65%) · C–D (35–65%) "
            "· E–F (<35%: kẹt cứng).",
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
            "Static Vmax Approximation: Các tuyến khuyết biển báo phải dùng Vmax suy diễn tĩnh theo cấp đường (Tier 2 "
            "& 3), chưa tự động phản ánh rào chắn thi công hay ngập nước cục bộ.",
            "Schema Drift Quarantine Risk: ANSI try_cast giữ pipeline không sập, nhưng nếu thiết bị GPS đổi định dạng "
            "hàng loạt sẽ làm tăng tỷ lệ bản ghi bị cách ly vào Quarantine.",
            "30m Slot Discretization: Khung gom 30 phút làm mượt các dao động vận tốc ngắn hạn dưới 5 phút tại ngã tư.",
        ],
        badge="Pipeline Risk",
        theme="navy",
        body_size=7.9,
    )
    return s8


def _add_ms_slide_09_modeling(prs: Presentation) -> DualSlide:
    """Slide 09 (Section 6): Model & Feature Research Design (no post-fit importances or ROC-AUC scores)."""
    s9 = _new_ms_slide(prs)
    s9.add_chrome(
        9,
        TOTAL_PRE_SLIDES,
        2,
        "SECTION 06 · MODELING — THIẾT KẾ NHÃN EXACT T+30M, KHÔNG GIAN 23 BIẾN & KIẾN TRÚC MÔ HÌNH",
        "6. Modeling: Thiết Kế Nhãn Exact t+30m, Vector 23 Biến & Mô Hình Dự Báo",
        "Nghiên cứu cơ chế ghép cặp chống rò rỉ tương lai, tổ hợp 23 biến không gian – thời gian và dự báo Cold-Start",
    )
    s9.add_card(
        0.55,
        2.00,
        7.35,
        3.08,
        "Thiết Kế Nhãn Exact t+30m & Chống Rò Rỉ",
        [
            "Exact (segment_id, slot_ts + 30m) Join: Chỉ ghép nhãn khi cùng đoạn đường có quan sát "
            "ở đúng khung kế tiếp t + 30 phút (7,885 cặp hợp lệ từ EDA; loại bỏ 15,514 cặp đứt quãng ≥ 60m).",
            "Chronological Split 70/15/15 + Burst Separation & Purge: Chia tập TRAIN / VAL / TEST theo trục thời "
            "gian, tách 7,192 burst windows tổng hợp khỏi TRAIN và cắt mẫu sát biên.",
            "Future-Shuffle Falsification Protocol: Kiểm chứng tự động bằng phép xáo trộn tương lai τ > t để đảm bảo "
            "ma trận đầu vào X_t hoàn toàn bất biến.",
        ],
        badge="Zero Leakage",
        theme="white",
        body_size=8.1,
    )
    s9.add_card(
        0.55,
        5.24,
        7.35,
        3.14,
        "Thiết Kế Tổ Hợp 23 Biến Đầu Vào (23D Features)",
        [
            "Nhóm 1 — Local State & Lags (7D): current_los_ordinal, mean_velocity, velocity_ratio, obs_count, "
            "lag1_los_ordinal, lag2_los_ordinal, trailing_severe_rate (nắm bắt động lượng ngắn hạn).",
            "Nhóm 2 — Cyclical ICT Time & Geometry (7D): period_sin, period_cos, weekday, gap_minutes, length_m, "
            "street_level, max_velocity (mã hóa chu kỳ cao điểm theo giờ VN & hình học đường).",
            "Nhóm 3 — Static Graph, Cluster & Causal Priors (9D): pagerank_score, in/out_degree, cluster_id, "
            "neighbor_los, segment/street_causal_prior (chỉ tích lũy nhân quả trên quá khứ τ < t).",
        ],
        badge="23D Features",
        theme="sky",
        body_size=8.2,
    )
    s9.add_card(
        8.10,
        2.00,
        7.35,
        3.08,
        "Mô Hình Nghiên Cứu & Cơ Chế Cold-Start",
        [
            "Mô hình Đề xuất — Random Forest (23D): Thiết kế 250 cây, max_depth=14, tuyển chọn khách quan bằng Live "
            "VAL Composite Gate (val_acc + val_pr + val_roc) / 3 trên dữ liệu organic thưa.",
            "Hệ đối chứng Ablation dự kiến: Đặt cạnh Persistence Baseline, Decision Tree & Binary GBT (14D vs. 23D); "
            "kết hợp Static Feature Store phủ 74,211 đoạn Cold-Start (84,206 đoạn cơ giới).",
        ],
        badge="Model Design",
        theme="sky_strong",
        body_size=8.3,
    )
    s9.add_card(
        8.10,
        5.24,
        7.35,
        3.14,
        "Limitations & Risks (Modeling)",
        [
            "Rủi ro bảo thủ tại biên chuyển pha: Do giao thông có quán tính giữ nguyên mức cũ rất cao giữa 2 khung 30 "
            "phút, ngưỡng argmax mặc định dễ bị thiên lệch về trạng thái tĩnh — cần hiệu chỉnh ngưỡng τ trên tập VAL.",
            "Exogenous Blindness: Vector 23 biến chưa tích hợp nguồn dữ liệu mưa ngập, tai nạn hay sự kiện đột xuất "
            "nên mô hình chỉ nhận diện bất thường sau khi vận tốc GPS bắt đầu giảm.",
        ],
        badge="Modeling Risk",
        theme="navy",
        body_size=8.3,
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
    s10.add_image(0.55, 2.00, 14.90, 4.14, batch_flow_img)

    s10.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Thiết Kế Cơ Chế Batch ETL, Đồ Thị & Phân Cụm",
        [
            "Distributed Spark ETL: Thiết kế BroadcastHashJoin ghim danh mục đường vào RAM; 2-Stage Key Salting (8 giỏ "
            "0..7) đi kèm bộ gộp giao hoán 5-Tuple Monoid (cnt, sum, min, max, sev) chống nghẽn giờ cao điểm và bảo "
            "toàn kết quả chính xác; lưu kho Gold Parquet phân vùng theo year_month.",
            "Graph & Cold-Start Mining: Cắt tỉa 90.97% điểm uốn cong để dựng Directed Line-Graph PageRank (β=0.85, "
            "loại U-turn, bù nút cụt); thiết kế 2-Pass SON / FPGrowth tìm tập đường đồng kẹt "
            "& 7D K-Means phủ kín 74,211 đoạn Cold-Start.",
        ],
        badge="Batch Design",
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
            "Nguy cơ cỡ mẫu thưa ở luật lan truyền đồng kẹt: Do tín hiệu GPS gốc chỉ phủ 11.87% đoạn cơ giới "
            "và đứt quãng, các luật lan truyền trễ +30m có thể có Support thấp — "
            "chỉ nên dùng làm giả thuyết thăm dò (Exploratory).",
            "Giới hạn phân cụm Cold-Start tĩnh: 74,211 đoạn thiếu GPS được gán cụm theo hình thái đồ thị và Vmax cấp "
            "đường, chưa phản ánh được các điểm họp chợ tự phát hay nút cổ chai vi mô trong hẻm.",
        ],
        badge="Batch Risk",
        theme="navy",
        body_size=7.9,
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
    s11.add_image(0.55, 2.00, 14.90, 4.14, stream_flow_img)

    s11.add_card(
        0.55,
        6.28,
        7.35,
        2.10,
        "Thiết Kế Luồng O(1), Đối Soát Hai Tầng & Serving",
        [
            "O(1) Sketches & WAL: Phân vùng Kafka KRaft theo segment_id; thiết kế bộ nhớ cố định 33.0 KiB gồm "
            "RotatingBloomFilter (32.0 KiB · 2×16 KiB, k=10) lọc trùng & HyperLogLog++ (1.0 KiB) đếm đường hoạt động; "
            "EWMA Hotspot (α=0.15) bắt điểm ĐANG kẹt lì bổ trợ Random Forest (+30m) kèm nhật ký WAL.",
            "Two-Tier Recon & MongoDB: Thiết kế đối soát Tier 1 (kiểm chuẩn đúng hạn) & Tier 2 (gom bù tin trễ > 30m, "
            "tính lại lag & re-score v2); Bulk Upsert kèm khóa $status_version & IXSCAN (mục tiêu < 10 ms) cho "
            "ReactJS + FastAPI TOC (:8000).",
        ],
        badge="33.0 KiB · SLA <10ms",
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
            "Giới hạn cửa sổ xoay vòng RotatingBloomFilter (2×16 KiB): Thiết kế 2 thế hệ tránh bão hòa bit dài ngày, "
            "nhưng bản tin trùng đến quá muộn sau 2 thế hệ sẽ dựa vào chỉ mục event_id duy nhất trên MongoDB.",
            "Độ trễ cập nhật của sự kiện quá muộn (> 30m Watermark): Bản tin trễ hơn 30 phút do mất sóng di động phải "
            "đợi chu kỳ Tier-2 Reconciliation cuối ngày mới được cập nhật vào bản chuẩn v2.",
        ],
        badge="Stream Risk",
        theme="navy",
        body_size=7.9,
    )
    return s11


def _add_ms_slide_12_limitations_and_risks(prs: Presentation) -> DualSlide:
    """Slide 12 (Section 9): Consolidated Design Limitations & System-Wide Risks (pre-experiment)."""
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
            (
                "5 dịch vụ mặc định chia sẻ tài nguyên trên 1 máy Docker (11.0/16 GB);\n"
                "độ trễ đồng bộ biến trễ giữa Phase B và Phase C"
            ),
            "Giới hạn cứng mem_limit từng container;\nphân tách đọc/ghi 3-Phase MLOps",
        ],
        [
            "5. Data Pipeline &\nData Processing",
            (
                "Các tuyến khuyết biển báo dùng Vmax suy diễn tĩnh (Tier 2/3);\n"
                "nguy cơ tăng cách ly Quarantine khi đổi schema"
            ),
            "Train-Only 3-Tier Vmax chống rò rỉ;\nANSI try_cast + Quarantine (0 silent drops)",
        ],
        [
            "6. Modeling",
            (
                "Nguy cơ ngưỡng mặc định bảo thủ & lớp B–E thưa;\n"
                "thiếu biến ngoại sinh trực tiếp (mưa ngập, tai nạn)"
            ),
            "Lọc burst TRAIN, VAL Composite Gate & hiệu chỉnh τ;\ncảnh báo sớm theo xác suất rủi ro liên tục",
        ],
        [
            "7. Batch Processing",
            "Luật đồng kẹt trễ +30m dễ có Support thấp do GPS thưa;\nphân cụm Cold-Start chỉ dựa trên hình thái tĩnh",
            "Gắn nhãn Exploratory Hypothesis;\nkết hợp Line-Graph PageRank bảo toàn khối lượng",
        ],
        [
            "8. Stream Processing",
            "RotatingBloom 32.0 KiB giải phóng sau 2 thế hệ;\ntin trễ > 30m phải chờ phiên đối soát cuối ngày",
            "MongoDB event_id unique + Two-Tier Recon bù trễ;\nkhóa $status_version chặn tin cũ v1 ghi đè bản v2",
        ],
    ]
    s12.add_table(0.55, 2.00, 8.55, 6.38, comp_headers, comp_rows, [0.22, 0.43, 0.35], font_size=8.0)

    s12.add_card(
        9.26,
        2.00,
        6.19,
        6.38,
        "4 Rủi Ro & Giới Hạn Chung Cấp Hệ Thống",
        [
            "1. Spatial & Fleet Representation Bias: Nguồn GPS cộng đồng chỉ phủ 11.87% đoạn cơ giới (lệch về ô tô "
            "trên trục chính), chưa đại diện đầy đủ cho luồng xe máy (> 80% lưu lượng TP.HCM).",
            "2. Open-World Concept Drift & Seasonality: Mô hình đóng băng ở Phase A có nguy cơ suy giảm độ chính xác "
            "khi chuyển sang mùa mưa ngập lịch sử hoặc phát sinh rào chắn thi công mới.",
            "3. Rare Sudden-Onset & Class Imbalance: Các pha đổi trạng thái đột ngột và lớp trung gian B–E chiếm tỷ "
            "trọng nhỏ, đòi hỏi lọc burst TRAIN, đánh giá PR-AUC và hiệu chỉnh ngưỡng.",
            "4. Proof-of-Scale & Human-in-the-Loop Scope: Thiết kế hiện hướng tới kiểm chứng trên tập mẫu chuẩn 38.50 "
            "MB và cung cấp khuyến nghị điều hành, chưa đóng vòng tự động vào đèn SCATS.",
        ],
        badge="System-Wide Risks",
        theme="navy",
        body_size=8.3,
    )
    return s12


def _add_ms_slide_13_plan_next_actions(prs: Presentation) -> DualSlide:
    """Slide 13 (Section 10): Current Research Progress (Sections 1–8) & Concrete Next Actions Plan."""
    s13 = _new_ms_slide(prs)
    s13.add_chrome(
        13,
        TOTAL_PRE_SLIDES,
        4,
        "SECTION 10 · PLAN & NEXT ACTIONS — TIẾN ĐỘ HOÀN THÀNH THIẾT KẾ (MỤC 1–8) & KẾ HOẠCH THỰC NGHIỆM TIẾP THEO",
        "10. Plan & Next Actions: Kế Hoạch Thực Nghiệm Mô Hình, Đo Kiểm Runtime & Demo",
        "Phân công thực thi giai đoạn tiếp theo sau khi hoàn tất nghiên cứu mô hình và thiết kế toàn bộ luồng xử lý",
    )
    t_headers = [
        "Thành viên Nhóm",
        "Đã Hoàn Thành (Mục 1–8: Nghiên cứu & Thiết kế)",
        "Next Actions (Thực nghiệm Mô hình, Đo kiểm Runtime & Demo)",
    ]
    t_rows = [
        [
            "1. Nguyễn Tạ Minh Quyền\n(MSSV: 25MS23280)",
            (
                "Hoàn tất thiết kế Mục 1, 4, 5 & 7A:\n"
                "Bài toán, kiểm toán EDA 5 bảng gốc,\n"
                "Quality Funnel, 3-Tier Vmax & Batch ETL"
            ),
            "Chạy thực nghiệm Spark Batch trên dữ liệu thực: kiểm chứng sai số 5-Tuple Monoid so với baseline.py, đo "
            "hệ số giảm lệch tải của 8 giỏ Salting, số Shuffle Stages (BroadcastJoin) & hiệu quả cắt tỉa Parquet.",
        ],
        [
            "2. Nguyễn Thành Đạt\n(MSSV: 25MS23306)",
            (
                "Hoàn tất thiết kế Mục 2, 6 & 7B:\n"
                "Literature Review, Line-Graph PageRank,\n"
                "SON/K-Means, nhãn t+30m & mô hình 23D"
            ),
            "Huấn luyện & chạy Ablation 7 cấu hình (14D vs. 23D) trên dữ liệu đã tách burst; tuyển chọn bằng VAL "
            "Composite Gate; kiểm định Future-Shuffle, McNemar & hiệu chỉnh ngưỡng chuyển pha τ trên VAL.",
        ],
        [
            "3. Phạm Công Minh\n(MSSV: 25MS23310)",
            (
                "Hoàn tất thiết kế Mục 3, 8, 9 & 10:\n"
                "Kiến trúc Zones A–F (Docker 11.0G),\n"
                "luồng Kafka, Sketches 33 KiB & Web TOC"
            ),
            "Đo kiểm Runtime luồng Kafka → Speed Layer (watermark thật) → MongoDB ($status_version & p50/p95 "
            "IXSCAN); kiểm thử WAL Recovery, Tier-2 Recon & dựng ReactJS + FastAPI TOC 5-Tab (:8000).",
        ],
    ]
    s13.add_table(0.55, 2.00, 14.90, 3.82, t_headers, t_rows, [0.19, 0.30, 0.51], font_size=8.1)

    s13.add_card(
        0.55,
        5.98,
        7.35,
        2.40,
        "Lộ Trình Thực Thi Kế Tiếp (Sprint 1 & Sprint 2)",
        [
            "Sprint 1 — Thực nghiệm Đồ thị & Học máy Dự báo +30m: Sinh kho Static Feature Store (84,206 đoạn cơ "
            "giới), tách burst khỏi TRAIN, khóa Frozen Registry, chạy Ablation 7 mô hình & đo phủ Cold-Start.",
            "Sprint 2 — Benchmark Runtime Hạ tầng & Tích hợp Web TOC: Khởi chạy cụm 5 container mặc định Docker "
            "(Kafka, Spark, MongoDB, live-consumer & ReactJS + FastAPI TOC :8000), đo IXSCAN & WAL.",
        ],
        badge="Immediate Next Actions",
        theme="sky_strong",
        body_size=8.2,
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
            "Độ tin cậy vận hành & Phục vụ: Độ đầy đủ cửa sổ sau đối soát Tier-2 đạt 100%; "
            "0 bản ghi trùng khi hồi phục sự cố WAL; truy vấn MongoDB Compound Index IXSCAN "
            "đạt SLA < 10 ms và pass 100% kiểm thử tự động pytest.",
        ],
        badge="Acceptance Targets",
        theme="navy",
        body_size=8.2,
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
    arch_img = OUTPUTS_DIR / "system_architecture.png"
    eda_chart = CHART_DIR / "chart_eda_anomalies.png"
    if not eda_chart.is_file():
        render_eda_chart(eda_chart)
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
