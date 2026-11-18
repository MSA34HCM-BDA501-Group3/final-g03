"""Build the Human-Centered HCMC Smart Traffic Final Project Report & Technical Specification PDF."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import matplotlib
from PIL import Image as PILImage
from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    KeepTogether,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

REPO_DIR = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = REPO_DIR / "outputs"
CHART_DIR = OUTPUTS_DIR / "slide_charts"


def _register_fonts() -> tuple[str, str, str, str]:
    mpl_ttf = Path(matplotlib.__file__).resolve().parent / "mpl-data" / "fonts" / "ttf"
    linux_dir = Path("/usr/share/fonts/truetype/dejavu")
    for font_dir in (mpl_ttf, linux_dir):
        if (font_dir / "DejaVuSans.ttf").is_file():
            pdfmetrics.registerFont(TTFont("DocSans", str(font_dir / "DejaVuSans.ttf")))
            pdfmetrics.registerFont(TTFont("DocSansBold", str(font_dir / "DejaVuSans-Bold.ttf")))
            pdfmetrics.registerFont(TTFont("DocSansItalic", str(font_dir / "DejaVuSans-Oblique.ttf")))
            pdfmetrics.registerFont(TTFont("DocSansBoldItalic", str(font_dir / "DejaVuSans-BoldOblique.ttf")))
            pdfmetrics.registerFont(TTFont("DocMono", str(font_dir / "DejaVuSansMono.ttf")))
            pdfmetrics.registerFontFamily(
                "DocSans",
                normal="DocSans",
                bold="DocSansBold",
                italic="DocSansItalic",
                boldItalic="DocSansBoldItalic",
            )
            return "DocSans", "DocSansBold", "DocSansItalic", "DocMono"
    return "Helvetica", "Helvetica-Bold", "Helvetica-Oblique", "Courier"


FONT_REG, FONT_BOLD, FONT_ITALIC, FONT_MONO = _register_fonts()

NAVY = colors.HexColor("#0F2942")
TEAL = colors.HexColor("#0D6E6E")
BLUE = colors.HexColor("#1D4ED8")
GREEN = colors.HexColor("#15803D")
AMBER = colors.HexColor("#B45309")
SLATE = colors.HexColor("#334155")
DARK_TEXT = colors.HexColor("#1E293B")
ALT_ROW = colors.HexColor("#F8FAFC")
BORDER_GRAY = colors.HexColor("#CBD5E1")
CALLOUT_BG = colors.HexColor("#EFF6FF")
CALLOUT_BORDER = colors.HexColor("#2563EB")
OK_BG = colors.HexColor("#ECFDF5")
OK_BORDER = colors.HexColor("#059669")
WARN_BG = colors.HexColor("#FFFBEB")
WARN_BORDER = colors.HexColor("#D97706")
FORMULA_BG = colors.HexColor("#F8FAFC")
CODE_BG = colors.HexColor("#0F172A")
WHITE = colors.white

CONTENT_W = 515.0


class NumberedCanvas(pdfcanvas.Canvas):
    """Two-pass canvas rendering exact 'Trang X / N' headers and footers."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._saved_page_states: list[dict] = []

    def showPage(self) -> None:
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self) -> None:
        total_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self._draw_chrome(total_pages)
            super().showPage()
        super().save()

    def _draw_chrome(self, total_pages: int) -> None:
        self.saveState()
        width, height = A4
        self.setStrokeColor(BORDER_GRAY)
        self.setLineWidth(0.6)
        if self._pageNumber > 1:
            self.line(40, height - 32, width - 40, height - 32)
            self.setFont(FONT_BOLD, 7.4)
            self.setFillColor(NAVY)
            self.drawString(40, height - 25, "BDA501 — Hệ Thống Giám Sát & Dự Báo Ùn Tắc Giao Thông TP.HCM")
            self.setFont(FONT_REG, 7.2)
            self.setFillColor(SLATE)
            self.drawRightString(width - 40, height - 25, "Báo Cáo Tổng Kết Đồ Án & Đặc Tả Kỹ Thuật Chi Tiết")

        self.line(40, 30, width - 40, 30)
        self.setFont(FONT_REG, 7.1)
        self.setFillColor(SLATE)
        self.drawString(
            40,
            18,
            "Nhóm thực hiện: Nguyễn Tạ Minh Quyền (25MS23280) · Nguyễn Thành Đạt (25MS23306) · "
            "Phạm Công Minh (25MS23310)",
        )
        self.setFont(FONT_BOLD, 7.4)
        self.setFillColor(TEAL)
        self.drawRightString(width - 40, 18, f"Trang {self._pageNumber} / {total_pages}")
        self.restoreState()


def _load_ground_truth() -> dict[str, str]:
    metrics = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    manifest = json.loads((OUTPUTS_DIR / "model_registry_manifest.json").read_text(encoding="utf-8"))
    mongo = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    spark = json.loads((OUTPUTS_DIR / "spark_runtime_evidence.json").read_text(encoding="utf-8"))
    ix = mongo["ixscan_explain_verification"]
    lat = mongo["uc2_arterial_query"]["latency_benchmark_ms"]
    ev = metrics["evaluation"]
    cj = spark["catalyst_join_spark"]
    ms = spark["monoid_salting_spark"]
    return {
        "model_sha12": manifest["sha256_digest"][:12],
        "static_sha12": manifest["static_feature_store_sha256"][:12],
        "vmax_sha12": manifest["vmax_lookup_sha256"][:12],
        "aug_rows": f"{metrics['lakehouse_c14']['year_month_partitions']['2020-08']:,}",
        "rf_acc": f"{ev['accuracy_6class']:.4f}",
        "rf_mf1": f"{ev['macro_f1_6class']:.4f}",
        "rf_f1": f"{ev['severe_f1']:.4f}",
        "rf_roc": f"{ev['roc_auc']:.4f}",
        "rf_pr": f"{ev['average_precision']:.4f}",
        "mongo_med": f"{lat['warm_median_ms']:.3f} ms",
        "mongo_p95": f"{lat['warm_p95_ms']:.3f} ms",
        "mongo_collscan": f"{lat['collscan_warm_median_ms']:.3f} ms",
        "mongo_speedup": f"{lat['ixscan_vs_collscan_speedup_x']:.2f}x",
        "mongo_stages": ", ".join(ix["winning_stages"]),
        "arterial_alerts": f"{mongo['uc2_arterial_query']['total_matching_windows']:,}",
        "join_base_ms": f"{cj['baseline_median_ms']:.2f}",
        "join_base_iqr": f"{cj['baseline_iqr_ms']:.2f}",
        "join_opt_ms": f"{cj['optimized_median_ms']:.2f}",
        "join_opt_iqr": f"{cj['optimized_iqr_ms']:.2f}",
        "join_speedup": f"{cj['join_speedup_pct']:.2f}%",
        "monoid_unsalted_ms": f"{ms['unsalted_median_ms']:.2f}",
        "monoid_salted_ms": f"{ms['salted_median_ms']:.2f}",
        "monoid_unsalted_iqr": f"{ms['unsalted_iqr_ms']:.2f}",
        "monoid_salted_iqr": f"{ms['salted_iqr_ms']:.2f}",
    }


def build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "DocTitle",
            parent=base["Normal"],
            fontName=FONT_BOLD,
            fontSize=14.6,
            leading=19.2,
            textColor=NAVY,
            spaceAfter=4,
        ),
        "subtitle": ParagraphStyle(
            "DocSubtitle",
            parent=base["Normal"],
            fontName=FONT_REG,
            fontSize=8.6,
            leading=12.6,
            textColor=SLATE,
            spaceAfter=6,
        ),
        "h1": ParagraphStyle(
            "SectionH1",
            parent=base["Normal"],
            fontName=FONT_BOLD,
            fontSize=11.6,
            leading=15.8,
            textColor=NAVY,
            spaceBefore=8,
            spaceAfter=4.5,
            keepWithNext=True,
        ),
        "h2": ParagraphStyle(
            "SectionH2",
            parent=base["Normal"],
            fontName=FONT_BOLD,
            fontSize=9.7,
            leading=13.5,
            textColor=TEAL,
            spaceBefore=6.5,
            spaceAfter=3.0,
            keepWithNext=True,
        ),
        "body": ParagraphStyle(
            "Body",
            parent=base["Normal"],
            fontName=FONT_REG,
            fontSize=8.75,
            leading=13.1,
            textColor=DARK_TEXT,
            alignment=TA_JUSTIFY,
            spaceAfter=4.5,
        ),
        "th": ParagraphStyle(
            "TableHead",
            parent=base["Normal"],
            fontName=FONT_BOLD,
            fontSize=7.8,
            leading=10.6,
            textColor=WHITE,
            alignment=TA_LEFT,
        ),
        "td": ParagraphStyle(
            "TableCell",
            parent=base["Normal"],
            fontName=FONT_REG,
            fontSize=7.7,
            leading=10.8,
            textColor=DARK_TEXT,
            alignment=TA_LEFT,
        ),
        "td_bold": ParagraphStyle(
            "TableCellBold",
            parent=base["Normal"],
            fontName=FONT_BOLD,
            fontSize=7.7,
            leading=10.8,
            textColor=NAVY,
            alignment=TA_LEFT,
        ),
        "callout": ParagraphStyle(
            "CalloutText",
            parent=base["Normal"],
            fontName=FONT_REG,
            fontSize=8.45,
            leading=12.6,
            textColor=DARK_TEXT,
        ),
        "code": ParagraphStyle(
            "CodeBlock",
            parent=base["Normal"],
            fontName=FONT_MONO,
            fontSize=7.6,
            leading=11.0,
            textColor=colors.HexColor("#E2E8F0"),
        ),
        "caption": ParagraphStyle(
            "Caption",
            parent=base["Normal"],
            fontName=FONT_ITALIC,
            fontSize=7.9,
            leading=11.0,
            textColor=SLATE,
            spaceBefore=3,
            spaceAfter=6,
        ),
    }


def make_callout(text: str, styles: dict[str, ParagraphStyle], bg=CALLOUT_BG, border=CALLOUT_BORDER) -> Table:
    tbl = Table([[Paragraph(text, styles["callout"])]], colWidths=[CONTENT_W])
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), bg),
                ("BOX", (0, 0), (-1, -1), 0.7, border),
                ("LINEBEFORE", (0, 0), (0, -1), 3.4, border),
                ("LEFTPADDING", (0, 0), (-1, -1), 9),
                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 5.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5.5),
            ]
        )
    )
    return tbl


def make_equation_box(
    img_path: Path, note_html: str, styles: dict[str, ParagraphStyle], width_ratio: float = 0.94
) -> Table:
    rows: list[list] = []
    if img_path.is_file():
        with PILImage.open(img_path) as im:
            w_px, h_px = im.size
        img_w = CONTENT_W * width_ratio
        img_h = img_w * (h_px / max(w_px, 1))
        eq_img = Image(str(img_path), width=img_w, height=img_h)
        eq_img.hAlign = "CENTER"
        rows.append([eq_img])
    rows.append([Paragraph(note_html, styles["callout"])])
    tbl = Table(rows, colWidths=[CONTENT_W])
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), FORMULA_BG),
                ("BOX", (0, 0), (-1, -1), 0.65, BORDER_GRAY),
                ("LINEBEFORE", (0, 0), (0, -1), 3.2, TEAL),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return tbl


def make_code_box(lines: list[str], styles: dict[str, ParagraphStyle]) -> Table:
    formatted = [
        f'<font name="{FONT_BOLD}" color="#93C5FD">{ln[3:-4]}</font>'
        if ln.startswith("<b>") and ln.endswith("</b>")
        else ln
        for ln in lines
    ]
    html = "<br/>".join(formatted)
    tbl = Table([[Paragraph(html, styles["code"])]], colWidths=[CONTENT_W])
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), CODE_BG),
                ("BOX", (0, 0), (-1, -1), 0.8, TEAL),
                ("LEFTPADDING", (0, 0), (-1, -1), 9),
                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 6.0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6.0),
            ]
        )
    )
    return tbl


def make_table(
    headers: list[str],
    rows: list[list[str]],
    col_widths: list[float],
    styles: dict[str, ParagraphStyle],
    bold_first_col: bool = True,
    highlight_row: int | None = None,
) -> Table:
    scale = CONTENT_W / sum(col_widths)
    scaled_widths = [w * scale for w in col_widths]
    data = [[Paragraph(h, styles["th"]) for h in headers]]
    for row in rows:
        cells = [
            Paragraph(cell, styles["td_bold" if (idx == 0 and bold_first_col) else "td"])
            for idx, cell in enumerate(row)
        ]
        data.append(cells)

    tbl = Table(data, colWidths=scaled_widths, repeatRows=1)
    cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.45, BORDER_GRAY),
        ("LEFTPADDING", (0, 0), (-1, -1), 5.0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5.0),
        ("TOPPADDING", (0, 0), (-1, -1), 3.8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.8),
    ]
    for r_idx in range(1, len(data)):
        bg = WHITE if r_idx % 2 == 1 else ALT_ROW
        if highlight_row is not None and r_idx == highlight_row:
            bg = OK_BG
        cmds.append(("BACKGROUND", (0, r_idx), (-1, r_idx), bg))
    tbl.setStyle(TableStyle(cmds))
    return tbl


def _add_centered_figure(story: list, img_path: Path, width_ratio: float, caption: str, styles: dict) -> None:
    if not img_path.is_file():
        return
    with PILImage.open(img_path) as im:
        w_px, h_px = im.size
    img_w = CONTENT_W * width_ratio
    img_h = img_w * (h_px / max(w_px, 1))
    img = Image(str(img_path), width=img_w, height=img_h)
    img.hAlign = "CENTER"
    story.append(KeepTogether([img, Paragraph(caption, styles["caption"])]))


def _draw_card_box(
    d: Drawing,
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    lines: list[str],
    header_fill=NAVY,
    fill_color=WHITE,
) -> None:
    d.add(Rect(x, y, w, h, rx=3.5, ry=3.5, fillColor=fill_color, strokeColor=header_fill, strokeWidth=0.8))
    hh = 15.0
    d.add(Rect(x, y + h - hh, w, hh, rx=3.5, ry=3.5, fillColor=header_fill, strokeColor=header_fill, strokeWidth=0.5))
    d.add(String(x + 6, y + h - 10.8, title, fontName=FONT_BOLD, fontSize=7.0, fillColor=WHITE))
    for i, txt in enumerate(lines):
        d.add(String(x + 6, y + h - hh - 10.5 - i * 10.0, txt, fontName=FONT_REG, fontSize=6.6, fillColor=DARK_TEXT))


def _draw_h_arrow(d: Drawing, x1: float, y1: float, x2: float, y2: float, col=TEAL) -> None:
    d.add(Line(x1, y1, x2, y2, strokeColor=col, strokeWidth=1.2))
    ang_sign = 1.0 if x2 >= x1 else -1.0
    pts = [x2, y2, x2 - 4.8 * ang_sign, y2 + 2.6, x2 - 4.8 * ang_sign, y2 - 2.6]
    d.add(Polygon(pts, fillColor=col, strokeColor=col))


def build_cover_banner() -> Drawing:
    d = Drawing(CONTENT_W, 32)
    d.add(Rect(0, 2, CONTENT_W, 28, rx=4.0, ry=4.0, fillColor=NAVY, strokeColor=NAVY, strokeWidth=0.5))
    d.add(Rect(0, 2, 6.5, 28, rx=2.0, ry=2.0, fillColor=colors.HexColor("#38BDF8"), strokeColor=NAVY, strokeWidth=0.0))
    d.add(
        String(
            14,
            18.5,
            "FPT SCHOOL OF BUSINESS & TECHNOLOGY (FSB) · HỌC PHẦN BDA501 — BIG DATA ANALYTICS (MSE AI)",
            fontName=FONT_BOLD,
            fontSize=7.8,
            fillColor=WHITE,
        )
    )
    d.add(
        String(
            14,
            7.8,
            "BÁO CÁO TỔNG KẾT ĐỒ ÁN & ĐẶC TẢ KỸ THUẬT CHI TIẾT (FINAL PROJECT REPORT & TECHNICAL SPECIFICATION)",
            fontName=FONT_REG,
            fontSize=6.9,
            fillColor=colors.HexColor("#BAE6FD"),
        )
    )
    return d


def build_boundary_drawing() -> Drawing:
    d = Drawing(CONTENT_W, 92)
    _draw_card_box(
        d,
        2,
        5,
        150,
        82,
        "1. External Inputs (2 Nguồn)",
        [
            "1) GPS Telemetry Stream (Kafka):",
            "   90,938 pings thô (90,787 hợp lệ)",
            "   (1% Duplicates, 1.5% Late > 30m)",
            "2) OSM / GIS Master Topology:",
            "   52,181 giao lộ, 84,206 đoạn cơ giới",
            "   (train.csv tách riêng đối chứng)",
        ],
        header_fill=NAVY,
    )
    _draw_card_box(
        d,
        166,
        5,
        174,
        82,
        "2. Unified Pipeline (P1 → P4)",
        [
            "• Bước 1 (P1): Quality Funnel & LOS",
            "• Bước 2 (P2): Line-Graph PageRank",
            "   & 7D K-Means (88.13% Cold-Start)",
            "• Bước 3 (P3): Predictive ML (+30m)",
            "• Bước 4 (P4): O(1) Stream Sketches",
            "   & Two-Tier Reconciliation",
        ],
        header_fill=TEAL,
        fill_color=colors.HexColor("#F0FDFA"),
    )
    _draw_card_box(
        d,
        354,
        5,
        159,
        82,
        "3. System Outputs (4 Đầu ra)",
        [
            "1) Medallion Lakehouse (Parquet)",
            "   (10 tháng · 33,394 cửa sổ 30m)",
            "2) Frozen Model & Feature Registry",
            "3) Quarantine & WAL Checkpoints",
            "4) MongoDB 8.0 IXSCAN (< 1 ms)",
            "   & ReactJS + FastAPI TOC (:8000)",
        ],
        header_fill=BLUE,
        fill_color=CALLOUT_BG,
    )
    _draw_h_arrow(d, 152, 46, 166, 46, NAVY)
    _draw_h_arrow(d, 340, 46, 354, 46, TEAL)
    return d


def build_mlops_drawing() -> Drawing:
    d = Drawing(CONTENT_W, 82)
    _draw_card_box(
        d,
        2,
        5,
        160,
        72,
        "Phase A: Offline Batch Fit (t <= T_train)",
        [
            "• Cutoff: ≤ 2020-12-12T08:00:00Z",
            "• 1,885 organic TRAIN (tách burst)",
            "• Chỉ Phase A gọi .fit() & khóa Registry",
            "• Ghi SHA-256 cho 3 Frozen Artifacts",
        ],
        header_fill=GREEN,
        fill_color=OK_BG,
    )
    _draw_card_box(
        d,
        180,
        5,
        158,
        72,
        "Phase B: Online Stream Transform",
        [
            "• starter_stream.py chỉ gọi .transform()",
            "• Xác thực SHA-256 trước & sau mỗi lô",
            "• Dự báo LOS(t+30m) & P(Severe)",
            "• Upsert STREAM_PROVISIONAL (v1)",
        ],
        header_fill=BLUE,
        fill_color=CALLOUT_BG,
    )
    _draw_card_box(
        d,
        356,
        5,
        157,
        72,
        "Phase C: Two-Tier Reconciliation",
        [
            "• Tier 1: exceptAll == 0 (0 diff)",
            "• Tier 2: Backfill 500 cửa sổ trễ > 30m",
            "• Heal Lag Store cho 609 cửa sổ sau",
            "• Re-score 384 cửa sổ → RECONCILED (v2)",
        ],
        header_fill=AMBER,
        fill_color=WARN_BG,
    )
    _draw_h_arrow(d, 162, 41, 180, 41, GREEN)
    _draw_h_arrow(d, 338, 41, 356, 41, BLUE)
    return d


def _append_cover_and_ch1_context(story: list, s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(build_cover_banner())
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "Hệ Thống Giám Sát Hiện Trạng &amp; Dự Báo Ùn Tắc Giao Thông Đô Thị TP. Hồ Chí Minh",
            s["title"],
        )
    )
    story.append(
        Paragraph(
            "<b>Nhóm thực hiện:</b> "
            "<b>1. Nguyễn Tạ Minh Quyền (25MS23280)</b> — <i>Làm sạch dữ liệu &amp; Xử lý phân tán Spark</i> · "
            "<b>2. Nguyễn Thành Đạt (25MS23306)</b> — <i>Đồ thị giao lộ, Phân cụm &amp; Dự báo trước 30 phút</i> · "
            "<b>3. Phạm Công Minh (25MS23310)</b> — <i>Luồng trực tuyến Kafka, Đối soát tin trễ, MongoDB "
            "&amp; Giao diện điều hành</i><br/>"
            "<b>Giảng viên hướng dẫn (Instructor):</b> <b>Đặng Trần Khánh</b> · <b>Học kỳ:</b> Fall 2026",
            s["subtitle"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Tóm tắt cốt lõi (Toàn cảnh bài toán &amp; lời giải trong 4 bước):</b><br/>"
                "<b>Mục tiêu của hệ thống</b> là giám sát hiện trạng và dự báo trước 30 phút nguy cơ kẹt xe trên toàn "
                "mạng lưới <b>84,206 đoạn đường cơ giới một chiều</b> tại TP.HCM (đã lọc 427 lối đi bộ/hầm không cơ "
                "giới từ 84,633 đoạn thô) từ dữ liệu hành trình GPS của xe cộ "
                "(chẳng hạn lúc 07:08 sáng trên đoạn đường Lý Thường Kiệt mã 47407, một chiếc xe gửi về tín hiệu báo "
                "đang di chuyển chậm ở vận tốc 4 km/h).<br/>"
                "Tuy nhiên, dữ liệu thực tế gặp <b>3 rào cản lớn</b>: <b>(1) Thiếu hụt thông tin:</b> Gần "
                "<b>88.13%</b> đoạn đường (74,211 đoạn nhánh, hẻm dân cư) không có xe gửi GPS về, và đa số "
                "đường bị thiếu thông tin tốc độ tối đa cho phép; <b>(2) Đứt quãng &amp; Burst:</b> Xe gửi tín hiệu "
                "không đều, 66.30% khoảng cách ≥ 60 phút và có 7,192 cửa sổ cụm tín hiệu tổng hợp; <b>(3) Nhiễu đường "
                "truyền:</b> Bản tin gửi về theo thời gian thực thường bị trùng lặp hoặc đến trễ.<br/>"
                "Để giải quyết trọn vẹn, hệ thống vận hành qua <b>4 bước nối tiếp nhau</b>:<br/>"
                "• <b>Bước 1 – Chuẩn hóa bức tranh hiện trạng (Spark Batch ETL):</b> Làm sạch 90,938 bản tin GPS thô "
                "còn 90,787 bản tin cơ giới hợp lệ, tự động ước lượng tốc độ tối đa, và gom theo khung 30 phút "
                "(33,394 cửa sổ cơ giới; 89.19% khớp BkTraffic) để gán mức Thông thoáng (A–B), Đông đúc (C–D) "
                "hay Kẹt cứng (E–F).<br/>"
                "• <b>Bước 2 – Lấp đầy 'vùng mù' bằng cấu trúc bản đồ (Đồ thị &amp; Phân cụm):</b> Với 88.13% đoạn "
                "đường không có GPS, hệ thống phân tích cấu trúc giao lộ (PageRank không U-turn) tìm nút cổ chai, "
                "đồng thời gom nhóm hành vi đường phố (7D K-Means) để suy đoán cho nơi thiếu cảm biến.<br/>"
                "• <b>Bước 3 – Dự báo trước 30 phút (Machine Learning):</b> Tách chuỗi burst tổng hợp khỏi tập học để "
                "còn 1,885 mẫu tự nhiên, kết hợp trạng thái hiện tại, giờ Việt Nam và đặc trưng đồ thị để huấn luyện "
                f"Random Forest dự báo kẹt cứng sau 30 phút (đạt <b>ROC-AUC = {gt['rf_roc']}</b>, "
                f"<b>PR-AUC = {gt['rf_pr']}</b>, <b>Severe F1 = {gt['rf_f1']}</b>).<br/>"
                "• <b>Bước 4 – Cảnh báo trực tuyến ReactJS + FastAPI TOC &amp; Đối soát dữ liệu trễ:</b> Phục vụ trực "
                f"tuyến qua trung tâm điều hành 5-Tab ReactJS + FastAPI (:8000) (<b>{gt['mongo_p95']}</b> p95, "
                f"nhanh hơn <b>{gt['mongo_speedup']}</b>); đồng thời cuối ngày đối soát bù 500 cửa sổ trễ và hàn gắn "
                "609 lag chính xác <b>100%</b>."
            ),
            s,
            bg=OK_BG,
            border=OK_BORDER,
        )
    )
    story.append(Spacer(1, 5))

    story.append(
        Paragraph(
            "Chương 1. Câu Chuyện Thực Tế Từ Dòng Tín Hiệu GPS, Dây Chuyền 4 Bước (P1–P4), "
            "Tình Huống Sử Dụng (UC-1–UC-6) &amp; Ranh Giới Hệ Thống",
            s["h1"],
        )
    )
    story.append(
        Paragraph(
            "1.1. Bối Cảnh Bài Toán Điều Hành Giao Thông TP.HCM, Dòng Dữ Liệu Thực Tế &amp; Vai Trò Proof-of-Scale",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Tại siêu đô thị TP. Hồ Chí Minh, mật độ phương tiện rất cao lưu thông trên mạng lưới hàng chục nghìn "
                "đoạn đường đan xen chặt chẽ khiến một điểm nghẽn cục bộ vào giờ cao điểm nhanh chóng lan truyền thành "
                "kẹt xe dây chuyền. Công tác quản lý hiện hữu gặp <b>ba rào cản thực tiễn</b>: <b>(1)</b> cảm biến "
                "vòng từ và camera CCTV cố định có chi phí đầu tư/bảo trì cao nên chỉ đặt tại một số nút giao chính, "
                "còn nguồn GPS hành trình cộng đồng lại lệch về đại lộ, bỏ trống phần lớn đường nhánh (<i>vùng mù quan "
                "trắc</i>) và thường xuyên ngắt quãng, trễ tin; <b>(2)</b> bảng giám sát hiện tại chỉ phản ánh nơi "
                "<b>đang kẹt</b> mà thiếu cảnh báo sớm trước 30 phút nơi <b>sắp chuyển sang kẹt cứng</b> để điều chỉnh "
                "đèn và phân luồng chủ động; <b>(3)</b> trực ban sự vụ tức thời tại Trung tâm Điều hành (TOC) vẫn tách "
                "rời khỏi quy hoạch điểm nghẽn dài hạn của Sở GTVT. "
                "Để thấy rõ dữ liệu thực địa gửi gì về hệ thống, hãy nhìn vào một bản ghi gốc trong bảng "
                "<i>segment_status.csv</i>: "
                "vào lúc <b>07:08:28 sáng giờ Việt Nam</b> ngày 03/08/2020 (bản ghi mã <i>15281</i>), một phương tiện "
                "đi ngang qua <b>đoạn đường Lý Thường Kiệt số 47407</b> (dài 16m tại Quận 10) gửi về vận tốc tức thời "
                "chỉ <b>4 km/h</b>; hoặc lúc <b>11:30 trưa</b> tại <b>Công Trường Quốc Tế (đoạn 21064, Quận 3)</b>, xe "
                "báo vận tốc <b>12 km/h</b>. Để biến hàng chục nghìn con số vận tốc rời rạc như vậy thành thông tin "
                "điều hành dễ hiểu trên toàn mạng lưới <b>84,206 đoạn cơ giới một chiều</b>, hệ thống quy đổi tỷ lệ "
                "giữa vận tốc thực tế so với tốc độ tối đa cho phép của đoạn đường sang <b>6 mức phục vụ giao thông "
                "(Level of Service — LOS từ A đến F)</b>: <b>mức A–B (đạt từ 65% tốc độ tối đa trở lên)</b> là đường "
                "thông thoáng, xe chạy tự do; <b>mức C–D (từ 35% đến dưới 65%)</b> là đường đông đúc giờ cao điểm; "
                "còn <b>mức E–F (dưới 35%, riêng mức F dưới 20% hoặc xe đứng yên)</b> là ùn ứ nghiêm trọng đến kẹt "
                "cứng tê liệt."
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "map_hcmc_hotspots_coldstart.png",
        0.80,
        "Hình 1.1. Bản đồ giao thông TP.HCM tái tạo từ 84,206 đoạn đường cơ giới cùng sông Sài Gòn và các quận "
        "trung tâm: phần xám nhạt là 74,211 đoạn đường nhánh không có xe gửi GPS (88.13%), các trục đỏ là điểm nóng "
        "hay kẹt cứng.",
        s,
    )
    story.append(
        make_callout(
            (
                "<b>Vai trò mẫu chuẩn kiểm chứng quy mô (Proof-of-Scale — Tại sao dùng Spark và Kafka cho tập dữ liệu "
                "~38.5 MB?):</b> Năm bảng công khai của bộ dữ liệu giao thông TP.HCM trên Kaggle có tổng dung lượng "
                "<b>38.50 MB</b> (riêng bảng vận tốc <i>segment_status.csv</i> nặng <b>3.58 MB</b> với 90,938 bản tin "
                "GPS). Nếu chỉ tính toán một lần trên tệp tĩnh này, thư viện Pandas trên máy cá nhân chỉ mất vài chục "
                "mili-giây. Tuy nhiên, trong đồ án Dữ liệu lớn, tập dữ liệu này đóng vai trò <b>mẫu chuẩn kiểm chứng "
                "quy mô (Proof-of-Scale)</b>: khi kết nối thực tế hơn 1 triệu xe buýt, taxi, xe công nghệ và xe tải "
                "tại TP.HCM phát tín hiệu mỗi 5–10 giây, hệ thống sẽ phải xử lý <b>8–17 tỷ bản tin/ngày "
                "(~1.5–3.0 TB/ngày)</b>. Ở quy mô đó, hiện tượng lệch tải giờ cao điểm (gấp 684.95 lần trung vị) và "
                "bộ nhớ lọc trùng không giới hạn sẽ làm tràn RAM máy đơn. Chạy kiến trúc phân tán trên mẫu chuẩn giúp "
                "nhóm đối chứng từng phép tính với <i>baseline.py</i> đạt <b>sai số bằng 0.0</b> trước khi mở rộng ra "
                "cụm máy chủ lớn."
            ),
            s,
        )
    )


def _append_ch1_problems_usecases_and_boundary(story: list, s: dict[str, ParagraphStyle]) -> None:
    story.append(Paragraph("1.2. Sự Liên Kết Nhân Quả Giữa Bốn Bài Toán Con (Coupled Sub-Problems P1 → P4)", s["h2"]))
    story.append(
        Paragraph(
            (
                "Bốn bài toán con <b>P1, P2, P3, P4</b> không phải là bốn công đoạn chạy mã nguồn rời rạc, mà là "
                "<b>bốn mắt xích nhân quả nối tiếp nhau</b>: kết quả chuẩn hóa của P1 nuôi dữ liệu cho P2; đặc trưng "
                "nút cổ chai (<i>Line-Graph PageRank</i>) và nhóm hành vi đường phố (<i>7D K-Means</i>) ở P2 được "
                "truyền thẳng làm biến đầu vào (<i>Static Features</i>) cho mô hình dự báo ở P3; và mô hình P3 được "
                "đóng băng để chấm điểm luồng trực tuyến kết hợp giám sát điểm nóng tức thời ở P4."
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_concrete_example_flow.png",
        0.95,
        "Hình 1.1b. Ví dụ minh họa xuyên suốt (End-to-End Walkthrough): Hành trình bản tin GPS thực tế trên đường "
        "Lý Thường Kiệt (segment_id = 47407) đi qua 4 bước xử lý liên kết nhân quả (P1 → P4).",
        s,
    )
    p_rows = [
        [
            "Bước 1 (P1)\nDescriptive",
            "Hiện tại và trong lịch sử, đoạn đường nào đang thông thoáng hay kẹt cứng?",
            "90,938 bản tin GPS thô (90,787 hợp lệ); 88.34% đoạn thiếu max_velocity.",
            (
                "33,394 cửa sổ 30 phút gắn mức LOS A–F chuẩn xác (sai số 0.0 so với baseline.py, "
                "89.19% khớp BkTraffic), làm nền dữ liệu sạch cho Bước 2 và Bước 3."
            ),
        ],
        [
            "Bước 2 (P2)\nDiagnostic",
            "Làm sao hiểu các nút cổ chai và 88.13% đoạn đường nhánh bị Cold-Start (0 GPS)?",
            "Đồ thị 84,206 đoạn cơ giới (92,453 cạnh chuyển tiếp bỏ U-turn) và các giỏ LOS E/F.",
            (
                "Điểm nút cổ chai Line-Graph PageRank, luật SON/FPGrowth và 6 cụm hành vi 7D K-Means — vừa phục vụ "
                "Sở GTVT, vừa cấp Static Features cho Bước 3."
            ),
        ],
        [
            "Bước 3 (P3)\nPredictive",
            "Đúng 30 phút nữa (t + 30m), đoạn đường nào sẽ chuyển sang kẹt cứng?",
            "7,885 cặp +30m (1,885 organic TRAIN sau lọc burst), giờ VN và Static Features.",
            (
                "Mô hình Random Forest (23D Feature Vector) dự báo LOS và xác suất kẹt cứng sau 30 phút "
                "(ROC-AUC = 0.9894, PR-AUC = 0.9543, Severe F1 = 0.9362), đóng băng cho Bước 4."
            ),
        ],
        [
            "Bước 4 (P4)\nPrescriptive",
            "Làm sao vừa cảnh báo dưới 1 ms trên Web TOC, vừa không mất Late Events?",
            "Luồng Kafka đổ về liên tục có lẫn 1% Duplicates (333) và 500 cửa sổ trễ > 30m.",
            (
                "O(1) Stream Sketches (33.0 KiB), MongoDB IXSCAN (0.360 ms median / 0.603 ms p95, 31.36x) phục vụ "
                "ReactJS + FastAPI TOC (:8000), và Recon bù 500 trễ + 609 lag."
            ),
        ],
    ]
    story.append(
        make_table(
            ["Sub-Problem", "Câu hỏi Thực tiễn Cần Giải quyết", "Input Nghiệp vụ", "Output & Vai trò Nối tiếp"],
            p_rows,
            [68, 136, 138, 173],
            s,
        )
    )

    story.append(Paragraph("1.3. Sáu Kịch Bản Vận Hành Thực Tế (Operational Use Cases UC-1–UC-6)", s["h2"]))
    uc_rows = [
        [
            "UC-1: Điểm nóng kéo dài\n(EWMA Hotspot)",
            "Trung tâm Điều hành (TOC)",
            "Danh sách Top-25 đoạn đường đang kẹt cứng liên tiếp nhiều giờ chưa thông (Lý Thường Kiệt, Trương Định).",
            "Phát cảnh báo lên bảng điện tử ngoài đường và cử lực lượng ra điều tiết ngay tại nút giao.",
        ],
        [
            "UC-2: Ưu tiên đèn xanh\n(IXSCAN < 1 ms)",
            "Kỹ sư Tín hiệu Đèn",
            (
                "Danh sách 1,234 trục cấp 1–2 có P(Severe) ≥ 70% sau 30 phút, phản hồi warm 0.360 ms "
                "(p95 0.603 ms, 31.36x)."
            ),
            "Chủ động kéo dài pha đèn xanh hướng trục chính trước khi dòng xe kịp dồn ứ khóa chặt ngã tư.",
        ],
        [
            "UC-3: Quy hoạch vùng mù\n(Cold-Start Audit)",
            "Chuyên viên Sở GTVT",
            "Bản đồ xếp hạng nút cổ chai PageRank (Nguyễn Kiệm #1 = 2.7799, Ngã tư Gò Dưa = 2.7196, Phú Định).",
            "Chọn đúng vị trí trọng yếu để ưu tiên lắp mới camera giám sát và biển phân luồng tải trọng.",
        ],
        [
            "UC-4: Né kẹt dây chuyền\n(Co-Congestion Rules)",
            "Ứng dụng Dẫn đường",
            "Cảnh báo cặp đường thường kẹt đồng thời (Nguyễn Thị Minh Khai ↔ Cống Quỳnh, Lift 15.23x) hoặc trễ +30m.",
            "Khuyến nghị tài xế rẽ sang lộ trình thay thế từ các ngã tư phía xa trước khi đi vào vùng ùn tắc.",
        ],
        [
            "UC-5: Phân bổ ca tuần tra\n(7D K-Means Regimes)",
            "Đội Tuần tra Giao thông",
            "Bản đồ phân hoạch toàn bộ 84,206 đoạn cơ giới thành 6 nhóm hành vi (7D K-Means) & tra cứu Peer Segments.",
            (
                "Tập trung lực lượng chốt trực đúng Cụm 1 (3,530 đoạn nội đô tỷ lệ kẹt E/F cao nhất 43.26%) "
                "vào giờ cao điểm."
            ),
        ],
        [
            "UC-6: Đối soát dữ liệu trễ\n(Two-Tier Recon)",
            "Kỹ sư Dữ liệu / MLOps",
            "Báo cáo Two-Tier Recon cuối ngày: bù 500 cửa sổ trễ, hàn 609 cửa sổ lag & chấm lại 384 dự báo.",
            "Đảm bảo 100% số liệu báo cáo tổng kết ngày và Medallion Lakehouse chính xác tuyệt đối.",
        ],
    ]
    story.append(
        make_table(
            ["Mã Use Case", "Tác Nhân (Actor)", "Thông tin Nhận trên Dashboard", "Quyết định Vận hành Thực tế"],
            uc_rows,
            [100, 88, 172, 155],
            s,
        )
    )

    story.append(Paragraph("1.4. Ranh Giới Hệ Thống (System Boundary: 2 External Inputs → 4 System Outputs)", s["h2"]))
    story.append(
        Paragraph(
            (
                "<b>Hai nguồn dữ liệu đầu vào độc lập (2 External Inputs):</b> (1) <i>GPS Telemetry Stream</i> "
                "(<i>producer.py</i>, <i>seed = 42</i>) phát 33,727 thông điệp cửa sổ (từ 90,938 pings thô / "
                "90,787 hợp lệ trong <i>segment_status.csv</i>) vào <i>Apache Kafka 4.1.0</i> (1.0% Duplicates và "
                "1.5% Late > 30m); (2) <i>OSM / GIS Master Topology</i> gồm <i>nodes.csv</i> (577,967 điểm tọa độ), "
                "<i>streets.csv</i> (5,553 tuyến đường) và <i>segments.csv</i> (84,633 đoạn thô → 84,206 đoạn cơ giới "
                "sau loại 427 non-road POIs), trong khi tệp <i>train.csv</i> được tách hoàn toàn khỏi dây chuyền xử lý "
                "và chỉ dùng để đối chiếu mức độ khớp LOS (89.19%). <b>Bốn nhóm kết quả đầu ra chính thức (4 System "
                "Outputs):</b> (1) <i>Medallion Lakehouse (Parquet)</i> chia 10 phân vùng tháng và <i>dataset.csv</i> "
                "(60 cột); (2) <i>Frozen Model Registry &amp; Static Feature Store</i> phủ đủ 84,206 đoạn cơ giới; "
                "(3) <i>Quarantine Parquet &amp; WAL Checkpoints</i>; và (4) <i>MongoDB 8.0 Operational Serving</i> "
                "kết nối <i>ReactJS + FastAPI TOC Command Center 5-Tab (:8000)</i>."
            ),
            s["body"],
        )
    )
    story.append(
        KeepTogether(
            [
                build_boundary_drawing(),
                Paragraph(
                    "Hình 1.2. Đặc tả System Boundary: 2 External Inputs đi qua Unified Pipeline 4 bước (P1 → P4) "
                    "tạo ra 4 System Outputs phục vụ điều hành trực tuyến và phân tích lịch sử.",
                    s["caption"],
                ),
            ]
        )
    )
    story.append(
        Paragraph(
            "1.5. Literature Review (Tổng Quan Các Hướng Tiếp Cận Quản Lý – Dự Báo Giao Thông) &amp; Khoảng Trống",
            s["h2"],
        )
    )
    lit_rows = [
        [
            "1. Quan trắc Hạ tầng Cố định\n&amp; Đèn Thích ứng (Fixed ITS)",
            "Cảm biến vòng từ, Camera CCTV; hệ thống đèn SCATS (Sims, 1980) &amp; SCOOT (Hunt, 1981)",
            "Đo lưu lượng liên tục, chính xác cao tại nút giao có thiết bị để chỉnh chu kỳ đèn cục bộ",
            "Chi phí đầu tư và bảo trì rất lớn; chỉ phủ số ít trục chính, để trống gần 90% đường nhánh (vùng mù)",
        ],
        [
            "2. Giám sát Dữ liệu Hành trình\n(Floating Car / Probe GPS)",
            "Định vị GPS từ đội xe buýt, taxi, bản đồ số (Herrera et al., 2010; Zheng, 2015)",
            "Mở rộng độ phủ diện rộng không cần lắp cảm biến mặt đường, phản ánh vận tốc thực tế",
            "Phân bố lệch về đại lộ, tín hiệu ngắt quãng dài, thiếu chuẩn tốc độ và mới dừng ở bản đồ hiện trạng",
        ],
        [
            "3. Nghiên cứu Dự báo Ùn tắc\nNgắn hạn (Short-Term Forecast)",
            "Chuỗi thời gian ARIMA / Quán tính (Vlahogianni, 2014); mạng đồ thị sâu DCRNN, Graph WaveNet (Li, 2018)",
            "Dự báo vận tốc hoặc mức ùn tắc trước 15–60 phút bằng tương quan không gian – thời gian",
            "Thử nghiệm chủ yếu trên cao tốc cảm biến dày đặc (PeMS, METR-LA); suy giảm khi dữ liệu đô thị thưa",
        ],
        [
            "4. Nền tảng Hỗ trợ Quyết định\nĐiều hành Đô thị (Urban DSS)",
            "Trung tâm TOC &amp; nền tảng dữ liệu lớn giao thông (Lv et al., 2015; Amini et al., 2017)",
            "Kết hợp lưu trữ lịch sử quy hoạch với bảng điều khiển giám sát trực tuyến cho điều hành viên",
            (
                "Mô hình dự báo thường tách rời luồng vận hành thực tế (tin GPS trễ/trùng); chia cắt trực ban "
                "&amp; quy hoạch"
            ),
        ],
    ]
    story.append(
        make_table(
            [
                "Hướng Tiếp Cận Nghiên Cứu",
                "Hệ Thống &amp; Công Trình Tiêu Biểu",
                "Đóng Góp Trong Quản Lý Giao Thông",
                "Hạn Chế Khi Áp Dụng Cho Đô Thị TP.HCM",
            ],
            lit_rows,
            [98, 128, 134, 155],
            s,
        )
    )
    story.append(
        make_callout(
            (
                "<b>Khoảng trống nghiên cứu &amp; Lý do cần nền tảng tích hợp HCMC Smart Traffic:</b> Các nghiên cứu "
                "dự báo học thuật thường giả định chuỗi cảm biến sạch, liên tục trên cao tốc, trong khi hệ thống giám "
                "sát đô thị thực tế lại thiếu cảnh báo sớm trước 30 phút và đối mặt vùng mù đường nhánh rộng lớn "
                "(88.13% đoạn trống chuỗi GPS; 66.30% cặp quan sát ngắt quãng ≥ 60 phút). Triển khai rời rạc từng công "
                "nghệ đơn lẻ không giải được bài toán: một mô hình dự báo độc lập không thể tự làm sạch luồng GPS di "
                "động nhiễu/trễ, không suy diễn được vùng thiếu cảm biến nếu tách rời phân tích cấu trúc mạng lưới "
                "đường, và không phục vụ đồng thời trực ban TOC lẫn quy hoạch Sở GTVT. Toàn bộ nội dung báo cáo và bộ "
                "Slide tuân thủ quy trình nghiên cứu thuận chiều qua 12 phần: (1) Problem Statement, (2) Literature "
                "Review, (3) System Architecture, (4) Data/EDA/Data Problem, (5) Data Pipeline/Processing, "
                "(6) Modeling, (7) Batch Processing, (8) Stream Processing, (9) Experiments, (10) Demonstration, "
                "(11) Limitations &amp; Risks (nêu tại từng thành phần giải pháp và tổng hợp ở Chương 7), và (12) Plan."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch2_data_and_funnel(story: list, s: dict[str, ParagraphStyle]) -> None:
    story.append(PageBreak())
    story.append(
        Paragraph(
            (
                "Chương 2. Bước 1A — Khảo Sát 5 Bảng Dữ Liệu Gốc (EDA), 10 Edge Cases Thực Nghiệm &amp; Ordered "
                "Quality Funnel"
            ),
            s["h1"],
        )
    )
    story.append(Paragraph("2.1. Kiểm Kê Chi Tiết Năm Bảng Dữ Liệu Nguồn (5-Table EDA · Giao thông TP.HCM)", s["h2"]))
    inv_rows = [
        [
            "nodes.csv",
            "577,967",
            "3 (19.44 MB)",
            "_id, long, lat",
            "Có tới 525,786 điểm (90.97%) chỉ là Intermediate Shape Points để vẽ độ cong thân đường trên "
            "OpenStreetMap; chỉ có 52,181 giao lộ (Active Intersection Nodes) thực sự kết nối các đoạn cơ giới.",
        ],
        [
            "streets.csv",
            "5,553",
            "5 (0.20 MB)",
            "_id, level, max_velocity, name, type",
            "Cấp đường level 1..4; 82.23% (4,566 tuyến) bị khuyết max_velocity; 695 tên đường thô lệch bảng mã Unicode "
            "NFD/NFC được chuẩn hóa về 641 tên chuẩn.",
        ],
        [
            "segments.csv",
            "84,633",
            "11 (10.29 MB)",
            "_id, s/e_node_id, length, street_id, max_velocity...",
            "88.34% (74,762 đoạn) khuyết max_velocity; 154 đoạn ≤ 0m; lọc 427 đoạn phi cơ giới còn 84,206 đoạn cơ giới "
            "(74,211 đoạn = 88.13% Cold-Start, 0 GPS).",
        ],
        [
            "segment_status.csv",
            "90,938",
            "4 (3.58 MB)",
            "_id, updated_at, segment_id, velocity",
            "Bản tin vận tốc GPS theo Event-Time UTC (07/2020–04/2021); cách ly 151 bản tin lỗi/phi cơ giới "
            "(còn 90,787 bản tin hợp lệ); giờ cao điểm bị Data Skew gấp 684.95 lần trung vị.",
        ],
        [
            "train.csv",
            "33,441",
            "18 (4.99 MB)",
            "_id, segment_id, date, period, LOS...",
            "Bảng đối chứng độc lập (33,394 cửa sổ cơ giới sau lọc 47 phi cơ giới; 89.19% khớp LOS); 85.21% khuyết "
            "Vmax; 631 dòng Phường/Quận; 4,305 cửa sổ đổi ngày UTC→ICT (+7h); 7,192 burst windows.",
        ],
    ]
    story.append(
        make_table(
            [
                "Bảng Nguồn",
                "Số Dòng",
                "Cột & Dung lượng",
                "Các Cột Khóa Chính",
                "Phát hiện từ 5-Table EDA & Tác động Kiến trúc",
            ],
            inv_rows,
            [86, 44, 62, 108, 215],
            s,
        )
    )
    story.append(Spacer(1, 3))
    _add_centered_figure(
        story,
        CHART_DIR / "chart_eda_anomalies.png",
        0.54,
        "Hình 2.1. Tỷ lệ khuyết max_velocity, Intermediate Shape Points, Cold-Start Segments và Sensor Gaps "
        "trên 5 bảng dữ liệu gốc.",
        s,
    )

    story.append(
        Paragraph("2.2. Danh Mục 10 Edge Cases Thực Nghiệm &amp; Cơ Chế Phòng Vệ Kỹ Thuật (EC-1–EC-10)", s["h2"])
    )
    ec_rows = [
        [
            "EC-1: Khuyết 85.21% max_velocity",
            "28,495 cửa sổ (85.21%), 74,762 đoạn",
            (
                "3-Tier Velocity Imputation chỉ học từ TRAIN: Gốc (4,946) → Segment P85 trên TRAIN (7,650) "
                "→ Trung vị cùng (level, type) (19,601) → Chuẩn đô thị 80/60/50/40 km/h (1,197)."
            ),
        ],
        [
            "EC-2: Đoạn ≤ 0m & Non-Motorway",
            "154 đoạn ≤ 0m, 427 non-road POIs",
            (
                "Ordered Quality Funnel cách ly 580 bản ghi đoạn lỗi địa lý vào Quarantine Parquet; điền "
                "chiều dài trung vị 27.0m; lọc 151 pings lỗi/phi cơ giới (còn 33,394 cửa sổ trên 84,206 đoạn)."
            ),
        ],
        [
            "EC-3: Lỗi Unicode NFD/NFC & Phường/Quận",
            "631 Phường/Quận, 2,247 cửa sổ lệch dấu",
            (
                "Street Entity Hygiene chuẩn hóa Unicode NFC (695 → 641 tên đường, gộp 54 biến thể); loại bỏ nhãn "
                "Phường/Quận khỏi giỏ giao dịch trước khi chạy SON / FPGrowth."
            ),
        ],
        [
            "EC-4: Lệch múi giờ UTC vs ICT (+7h)",
            "4,305 / 33,394 cửa sổ (12.89%)",
            (
                "Giữ khóa ngày UTC để đối chứng chuẩn, đồng thời dịch sang giờ ICT <i>h</i><sub>VN</sub> = "
                "(<i>h</i><sub>UTC</sub> + 7) mod 24, cộng 1 ngày khi <i>h</i><sub>UTC</sub> ≥ 17 và mã hóa sin/cos."
            ),
        ],
        [
            "EC-5: Chuỗi đứt quãng & Burst tổng hợp",
            "7,885 / 23,399 cặp +30m; 7,192 burst",
            (
                "Exact +30m Contiguous Join trên (segment_id, slot_ts + 30m) loại 15,514 bước nhảy ≥ 60m (66.30%), "
                "đồng thời tách 7,192 burst windows (3,596 cặp) khỏi tập TRAIN (còn 1,885 organic TRAIN)."
            ),
        ],
        [
            "EC-6: 88.13% đoạn bị Cold-Start",
            "74,211 / 84,206 đoạn không có GPS",
            (
                "Xây dựng Static Feature Store (segment_static_features.parquet) phủ kín 84,206 đoạn cơ giới nhờ điểm "
                "Line-Graph PageRank và tâm cụm 7D Behavioral K-Means."
            ),
        ],
        [
            "EC-7: Shape Points & 314 Dangling Nodes",
            "525,786 Shape Points, 314 đoạn cụt",
            (
                "Cắt tỉa 90.97% Intermediate Shape Points và loại U-turn trước khi dựng Line Graph (92,453 cạnh); "
                "phân bổ lại 314 Dangling Nodes ở mỗi vòng lặp để bảo toàn tổng PageRank = 84,206.0."
            ),
        ],
        [
            "EC-8: Data Skew & Bẫy trung bình",
            "Skew 684.95x; sai số 11.11 km/h",
            (
                "5-Tuple Commutative Monoid Combiner kết hợp 2-Stage Key Salting (8 buckets) kéo độ lệch slot giờ cao "
                "điểm về 1.020x (CV = 0.0059), đạt sai số bằng 0.0 tuyệt đối."
            ),
        ],
        [
            "EC-9: 1% Duplicates & 1.5% Late Events",
            "333 tin trùng, 500 cửa sổ trễ / 226 khung",
            (
                "Chặn 333/333 tin trùng bằng 32.0 KiB RotatingBloomFilter (2×16 KiB); Sink 1 lưu 100% raw events; "
                "Phase C Two-Tier Reconciliation backfill 500 cửa sổ trễ và heal Lag Store cho 609 cửa sổ kế tiếp."
            ),
        ],
        [
            "EC-10: Crash Recovery & Stale Overwrite",
            "Ngắt tiến trình & tin cũ đến muộn",
            (
                "Khôi phục trạng thái từ WAL Checkpoints (0 nhân đôi) & Spark Real Watermark; atomic pipeline "
                "$status_version trong MongoDB chặn 25/25 tin v1 ghi đè lên bản BATCH_VERIFIED v2."
            ),
        ],
    ]
    story.append(
        make_table(
            ["Mã Edge Case", "Quy mô Ghi nhận Thực tế", "Cơ chế Phòng vệ Kỹ thuật Đã Triển khai & Kiểm chứng"],
            ec_rows,
            [118, 130, 267],
            s,
        )
    )

    story.append(Paragraph("2.3. Ordered Quality Funnel (ANSI try_cast · Bảo Toàn 100% Quân Số Bản Ghi)", s["h2"]))
    r_rows = [
        [
            '<font size="6.3">R1_NONPOSITIVE_SEGMENT_LENGTH</font>',
            "segments.csv",
            "try_cast(length AS DOUBLE) ≤ 0 OR NULL",
            "154 đoạn (57 cửa sổ)",
            "Lưu vết cách ly &amp; điền trung vị 27.0m.",
        ],
        [
            '<font size="6.3">R2_NON_ROAD_OSM_TAG</font>',
            "segments.csv",
            "street_type IN (service, footway, path, steps...)",
            "427 đoạn (580 lỗi địa lý)",
            "Cách ly 427 POIs (còn 84,206 cơ giới); 47 cửa sổ.",
        ],
        [
            '<font size="6.3">R3_ADMIN_POLYGON_STREET_NAME</font>',
            "streets / windows",
            "street_name khớp mẫu hành chính ^(Phường|Quận)",
            "631 cửa sổ",
            "Loại khỏi giỏ FPGrowth và Top PageRank.",
        ],
        [
            '<font size="6.3">R4_UNICODE_NFD_AND_PREFIX_DRIFT</font>',
            "Chuẩn hóa tên",
            "street_name ≠ NFC(trim(name))",
            "2,247 cửa sổ (54 tên gộp)",
            "Chuẩn hóa dấu tiếng Việt NFC (695 → 641 tên).",
        ],
        [
            '<font size="6.3">R5_MISSING_MAX_VELOCITY</font>',
            "segments / windows",
            "max_velocity IS NULL OR max_velocity ≤ 0",
            "28,495 cửa sổ / 74,762 đoạn",
            "Điền khuyết 3 bậc chỉ học từ tập TRAIN.",
        ],
        [
            '<font size="6.3">R6_STATIONARY_OR_INVALID_PING</font>',
            "segment_status / luồng",
            "velocity ≤ 0 / phi cơ giới (517 flagged)",
            "151 pings cách ly (448 cửa sổ)",
            "Cách ly 151 pings lỗi; giữ 90,787 pings cơ giới.",
        ],
    ]
    story.append(
        make_table(
            [
                "Mã Quy tắc Thứ tự",
                "Bảng Nguồn",
                "Điều kiện Kiểm tra (ANSI try_cast)",
                "Quy mô Ghi nhận",
                "Hành động Chuẩn hóa",
            ],
            r_rows,
            [154, 68, 114, 74, 105],
            s,
        )
    )

    story.append(
        Paragraph(
            "2.4. Ước Lượng Tốc Độ Tối Đa Chỉ Học Từ Tập TRAIN &amp; Quy Tắc Xác Định Mức LOS A–F",
            s["h2"],
        )
    )
    story.append(
        make_equation_box(
            CHART_DIR / "eq_vmax_los.png",
            (
                "<b>Bảo toàn quân số bản ghi:</b> Tổng bản tin đầu vào 90,938 = Bản trùng loại bỏ (0) + Hợp lệ "
                "(90,787) + Cách ly lỗi R6/phi cơ giới (151); 90,787 bản tin đường cơ giới (42,124 severe pings) tạo "
                "nên <b>33,394 cửa sổ cơ giới</b> (tách 47 cửa sổ phi cơ giới khỏi 33,441 dòng <i>train.csv</i>; khớp "
                "<b>89.19%</b> nhãn LOS chuẩn BkTraffic). <b>Phân bổ Vmax trên 33,394 cửa sổ (học trên TRAIN):</b> "
                "Bậc 1a gốc (4,946) → Bậc 1b P85 trên TRAIN (7,650) → Bậc 2 trung vị level×type (19,601) → Bậc 3 "
                "chuẩn đô thị (1,197). <b>Phân bố 6 mức LOS suy dẫn:</b> LOS A (13,309) · B (4,926) · C (2,657) · "
                "D (3,730) · E (3,401) · F (5,371) [đối chứng gốc: A 13,267 · B 4,694 · C 3,827 · D 3,701 · "
                "E 3,821 · F 4,084]."
            ),
            s,
        )
    )
    story.append(
        Paragraph(
            "2.5. Hạn Chế &amp; Rủi Ro Thành Phần Dữ Liệu, EDA &amp; Data Pipeline (Component Limitations &amp; Risks)",
            s["h2"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Limitations &amp; Risks (Mục 4 Data/EDA &amp; Mục 5 Data Pipeline):</b> "
                "<b>(1) Thiên lệch không gian của cảm biến (Spatial Probe Selection Bias):</b> Chỉ có 9,995 / 84,206 "
                "đoạn đường cơ giới (11.87%) có dữ liệu GPS thực tế; dữ liệu tập trung mạnh ở các trục chính và đội xe "
                "thương mại, chưa phản ánh trực tiếp vận tốc luồng xe máy trong hẻm nhỏ. "
                "<b>(2) Giả định tốc độ tối đa tĩnh (Static Vmax Approximation):</b> 20,798 / 33,394 cửa sổ (62.28%) "
                "sử dụng tốc độ tối đa suy diễn theo cấp và loại đường (Tier 2 &amp; Tier 3), chưa tự động hạ trần khi "
                "đoạn đường bị rào chắn thi công hoặc ngập nước cục bộ. "
                "<b>(3) Rủi ro tăng đột biến Quarantine khi đổi Schema đầu nguồn:</b> Cơ chế <i>ANSI try_cast</i> bảo "
                "vệ tiến trình không bị sập, nhưng nếu thiết bị định vị thay đổi định dạng bản tin mà không cập nhật "
                "schema, các bản tin mới sẽ bị đẩy vào kho cách ly <i>Quarantine [C15]</i>."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch3_architecture_and_spark(story: list, s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            (
                "Chương 3. Kiến Trúc Tổng Thể (Zones A–F, Components [C1]–[C17]), Zero-Conflict MLOps &amp; "
                "Xử Lý Phân Tán Trên Apache Spark 3.5.7"
            ),
            s["h1"],
        )
    )
    story.append(
        Paragraph(
            (
                "3.1. Sơ Đồ Kiến Trúc Giải Pháp Tổng Thể (Zones A–F), Danh Mục 17 Components ([C1]–[C17]) &amp; 10 "
                "Luồng I/O"
            ),
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Hình 3.1 trình bày <b>Sơ đồ Kiến trúc Giải pháp Tổng thể (Enterprise Cloud / Containerized Cluster "
                "Reference Architecture)</b> trên cụm Docker <b>11.0 / 16.0 GB RAM</b> với <b>5 dịch vụ mặc định</b> "
                "(hợp nhất <i>Apache Kafka 4.1.0 KRaft</i>, <i>Apache Spark 3.5.7 · Java 17</i>, "
                "<i>Medallion Lakehouse (Parquet)</i>, <i>MongoDB 8.0.34</i>, <i>live-consumer</i> và "
                "<i>ReactJS + FastAPI TOC Command Center 5-Tab :8000</i>, kèm 2 profile on-demand "
                "<i>batch/stream</i>). "
                "Để người đọc dễ dàng tra cứu, Bảng 3.1a liệt kê đầy đủ từng thành phần <b>[C1]–[C17]</b> theo "
                "6 phân vùng (<b>Zone A–F</b>), và Bảng 3.1b giải thích chi tiết <b>10 luồng dữ liệu đánh số "
                "(1–10)</b>:"
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        OUTPUTS_DIR / "system_architecture.png",
        0.96,
        (
            "Hình 3.1. Kiến trúc hệ thống tổng thể (Zones A–F, 17 Components [C1]–[C17], và 10 luồng dữ liệu đánh số "
            "1–10)."
        ),
        s,
    )

    comp_rows = [
        [
            "Zone A (External)",
            "[C1] GPS Probe Fleet",
            "producer.py (Python 3.11)",
            (
                "Phát lại 33,394 cửa sổ (33,727 sự kiện kèm 1% Duplicates = 333 & 500 Late > 30m) hoặc 90,938 raw "
                "pings (90,787 cơ giới)."
            ),
        ],
        [
            "Zone A (External)",
            "[C2] OSM / GIS Topology",
            "raw_data/*.csv Loader",
            (
                "Cung cấp đồ thị tĩnh: nodes.csv (577,967 điểm; 52,181 giao lộ thực), streets.csv (5,553 tuyến), "
                "segments.csv (84,633 thô → 84,206 cơ giới)."
            ),
        ],
        [
            "Zone A (External)",
            "[C3] Traffic Ops & Users",
            "ReactJS + FastAPI TOC (:8000)",
            (
                "Trung tâm TOC, kỹ sư tín hiệu đèn, Sở GTVT và người đi đường tra cứu dự báo, nút cổ chai, điểm nóng "
                "và đối soát."
            ),
        ],
        [
            "Zone B (Ingestion)",
            "[C4] Apache Kafka 4.1",
            "apache/kafka:4.1.0 (KRaft)",
            (
                "Event broker không cần ZooKeeper (1.5 GB RAM), quản lý topic hcmc-traffic-events (3 partitions băm "
                "theo segment_id)."
            ),
        ],
        [
            "Zone B (Ingestion)",
            "[C5] Quality Funnel",
            "PySpark SQL (try_cast, NFC)",
            (
                "Ordered Quality Funnel 5 bước: ANSI try_cast -> loại 427 POIs -> Unicode NFC (695 -> 641 tên) -> "
                "ICT (+7h) -> 3-Tier Vmax Imputation."
            ),
        ],
        [
            "Zone C (Speed Layer)",
            "[C6] Watermark (30m)",
            "starter_stream.py / spark_engine",
            (
                "Áp dụng Watermark thật 30 phút (2020-12-01T12:30Z) và tách Dual Sink: Sink 1 Parquet WAL; Sink 2 "
                "trạng thái luồng."
            ),
        ],
        [
            "Zone C (Speed Layer)",
            "[C7] O(1) Stream Sketches",
            "Bloom + HLL++ + EWMA (33 KiB)",
            (
                "Duy trì bộ nhớ hằng số O(1): 32.0 KiB RotatingBloomFilter (2×16 KiB) lọc 333/333 trùng (0.0389% FPR), "
                "1.0 KiB HLL++ đếm đoạn, và EWMA Hotspot (α=0.15)."
            ),
        ],
        [
            "Zone C (Speed Layer)",
            "[C8] Online Scorer",
            "Read-Only .transform()",
            (
                "Broadcast-join đặc trưng cửa sổ trong Watermark với Static Feature Store, gọi Frozen Model "
                ".predict_proba() và gắn STREAM_PROVISIONAL."
            ),
        ],
        [
            "Zone D (Batch Layer)",
            "[C9] Combiner & Join",
            "pipeline.py / spark_engine.py",
            (
                "5-Tuple Monoid + 8-Bucket Salting (skew 684.95x → 1.020x) + Catalyst BroadcastHashJoin "
                f"(5 → 1 Exchange, {gt['join_speedup']} speedup)."
            ),
        ],
        [
            "Zone D (Batch Layer)",
            "[C10] Graph & Mining",
            "PageRank · FPGrowth · K-Means",
            (
                "(1) Line-Graph PageRank (84,206 cơ giới, 92,453 cạnh bỏ U-turn, β=0.85); (2) SON (12+14 luật) / "
                "FPGrowth (32 luật trên organic TRAIN); (3) Train-Only 7D K-Means (k=6, Sil=0.2241)."
            ),
        ],
        [
            "Zone D (Batch Layer)",
            "[C11] Causal ML Fit",
            "pipeline.py + baseline.py",
            (
                "Chia Chronological 70/15/15 (lọc 7,192 burst → 1,885 organic TRAIN / 1,167 VAL / 1,182 TEST); "
                "VAL Gate (0.8882 vs DT 23D 0.8812); RF 23D (τ_sev=0.25, τ_chg=0.40)."
            ),
        ],
        [
            "Zone D (Batch Layer)",
            "[C12] Two-Tier Recon",
            "exceptAll + Late Backfill",
            (
                "Tier 1 exceptAll_diff == 0 (100% khớp); Tier 2 backfill 500 cửa sổ trễ, heal 609 downstream lag "
                "windows và re-score 384 cửa sổ (213 đổi prob)."
            ),
        ],
        [
            "Zone E (Registry)",
            "[C13a] Frozen Model",
            "Frozen RF / PipelineModel",
            (
                "Lưu bất biến mô hình đã huấn luyện ở Phase A (model.joblib / Spark PipelineModel) kèm khóa SHA-256 "
                "chỉ đọc cho [C8] và [C12]."
            ),
        ],
        [
            "Zone E (Registry)",
            "[C13b] Static Feature Store",
            "segment_static_features.parquet",
            (
                "Lưu PageRank, K-Means cluster, bậc vào/ra và Priors phủ kín 100% mạng lưới cơ giới (84,206 đoạn, "
                "gồm 74,211 Cold-Start) + vmax_lookup.json."
            ),
        ],
        [
            "Zone F (Storage/UI)",
            "[C14] Medallion Lakehouse",
            "Partitioned Apache Parquet",
            (
                "Lưu trữ phân tầng cột Bronze / Silver / Gold Parquet chia 10 phân vùng tháng "
                "(year_month=2020-07..2021-04) hỗ trợ Partition & Column Pruning."
            ),
        ],
        [
            "Zone F (Storage/UI)",
            "[C15] Quarantine & WAL",
            "quarantine/ · checkpoints/",
            (
                "Lưu vết 580 đoạn & 151 pings lỗi (quarantine/), nhật ký hồi phục sự cố Write-Ahead Log "
                "(checkpoints/) và báo cáo đối soát."
            ),
        ],
        [
            "Zone F (Storage/UI)",
            "[C16] MongoDB 8.0",
            "mongo:8.0.34 (traffic_scores)",
            (
                "Nhận 33,394 docs qua 4 partition (69 batches) UpdateOne(upsert=True) kèm atomic $status_version "
                f"pipeline guard & IXSCAN ({gt['mongo_med']} / p95 {gt['mongo_p95']})."
            ),
        ],
        [
            "Zone F (Storage/UI)",
            "[C17] TOC Command Center",
            "ReactJS + FastAPI (:8000)",
            (
                "Trung tâm Điều hành ReactJS + FastAPI (:8000) + Leaflet 5-Tab: Command Center +30m, Live Kafka & "
                "33 KiB Sketches, PageRank & Rules, 6 Cụm K-Means Cold-Start, Spark 3.5.7 / Ablation / Recon."
            ),
        ],
    ]
    story.append(
        make_table(
            [
                "Phân Vùng (Zone)",
                "Mã & Tên Component",
                "Runtime / Tệp Thực Thi",
                "Trách Nhiệm Kiến Trúc & Hợp Đồng Input / Output",
            ],
            comp_rows,
            [74, 102, 98, 241],
            s,
        )
    )
    story.append(
        Paragraph(
            "Bảng 3.1a. Danh mục đầy đủ 17 thành phần kiến trúc ([C1]–[C17]) thuộc 6 phân vùng (Zones A–F) trên "
            "Hình 3.1; "
            "bản sao độ phân giải cao lưu tại outputs/slide_charts/arch_detailed_reference.png.",
            s["caption"],
        )
    )
    story.append(Spacer(1, 3))

    flow_rows = [
        [
            "(1) Telemetry Stream",
            "[C1] Producer → [C4] Kafka 4.1",
            (
                "producer.py phát 33,727 sự kiện (33,394 cửa sổ cơ giới + 333 trùng + 500 trễ > 30m) hoặc 91,694 "
                "pings thô vào hcmc-traffic-events."
            ),
        ],
        [
            "(2) GIS Ingestion",
            "[C2] GIS Topology → [C5] Funnel",
            (
                "Các bảng bản đồ tĩnh (nodes, streets, segments) đi vào [C5] Quality Funnel để chuẩn hóa Unicode NFC "
                "và thực hiện 3-Tier Velocity Imputation."
            ),
        ],
        [
            "(3) Dual Kafka Feed",
            "[C4] Kafka → (3a) [C6] & (3b) [C5]",
            (
                "(3a) Luồng trực tuyến đi vào [C6] Watermark (30m); (3b) Luồng bản tin thô cấp xuống [C5] Quality "
                "Funnel để kiểm định ANSI try_cast."
            ),
        ],
        [
            "(4) Funnel & Quarantine",
            "[C5] → (4a) [C9] & (4b) [C15]",
            (
                "(4a) Bản ghi hợp lệ + bảng đường đã chuẩn hóa NFC đi sang [C9] Combiner & Join; (4b) Đường nét đứt "
                "cam cô lập bản ghi lỗi vào [C15] Quarantine."
            ),
        ],
        [
            "(5) O(1) Stream Sketches",
            "[C6] → [C7] Sketches → [C8] Scorer",
            (
                "Các cửa sổ trong 30m Watermark (98.50%) đi qua [C7] (32.0 KiB Bloom Filter, 1.0 KiB HLL++, EWMA "
                "Hotspot) rồi chuyển sang [C8] Online Scorer."
            ),
        ],
        [
            "(6) Lakehouse & WAL Bus",
            "[C6]→[C15]→[C14]; [C9/10]→[C14]→[C12]",
            (
                "(6a) [C6] ghi WAL xuống [C15] và append 100% raw events vào [C14]; (6b) [C9]–[C10] ghi Gold Parquet "
                "xuống [C14]; (6c) [C14] cấp 100% raw events lên [C12]."
            ),
        ],
        [
            "(7) Phase A Batch Fit",
            "[C9] → [C10] → [C11] → [C13a/b]",
            (
                "Phase A (t <= T_train): [C9] cấp cửa sổ sạch cho [C10] (PageRank, FPGrowth, 7D K-Means) -> [C11] huấn "
                "luyện Random Forest 23D -> đóng băng [C13a] & [C13b]."
            ),
        ],
        [
            "(8) Read-Only Transform",
            "[C13a] & [C13b] ⇢ [C8] & [C12]",
            (
                "Phase B & Phase C: [C13a] PipelineModel và [C13b] Static Feature Store cấp .transform() chỉ đọc (kèm "
                "kiểm tra SHA-256) cho [C8] Scorer và [C12] Recon."
            ),
        ],
        [
            "(9) Idempotent Upsert",
            "[C8] & [C12] → [C16] MongoDB 8.0",
            (
                "Ghi lô 500 bản ghi bằng UpdateOne(upsert=True): [C8] ghi STREAM_PROVISIONAL (v1); [C12] ghi đè "
                "BATCH_VERIFIED (v2) có atomic $status_version pipeline guard."
            ),
        ],
        [
            "(10) IXSCAN BI Gateway",
            "[C16] + [C14] + [C15] → [C17] → [C3]",
            (
                f"[C16] MongoDB (IXSCAN warm {gt['mongo_med']} / p95 {gt['mongo_p95']}), [C14] Lakehouse & [C15] Audit "
                "cấp dữ liệu cho [C17] ReactJS + FastAPI TOC (:8000) phục vụ [C3] Users."
            ),
        ],
    ]
    story.append(
        make_table(
            ["Bước (Step)", "Luồng Kết Nối (Source → Target)", "Hợp Đồng Giao Tiếp Dữ Liệu & Cơ Chế Thực Thi"],
            flow_rows,
            [82, 128, 305],
            s,
        )
    )
    story.append(
        Paragraph(
            (
                "Bảng 3.1b. Giải thích chi tiết 10 luồng dữ liệu đánh số (Steps 1–10) kết nối giữa 17 thành phần trên "
                "Hình 3.1."
            ),
            s["caption"],
        )
    )

    story.append(
        KeepTogether(
            [
                Paragraph("3.2. Zero-Conflict 3-Phase MLOps Lifecycle &amp; Khóa Chữ Ký Toàn Vẹn SHA-256", s["h2"]),
                build_mlops_drawing(),
                Paragraph(
                    "Hình 3.2. Tách biệt rạch ròi quyền huấn luyện (chỉ học một lần ở Phase A trên 1,885 organic TRAIN "
                    "≤ 2020-12-12T08:00:00Z sau tách 7,192 burst) và quyền chấm điểm chỉ đọc (.transform) Phase B, C. "
                    f"Ba chữ ký SHA-256 khóa cố định: <b>model.joblib = {gt['model_sha12']}...</b> · "
                    f"<b>segment_static_features.parquet = {gt['static_sha12']}...</b> · "
                    f"<b>vmax_lookup.json = {gt['vmax_sha12']}...</b>",
                    s["caption"],
                ),
            ]
        )
    )

    story.append(
        Paragraph(
            "3.3. Bước 1B — 5-Tuple Commutative Monoid Combiner &amp; 2-Stage Key Salting Chống Data Skew", s["h2"]
        )
    )
    story.append(
        make_equation_box(
            CHART_DIR / "eq_monoid.png",
            (
                "<b>Vì sao không tính trung bình từng phần rồi cộng lại?</b> Trên 3 phân vùng máy tính có số bản tin "
                "lệch nhau [1, 2, 3], phép lấy trung bình cục bộ rồi chia trung bình lần hai gây sai số tới "
                "<b>11.111111 km/h</b>. Ngược lại, khi nâng mỗi bản tin lên <b>5-Tuple Commutative Monoid</b> "
                "<i>m = (count, sum_v, min_v, max_v, severe_cnt)</i>, kết quả gộp trên Spark khớp 100% với "
                "<i>baseline.py</i> trên toàn bộ 33,394 cửa sổ cơ giới (42,124 bản tin kẹt, sai số = <b>0.0</b>). Kết "
                "hợp kỹ thuật <b>2-Stage Key Salting</b> (8 buckets, 59,655 nhóm trung gian ở pipeline / 59,714 ở "
                "Spark runtime), độ lệch tải (<i>Data Skew</i>) giờ cao điểm giảm mạnh từ "
                "<b>684.95x xuống còn 1.020x (CV = 0.0059)</b>."
            ),
            s,
        )
    )

    story.append(
        Paragraph(
            "3.4. Tối Ưu Hóa Catalyst BroadcastHashJoin &amp; Partitioned Medallion Lakehouse (Parquet)",
            s["h2"],
        )
    )
    spark_rows = [
        [
            "2-Stage Key Salting (8 Buckets)",
            (
                "Stage 1 gộp cục bộ theo (window_key, salt_bucket 0..7) tạo 59,714 nhóm Spark; Stage 2 bỏ salt gộp về "
                "33,394 cửa sổ."
            ),
            (
                f"Kéo lệch slot 684.95x → 1.020x (CV=0.0059); trên tập mẫu 90.8k pings, phép gộp 2 tầng thêm chi phí "
                f"{gt['monoid_unsalted_ms']} ms (IQR {gt['monoid_unsalted_iqr']}) → {gt['monoid_salted_ms']} ms "
                f"(IQR {gt['monoid_salted_iqr']}) (exceptAll diff = 0)."
            ),
        ],
        [
            "BroadcastHashJoin (spark_engine.py)",
            "Đối chiếu SortMergeJoin (autoBroadcast=-1) với BroadcastHashJoin trên 90,787 pings × segments × streets.",
            (
                "Loại bỏ SortMergeJoin (0 nút), giảm Shuffle Exchanges từ 5 xuống 1 (-80%), rút ngắn thời gian "
                f"{gt['join_base_ms']} ms (IQR {gt['join_base_iqr']}) → {gt['join_opt_ms']} ms "
                f"(IQR {gt['join_opt_iqr']}) (+{gt['join_speedup']}; diff=0)."
            ),
        ],
        [
            "Structured Streaming & Watermark",
            "Sink 1 WAL Bronze Parquet (200 rows) & Sink 2 hcmc_agg_sink2 (3 micro-batches, stateStoreSave).",
            (
                "Watermark thật dịch tới 2020-12-01T12:30:00Z, loại 10 rows hết hạn khỏi State Store "
                "(numRowsRemoved = 10, WAL 0 dupes)."
            ),
        ],
        [
            "Parquet Partition & Column Pruning",
            (
                "Lưu 10 phân vùng tháng year_month=2020-07..2021-04 (48 · 7,268 · 204 · 13 · 5,002 · 15,102 · "
                "2,790 · 1 · 1,737 · 1,229 dòng)."
            ),
            (
                f"Truy vấn lọc tháng 2020-08 chỉ đọc đúng {gt['aug_rows']} dòng (thay vì quét cả 33,394 dòng) và "
                "chỉ nạp 4 / 60 cột cần dùng."
            ),
        ],
    ]
    story.append(
        make_table(
            ["Kỹ thuật Phân tán Spark", "Cấu hình Thực thi Vật lý (Physical Plan)", "Bằng chứng Đo đạc Thực tế"],
            spark_rows,
            [122, 183, 210],
            s,
        )
    )
    story.append(Spacer(1, 4))
    _add_centered_figure(
        story,
        CHART_DIR / "chart_spark_skew_catalyst.png",
        0.62,
        "Hình 3.3. Kết quả cân bằng tải giờ cao điểm bằng 2-Stage Key Salting (trên) và loại bỏ Shuffle dư thừa "
        "nhờ Catalyst BroadcastHashJoin trên Spark 3.5.7 (dưới).",
        s,
    )
    story.append(
        Paragraph(
            "3.5. Hạn Chế &amp; Rủi Ro Thành Phần Kiến Trúc &amp; Xử Lý Lô Spark (Component Limitations &amp; Risks)",
            s["h2"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Limitations &amp; Risks (Mục 3 System Architecture &amp; Mục 7A Spark Batch Processing):</b> "
                "<b>(1) Tranh chấp tài nguyên trên máy chủ đơn (Single-Host Resource Contention):</b> Việc đóng gói "
                "5 container mặc định (Kafka 1.5 GB, Spark 6.0 GB, MongoDB 2.0 GB, ReactJS + FastAPI TOC :8000 1.0 GB, "
                "live-consumer 0.5 GB = 11.0 / 16.0 GB RAM) trên một máy có rủi ro cạnh tranh I/O đĩa khi Spark "
                "Shuffle cùng lúc MongoDB ghi. "
                "<b>(2) Độ trễ nhất quán cuối cùng (Eventual Consistency Gap):</b> Nhánh <i>Phase B (Speed Layer)</i> "
                "phát cảnh báo tạm thời <i>v1</i> tức thì, nhưng các bản tin đến muộn quá 30 phút chỉ được cập nhật "
                "vào biến trễ lịch sử sau khi tác vụ <i>Phase C (Batch Reconciliation)</i> hoàn tất vào cuối chu kỳ. "
                "<b>(3) Chi phí nhân bản khóa của 2-Stage Salting (Overhead Tradeoff):</b> Chia 8 giỏ muối giúp khử "
                "lệch tải giờ cao điểm (684.95x → 1.020x, CV = 0.0059), nhưng ở dữ liệu mẫu 90.8k pings phép gộp 2 "
                f"tầng thêm chi phí ({gt['monoid_unsalted_ms']} ms / IQR {gt['monoid_unsalted_iqr']} → "
                f"{gt['monoid_salted_ms']} ms / IQR {gt['monoid_salted_iqr']}) do phát sinh Shuffle thứ hai."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch4_graph_patterns_and_kmeans(story: list, s: dict[str, ParagraphStyle]) -> None:
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "Chương 4. Bước 2 — Lấp Đầy 'Vùng Mù' 88.13% Đường Thiếu GPS Bằng Đồ Thị PageRank &amp; Phân Cụm K-Means",
            s["h1"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Tại sao cần Chương 4 trước khi xây mô hình dự báo ở Chương 5? (Sợi dây liên kết nhân quả):</b> "
                "Sau Chương 2–3, chúng ta mới chỉ biết trạng thái giao thông trên 9,995 đoạn đường cơ giới có GPS, "
                "còn <b>74,211 đoạn đường nhánh (88.13% thành phố) hoàn toàn trắng dữ liệu</b>. Chương 4 giải quyết "
                "khoảng trống này với <b>hai mục đích gắn kết chặt chẽ</b>: <b>(1) Phục vụ điều hành trực tiếp:</b> "
                "chỉ ra các nút cổ chai tự nhiên trên bản đồ cho Sở GTVT lắp camera và tìm các cặp đường hay kẹt dây "
                "chuyền; <b>(2) Tạo biến đầu vào không gian cho Chương 5:</b> trích xuất điểm nút cổ chai "
                "(<i>pagerank_score</i>), số nhánh vào/ra và mã nhóm hành vi (<i>cluster_id</i>) cho toàn bộ 84,206 "
                "đoạn đường cơ giới để truyền thẳng vào mô hình <b>Random Forest</b> ở Chương 5."
            ),
            s,
        )
    )
    story.append(
        Paragraph("4.1. Lọc Bỏ Điểm Uốn Cong Trung Gian &amp; Xếp Hạng Nút Cổ Chai Bằng Đồ Thị PageRank", s["h2"])
    )
    story.append(
        Paragraph(
            (
                "Trong 577,967 điểm tọa độ của bảng <i>nodes.csv</i> trích từ bản đồ OpenStreetMap, có tới "
                "<b>525,786 điểm (90.97%) chỉ là các điểm hình học nằm dọc thân đường để vẽ độ uốn cong</b> "
                "(<i>shape points</i>), hoàn toàn không phải ngã rẽ giao lộ. Hệ thống lọc bỏ toàn bộ các điểm uốn cong "
                "trung gian này và giữ lại đúng <b>52,181 giao lộ thực sự</b>. Tiếp đó, hệ thống dựng <b>đồ thị nối "
                "đoạn đường có hướng (loại bỏ U-turn)</b>: mỗi đoạn trong <b>84,206 đoạn cơ giới</b> là 1 đỉnh; hai "
                "đoạn nối với nhau khi điểm cuối đoạn trước trùng điểm đầu đoạn sau, tạo thành <b>92,453 hướng rẽ hợp "
                "lệ</b> và <b>314 đoạn cụt</b> không có lối ra, rồi chạy 25 vòng lặp PageRank (hệ số cản β = 0.85):"
            ),
            s["body"],
        )
    )
    story.append(
        make_equation_box(
            CHART_DIR / "eq_pagerank.png",
            "<b>Bảo toàn tổng điểm PageRank toàn mạng lưới:</b> Nhờ chia đều lại xác suất của 314 đoạn cụt "
            "ở mỗi vòng lặp, tổng điểm PageRank trên toàn bộ 84,206 đoạn cơ giới bảo toàn đúng bằng <b>84,206.0</b>.",
            s,
        )
    )
    story.append(Spacer(1, 4))

    pr_rows = [
        [
            "#1",
            "21969",
            "Nguyễn Kiệm",
            "Cấp 2",
            "3 → 1",
            "2.7799",
            "Nút cổ chai chụm 3 nhánh vào 1 hướng về vòng xoay Tân Sơn Nhất (Observed)",
        ],
        [
            "#2",
            "37345",
            "Cầu vượt Ngã tư Gò Dưa",
            "Cấp 3",
            "2 → 1",
            "2.7196",
            "Nút thắt cửa ngõ cầu vượt Quốc lộ 1 – Thủ Đức (Cold-Start, 0 GPS)",
        ],
        [
            "#3",
            "34723",
            "Quốc lộ 1",
            "Cấp 1",
            "2 → 1",
            "2.6894",
            "Trục vành đai liên tỉnh chịu tải xe vận tải hàng hóa lớn (Cold-Start)",
        ],
        [
            "#4",
            "40948",
            "Quốc lộ 1",
            "Cấp 1",
            "2 → 1",
            "2.6516",
            "Nhánh gom tuyến vành đai phía Đông – Thủ Đức",
        ],
        [
            "#5",
            "81815",
            "Song Hành Xa lộ Hà Nội",
            "Cấp 4",
            "2 → 2",
            "2.6508",
            "Hành lang gom lưu lượng khu Đông dọc Xa lộ Hà Nội (Observed)",
        ],
        [
            "#6",
            "41134",
            "Dương Bá Trạc",
            "Cấp 2",
            "2 → 1",
            "2.6435",
            "Đoạn dẫn qua cầu Kênh Xáng kết nối Quận 8 vào trung tâm Quận 5",
        ],
        [
            "#7",
            "15011",
            "Phan Đăng Lưu",
            "Cấp 2",
            "2 → 2",
            "2.6159",
            "Trục huyết mạch quận Phú Nhuận – Bình Thạnh hướng Ngã tư Hàng Xanh",
        ],
        [
            "#8",
            "40916",
            "Quốc lộ 1",
            "Cấp 1",
            "2 → 1",
            "2.5677",
            "Đoạn nối vành đai tập trung lưu lượng liên quận cửa ngõ Tây – Bắc",
        ],
        [
            "#9",
            "30616",
            "Quốc Lộ 50",
            "Cấp 3",
            "2 → 1",
            "2.5502",
            "Trục kết nối Bình Chánh – Quận 8 qua cửa ngõ Tây Nam",
        ],
        [
            "#10",
            "51321",
            "Hoàng Văn Thụ",
            "Cấp 2",
            "3 → 2",
            "2.5432",
            "Hành lang cửa ngõ sân bay Tân Sơn Nhất (chụm 3 nhánh vào 2)",
        ],
    ]
    story.append(
        make_table(
            [
                "Hạng",
                "Mã Đoạn",
                "Tuyến Đường",
                "Cấp",
                "Vào/Ra",
                "PageRank",
                "Ý nghĩa Cấu trúc Giao thông Thực tế (UC-3)",
            ],
            pr_rows,
            [36, 44, 88, 36, 44, 52, 215],
            s,
        )
    )

    story.append(
        Paragraph("4.2. Khai Phá Các Cặp Đường Hay Kẹt Cùng Lúc (SON / FPGrowth) &amp; Lưu Ý Cỡ Mẫu Nhỏ", s["h2"])
    )
    story.append(
        Paragraph(
            (
                "Mỗi khung 30 phút tạo một giỏ gồm tất cả tuyến đường cơ giới đã chuẩn hóa dấu tiếng Việt NFC "
                "đang bị kẹt cứng (<i>LOS E/F</i>; đã loại R3 Phường/Quận). Trên <i>Apache Spark 3.5.7</i>, thuật toán "
                "<b>FPGrowth</b> tìm ra <b>32 luật kết hợp (trên tập organic TRAIN)</b>; thuật toán <b>2-Pass SON</b> "
                "trích xuất <b>12 luật kẹt đồng thời</b> hai chiều và <b>14 luật kẹt nối tiếp sau 30 phút</b> (đã kiểm "
                "tính tự trễ <i>persistence_controlled = True</i> để tránh trùng lặp quán tính cùng tuyến):"
            ),
            s["body"],
        )
    )
    rule_rows = [
        [
            "Đồng thời (t ↔ t)",
            "Nguyễn Thị Minh Khai ↔ Cống Quỳnh (9 cửa sổ)",
            "0.0292",
            "69.2% / 64.3%",
            "15.2308x",
            "Cụm ngã tư trung tâm Quận 1 – Quận 3",
        ],
        [
            "Đồng thời (t ↔ t)",
            "Cống Quỳnh ↔ Trần Hưng Đạo (10 cửa sổ)",
            "0.0325",
            "71.4% / 55.6%",
            "12.2222x",
            "Nút giao bệnh viện Từ Dũ – trục Trần Hưng Đạo Q.1",
        ],
        [
            "Đồng thời (t ↔ t)",
            "Nguyễn Thị Minh Khai ↔ Trần Hưng Đạo (9 cửa sổ)",
            "0.0292",
            "69.2% / 50.0%",
            "11.8462x",
            "Cặp hành lang song song dồn lưu lượng vào Q.1",
        ],
        [
            "Đồng thời (t ↔ t)",
            "Nguyễn Đình Chiểu ↔ Trần Hưng Đạo (8 cửa sổ)",
            "0.0260",
            "66.7% / 44.4%",
            "11.4074x",
            "Hai trục thương mại đông đúc Q.3 ↔ Q.1",
        ],
        [
            "Trễ (t → t+30p)",
            "Lê Quang Định (t) → Trần Quang Khải (t+30p)",
            "0.0298",
            "71.4% (5 cửa sổ)",
            "24.0000x",
            "Giả thuyết dòng xe Bình Thạnh đổ vào Q.1",
        ],
        [
            "Trễ (t → t+30p)",
            "Lê Quang Định (t) → Trần Quốc Toản (t+30p)",
            "0.0298",
            "71.4% (5 cửa sổ)",
            "24.0000x",
            "Giả thuyết làn xe di chuyển qua Q.3 sau 30 phút",
        ],
        [
            "Trễ (t → t+30p)",
            "Lê Quang Định (t) → Trần Quốc Thảo (t+30p)",
            "0.0298",
            "71.4% (5 cửa sổ)",
            "20.0000x",
            "Giả thuyết dồn ứ hướng cầu Lê Văn Sỹ – Q.3",
        ],
        [
            "Trễ (t → t+30p)",
            "Nguyễn Thái Sơn (t) → Trần Quang Khải (t+30p)",
            "0.0298",
            "55.6% (5 cửa sổ)",
            "18.6667x",
            "Giả thuyết luồng xe từ Gò Vấp về Quận 1",
        ],
    ]
    story.append(
        make_table(
            [
                "Nhóm Luật",
                "Cặp Tuyến Đường Thực nghiệm",
                "Tần suất",
                "Độ tin cậy",
                "Hệ số Lift",
                "Ý nghĩa Tham khảo (UC-4)",
            ],
            rule_rows,
            [68, 166, 50, 68, 50, 113],
            s,
        )
    )
    story.append(Spacer(1, 4))
    story.append(
        make_callout(
            (
                "<b>Đánh giá trung thực về hạn chế cỡ mẫu nhỏ:</b> Các luật lan truyền sau 30 phút có hệ số Lift cao "
                "(tới 24.0000x khi đã kiểm soát persistence) nhưng chỉ cùng xuất hiện trong <b>5 đến 8 cửa sổ "
                "30 phút</b> (tương đương <b>2.5–4.0 giờ quan sát</b> trong suốt 10 tháng) do độ phủ GPS gốc thưa "
                "(11.87%). Với cỡ mẫu nhỏ như vậy, chưa thể loại trừ sự trùng hợp ngẫu nhiên giờ cao điểm chung. Vì "
                "thế, hệ thống chỉ xem 14 luật trễ này là <b>giả thuyết cảnh báo thăm dò</b> kết hợp đối chiếu trên "
                "bản đồ giao lộ chứ không dùng làm quy luật cứng."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )

    story.append(
        Paragraph(
            "4.3. Phân 6 Nhóm Hành Vi Đường Phố Bằng K-Means (Organic Train-Only) Phủ Kín 84,206 Đoạn Cơ Giới",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Thuật toán <b>StandardScaler + KMeans(k=6, seed=42)</b> chỉ học trên <b>3,609 đoạn đường thuộc tập "
                "organic TRAIN</b> (kích thước cụm trên TRAIN: 846 / 924 / 708 / 136 / 840 / 155) trong không gian "
                "7 đặc trưng chuẩn hóa, đạt <b>Silhouette = 0.2241</b>, <b>Davies-Bouldin = 1.3284</b> và "
                "<b>Calinski-Harabasz = 806.49</b>. Từ tâm cụm này, toàn bộ <b>84,206 đoạn cơ giới</b> (9,995 có GPS "
                "&amp; <b>74,211 đoạn Cold-Start</b>) được gán nhóm hành vi đầy đủ để phục vụ tra cứu đoạn tương đồng:"
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "map_hcmc_clusters_7d.png",
        0.52,
        "Hình 4.1. Bản đồ 6 nhóm hành vi đường phố (K-Means, k = 6) phủ kín 100% mạng lưới 84,206 đoạn đường cơ giới "
        "TP.HCM trên nền sông Sài Gòn và các quận.",
        s,
    )
    cl_rows = [
        [
            "Nhóm 0",
            "6,077",
            "30.66 km/h",
            "42.94 km/h",
            "29.06%",
            "0.9029",
            "1,880 / 11,631",
            "Đường nội đô hẹp, giới hạn tốc độ thấp (Vmax ~43 km/h), dễ ùn ứ",
        ],
        [
            "Nhóm 1",
            "8,718",
            "20.44 km/h",
            "50.29 km/h",
            "43.26%",
            "1.0097",
            "1,365 / 2,165",
            "Trục nội đô hay ùn ứ giờ cao điểm nhất (tỷ lệ kẹt E/F cao nhất 43.26%)",
        ],
        [
            "Nhóm 2",
            "9,400",
            "44.87 km/h",
            "51.07 km/h",
            "7.30%",
            "1.0086",
            "4,464 / 53,330",
            "Đường nhánh & hành lang thông thoáng rộng khắp (phủ 53,330 đoạn Cold-Start)",
        ],
        [
            "Nhóm 3",
            "454",
            "65.38 km/h",
            "50.54 km/h",
            "14.32%",
            "0.9924",
            "132 / 0",
            "Tuyến tốc độ cao / cửa ngõ lưu thông nhanh (100% có GPS, 132 đoạn)",
        ],
        [
            "Nhóm 4",
            "7,542",
            "33.51 km/h",
            "60.27 km/h",
            "19.80%",
            "1.1944",
            "1,797 / 4,434",
            "Trục chính & nút cổ chai PageRank cao nhất (TB mạng lưới 1.2948), Vmax cao",
        ],
        [
            "Nhóm 5",
            "1,203",
            "41.29 km/h",
            "59.96 km/h",
            "10.31%",
            "0.9880",
            "357 / 2,651",
            "Đoạn liên kết dài (~233.9m), tốc độ thiết kế cao, ít kẹt (E/F 10.31%)",
        ],
    ]
    story.append(
        make_table(
            [
                "Nhóm",
                "Cửa sổ",
                "Vận tốc TB",
                "Tốc độ Max",
                "Kẹt E/F",
                "PageRank",
                "Có / Thiếu GPS",
                "Đặc tính Hành vi Giao thông Thực tế (UC-5)",
            ],
            cl_rows,
            [48, 42, 50, 52, 42, 54, 78, 149],
            s,
        )
    )
    story.append(
        Paragraph(
            (
                "4.4. Hạn Chế &amp; Rủi Ro Thành Phần Đồ Thị PageRank, SON &amp; 7D K-Means (Component Limitations "
                "&amp; Risks)"
            ),
            s["h2"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Limitations &amp; Risks (Mục 7B Batch Graph Mining &amp; Cold-Start Clustering):</b> "
                "<b>(1) Cỡ mẫu nhỏ của các luật lan truyền trễ (+30m SON / FPGrowth):</b> Như đã phân tích ở Mục 4.2, "
                "14 luật kẹt nối tiếp sau 30 phút chỉ xuất hiện trong 5 đến 8 cửa sổ (2.5–4.0 giờ quan sát) do độ phủ "
                "GPS thưa; nếu áp dụng máy móc như quy luật cứng sẽ dẫn đến báo động giả vào giờ cao điểm. "
                "<b>(2) Trọng số chuyển tiếp đồng nhất trong Line-Graph PageRank:</b> Đồ thị đoạn đường (92,453 cạnh "
                "không U-turn) hiện chia đều xác suất rẽ tại mỗi giao lộ theo bậc ra (<i>1 / out_degree</i>) vì bản đồ "
                "OpenStreetMap thiếu dữ liệu đếm lưu lượng rẽ thực tế theo làn. "
                "<b>(3) Đặc trưng tĩnh trên vùng Cold-Start (74,211 đoạn):</b> Nhãn cụm <i>7D K-Means</i> cho 88.13% "
                "đoạn đường không có GPS dựa trên cấu trúc hình học và tốc độ tối đa suy diễn theo cấp đường, do đó "
                "chưa phân biệt được các đoạn hẻm có họp chợ tự phát hay rào chắn tạm thời."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch5_forecasting_and_ablation(story: list, s: dict[str, ParagraphStyle]) -> None:
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "Chương 5. Bước 3 — Mô Hình Random Forest Dự Báo Trước 30 Phút &amp; Đối Chứng Trực Diện Với Baseline",
            s["h1"],
        )
    )
    story.append(Paragraph("5.1. Ghép Cặp Quan Sát Cách Đúng 30 Phút &amp; Cắt Sạch Biên Thời Gian", s["h2"]))
    story.append(
        Paragraph(
            (
                "Do xe gửi GPS ngắt quãng (15,514 / 23,399 cặp liên tiếp cách nhau ≥ 60 phút = 66.30%), "
                "nếu dùng lệnh trượt dòng tuần tự <i>shift(-1)</i> thì mô hình sẽ học sai thời điểm. Hệ thống chỉ "
                "ghép nhãn bằng phép nối chính xác trên khóa <b>(segment_id, slot_ts + 30 phút)</b> thu được "
                "<b>7,885 cặp chuẩn xác</b>, đồng thời <b>loại 7,192 burst windows tổng hợp (3,596 cặp khỏi TRAIN)</b> "
                "và cắt các mẫu sát ranh giới có nhãn vắt sang tập sau:"
            ),
            s["body"],
        )
    )
    split_rows = [
        [
            "Giai đoạn Khởi tạo",
            "slot_ts ≤ 2020-08-02T23:30:00Z",
            "Cố định tâm cụm K-Means & tỷ lệ kẹt nền ban đầu",
            "Không rò rỉ dữ liệu sang TRAIN/VAL/TEST",
        ],
        [
            "Tập Organic TRAIN",
            "slot_ts ≤ 2020-12-12T08:00:00Z",
            "1,885 mẫu tự nhiên (cắt 50 biên & tách 3,596 burst)",
            "max(target_ts) = 2020-12-12T08:00:00Z",
        ],
        [
            "Tập VAL (15%)",
            "2020-12-12T08:00Z < slot_ts ≤ 2020-12-25T03:30:00Z",
            "1,167 mẫu hợp lệ (đã cắt 5 mẫu sát biên)",
            "max(target_ts) = 2020-12-25T03:30:00Z",
        ],
        [
            "Tập TEST (15%)",
            "slot_ts > 2020-12-25T03:30:00Z",
            "1,182 mẫu kiểm thử độc lập (185 mẫu kẹt LOS E/F)",
            "Chỉ chấm điểm đúng 1 lần sau khi chốt mô hình",
        ],
    ]
    story.append(
        make_table(
            ["Tập Dữ liệu", "Điều kiện Mốc Thời gian (UTC)", "Quy mô Sau Khi Lọc & Cắt Biên", "Bảo chứng Chống Rò rỉ"],
            split_rows,
            [82, 168, 152, 113],
            s,
        )
    )

    story.append(Paragraph("5.2. Tỷ Lệ Kẹt Tích Lũy Quá Khứ &amp; Tổ Hợp 23 Biến Đầu Vào Của Mô Hình", s["h2"]))
    story.append(
        make_equation_box(
            CHART_DIR / "eq_prior.png",
            "<b>Chứng minh không nhìn trộm tương lai:</b> Tỷ lệ kẹt lịch sử của từng đoạn đường và tuyến đường chỉ "
            "cộng dồn trên các khung giờ quá khứ chặt τ &lt; t; bài kiểm thử tự động đảo trộn ngẫu nhiên toàn bộ tương "
            "lai τ &gt; t xác nhận cả 23 biến đầu vào tại thời điểm t giữ nguyên tuyệt đối (<b>sai lệch = 0.0</b>).",
            s,
        )
    )
    story.append(Spacer(1, 4))
    feat_rows = [
        [
            "1. Trạng thái hiện tại &amp; lịch sử gần (7 biến · 78.13%)",
            "mean_velocity (0.4635), current_los_ordinal (0.1756), velocity_ratio (0.1064), obs_count (0.0142), "
            "lag1_los_ordinal (0.0120), lag2_los_ordinal (0.0074), trailing_severe_rate (0.0022)",
        ],
        [
            "2. Chu kỳ giờ Việt Nam &amp; hình học đường (7 biến · 11.37%)",
            "period_sin (0.0425), period_cos (0.0189), max_velocity (0.0175), gap_minutes (0.0155), length_m (0.0109), "
            "weekday (0.0076), street_level (0.0008)",
        ],
        [
            "3. Đặc trưng bản đồ từ Bước 2 &amp; tỷ lệ kẹt quá khứ (9 biến · 10.50%)",
            "pagerank_score (0.0277), neighbor_los_ordinal (0.0235), street_causal_prior_los (0.0214), "
            "segment_train_severe_prior (0.0101), segment_train_prior_los (0.0091), street_causal_prior_sev (0.0080), "
            "cluster_id (0.0046), in_degree (0.0003), out_degree (0.0002)",
        ],
    ]
    story.append(
        make_table(
            ["Nhóm Biến Đầu vào (Tổng = 23 biến)", "Danh sách Biến & Mức đóng góp trong Random Forest"],
            feat_rows,
            [154, 361],
            s,
        )
    )

    story.append(
        Paragraph(
            "5.3. Tuyển Chọn Qua Live VAL Composite Gate, Tách Burst, Bảng 7 Cấu Hình &amp; Ma Trận 6 Mức",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Hệ thống thử nghiệm 7 cấu hình ứng viên trên <b>1,885 mẫu organic TRAIN</b> (đối chứng ablation tách "
                "burst: kèm burst 5,481 mẫu đạt TEST Acc = 0.9399 / Severe F1 = 0.9344 so với chỉ giữ organic 1,885 "
                "mẫu đạt Acc = 0.9255 / Severe F1 = 0.9362) và dùng <b>Live VAL Composite Gate = (val_acc + val_pr "
                "+ val_roc) / 3</b> trên <b>VAL (1,167 mẫu)</b>: <b>#5 Random Forest (23 biến)</b> thắng ở "
                "<b>0.8882</b> (VAL Acc = 0.8166, VAL PR-AUC = 0.8912, VAL ROC-AUC = 0.9567, VAL Severe F1 = 0.8912, "
                "τ_severe = 0.25, τ_change = 0.40, τ_onset = 0.25) vượt <b>DT 23D (0.8812)</b>, <b>DT 14D (0.8115)</b> "
                "và <b>RF 14D (0.6794)</b> nên được chọn làm mô hình vận hành duy nhất."
            ),
            s["body"],
        )
    )
    ab_rows = [
        [
            "1. Baseline (Đoán LOS(t+30p) = LOS(t) hiện tại)",
            "1 biến",
            "0.9484",
            "0.5654",
            "0.9365",
            "0.9568",
            "0.9465",
            "0.9867",
            "0.9367",
        ],
        [
            "2. Cây quyết định Decision Tree (Chỉ biến cục bộ)",
            "14 biến",
            "0.8697",
            "0.4226",
            "0.7106",
            "0.9027",
            "0.7952",
            "0.9767",
            "0.9198",
        ],
        [
            "3. Cây quyết định Decision Tree (+PageRank &amp; K-Means)",
            "23 biến",
            "0.8790",
            "0.3856",
            "0.7479",
            "0.9459",
            "0.8353",
            "0.9853",
            "0.9479",
        ],
        [
            "4. Random Forest (Chỉ biến cục bộ &amp; giờ VN)",
            "14 biến",
            "0.9222",
            "0.3976",
            "0.9355",
            "0.9405",
            "0.9380",
            "0.9881",
            "0.9554",
        ],
        [
            "5. Random Forest (+PageRank, Tỷ lệ quá khứ &amp; K-Means) [Chọn]",
            "23 biến",
            "0.9255",
            "0.4253",
            "0.9215",
            "0.9514",
            "0.9362",
            "0.9894",
            "0.9543",
        ],
        [
            "6. GBT Nhị phân (Chỉ báo kẹt cứng E/F, biến cục bộ)",
            "14 biến",
            "N/A",
            "N/A",
            "0.9267",
            "0.9568",
            "0.9415",
            "0.9751",
            "0.9488",
        ],
        [
            "7. GBT Nhị phân (+PageRank, Tỷ lệ quá khứ &amp; K-Means)",
            "23 biến",
            "N/A",
            "N/A",
            "0.9227",
            "0.9676",
            "0.9446",
            "0.9830",
            "0.9498",
        ],
    ]
    story.append(
        make_table(
            [
                "Cấu hình Thử nghiệm (Mô hình vận hành chính: #5 RF 23 biến)",
                "Số biến",
                "Đúng 6 mức",
                "Macro F1",
                "Prec Kẹt",
                "Rec Kẹt",
                "F1 Kẹt",
                "ROC AUC",
                "PR AUC",
            ],
            ab_rows,
            [171, 40, 46, 44, 43, 43, 42, 43, 43],
            s,
            highlight_row=5,
        )
    )
    story.append(Spacer(1, 4))

    cm_rows = [
        ["Thực tế LOS A (959 · 81.13%)", "944", "7", "0", "4", "1", "3"],
        ["Thực tế LOS B (10 · 0.85%)", "3", "3", "2", "0", "0", "2"],
        ["Thực tế LOS C (15 · 1.27%)", "6", "0", "1", "5", "0", "3"],
        ["Thực tế LOS D (13 · 1.10%)", "2", "5", "3", "3", "0", "0"],
        ["Thực tế LOS E (20 · 1.69%)", "1", "1", "1", "6", "5", "6"],
        ["Thực tế LOS F (165 · 13.96%)", "1", "2", "1", "6", "17", "138"],
    ]
    story.append(
        KeepTogether(
            [
                make_table(
                    [
                        "Ma trận Phân loại 6 Mức (RF 23 biến · TEST)",
                        "Dự báo A",
                        "Dự báo B",
                        "Dự báo C",
                        "Dự báo D",
                        "Dự báo E",
                        "Dự báo F",
                    ],
                    cm_rows,
                    [165, 58, 58, 58, 58, 59, 59],
                    s,
                ),
                Paragraph(
                    "Bảng 5.4. Kết quả phân loại từng mức LOS A–F trên 1,182 mẫu TEST của Random Forest (23 biến): "
                    "đường chéo chính [944, 3, 1, 3, 5, 138] nhận diện đủ cả 6 lớp (Bootstrap 95% CI: ROC-AUC "
                    "[0.9824, 0.9955], Severe F1 [0.9091, 0.9602], PR-AUC diff [-0.0029, 0.0444]); Weighted-F1 = "
                    "0.9297, Macro-F1 = 0.4253 do lớp B–E rất thưa (10–20 mẫu).",
                    s["caption"],
                ),
            ]
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_ablation_t30m.png",
        0.58,
        "Hình 5.1. So sánh hiệu năng cảnh báo kẹt cứng LOS E–F và phân loại 6 mức LOS A–F giữa các ứng viên trên "
        "tập TEST.",
        s,
    )

    story.append(
        Paragraph(
            "5.4. So Sánh Trực Diện Giữa Baseline Và Random Forest (23 Biến) Khi Giao Thông Thay Đổi Trạng Thái",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Nhìn vào bảng tổng gộp 1,182 mẫu kiểm thử ở trên, cách đoán đơn giản <b>Baseline</b> (hiện tại đường "
                "đang ở mức nào thì đoán 30 phút nữa giữ nguyên mức đó) đạt độ chính xác 6 mức tới <b>94.84%</b> "
                "(so với 92.55% của Random Forest sau khi tách chuỗi burst khỏi tập huấn luyện). Nguyên nhân rất rõ "
                "ràng: <b>94.84% thời gian (1,121 / 1,182 mẫu) đoạn đường giữ nguyên trạng thái sau 30 phút</b> "
                "(riêng đường thông thoáng mức A đã chiếm 81.13%). Tuy nhiên, ở <b>5.16% thời điểm giao thông thực sự "
                "chuyển trạng thái</b> (61 trường hợp từ thoáng sang đông/kẹt hoặc ngược lại), khi tín hiệu GPS bị "
                "đứt quãng, và trên <b>88.13% đoạn đường thiếu GPS</b>, phép so sánh trực diện 1-đối-1 dưới đây chỉ "
                "rõ ưu thế có ý nghĩa thống kê của mô hình chính <b>Random Forest (23 biến)</b>:"
            ),
            s["body"],
        )
    )
    tr_rows = [
        [
            "1. Nhóm đổi trạng thái\nLOS(t) ≠ LOS(t+30p)\n(61 / 1,182 mẫu TEST)",
            "Đoán đúng mức mới: 0 / 61 (0.00%)\nDetect biến động ROC-AUC = 0.5000\n"
            "PR-AUC kẹt cứng = 0.5408\nSai số xác suất Brier = 0.3279",
            "Detect biến động: ROC-AUC = 0.8822 (PR-AUC = 0.4009; vs DT 0.8511)\n"
            "Đoán đúng mức mới: 14 / 61 (22.95% ở ngưỡng VAL τ=0.40, tổng thể 90.19%; mặc định 10/61 = 16.39%)\n"
            "Top-1 hướng mới: 19 / 61 (31.15%) · PR-AUC = 0.5556 · Brier = 0.2811",
            "Baseline mù 100% (0/61); RF vượt trội có ý nghĩa thống kê (McNemar 10 vs 0, p = 0.001953) & giảm "
            "14.3% sai số xác suất Brier.",
        ],
        [
            "2. Đường đang thoáng (A–D)\nđột ngột hóa kẹt cứng (E–F)\n(chỉ có 8 / 993 mẫu TEST)",
            "Bắt trúng ở ngưỡng cứng: 0 / 8 (0.00%)\nGán xác suất kẹt = 0% đồng loạt cho mọi đoạn đường mức A–D",
            "Bắt trúng ở ngưỡng cứng mặc định: 0 / 8 (0.00% do cỡ mẫu quá hiếm 0.81%)\n"
            "Xếp hạng rủi ro theo xác suất: Onset ROC-AUC = 0.8438; xác suất cảnh báo tăng 3.26x "
            "(9.40% so với 2.88% đường thoáng ổn định)",
            "Ghi nhận trung thực: với vỏn vẹn 8 ca đột ngột trong TEST, ngưỡng cứng chưa bắt kịp, nhưng xác "
            "suất rủi ro của Random Forest tăng 3.26x.",
        ],
        [
            "3. Tín hiệu GPS ngắt quãng\ncách nhau trên 30 phút\n(3,605 mẫu · 49.21% đổi mức)",
            "PR-AUC kẹt cứng = 0.5732\nROC-AUC = 0.7940\n(quán tính cũ mất tác dụng khi ngắt quãng lâu)",
            "PR-AUC kẹt cứng = 0.6405 (+6.73 điểm)\nROC-AUC = 0.7913\n"
            "(chu kỳ giờ VN + PageRank + K-Means)",
            "Khi xe gửi tín hiệu thưa (chiếm 66.30% thực tế), các biến chu kỳ giờ Việt Nam + đồ thị giúp RF vượt "
            "xa Baseline về PR-AUC (+6.73%).",
        ],
        [
            "4. Vùng mù thiếu cảm biến\ntrên toàn mạng lưới TP.HCM\n(74,211 / 84,206 đoạn cơ giới)",
            "Độ phủ dự báo = 11.87% (Cold-Start = 0.0%)\n(không thể chạy vì không có LOS hiện tại trên đường)",
            "Độ phủ dự báo = 100.0% (đủ 84,206 đoạn cơ giới)\n"
            "Masked-GPS Proxy: 6-Class Acc = 58.63%, ROC-AUC = 0.7000, PR-AUC = 0.5152",
            "Chỉ duy nhất Random Forest (23 biến) mới chấm điểm rủi ro được cho 88.13% mạng lưới đường nhánh "
            "chưa có xe gửi GPS.",
        ],
    ]
    story.append(
        KeepTogether(
            [
                make_table(
                    [
                        "Tình huống Kiểm định Thực tế",
                        "Baseline (Giữ nguyên hiện tại)",
                        "Mô hình chính: Random Forest (23 biến)",
                        "Kết luận Thực tiễn & Ghi nhận Khoa học",
                    ],
                    tr_rows,
                    [102, 118, 152, 143],
                    s,
                ),
                Paragraph(
                    "Bảng 5.5. Đối chứng trực diện 1-đối-1 giữa Baseline và mô hình duy nhất được lựa chọn trên Live "
                    "VAL Gate (Random Forest 23 biến). Mọi giới hạn thực nghiệm (10/61 mặc định → 14/61 ở τ=0.40, "
                    "McNemar p=0.001953, Hard Recall 0/8 ca khởi phát hiếm & Macro-F1 = 0.4253) "
                    "được báo cáo minh bạch.",
                    s["caption"],
                ),
            ]
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_transition_analysis.png",
        0.64,
        "Hình 5.2. So sánh trực diện Baseline và Random Forest (23 biến) ở các tình huống khó nhất (trên), cùng phân "
        "bố mẫu kiểm thử giải thích vì sao 94.84% đường giữ nguyên trạng thái tĩnh (dưới).",
        s,
    )
    story.append(
        Paragraph(
            "5.5. Hạn Chế &amp; Rủi Ro Thành Phần Mô Hình Dự Báo &amp; Thực Nghiệm (Component Limitations &amp; Risks)",
            s["h2"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Limitations &amp; Risks (Mục 6 Modeling &amp; Mục 9 Experiments):</b> "
                "<b>(1) Độ nhạy ngưỡng cứng trên các ca đột ngột hóa kẹt cứng (Sudden Severe Onset Recall = 0 / "
                "8):</b> Do trong 993 mẫu TEST đang thông thoáng (LOS A–D) chỉ có đúng 8 mẫu (0.81%) đột ngột chuyển "
                "sang kẹt cứng (LOS E–F) sau 30 phút, bộ phân lớp cứng chưa bắt trúng ca nào (0/8), dù xếp hạng xác "
                "suất đạt Onset ROC-AUC = 0.8438 và rủi ro trung bình tăng 3.26x. "
                "<b>(2) Tính bảo thủ khi chuyển pha &amp; Macro-F1 trung gian (0.4253):</b> Trên 61 ca đổi trạng thái, "
                "mặc định đạt 10/61 (16.39%) và hiệu chỉnh VAL (τ = 0.40) nâng lên 14/61 (22.95%; McNemar p=0.001953); "
                "các lớp trung gian B–E chỉ có 10–20 mẫu TEST nên kéo Macro-F1 xuống 0.4253 dù Severe F1 đạt 0.9362. "
                "<b>(3) Thiếu biến ngoại sinh (Exogenous Blindness) &amp; giới hạn mùa vụ tập TEST:</b> Mô hình chưa "
                "có dữ liệu mưa ngập hay va chạm giao thông tức thời, và tập TEST (12/2020–04/2021) thiên về mùa khô."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch6_streaming_recon_and_serving(story: list, s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(PageBreak())
    story.append(
        Paragraph(
            (
                "Chương 6. P4 — Prescriptive Speed Layer (O(1) Stream Sketches 33.0 KiB), "
                "Two-Tier Batch–Stream Reconciliation &amp; Serving Layer"
            ),
            s["h1"],
        )
    )
    story.append(
        Paragraph(
            "6.1. Kafka 4.1.0 KRaft Ingestion, Watermark (30m) Dual Sink &amp; WAL Crash-Recovery",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Trình phát <b>producer.py</b> (<i>seed = 42</i>) đẩy luồng cửa sổ (<b>33,727 events</b> = "
                "33,394 cửa sổ cơ giới + 333 duplicates, gồm 1,168 on-time late ≤ 30p và 500 beyond-watermark "
                "late &gt; 30p trên 226 khung giờ) cùng luồng Raw GPS Pings (<b>91,694 events</b> = 90,787 hợp lệ "
                "+ 907 trùng) qua <b>apache/kafka:4.1.0 KRaft</b> (3 partitions, kiểm chứng 3,000 JSON messages). "
                "Mô-đun <b>Watermark (30m) &amp; Dual Sink [C6]</b> ghi nguyên vẹn 100% sự kiện vào "
                "<b>Medallion Lakehouse [C9]</b> và kiểm chứng watermark thật trên Spark Structured Streaming "
                "(<i>2020-12-01T12:30:00Z</i>, evict 10 rows hết hạn khỏi State Store). "
                "Thử nghiệm ngắt đột ngột <i>starter_stream.py --kill-after-batch 1</i> tự khôi phục từ "
                "<b>WAL Checkpoint</b> và chạy tiếp micro-batch 2..3 với <b>0 duplicate side effects</b>."
            ),
            s["body"],
        )
    )

    story.append(
        Paragraph(
            "6.2. Vì Sao Cần O(1) Stream Sketches (33.0 KiB) &amp; EWMA Hotspot Bên Cạnh Random Forest?",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Một câu hỏi rất tự nhiên là: <i>Ở Chương 5 đã có mô hình Random Forest dự báo kẹt xe rồi, vì sao sang "
                "Chương 6 Speed Layer lại trang bị thêm Bit-Packed Bloom Filter, HyperLogLog++ và EWMA Hotspot?</i> "
                "Bởi vì trên luồng dữ liệu đổ về liên tục 24/7, chúng giải quyết <b>hai bài toán vận hành hoàn toàn "
                "khác nhau nhưng bổ trợ trực tiếp cho nhau</b>: (1) <b>Random Forest (Chương 5) nhìn về tương lai "
                "t+30m</b> — trả lời câu hỏi <i>'Đoạn đường nào SẮP chuyển sang kẹt cứng?'</i> để kỹ sư đèn tín "
                "hiệu can thiệp phòng ngừa từ sớm; còn (2) <b>EWMA Hotspot (α = 0.15) giám sát độ lì của ùn tắc "
                "ngay hiện tại</b> — trả lời câu hỏi <i>'Đoạn đường nào ĐANG kẹt cứng lặp đi lặp lại suốt nhiều giờ "
                "liền chưa giải tỏa được?'</i> (chẳng hạn đoạn Lý Thường Kiệt số 47407 tích lũy EWMA Score lên tới "
                "6.67, tách biệt hẳn với những đoạn chỉ chậm thoáng qua 1 khung rồi thông ngay). Đồng thời, "
                "<b>RotatingBloomFilter (32.0 KiB = 2×16 KiB)</b> và <b>HyperLogLog++ (1.0 KiB)</b> thực hiện "
                "deduplication và đếm cardinality chỉ trong <b>33.0 KiB bộ nhớ O(1) cố định</b>, không làm tràn RAM "
                "khi chạy dài hạn:"
            ),
            s["body"],
        )
    )
    story.append(
        make_equation_box(
            CHART_DIR / "eq_sketches.png",
            "<b>O(1) Memory Footprint (33.0 KiB) không phình to theo thời gian:</b> Toàn bộ mảng bit "
            "RotatingBloomFilter (2 thế hệ × 16.0 KiB = 32,768 Bytes) và thanh ghi HyperLogLog++ (1,024 Bytes) "
            "được đóng gói vật lý trong đúng <b>33.0 KiB</b>; bộ theo dõi EWMA Hotspot tự động prune các đoạn đường "
            "đã thông thoáng khi điểm giảm dưới ngưỡng ε = 10⁻⁴.",
            s,
        )
    )
    story.append(Spacer(1, 4))
    sketch_rows = [
        [
            "RotatingBloomFilter (Deduplication)",
            "2 thế hệ × 16.0 KiB (262,144 bits = 32,768 Bytes = 32.0 KiB), k = 10 hash functions, capacity 8,000/gen.",
            (
                "Chặn chính xác 333 / 333 (100%) duplicate events; chỉ 13 FPs — empirical FPR = 0.0389% "
                "(< 0.0792% theoretical bound)."
            ),
        ],
        [
            "HyperLogLog++ (Active Cardinality)",
            "1,024 registers đóng gói trong mảng uint8[1024] = đúng 1,024 Bytes (1.0 KiB), RSE = 3.25%.",
            "Ước lượng 9,620.2 active segments so với 9,995 đoạn thực tế (relative error 3.75% < 5.0%).",
        ],
        [
            "EWMA Hotspot Decay Tracker",
            "Exponential decay factor α = 0.15 sau mỗi khung 30 phút; auto-pruning khi score < ε = 10⁻⁴.",
            (
                "Bắt chính xác 3 điểm kẹt dai dẳng nhất TP.HCM: #1 Segment 47407 Lý Thường Kiệt (6.6652), "
                "#2 Segment 21263 Trương Định (6.6635), #3 Segment 22684 Nguyễn Đình Chiểu (6.6300)."
            ),
        ],
    ]
    story.append(
        make_table(
            ["O(1) Stream Sketch / Tracker [C7]", "Cấu trúc Bộ nhớ O(1) & Tham số", "Kết quả Kiểm chứng Thực tế"],
            sketch_rows,
            [134, 178, 203],
            s,
        )
    )

    story.append(
        Paragraph(
            "6.3. Two-Tier Batch–Stream Reconciliation, Late-Data Backfill &amp; Atomic $status_version Guard", s["h2"]
        )
    )
    story.append(
        Paragraph(
            (
                "<b>Tier 1 (On-Time Subset Parity ≤ 30m):</b> Phép đối chiếu hai chiều <i>exceptAll() == 0</i> "
                "(<i>tier1_exceptAll_diff = 0</i>) xác nhận <b>100.0% in-watermark parity</b> giữa Speed Layer và "
                "Batch Layer. <b>Tier 2 (Late-Data Backfill &amp; Causal Re-Scoring):</b> Cuối ngày, mô-đun "
                "<b>[C12]</b> quét lại <b>Medallion Lakehouse</b> để backfill đủ <b>500 beyond-watermark late windows "
                "(&gt; 30m)</b> trên <b>226 khung giờ</b>, nâng completeness từ <b>98.50% lên 100.00%</b>; đồng thời "
                "hàn gắn lag/priors cho <b>609 downstream windows</b> và <b>re-score 384 forecast windows</b> (cập "
                "nhật xác suất cho 213 cửa sổ). Kho phục vụ MongoDB 8.0 lưu rõ <b>33,394 documents</b> theo lifecycle: "
                "<b>489 STREAM_PROVISIONAL (v1)</b>, <b>500 BATCH_RECONCILED (v2)</b> và <b>32,405 BATCH_VERIFIED "
                "(v2)</b>; cơ chế <b>atomic $status_version pipeline guard</b> đã chặn <b>25/25</b> stale v1 events "
                "không cho ghi đè lùi lên bản chuẩn v2."
            ),
            s["body"],
        )
    )

    story.append(
        Paragraph(
            "6.4. MongoDB 8.0.34 Serving Store (< 1 ms) &amp; ReactJS + FastAPI TOC Command Center (:8000)",
            s["h2"],
        )
    )
    serve_rows = [
        [
            "Idempotent Upsert Engine",
            (
                "Chia 33,394 documents qua 4 write partitions (8,119 / 8,472 / 8,209 / 8,594), thực thi 69 batches × "
                "500 lệnh UpdateOne(upsert=True) kèm $status_version guard."
            ),
            (
                "Chạy replay nhiều lần vẫn giữ đúng 33,394 documents (0 duplicates); chặn 25/25 bản tin cũ v1."
            ),
        ],
        [
            "Compound Index IXSCAN (UC-2)",
            (
                "Compound Index level_prob_segment_idx {street_level: 1, severe_prob: -1, segment_id: 1}; lọc trục "
                "cấp 1–2 có P(Severe) ≥ 70%."
            ),
            (
                f"Khớp {gt['arterial_alerts']} cửa sổ trục chính, trả Top-25 qua [{gt['mongo_stages']}], 0 in-memory "
                f"SORT, warm median = {gt['mongo_med']} / p95 = {gt['mongo_p95']} (tăng tốc {gt['mongo_speedup']} "
                f"so với COLLSCAN {gt['mongo_collscan']})."
            ),
        ],
        [
            "ReactJS + FastAPI TOC (:8000)",
            (
                "Trung tâm Điều hành TOC Command Center (ReactJS + Leaflet + FastAPI :8000) gồm 5 Tab truy vấn trực "
                "tiếp MongoDB 8.0 và luồng Kafka."
            ),
            (
                "5 Tab hợp nhất: (1) Command Center & Dự báo +30p (UC-1, UC-2) · (2) Live Kafka & 33 KiB Sketches · "
                "(3) PageRank & Luật đồng kẹt (UC-3, UC-4) · (4) 6 Cụm K-Means Cold-Start (UC-5) · (5) Spark 3.5.7 / "
                "Ablation / Đối soát (UC-6)."
            ),
        ],
    ]
    story.append(
        make_table(
            ["Serving Component [C16]–[C17]", "Thiết kế Compound Index & Cơ chế Vận hành", "Kết quả Đo đạc Thực tế"],
            serve_rows,
            [122, 174, 219],
            s,
        )
    )
    story.append(Spacer(1, 4))
    _add_centered_figure(
        story,
        CHART_DIR / "chart_streaming_serving.png",
        0.62,
        f"Hình 6.1. Phân bố 33,394 cửa sổ theo Two-Tier Reconciliation Lifecycle (trên) và độ trễ IXSCAN "
        f"{gt['mongo_med']} / p95 {gt['mongo_p95']} vs COLLSCAN {gt['mongo_collscan']} ({gt['mongo_speedup']}) trên "
        "MongoDB 8.0.34 (dưới).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "ui_tab1_overview_hotspots.png",
        0.88,
        "Hình 6.2. Giao diện ReactJS + FastAPI TOC Command Center (:8000 ↔ MongoDB 8.0) — Tab 01 (Điều Hành & Dự Báo "
        "+30p): bản đồ tương tác Leaflet tại nút giao Nguyễn Bỉnh Khiêm (#21262), bảng cảnh báo sớm +30p và thẻ "
        f"kiểm chứng MongoDB 8.0 IXSCAN (median {gt['mongo_med']}, p95 {gt['mongo_p95']}, tăng tốc "
        f"{gt['mongo_speedup']} so với COLLSCAN).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "ui_tab2_live_forecast_mongo.png",
        0.88,
        "Hình 6.3. Ảnh chụp cận cảnh Kịch bản 2 trên ReactJS + FastAPI TOC (:8000) tại đoạn #21064 — Công Trường "
        "Quốc Tế (2021-04-22, thu thập [11:30–12:00), chốt phát lệnh t = 12:00 ICT dự báo cho [12:00–12:30), LOS E · "
        "12.0 km/h: RF 23D kích hoạt cảnh báo nguy cơ E/F = 68.7% > 25% và xác suất đổi pha 48.9% > 40% trước khi "
        "thực tế chuyển LOS F) kèm bảng kiểm chứng trực tiếp MongoDB 8.0 IXSCAN (31.36x, 0 SORT trên RAM).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "ui_tab4_kmeans_coldstart.png",
        0.88,
        "Hình 6.4. Ảnh chụp cận cảnh Tab 04 (6 Cụm K-Means & Cold-Start · UC-5) trên ReactJS + FastAPI TOC (:8000): "
        "thống kê 6 nhóm hành vi (Silhouette = 0.2241 trên 3,609 đoạn TRAIN) phủ kín 74,211 đoạn Cold-Start (88.13%) "
        "và tra cứu đoạn #37345 — Cầu vượt Ngã tư Gò Dưa (0 GPS, PageRank = 2.7196, Cụm #4) đối chiếu ngay các đoạn "
        "cảm biến tương đồng nhất (Song Hành Xa lộ Hà Nội, Nguyễn Kiệm, Dương Bá Trạc...).",
        s,
    )
    story.append(
        Paragraph(
            (
                "6.5. Hạn Chế &amp; Rủi Ro Thành Phần Xử Lý Luồng &amp; Giao Diện Điều Hành (Component Limitations "
                "&amp; Risks)"
            ),
            s["h2"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Limitations &amp; Risks (Mục 8 Stream Processing &amp; Mục 10 Demonstration):</b> "
                "<b>(1) Giới hạn cửa sổ xoay vòng của RotatingBloomFilter (2 × 16.0 KiB):</b> Thiết kế xoay vòng 2 "
                "thế hệ (capacity 8,000 bản tin/thế hệ, k = 10) đã khử bão hòa bit và hạ empirical FPR xuống "
                "<b>0.0389%</b> (13 FPs); tuy nhiên các bản tin trùng đến quá muộn sau hơn 2 thế hệ sẽ lọt qua bộ "
                "lọc RAM và phải dựa vào chỉ mục duy nhất <i>event_id</i> trong MongoDB để chống ghi trùng. "
                "<b>(2) Độ trễ phản ánh của các bản tin đến muộn quá Watermark (&gt; 30 phút):</b> Các gói tin bị kẹt "
                "trên mạng di động quá 30 phút (500 cửa sổ) bị nhánh trực tuyến bỏ qua để giữ tốc độ phản hồi và phải "
                "chờ tiến trình <i>Two-Tier Reconciliation</i> cuối ngày mới được bù vào kho phục vụ. "
                "<b>(3) Giới hạn hỗ trợ quyết định (Human-in-the-Loop UI):</b> Cổng <i>ReactJS + FastAPI TOC "
                "(:8000)</i> hiện đóng vai trò bảng điều khiển khuyến nghị cho con người, chưa đấu nối điều khiển tự "
                "động vòng kín trực tiếp vào tủ tín hiệu đèn giao thông SCATS."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch7_team_progress_and_runbook(story: list, s: dict[str, ParagraphStyle]) -> None:
    story.append(PageBreak())
    story.append(
        Paragraph(
            "Chương 7. Phân Công Đóng Góp Nhóm, Automated Test Suite, Docker RAM Budget &amp; Runbook",
            s["h1"],
        )
    )
    story.append(Paragraph("7.1. Phân Công Chuyên Trách &amp; Kết Quả Bàn Giao Của Ba Thành Viên (100%)", s["h2"]))
    team_rows = [
        [
            "1. Nguyễn Tạ Minh Quyền\n(MSSV: 25MS23280)",
            "P1: Spark Batch ETL, Quality Funnel &amp; Medallion Lakehouse",
            (
                "spark_batch.py, baseline.py,<br/>traffic_data/spark_etl.py,<br/>"
                "compose.quyen.yml,<br/>tests/test_quyen_spark.py"
            ),
            "Ordered Quality Funnel (ANSI try_cast, loại 427 POIs), Unicode NFC (695→641) &amp; UTC+7, "
            "Train-Only Vmax Lookup, 5-Tuple Monoid (diff=0.0), Key Salting (684.95x → 1.020x), BroadcastHashJoin "
            "(5→1 Exch, +25.76%) &amp; Parquet Pruning (7,268/33,394 dòng).",
        ],
        [
            "2. Nguyễn Thành Đạt\n(MSSV: 25MS23306)",
            "P2 &amp; P3: Graph Mining, 7D K-Means &amp; +30m Forecasting",
            "pipeline.py, spark_engine.py,<br/>Automated Test Suite tests/",
            (
                "Lọc 90.97% shape points, Line-Graph PageRank 84,206 cơ giới (92,453 cạnh bỏ U-turn), FPGrowth "
                "(32 rules organic TRAIN) &amp; SON (12+14), 7D K-Means (Sil=0.2241) phủ 74,211 Cold-Start, tách "
                "7,192 burst → 1,885 organic TRAIN &amp; RF 23D (VAL 0.8882; ROC=0.9894, PR=0.9543, Sev-F1=0.9362, "
                "Detect=0.8822, McNemar p=0.001953)."
            ),
        ],
        [
            "3. Phạm Công Minh\n(MSSV: 25MS23310)",
            "P4: Kafka Streaming, Recon, MongoDB &amp; ReactJS + FastAPI TOC (:8000)",
            "producer.py, starter_stream.py,<br/>toc_backend/*, toc_frontend/*,<br/>scripts/*",
            (
                "Hạ tầng Docker 5 container mặc định 11.0/16.0 GB RAM, Kafka 4.1 KRaft, Spark Streaming Watermark "
                "thật (2020-12-01T12:30Z), RotatingBloom 32.0 KiB (0.0389% FPR) + HLL++ 1.0 KiB, WAL (0 dupes), "
                "Recon (500 trễ, 609 lag, 384 rescore / 213 đổi prob), MongoDB 8.0.34 ($status_version &amp; IXSCAN "
                "0.360/0.603 ms, 31.36x) &amp; ReactJS + FastAPI TOC 5-Tab (:8000)."
            ),
        ],
    ]
    story.append(
        make_table(
            [
                "Thành viên Nhóm",
                "Mảng Chuyên trách",
                "Tệp Mã nguồn Phụ trách",
                "Kết quả Thực nghiệm Bàn giao",
            ],
            team_rows,
            [96, 102, 116, 201],
            s,
        )
    )

    story.append(Paragraph("7.2. Automated Test Suite (pytest &amp; Spark Unittest) &amp; Code Quality", s["h2"]))
    story.append(
        Paragraph(
            (
                "Mọi phép biến đổi dữ liệu và bất biến toán học trong báo cáo đều được kiểm tra tự động qua bộ kiểm "
                "thử <b>pytest</b> &amp; <b>Spark unittest</b>: (1) Bảo toàn quân số Quality Funnel, loại 427 POIs còn "
                "84,206 đoạn cơ giới, ngưỡng 6 mức <i>derive_los</i> (89.19% khớp BkTraffic) và bất biến của "
                "<i>vmax_lookup.json</i>; (2) Tính kết hợp <b>5-Tuple Monoid Combiner</b> trên phân vùng lệch &amp; "
                "kế hoạch <b>BroadcastHashJoin</b> (5 → 1 Exchange, 0 exceptAll diff) + Parquet Pruning; (3) Bảo toàn "
                "tổng <b>Line-Graph PageRank</b> (84,206.0; 92,453 cạnh bỏ U-turn), chuẩn hóa <i>Unicode NFC</i> &amp; "
                "tra cứu Cold-Start (74,211 đoạn); (4) Tách 7,192 burst khỏi TRAIN, ghép +30m, cắt biên, kiểm thử "
                "Future-Shuffle (<i>diff = 0.0</i>), VAL Gate &amp; <i>SHA-256</i>; (5) <b>RotatingBloomFilter</b> "
                "(2 × 16.0 KiB = 32,768 Bytes), Spark real watermark, <b>$status_version</b> chặn ghi đè &amp; WAL "
                "(0 dupes). Mã nguồn tuân thủ <b>Ruff</b> (0 lỗi, độ phức tạp hàm <i>McCabe ≤ 14</i>)."
            ),
            s["body"],
        )
    )

    story.append(
        Paragraph("7.3. Ngân Sách Tài Nguyên Cụm Docker (11.0 / 16.0 GB RAM) &amp; Từ Điển 60 Cột dataset.csv", s["h2"])
    )
    ram_rows = [
        [
            "kafka",
            "apache/kafka:4.1.0 (KRaft Mode)",
            "9092:9092",
            "1.5 GB",
            "Tiếp nhận luồng bản tin GPS 3 phân vùng cho chủ đề hcmc-traffic-events ([C4])",
        ],
        [
            "mongo",
            "mongo:8.0 (MongoDB 8.0.34)",
            "27017:27017",
            "2.0 GB",
            "Kho phục vụ bda501.traffic_scores với IXSCAN (0.360/0.603 ms, 31.36x) &amp; $status_version ([C16])",
        ],
        [
            "spark",
            "hcmc-traffic-spark:3.5.7 (PySpark 3.5.7)",
            "Nội bộ (bda501-net)",
            "6.0 GB",
            "Xử lý phân tán làm sạch, đồ thị, học máy &amp; đối soát cuối ngày ([C5]–[C12])",
        ],
        [
            "toc-web",
            "groupexe-knowledgetest-app (ReactJS + FastAPI)",
            "8000:8000",
            "1.0 GB",
            "Trung tâm Điều hành ReactJS + FastAPI + Leaflet 5-Tab kết nối Kafka &amp; MongoDB ([C17])",
        ],
        [
            "live-consumer",
            "groupexe-knowledgetest-app (Stream)",
            "Nội bộ (bda501-net)",
            "0.5 GB",
            "Consumer Kafka → RotatingBloom/HLL++/EWMA → MongoDB v1 liên tục ([C6]–[C8])",
        ],
        [
            "Tổng cộng",
            "5 Dịch vụ Mặc định (kèm 2 profile batch/stream)",
            "—",
            "11.0 GB",
            "Vận hành ổn định trên máy cá nhân 16.0 GB RAM (dư 5.0 GB cho hệ điều hành)",
        ],
    ]
    story.append(
        make_table(
            ["Dịch vụ", "Ảnh Container Chuẩn hóa", "Cổng Mạng", "Giới hạn RAM", "Vai trò Vận hành Thực tế"],
            ram_rows,
            [62, 152, 66, 50, 185],
            s,
            highlight_row=6,
        )
    )
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            (
                "<b>Từ điển 60 cột của bảng dữ liệu chuẩn hóa data/dataset.csv (33,394 dòng cơ giới):</b> "
                "(1) <i>Định danh &amp; Tọa độ (10 cột):</i> event_id, segment_id, street_id, street_name, "
                "street_type, street_level, long_snode, lat_snode, long_enode, lat_enode · "
                "(2) <i>Thời gian UTC/VN, Phân tập &amp; Burst (15 cột):</i> date, ict_date, weekday, is_weekend, "
                "period, ict_period, period_idx, ict_period_idx, vn_hour, period_sin, period_cos, slot_ts, "
                "target_ts, split_set, is_synthetic_burst · "
                "(3) <i>Tổng hợp vận tốc &amp; mức LOS (13 cột):</i> LOS, derived_los, los_ordinal, is_severe, "
                "mean_velocity, sum_velocity, min_velocity, max_velocity_obs, severe_ping_count, max_velocity, "
                "vmax_impute_tier, velocity_ratio, obs_count · "
                "(4) <i>Đồ thị, Phân cụm &amp; Biến quá khứ (16 cột):</i> length_m, pagerank_score, in_degree, "
                "out_degree, lag1_los_ordinal, lag2_los_ordinal, trailing_severe_rate, neighbor_los_ordinal, "
                "segment_train_prior_los, segment_train_severe_prior, street_causal_prior_los, "
                "street_causal_prior_sev, gap_minutes, is_contiguous_30m, cluster_id, cluster · "
                "(5) <i>Nhãn dự báo +30 phút &amp; đối chiếu phụ (6 cột):</i> target_next_los_ordinal, "
                "target_next_severe, target_t30m_los_ordinal, target_t30m_severe, next_obs_los_ordinal, "
                "next_obs_severe."
            ),
            s["body"],
        )
    )

    story.append(
        Paragraph(
            "7.4. Hướng Dẫn Khởi Chạy &amp; Tái Lập Toàn Bộ Kết Quả Thực Nghiệm (PowerShell / Docker CLI)", s["h2"]
        )
    )
    cli_lines = [
        (
            "<b># 1. Khởi chạy 5 dịch vụ Docker mặc định (11.0 GB RAM: Kafka 4.1, Mongo 8.0, Spark 3.5.7, "
            "TOC :8000, live-consumer)</b>"
        ),
        "docker compose up -d --build",
        "",
        (
            "<b># 2. Chạy trọn vẹn Unified Pipeline (P1 → P4, gồm nạp MongoDB 8.0) &amp; "
            "kiểm chứng Monoid (baseline.py)</b>"
        ),
        "python pipeline.py",
        "python baseline.py",
        "",
        "<b># 3. Thực thi kiểm chứng runtime &amp; Watermark thật trên container Apache Spark 3.5.7</b>",
        "docker compose exec -T spark python3 spark_engine.py",
        "",
        "<b># 4. Kiểm chứng hồi phục luồng WAL (--kill-after-batch 1) &amp; mở giao diện http://localhost:8000</b>",
        "python starter_stream.py --kill-after-batch 1",
        "",
        "<b># 5. Xuất bản Sơ đồ Kiến trúc, Bản đồ TP.HCM, Báo cáo PDF &amp; Bộ Slide Thuyết trình (.pptx + .pdf)</b>",
        "python scripts/render_arch_diagram.py",
        "python scripts/render_component_flows.py",
        "python scripts/render_method_explainers.py",
        "python scripts/render_visual_assets.py",
        "python scripts/render_pre_experiment_visuals.py",
        "python scripts/build_proposal_pdf.py",
        "python scripts/build_slides.py",
        "python scripts/build_slides_pre_experiment.py",
        "",
        "<b># 6. Chạy bộ kiểm thử tự động toàn diện &amp; kiểm tra chất lượng mã nguồn Ruff (max-complexity=14)</b>",
        "pytest -v",
        (
            'python -m ruff check . --select E,F,W,C901 '
            '--config "lint.mccabe.max-complexity=14" --config "line-length=120"'
        ),
    ]
    story.append(make_code_box(cli_lines, s))
    story.append(
        Paragraph(
            (
                "7.5. Tổng Hợp Limitations &amp; Risks Toàn Hệ Thống (Mục 11) &amp; Kế Hoạch Mở Rộng 3 Giai Đoạn (Mục "
                "12 — Plan)"
            ),
            s["h2"],
        )
    )
    risk_rows = [
        [
            "1. Thiên lệch đại diện phương tiện\n(Fleet Representation Bias)",
            "Nguồn GPS cộng đồng chỉ phủ 11.87% đoạn cơ giới (9,995/84,206) và nghiêng về ô tô trên trục chính, "
            "chưa phản ánh đầy đủ luồng xe máy (&gt; 80% lưu lượng TP.HCM) và mạng lưới hẻm dân cư.",
            "Kết hợp Line-Graph PageRank + 7D K-Means để suy diễn vùng mù; Giai đoạn 2 tích hợp thêm nguồn GPS xe "
            "buýt, xe công nghệ 2 bánh và camera giao thông.",
        ],
        [
            "2. Trôi dạt phân phối &amp; lớp B–E thưa\n(Concept Drift &amp; Imbalance)",
            "Mô hình khóa ở Phase A trên 1,885 organic TRAIN sẽ suy giảm khi mưa ngập mới; các lớp trung gian B–E chỉ "
            "có 10–20 mẫu TEST kéo Macro-F1 xuống 0.4253 (dù Severe F1 = 0.9362).",
            "Thiết lập giám sát PSI, tự động huấn luyện lại Phase A hàng tuần và bổ sung thu thập mẫu gán nhãn đều "
            "cho các lớp chuyển tiếp B–E.",
        ],
        [
            "3. Khan hiếm mẫu chuyển pha đột ngột\n(Rare Sudden-Onset Scarcity)",
            "Chỉ có 8 / 993 mẫu TEST (0.81%) chuyển đột ngột từ thoáng (A–D) sang kẹt cứng (E–F), khiến ngưỡng "
            "cứng đạt Recall 0/8 (dù Onset ROC-AUC = 0.8438 và rủi ro tăng 3.26x).",
            "Giai đoạn 1 bổ sung hàm phạt nhạy chi phí (Cost-Sensitive Focal Loss), hiệu chỉnh ngưỡng động theo cấp "
            "đường và tích hợp API thời tiết/sự cố.",
        ],
        [
            "4. Trần mở rộng của cụm máy đơn\n(Single-Host Deployment Ceiling)",
            "Cụm Docker Compose 11.0 / 16.0 GB RAM (5 container mặc định) đáp ứng tốt Proof-of-Scale (38.5 MB) và "
            "ReactJS + FastAPI TOC (:8000), nhưng sẽ nghẽn I/O ở quy mô 1.5–3.0 TB/ngày toàn thành phố.",
            "Giai đoạn 3 chuyển đổi sang cụm Kubernetes đa máy chủ, phân mảnh (Sharding) kho MongoDB 8.0 và mở rộng "
            "phân vùng Kafka (cùng RotatingBloomFilter đã triển khai).",
        ],
    ]
    story.append(
        make_table(
            [
                "Rủi ro &amp; Hạn chế Chung Toàn Cục",
                "Nguyên nhân Gốc rễ &amp; Tác động Vận hành",
                "Kế hoạch Khắc phục Trong Lộ trình Mở rộng (Mục 12 — Plan)",
            ],
            risk_rows,
            [125, 195, 195],
            s,
        )
    )


def build_proposal_pdf(out_path: Path) -> None:
    styles = build_styles()
    gt = _load_ground_truth()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc = BaseDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=38,
        title="HCMC Smart Traffic Big Data Analytics & Congestion Prediction — Final Project Report & Specification",
        author="Nguyễn Tạ Minh Quyền (25MS23280), Nguyễn Thành Đạt (25MS23306), Phạm Công Minh (25MS23310)",
        subject="FSB Course BDA501 — Big Data Analytics Final Project Report & Comprehensive Technical Specification",
    )
    frame = Frame(
        40, 38, CONTENT_W, A4[1] - 78, id="main_frame", leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0
    )
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame])])

    story: list = []
    _append_cover_and_ch1_context(story, styles, gt)
    _append_ch1_problems_usecases_and_boundary(story, styles)
    _append_ch2_data_and_funnel(story, styles)
    _append_ch3_architecture_and_spark(story, styles, gt)
    _append_ch4_graph_patterns_and_kmeans(story, styles)
    _append_ch5_forecasting_and_ablation(story, styles)
    _append_ch6_streaming_recon_and_serving(story, styles, gt)
    _append_ch7_team_progress_and_runbook(story, styles)
    doc.build(story, canvasmaker=NumberedCanvas)


def main() -> None:
    primary_pdf = REPO_DIR / "HCMC_Smart_Traffic_Final_Report.pdf"
    build_proposal_pdf(primary_pdf)
    sync_targets = [
        OUTPUTS_DIR / "HCMC_Smart_Traffic_Final_Report.pdf",
    ]
    for target in sync_targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            if primary_pdf.resolve() != target.resolve():
                shutil.copy2(primary_pdf, target)
        except OSError:
            pass
    print(f"Built {primary_pdf} ({primary_pdf.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
