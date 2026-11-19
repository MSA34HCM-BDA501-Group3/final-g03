"""Build the Human-Centered HCMC Smart Traffic Final Project Report & Technical Specification PDF."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import matplotlib
from PIL import Image as PILImage
from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.platypus import (
    BaseDocTemplate,
    CondPageBreak,
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
from reportlab.platypus.tableofcontents import TableOfContents

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
TOP_MARGIN = 44.0
BOTTOM_MARGIN = 44.0


class NumberedCanvas(pdfcanvas.Canvas):
    """Two-pass canvas rendering exact 'Trang X / N' chrome and accurately indexed PDF outlines."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._saved_page_states: list[dict[str, Any]] = []
        self._page_outlines: list[tuple[str, str, int, bool]] = []
        self._all_outlines: list[list[tuple[str, str, int, bool]]] = []

    def register_outline(self, key: str, title: str, level: int, add_outline: bool = True) -> None:
        self._page_outlines.append((key, title, level, add_outline))

    def showPage(self) -> None:
        self._saved_page_states.append(dict(self.__dict__))
        self._all_outlines.append(list(self._page_outlines))
        self._page_outlines = []
        self._startPage()

    def save(self) -> None:
        pages_and_outlines = list(zip(self._saved_page_states, self._all_outlines))
        total_pages = len(pages_and_outlines)
        for state, outlines in pages_and_outlines:
            self.__dict__.update(state)
            self._draw_chrome(total_pages)
            for key, title, level, add_outline in outlines:
                self.bookmarkPage(key)
                if add_outline:
                    self.addOutlineEntry(title, key, level=level, closed=False)
            super().showPage()
        super().save()

    def _draw_chrome(self, total_pages: int) -> None:
        if self._pageNumber <= 1:
            return
        self.saveState()
        width, height = A4
        self.setStrokeColor(BORDER_GRAY)
        self.setLineWidth(0.6)
        self.line(40, height - 32, width - 40, height - 32)
        self.setFont(FONT_BOLD, 7.4)
        self.setFillColor(NAVY)
        self.drawString(40, height - 24, "BDA501 — Hệ Thống Giám Sát & Dự Báo Ùn Tắc Giao Thông TP.HCM")
        self.setFont(FONT_REG, 7.2)
        self.setFillColor(SLATE)
        self.drawRightString(width - 40, height - 24, "Báo Cáo Tổng Kết Đồ Án & Đặc Tả Kỹ Thuật Chi Tiết")

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


class TaggedTOC(TableOfContents):
    """TableOfContents variant listening to custom notification tags (TOCEntry / LOFEntry / LOTEntry)."""

    def __init__(self, notify_kind: str = "TOCEntry", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._notifyKind = notify_kind


def _shorten_caption(text: str, max_len: int = 112) -> str:
    head = text.split(":", 1)[0].strip() if ":" in text and text.index(":") <= max_len else text.strip()
    if len(head) <= max_len:
        return head
    cut = head[: max_len - 1].rsplit(" ", 1)[0].rstrip(",;–-")
    return f"{cut}…"


class ReportDocTemplate(BaseDocTemplate):
    """DocTemplate registering headings, figures, and tables into TOC/LOF/LOT and PDF Outline bookmarks."""

    def handle_documentBegin(self) -> None:
        super().handle_documentBegin()
        self._bk_seq = 0

    def afterFlowable(self, flowable: Any) -> None:
        if not isinstance(flowable, Paragraph):
            return
        style_name = flowable.style.name
        plain = flowable.getPlainText().strip()
        if not plain:
            return
        if style_name in ("SectionH1", "SectionH2"):
            level = 0 if style_name == "SectionH1" else 1
            self._bk_seq += 1
            key = f"sec_{self._bk_seq}"
            self.canv.register_outline(key, plain, level, add_outline=True)
            self.notify("TOCEntry", (level, plain, self.page, key))
        elif style_name == "FigCaption":
            self._bk_seq += 1
            key = f"fig_{self._bk_seq}"
            self.canv.register_outline(key, plain, 0, add_outline=False)
            self.notify("LOFEntry", (0, _shorten_caption(plain), self.page, key))
        elif style_name == "TableCaption":
            self._bk_seq += 1
            key = f"tbl_{self._bk_seq}"
            self.canv.register_outline(key, plain, 0, add_outline=False)
            self.notify("LOTEntry", (0, _shorten_caption(plain), self.page, key))


def _load_core_dataset_gt(metrics: dict[str, Any], manifest: dict[str, Any]) -> dict[str, str]:
    ds = metrics["dataset"]
    qf = metrics["quality_funnel_c5"]
    gp = metrics["graph_and_patterns"]
    mdl = metrics["model"]
    sfs = metrics["static_feature_store_c13b"]
    return {
        "model_sha12": manifest["sha256_digest"][:12],
        "static_sha12": manifest["static_feature_store_sha256"][:12],
        "vmax_sha12": manifest["vmax_lookup_sha256"][:12],
        "nodes_n": f"{ds['nodes_count']:,}",
        "active_nodes": f"{ds['active_intersection_nodes_count']:,}",
        "pruned_nodes": f"{ds['pruned_orphan_nodes_count']:,}",
        "streets_n": f"{ds['streets_count']:,}",
        "raw_segs": f"{ds['raw_segments_count']:,}",
        "clean_segs": f"{ds['segments_count']:,}",
        "active_segs": f"{ds['distinct_active_segments']:,}",
        "cold_segs": f"{ds['cold_start_segments_count']:,}",
        "cold_pct": f"{sfs['cold_start_ratio_pct']:.2f}%",
        "raw_pings": f"{ds['raw_segment_status_events']:,}",
        "clean_pings": f"{ds['segment_status_events']:,}",
        "rej_pings": f"{qf['status_rejected_pings']:,}",
        "windows_n": f"{ds['labeled_windows_count']:,}",
        "burst_n": f"{ds['synthetic_burst_windows_flagged']:,}",
        "t30m_pairs": f"{ds['exact_t30m_supervised_windows_count']:,}",
        "los_match": f"{ds['derived_vs_reference_los_exact_match_pct']:.2f}%",
        "raw_st_names": f"{qf['raw_distinct_street_names']:,}",
        "nfc_st_names": f"{qf['canonical_nfc_street_names']:,}",
        "merged_st_names": f"{qf['street_entities_merged_by_nfc']:,}",
        "miss_vmax_seg_pct": f"{qf['missing_max_velocity_segments_pct']:.2f}%",
        "miss_vmax_raw_pct": f"{qf['missing_max_velocity_train_pct']:.2f}%",
        "lg_edges": f"{ds['line_graph_transition_edges']:,}",
        "dangling_n": f"{ds['dangling_nodes_count']:,}",
        "top_pr_seg": str(gp["top_bottleneck_segment_id"]),
        "top_pr_street": str(gp["top_bottleneck_street"]),
        "top_pr_score": f"{gp['top_bottleneck_pagerank']:.4f}",
        "co_rules": str(gp["co_congestion_rules_found"]),
        "lag_rules": str(gp["lagged_transition_rules_found"]),
        "km_segs": f"{mdl['segments_clustered']:,}",
        "km_sil": f"{mdl['silhouette_score']:.4f}",
        "km_dbi": f"{mdl['davies_bouldin_index']:.4f}",
        "km_ch": f"{mdl['calinski_harabasz_score']:.2f}",
        "aug_rows": f"{metrics['lakehouse_c14']['year_month_partitions']['2020-08']:,}",
    }


def _load_ml_gt(metrics: dict[str, Any]) -> dict[str, str]:
    ev = metrics["evaluation"]
    ta = ev["transition_analysis"]
    st = ta["statistical_significance_test"]
    cs = ta["spatial_cold_start_masked_cv"]
    gap = ta["next_observed_gap_regime"]
    return {
        "train_n": f"{ev['train_samples']:,}",
        "val_n": f"{ev['val_samples']:,}",
        "test_n": f"{ev['test_samples']:,}",
        "test_sev": f"{ev['positive_severe_test_samples']:,}",
        "rf_acc": f"{ev['accuracy_6class']:.4f}",
        "rf_mf1": f"{ev['macro_f1_6class']:.4f}",
        "rf_wf1": f"{ev['weighted_f1_6class']:.4f}",
        "rf_prec": f"{ev['severe_precision']:.4f}",
        "rf_rec": f"{ev['severe_recall']:.4f}",
        "rf_f1": f"{ev['severe_f1']:.4f}",
        "rf_roc": f"{ev['roc_auc']:.4f}",
        "rf_pr": f"{ev['average_precision']:.4f}",
        "steady_pct": f"{ta['steady_state_ratio_pct']:.2f}%",
        "trans_n": str(ta["transition_samples"]),
        "trans_pct": f"{ta['transition_ratio_pct']:.2f}%",
        "detect_roc": f"{ta['rf_23d_transition_detect_roc_auc']:.4f}",
        "detect_pr": f"{ta['rf_23d_transition_detect_pr_auc']:.4f}",
        "onset_n": str(ta["congestion_onset_samples"]),
        "non_sev_n": str(ta["non_severe_t_samples"]),
        "onset_roc": f"{ta['rf_23d_onset_roc_auc']:.4f}",
        "onset_lift": f"{ta['rf_23d_onset_risk_lift_ratio']:.2f}x",
        "mcnemar_p": f"{st['mcnemar_transition_p_value']:.6f}",
        "cold_acc": f"{cs['rf_23d_masked_cold_start_6class_acc'] * 100:.2f}%",
        "cold_roc": f"{cs['rf_23d_masked_cold_start_roc_auc']:.4f}",
        "cold_pr": f"{cs['rf_23d_masked_cold_start_pr_auc']:.4f}",
        "gap_n": f"{gap['test_samples']:,}",
        "gap_rf_pr": f"{gap['rf_23d_severe_pr_auc']:.4f}",
        "gap_pers_pr": f"{gap['persistence_severe_pr_auc']:.4f}",
    }


def _load_spark_stream_mongo_gt(
    metrics: dict[str, Any],
    mongo: dict[str, Any],
    spark: dict[str, Any],
    qp: dict[str, Any],
    quyen: dict[str, Any],
) -> dict[str, str]:
    ix = mongo["ixscan_explain_verification"]
    lat = mongo["uc2_arterial_query"]["latency_benchmark_ms"]
    cj = spark["catalyst_join_spark"]
    ms = spark["monoid_salting_spark"]
    sm = metrics["streaming_mmds"]
    bf = sm["bloom_filter"]
    hll = sm["hyperloglog"]
    rec = sm["reconciliation"]
    return {
        "mongo_med": f"{lat['warm_median_ms']:.3f} ms",
        "mongo_p95": f"{lat['warm_p95_ms']:.3f} ms",
        "mongo_collscan": f"{lat['collscan_warm_median_ms']:.3f} ms",
        "mongo_speedup": f"{lat['ixscan_vs_collscan_speedup_x']:.2f}x",
        "mongo_stages": ", ".join(ix["winning_stages"]),
        "arterial_alerts": f"{mongo['uc2_arterial_query']['total_matching_windows']:,}",
        "mongo_stale_rej": str(mongo["stale_v1_rejected_by_mongo"]),
        "join_base_ms": f"{cj['baseline_median_ms']:.2f}",
        "join_base_iqr": f"{cj['baseline_iqr_ms']:.2f}",
        "join_opt_ms": f"{cj['optimized_median_ms']:.2f}",
        "join_opt_iqr": f"{cj['optimized_iqr_ms']:.2f}",
        "join_speedup": f"{cj['join_speedup_pct']:.2f}%",
        "monoid_unsalted_ms": f"{ms['unsalted_median_ms']:.2f}",
        "monoid_salted_ms": f"{ms['salted_median_ms']:.2f}",
        "monoid_unsalted_iqr": f"{ms['unsalted_iqr_ms']:.2f}",
        "monoid_salted_iqr": f"{ms['salted_iqr_ms']:.2f}",
        "spark_fp_rules": str(spark["spark_fpgrowth_rules_count"]),
        "spark_km_cost": f"{spark['spark_kmeans_training_cost']:,.4f}",
        "spark_rf_acc": f"{spark['spark_rf_test_accuracy_6class']:.4f}",
        "spark_rf_roc": f"{spark['spark_rf_test_severe_roc_auc']:.4f}",
        "quyen_smj_s": f"{quyen['join_seconds']['sort_merge']:.3f}s",
        "quyen_bhj_s": f"{quyen['join_seconds']['broadcast']:.3f}s",
        "quyen_unsalted_s": f"{quyen['aggregate_seconds']['monoid_unsalted']:.3f}s",
        "quyen_salted_s": f"{quyen['aggregate_seconds']['monoid_salted']:.3f}s",
        "stream_events": f"{sm['total_stream_events_processed']:,}",
        "dupes_caught": str(bf["duplicates_flagged"]),
        "bloom_kb": f"{bf['memory_kb']:.1f} KiB",
        "bloom_fpr": f"{bf['empirical_fp_rate'] * 100:.4f}%",
        "bloom_fp_cnt": str(bf["false_positives"]),
        "hll_kb": f"{hll['memory_kb']:.1f} KiB",
        "hll_est": f"{hll['hll_estimated_segments']:,.1f}",
        "hll_err": f"{hll['relative_error_pct']:.2f}%",
        "wal_kb": f"{qp['wal_state_bytes'] / 1000:.1f} KB",
        "wal_bytes": f"{qp['wal_state_bytes']:,}",
        "late_wins": str(rec["affected_late_segment_windows_recomputed"]),
        "late_slots": str(rec["affected_late_windows_recomputed"]),
        "healed_lags": str(rec["healed_downstream_lag_windows"]),
        "rescored_wins": str(rec["healed_downstream_rescored_windows"]),
        "prob_changed": str(rec["rescore_prob_changed_windows_count"]),
        "prov_retained": str(rec["Phase_B_in_flight_provisional_retained"]),
    }


def _load_ground_truth() -> dict[str, str]:
    metrics = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    manifest = json.loads((OUTPUTS_DIR / "model_registry_manifest.json").read_text(encoding="utf-8"))
    mongo = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    spark = json.loads((OUTPUTS_DIR / "spark_runtime_evidence.json").read_text(encoding="utf-8"))
    qp = json.loads((OUTPUTS_DIR / "query_progress.json").read_text(encoding="utf-8"))
    quyen = json.loads((OUTPUTS_DIR / "quyen_validation" / "manifest.json").read_text(encoding="utf-8"))
    gt = _load_core_dataset_gt(metrics, manifest)
    gt.update(_load_ml_gt(metrics))
    gt.update(_load_spark_stream_mongo_gt(metrics, mongo, spark, qp, quyen))
    return gt


def build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    norm = base["Normal"]
    return {
        "cover_title": ParagraphStyle(
            "CoverTitle",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=17.5,
            leading=23.5,
            textColor=NAVY,
            alignment=TA_CENTER,
            spaceBefore=6,
            spaceAfter=4,
        ),
        "cover_en": ParagraphStyle(
            "CoverSubtitleEn",
            parent=norm,
            fontName=FONT_ITALIC,
            fontSize=10.2,
            leading=14.0,
            textColor=TEAL,
            alignment=TA_CENTER,
            spaceAfter=10,
        ),
        "cover_meta": ParagraphStyle(
            "CoverMeta",
            parent=norm,
            fontName=FONT_REG,
            fontSize=8.8,
            leading=13.2,
            textColor=DARK_TEXT,
            alignment=TA_CENTER,
            spaceAfter=6,
        ),
        "front_h1": ParagraphStyle(
            "FrontH1",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=12.0,
            leading=16.0,
            textColor=NAVY,
            spaceBefore=6,
            spaceAfter=6,
            keepWithNext=True,
        ),
        "h1": ParagraphStyle(
            "SectionH1",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=11.4,
            leading=15.4,
            textColor=NAVY,
            spaceBefore=8,
            spaceAfter=4.5,
            keepWithNext=True,
        ),
        "h2": ParagraphStyle(
            "SectionH2",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=9.6,
            leading=13.4,
            textColor=TEAL,
            spaceBefore=6.5,
            spaceAfter=3.0,
            keepWithNext=True,
        ),
        "body": ParagraphStyle(
            "Body",
            parent=norm,
            fontName=FONT_REG,
            fontSize=8.7,
            leading=13.0,
            textColor=DARK_TEXT,
            alignment=TA_JUSTIFY,
            spaceAfter=4.5,
        ),
        "ref_item": ParagraphStyle(
            "RefItem",
            parent=norm,
            fontName=FONT_REG,
            fontSize=8.1,
            leading=11.8,
            textColor=DARK_TEXT,
            leftIndent=18,
            firstLineIndent=-18,
            alignment=TA_LEFT,
            spaceAfter=3.2,
        ),
        "th": ParagraphStyle(
            "TableHead",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=7.7,
            leading=10.4,
            textColor=WHITE,
            alignment=TA_LEFT,
        ),
        "td": ParagraphStyle(
            "TableCell",
            parent=norm,
            fontName=FONT_REG,
            fontSize=7.6,
            leading=10.6,
            textColor=DARK_TEXT,
            alignment=TA_LEFT,
        ),
        "td_bold": ParagraphStyle(
            "TableCellBold",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=7.6,
            leading=10.6,
            textColor=NAVY,
            alignment=TA_LEFT,
        ),
        "callout": ParagraphStyle(
            "CalloutText",
            parent=norm,
            fontName=FONT_REG,
            fontSize=8.35,
            leading=12.4,
            textColor=DARK_TEXT,
        ),
        "eq_title": ParagraphStyle(
            "EqTitle",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=8.3,
            leading=11.4,
            textColor=NAVY,
            alignment=TA_LEFT,
        ),
        "code": ParagraphStyle(
            "CodeBlock",
            parent=norm,
            fontName=FONT_MONO,
            fontSize=7.5,
            leading=10.8,
            textColor=colors.HexColor("#E2E8F0"),
        ),
        "fig_caption": ParagraphStyle(
            "FigCaption",
            parent=norm,
            fontName=FONT_ITALIC,
            fontSize=7.85,
            leading=10.9,
            textColor=SLATE,
            spaceBefore=3,
            spaceAfter=6,
        ),
        "tbl_caption": ParagraphStyle(
            "TableCaption",
            parent=norm,
            fontName=FONT_ITALIC,
            fontSize=7.85,
            leading=10.9,
            textColor=SLATE,
            spaceBefore=2.5,
            spaceAfter=6,
        ),
        "toc_l0": ParagraphStyle(
            "TOCL0",
            parent=norm,
            fontName=FONT_BOLD,
            fontSize=8.2,
            leading=11.6,
            textColor=NAVY,
            leftIndent=0,
            rightIndent=28,
            spaceBefore=2.6,
            spaceAfter=0.5,
        ),
        "toc_l1": ParagraphStyle(
            "TOCL1",
            parent=norm,
            fontName=FONT_REG,
            fontSize=7.7,
            leading=10.6,
            textColor=DARK_TEXT,
            leftIndent=14,
            rightIndent=28,
            spaceBefore=0.5,
            spaceAfter=0.3,
        ),
        "lof_item": ParagraphStyle(
            "LOFItem",
            parent=norm,
            fontName=FONT_REG,
            fontSize=7.6,
            leading=10.4,
            textColor=DARK_TEXT,
            leftIndent=0,
            rightIndent=28,
            spaceBefore=0.8,
            spaceAfter=0.4,
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
                ("TOPPADDING", (0, 0), (-1, -1), 5.0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5.0),
            ]
        )
    )
    return tbl


def make_equation_box(
    eq_num: str,
    eq_title: str,
    img_path: Path,
    note_html: str,
    styles: dict[str, ParagraphStyle],
    width_ratio: float = 0.92,
) -> KeepTogether:
    rows: list[list[Any]] = [[Paragraph(f"Công thức ({eq_num}) — {eq_title}", styles["eq_title"])]]
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
                ("ALIGN", (0, 1), (-1, 1), "CENTER"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 4.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4.5),
            ]
        )
    )
    return KeepTogether([tbl, Spacer(1, 4)])


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
    vpad: float = 3.6,
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
    cmds: list[tuple[Any, ...]] = [
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.45, BORDER_GRAY),
        ("LEFTPADDING", (0, 0), (-1, -1), 4.8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4.8),
        ("TOPPADDING", (0, 0), (-1, -1), vpad),
        ("BOTTOMPADDING", (0, 0), (-1, -1), vpad),
    ]
    for r_idx in range(1, len(data)):
        bg = WHITE if r_idx % 2 == 1 else ALT_ROW
        if highlight_row is not None and r_idx == highlight_row:
            bg = OK_BG
        cmds.append(("BACKGROUND", (0, r_idx), (-1, r_idx), bg))
    tbl.setStyle(TableStyle(cmds))
    return tbl


def _add_captioned_table(
    story: list[Any],
    headers: list[str],
    rows: list[list[str]],
    col_widths: list[float],
    caption: str,
    styles: dict[str, ParagraphStyle],
    bold_first_col: bool = True,
    highlight_row: int | None = None,
    keep_together: bool = False,
    vpad: float = 3.6,
) -> None:
    tbl = make_table(
        headers,
        rows,
        col_widths,
        styles,
        bold_first_col=bold_first_col,
        highlight_row=highlight_row,
        vpad=vpad,
    )
    cap_p = Paragraph(caption, styles["tbl_caption"])
    if keep_together:
        story.append(KeepTogether([tbl, cap_p]))
    else:
        story.append(tbl)
        story.append(cap_p)


def _add_centered_figure(
    story: list[Any],
    img_path: Path,
    width_ratio: float,
    caption: str,
    styles: dict[str, ParagraphStyle],
) -> None:
    if not img_path.is_file():
        return
    with PILImage.open(img_path) as im:
        w_px, h_px = im.size
    img_w = CONTENT_W * width_ratio
    img_h = img_w * (h_px / max(w_px, 1))
    img = Image(str(img_path), width=img_w, height=img_h)
    img.hAlign = "CENTER"
    story.append(KeepTogether([img, Paragraph(caption, styles["fig_caption"])]))


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
    title_size: float = 6.7,
) -> None:
    d.add(Rect(x, y, w, h, rx=3.5, ry=3.5, fillColor=fill_color, strokeColor=header_fill, strokeWidth=0.8))
    hh = 15.0
    d.add(Rect(x, y + h - hh, w, hh, rx=3.5, ry=3.5, fillColor=header_fill, strokeColor=header_fill, strokeWidth=0.5))
    d.add(String(x + 5.5, y + h - 10.6, title, fontName=FONT_BOLD, fontSize=title_size, fillColor=WHITE))
    for i, txt in enumerate(lines):
        d.add(String(x + 5.5, y + h - hh - 10.5 - i * 10.0, txt, fontName=FONT_REG, fontSize=6.4, fillColor=DARK_TEXT))


def _draw_h_arrow(d: Drawing, x1: float, y1: float, x2: float, y2: float, col=TEAL) -> None:
    d.add(Line(x1, y1, x2, y2, strokeColor=col, strokeWidth=1.2))
    ang_sign = 1.0 if x2 >= x1 else -1.0
    pts = [x2, y2, x2 - 4.8 * ang_sign, y2 + 2.6, x2 - 4.8 * ang_sign, y2 - 2.6]
    d.add(Polygon(pts, fillColor=col, strokeColor=col))


def build_cover_banner() -> Drawing:
    d = Drawing(CONTENT_W, 48)
    d.add(Rect(0, 2, CONTENT_W, 44, rx=5.0, ry=5.0, fillColor=NAVY, strokeColor=NAVY, strokeWidth=0.6))
    d.add(Rect(0, 2, 7.5, 44, rx=2.5, ry=2.5, fillColor=colors.HexColor("#38BDF8"), strokeColor=NAVY, strokeWidth=0.0))
    d.add(
        String(
            18,
            31.5,
            "ĐẠI HỌC FPT · VIỆN QUẢN TRỊ & CÔNG NGHỆ FSB (FPT SCHOOL OF BUSINESS & TECHNOLOGY)",
            fontName=FONT_BOLD,
            fontSize=8.5,
            fillColor=WHITE,
        )
    )
    d.add(
        String(
            18,
            19.5,
            "CHƯƠNG TRÌNH THẠC SĨ KỸ THUẬT PHẦN MỀM CHUYÊN NGÀNH AI (MSE AI) · HỌC PHẦN BDA501 — BIG DATA ANALYTICS",
            fontName=FONT_REG,
            fontSize=7.2,
            fillColor=colors.HexColor("#BAE6FD"),
        )
    )
    d.add(
        String(
            18,
            8.2,
            "BÁO CÁO TỔNG KẾT ĐỒ ÁN & ĐẶC TẢ KỸ THUẬT CHI TIẾT (FINAL PROJECT REPORT & TECHNICAL SPECIFICATION)",
            fontName=FONT_BOLD,
            fontSize=7.1,
            fillColor=colors.HexColor("#E0F2FE"),
        )
    )
    return d


def build_boundary_drawing() -> Drawing:
    d = Drawing(CONTENT_W, 92)
    _draw_card_box(
        d,
        2,
        5,
        152,
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
        168,
        5,
        172,
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
    _draw_h_arrow(d, 154, 46, 168, 46, NAVY)
    _draw_h_arrow(d, 340, 46, 354, 46, TEAL)
    return d


def build_mlops_drawing() -> Drawing:
    d = Drawing(CONTENT_W, 82)
    _draw_card_box(
        d,
        2,
        5,
        168,
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
        title_size=6.35,
    )
    _draw_card_box(
        d,
        184,
        5,
        156,
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
        title_size=6.35,
    )
    _draw_card_box(
        d,
        354,
        5,
        159,
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
        title_size=6.35,
    )
    _draw_h_arrow(d, 170, 41, 184, 41, GREEN)
    _draw_h_arrow(d, 340, 41, 354, 41, BLUE)
    return d


def _append_cover_page(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(build_cover_banner())
    story.append(Spacer(1, 14))
    story.append(
        Paragraph(
            "HỆ THỐNG GIÁM SÁT HIỆN TRẠNG &amp; DỰ BÁO ÙN TẮC<br/>GIAO THÔNG ĐÔ THỊ TP. HỒ CHÍ MINH",
            s["cover_title"],
        )
    )
    story.append(
        Paragraph(
            "HCMC Smart Traffic Big Data Analytics &amp; +30-Minute Congestion Prediction Platform",
            s["cover_en"],
        )
    )
    story.append(
        Paragraph(
            "<b>Học phần:</b> BDA501 — Big Data Analytics (MSE AI · Fall 2026) · "
            "<b>Giảng viên hướng dẫn:</b> Đặng Trần Khánh",
            s["cover_meta"],
        )
    )
    story.append(Spacer(1, 6))

    author_rows = [
        [
            "1. Nguyễn Tạ Minh Quyền",
            "25MS23280",
            "Bước 1 (P1): Ordered Quality Funnel, Train-Only Vmax Imputation &amp; Xử lý phân tán Apache Spark",
        ],
        [
            "2. Nguyễn Thành Đạt",
            "25MS23306",
            "Bước 2 &amp; 3 (P2–P3): Đồ thị Line-Graph PageRank, 7D K-Means Cold-Start &amp; Dự báo +30 phút",
        ],
        [
            "3. Phạm Công Minh",
            "25MS23310",
            "Bước 4 (P4): Luồng Kafka 4.1, O(1) Sketches 33 KiB, Đối soát WAL, MongoDB 8.0 &amp; Web TOC :8000",
        ],
    ]
    story.append(
        make_table(
            ["Thành viên Thực hiện", "MSSV", "Phạm vi Chuyên trách Chính"],
            author_rows,
            [132, 72, 311],
            s,
        )
    )
    story.append(Spacer(1, 8))
    story.append(
        make_callout(
            (
                "<b>Xuất xứ Bộ dữ liệu Thực nghiệm (Dataset Provenance — Kaggle v6):</b> "
                "<i>thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam</i> · "
                "<b>Version 6</b> (cập nhật 2021-05-20) · Giấy phép <b>CC0: Public Domain</b> · "
                "Nguồn gốc hệ thống <b>BkTraffic (ĐH Bách Khoa TP.HCM / HCMUT)</b> · "
                f"Quy mô <b>5 tệp CSV / 41 cột / 38.50 MB</b> (36.72 MiB), giai đoạn 07/2020–04/2021 "
                f"(<b>{gt['raw_pings']}</b> bản tin GPS thô, <b>{gt['raw_segs']}</b> đoạn đường thô, "
                "<b>46 / 48</b> khung 30 phút UTC có quan sát)."
            ),
            s,
            bg=CALLOUT_BG,
            border=CALLOUT_BORDER,
        )
    )
    story.append(Spacer(1, 8))
    story.append(
        make_callout(
            (
                "<b>Tóm tắt điều hành (Executive Summary — Toàn cảnh bài toán &amp; lời giải trong 4 bước):</b><br/>"
                "<b>Mục tiêu:</b> Giám sát hiện trạng và dự báo trước 30 phút nguy cơ kẹt xe trên toàn mạng lưới "
                f"<b>{gt['clean_segs']} đoạn đường cơ giới một chiều</b> tại TP.HCM (lọc 427 lối đi bộ/POI — "
                "<i>Point of Interest</i>, địa điểm/công trình phi đường bộ từ bản đồ OSM — từ "
                f"{gt['raw_segs']} đoạn thô) từ dữ liệu GPS hành trình (ví dụ lúc 07:08 sáng trên đoạn Lý Thường Kiệt "
                "mã 47407 tại Quận 10, phương tiện báo vận tốc chỉ 4 km/h). Thực địa gặp <b>3 rào cản lớn</b>: "
                f"(1) <b>{gt['cold_pct']}</b> đoạn đường ({gt['cold_segs']} nhánh/hẻm) trống GPS và 88.34% đoạn thiếu "
                "tốc độ tối đa; (2) tín hiệu đứt quãng dài (66.30% cách nhau ≥ 60 phút) kèm 7,192 cửa sổ burst tổng "
                "hợp; (3) luồng trực tuyến lẫn bản tin trùng lặp và đến trễ quá hạn.<br/>"
                "• <b>Bước 1 (P1 — Chuẩn hóa hiện trạng &amp; Spark ETL):</b> Làm sạch "
                f"{gt['raw_pings']} → <b>{gt['clean_pings']}</b> pings cơ giới, ước lượng Vmax 3 bậc chỉ học từ TRAIN "
                f"hữu cơ, gộp Pure 5-Tuple Monoid thành <b>{gt['windows_n']} cửa sổ 30 phút</b> (sai số 0.0 so với "
                f"<i>baseline.py</i> — mã Python tham chiếu đơn máy; <b>{gt['los_match']}</b> khớp chuẩn BkTraffic)."
                "<br/>"
                "• <b>Bước 2 (P2 — Lấp vùng mù Cold-Start 88.13%):</b> Tỉa 90.97% điểm hình học phụ, dựng Line-Graph "
                f"PageRank không U-turn ({gt['lg_edges']} cạnh, 30 vòng lặp) và phân 6 cụm 7D K-Means "
                f"(Silhouette = {gt['km_sil']}) phủ kín <b>100%</b> {gt['clean_segs']} đoạn cơ giới.<br/>"
                "• <b>Bước 3 (P3 — Dự báo Predictive ML +30 phút):</b> Lọc bỏ 3,596 cặp burst khỏi tập học để giữ "
                f"<b>{gt['train_n']} organic TRAIN / {gt['val_n']} VAL / {gt['test_n']} TEST</b>; Random Forest 23D "
                f"thắng cổng VAL (0.8882) đạt <b>ROC-AUC = {gt['rf_roc']}</b>, <b>PR-AUC = {gt['rf_pr']}</b>, "
                f"<b>Severe F1 = {gt['rf_f1']}</b>, <b>Acc = {gt['rf_acc']}</b> (McNemar p = {gt['mcnemar_p']} trên "
                "các ca đổi trạng thái).<br/>"
                "• <b>Bước 4 (P4 — Luồng Kafka, O(1) Sketches 33 KiB, Recon &amp; Web TOC :8000):</b> Chạy trên 5 dịch "
                "vụ Docker (11.0 GB RAM), 32.0 KiB RotatingBloomFilter + 1.0 KiB HLL++ + WAL 73.7 KB, phục vụ MongoDB "
                f"8.0 IXSCAN (<b>{gt['mongo_med']}</b> median / <b>{gt['mongo_p95']}</b> p95, nhanh hơn "
                f"<b>{gt['mongo_speedup']}</b>) qua Web TOC (<i>Traffic Operations Center</i> — Trung tâm Điều hành "
                f"Giao thông), đối soát bù {gt['late_wins']} cửa sổ trễ và hàn gắn "
                f"{gt['healed_lags']} lag đạt độ đầy đủ <b>100.0%</b>; xác thực tự động <b>44/44 pytest</b>."
            ),
            s,
            bg=OK_BG,
            border=OK_BORDER,
        )
    )
    story.append(Spacer(1, 10))
    story.append(
        Paragraph(
            "<b>TP. Hồ Chí Minh · Học kỳ Fall 2026</b> — Viện Quản trị &amp; Công nghệ FSB · Đại học FPT",
            s["cover_meta"],
        )
    )
    story.append(PageBreak())


def _append_front_matter_toc(story: list[Any], s: dict[str, ParagraphStyle]) -> None:
    story.append(Paragraph("MỤC LỤC (TABLE OF CONTENTS)", s["front_h1"]))
    toc = TaggedTOC(notify_kind="TOCEntry")
    toc.dotsMinLevel = 0
    toc.levelStyles = [s["toc_l0"], s["toc_l1"]]
    story.append(toc)
    story.append(Spacer(1, 10))

    story.append(CondPageBreak(200))
    story.append(Paragraph("DANH MỤC HÌNH (LIST OF FIGURES)", s["front_h1"]))
    lof = TaggedTOC(notify_kind="LOFEntry")
    lof.dotsMinLevel = 0
    lof.levelStyles = [s["lof_item"]]
    story.append(lof)
    story.append(Spacer(1, 10))

    story.append(CondPageBreak(200))
    story.append(Paragraph("DANH MỤC BẢNG (LIST OF TABLES)", s["front_h1"]))
    lot = TaggedTOC(notify_kind="LOTEntry")
    lot.dotsMinLevel = 0
    lot.levelStyles = [s["lof_item"]]
    story.append(lot)
    story.append(PageBreak())


def _glossary_core_rows() -> list[list[str]]:
    return [
        [
            "TOC",
            "<i>Traffic Operations Center</i> — Trung tâm Điều hành Giao thông đô thị (cổng Web TOC :8000; "
            "<b>tránh nhầm</b> với <i>Table of Contents</i> — Mục lục tài liệu).",
        ],
        [
            "LOS (A–F)",
            "<i>Level of Service</i> — 6 mức phục vụ giao thông từ A (thông thoáng) đến F (kẹt cứng) theo thang "
            "đô thị BkTraffic (<i>derive_urban_los</i>); mức E–F gọi là kẹt nặng (<i>Severe</i>).",
        ],
        [
            "POI",
            "<i>Point of Interest</i> — địa điểm, công trình hoặc lối đi bộ phi cơ giới từ bản đồ OSM (công viên, "
            "sân chơi, đường bộ hành...) bị lọc khỏi mạng lưới đường cơ giới.",
        ],
        [
            "Persistence Baseline",
            "Mốc đối chứng tối giản <b>0-ML</b> do chính nhóm tự thiết lập theo quy tắc "
            "<i>LOS<sub>dự báo</sub>(t+30p) = LOS(t)</i> (sao chép nguyên hiện trạng, đại diện cho bản đồ chỉ giám "
            "sát chưa có AI).",
        ],
        [
            "Burst tổng hợp &amp;\nOrganic TRAIN",
            "Đợt dồn bản tin dị thường sáng 03/08/2020 ICT (36,096 pings = 39.7% tổng 10 tháng, mật độ đỉnh ~685x "
            "trung vị, tạo 7,192 cửa sổ); tách ra để <i>Organic TRAIN</i> (1,885 cặp) chỉ học ngày tự nhiên.",
        ],
        [
            "VAL Composite Gate",
            "Cổng tuyển chọn mô hình duy nhất trên tập kiểm định VAL bằng trung bình "
            "<i>(val_acc + val_pr + val_roc) / 3</i>, chống chọn thiên lệch nhìn trộm TEST (<i>test-set peeking</i>).",
        ],
        [
            "Masked-GPS Proxy",
            "Kịch bản kiểm định giả lập vùng mù 0-GPS: lấy các đoạn có GPS trên TEST, cố tình che toàn bộ tín hiệu "
            "GPS hiện tại thành <i>NaN</i> rồi chấm lại đối chiếu với nhãn LOS thật.",
        ],
        [
            "Future-Shuffle",
            "Kiểm thử chống rò rỉ tương lai: đảo trộn ngẫu nhiên toàn bộ quan sát ở <i>τ &gt; t</i> và xác nhận mọi "
            "biến đầu vào tại thời điểm <i>t</i> không đổi tuyệt đối (<i>max_abs_diff = 0.0</i>).",
        ],
        [
            "McNemar / Brier /\nSilhouette",
            "<i>McNemar</i>: kiểm định ghép cặp phi tham số trên cùng 61 ca đổi pha; <i>Brier Score</i> ∈ [0, 1]: "
            "sai số bình phương xác suất (càng gần 0 càng tốt); <i>Silhouette</i> ∈ [−1, +1]: độ tách cụm K-Means.",
        ],
        [
            "ROC-AUC / PR-AUC /\nMacro-F1",
            "<i>ROC-AUC</i> (đường cong thu nhận) &amp; <i>PR-AUC</i> (<i>Precision–Recall AUC</i> — bắt buộc nhìn "
            "trên tập lệch 81.13% LOS A để đo thực chất lớp kẹt hiếm); <i>Macro-F1</i>: trung bình F1 đều 6 lớp.",
        ],
        [
            "5-Tuple Monoid",
            "Cấu trúc đại số giao hoán – kết hợp <i>(cnt, sum_v, min_v, max_v, severe_cnt)</i> với phần tử đơn vị "
            "<i>(0, 0.0, +∞, −∞, 0)</i> giúp gộp phân tán song song sai số 0.0 (tránh bẫy trung bình từng phần).",
        ],
        [
            "Key Salting",
            "Kỹ thuật chia khóa nóng giờ cao điểm sang 8 giỏ phụ (<i>salt buckets 0..7</i>) ở bước gộp cục bộ rồi "
            "gộp lại ở bước 2, hạ lệch tải (<i>Data Skew</i>) từ 684.95x xuống 1.020x.",
        ],
        [
            "BHJ / SortMergeJoin\n/ IQR",
            "<i>BroadcastHashJoin (BHJ)</i>: phát tán bảng chiều nhỏ vào RAM executor loại bỏ Shuffle vs "
            "<i>SortMergeJoin</i> (sắp xếp và xáo trộn hai phía); <i>IQR</i>: khoảng tứ phân vị Q3 − Q1.",
        ],
        [
            "Line-Graph PageRank",
            "Thuật toán PageRank chạy trên đồ thị nối đoạn đường có hướng (mỗi đoạn cơ giới là 1 đỉnh, bỏ cung quay "
            "đầu U-turn) để xếp hạng nút cổ chai cấu trúc ngay cả vùng 0-GPS.",
        ],
        [
            "SON / FPGrowth /\nLift",
            "Thuật toán khai phá tập phổ biến phân tán 2 lượt <i>Savasere–Omiecinski–Navathe (SON)</i> &amp; "
            "<i>FPGrowth</i>; hệ số <i>Lift &gt; 1</i> đo mức tăng xác suất hai tuyến cùng kẹt vượt ngẫu nhiên.",
        ],
        [
            "WAL",
            "<i>Write-Ahead Log</i> — nhật ký ghi trước trạng thái xuống đĩa trước khi xác nhận xử lý micro-batch, "
            "giúp tự hồi phục sau sự cố sập tiến trình mà không nhân đôi bản ghi (0 duplicates).",
        ],
        [
            "FPR",
            "<i>False Positive Rate</i> — tỷ lệ báo trùng giả của bộ lọc xác suất RotatingBloomFilter (đo thực "
            "nghiệm 0.0389%, thấp hơn cận lý thuyết 0.0792%).",
        ],
        [
            "HLL++",
            "<i>HyperLogLog++</i> — cấu trúc phác thảo xác suất 64-bit ước lượng số đoạn đường phân biệt trong bộ "
            "nhớ hằng số O(1) chỉ 1.0 KiB (sai số thực nghiệm 3.75%).",
        ],
        [
            "EWMA",
            "<i>Exponentially Weighted Moving Average</i> — trung bình trượt hàm mũ (α = 0.15) ưu tiên khung gần "
            "nhất để theo dõi độ lì điểm nóng kẹt xe liên tiếp ngay hiện tại.",
        ],
        [
            "ESR / IXSCAN /\nCOLLSCAN",
            "Quy tắc chỉ mục ghép MongoDB <i>Equality – Sort – Range (ESR)</i> giúp chạy <i>Index Scan (IXSCAN)</i> "
            "trên cây B-Tree dưới 1 ms (0 SORT RAM) thay vì <i>Collection Scan (COLLSCAN)</i> quét toàn bảng.",
        ],
        [
            "PSI",
            "<i>Population Stability Index</i> — chỉ số đo độ trôi dạt phân phối đặc trưng đầu vào giữa dữ liệu vận "
            "hành mới và tập huấn luyện nhằm kích hoạt cảnh báo tái huấn luyện.",
        ],
        [
            "KRaft",
            "<i>Kafka Raft</i> — giao thức đồng thuận quản lý metadata tích hợp sẵn trong Apache Kafka 4.1.0, loại "
            "bỏ hoàn toàn cụm ZooKeeper phụ thuộc.",
        ],
        [
            "ICT / UTC",
            "<i>Indochina Time</i> (giờ Việt Nam UTC+7) và <i>Coordinated Universal Time</i> (giờ phối hợp quốc tế — "
            "mốc khóa gốc của <i>segment_status.csv</i>; độ lệch +7h làm đổi ngày 12.89% cửa sổ).",
        ],
        [
            "OSM / GIS",
            "<i>OpenStreetMap</i> &amp; <i>Geographic Information System</i> — hệ thống thông tin địa lý cung cấp "
            "mạng lưới nút giao (<i>nodes.csv</i>), tuyến đường (<i>streets.csv</i>) và đoạn đường "
            "(<i>segments.csv</i>).",
        ],
    ]


def _glossary_disambiguation_rows() -> list[list[str]]:
    return [
        [
            "[I] vs [P]",
            "<b>[I] (Implemented)</b>: hạng mục đã lập trình, chạy thật và đo kiểm trong cụm Docker 11.0 GB RAM. "
            "<b>[P] (Proposed / Production Target)</b>: kiến trúc đề xuất mở rộng Cloud / Kubernetes / YARN.",
        ],
        [
            "3 ngữ cảnh của từ\n«Baseline»",
            "<b>(1) Persistence Baseline</b>: mốc đối chứng 0-ML dự báo <i>LOS<sub>dự báo</sub>(t+30p) = LOS(t)</i> "
            "(Chương 5). <b>(2) Spark Mặc định (SortMergeJoin / Unsalted)</b>: cấu hình chưa tối ưu để đo tốc độ cải "
            "thiện (Chương 3). <b>(3) baseline.py</b>: mã Python tham chiếu đơn máy kiểm sai số Monoid = 0.0 "
            "(Chương 1, 3).",
        ],
        [
            "2 nghĩa của từ\n«Persistence»",
            "<b>(1) Persistence Baseline</b> (Chương 5): quy tắc dự báo giữ nguyên mức LOS hiện tại sau 30 phút. "
            "<b>(2) persistence_controlled = True</b> (Chương 4): bộ lọc loại tuyến đích đã kẹt sẵn ở <i>t</i> khi "
            "khai phá luật lan truyền trễ <i>t → t+30p</i>.",
        ],
        [
            "v1 vs v2\n($status_version)",
            "Nhãn vòng đời của <b>bản ghi tài liệu trong MongoDB</b> (<b>v1</b> = dự báo tạm thời "
            "<i>STREAM_PROVISIONAL</i> từ Speed Layer; <b>v2</b> = bản ghi chuẩn <i>BATCH_VERIFIED / "
            "BATCH_RECONCILED</i> sau đối soát Batch Layer), <b>không phải</b> phiên bản mô hình ML.",
        ],
        [
            "Phase A/B/C vs\nGiai đoạn 1/2/3 &amp;\nTier 1–3 vs Tier-1/2",
            "<b>Phase A / B / C</b> là 3 pha vòng đời MLOps không xung đột (Chương 3), tách biệt với "
            "<b>Giai đoạn 1 / 2 / 3</b> là lộ trình phát triển 6 tháng sau nghiệm thu (Chương 7). "
            "<b>Tier 1–3</b> là 3 bậc nội suy Vmax không rò rỉ (Chương 2), tách biệt với "
            "<b>Tier-1 / Tier-2</b> là 2 tầng đối soát Batch–Stream (Chương 6).",
        ],
        [
            "Tiền tố causal_prior\n(street_causal_prior_*)",
            "Biểu thị <b>tiên nghiệm quá khứ chặt theo trục thời gian (τ &lt; t, causal temporal filtering)</b> — "
            "chỉ cộng dồn dữ liệu đã xảy ra trước thời điểm <i>t</i> nhằm chống rò rỉ tương lai, <b>không phải</b> "
            "suy luận nhân quả can thiệp (<i>do-calculus</i>).",
        ],
    ]


def _append_front_matter_glossary(story: list[Any], s: dict[str, ParagraphStyle]) -> None:
    story.append(Paragraph("Danh Mục Thuật Ngữ, Chữ Viết Tắt &amp; Quy Ước Ký Hiệu", s["h1"]))
    story.append(
        Paragraph(
            (
                "Phần này hệ thống hóa các thuật ngữ chuyên ngành, chữ viết tắt cốt lõi (Bảng 0.1) và quy ước ký "
                "hiệu phân tách những khái niệm dễ nhầm lẫn (Bảng 0.2) nhằm bảo đảm cách hiểu thống nhất từ Chương 1 "
                "đến Chương 8."
            ),
            s["body"],
        )
    )
    _add_captioned_table(
        story,
        ["Thuật ngữ / Ký hiệu", "Tên đầy đủ, Định nghĩa Ngắn gọn &amp; Ngữ cảnh Trong Hệ thống"],
        _glossary_core_rows(),
        [116, 399],
        "Bảng 0.1. Danh mục 24 thuật ngữ kỹ thuật và chữ viết tắt cốt lõi sử dụng xuyên suốt báo cáo.",
        s,
        vpad=2.1,
    )
    story.append(CondPageBreak(260))
    _add_captioned_table(
        story,
        ["Ký hiệu / Khái niệm", "Quy ước Phân tách Ngữ nghĩa &amp; Ngữ cảnh Áp dụng"],
        _glossary_disambiguation_rows(),
        [116, 399],
        "Bảng 0.2. Quy ước ký hiệu và phân tách dứt khoát sáu nhóm thuật ngữ dễ nhầm lẫn trong báo cáo.",
        s,
        keep_together=True,
    )
    story.append(Spacer(1, 6))
    story.append(
        make_callout(
            (
                "<b>Hướng dẫn tra cứu nhanh hệ thống mã chỉ mục trong báo cáo:</b> "
                "<b>P1–P4</b> là 4 bài toán con nối tiếp (Mô tả → Chẩn đoán → Dự báo → Hỗ trợ quyết định, Chương 1); "
                "<b>UC-1–UC-6</b> là 6 kịch bản vận hành thực địa (Bảng 1.2 &amp; nghiệm thu KPI ở Bảng 8.1); "
                "<b>EC-1–EC-10</b> là 10 tình huống biên thực nghiệm của dữ liệu TP.HCM và <b>R1–R6</b> là 6 quy tắc "
                "của phễu làm sạch <i>Ordered Quality Funnel</i> (Chương 2); "
                "<b>[C1]–[C17]</b> là 17 thành phần kỹ thuật phân bố trên 6 phân vùng <b>Zones A–F</b> kèm 10 luồng "
                "dữ liệu <b>Flows 1–10</b> (Chương 3); và <b>F1–F3</b> là 3 kịch bản kiểm định khôi phục sự cố sản "
                "xuất trên Kafka/WAL, MongoDB và Spark (Chương 7)."
            ),
            s,
            bg=CALLOUT_BG,
            border=CALLOUT_BORDER,
        )
    )
    story.append(PageBreak())


def _append_ch1_context(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
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
                "đèn và phân luồng chủ động; <b>(3)</b> trực ban sự vụ tức thời tại Trung tâm Điều hành Giao thông "
                "(<i>TOC — Traffic Operations Center</i>, tránh nhầm với Mục lục tài liệu) vẫn tách rời khỏi quy hoạch "
                "điểm nghẽn dài hạn của Sở GTVT. "
                "Quan sát một bản ghi thực địa trong bảng <i>segment_status.csv</i>: vào lúc <b>07:08:28 sáng giờ "
                "Việt Nam</b> ngày 03/08/2020 (bản ghi mã <i>15281</i>), một phương tiện đi ngang qua <b>đoạn đường "
                "Lý Thường Kiệt số 47407</b> (dài 16m tại Quận 10) gửi về vận tốc tức thời chỉ <b>4 km/h</b>; hoặc lúc "
                "<b>11:30 trưa</b> tại <b>Công Trường Quốc Tế (đoạn 21064, Quận 3)</b>, xe báo vận tốc <b>12 km/h</b>. "
                "Để chuyển đổi hàng chục nghìn bản ghi vận tốc rời rạc thành bức tranh điều hành nhất quán trên toàn "
                f"mạng lưới <b>{gt['clean_segs']} đoạn cơ giới một chiều</b> (Hình 1.1), hệ thống quy đổi vận tốc theo "
                "<b>6 mức phục vụ giao thông (Level of Service — LOS từ A đến F)</b> theo chuẩn đô thị BkTraffic "
                "(<i>derive_urban_los</i>, khớp <b>89.19%</b> nhãn gốc, đồng thời tách biệt với hàm chuẩn hóa tỷ lệ "
                "<i>derive_los</i>): <b>mức A–B</b> là đường thông thoáng, xe chạy tự do; <b>mức C–D</b> là đường đông "
                "đúc giờ cao điểm; còn <b>mức E–F</b> là ùn ứ nghiêm trọng đến kẹt cứng tê liệt."
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "map_hcmc_hotspots_coldstart.png",
        0.74,
        f"Hình 1.1. Bản đồ giao thông TP.HCM tái tạo từ {gt['clean_segs']} đoạn đường cơ giới cùng sông Sài Gòn và "
        f"các quận trung tâm: phần xám nhạt là {gt['cold_segs']} đoạn nhánh trống GPS ({gt['cold_pct']}), các trục "
        "đỏ là điểm nóng hay kẹt cứng.",
        s,
    )
    story.append(
        make_callout(
            (
                "<b>Vai trò mẫu chuẩn kiểm chứng quy mô (Proof-of-Scale — Vì sao dùng Spark và Kafka cho tập dữ liệu "
                "38.50 MB?):</b> Năm bảng công khai của bộ dữ liệu giao thông TP.HCM trên Kaggle v6 [16] có tổng dung "
                "lượng <b>38.50 MB</b> (riêng bảng vận tốc <i>segment_status.csv</i> nặng <b>3.58 MB</b> với "
                f"{gt['raw_pings']} bản tin GPS). Nếu chỉ tính toán một lần trên tệp tĩnh, Pandas trên máy cá nhân chỉ "
                "mất vài chục mili-giây. Tuy nhiên, trong học phần Dữ liệu lớn, tập dữ liệu này đóng vai trò <b>mẫu "
                "chuẩn kiểm chứng quy mô (Proof-of-Scale)</b>: khi kết nối thực tế hơn 1 triệu xe buýt, taxi, xe công "
                "nghệ và xe tải phát tín hiệu mỗi 5–10 giây, hệ thống sẽ phải tiếp nhận <b>8–17 tỷ bản tin/ngày "
                "(~1.5–3.0 TB/ngày)</b>. Ở quy mô đó, lệch tải giờ cao điểm (gấp 684.95 lần trung vị) và bộ nhớ lọc "
                "trùng không giới hạn sẽ làm tràn RAM máy đơn. Chạy kiến trúc phân tán trên mẫu chuẩn cho phép đối "
                "chứng từng phép tính với <i>baseline.py</i> (mã Python tham chiếu đơn máy tính tổng hợp trực tiếp) "
                "đạt <b>sai số 0.0</b> trước khi mở rộng cụm máy chủ."
            ),
            s,
        )
    )


def _append_ch1_problems_and_usecases(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(Paragraph("1.2. Sự Liên Kết Nhân Quả Giữa Bốn Bài Toán Con (Coupled Sub-Problems P1 → P4)", s["h2"]))
    story.append(
        Paragraph(
            (
                "Bốn bài toán con <b>P1, P2, P3, P4</b> vận hành như <b>bốn mắt xích nhân quả nối tiếp nhau</b> "
                "(minh họa xuyên suốt tại Hình 1.2 và tổng hợp hợp đồng dữ liệu ở Bảng 1.1): kết quả chuẩn hóa của "
                "Bước 1 (P1) nuôi dữ liệu cho Bước 2 (P2); đặc trưng nút cổ chai (<i>Line-Graph PageRank</i>) và nhóm "
                "hành vi đường phố (<i>7D K-Means</i>) ở P2 được truyền thẳng làm đặc trưng bản đồ tĩnh cho mô hình "
                "dự báo Bước 3 (P3); và mô hình P3 được đóng băng phục vụ chấm điểm trực tuyến cùng giám sát điểm "
                "nóng tức thời ở Bước 4 (P4)."
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_concrete_example_flow.png",
        0.94,
        "Hình 1.2. Ví dụ minh họa xuyên suốt (End-to-End Walkthrough): Hành trình bản tin GPS thực tế trên đoạn "
        "Lý Thường Kiệt (segment_id = 47407) đi qua 4 bước xử lý liên kết nhân quả (P1 → P4).",
        s,
    )
    p_rows = [
        [
            "Bước 1 (P1)\nDescriptive",
            "Hiện tại và trong lịch sử, đoạn đường nào đang thông thoáng hay kẹt cứng?",
            f"{gt['raw_pings']} pings thô ({gt['clean_pings']} hợp lệ); {gt['miss_vmax_seg_pct']} đoạn thiếu Vmax.",
            (
                f"{gt['windows_n']} cửa sổ 30 phút gắn LOS A–F (sai số 0.0 vs baseline.py, {gt['los_match']} khớp "
                "BkTraffic), tạo nền dữ liệu sạch cho Bước 2 và Bước 3."
            ),
        ],
        [
            "Bước 2 (P2)\nDiagnostic",
            f"Làm sao hiểu các nút cổ chai và {gt['cold_pct']} đoạn nhánh Cold-Start (0 GPS)?",
            f"Đồ thị {gt['clean_segs']} đoạn cơ giới ({gt['lg_edges']} cạnh bỏ U-turn) và giỏ LOS E/F trên TRAIN.",
            (
                "Điểm nút cổ chai Line-Graph PageRank, luật SON/FPGrowth và 6 cụm 7D K-Means — phục vụ Sở GTVT và "
                "cấp Static Features cho Bước 3."
            ),
        ],
        [
            "Bước 3 (P3)\nPredictive",
            "Đúng 30 phút nữa (t + 30m), đoạn đường nào sẽ chuyển sang kẹt cứng?",
            f"{gt['t30m_pairs']} cặp +30m ({gt['train_n']} organic TRAIN sau tách burst), giờ VN và Static Features.",
            (
                f"Random Forest (23D) dự báo LOS và xác suất kẹt sau 30 phút (ROC-AUC = {gt['rf_roc']}, "
                f"PR-AUC = {gt['rf_pr']}, Severe F1 = {gt['rf_f1']}), đóng băng cho Bước 4."
            ),
        ],
        [
            "Bước 4 (P4)\nPrescriptive",
            "Làm sao vừa cảnh báo < 1 ms trên Web TOC, vừa không mất Late Events?",
            f"Luồng Kafka {gt['stream_events']} sự kiện gồm 1% Duplicates ({gt['dupes_caught']}) và {gt['late_wins']} "
            "cửa sổ trễ > 30m.",
            (
                f"O(1) Stream Sketches (33.0 KiB), MongoDB IXSCAN ({gt['mongo_med']} / p95 {gt['mongo_p95']}, "
                f"{gt['mongo_speedup']}) phục vụ TOC (:8000), Recon bù {gt['late_wins']} trễ + {gt['healed_lags']} lag."
            ),
        ],
    ]
    _add_captioned_table(
        story,
        ["Sub-Problem", "Câu hỏi Thực tiễn Cần Giải quyết", "Input Nghiệp vụ", "Output &amp; Vai trò Nối tiếp"],
        p_rows,
        [68, 134, 140, 173],
        "Bảng 1.1. Chuỗi liên kết nhân quả giữa bốn bài toán con (Coupled Sub-Problems P1 → P4) trong hệ thống.",
        s,
    )

    story.append(Paragraph("1.3. Sáu Kịch Bản Vận Hành Thực Tế (Operational Use Cases UC-1–UC-6)", s["h2"]))
    story.append(
        Paragraph(
            (
                "Để bảo đảm mọi mô-đun kỹ thuật đều gắn trực tiếp với quyết định thực địa tại TP.HCM, Bảng 1.2 "
                "chuẩn hóa sáu kịch bản vận hành (<i>UC-1–UC-6</i>) tương ứng với sáu nhóm tác nhân khai thác hệ thống."
            ),
            s["body"],
        )
    )
    uc_rows = [
        [
            "UC-1: Điểm nóng kéo dài\n(EWMA Hotspot)",
            "Trung tâm Điều hành (TOC)",
            "Top-25 đoạn kẹt cứng liên tiếp nhiều giờ chưa thông (#47407 Lý Thường Kiệt 6.6652, #21263 Trương Định).",
            "Phát cảnh báo lên bảng điện tử ngoài đường và cử lực lượng ra điều tiết ngay tại nút giao.",
        ],
        [
            "UC-2: Ưu tiên đèn xanh\n(IXSCAN < 1 ms)",
            "Kỹ sư Tín hiệu Đèn",
            (
                f"Danh sách {gt['arterial_alerts']} trục cấp 1–2 có P(Severe) ≥ 70% sau 30 phút, phản hồi warm "
                f"{gt['mongo_med']} (p95 {gt['mongo_p95']}, nhanh hơn {gt['mongo_speedup']})."
            ),
            "Chủ động kéo dài pha đèn xanh hướng trục chính trước khi dòng xe kịp dồn ứ khóa chặt ngã tư.",
        ],
        [
            "UC-3: Quy hoạch vùng mù\n(Cold-Start Audit)",
            "Chuyên viên Sở GTVT",
            (
                "Bản đồ xếp hạng nút cổ chai PageRank (#1 Nguyễn Kiệm = 2.7799, #2 Cầu vượt Ngã tư Gò Dưa = 2.7196, "
                "#3 Quốc lộ 1 = 2.6894)."
            ),
            "Chọn đúng vị trí trọng yếu (kể cả vùng 0 GPS) để ưu tiên lắp camera giám sát và phân luồng tải trọng.",
        ],
        [
            "UC-4: Né kẹt dây chuyền\n(Co-Congestion Rules)",
            "Ứng dụng Dẫn đường",
            "Cảnh báo cặp đường hay kẹt đồng thời (Nguyễn Thị Minh Khai ↔ Cống Quỳnh, Lift 15.23x) hoặc trễ +30m.",
            "Khuyến nghị tài xế rẽ sang lộ trình thay thế từ các ngã tư phía xa trước khi đi vào vùng ùn tắc.",
        ],
        [
            "UC-5: Phân bổ ca tuần tra\n(7D K-Means Regimes)",
            "Đội Tuần tra Giao thông",
            f"Phân hoạch toàn bộ {gt['clean_segs']} đoạn cơ giới thành 6 cụm hành vi (7D K-Means) &amp; tra cứu Peer.",
            "Tập trung chốt trực đúng Cụm 1 (3,530 đoạn nội đô tỷ lệ kẹt E/F cao nhất 43.26%) vào giờ cao điểm.",
        ],
        [
            "UC-6: Đối soát dữ liệu trễ\n(Two-Tier Recon)",
            "Kỹ sư Dữ liệu / MLOps",
            (
                f"Báo cáo Two-Tier Recon cuối ngày: bù {gt['late_wins']} cửa sổ trễ, hàn {gt['healed_lags']} cửa sổ "
                f"lag và chấm lại {gt['rescored_wins']} dự báo."
            ),
            "Bảo đảm 100% số liệu tổng kết ngày và Medallion Lakehouse khớp tuyệt đối trước khi tái huấn luyện.",
        ],
    ]
    _add_captioned_table(
        story,
        ["Mã Use Case", "Tác Nhân (Actor)", "Thông tin Nhận trên Dashboard", "Quyết định Vận hành Thực tế"],
        uc_rows,
        [100, 86, 174, 155],
        "Bảng 1.2. Sáu kịch bản vận hành thực tế (Operational Use Cases UC-1–UC-6) gắn với từng nhóm người dùng.",
        s,
    )


def _append_ch1_boundary_and_literature(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(Paragraph("1.4. Ranh Giới Hệ Thống (System Boundary: 2 External Inputs → 4 System Outputs)", s["h2"]))
    story.append(
        Paragraph(
            (
                "Như minh họa ở Hình 1.3, hệ thống xác định rõ <b>2 nguồn đầu vào độc lập (External Inputs)</b>: "
                f"(1) <i>GPS Telemetry Stream</i> (<i>producer.py</i>, <i>seed = 42</i>) phát {gt['stream_events']} "
                f"thông điệp cửa sổ (từ {gt['raw_pings']} pings thô / {gt['clean_pings']} hợp lệ) vào "
                "<i>Apache Kafka 4.1.0</i> (1.0% Duplicates và 1.5% Late > 30m); (2) <i>OSM / GIS Master Topology</i> "
                f"gồm <i>nodes.csv</i> ({gt['nodes_n']} tọa độ), <i>streets.csv</i> ({gt['streets_n']} tuyến) và "
                f"<i>segments.csv</i> ({gt['raw_segs']} đoạn thô → {gt['clean_segs']} cơ giới sau loại 427 địa điểm/"
                "lối đi bộ phi cơ giới — <i>POI, Point of Interest</i> từ OSM), trong khi <i>train.csv</i> được tách "
                f"riêng chỉ để đối chứng LOS ({gt['los_match']}). "
                "<b>Bốn nhóm đầu ra chính thức (4 System Outputs)</b> gồm: (1) <i>Medallion Lakehouse (Parquet)</i> "
                f"10 phân vùng tháng và <i>dataset.csv</i> (60 cột); (2) <i>Frozen Model Registry &amp; Static Feature "
                f"Store</i> phủ đủ {gt['clean_segs']} đoạn cơ giới; (3) <i>Quarantine Parquet &amp; WAL Checkpoints "
                f"({gt['wal_kb']})</i>; và (4) <i>MongoDB 8.0 Operational Serving</i> kết nối <i>ReactJS + FastAPI TOC "
                "Command Center 5-Tab (:8000)</i>."
            ),
            s["body"],
        )
    )
    story.append(
        KeepTogether(
            [
                build_boundary_drawing(),
                Paragraph(
                    "Hình 1.3. Đặc tả Ranh giới Hệ thống (System Boundary): 2 External Inputs đi qua Unified Pipeline "
                    "4 bước (P1 → P4) tạo ra 4 System Outputs phục vụ điều hành trực tuyến và phân tích lịch sử.",
                    s["fig_caption"],
                ),
            ]
        )
    )
    story.append(
        Paragraph(
            "1.5. Tổng Quan Nghiên Cứu (Literature Review), Khoảng Trống Thực Tiễn &amp; Cấu Trúc Báo Cáo",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Bảng 1.3 tổng hợp bốn hướng tiếp cận tiêu biểu trong quản lý và dự báo giao thông đô thị từ các "
                "công trình kinh điển [1]–[8], qua đó làm rõ khoảng trống khoa học và kỹ thuật khi áp dụng cho bối "
                "cảnh đặc thù TP.HCM."
            ),
            s["body"],
        )
    )
    lit_rows = [
        [
            "1. Quan trắc Hạ tầng Cố định\n&amp; Đèn Thích ứng (Fixed ITS)",
            "Cảm biến vòng từ, CCTV; hệ thống đèn SCATS (Sims &amp; Dobinson, 1980 [1]) &amp; SCOOT (Hunt et al., "
            "1981 [2])",
            "Đo lưu lượng liên tục, chính xác cao tại nút giao có thiết bị để điều chỉnh chu kỳ đèn cục bộ",
            "Chi phí đầu tư và bảo trì rất lớn; chỉ phủ số ít trục chính, bỏ trống gần 90% đường nhánh (vùng mù)",
        ],
        [
            "2. Giám sát Dữ liệu Hành trình\n(Floating Car / Probe GPS)",
            "Định vị GPS từ đội xe buýt, taxi, bản đồ số (Herrera et al., 2010 [3]; Zheng, 2015 [6])",
            "Mở rộng độ phủ diện rộng không cần lắp cảm biến mặt đường, phản ánh trực tiếp vận tốc thực tế",
            "Phân bố lệch về đại lộ, tín hiệu ngắt quãng dài, thiếu chuẩn tốc độ và mới dừng ở bản đồ hiện trạng",
        ],
        [
            "3. Dự báo Ùn tắc Ngắn hạn\n(Short-Term Forecasting)",
            "Chuỗi thời gian ARIMA / Quán tính (Vlahogianni et al., 2014 [4]); mạng đồ thị sâu DCRNN (Li et al., "
            "2018 [8])",
            "Dự báo vận tốc hoặc mức ùn tắc trước 15–60 phút bằng tương quan không gian – thời gian",
            "Thử nghiệm chủ yếu trên cao tốc cảm biến dày đặc (PeMS, METR-LA); suy giảm khi dữ liệu đô thị thưa",
        ],
        [
            "4. Nền tảng Hỗ trợ Quyết định\nĐiều hành Đô thị (Urban DSS)",
            "Trung tâm TOC &amp; nền tảng dữ liệu lớn giao thông (Lv et al., 2015 [5]; Amini et al., 2017 [7])",
            "Kết hợp lưu trữ lịch sử quy hoạch với bảng điều khiển giám sát trực tuyến cho điều hành viên",
            "Mô hình dự báo thường tách rời luồng vận hành thực tế (tin GPS trễ/trùng); chia cắt trực ban &amp; "
            "quy hoạch",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Hướng Tiếp Cận Nghiên Cứu",
            "Hệ Thống &amp; Công Trình Tiêu Biểu",
            "Đóng Góp Trong Quản Lý Giao Thông",
            "Hạn Chế Khi Áp Dụng Cho Đô Thị TP.HCM",
        ],
        lit_rows,
        [98, 130, 132, 155],
        "Bảng 1.3. Tổng quan bốn hướng tiếp cận quản lý – dự báo giao thông đô thị [1]–[8] và khoảng trống tại TP.HCM.",
        s,
    )
    story.append(
        make_callout(
            (
                "<b>Khoảng trống nghiên cứu &amp; Lộ trình 8 chương của báo cáo (Chương 1 → Chương 8):</b> "
                "Các nghiên cứu dự báo học thuật thường giả định chuỗi cảm biến liên tục trên cao tốc, còn hệ thống "
                f"đô thị thực tế lại thiếu dự báo sớm +30 phút và đối mặt vùng mù rộng ({gt['cold_pct']} đoạn trống "
                "GPS; 66.30% cặp quan sát cách nhau ≥ 60 phút). Báo cáo trình bày lời giải tích hợp xuyên suốt 8 "
                "chương: <b>Chương 1</b> định nghĩa bài toán, Use Cases và khoảng trống; <b>Chương 2</b> kiểm toán bộ "
                "dữ liệu Kaggle v6, 10 Edge Cases và Quality Funnel; <b>Chương 3</b> đặc tả kiến trúc Zones A–F, "
                "MLOps 3 pha và thực thi phân tán Apache Spark 3.5.7; <b>Chương 4</b> giải bài toán vùng mù bằng "
                "Line-Graph PageRank, SON/FPGrowth và 7D K-Means; <b>Chương 5</b> huấn luyện và kiểm định mô hình "
                "Predictive ML +30 phút (Random Forest 23D); <b>Chương 6</b> trình bày Speed Layer O(1) Sketches "
                "(33.0 KiB), đối soát Two-Tier Reconciliation, MongoDB IXSCAN và Web TOC (:8000); <b>Chương 7</b> "
                "đánh giá sẵn sàng sản xuất (Cloud/K8s, F1–F3, Observability, Bảo mật, Chi phí–Hiệu năng, Lộ trình "
                "3 giai đoạn &amp; Runbook); và <b>Chương 8</b> tổng kết nghiệm thu KPI kèm <b>Tài liệu tham khảo</b>."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch2_data_audit_and_ec(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(CondPageBreak(220))
    story.append(
        Paragraph(
            "Chương 2. Bước 1A — Kiểm Toán 5 Bảng Dữ Liệu Kaggle v6 (EDA), 10 Edge Cases Thực Nghiệm &amp; "
            "Ordered Quality Funnel",
            s["h1"],
        )
    )
    story.append(
        Paragraph("2.1. Kiểm Toán Nguồn Gốc &amp; Năm Bảng Dữ Liệu Gốc (Kaggle v6 · BkTraffic HCMUT)", s["h2"])
    )
    story.append(
        Paragraph(
            (
                "Bộ dữ liệu thực nghiệm được công bố trên Kaggle bởi tác giả <i>Thanh Nguyen</i> "
                "(<i>thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam</i>, <b>Version 6</b> phát hành "
                "ngày 2021-05-20, giấy phép <b>CC0: Public Domain</b>, xuất xứ hệ thống <b>BkTraffic — Đại học Bách "
                "Khoa TP.HCM / HCMUT</b> [16]), gồm đúng <b>5 tệp CSV · 41 cột · tổng dung lượng 38.50 MB</b> "
                "(36.72 MiB) thu thập giai đoạn 07/2020–04/2021. Kiểm toán trực tiếp dữ liệu thô (Bảng 2.1 và Hình "
                "2.1) ghi nhận 5 đặc trưng quan trọng chi phối thiết kế kiến trúc: <b>(1)</b> chỉ có <b>46 / 48</b> "
                "khung 30 phút UTC xuất hiện quan sát (trống hoàn toàn hai khung <i>period_20_30</i> và "
                "<i>period_21_00</i> UTC, tương ứng 03:30–04:30 sáng giờ Việt Nam); <b>(2)</b> chuyển đổi múi giờ "
                "<i>UTC → ICT (+7h)</i> làm <b>4,305 / 33,394 cửa sổ (12.89%)</b> bước sang ngày lịch mới; <b>(3)</b> "
                "khuyết tốc độ tối đa trên <i>train.csv</i> gốc là <b>28,495 / 33,441 = 85.21%</b> và sau khi loại "
                "47 dòng phi cơ giới/POI là <b>28,448 / 33,394 = 85.19%</b> (phân tách minh bạch, không trộn mẫu số "
                "thô với sạch); <b>(4)</b> chuẩn hóa <i>Unicode NFD → NFC</i> xử lý <b>18 tên đường duy nhất</b> mang "
                "mã tổ hợp NFD trên 430 dòng <i>segments.csv</i> và gộp <b>695 → 641 tên chuẩn</b> trên 2,247 cửa sổ "
                "<i>train.csv</i>; <b>(5)</b> cắt lọc 427 đoạn phi cơ giới/POI (<i>84,633 → 84,206</i>), 151 pings "
                "lỗi/phi cơ giới (<i>90,938 → 90,787</i>) và gắn cờ 154 đoạn chiều dài ≤ 0m; <b>(6) Đợt burst tổng "
                "hợp ngày 03/08/2020:</b> riêng buổi sáng 03/08/2020 theo giờ Việt Nam (tương ứng hai khung UTC "
                "<i>2020-08-02 23:30</i> và <i>2020-08-03 00:00</i>) dồn đột biến <b>36,096 bản tin GPS (39.7% tổng "
                "cả 10 tháng</b>, mật độ đỉnh <b>~685x</b> [684.95x trên tập sạch] so với trung vị), tạo "
                "<b>7,192 cửa sổ dị thường</b> (3,596 đoạn cơ giới × 2 khung). Hệ thống tách riêng đợt này để tập "
                "<b>Organic TRAIN</b> chỉ học từ các ngày giao thông tự nhiên."
            ),
            s["body"],
        )
    )
    inv_rows = [
        [
            "nodes.csv",
            gt["nodes_n"],
            "3 (19.44 MB)",
            "_id, long, lat",
            f"{gt['pruned_nodes']} điểm (90.97%) chỉ là Intermediate Shape Points vẽ độ cong OSM; giữ đúng "
            f"{gt['active_nodes']} giao lộ thực sự kết nối các đoạn cơ giới.",
        ],
        [
            "streets.csv",
            gt["streets_n"],
            "5 (0.20 MB)",
            "_id, level, max_velocity, name, type",
            "Cấp đường level 1..4; 82.23% (4,566 tuyến) khuyết max_velocity; 17 tên đường gốc mang mã NFD "
            "(695 → 641 tên chuẩn NFC sau hợp nhất biến thể).",
        ],
        [
            "segments.csv",
            gt["raw_segs"],
            "11 (10.29 MB)",
            "_id, s/e_node_id, length, street_id, max_velocity...",
            f"{gt['miss_vmax_seg_pct']} (74,762 đoạn) khuyết Vmax; 154 đoạn ≤ 0m; 18 tên đường NFD (430 dòng); "
            f"lọc 427 POI còn {gt['clean_segs']} cơ giới ({gt['cold_segs']} = {gt['cold_pct']} Cold-Start).",
        ],
        [
            "segment_status.csv",
            gt["raw_pings"],
            "4 (3.58 MB)",
            "_id, updated_at, segment_id, velocity",
            f"GPS Event-Time UTC (07/2020–04/2021); cách ly {gt['rej_pings']} pings lỗi/phi cơ giới còn "
            f"{gt['clean_pings']} hợp lệ; giờ cao điểm lệch tải Data Skew 684.95x trung vị.",
        ],
        [
            "train.csv",
            "33,441",
            "18 (4.99 MB)",
            "_id, segment_id, date, period, LOS...",
            f"Đối chứng độc lập (46/48 periods; {gt['windows_n']} cơ giới sau lọc 47 POI; {gt['los_match']} khớp "
            "LOS); khuyết Vmax 28,495/33,441 (85.21% thô) hay 28,448/33,394 (85.19% cơ giới).",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Bảng Nguồn",
            "Số Dòng",
            "Cột &amp; Dung lượng",
            "Các Cột Khóa Chính",
            "Phát hiện Kiểm toán 5-Table EDA &amp; Tác động Kiến trúc",
        ],
        inv_rows,
        [96, 44, 62, 104, 209],
        "Bảng 2.1. Kiểm kê chi tiết 5 bảng dữ liệu nguồn Kaggle v6 (41 cột, 38.50 MB, CC0, BkTraffic HCMUT [16]).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_eda_anomalies.png",
        0.54,
        "Hình 2.1. Tỷ lệ khuyết max_velocity, điểm hình học trung gian, vùng Cold-Start và khoảng ngắt quãng GPS "
        "trên 5 bảng dữ liệu gốc.",
        s,
    )

    story.append(
        Paragraph("2.2. Danh Mục 10 Edge Cases Thực Nghiệm &amp; Cơ Chế Phòng Vệ Kỹ Thuật (EC-1–EC-10)", s["h2"])
    )
    story.append(
        Paragraph(
            (
                "Từ kết quả kiểm toán năm bảng gốc, nhóm hệ thống hóa <b>10 tình huống biên thực nghiệm "
                "(EC-1–EC-10)</b> trong Bảng 2.2 cùng cơ chế phòng vệ đã kiểm chứng định lượng."
            ),
            s["body"],
        )
    )
    ec_rows = [
        [
            "EC-1: Khuyết max_velocity diện rộng",
            "28,495/33,441 thô (85.21%); 28,448/33,394 cơ giới (85.19%); 74,762 đoạn",
            (
                "3-Tier Velocity Imputation chỉ học trên organic TRAIN: Gốc (4,946) → Segment P85 trên TRAIN "
                "(7,650) → Trung vị (level, type) (19,601) → Chuẩn đô thị 80/60/50/40 km/h (1,197)."
            ),
        ],
        [
            "EC-2: Đoạn ≤ 0m &amp; Non-Motorway",
            "154 đoạn ≤ 0m, 427 non-road POIs",
            (
                f"Ordered Quality Funnel cách ly 580 đoạn lỗi vào Quarantine Parquet; điền trung vị 27.0m; lọc "
                f"{gt['rej_pings']} pings lỗi/POI còn {gt['windows_n']} cửa sổ trên {gt['clean_segs']} đoạn."
            ),
        ],
        [
            "EC-3: Unicode NFD/NFC &amp; Phường/Quận",
            "18 tên NFD segments; 631 Phường/Quận; 2,247 cửa sổ lệch dấu",
            (
                f"Street Entity Hygiene chuẩn hóa Unicode NFC ({gt['raw_st_names']} → {gt['nfc_st_names']} tên chuẩn, "
                f"gộp {gt['merged_st_names']} biến thể); loại nhãn Phường/Quận khỏi giỏ SON / FPGrowth."
            ),
        ],
        [
            "EC-4: Lệch múi giờ UTC vs ICT (+7h)",
            "4,305 / 33,394 cửa sổ (12.89%); 46/48 active periods",
            (
                "Giữ khóa ngày UTC để đối chứng chuẩn, đồng thời dịch sang giờ ICT <i>h</i><sub>VN</sub> = "
                "(<i>h</i><sub>UTC</sub> + 7) mod 24, cộng +1 ngày khi <i>h</i><sub>UTC</sub> ≥ 17 và mã hóa sin/cos."
            ),
        ],
        [
            "EC-5: Chuỗi đứt quãng &amp; Burst tổng hợp",
            f"{gt['t30m_pairs']} / 23,399 cặp +30m; {gt['burst_n']} burst "
            "(36,096 pings sáng 03/08/2020 = 39.7%, ~685x)",
            (
                "Exact +30m Contiguous Join trên (segment_id, slot_ts + 30m) loại 15,514 bước nhảy ≥ 60m (66.30%), "
                f"đồng thời tách 3,596 cặp burst (7,192 cửa sổ dị thường) khỏi TRAIN để giữ {gt['train_n']} "
                "Organic TRAIN tự nhiên."
            ),
        ],
        [
            "EC-6: 88.13% đoạn bị Cold-Start",
            f"{gt['cold_segs']} / {gt['clean_segs']} đoạn trống GPS",
            (
                f"Static Feature Store phủ kín {gt['clean_segs']} đoạn cơ giới nhờ điểm Line-Graph PageRank và tâm "
                "cụm 7D Behavioral K-Means học trên organic TRAIN."
            ),
        ],
        [
            "EC-7: Shape Points &amp; 314 Dangling Nodes",
            f"{gt['pruned_nodes']} Shape Points, {gt['dangling_n']} đoạn cụt",
            (
                f"Cắt tỉa 90.97% Intermediate Shape Points và loại U-turn trước khi dựng Line Graph ({gt['lg_edges']} "
                f"cạnh); phân bổ lại {gt['dangling_n']} Dangling Nodes bảo toàn tổng PageRank = 84,206.0."
            ),
        ],
        [
            "EC-8: Data Skew &amp; Bẫy trung bình",
            "Skew 684.95x; sai số 11.11 km/h",
            (
                "5-Tuple Commutative Monoid Combiner kết hợp 2-Stage Key Salting (8 buckets) kéo độ lệch slot giờ "
                "cao điểm về 1.020x (CV = 0.0059), đạt sai số tuyệt đối bằng 0.0."
            ),
        ],
        [
            "EC-9: 1% Duplicates &amp; 1.5% Late Events",
            f"{gt['dupes_caught']} tin trùng, {gt['late_wins']} cửa sổ trễ / {gt['late_slots']} khung",
            (
                f"Chặn {gt['dupes_caught']}/{gt['dupes_caught']} trùng bằng {gt['bloom_kb']} RotatingBloomFilter "
                f"({gt['bloom_fpr']} FPR); Phase C Two-Tier Recon backfill {gt['late_wins']} trễ &amp; heal "
                f"{gt['healed_lags']} lag."
            ),
        ],
        [
            "EC-10: Crash Recovery &amp; Stale Overwrite",
            "Ngắt tiến trình &amp; tin cũ đến muộn",
            (
                f"Hồi phục từ WAL Checkpoints ({gt['wal_kb']}, 0 dupes) &amp; Spark Real Watermark; atomic "
                f"$status_version trong MongoDB chặn {gt['mongo_stale_rej']}/{gt['mongo_stale_rej']} tin v1 ghi đè v2."
            ),
        ],
    ]
    _add_captioned_table(
        story,
        ["Mã Edge Case", "Quy mô Ghi nhận Thực tế", "Cơ chế Phòng vệ Kỹ thuật Đã Triển khai &amp; Kiểm chứng"],
        ec_rows,
        [118, 134, 263],
        "Bảng 2.2. Danh mục 10 tình huống biên thực nghiệm (EC-1–EC-10) và cơ chế phòng vệ tương ứng.",
        s,
    )


def _append_ch2_funnel_los_and_limits(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(Paragraph("2.3. Ordered Quality Funnel (ANSI try_cast · Bảo Toàn 100% Quân Số Bản Ghi)", s["h2"]))
    story.append(
        Paragraph(
            (
                "Mọi bản ghi đi qua cổng kiểm định <b>6 quy tắc thứ tự (R1–R6)</b> ở Bảng 2.3 dưới chế độ "
                "<i>spark.sql.ansi.enabled = true</i> cùng phép ép kiểu an toàn <i>try_cast</i>, bảo đảm mỗi bản ghi "
                "lỗi chỉ nhận đúng một mã lỗi đầu tiên (<i>first-failing rule</i>) và bảo toàn 100% quân số bản ghi."
            ),
            s["body"],
        )
    )
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
            "427 đoạn (426 first-fail)",
            f"Cách ly 427 POIs (còn {gt['clean_segs']} cơ giới); loại 47 cửa sổ.",
        ],
        [
            '<font size="6.3">R3_ADMIN_POLYGON_STREET_NAME</font>',
            "streets / windows",
            "street_name khớp mẫu hành chính ^(Phường|Quận)",
            "631 cửa sổ",
            "Giữ cho vận tốc nhưng loại khỏi FPGrowth &amp; Top PageRank.",
        ],
        [
            '<font size="6.3">R4_UNICODE_NFD_AND_PREFIX_DRIFT</font>',
            "Chuẩn hóa tên",
            "street_name ≠ NFC(trim(name))",
            "2,247 cửa sổ (18 NFD seg)",
            f"Chuẩn hóa dấu tiếng Việt NFC ({gt['raw_st_names']} → {gt['nfc_st_names']} tên).",
        ],
        [
            '<font size="6.3">R5_MISSING_MAX_VELOCITY</font>',
            "segments / train",
            "max_velocity IS NULL OR max_velocity ≤ 0",
            "28,495 thô / 28,448 cơ giới",
            "Điền khuyết 3 bậc chỉ học từ tập organic TRAIN.",
        ],
        [
            '<font size="6.3">R6_STATIONARY_OR_INVALID_PING</font>',
            "segment_status.csv",
            "velocity ≤ 0 / phi cơ giới (517 flagged)",
            f"{gt['rej_pings']} pings cách ly (448 win)",
            f"Cách ly {gt['rej_pings']} pings POI; giữ {gt['clean_pings']} pings cơ giới.",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Mã Quy tắc Thứ tự",
            "Bảng Nguồn",
            "Điều kiện Kiểm tra (ANSI try_cast)",
            "Quy mô Ghi nhận",
            "Hành động Chuẩn hóa",
        ],
        r_rows,
        [152, 72, 114, 76, 101],
        "Bảng 2.3. Sáu quy tắc thứ tự (R1–R6) của Ordered Quality Funnel dưới chế độ ANSI try_cast.",
        s,
    )

    story.append(
        Paragraph(
            "2.4. Ước Lượng Tốc Độ Tối Đa Chỉ Học Từ TRAIN Hữu Cơ &amp; Phân Tách derive_urban_los vs derive_los",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Như định nghĩa tại Công thức (2.1), tốc độ tối đa khuyết thiếu được suy diễn theo 3 bậc hoàn toàn "
                "không rò rỉ tương lai (chỉ học trên tập organic TRAIN). Đồng thời, hệ thống tách bạch rõ hai hàm "
                "quy đổi 6 mức phục vụ giao thông để tránh trộn lẫn thang đo: <b>(a) <i>derive_urban_los</i></b> "
                "(hàm vận hành chính thức theo thang bước 5 km/h đô thị BkTraffic: ≥ 35/30/25/20/15 km/h, dịch +5 "
                "khi <i>raw_max ≥ 60</i> và +10 khi <i>raw_max ≥ 70</i>, đạt độ khớp chính xác <b>89.19%</b> với "
                "nhãn <i>LOS</i> tham chiếu của <i>train.csv</i>); và <b>(b) <i>derive_los(velocity_ratio)</i></b> "
                "(tiện ích chuẩn hóa theo tỷ lệ <i>v̄ / v̂_max</i> với các mốc 0.80 / 0.65 / 0.50 / 0.35 / 0.20 "
                "phục vụ kiểm định tỷ lệ độc lập trong <i>spark_etl.py</i>)."
            ),
            s["body"],
        )
    )
    story.append(
        make_equation_box(
            "2.1",
            "Ước Lượng Tốc Độ Tối Đa 3 Bậc (TRAIN-Only) &amp; Quy Đổi Mức LOS (derive_urban_los vs derive_los)",
            CHART_DIR / "eq_vmax_los.png",
            (
                f"<b>Bảo toàn quân số:</b> {gt['raw_pings']} pings thô = 0 trùng + {gt['clean_pings']} hợp lệ + "
                f"{gt['rej_pings']} cách ly R6/POI; {gt['clean_pings']} pings (42,124 severe pings) tạo thành "
                f"<b>{gt['windows_n']} cửa sổ cơ giới</b> (tách 47 POI khỏi 33,441 dòng <i>train.csv</i>; "
                f"<b>derive_urban_los</b> khớp <b>{gt['los_match']}</b> BkTraffic). <b>Phân bổ Vmax trên "
                f"{gt['windows_n']} cửa sổ:</b> Bậc 1a gốc (4,946) → Bậc 1b P85 trên organic TRAIN (7,650) → Bậc 2 "
                "trung vị level×type (19,601) → Bậc 3 chuẩn đô thị 80/60/50/40 km/h (1,197). <b>Phân bố 6 mức "
                "derive_urban_los:</b> A (13,309) · B (4,926) · C (2,657) · D (3,730) · E (3,401) · F (5,371) "
                "[đối chứng BkTraffic: A 13,267 · B 4,694 · C 3,827 · D 3,701 · E 3,821 · F 4,084]."
            ),
            s,
        )
    )
    story.append(Paragraph("2.5. Hạn Chế &amp; Rủi Ro Thành Phần Dữ Liệu, EDA &amp; Quality Funnel", s["h2"]))
    story.append(
        make_callout(
            (
                "<b>Hạn chế &amp; Rủi ro thành phần Dữ liệu &amp; Tiền xử lý:</b> "
                f"<b>(1) Thiên lệch không gian của cảm biến (Spatial Probe Bias):</b> Chỉ {gt['active_segs']} / "
                f"{gt['clean_segs']} đoạn cơ giới (11.87%) có dữ liệu GPS thực tế; dữ liệu tập trung mạnh ở trục "
                "chính và đội xe thương mại, chưa phản ánh trực tiếp luồng xe máy trong hẻm nhỏ. "
                "<b>(2) Tốc độ tối đa tĩnh (Static Vmax Approximation):</b> 20,798 / 33,394 cửa sổ (62.28%) dùng "
                "Vmax suy diễn theo cấp và loại đường (Tier 2 &amp; Tier 3), chưa tự hạ trần khi có rào chắn hay "
                "ngập cục bộ. <b>(3) Rủi ro tăng đột biến Quarantine khi đổi Schema đầu nguồn:</b> "
                "<i>ANSI try_cast</i> giữ tiến trình không sập nhưng cần cảnh báo tỷ lệ cách ly nếu thiết bị định "
                "vị đổi định dạng bản tin."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch3_architecture_tables(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(CondPageBreak(220))
    story.append(
        Paragraph(
            "Chương 3. Kiến Trúc Tổng Thể (Zones A–F, 4 Tuyến Pipeline), Zero-Conflict MLOps &amp; "
            "Xử Lý Phân Tán Trên Apache Spark 3.5.7",
            s["h1"],
        )
    )
    story.append(
        Paragraph(
            "3.1. Sơ Đồ Kiến Trúc Tổng Thể (Zones A–F), Ánh Xạ 17 Thành Phần ([C1]–[C17]) &amp; 10 Luồng Dữ Liệu",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Hình 3.1 trình bày <b>Sơ đồ Kiến trúc Giải pháp Tổng thể</b> của nền tảng HCMC Smart Traffic vận "
                "hành trên cụm Docker Compose <b>11.0 / 16.0 GB RAM</b> (5 dịch vụ mặc định: <i>Apache Kafka 4.1.0 "
                "KRaft</i> [14], <i>Apache Spark 3.5.7 · Java 17</i> [13], <i>MongoDB 8.0.34</i> [15], "
                "<i>live-consumer</i> và <i>ReactJS + FastAPI TOC Command Center :8000</i>, kèm 2 profile on-demand "
                "<i>batch/stream</i>). Sơ đồ được tổ chức trực quan theo <b>6 phân vùng (Zones A–F)</b> và <b>4 tuyến "
                "luồng mã hóa màu sắc</b>: <b>Tím (Speed Pipeline)</b> cho xử lý luồng thời gian thực; <b>Xanh lam "
                "(Batch Pipeline)</b> cho làm sạch, khai phá đồ thị, huấn luyện Predictive ML và đối soát cuối ngày; "
                "<b>Xanh ngọc (MLOps Registry)</b> cho vòng đời khóa và phục vụ mô hình chỉ đọc; và <b>Xanh lá "
                "(Storage &amp; Serving)</b> cho lưu trữ đa mô hình và cổng điều hành TOC. "
                "Để phục vụ tra cứu kỹ thuật và đối chiếu mã nguồn, <b>Bảng 3.1</b> cung cấp bảng ánh xạ chỉ mục "
                "<b>17 thành phần [C1]–[C17]</b> tương ứng các khối trên 6 phân vùng của Hình 3.1, và <b>Bảng 3.2</b> "
                "đặc tả chi tiết <b>10 luồng dữ liệu đầu-cuối (Flows 1–10)</b> tương ứng với 4 tuyến pipeline màu "
                "trên Hình 3.1."
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        OUTPUTS_DIR / "system_architecture.png",
        0.96,
        "Hình 3.1. Kiến trúc hệ thống tổng thể (Zones A–F và 4 tuyến pipeline mã hóa màu: Speed Pipeline, "
        "Batch Pipeline, MLOps Registry và Storage &amp; Serving; ánh xạ chi tiết tại Bảng 3.1 và Bảng 3.2).",
        s,
    )

    comp_rows = [
        [
            "Zone A\n(External)",
            "[C1] GPS Probe Fleet",
            "producer.py\n(Python 3.11)",
            f"Phát lại {gt['stream_events']} sự kiện cửa sổ ({gt['dupes_caught']} trùng &amp; {gt['late_wins']} "
            f"trễ &gt; 30m) hoặc {gt['raw_pings']} pings thô.",
        ],
        [
            "Zone A\n(External)",
            "[C2] OSM / GIS Topology",
            "raw_data/*.csv\nLoader",
            f"Đồ thị tĩnh: nodes.csv ({gt['active_nodes']} giao lộ thực), streets.csv ({gt['streets_n']}), "
            f"segments.csv ({gt['clean_segs']} cơ giới).",
        ],
        [
            "Zone A\n(External)",
            "[C3] Traffic Ops &amp; Users",
            "Web / REST Clients\n(:8000)",
            "Cán bộ TOC, kỹ sư tín hiệu đèn, chuyên viên Sở GTVT và người đi đường khai thác 5-Tab dashboard.",
        ],
        [
            "Zone B\n(Ingestion)",
            "[C4] Apache Kafka Broker",
            "apache/kafka:4.1.0\n(KRaft Mode)",
            "Event broker không cần ZooKeeper (1.5 GB RAM), topic hcmc-traffic-events (3 partitions theo segment_id).",
        ],
        [
            "Zone B\n(Ingestion)",
            "[C5] Quality Funnel",
            "spark_etl.py /\npipeline.py",
            f"Ordered Quality Funnel 6 quy tắc (R1–R6): ANSI try_cast → loại 427 POIs → Unicode NFC "
            f"({gt['raw_st_names']}→{gt['nfc_st_names']}) → ICT (+7h) → 3-Tier Vmax.",
        ],
        [
            "Zone C\n(Speed Layer)",
            "[C6] Watermark &amp; Windowing",
            "starter_stream.py /\nspark_engine.py",
            "Áp dụng Watermark thật 30 phút (2020-12-01T12:30Z) và tách Dual Sink: Sink 1 Parquet WAL; Sink 2 luồng.",
        ],
        [
            "Zone C\n(Speed Layer)",
            "[C7] Stream Sketches",
            "Bloom + HLL++ +\nEWMA (33 KiB)",
            f"Bộ nhớ hằng số O(1): {gt['bloom_kb']} RotatingBloomFilter ({gt['bloom_fpr']} FPR), "
            f"{gt['hll_kb']} HLL++ ({gt['hll_err']} err), EWMA (α=0.15).",
        ],
        [
            "Zone C\n(Speed Layer)",
            "[C8] Online Scorer",
            "Read-Only\n.transform()",
            "Broadcast-join đặc trưng cửa sổ trong Watermark với Static Store, gọi Frozen Model và gắn v1.",
        ],
        [
            "Zone D\n(Batch Layer)",
            "[C9] Combiner &amp; Join",
            "spark_batch.py /\nspark_engine.py",
            f"5-Tuple Monoid + 8-Bucket Salting (skew 684.95x → 1.020x) + BroadcastHashJoin "
            f"(5 → 1 Exchange, +{gt['join_speedup']}).",
        ],
        [
            "Zone D\n(Batch Layer)",
            "[C10] Graph &amp; Pattern Mining",
            "PageRank · SON ·\nFPGrowth · K-Means",
            f"Line-Graph PageRank ({gt['clean_segs']} cơ giới, {gt['lg_edges']} cạnh, 30 vòng), FPGrowth "
            f"({gt['spark_fp_rules']} luật) / SON (12+14), 7D K-Means (Sil={gt['km_sil']}).",
        ],
        [
            "Zone D\n(Batch Layer)",
            "[C11] Predictive ML Training",
            "pipeline.py +\nspark_engine.py",
            f"Chronological 70/15/15 (lọc burst → {gt['train_n']} organic TRAIN / {gt['val_n']} VAL / "
            f"{gt['test_n']} TEST); VAL Gate (0.8882); RF 23D.",
        ],
        [
            "Zone D\n(Batch Layer)",
            "[C12] Two-Tier Reconciliation",
            "exceptAll +\nLate Backfill",
            f"Tier 1 exceptAll == 0; Tier 2 backfill {gt['late_wins']} cửa sổ trễ, heal {gt['healed_lags']} lag "
            f"và re-score {gt['rescored_wins']} cửa sổ ({gt['prob_changed']} đổi prob).",
        ],
        [
            "Zone E\n(Registry)",
            "[C13a] Trained ML Model",
            "model.joblib /\nSpark PipelineModel",
            f"Lưu bất biến mô hình Phase A kèm chữ ký SHA-256 ({gt['model_sha12']}...) chỉ đọc cho [C8] và [C12].",
        ],
        [
            "Zone E\n(Registry)",
            "[C13b] Static Feature Store",
            '<font size="6.7">segment_static_features.parquet</font><br/>vmax_lookup.json',
            f"Lưu PageRank, K-Means cluster, bậc vào/ra và Priors phủ kín {gt['clean_segs']} đoạn "
            f"({gt['cold_segs']} Cold-Start).",
        ],
        [
            "Zone F\n(Storage/UI)",
            "[C14] Medallion Lakehouse",
            "Bronze · Silver ·\nGold Parquet",
            "Lưu trữ cột chia 10 phân vùng tháng (year_month=2020-07..2021-04) hỗ trợ Partition &amp; Column Pruning.",
        ],
        [
            "Zone F\n(Storage/UI)",
            "[C15] Quarantine &amp; WAL",
            "quarantine/ ·\ncheckpoints/",
            f"Cách ly 580 đoạn &amp; {gt['rej_pings']} pings lỗi; nhật ký WAL ({gt['wal_kb']}) đảm bảo 0 duplicate "
            "khi hồi phục sự cố.",
        ],
        [
            "Zone F\n(Storage/UI)",
            "[C16] MongoDB Serving Store",
            "mongo:8.0.34\n(traffic_scores)",
            f"Upsert {gt['windows_n']} docs (4 partitions, 69 batches) với atomic $status_version guard &amp; "
            f"IXSCAN ({gt['mongo_med']} / p95 {gt['mongo_p95']}).",
        ],
        [
            "Zone F\n(Storage/UI)",
            "[C17] TOC Command Center",
            "ReactJS + FastAPI\n(:8000)",
            "Cổng điều hành 5-Tab + Leaflet: Command Center +30p, Live Kafka, PageRank/Rules, K-Means Cold-Start, "
            "Spark/Ablation/Recon.",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Phân Vùng",
            "Mã &amp; Tên Thành Phần",
            "Runtime / Tệp Thực Thi",
            "Trách Nhiệm Kiến Trúc &amp; Hợp Đồng Input / Output",
        ],
        comp_rows,
        [64, 102, 126, 223],
        "Bảng 3.1. Ánh xạ chỉ mục 17 thành phần kỹ thuật ([C1]–[C17]) tương ứng với 6 Phân vùng (Zones A–F) trên "
        "Hình 3.1.",
        s,
    )

    flow_rows = [
        [
            "Flow 1\n(Speed · Tím)",
            "[C1] GPS Probe → [C4] Kafka",
            f"producer.py phát {gt['stream_events']} sự kiện cửa sổ ({gt['dupes_caught']} trùng + {gt['late_wins']} "
            f"trễ > 30m) hoặc 91,694 pings vào hcmc-traffic-events.",
        ],
        [
            "Flow 2\n(Batch · Lam)",
            "[C2] GIS Topology → [C5] Funnel",
            "Ba bảng bản đồ tĩnh (nodes, streets, segments) đi vào [C5] chuẩn hóa Unicode NFC, loại 427 POIs và "
            "suy diễn Vmax 3 bậc.",
        ],
        [
            "Flow 3\n(Speed/Batch)",
            "[C4] Kafka → [C6] &amp; [C5]",
            "Tuyến Tím cấp sự kiện trực tuyến vào [C6] Watermark (30m); tuyến nét đứt Lam đưa raw events xuống "
            "[C5] kiểm định ANSI try_cast.",
        ],
        [
            "Flow 4\n(Batch · Lam)",
            "[C5] → [C9] &amp; [C15] Quarantine",
            "Bản ghi hợp lệ đi sang [C9] Combiner &amp; Join; tuyến nét đứt cô lập 580 đoạn &amp; 151 pings lỗi "
            "xuống [C15] Quarantine.",
        ],
        [
            "Flow 5\n(Speed · Tím)",
            "[C6] → [C7] Sketches → [C8]",
            f"Các sự kiện trong Watermark 30m (98.50%) đi qua [C7] ({gt['bloom_kb']} Bloom, {gt['hll_kb']} HLL++, "
            "EWMA) rồi sang [C8] Online Scorer.",
        ],
        [
            "Flow 6\n(Storage · Lá)",
            "[C6]→[C15]/[C14]; [C9/10]→[C14]→[C12]",
            "[C6] ghi WAL xuống [C15] và append 100% raw events vào [C14]; [C9]–[C10] ghi Gold Parquet vào [C14] "
            "cấp đầu vào đối soát cho [C12].",
        ],
        [
            "Flow 7\n(Batch/MLOps)",
            "[C9] → [C10] → [C11] → [C13a/b]",
            "Phase A (t ≤ T_train): [C9] cấp cửa sổ sạch cho [C10] (PageRank, SON/FPGrowth, K-Means) → [C11] huấn "
            "luyện RF 23D → đóng băng [C13a/b].",
        ],
        [
            "Flow 8\n(MLOps · Ngọc)",
            "[C13a] &amp; [C13b] ⇢ [C8] &amp; [C12]",
            "Phase B &amp; Phase C: [C13a] Trained Model và [C13b] Static Store cấp .transform() chỉ đọc (xác thực "
            "SHA-256) cho [C8] và [C12].",
        ],
        [
            "Flow 9\n(Serving · Lá)",
            "[C8] &amp; [C12] → [C16] MongoDB",
            "Ghi lô 500 bản ghi bằng UpdateOne(upsert=True): [C8] ghi STREAM_PROVISIONAL (v1); [C12] nâng cấp "
            "BATCH_VERIFIED/RECONCILED (v2) kèm $status_version guard.",
        ],
        [
            "Flow 10\n(Serving · Lá)",
            "[C16] + [C14] + [C15] → [C17] → [C3]",
            f"[C16] MongoDB (IXSCAN {gt['mongo_med']} / p95 {gt['mongo_p95']}), [C14] Lakehouse &amp; [C15] Audit "
            "phục vụ [C17] Web TOC (:8000) tới [C3] người dùng.",
        ],
    ]
    _add_captioned_table(
        story,
        ["Luồng &amp; Tuyến Màu", "Kết Nối (Source → Target)", "Hợp Đồng Giao Tiếp Dữ Liệu &amp; Cơ Chế Thực Thi"],
        flow_rows,
        [82, 134, 299],
        "Bảng 3.2. Đặc tả 10 luồng dữ liệu đầu-cuối (Flows 1–10) tương ứng 4 tuyến pipeline mã hóa màu trên Hình 3.1.",
        s,
    )


def _append_ch3_mlops_and_spark(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(
        KeepTogether(
            [
                Paragraph("3.2. Zero-Conflict 3-Phase MLOps Lifecycle &amp; Khóa Chữ Ký Toàn Vẹn SHA-256", s["h2"]),
                Paragraph(
                    (
                        "Để loại trừ hoàn toàn xung đột ghi giữa nhánh trực tuyến và nhánh xử lý lô, Hình 3.2 minh "
                        "họa quy trình MLOps 3 pha tách biệt nghiêm ngặt quyền <i>.fit()</i> (duy nhất Phase A) với "
                        "quyền <i>.transform()</i> chỉ đọc kèm xác thực chữ ký SHA-256 (Phase B và Phase C)."
                    ),
                    s["body"],
                ),
                build_mlops_drawing(),
                Paragraph(
                    f"Hình 3.2. Zero-Conflict 3-Phase MLOps Lifecycle: khóa huấn luyện trên {gt['train_n']} organic "
                    "TRAIN ≤ 2020-12-12T08:00:00Z và xác thực SHA-256 chỉ đọc ở Phase B, C "
                    f"(<b>model.joblib = {gt['model_sha12']}...</b> · "
                    f"<b>segment_static_features.parquet = {gt['static_sha12']}...</b> · "
                    f"<b>vmax_lookup.json = {gt['vmax_sha12']}...</b>).",
                    s["fig_caption"],
                ),
            ]
        )
    )

    story.append(
        Paragraph(
            "3.3. Bước 1B — 5-Tuple Commutative Monoid Combiner &amp; 2-Stage Key Salting Chống Data Skew",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Khi tổng hợp vận tốc song song trên nhiều nút máy chủ có kích thước phân vùng không đều, việc tính "
                "trung bình cục bộ rồi chia trung bình lần hai gây sai số nghiêm trọng (bẫy trung bình của các trung "
                "bình). Công thức (3.1) định nghĩa cấu trúc đại số <b>5-Tuple Commutative Monoid</b> giúp phép gộp "
                "đạt tính kết hợp và giao hoán tuyệt đối [9], [13]."
            ),
            s["body"],
        )
    )
    story.append(
        make_equation_box(
            "3.1",
            "Cấu Trúc Đại Số 5-Tuple Commutative Monoid Combiner Cho Tổng Hợp Phân Tán",
            CHART_DIR / "eq_monoid.png",
            (
                "<b>Chứng minh tránh bẫy trung bình từng phần:</b> Trên 3 phân vùng lệch [1, 2, 3], lấy trung bình "
                "cục bộ gây sai số <b>11.111111 km/h</b>; còn 5-Tuple Monoid <i>m = (count, sum_v, min_v, max_v, "
                f"severe_cnt)</i> khớp 100% với <i>baseline.py</i> (mã Python tham chiếu đơn máy) trên toàn bộ "
                f"{gt['windows_n']} cửa sổ (42,124 "
                "severe pings, sai số = <b>0.0 km/h</b>). Kết hợp <b>2-Stage Key Salting</b> (8 buckets, 59,655 nhóm "
                "pipeline / 59,714 nhóm Spark), lệch tải giờ cao điểm giảm từ <b>684.95x xuống 1.020x "
                "(CV = 0.0059)</b>."
            ),
            s,
        )
    )

    story.append(
        Paragraph(
            "3.4. Bằng Chứng Thực Thi Phân Tán Trên Apache Spark 3.5.7 (spark_engine.py &amp; spark_batch.py)",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Hiệu quả tối ưu vật lý trên <i>Apache Spark 3.5.7 (Java 17)</i> được kiểm chứng độc lập trên hai "
                "đường ống thực thi: <i>spark_engine.py</i> (kiểm định toàn diện Catalyst Join, Monoid Salting, "
                "Parquet Pruning, Structured Streaming và Spark MLlib) và <i>spark_batch.py</i> "
                "(<i>outputs/quyen_validation/manifest.json</i>, tái lập ETL độc lập từ CSV thô). Trong chương này, "
                "cấu hình chưa tối ưu được gọi rõ là <b>Spark Mặc định (SortMergeJoin / Unsalted)</b> (phân biệt với "
                "<i>Persistence Baseline</i> ở Chương 5 và mã đơn máy <i>baseline.py</i>); độ ổn định thời gian chạy "
                "đo bằng <b>IQR (Interquartile Range — khoảng tứ phân vị Q3 − Q1)</b> qua nhiều lần chạy lặp. Chi "
                "tiết đo đạc được tổng hợp tại Bảng 3.3 và trực quan hóa ở Hình 3.3."
            ),
            s["body"],
        )
    )
    spark_rows = [
        [
            "2-Stage Key Salting\n(vs Spark Mặc định Unsalted)",
            "Stage 1 gộp cục bộ theo (window_key, salt 0..7) tạo 59,714 nhóm Spark; Stage 2 bỏ salt về 33,394 cửa sổ.",
            (
                f"Skew slot 684.95x → 1.020x (CV=0.0059); Spark Mặc định (Unsalted): {gt['monoid_unsalted_ms']} ms "
                f"(IQR {gt['monoid_unsalted_iqr']}) → 8-Bucket Salted: {gt['monoid_salted_ms']} ms (IQR "
                f"{gt['monoid_salted_iqr']}); spark_batch.py: {gt['quyen_unsalted_s']} → {gt['quyen_salted_s']} "
                "(exceptAll diff = 0)."
            ),
        ],
        [
            "Catalyst BroadcastHashJoin\n(vs Spark Mặc định SortMergeJoin)",
            "Đối chiếu Spark Mặc định (SortMergeJoin, autoBroadcast=-1) với BroadcastHashJoin trên 90,787 pings × "
            "segments × streets.",
            (
                f"spark_engine.py: giảm Shuffle Exchanges 5 → 1 (-80%), {gt['join_base_ms']} ms (IQR "
                f"{gt['join_base_iqr']}) → {gt['join_opt_ms']} ms (IQR {gt['join_opt_iqr']}) (+{gt['join_speedup']}); "
                f"spark_batch.py (quyen_validation): 5 Exchange ({gt['quyen_smj_s']}) → 3 BHJ / 0 shuffle "
                f"({gt['quyen_bhj_s']}); exceptAll = 0."
            ),
        ],
        [
            "Structured Streaming\n&amp; Real Watermark",
            "Sink 1 WAL Bronze Parquet (200 rows) &amp; Sink 2 hcmc_agg_sink2 (3 micro-batches, stateStoreSave).",
            "Watermark thật dịch tới 2020-12-01T12:30:00Z, loại 10 rows hết hạn khỏi State Store (WAL 0 dupes).",
        ],
        [
            "Parquet Partition &amp;\nColumn Pruning",
            "10 phân vùng tháng year_month=2020-07..2021-04 (đọc sạch bởi cả Spark 3.5.7 &amp; PyArrow).",
            f"Lọc year_month = 2020-08 chỉ đọc đúng {gt['aug_rows']} / {gt['windows_n']} dòng và nạp 4 / 60 cột "
            "(PartitionFilters &amp; ReadSchema xác thực).",
        ],
        [
            "Spark MLlib Parity\n(Organic TRAIN)",
            "Chạy FPGrowth, KMeans(k=6) &amp; RandomForestClassifier (23D) trong container Spark 3.5.7.",
            (
                f"FPGrowth tìm {gt['spark_fp_rules']} luật; KMeans cost = {gt['spark_km_cost']} ({gt['km_segs']} "
                f"đoạn); Spark RF (không class_weight 0.65) đạt Acc = {gt['spark_rf_acc']} / ROC-AUC = "
                f"{gt['spark_rf_roc']} (reload parity = true; phân tích đối chiếu sklearn 0.9255 ở Chương 5)."
            ),
        ],
    ]
    _add_captioned_table(
        story,
        ["Kỹ thuật Phân tán Spark", "Cấu hình Thực thi Vật lý (Physical Plan)", "Bằng chứng Đo đạc Thực tế"],
        spark_rows,
        [114, 178, 223],
        "Bảng 3.3. Bằng chứng thực thi trên Apache Spark 3.5.7 (đối chiếu spark_engine.py và spark_batch.py / "
        "quyen_validation).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_spark_skew_catalyst.png",
        0.60,
        "Hình 3.3. Kết quả cân bằng tải giờ cao điểm bằng 2-Stage Key Salting (trên) và loại bỏ Shuffle dư thừa "
        "nhờ Catalyst BroadcastHashJoin trên Apache Spark 3.5.7 (dưới).",
        s,
    )
    story.append(Paragraph("3.5. Hạn Chế &amp; Rủi Ro Thành Phần Kiến Trúc &amp; Xử Lý Lô Spark", s["h2"]))
    story.append(
        make_callout(
            (
                "<b>Hạn chế &amp; Rủi ro thành phần Kiến trúc &amp; Xử lý lô:</b> "
                "<b>(1) Tranh chấp I/O trên máy chủ đơn:</b> Đóng gói 5 container mặc định (11.0 / 16.0 GB RAM) trên "
                "một máy tiềm ẩn cạnh tranh đĩa khi Spark Shuffle đồng thời với MongoDB ghi. "
                "<b>(2) Khoảng trễ nhất quán cuối cùng (Eventual Consistency):</b> Nhánh Speed phát cảnh báo v1 tức "
                "thì nhưng dữ liệu trễ > 30 phút chỉ cập nhật vào biến lag lịch sử sau Phase C cuối chu kỳ. "
                "<b>(3) Đánh đổi chi phí của 2-Stage Salting:</b> Khử skew 684.95x → 1.020x ở quy mô lớn, nhưng ở "
                f"mẫu chuẩn 90.8k pings phép gộp 2 tầng thêm chi phí Shuffle thứ hai ({gt['monoid_unsalted_ms']} ms "
                f"→ {gt['monoid_salted_ms']} ms)."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch4_pagerank_and_rules(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(CondPageBreak(220))
    story.append(
        Paragraph(
            f"Chương 4. Bước 2 — Lấp Đầy 'Vùng Mù' {gt['cold_pct']} Đường Thiếu GPS Bằng Đồ Thị PageRank &amp; "
            "Phân Cụm K-Means",
            s["h1"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Sợi dây liên kết nhân quả từ Chương 3 sang Chương 5:</b> "
                f"Sau Chương 2–3, hệ thống mới có số đo GPS trên {gt['active_segs']} đoạn cơ giới (11.87%), còn "
                f"<b>{gt['cold_segs']} đoạn đường nhánh ({gt['cold_pct']} thành phố) hoàn toàn trắng GPS</b>. "
                "Chương 4 giải quyết đồng thời hai mục tiêu: <b>(1)</b> xác định nút cổ chai cấu trúc cho Sở GTVT và "
                "khai phá cặp đường hay kẹt dây chuyền; <b>(2)</b> xuất các đặc trưng bản đồ tĩnh "
                "(<i>pagerank_score</i>, bậc vào/ra, mã cụm <i>cluster_id</i>) phủ đủ 100% mạng lưới để cấp trực "
                "tiếp vào vector 23 biến của mô hình Random Forest ở Chương 5."
            ),
            s,
        )
    )
    story.append(
        Paragraph("4.1. Lọc Điểm Uốn Cong Trung Gian &amp; Xếp Hạng Nút Cổ Chai Bằng Line-Graph PageRank", s["h2"])
    )
    story.append(
        Paragraph(
            (
                f"Trong {gt['nodes_n']} điểm tọa độ của <i>nodes.csv</i>, có tới <b>{gt['pruned_nodes']} điểm "
                "(90.97%) là điểm hình học trung gian</b> (<i>shape points</i>) vẽ độ cong thân đường. Sau khi lọc "
                f"bỏ để giữ đúng <b>{gt['active_nodes']} giao lộ thực</b>, hệ thống dựng <b>đồ thị nối đoạn đường có "
                f"hướng không U-turn</b>: mỗi đoạn trong <b>{gt['clean_segs']} đoạn cơ giới</b> là 1 đỉnh; cạnh có "
                "hướng nối hai đoạn liên tiếp thỏa điều kiện không quay đầu ngay trên cùng đoạn hai chiều, tạo thành "
                f"<b>{gt['lg_edges']} cạnh chuyển tiếp</b> và <b>{gt['dangling_n']} đoạn cụt</b>. Thuật toán "
                "PageRank [12] thực thi <b>30 vòng lặp</b> (hệ số cản β = 0.85, Công thức (4.1)), cho ra Top-10 nút "
                "cổ chai cấu trúc tại Bảng 4.1."
            ),
            s["body"],
        )
    )
    story.append(
        make_equation_box(
            "4.1",
            "Đệ Quy Line-Graph PageRank Không U-Turn Bảo Toàn Tổng Khối Lượng (30 Vòng Lặp, β = 0.85)",
            CHART_DIR / "eq_pagerank.png",
            f"<b>Bảo toàn tổng khối lượng toàn mạng:</b> Nhờ phân bổ đều xác suất của {gt['dangling_n']} đoạn cụt "
            f"qua đủ <b>30 vòng lặp</b> (<i>pipeline.py:3211</i>), tổng điểm PageRank trên toàn bộ {gt['clean_segs']} "
            "đoạn cơ giới bảo toàn chính xác bằng <b>84,206.0</b>.",
            s,
        )
    )
    pr_rows = [
        [
            "#1",
            "21969",
            "Nguyễn Kiệm",
            "Cấp 2",
            "3 → 1",
            "2.7799",
            "Chụm 3 nhánh vào 1 hướng vòng xoay Tân Sơn Nhất (Observed)",
        ],
        [
            "#2",
            "37345",
            "Cầu vượt Ngã tư Gò Dưa",
            "Cấp 3",
            "2 → 1",
            "2.7196",
            "Nút thắt cầu vượt Quốc lộ 1 – Thủ Đức (Cold-Start, 0 GPS)",
        ],
        [
            "#3",
            "34723",
            "Quốc lộ 1",
            "Cấp 1",
            "2 → 1",
            "2.6894",
            "Trục vành đai liên tỉnh tải trọng xe hàng lớn (Cold-Start)",
        ],
        ["#4", "40948", "Quốc lộ 1", "Cấp 1", "2 → 1", "2.6516", "Nhánh gom tuyến vành đai phía Đông – Thủ Đức"],
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
            "Đoạn dẫn cầu Kênh Xáng kết nối Quận 8 vào trung tâm Quận 5",
        ],
        [
            "#7",
            "15011",
            "Phan Đăng Lưu",
            "Cấp 2",
            "2 → 2",
            "2.6159",
            "Trục huyết mạch Phú Nhuận – Bình Thạnh hướng Hàng Xanh",
        ],
        [
            "#8",
            "40916",
            "Quốc lộ 1",
            "Cấp 1",
            "2 → 1",
            "2.5677",
            "Đoạn nối vành đai tập trung lưu lượng cửa ngõ Tây – Bắc",
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
            "Hành lang cửa ngõ sân bay Tân Sơn Nhất (chụm 3 vào 2)",
        ],
    ]
    _add_captioned_table(
        story,
        ["Hạng", "Mã Đoạn", "Tuyến Đường", "Cấp", "Vào/Ra", "PageRank", "Ý nghĩa Cấu trúc Giao thông Thực tế (UC-3)"],
        pr_rows,
        [34, 44, 92, 36, 44, 56, 209],
        "Bảng 4.1. Top-10 nút cổ chai cấu trúc toàn mạng lưới TP.HCM theo Line-Graph PageRank (30 vòng lặp, β = 0.85).",
        s,
    )

    story.append(Paragraph("4.2. Khai Phá Luật Kẹt Đồng Thời &amp; Lan Truyền Trễ (SON / Spark FPGrowth)", s["h2"]))
    story.append(
        Paragraph(
            (
                "Mỗi khung 30 phút trong tập organic TRAIN tạo một giỏ gồm các tuyến đường cơ giới chuẩn hóa NFC đang "
                "kẹt cứng (<i>LOS E/F</i>; đã loại nhãn Phường/Quận R3). Trên <i>Apache Spark 3.5.7</i>, thuật toán "
                f"<b>FPGrowth</b> trích xuất <b>{gt['spark_fp_rules']} luật kết hợp</b>; song song đó thuật toán "
                "<b>2-Pass SON (Savasere–Omiecinski–Navathe — thuật toán phân tán 2 lượt quét bảo đảm không bỏ sót "
                f"tập phổ biến toàn cục)</b> [9] khai phá <b>{gt['co_rules']} luật kẹt đồng thời</b> và "
                f"<b>{gt['lag_rules']} luật lan truyền trễ +30 phút</b> có kiểm soát tính dai dẳng "
                "(<b><i>persistence_controlled = True</i></b> — tức loại bỏ các trường hợp tuyến đường đích vốn đã "
                "kẹt sẵn ở thời điểm <i>t</i>, chỉ giữ ca thực sự mới chuyển sang kẹt ở <i>t + 30 phút</i> để tránh "
                "nhầm quán tính sẵn có là lan truyền), tổng hợp tiêu biểu tại Bảng 4.2."
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
            "Nút giao BV Từ Dũ – trục Trần Hưng Đạo Q.1",
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
            "Tô Hiến Thành ↔ Lý Thường Kiệt (17 cửa sổ)",
            "0.0552",
            "89.5% / 20.0%",
            "3.2421x",
            "Bền vững trên hold-out (20 khung, 54.1%)",
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
            "Giả thuyết làn xe di chuyển qua Q.3 sau 30p",
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
    _add_captioned_table(
        story,
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
        "Bảng 4.2. Các luật kẹt xe đồng thời và lan truyền trễ +30 phút tiêu biểu (khai phá trên organic TRAIN).",
        s,
    )
    story.append(
        make_callout(
            (
                "<b>Đánh giá trung thực về hạn chế cỡ mẫu nhỏ:</b> Các luật lan truyền sau 30 phút có hệ số Lift cao "
                "(tới 24.0x khi đã kiểm soát persistence) nhưng chỉ cùng xuất hiện trong <b>5 đến 8 cửa sổ 30 phút</b> "
                "(2.5–4.0 giờ quan sát) do độ phủ GPS thưa (11.87%). Trong khi cặp <i>Tô Hiến Thành ↔ Lý Thường "
                "Kiệt</i> lặp lại bền vững trên 20 khung hold-out, 14 luật trễ chỉ được dùng như <b>giả thuyết cảnh "
                "báo thăm dò</b> kết hợp kiểm chứng trên đồ thị đường bộ thay vì quy luật cứng."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch4_kmeans_bridge_and_limits(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(
        Paragraph(
            (
                f"4.3. Phân 6 Nhóm Hành Vi 7D K-Means (Organic Train-Only) Phủ Kín {gt['clean_segs']} Đoạn "
                "&amp; Cầu Nối Sang Chương 5"
            ),
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                f"Pipeline <b>StandardScaler + KMeans(k=6, seed=42)</b> chỉ học trên <b>{gt['km_segs']} đoạn thuộc "
                f"organic TRAIN</b> trong không gian 7 đặc trưng chuẩn hóa, đạt <b>Silhouette Score = {gt['km_sil']}"
                "</b> (thước đo độ gắn kết nội cụm so với cụm lân cận nằm trong <i>[−1, +1]</i>; mức 0.2241 là hợp lý "
                "với hình thái đường phố đô thị chuyển tiếp liên tục từ hẻm dân cư đến trục phụ và đại lộ thay vì tách "
                f"rời thành các đảo cô lập), <b>Davies-Bouldin = {gt['km_dbi']}</b> và "
                f"<b>Calinski-Harabasz = {gt['km_ch']}</b> (tương đồng với "
                f"Spark MLlib KMeans cost = {gt['spark_km_cost']}). Từ 6 tâm cụm này, toàn bộ <b>{gt['clean_segs']} "
                f"đoạn cơ giới</b> (bao gồm <b>{gt['cold_segs']} đoạn Cold-Start</b>) được gán nhóm hành vi như thể "
                "hiện trên bản đồ Hình 4.1 và thống kê định lượng tại Bảng 4.3. Đồng thời, Hình 4.2 chứng minh vai "
                "trò cầu nối trực tiếp của nhóm đặc trưng bản đồ và phân cụm từ Chương 4 (chiếm 10.50% tổng trọng số "
                "quyết định trong vector 23 chiều của Random Forest ở Chương 5)."
            ),
            s["body"],
        )
    )
    _add_centered_figure(
        story,
        CHART_DIR / "map_hcmc_clusters_7d.png",
        0.52,
        f"Hình 4.1. Bản đồ 6 nhóm hành vi đường phố (7D K-Means, k = 6) phủ kín 100% mạng lưới {gt['clean_segs']} "
        "đoạn cơ giới TP.HCM trên nền sông Sài Gòn và các quận.",
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
            "Nội đô hẹp, Vmax ~43 km/h, dễ ùn ứ",
        ],
        [
            "Nhóm 1",
            "8,718",
            "20.44 km/h",
            "50.29 km/h",
            "43.26%",
            "1.0097",
            "1,365 / 2,165",
            "Điểm nghẽn nội đô kẹt E/F cao nhất (43.26%)",
        ],
        [
            "Nhóm 2",
            "9,400",
            "44.87 km/h",
            "51.07 km/h",
            "7.30%",
            "1.0086",
            "4,464 / 53,330",
            "Đường nhánh thông thoáng (phủ 53,330 Cold-Start)",
        ],
        [
            "Nhóm 3",
            "454",
            "65.38 km/h",
            "50.54 km/h",
            "14.32%",
            "0.9924",
            "132 / 0",
            "Tuyến tốc độ cao / cửa ngõ (100% có GPS)",
        ],
        [
            "Nhóm 4",
            "7,542",
            "33.51 km/h",
            "60.27 km/h",
            "19.80%",
            "1.1944",
            "1,797 / 4,434",
            "Trục chính &amp; nút cổ chai PageRank cao nhất",
        ],
        [
            "Nhóm 5",
            "1,203",
            "41.29 km/h",
            "59.96 km/h",
            "10.31%",
            "0.9880",
            "357 / 2,651",
            "Đoạn liên kết dài (~233.9m), tốc độ cao, ít kẹt",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Nhóm",
            "Cửa sổ",
            "Vận tốc TB",
            "Tốc độ Max",
            "Kẹt E/F",
            "PageRank",
            "Có / Thiếu GPS",
            "Đặc tính Hành vi (UC-5)",
        ],
        cl_rows,
        [48, 44, 52, 52, 44, 56, 76, 143],
        "Bảng 4.3. Đặc tính định lượng 6 cụm hành vi đường phố 7D K-Means phủ kín 84,206 đoạn cơ giới TP.HCM.",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_feature_23d_importance.png",
        0.84,
        "Hình 4.2. Phân bổ độ quan trọng của vector 23 đặc trưng trong Random Forest (model.joblib): kết hợp trạng "
        "thái hiện tại (78.13%), chu kỳ giờ VN &amp; hình học (11.37%) với nhóm đặc trưng đồ thị PageRank, K-Means "
        "và tỷ lệ lịch sử từ Chương 4 (10.50%).",
        s,
    )
    story.append(Paragraph("4.4. Hạn Chế &amp; Rủi Ro Thành Phần Đồ Thị PageRank, SON &amp; 7D K-Means", s["h2"]))
    story.append(
        make_callout(
            (
                "<b>Hạn chế &amp; Rủi ro thành phần Khai phá Đồ thị &amp; Phân cụm:</b> "
                "<b>(1) Cỡ mẫu nhỏ của 14 luật lan truyền trễ (+30m):</b> Chỉ xuất hiện 5–8 cửa sổ do độ phủ GPS "
                "thưa nên chỉ mang giá trị giả thuyết tham khảo. "
                f"<b>(2) Trọng số rẽ đồng nhất trong Line-Graph PageRank:</b> {gt['lg_edges']} cạnh chia đều xác suất "
                "<i>1 / out_degree</i> vì bản đồ OpenStreetMap chưa có lưu lượng rẽ theo làn. "
                f"<b>(3) Đặc trưng tĩnh cho vùng Cold-Start ({gt['cold_segs']} đoạn):</b> Nhãn cụm K-Means dựa vào "
                "hình học và Vmax suy diễn nên chưa nhận diện các biến cố tức thời như họp chợ hay rào chắn thi công."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch5_split_features_and_ablation(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(CondPageBreak(220))
    story.append(
        Paragraph(
            "Chương 5. Bước 3 — Mô Hình Predictive ML (Random Forest 23D) Dự Báo Trước 30 Phút &amp; "
            "Đối Chứng Trực Diện Với Persistence Baseline",
            s["h1"],
        )
    )
    story.append(Paragraph("5.1. Ghép Cặp Đúng +30 Phút, Lọc Burst Tổng Hợp &amp; Cắt Sạch Biên Thời Gian", s["h2"]))
    story.append(
        Paragraph(
            (
                "Do xe gửi GPS ngắt quãng (15,514 / 23,399 cặp liên tiếp cách nhau ≥ 60 phút = 66.30%), phép trượt "
                "dòng <i>shift(-1)</i> sẽ làm lệch chân trời dự báo. Hệ thống chỉ ghép nhãn bằng phép nối chính xác "
                f"<b>(segment_id, slot_ts + 30 phút)</b> thu được <b>{gt['t30m_pairs']} cặp chuẩn xác</b>, đồng thời "
                f"<b>loại hoàn toàn {gt['burst_n']} burst windows tổng hợp (3,596 cặp khỏi TRAIN)</b> và cắt bỏ mẫu "
                "sát biên có nhãn vắt sang tập sau theo Bảng 5.1."
            ),
            s["body"],
        )
    )
    split_rows = [
        [
            "Giai đoạn Khởi tạo",
            "slot_ts ≤ 2020-08-02T23:30:00Z",
            "Cố định tâm cụm K-Means &amp; tỷ lệ kẹt nền ban đầu",
            "Không rò rỉ dữ liệu sang TRAIN/VAL/TEST",
        ],
        [
            "Tập Organic TRAIN",
            "slot_ts ≤ 2020-12-12T08:00:00Z",
            f"{gt['train_n']} mẫu tự nhiên (cắt 50 biên &amp; tách 3,596 burst)",
            "max(target_ts) = 2020-12-12T08:00:00Z",
        ],
        [
            "Tập VAL (15%)",
            "2020-12-12T08:00Z &lt; slot_ts ≤ 2020-12-25T03:30:00Z",
            f"{gt['val_n']} mẫu hợp lệ (đã cắt 5 mẫu sát biên)",
            "max(target_ts) = 2020-12-25T03:30:00Z",
        ],
        [
            "Tập TEST (15%)",
            "slot_ts &gt; 2020-12-25T03:30:00Z",
            f"{gt['test_n']} mẫu kiểm thử độc lập ({gt['test_sev']} mẫu kẹt LOS E/F)",
            "Chỉ chấm điểm đúng 1 lần sau khi chốt mô hình",
        ],
    ]
    _add_captioned_table(
        story,
        ["Tập Dữ liệu", "Điều kiện Mốc Thời gian (UTC)", "Quy mô Sau Lọc &amp; Cắt Biên", "Bảo chứng Chống Rò rỉ"],
        split_rows,
        [82, 168, 152, 113],
        "Bảng 5.1. Quy chuẩn phân chia Chronological 70/15/15, loại bỏ burst tổng hợp và cắt sạch biên thời gian.",
        s,
    )

    story.append(Paragraph("5.2. Tỷ Lệ Kẹt Tích Lũy Quá Khứ (τ &lt; t) &amp; Tổ Hợp 23 Biến Đầu Vào", s["h2"]))
    story.append(
        Paragraph(
            (
                "Để vừa tận dụng trí nhớ lịch sử của từng đoạn đường và tuyến đường vừa bảo đảm không rò rỉ tương "
                "lai, Công thức (5.1) chỉ cộng dồn tỷ lệ kẹt quá khứ nghiêm ngặt <i>τ &lt; t</i>. Toàn bộ 23 biến "
                "đầu vào được phân nhóm và định lượng đóng góp trong Bảng 5.2 (trực quan hóa tại Hình 4.2)."
            ),
            s["body"],
        )
    )
    story.append(
        make_equation_box(
            "5.1",
            "Tỷ Lệ Kẹt Tích Lũy Quá Khứ Nghiêm Ngặt (τ < t) Chống Rò Rỉ Tương Lai",
            CHART_DIR / "eq_prior.png",
            "<b>Kiểm thử Future-Shuffle (sai lệch = 0.0):</b> Khi đảo trộn ngẫu nhiên toàn bộ tương lai τ &gt; t, "
            "cả 23 biến đầu vào tại thời điểm t giữ nguyên tuyệt đối (<i>max_abs_diff = 0.0</i>).",
            s,
        )
    )
    feat_rows = [
        [
            "1. Trạng thái hiện tại &amp; lịch sử gần (7 biến · 78.13%)",
            "mean_velocity (0.4635), current_los_ordinal (0.1756), velocity_ratio (0.1064), obs_count (0.0142), "
            "lag1_los_ordinal (0.0120), lag2_los_ordinal (0.0074), trailing_severe_rate (0.0022)",
        ],
        [
            "2. Chu kỳ giờ Việt Nam &amp; hình học đường (7 biến · 11.37%)",
            "period_sin (0.0425), period_cos (0.0189), max_velocity (0.0175), gap_minutes (0.0155), length_m "
            "(0.0109), weekday (0.0076), street_level (0.0008)",
        ],
        [
            "3. Đặc trưng bản đồ Chương 4 &amp; tỷ lệ quá khứ (9 biến · 10.50%)",
            "pagerank_score (0.0277), neighbor_los_ordinal (0.0235), street_causal_prior_los (0.0214), "
            "segment_train_severe_prior (0.0101), segment_train_prior_los (0.0091), street_causal_prior_sev "
            "(0.0080), cluster_id (0.0046), in_degree (0.0003), out_degree (0.0002)",
        ],
    ]
    _add_captioned_table(
        story,
        ["Nhóm Biến Đầu vào (Tổng = 23 biến)", "Danh sách Biến &amp; Mức Đóng Góp Trong Random Forest"],
        feat_rows,
        [154, 361],
        (
            "Bảng 5.2. Phân nhóm vector 23 đặc trưng đầu vào và trọng số đóng góp thực tế trong Random Forest "
            "(model.joblib)."
        ),
        s,
    )
    story.append(
        make_callout(
            (
                "<b>Lưu ý thuật ngữ tên cột <i>street_causal_prior_*</i>:</b> Tiền tố <i>causal_prior</i> trong hai "
                "biến <i>street_causal_prior_los</i> và <i>street_causal_prior_sev</i> biểu thị <b>tiên nghiệm quá "
                "khứ chặt theo trục thời gian (τ &lt; t, causal temporal filtering)</b> — chỉ cộng dồn quan sát đã "
                "xảy ra trước thời điểm dự báo <i>t</i> để loại trừ rò rỉ tương lai, <b>không phải</b> suy luận nhân "
                "quả can thiệp (<i>do-calculus</i>)."
            ),
            s,
        )
    )
    story.append(Spacer(1, 4))

    story.append(
        Paragraph(
            "5.3. Tuyển Chọn Qua Live VAL Composite Gate, Bảng 7 Cấu Hình &amp; Ma Trận Phân Loại 6 Mức",
            s["h2"],
        )
    )
    story.append(
        make_callout(
            (
                "<b>Định nghĩa mốc đối chứng &amp; thước đo cốt lõi:</b> "
                "<b>(1) Persistence Baseline</b> là mô hình đối chứng tối giản <b>không dùng Machine Learning "
                "(0-ML)</b> do chính nhóm tự thiết lập theo quy tắc <i>LOS<sub>dự báo</sub>(t+30p) = LOS(t)</i> — giả "
                "định trạng thái sau 30 phút không đổi, đại diện cho bản đồ chỉ giám sát hiện trạng chưa có AI. Vì "
                "<b>94.84%</b> mẫu TEST giữ nguyên trạng thái sau 30 phút, việc đối chứng trực diện với "
                "<i>Persistence Baseline</i> là bắt buộc để tránh ngộ nhận độ chính xác tổng thể (Accuracy). "
                "<b>(2) VAL Composite Gate</b> chốt mô hình và ngưỡng quyết định chỉ trên tập kiểm định VAL bằng "
                "trung bình <i>(val_acc + val_pr + val_roc) / 3</i>, ngăn chặn nhìn trộm tập TEST "
                "(<i>test-set peeking</i>). "
                "<b>(3) ROC-AUC &amp; PR-AUC:</b> <i>ROC-AUC</i> (<i>Receiver Operating Characteristic AUC</i>) đo "
                "phân tách tổng quát mọi ngưỡng, nhưng trên tập mất cân bằng nặng (<b>81.13% LOS A</b>), bắt buộc "
                "đánh giá bằng <b>PR-AUC</b> (<i>Precision–Recall AUC</i>) để đo thực chất độ nhạy – độ chính xác "
                "của lớp kẹt cứng hiếm (LOS E/F)."
            ),
            s,
            bg=CALLOUT_BG,
            border=CALLOUT_BORDER,
        )
    )
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            (
                f"Hệ thống huấn luyện sạch 7 cấu hình ứng viên [11] trên <b>{gt['train_n']} mẫu organic TRAIN</b> "
                "(ablation tách burst: kèm burst 5,481 mẫu đạt TEST Acc = 0.9399 / Severe F1 = 0.9344, so với "
                f"organic-only đạt Acc = {gt['rf_acc']} / Severe F1 = {gt['rf_f1']}) bằng trọng số lớp mềm lũy thừa "
                "0.65 (không dùng sample_weight nhân tạo). Qua cổng chọn <b>Live VAL Composite Gate = (val_acc + "
                f"val_pr + val_roc) / 3</b> trên <b>{gt['val_n']} mẫu VAL</b>, <b>#5 Random Forest (23 biến)</b> "
                "thắng ở <b>0.8882</b> (VAL Acc = 0.8166, VAL PR = 0.8912, VAL ROC = 0.9567, τ_severe = 0.25, "
                "τ_change = 0.40, τ_onset = 0.25), vượt <b>DT 23D (0.8812)</b>, <b>DT 14D (0.8115)</b> và "
                "<b>RF 14D (0.6794)</b>. Kết quả đánh giá một lần trên <b>1,182 mẫu TEST</b> được báo cáo tại Bảng "
                "5.3 và Hình 5.1; ma trận phân loại 6 mức chi tiết thể hiện tại Bảng 5.4 và biểu đồ nhiệt Hình 5.2. "
                "<i>Lưu ý đối chiếu Spark MLlib:</i> Mô hình <i>Spark MLlib RF 23D</i> (Bảng 3.3, không áp dụng "
                f"trọng số lớp lũy thừa 0.65) đạt Acc = <b>{gt['spark_rf_acc']}</b> / ROC-AUC = "
                f"<b>{gt['spark_rf_roc']}</b> trên cùng 1,182 mẫu TEST — cao hơn sklearn ({gt['rf_acc']}) chủ yếu "
                "do ưu tiên lớp đa số LOS A, trong khi <i>sklearn RF 23D</i> chủ đích tăng độ nhạy cho các lớp kẹt "
                "và ca chuyển pha hiếm."
            ),
            s["body"],
        )
    )
    ab_rows = [
        [
            "1. Persistence Baseline (0-ML: đoán LOS(t+30p) = LOS(t))",
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
            "2. Decision Tree (Chỉ biến cục bộ)",
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
            "3. Decision Tree (+PageRank &amp; K-Means)",
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
            gt["rf_acc"],
            gt["rf_mf1"],
            gt["rf_prec"],
            gt["rf_rec"],
            gt["rf_f1"],
            gt["rf_roc"],
            gt["rf_pr"],
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
    _add_captioned_table(
        story,
        [
            "Cấu hình Thử nghiệm (Mô hình vận hành chính: #5 RF 23D)",
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
        f"Bảng 5.3. Kết quả kiểm thử một lần của 7 cấu hình ứng viên trên {gt['test_n']} mẫu TEST (chọn #5 RF 23D "
        "qua Live VAL Gate 0.8882).",
        s,
        highlight_row=5,
    )

    cm_rows = [
        ["Thực tế LOS A (959 · 81.13%)", "944", "7", "0", "4", "1", "3"],
        ["Thực tế LOS B (10 · 0.85%)", "3", "3", "2", "0", "0", "2"],
        ["Thực tế LOS C (15 · 1.27%)", "6", "0", "1", "5", "0", "3"],
        ["Thực tế LOS D (13 · 1.10%)", "2", "5", "3", "3", "0", "0"],
        ["Thực tế LOS E (20 · 1.69%)", "1", "1", "1", "6", "5", "6"],
        ["Thực tế LOS F (165 · 13.96%)", "1", "2", "1", "6", "17", "138"],
    ]
    _add_captioned_table(
        story,
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
        f"Bảng 5.4. Kết quả phân loại từng mức LOS A–F trên {gt['test_n']} mẫu TEST của Random Forest (23 biến): "
        "đường chéo chính [944, 3, 1, 3, 5, 138] nhận diện đủ cả 6 lớp (Bootstrap 95% CI: ROC-AUC [0.9824, 0.9955], "
        f"Severe F1 [0.9091, 0.9602], PR-AUC diff [-0.0029, 0.0444]); Weighted-F1 = {gt['rf_wf1']}, Macro-F1 = "
        f"{gt['rf_mf1']} do lớp B–E thưa (10–20 mẫu).",
        s,
        keep_together=True,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_ablation_t30m.png",
        0.56,
        "Hình 5.1. So sánh hiệu năng cảnh báo kẹt cứng LOS E–F và phân loại 6 mức LOS A–F giữa các ứng viên trên "
        "tập TEST.",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_confusion_matrix_test.png",
        0.58,
        f"Hình 5.2. Biểu đồ nhiệt ma trận phân loại 6 mức LOS A–F trên {gt['test_n']} mẫu TEST của Random Forest "
        "(23 biến), làm rõ độ chính xác cao ở hai cực A/F và sự thưa mẫu ở các lớp trung gian B–E.",
        s,
    )


def _append_ch5_transitions_and_limits(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(
        Paragraph(
            "5.4. So Sánh Trực Diện Persistence Baseline vs Random Forest (23 Biến) Khi Giao Thông Thay Đổi Trạng Thái",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                f"Trên tổng thể {gt['test_n']} mẫu TEST, <b>{gt['steady_pct']} (1,121 / 1,182 mẫu)</b> giữ nguyên "
                "mức LOS sau 30 phút nên <b>Persistence Baseline</b> (mốc 0-ML sao chép nguyên hiện trạng) đạt "
                "0.9484. Tuy nhiên, tại <b>5.16% thời điểm chuyển pha thực sự</b> "
                f"({gt['trans_n']} ca đổi mức), khi GPS ngắt quãng dài (&gt; 30 phút) và trên <b>{gt['cold_pct']} "
                "đoạn đường trống GPS</b>, Bảng 5.5 và Hình 5.3 chứng minh ưu thế có ý nghĩa thống kê của Random "
                "Forest (23 biến). Ba thước đo chuyên sâu được sử dụng để đối chứng khách quan gồm: "
                "<b>(a) Kiểm định McNemar</b> — kiểm định ghép cặp phi tham số trên đúng cùng <b>61 ca đổi pha</b> "
                "của tập TEST để kiểm chứng ý nghĩa thống kê khi chỉ một mô hình đoán đúng (<i>b = 10</i> vs "
                "<i>c = 0</i>); <b>(b) Brier Score</b> — sai số bình phương trung bình của xác suất dự báo nằm trong "
                "<i>[0, 1]</i> (càng gần 0 càng hiệu chỉnh sát thực tế); và <b>(c) Masked-GPS Proxy</b> — lấy các "
                "đoạn <i>có</i> GPS trên tập TEST, cố tình che toàn bộ tín hiệu GPS hiện tại thành <i>NaN</i> để giả "
                "lập vùng mù 0-GPS rồi chấm điểm đối chiếu lại với nhãn LOS thật."
            ),
            s["body"],
        )
    )
    tr_rows = [
        [
            f"1. Nhóm đổi trạng thái\nLOS(t) ≠ LOS(t+30p)\n({gt['trans_n']} / {gt['test_n']} mẫu TEST)",
            "Đoán đúng mức mới: 0 / 61 (0.00%)\nDetect biến động ROC-AUC = 0.5000\n"
            "PR-AUC kẹt cứng = 0.5408\nSai số xác suất Brier = 0.3279",
            f"Detect biến động: ROC-AUC = {gt['detect_roc']} (PR-AUC = {gt['detect_pr']}; vs DT 0.8511)\n"
            "Đoán đúng mức mới: 14 / 61 (22.95% ở ngưỡng VAL τ=0.40, tổng 90.19%; mặc định 10/61 = 16.39%)\n"
            "Top-1 hướng mới: 19 / 61 (31.15%) · PR-AUC = 0.5556 · Brier = 0.2811",
            f"Baseline mù 100% (0/61); RF vượt trội có ý nghĩa thống kê (McNemar 10 vs 0, p = {gt['mcnemar_p']}) "
            "&amp; giảm 14.3% sai số Brier.",
        ],
        [
            f"2. Đường thoáng (A–D)\nđột ngột hóa kẹt (E–F)\n({gt['onset_n']} / {gt['non_sev_n']} mẫu TEST)",
            "Bắt trúng ở ngưỡng cứng: 0 / 8 (0.00%)\nGán xác suất kẹt = 0% đồng loạt cho mọi đoạn mức A–D",
            "Bắt trúng ở ngưỡng cứng mặc định: 0 / 8 (0.00% do cỡ mẫu quá hiếm 0.81%)\n"
            f"Xếp hạng rủi ro xác suất: Onset ROC-AUC = {gt['onset_roc']}; rủi ro cảnh báo tăng {gt['onset_lift']} "
            "(9.40% vs 2.88% đường thoáng ổn định)",
            f"Ghi nhận trung thực: chỉ có {gt['onset_n']} ca đột ngột trong TEST nên ngưỡng cứng chưa bắt kịp, "
            f"nhưng xác suất rủi ro RF tăng {gt['onset_lift']}.",
        ],
        [
            f"3. Tín hiệu GPS ngắt quãng\ncách nhau &gt; 30 phút\n({gt['gap_n']} mẫu · 49.21% đổi mức)",
            f"PR-AUC kẹt cứng = {gt['gap_pers_pr']}\nROC-AUC = 0.7940\n(quán tính cũ suy giảm khi ngắt lâu)",
            f"PR-AUC kẹt cứng = {gt['gap_rf_pr']} (+6.73 điểm)\nROC-AUC = 0.7913\n(chu kỳ giờ VN + PageRank + K-Means)",
            "Khi GPS gửi thưa (66.30% thực tế), biến chu kỳ giờ VN + đặc trưng đồ thị giúp RF vượt Baseline "
            "+6.73% PR-AUC.",
        ],
        [
            f"4. Vùng mù thiếu cảm biến\ntoàn mạng TP.HCM\n({gt['cold_segs']} / {gt['clean_segs']} đoạn)",
            "Độ phủ dự báo = 11.87% (Cold-Start = 0.0%)\n(không chạy được do thiếu LOS hiện tại)",
            f"Độ phủ dự báo = 100.0% (đủ {gt['clean_segs']} đoạn cơ giới)\n"
            f"Masked-GPS Proxy: 6-Class Acc = {gt['cold_acc']}, ROC-AUC = {gt['cold_roc']}, PR-AUC = {gt['cold_pr']}",
            f"Chỉ duy nhất Random Forest (23 biến) chấm điểm rủi ro được cho {gt['cold_pct']} đường nhánh chưa có GPS.",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Tình huống Kiểm định Thực tế",
            "Persistence Baseline (0-ML: giữ LOS(t))",
            "Mô hình chính: Random Forest (23 biến)",
            "Kết luận Thực tiễn &amp; Ghi nhận Khoa học",
        ],
        tr_rows,
        [102, 118, 152, 143],
        "Bảng 5.5. Đối chứng trực diện 1-đối-1 giữa Persistence Baseline (0-ML) và Random Forest (23 biến). Mọi giới "
        f"hạn thực nghiệm (10/61 mặc định → 14/61 ở τ=0.40, McNemar p={gt['mcnemar_p']}, Hard Recall 0/8 ca hiếm "
        f"&amp; Macro-F1 = {gt['rf_mf1']}) được báo cáo minh bạch.",
        s,
        keep_together=True,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_transition_analysis.png",
        0.60,
        "Hình 5.3. So sánh trực diện Baseline và Random Forest (23 biến) ở các tình huống khó nhất (trên), cùng "
        f"phân bố mẫu kiểm thử giải thích vì sao {gt['steady_pct']} đường giữ nguyên trạng thái tĩnh (dưới).",
        s,
    )
    story.append(Paragraph("5.5. Hạn Chế &amp; Rủi Ro Thành Phần Mô Hình Dự Báo &amp; Thực Nghiệm", s["h2"]))
    story.append(
        make_callout(
            (
                "<b>Hạn chế &amp; Rủi ro thành phần Mô hình Dự báo:</b> "
                f"<b>(1) Độ nhạy ngưỡng cứng trên ca đột ngột hóa kẹt (0 / {gt['onset_n']}):</b> Trong "
                f"{gt['non_sev_n']} mẫu TEST thông thoáng chỉ có {gt['onset_n']} ca (0.81%) đột ngột chuyển kẹt cứng, "
                f"nên ngưỡng cứng chưa bắt trúng dù Onset ROC-AUC = {gt['onset_roc']} và risk lift = "
                f"{gt['onset_lift']}. "
                f"<b>(2) Bảo thủ chuyển pha &amp; Macro-F1 ({gt['rf_mf1']}):</b> Các lớp trung gian B–E chỉ có 10–20 "
                f"mẫu TEST kéo Macro-F1 xuống {gt['rf_mf1']} dù Severe F1 = {gt['rf_f1']}. "
                "<b>(3) Thiếu biến ngoại sinh &amp; thiên lệch mùa khô:</b> Mô hình chưa tích hợp mưa ngập hay sự cố "
                "giao thông tức thời, và tập TEST (12/2020–04/2021) rơi chủ yếu vào mùa khô."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch6_streaming_and_recon(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(CondPageBreak(220))
    story.append(
        Paragraph(
            "Chương 6. Bước 4 — Prescriptive Speed Layer (O(1) Stream Sketches 33.0 KiB), "
            "Two-Tier Batch–Stream Reconciliation &amp; Serving Layer",
            s["h1"],
        )
    )
    story.append(
        Paragraph(
            f"6.1. Kafka 4.1.0 KRaft Ingestion, Watermark (30m) Dual Sink &amp; WAL Crash-Recovery ({gt['wal_kb']})",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                f"Trình phát <b>producer.py</b> (<i>seed = 42</i>) đẩy luồng cửa sổ "
                f"(<b>{gt['stream_events']} events</b> = {gt['windows_n']} cửa sổ cơ giới + {gt['dupes_caught']} "
                "duplicates, gồm 1,168 on-time late ≤ 30p "
                f"và {gt['late_wins']} beyond-watermark late &gt; 30p trên {gt['late_slots']} khung giờ) cùng luồng "
                f"Raw GPS Pings (<b>91,694 events</b> = {gt['clean_pings']} hợp lệ + 907 trùng) qua "
                "<b>apache/kafka:4.1.0 KRaft</b> [14] (3 partitions, kiểm chứng 3,000 JSON messages). "
                "Mô-đun <b>Watermark &amp; Windowing [C6]</b> ghi nguyên vẹn 100% sự kiện vào <b>Medallion Lakehouse "
                "[C14]</b> và kiểm chứng watermark thật trên Spark Structured Streaming (<i>2020-12-01T12:30:00Z</i>, "
                "evict 10 rows hết hạn khỏi State Store). Thử nghiệm ngắt đột ngột "
                "<i>starter_stream.py --kill-after-batch 1</i> lưu nhật ký ghi trước <b>WAL (Write-Ahead Log — nhật "
                f"ký ghi trạng thái xuống đĩa trước khi xác nhận xử lý) Checkpoint = {gt['wal_kb']} "
                f"({gt['wal_bytes']} Bytes)</b> tại [C15], tự bỏ qua batch 1 đã commit và tiếp tục "
                "micro-batch 2..3 tới 90,741 sự kiện trong watermark với <b>0 duplicate side effects</b>."
            ),
            s["body"],
        )
    )

    story.append(
        Paragraph("6.2. Cấu Trúc O(1) Stream Sketches (33.0 KiB) &amp; EWMA Hotspot Bổ Trợ Random Forest", s["h2"])
    )
    story.append(
        Paragraph(
            (
                "Trên luồng dữ liệu 24/7, <b>Random Forest (Chương 5)</b> nhìn về tương lai <i>t + 30m</i> để cảnh "
                "báo sớm nguy cơ kẹt cứng, trong khi <b>EWMA (Exponentially Weighted Moving Average — trung bình "
                "trượt hàm mũ, hệ số suy giảm α = 0.15)</b> đo độ lì ùn tắc ngay hiện tại "
                "(tách biệt nút nghẽn mãn tính như #47407 Lý Thường Kiệt khỏi chậm cục bộ thoáng qua). Đồng thời, "
                "<b>RotatingBloomFilter (32.0 KiB)</b> (kiểm soát <b>FPR — False Positive Rate</b>, tỷ lệ báo trùng "
                "giả của bộ lọc xác suất, xuống chỉ 0.0389%) và <b>64-bit HyperLogLog++ (1.0 KiB)</b> [9] lọc trùng "
                "và đếm độ phủ trong đúng <b>33.0 KiB RAM O(1)</b> theo Công thức (6.1), kiểm chứng chi tiết tại "
                "Bảng 6.1."
            ),
            s["body"],
        )
    )
    story.append(
        make_equation_box(
            "6.1",
            "Cấu Trúc Bộ Nhớ O(1) (33.0 KiB) Cho RotatingBloomFilter, 64-Bit HLL++ &amp; EWMA Hotspot",
            CHART_DIR / "eq_sketches.png",
            f"<b>Bộ nhớ cố định 33.0 KiB không phình to theo thời gian:</b> {gt['bloom_kb']} RotatingBloomFilter "
            f"(2 thế hệ × 16.0 KiB = 32,768 Bytes) cộng {gt['hll_kb']} HyperLogLog++ (1,024 Bytes uint8); EWMA "
            "Hotspot tự động prune các đoạn đã thông thoáng khi điểm giảm dưới ngưỡng ε = 10⁻⁴.",
            s,
        )
    )
    sketch_rows = [
        [
            "RotatingBloomFilter\n(Deduplication)",
            "2 thế hệ × 16.0 KiB (262,144 bits = 32,768 Bytes = 32.0 KiB), k = 10 hàm băm, capacity 8,000/gen.",
            f"Chặn chính xác {gt['dupes_caught']} / {gt['dupes_caught']} (100%) duplicates; chỉ {gt['bloom_fp_cnt']} "
            f"FPs — empirical FPR = {gt['bloom_fpr']} (&lt; 0.0792% theoretical bound).",
        ],
        [
            "64-bit HyperLogLog++\n(Active Cardinality)",
            "p = 10 → 1,024 registers đóng gói trong mảng uint8[1024] = đúng 1,024 Bytes (1.0 KiB), RSE = 3.25%.",
            f"Ước lượng {gt['hll_est']} active segments so với {gt['active_segs']} đoạn thực tế (sai số "
            f"{gt['hll_err']} &lt; 5.0%).",
        ],
        [
            "EWMA Hotspot Decay\nTracker",
            "Exponential decay factor α = 0.15 mỗi khung 30 phút; auto-pruning khi score &lt; ε = 10⁻⁴ (77 khóa cuối).",
            "Bắt chính xác Top-3 kẹt dai dẳng: #1 47407 Lý Thường Kiệt (6.6652), #2 21263 Trương Định (6.6635), "
            "#3 22684 Nguyễn Đình Chiểu (6.6300).",
        ],
    ]
    _add_captioned_table(
        story,
        ["O(1) Stream Sketch / Tracker [C7]", "Cấu trúc Bộ nhớ O(1) &amp; Tham số", "Kết quả Kiểm chứng Thực tế"],
        sketch_rows,
        [124, 182, 209],
        "Bảng 6.1. Thông số bộ nhớ O(1) (33.0 KiB) và kết quả kiểm chứng thực nghiệm các bộ đếm luồng [C7].",
        s,
    )

    story.append(
        Paragraph(
            "6.3. Two-Tier Batch–Stream Reconciliation, Late-Data Backfill &amp; Atomic $status_version Guard",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "<b>Tier 1 (On-Time Subset Parity ≤ 30m):</b> Phép đối chiếu hai chiều <i>exceptAll() == 0</i> "
                "(<i>tier1_exceptAll_diff = 0</i>) xác nhận <b>100.0% in-watermark parity</b> giữa Speed và Batch. "
                "<b>Tier 2 (Late-Data Backfill &amp; Leak-Free Re-Scoring):</b> Cuối ngày, <b>[C12]</b> quét lại "
                f"<b>Medallion Lakehouse [C14]</b> để backfill đủ <b>{gt['late_wins']} beyond-watermark late windows "
                f"(&gt; 30m)</b> trên <b>{gt['late_slots']} khung giờ</b>, nâng completeness từ "
                "<b>98.50% → 100.00%</b>; "
                f"đồng thời hàn gắn lag/priors cho <b>{gt['healed_lags']} downstream windows</b> và <b>re-score "
                f"{gt['rescored_wins']} forecast windows</b> ({gt['prob_changed']} cửa sổ cập nhật xác suất). Kho "
                f"MongoDB 8.0 lưu <b>{gt['windows_n']} documents</b> theo vòng đời: <b>{gt['prov_retained']} "
                "STREAM_PROVISIONAL (v1)</b>, <b>500 BATCH_RECONCILED (v2)</b> và <b>32,405 BATCH_VERIFIED (v2)</b>; "
                f"atomic <b>$status_version pipeline guard</b> chặn đứng <b>{gt['mongo_stale_rej']}/"
                f"{gt['mongo_stale_rej']}</b> stale v1 events ghi đè lùi lên v2."
            ),
            s["body"],
        )
    )


def _append_ch6_serving_ui_and_limits(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(
        Paragraph(
            "6.4. MongoDB 8.0.34 Serving Store (&lt; 1 ms) &amp; ReactJS + FastAPI TOC Command Center (:8000)",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Tầng phục vụ vận hành kết hợp kho tài liệu đánh chỉ mục kép <i>MongoDB 8.0.34</i> [15] với cổng "
                "điều hành hợp nhất <i>ReactJS + FastAPI TOC Command Center (:8000)</i>. Chỉ mục ghép "
                "<i>level_prob_segment_idx</i> tuân thủ quy tắc <b>ESR (Equality – Sort – Range — sắp cột theo điều "
                "kiện bằng → sắp xếp → khoảng)</b> giúp truy vấn vận hành theo cơ chế <b>IXSCAN (Index Scan — quét "
                "trực tiếp cây chỉ mục B-Tree dưới 1 ms, 0 SORT trên RAM)</b> thay vì <b>COLLSCAN (Collection Scan — "
                "quét tuần tự toàn bộ tài liệu trong bảng)</b>. Bảng 6.2 và Hình 6.1 tổng "
                "hợp kết quả đo đạc độ trễ dưới 1 ms; các giao diện thực nghiệm trực tiếp trên hệ thống sống được "
                "minh họa qua Hình 6.2 (kịch bản sáng <i>#50026 Điện Biên Phủ</i>), Hình 6.3 (kịch bản chuyển pha "
                "<i>#21064 Công Trường Quốc Tế</i> kèm kiểm chứng IXSCAN) và Hình 6.4 (tra cứu vùng mù Cold-Start "
                "<i>#37345 Cầu vượt Ngã tư Gò Dưa</i>)."
            ),
            s["body"],
        )
    )
    serve_rows = [
        [
            "Idempotent Upsert Engine",
            f"Chia {gt['windows_n']} docs qua 4 write partitions (8,119 / 8,472 / 8,209 / 8,594), 69 batches × 500 "
            "UpdateOne(upsert=True) kèm $status_version guard.",
            f"Chạy replay nhiều lần giữ đúng {gt['windows_n']} docs (0 duplicates); chặn {gt['mongo_stale_rej']}/"
            f"{gt['mongo_stale_rej']} tin cũ v1.",
        ],
        [
            "Compound Index IXSCAN\n(UC-2)",
            "Compound Index level_prob_segment_idx {street_level: 1, severe_prob: -1, segment_id: 1}; lọc cấp 1–2 "
            "có P(Severe) ≥ 70%.",
            f"Khớp {gt['arterial_alerts']} cửa sổ trục chính, Top-25 qua [{gt['mongo_stages']}], 0 in-memory SORT, "
            f"warm median = {gt['mongo_med']} / p95 = {gt['mongo_p95']} (nhanh hơn {gt['mongo_speedup']} vs "
            f"COLLSCAN {gt['mongo_collscan']}).",
        ],
        [
            "ReactJS + FastAPI TOC\n(:8000)",
            "Trung tâm Điều hành TOC Command Center (ReactJS + Leaflet + FastAPI :8000) gồm 5 Tab truy vấn trực "
            "tiếp MongoDB 8.0 &amp; Kafka.",
            "5 Tab hợp nhất: (1) Command Center +30p · (2) Live Kafka &amp; 33 KiB Sketches · (3) PageRank &amp; "
            "Luật đồng kẹt · (4) 6 Cụm K-Means Cold-Start · (5) Spark 3.5.7 / Ablation / Recon.",
        ],
    ]
    _add_captioned_table(
        story,
        ["Serving Component [C16]–[C17]", "Thiết kế Compound Index &amp; Cơ chế Vận hành", "Kết quả Đo đạc Thực tế"],
        serve_rows,
        [120, 176, 219],
        "Bảng 6.2. Thiết kế và bằng chứng hiệu năng tầng phục vụ MongoDB 8.0.34 và cổng ReactJS + FastAPI TOC (:8000).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "chart_streaming_serving.png",
        0.58,
        f"Hình 6.1. Phân bố {gt['windows_n']} cửa sổ theo Two-Tier Reconciliation Lifecycle (trên) và độ trễ IXSCAN "
        f"{gt['mongo_med']} / p95 {gt['mongo_p95']} vs COLLSCAN {gt['mongo_collscan']} ({gt['mongo_speedup']}) trên "
        "MongoDB 8.0.34 (dưới).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "ui_tab1_overview_hotspots.png",
        0.86,
        "Hình 6.2. Giao diện ReactJS + FastAPI TOC Command Center (:8000 ↔ MongoDB 8.0) — Tab 01 (Điều Hành &amp; Dự "
        "Báo +30p) tại điểm nóng #50026 — Điện Biên Phủ (2021-01-04 07:00 ICT, 122 đoạn hoạt động, 38 cảnh báo sớm, "
        "dự báo chính xác chuyển pha E → F với P(E/F) = 87.2%) kèm thẻ kiểm chứng MongoDB 8.0 IXSCAN.",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "ui_tab2_live_forecast_mongo.png",
        0.86,
        "Hình 6.3. Cận cảnh Kịch bản #21064 — Công Trường Quốc Tế trên ReactJS + FastAPI TOC (:8000) (2021-04-22, "
        "thu thập [11:30–12:00), chốt phát lệnh t = 12:00 ICT dự báo cho [12:00–12:30): RF 23D kích hoạt cảnh báo "
        "P(E/F) = 68.7% &gt; 25% và xác suất đổi pha P(chg) = 48.9% &gt; 40% trước khi thực tế chuyển LOS F) kèm "
        f"bảng kiểm chứng trực tiếp MongoDB 8.0 IXSCAN ({gt['mongo_speedup']}, 0 SORT trên RAM).",
        s,
    )
    _add_centered_figure(
        story,
        CHART_DIR / "ui_tab4_kmeans_coldstart.png",
        0.86,
        f"Hình 6.4. Giao diện Tab 04 (6 Cụm K-Means &amp; Cold-Start · UC-5) trên ReactJS + FastAPI TOC (:8000): "
        f"thống kê 6 nhóm hành vi (Silhouette = {gt['km_sil']} trên {gt['km_segs']} đoạn TRAIN) phủ kín "
        f"{gt['cold_segs']} đoạn Cold-Start ({gt['cold_pct']}) và tra cứu đoạn #37345 — Cầu vượt Ngã tư Gò Dưa "
        "(0 GPS, PageRank = 2.7196, Cụm #4) đối chiếu các đoạn cảm biến tương đồng nhất.",
        s,
    )
    story.append(Paragraph("6.5. Hạn Chế &amp; Rủi Ro Thành Phần Xử Lý Luồng &amp; Giao Diện Điều Hành", s["h2"]))
    story.append(
        make_callout(
            (
                "<b>Hạn chế &amp; Rủi ro thành phần Xử lý Luồng &amp; Phục vụ:</b> "
                "<b>(1) Cửa sổ xoay vòng của RotatingBloomFilter (2 × 16.0 KiB):</b> Thiết kế xoay vòng 2 thế hệ "
                f"đã hạ empirical FPR xuống <b>{gt['bloom_fpr']}</b> ({gt['bloom_fp_cnt']} FPs); bản tin trùng đến "
                "sau hơn 2 thế hệ sẽ dựa vào chỉ mục duy nhất <i>event_id</i> trong MongoDB để chặn ghi trùng. "
                f"<b>(2) Độ trễ phản ánh của gói tin vượt Watermark (&gt; 30p):</b> {gt['late_wins']} cửa sổ trễ quá "
                "30 phút phải chờ <i>Two-Tier Reconciliation</i> cuối ngày mới bổ sung vào kho phục vụ. "
                "<b>(3) Chế độ Human-in-the-Loop UI:</b> Cổng TOC (:8000) đóng vai trò hỗ trợ quyết định cho điều "
                "hành viên, chưa kết nối vòng kín tự động xuống tủ đèn tín hiệu SCATS."
            ),
            s,
            bg=WARN_BG,
            border=WARN_BORDER,
        )
    )


def _append_ch7_team_tests_and_infra(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(CondPageBreak(220))
    story.append(
        Paragraph(
            "Chương 7. Phân Công Nhóm, Kiểm Thử Tự Động (44/44 pytest), Sẵn Sàng Sản Xuất &amp; Lộ Trình Triển Khai",
            s["h1"],
        )
    )
    story.append(Paragraph("7.1. Phân Công Chuyên Trách &amp; Kết Quả Bàn Giao Của Ba Thành Viên (100%)", s["h2"]))
    story.append(
        Paragraph(
            "Bảng 7.1 ghi nhận rõ phạm vi mã nguồn và các bằng chứng nghiệm thu bàn giao của ba thành viên nhóm.",
            s["body"],
        )
    )
    team_rows = [
        [
            "1. Nguyễn Tạ Minh Quyền\n(MSSV: 25MS23280)",
            "P1: Spark Batch ETL, Quality Funnel &amp; Medallion Lakehouse",
            "spark_batch.py, baseline.py,<br/>traffic_data/spark_etl.py,<br/>compose.quyen.yml,<br/>"
            "tests/test_quyen_spark.py",
            f"Ordered Quality Funnel 6 quy tắc R1–R6 (ANSI try_cast, loại 427 POIs), Unicode NFC &amp; UTC+7, "
            f"Train-Only Vmax, 5-Tuple Monoid (diff=0.0), Key Salting (684.95x → 1.020x), BroadcastHashJoin "
            f"(+{gt['join_speedup']}; spark_batch {gt['quyen_smj_s']}→{gt['quyen_bhj_s']}) &amp; Parquet Pruning.",
        ],
        [
            "2. Nguyễn Thành Đạt\n(MSSV: 25MS23306)",
            "P2 &amp; P3: Graph Mining, 7D K-Means &amp; +30m Forecasting",
            "pipeline.py, spark_engine.py,<br/>tests/test_platform_v3.py",
            f"Tỉa 90.97% shape points, Line-Graph PageRank {gt['clean_segs']} cơ giới ({gt['lg_edges']} cạnh, 30 "
            f"vòng), FPGrowth ({gt['spark_fp_rules']}) &amp; SON (12+14), 7D K-Means (Sil={gt['km_sil']}) phủ "
            f"{gt['cold_segs']} Cold-Start, tách burst → {gt['train_n']} organic TRAIN &amp; RF 23D (ROC="
            f"{gt['rf_roc']}, PR={gt['rf_pr']}, Sev-F1={gt['rf_f1']}, McNemar p={gt['mcnemar_p']}).",
        ],
        [
            "3. Phạm Công Minh\n(MSSV: 25MS23310)",
            "P4: Kafka Streaming, Recon, MongoDB &amp; Web TOC (:8000)",
            "producer.py, starter_stream.py,<br/>toc_backend/*, toc_frontend/*,<br/>scripts/*",
            f"Docker 5 container mặc định 11.0/16.0 GB RAM, Kafka 4.1 KRaft, Spark Streaming Watermark thật, "
            f"RotatingBloom {gt['bloom_kb']} ({gt['bloom_fpr']} FPR) + HLL++ {gt['hll_kb']}, WAL {gt['wal_kb']} "
            f"(0 dupes), Recon ({gt['late_wins']} trễ, {gt['healed_lags']} lag, {gt['rescored_wins']} rescore), "
            f"MongoDB 8.0 ({gt['mongo_med']}/{gt['mongo_p95']}, {gt['mongo_speedup']}) &amp; Web TOC (:8000).",
        ],
    ]
    _add_captioned_table(
        story,
        ["Thành viên Nhóm", "Mảng Chuyên trách", "Tệp Mã nguồn Phụ trách", "Kết quả Thực nghiệm Bàn giao"],
        team_rows,
        [98, 100, 114, 203],
        "Bảng 7.1. Phân công chuyên trách, tệp mã nguồn và kết quả thực nghiệm bàn giao của 3 thành viên.",
        s,
    )

    story.append(Paragraph("7.2. Automated Test Suite (44/44 pytest &amp; Spark Unittest) &amp; Code Quality", s["h2"]))
    story.append(
        Paragraph(
            (
                "Toàn bộ bất biến toán học và hợp đồng dữ liệu được xác thực tự động bằng <b>44 / 44 kiểm thử "
                "pytest</b> (39 kiểm thử nền tảng &amp; hồi quy trong <i>tests/test_platform_v3.py</i> cùng 5 bộ "
                "kiểm thử Spark ETL độc lập trong <i>tests/test_quyen_spark.py</i>): (1) bảo toàn quân số Quality "
                f"Funnel, loại 427 POIs còn {gt['clean_segs']} đoạn cơ giới, tách bạch <i>derive_urban_los</i> "
                f"({gt['los_match']} khớp BkTraffic) với <i>derive_los</i> và bất biến <i>vmax_lookup.json</i>; "
                "(2) tính kết hợp <b>5-Tuple Monoid Combiner</b>, <b>BroadcastHashJoin</b> (exceptAll == 0) và "
                "Parquet Partition/Column Pruning; (3) bảo toàn tổng <b>Line-Graph PageRank</b> (84,206.0; 30 vòng "
                "lặp không U-turn), chuẩn hóa Unicode NFC và tra cứu Cold-Start; (4) tách burst, ghép +30m, cắt "
                "biên, kiểm thử Future-Shuffle (diff = 0.0), VAL Gate và chữ ký SHA-256; (5) "
                "<b>RotatingBloomFilter</b> "
                f"(32.0 KiB), Spark real watermark, <b>$status_version</b> chặn ghi đè và hồi phục WAL "
                f"({gt['wal_kb']}, 0 dupes). Mã nguồn đạt <b>Ruff 0 lỗi</b> với độ phức tạp nhận thức "
                "<i>McCabe &lt; 15</i>."
            ),
            s["body"],
        )
    )

    story.append(
        Paragraph(
            "7.3. Ngân Sách Tài Nguyên Cụm Docker (11.0 GB RAM [I]), Kiến Trúc Mở Rộng Cloud / K8s / YARN [P] &amp; "
            "Từ Điển 60 Cột",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Nhằm minh bạch ranh giới giữa hạ tầng đã triển khai thực tế <b>[I] (Implemented)</b> trên máy đơn "
                "16 GB RAM với thiết kế mở rộng sản xuất <b>[P] (Production Target)</b>, Bảng 7.2 liệt kê ngân sách "
                "RAM cụm Docker 5 dịch vụ hiện tại và Bảng 7.3 đối chiếu cấu trúc mở rộng Cloud / Kubernetes / YARN "
                "cho lưu lượng toàn thành phố (1.5–3.0 TB/ngày)."
            ),
            s["body"],
        )
    )
    ram_rows = [
        [
            "kafka",
            "apache/kafka:4.1.0 (KRaft)",
            "9092:9092",
            "1.5 GB",
            "Event broker 3 phân vùng cho hcmc-traffic-events ([C4])",
        ],
        [
            "mongo",
            "mongo:8.0 (MongoDB 8.0.34)",
            "27017:27017",
            "2.0 GB",
            f"Kho phục vụ IXSCAN ({gt['mongo_med']}/{gt['mongo_p95']}) &amp; $status_version ([C16])",
        ],
        [
            "spark",
            "hcmc-traffic-spark:3.5.7",
            "Nội bộ bda501-net",
            "6.0 GB",
            "Xử lý phân tán làm sạch, đồ thị, học máy &amp; đối soát ([C5]–[C12])",
        ],
        [
            "toc-web",
            "groupexe-knowledgetest-app",
            "8000:8000",
            "1.0 GB",
            "Cổng điều hành ReactJS + FastAPI + Leaflet 5-Tab ([C17])",
        ],
        [
            "live-consumer",
            "groupexe-knowledgetest-app",
            "Nội bộ bda501-net",
            "0.5 GB",
            "Consumer Kafka → 33 KiB Sketches → MongoDB v1 liên tục ([C6]–[C8])",
        ],
        [
            "Tổng cộng [I]",
            "5 Dịch vụ Mặc định (+2 profiles)",
            "—",
            "11.0 GB",
            "Ổn định trên máy 16.0 GB RAM (dư 5.0 GB cho hệ điều hành)",
        ],
    ]
    _add_captioned_table(
        story,
        ["Dịch vụ", "Ảnh Container Chuẩn hóa", "Cổng Mạng", "RAM [I]", "Vai trò Vận hành Thực tế"],
        ram_rows,
        [74, 144, 68, 48, 181],
        "Bảng 7.2. Ngân sách tài nguyên cụm Docker Compose 5 dịch vụ mặc định (11.0 / 16.0 GB RAM [I]).",
        s,
        highlight_row=6,
    )
    scale_rows = [
        [
            "Ingestion Broker\n(Kafka [C4])",
            "1 broker KRaft, 3 partitions, 1.5 GB RAM",
            "Cụm 3–5 brokers KRaft đa vùng (RF = 3, min.insync.replicas = 2), 32–64 partitions băm theo segment_id.",
        ],
        [
            "Compute Engine\n(Spark [C5]–[C12])",
            "1 container local[*], 6.0 GB RAM",
            "Spark on Kubernetes / YARN với Dynamic Allocation (8–32 executors), Adaptive Query Execution (AQE) &amp; "
            "S3/HDFS checkpointing.",
        ],
        [
            "Lakehouse &amp; Registry\n([C13]–[C15])",
            "Local Parquet volume (10 phân vùng tháng)",
            "Object Storage (S3 / GCS / MinIO) với định dạng bảng ACID (Delta Lake / Apache Iceberg) &amp; MLflow "
            "Model Registry.",
        ],
        [
            "Serving &amp; Web TOC\n([C16]–[C17])",
            "1 node MongoDB 8.0 (2.0 GB) + 1 pod FastAPI",
            "MongoDB Sharded Replica Set (3 shards theo street_level/district) + HPA (Horizontal Pod Autoscaler — "
            "tự co giãn pod Kubernetes theo tải truy vấn) cho FastAPI TOC sau Load Balancer.",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Tầng Kiến trúc",
            "Triển khai Proof-of-Scale [I] (11.0 GB RAM)",
            "Kiến trúc Mở rộng Cloud / Kubernetes / YARN [P]",
        ],
        scale_rows,
        [98, 152, 265],
        "Bảng 7.3. Đối chiếu cấu hình triển khai hiện hữu [I] và kiến trúc mở rộng Cloud / Kubernetes / YARN [P].",
        s,
    )
    story.append(
        Paragraph(
            (
                f"<b>Từ điển 60 cột của bảng chuẩn hóa data/dataset.csv ({gt['windows_n']} dòng cơ giới):</b> "
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


def _append_ch7_production_readiness(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(
        Paragraph(
            "7.4. Sẵn Sàng Sản Xuất: Ma Trận Khôi Phục Lỗi (F1–F3), Observability, Bảo Mật &amp; "
            "Đánh Đổi Chi Phí – Hiệu Năng",
            s["h2"],
        )
    )
    story.append(
        Paragraph(
            (
                "Để đáp ứng tiêu chuẩn vận hành dữ liệu lớn trong thực tế [10], hệ thống được thiết kế và kiểm "
                "chứng sẵn ba trụ cột sẵn sàng sản xuất: ma trận chịu lỗi và khôi phục (Bảng 7.4), khung giám sát – "
                "bảo mật (Bảng 7.5) và phân tích đánh đổi chi phí – hiệu năng (Bảng 7.6)."
            ),
            s["body"],
        )
    )
    f_rows = [
        [
            "F1: Kafka Broker /\nConsumer Crash",
            "Tiến trình tiêu thụ luồng ngắt đột ngột giữa micro-batch hoặc mất kết nối broker.",
            f"Ghi nguyên tử offset_log.json và WAL state_wal.json ({gt['wal_kb']}) sau mỗi micro-batch; khởi động "
            "lại bỏ qua batch đã commit.",
            "Kiểm chứng --kill-after-batch 1: bỏ qua batch 1, chạy tiếp tới 90,741 events, 0 duplicate side effects.",
        ],
        [
            "F2: MongoDB Upsert\nRetry / Stale Overwrite",
            "Gửi trùng lô ghi hoặc bản tin v1 đến muộn sau khi Phase C đã xác thực v2.",
            "Chỉ mục duy nhất event_id_unique + UpdateOne(upsert=True) + atomic $status_version pipeline guard "
            "trong MongoDB 8.0.",
            f"Replay toàn bộ {gt['windows_n']} docs giữ 0 trùng lặp; chặn đứng {gt['mongo_stale_rej']}/"
            f"{gt['mongo_stale_rej']} tin v1 ghi đè lùi lên v2.",
        ],
        [
            "F3: Spark Worker Crash\n&amp; Late Data (&gt; 30m)",
            "Mất executor giữa tác vụ hoặc gói tin di động trễ quá Watermark 30 phút.",
            "Sink 1 lưu 100% raw events xuống Bronze Parquet [C14]; Phase C Two-Tier Recon quét lại backfill &amp; "
            "chữa lành biến lag hạ lưu.",
            f"Tier 1 exceptAll == 0; Tier 2 bù {gt['late_wins']} cửa sổ trễ, heal {gt['healed_lags']} lag và "
            f"re-score {gt['rescored_wins']} cửa sổ (100.0% completeness).",
        ],
    ]
    _add_captioned_table(
        story,
        ["Mã Kịch bản Lỗi", "Tác động Sự cố", "Cơ chế Khôi phục &amp; Bất biến", "Bằng chứng Kiểm chứng Thực tế"],
        f_rows,
        [92, 114, 158, 151],
        "Bảng 7.4. Ma trận khôi phục lỗi (Failure Recovery Matrix F1–F3) trên Kafka/WAL, MongoDB và Spark.",
        s,
    )
    obs_rows = [
        [
            "Observability &amp;\nMetrics Endpoints",
            "FastAPI /api/health, /api/summary; nhật ký JSON chuẩn hóa query_progress.json, "
            "spark_runtime_evidence.json, mongo_verify.json.",
            "Theo dõi độ trễ IXSCAN p95 (&lt; 10 ms SLA), tiến độ watermark, số lượng quarantine và độ lệch skew.",
        ],
        [
            "Data Quality Alerting\n&amp; Schema Quarantine",
            "Cổng ANSI try_cast [C5] định tuyến bản ghi sai định dạng sang quarantine_events.parquet [C15] kèm "
            "first-failing rule R1–R6.",
            "Cảnh báo khi tỷ lệ quarantine vượt ngưỡng nền (&gt; 2%) hoặc cột đầu vào thiếu khóa bắt buộc.",
        ],
        [
            "Model Integrity &\nRead-Only Registry",
            "Khóa bất biến 3 artifact Phase A bằng SHA-256 trong model_registry_manifest.json; xác thực trước &amp; "
            "sau mỗi micro-batch.",
            "Ngăn chặn tuyệt đối silent drift hoặc ghi đè trái phép từ nhánh Speed Layer; sẵn sàng giám sát trôi "
            "dạt phân phối bằng PSI (Population Stability Index — chỉ số trôi dạt phân phối).",
        ],
        [
            "Security &amp; Privacy\nControls",
            "Bind cổng nội bộ 127.0.0.1 trong Docker Compose, phân quyền đọc/ghi tối thiểu cho DB/Registry; dữ liệu "
            "GPS ẩn danh hoàn toàn (PII-free).",
            "Không lưu biển số xe, mã tài xế hay định danh cá nhân; loại bỏ rủi ro rò rỉ dữ liệu nhạy cảm.",
        ],
    ]
    _add_captioned_table(
        story,
        ["Trụ cột Kiểm soát", "Cơ chế Triển khai [I] &amp; Tích hợp [P]", "Mục tiêu Bảo vệ Vận hành"],
        obs_rows,
        [102, 232, 181],
        "Bảng 7.5. Khung giám sát (Observability), cảnh báo (Alerting) và kiểm soát bảo mật (Security Controls).",
        s,
    )
    trade_rows = [
        [
            "33.0 KiB O(1) Sketches vs\nUnbounded Hash State",
            "Cố định 32.0 KiB RotatingBloom + 1.0 KiB HLL++ thay vì HashSet tăng tuyến tính hàng GB theo thời gian.",
            f"Chấp nhận FPR cực nhỏ {gt['bloom_fpr']} và sai số đếm {gt['hll_err']} để đổi lấy RAM O(1) ổn định 24/7.",
        ],
        [
            "BroadcastHashJoin vs\nSortMergeJoin",
            "Phát tán bảng chiều nhỏ (segments, streets ~10.5 MB) vào RAM executor để tránh sắp xếp &amp; shuffle lớn.",
            (
                f"Giảm Shuffle Exchanges 5 → 1 (-80%) và tăng tốc +{gt['join_speedup']} (phù hợp khi bảng chiều "
                "&lt; 100 MB)."
            ),
        ],
        [
            "MongoDB Compound IXSCAN\nvs COLLSCAN",
            "Tạo chỉ mục {street_level: 1, severe_prob: -1, segment_id: 1} tăng nhẹ chi phí ghi RAM/đĩa cho index.",
            f"Giảm độ trễ đọc UC-2 từ {gt['mongo_collscan']} xuống {gt['mongo_med']} (nhanh hơn {gt['mongo_speedup']}, "
            "0 in-memory SORT).",
        ],
        [
            "2-Stage Key Salting vs\nSingle-Stage Aggregation",
            "Bổ sung bước gộp muối 8 giỏ tạo thêm 1 vòng Shuffle thứ hai khi tổng hợp cửa sổ 30 phút.",
            f"Ở mẫu 90.8k pings thêm chi phí ({gt['monoid_unsalted_ms']} → {gt['monoid_salted_ms']} ms), "
            "nhưng ở quy mô TB/ngày tránh sập OOM do skew 684.95x → 1.020x.",
        ],
    ]
    _add_captioned_table(
        story,
        ["Quyết định Kiến trúc", "Chi phí Tài nguyên / Đánh đổi", "Lợi ích Hiệu năng Định lượng"],
        trade_rows,
        [118, 198, 199],
        "Bảng 7.6. Phân tích đánh đổi Chi phí – Hiệu năng (Cost-Performance Trade-offs) của các quyết định thiết kế.",
        s,
    )


def _append_ch7_risks_roadmap_and_runbook(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(
        Paragraph("7.5. Tổng Hợp Hạn Chế – Rủi Ro Toàn Hệ Thống &amp; Lộ Trình Triển Khai 3 Giai Đoạn", s["h2"])
    )
    story.append(
        Paragraph(
            (
                "Tổng hợp từ các phân tích thành phần ở Chương 2–6, Bảng 7.7 hệ thống hóa bốn rủi ro trọng yếu toàn "
                "cục và Bảng 7.8 xác định lộ trình triển khai 3 giai đoạn tiếp theo (chỉ bao gồm các hạng mục nâng "
                "cấp mới, không lặp lại các thành phần đã hoàn thiện như RotatingBloomFilter hay Web TOC 5-Tab)."
            ),
            s["body"],
        )
    )
    risk_rows = [
        [
            "1. Thiên lệch phương tiện\n(Fleet Representation Bias)",
            f"GPS cộng đồng chỉ phủ 11.87% đoạn cơ giới ({gt['active_segs']}/{gt['clean_segs']}) và nghiêng về ô tô "
            "trục chính, chưa phản ánh đầy đủ xe máy trong hẻm dân cư.",
            "Đã có Line-Graph PageRank + 7D K-Means suy diễn vùng mù; Giai đoạn 2 tích hợp thêm GPS xe buýt, xe "
            "công nghệ 2 bánh và camera giao thông.",
        ],
        [
            "2. Trôi dạt phân phối &amp; lớp B–E thưa\n(Concept Drift &amp; Imbalance)",
            f"Mô hình khóa Phase A trên {gt['train_n']} organic TRAIN sẽ giảm nhạy khi mưa ngập; lớp trung gian B–E "
            f"chỉ có 10–20 mẫu TEST kéo Macro-F1 xuống {gt['rf_mf1']}.",
            "Giai đoạn 1 thiết lập giám sát PSI (Population Stability Index — chỉ số trôi dạt phân phối đặc trưng), "
            "lịch tái huấn luyện Phase A hàng tuần và bổ sung mẫu gán nhãn đều cho các lớp chuyển tiếp B–E.",
        ],
        [
            "3. Khan hiếm mẫu khởi phát đột ngột\n(Rare Sudden-Onset Scarcity)",
            f"Chỉ có {gt['onset_n']} / {gt['non_sev_n']} mẫu TEST (0.81%) đột ngột hóa kẹt E–F nên ngưỡng cứng đạt "
            f"0/8 (dù Onset ROC-AUC = {gt['onset_roc']} và lift {gt['onset_lift']}).",
            "Giai đoạn 1 áp dụng hàm phạt nhạy chi phí (Cost-Sensitive Focal Loss), hiệu chỉnh ngưỡng động theo cấp "
            "đường và tích hợp API thời tiết/sự cố.",
        ],
        [
            "4. Trần mở rộng máy chủ đơn\n(Single-Host Ceiling)",
            "Cụm Docker Compose 11.0 / 16.0 GB RAM đáp ứng tốt Proof-of-Scale (38.50 MB), nhưng sẽ nghẽn I/O đĩa "
            "khi mở rộng lên 1.5–3.0 TB/ngày toàn đô thị.",
            "Giai đoạn 3 chuyển đổi sang cụm Kubernetes / YARN đa máy chủ, phân mảnh MongoDB 8.0 và mở rộng 32–64 "
            "phân vùng Kafka KRaft.",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Rủi ro &amp; Hạn chế Toàn cục",
            "Nguyên nhân Gốc rễ &amp; Tác động Vận hành",
            "Biện pháp Kiểm soát &amp; Khắc phục",
        ],
        risk_rows,
        [124, 196, 195],
        "Bảng 7.7. Tổng hợp bốn rủi ro và hạn chế chung toàn hệ thống cùng phương án kiểm soát.",
        s,
    )
    roadmap_rows = [
        [
            "Giai đoạn 1\n(Tháng 1–2 · Hiệu chỉnh Mô hình)",
            "Cải thiện độ nhạy chuyển pha &amp; lớp B–E; tự động hóa kiểm soát trôi dạt phân phối.",
            "Áp dụng Cost-Sensitive Focal Loss, tích hợp API thời tiết/mưa ngập TP.HCM, thiết lập giám sát PSI và "
            "pipeline tái huấn luyện Phase A tự động theo lịch tuần.",
            "Nâng Sudden-Onset Recall &gt; 40%, Macro-F1 &gt; 0.60 và cảnh báo PSI drift &lt; 15 phút.",
        ],
        [
            "Giai đoạn 2\n(Tháng 3–4 · Mở rộng Cảm biến)",
            "Thu hẹp vùng mù Cold-Start 88.13% và bổ sung nguồn dữ liệu thực thời gian đa phương thức.",
            "Kết nối luồng GPS xe buýt công cộng, xe công nghệ 2 bánh và trích xuất lưu lượng từ camera CCTV nút "
            "giao trọng điểm (cập nhật trọng số rẽ thực cho PageRank).",
            "Tăng độ phủ quan trắc thực địa từ 11.87% lên &gt; 45% mạng lưới cơ giới và cập nhật Vmax động.",
        ],
        [
            "Giai đoạn 3\n(Tháng 5–6 · Sản xuất Đô thị)",
            "Mở rộng hạ tầng Cloud / Kubernetes [P] chịu tải 1.5–3.0 TB/ngày và thử nghiệm liên động đèn.",
            "Triển khai Spark on Kubernetes, cụm 3–5 Kafka KRaft brokers, Delta/Iceberg Lakehouse, MongoDB Sharded "
            "Cluster và thử nghiệm khuyến nghị pha đèn xanh bán tự động với hệ thống SCATS.",
            "Chịu tải &gt; 100,000 events/s, SLA đọc TOC p95 &lt; 5 ms và giảm 10–15% thời gian chờ tại nút thí điểm.",
        ],
    ]
    _add_captioned_table(
        story,
        [
            "Giai đoạn &amp; Thời gian",
            "Mục tiêu Chiến lược",
            "Hạng mục Kỹ thuật Triển khai Mới",
            "Tiêu chí Nghiệm thu (KPI)",
        ],
        roadmap_rows,
        [94, 118, 178, 125],
        "Bảng 7.8. Lộ trình triển khai 3 giai đoạn mở rộng sản xuất (Mục 7.5 — chỉ gồm hạng mục nâng cấp tiếp theo).",
        s,
    )

    story.append(
        Paragraph(
            "7.6. Hướng Dẫn Khởi Chạy &amp; Tái Lập Toàn Bộ Kết Quả Thực Nghiệm (PowerShell / Docker CLI)", s["h2"]
        )
    )
    cli_lines = [
        (
            "<b># 1. Khởi chạy 5 dịch vụ Docker mặc định (11.0 GB RAM: Kafka 4.1, Mongo 8.0, Spark 3.5.7, "
            "TOC :8000, live-consumer)</b>"
        ),
        "docker compose up -d --build",
        "",
        "<b># 2. Chạy trọn vẹn Unified Pipeline (P1 → P4, nạp MongoDB 8.0) &amp; kiểm chứng Monoid (baseline.py)</b>",
        "python pipeline.py",
        "python baseline.py",
        "",
        (
            "<b># 3. Thực thi kiểm chứng Spark 3.5.7 (spark_engine.py) &amp; Spark ETL độc lập từ CSV thô "
            "(spark_batch.py)</b>"
        ),
        "docker compose exec -T spark python3 spark_engine.py",
        "$env:TRAFFIC_FIT_CUTOFF = '2020-08-01T00:00:00Z'",
        "docker compose -f compose.quyen.yml run --rm batch",
        "",
        "<b># 4. Kiểm chứng hồi phục luồng WAL (--kill-after-batch 1) &amp; mở Web TOC http://localhost:8000</b>",
        "python starter_stream.py --kill-after-batch 1",
        "",
        "<b># 5. Chạy 44/44 bộ kiểm thử tự động &amp; kiểm tra chất lượng mã nguồn Ruff (McCabe &lt; 15)</b>",
        "python -m pytest -v",
        (
            'python -m ruff check . --select E,F,W,C901 '
            '--config "lint.mccabe.max-complexity=14" --config "line-length=120"'
        ),
    ]
    story.append(make_code_box(cli_lines, s))


def _append_ch8_conclusion(story: list[Any], s: dict[str, ParagraphStyle], gt: dict[str, str]) -> None:
    story.append(CondPageBreak(220))
    story.append(Paragraph("Chương 8. Kết Luận &amp; Nghiệm Thu Toàn Diện", s["h1"]))
    story.append(Paragraph("8.1. Tóm Tắt Đóng Góp Khoa Học &amp; Kỹ Thuật Cốt Lõi", s["h2"]))
    story.append(
        Paragraph(
            (
                "Đồ án đã thiết kế, hiện thực hóa và kiểm chứng đầy đủ một nền tảng phân tích dữ liệu lớn tích hợp "
                "cho bài toán giám sát hiện trạng và dự báo ùn tắc giao thông trước 30 phút tại TP. Hồ Chí Minh trên "
                "bộ dữ liệu thực nghiệm Kaggle v6 (BkTraffic HCMUT, CC0). Bốn đóng góp cốt lõi bao gồm: "
                "<b>(1) Chuẩn hóa dữ liệu không rò rỉ &amp; gộp phân tán chính xác tuyệt đối:</b> Ordered Quality "
                f"Funnel 6 quy tắc (R1–R6) dưới ANSI <i>try_cast</i> kết hợp 3-Tier Vmax học trên organic TRAIN và "
                f"5-Tuple Commutative Monoid + 8-Bucket Salting bảo toàn 100% quân số ({gt['windows_n']} cửa sổ, sai "
                f"số 0.0 vs <i>baseline.py</i>, {gt['los_match']} khớp BkTraffic, Catalyst BroadcastHashJoin giảm "
                f"80% Shuffle Exchanges và tăng tốc +{gt['join_speedup']}); "
                f"<b>(2) Giải quyết vùng mù Cold-Start {gt['cold_pct']}:</b> Line-Graph PageRank không U-turn "
                f"(30 vòng lặp, tổng khối lượng bảo toàn 84,206.0) và 7D K-Means (Silhouette = {gt['km_sil']}) mở "
                f"rộng độ phủ dự báo từ 11.87% lên <b>100.0%</b> toàn mạng ({gt['clean_segs']} đoạn cơ giới); "
                "<b>(3) Predictive ML trung thực &amp; chống rò rỉ:</b> Loại bỏ 7,192 burst windows tổng hợp, ghép "
                "cặp đúng +30 phút và kiểm chứng Future-Shuffle (diff = 0.0) giúp Random Forest 23D đạt "
                f"<b>ROC-AUC = {gt['rf_roc']}</b>, <b>PR-AUC = {gt['rf_pr']}</b>, <b>Severe F1 = {gt['rf_f1']}</b> "
                f"và vượt Baseline có ý nghĩa thống kê trên các ca chuyển pha (McNemar p = {gt['mcnemar_p']}); "
                "<b>(4) Speed Layer O(1) 33.0 KiB &amp; Two-Tier Reconciliation:</b> Chạy bền vững trong 11.0 GB RAM "
                f"với 32.0 KiB RotatingBloomFilter ({gt['bloom_fpr']} FPR), 1.0 KiB HLL++ ({gt['hll_err']} err), "
                f"WAL {gt['wal_kb']} (0 dupes), MongoDB IXSCAN ({gt['mongo_med']} / p95 {gt['mongo_p95']}, "
                f"{gt['mongo_speedup']}) và bù 100% dữ liệu trễ."
            ),
            s["body"],
        )
    )
    story.append(Paragraph("8.2. Nghiệm Thu KPI Đối Chiếu Sáu Tình Huống Sử Dụng (UC-1–UC-6)", s["h2"]))
    story.append(
        Paragraph(
            "Bảng 8.1 tổng hợp kết quả nghiệm thu định lượng của toàn hệ thống đối chiếu với 6 tình huống vận hành "
            "đã đề ra tại Chương 1 và bộ kiểm thử tự động.",
            s["body"],
        )
    )
    kpi_rows = [
        [
            "UC-1: Điểm nóng kéo dài",
            "Phát hiện nút nghẽn mãn tính theo thời gian thực với RAM O(1)",
            "EWMA α=0.15 bắt #47407 Lý Thường Kiệt (6.6652), #21263 Trương Định (6.6635) trong 33.0 KiB Sketches.",
            "Đạt (100%)",
        ],
        [
            "UC-2: Ưu tiên đèn xanh",
            "Truy vấn cảnh báo trục cấp 1–2 P(Severe) ≥ 70% dưới 10 ms",
            (
                f"MongoDB IXSCAN khớp {gt['arterial_alerts']} cửa sổ, warm median {gt['mongo_med']} / "
                f"p95 {gt['mongo_p95']} ({gt['mongo_speedup']}, 0 SORT)."
            ),
            "Đạt (100%)",
        ],
        [
            "UC-3: Quy hoạch vùng mù",
            "Xếp hạng nút cổ chai toàn mạng gồm cả đoạn 0 GPS",
            (
                f"Line-Graph PageRank (30 vòng, {gt['lg_edges']} cạnh): #1 Nguyễn Kiệm 2.7799, "
                "#2 Gò Dưa 2.7196, #3 Quốc lộ 1 2.6894."
            ),
            "Đạt (100%)",
        ],
        [
            "UC-4: Né kẹt dây chuyền",
            "Khai phá cặp đường kẹt đồng thời / trễ không rò rỉ tương lai",
            (
                f"Spark FPGrowth {gt['spark_fp_rules']} luật + SON {gt['co_rules']} đồng thời (Lift 15.23x) + "
                f"{gt['lag_rules']} trễ (persistence_controlled)."
            ),
            "Đạt (100%)",
        ],
        [
            "UC-5: Phân bổ ca tuần tra",
            f"Phủ kín 100% {gt['clean_segs']} đoạn cơ giới (gồm Cold-Start)",
            (
                f"7D K-Means (Sil={gt['km_sil']}) phủ đủ {gt['clean_segs']} đoạn; Cụm 1 (43.26% E/F) &amp; "
                f"Masked-GPS Acc {gt['cold_acc']}."
            ),
            "Đạt (100%)",
        ],
        [
            "UC-6: Đối soát dữ liệu trễ",
            "Khớp 100% trong Watermark &amp; bù đủ gói tin trễ &gt; 30m",
            (
                f"Tier 1 exceptAll = 0; Tier 2 bù {gt['late_wins']} trễ, heal {gt['healed_lags']} lag, "
                f"re-score {gt['rescored_wins']}, chặn 25/25 v1 cũ."
            ),
            "Đạt (100%)",
        ],
        [
            "Kiểm thử &amp; Mã nguồn",
            "Tự động kiểm định toàn bộ bất biến &amp; chuẩn chất lượng",
            "44/44 pytest PASS (test_platform_v3 + test_quyen_spark); Ruff 0 errors (McCabe &lt; 15); 0 rò rỉ SHA-256.",
            "Đạt (100%)",
        ],
    ]
    _add_captioned_table(
        story,
        ["Hạng mục Nghiệm thu", "Mục tiêu Kỹ thuật Đề ra", "Kết quả Đo đạc Thực nghiệm Xác thực", "Trạng thái"],
        kpi_rows,
        [100, 134, 223, 58],
        "Bảng 8.1. Tổng kết nghiệm thu KPI đối chiếu 6 kịch bản vận hành (UC-1–UC-6) và kiểm thử tự động.",
        s,
        highlight_row=7,
    )
    story.append(Paragraph("8.3. Đánh Giá Mức Độ Sẵn Sàng Triển Khai (Deployment Readiness)", s["h2"]))
    story.append(
        Paragraph(
            (
                "Hệ thống hoàn thiện ở mức độ <b>Proof-of-Scale sẵn sàng chuyển giao thí điểm</b>: toàn bộ 5 dịch vụ "
                "container (<i>Kafka 4.1.0, Spark 3.5.7, MongoDB 8.0.34, live-consumer, Web TOC ReactJS + FastAPI "
                ":8000</i>) khởi chạy lặp lại bằng một lệnh <i>docker compose up -d --build</i> trong 11.0 GB RAM, "
                "có đầy đủ cơ chế hồi phục lỗi <i>F1–F3</i>, khóa toàn vẹn SHA-256, kiểm thử tự động 44/44 và bản "
                "thiết kế chuyển đổi rõ ràng sang cụm Cloud / Kubernetes / YARN [P] theo lộ trình 3 giai đoạn ở "
                "Chương 7."
            ),
            s["body"],
        )
    )


def _append_references(story: list[Any], s: dict[str, ParagraphStyle]) -> None:
    story.append(CondPageBreak(200))
    story.append(Paragraph("Tài Liệu Tham Khảo (References)", s["h1"]))
    refs = [
        "[1] Sims, A. G., &amp; Dobinson, K. W. (1980). The Sydney Coordinated Adaptive Traffic (SCAT) system "
        "philosophy and benefits. <i>IEEE Transactions on Vehicular Technology</i>, 29(2), 130–137.",
        "[2] Hunt, P. B., Robertson, D. I., Bretherton, R. D., &amp; Winton, R. I. (1981). <i>SCOOT — A Traffic "
        "Responsive Method of Coordinating Signals</i> (TRRL Laboratory Report LR 1014). Transport and Road Research "
        "Laboratory, Crowthorne, UK.",
        "[3] Herrera, J. C., Work, D. B., Herring, R., Ban, X. J., Jacobson, Q., &amp; Bayen, A. M. (2010). "
        "Evaluation of traffic data obtained via GPS-enabled mobile phones: The Mobile Century field experiment. "
        "<i>Transportation Research Part C: Emerging Technologies</i>, 18(4), 568–583.",
        "[4] Vlahogianni, E. I., Karlaftis, M. G., &amp; Golias, J. C. (2014). Short-term traffic forecasting: "
        "Where we are and where we're going. <i>Transportation Research Part C: Emerging Technologies</i>, 43, 3–19.",
        "[5] Lv, Y., Duan, Y., Kang, W., Li, Z., &amp; Wang, F.-Y. (2015). Traffic flow prediction with big data: "
        "A deep learning approach. <i>IEEE Transactions on Intelligent Transportation Systems</i>, 16(2), 865–873.",
        "[6] Zheng, Y. (2015). Trajectory data mining: An overview. <i>ACM Transactions on Intelligent Systems and "
        "Technology (TIST)</i>, 6(3), Article 29, 1–41.",
        "[7] Amini, S., Gerostathopoulos, I., &amp; Prehofer, C. (2017). Big data analytics architecture for "
        "real-time traffic control. <i>Proceedings of the 5th IEEE International Conference on Models and "
        "Technologies for Intelligent Transportation Systems (MT-ITS)</i>, 710–715.",
        "[8] Li, Y., Yu, R., Shahabi, C., &amp; Liu, Y. (2018). Diffusion Convolutional Recurrent Neural Network: "
        "Data-driven traffic forecasting. <i>International Conference on Learning Representations (ICLR 2018)</i>.",
        "[9] Leskovec, J., Rajaraman, A., &amp; Ullman, J. D. (2020). <i>Mining of Massive Datasets</i> (3rd ed.). "
        "Cambridge University Press.",
        "[10] Bahga, A., &amp; Madisetti, V. (2019). <i>Big Data Science &amp; Analytics: A Hands-On Approach</i>. "
        "VPT Press.",
        "[11] Breiman, L. (2001). Random Forests. <i>Machine Learning</i>, 45(1), 5–32.",
        "[12] Brin, S., &amp; Page, L. (1998). The anatomy of a large-scale hypertextual Web search engine. "
        "<i>Computer Networks and ISDN Systems</i>, 30(1–7), 107–117.",
        "[13] Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., "
        "Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., &amp; Stoica, I. (2016). Apache "
        "Spark: A unified engine for big data processing. <i>Communications of the ACM</i>, 59(11), 56–65.",
        "[14] Kreps, J., Narkhede, N., &amp; Rao, J. (2011). Kafka: A distributed messaging system for log "
        "processing. <i>Proceedings of the 6th International Workshop on Networking Meets Databases (NetDB)</i>.",
        "[15] MongoDB, Inc. (2024). <i>MongoDB 8.0 Documentation — Indexes, Aggregation Pipeline Updates, and "
        "Query Plan Verification (IXSCAN)</i>. https://www.mongodb.com/docs/v8.0/",
        "[16] Thanh Nguyen (2021). <i>Traffic Flow data in Ho Chi Minh City, Viet Nam</i> (Version 6, 2021-05-20; "
        "Nguồn BkTraffic — ĐH Bách Khoa TP.HCM / HCMUT; Giấy phép CC0: Public Domain). Kaggle. "
        "https://www.kaggle.com/datasets/thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam",
    ]
    for ref in refs:
        story.append(Paragraph(ref, s["ref_item"]))


def build_proposal_pdf(out_path: Path) -> None:
    styles = build_styles()
    gt = _load_ground_truth()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc = ReportDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=TOP_MARGIN,
        bottomMargin=BOTTOM_MARGIN,
        title="HCMC Smart Traffic Big Data Analytics & Congestion Prediction — Final Project Report & Specification",
        author="Nguyễn Tạ Minh Quyền (25MS23280), Nguyễn Thành Đạt (25MS23306), Phạm Công Minh (25MS23310)",
        subject="FSB Course BDA501 — Big Data Analytics Final Project Report & Comprehensive Technical Specification",
    )
    frame = Frame(
        40,
        BOTTOM_MARGIN,
        CONTENT_W,
        A4[1] - TOP_MARGIN - BOTTOM_MARGIN,
        id="main_frame",
        leftPadding=0,
        rightPadding=0,
        topPadding=0,
        bottomPadding=0,
    )
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame])])

    story: list[Any] = []
    _append_cover_page(story, styles, gt)
    _append_front_matter_toc(story, styles)
    _append_front_matter_glossary(story, styles)
    _append_ch1_context(story, styles, gt)
    _append_ch1_problems_and_usecases(story, styles, gt)
    _append_ch1_boundary_and_literature(story, styles, gt)
    _append_ch2_data_audit_and_ec(story, styles, gt)
    _append_ch2_funnel_los_and_limits(story, styles, gt)
    _append_ch3_architecture_tables(story, styles, gt)
    _append_ch3_mlops_and_spark(story, styles, gt)
    _append_ch4_pagerank_and_rules(story, styles, gt)
    _append_ch4_kmeans_bridge_and_limits(story, styles, gt)
    _append_ch5_split_features_and_ablation(story, styles, gt)
    _append_ch5_transitions_and_limits(story, styles, gt)
    _append_ch6_streaming_and_recon(story, styles, gt)
    _append_ch6_serving_ui_and_limits(story, styles, gt)
    _append_ch7_team_tests_and_infra(story, styles, gt)
    _append_ch7_production_readiness(story, styles, gt)
    _append_ch7_risks_roadmap_and_runbook(story, styles, gt)
    _append_ch8_conclusion(story, styles, gt)
    _append_references(story, styles)
    doc.multiBuild(story, canvasmaker=NumberedCanvas)


def main() -> None:
    primary_pdf = REPO_DIR / "HCMC_Smart_Traffic_Final_Report.pdf"
    build_proposal_pdf(primary_pdf)
    sync_targets = [
        OUTPUTS_DIR / "HCMC_Smart_Traffic_Final_Report.pdf",
        REPO_DIR.parent / "HCMC_Smart_Traffic_Final_Report.pdf",
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
