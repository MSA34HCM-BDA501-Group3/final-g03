"""Dual-backend 16:9 Editorial Slide Renderer (Behance/Dribbble Chamfered-Card Design + Native python-pptx)."""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Circle, FancyBboxPatch, Polygon, Rectangle, Wedge
from PIL import Image as PILImage
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt

plt.rcParams["font.sans-serif"] = ["Segoe UI", "DejaVu Sans", "Liberation Sans", "Arial"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["pdf.fonttype"] = 42
plt.rcParams["ps.fonttype"] = 42

SLIDE_W = 16.0
SLIDE_H = 9.0

HEX_WHITE = "#FFFFFF"
HEX_BG = "#FFFFFF"
HEX_PAGE_SOFT = "#F8FBFF"
HEX_SKY_PALE = "#E0F2FE"
HEX_SKY_LIGHT = "#BAE6FD"
HEX_SKY_MID = "#38BDF8"
HEX_SKY_PRIMARY = "#0EA5E9"
HEX_SKY_DEEP = "#0284C7"
HEX_SKY_DARK = "#0369A1"
HEX_NAVY = "#0F172A"
HEX_SLATE_DARK = "#1E293B"
HEX_SLATE = "#334155"
HEX_MUTED = "#64748B"
HEX_BORDER = "#CBD5E1"
HEX_OK_BG = "#DCFCE7"
HEX_OK_BORDER = "#15803D"

NAV_TABS = [
    "01–02. Problem & Lit",
    "03–04. Arch & EDA",
    "05–06. Pipe & Model",
    "07–08. Batch & Stream",
    "09. Experiments",
    "10–12. Demo, Risk, Plan",
]


def rgb(hex_str: str) -> RGBColor:
    h = hex_str.lstrip("#")
    return RGBColor(int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _zero_shape_margins(shp) -> None:
    tf = shp.text_frame
    tf.margin_left = Inches(0)
    tf.margin_right = Inches(0)
    tf.margin_top = Inches(0)
    tf.margin_bottom = Inches(0)


def card_palette(theme: str) -> tuple[str, str, str, str, str]:
    """Return (fill_hex, border_hex, title_hex, body_hex, accent_hex)."""
    if theme == "navy":
        return HEX_NAVY, HEX_NAVY, HEX_WHITE, "#E2E8F0", HEX_SKY_MID
    if theme == "sky_strong":
        return HEX_SKY_LIGHT, HEX_SKY_DARK, HEX_NAVY, HEX_SLATE_DARK, HEX_NAVY
    if theme == "sky":
        return HEX_SKY_PALE, HEX_SKY_DEEP, HEX_NAVY, HEX_SLATE_DARK, HEX_NAVY
    if theme == "emerald":
        return "#ECFDF5", HEX_OK_BORDER, "#064E3B", HEX_SLATE_DARK, HEX_OK_BORDER
    return HEX_WHITE, HEX_SLATE_DARK, HEX_NAVY, HEX_SLATE, HEX_SKY_PRIMARY


def wrap_paragraph(text: str, max_chars: int) -> list[str]:
    lines: list[str] = []
    for raw in text.split("\n"):
        if len(raw) <= max_chars:
            lines.append(raw)
        else:
            lines.extend(textwrap.wrap(raw, width=max_chars, break_long_words=False))
    return lines


def _pptx_textbox(
    slide,
    x: float,
    y_top: float,
    w: float,
    h: float,
    text: str,
    font_size: float,
    color_hex: str,
    bold: bool = False,
    font_name: str = "Segoe UI",
    align=PP_ALIGN.LEFT,
) -> None:
    tb = slide.shapes.add_textbox(Inches(x), Inches(y_top), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = Inches(0.03)
    tf.margin_right = Inches(0.03)
    tf.margin_top = Inches(0.02)
    tf.margin_bottom = Inches(0.02)
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.name = font_name
    run.font.size = Pt(font_size)
    run.font.bold = bold
    run.font.color.rgb = rgb(color_hex)


def _pptx_pill(
    slide,
    x: float,
    y_top: float,
    w: float,
    h: float,
    text: str,
    fill_hex: str,
    border_hex: str,
    text_hex: str,
    font_size: float = 8.5,
) -> None:
    shp = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y_top), Inches(w), Inches(h))
    if len(shp.adjustments) > 0:
        shp.adjustments[0] = 0.5
    shp.fill.solid()
    shp.fill.fore_color.rgb = rgb(fill_hex)
    shp.line.color.rgb = rgb(border_hex)
    shp.line.width = Pt(1.0)
    tf = shp.text_frame
    tf.word_wrap = False
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    tf.margin_left = Inches(0.04)
    tf.margin_right = Inches(0.04)
    tf.margin_top = Inches(0.01)
    tf.margin_bottom = Inches(0.01)
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = text
    r.font.name = "Segoe UI"
    r.font.size = Pt(font_size)
    r.font.bold = True
    r.font.color.rgb = rgb(text_hex)


class DualSlide:
    """Simultaneously draws one 16x9 editorial slide onto Matplotlib and native python-pptx."""

    def __init__(
        self,
        prs: Presentation,
        nav_tabs: list[str] | None = None,
        footer_left: str | None = None,
    ) -> None:
        self.nav_tabs = nav_tabs if nav_tabs is not None else NAV_TABS
        self.footer_left = (
            footer_left
            if footer_left is not None
            else "BDA501 · HCMC Smart Traffic Big Data Platform · Final Project Report & Specification"
        )
        self.fig = plt.figure(figsize=(SLIDE_W, SLIDE_H), dpi=200, facecolor=HEX_WHITE)
        self.ax = self.fig.add_axes([0.0, 0.0, 1.0, 1.0])
        self.ax.set_xlim(0, SLIDE_W)
        self.ax.set_ylim(0, SLIDE_H)
        self.ax.axis("off")
        self.ax.add_patch(Rectangle((0, 0), SLIDE_W, SLIDE_H, facecolor=HEX_WHITE, edgecolor="none", zorder=0))

        blank_layout = prs.slide_layouts[6]
        self.slide = prs.slides.add_slide(blank_layout)
        bg = self.slide.background
        fill = bg.fill
        fill.solid()
        fill.fore_color.rgb = rgb(HEX_WHITE)

    def _ym(self, y_top: float, h: float = 0.0) -> float:
        return SLIDE_H - y_top - h

    def add_se_arrow(
        self, cx: float, cy_top: float, size: float = 0.15, color: str = HEX_SKY_DEEP, lw: float = 2.0, zorder: int = 5
    ) -> None:
        """Draws a crisp vector South-East diagonal arrow icon (↘) on the slide canvas."""
        cy = self._ym(cy_top)
        half = size / 2.0
        x0, y0 = cx - half, cy + half
        x1, y1 = cx + half, cy - half
        self.ax.plot([x0, x1], [y0, y1], color=color, lw=lw, solid_capstyle="round", zorder=zorder)
        self.ax.plot(
            [x1 - size * 0.65, x1, x1],
            [y1, y1, y1 + size * 0.65],
            color=color,
            lw=lw,
            solid_capstyle="round",
            zorder=zorder,
        )

    def add_grid_line(
        self,
        x1: float,
        y1_top: float,
        x2: float,
        y2_top: float,
        dot_start: bool = False,
        dot_end: bool = False,
        color: str = HEX_BORDER,
        lw: float = 0.9,
    ) -> None:
        """Draws an architectural hairline divider with optional sky-blue node dots."""
        y1 = self._ym(y1_top)
        y2 = self._ym(y2_top)
        self.ax.plot([x1, x2], [y1, y2], color=color, lw=lw, zorder=1)
        if dot_start:
            self.ax.add_patch(Circle((x1, y1), 0.055, facecolor=HEX_SKY_MID, edgecolor=HEX_NAVY, lw=0.9, zorder=3))
        if dot_end:
            self.ax.add_patch(Circle((x2, y2), 0.055, facecolor=HEX_SKY_MID, edgecolor=HEX_NAVY, lw=0.9, zorder=3))

    def add_target_bullet(
        self,
        x: float,
        y_top: float,
        title: str,
        subtitle: str = "",
        color: str = HEX_SKY_PRIMARY,
        title_size: float = 9.8,
        sub_size: float = 8.4,
    ) -> None:
        """Draws the concentric circular target bullet icon from the reference slides."""
        cy = self._ym(y_top)
        self.ax.add_patch(Circle((x, cy), 0.11, facecolor=HEX_SKY_PALE, edgecolor=color, lw=1.25, zorder=3))
        self.ax.add_patch(Circle((x, cy), 0.045, facecolor=HEX_NAVY, edgecolor="none", zorder=4))
        self.ax.text(
            x + 0.22,
            cy + (0.05 if subtitle else 0.0),
            title,
            fontsize=title_size,
            fontweight="bold",
            color=HEX_NAVY,
            ha="left",
            va="center",
            zorder=4,
        )
        if subtitle:
            self.ax.text(
                x + 0.22, cy - 0.17, subtitle, fontsize=sub_size, color=HEX_SLATE, ha="left", va="center", zorder=4
            )

    def add_pill_badge(
        self, x: float, y_top: float, w: float, h: float, text: str, theme: str = "sky", font_size: float = 8.3
    ) -> None:
        """Draws an editorial rounded pill badge on both Matplotlib and PPTX."""
        if theme == "navy":
            fc, ec, tc = HEX_NAVY, HEX_NAVY, HEX_WHITE
        elif theme == "sky":
            fc, ec, tc = HEX_SKY_MID, HEX_NAVY, HEX_NAVY
        elif theme == "sky_pale":
            fc, ec, tc = HEX_SKY_PALE, HEX_SKY_DEEP, HEX_NAVY
        else:
            fc, ec, tc = HEX_WHITE, HEX_NAVY, HEX_NAVY
        ym = self._ym(y_top, h)
        self.ax.add_patch(
            FancyBboxPatch(
                (x, ym),
                w,
                h,
                boxstyle=f"round,pad=0.01,rounding_size={h / 2:.2f}",
                facecolor=fc,
                edgecolor=ec,
                lw=1.1,
                zorder=4,
            )
        )
        self.ax.text(
            x + w / 2,
            ym + h / 2,
            text,
            fontsize=font_size,
            fontweight="bold",
            color=tc,
            ha="center",
            va="center",
            zorder=5,
        )

    def add_ring_gauge(
        self,
        cx: float,
        cy_top: float,
        radius: float,
        pct: float,
        label_top: str,
        label_sub: str,
        theme: str = "sky",
        zorder: int = 5,
    ) -> None:
        """Draws a circular progress ring gauge."""
        cy = self._ym(cy_top)
        bg_ring = Wedge(
            (cx, cy), radius, 0, 360, width=radius * 0.24, facecolor="#E2E8F0", edgecolor="none", zorder=zorder
        )
        self.ax.add_patch(bg_ring)
        angle_end = 90.0
        angle_start = 90.0 - (pct / 100.0) * 360.0
        fg_col = HEX_NAVY if theme == "sky" else HEX_SKY_MID
        fg_ring = Wedge(
            (cx, cy),
            radius,
            angle_start,
            angle_end,
            width=radius * 0.24,
            facecolor=fg_col,
            edgecolor="none",
            zorder=zorder + 1,
        )
        self.ax.add_patch(fg_ring)
        tc = HEX_NAVY if theme == "sky" else HEX_WHITE
        sc = HEX_SLATE if theme == "sky" else HEX_SKY_LIGHT
        self.ax.text(
            cx,
            cy + 0.03,
            label_top,
            fontsize=13.0,
            fontweight="bold",
            color=tc,
            ha="center",
            va="center",
            zorder=zorder + 2,
        )
        self.ax.text(
            cx,
            cy - radius - 0.19,
            label_sub,
            fontsize=7.8,
            fontweight="bold",
            color=sc,
            ha="center",
            va="center",
            zorder=zorder + 2,
        )

    def add_chrome(
        self,
        slide_num: int,
        total_slides: int,
        active_tab: int,
        kicker: str,
        title: str,
        subtitle: str,
    ) -> None:
        self._draw_navbar(slide_num, total_slides, active_tab)
        self.add_se_arrow(0.68, 0.94, size=0.14, color=HEX_SKY_DEEP, lw=2.0)
        self.ax.text(
            0.90, self._ym(0.84), kicker, fontsize=9.0, fontweight="bold", color=HEX_SKY_DEEP, ha="left", va="top"
        )
        self.ax.text(0.58, self._ym(1.10), title, fontsize=19.8, fontweight="bold", color=HEX_NAVY, ha="left", va="top")
        self.ax.text(0.58, self._ym(1.56), subtitle, fontsize=9.8, color=HEX_SLATE, ha="left", va="top")
        self.add_grid_line(0.58, 1.88, 15.42, 1.88, dot_start=True, dot_end=True, color=HEX_BORDER, lw=0.85)

        _pptx_textbox(self.slide, 0.55, 0.76, 14.8, 0.28, kicker, 9.5, HEX_SKY_DEEP, bold=True)
        _pptx_textbox(self.slide, 0.55, 1.01, 14.8, 0.48, title, 18.5, HEX_NAVY, bold=True)
        _pptx_textbox(self.slide, 0.55, 1.49, 14.8, 0.34, subtitle, 10.0, HEX_MUTED, bold=False)

        self._draw_footer(slide_num, total_slides)

    def _draw_navbar(self, slide_num: int, total_slides: int, active_tab: int) -> None:
        y_line = self._ym(0.66)
        self.ax.plot([0.55, 15.45], [y_line, y_line], color=HEX_BORDER, lw=0.9, zorder=1)
        self.ax.text(
            0.58,
            self._ym(0.40),
            "FSB · FPT University",
            fontsize=9.5,
            fontweight="bold",
            color=HEX_NAVY,
            ha="left",
            va="center",
        )
        self.ax.text(
            2.30,
            self._ym(0.40),
            "|  HCMC Smart Traffic Platform",
            fontsize=8.8,
            color=HEX_MUTED,
            ha="left",
            va="center",
        )

        _pptx_textbox(self.slide, 0.55, 0.24, 2.35, 0.30, "FSB · BDA501", 9.5, HEX_NAVY, bold=True)
        div = self.slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.55), Inches(0.65), Inches(14.90), Inches(0.015))
        div.fill.solid()
        div.fill.fore_color.rgb = rgb(HEX_BORDER)
        div.line.fill.background()
        _zero_shape_margins(div)

        if len(self.nav_tabs) == 5:
            tab_xs = [5.55, 7.50, 9.45, 11.40, 13.35]
        else:
            tab_xs = [5.35, 7.05, 8.82, 10.55, 12.12, 13.55]
        for idx, (tx, label) in enumerate(zip(tab_xs, self.nav_tabs)):
            is_act = idx == active_tab
            col = HEX_NAVY if is_act else "#94A3B8"
            self.ax.text(
                tx,
                self._ym(0.40),
                label,
                fontsize=8.0,
                fontweight="bold" if is_act else "normal",
                color=col,
                ha="center",
                va="center",
            )
            if is_act:
                self.ax.add_patch(
                    Circle((tx, self._ym(0.56)), 0.044, facecolor=HEX_SKY_PRIMARY, edgecolor="none", zorder=3)
                )
                dot = self.slide.shapes.add_shape(
                    MSO_SHAPE.OVAL, Inches(tx - 0.042), Inches(0.518), Inches(0.084), Inches(0.084)
                )
                dot.fill.solid()
                dot.fill.fore_color.rgb = rgb(HEX_SKY_PRIMARY)
                dot.line.fill.background()
                _zero_shape_margins(dot)
            _pptx_textbox(self.slide, tx - 0.82, 0.24, 1.64, 0.30, label, 8.0, col, bold=is_act, align=PP_ALIGN.CENTER)

        badge_txt = f"Slide {slide_num:02d} / {total_slides:02d}"
        ym_pill = self._ym(0.25, 0.28)
        self.ax.add_patch(
            FancyBboxPatch(
                (14.32, ym_pill),
                1.12,
                0.28,
                boxstyle="round,pad=0.01,rounding_size=0.14",
                facecolor=HEX_SKY_PALE,
                edgecolor=HEX_SKY_DEEP,
                lw=1.0,
                zorder=2,
            )
        )
        self.ax.text(
            14.88,
            ym_pill + 0.14,
            badge_txt,
            fontsize=8.1,
            fontweight="bold",
            color=HEX_NAVY,
            ha="center",
            va="center",
            zorder=3,
        )
        _pptx_pill(self.slide, 14.32, 0.24, 1.12, 0.28, badge_txt, HEX_SKY_PALE, HEX_SKY_DEEP, HEX_NAVY, 8.0)

    def _draw_footer(self, slide_num: int, total_slides: int) -> None:
        y_line = self._ym(8.56)
        self.ax.plot([0.55, 15.45], [y_line, y_line], color=HEX_BORDER, lw=0.8, zorder=1)
        left_txt = self.footer_left
        right_txt = f"FSB · Slide {slide_num:02d}/{total_slides:02d}"
        self.ax.text(0.58, self._ym(8.74), left_txt, fontsize=7.8, color=HEX_MUTED, ha="left", va="center")
        self.ax.text(
            15.42,
            self._ym(8.74),
            right_txt,
            fontsize=7.8,
            fontweight="bold",
            color=HEX_SKY_DARK,
            ha="right",
            va="center",
        )

        fdiv = self.slide.shapes.add_shape(
            MSO_SHAPE.RECTANGLE, Inches(0.55), Inches(8.55), Inches(14.90), Inches(0.012)
        )
        fdiv.fill.solid()
        fdiv.fill.fore_color.rgb = rgb(HEX_BORDER)
        fdiv.line.fill.background()
        _zero_shape_margins(fdiv)
        _pptx_textbox(self.slide, 0.55, 8.59, 10.1, 0.28, left_txt, 7.8, HEX_MUTED, bold=False)
        _pptx_textbox(self.slide, 10.7, 8.59, 4.75, 0.28, right_txt, 7.8, HEX_SKY_DARK, bold=True, align=PP_ALIGN.RIGHT)

    def add_kpi(
        self,
        x: float,
        y_top: float,
        w: float,
        h: float,
        value: str,
        label: str,
        sublabel: str,
        theme: str = "sky",
    ) -> None:
        fc, ec, tc, bc, ac = card_palette(theme)
        ym = self._ym(y_top, h)
        cut = 0.24
        verts = [(x, ym), (x + w, ym), (x + w, ym + h - cut), (x + w - cut, ym + h), (x, ym + h)]
        self.ax.add_patch(Polygon(verts, closed=True, facecolor=fc, edgecolor=ec, lw=1.3, zorder=2))
        tri = [
            (x + w - cut - 0.05, ym + h - 0.05),
            (x + w - 0.05, ym + h - cut - 0.05),
            (x + w - cut - 0.05, ym + h - cut - 0.05),
        ]
        self.ax.add_patch(Polygon(tri, closed=True, facecolor=ac, edgecolor="none", zorder=3))
        self.add_se_arrow(
            x + w - cut - 0.18,
            y_top + 0.22,
            size=0.12,
            color=HEX_SKY_MID if theme == "navy" else HEX_SKY_DEEP,
            lw=1.7,
            zorder=4,
        )

        val_col = HEX_SKY_MID if theme == "navy" else HEX_NAVY
        lbl_col = HEX_WHITE if theme == "navy" else HEX_SKY_DARK
        self.ax.text(
            x + 0.20,
            ym + h - 0.20,
            value,
            fontsize=19.5,
            fontweight="bold",
            color=val_col,
            ha="left",
            va="top",
            zorder=4,
        )
        self.ax.text(
            x + 0.20,
            ym + h - 0.68,
            label,
            fontsize=9.3,
            fontweight="bold",
            color=lbl_col,
            ha="left",
            va="top",
            zorder=4,
        )
        wrapped_sub = "\n".join(wrap_paragraph(sublabel, max(22, int(w * 13))))
        self.ax.text(
            x + 0.20,
            ym + h - 0.94,
            wrapped_sub,
            fontsize=8.0,
            color=bc,
            ha="left",
            va="top",
            linespacing=1.25,
            zorder=4,
        )

        shp = self.slide.shapes.add_shape(MSO_SHAPE.SNIP_1_RECTANGLE, Inches(x), Inches(y_top), Inches(w), Inches(h))
        if len(shp.adjustments) > 0:
            shp.adjustments[0] = round(min(0.45, cut / max(min(w, h), 1e-3)), 5)
        shp.fill.solid()
        shp.fill.fore_color.rgb = rgb(fc)
        shp.line.color.rgb = rgb(ec)
        shp.line.width = Pt(1.2)
        _zero_shape_margins(shp)
        _pptx_textbox(self.slide, x + 0.14, y_top + 0.10, w - 0.30, 0.44, value, 18.5, val_col, bold=True)
        _pptx_textbox(self.slide, x + 0.14, y_top + 0.56, w - 0.26, 0.28, label, 9.2, lbl_col, bold=True)
        _pptx_textbox(self.slide, x + 0.14, y_top + 0.84, w - 0.26, h - 0.90, sublabel, 7.8, bc, bold=False)

    def add_card(
        self,
        x: float,
        y_top: float,
        w: float,
        h: float,
        title: str,
        bullets: list[str],
        badge: str | None = None,
        theme: str = "white",
        body_size: float = 8.6,
    ) -> None:
        fc, ec, tc, bc, ac = card_palette(theme)
        ym = self._ym(y_top, h)
        cut = 0.24
        verts = [(x, ym), (x + w, ym), (x + w, ym + h - cut), (x + w - cut, ym + h), (x, ym + h)]
        self.ax.add_patch(Polygon(verts, closed=True, facecolor=fc, edgecolor=ec, lw=1.25, zorder=2))
        tri = [
            (x + w - cut - 0.05, ym + h - 0.05),
            (x + w - 0.05, ym + h - cut - 0.05),
            (x + w - cut - 0.05, ym + h - cut - 0.05),
        ]
        self.ax.add_patch(Polygon(tri, closed=True, facecolor=ac, edgecolor="none", zorder=3))

        # Target bullet dot next to card title for signature editorial look
        t_cy = ym + h - 0.28
        self.ax.add_patch(
            Circle(
                (x + 0.26, t_cy),
                0.085,
                facecolor=HEX_SKY_PALE if theme != "navy" else "#1E293B",
                edgecolor=HEX_SKY_MID if theme == "navy" else HEX_SKY_DEEP,
                lw=1.1,
                zorder=4,
            )
        )
        self.ax.add_patch(
            Circle(
                (x + 0.26, t_cy),
                0.036,
                facecolor=HEX_SKY_MID if theme == "navy" else HEX_NAVY,
                edgecolor="none",
                zorder=5,
            )
        )
        self.ax.text(
            x + 0.42,
            ym + h - 0.18,
            title,
            fontsize=10.3,
            fontweight="bold",
            color=tc,
            ha="left",
            va="top",
            zorder=4,
        )
        div_y = ym + h - 0.48
        self.ax.plot([x + 0.18, x + w - 0.22], [div_y, div_y], color=ec, lw=0.75, alpha=0.55, zorder=3)

        if badge:
            bw = min(1.85, max(0.95, len(badge) * 0.085 + 0.24))
            bx = x + w - cut - bw - 0.12
            by = ym + h - 0.40
            p_fc = HEX_SKY_MID if theme == "navy" else HEX_NAVY
            p_tc = HEX_NAVY if theme == "navy" else HEX_WHITE
            self.ax.add_patch(
                FancyBboxPatch(
                    (bx, by),
                    bw,
                    0.24,
                    boxstyle="round,pad=0.01,rounding_size=0.12",
                    facecolor=p_fc,
                    edgecolor="none",
                    zorder=4,
                )
            )
            self.ax.text(
                bx + bw / 2,
                by + 0.12,
                badge,
                fontsize=7.5,
                fontweight="bold",
                color=p_tc,
                ha="center",
                va="center",
                zorder=5,
            )

        max_chars = max(26, int((w - 0.36) * (108.0 / body_size)))
        wrapped_lines: list[str] = []
        for b in bullets:
            sub = wrap_paragraph(b, max_chars)
            for idx, line in enumerate(sub):
                prefix = "• " if idx == 0 else "   "
                wrapped_lines.append(prefix + line)
        self.ax.text(
            x + 0.20,
            ym + h - 0.58,
            "\n".join(wrapped_lines),
            fontsize=body_size,
            color=bc,
            ha="left",
            va="top",
            linespacing=1.32,
            zorder=4,
        )

        shp = self.slide.shapes.add_shape(MSO_SHAPE.SNIP_1_RECTANGLE, Inches(x), Inches(y_top), Inches(w), Inches(h))
        if len(shp.adjustments) > 0:
            shp.adjustments[0] = round(min(0.45, cut / max(min(w, h), 1e-3)), 5)
        shp.fill.solid()
        shp.fill.fore_color.rgb = rgb(fc)
        shp.line.color.rgb = rgb(ec)
        shp.line.width = Pt(1.15)
        _zero_shape_margins(shp)

        cdiv = self.slide.shapes.add_shape(
            MSO_SHAPE.RECTANGLE, Inches(x + 0.18), Inches(y_top + 0.45), Inches(w - 0.38), Inches(0.01)
        )
        cdiv.fill.solid()
        cdiv.fill.fore_color.rgb = rgb(ec)
        cdiv.line.fill.background()
        _zero_shape_margins(cdiv)

        bw = min(1.85, max(0.95, len(badge) * 0.085 + 0.24)) if badge else 0.0
        title_w = w - (bw + cut + 0.28 if badge else 0.35)
        _pptx_textbox(self.slide, x + 0.14, y_top + 0.10, title_w, 0.34, title, 10.2, tc, bold=True)
        if badge:
            p_fc = HEX_SKY_MID if theme == "navy" else HEX_NAVY
            p_tc = HEX_NAVY if theme == "navy" else HEX_WHITE
            _pptx_pill(self.slide, x + w - cut - bw - 0.12, y_top + 0.11, bw, 0.24, badge, p_fc, p_fc, p_tc, 7.4)

        tb = self.slide.shapes.add_textbox(Inches(x + 0.14), Inches(y_top + 0.48), Inches(w - 0.28), Inches(h - 0.56))
        tf = tb.text_frame
        tf.word_wrap = True
        tf.margin_left = Inches(0.03)
        tf.margin_right = Inches(0.03)
        tf.margin_top = Inches(0.02)
        tf.margin_bottom = Inches(0.02)
        for idx, b in enumerate(bullets):
            p = tf.paragraphs[0] if idx == 0 else tf.add_paragraph()
            p.space_after = Pt(3.0)
            r = p.add_run()
            r.text = f"• {b}"
            r.font.name = "Segoe UI"
            r.font.size = Pt(body_size)
            r.font.color.rgb = rgb(bc)

    def add_table(
        self,
        x: float,
        y_top: float,
        w: float,
        h: float,
        headers: list[str],
        rows: list[list[str]],
        col_fracs: list[float],
        highlight_row: int | None = None,
        font_size: float = 8.1,
    ) -> None:
        total_rows = len(rows) + 1
        row_h = h / total_rows
        ym_bottom = self._ym(y_top, h)
        col_ws = [w * f for f in col_fracs]

        # Outer chamfered sky frame around table for Behance editorial cohesion
        pad_f = 0.05
        self.ax.add_patch(
            Rectangle(
                (x - pad_f, ym_bottom - pad_f),
                w + 2 * pad_f,
                h + 2 * pad_f,
                facecolor=HEX_SKY_PALE,
                edgecolor=HEX_SKY_DEEP,
                lw=1.1,
                zorder=1,
            )
        )

        # Matplotlib rendering
        for r_idx in range(total_rows):
            ry = ym_bottom + (total_rows - 1 - r_idx) * row_h
            if r_idx == 0:
                bg_hex, txt_hex, is_bold = HEX_NAVY, HEX_WHITE, True
            elif highlight_row is not None and r_idx == highlight_row:
                bg_hex, txt_hex, is_bold = HEX_OK_BG, HEX_NAVY, True
            else:
                bg_hex = HEX_WHITE if r_idx % 2 == 1 else "#F1F5F9"
                txt_hex, is_bold = HEX_SLATE_DARK, False

            cx = x
            row_vals = headers if r_idx == 0 else rows[r_idx - 1]
            for c_idx, (cw, val) in enumerate(zip(col_ws, row_vals)):
                self.ax.add_patch(
                    Rectangle((cx, ry), cw, row_h, facecolor=bg_hex, edgecolor=HEX_BORDER, lw=0.65, zorder=2)
                )
                max_c = max(8, int((cw - 0.12) * (112.0 / font_size)))
                wrapped = "\n".join(wrap_paragraph(val, max_c))
                cell_bold = is_bold or (c_idx == 0)
                cell_col = HEX_WHITE if r_idx == 0 else (HEX_NAVY if c_idx == 0 else txt_hex)
                self.ax.text(
                    cx + 0.08,
                    ry + row_h / 2,
                    wrapped,
                    fontsize=font_size,
                    fontweight="bold" if cell_bold else "normal",
                    color=cell_col,
                    ha="left",
                    va="center",
                    linespacing=1.15,
                    zorder=3,
                )
                cx += cw

        # Native python-pptx Table rendering
        tbl_shape = self.slide.shapes.add_table(
            total_rows, len(headers), Inches(x), Inches(y_top), Inches(w), Inches(h)
        )
        tbl = tbl_shape.table
        for c_idx, cw in enumerate(col_ws):
            tbl.columns[c_idx].width = Inches(cw)
        for r_idx in range(total_rows):
            tbl.rows[r_idx].height = Inches(row_h)
            row_vals = headers if r_idx == 0 else rows[r_idx - 1]
            for c_idx, val in enumerate(row_vals):
                cell = tbl.cell(r_idx, c_idx)
                cell.vertical_anchor = MSO_ANCHOR.MIDDLE
                cell.margin_left = Inches(0.05)
                cell.margin_right = Inches(0.05)
                cell.margin_top = Inches(0.02)
                cell.margin_bottom = Inches(0.02)
                cell.fill.solid()
                if r_idx == 0:
                    bg_hex, fg_hex, bld = HEX_NAVY, HEX_WHITE, True
                elif highlight_row is not None and r_idx == highlight_row:
                    bg_hex, fg_hex, bld = HEX_OK_BG, HEX_NAVY, True
                else:
                    bg_hex = HEX_WHITE if r_idx % 2 == 1 else "#F1F5F9"
                    fg_hex = HEX_NAVY if c_idx == 0 else HEX_SLATE_DARK
                    bld = c_idx == 0
                cell.fill.fore_color.rgb = rgb(bg_hex)
                p = cell.text_frame.paragraphs[0]
                p.alignment = PP_ALIGN.LEFT
                run = p.add_run()
                run.text = val
                run.font.name = "Segoe UI"
                run.font.size = Pt(font_size)
                run.font.bold = bld
                run.font.color.rgb = rgb(fg_hex)

    def add_image(self, x: float, y_top: float, w: float, h: float, img_path: Path) -> None:
        ym = self._ym(y_top, h)
        cut = 0.24
        verts = [(x, ym), (x + w, ym), (x + w, ym + h - cut), (x + w - cut, ym + h), (x, ym + h)]
        self.ax.add_patch(
            Polygon(verts, closed=True, facecolor=HEX_SKY_PALE, edgecolor=HEX_SKY_DEEP, lw=1.25, zorder=2)
        )
        tri = [
            (x + w - cut - 0.05, ym + h - 0.05),
            (x + w - 0.05, ym + h - cut - 0.05),
            (x + w - cut - 0.05, ym + h - cut - 0.05),
        ]
        self.ax.add_patch(Polygon(tri, closed=True, facecolor=HEX_NAVY, edgecolor="none", zorder=3))
        inner_pad = 0.07
        self.ax.add_patch(
            Rectangle(
                (x + inner_pad, ym + inner_pad),
                w - 2 * inner_pad,
                h - 2 * inner_pad,
                facecolor=HEX_WHITE,
                edgecolor=HEX_BORDER,
                lw=0.8,
                zorder=2.5,
            )
        )
        pil_img = PILImage.open(img_path)
        iw, ih = pil_img.size
        box_ratio = w / h
        img_ratio = iw / ih
        pad = 0.11
        avail_w, avail_h = w - 2 * pad, h - 2 * pad
        if img_ratio >= box_ratio:
            draw_w = avail_w
            draw_h = avail_w / img_ratio
        else:
            draw_h = avail_h
            draw_w = avail_h * img_ratio
        dx = x + (w - draw_w) / 2
        dy = ym + (h - draw_h) / 2
        self.ax.imshow(pil_img, extent=[dx, dx + draw_w, dy, dy + draw_h], aspect="auto", zorder=3)

        bg = self.slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y_top), Inches(w), Inches(h))
        bg.fill.solid()
        bg.fill.fore_color.rgb = rgb(HEX_WHITE)
        bg.line.color.rgb = rgb(HEX_SKY_DEEP)
        bg.line.width = Pt(1.1)
        _zero_shape_margins(bg)
        px = x + (w - draw_w) / 2
        py = y_top + (h - draw_h) / 2
        self.slide.shapes.add_picture(
            str(img_path), Inches(px), Inches(py), width=Inches(draw_w), height=Inches(draw_h)
        )

    def save_png(self, out_path: Path, close_fig: bool = True) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        self.fig.savefig(out_path, dpi=200, facecolor=HEX_WHITE)
        if close_fig:
            plt.close(self.fig)


def _write_slides_pdf(
    slide_pngs: list[Path],
    pdf_paths: list[Path],
    slide_figs: list[plt.Figure] | None = None,
    pdf_title: str | None = None,
) -> None:
    if not pdf_paths:
        return
    primary_pdf = pdf_paths[0]
    primary_pdf.parent.mkdir(parents=True, exist_ok=True)
    pdf_meta = {
        "Title": pdf_title
        or "HCMC Smart Traffic Big Data Analytics & Congestion Prediction — Final Project Presentation (BDA501)",
        "Author": "Nguyễn Tạ Minh Quyền (25MS23280), Nguyễn Thành Đạt (25MS23306), Phạm Công Minh (25MS23310)",
        "Subject": "FSB Course BDA501 — Big Data Analytics Presentation",
    }
    if slide_figs:
        with PdfPages(primary_pdf, metadata=pdf_meta) as pdf:
            for fig in slide_figs:
                pdf.savefig(fig, facecolor=HEX_WHITE)
                plt.close(fig)
    else:
        imgs = [PILImage.open(p).convert("RGB") for p in slide_pngs]
        imgs[0].save(primary_pdf, save_all=True, append_images=imgs[1:], resolution=200.0)

    for extra_pdf in pdf_paths[1:]:
        if extra_pdf.parent.exists():
            try:
                shutil.copy2(primary_pdf, extra_pdf)
            except OSError:
                pass


def export_overview_grid_and_pdf(
    slide_pngs: list[Path],
    grid_paths: list[Path],
    pdf_paths: list[Path],
    slide_figs: list[plt.Figure] | None = None,
    pdf_title: str | None = None,
) -> None:
    _write_slides_pdf(slide_pngs, pdf_paths, slide_figs=slide_figs, pdf_title=pdf_title)
    imgs = [PILImage.open(p).convert("RGB") for p in slide_pngs]

    cols = 4
    rows = (len(imgs) + cols - 1) // cols
    thumb_w, thumb_h = 800, 450
    pad = 28
    header_h = 86
    grid_w = cols * thumb_w + (cols + 1) * pad
    grid_h = header_h + rows * thumb_h + (rows + 1) * pad

    # Signature Sky-Blue Behance/Dribbble Overview Canvas (#D8F3FA)
    canvas = PILImage.new("RGB", (grid_w, grid_h), (216, 243, 250))
    for idx, im in enumerate(imgs):
        r = idx // cols
        c = idx % cols
        thumb = im.resize((thumb_w, thumb_h), PILImage.Resampling.LANCZOS)
        gx = pad + c * (thumb_w + pad)
        gy = header_h + pad + r * (thumb_h + pad)
        # Draw subtle dark-slate frame around each slide thumbnail
        frame = PILImage.new("RGB", (thumb_w + 4, thumb_h + 4), (15, 23, 42))
        canvas.paste(frame, (gx - 2, gy - 2))
        canvas.paste(thumb, (gx, gy))

    fig = plt.figure(figsize=(grid_w / 200, grid_h / 200), dpi=200)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.imshow(canvas)
    ax.axis("off")
    ax.text(
        pad,
        header_h * 0.54,
        (f"HCMC Smart Traffic Big Data Platform — Editorial {len(imgs)}-Slide Presentation Overview (FSB · BDA501)"),
        fontsize=15.5,
        fontweight="bold",
        color=HEX_NAVY,
        va="center",
    )
    for gpath in grid_paths:
        if gpath.parent.exists():
            try:
                fig.savefig(gpath, dpi=200)
            except OSError:
                pass
    plt.close(fig)
