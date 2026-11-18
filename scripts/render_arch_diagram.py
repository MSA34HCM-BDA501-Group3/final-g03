"""Render the Enterprise / AWS-style System Architecture Diagram (PNG) and Concrete Example Flow:
1. Every numbered badge (1..10) sits NEXT TO its arrow in open whitespace (never obscuring lines/corners).
2. Components are aligned on clean horizontal tiers (y=7.55, y=4.95, y=1.95) so connectors are straight and unambiguous.
3. Icon captions are concise (2 lines max) using standard English technical terms,
   leaving detailed explanations for the document text.
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Circle, FancyBboxPatch, Polygon, Rectangle

REPO_DIR = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = REPO_DIR / "outputs"


def draw_rounded_box(
    ax,
    x: float,
    y: float,
    w: float,
    h: float,
    edgecolor: str = "#1E293B",
    facecolor: str = "none",
    lw: float = 1.5,
    ls: str = "-",
    radius: float = 0.08,
    zorder: int = 2,
):
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle=f"round,pad=0.0,rounding_size={radius}",
        edgecolor=edgecolor,
        facecolor=facecolor,
        linewidth=lw,
        linestyle=ls,
        zorder=zorder,
    )
    ax.add_patch(patch)
    return patch


def _draw_badge_cloud(ax, cx: float, cy: float, size: float, zorder: int) -> None:
    specs = [(-0.14, -0.04, 0.16), (0.02, 0.06, 0.19), (size - size * 0.83, -0.04, 0.14)]
    for idx, (dx, dy, rad) in enumerate(specs):
        x_pos = cx + dx if idx == 2 else cx + size * dx
        ax.add_patch(
            Circle(
                (x_pos, cy + size * dy),
                size * rad,
                facecolor="none",
                edgecolor="white",
                lw=1.3,
                zorder=zorder + 1,
            )
        )


def _draw_badge_flag(ax, cx: float, cy: float, size: float, zorder: int) -> None:
    ax.plot(
        [cx - size * 0.22, cx - size * 0.22],
        [cy - size * 0.30, cy + size * 0.30],
        color="white",
        lw=1.4,
        zorder=zorder + 1,
    )
    poly = Polygon(
        [
            [cx - size * 0.22, cy + size * 0.28],
            [cx + size * 0.26, cy + size * 0.14],
            [cx - size * 0.22, cy + size * 0.0],
        ],
        closed=True,
        facecolor="none",
        edgecolor="white",
        lw=1.3,
        zorder=zorder + 1,
    )
    ax.add_patch(poly)


def _draw_badge_vpc(ax, cx: float, cy: float, size: float, zorder: int) -> None:
    c = Circle((cx, cy), size * 0.26, facecolor="none", edgecolor="white", lw=1.3, zorder=zorder + 1)
    ax.add_patch(c)


def _draw_badge_exec(ax, cx: float, cy: float, size: float, zorder: int) -> None:
    for dx, dy in [(-0.16, -0.16), (0.16, -0.16), (0.0, 0.16)]:
        r = Rectangle(
            (cx + dx * size - size * 0.10, cy + dy * size - size * 0.10),
            size * 0.20,
            size * 0.20,
            facecolor="none",
            edgecolor="white",
            lw=1.2,
            zorder=zorder + 1,
        )
        ax.add_patch(r)


_BADGE_DRAWERS = {
    "cloud": _draw_badge_cloud,
    "flag": _draw_badge_flag,
    "vpc": _draw_badge_vpc,
    "exec": _draw_badge_exec,
}


def draw_header_badge(
    ax,
    x: float,
    y: float,
    size: float,
    bgcolor: str,
    badge_type: str = "cloud",
    zorder: int = 5,
) -> None:
    rect = Rectangle((x, y), size, size, facecolor=bgcolor, edgecolor="none", zorder=zorder)
    ax.add_patch(rect)
    drawer = _BADGE_DRAWERS.get(badge_type)
    if drawer is not None:
        drawer(ax, x + size / 2, y + size / 2, size, zorder)


def _glyph_kafka(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    c_main = Circle((cx - size * 0.10, cy), size * 0.12, facecolor="none", edgecolor="white", lw=1.5, zorder=zorder + 2)
    ax.add_patch(c_main)
    for angle in [45, -45, 180]:
        rad = np.deg2rad(angle)
        nx = cx - size * 0.05 + np.cos(rad) * size * 0.24
        ny = cy + np.sin(rad) * size * 0.24
        cn = Circle((nx, ny), size * 0.07, facecolor="none", edgecolor="white", lw=1.4, zorder=zorder + 2)
        ax.add_patch(cn)
        ax.plot(
            [cx - size * 0.05 + np.cos(rad) * size * 0.11, nx - np.cos(rad) * size * 0.07],
            [cy + np.sin(rad) * size * 0.11, ny - np.sin(rad) * size * 0.07],
            color="white",
            lw=1.3,
            zorder=zorder + 1,
        )


def _glyph_funnel(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    poly = Polygon(
        [
            [cx - size * 0.28, cy + size * 0.24],
            [cx + size * 0.28, cy + size * 0.24],
            [cx + size * 0.07, cy - size * 0.02],
            [cx + size * 0.07, cy - size * 0.26],
            [cx - size * 0.07, cy - size * 0.26],
            [cx - size * 0.07, cy - size * 0.02],
        ],
        closed=True,
        facecolor="none",
        edgecolor="white",
        lw=1.5,
        zorder=zorder + 2,
    )
    ax.add_patch(poly)


def _glyph_spark(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    angles = np.linspace(0, 2 * np.pi, 7)[:-1] + np.pi / 6
    pts = [[cx + np.cos(a) * size * 0.30, cy + np.sin(a) * size * 0.30] for a in angles]
    ax.add_patch(Polygon(pts, closed=True, facecolor="none", edgecolor="white", lw=1.4, zorder=zorder + 2))
    bolt = Polygon(
        [
            [cx + size * 0.04, cy + size * 0.18],
            [cx - size * 0.12, cy + size * 0.00],
            [cx + size * 0.01, cy + size * 0.00],
            [cx - size * 0.04, cy - size * 0.18],
            [cx + size * 0.12, cy + size * 0.02],
            [cx - size * 0.01, cy + size * 0.02],
        ],
        closed=True,
        facecolor="white",
        edgecolor="white",
        lw=0.8,
        zorder=zorder + 3,
    )
    ax.add_patch(bolt)


def _glyph_ml(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    ax.add_patch(Circle((cx, cy), size * 0.27, facecolor="none", edgecolor="white", lw=1.4, zorder=zorder + 2))
    ax.plot([cx, cx], [cy - size * 0.25, cy + size * 0.25], color="white", lw=1.2, zorder=zorder + 2)
    for dx, dy in [(-0.14, 0.10), (-0.14, -0.10), (0.14, 0.10), (0.14, -0.10)]:
        nd = Circle(
            (cx + dx * size, cy + dy * size),
            size * 0.05,
            facecolor="white",
            edgecolor="white",
            zorder=zorder + 3,
        )
        ax.add_patch(nd)
        ax.plot([cx, cx + dx * size], [cy, cy + dy * size], color="white", lw=1.0, zorder=zorder + 2)


def _glyph_graph(ax, cx: float, cy: float, size: float, color: str, zorder: int) -> None:
    pts = [(cx - size * 0.18, cy - size * 0.14), (cx + size * 0.18, cy - size * 0.14), (cx, cy + size * 0.18)]
    for i, j in [(0, 1), (0, 2), (1, 2)]:
        ax.plot([pts[i][0], pts[j][0]], [pts[i][1], pts[j][1]], color="white", lw=1.4, zorder=zorder + 2)
    for px, py in pts:
        ax.add_patch(Circle((px, py), size * 0.08, facecolor=color, edgecolor="white", lw=1.4, zorder=zorder + 3))


def _glyph_recon(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    for sign in (-1, 1):
        ax.add_patch(
            Circle(
                (cx + sign * size * 0.10, cy),
                size * 0.18,
                facecolor="none",
                edgecolor="white",
                lw=1.4,
                zorder=zorder + 2,
            )
        )
    ax.plot(
        [cx - size * 0.06, cx - size * 0.01, cx + size * 0.08],
        [cy, cy - size * 0.06, cy + size * 0.07],
        color="white",
        lw=1.6,
        zorder=zorder + 3,
    )


def _glyph_bucket(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    poly = Polygon(
        [
            [cx - size * 0.24, cy + size * 0.18],
            [cx + size * 0.24, cy + size * 0.18],
            [cx + size * 0.17, cy - size * 0.24],
            [cx - size * 0.17, cy - size * 0.24],
        ],
        closed=True,
        facecolor="none",
        edgecolor="white",
        lw=1.5,
        zorder=zorder + 2,
    )
    ax.add_patch(poly)
    theta = np.linspace(0, np.pi, 30)
    ax.plot(
        cx + np.cos(theta) * size * 0.20,
        cy + size * 0.18 + np.sin(theta) * size * 0.10,
        color="white",
        lw=1.3,
        zorder=zorder + 2,
    )


def _glyph_shield(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    poly = Polygon(
        [
            [cx, cy + size * 0.28],
            [cx + size * 0.24, cy + size * 0.16],
            [cx + size * 0.20, cy - size * 0.10],
            [cx, cy - size * 0.28],
            [cx - size * 0.20, cy - size * 0.10],
            [cx - size * 0.24, cy + size * 0.16],
        ],
        closed=True,
        facecolor="none",
        edgecolor="white",
        lw=1.5,
        zorder=zorder + 2,
    )
    ax.add_patch(poly)
    ax.plot([cx, cx], [cy - size * 0.06, cy + size * 0.12], color="white", lw=1.6, zorder=zorder + 3)
    ax.add_patch(Circle((cx, cy - size * 0.15), size * 0.03, facecolor="white", edgecolor="white", zorder=zorder + 3))


def _glyph_db(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    for dy in (0.18, -0.18):
        ell = mpatches.Ellipse(
            (cx, cy + size * dy),
            size * 0.44,
            size * 0.15,
            facecolor="none",
            edgecolor="white",
            lw=1.4,
            zorder=zorder + 2,
        )
        ax.add_patch(ell)
    for dx in (-0.22, 0.22):
        ax.plot(
            [cx + size * dx, cx + size * dx],
            [cy - size * 0.18, cy + size * 0.18],
            color="white",
            lw=1.4,
            zorder=zorder + 2,
        )


def _glyph_cache(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    r = Rectangle(
        (cx - size * 0.20, cy - size * 0.20),
        size * 0.40,
        size * 0.40,
        facecolor="none",
        edgecolor="white",
        lw=1.4,
        zorder=zorder + 2,
    )
    ax.add_patch(r)
    for off in [-0.12, 0.0, 0.12]:
        for ys in ([cy + size * 0.20, cy + size * 0.29], [cy - size * 0.20, cy - size * 0.29]):
            ax.plot([cx + off * size, cx + off * size], ys, color="white", lw=1.2, zorder=zorder + 2)
        for xs in ([cx + size * 0.20, cx + size * 0.29], [cx - size * 0.20, cx - size * 0.29]):
            ax.plot(xs, [cy + off * size, cy + off * size], color="white", lw=1.2, zorder=zorder + 2)


def _glyph_ui(ax, cx: float, cy: float, size: float, _color: str, zorder: int) -> None:
    r = Rectangle(
        (cx - size * 0.26, cy - size * 0.20),
        size * 0.52,
        size * 0.40,
        facecolor="none",
        edgecolor="white",
        lw=1.4,
        zorder=zorder + 2,
    )
    ax.add_patch(r)
    for dx, top in ((-0.14, 0.06), (0.0, 0.14), (0.14, 0.01)):
        ax.plot(
            [cx + size * dx, cx + size * dx],
            [cy - size * 0.12, cy + size * top],
            color="white",
            lw=2.2,
            zorder=zorder + 3,
        )


_SERVICE_GLYPH_DRAWERS = {
    "kafka": _glyph_kafka,
    "funnel": _glyph_funnel,
    "spark": _glyph_spark,
    "ml": _glyph_ml,
    "graph": _glyph_graph,
    "recon": _glyph_recon,
    "bucket": _glyph_bucket,
    "shield": _glyph_shield,
    "db": _glyph_db,
    "cache": _glyph_cache,
    "ui": _glyph_ui,
}


def _draw_service_glyph(ax, cx: float, cy: float, size: float, color: str, glyph: str, zorder: int) -> None:
    drawer = _SERVICE_GLYPH_DRAWERS.get(glyph)
    if drawer is not None:
        drawer(ax, cx, cy, size, color, zorder)


def draw_service_icon(
    ax,
    cx: float,
    cy: float,
    color: str,
    glyph: str,
    title_lines: list[str],
    size: float = 0.62,
    zorder: int = 6,
) -> None:
    half = size / 2
    tile = FancyBboxPatch(
        (cx - half, cy - half),
        size,
        size,
        boxstyle="round,pad=0.0,rounding_size=0.09",
        facecolor=color,
        edgecolor="white",
        linewidth=1.3,
        zorder=zorder,
    )
    ax.add_patch(tile)
    _draw_service_glyph(ax, cx, cy, size, color, glyph, zorder)

    for idx, line in enumerate(title_lines):
        weight = "bold" if idx == 0 else "normal"
        fsize = 8.2 if idx == 0 else 7.2
        col = "#0F172A" if idx == 0 else "#475569"
        ax.text(
            cx,
            cy - half - 0.15 - idx * 0.22,
            line,
            ha="center",
            va="top",
            fontsize=fsize,
            fontweight=weight,
            color=col,
            zorder=zorder + 3,
        )


def draw_client_card(
    ax,
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    sub1: str,
    sub2: str,
    color: str = "#334155",
) -> None:
    draw_rounded_box(ax, x, y, w, h, edgecolor=color, facecolor="white", lw=1.4, radius=0.04)
    ax.text(x + w / 2, y + h - 0.28, title, ha="center", va="center", fontsize=8.4, fontweight="bold", color=color)
    mx, my = x + w / 2, y + h / 2 + 0.05
    scr = FancyBboxPatch(
        (mx - 0.30, my - 0.10),
        0.60,
        0.38,
        boxstyle="round,pad=0.0,rounding_size=0.06",
        facecolor=color,
        edgecolor="white",
        lw=1.2,
        zorder=4,
    )
    scr_in = Rectangle((mx - 0.22, my - 0.03), 0.44, 0.24, facecolor="none", edgecolor="white", lw=1.2, zorder=5)
    base = FancyBboxPatch(
        (mx - 0.34, my - 0.23),
        0.68,
        0.09,
        boxstyle="round,pad=0.0,rounding_size=0.02",
        facecolor=color,
        edgecolor="white",
        lw=1.0,
        zorder=4,
    )
    ax.add_patch(scr)
    ax.add_patch(scr_in)
    ax.add_patch(base)
    ax.text(x + w / 2, y + 0.44, sub1, ha="center", va="center", fontsize=7.5, color="#1E293B", fontweight="bold")
    ax.text(x + w / 2, y + 0.20, sub2, ha="center", va="center", fontsize=7.0, color="#475569")


def draw_ortho_arrow(
    ax,
    points: list[tuple[float, float]],
    color: str = "#1E293B",
    lw: float = 1.5,
    ls: str = "-",
    arrow_end: bool = True,
    zorder: int = 4,
) -> None:
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    if arrow_end:
        ax.plot(xs[:-1], ys[:-1], color=color, lw=lw, ls=ls, zorder=zorder, solid_capstyle="round")
        p_prev = points[-2]
        p_last = points[-1]
        ax.annotate(
            "",
            xy=p_last,
            xytext=p_prev,
            arrowprops={
                "arrowstyle": "->,head_width=0.24,head_length=0.28",
                "color": color,
                "lw": lw,
                "linestyle": ls,
                "shrinkA": 0,
                "shrinkB": 0,
            },
            zorder=zorder + 1,
        )
    else:
        ax.plot(xs, ys, color=color, lw=lw, ls=ls, zorder=zorder, solid_capstyle="round")


def draw_step_badge(ax, x: float, y: float, number: int, color: str = "#0F172A", zorder: int = 10) -> None:
    """Draw a numbered circular badge in open whitespace NEXT TO a connector line."""
    c = Circle((x, y), 0.20, facecolor=color, edgecolor="white", lw=1.3, zorder=zorder)
    ax.add_patch(c)
    ax.text(
        x, y, str(number), ha="center", va="center", fontsize=8.4, fontweight="bold", color="white", zorder=zorder + 1
    )


ZONE_A_COLOR = "#334155"  # Slate Navy — External Sources & Downstream Clients ([C1]..[C3])
ZONE_B_COLOR = "#D97706"  # Amber Orange — Ingestion & Quality Boundary ([C4], [C5])
ZONE_C_COLOR = "#7C3AED"  # Vivid Purple — Speed Layer Streaming ([C6]..[C8])
ZONE_D_COLOR = "#2563EB"  # Royal Blue — Batch Layer ETL, Graph, ML & Recon ([C9]..[C12])
ZONE_E_COLOR = "#0D9488"  # Teal — Frozen Model & Static Feature Registry ([C13a], [C13b])
ZONE_F_COLOR = "#15803D"  # Forest Green — Polyglot Storage, WAL & BI Serving ([C14]..[C17])

PIPE_STREAM_COLOR = "#7C3AED"  # Speed Pipeline: [1], [3], [5]
PIPE_BATCH_COLOR = "#2563EB"  # Batch & Mining Pipeline: [2], [4], [7]
PIPE_MLOPS_COLOR = "#0D9488"  # Frozen Registry Scoring Pipeline: [8]
PIPE_STORAGE_COLOR = "#15803D"  # Storage, Upsert & BI Serving Pipeline: [6], [9], [10]


def _draw_pipeline_legend(ax) -> None:
    """Draw a compact Pipeline Arrow Color Legend in the top-right header area."""
    items = [
        (11.10, PIPE_STREAM_COLOR, "Speed Pipeline"),
        (12.75, PIPE_BATCH_COLOR, "Batch Pipeline"),
        (14.38, PIPE_MLOPS_COLOR, "MLOps Registry"),
        (16.02, PIPE_STORAGE_COLOR, "Storage & Serving"),
    ]
    for lx, lcol, ltxt in items:
        draw_ortho_arrow(ax, [(lx, 9.95), (lx + 0.32, 9.95)], color=lcol, lw=2.0, zorder=8)
        ax.text(lx + 0.38, 9.95, ltxt, ha="left", va="center", fontsize=7.2, fontweight="bold", color=lcol, zorder=8)


def _draw_arch_zones_and_components(ax) -> None:
    # 1. ZONE A: EXTERNAL SOURCES & CONSUMERS — ALL ZONE_A_COLOR
    draw_rounded_box(
        ax, 0.08, 0.72, 2.44, 8.84, edgecolor=ZONE_A_COLOR, facecolor="#F8FAFC", lw=1.5, ls="--", radius=0.04
    )
    ax.text(
        1.30,
        9.30,
        "Zone A: External Sources",
        ha="center",
        va="center",
        fontsize=8.5,
        style="italic",
        fontweight="bold",
        color=ZONE_A_COLOR,
    )
    ax.text(
        1.30,
        9.06,
        "& Downstream Clients",
        ha="center",
        va="center",
        fontsize=8.0,
        style="italic",
        color=ZONE_A_COLOR,
    )

    draw_client_card(
        ax,
        0.22,
        6.75,
        2.15,
        2.05,
        "GPS Probe Fleet",
        "Real-Time Telemetry Stream",
        "Vehicle Speed Observations",
        color=ZONE_A_COLOR,
    )
    draw_client_card(
        ax,
        0.22,
        3.95,
        2.15,
        2.05,
        "OSM / GIS Topology",
        "Urban Road Network Graph",
        "Directed Street Segments",
        color=ZONE_A_COLOR,
    )
    draw_client_card(
        ax,
        0.22,
        0.95,
        2.15,
        2.05,
        "Traffic Ops & Users",
        "Analysts & Commuters",
        "Web / REST Query Clients",
        color=ZONE_A_COLOR,
    )

    # 2. OUTER PLATFORM BOUNDARY (CLOUD VPC / DOCKER CLUSTER)
    draw_rounded_box(ax, 3.20, 0.22, 14.78, 9.95, edgecolor="#0F172A", facecolor="white", lw=2.0, radius=0.0)
    draw_header_badge(ax, 3.20, 9.73, 0.44, "#0F172A", "cloud")
    ax.text(
        3.78,
        9.95,
        "HCMC Smart Traffic Big Data Platform (Containerized Cluster)",
        ha="left",
        va="center",
        fontsize=10.2,
        fontweight="bold",
        color="#0F172A",
    )
    _draw_pipeline_legend(ax)

    # 3. UNIFIED DATA & COMPUTE PLANE (DASHED SLATE REGION)
    draw_rounded_box(ax, 3.40, 0.38, 14.38, 9.18, edgecolor="#475569", facecolor="#F8FAFC", lw=1.4, ls="--", radius=0.0)
    draw_header_badge(ax, 3.40, 9.12, 0.44, "#475569", "flag")
    ax.text(
        3.96,
        9.34,
        "Unified Data & Compute Plane (Event Streaming · Distributed Batch · Model Registry · Polyglot Serving)",
        ha="left",
        va="center",
        fontsize=9.4,
        fontweight="bold",
        color="#334155",
    )

    # 4. ZONE B: INGESTION & QUALITY BOUNDARY — ALL ZONE_B_COLOR
    draw_rounded_box(ax, 3.65, 3.65, 2.05, 5.20, edgecolor=ZONE_B_COLOR, facecolor="white", lw=1.5, ls="--", radius=0.0)
    ax.text(
        4.675,
        8.60,
        "Zone B: Ingestion",
        ha="center",
        va="center",
        fontsize=8.5,
        style="italic",
        fontweight="bold",
        color=ZONE_B_COLOR,
    )

    draw_service_icon(
        ax, 4.675, 7.65, ZONE_B_COLOR, "kafka", ["Apache Kafka Broker", "Partitioned Event Stream"], size=0.64
    )
    draw_service_icon(ax, 4.675, 5.00, ZONE_B_COLOR, "funnel", ["Quality Funnel", "Schema & Hygiene Gate"], size=0.64)

    # 5. SPARK UNIFIED COMPUTE CLUSTER ENVELOPE (CONTAINING ZONE C & ZONE D)
    draw_rounded_box(ax, 6.55, 3.30, 8.10, 5.58, edgecolor="#334155", facecolor="white", lw=1.6, radius=0.0)
    draw_header_badge(ax, 6.55, 8.48, 0.40, "#334155", "vpc")
    ax.text(
        7.06,
        8.68,
        "Apache Spark Unified Compute Cluster",
        ha="left",
        va="center",
        fontsize=9.5,
        fontweight="bold",
        color="#1E293B",
    )

    draw_rounded_box(ax, 6.72, 3.44, 7.76, 4.92, edgecolor="#64748B", facecolor="#F8FAFC", lw=1.2, radius=0.0)
    draw_header_badge(ax, 6.72, 7.98, 0.38, "#64748B", "exec")
    ax.text(
        7.20,
        8.17,
        "Distributed Spark Executors (Unified Stream & Batch Compute Engine)",
        ha="left",
        va="center",
        fontsize=8.5,
        fontweight="bold",
        color="#334155",
    )

    # --- ZONE C: SPEED LAYER (TOP CARD INSIDE SPARK) — ALL ZONE_C_COLOR ---
    draw_rounded_box(ax, 6.88, 6.28, 7.44, 1.58, edgecolor=ZONE_C_COLOR, facecolor="white", lw=1.5, radius=0.04)
    ax.text(
        10.60,
        7.68,
        "Zone C: Speed Layer — Spark Structured Streaming (Real-Time Processing)",
        ha="center",
        va="center",
        fontsize=8.2,
        fontweight="bold",
        color=ZONE_C_COLOR,
    )

    draw_service_icon(
        ax, 7.80, 7.12, ZONE_C_COLOR, "spark", ["Watermark & Windowing", "Dual-Sink Stream Router"], size=0.56
    )
    draw_service_icon(
        ax, 10.60, 7.12, ZONE_C_COLOR, "cache", ["Stream Sketches", "Bloom · HyperLogLog · EWMA"], size=0.56
    )
    draw_service_icon(ax, 13.35, 7.12, ZONE_C_COLOR, "ml", ["Online Scorer", "Read-Only Stream Inference"], size=0.56)

    # --- ZONE D: BATCH, MINING & RECON LAYER (BOTTOM CARD INSIDE SPARK) — ALL ZONE_D_COLOR ---
    draw_rounded_box(ax, 6.88, 3.52, 7.44, 2.54, edgecolor=ZONE_D_COLOR, facecolor="white", lw=1.5, radius=0.04)
    ax.text(
        10.60,
        5.86,
        "Zone D: Batch Layer — Distributed ETL, Graph Mining, ML Training & Reconciliation",
        ha="center",
        va="center",
        fontsize=8.2,
        fontweight="bold",
        color=ZONE_D_COLOR,
    )

    draw_service_icon(
        ax, 7.68, 5.06, ZONE_D_COLOR, "spark", ["Combiner & Join", "Monoid Aggregation · Salting"], size=0.56
    )
    draw_service_icon(
        ax, 9.62, 5.06, ZONE_D_COLOR, "graph", ["Graph & Pattern Mining", "PageRank · SON · K-Means"], size=0.56
    )
    draw_service_icon(
        ax, 11.56, 5.06, ZONE_D_COLOR, "ml", ["Causal ML Training", "Feature Pipeline · Model Fit"], size=0.56
    )
    draw_service_icon(
        ax, 13.42, 5.06, ZONE_D_COLOR, "recon", ["Two-Tier Reconciliation", "Late Backfill & Re-Score"], size=0.56
    )

    sub_pills = [
        (7.72, "Line-Graph PageRank"),
        (9.60, "SON / FPGrowth Rules"),
        (11.52, "K-Means Clustering"),
        (13.42, "Exact Reference Audit"),
    ]
    for px, ptext in sub_pills:
        draw_rounded_box(
            ax, px - 0.78, 3.60, 1.56, 0.32, edgecolor=ZONE_D_COLOR, facecolor="#EFF6FF", lw=1.1, radius=0.04
        )
        ax.text(px, 3.76, ptext, ha="center", va="center", fontsize=6.8, fontweight="bold", color=ZONE_D_COLOR)

    # 6. ZONE E: FROZEN MODEL & FEATURE REGISTRY — ALL ZONE_E_COLOR
    draw_rounded_box(ax, 15.42, 3.52, 2.15, 5.36, edgecolor=ZONE_E_COLOR, facecolor="white", lw=1.5, radius=0.0)
    ax.text(
        16.495,
        8.62,
        "Zone E: Frozen Registry",
        ha="center",
        va="center",
        fontsize=8.4,
        fontweight="bold",
        color=ZONE_E_COLOR,
    )
    ax.text(
        16.495,
        8.38,
        "(Zero-Conflict MLOps)",
        ha="center",
        va="center",
        fontsize=7.5,
        style="italic",
        color=ZONE_E_COLOR,
    )

    draw_service_icon(ax, 16.495, 7.38, ZONE_E_COLOR, "ml", ["Trained ML Model", "Frozen Forecasting Model"], size=0.60)
    draw_service_icon(
        ax,
        16.495,
        4.92,
        ZONE_E_COLOR,
        "bucket",
        ["Static Feature Store", "Graph · Cluster · Priors"],
        size=0.60,
    )

    # 7. ZONE F: POLYGLOT STORAGE, CHECKPOINTS, SERVING & BI UI — ALL ZONE_F_COLOR
    draw_rounded_box(
        ax, 3.65, 0.92, 13.92, 2.04, edgecolor=ZONE_F_COLOR, facecolor="white", lw=1.5, ls="--", radius=0.04
    )
    ax.text(
        8.45,
        2.76,
        "Zone F: Polyglot Storage, Fault-Tolerance & BI Serving Layer",
        ha="center",
        va="center",
        fontsize=8.3,
        style="italic",
        fontweight="bold",
        color=ZONE_F_COLOR,
        bbox={"boxstyle": "square,pad=0.10", "facecolor": "white", "edgecolor": "none"},
        zorder=6,
    )

    draw_service_icon(
        ax, 4.675, 1.96, ZONE_F_COLOR, "ui", ["TOC Command Center", "ReactJS + FastAPI (:8000)"], size=0.60
    )
    draw_service_icon(
        ax, 7.80, 1.96, ZONE_F_COLOR, "shield", ["Quarantine & WAL", "Dead-Letter & Checkpoints"], size=0.60
    )
    draw_service_icon(
        ax,
        11.56,
        1.96,
        ZONE_F_COLOR,
        "bucket",
        ["Medallion Lakehouse", "Bronze · Silver · Gold Parquet"],
        size=0.60,
    )
    draw_service_icon(
        ax, 16.35, 1.96, ZONE_F_COLOR, "db", ["MongoDB Serving Store", "Idempotent Upsert · Indexed"], size=0.60
    )


def _draw_arch_connectors(ax) -> None:
    # --- PIPELINE 1: SPEED / STREAMING PIPELINE (PIPE_STREAM_COLOR = #7C3AED) ---
    draw_ortho_arrow(ax, [(2.37, 7.65), (4.30, 7.65)], color=PIPE_STREAM_COLOR, lw=1.6)
    draw_ortho_arrow(ax, [(5.05, 7.65), (6.12, 7.65), (6.12, 7.12), (7.48, 7.12)], color=PIPE_STREAM_COLOR, lw=1.6)
    draw_ortho_arrow(ax, [(8.12, 7.12), (10.28, 7.12)], color=PIPE_STREAM_COLOR, lw=1.6)
    draw_ortho_arrow(ax, [(10.92, 7.12), (13.03, 7.12)], color=PIPE_STREAM_COLOR, lw=1.6)

    # --- PIPELINE 2: BATCH ETL, GRAPH/MINING & TRAINING PIPELINE (PIPE_BATCH_COLOR = #2563EB) ---
    draw_ortho_arrow(ax, [(2.37, 5.06), (4.30, 5.06)], color=PIPE_BATCH_COLOR, lw=1.6)
    draw_ortho_arrow(ax, [(4.675, 6.72), (4.675, 5.42)], color=PIPE_BATCH_COLOR, lw=1.4, ls="--")
    draw_ortho_arrow(ax, [(5.05, 5.06), (7.36, 5.06)], color=PIPE_BATCH_COLOR, lw=1.6)
    draw_ortho_arrow(ax, [(5.95, 5.06), (5.95, 1.96), (7.44, 1.96)], color=PIPE_BATCH_COLOR, lw=1.4, ls="--")
    draw_ortho_arrow(ax, [(8.00, 5.06), (9.30, 5.06)], color=PIPE_BATCH_COLOR, lw=1.6)
    draw_ortho_arrow(ax, [(9.94, 5.06), (11.24, 5.06)], color=PIPE_BATCH_COLOR, lw=1.6)
    draw_ortho_arrow(
        ax, [(11.56, 5.38), (11.56, 5.54), (15.22, 5.54), (15.22, 7.56), (16.14, 7.56)], color=PIPE_BATCH_COLOR, lw=1.6
    )
    draw_ortho_arrow(ax, [(15.22, 5.54), (15.22, 5.08), (16.14, 5.08)], color=PIPE_BATCH_COLOR, lw=1.6)

    # --- PIPELINE 3: FROZEN MLOPS REGISTRY READ-ONLY SCORING (PIPE_MLOPS_COLOR = #0D9488) ---
    draw_ortho_arrow(ax, [(16.14, 7.22), (13.67, 7.22)], color=PIPE_MLOPS_COLOR, lw=1.6, ls="--")
    draw_ortho_arrow(
        ax, [(16.14, 4.78), (14.82, 4.78), (14.82, 7.22)], color=PIPE_MLOPS_COLOR, lw=1.5, ls="--", arrow_end=False
    )
    draw_ortho_arrow(ax, [(14.82, 5.22), (13.74, 5.22)], color=PIPE_MLOPS_COLOR, lw=1.6, ls="--")

    # --- PIPELINE 4: STORAGE, RECONCILIATION & BI SERVING PIPELINE (PIPE_STORAGE_COLOR = #15803D) ---
    draw_ortho_arrow(ax, [(7.48, 6.95), (6.32, 6.95), (6.32, 2.14), (7.44, 2.14)], color=PIPE_STORAGE_COLOR, lw=1.5)
    draw_ortho_arrow(ax, [(8.16, 1.96), (11.20, 1.96)], color=PIPE_STORAGE_COLOR, lw=1.5)
    draw_ortho_arrow(ax, [(9.94, 4.90), (10.55, 4.90), (10.55, 2.14), (11.20, 2.14)], color=PIPE_STORAGE_COLOR, lw=1.5)
    draw_ortho_arrow(ax, [(11.92, 2.14), (12.46, 2.14), (12.46, 4.92), (13.10, 4.92)], color=PIPE_STORAGE_COLOR, lw=1.5)
    draw_ortho_arrow(
        ax,
        [(13.67, 6.96), (15.02, 6.96), (15.02, 3.16), (16.50, 3.16), (16.50, 2.32)],
        color=PIPE_STORAGE_COLOR,
        lw=1.6,
    )
    draw_ortho_arrow(
        ax,
        [(13.74, 4.92), (14.62, 4.92), (14.62, 3.02), (16.20, 3.02), (16.20, 2.32)],
        color=PIPE_STORAGE_COLOR,
        lw=1.6,
    )
    draw_ortho_arrow(ax, [(16.35, 1.12), (16.35, 0.62), (4.675, 0.62), (4.675, 1.12)], color=PIPE_STORAGE_COLOR, lw=1.6)
    draw_ortho_arrow(ax, [(11.56, 1.12), (11.56, 0.62)], color=PIPE_STORAGE_COLOR, lw=1.4, arrow_end=False)
    draw_ortho_arrow(ax, [(7.80, 1.12), (7.80, 0.62)], color=PIPE_STORAGE_COLOR, lw=1.4, arrow_end=False)
    draw_ortho_arrow(ax, [(4.30, 1.96), (2.37, 1.96)], color=PIPE_STORAGE_COLOR, lw=1.6)


def build_architecture_png(out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(18.2, 10.4), dpi=300)
    ax.set_xlim(0, 18.2)
    ax.set_ylim(0, 10.4)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    _draw_arch_zones_and_components(ax)
    _draw_arch_connectors(ax)

    plt.tight_layout(pad=0.15)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def build_detailed_reference_png(out_path: Path) -> None:
    build_architecture_png(out_path)


def _load_walkthrough_metrics() -> dict[str, str]:
    m = json.loads((OUTPUTS_DIR / "metrics.json").read_text(encoding="utf-8"))
    mv = json.loads((OUTPUTS_DIR / "mongo_verify.json").read_text(encoding="utf-8"))
    ds = m["dataset"]
    qf = m["quality_funnel_c5"]
    ev = m["evaluation"]
    tr = ev["transition_analysis"]
    rec = m["streaming_mmds"]["reconciliation"]
    lat = mv["uc2_arterial_query"]["latency_benchmark_ms"]
    return {
        "miss_pct": f"{qf['missing_max_velocity_train_pct']:.2f}%",
        "cold_pct": f"{m['static_feature_store_c13b']['cold_start_ratio_pct']:.2f}%",
        "windows": f"{ds['labeled_windows_count']:,}",
        "segs": f"{ds['segments_count']:,}",
        "edges": f"{ds['line_graph_transition_edges']:,}",
        "co_rules": f"{m['graph_and_patterns']['co_congestion_rules_found']}",
        "fp_rules": f"{m['spark_runtime_verification']['spark_fpgrowth_rules_count']}",
        "cold_segs": f"{ds['cold_start_segments_count']:,}",
        "roc": f"{ev['roc_auc']:.4f}",
        "det_auc": f"{tr['rf_23d_transition_detect_roc_auc']:.4f}",
        "p95_ms": f"{lat['warm_p95_ms']:.3f} ms",
        "late_cnt": f"{rec['affected_late_segment_windows_recomputed']:,}",
        "heal_cnt": f"{rec['healed_downstream_lag_windows']:,}",
    }


def _walkthrough_steps(wm: dict[str, str]) -> list[tuple[float, str, str, str, str, list[str]]]:
    return [
        (
            0.22,
            "RAW INPUT (ZONE A)",
            "1. Bản tin GPS & Bản đồ GIS",
            "#1E293B",
            "#F8FAFC",
            [
                "• Lúc 07:08 sáng, xe qua đoạn\n  Lý Thường Kiệt #47407 gửi:\n  velocity = 4 km/h.",
                f"• EC-1: {wm['miss_pct']} cửa sổ\n  khuyết max_velocity.",
                f"• EC-6: {wm['cold_pct']} đoạn nhánh\n  bị Cold-Start (0 GPS).",
            ],
        ),
        (
            3.92,
            "P1 · QUALITY & MONOID",
            "2. Chuẩn hóa & Gán nhãn LOS",
            "#0284C7",
            "#F0F9FF",
            [
                "• 3-Tier Imputation nội suy\n  max_velocity = 40 km/h (TRAIN).",
                "• 5-Tuple Monoid gộp cửa sổ\n  07:00–07:30: mean_v = 4 km/h.",
                f"• r = 4/40 = 0.10 (< 0.20) → LOS F\n  trên {wm['windows']} cửa sổ (89.19% khớp).",
            ],
        ),
        (
            7.62,
            "P2 · GRAPH & K-MEANS",
            "3. Khai phá Đồ thị & Hành vi",
            "#0D9488",
            "#F0FDFA",
            [
                f"• Line-Graph PageRank trên\n  {wm['segs']} đoạn & {wm['edges']} cạnh.",
                f"• {wm['fp_rules']} luật FPGrowth / {wm['co_rules']} đồng kẹt;\n  7D K-Means gom k=6 cụm.",
                f"• Cấp Static Features cho đủ\n  {wm['cold_segs']} đoạn Cold-Start.",
            ],
        ),
        (
            11.32,
            "P3 · CAUSAL ML (+30M)",
            "4. Dự báo Nhân quả t+30m",
            "#15803D",
            "#ECFDF5",
            [
                "• Ghép 14 biến cục bộ + 9 biến\n  PageRank, K-Means & Priors\n  thành 23D Feature Vector.",
                (
                    f"• Frozen RF (Organic Train)\n  dự báo t+30m: ROC-AUC {wm['roc']},\n  Detect AUC {wm['det_auc']}."
                ),
                f"• Phủ 100% {wm['segs']} đoạn cơ giới.",
            ],
        ),
        (
            15.02,
            "P4 · STREAM & SERVING",
            "5. O(1) Sketches & IXSCAN",
            "#7C3AED",
            "#FAF5FF",
            [
                "• 32 KiB RotatingBloom + 1 KiB HLL++\n  + EWMA Hotspot (score = 6.67).",
                f"• MongoDB 8.0 IXSCAN p95 {wm['p95_ms']};\n  ReactJS + FastAPI TOC (:8000).",
                f"• Two-Tier Recon bù {wm['late_cnt']} trễ,\n  heal {wm['heal_cnt']} lag (100%).",
            ],
        ),
    ]


def build_concrete_example_flow_png(out_path: Path) -> None:
    """Render an intuitive step-by-step visual example tracing 1 real street (Lý Thường Kiệt #47407) for newcomers."""
    fig, ax = plt.subplots(figsize=(18.6, 5.18), dpi=240)
    ax.set_xlim(0.05, 18.55)
    ax.set_ylim(0.08, 5.12)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.text(
        9.30,
        4.84,
        "END-TO-END WALKTHROUGH: HÀNH TRÌNH 1 BẢN TIN GPS THỰC TẾ QUA 4 BƯỚC XỬ LÝ (P1 → P4)",
        ha="center",
        va="center",
        fontsize=13.4,
        fontweight="bold",
        color="#0F172A",
    )
    subtitle_txt = (
        "Theo dõi đoạn đường Lý Thường Kiệt (segment_id = 47407, Quận 10) "
        "từ Raw GPS Ping đến cảnh báo trên MongoDB 8.0 & ReactJS + FastAPI TOC (:8000)"
    )
    ax.text(9.30, 4.48, subtitle_txt, ha="center", va="center", fontsize=10.0, color="#475569")

    steps = _walkthrough_steps(_load_walkthrough_metrics())

    for idx, (x, badge, title, col, bg, bullets) in enumerate(steps):
        w, y, h = 3.24, 0.22, 4.02
        card = FancyBboxPatch(
            (x, y), w, h, boxstyle="round,pad=0.0,rounding_size=0.12", edgecolor=col, facecolor=bg, lw=1.9, zorder=2
        )
        hdr = FancyBboxPatch(
            (x, y + h - 0.78),
            w,
            0.78,
            boxstyle="round,pad=0.0,rounding_size=0.12",
            edgecolor=col,
            facecolor=col,
            lw=1.0,
            zorder=3,
        )
        ax.add_patch(card)
        ax.add_patch(hdr)
        ax.add_patch(Rectangle((x, y + h - 0.78), w, 0.26, facecolor=col, edgecolor="none", zorder=3))
        ax.text(
            x + w / 2,
            y + h - 0.24,
            badge,
            ha="center",
            va="center",
            fontsize=9.5,
            fontweight="bold",
            color="#E0F2FE",
            zorder=4,
        )
        ax.text(
            x + w / 2,
            y + h - 0.56,
            title,
            ha="center",
            va="center",
            fontsize=10.2,
            fontweight="bold",
            color="white",
            zorder=4,
        )
        for b_idx, b_txt in enumerate(bullets):
            ax.text(
                x + 0.14,
                y + h - 0.98 - b_idx * 1.03,
                b_txt,
                ha="left",
                va="top",
                fontsize=9.1,
                color="#0F172A",
                linespacing=1.28,
                zorder=4,
            )
        if idx < len(steps) - 1:
            draw_ortho_arrow(ax, [(x + w + 0.03, y + h / 2), (x + w + 0.43, y + h / 2)], color=PIPE_BATCH_COLOR, lw=2.4)

    plt.tight_layout(pad=0.05)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


if __name__ == "__main__":
    import shutil

    primary = REPO_DIR / "outputs" / "system_architecture.png"
    detailed = REPO_DIR / "outputs" / "slide_charts" / "arch_detailed_reference.png"
    concrete_flow = REPO_DIR / "outputs" / "slide_charts" / "chart_concrete_example_flow.png"
    build_architecture_png(primary)
    shutil.copy2(primary, detailed)
    build_concrete_example_flow_png(concrete_flow)
    print(f"Rendered AWS architecture: {primary} ({primary.stat().st_size:,} bytes)")
