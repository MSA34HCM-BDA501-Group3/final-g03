"""Render clear, zero-crossing Component & Dataflow diagrams for Batch Processing & Stream Processing slides."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle

ZONE_A_COLOR = "#334155"  # Slate — External Sources
ZONE_B_COLOR = "#D97706"  # Amber — Ingestion & Quality Gate
ZONE_C_COLOR = "#7C3AED"  # Purple — Speed Layer
ZONE_D_COLOR = "#2563EB"  # Royal Blue — Batch Layer
ZONE_E_COLOR = "#0D9488"  # Teal — Frozen Model & Static Feature Registry
ZONE_F_COLOR = "#15803D"  # Forest Green — Polyglot Storage, WAL & Serving

PIPE_INGEST_COLOR = "#D97706"
PIPE_STREAM_COLOR = "#7C3AED"
PIPE_BATCH_COLOR = "#2563EB"
PIPE_MLOPS_COLOR = "#0D9488"
PIPE_STORAGE_COLOR = "#15803D"


def _box(
    ax: plt.Axes,
    x: float,
    y: float,
    w: float,
    h: float,
    fc: str,
    ec: str,
    lw: float = 1.6,
    ls: str = "-",
    radius: float = 0.09,
    zorder: int = 2,
) -> None:
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle=f"round,pad=0.0,rounding_size={radius}",
        facecolor=fc,
        edgecolor=ec,
        linewidth=lw,
        linestyle=ls,
        zorder=zorder,
    )
    ax.add_patch(patch)


def _comp_card(
    ax: plt.Axes,
    x: float,
    y: float,
    w: float,
    h: float,
    zone_tag: str,
    title: str,
    bullets: list[str],
    color: str,
    bg: str = "#FFFFFF",
    header_h: float = 0.58,
    body_fs: float = 8.5,
) -> None:
    _box(ax, x, y, w, h, bg, color, lw=1.7, radius=0.09, zorder=3)
    _box(ax, x, y + h - header_h, w, header_h, color, color, lw=1.0, radius=0.09, zorder=4)
    ax.add_patch(Rectangle((x, y + h - header_h), w, 0.18, facecolor=color, edgecolor="none", zorder=4))
    if zone_tag:
        ax.text(
            x + w / 2,
            y + h - 0.17,
            zone_tag,
            fontsize=7.3,
            fontweight="bold",
            color="#E2E8F0",
            ha="center",
            va="center",
            zorder=5,
        )
        title_y = y + h - 0.41
    else:
        title_y = y + h - header_h / 2
    ax.text(
        x + w / 2,
        title_y,
        title,
        fontsize=9.3,
        fontweight="bold",
        color="white",
        ha="center",
        va="center",
        zorder=5,
    )
    body_txt = "\n".join(bullets)
    ax.text(
        x + 0.13,
        y + (h - header_h) / 2,
        body_txt,
        fontsize=body_fs,
        color="#0F172A",
        ha="left",
        va="center",
        linespacing=1.32,
        zorder=5,
    )


def _ortho_arrow(
    ax: plt.Axes,
    pts: list[tuple[float, float]],
    color: str,
    lw: float = 2.0,
    ls: str = "-",
    zorder: int = 6,
) -> None:
    if len(pts) > 2:
        xs = [p[0] for p in pts[:-1]]
        ys = [p[1] for p in pts[:-1]]
        ax.plot(xs, ys, color=color, lw=lw, ls=ls, solid_capstyle="round", zorder=zorder)
    ax.annotate(
        "",
        xy=pts[-1],
        xytext=pts[-2],
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


def _draw_legend(ax: plt.Axes, items: list[tuple[float, str, str, str]]) -> None:
    for lx, lcol, lls, ltxt in items:
        _ortho_arrow(ax, [(lx, 4.76), (lx + 0.36, 4.76)], color=lcol, lw=2.1, ls=lls, zorder=7)
        ax.text(lx + 0.43, 4.76, ltxt, fontsize=8.0, fontweight="bold", color=lcol, va="center", zorder=7)


def _draw_batch_boxes(ax: plt.Axes) -> None:
    ax.text(
        0.20,
        4.76,
        "LUỒNG DỮ LIỆU & THÀNH PHẦN XỬ LÝ LÔ (BATCH PROCESSING FLOW · ZONES A → B → D → F / E)",
        fontsize=10.6,
        fontweight="bold",
        color="#0F172A",
        va="center",
    )
    _draw_legend(
        ax,
        [
            (10.65, PIPE_INGEST_COLOR, "-", "Ingestion"),
            (12.22, PIPE_BATCH_COLOR, "-", "Batch Pipeline"),
            (14.18, PIPE_STORAGE_COLOR, "-", "Lakehouse Write"),
            (16.18, PIPE_MLOPS_COLOR, "-", "Registry Lock"),
        ],
    )

    # Column 1: Zone A & Zone B
    _comp_card(
        ax,
        0.20,
        2.56,
        2.75,
        1.82,
        "ZONE A · EXTERNAL SOURCES",
        "GPS Logs & OSM Topology",
        [
            "• Bản tin vận tốc GPS hành trình",
            "• Danh mục đường, đoạn & nút giao",
            "• Cửa sổ đối chứng kiểm chuẩn",
        ],
        ZONE_A_COLOR,
        "#F8FAFC",
    )
    _comp_card(
        ax,
        0.20,
        0.26,
        2.75,
        1.88,
        "ZONE B · QUALITY GATE",
        "Quality Funnel",
        [
            "• Ép kiểu an toàn ANSI try_cast",
            "• Chuẩn hóa dấu NFC & giờ UTC+7",
            "• Tách bản ghi hỏng → Quarantine",
        ],
        ZONE_B_COLOR,
        "#FFFBEB",
    )

    # Column 2: Zone D Container (Apache Spark Batch Layer)
    _box(ax, 3.55, 0.22, 10.00, 4.22, "#F8FAFC", ZONE_D_COLOR, lw=1.7, ls="--", radius=0.10, zorder=1)
    ax.text(
        8.55,
        4.24,
        "ZONE D · APACHE SPARK BATCH LAYER (XỬ LÝ LÔ PHÂN TÁN & KHAI PHÁ ĐỒ THỊ)",
        fontsize=8.6,
        fontweight="bold",
        color=ZONE_D_COLOR,
        ha="center",
        va="center",
        zorder=3,
    )

    _comp_card(
        ax,
        3.80,
        2.44,
        4.38,
        1.54,
        "",
        "Broadcast Join & 3-Tier Vmax",
        [
            "• Ghép bảng danh mục đường trong bộ nhớ",
            "• Tra cứu Vmax 3 tầng khóa trên tập TRAIN",
            "• Giảm trộn mạng lớn (10 → 2 Shuffle Stages)",
        ],
        ZONE_D_COLOR,
        "#FFFFFF",
        header_h=0.46,
    )
    _comp_card(
        ax,
        8.76,
        2.44,
        4.54,
        1.54,
        "",
        "2-Stage Key Salting & 5-Tuple Monoid",
        [
            "• Salting: Gắn đuôi 0..7 chia 8 giỏ chống nghẽn",
            "• Un-salt: Cộng bộ 5 số (cnt, sum, min, max, sev)",
            "• Giảm lệch tải 687.43x → 1.023x · Sai số = 0.0",
        ],
        ZONE_D_COLOR,
        "#FFFFFF",
        header_h=0.46,
    )
    _comp_card(
        ax,
        3.80,
        0.42,
        4.38,
        1.54,
        "",
        "Shape-Point Pruner & Line-Graph",
        [
            "• Cắt tỉa 90.93% điểm uốn cong trên bản đồ",
            "• Đảo mỗi đoạn đường một chiều thành 1 đỉnh",
            "• Tính PageRank có hướng & bù nút đường cụt",
        ],
        ZONE_D_COLOR,
        "#EFF6FF",
        header_h=0.46,
    )
    _comp_card(
        ax,
        8.76,
        0.42,
        4.54,
        1.54,
        "",
        "SON / FPGrowth & 7D K-Means",
        [
            "• Khai phá tập phổ biến & luật đường đồng kẹt",
            "• Phân cụm 7D K-Means (k = 6) học trên TRAIN",
            "• Suy diễn nhóm hành vi cho vùng mù Cold-Start",
        ],
        ZONE_D_COLOR,
        "#EFF6FF",
        header_h=0.46,
    )

    # Column 3: Zone F Lakehouse & Zone E Static Feature Store
    _comp_card(
        ax,
        14.12,
        2.44,
        3.68,
        1.78,
        "ZONE F · MEDALLION STORAGE",
        "Curated Gold Parquet",
        [
            "• Lưu trữ phân vùng theo year_month",
            "• Cắt tỉa phân vùng khi đọc lịch sử",
            "• Cấp dữ liệu chuẩn cho ML & Đối soát",
        ],
        ZONE_F_COLOR,
        "#F0FDF4",
    )
    _comp_card(
        ax,
        14.12,
        0.26,
        3.68,
        1.82,
        "ZONE E · FROZEN REGISTRY",
        "Static Feature Store",
        [
            "• Lưu PageRank, bậc nút, cụm K-Means",
            "• Phủ kín 100% mạng lưới (84,633 đoạn)",
            "• Cấp biến tĩnh cho Modeling & Luồng",
        ],
        ZONE_E_COLOR,
        "#F0FDFA",
    )


def _draw_batch_connectors(ax: plt.Axes) -> None:
    # Zone A -> Zone B (Ingestion Pipeline)
    _ortho_arrow(ax, [(1.575, 2.56), (1.575, 2.14)], color=PIPE_INGEST_COLOR, lw=2.1)
    # Zone B -> Zone D (splits cleanly to Upper ETL & Lower Graph Mining)
    _ortho_arrow(ax, [(2.95, 1.20), (3.26, 1.20), (3.26, 3.21), (3.80, 3.21)], color=PIPE_BATCH_COLOR, lw=2.1)
    _ortho_arrow(ax, [(3.26, 1.20), (3.80, 1.20)], color=PIPE_BATCH_COLOR, lw=2.1)
    # Inside Zone D horizontal connectors
    _ortho_arrow(ax, [(8.18, 3.21), (8.76, 3.21)], color=PIPE_BATCH_COLOR, lw=2.1)
    _ortho_arrow(ax, [(8.18, 1.19), (8.76, 1.19)], color=PIPE_BATCH_COLOR, lw=2.1)
    # Aggregated TRAIN windows feed SON/FPGrowth & 7D K-Means
    _ortho_arrow(ax, [(11.03, 2.44), (11.03, 1.96)], color=PIPE_BATCH_COLOR, lw=2.0)
    # Zone D -> Zone F (Lakehouse Write) & Zone D -> Zone E (Phase A Registry Lock)
    _ortho_arrow(ax, [(13.30, 3.21), (14.12, 3.21)], color=PIPE_STORAGE_COLOR, lw=2.2)
    _ortho_arrow(ax, [(13.30, 1.19), (14.12, 1.19)], color=PIPE_MLOPS_COLOR, lw=2.2)


def render_batch_flow_diagram(out_path: Path) -> Path:
    """Render clean component & dataflow diagram for Section 7: Batch Processing."""
    fig, ax = plt.subplots(figsize=(18.0, 5.02), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.0)
    ax.set_ylim(0.0, 5.02)
    ax.axis("off")
    _draw_batch_boxes(ax)
    _draw_batch_connectors(ax)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _draw_stream_boxes(ax: plt.Axes) -> None:
    ax.text(
        0.20,
        4.76,
        "LUỒNG XỬ LÝ THỜI GIAN THỰC & ĐỐI SOÁT TRỄ (STREAM PROCESSING FLOW · ZONES B → C / E / D → F)",
        fontsize=10.4,
        fontweight="bold",
        color="#0F172A",
        va="center",
    )
    _draw_legend(
        ax,
        [
            (11.15, PIPE_STREAM_COLOR, "-", "Speed Pipeline"),
            (12.92, PIPE_MLOPS_COLOR, "--", "MLOps Read-Only"),
            (14.88, PIPE_BATCH_COLOR, "-", "Tier-2 Recon"),
            (16.48, PIPE_STORAGE_COLOR, "-", "Serving"),
        ],
    )

    # Column 1: Apache Kafka Broker (Zone B)
    _comp_card(
        ax,
        0.20,
        1.52,
        2.62,
        2.64,
        "ZONE B · INGESTION",
        "Apache Kafka Broker",
        [
            "• Cụm sự kiện chế độ KRaft",
            "• Phân vùng theo segment_id",
            "  giữ đúng thứ tự từng đường",
            "• Tiếp nhận cả tin đúng hạn,",
            "  tin gửi trùng & tin trễ mạng",
        ],
        ZONE_B_COLOR,
        "#FFFBEB",
    )

    # Column 2 Top: Zone C Speed Layer Container
    _box(ax, 3.30, 2.26, 10.12, 2.18, "#FAF5FF", ZONE_C_COLOR, lw=1.7, ls="--", radius=0.10, zorder=1)
    ax.text(
        8.36,
        4.25,
        "ZONE C · SPEED LAYER — LUỒNG CẢNH BÁO TRỰC TUYẾN (< 1 GIÂY · WATERMARK 30 PHÚT)",
        fontsize=8.5,
        fontweight="bold",
        color=ZONE_C_COLOR,
        ha="center",
        va="center",
        zorder=3,
    )

    _comp_card(
        ax,
        3.50,
        2.40,
        2.92,
        1.62,
        "",
        "Watermark & Dual-Sink",
        [
            "• Ghi nhật ký WAL chống mất tin",
            "• Tin ≤ 30m → đi thẳng luồng nhanh",
            "• Tin trễ > 30m → lưu kho chờ bù",
        ],
        ZONE_C_COLOR,
        "#FFFFFF",
        header_h=0.46,
        body_fs=8.1,
    )
    _comp_card(
        ax,
        6.84,
        2.40,
        3.00,
        1.62,
        "",
        "O(1) Stream Sketches",
        [
            "• Bloom (32 KiB): chặn tin trùng",
            "• HLL++ (1 KiB): đếm đường active",
            "• EWMA: theo dõi điểm ĐANG kẹt lì",
        ],
        ZONE_C_COLOR,
        "#FFFFFF",
        header_h=0.46,
        body_fs=8.1,
    )
    _comp_card(
        ax,
        10.26,
        2.40,
        2.96,
        1.62,
        "",
        "Online Stream Scorer",
        [
            "• Ghép biến tức thời + biến tĩnh",
            "• Dự báo chỉ-đọc xác suất kẹt +30m",
            "• Phát cảnh báo tạm thời (v1)",
        ],
        ZONE_C_COLOR,
        "#FFFFFF",
        header_h=0.46,
        body_fs=8.1,
    )

    # Column 2 Bottom: WAL & Bronze (Zone F), Frozen Registry (Zone E), Two-Tier Recon (Zone D)
    _comp_card(
        ax,
        3.50,
        0.38,
        2.92,
        1.52,
        "ZONE F · FAULT TOLERANCE",
        "WAL & Bronze Landing",
        [
            "• Checkpoint chống nhân đôi khi sập",
            "• Lưu 100% sự kiện gốc (kể cả trễ)",
        ],
        ZONE_F_COLOR,
        "#F0FDF4",
        header_h=0.54,
        body_fs=8.0,
    )
    _comp_card(
        ax,
        6.84,
        0.38,
        3.00,
        1.52,
        "ZONE E · FROZEN REGISTRY",
        "ML Model & Static Store",
        [
            "• Mô hình Random Forest đóng băng",
            "• Kho biến tĩnh 84,633 đoạn (chỉ đọc)",
        ],
        ZONE_E_COLOR,
        "#F0FDFA",
        header_h=0.54,
        body_fs=8.0,
    )
    _comp_card(
        ax,
        10.26,
        0.38,
        2.96,
        1.52,
        "ZONE D · LATE BACKFILL",
        "Two-Tier Reconciliation",
        [
            "• Gom bù cửa sổ trễ > 30m cuối ngày",
            "• Tính lại biến lag & re-score chuẩn v2",
        ],
        ZONE_D_COLOR,
        "#EFF6FF",
        header_h=0.54,
        body_fs=8.0,
    )

    # Column 3: MongoDB Serving Store & Streamlit Dashboard (Zone F)
    _comp_card(
        ax,
        14.04,
        2.38,
        3.76,
        1.94,
        "ZONE F · SERVING STORE",
        "MongoDB Serving Store",
        [
            "• Nhận cả cảnh báo v1 & bản chuẩn v2",
            "• Bulk Upsert kèm khóa version_ts",
            "  (chặn tin cũ v1 ghi đè bản chuẩn v2)",
            "• Compound Index IXSCAN (< 1 ms)",
        ],
        ZONE_F_COLOR,
        "#F0FDF4",
        body_fs=8.1,
    )
    _comp_card(
        ax,
        14.04,
        0.38,
        3.76,
        1.52,
        "ZONE F · LIVE OPERATIONS UI",
        "Streamlit Dashboard",
        [
            "• Cổng điều hành 5 Tab làm mới mỗi 5s",
            "• Bản đồ điểm nóng, dự báo & kiểm toán",
        ],
        ZONE_F_COLOR,
        "#F0FDF4",
        header_h=0.54,
        body_fs=8.1,
    )


def _draw_stream_connectors(ax: plt.Axes) -> None:
    # 1. Speed pipeline across Zone B -> Zone C
    _ortho_arrow(ax, [(2.82, 3.21), (3.50, 3.21)], color=PIPE_STREAM_COLOR, lw=2.1)
    _ortho_arrow(ax, [(6.42, 3.21), (6.84, 3.21)], color=PIPE_STREAM_COLOR, lw=2.1)
    _ortho_arrow(ax, [(9.84, 3.21), (10.26, 3.21)], color=PIPE_STREAM_COLOR, lw=2.1)

    # 2. Dual-Sink raw landing: Watermark & Dual-Sink -> WAL & Bronze Landing
    _ortho_arrow(ax, [(4.96, 2.40), (4.96, 1.90)], color=PIPE_STORAGE_COLOR, lw=1.9)

    # 3. Late events backfill: WAL & Bronze Landing -> Two-Tier Reconciliation (bottom channel, 0 crossings)
    _ortho_arrow(
        ax,
        [(4.96, 0.38), (4.96, 0.13), (11.74, 0.13), (11.74, 0.38)],
        color=PIPE_BATCH_COLOR,
        lw=1.9,
    )

    # 4. Read-only MLOps feeds from Frozen Registry -> Online Stream Scorer & Two-Tier Reconciliation
    _ortho_arrow(
        ax,
        [(9.84, 1.44), (10.05, 1.44), (10.05, 2.76), (10.26, 2.76)],
        color=PIPE_MLOPS_COLOR,
        lw=1.9,
        ls="--",
    )
    _ortho_arrow(ax, [(9.84, 0.92), (10.26, 0.92)], color=PIPE_MLOPS_COLOR, lw=1.9, ls="--")

    # 5. Upsert v1 (Speed) & Upsert v2 (Recon) -> MongoDB Serving Store -> Streamlit Dashboard
    _ortho_arrow(ax, [(13.22, 3.48), (14.04, 3.48)], color=PIPE_STORAGE_COLOR, lw=2.1)
    _ortho_arrow(
        ax,
        [(13.22, 1.14), (13.66, 1.14), (13.66, 2.78), (14.04, 2.78)],
        color=PIPE_STORAGE_COLOR,
        lw=2.0,
    )
    _ortho_arrow(ax, [(15.92, 2.38), (15.92, 1.90)], color=PIPE_STORAGE_COLOR, lw=2.1)


def render_stream_flow_diagram(out_path: Path) -> Path:
    """Render clean component & dataflow diagram for Section 8: Stream Processing."""
    fig, ax = plt.subplots(figsize=(18.0, 5.02), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.0)
    ax.set_ylim(0.0, 5.02)
    ax.axis("off")
    _draw_stream_boxes(ax)
    _draw_stream_connectors(ax)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


if __name__ == "__main__":
    chart_dir = Path(__file__).resolve().parent.parent / "outputs" / "slide_charts"
    b_path = render_batch_flow_diagram(chart_dir / "flow_batch_components.png")
    s_path = render_stream_flow_diagram(chart_dir / "flow_stream_components.png")
    print(f"Rendered: {b_path.name} ({b_path.stat().st_size:,} B), {s_path.name} ({s_path.stat().st_size:,} B)")
