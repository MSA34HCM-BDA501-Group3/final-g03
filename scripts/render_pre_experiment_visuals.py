"""Render pre-experiment (design & methodology milestone) visual diagrams without empirical/runtime results."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, FancyBboxPatch, Rectangle

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from render_arch_diagram import PIPE_BATCH_COLOR, draw_ortho_arrow  # noqa: E402
from render_component_flows import (  # noqa: E402
    PIPE_INGEST_COLOR,
    PIPE_MLOPS_COLOR,
    PIPE_STORAGE_COLOR,
    PIPE_STREAM_COLOR,
    ZONE_A_COLOR,
    ZONE_B_COLOR,
    ZONE_C_COLOR,
    ZONE_D_COLOR,
    ZONE_E_COLOR,
    ZONE_F_COLOR,
    _box,
    _comp_card,
    _draw_legend,
    _draw_stream_connectors,
    _ortho_arrow,
)
from render_method_explainers import _arrow, _panel_header, _rbox  # noqa: E402

REPO_DIR = SCRIPT_DIR.parent
CHART_DIR = REPO_DIR / "outputs" / "slide_charts"


def _draw_pre_imputation_panel(ax: plt.Axes) -> None:
    _panel_header(
        ax,
        0.18,
        0.15,
        8.65,
        5.10,
        "1. Thiết Kế Nội Suy Tốc Độ 3 Tầng (Train-Only 3-Tier Vmax)",
        "Giải EC-1 (85.21% thiếu Vmax)",
        "#0284C7",
    )
    _rbox(ax, 0.42, 4.10, 8.17, 0.44, "#E0F2FE", "#0284C7", lw=1.2, radius=0.09, zorder=2)
    ax.text(
        4.50,
        4.32,
        "NGUYÊN TẮC CHỐNG RÒ RỈ (TRAIN-ONLY): Chỉ học bảng tra cứu từ quá khứ (TRAIN), áp cố định cho VAL & TEST",
        fontsize=8.3,
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
            "Ưu tiên 0 — Giá trị gốc hợp lệ trên bản đồ đường",
            "Đường có sẵn biển tốc độ hợp lệ (> 0 km/h) → Giữ nguyên vận tốc thiết kế (vd: đại lộ 60 km/h)",
        ),
        (
            2.58,
            7.65,
            "#E0F2FE",
            "#0284C7",
            "Tầng 1 — Phân vị P85 Lịch sử của Chính Đoạn Đường (TRAIN)",
            "Đoạn có đủ mẫu lịch sử → Lấy phân vị 85% vận tốc lúc đường thoáng trên tập TRAIN",
        ),
        (
            1.82,
            7.13,
            "#FEF3C7",
            "#D97706",
            "Tầng 2 — Trung vị theo Cấp Đường × Loại Đường (TRAIN)",
            "Đoạn ít lịch sử → Mượn vận tốc trung vị của nhóm đường cùng cấp (street_level × street_type)",
        ),
        (
            1.06,
            6.61,
            "#F3E8FF",
            "#7C3AED",
            "Tầng 3 — Quy chuẩn Tốc độ Đô thị Dự phòng",
            "Dự phòng cuối theo cấp đường 1..4: gán ngưỡng chuẩn đô thị 80 / 60 / 50 / 40 km/h",
        ),
    ]
    for idx, (ty, tw, fc, ec, head, desc) in enumerate(tiers):
        tx = 0.42 + (8.17 - tw) / 2
        _rbox(ax, tx, ty, tw, 0.60, fc, ec, lw=1.4, radius=0.09, zorder=3)
        ax.text(tx + 0.16, ty + 0.41, head, fontsize=8.5, fontweight="bold", color="#0F172A", va="center", zorder=4)
        ax.text(tx + 0.16, ty + 0.16, desc, fontsize=7.9, color="#334155", va="center", zorder=4)
        if idx < len(tiers) - 1:
            _arrow(ax, 4.50, ty - 0.01, 4.50, ty - 0.16, color="#64748B", lw=1.6)

    _rbox(ax, 0.42, 0.30, 8.17, 0.56, "#0F172A", "#0F172A", lw=1.2, radius=0.10, zorder=3)
    ax.text(
        4.50,
        0.58,
        "MỤC TIÊU THIẾT KẾ: 100% cửa sổ có Vmax hợp lệ → Tính r = vận tốc thực / Vmax → Chuẩn hóa 6 mức LOS A–F",
        fontsize=8.3,
        fontweight="bold",
        color="#38BDF8",
        ha="center",
        va="center",
        zorder=4,
    )


def _draw_pre_monoid_panel(ax: plt.Axes) -> None:
    _panel_header(
        ax,
        9.12,
        0.15,
        8.80,
        5.10,
        "2. Bộ Gộp 5-Tuple Monoid & Chia Giỏ Key Salting",
        "Bảo toàn Toán học",
        "#0F172A",
    )
    _rbox(ax, 9.34, 4.10, 8.36, 0.44, "#F1F5F9", "#64748B", lw=1.2, radius=0.09, zorder=2)
    ax.text(
        13.52,
        4.32,
        "VÍ DỤ GIẢ ĐỊNH: 4 xe trên cùng đoạn đường nằm ở 2 máy — "
        "Máy A: [10 km/h] · Máy B: [40, 40, 40 km/h] (Chuẩn = 32.5)",
        fontsize=8.1,
        fontweight="bold",
        color="#0F172A",
        ha="center",
        va="center",
        zorder=3,
    )

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
        "Lấy trung bình của trung bình:\n(10.0 + 40.0) / 2 = 25.0 km/h\n→ LỆCH 7.5 km/h so với chuẩn 32.5!",
        fontsize=8.0,
        fontweight="bold",
        color="#991B1B",
        ha="center",
        va="center",
        zorder=4,
    )

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
        "→ BẢO TOÀN CHÍNH XÁC DÙ CHIA N GIỎ!",
        fontsize=8.0,
        fontweight="bold",
        color="#065F46",
        ha="center",
        va="center",
        zorder=4,
    )

    _rbox(ax, 9.34, 0.30, 8.36, 0.64, "#0F172A", "#0F172A", lw=1.2, radius=0.10, zorder=3)
    ax.text(
        13.52,
        0.62,
        "THIẾT KẾ 2-STAGE KEY SALTING: Gắn đuôi ngẫu nhiên 0..7 tán đều bản tin giờ cao điểm ra 8 giỏ song song,\n"
        "sau đó cộng gộp 5-Tuple Monoid để vừa chống nghẽn phân vùng nóng vừa bảo toàn kết quả toán học!",
        fontsize=7.9,
        fontweight="bold",
        color="#E2E8F0",
        ha="center",
        va="center",
        zorder=4,
    )


def render_pre_exp_imputation_monoid(out_path: Path) -> Path:
    """Render Pre-Experiment Visual Explainer Slide 1 (no empirical post-run counts)."""
    fig, ax = plt.subplots(figsize=(18.1, 5.40), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.1)
    ax.set_ylim(0.0, 5.40)
    ax.axis("off")
    _draw_pre_imputation_panel(ax)
    _draw_pre_monoid_panel(ax)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _draw_pre_pagerank_col(ax: plt.Axes) -> None:
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
    _rbox(ax, 0.34, 3.38, 5.44, 1.12, "white", "#CBD5E1", lw=1.2, radius=0.10, zorder=2)
    ax.text(
        0.50,
        4.28,
        "Bước A — Cắt tỉa 90.97% điểm uốn cong bản đồ OSM:",
        fontsize=8.3,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
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
        "Đoạn Cổ Chai D\nĐiểm hội tụ cao\n(Dòng xe dồn tụ!)",
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
        "Ý NGHĨA: Mỗi đoạn đường là 1 nút. Nhiều đường lớn đổ vào 1 lối hẹp\n"
        "→ Chỉ số PageRank tăng vọt ngay cả khi đoạn đó chưa lắp GPS!",
        fontsize=7.8,
        fontweight="bold",
        color="#5EEAD4",
        ha="center",
        va="center",
        zorder=4,
    )


def _draw_pre_coldstart_col(ax: plt.Axes) -> None:
    _panel_header(
        ax,
        6.14,
        0.15,
        5.82,
        5.10,
        "4. Phân Cụm 7D K-Means Cho Vùng Mù",
        "88.13% Cold-Start",
        "#2563EB",
    )
    _rbox(ax, 6.32, 3.38, 5.46, 1.12, "white", "#CBD5E1", lw=1.2, radius=0.10, zorder=2)
    ax.text(
        6.48,
        4.28,
        "Thách thức EDA: 74,211 / 84,206 đoạn cơ giới có 0 bản tin GPS",
        fontsize=8.2,
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
        "Trích 7 đặc trưng bản đồ\n(PageRank, bậc, cấp, Vmax...)",
        fontsize=7.7,
        fontweight="bold",
        color="#1E3A8A",
        ha="center",
        va="center",
    )

    _rbox(ax, 6.32, 1.06, 5.46, 2.16, "#EFF6FF", "#2563EB", lw=1.3, radius=0.10, zorder=2)
    ax.text(
        6.48,
        3.00,
        "Gom 84,206 đoạn cơ giới thành k nhóm hành vi đường phố:",
        fontsize=8.2,
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
    clusters = [
        (7.42, 1.96, "#FEE2E2", "#DC2626", "Nhóm A: Ngã tư\nnội đô hay kẹt"),
        (9.12, 1.96, "#DBEAFE", "#2563EB", "Nhóm B: Đại lộ\ntốc độ cao"),
        (10.80, 1.96, "#F1F5F9", "#64748B", "Nhóm C: Đường\nnhánh dân cư"),
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
        "→ Thiết kế phủ kín dự báo cho 100% mạng lưới (84,206 đoạn)!",
        fontsize=7.8,
        fontweight="bold",
        color="#93C5FD",
        ha="center",
        va="center",
        zorder=4,
    )


def _draw_pre_sketches_col(ax: plt.Axes) -> None:
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
    _rbox(ax, 12.32, 3.42, 5.44, 1.08, "#FAF5FF", "#7C3AED", lw=1.3, radius=0.10, zorder=2)
    ax.text(
        12.48,
        4.28,
        "Bloom Filter (32.0 KiB · 262,144 bits, k = 4) — Lọc tin trùng:",
        fontsize=8.1,
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
        "Cả 4 ô đều = 1\n→ CHẶN TIN TRÙNG!\n(Cố định 32 KiB)",
        fontsize=7.6,
        fontweight="bold",
        color="#5B21B6",
        ha="left",
        va="center",
    )

    _rbox(ax, 12.32, 2.24, 5.44, 1.04, "white", "#CBD5E1", lw=1.2, radius=0.10, zorder=2)
    ax.text(
        12.48,
        3.04,
        "HyperLogLog++ (1.0 KiB · p = 10 → 1,024 thanh ghi):",
        fontsize=8.1,
        fontweight="bold",
        color="#0F172A",
        va="center",
        zorder=3,
    )
    ax.text(
        12.48,
        2.60,
        "Quan sát chuỗi số 0 đầu mã băm (0001... hiếm = nhiều phần tử)\n"
        "→ Đếm số đường hoạt động trong 1 KiB RAM (sai số lý thuyết ≈ 3.25%).",
        fontsize=7.6,
        color="#334155",
        va="center",
        zorder=3,
    )

    _rbox(ax, 12.32, 1.06, 5.44, 1.04, "#FEF3C7", "#D97706", lw=1.3, radius=0.10, zorder=2)
    ax.text(
        12.48,
        1.86,
        "EWMA Hotspot (α = 0.15) bổ trợ 2 chiều cùng Random Forest:",
        fontsize=8.1,
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


def render_pre_exp_graph_sketches(out_path: Path) -> Path:
    """Render Pre-Experiment Visual Explainer Slide 2 (no empirical post-run counts)."""
    fig, ax = plt.subplots(figsize=(18.1, 5.40), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.1)
    ax.set_ylim(0.0, 5.40)
    ax.axis("off")
    _draw_pre_pagerank_col(ax)
    _draw_pre_coldstart_col(ax)
    _draw_pre_sketches_col(ax)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def render_pre_exp_pipeline_walkthrough(out_path: Path) -> Path:
    """Render Pre-Experiment End-to-End Design Walkthrough (Slide 08) without model/runtime results."""
    fig, ax = plt.subplots(figsize=(18.5, 5.15), dpi=240)
    ax.set_xlim(0, 18.5)
    ax.set_ylim(0, 5.15)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.text(
        9.25,
        4.86,
        "END-TO-END DESIGN WALKTHROUGH: LUỒNG XỬ LÝ 1 BẢN TIN GPS QUA 4 CHẶNG THIẾT KẾ (P1 → P4)",
        ha="center",
        va="center",
        fontsize=13.0,
        fontweight="bold",
        color="#0F172A",
    )
    ax.text(
        9.25,
        4.52,
        "Minh họa kịch bản bản tin gốc lúc 07:08 trên đường Lý Thường Kiệt (segment_id = 47407, velocity = 4 km/h) "
        "đi qua 4 chặng xử lý thiết kế",
        ha="center",
        va="center",
        fontsize=9.6,
        color="#475569",
    )

    steps = [
        (
            0.22,
            "RAW INPUT (ZONE A)",
            "Bản tin GPS & Bản đồ GIS",
            "#1E293B",
            "#F8FAFC",
            [
                "• Lúc 07:08 sáng, xe qua đoạn\n  Lý Thường Kiệt #47407 gửi:\n  velocity = 4 km/h.",
                "• Edge Case EC-1: Cột max_velocity\n  bị khuyết 85.21% trên bảng gốc.",
                "• Edge Case EC-6: 88.13% đoạn\n  cơ giới bị Cold-Start (0 GPS).",
            ],
        ),
        (
            3.92,
            "P1 · QUALITY & MONOID",
            "Chuẩn hóa & Gán nhãn LOS",
            "#0284C7",
            "#F0F9FF",
            [
                "• ANSI try_cast + NFC + UTC+7;\n  loại 427 POI; 3-Tier Vmax\n  khóa trên tập TRAIN.",
                "• 5-Tuple Monoid Combiner gộp\n  khung 30 phút (07:00–07:30).",
                "• Chuẩn hóa tỷ lệ r = v / Vmax\n  sang 6 mức trạng thái LOS A–F.",
            ],
        ),
        (
            7.62,
            "P2 · GRAPH & K-MEANS",
            "Khai phá Đồ thị & Hành vi",
            "#0D9488",
            "#F0FDFA",
            [
                "• Cắt tỉa 90.97% điểm uốn cong;\n  tính Line-Graph PageRank trên\n  84,206 đoạn (loại U-turn).",
                "• SON / FPGrowth tìm luật đồng kẹt;\n  7D K-Means trên Organic TRAIN.",
                "• Khóa Static Feature Store cấp\n  cho 74,211 đoạn Cold-Start.",
            ],
        ),
        (
            11.32,
            "P3 · CAUSAL ML (+30M)",
            "Dự báo Nhân quả t+30m",
            "#15803D",
            "#ECFDF5",
            [
                "• Ghép 14 biến cục bộ + 9 biến\n  PageRank, K-Means & Priors\n  thành vector 23D Features.",
                "• Mô hình Random Forest (tách\n  synthetic burst khỏi TRAIN)\n  dự báo LOS ở khung t + 30 phút.",
                "• Chấm điểm phủ 100% 84,206 đoạn.",
            ],
        ),
        (
            15.02,
            "P4 · STREAM & SERVING",
            "O(1) Sketches & Serving",
            "#7C3AED",
            "#FAF5FF",
            [
                "• 32 KiB Bloom + 1 KiB HLL++\n  + EWMA Hotspot bắt điểm kẹt lì.",
                "• Idempotent Upsert vào MongoDB\n  kèm guard $status_version &\n  Compound Index IXSCAN.",
                "• Two-Tier Recon & Dual UI\n  (:8000 TOC Web & :8501 Streamlit).",
            ],
        ),
    ]

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
            x + w / 2, y + h - 0.24, badge, ha="center", va="center", fontsize=9.4, fontweight="bold", color="#E0F2FE"
        )
        ax.text(
            x + w / 2, y + h - 0.56, title, ha="center", va="center", fontsize=10.1, fontweight="bold", color="white"
        )
        for b_idx, b_txt in enumerate(bullets):
            ax.text(
                x + 0.14,
                y + h - 0.98 - b_idx * 1.03,
                b_txt,
                ha="left",
                va="top",
                fontsize=8.9,
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
    return out_path


def _draw_pre_batch_boxes(ax: plt.Axes) -> None:
    ax.text(
        0.20,
        4.76,
        "THIẾT KẾ LUỒNG DỮ LIỆU & THÀNH PHẦN XỬ LÝ LÔ (BATCH PROCESSING FLOW · ZONES A → B → D → F / E)",
        fontsize=10.4,
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

    _box(ax, 3.55, 0.22, 10.00, 4.22, "#F8FAFC", ZONE_D_COLOR, lw=1.7, ls="--", radius=0.10, zorder=1)
    ax.text(
        8.55,
        4.24,
        "ZONE D · APACHE SPARK BATCH LAYER (THIẾT KẾ XỬ LÝ LÔ PHÂN TÁN & KHAI PHÁ ĐỒ THỊ)",
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
            "• Ghim bảng danh mục đường vào bộ nhớ",
            "• Tra cứu Vmax 3 tầng khóa trên tập TRAIN",
            "• Loại bỏ phép trộn mạng SortMergeJoin lớn",
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
            "• Cân bằng phân vùng nóng & bảo toàn toán học",
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
            "• Cắt tỉa 90.97% điểm uốn cong trên bản đồ",
            "• Đảo mỗi đoạn thành 1 đỉnh (loại U-turn)",
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
            "• Phân cụm 7D K-Means học trên Organic TRAIN",
            "• Suy diễn nhóm hành vi cho vùng mù Cold-Start",
        ],
        ZONE_D_COLOR,
        "#EFF6FF",
        header_h=0.46,
    )
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
            "• Hỗ trợ Partition Pruning khi đọc lịch sử",
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
            "• Phủ kín 100% mạng lưới (84,206 đoạn)",
            "• Cấp biến tĩnh cho Modeling & Luồng",
        ],
        ZONE_E_COLOR,
        "#F0FDFA",
    )


def render_pre_exp_batch_flow(out_path: Path) -> Path:
    """Render Pre-Experiment Batch Processing component flow diagram."""
    fig, ax = plt.subplots(figsize=(18.0, 5.02), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.0)
    ax.set_ylim(0.0, 5.02)
    ax.axis("off")
    _draw_pre_batch_boxes(ax)
    _ortho_arrow(ax, [(1.575, 2.56), (1.575, 2.14)], color=PIPE_INGEST_COLOR, lw=2.1)
    _ortho_arrow(ax, [(2.95, 1.20), (3.26, 1.20), (3.26, 3.21), (3.80, 3.21)], color=PIPE_BATCH_COLOR, lw=2.1)
    _ortho_arrow(ax, [(3.26, 1.20), (3.80, 1.20)], color=PIPE_BATCH_COLOR, lw=2.1)
    _ortho_arrow(ax, [(8.18, 3.21), (8.76, 3.21)], color=PIPE_BATCH_COLOR, lw=2.1)
    _ortho_arrow(ax, [(8.18, 1.19), (8.76, 1.19)], color=PIPE_BATCH_COLOR, lw=2.1)
    _ortho_arrow(ax, [(11.03, 2.44), (11.03, 1.96)], color=PIPE_BATCH_COLOR, lw=2.0)
    _ortho_arrow(ax, [(13.30, 3.21), (14.12, 3.21)], color=PIPE_STORAGE_COLOR, lw=2.2)
    _ortho_arrow(ax, [(13.30, 1.19), (14.12, 1.19)], color=PIPE_MLOPS_COLOR, lw=2.2)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _draw_pre_stream_boxes(ax: plt.Axes) -> None:
    ax.text(
        0.20,
        4.76,
        "THIẾT KẾ LUỒNG THỜI GIAN THỰC & ĐỐI SOÁT TRỄ (STREAM PROCESSING FLOW · ZONES B → C / E / D → F)",
        fontsize=10.2,
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
    _box(ax, 3.30, 2.26, 10.12, 2.18, "#FAF5FF", ZONE_C_COLOR, lw=1.7, ls="--", radius=0.10, zorder=1)
    ax.text(
        8.36,
        4.25,
        "ZONE C · SPEED LAYER — LUỒNG CẢNH BÁO TRỰC TUYẾN (WATERMARK 30 PHÚT · NGÂN SÁCH 33.0 KiB)",
        fontsize=8.4,
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
            "• Kho biến tĩnh 84,206 đoạn (chỉ đọc)",
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
            "• Bulk Upsert + guard $status_version",
            "  (chặn tin cũ v1 ghi đè bản chuẩn v2)",
            "• Compound Index IXSCAN (SLA < 10 ms)",
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
        "ZONE F · DUAL LIVE UI",
        "TOC Web & Streamlit",
        [
            "• FastAPI :8000 + ReactJS + Leaflet TOC",
            "• Streamlit :8501 (5-Tab kiểm toán)",
        ],
        ZONE_F_COLOR,
        "#F0FDF4",
        header_h=0.54,
        body_fs=8.1,
    )


def render_pre_exp_stream_flow(out_path: Path) -> Path:
    """Render Pre-Experiment Stream Processing component flow diagram."""
    fig, ax = plt.subplots(figsize=(18.0, 5.02), dpi=240, facecolor="white")
    ax.set_xlim(0.0, 18.0)
    ax.set_ylim(0.0, 5.02)
    ax.axis("off")
    _draw_pre_stream_boxes(ax)
    _draw_stream_connectors(ax)
    plt.tight_layout(pad=0.04)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


if __name__ == "__main__":
    p1 = render_pre_exp_imputation_monoid(CHART_DIR / "pre_exp_methods_imputation_monoid.png")
    p2 = render_pre_exp_graph_sketches(CHART_DIR / "pre_exp_methods_graph_coldstart_sketches.png")
    p3 = render_pre_exp_pipeline_walkthrough(CHART_DIR / "pre_exp_concrete_example_flow.png")
    p4 = render_pre_exp_batch_flow(CHART_DIR / "pre_exp_flow_batch_components.png")
    p5 = render_pre_exp_stream_flow(CHART_DIR / "pre_exp_flow_stream_components.png")
    print("Rendered pre-experiment visuals:", [p.name for p in (p1, p2, p3, p4, p5)])
