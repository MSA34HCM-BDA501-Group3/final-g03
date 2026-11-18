"""Curated Real Empirical HCMC Rush-Hour Scenarios & Explicit 30m Window Clock Formatting."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


def format_clock_hhmm(vn_hour: float) -> str:
    """Convert numeric Vietnam hour (e.g. 11.5) to HH:MM string ('11:30')."""
    total_min = int(round(float(vn_hour) * 60)) % (24 * 60)
    return f"{total_min // 60:02d}:{total_min % 60:02d}"


def build_window_semantics(ict_date: str, vn_hour: float) -> dict[str, str]:
    """Build crystal-clear 30m window labels: [t-30m, t) -> Issue at t -> Forecast [t, t+30m)."""
    start_dt = datetime.strptime(ict_date, "%Y-%m-%d") + timedelta(minutes=int(round(float(vn_hour) * 60)))
    issue_dt = start_dt + timedelta(minutes=30)
    target_end_dt = start_dt + timedelta(minutes=60)

    start_date_str = start_dt.strftime("%Y-%m-%d")
    issue_date_str = issue_dt.strftime("%Y-%m-%d")
    target_end_date_str = target_end_dt.strftime("%Y-%m-%d")

    start_str = start_dt.strftime("%H:%M")
    issue_str = issue_dt.strftime("%H:%M")
    target_end_str = target_end_dt.strftime("%H:%M")

    issue_suffix = f" ({issue_date_str})" if issue_date_str != start_date_str else ""
    target_suffix = f" ({target_end_date_str})" if target_end_date_str != start_date_str else ""
    issue_label = f"{issue_str} ICT{issue_suffix}"
    forecast_label = f"[{issue_str} – {target_end_str}){target_suffix}"

    return {
        "ict_date": start_date_str,
        "issue_date_ict": issue_date_str,
        "slot_start_ict": start_str,
        "issue_time_ict": issue_label,
        "collect_window": f"[{start_str} – {issue_str})",
        "forecast_window": forecast_label,
        "target_eval_time_ict": f"{target_end_str} ICT{target_suffix}",
        "headline": (
            f"Ngày {start_date_str} · Thu thập GPS {start_str}–{issue_str} ➔ "
            f"Chốt phát lệnh t = {issue_label} ➔ Dự báo cho khung {forecast_label}"
        ),
    }


CURATED_SCENARIOS: list[dict[str, Any]] = [
    {
        "id": "noon_transition",
        "title": "Kịch bản 1 (TEST): Cao điểm Trưa Quận 1–3 — Nguyễn Bỉnh Khiêm & Trương Định (E ➔ F thực tế)",
        "short_label": "Kịch bản 1 · Nguyễn Bỉnh Khiêm & Trương Định (E ➔ F)",
        "split_badge": "TEST",
        "ict_date": "2021-01-05",
        "default_vn_hour": 12.0,
        "hour_min": 10.5,
        "hour_max": 15.0,
        "spotlight_segment_id": 21262,
        "description": (
            "Chuỗi 10 khung 30 phút liên tiếp (10:30–15:30 ICT). Tại khung thu thập [12:00–12:30) chốt lúc "
            "t = 12:30 ICT, cả 2 trục Nguyễn Bỉnh Khiêm (#21262, 8 pings) và Trương Định (#21263, 12 pings) "
            "đang ở mức E và thực tế đều chuyển sang kẹt cứng LOS F ở [12:30–13:00); Random Forest 23D phát hiện "
            "xác suất đổi pha cao (so với Persistence dự báo giữ nguyên mức E)."
        ),
    },
    {
        "id": "cong_truong_quoc_te",
        "title": "Kịch bản 2 (TEST): Vòng xoay Hồ Con Rùa — Công Trường Quốc Tế (#21064) sập tốc độ 23 ➔ 3 km/h",
        "short_label": "Kịch bản 2 · Công Trường Quốc Tế #21064 (E ➔ F)",
        "split_badge": "TEST · Case Study",
        "ict_date": "2021-04-22",
        "default_vn_hour": 11.5,
        "hour_min": 8.5,
        "hour_max": 12.0,
        "spotlight_segment_id": 21064,
        "description": (
            "Case Study trung tâm trên Báo cáo & Slide 14: 4 bản tin GPS thô trong khung [11:30–12:00) giảm gắt "
            "từ 23 km/h (11:34) xuống 9 km/h và 3 km/h (11:50). Chốt lúc t = 12:00 ICT (đang LOS E), Random Forest "
            "23D phát cảnh báo kẹt nghiêm trọng P(E/F) = 68.7% (vượt ngưỡng VAL 25%) và xác suất đổi pha 48.9% "
            "(vượt ngưỡng VAL 40%), báo động sớm trước khi đoạn chuyển sang kẹt cứng LOS F ở [12:00–12:30)."
        ),
    },
    {
        "id": "morning_rush_val",
        "title": "Kịch bản 3 (VAL): Cao điểm Sáng Cửa ngõ Tân Sơn Nhất — Cộng Hòa, Hoàng Văn Thụ & Lý Thường Kiệt",
        "short_label": "Kịch bản 3 · Cao điểm Sáng Cộng Hòa & Tân Bình",
        "split_badge": "VAL · 192 đoạn",
        "ict_date": "2020-12-24",
        "default_vn_hour": 7.0,
        "hour_min": 6.5,
        "hour_max": 10.5,
        "spotlight_segment_id": 37420,
        "description": (
            "Bức tranh toàn đô thị sáng 24/12/2020 (192 đoạn đường hoạt động cùng lúc chốt lúc 07:30 ICT): trục "
            "Cộng Hòa (#37420) và Hoàng Văn Thụ (#36232) chuyển thực tế từ E sang F, trong khi Lý Thường Kiệt "
            "(#63481) chuyển từ B sang D ở khung [07:30–08:00)."
        ),
    },
    {
        "id": "mega_city_rush",
        "title": "Kịch bản 4 (Quy mô Lớn): Toàn Mạng Lưới 3,596 Đoạn Đường Đồng Loạt Chuyển Pha Sáng Sớm",
        "short_label": "Kịch bản 4 · Toàn TP.HCM Đồng Loạt Chuyển Pha Sáng (3,596 đoạn)",
        "split_badge": "TRAIN · Đợt Burst Tổng Hợp",
        "ict_date": "2020-08-03",
        "default_vn_hour": 6.5,
        "hour_min": 6.5,
        "hour_max": 7.0,
        "spotlight_segment_id": 47407,
        "description": (
            "Khung giờ có độ phủ cảm biến dày nhất bộ dữ liệu (3,596 đoạn đường cơ giới hoạt động đồng thời "
            "tại [06:30–07:00) và [07:00–07:30) ICT), minh họa làn sóng phương tiện giờ cao điểm sáng lan khắp "
            "TP.HCM cùng điểm nóng số 1 Lý Thường Kiệt (#47407)."
        ),
    },
]
