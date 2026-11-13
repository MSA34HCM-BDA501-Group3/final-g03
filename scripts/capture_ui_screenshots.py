"""Capture zoomed, tightly cropped high-DPI screenshots of focal panels on the live Streamlit UI (:8501)."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import Page, sync_playwright

REPO_DIR = Path(__file__).resolve().parent.parent
CHART_DIR = REPO_DIR / "outputs" / "slide_charts"
CHROME_CANDIDATES = (
    Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
)


def _find_browser_exe() -> str | None:
    for cand in CHROME_CANDIDATES:
        if cand.is_file():
            return str(cand)
    return None


def _clip_between_headings(
    page: Page, start_text: str, end_text: str | None, extra_bottom: float = 210.0, pad: float = 14.0
) -> dict[str, float]:
    vp = page.viewport_size or {"width": 1120, "height": 960}
    start_box = page.get_by_text(start_text, exact=False).first.bounding_box()
    if start_box is None:
        return {"x": 24.0, "y": 180.0, "width": vp["width"] - 48.0, "height": 540.0}
    y_top = max(0.0, start_box["y"] - pad)
    if end_text:
        end_box = page.get_by_text(end_text, exact=False).first.bounding_box()
        if end_box is not None and end_box["y"] > y_top + 120:
            height = min(vp["height"] - y_top, (end_box["y"] - y_top) + extra_bottom)
        else:
            height = min(vp["height"] - y_top, 560.0)
    else:
        height = min(vp["height"] - y_top, extra_bottom)
    return {
        "x": 28.0,
        "y": y_top,
        "width": float(vp["width"] - 56.0),
        "height": max(260.0, float(height)),
    }


def capture_dashboard_screenshots(base_url: str = "http://localhost:8501") -> list[Path]:
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    exe_path = _find_browser_exe()
    saved: list[Path] = []

    with sync_playwright() as pw:
        launch_opts: dict = {"headless": True}
        if exe_path is not None:
            launch_opts["executable_path"] = exe_path
        browser = pw.chromium.launch(**launch_opts)
        context = browser.new_context(viewport={"width": 1140, "height": 1060}, device_scale_factor=2.0)
        page = context.new_page()
        page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector('[data-testid="stMetric"]', timeout=25000)
        page.wait_for_timeout(3000)

        page.add_style_tag(
            content=(
                "header[data-testid='stHeader'], [data-testid='stToolbar'] { display: none !important; } "
                ".block-container { padding-top: 0.8rem !important; padding-bottom: 0.8rem !important; "
                "padding-left: 2.0rem !important; padding-right: 2.0rem !important; max-width: 1140px !important; }"
            )
        )
        page.wait_for_timeout(600)

        # 1) Zoomed crop of Tab 1: HCMC Map + Persistent Hotspots Table (Lý Thường Kiệt #1)
        p1 = CHART_DIR / "ui_tab1_overview_hotspots.png"
        clip1 = _clip_between_headings(
            page,
            "1. Bản đồ Điểm nóng",
            "3. Thống kê Làm sạch",
            extra_bottom=-18.0,
            pad=10.0,
        )
        page.screenshot(path=str(p1), clip=clip1)
        saved.append(p1)

        tabs = page.locator('[role="tab"]')
        if tabs.count() >= 4:
            # 2) Zoomed crop of Tab 2: Forecast E -> F on Công Trường Quốc Tế + Live MongoDB 8.0 header & top rows
            tabs.nth(1).click()
            page.wait_for_timeout(2600)
            p2 = CHART_DIR / "ui_tab2_live_forecast_mongo.png"
            clip2 = _clip_between_headings(
                page,
                "1. Dự báo Trạng thái Giao thông",
                "2. Cảnh báo Trục chính",
                extra_bottom=232.0,
                pad=10.0,
            )
            page.screenshot(path=str(p2), clip=clip2)
            saved.append(p2)

            # 3) Zoomed crop of Tab 4: Peer Lookup panel for unsensored streets (Phú Định #61781)
            tabs.nth(3).click()
            page.set_viewport_size({"width": 1140, "height": 1360})
            page.wait_for_timeout(2600)
            p4 = CHART_DIR / "ui_tab4_kmeans_coldstart.png"
            clip4 = _clip_between_headings(
                page,
                "3. Tra cứu Đoạn Đường Tương Đồng",
                None,
                extra_bottom=430.0,
                pad=12.0,
            )
            page.screenshot(path=str(p4), clip=clip4)
            saved.append(p4)

        browser.close()
    return saved


if __name__ == "__main__":
    paths = capture_dashboard_screenshots()
    print("Captured zoomed UI focal screenshots:", [p.name for p in paths])
