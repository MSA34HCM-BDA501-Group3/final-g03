"""Capture zoomed high-DPI focal screenshots directly from the ReactJS + FastAPI TOC UI (:8000)."""

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


def _launch_browser(pw):
    launch_opts: dict = {"headless": True}
    exe_path = _find_browser_exe()
    if exe_path is not None:
        launch_opts["executable_path"] = exe_path
    return pw.chromium.launch(**launch_opts)


def _capture_clip(page: Page, out_path: Path, y_top: float, height: float) -> Path:
    vp = page.viewport_size or {"width": 1440, "height": 960}
    clip = {"x": 0.0, "y": y_top, "width": float(vp["width"]), "height": min(float(vp["height"]) - y_top, height)}
    page.screenshot(path=str(out_path), clip=clip)
    return out_path


def capture_dashboard_screenshots(base_url: str = "http://localhost:8000") -> list[Path]:
    """Capture 3 high-DPI ReactJS TOC screenshots embedded in the Final Report PDF and 17-slide deck."""
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []

    with sync_playwright() as pw:
        browser = _launch_browser(pw)
        context = browser.new_context(viewport={"width": 1440, "height": 960}, device_scale_factor=2.0)
        page = context.new_page()
        page.goto(base_url, wait_until="networkidle", timeout=35000)
        page.wait_for_selector(".nav-tab", timeout=20000)
        page.wait_for_timeout(1800)

        # 1) Tab 01 Command Center: Top Clock Bar + KPI Cards + Interactive Map + Forecast Timeline
        saved.append(_capture_clip(page, CHART_DIR / "ui_tab1_overview_hotspots.png", 0.0, 900.0))

        # 2) Tab 01 Live Forecast (Scenario 2 Công Trường Quốc Tế) + MongoDB 8.0 IXSCAN panel
        scenario_select = page.locator("select").first
        if scenario_select.count() > 0:
            scenario_select.select_option("cong_truong_quoc_te")
            page.wait_for_timeout(1400)
        mongo_btn = page.get_by_role("button", name="Mongo IXSCAN")
        if mongo_btn.count() > 0:
            mongo_btn.first.click()
            page.wait_for_timeout(1200)
        saved.append(_capture_clip(page, CHART_DIR / "ui_tab2_live_forecast_mongo.png", 60.0, 880.0))

        # 3) Tab 04: 6 K-Means Archetypes & Cold-Start Peer Lookup
        tabs = page.locator(".nav-tab")
        if tabs.count() >= 4:
            tabs.nth(3).click()
            page.wait_for_timeout(1600)
            saved.append(_capture_clip(page, CHART_DIR / "ui_tab4_kmeans_coldstart.png", 60.0, 880.0))

        browser.close()
    return saved


if __name__ == "__main__":
    paths = capture_dashboard_screenshots()
    print("Captured ReactJS TOC UI screenshots:", [p.name for p in paths])
