"""Capture crisp 16:9 high-DPI screenshots from the ReactJS + FastAPI TOC UI (:8000)."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import Page, sync_playwright

REPO_DIR = Path(__file__).resolve().parent.parent
CHART_DIR = REPO_DIR / "outputs" / "slide_charts"
VIEWPORT = {"width": 1360, "height": 765}
CHROME_CANDIDATES = (
    Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
)

COMPACT_CLOCK_CSS = """
.frame-scrubber { display: none !important; }
.toc-header { padding: 7px 20px !important; }
.clock-bar { margin-top: 0 !important; padding: 6px 12px !important; flex-wrap: nowrap !important; }
.select-dark { max-width: 360px !important; font-size: 11.5px !important; }
.window-pill-strip { font-size: 11.3px !important; padding: 5px 10px !important; white-space: nowrap !important; }
"""

COMPACT_TAB1_HERO_CSS = """
.frame-scrubber { display: none !important; }
.toc-header { padding: 6px 20px !important; }
.clock-bar { margin-top: 0 !important; padding: 6px 12px !important; flex-wrap: nowrap !important; }
.select-dark { max-width: 360px !important; font-size: 11.5px !important; }
.window-pill-strip { font-size: 11.3px !important; padding: 5px 10px !important; white-space: nowrap !important; }
.toc-main { padding: 10px 20px 14px !important; gap: 10px !important; }
.kpi-card { padding: 8px 12px !important; }
.kpi-val { font-size: 19px !important; margin-top: 2px !important; }
.card { padding: 10px 14px !important; }
.card-header { margin-bottom: 6px !important; }
.leaflet-box { height: 290px !important; }
.table-scroll { max-height: 290px !important; }
table.toc-table td { padding: 6px 8px !important; }
"""


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


def _set_capture_css(page: Page, css_text: str) -> None:
    page.evaluate(
        """(css) => {
            let tag = document.getElementById('toc-capture-style');
            if (!tag) {
                tag = document.createElement('style');
                tag.id = 'toc-capture-style';
                document.head.appendChild(tag);
            }
            tag.textContent = css;
            window.dispatchEvent(new Event('resize'));
        }""",
        css_text,
    )


def _clip_between(
    page: Page,
    out_path: Path,
    start_sel: str,
    end_sel: str,
    pad_top: float = 0.0,
    pad_bottom: float = 12.0,
) -> Path:
    vp = page.viewport_size or VIEWPORT
    start_box = page.locator(start_sel).first.bounding_box()
    end_box = page.locator(end_sel).last.bounding_box()
    if not start_box or not end_box:
        page.screenshot(path=str(out_path))
        return out_path
    y0 = max(0.0, float(start_box["y"]) - pad_top)
    y1 = float(end_box["y"]) + float(end_box["height"]) + pad_bottom
    clip = {"x": 0.0, "y": y0, "width": float(vp["width"]), "height": max(200.0, y1 - y0)}
    page.screenshot(path=str(out_path), clip=clip, full_page=True)
    return out_path


def _capture_tab2_scenario2_mongo(page: Page) -> Path:
    """Capture Tab 01 Scenario 2 (#21064 Công Trường Quốc Tế) + MongoDB 8.0 IXSCAN panel."""
    _set_capture_css(page, COMPACT_CLOCK_CSS)
    scenario_select = page.locator("select").first
    if scenario_select.count() > 0:
        scenario_select.select_option("cong_truong_quoc_te")
        page.wait_for_timeout(1600)
    mongo_btn = page.get_by_role("button", name="Mongo IXSCAN")
    if mongo_btn.count() > 0:
        mongo_btn.first.click()
        page.wait_for_timeout(1000)
    return _clip_between(
        page,
        CHART_DIR / "ui_tab2_live_forecast_mongo.png",
        ".clock-bar",
        ".grid-hero",
        pad_top=8.0,
        pad_bottom=12.0,
    )


def _capture_tab4_kmeans_coldstart(page: Page) -> Path:
    """Capture Tab 04 (6 K-Means Archetypes & Cold-Start) cleanly below the scenario clock bar."""
    _set_capture_css(page, "")
    tabs = page.locator(".nav-tab")
    if tabs.count() >= 4:
        tabs.nth(3).click()
        page.wait_for_timeout(1500)
    return _clip_between(
        page,
        CHART_DIR / "ui_tab4_kmeans_coldstart.png",
        ".nav-tabs",
        ".grid-2",
        pad_top=0.0,
        pad_bottom=14.0,
    )


def _capture_tab1_scenario3_dien_bien_phu(page: Page) -> Path:
    """Capture Tab 01 Scenario 3 (#50026 Điện Biên Phủ, 122 active, 38 alerts, map + timeline)."""
    tabs = page.locator(".nav-tab")
    if tabs.count() >= 1:
        tabs.nth(0).click()
        page.wait_for_timeout(600)
    _set_capture_css(page, COMPACT_TAB1_HERO_CSS)
    page.wait_for_timeout(300)
    scenario_select = page.locator("select").first
    if scenario_select.count() > 0:
        scenario_select.select_option("morning_rush_dien_bien_phu")
        page.wait_for_timeout(1800)
    return _clip_between(
        page,
        CHART_DIR / "ui_tab1_overview_hotspots.png",
        ".clock-bar",
        ".toc-main > div > .card",
        pad_top=6.0,
        pad_bottom=10.0,
    )


def capture_dashboard_screenshots(base_url: str = "http://localhost:8000") -> list[Path]:
    """Capture 3 crisp high-DPI ReactJS TOC screenshots for slides and the final report."""
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        browser = _launch_browser(pw)
        context = browser.new_context(viewport=VIEWPORT, device_scale_factor=2.0)
        page = context.new_page()
        page.goto(base_url, wait_until="networkidle", timeout=35000)
        page.wait_for_selector(".nav-tab", timeout=20000)
        page.wait_for_timeout(1200)

        p2 = _capture_tab2_scenario2_mongo(page)
        p4 = _capture_tab4_kmeans_coldstart(page)
        p1 = _capture_tab1_scenario3_dien_bien_phu(page)
        browser.close()
    return [p1, p2, p4]


if __name__ == "__main__":
    paths = capture_dashboard_screenshots()
    print("Captured ReactJS TOC UI screenshots:", [p.name for p in paths])
