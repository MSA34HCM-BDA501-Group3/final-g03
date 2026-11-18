"""Capture the real dashboard and check the selected replay run in browser text."""

import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    root = Path("evidence/session9")
    smoke = json.loads((root / "raw_smoke.json").read_text())
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1100})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto("http://session9-ui:8501", wait_until="domcontentloaded")
        page.get_by_role("tab").nth(1).wait_for(timeout=60000)
        page.get_by_role("tab").nth(1).click()
        page.get_by_text("Run: " + smoke["run_id"], exact=False).wait_for(timeout=30000)
        page.get_by_text("Freshness của score mới nhất", exact=False).scroll_into_view_if_needed()
        page.screenshot(path=str(root / "F_ui.png"))
        text = page.locator("body").inner_text()
        assert smoke["run_id"] in text and "Freshness của score mới nhất" in text
        assert "Traceback" not in text and not errors, (errors, text)
        (root / "F_ui_check.json").write_text(
            json.dumps(
                dict(run_id=smoke["run_id"], browser_errors=errors, body_text=text, url="http://session9-ui:8501"),
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        browser.close()


if __name__ == "__main__":
    main()
