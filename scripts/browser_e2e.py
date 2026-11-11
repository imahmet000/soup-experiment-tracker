"""Browser + API check for live updates and catch-up after a network drop.

Install optional browser tooling, start the app, then run:
  pip install -r requirements-e2e.txt
  python -m playwright install chromium
  python scripts/browser_e2e.py
"""

import os
import time
from uuid import uuid4

from playwright.sync_api import sync_playwright


BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


def wait_for_event_count(page, expected: int) -> None:
    page.wait_for_function(
        "expected => Number(document.querySelector('#event-count').textContent) >= expected",
        arg=expected,
        timeout=15_000,
    )


def main() -> None:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(BASE_URL, wait_until="networkidle")

        page.locator("#project-name").fill(f"browser-e2e-{uuid4().hex[:8]}")
        page.locator("#project-form button[type=submit]").click()
        page.locator("#new-run").wait_for(state="visible")
        page.locator("#new-run").click()
        page.locator("#run-name").fill("live-and-reconnect")
        page.locator("#run-form button[type=submit]").click()
        page.locator("#send-demo").wait_for(state="visible")

        page.locator("#send-demo").click()
        wait_for_event_count(page, 1)
        api_key = page.evaluate("localStorage.getItem('soup_api_key')")
        api = playwright.request.new_context(base_url=BASE_URL, extra_http_headers={"X-API-Key": api_key})
        runs = api.get("/api/runs").json()["runs"]
        run_id = runs[0]["id"]

        # Disconnect only the browser. The independent API client continues sending metrics.
        context.set_offline(True)
        time.sleep(1)
        for value in (0.25, 0.5):
            response = api.post(
                f"/api/runs/{run_id}/metrics",
                data={
                    "batch_id": str(uuid4()),
                    "events": [{
                        "event_id": str(uuid4()),
                        "metric_name": "reconnect_probe",
                        "value": value,
                        "client_time": "2026-11-10T10:00:00Z",
                    }],
                },
            )
            assert response.status == 200, response.text()

        context.set_offline(False)
        wait_for_event_count(page, 3)
        persisted = api.get(f"/api/runs/{run_id}").json()
        page_cursor = int(page.locator("#cursor-value").inner_text())
        assert page_cursor == persisted["cursor"], (page_cursor, persisted["cursor"])
        assert len(persisted["events"]) == 3
        print(f"PASS: browser reconnected and caught up to server_seq={page_cursor}; database snapshot has 3 events")

        api.dispose()
        browser.close()


if __name__ == "__main__":
    main()
