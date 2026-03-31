from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    sync_playwright = None

from workbench.store import ROOT_DIR


def find_chromium_executable() -> str | None:
    env_path = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    candidates = [
        *ROOT_DIR.glob(".playwright-browsers/chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium"),
        *Path.home().glob("Library/Caches/ms-playwright/chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium"),
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
        Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", 0))
        except PermissionError as error:  # pragma: no cover - sandbox-specific
            raise unittest.SkipTest(f"Local port binding is not permitted in this environment: {error}") from error
        return int(sock.getsockname()[1])


@unittest.skipUnless(sync_playwright and find_chromium_executable(), "Playwright or Chromium binary not available.")
class BrowserE2ETests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        shutil.copytree(ROOT_DIR / "data", self.root / "data")
        shutil.copytree(ROOT_DIR / "specs", self.root / "specs")
        if (ROOT_DIR / "docs").exists():
            shutil.copytree(ROOT_DIR / "docs", self.root / "docs")
        (self.root / "runtime" / "plans").mkdir(parents=True, exist_ok=True)
        (self.root / "runtime" / "cache").mkdir(parents=True, exist_ok=True)

        self.port = find_free_port()
        env = os.environ.copy()
        env["PYTHONPATH"] = "src"
        env["WORKBENCH_ROOT_DIR"] = str(self.root)
        env["WORKBENCH_HOST"] = "127.0.0.1"
        env["WORKBENCH_PORT"] = str(self.port)
        self.server = subprocess.Popen(
            [str(ROOT_DIR / ".venv" / "bin" / "python"), "-m", "workbench.app"],
            cwd=ROOT_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        wait_for_server(f"http://127.0.0.1:{self.port}/healthz")

    def tearDown(self) -> None:
        if getattr(self, "server", None) and self.server.poll() is None:
            self.server.terminate()
            try:
                self.server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server.kill()
        self.temp_dir.cleanup()

    def test_project_survey_loads_into_import_form_and_imports_asset(self) -> None:
        executable_path = find_chromium_executable()
        assert executable_path is not None

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, executable_path=executable_path)
            page = browser.new_page()
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            page.get_by_text("More").click()
            page.get_by_role("button", name="Discover Project").click()
            page.get_by_role("button", name="Add / Import").click()
            page.locator("#authoring-drawer").wait_for(timeout=10_000)
            page.wait_for_function("() => { const button = document.getElementById('project-profile-button'); return Boolean(button) && !button.disabled; }", timeout=10_000)
            page.locator('[data-project-wizard-step="2"]').click()
            page.locator("[data-project-import-load]").first.wait_for(timeout=10_000)
            page.locator("[data-project-import-load]").first.click()

            raw_asset_value = page.locator('[data-authoring-path="import.rawAssetValue"]').input_value()
            self.assertTrue(raw_asset_value)

            page.get_by_role("button", name="Import asset into graph").click()
            page.wait_for_function("() => (document.getElementById('status-text')?.textContent || '').includes('Asset imported')", timeout=10_000)
            dirty_label = page.locator("#dirty-indicator").text_content() or ""
            self.assertIn("Unsaved", dirty_label)

            browser.close()

    def test_graph_edit_controls_only_show_in_edit_mode_and_add_cancel_remove_work(self) -> None:
        executable_path = find_chromium_executable()
        assert executable_path is not None

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, executable_path=executable_path)
            page = browser.new_page(viewport={"width": 1680, "height": 1200})
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            pricing_features = page.locator('[data-node-id="data:pricing_features"]').first
            pricing_features.locator('[data-graph-toggle-expand="data:pricing_features"]').click()

            self.assertEqual(
                page.locator('[data-node-id="data:pricing_features"] [data-graph-column-add="data:pricing_features"]').count(),
                0,
            )

            page.locator("#edit-mode-button").click()

            add_button = page.locator('[data-node-id="data:pricing_features"] [data-graph-column-add="data:pricing_features"]')
            add_button.wait_for(timeout=10_000)
            add_button.click()

            pending_name = page.locator('[data-node-id="data:pricing_features"] .graph-table-add-row [data-graph-column-field="name"]').first
            pending_name.fill("browser_test_metric")
            page.locator('[data-node-id="data:pricing_features"] [data-graph-column-cancel^="data:pricing_features:"]').first.click()
            self.assertEqual(
                page.locator('[data-node-id="data:pricing_features"] .graph-table-add-row [data-graph-column-field="name"]').count(),
                0,
            )

            add_button.click()
            pending_name = page.locator('[data-node-id="data:pricing_features"] .graph-table-add-row [data-graph-column-field="name"]').first
            pending_name.fill("browser_test_metric")
            page.locator('[data-node-id="data:pricing_features"] [data-graph-column-commit^="data:pricing_features:"]').first.click()
            page.locator('[data-node-id="data:pricing_features"] [data-graph-column-field="name"][value="browser_test_metric"]').first.wait_for(timeout=10_000)

            note_inputs = page.locator('[data-node-id="data:pricing_features"] [data-graph-work-item-field="text"][data-graph-node-id="data:pricing_features"]')
            before_count = note_inputs.count()
            page.locator('[data-node-id="data:pricing_features"] [data-graph-work-item-add="data:pricing_features"]').click()
            self.assertEqual(note_inputs.count(), before_count + 1)
            page.locator('[data-node-id="data:pricing_features"] [data-graph-work-item-remove^="data:pricing_features:"]').last.click()
            page.locator("#confirm-modal").wait_for(timeout=10_000)
            page.locator("#confirm-modal-ok").click()
            self.assertEqual(note_inputs.count(), before_count)

            browser.close()

    def test_structure_review_surface_shows_contradictions_and_review_actions(self) -> None:
        executable_path = find_chromium_executable()
        assert executable_path is not None

        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    median_home_price = Column(Numeric)
    pricing_score = Column(Numeric)
    shadow_inventory_index = Column(Numeric)

@router.get("/api/markets/snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {
        "median_home_price": signal.median_home_price,
        "pricing_score": signal.pricing_score,
        "shadow_inventory_index": signal.shadow_inventory_index,
    }
""".strip(),
            encoding="utf-8",
        )

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, executable_path=executable_path)
            page = browser.new_page(viewport={"width": 1680, "height": 1200})
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            page.get_by_role("button", name="Scout scan").click()
            page.locator("#review-drawer-toggle").click()
            page.locator("#review-drawer-content").get_by_text("Contradiction review").wait_for(timeout=15_000)
            page.locator("#review-drawer-content").get_by_text("Patch review inbox").wait_for(timeout=15_000)
            page.locator("#review-drawer-content").get_by_text("PricingScoreCard will not render").first.wait_for(timeout=15_000)

            adopt_observed = page.locator("#review-drawer-content").get_by_role("button", name="Adopt observed").first
            adopt_observed.wait_for(timeout=10_000)
            adopt_observed.click()
            page.locator("#review-action-modal").wait_for(timeout=10_000)
            page.locator("#review-action-modal-note").fill("Observed backend binding is ready to adopt.")
            page.locator("#review-action-modal").get_by_role("button", name="Adopt observed").click()
            page.locator("#review-drawer-content").get_by_text("accepted: 1").first.wait_for(timeout=15_000)
            page.locator("#review-drawer-content").get_by_text("Contradiction audit trail").first.wait_for(timeout=15_000)

            browser.close()

    def test_structure_review_workflow_assignment_and_keyboard_review(self) -> None:
        executable_path = find_chromium_executable()
        assert executable_path is not None

        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "keyboard_review.md").write_text(
            "GET /api/demo/keyboard-a\nGET /api/demo/keyboard-b\n",
            encoding="utf-8",
        )

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, executable_path=executable_path)
            page = browser.new_page(viewport={"width": 1680, "height": 1200})
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            page.get_by_role("button", name="Scout scan").click()
            page.locator("#review-drawer-toggle").click()
            page.locator("#review-drawer-content").get_by_role("heading", name="Review workflow").wait_for(timeout=15_000)

            page.locator('#review-drawer-content [data-structure-pref="reviewer_identity"]').fill("ux-reviewer")
            page.locator("#review-drawer-content").get_by_role("button", name="Assign to me").click()
            page.locator("#review-drawer-content").get_by_text("reviewer ux-reviewer").first.wait_for(timeout=15_000)

            page.locator("#review-drawer-content .structure-keyboard-help").click()
            page.keyboard.press("a")
            page.locator("#review-action-modal").wait_for(timeout=10_000)
            page.locator("#review-action-modal").get_by_role("button", name="Accept").click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Patch review updated')",
                timeout=15_000,
            )

            browser.close()


def wait_for_server(url: str, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url) as response:  # noqa: S310
                if response.status == 200:
                    return
        except Exception:  # noqa: BLE001
            time.sleep(0.2)
    raise TimeoutError(f"Server did not become ready: {url}")


if __name__ == "__main__":
    unittest.main()
