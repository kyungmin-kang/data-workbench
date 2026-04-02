from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import sys
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


def find_playwright_managed_chromium_executable() -> str | None:
    env_path = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    browser_roots = [
        ROOT_DIR / ".playwright-browsers",
        Path(os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "")).expanduser() if os.environ.get("PLAYWRIGHT_BROWSERS_PATH") else None,
        Path.home() / "Library" / "Caches" / "ms-playwright",
        Path.home() / ".cache" / "ms-playwright",
        Path(os.environ.get("LOCALAPPDATA", "")) / "ms-playwright" if os.environ.get("LOCALAPPDATA") else None,
    ]
    patterns = (
        "chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium",
        "chromium-*/chrome-mac*/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "chromium_headless_shell-*/chrome-headless-shell-mac*/chrome-headless-shell",
        "chromium-*/chrome-linux/chrome",
        "chromium-*/chrome-win/chrome.exe",
        "chromium-*/chrome-win64/chrome.exe",
    )
    candidates: list[Path] = []
    for browser_root in browser_roots:
        if not browser_root:
            continue
        for pattern in patterns:
            candidates.extend(browser_root.glob(pattern))
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def find_system_chromium_executable() -> str | None:
    candidates = [
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
        Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
        Path("/usr/bin/chromium"),
        Path("/usr/bin/chromium-browser"),
        Path("/usr/bin/google-chrome"),
        Path("/usr/bin/google-chrome-stable"),
        Path("/opt/google/chrome/chrome"),
        Path("/snap/bin/chromium"),
        Path("/usr/bin/microsoft-edge"),
        Path("/usr/bin/msedge"),
        Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ]
    for executable_name in ("chromium", "chromium-browser", "google-chrome", "google-chrome-stable", "msedge"):
        resolved = shutil.which(executable_name)
        if resolved:
            candidates.append(Path(resolved))
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def build_browser_launch_options() -> dict[str, str | bool] | None:
    executable_path = find_playwright_managed_chromium_executable()
    if executable_path:
        return {"headless": True, "executable_path": executable_path}
    channel = os.environ.get("PLAYWRIGHT_CHROMIUM_CHANNEL")
    if channel:
        return {"headless": True, "channel": channel}
    if os.environ.get("PLAYWRIGHT_ALLOW_SYSTEM_BROWSER") == "1":
        system_executable = find_system_chromium_executable()
        if system_executable:
            return {"headless": True, "executable_path": system_executable}
    return None


def find_python_executable() -> str | None:
    env_path = os.environ.get("WORKBENCH_E2E_PYTHON")
    candidates = [
        Path(env_path).expanduser() if env_path else None,
        ROOT_DIR / ".venv" / "bin" / "python",
        ROOT_DIR / ".venv" / "Scripts" / "python.exe",
        Path(sys.executable).resolve() if sys.executable else None,
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return str(candidate)
    return None


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", 0))
        except PermissionError as error:  # pragma: no cover - sandbox-specific
            raise unittest.SkipTest(f"Local port binding is not permitted in this environment: {error}") from error
        return int(sock.getsockname()[1])


@unittest.skipUnless(sync_playwright, "Playwright is not installed.")
class BrowserE2ETests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.browser_launch_options = build_browser_launch_options()
        if cls.browser_launch_options is None:
            raise unittest.SkipTest(
                "No Playwright Chromium install was found. Run `python -m playwright install chromium`, "
                "set PLAYWRIGHT_BROWSERS_PATH, or provide PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH."
            )
        browser = None
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(**cls.browser_launch_options)
        except Exception as error:  # pragma: no cover - environment-specific
            raise unittest.SkipTest(f"Chromium could not be launched for E2E tests: {error}") from error
        finally:
            if browser is not None:
                with contextlib.suppress(Exception):
                    browser.close()

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.server: subprocess.Popen[str] | None = None
        shutil.copytree(ROOT_DIR / "data", self.root / "data")
        shutil.copytree(ROOT_DIR / "specs", self.root / "specs")
        if (ROOT_DIR / "docs").exists():
            shutil.copytree(ROOT_DIR / "docs", self.root / "docs")
        if (ROOT_DIR / "examples").exists():
            shutil.copytree(ROOT_DIR / "examples", self.root / "examples")
        (self.root / "runtime" / "plans").mkdir(parents=True, exist_ok=True)
        (self.root / "runtime" / "cache").mkdir(parents=True, exist_ok=True)

        self.port = find_free_port()
        python_executable = find_python_executable()
        if python_executable is None:
            self.skipTest("No Python interpreter was found for launching the app server.")
        env = os.environ.copy()
        env["PYTHONPATH"] = "src"
        env["WORKBENCH_ROOT_DIR"] = str(self.root)
        env["WORKBENCH_HOST"] = "127.0.0.1"
        env["WORKBENCH_PORT"] = str(self.port)
        try:
            self.server = subprocess.Popen(
                [python_executable, "-m", "workbench.app"],
                cwd=ROOT_DIR,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            wait_for_server(f"http://127.0.0.1:{self.port}/healthz", process=self.server)
        except Exception:
            self._cleanup_server()
            self.temp_dir.cleanup()
            raise

    def tearDown(self) -> None:
        self._cleanup_server()
        self.temp_dir.cleanup()

    def _cleanup_server(self) -> None:
        if self.server is None:
            return
        if self.server.poll() is None:
            self.server.terminate()
            try:
                self.server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=5)
        for stream_name in ("stdout", "stderr"):
            stream = getattr(self.server, stream_name)
            if stream is not None:
                with contextlib.suppress(Exception):
                    stream.close()
        self.server = None

    @contextlib.contextmanager
    def browser_page(self, **page_kwargs: object):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(**self.browser_launch_options)
            page = None
            try:
                page = browser.new_page(**page_kwargs)
                yield page
            finally:
                if page is not None:
                    with contextlib.suppress(Exception):
                        page.close()
                with contextlib.suppress(Exception):
                    browser.close()

    def test_project_survey_loads_into_import_form_and_imports_asset(self) -> None:
        with self.browser_page() as page:
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

    def test_graph_edit_controls_only_show_in_edit_mode_and_add_cancel_remove_work(self) -> None:
        with self.browser_page(viewport={"width": 1680, "height": 1200}) as page:
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

    def test_structure_review_surface_shows_contradictions_and_review_actions(self) -> None:
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

        with self.browser_page(viewport={"width": 1680, "height": 1200}) as page:
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

    def test_structure_review_workflow_assignment_and_keyboard_review(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "keyboard_review.md").write_text(
            "GET /api/demo/keyboard-a\nGET /api/demo/keyboard-b\n",
            encoding="utf-8",
        )

        with self.browser_page(viewport={"width": 1680, "height": 1200}) as page:
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

    def test_execution_panel_can_save_and_derive_tasks(self) -> None:
        with self.browser_page(viewport={"width": 1680, "height": 1200}) as page:
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            execution = page.locator("#execution-summary")
            execution.get_by_text("Agent contracts").wait_for(timeout=10_000)

            execution.locator('[data-execution-action="add-item"][data-execution-collection="decisions"]').click()
            execution.locator('[data-execution-collection="decisions"][data-execution-field="title"]').last.fill("Browser decision")
            execution.locator('[data-execution-collection="decisions"][data-execution-field="linked_refs"]').last.fill("data:market_signals")
            execution.locator('[data-execution-action="save"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Execution state saved')",
                timeout=15_000,
            )

            execution.locator('[data-execution-action="derive-tasks"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Execution tasks derived')",
                timeout=15_000,
            )
            execution.locator('[data-execution-action="set-view"][data-execution-view="evidence"]').click()
            execution.locator('[data-execution-action="add-item"][data-execution-collection="acceptance_checks"]').click()
            execution.locator('[data-execution-collection="acceptance_checks"][data-execution-field="label"]').last.fill("Browser proof check")
            execution.locator('[data-execution-collection="acceptance_checks"][data-execution-field="linked_refs"]').last.fill("data:market_signals")
            execution.locator('[data-execution-action="create-proof"]').last.click()
            execution.locator('[data-execution-action="set-view"][data-execution-view="all"]').click()
            execution.locator('[data-execution-collection="decisions"][data-execution-field="title"][value="Browser decision"]').first.wait_for(timeout=10_000)
            execution.get_by_text("Role lanes").wait_for(timeout=10_000)

            execution.locator('[data-execution-action="task-create-run"]').first.click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Agent run created')",
                timeout=15_000,
            )
            execution.locator('[data-execution-action="run-quick-status"][data-execution-run-status="waiting"]').first.click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Run updated')",
                timeout=15_000,
            )
            execution.get_by_text("Handoff queue").wait_for(timeout=10_000)
            execution.locator('[data-execution-action="load-brief"][data-execution-run-id]').first.click()
            execution.get_by_text("Agent assignment brief").wait_for(timeout=10_000)
            execution.get_by_text("# workbench-builder brief").first.wait_for(timeout=10_000)

            page.reload(wait_until="networkidle")
            page.locator('#execution-summary [data-execution-collection="decisions"][data-execution-field="title"][value="Browser decision"]').first.wait_for(timeout=10_000)
            page.locator('#execution-summary').get_by_text("Agent contracts").wait_for(timeout=10_000)

    def test_demo_onboarding_walkthrough_bootstraps_saves_plan_and_presets(self) -> None:
        demo_root = self.root / "examples" / "onboarding_wizard_demo"
        with self.browser_page(viewport={"width": 1680, "height": 1200}) as page:
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            page.get_by_text("More").click()
            page.get_by_role("button", name="Add / Import").click()
            page.locator("#authoring-drawer").wait_for(timeout=10_000)

            root_input = page.locator('[data-project-profile-root="true"]')
            root_input.wait_for(timeout=10_000)
            root_input.fill(str(demo_root))
            include_internal = page.locator('[data-project-profile-option="includeInternal"]')
            if include_internal.is_checked():
                include_internal.uncheck()

            page.locator('[data-project-rescan="true"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Project discovery ready')",
                timeout=20_000,
            )

            page.locator('[data-project-wizard-step="2"]').click()
            page.locator("[data-project-import-load]").first.wait_for(timeout=10_000)
            page.locator('[data-project-select-all="true"]').click()

            page.locator('[data-project-wizard-step="3"]').click()
            page.locator('[data-project-api-select-all="true"]').click()
            page.locator('[data-project-ui-select-all="true"]').click()

            page.locator('[data-project-wizard-step="4"]').click()
            page.locator('[data-project-bootstrap="true"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Graph bootstrap complete')",
                timeout=20_000,
            )
            page.wait_for_function(
                "() => (document.getElementById('dirty-indicator')?.textContent || '').includes('Unsaved graph edits')",
                timeout=10_000,
            )

            page.locator("#save-button").click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Plan generated')",
                timeout=20_000,
            )
            page.locator("#plan-summary").get_by_text("Latest Markdown").wait_for(timeout=10_000)
            page.locator("#plan-summary").get_by_text("Metadata tracked").wait_for(timeout=10_000)

            page.locator('[data-project-wizard-step="1"]').click()
            page.locator('[data-project-preset-field="name"]').fill("Demo onboarding preset")
            page.locator('[data-project-preset-save="true"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Preset saved')",
                timeout=15_000,
            )
            page.locator('[data-project-preset-select="true"]').select_option(label="Demo onboarding preset")

            execution = page.locator("#execution-summary")
            execution.get_by_text("Agent contracts").wait_for(timeout=10_000)
            execution.locator('[data-execution-action="derive-tasks"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Execution tasks derived')",
                timeout=15_000,
            )

    def test_project_discovery_can_check_root_and_reload_saved_discovery(self) -> None:
        demo_root = self.root / "examples" / "onboarding_wizard_demo"
        with self.browser_page(viewport={"width": 1680, "height": 1200}) as page:
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            page.get_by_text("More").click()
            page.get_by_role("button", name="Add / Import").click()
            page.locator("#authoring-drawer").wait_for(timeout=10_000)

            root_input = page.locator('[data-project-profile-root="true"]')
            root_input.fill(str(demo_root))
            include_internal = page.locator('[data-project-profile-option="includeInternal"]')
            if include_internal.is_checked():
                include_internal.uncheck()

            page.locator('[data-project-rescan="true"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Project discovery ready')",
                timeout=20_000,
            )

            page.reload(wait_until="networkidle")
            page.get_by_text("More").click()
            page.get_by_role("button", name="Add / Import").click()
            page.locator("#authoring-drawer").wait_for(timeout=10_000)

            root_input = page.locator('[data-project-profile-root="true"]')
            root_input.fill(str(demo_root))
            include_internal = page.locator('[data-project-profile-option="includeInternal"]')
            if include_internal.is_checked():
                include_internal.uncheck()

            page.locator('[data-project-root-check="true"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Project root ready')",
                timeout=15_000,
            )
            page.locator("#project-profile-summary").get_by_text("Saved discovery available").wait_for(timeout=10_000)

            page.locator('[data-project-load-cached="true"]').click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Project discovery ready')",
                timeout=20_000,
            )
            page.locator("#project-profile-summary").get_by_text("Loaded discovery snapshot").wait_for(timeout=10_000)
            page.locator("#project-profile-summary").get_by_text("Cache file:").wait_for(timeout=10_000)

    def test_execution_contract_workflow_can_load_and_launch_guided_run(self) -> None:
        with self.browser_page(viewport={"width": 1680, "height": 1200}) as page:
            page.goto(f"http://127.0.0.1:{self.port}/", wait_until="networkidle")

            execution = page.locator("#execution-summary")
            execution.get_by_text("Agent contracts").wait_for(timeout=10_000)
            execution.locator('[data-execution-action="load-workflow"][data-execution-contract-id="workbench-architect"]').first.click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Agent workflow ready')",
                timeout=15_000,
            )
            execution.get_by_text("Agent workflow").wait_for(timeout=10_000)
            execution.get_by_text("workbench-architect").first.wait_for(timeout=10_000)

            execution.locator('[data-execution-action="launch-workflow"][data-execution-contract-id="workbench-architect"]').first.click()
            page.wait_for_function(
                "() => (document.getElementById('status-text')?.textContent || '').includes('Workflow launch ready')",
                timeout=15_000,
            )
            execution.locator('[data-execution-action="set-view"][data-execution-view="runs"]').click()
            execution.locator('[data-execution-collection="agent_runs"][data-execution-field="objective"]').first.wait_for(timeout=10_000)


def wait_for_server(url: str, timeout_seconds: float = 10.0, process: subprocess.Popen[str] | None = None) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process and process.poll() is not None:
            stdout = process.stdout.read().strip() if process.stdout else ""
            stderr = process.stderr.read().strip() if process.stderr else ""
            details = "\n".join(part for part in (stdout, stderr) if part)
            raise RuntimeError(f"Server exited before becoming ready: {details or 'no output captured'}")
        try:
            with urllib.request.urlopen(url) as response:  # noqa: S310
                if response.status == 200:
                    return
        except Exception:  # noqa: BLE001
            time.sleep(0.2)
    raise TimeoutError(f"Server did not become ready: {url}")


if __name__ == "__main__":
    unittest.main()
