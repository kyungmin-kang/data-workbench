from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from workbench.cli import main
from workbench.store import ROOT_DIR


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        shutil.copytree(ROOT_DIR / "data", self.root / "data")
        shutil.copytree(ROOT_DIR / "specs", self.root / "specs")
        shutil.copytree(ROOT_DIR / "src", self.root / "src")
        shutil.copytree(ROOT_DIR / "static", self.root / "static")
        if (ROOT_DIR / "docs").exists():
            shutil.copytree(ROOT_DIR / "docs", self.root / "docs")
        (self.root / "runtime" / "plans").mkdir(parents=True, exist_ok=True)
        (self.root / "runtime" / "cache").mkdir(parents=True, exist_ok=True)
        self.previous_root = os.environ.get("WORKBENCH_ROOT_DIR")
        os.environ["WORKBENCH_ROOT_DIR"] = str(self.root)

    def tearDown(self) -> None:
        if self.previous_root is None:
            os.environ.pop("WORKBENCH_ROOT_DIR", None)
        else:
            os.environ["WORKBENCH_ROOT_DIR"] = self.previous_root
        self.temp_dir.cleanup()

    def test_export_command_prints_yaml(self) -> None:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code = main(["export"])
        self.assertEqual(exit_code, 0)
        self.assertIn("structure_version:", buffer.getvalue())

    def test_sync_command_emits_bundle_summary(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "cli_scan.md").write_text("GET /api/cli/readiness\n", encoding="utf-8")

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code = main(
                [
                    "sync",
                    str(self.root),
                    "--agent",
                    "scout",
                    "--scope",
                    "full",
                    "--include-tests",
                    "--doc-path",
                    "docs/cli_scan.md",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = json.loads(buffer.getvalue())
        self.assertIn("bundle_id", payload)
        self.assertIn("patch_count", payload)

