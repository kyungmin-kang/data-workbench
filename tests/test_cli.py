from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout

import yaml

from workbench.cli import main
from tests.workbench_test_support import WorkbenchTempRootMixin


class CliTests(WorkbenchTempRootMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.setUpWorkbenchRoot()

    def tearDown(self) -> None:
        self.tearDownWorkbenchRoot()

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
        self.assertIn("planned_missing", payload)
        self.assertIn("observed_untracked", payload)
        self.assertIn("implemented_differently", payload)
        self.assertIn("uncertain_matches", payload)
        self.assertIn("binding_mismatches", payload)
        self.assertIn("column_mismatches", payload)
        self.assertIn("field_matrix_review_required_count", payload)
        self.assertIn("contradiction_cluster_count", payload)
        self.assertIn("open_contradiction_clusters", payload)
        self.assertIn("resolved_contradiction_clusters", payload)
        self.assertIn("mixed_contradiction_clusters", payload)
        self.assertIn("high_severity_contradictions", payload)
        self.assertIn("downstream_breakage_count", payload)
        self.assertIn("merge_plan_status", payload)
        self.assertIn("merge_plan_noop_count", payload)

    def test_rebase_command_preserves_review_state(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "cli_rebase.md").write_text("GET /api/cli/rebase\n", encoding="utf-8")

        sync_buffer = io.StringIO()
        with redirect_stdout(sync_buffer):
            sync_exit = main(
                [
                    "sync",
                    str(self.root),
                    "--agent",
                    "scout",
                    "--scope",
                    "full",
                    "--doc-path",
                    "docs/cli_rebase.md",
                ]
            )
        self.assertEqual(sync_exit, 0)
        bundle_id = json.loads(sync_buffer.getvalue())["bundle_id"]

        bundle_buffer = io.StringIO()
        with redirect_stdout(bundle_buffer):
            bundle_exit = main(["bundle", bundle_id])
        self.assertEqual(bundle_exit, 0)
        bundle = yaml.safe_load(bundle_buffer.getvalue())
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_cli_rebase"
        )

        review_buffer = io.StringIO()
        with redirect_stdout(review_buffer):
            review_exit = main(["review", bundle_id, patch_id, "accepted"])
        self.assertEqual(review_exit, 0)

        import_buffer = io.StringIO()
        with redirect_stdout(import_buffer):
            import_exit = main(["import", str(self.root / "specs" / "structure" / "spec.yaml"), "--updated-by", "cli-rebase"])
        self.assertEqual(import_exit, 0)

        rebase_buffer = io.StringIO()
        with redirect_stdout(rebase_buffer):
            rebase_exit = main(["rebase", bundle_id])
        self.assertEqual(rebase_exit, 0)
        rebase_payload = json.loads(rebase_buffer.getvalue())
        self.assertEqual(rebase_payload["transferred_review_count"], 1)
        self.assertEqual(rebase_payload["preserved_review_states"]["accepted"], 1)
        self.assertEqual(rebase_payload["dropped_review_count"], 0)
        self.assertEqual(rebase_payload["merge_plan_status"], "ready")
        self.assertIn("merge_plan_noop_count", rebase_payload)

        rebased_bundle_buffer = io.StringIO()
        with redirect_stdout(rebased_bundle_buffer):
            rebased_bundle_exit = main(["bundle", rebase_payload["bundle_id"]])
        self.assertEqual(rebased_bundle_exit, 0)
        rebased_bundle = yaml.safe_load(rebased_bundle_buffer.getvalue())
        rebased_patch = next(
            patch
            for patch in rebased_bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_cli_rebase"
        )
        self.assertEqual(rebased_patch["review_state"], "accepted")

    def test_rebase_preview_command_emits_preview_summary(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "cli_rebase_preview.md").write_text("GET /api/cli/rebase-preview\n", encoding="utf-8")

        sync_buffer = io.StringIO()
        with redirect_stdout(sync_buffer):
            sync_exit = main(
                [
                    "sync",
                    str(self.root),
                    "--agent",
                    "scout",
                    "--scope",
                    "full",
                    "--doc-path",
                    "docs/cli_rebase_preview.md",
                ]
            )
        self.assertEqual(sync_exit, 0)
        bundle_id = json.loads(sync_buffer.getvalue())["bundle_id"]

        import_buffer = io.StringIO()
        with redirect_stdout(import_buffer):
            import_exit = main(["import", str(self.root / "specs" / "structure" / "spec.yaml"), "--updated-by", "cli-preview"])
        self.assertEqual(import_exit, 0)

        preview_buffer = io.StringIO()
        with redirect_stdout(preview_buffer):
            preview_exit = main(["rebase-preview", bundle_id])
        self.assertEqual(preview_exit, 0)
        preview_payload = json.loads(preview_buffer.getvalue())
        self.assertEqual(preview_payload["base_version"], 1)
        self.assertGreaterEqual(preview_payload["current_version"], 2)
        self.assertIn("changed_target_count", preview_payload)
        self.assertIn("changed_targets", preview_payload)
        self.assertIn("merge_plan_status", preview_payload)
        self.assertIn("merge_plan_noop_count", preview_payload)
        self.assertIn("merge_plan_blocked_step_count", preview_payload)
