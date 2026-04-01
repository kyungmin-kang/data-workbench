from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from workbench import worker
from workbench.app import app
from workbench.profile import profile_graph
from workbench.project_profiler import profile_project
from workbench.store import ROOT_DIR, load_graph
from workbench.types import validate_graph
from workbench.validation import build_validation_report
from tests.workbench_test_support import WorkbenchTempRootMixin


class RegressionTests(WorkbenchTempRootMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.setUpWorkbenchRoot()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.tearDownWorkbenchRoot()

    def test_project_hint_import_honors_root_path(self) -> None:
        with tempfile.TemporaryDirectory() as alt_dir:
            alt_root = Path(alt_dir)
            backend_dir = alt_root / "backend"
            backend_dir.mkdir(parents=True, exist_ok=True)
            (backend_dir / "pricing.py").write_text(
                """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/alt-only")
def alt_only():
    return {"hello": 1}
""".strip(),
                encoding="utf-8",
            )

            profile_response = self.client.get(
                "/api/project/profile",
                params={"root_path": str(alt_root), "include_internal": "false"},
            )
            self.assertEqual(profile_response.status_code, 200)
            profile = profile_response.json()["project_profile"]
            hint = next(hint for hint in profile["api_contract_hints"] if hint["route"] == "GET /api/alt-only")

            graph = self.client.get("/api/graph").json()["graph"]
            import_response = self.client.post(
                "/api/import/project-hint",
                json={
                    "graph": graph,
                    "hint_kind": "api",
                    "hint_id": hint["id"],
                    "root_path": str(alt_root),
                    "include_internal": False,
                },
            )
            self.assertEqual(import_response.status_code, 200)
            imported_graph = import_response.json()["graph"]
            self.assertTrue(
                any(
                    node["kind"] == "contract"
                    and node.get("contract", {}).get("route") == "GET /api/alt-only"
                    for node in imported_graph["nodes"]
                )
            )

    def test_project_bootstrap_honors_root_path(self) -> None:
        with tempfile.TemporaryDirectory() as alt_dir:
            alt_root = Path(alt_dir)
            backend_dir = alt_root / "backend"
            backend_dir.mkdir(parents=True, exist_ok=True)
            (backend_dir / "pricing.py").write_text(
                """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/bootstrap-only")
def bootstrap_only():
    return {"hello": 1}
""".strip(),
                encoding="utf-8",
            )

            profile = profile_project(alt_root, include_internal=False)
            hint = next(hint for hint in profile["api_contract_hints"] if hint["route"] == "GET /api/bootstrap-only")
            graph = self.client.get("/api/graph").json()["graph"]
            bootstrap_response = self.client.post(
                "/api/import/project-bootstrap",
                json={
                    "graph": graph,
                    "root_path": str(alt_root),
                    "include_tests": False,
                    "include_internal": False,
                    "api_hint_ids": [hint["id"]],
                    "import_assets": False,
                    "import_api_hints": True,
                    "import_ui_hints": False,
                },
            )
            self.assertEqual(bootstrap_response.status_code, 200)
            imported_graph = bootstrap_response.json()["graph"]
            self.assertTrue(
                any(
                    node["kind"] == "contract"
                    and node.get("contract", {}).get("route") == "GET /api/bootstrap-only"
                    for node in imported_graph["nodes"]
                )
            )

    def test_profile_project_returns_full_discovery_lists(self) -> None:
        with tempfile.TemporaryDirectory() as repo_dir:
            repo_root = Path(repo_dir)
            for index in range(25):
                (repo_root / f"api_{index}.py").write_text(
                    f"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/r{index}")
def route_{index}():
    return {{"value_{index}": 1}}
""".strip(),
                    encoding="utf-8",
                )
            for index in range(14):
                (repo_root / f"data_{index}.csv").write_text("a,b\n1,2\n", encoding="utf-8")

            profile = profile_project(repo_root)
            self.assertEqual(profile["summary"]["api_contract_hints"], len(profile["api_contract_hints"]))
            self.assertEqual(profile["summary"]["data_assets"], len(profile["data_assets"]))

    def test_validation_flags_invalid_compute_edge_mappings(self) -> None:
        graph = load_graph()
        edge = next(
            item
            for item in graph["edges"]
            if item["source"] == "compute:transform.feature_builder" and item["target"] == "data:pricing_features"
        )
        edge["column_mappings"][0]["source_column"] = "not_a_real_column"

        report = build_validation_report(validate_graph(graph))
        self.assertTrue(
            any(
                issue["category"] == "invalid_edge_source_mapping"
                and issue["edge_id"] == edge["id"]
                for issue in report["errors"]
            )
        )

    def test_profile_graph_clears_stale_metrics_when_asset_missing(self) -> None:
        graph = profile_graph(load_graph(), ROOT_DIR)
        node = next(item for item in graph["nodes"] if item["id"] == "data:fred_home_price_raw")
        self.assertEqual(node["data"]["row_count"], 5)

        node["data"]["local_path"] = "data/demo/does_not_exist.csv"
        source_node = next(item for item in graph["nodes"] if item["id"] == "source:fred.home_price_series")
        source_node["source"]["raw_assets"] = []
        refreshed = profile_graph(graph, ROOT_DIR)
        refreshed_node = next(item for item in refreshed["nodes"] if item["id"] == "data:fred_home_price_raw")

        self.assertEqual(refreshed_node["profile_status"], "schema_only")
        self.assertIsNone(refreshed_node["data"]["row_count"])
        self.assertEqual(refreshed_node["columns"][0]["stats"], {})
        self.assertIsNone(refreshed_node["columns"][0]["null_pct"])

    @patch("workbench.worker.save_graph")
    @patch("workbench.worker.profile_graph")
    @patch("workbench.worker.load_graph")
    def test_worker_run_cycle_skips_save_when_profile_is_unchanged(self, load_graph_mock, profile_graph_mock, save_graph_mock) -> None:
        graph = load_graph()
        load_graph_mock.return_value = graph
        profile_graph_mock.return_value = graph

        heartbeat = worker.run_cycle(auto_profile=True)

        save_graph_mock.assert_not_called()
        self.assertEqual(heartbeat["status"], "ok")
        self.assertTrue(heartbeat["profiled"])
        self.assertFalse(heartbeat["changed"])
