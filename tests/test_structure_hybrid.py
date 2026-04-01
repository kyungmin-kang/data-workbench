from __future__ import annotations

import json
import unittest

from fastapi.testclient import TestClient

from workbench.app import app
from tests.workbench_test_support import WorkbenchTempRootMixin


class StructureHybridTests(WorkbenchTempRootMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.setUpWorkbenchRoot()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.tearDownWorkbenchRoot()

    def test_partial_graph_plan_reports_field_level_hybrid_differences(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/hybrid-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market}
""".strip(),
            encoding="utf-8",
        )

        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "hybrid_plan.json").write_text(
            json.dumps(
                {
                    "metadata": {"name": "Hybrid Plan"},
                    "nodes": [
                        {
                            "id": "data:analytics_market_signals",
                            "kind": "data",
                            "extension_type": "table",
                            "label": "Market Signals",
                            "tags": ["sql_relation:analytics.market_signals"],
                            "columns": [
                                {"name": "market", "data_type": "string"},
                                {"name": "pricing_score", "data_type": "float"},
                            ],
                        },
                        {
                            "id": "contract:api.get__api_hybrid_pricing_snapshot",
                            "kind": "contract",
                            "extension_type": "api",
                            "label": "Hybrid Pricing Snapshot",
                            "contract": {
                                "route": "GET /api/hybrid-pricing-snapshot",
                                "fields": [
                                    {
                                        "name": "pricing_score",
                                        "required": True,
                                        "sources": [
                                            {
                                                "node_id": "data:analytics_market_signals",
                                                "column": "market",
                                            }
                                        ],
                                    },
                                    {
                                        "name": "planned_only",
                                        "required": True,
                                    },
                                ],
                            },
                        },
                    ],
                    "edges": [],
                }
            ),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/hybrid_plan.json"],
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        comparison = bundle["reconciliation"]["comparison"]
        self.assertEqual(comparison["binding_mismatches"], 1)
        self.assertGreaterEqual(comparison["missing_fields"], 1)
        self.assertGreaterEqual(comparison["unplanned_fields"], 1)
        field_matrix = bundle["reconciliation"]["field_matrix"]
        field_rows = {row["subject"]: row for row in field_matrix}
        self.assertEqual(field_rows["GET /api/hybrid-pricing-snapshot.pricing_score"]["status"], "binding_mismatch")
        self.assertEqual(field_rows["GET /api/hybrid-pricing-snapshot.planned_only"]["status"], "planned_missing")
        self.assertEqual(field_rows["GET /api/hybrid-pricing-snapshot.market"]["status"], "observed_unplanned")
        field_matrix_summary = bundle["reconciliation"]["field_matrix_summary"]
        self.assertGreaterEqual(field_matrix_summary["review_required_count"], 3)
        self.assertEqual(field_matrix_summary["status_counts"]["binding_mismatch"], 1)
        self.assertGreaterEqual(field_matrix_summary["status_counts"]["planned_missing"], 1)
        self.assertGreaterEqual(field_matrix_summary["status_counts"]["observed_unplanned"], 1)
        self.assertTrue(any("planned_only" in item["message"] for item in bundle["reconciliation"]["planned_missing"]))
        self.assertTrue(any(".market was observed" in item["message"] for item in bundle["reconciliation"]["observed_untracked"]))
        self.assertTrue(any(item.get("kind") == "hybrid_binding_mismatch" for item in bundle["contradictions"]))
        self.assertTrue(
            any(
                patch["type"] == "add_field"
                and patch["payload"].get("field", {}).get("name") == "planned_only"
                and "plan_yaml" in patch.get("evidence", [])
                for patch in bundle["patches"]
            )
        )

    def test_partial_graph_plan_does_not_count_as_missing_route_implementation_evidence(self) -> None:
        graph = self.client.get("/api/graph").json()["graph"]
        graph["nodes"].append(
            {
                "id": "contract:api.plan_only_route",
                "kind": "contract",
                "extension_type": "api",
                "label": "Plan Only Route",
                "contract": {
                    "route": "GET /api/plan-only-route",
                    "fields": [{"name": "pricing_score", "required": True}],
                },
                "state": "confirmed",
                "verification_state": "confirmed",
                "confidence": "high",
            }
        )
        import_response = self.client.post("/api/structure/import", json={"spec": graph, "updated_by": "test-suite"})
        self.assertEqual(import_response.status_code, 200)

        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "plan_only_route.json").write_text(
            json.dumps(
                {
                    "metadata": {"name": "Plan Route"},
                    "nodes": [
                        {
                            "id": "contract:api.plan_only_route",
                            "kind": "contract",
                            "extension_type": "api",
                            "label": "Plan Only Route",
                            "contract": {
                                "route": "GET /api/plan-only-route",
                                "fields": [{"name": "pricing_score", "required": True}],
                            },
                        }
                    ],
                    "edges": [],
                }
            ),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/plan_only_route.json"],
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        self.assertTrue(
            any(
                item.get("kind") == "missing_confirmed_route"
                and item.get("target_id") == "contract:api.plan_only_route"
                for item in bundle["contradictions"]
            )
        )

    def test_merge_blocks_when_accepted_edge_depends_on_unaccepted_nodes(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "edge_only_plan.json").write_text(
            json.dumps(
                {
                    "metadata": {"name": "Edge Only Plan"},
                    "nodes": [
                        {
                            "id": "source:plan_demo_source",
                            "kind": "source",
                            "extension_type": "disk_path",
                            "label": "Plan Demo Source",
                        },
                        {
                            "id": "data:plan_demo_dataset",
                            "kind": "data",
                            "extension_type": "raw_dataset",
                            "label": "Plan Demo Dataset",
                        },
                    ],
                    "edges": [
                        {
                            "id": "edge.ingests.source_plan_demo_source.data_plan_demo_dataset",
                            "type": "ingests",
                            "source": "source:plan_demo_source",
                            "target": "data:plan_demo_dataset",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/edge_only_plan.json"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        edge_patch_id = next(patch["id"] for patch in bundle["patches"] if patch["type"] == "add_edge")

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": edge_patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)

        merge_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/merge",
            json={"merged_by": "test-suite"},
        )
        self.assertEqual(merge_response.status_code, 400)
        self.assertIn("depends on nodes", merge_response.json()["detail"])

        bundle_after = self.client.get(f"/api/structure/bundles/{bundle['bundle_id']}").json()["bundle"]
        self.assertEqual(bundle_after["review"]["merge_status"], "blocked")
        self.assertTrue(bundle_after["review"]["merge_blockers"])

    def test_stale_bundle_marks_rebase_required(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "stale_route.md").write_text("GET /api/stale-route\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/stale_route.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_stale_route"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "rebase-test"},
        )
        self.assertEqual(import_response.status_code, 200)

        merge_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/merge",
            json={"merged_by": "test-suite"},
        )
        self.assertEqual(merge_response.status_code, 400)
        self.assertIn("stale", merge_response.json()["detail"].lower())

        bundle_after = self.client.get(f"/api/structure/bundles/{bundle['bundle_id']}").json()["bundle"]
        self.assertEqual(bundle_after["review"]["merge_status"], "stale")
        self.assertTrue(bundle_after["review"]["rebase_required"])

    def test_reviewing_stale_bundle_keeps_rebase_required(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "stale_review.md").write_text("GET /api/stale-review\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/stale_review.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_stale_review"
        )

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "stale-review"},
        )
        self.assertEqual(import_response.status_code, 200)

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_bundle = review_response.json()["bundle"]
        self.assertEqual(reviewed_bundle["review"]["merge_status"], "stale")
        self.assertTrue(reviewed_bundle["review"]["rebase_required"])
        self.assertEqual(reviewed_bundle["review"]["merge_blockers"][0]["type"], "stale_bundle")

    def test_runtime_latest_markdown_plan_drives_hybrid_comparison(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "markdown_hybrid.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/markdown-hybrid")
def markdown_hybrid_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market}
""".strip(),
            encoding="utf-8",
        )

        latest_plan_path = self.root / "runtime" / "plans" / "latest.plan.md"
        latest_plan_path.write_text(
            """
# API: GET /api/markdown-hybrid
Fields:
- pricing_score <- analytics.market_signals.market
- market <- analytics.market_signals.market
- planned_only

# Table: analytics.market_signals
Columns:
- market (string)
- pricing_score (float)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        comparison = bundle["reconciliation"]["comparison"]
        self.assertEqual(comparison["plan_candidates"], 1)
        self.assertIn(str(latest_plan_path.resolve()), comparison["plan_paths"])
        self.assertEqual(comparison["binding_mismatches"], 1)
        self.assertGreaterEqual(comparison["missing_fields"], 1)
        self.assertTrue(any("planned_only" in item["message"] for item in bundle["reconciliation"]["planned_missing"]))
        self.assertTrue(any(item.get("kind") == "hybrid_binding_mismatch" for item in bundle["contradictions"]))
        contradiction_summary = bundle["reconciliation"]["contradiction_summary"]
        self.assertEqual(contradiction_summary["count"], len(bundle["contradictions"]))
        self.assertTrue(any(item["kind"] == "hybrid_binding_mismatch" for item in contradiction_summary["kinds"]))

    def test_freeform_markdown_plan_ingestion_and_merge_plan_artifact(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "freeform_hybrid.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/freeform-hybrid")
def freeform_hybrid_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market}
""".strip(),
            encoding="utf-8",
        )

        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "freeform_plan.md").write_text(
            """
# Endpoint: GET /api/freeform-hybrid
Response schema:
- pricing_score: float from table analytics.market_signals.pricing_score
- market: string from analytics.market_signals.market
Required fields: planned_only

# Transform: analytics.market_signal_features
Input: table analytics.market_signals
Output: view analytics.market_signal_features
Output fields:
- pricing_score: float from analytics.market_signals.pricing_score
""".strip(),
            encoding="utf-8",
        )

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/freeform_plan.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        field_rows = {row["subject"]: row for row in bundle["reconciliation"]["field_matrix"]}
        self.assertEqual(field_rows["GET /api/freeform-hybrid.pricing_score"]["status"], "matched")
        self.assertEqual(field_rows["GET /api/freeform-hybrid.planned_only"]["status"], "planned_missing")

        route_patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["type"] == "add_node" and patch["target_id"] == "contract:api.get__api_freeform_hybrid"
        )
        planned_only_patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["type"] == "add_field" and patch["payload"].get("field", {}).get("name") == "planned_only"
        )
        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review-batch",
            json={"patch_ids": [planned_only_patch_id, route_patch_id], "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_bundle = review_response.json()["bundle"]
        merge_plan = reviewed_bundle["review"]["merge_plan"]
        self.assertEqual(merge_plan["status"], "ready")
        self.assertEqual(merge_plan["accepted_patch_count"], 2)
        self.assertEqual(merge_plan["step_count"], 2)
        self.assertEqual(merge_plan["steps"][0]["patch_id"], route_patch_id)
        self.assertEqual(merge_plan["steps"][1]["patch_id"], planned_only_patch_id)
        self.assertEqual(merge_plan["steps"][0]["status"], "ready")

    def test_reviewing_contradiction_updates_cluster_resolution_state(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "contradiction_cluster.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/cluster-review")
def cluster_review(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market}
""".strip(),
            encoding="utf-8",
        )

        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "cluster_review_plan.json").write_text(
            json.dumps(
                {
                    "metadata": {"name": "Cluster Review Plan"},
                    "nodes": [
                        {
                            "id": "data:analytics_market_signals",
                            "kind": "data",
                            "extension_type": "table",
                            "label": "Market Signals",
                            "tags": ["sql_relation:analytics.market_signals"],
                            "columns": [
                                {"name": "market", "data_type": "string"},
                                {"name": "pricing_score", "data_type": "float"},
                            ],
                        },
                        {
                            "id": "contract:api.get__api_cluster_review",
                            "kind": "contract",
                            "extension_type": "api",
                            "label": "Cluster Review",
                            "contract": {
                                "route": "GET /api/cluster-review",
                                "fields": [
                                    {
                                        "name": "pricing_score",
                                        "required": True,
                                        "sources": [
                                            {
                                                "node_id": "data:analytics_market_signals",
                                                "column": "market",
                                            }
                                        ],
                                    }
                                ],
                            },
                        },
                    ],
                    "edges": [],
                }
            ),
            encoding="utf-8",
        )

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/cluster_review_plan.json"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        contradiction = next(item for item in bundle["contradictions"] if item["kind"] == "hybrid_binding_mismatch")
        contradiction_target = contradiction["field_id"] or contradiction["target_id"]
        cluster_before = next(
            item
            for item in bundle["reconciliation"]["contradiction_clusters"]
            if item["target_id"] == contradiction_target and item["kind"] == "hybrid_binding_mismatch"
        )
        self.assertEqual(cluster_before["resolution_state"], "pending")

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review-contradiction",
            json={"contradiction_id": contradiction["id"], "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_bundle = review_response.json()["bundle"]
        cluster_after = next(
            item
            for item in reviewed_bundle["reconciliation"]["contradiction_clusters"]
            if item["target_id"] == contradiction_target and item["kind"] == "hybrid_binding_mismatch"
        )
        self.assertEqual(cluster_after["resolution_state"], "accepted")
        self.assertTrue(cluster_after["resolved"])
        self.assertEqual(cluster_after["review_state_counts"]["accepted"], 1)

    def test_plan_only_markdown_rebinds_existing_targets_and_clears_missing_binding_readiness(self) -> None:
        import_response = self.client.post(
            "/api/structure/import",
            json={
                "spec": {
                    "metadata": {"name": "Plan First"},
                    "nodes": [
                        {
                            "id": "data:signals_table",
                            "kind": "data",
                            "extension_type": "table",
                            "label": "Signals Table",
                            "tags": ["sql_relation:analytics.market_signals"],
                            "columns": [{"name": "score_raw", "data_type": "float"}],
                            "data": {"persistence": "hot", "persisted": True},
                            "state": "proposed",
                        },
                        {
                            "id": "contract:api.plan_ready",
                            "kind": "contract",
                            "extension_type": "api",
                            "label": "Plan Ready",
                            "contract": {
                                "route": "GET /api/plan-ready",
                                "fields": [{"name": "pricing_score", "required": True, "primary_binding": ""}],
                            },
                            "state": "proposed",
                        },
                    ],
                    "edges": [],
                },
                "updated_by": "plan-first-test",
            },
        )
        self.assertEqual(import_response.status_code, 200)

        latest_plan_path = self.root / "runtime" / "plans" / "latest.plan.md"
        latest_plan_path.write_text(
            """
# API: GET /api/plan-ready
Fields:
- pricing_score <- analytics.market_signals.score_raw

# Table: analytics.market_signals
Columns:
- score_raw (float)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        data_node = observed_nodes["data:signals_table"]
        api_node = observed_nodes["contract:api.plan_ready"]
        data_field_id = next(column["id"] for column in data_node["columns"] if column["name"] == "score_raw")
        pricing_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")

        self.assertEqual(pricing_field["primary_binding"], data_field_id)
        self.assertEqual(bundle["readiness"]["summary"]["required_binding_failures"], 0)
        self.assertFalse(any(issue.get("target_id") == pricing_field["id"] for issue in bundle["readiness"]["issues"]))
        self.assertTrue(
            any(
                patch["type"] == "add_binding"
                and patch.get("field_id") == pricing_field["id"]
                and "plan_yaml" in patch.get("evidence", [])
                for patch in bundle["patches"]
            )
        )
        self.assertIn(str(latest_plan_path.resolve()), bundle["reconciliation"]["comparison"]["plan_paths"])

    def test_rebase_endpoint_preserves_matching_review_state(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "rebase_bundle.md").write_text("GET /api/rebase-bundle\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/rebase_bundle.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_rebase_bundle"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "rebase-preserve"},
        )
        self.assertEqual(import_response.status_code, 200)

        rebase_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/rebase",
            json={"preserve_reviews": True},
        )
        self.assertEqual(rebase_response.status_code, 200)
        payload = rebase_response.json()
        rebased_bundle = payload["bundle"]
        self.assertNotEqual(rebased_bundle["bundle_id"], bundle["bundle_id"])
        self.assertEqual(payload["rebased_from_bundle_id"], bundle["bundle_id"])
        self.assertEqual(payload["transferred_review_count"], 1)
        self.assertEqual(payload["preserved_review_states"]["accepted"], 1)
        self.assertEqual(rebased_bundle["review"]["rebased_from_bundle_id"], bundle["bundle_id"])
        rebased_patch = next(
            patch
            for patch in rebased_bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_rebase_bundle"
        )
        self.assertEqual(rebased_patch["review_state"], "accepted")

        source_bundle = self.client.get(f"/api/structure/bundles/{bundle['bundle_id']}").json()["bundle"]
        self.assertEqual(source_bundle["review"]["merge_status"], "superseded")
        self.assertEqual(source_bundle["review"]["superseded_by_bundle_id"], rebased_bundle["bundle_id"])

        merge_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/merge",
            json={"merged_by": "test-suite"},
        )
        self.assertEqual(merge_response.status_code, 400)
        self.assertIn("superseded", merge_response.json()["detail"].lower())

    def test_rebase_endpoint_reports_dropped_review_when_patch_disappears(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "rebase_drop.md").write_text("GET /api/rebase-drop\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/rebase_drop.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_rebase_drop"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)

        graph = self.client.get("/api/graph").json()["graph"]
        graph["nodes"].append(
            {
                "id": "contract:api.get__api_rebase_drop",
                "kind": "contract",
                "extension_type": "api",
                "label": "GET /api/rebase-drop",
                "contract": {"route": "GET /api/rebase-drop", "fields": []},
                "state": "confirmed",
                "verification_state": "confirmed",
                "confidence": "high",
            }
        )
        import_response = self.client.post("/api/structure/import", json={"spec": graph, "updated_by": "rebase-drop"})
        self.assertEqual(import_response.status_code, 200)

        rebase_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/rebase",
            json={"preserve_reviews": True},
        )
        self.assertEqual(rebase_response.status_code, 200)
        payload = rebase_response.json()
        self.assertEqual(payload["transferred_review_count"], 0)
        self.assertEqual(payload["preserved_review_states"]["accepted"], 0)
        self.assertEqual(payload["dropped_review_count"], 1)
        self.assertEqual(payload["dropped_review_states"]["accepted"], 1)
        self.assertFalse(
            any(patch["target_id"] == "contract:api.get__api_rebase_drop" for patch in payload["bundle"]["patches"])
        )

    def test_rebase_chain_marks_superseded_bundles_in_listing(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "rebase_chain.md").write_text("GET /api/rebase-chain\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/rebase_chain.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle_a = scan_response.json()["bundle"]
        patch_id = next(
            patch["id"]
            for patch in bundle_a["patches"]
            if patch["target_id"] == "contract:api.get__api_rebase_chain"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle_a['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "rebase-chain-a"},
        )
        self.assertEqual(import_response.status_code, 200)

        rebase_a_response = self.client.post(
            f"/api/structure/bundles/{bundle_a['bundle_id']}/rebase",
            json={"preserve_reviews": True},
        )
        self.assertEqual(rebase_a_response.status_code, 200)
        bundle_b = rebase_a_response.json()["bundle"]

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "rebase-chain-b"},
        )
        self.assertEqual(import_response.status_code, 200)

        rebase_b_response = self.client.post(
            f"/api/structure/bundles/{bundle_b['bundle_id']}/rebase",
            json={"preserve_reviews": True},
        )
        self.assertEqual(rebase_b_response.status_code, 200)
        bundle_c = rebase_b_response.json()["bundle"]

        bundles_response = self.client.get("/api/structure/bundles")
        self.assertEqual(bundles_response.status_code, 200)
        bundle_items = {
            item["bundle_id"]: item
            for item in bundles_response.json()["bundles"]
            if item["bundle_id"] in {bundle_a["bundle_id"], bundle_b["bundle_id"], bundle_c["bundle_id"]}
        }

        self.assertEqual(bundle_items[bundle_a["bundle_id"]]["merge_status"], "superseded")
        self.assertEqual(bundle_items[bundle_a["bundle_id"]]["superseded_by_bundle_id"], bundle_b["bundle_id"])
        self.assertFalse(bundle_items[bundle_a["bundle_id"]]["ready_to_merge"])

        self.assertEqual(bundle_items[bundle_b["bundle_id"]]["rebased_from_bundle_id"], bundle_a["bundle_id"])
        self.assertEqual(bundle_items[bundle_b["bundle_id"]]["merge_status"], "superseded")
        self.assertEqual(bundle_items[bundle_b["bundle_id"]]["superseded_by_bundle_id"], bundle_c["bundle_id"])

        self.assertEqual(bundle_items[bundle_c["bundle_id"]]["rebased_from_bundle_id"], bundle_b["bundle_id"])
        self.assertEqual(bundle_items[bundle_c["bundle_id"]]["superseded_by_bundle_id"], "")
        self.assertEqual(bundle_items[bundle_c["bundle_id"]]["merge_plan_status"], "ready")

    def test_contradiction_and_downstream_summaries_group_impacts(self) -> None:
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

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        contradiction_summary = bundle["reconciliation"]["contradiction_summary"]
        self.assertGreaterEqual(contradiction_summary["count"], 1)
        self.assertTrue(any(item["kind"] == "binding_mismatch" for item in contradiction_summary["kinds"]))
        self.assertGreaterEqual(contradiction_summary["severity_counts"]["high"], 1)

        downstream_breakage = bundle["reconciliation"]["downstream_breakage"]
        self.assertGreaterEqual(downstream_breakage["count"], 1)
        self.assertTrue(any(item["source"] == "binding_mismatch" for item in downstream_breakage["by_source"]))
        self.assertTrue(any(item["target_id"] == "field.api_market_snapshot.pricing_score" for item in downstream_breakage["targets"]))
