from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

from tests.api_test_case import ApiTestCase


class ApiTests(ApiTestCase):
    def test_get_graph_endpoint_returns_graph(self) -> None:
        response = self.client.get("/api/graph")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["graph"]["metadata"]["name"], "Data Workbench Demo Graph")
        self.assertIn("plan_state", payload)
        self.assertIn("source_of_truth", payload)
        self.assertIn("contract:api.market_snapshot", payload["diagnostics"]["contracts"])
        self.assertEqual(payload["validation"]["summary"]["errors"], 0)
        nodes = {node["id"]: node for node in payload["graph"]["nodes"]}
        self.assertTrue(nodes["source:fred"]["source"]["data_dictionaries"])
        self.assertTrue(nodes["source:fred.home_price_series"]["source"]["raw_assets"])
        self.assertTrue(nodes["compute:model.pricing_v1"]["compute"]["feature_selection"][0]["labels"])
        self.assertEqual(payload["diagnostics"]["nodes"]["contract:ui.market_dashboard"]["ui_role"], "screen")
        binding = payload["diagnostics"]["nodes"]["data:market_signals"]["bindings"]["median_home_price"]
        self.assertIn("upstream_count", binding)
        self.assertIn("downstream_count", binding)
        self.assertIsInstance(binding["upstream_count"], int)
        self.assertIsInstance(binding["downstream_count"], int)

    def test_profile_refresh_endpoint_updates_profiles(self) -> None:
        response = self.client.post("/api/profile/refresh")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        nodes = {node["id"]: node for node in payload["graph"]["nodes"]}
        self.assertEqual(nodes["data:fred_home_price_raw"]["data"]["row_count"], 5)
        self.assertEqual(nodes["data:fred_home_price_raw"]["profile_status"], "profiled")
        self.assertEqual(nodes["data:fred_home_price_raw"]["data"]["profile_target"], "data/demo/fred_home_price.csv")

    def test_structure_export_and_import_accept_yaml_text(self) -> None:
        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        self.assertIn("structure_version:", export_response.text)

        import_response = self.client.post(
            "/api/structure/import",
            json={
                "yaml_text": export_response.text,
                "updated_by": "test-suite",
            },
        )
        self.assertEqual(import_response.status_code, 200)
        payload = import_response.json()
        self.assertEqual(payload["structure"]["updated_by"], "test-suite")
        self.assertEqual(payload["graph"]["metadata"]["updated_by"], "test-suite")

    def test_structure_scan_review_and_merge_flow(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "structure_scan.md").write_text("GET /api/demo/markets/readiness\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/structure_scan.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        self.assertTrue(bundle["patches"])
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_demo_markets_readiness"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_bundle = review_response.json()["bundle"]
        reviewed_patch = next(patch for patch in reviewed_bundle["patches"] if patch["id"] == patch_id)
        self.assertEqual(reviewed_patch["review_state"], "accepted")

        merge_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/merge",
            json={"merged_by": "test-suite"},
        )
        self.assertEqual(merge_response.status_code, 200)
        merged_payload = merge_response.json()
        self.assertEqual(merged_payload["structure"]["updated_by"], "test-suite")
        self.assertTrue(
            any(
                node.get("kind") == "contract"
                and node.get("extension_type") == "api"
                and node.get("contract", {}).get("route") == "GET /api/demo/markets/readiness"
                for node in merged_payload["graph"]["nodes"]
            )
        )

    def test_structure_scan_is_idempotent_for_same_inputs(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "structure_scan_repeat.md").write_text("GET /api/demo/repeat\n", encoding="utf-8")

        payload = {
            "role": "scout",
            "scope": "full",
            "root_path": str(self.root),
            "doc_paths": ["docs/structure_scan_repeat.md"],
            "include_internal": False,
        }
        first = self.client.post("/api/structure/scan", json=payload)
        second = self.client.post("/api/structure/scan", json=payload)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["bundle"]["bundle_id"], second.json()["bundle"]["bundle_id"])

    def test_structure_scan_accepts_absolute_doc_paths_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as external_dir:
            external_doc = Path(external_dir) / "serving.md"
            external_doc.write_text(
                """
GET /api/external-serving
data/external.csv
""".strip(),
                encoding="utf-8",
            )

            scan_response = self.client.post(
                "/api/structure/scan",
                json={
                    "role": "scout",
                    "scope": "full",
                    "root_path": str(self.root),
                    "doc_paths": [str(external_doc)],
                    "include_internal": False,
                },
            )
            self.assertEqual(scan_response.status_code, 200)
            bundle = scan_response.json()["bundle"]
            self.assertTrue(
                any(
                    patch.get("target_id") == "contract:api.get__api_external_serving"
                    for patch in bundle.get("patches", [])
                )
            )

    def test_structure_scan_from_openapi_doc_extracts_fields_and_reconciliation(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "pricing_api.yaml").write_text(
            """
openapi: 3.0.0
info:
  title: Pricing API
paths:
  /api/demo/pricing:
    get:
      summary: Demo Pricing Route
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                properties:
                  pricing_score:
                    type: number
                  rent_index:
                    type: number
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/pricing_api.yaml"],
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        self.assertGreaterEqual(bundle["reconciliation"]["summary"]["observed_untracked"], 1)
        observed_api = next(
            node
            for node in bundle["observed"]["nodes"]
            if node["kind"] == "contract" and node["contract"]["route"] == "GET /api/demo/pricing"
        )
        self.assertEqual(
            sorted(field["name"] for field in observed_api["contract"]["fields"]),
            ["pricing_score", "rent_index"],
        )

    def test_structure_batch_review_endpoint_updates_multiple_patches(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "batch_review.md").write_text("GET /api/demo/a\nGET /api/demo/b\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/batch_review.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_ids = [patch["id"] for patch in bundle["patches"][:2]]

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review-batch",
            json={"patch_ids": patch_ids, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_bundle = review_response.json()["bundle"]
        reviewed_states = {
            patch["id"]: patch["review_state"]
            for patch in reviewed_bundle["patches"]
            if patch["id"] in patch_ids
        }
        self.assertEqual(reviewed_states, {patch_id: "accepted" for patch_id in patch_ids})

    def test_structure_batch_review_persists_reviewer_note_and_ready_state(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "batch_review_notes.md").write_text("GET /api/demo/review-notes\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/batch_review_notes.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_ids = [patch["id"] for patch in bundle["patches"]]
        self.assertTrue(patch_ids)

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review-batch",
            json={
                "patch_ids": patch_ids,
                "decision": "accepted",
                "reviewed_by": "review-manager",
                "note": "Reviewed as a clean bundle-level addition.",
            },
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_bundle = review_response.json()["bundle"]
        self.assertEqual(reviewed_bundle["review"]["last_reviewed_by"], "review-manager")
        self.assertEqual(reviewed_bundle["review"]["last_review_note"], "Reviewed as a clean bundle-level addition.")
        for patch in reviewed_bundle["patches"]:
            self.assertEqual(patch["review_state"], "accepted")
            self.assertEqual(patch["reviewed_by"], "review-manager")
            self.assertEqual(patch["review_note"], "Reviewed as a clean bundle-level addition.")
            self.assertTrue(patch["review_history"])

        list_response = self.client.get("/api/structure/bundles")
        self.assertEqual(list_response.status_code, 200)
        bundle_summary = next(
            item
            for item in list_response.json()["bundles"]
            if item["bundle_id"] == bundle["bundle_id"]
        )
        self.assertEqual(bundle_summary["last_reviewed_by"], "review-manager")
        self.assertTrue(bundle_summary["last_reviewed_at"])
        self.assertEqual(bundle_summary["accepted_count"], bundle_summary["patch_count"])
        self.assertEqual(bundle_summary["pending_count"], 0)
        self.assertEqual(
            bundle_summary["ready_to_merge"],
            bundle_summary["review_required_count"] == 0 and not bundle_summary["rebase_required"],
        )

    def test_structure_workflow_endpoint_persists_assignment_triage_and_list_metadata(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "workflow_bundle.md").write_text("GET /api/demo/workflow\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/workflow_bundle.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]

        workflow_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/workflow",
            json={
                "bundle_owner": "platform-data",
                "assigned_reviewer": "review-manager",
                "triage_state": "blocked",
                "triage_note": "Waiting on API owners to confirm the route migration path.",
                "updated_by": "review-manager",
                "note": "Blocked until upstream contract owners sign off.",
            },
        )
        self.assertEqual(workflow_response.status_code, 200)
        updated_bundle = workflow_response.json()["bundle"]
        self.assertEqual(updated_bundle["review"]["bundle_owner"], "platform-data")
        self.assertEqual(updated_bundle["review"]["assigned_reviewer"], "review-manager")
        self.assertEqual(updated_bundle["review"]["triage_state"], "blocked")
        self.assertEqual(updated_bundle["review"]["triage_note"], "Waiting on API owners to confirm the route migration path.")
        self.assertTrue(updated_bundle["review"]["workflow_history"])
        self.assertEqual(updated_bundle["review"]["workflow_history"][-1]["updated_by"], "review-manager")
        self.assertEqual(updated_bundle["review"]["workflow_history"][-1]["changes"]["assigned_reviewer"], "review-manager")

        list_response = self.client.get("/api/structure/bundles")
        self.assertEqual(list_response.status_code, 200)
        bundle_summary = next(
            item
            for item in list_response.json()["bundles"]
            if item["bundle_id"] == bundle["bundle_id"]
        )
        self.assertEqual(bundle_summary["bundle_owner"], "platform-data")
        self.assertEqual(bundle_summary["assigned_reviewer"], "review-manager")
        self.assertEqual(bundle_summary["triage_state"], "blocked")
        self.assertEqual(bundle_summary["triage_note"], "Waiting on API owners to confirm the route migration path.")

    def test_structure_bundle_listing_surfaces_review_counts_and_merge_metadata(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "bundle_listing.md").write_text(
            "GET /api/demo/listing/a\nGET /api/demo/listing/b\nGET /api/demo/listing/c\n",
            encoding="utf-8",
        )

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/bundle_listing.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        self.assertGreaterEqual(len(bundle["patches"]), 3)

        patch_ids = [patch["id"] for patch in bundle["patches"][:3]]
        self.assertEqual(
            self.client.post(
                f"/api/structure/bundles/{bundle['bundle_id']}/review",
                json={"patch_id": patch_ids[0], "decision": "accepted"},
            ).status_code,
            200,
        )
        self.assertEqual(
            self.client.post(
                f"/api/structure/bundles/{bundle['bundle_id']}/review",
                json={"patch_id": patch_ids[1], "decision": "deferred"},
            ).status_code,
            200,
        )
        self.assertEqual(
            self.client.post(
                f"/api/structure/bundles/{bundle['bundle_id']}/review",
                json={"patch_id": patch_ids[2], "decision": "rejected"},
            ).status_code,
            200,
        )

        merge_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/merge",
            json={"merged_by": "test-suite"},
        )
        self.assertEqual(merge_response.status_code, 200)

        list_response = self.client.get("/api/structure/bundles")
        self.assertEqual(list_response.status_code, 200)
        bundle_summary = next(
            item
            for item in list_response.json()["bundles"]
            if item["bundle_id"] == bundle["bundle_id"]
        )
        self.assertEqual(bundle_summary["accepted_count"], 1)
        self.assertEqual(bundle_summary["deferred_count"], 1)
        self.assertEqual(bundle_summary["rejected_count"], 1)
        self.assertEqual(
            bundle_summary["accepted_count"]
            + bundle_summary["deferred_count"]
            + bundle_summary["rejected_count"]
            + bundle_summary["pending_count"],
            bundle_summary["patch_count"],
        )
        self.assertTrue(bundle_summary["merged_at"])
        self.assertEqual(bundle_summary["merged_by"], "test-suite")

    def test_structure_merge_endpoint_rejects_stale_bundle(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "stale_bundle.md").write_text("GET /api/demo/stale-check\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/stale_bundle.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = bundle["patches"][0]["id"]

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted"},
        )
        self.assertEqual(review_response.status_code, 200)

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "stale-check"},
        )
        self.assertEqual(import_response.status_code, 200)

        merge_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/merge",
            json={"merged_by": "test-suite"},
        )
        self.assertEqual(merge_response.status_code, 400)
        self.assertIn("stale", merge_response.json()["detail"])

    def test_structure_scan_surfaces_binding_contradictions_with_downstream_impacts(self) -> None:
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

        contradiction = next(
            item
            for item in bundle["contradictions"]
            if item["field_id"] == "field.api_market_snapshot.pricing_score"
        )
        self.assertTrue(contradiction["review_required"])
        self.assertEqual(contradiction["existing_belief"]["primary_binding"], "field.model_pricing_v1.pricing_score")
        self.assertEqual(contradiction["new_evidence"]["primary_binding"], "field.analytics_market_signals.pricing_score")
        self.assertIn("PricingScoreCard will not render", contradiction["downstream_impacts"][0])

        change_patch = next(
            patch
            for patch in bundle["patches"]
            if patch["type"] == "change_binding"
            and patch["field_id"] == "field.api_market_snapshot.pricing_score"
        )
        self.assertEqual(change_patch["review_state"], "pending")
        self.assertEqual(change_patch["payload"]["previous_binding"], "field.model_pricing_v1.pricing_score")
        self.assertEqual(change_patch["payload"]["new_binding"], "field.analytics_market_signals.pricing_score")
        self.assertEqual(change_patch["evidence"], ["code_reference"])

    def test_structure_contradiction_review_updates_related_patches_and_history(self) -> None:
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

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        contradiction = next(
            item
            for item in bundle["contradictions"]
            if item["field_id"] == "field.api_market_snapshot.pricing_score"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review-contradiction",
            json={
                "contradiction_id": contradiction["id"],
                "decision": "rejected",
                "reviewed_by": "review-manager",
                "note": "Keep canonical binding until model migration is approved.",
            },
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_bundle = review_response.json()["bundle"]
        reviewed_contradiction = next(
            item for item in reviewed_bundle["contradictions"] if item["id"] == contradiction["id"]
        )
        self.assertFalse(reviewed_contradiction["review_required"])
        self.assertEqual(reviewed_contradiction["review_state"], "rejected")
        self.assertEqual(reviewed_contradiction["reviewed_by"], "review-manager")
        self.assertEqual(reviewed_contradiction["review_note"], "Keep canonical binding until model migration is approved.")
        self.assertTrue(reviewed_contradiction["review_history"])

        related_patch = next(
            patch
            for patch in reviewed_bundle["patches"]
            if patch["type"] == "change_binding"
            and patch["field_id"] == "field.api_market_snapshot.pricing_score"
        )
        self.assertEqual(related_patch["review_state"], "rejected")
        self.assertEqual(related_patch["reviewed_by"], "review-manager")
        self.assertEqual(related_patch["review_note"], "Keep canonical binding until model migration is approved.")
        self.assertTrue(related_patch["review_history"])
        plan_state = review_response.json()["plan_state"]
        self.assertTrue(any(blocker["id"].startswith("blocker.review.") for blocker in plan_state["blockers"]))
        self.assertTrue(any(task["status"] == "blocked" for task in plan_state["tasks"]))

    def test_structure_rebase_preview_reports_preserved_and_dropped_reviews(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        review_doc = docs_dir / "rebase_preview.md"
        review_doc.write_text("GET /api/demo/rebase-old\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/rebase_preview.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = bundle["patches"][0]["id"]

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={
                "patch_id": patch_id,
                "decision": "accepted",
                "reviewed_by": "review-manager",
                "note": "Original route proposal approved.",
            },
        )
        self.assertEqual(review_response.status_code, 200)

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "rebase-preview"},
        )
        self.assertEqual(import_response.status_code, 200)

        review_doc.write_text("GET /api/demo/rebase-new\n", encoding="utf-8")

        preview_response = self.client.get(
            f"/api/structure/bundles/{bundle['bundle_id']}/rebase-preview?preserve_reviews=true"
        )
        self.assertEqual(preview_response.status_code, 200)
        preview = preview_response.json()["preview"]
        self.assertGreater(preview["current_version"], preview["base_version"])
        self.assertEqual(preview["transferred_review_count"], 0)
        self.assertGreaterEqual(preview["dropped_review_states"]["accepted"], 1)
        self.assertTrue(preview["changed_targets"])
        self.assertTrue(preview["dropped_review_units"])
        self.assertEqual(preview["dropped_review_units"][0]["decision"], "accepted")

    def test_structure_rebase_preserves_review_units_and_workflow_when_targets_still_match(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        review_doc = docs_dir / "rebase_preserve.md"
        review_doc.write_text("GET /api/demo/rebase-preserve\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/rebase_preserve.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = bundle["patches"][0]["id"]

        workflow_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/workflow",
            json={
                "bundle_owner": "platform-data",
                "assigned_reviewer": "review-manager",
                "triage_state": "in_review",
                "triage_note": "Carry this route forward during the version bump.",
                "updated_by": "review-manager",
            },
        )
        self.assertEqual(workflow_response.status_code, 200)

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={
                "patch_id": patch_id,
                "decision": "accepted",
                "reviewed_by": "review-manager",
                "note": "Route addition approved before the version bump.",
            },
        )
        self.assertEqual(review_response.status_code, 200)

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "rebase-preserve"},
        )
        self.assertEqual(import_response.status_code, 200)

        preview_response = self.client.get(
            f"/api/structure/bundles/{bundle['bundle_id']}/rebase-preview?preserve_reviews=true"
        )
        self.assertEqual(preview_response.status_code, 200)
        preview = preview_response.json()["preview"]
        self.assertGreaterEqual(preview["transferred_review_count"], 1)
        self.assertTrue(preview["preserved_review_units"])
        self.assertEqual(preview["preserved_review_units"][0]["decision"], "accepted")

        rebase_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/rebase",
            json={"preserve_reviews": True},
        )
        self.assertEqual(rebase_response.status_code, 200)
        rebased_bundle = rebase_response.json()["bundle"]
        self.assertEqual(rebased_bundle["review"]["bundle_owner"], "platform-data")
        self.assertEqual(rebased_bundle["review"]["assigned_reviewer"], "review-manager")
        self.assertEqual(rebased_bundle["review"]["triage_state"], "in_review")
        self.assertEqual(rebased_bundle["review"]["triage_note"], "Carry this route forward during the version bump.")

    def test_structure_scan_reconciliation_flags_planned_missing_route(self) -> None:
        graph = self.client.get("/api/graph").json()["graph"]
        graph["nodes"].append(
            {
                "id": "contract:api.missing_pricing_route",
                "kind": "contract",
                "extension_type": "api",
                "label": "Missing Pricing Route",
                "description": "Planned route not yet implemented.",
                "owner": "planning",
                "position": {"x": 0, "y": 0},
                "contract": {
                    "route": "GET /api/missing-pricing",
                    "component": "",
                    "ui_role": "",
                    "fields": [
                        {
                            "id": "field.contract_api_missing_pricing_route.pricing_score",
                            "name": "pricing_score",
                            "required": True,
                            "primary_binding": "",
                            "alternatives": [],
                        }
                    ],
                },
                "columns": [],
                "source": {},
                "data": {},
                "compute": {},
                "state": "proposed",
                "verification_state": "observed",
                "confidence": "low",
                "evidence": ["user_defined"],
            }
        )
        import_response = self.client.post(
            "/api/structure/import",
            json={"spec": graph, "updated_by": "test-suite"},
        )
        self.assertEqual(import_response.status_code, 200)

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        planned_missing = scan_response.json()["bundle"]["reconciliation"]["planned_missing"]
        self.assertTrue(any("GET /api/missing-pricing" in item["message"] for item in planned_missing))

    def test_structure_scan_observes_sql_relations_and_transforms(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    median_home_price numeric,
    rent_index numeric
);

create view analytics.market_signals as
select
    inputs.market as market,
    inputs.median_home_price as median_home_price,
    inputs.rent_index as rent_index
from analytics.market_inputs as inputs;
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
        self.assertIn("data:analytics_market_inputs", observed_nodes)
        self.assertIn("data:analytics_market_signals", observed_nodes)
        self.assertIn("compute:transform.analytics_market_signals", observed_nodes)
        transform_node = observed_nodes["compute:transform.analytics_market_signals"]
        self.assertEqual(transform_node["compute"]["runtime"], "sql")
        self.assertIn("data:analytics_market_inputs", transform_node["compute"]["inputs"])
        self.assertIn("data:analytics_market_signals", transform_node["compute"]["outputs"])
        self.assertTrue(
            any(
                edge["type"] == "produces"
                and edge["source"] == "compute:transform.analytics_market_signals"
                and edge["target"] == "data:analytics_market_signals"
                for edge in bundle["observed"]["edges"]
            )
        )
        self.assertGreaterEqual(bundle["reconciliation"]["summary"]["observed_untracked"], 1)

    def test_structure_scan_observes_orm_relations(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Integer, Numeric, String, ForeignKey
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market: Mapped[str] = mapped_column(String(32))
    pricing_score: Mapped[float] = mapped_column(Numeric)

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_signal_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_signals.id"))
    signal = relationship("MarketSignal")
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
        self.assertIn("data:analytics_market_signals", observed_nodes)
        self.assertIn("data:analytics_market_snapshots", observed_nodes)
        market_signals = observed_nodes["data:analytics_market_signals"]
        snapshots = observed_nodes["data:analytics_market_snapshots"]
        self.assertEqual(
            [column["name"] for column in market_signals["columns"]],
            ["id", "market", "pricing_score"],
        )
        self.assertEqual(
            [column["name"] for column in snapshots["columns"]],
            ["id", "market_signal_id"],
        )
        snapshot_fk = next(column for column in snapshots["columns"] if column["name"] == "market_signal_id")
        self.assertEqual(snapshot_fk["foreign_key"], "analytics.market_signals.id")
        self.assertEqual(len(snapshot_fk["lineage_inputs"]), 1)
        self.assertTrue(
            any(
                edge["type"] == "depends_on"
                and edge["source"] == "data:analytics_market_signals"
                and edge["target"] == "data:analytics_market_snapshots"
                for edge in bundle["observed"]["edges"]
            )
        )
        self.assertGreaterEqual(bundle["reconciliation"]["summary"]["observed_untracked"], 1)

    def test_save_endpoint_writes_plan_artifacts(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post("/api/graph/save", json={"graph": graph})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(sorted(payload["plan"]["tiers"].keys()), ["tier_1", "tier_2", "tier_3"])
        self.assertTrue((self.root / payload["artifacts"]["latest_markdown"]).exists())

    def test_graph_payload_includes_latest_plan_artifacts_after_save(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        save_response = self.client.post("/api/graph/save", json={"graph": graph})
        self.assertEqual(save_response.status_code, 200)

        graph_response = self.client.get("/api/graph")
        self.assertEqual(graph_response.status_code, 200)
        payload = graph_response.json()
        self.assertEqual(payload["latest_artifacts"]["latest_json"], "runtime/plans/latest.plan.json")
        self.assertEqual(payload["latest_artifacts"]["latest_markdown"], "runtime/plans/latest.plan.md")
        self.assertIn("artifact_storage", payload)
        self.assertIn("object_store", payload["artifact_storage"])

    def test_validate_endpoint_returns_blocking_issues_for_invalid_graph(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        api_node = next(node for node in graph["nodes"] if node["id"] == "contract:api.market_snapshot")
        api_node["contract"]["fields"].append({"name": "pricing_score", "sources": []})
        response = self.client.post("/api/graph/validate", json={"graph": graph})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertGreaterEqual(payload["validation"]["summary"]["errors"], 1)
        self.assertTrue(any(issue["category"] == "duplicate_contract_field" for issue in payload["validation"]["errors"]))
        self.assertEqual(payload["diagnostics"]["nodes"]["contract:api.market_snapshot"]["health"], "broken")

    def test_contract_suggestions_endpoint_returns_api_and_ui_candidates(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))

        api_response = self.client.post(
            "/api/contract/suggestions",
            json={
                "graph": graph,
                "node_id": "contract:api.market_snapshot",
                "field_name": "pricing_score",
            },
        )
        self.assertEqual(api_response.status_code, 200)
        api_payload = api_response.json()
        self.assertEqual(api_payload["auto_suggestion"]["source"]["node_id"], "compute:model.pricing_v1")
        self.assertEqual(api_payload["auto_suggestion"]["source"]["column"], "pricing_score")

        ui_response = self.client.post(
            "/api/contract/suggestions",
            json={
                "graph": graph,
                "node_id": "contract:ui.pricing_score_card",
                "field_name": "pricingScoreCard",
            },
        )
        self.assertEqual(ui_response.status_code, 200)
        ui_payload = ui_response.json()
        self.assertEqual(ui_payload["auto_suggestion"]["source"]["node_id"], "contract:api.market_snapshot")
        self.assertEqual(ui_payload["auto_suggestion"]["source"]["field"], "pricing_score")

    def test_save_endpoint_blocks_when_validation_has_errors(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        model_node = next(node for node in graph["nodes"] if node["id"] == "compute:model.pricing_v1")
        model_node["compute"]["feature_selection"].append(
            {
                "column_ref": "data:market_signals.not_real",
                "status": "candidate",
                "persisted": False,
                "stage": "",
                "category": "",
                "labels": [],
                "order": None,
            }
        )
        response = self.client.post("/api/graph/save", json={"graph": graph})
        self.assertEqual(response.status_code, 400)
        self.assertIn("upstream binding missing", response.json()["detail"])

    def test_import_asset_endpoint_adds_profiled_source_and_data_nodes(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post(
            "/api/import/asset",
            json={
                "graph": graph,
                "import_spec": {
                    "source_label": "Imported FRED Extract",
                    "source_extension_type": "object",
                    "source_description": "Imported from onboarding flow.",
                    "source_provider": "FRED",
                    "source_origin_kind": "api_endpoint",
                    "source_origin_value": "https://api.stlouisfed.org/fred/series/observations?series_id=NYXRSA",
                    "raw_asset_label": "Local landing extract",
                    "raw_asset_kind": "file",
                    "raw_asset_format": "unknown",
                    "raw_asset_value": "data/demo/fred_home_price.csv",
                    "data_label": "Imported Home Price Raw",
                    "data_extension_type": "raw_dataset",
                    "update_frequency": "monthly",
                    "persistence": "cold",
                    "persisted": False,
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        nodes = {node["id"]: node for node in payload["graph"]["nodes"]}
        imported_data_id = payload["imported"]["data_node_id"]
        imported_source_id = payload["imported"]["source_node_id"]
        self.assertEqual(nodes[imported_data_id]["data"]["row_count"], 5)
        self.assertEqual(nodes[imported_data_id]["profile_status"], "profiled")
        self.assertEqual(nodes[imported_data_id]["data"]["profile_target"], "data/demo/fred_home_price.csv")
        self.assertEqual(nodes[imported_source_id]["source"]["raw_assets"][0]["value"], "data/demo/fred_home_price.csv")

    def test_import_asset_endpoint_supports_schema_only_bootstrap(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post(
            "/api/import/asset",
            json={
                "graph": graph,
                "import_spec": {
                    "source_label": "Warehouse Export Pointer",
                    "source_extension_type": "bucket_path",
                    "source_provider": "warehouse",
                    "raw_asset_label": "Landing pointer",
                    "raw_asset_kind": "object_storage",
                    "raw_asset_format": "csv",
                    "raw_asset_value": "s3://warehouse-raw/acris/export.csv",
                    "profile_ready": True,
                    "data_label": "Acris Export Raw",
                    "data_extension_type": "raw_dataset",
                    "persistence": "cold",
                    "persisted": False,
                    "schema_columns": [
                        {"name": "doc_id", "data_type": "string"},
                        {"name": "sale_price", "data_type": "float"},
                    ],
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        nodes = {node["id"]: node for node in payload["graph"]["nodes"]}
        imported_data = nodes[payload["imported"]["data_node_id"]]
        self.assertEqual(imported_data["profile_status"], "schema_only")
        self.assertEqual([column["name"] for column in imported_data["columns"]], ["doc_id", "sale_price"])
        self.assertEqual(imported_data["data"]["profile_target"], "s3://warehouse-raw/acris/export.csv")

    def test_bulk_import_endpoint_imports_once_and_skips_duplicates(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        sample_path = self.root / "data" / "demo" / "bulk_import.csv"
        sample_path.write_text("date,value\n2025-01-01,1\n2025-02-01,3\n", encoding="utf-8")
        import_spec = {
            "source_label": "Bulk Asset",
            "source_extension_type": "disk_path",
            "source_provider": "local",
            "raw_asset_label": "Landing copy",
            "raw_asset_kind": "file",
            "raw_asset_format": "csv",
            "raw_asset_value": "data/demo/bulk_import.csv",
            "data_label": "Bulk Asset Raw",
            "data_extension_type": "raw_dataset",
            "persistence": "cold",
            "persisted": False,
        }
        response = self.client.post(
            "/api/import/assets/bulk",
            json={
                "graph": graph,
                "import_specs": [import_spec, import_spec],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["imported"]), 1)
        self.assertEqual(len(payload["skipped"]), 1)
        imported_data_id = payload["imported"][0]["data_node_id"]
        nodes = {node["id"]: node for node in payload["graph"]["nodes"]}
        self.assertEqual(nodes[imported_data_id]["data"]["row_count"], 2)

    def test_import_openapi_endpoint_adds_api_contract_nodes_from_inline_yaml(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        spec_text = """
openapi: 3.0.0
info:
  title: Market Service
paths:
  /api/markets/detail:
    get:
      summary: Market Detail
      tags: [backend]
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                type: object
                properties:
                  median_home_price:
                    type: number
                  pricing_score:
                    type: number
"""
        response = self.client.post(
            "/api/import/openapi",
            json={
                "graph": graph,
                "import_spec": {
                    "spec_text": spec_text,
                    "owner": "backend",
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        imported_ids = payload["imported"]["contract_node_ids"]
        self.assertEqual(len(imported_ids), 1)
        imported_node = next(node for node in payload["graph"]["nodes"] if node["id"] == imported_ids[0])
        self.assertEqual(imported_node["kind"], "contract")
        self.assertEqual(imported_node["contract"]["route"], "GET /api/markets/detail")
        self.assertEqual([field["name"] for field in imported_node["contract"]["fields"]], ["median_home_price", "pricing_score"])

    def test_import_openapi_endpoint_updates_existing_route_and_autobinds_new_field(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        spec_text = """
openapi: 3.0.0
info:
  title: Market Service
paths:
  /api/markets/snapshot:
    get:
      summary: Market Snapshot
      tags: [backend]
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                type: object
                properties:
                  median_home_price:
                    type: number
                  rent_index:
                    type: number
                  pricing_score:
                    type: number
"""
        response = self.client.post(
            "/api/import/openapi",
            json={
                "graph": graph,
                "import_spec": {
                    "spec_text": spec_text,
                    "owner": "backend",
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["imported"]["contract_node_ids"])
        self.assertEqual(payload["imported"]["updated_contract_node_ids"], ["contract:api.market_snapshot"])
        api_node = next(node for node in payload["graph"]["nodes"] if node["id"] == "contract:api.market_snapshot")
        rent_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "rent_index")
        self.assertEqual(rent_field["sources"][0]["node_id"], "data:market_signals")
        self.assertEqual(rent_field["sources"][0]["column"], "rent_index")
        self.assertTrue(
            any(binding["source_ref"] == "data:market_signals.rent_index" for binding in payload["imported"]["binding_summary"]["applied"])
        )

    def test_import_openapi_endpoint_supports_file_path(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        spec_path = self.root / "specs" / "sample.openapi.yaml"
        spec_path.write_text(
            """
openapi: 3.0.0
info:
  title: UI Service
paths:
  /api/dashboard:
    get:
      operationId: dashboard_get
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                type: object
                properties:
                  heroMedianPrice:
                    type: number
                  heroPricingScore:
                    type: number
""".strip(),
            encoding="utf-8",
        )
        response = self.client.post(
            "/api/import/openapi",
            json={
                "graph": graph,
                "import_spec": {
                    "spec_path": "specs/sample.openapi.yaml",
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        imported_node = next(node for node in payload["graph"]["nodes"] if node["id"] == payload["imported"]["contract_node_ids"][0])
        self.assertEqual(imported_node["label"], "dashboard_get")
        self.assertEqual(imported_node["contract"]["route"], "GET /api/dashboard")

    def test_project_profile_endpoint_returns_summary(self) -> None:
        response = self.client.get("/api/project/profile")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertEqual(payload["root"], str(self.root))
        self.assertEqual(payload["summary"]["profiling_mode"], "metadata_only")
        self.assertEqual(payload["cache"]["profiling_mode"], "metadata_only")
        self.assertEqual(payload["cache"]["version"], "5")
        self.assertGreaterEqual(payload["summary"]["data_assets"], 1)
        self.assertGreaterEqual(payload["summary"]["import_suggestions"], 1)
        self.assertGreaterEqual(payload["summary"]["api_contract_hints"], 1)
        self.assertGreaterEqual(payload["summary"]["ui_contract_hints"], 1)
        self.assertTrue(payload["data_assets"])
        self.assertTrue(payload["data_assets"][0]["suggested_import"])
        self.assertEqual(payload["data_assets"][0]["profile_status"], "schema_only")
        self.assertTrue(payload["api_contract_hints"])
        self.assertTrue(payload["ui_contract_hints"])

    def test_project_profile_job_endpoint_completes_with_progress_and_result(self) -> None:
        response = self.client.post(
            "/api/project/profile/jobs",
            json={
                "include_internal": False,
                "root_path": str(self.root),
                "profiling_mode": "metadata_only",
            },
        )
        self.assertEqual(response.status_code, 200)
        job_id = response.json()["job"]["job_id"]

        job = self.wait_for_project_profile_job(job_id)
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["progress"]["phase"], "completed")
        self.assertEqual(job["project_profile"]["root"], str(self.root))
        self.assertGreaterEqual(job["project_profile"]["summary"]["files_scanned"], 1)

    def test_project_profile_endpoint_surfaces_planning_hints_from_latest_plan(self) -> None:
        latest_plan_path = self.root / "runtime" / "plans" / "latest.plan.md"
        latest_plan_path.write_text(
            """
# API: GET /api/profile-planned
Fields:
- pricing_score <- analytics.market_signals.pricing_score
- market <- analytics.market_signals.market

# Table: analytics.market_signals
Columns:
- pricing_score (float)
- market (string)

# Compute: Build analytics.market_features
Inputs: analytics.market_signals
Outputs: analytics.market_features
Columns:
- pricing_delta (float) <- analytics.market_signals.pricing_score
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertGreaterEqual(payload["summary"]["planning_api_hints"], 1)
        self.assertGreaterEqual(payload["summary"]["planning_data_hints"], 1)
        self.assertGreaterEqual(payload["summary"]["planning_compute_hints"], 1)

        api_hint = next(hint for hint in payload["planning_api_hints"] if hint["route"] == "GET /api/profile-planned")
        self.assertEqual(sorted(api_hint["response_fields"]), ["market", "pricing_score"])
        self.assertEqual(sorted(api_hint["required_fields"]), ["market", "pricing_score"])
        pricing_sources = next(item for item in api_hint["response_field_sources"] if item["name"] == "pricing_score")
        self.assertEqual(pricing_sources["source_fields"][0]["relation"], "analytics.market_signals")
        self.assertEqual(pricing_sources["source_fields"][0]["column"], "pricing_score")

        data_hint = next(
            hint for hint in payload["planning_data_hints"] if hint["relation"] == "analytics.market_signals"
        )
        self.assertEqual(sorted(field["name"] for field in data_hint["fields"]), ["market", "pricing_score"])

        compute_hint = next(
            hint for hint in payload["planning_compute_hints"] if hint["relation"] == "analytics.market_features"
        )
        self.assertEqual(compute_hint["inputs"], ["data:analytics_market_signals"])
        self.assertEqual(compute_hint["outputs"], ["data:analytics_market_features"])
        pricing_delta = next(field for field in compute_hint["fields"] if field["name"] == "pricing_delta")
        self.assertEqual(pricing_delta["source_fields"][0]["relation"], "analytics.market_signals")
        self.assertEqual(pricing_delta["source_fields"][0]["column"], "pricing_score")

    def test_project_profile_endpoint_filters_narrative_markdown_headings_from_planning_data_hints(self) -> None:
        latest_plan_path = self.root / "runtime" / "plans" / "latest.plan.md"
        latest_plan_path.write_text(
            """
## Data Flow
This is narrative text about orchestration.

## Data Ingestion Scripts
These scripts fetch data from providers.

## Data Quality Notes
These are process notes for analysts.

## Table: analytics.market_signals
Columns:
- pricing_score (float)
- market (string)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        relations = {hint["relation"] for hint in payload["planning_data_hints"]}
        self.assertIn("analytics.market_signals", relations)
        self.assertNotIn("Flow", relations)
        self.assertNotIn("Ingestion Scripts", relations)
        self.assertNotIn("Quality Notes", relations)

    def test_project_profile_endpoint_keeps_json_partial_graph_data_hints_without_columns(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "plan_graph.json").write_text(
            json.dumps(
                {
                    "metadata": {"name": "Plan Graph"},
                    "nodes": [
                        {
                            "id": "data:analytics_market_signals",
                            "kind": "data",
                            "extension_type": "table",
                            "label": "Market Signals",
                            "tags": ["sql_relation:analytics.market_signals"],
                            "columns": [],
                        }
                    ],
                    "edges": [],
                }
            ),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        relations = {hint["relation"] for hint in payload["planning_data_hints"]}
        self.assertIn("analytics.market_signals", relations)

    def test_project_profile_endpoint_accepts_custom_root_path(self) -> None:
        nested_root = self.root / "examples" / "nested"
        nested_root.mkdir(parents=True, exist_ok=True)
        (nested_root / "package.json").write_text('{"name":"nested-app"}', encoding="utf-8")
        (nested_root / "sample.csv").write_text("date,value\n2025-01-01,1\n", encoding="utf-8")

        response = self.client.get("/api/project/profile", params={"root_path": str(nested_root), "include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertEqual(payload["root"], str(nested_root))
        self.assertEqual(payload["summary"]["manifests"], 1)
        self.assertEqual(payload["summary"]["data_assets"], 1)

    def test_project_profile_skips_generated_or_oversized_hint_files(self) -> None:
        frontend_dir = self.root / "frontend"
        frontend_dir.mkdir(parents=True, exist_ok=True)
        heavy_bundle = frontend_dir / "app.min.js"
        heavy_bundle.write_text(
            "const Dashboard = () => fetch('/api/too-big').then((response) => response.json());\n"
            + ("x" * (900 * 1024)),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertGreaterEqual(payload["summary"]["skipped_heavy_hint_files"], 1)
        self.assertTrue(any("Skipped" in note for note in payload.get("notes", [])))
        self.assertFalse(
            any(
                route == "/api/too-big"
                for hint in payload["ui_contract_hints"]
                for route in hint.get("api_routes", [])
            )
        )

    def test_project_profile_endpoint_reuses_cached_discovery_until_force_refresh(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        route_path = backend_dir / "cached_routes.py"
        route_path.write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/cached-one")
def cached_one():
    return {"cached_one": 1}
""".strip(),
            encoding="utf-8",
        )

        first = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(first.status_code, 200)
        first_profile = first.json()["project_profile"]
        self.assertFalse(first_profile["cache"]["cached"])
        first_routes = {hint["route"] for hint in first_profile["api_contract_hints"]}
        self.assertIn("GET /api/cached-one", first_routes)

        route_path.write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/cached-one")
def cached_one():
    return {"cached_one": 1}

@router.get("/api/cached-two")
def cached_two():
    return {"cached_two": 2}
""".strip(),
            encoding="utf-8",
        )

        cached = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(cached.status_code, 200)
        cached_profile = cached.json()["project_profile"]
        self.assertTrue(cached_profile["cache"]["cached"])
        cached_routes = {hint["route"] for hint in cached_profile["api_contract_hints"]}
        self.assertNotIn("GET /api/cached-two", cached_routes)

        refreshed = self.client.get(
            "/api/project/profile",
            params={"include_internal": "false", "force_refresh": "true"},
        )
        self.assertEqual(refreshed.status_code, 200)
        refreshed_profile = refreshed.json()["project_profile"]
        self.assertFalse(refreshed_profile["cache"]["cached"])
        refreshed_routes = {hint["route"] for hint in refreshed_profile["api_contract_hints"]}
        self.assertIn("GET /api/cached-two", refreshed_routes)

    def test_project_hint_import_can_use_cached_profile_token(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        route_path = backend_dir / "token_import.py"
        route_path.write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/token-import")
def token_import():
    return {"token_value": 1}
""".strip(),
            encoding="utf-8",
        )

        profile_response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(profile_response.status_code, 200)
        project_profile = profile_response.json()["project_profile"]
        hint = next(hint for hint in project_profile["api_contract_hints"] if hint["route"] == "GET /api/token-import")
        token = project_profile["cache"]["token"]
        route_path.unlink()

        graph = self.client.get("/api/graph").json()["graph"]
        import_response = self.client.post(
            "/api/import/project-hint",
            json={
                "graph": graph,
                "hint_kind": "api",
                "hint_id": hint["id"],
                "profile_token": token,
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(import_response.status_code, 200)
        imported_graph = import_response.json()["graph"]
        self.assertTrue(
            any(
                node["kind"] == "contract"
                and node.get("contract", {}).get("route") == "GET /api/token-import"
                for node in imported_graph["nodes"]
            )
        )

    def test_project_bootstrap_can_use_cached_profile_token(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        route_path = backend_dir / "token_bootstrap.py"
        route_path.write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/token-bootstrap")
def token_bootstrap():
    return {"bootstrap_value": 1}
""".strip(),
            encoding="utf-8",
        )

        profile_response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(profile_response.status_code, 200)
        project_profile = profile_response.json()["project_profile"]
        hint = next(hint for hint in project_profile["api_contract_hints"] if hint["route"] == "GET /api/token-bootstrap")
        token = project_profile["cache"]["token"]
        route_path.unlink()

        graph = self.client.get("/api/graph").json()["graph"]
        bootstrap_response = self.client.post(
            "/api/import/project-bootstrap",
            json={
                "graph": graph,
                "profile_token": token,
                "root_path": str(self.root),
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
                and node.get("contract", {}).get("route") == "GET /api/token-bootstrap"
                for node in imported_graph["nodes"]
            )
        )

    def test_structure_scan_can_use_cached_profile_token_and_force_refresh(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        route_path = backend_dir / "token_scan.py"
        route_path.write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/token-scan-one")
def token_scan_one():
    return {"scan_one": 1}
""".strip(),
            encoding="utf-8",
        )

        profile_response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(profile_response.status_code, 200)
        token = profile_response.json()["project_profile"]["cache"]["token"]

        route_path.write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/token-scan-one")
def token_scan_one():
    return {"scan_one": 1}

@router.get("/api/token-scan-two")
def token_scan_two():
    return {"scan_two": 2}
""".strip(),
            encoding="utf-8",
        )

        cached_scan = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "profile_token": token,
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(cached_scan.status_code, 200)
        cached_profile = cached_scan.json()["project_profile"]
        self.assertTrue(cached_profile["cache"]["cached"])
        cached_routes = {hint["route"] for hint in cached_profile["api_contract_hints"]}
        self.assertNotIn("GET /api/token-scan-two", cached_routes)

        refreshed_scan = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "profile_token": token,
                "force_refresh": True,
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(refreshed_scan.status_code, 200)
        refreshed_profile = refreshed_scan.json()["project_profile"]
        self.assertFalse(refreshed_profile["cache"]["cached"])
        refreshed_routes = {hint["route"] for hint in refreshed_profile["api_contract_hints"]}
        self.assertIn("GET /api/token-scan-two", refreshed_routes)

    def test_project_directories_endpoint_returns_matching_directories(self) -> None:
        nested_root = self.root / "examples" / "client_app"
        nested_root.mkdir(parents=True, exist_ok=True)
        (nested_root / "dashboard").mkdir(parents=True, exist_ok=True)
        (nested_root / "data_assets").mkdir(parents=True, exist_ok=True)

        response = self.client.get(
            "/api/project/directories",
            params={"query": "dash", "base_path": str(self.root / "examples")},
        )
        self.assertEqual(response.status_code, 200)
        results = response.json()["directories"]
        self.assertTrue(any(item["path"].endswith("dashboard") for item in results))

    def test_project_directories_endpoint_accepts_exact_path_queries(self) -> None:
        exact_root = self.root / "examples" / "exact_root"
        exact_root.mkdir(parents=True, exist_ok=True)

        response = self.client.get(
            "/api/project/directories",
            params={"query": str(exact_root), "base_path": str(self.root / "examples")},
        )
        self.assertEqual(response.status_code, 200)
        results = response.json()["directories"]
        self.assertTrue(any(item["path"] == str(exact_root) for item in results))

    def test_project_root_check_reports_cache_availability(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        routes_path = backend_dir / "root_check_routes.py"
        routes_path.write_text(
            "\n".join(
                [
                    "from fastapi import APIRouter",
                    "",
                    "router = APIRouter()",
                    "",
                    "@router.get('/api/root-check')",
                    "def root_check():",
                    "    return {'ok': True}",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        self.client.get(
            "/api/project/profile",
            params={
                "root_path": str(self.root),
                "include_internal": "false",
                "profiling_mode": "metadata_only",
            },
        )

        response = self.client.get(
            "/api/project/root-check",
            params={
                "path": str(self.root),
                "include_internal": "false",
                "profiling_mode": "metadata_only",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()["root"]
        self.assertTrue(payload["exists"])
        self.assertTrue(payload["is_directory"])
        self.assertTrue(payload["cache"]["available"])
        self.assertTrue(payload["cache"]["path"].endswith(".json"))

    def test_onboarding_preset_endpoints_save_list_and_delete_presets(self) -> None:
        list_response = self.client.get("/api/onboarding/presets")
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.json()["presets"], [])

        save_response = self.client.post(
            "/api/onboarding/presets",
            json={
                "preset": {
                    "name": "Local Housing Import",
                    "description": "Survey local files and API hints only.",
                    "root": str(self.root),
                    "include_tests": False,
                    "include_internal": False,
                    "bootstrap_options": {
                        "assets": True,
                        "apiHints": True,
                        "uiHints": False,
                        "sqlHints": True,
                        "ormHints": False,
                    },
                    "selected_asset_paths": ["data/demo/fred_home_price.csv"],
                    "selected_api_hint_ids": ["api:frontend-routes-py:get-api-custom-pricing"],
                    "selected_ui_hint_ids": [],
                    "selected_sql_hint_ids": ["sql:analytics.market_signals"],
                    "selected_orm_hint_ids": [],
                }
            },
        )
        self.assertEqual(save_response.status_code, 200)
        saved = save_response.json()["saved"]
        self.assertTrue(saved["id"].startswith("preset:local-housing-import"))

        list_response = self.client.get("/api/onboarding/presets")
        self.assertEqual(list_response.status_code, 200)
        presets = list_response.json()["presets"]
        self.assertEqual(len(presets), 1)
        self.assertEqual(presets[0]["name"], "Local Housing Import")
        self.assertFalse(presets[0]["include_internal"])
        self.assertFalse(presets[0]["bootstrap_options"]["uiHints"])
        self.assertTrue(presets[0]["bootstrap_options"]["sqlHints"])
        self.assertFalse(presets[0]["bootstrap_options"]["ormHints"])

        update_response = self.client.post(
            "/api/onboarding/presets",
            json={
                "preset": {
                    **saved,
                    "description": "Updated preset description.",
                    "bootstrap_options": {
                        "assets": False,
                        "apiHints": True,
                        "uiHints": True,
                        "sqlHints": False,
                        "ormHints": True,
                    },
                }
            },
        )
        self.assertEqual(update_response.status_code, 200)
        updated = update_response.json()["saved"]
        self.assertEqual(updated["description"], "Updated preset description.")
        self.assertFalse(updated["bootstrap_options"]["assets"])
        self.assertTrue(updated["bootstrap_options"]["uiHints"])
        self.assertFalse(updated["bootstrap_options"]["sqlHints"])
        self.assertTrue(updated["bootstrap_options"]["ormHints"])

        delete_response = self.client.delete(f"/api/onboarding/presets/{updated['id']}")
        self.assertEqual(delete_response.status_code, 200)
        final_list_response = self.client.get("/api/onboarding/presets")
        self.assertEqual(final_list_response.status_code, 200)
        self.assertEqual(final_list_response.json()["presets"], [])

    def test_project_profile_groups_split_parquet_and_suggests_import(self) -> None:
        parts_dir = self.root / "data" / "split_metrics"
        parts_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"id": [1, 2], "value": [10.0, 12.5]}).write_parquet(parts_dir / "part-1.parquet")
        pl.DataFrame({"id": [3], "value": [15.5]}).write_parquet(parts_dir / "part-2.parquet")

        response = self.client.get("/api/project/profile")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        grouped_asset = next(
            asset for asset in payload["data_assets"] if asset["path"] == "data/split_metrics/*.parquet"
        )

        self.assertEqual(grouped_asset["format"], "parquet_collection")
        self.assertEqual(grouped_asset["kind"], "glob")
        self.assertEqual(grouped_asset["profile_status"], "schema_only")
        self.assertEqual(grouped_asset["profiling_skipped_reason"], "metadata_only")
        self.assertEqual(grouped_asset["member_count"], 2)
        self.assertEqual(grouped_asset["suggested_import"]["raw_asset_kind"], "glob")
        self.assertEqual(grouped_asset["suggested_import"]["raw_asset_format"], "parquet_collection")

        with patch.dict(os.environ, {"WORKBENCH_PROJECT_PROFILE_ALLOW_PARQUET": "1"}, clear=False):
            refreshed_response = self.client.get(
                "/api/project/profile",
                params={"force_refresh": "true", "profiling_mode": "profile_assets"},
            )
        self.assertEqual(refreshed_response.status_code, 200)
        refreshed_payload = refreshed_response.json()["project_profile"]
        refreshed_asset = next(
            asset for asset in refreshed_payload["data_assets"] if asset["path"] == "data/split_metrics/*.parquet"
        )
        self.assertEqual(refreshed_asset["row_count"], 3)

    def test_project_profile_groups_partitioned_csv_collections(self) -> None:
        monthly_dir = self.root / "data" / "monthly"
        monthly_dir.mkdir(parents=True, exist_ok=True)
        (monthly_dir / "sales_2024-01.csv").write_text("month,value\n2024-01,10\n", encoding="utf-8")
        (monthly_dir / "sales_2024-02.csv").write_text("month,value\n2024-02,12\n", encoding="utf-8")
        (monthly_dir / "customers.csv").write_text("id,name\n1,Ada\n", encoding="utf-8")

        response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        grouped_asset = next(asset for asset in payload["data_assets"] if asset["format"] == "csv_collection")

        self.assertEqual(grouped_asset["path"], "data/monthly/sales*.csv")
        self.assertEqual(grouped_asset["kind"], "glob")
        self.assertEqual(grouped_asset["member_count"], 2)
        self.assertEqual(grouped_asset["group_reason"], "normalized_partition_stem")
        self.assertEqual(grouped_asset["profiling_skipped_reason"], "metadata_only")
        self.assertEqual(grouped_asset["suggested_import"]["raw_asset_format"], "csv_collection")
        individual_paths = {asset["path"] for asset in payload["data_assets"]}
        self.assertIn("data/monthly/customers.csv", individual_paths)

    def test_project_profile_endpoint_can_exclude_paths_per_request(self) -> None:
        raw_dir = self.root / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "excluded.csv").write_text("id,value\n1,2\n", encoding="utf-8")
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "serving.py").write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/per-request-exclude")
def per_request_exclude():
    return {"ok": True}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get(
            "/api/project/profile",
            params=[("include_internal", "false"), ("exclude_paths", "raw")],
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        asset_paths = {asset["path"] for asset in payload["data_assets"]}
        self.assertNotIn("raw/excluded.csv", asset_paths)
        self.assertTrue(payload["cache"]["exclude_paths"])

    def test_project_asset_profile_job_profiles_selected_assets_on_demand(self) -> None:
        response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        asset = next(item for item in payload["data_assets"] if item["path"] == "data/demo/fred_home_price.csv")
        self.assertEqual(asset["profile_status"], "schema_only")

        create_response = self.client.post(
            "/api/project/profile/assets/jobs",
            json={
                "root_path": str(self.root),
                "include_internal": False,
                "profile_token": payload["cache"]["token"],
                "asset_paths": ["data/demo/fred_home_price.csv"],
            },
        )
        self.assertEqual(create_response.status_code, 200)
        job = self.wait_for_project_asset_profile_job(create_response.json()["job"]["job_id"])
        self.assertEqual(job["status"], "completed")
        profiled_asset = job["asset_profiles"][0]
        self.assertEqual(profiled_asset["path"], "data/demo/fred_home_price.csv")
        self.assertEqual(profiled_asset["profile_status"], "profiled")
        self.assertEqual(profiled_asset["row_count"], 5)

    def test_project_asset_profile_job_handles_asset_profile_errors_without_failing_job(self) -> None:
        broken_zip = self.root / "data" / "broken_bundle.zip"
        broken_zip.parent.mkdir(parents=True, exist_ok=True)
        broken_zip.write_bytes(b"not-a-real-zip")

        response = self.client.get("/api/project/profile", params={"include_internal": "false"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        create_response = self.client.post(
            "/api/project/profile/assets/jobs",
            json={
                "root_path": str(self.root),
                "include_internal": False,
                "profile_token": payload["cache"]["token"],
                "asset_paths": ["data/broken_bundle.zip"],
            },
        )
        self.assertEqual(create_response.status_code, 200)
        job = self.wait_for_project_asset_profile_job(create_response.json()["job"]["job_id"])
        self.assertEqual(job["status"], "completed")
        profiled_asset = job["asset_profiles"][0]
        self.assertEqual(profiled_asset["profile_status"], "schema_only")
        self.assertTrue(profiled_asset["error"])

    def test_import_project_hint_seeds_api_fields_and_bindings(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/pricing-snapshot")
def pricing_snapshot():
    return {"pricing_score": 91.2, "rent_index": 1.04}
""".strip(),
            encoding="utf-8",
        )

        profile_response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(profile_response.status_code, 200)
        api_hint = next(
            hint
            for hint in profile_response.json()["project_profile"]["api_contract_hints"]
            if hint["route"] == "GET /api/pricing-snapshot"
        )

        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post(
            "/api/import/project-hint",
            json={
                "graph": graph,
                "hint_kind": "api",
                "hint_id": api_hint["id"],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        node = next(node for node in payload["graph"]["nodes"] if node["id"] == payload["imported"]["node_id"])
        self.assertEqual([field["name"] for field in node["contract"]["fields"]], ["pricing_score", "rent_index"])
        pricing_field = next(field for field in node["contract"]["fields"] if field["name"] == "pricing_score")
        rent_field = next(field for field in node["contract"]["fields"] if field["name"] == "rent_index")
        self.assertEqual(pricing_field["sources"][0]["column"], "pricing_score")
        self.assertEqual(rent_field["sources"][0]["column"], "rent_index")
        self.assertTrue(payload["imported"]["binding_summary"]["applied"])

    def test_import_project_hint_seeds_ui_fields_from_code_usage(self) -> None:
        frontend_dir = self.root / "frontend"
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (frontend_dir / "CustomMarketDashboard.tsx").write_text(
            """
export async function CustomMarketDashboard() {
  const snapshot = await fetch("/api/markets/snapshot").then((response) => response.json());
  return `${snapshot.pricingScore} ${snapshot.medianHomePrice}`;
}
""".strip(),
            encoding="utf-8",
        )

        profile_response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(profile_response.status_code, 200)
        ui_hint = next(
            hint
            for hint in profile_response.json()["project_profile"]["ui_contract_hints"]
            if hint["component"] == "CustomMarketDashboard"
        )

        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post(
            "/api/import/project-hint",
            json={
                "graph": graph,
                "hint_kind": "ui",
                "hint_id": ui_hint["id"],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        node = next(node for node in payload["graph"]["nodes"] if node["id"] == payload["imported"]["node_id"])
        field_names = [field["name"] for field in node["contract"]["fields"]]
        self.assertEqual(sorted(field_names), ["medianHomePrice", "pricingScore"])
        pricing_field = next(field for field in node["contract"]["fields"] if field["name"] == "pricingScore")
        median_field = next(field for field in node["contract"]["fields"] if field["name"] == "medianHomePrice")
        self.assertEqual(pricing_field["sources"][0]["node_id"], "contract:api.market_snapshot")
        self.assertEqual(pricing_field["sources"][0]["field"], "pricing_score")
        self.assertEqual(median_field["sources"][0]["node_id"], "contract:api.market_snapshot")
        self.assertEqual(median_field["sources"][0]["field"], "median_home_price")
        self.assertTrue(payload["imported"]["binding_summary"]["applied"])

    def test_import_project_hint_supports_sql_and_orm_structure_hints(self) -> None:
        sql_dir = self.root / "sql"
        backend_dir = self.root / "backend"
        sql_dir.mkdir(parents=True, exist_ok=True)
        backend_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    median_home_price numeric
);

create materialized view analytics.market_signals as
select
    inputs.market as market,
    inputs.median_home_price as pricing_score
from analytics.market_inputs as inputs;
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Integer, Numeric, String
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market: Mapped[str] = mapped_column(String(32))
    pricing_score: Mapped[float] = mapped_column(Numeric)
""".strip(),
            encoding="utf-8",
        )

        profile_response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(profile_response.status_code, 200)
        project_profile = profile_response.json()["project_profile"]
        sql_hint = next(
            hint for hint in project_profile["sql_structure_hints"] if hint["relation"] == "analytics.market_signals"
        )
        orm_hint = next(
            hint for hint in project_profile["orm_structure_hints"] if hint["relation"] == "analytics.market_snapshots"
        )

        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        sql_response = self.client.post(
            "/api/import/project-hint",
            json={
                "graph": graph,
                "hint_kind": "sql",
                "hint_id": sql_hint["id"],
            },
        )
        self.assertEqual(sql_response.status_code, 200)
        sql_payload = sql_response.json()
        sql_nodes = {node["id"]: node for node in sql_payload["graph"]["nodes"]}
        sql_source_node = next(
            node for node in sql_payload["graph"]["nodes"]
            if node["kind"] == "source" and node.get("source", {}).get("origin", {}).get("value") == "sql/market_signals.sql"
        )
        self.assertIn("compute:transform.analytics_market_signals", sql_nodes)
        self.assertIn("data:analytics_market_signals", sql_nodes)
        self.assertTrue(
            any(
                edge["type"] == "produces"
                and edge["source"] == "compute:transform.analytics_market_signals"
                and edge["target"] == "data:analytics_market_signals"
                for edge in sql_payload["graph"]["edges"]
            )
        )
        self.assertTrue(
            any(
                edge["type"] == "ingests"
                and edge["source"] == sql_source_node["id"]
                and edge["target"] == "compute:transform.analytics_market_signals"
                for edge in sql_payload["graph"]["edges"]
            )
        )

        orm_response = self.client.post(
            "/api/import/project-hint",
            json={
                "graph": sql_payload["graph"],
                "hint_kind": "orm",
                "hint_id": orm_hint["id"],
            },
        )
        self.assertEqual(orm_response.status_code, 200)
        orm_payload = orm_response.json()
        orm_nodes = {node["id"]: node for node in orm_payload["graph"]["nodes"]}
        self.assertIn("data:analytics_market_snapshots", orm_nodes)
        self.assertEqual(orm_payload["imported"]["node_id"], "data:analytics_market_snapshots")
        self.assertTrue(
            any(
                node["kind"] == "source"
                and node.get("source", {}).get("origin", {}).get("value") == "backend/models.py"
                for node in orm_payload["graph"]["nodes"]
            )
        )

    def test_project_bootstrap_endpoint_imports_selected_sql_and_orm_hints(self) -> None:
        sql_dir = self.root / "sql"
        backend_dir = self.root / "backend"
        sql_dir.mkdir(parents=True, exist_ok=True)
        backend_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    median_home_price numeric
);

create materialized view analytics.market_signals as
select
    inputs.market as market,
    inputs.median_home_price as pricing_score
from analytics.market_inputs as inputs;
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Integer, Numeric, String
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market: Mapped[str] = mapped_column(String(32))
    pricing_score: Mapped[float] = mapped_column(Numeric)
""".strip(),
            encoding="utf-8",
        )

        profile_response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(profile_response.status_code, 200)
        project_profile = profile_response.json()["project_profile"]
        sql_hint_id = next(
            hint["id"] for hint in project_profile["sql_structure_hints"] if hint["relation"] == "analytics.market_signals"
        )
        orm_hint_id = next(
            hint["id"] for hint in project_profile["orm_structure_hints"] if hint["relation"] == "analytics.market_snapshots"
        )

        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post(
            "/api/import/project-bootstrap",
            json={
                "graph": graph,
                "include_internal": False,
                "sql_hint_ids": [sql_hint_id],
                "orm_hint_ids": [orm_hint_id],
                "import_assets": False,
                "import_api_hints": False,
                "import_ui_hints": False,
                "import_sql_hints": True,
                "import_orm_hints": True,
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["imported"]["selection"]["sql_hint_ids"], [sql_hint_id])
        self.assertEqual(payload["imported"]["selection"]["orm_hint_ids"], [orm_hint_id])
        self.assertTrue(payload["imported"]["sql_created"])
        self.assertTrue(payload["imported"]["orm_created"])
        nodes = {node["id"]: node for node in payload["graph"]["nodes"]}
        self.assertIn("data:analytics_market_signals", nodes)
        self.assertIn("compute:transform.analytics_market_signals", nodes)
        self.assertIn("data:analytics_market_snapshots", nodes)
        self.assertTrue(
            any(
                node["kind"] == "source"
                and node.get("source", {}).get("origin", {}).get("value") == "sql/market_signals.sql"
                for node in payload["graph"]["nodes"]
            )
        )
        self.assertTrue(
            any(
                node["kind"] == "source"
                and node.get("source", {}).get("origin", {}).get("value") == "backend/models.py"
                for node in payload["graph"]["nodes"]
            )
        )

    def test_project_bootstrap_endpoint_imports_selected_assets_and_hints(self) -> None:
        backend_dir = self.root / "backend"
        frontend_dir = self.root / "frontend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "custom_pricing.py").write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/custom-pricing")
def custom_pricing():
    return {"pricing_score": 88.1, "median_home_price": 740000}
""".strip(),
            encoding="utf-8",
        )
        (frontend_dir / "CustomPricingPanel.tsx").write_text(
            """
export async function CustomPricingPanel() {
  const snapshot = await fetch("/api/custom-pricing").then((response) => response.json());
  return `${snapshot.pricingScore} ${snapshot.medianHomePrice}`;
}
""".strip(),
            encoding="utf-8",
        )
        bootstrap_asset_path = self.root / "data" / "demo" / "bootstrap_metrics.csv"
        bootstrap_asset_path.write_text("date,value\n2025-01-01,7\n2025-02-01,9\n", encoding="utf-8")

        profile_response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(profile_response.status_code, 200)
        project_profile = profile_response.json()["project_profile"]
        api_hint_id = next(
            hint["id"]
            for hint in project_profile["api_contract_hints"]
            if hint["route"] == "GET /api/custom-pricing"
        )
        ui_hint_id = next(
            hint["id"]
            for hint in project_profile["ui_contract_hints"]
            if hint["component"] == "CustomPricingPanel"
        )

        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post(
            "/api/import/project-bootstrap",
            json={
                "graph": graph,
                "include_internal": False,
                "asset_paths": ["data/demo/bootstrap_metrics.csv"],
                "api_hint_ids": [api_hint_id],
                "ui_hint_ids": [ui_hint_id],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["imported"]["asset_imported"]), 1)
        self.assertEqual(payload["imported"]["asset_skipped"], [])
        self.assertEqual(payload["imported"]["api_created"], ["contract:api.get--api-custom-pricing"])
        self.assertEqual(payload["imported"]["ui_created"], ["contract:ui.custompricingpanel"])

        nodes = {node["id"]: node for node in payload["graph"]["nodes"]}
        imported_data_id = payload["imported"]["asset_imported"][0]["data_node_id"]
        self.assertEqual(nodes[imported_data_id]["data"]["row_count"], 2)

        api_node = nodes["contract:api.get--api-custom-pricing"]
        self.assertEqual(api_node["contract"]["route"], "GET /api/custom-pricing")
        self.assertEqual([field["name"] for field in api_node["contract"]["fields"]], ["median_home_price", "pricing_score"])

        ui_node = nodes["contract:ui.custompricingpanel"]
        self.assertEqual(sorted(field["name"] for field in ui_node["contract"]["fields"]), ["medianHomePrice", "pricingScore"])
        self.assertTrue(any(edge["source"] == api_node["id"] and edge["target"] == ui_node["id"] for edge in payload["graph"]["edges"]))

    def test_project_bootstrap_endpoint_skips_existing_assets_and_updates_existing_hints(self) -> None:
        backend_dir = self.root / "backend"
        frontend_dir = self.root / "frontend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "custom_pricing.py").write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/custom-pricing")
def custom_pricing():
    return {"pricing_score": 88.1, "median_home_price": 740000}
""".strip(),
            encoding="utf-8",
        )
        (frontend_dir / "CustomPricingPanel.tsx").write_text(
            """
export async function CustomPricingPanel() {
  const snapshot = await fetch("/api/custom-pricing").then((response) => response.json());
  return `${snapshot.pricingScore} ${snapshot.medianHomePrice}`;
}
""".strip(),
            encoding="utf-8",
        )
        bootstrap_asset_path = self.root / "data" / "demo" / "bootstrap_metrics.csv"
        bootstrap_asset_path.write_text("date,value\n2025-01-01,7\n2025-02-01,9\n", encoding="utf-8")

        profile_response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(profile_response.status_code, 200)
        project_profile = profile_response.json()["project_profile"]
        api_hint_id = next(
            hint["id"]
            for hint in project_profile["api_contract_hints"]
            if hint["route"] == "GET /api/custom-pricing"
        )
        ui_hint_id = next(
            hint["id"]
            for hint in project_profile["ui_contract_hints"]
            if hint["component"] == "CustomPricingPanel"
        )

        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        first_response = self.client.post(
            "/api/import/project-bootstrap",
            json={
                "graph": graph,
                "include_internal": False,
                "asset_paths": ["data/demo/bootstrap_metrics.csv"],
                "api_hint_ids": [api_hint_id],
                "ui_hint_ids": [ui_hint_id],
            },
        )
        self.assertEqual(first_response.status_code, 200)
        updated_graph = first_response.json()["graph"]

        second_response = self.client.post(
            "/api/import/project-bootstrap",
            json={
                "graph": updated_graph,
                "include_internal": False,
                "asset_paths": ["data/demo/bootstrap_metrics.csv"],
                "api_hint_ids": [api_hint_id],
                "ui_hint_ids": [ui_hint_id],
            },
        )
        self.assertEqual(second_response.status_code, 200)
        payload = second_response.json()
        self.assertEqual(payload["imported"]["asset_imported"], [])
        self.assertEqual(len(payload["imported"]["asset_skipped"]), 1)
        self.assertEqual(payload["imported"]["api_created"], [])
        self.assertEqual(payload["imported"]["ui_created"], [])
        self.assertEqual(payload["imported"]["api_updated"], ["contract:api.get--api-custom-pricing"])
        self.assertEqual(payload["imported"]["ui_updated"], ["contract:ui.custompricingpanel"])

        api_nodes = [
            node for node in payload["graph"]["nodes"]
            if node["kind"] == "contract" and node["extension_type"] == "api" and node["contract"]["route"] == "GET /api/custom-pricing"
        ]
        ui_nodes = [
            node for node in payload["graph"]["nodes"]
            if node["kind"] == "contract" and node["extension_type"] == "ui" and node["contract"]["component"] == "CustomPricingPanel"
        ]
        self.assertEqual(len(api_nodes), 1)
        self.assertEqual(len(ui_nodes), 1)

    def test_project_bootstrap_endpoint_respects_category_flags(self) -> None:
        backend_dir = self.root / "backend"
        frontend_dir = self.root / "frontend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "custom_pricing.py").write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/custom-pricing")
def custom_pricing():
    return {"pricing_score": 88.1}
""".strip(),
            encoding="utf-8",
        )
        (frontend_dir / "CustomPricingPanel.tsx").write_text(
            """
export async function CustomPricingPanel() {
  const snapshot = await fetch("/api/custom-pricing").then((response) => response.json());
  return `${snapshot.pricingScore}`;
}
""".strip(),
            encoding="utf-8",
        )
        bootstrap_asset_path = self.root / "data" / "demo" / "bootstrap_metrics.csv"
        bootstrap_asset_path.write_text("date,value\n2025-01-01,7\n", encoding="utf-8")

        profile_response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(profile_response.status_code, 200)
        project_profile = profile_response.json()["project_profile"]
        api_hint_id = next(
            hint["id"]
            for hint in project_profile["api_contract_hints"]
            if hint["route"] == "GET /api/custom-pricing"
        )
        ui_hint_id = next(
            hint["id"]
            for hint in project_profile["ui_contract_hints"]
            if hint["component"] == "CustomPricingPanel"
        )

        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post(
            "/api/import/project-bootstrap",
            json={
                "graph": graph,
                "include_internal": False,
                "asset_paths": ["data/demo/bootstrap_metrics.csv"],
                "api_hint_ids": [api_hint_id],
                "ui_hint_ids": [ui_hint_id],
                "import_assets": False,
                "import_api_hints": True,
                "import_ui_hints": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["imported"]["asset_imported"], [])
        self.assertEqual(payload["imported"]["asset_skipped"], [])
        self.assertEqual(payload["imported"]["api_created"], ["contract:api.get--api-custom-pricing"])
        self.assertEqual(payload["imported"]["ui_created"], [])
        self.assertEqual(payload["imported"]["selection"]["asset_paths"], [])
        self.assertEqual(payload["imported"]["selection"]["api_hint_ids"], [api_hint_id])
        self.assertEqual(payload["imported"]["selection"]["ui_hint_ids"], [])


if __name__ == "__main__":
    unittest.main()
