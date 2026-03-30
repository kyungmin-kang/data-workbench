from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import polars as pl
from fastapi.testclient import TestClient

from workbench.app import app
from workbench.store import ROOT_DIR


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        shutil.copytree(ROOT_DIR / "data", self.root / "data")
        shutil.copytree(ROOT_DIR / "specs", self.root / "specs")
        shutil.copytree(ROOT_DIR / "src", self.root / "src")
        shutil.copytree(ROOT_DIR / "static", self.root / "static")
        if (ROOT_DIR / "docs").exists():
            shutil.copytree(ROOT_DIR / "docs", self.root / "docs")
        for filename in ("pyproject.toml", "docker-compose.yml", "Dockerfile"):
            source = ROOT_DIR / filename
            if source.exists():
                shutil.copy2(source, self.root / filename)
        (self.root / "runtime" / "plans").mkdir(parents=True, exist_ok=True)
        (self.root / "runtime" / "cache").mkdir(parents=True, exist_ok=True)
        self.previous_root = os.environ.get("WORKBENCH_ROOT_DIR")
        os.environ["WORKBENCH_ROOT_DIR"] = str(self.root)
        self.client = TestClient(app)

    def tearDown(self) -> None:
        if self.previous_root is None:
            os.environ.pop("WORKBENCH_ROOT_DIR", None)
        else:
            os.environ["WORKBENCH_ROOT_DIR"] = self.previous_root
        self.temp_dir.cleanup()

    def test_get_graph_endpoint_returns_graph(self) -> None:
        response = self.client.get("/api/graph")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["graph"]["metadata"]["name"], "Data Workbench Demo Graph")
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
        self.assertEqual(first.json()["bundle"], second.json()["bundle"])

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

    def test_structure_scan_binds_api_fields_from_python_code_references(self) -> None:
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

@router.get("/api/code-ref-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market}
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
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/code-ref-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])
        binding_patches = [
            patch for patch in bundle["patches"]
            if patch["type"] == "add_binding" and patch["target_id"] in {pricing_score_field["id"], market_field["id"]}
        ]
        self.assertEqual(len(binding_patches), 2)
        self.assertTrue(all("code_reference" in patch["evidence"] for patch in binding_patches))

    def test_structure_scan_binds_api_fields_from_helper_serializer(self) -> None:
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

def serialize_signal(signal):
    score = signal.pricing_score
    market_name = signal.market
    return {"pricing_score": score, "market": market_name}

@router.get("/api/helper-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return serialize_signal(signal)
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
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/helper-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_save_endpoint_writes_plan_artifacts(self) -> None:
        graph = json.loads((self.root / "specs" / "workbench.graph.json").read_text(encoding="utf-8"))
        response = self.client.post("/api/graph/save", json={"graph": graph})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(sorted(payload["plan"]["tiers"].keys()), ["tier_1", "tier_2", "tier_3"])
        self.assertTrue((self.root / payload["artifacts"]["latest_markdown"]).exists())

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
        self.assertGreaterEqual(payload["summary"]["data_assets"], 1)
        self.assertGreaterEqual(payload["summary"]["import_suggestions"], 1)
        self.assertGreaterEqual(payload["summary"]["api_contract_hints"], 1)
        self.assertGreaterEqual(payload["summary"]["ui_contract_hints"], 1)
        self.assertTrue(payload["data_assets"])
        self.assertTrue(payload["data_assets"][0]["suggested_import"])
        self.assertTrue(payload["api_contract_hints"])
        self.assertTrue(payload["ui_contract_hints"])

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
                    },
                    "selected_asset_paths": ["data/demo/fred_home_price.csv"],
                    "selected_api_hint_ids": ["api:frontend-routes-py:get-api-custom-pricing"],
                    "selected_ui_hint_ids": [],
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
                    },
                }
            },
        )
        self.assertEqual(update_response.status_code, 200)
        updated = update_response.json()["saved"]
        self.assertEqual(updated["description"], "Updated preset description.")
        self.assertFalse(updated["bootstrap_options"]["assets"])
        self.assertTrue(updated["bootstrap_options"]["uiHints"])

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
        self.assertEqual(grouped_asset["row_count"], 3)
        self.assertEqual(grouped_asset["suggested_import"]["raw_asset_kind"], "glob")
        self.assertEqual(grouped_asset["suggested_import"]["raw_asset_format"], "parquet_collection")

    def test_project_profile_detects_custom_api_and_ui_code_hints(self) -> None:
        backend_dir = self.root / "backend"
        frontend_dir = self.root / "frontend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "routes.py").write_text(
            """
from fastapi import APIRouter
router = APIRouter()

@router.get("/api/custom-health")
def custom_health():
    return {"ok": True}
""".strip(),
            encoding="utf-8",
        )
        (frontend_dir / "MarketDashboard.tsx").write_text(
            """
export function MarketDashboard() {
  return fetch("/api/custom-health");
}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertTrue(any(hint["route"] == "GET /api/custom-health" for hint in payload["api_contract_hints"]))
        self.assertTrue(
            any(
                hint["component"] == "MarketDashboard" and "/api/custom-health" in hint["api_routes"]
                for hint in payload["ui_contract_hints"]
            )
        )

    def test_project_profile_infers_api_response_fields_from_python_route(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class PricingSnapshot(BaseModel):
    pricing_score: float
    rent_index: float

@router.get("/api/pricing-snapshot", response_model=PricingSnapshot)
def pricing_snapshot():
    payload = {"pricing_score": 91.2, "rent_index": 1.04}
    return payload
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/pricing-snapshot")
        self.assertEqual(hint["detected_from"], "fastapi_ast")
        self.assertEqual(hint["response_model"], "PricingSnapshot")
        self.assertEqual(hint["response_fields"], ["pricing_score", "rent_index"])

    def test_project_profile_infers_api_response_field_sources_from_python_route(self) -> None:
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

@router.get("/api/code-ref-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/code-ref-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_helper_and_aliases(self) -> None:
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

def serialize_signal(signal):
    score = signal.pricing_score
    market_name = signal.market
    return {"pricing_score": score, "market": market_name}

@router.get("/api/helper-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    payload = serialize_signal(signal)
    return payload
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/helper-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_piecewise_dict_mutation(self) -> None:
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

@router.get("/api/mutated-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    payload = {}
    payload["pricing_score"] = signal.pricing_score
    payload["market"] = signal.market
    return payload
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/mutated-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_dict_constructor(self) -> None:
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

@router.get("/api/dict-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return dict(pricing_score=signal.pricing_score, market=signal.market)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/dict-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_pydantic_model_dump(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from pydantic import BaseModel
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

class PricingSnapshot(BaseModel):
    pricing_score: float
    market: str

@router.get("/api/pydantic-pricing-snapshot", response_model=PricingSnapshot)
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return PricingSnapshot(pricing_score=signal.pricing_score, market=signal.market).model_dump()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/pydantic-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_list_serializer(self) -> None:
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

def serialize_signal(signal):
    return {"pricing_score": signal.pricing_score, "market": signal.market}

@router.get("/api/list-pricing-snapshot")
def pricing_snapshot(session):
    signals = session.query(MarketSignal).all()
    return [serialize_signal(signal) for signal in signals]
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/list-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_query_helper_chain(self) -> None:
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

def fetch_signal(session):
    return session.query(MarketSignal).first()

def build_snapshot(session):
    signal = fetch_signal(session)
    return {"pricing_score": signal.pricing_score, "market": signal.market}

@router.get("/api/helper-chain-pricing-snapshot")
def pricing_snapshot(session):
    return build_snapshot(session)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/helper-chain-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_serializer_object(self) -> None:
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

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

@router.get("/api/object-serializer-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    serializer = SnapshotSerializer(signal)
    return serializer.to_dict()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/object-serializer-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_service_object_method(self) -> None:
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

def fetch_signal(session):
    return session.query(MarketSignal).first()

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, session):
        self.session = session

    def fetch_signal(self):
        return fetch_signal(self.session)

    def build_snapshot(self):
        signal = self.fetch_signal()
        return SnapshotSerializer(signal).to_dict()

@router.get("/api/service-pricing-snapshot")
def pricing_snapshot(session):
    service = SnapshotService(session)
    return service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/service-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_structure_scan_binds_api_fields_from_pydantic_model_dump(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from pydantic import BaseModel
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

class PricingSnapshot(BaseModel):
    pricing_score: float
    market: str

@router.get("/api/pydantic-scan-pricing-snapshot", response_model=PricingSnapshot)
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    payload = PricingSnapshot(pricing_score=signal.pricing_score, market=signal.market)
    return payload.model_dump()
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
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/pydantic-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_from_service_object_method(self) -> None:
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

def fetch_signal(session):
    return session.query(MarketSignal).first()

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, session):
        self.session = session

    def fetch_signal(self):
        return fetch_signal(self.session)

    def build_snapshot(self):
        signal = self.fetch_signal()
        serializer = SnapshotSerializer(signal)
        return serializer.to_dict()

@router.get("/api/service-scan-pricing-snapshot")
def pricing_snapshot(session):
    return SnapshotService(session).build_snapshot()
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
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/service-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_project_profile_infers_sql_structure_hints(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    median_home_price numeric,
    rent_index numeric
);

create materialized view analytics.market_signals as
select
    inputs.market as market,
    inputs.median_home_price as median_home_price,
    inputs.rent_index as rent_index,
    inputs.median_home_price / nullif(inputs.rent_index, 0) as pricing_score
from analytics.market_inputs as inputs;
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertGreaterEqual(payload["summary"]["sql_structure_hints"], 2)
        table_hint = next(
            hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_inputs"
        )
        view_hint = next(
            hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_signals"
        )
        self.assertEqual(table_hint["object_type"], "table")
        self.assertEqual(
            [field["name"] for field in table_hint["fields"]],
            ["market", "median_home_price", "rent_index"],
        )
        self.assertEqual(view_hint["object_type"], "materialized_view")
        self.assertEqual(view_hint["upstream_relations"], ["analytics.market_inputs"])
        self.assertTrue(any(field["name"] == "pricing_score" for field in view_hint["fields"]))

    def test_project_profile_infers_orm_structure_hints(self) -> None:
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

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertGreaterEqual(payload["summary"]["orm_structure_hints"], 2)
        market_signal_hint = next(
            hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.market_signals"
        )
        snapshot_hint = next(
            hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.market_snapshots"
        )
        self.assertEqual(market_signal_hint["detected_from"], "sqlalchemy_model")
        self.assertEqual(
            [field["name"] for field in market_signal_hint["fields"]],
            ["id", "market", "pricing_score"],
        )
        self.assertEqual(
            [field["data_type"] for field in market_signal_hint["fields"]],
            ["integer", "string", "float"],
        )
        market_signal_id_field = next(field for field in market_signal_hint["fields"] if field["name"] == "id")
        self.assertTrue(market_signal_id_field["primary_key"])
        self.assertEqual(
            [field["name"] for field in snapshot_hint["fields"]],
            ["id", "market_signal_id"],
        )
        self.assertEqual(
            [field["data_type"] for field in snapshot_hint["fields"]],
            ["integer", "integer"],
        )
        snapshot_id_field = next(field for field in snapshot_hint["fields"] if field["name"] == "id")
        self.assertTrue(snapshot_id_field["primary_key"])
        self.assertEqual(snapshot_hint["upstream_relations"], ["analytics.market_signals"])
        snapshot_fk = next(field for field in snapshot_hint["fields"] if field["name"] == "market_signal_id")
        self.assertEqual(snapshot_fk["foreign_key"], "analytics.market_signals.id")
        self.assertEqual(
            snapshot_fk["source_fields"],
            [{"relation": "analytics.market_signals", "column": "id"}],
        )

    def test_project_profile_infers_ui_used_fields_from_fetch_json_flow(self) -> None:
        frontend_dir = self.root / "frontend"
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (frontend_dir / "MarketDashboard.tsx").write_text(
            """
export async function MarketDashboard() {
  const snapshot = await fetch("/api/markets/snapshot").then((response) => response.json());
  const { medianHomePrice } = snapshot;
  return `${snapshot.pricingScore} ${snapshot["rentIndex"]} ${medianHomePrice}`;
}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["ui_contract_hints"] if hint["component"] == "MarketDashboard")
        self.assertEqual(hint["api_routes"], ["/api/markets/snapshot"])
        self.assertEqual(hint["used_fields"], ["medianHomePrice", "pricingScore", "rentIndex"])
        self.assertEqual(
            hint["route_field_hints"]["/api/markets/snapshot"],
            ["medianHomePrice", "pricingScore", "rentIndex"],
        )

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
