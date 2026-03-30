from __future__ import annotations

import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile

import polars as pl

from workbench.diff import analyze_contracts, generate_change_plan
from workbench.diagnostics import build_graph_diagnostics
from workbench.profile import build_asset_descriptor, profile_asset, profile_csv, profile_graph
from workbench.store import ROOT_DIR, load_graph
from workbench.types import GraphValidationError, validate_graph
from workbench.validation import build_validation_report
from workbench.views import project_graph


class GraphEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.graph = load_graph()

    def test_graph_validation_accepts_demo_graph(self) -> None:
        validated = validate_graph(self.graph)
        self.assertEqual(len(validated["nodes"]), len(self.graph["nodes"]))

    def test_graph_validation_rejects_invalid_extension(self) -> None:
        broken = deepcopy(self.graph)
        broken["nodes"][0]["extension_type"] = "table"
        with self.assertRaises(GraphValidationError):
            validate_graph(broken)

    def test_views_project_expected_nodes(self) -> None:
        data_view = project_graph(self.graph, "data")
        contract_view = project_graph(self.graph, "contract")
        ui_view = project_graph(self.graph, "ui")

        self.assertTrue(all(node["kind"] != "contract" or node["extension_type"] == "api" for node in contract_view["nodes"]))
        self.assertTrue(any(node["kind"] == "contract" and node["extension_type"] == "ui" for node in ui_view["nodes"]))
        self.assertTrue(all(node["kind"] != "contract" for node in data_view["nodes"]))

    def test_profile_csv_extracts_summary_stats(self) -> None:
        sample_path = ROOT_DIR / "data" / "demo" / "fred_home_price.csv"
        profile = profile_csv(sample_path)
        price_column = next(column for column in profile["columns"] if column["name"] == "price_index")
        self.assertEqual(profile["row_count"], 5)
        self.assertEqual(price_column["data_type"], "float")
        self.assertIn("mean", price_column["stats"])

    def test_profile_graph_marks_schema_only_when_missing(self) -> None:
        graph = deepcopy(self.graph)
        target_node = next(node for node in graph["nodes"] if node["id"] == "data:fred_home_price_raw")
        target_node["data"]["local_path"] = "data/demo/does_not_exist.csv"
        source_node = next(node for node in graph["nodes"] if node["id"] == "source:fred.home_price_series")
        source_node["source"]["raw_assets"] = []
        profiled = profile_graph(graph, ROOT_DIR)
        node = next(node for node in profiled["nodes"] if node["id"] == "data:fred_home_price_raw")
        self.assertEqual(node["profile_status"], "schema_only")

    def test_profile_graph_uses_accessible_source_raw_asset_fallback(self) -> None:
        graph = deepcopy(self.graph)
        data_node = next(node for node in graph["nodes"] if node["id"] == "data:fred_home_price_raw")
        data_node["data"]["local_path"] = ""
        source_node = next(node for node in graph["nodes"] if node["id"] == "source:fred.home_price_series")
        source_node["source"]["raw_assets"] = [
            {
                "label": "Remote landing pointer",
                "kind": "object_storage",
                "format": "csv",
                "value": "s3://demo-raw/fred/nyxrsa/latest.csv",
                "profile_ready": True,
            },
            {
                "label": "Local landing extract",
                "kind": "file",
                "format": "csv",
                "value": "data/demo/fred_home_price.csv",
                "profile_ready": True,
            },
        ]

        profiled = profile_graph(graph, ROOT_DIR)
        node = next(node for node in profiled["nodes"] if node["id"] == "data:fred_home_price_raw")

        self.assertEqual(node["profile_status"], "profiled")
        self.assertEqual(node["data"]["row_count"], 5)
        self.assertEqual(node["data"]["profile_target"], "data/demo/fred_home_price.csv")

    def test_profile_asset_supports_zip_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            zip_path = root / "sample.zip"
            with ZipFile(zip_path, "w") as archive:
                archive.writestr(
                    "sample.csv",
                    "date,value\n2025-01-01,1\n2025-02-01,3\n",
                )

            asset = build_asset_descriptor("sample.zip", root_dir=root, kind="file", fmt="zip_csv")
            self.assertIsNotNone(asset)
            profile = profile_asset(asset, root)

        assert profile is not None
        value_column = next(column for column in profile["columns"] if column["name"] == "value")
        self.assertEqual(profile["row_count"], 2)
        self.assertEqual(value_column["data_type"], "integer")
        self.assertIn("mean", value_column["stats"])

    def test_profile_asset_supports_parquet_collection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            parts_dir = root / "parts"
            parts_dir.mkdir()
            pl.DataFrame({"date": ["2025-01-01", "2025-02-01"], "value": [5.0, 7.5]}).write_parquet(parts_dir / "part-1.parquet")
            pl.DataFrame({"date": ["2025-03-01"], "value": [8.5]}).write_parquet(parts_dir / "part-2.parquet")

            asset = build_asset_descriptor("parts/*.parquet", root_dir=root, kind="glob", fmt="parquet_collection")
            self.assertIsNotNone(asset)
            profile = profile_asset(asset, root)

        assert profile is not None
        self.assertEqual(profile["row_count"], 3)
        value_column = next(column for column in profile["columns"] if column["name"] == "value")
        self.assertEqual(value_column["data_type"], "float")

    def test_change_plan_captures_removed_columns_and_impacted_apis(self) -> None:
        updated = deepcopy(self.graph)
        market_signals = next(node for node in updated["nodes"] if node["id"] == "data:market_signals")
        market_signals["columns"] = [
            column
            for column in market_signals["columns"]
            if column["name"] != "shadow_inventory_index"
        ]
        plan = generate_change_plan(self.graph, validate_graph(updated))

        self.assertIn("data:market_signals.shadow_inventory_index", plan["tiers"]["tier_1"]["removed_columns"])
        self.assertIn("contract:api.market_snapshot", plan["tiers"]["tier_2"]["impacted_apis"])
        self.assertTrue(plan["tiers"]["tier_1"]["contract_violations"])

    def test_contract_diagnostics_flags_unused_api_fields(self) -> None:
        graph = deepcopy(self.graph)
        api_node = next(node for node in graph["nodes"] if node["id"] == "contract:api.market_snapshot")
        api_node["contract"]["fields"].append(
            {"name": "unused_field", "sources": [{"node_id": "data:market_signals", "column": "rent_index"}]}
        )
        diagnostics = analyze_contracts(validate_graph(graph))
        self.assertIn("unused_field", diagnostics["contract:api.market_snapshot"]["unused_fields"])

    def test_contract_diagnostics_accepts_valid_demo_bindings(self) -> None:
        diagnostics = analyze_contracts(validate_graph(self.graph))
        self.assertFalse(diagnostics["contract:api.market_snapshot"]["missing_dependencies"])
        self.assertFalse(diagnostics["contract:ui.pricing_score_card"]["missing_dependencies"])

    def test_graph_diagnostics_rolls_up_ui_health_and_impacts(self) -> None:
        graph = deepcopy(self.graph)
        api_node = next(node for node in graph["nodes"] if node["id"] == "contract:api.market_snapshot")
        pricing_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        pricing_field["sources"] = []

        diagnostics = build_graph_diagnostics(validate_graph(graph), validation_report=build_validation_report(validate_graph(graph)))

        pricing_component = diagnostics["nodes"]["contract:ui.pricing_score_card"]
        api_diagnostics = diagnostics["contracts"]["contract:api.market_snapshot"]["bindings"]["pricing_score"]
        self.assertEqual(api_diagnostics["health"], "broken")
        self.assertIn("PricingScoreCard will not render", api_diagnostics["downstream_impacts"][0])
        self.assertEqual(pricing_component["ui_role"], "component")

    def test_change_plan_tracks_feature_and_dictionary_metadata(self) -> None:
        updated = deepcopy(self.graph)
        model_node = next(node for node in updated["nodes"] if node["id"] == "compute:model.pricing_v1")
        model_node["compute"]["feature_selection"][0]["labels"] = ["pricing", "curated"]
        model_node["compute"]["feature_selection"][0]["category"] = "building_enriched"
        source_node = next(node for node in updated["nodes"] if node["id"] == "source:fred")
        source_node["source"]["data_dictionaries"][0]["value"] = "https://fred.stlouisfed.org/docs/api/fred/series.html"
        source_node["source"]["raw_assets"] = [
            {
                "label": "Provider catalog mirror",
                "kind": "object_storage",
                "format": "csv",
                "value": "s3://demo-raw/fred/catalog.csv",
                "profile_ready": False,
            }
        ]

        plan = generate_change_plan(self.graph, validate_graph(updated))

        self.assertIn("compute:model.pricing_v1", plan["diff"]["changed_feature_selection"])
        self.assertIn("source:fred", plan["diff"]["changed_source_dictionaries"])
        self.assertIn("source:fred", plan["diff"]["changed_source_raw_assets"])

    def test_validation_report_flags_duplicate_contract_fields_and_invalid_feature_refs(self) -> None:
        graph = deepcopy(self.graph)
        api_node = next(node for node in graph["nodes"] if node["id"] == "contract:api.market_snapshot")
        api_node["contract"]["fields"].append({"name": "pricing_score", "sources": []})
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
        report = build_validation_report(validate_graph(graph))
        self.assertGreaterEqual(report["summary"]["errors"], 2)
        self.assertTrue(any(issue["category"] == "duplicate_contract_field" for issue in report["errors"]))
        self.assertTrue(any(issue["category"] == "invalid_feature_ref" for issue in report["errors"]))


if __name__ == "__main__":
    unittest.main()
