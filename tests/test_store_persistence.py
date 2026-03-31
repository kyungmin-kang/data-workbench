from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import workbench.store as store


class FakeDocumentStore:
    def __init__(self) -> None:
        self.documents: dict[str, str] = {}

    def read_text(self, key: str) -> str | None:
        return self.documents.get(key)

    def write_text(self, key: str, content: str, *, content_type: str = "text/plain") -> None:
        self.documents[key] = content

    def list_documents(self, prefix: str) -> list[tuple[str, str]]:
        return [
            (key, value)
            for key, value in sorted(self.documents.items())
            if key.startswith(prefix)
        ]


class StorePersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.previous_root = os.environ.get("WORKBENCH_ROOT_DIR")
        os.environ["WORKBENCH_ROOT_DIR"] = str(self.root)

    def tearDown(self) -> None:
        if self.previous_root is None:
            os.environ.pop("WORKBENCH_ROOT_DIR", None)
        else:
            os.environ["WORKBENCH_ROOT_DIR"] = self.previous_root
        self.temp_dir.cleanup()

    def test_postgres_backend_round_trips_graph_without_local_spec_files(self) -> None:
        fake_store = FakeDocumentStore()
        graph = {
            "metadata": {"name": "Persisted Graph", "structure_version": 3},
            "nodes": [],
            "edges": [],
        }

        with patch.dict(
            os.environ,
            {
                "WORKBENCH_PERSISTENCE_BACKEND": "postgres",
                "WORKBENCH_POSTGRES_DSN": "postgresql://demo",
            },
            clear=False,
        ), patch.object(store, "_get_postgres_document_store", return_value=fake_store):
            saved = store.save_graph(graph, updated_by="postgres-test", increment_version=False)
            loaded = store.load_graph()

        self.assertEqual(saved["metadata"]["updated_by"], "postgres-test")
        self.assertEqual(loaded["metadata"]["name"], "Persisted Graph")
        self.assertIn(store.storage_key(store.GRAPH_JSON_KEY), fake_store.documents)
        self.assertIn(store.storage_key(store.GRAPH_YAML_KEY), fake_store.documents)
        self.assertFalse((self.root / "specs" / "structure" / "spec.yaml").exists())

    def test_mirror_backend_writes_local_and_external_plans_and_presets(self) -> None:
        fake_store = FakeDocumentStore()
        fake_object_store = FakeDocumentStore()
        presets = [
            {
                "id": "preset:demo",
                "name": "Demo",
                "description": "Mirror mode preset",
                "root": str(self.root),
            }
        ]
        plan = {
            "markdown": "# Demo Plan\n",
            "tiers": {"tier_1": {"breaking_changes": []}},
        }

        with patch.dict(
            os.environ,
            {
                "WORKBENCH_PERSISTENCE_BACKEND": "mirror",
                "WORKBENCH_POSTGRES_DSN": "postgresql://demo",
                "WORKBENCH_OBJECT_STORE_BACKEND": "minio",
                "WORKBENCH_MINIO_BUCKET": "workbench-artifacts",
            },
            clear=False,
        ), patch.object(store, "_get_postgres_document_store", return_value=fake_store), patch.object(store, "_get_object_store", return_value=fake_object_store):
            store.save_onboarding_presets_payload(presets)
            artifacts = store.write_plan_artifacts(plan)
            loaded_plan = store.load_latest_plan()

        presets_path = self.root / "specs" / "onboarding_presets.json"
        self.assertTrue(presets_path.exists())
        self.assertEqual(json.loads(presets_path.read_text(encoding="utf-8")), presets)
        self.assertEqual(json.loads(fake_store.documents[store.storage_key(store.ONBOARDING_PRESETS_KEY)]), presets)
        self.assertEqual(json.loads(fake_object_store.documents[store.storage_key(store.ONBOARDING_PRESETS_KEY)]), presets)
        self.assertTrue((self.root / artifacts["latest_json"]).exists())
        self.assertTrue((self.root / artifacts["latest_markdown"]).exists())
        self.assertIn(store.storage_key(store.LATEST_PLAN_JSON_KEY), fake_store.documents)
        self.assertIn(store.storage_key(store.LATEST_PLAN_JSON_KEY), fake_object_store.documents)
        self.assertEqual(store.load_latest_plan_artifacts(), artifacts)
        self.assertEqual(json.loads(fake_store.documents[store.storage_key(store.LATEST_PLAN_ARTIFACTS_KEY)]), artifacts)
        self.assertEqual(json.loads(fake_object_store.documents[store.storage_key(store.LATEST_PLAN_ARTIFACTS_KEY)]), artifacts)
        self.assertEqual(artifacts["remote_latest_json"], "minio://workbench-artifacts/plans/latest.plan.json")
        self.assertEqual(loaded_plan["markdown"], "# Demo Plan\n")

    def test_postgres_backend_round_trips_bundle_listing(self) -> None:
        fake_store = FakeDocumentStore()
        bundle = {
            "bundle_id": "bundle-1",
            "base_structure_version": 1,
            "scan": {
                "bundle_id": "bundle-1",
                "role": "scout",
                "scope": "full",
                "created_at": "2026-03-30T00:00:00+00:00",
            },
            "observed": {"nodes": [], "edges": []},
            "patches": [],
            "contradictions": [],
            "impacts": [],
            "reconciliation": {},
            "review": {},
            "readiness": {},
        }

        with patch.dict(
            os.environ,
            {
                "WORKBENCH_PERSISTENCE_BACKEND": "postgres",
                "WORKBENCH_POSTGRES_DSN": "postgresql://demo",
            },
            clear=False,
        ), patch.object(store, "_get_postgres_document_store", return_value=fake_store):
            saved = store.save_bundle(bundle)
            loaded = store.load_bundle("bundle-1")
            listings = store.list_bundles()

        self.assertEqual(saved["bundle_id"], "bundle-1")
        self.assertEqual(loaded["bundle_id"], "bundle-1")
        self.assertEqual(listings[0]["bundle_id"], "bundle-1")
        self.assertIn(store.storage_key("bundles/bundle-1.yaml"), fake_store.documents)

    def test_object_store_backend_round_trips_graph_and_bundles(self) -> None:
        fake_object_store = FakeDocumentStore()
        graph = {
            "metadata": {"name": "Object Store Graph", "structure_version": 2},
            "nodes": [],
            "edges": [],
        }
        bundle = {
            "bundle_id": "bundle-object",
            "base_structure_version": 1,
            "scan": {
                "bundle_id": "bundle-object",
                "role": "scout",
                "scope": "full",
                "created_at": "2026-03-30T00:00:00+00:00",
            },
            "observed": {"nodes": [], "edges": []},
            "patches": [],
            "contradictions": [],
            "impacts": [],
            "reconciliation": {},
            "review": {},
            "readiness": {},
        }

        with patch.dict(
            os.environ,
            {
                "WORKBENCH_OBJECT_STORE_BACKEND": "minio",
                "WORKBENCH_MINIO_BUCKET": "workbench-artifacts",
            },
            clear=False,
        ), patch.object(store, "_get_object_store", return_value=fake_object_store):
            saved_graph = store.save_graph(graph, updated_by="object-store-test", increment_version=False)
            loaded_graph = store.load_graph()
            saved_bundle = store.save_bundle(bundle)
            loaded_bundle = store.load_bundle("bundle-object")
            listings = store.list_bundles()

        self.assertEqual(saved_graph["metadata"]["updated_by"], "object-store-test")
        self.assertEqual(loaded_graph["metadata"]["name"], "Object Store Graph")
        self.assertEqual(saved_bundle["bundle_id"], "bundle-object")
        self.assertEqual(loaded_bundle["bundle_id"], "bundle-object")
        self.assertEqual(listings[0]["bundle_id"], "bundle-object")
        self.assertIn(store.storage_key(store.GRAPH_JSON_KEY), fake_object_store.documents)
        self.assertIn(store.storage_key("bundles/bundle-object.yaml"), fake_object_store.documents)


if __name__ == "__main__":
    unittest.main()
