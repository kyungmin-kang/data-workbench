from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from pathlib import Path

import workbench.store as store


class PersistenceIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        if os.environ.get("WORKBENCH_RUN_PERSISTENCE_INTEGRATION") != "1":
            self.skipTest("Persistence integration is disabled for this environment.")
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.env_keys = [
            "WORKBENCH_ROOT_DIR",
            "WORKBENCH_PERSISTENCE_BACKEND",
            "WORKBENCH_OBJECT_STORE_BACKEND",
            "WORKBENCH_PERSISTENCE_PREFIX",
        ]
        self.previous_env = {key: os.environ.get(key) for key in self.env_keys}
        os.environ["WORKBENCH_ROOT_DIR"] = str(self.root)
        os.environ["WORKBENCH_PERSISTENCE_BACKEND"] = "postgres"
        os.environ["WORKBENCH_OBJECT_STORE_BACKEND"] = "minio"
        os.environ["WORKBENCH_PERSISTENCE_PREFIX"] = f"integration/{self.root.name}"
        store._get_postgres_document_store.cache_clear()
        store._get_object_store.cache_clear()

        try:
            self.psycopg = importlib.import_module("psycopg")
            self.minio_module = importlib.import_module("minio")
        except ModuleNotFoundError as error:  # pragma: no cover - dependency gated
            self.skipTest(str(error))

    def tearDown(self) -> None:
        for key, value in self.previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        store._get_postgres_document_store.cache_clear()
        store._get_object_store.cache_clear()
        self.temp_dir.cleanup()

    def test_postgres_and_minio_round_trip_graph_bundle_plan_and_presets(self) -> None:
        graph = {
            "metadata": {"name": "Integration Graph", "structure_version": 7},
            "nodes": [],
            "edges": [],
        }
        bundle = {
            "bundle_id": "integration-bundle",
            "base_structure_version": 7,
            "scan": {
                "bundle_id": "integration-bundle",
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
        plan = {
            "markdown": "# Integration Plan\n",
            "tiers": {"tier_1": {"breaking_changes": []}},
        }
        presets = [
            {
                "id": "preset:integration",
                "name": "Integration",
                "description": "Persistence integration preset",
                "root": str(self.root),
            }
        ]

        saved_graph = store.save_graph(graph, updated_by="integration-test", increment_version=False)
        saved_bundle = store.save_bundle(bundle)
        store.save_onboarding_presets_payload(presets)
        artifacts = store.write_plan_artifacts(plan)

        loaded_graph = store.load_graph()
        loaded_bundle = store.load_bundle("integration-bundle")
        listings = store.list_bundles()
        loaded_plan = store.load_latest_plan()
        loaded_presets = store.load_onboarding_presets_payload()

        self.assertEqual(saved_graph["metadata"]["updated_by"], "integration-test")
        self.assertEqual(saved_bundle["bundle_id"], "integration-bundle")
        self.assertEqual(loaded_graph["metadata"]["name"], "Integration Graph")
        self.assertEqual(loaded_bundle["bundle_id"], "integration-bundle")
        self.assertEqual(listings[0]["bundle_id"], "integration-bundle")
        self.assertEqual(loaded_plan["markdown"], "# Integration Plan\n")
        self.assertEqual(loaded_presets, presets)
        self.assertFalse((self.root / "specs" / "structure" / "spec.yaml").exists())
        self.assertEqual(artifacts["remote_latest_json"], store.object_store_uri(store.LATEST_PLAN_JSON_KEY))

        with self.psycopg.connect(os.environ["WORKBENCH_POSTGRES_DSN"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT key FROM workbench_documents WHERE key LIKE %s ORDER BY key",
                    (f"{store.get_persistence_prefix()}%",),
                )
                postgres_keys = {row[0] for row in cursor.fetchall()}

        self.assertIn(store.storage_key(store.GRAPH_JSON_KEY), postgres_keys)
        self.assertIn(store.storage_key(store.LATEST_PLAN_JSON_KEY), postgres_keys)
        self.assertIn(store.storage_key("bundles/integration-bundle.yaml"), postgres_keys)
        self.assertIn(store.storage_key(store.ONBOARDING_PRESETS_KEY), postgres_keys)

        minio_client = self.minio_module.Minio(
            os.environ["WORKBENCH_MINIO_ENDPOINT"],
            access_key=os.environ["WORKBENCH_MINIO_ACCESS_KEY"],
            secret_key=os.environ["WORKBENCH_MINIO_SECRET_KEY"],
            secure=(os.environ.get("WORKBENCH_MINIO_SECURE") or "0") not in {"0", "false", "False"},
        )
        object_names = {
            entry.object_name
            for entry in minio_client.list_objects(
                os.environ["WORKBENCH_MINIO_BUCKET"],
                prefix=store.get_persistence_prefix(),
                recursive=True,
            )
        }

        self.assertIn(store.storage_key(store.GRAPH_JSON_KEY), object_names)
        self.assertIn(store.storage_key(store.LATEST_PLAN_JSON_KEY), object_names)
        self.assertIn(store.storage_key("bundles/integration-bundle.yaml"), object_names)
        self.assertIn(store.storage_key(store.ONBOARDING_PRESETS_KEY), object_names)


if __name__ == "__main__":
    unittest.main()
