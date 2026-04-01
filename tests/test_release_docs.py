from __future__ import annotations

import unittest
from pathlib import Path

from tests.api_test_case import ApiTestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
README_PATH = REPO_ROOT / "README.md"
CONTRIBUTING_PATH = REPO_ROOT / "CONTRIBUTING.md"
ARCHITECTURE_PATH = REPO_ROOT / "docs" / "architecture.md"
QUICKSTART_PATH = REPO_ROOT / "docs" / "onboarding_wizard_quickstart.md"
DEMO_README_PATH = REPO_ROOT / "examples" / "onboarding_wizard_demo" / "README.md"
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"
DOCKER_COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
RELEASE_NOTES_PATH = REPO_ROOT / "docs" / "release_notes_0_2_0.md"
RELEASE_CHECKLIST_PATH = REPO_ROOT / "docs" / "release_checklist_0_2_0.md"


class ReleaseDocsParityTests(ApiTestCase):
    def test_public_docs_align_on_demo_walkthrough(self) -> None:
        readme = README_PATH.read_text(encoding="utf-8")
        contributing = CONTRIBUTING_PATH.read_text(encoding="utf-8")
        quickstart = QUICKSTART_PATH.read_text(encoding="utf-8")
        demo = DEMO_README_PATH.read_text(encoding="utf-8")

        self.assertIn("examples/onboarding_wizard_demo", readme)
        self.assertIn("examples/onboarding_wizard_demo", quickstart)
        for content in (readme, quickstart, demo):
            self.assertIn("latest plan", content)

        self.assertIn("docker compose up --build", readme)
        self.assertIn("docker compose up --build", quickstart)
        self.assertIn("Execution State", quickstart)
        self.assertIn("Bootstrap graph", quickstart)
        self.assertIn("Save the graph", demo)
        self.assertIn("Run a structure scan", demo)
        self.assertIn("usable preview", readme)
        self.assertIn("formal `0.2.0`", readme)
        self.assertIn("public-preview stage", contributing)

    def test_docs_match_frozen_public_contract_and_runtime_story(self) -> None:
        readme = README_PATH.read_text(encoding="utf-8")
        architecture = ARCHITECTURE_PATH.read_text(encoding="utf-8")
        release_notes = RELEASE_NOTES_PATH.read_text(encoding="utf-8")
        response = self.client.get("/api/source-of-truth")
        self.assertEqual(response.status_code, 200)
        contract = response.json()["source_of_truth"]["contract"]

        self.assertEqual(contract["default_runtime_mode"], "docker-compose")
        self.assertEqual(contract["supported_runtime_modes"], ["docker-compose", "local-python"])
        self.assertEqual(contract["supported_platforms"], ["macos", "linux"])

        for endpoint in (
            "/api/source-of-truth",
            "/api/plan-state",
            "/api/agent-contracts",
            "/api/agent-contracts/{id}/brief",
            "/api/agent-contracts/{id}/workflow",
            "/api/agent-contracts/{id}/launch",
        ):
            self.assertIn(endpoint, readme)
            self.assertIn(endpoint, architecture)
            if endpoint != "/api/plan-state":
                self.assertIn(endpoint, release_notes)
            self.assertIn(endpoint, contract["supported_endpoints"])

        self.assertIn("Docker Compose: primary", readme)
        self.assertIn("local Python: contributor path", readme)
        self.assertIn("Windows may work through Docker Desktop", readme)
        self.assertIn("Docker Compose is the primary public runtime path.", architecture)
        self.assertIn("Windows may work through Docker", architecture)
        self.assertIn("Local Python setup is the contributor path.", architecture)
        self.assertIn("Docker-first", release_notes)
        self.assertIn("Stable in this release", release_notes)
        self.assertIn("Experimental or intentionally light", release_notes)
        self.assertIn("Good first contribution areas", release_notes)
        self.assertIn("prepared release note draft", release_notes)
        self.assertIn("publicly before that tag as a preview", release_notes)
        self.assertIn("Windows may work through Docker", release_notes)


class ReleaseReadinessFilesTests(unittest.TestCase):
    def test_release_smoke_files_and_demo_assets_exist(self) -> None:
        self.assertTrue(README_PATH.exists())
        self.assertTrue(CONTRIBUTING_PATH.exists())
        self.assertTrue(ARCHITECTURE_PATH.exists())
        self.assertTrue(QUICKSTART_PATH.exists())
        self.assertTrue(DEMO_README_PATH.exists())
        self.assertTrue(RELEASE_NOTES_PATH.exists())
        self.assertTrue(RELEASE_CHECKLIST_PATH.exists())
        self.assertTrue(ENV_EXAMPLE_PATH.exists())
        self.assertTrue(DOCKER_COMPOSE_PATH.exists())
        self.assertTrue((REPO_ROOT / "examples" / "onboarding_wizard_demo" / "backend" / "demo_routes.py").exists())
        self.assertTrue((REPO_ROOT / "examples" / "onboarding_wizard_demo" / "frontend" / "DemoPricingPanel.tsx").exists())
        self.assertTrue((REPO_ROOT / "examples" / "onboarding_wizard_demo" / "data" / "demo_pricing.csv").exists())

    def test_docker_compose_and_env_example_support_documented_runtime(self) -> None:
        compose = DOCKER_COMPOSE_PATH.read_text(encoding="utf-8")
        env_example = ENV_EXAMPLE_PATH.read_text(encoding="utf-8")

        for service_name in ("app:", "worker:", "postgres:", "minio:"):
            self.assertIn(service_name, compose)
        self.assertIn("command: python -m workbench.app", compose)
        self.assertIn("healthcheck:", compose)
        self.assertIn("WORKBENCH_PERSISTENCE_BACKEND", env_example)
        self.assertIn("WORKBENCH_OBJECT_STORE_BACKEND", env_example)
        self.assertIn("WORKBENCH_MINIO_ENDPOINT", env_example)

    def test_release_checklist_covers_final_gates(self) -> None:
        checklist = RELEASE_CHECKLIST_PATH.read_text(encoding="utf-8")
        for phrase in (
            "docker compose up --build",
            "PYTHONPATH=src python -m unittest discover -s tests",
            "browser E2E suite passes",
            "persistence integration suite passes",
            "agent workflow brief and guided launch loop works",
            "release notes ready to use as the GitHub release body",
        ):
            self.assertIn(phrase, checklist)
