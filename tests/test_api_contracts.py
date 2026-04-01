from __future__ import annotations

import json
import unittest
from pathlib import Path

from tests.api_test_case import ApiTestCase


EXPECTED_SUPPORTED_ENDPOINTS = [
    "/api/source-of-truth",
    "/api/plan-state",
    "/api/plan-state/derive-tasks",
    "/api/agent-contracts",
    "/api/agent-contracts/{id}/brief",
    "/api/agent-contracts/{id}/workflow",
    "/api/agent-contracts/{id}/launch",
    "/api/agent-runs",
    "/api/agent-runs/{run_id}",
    "/api/agent-runs/{run_id}/events",
    "/api/structure/scan",
    "/api/structure/bundles",
    "/api/structure/bundles/{bundle_id}",
    "/api/structure/bundles/{bundle_id}/review",
    "/api/structure/bundles/{bundle_id}/review-batch",
    "/api/structure/bundles/{bundle_id}/review-contradiction",
    "/api/structure/bundles/{bundle_id}/workflow",
    "/api/structure/bundles/{bundle_id}/rebase-preview",
    "/api/structure/bundles/{bundle_id}/rebase",
    "/api/structure/bundles/{bundle_id}/merge",
]


class ApiContractTests(ApiTestCase):
    def test_source_of_truth_contract_surface_is_frozen(self) -> None:
        response = self.client.get("/api/source-of-truth")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["source_of_truth"]
        contract = payload["contract"]

        self.assertEqual(contract["api_version"], "0.2.0")
        self.assertEqual(contract["contract_version"], "0.2.0")
        self.assertEqual(contract["stability"], "beta")
        self.assertEqual(contract["default_runtime_mode"], "docker-compose")
        self.assertEqual(contract["supported_runtime_modes"], ["docker-compose", "local-python"])
        self.assertEqual(contract["supported_platforms"], ["macos", "linux"])
        self.assertEqual(contract["supported_endpoints"], EXPECTED_SUPPORTED_ENDPOINTS)
        self.assertEqual(contract["supplemental_endpoints"], ["/api/graph", "/api/plans/latest", "/api/project/profile"])
        self.assertEqual(
            contract["truth_layers"],
            {
                "graph": "structure",
                "bundles": "proposal",
                "plan_state": "execution",
            },
        )
        self.assertEqual(
            contract["governance"],
            {
                "proposal_then_approve": True,
                "agents_may_write_canonical_structure": False,
                "canonical_structure_mutations_require_review_merge": True,
                "execution_writes_require_revision_checks": True,
            },
        )
        self.assertEqual(
            contract["agent_contract_ids"],
            [
                "workbench-architect",
                "workbench-scout",
                "workbench-builder",
                "workbench-qa",
            ],
        )

    def test_graph_and_plan_state_payloads_reuse_same_contract_version(self) -> None:
        graph_response = self.client.get("/api/graph")
        self.assertEqual(graph_response.status_code, 200)
        self.assertEqual(graph_response.json()["source_of_truth"]["contract"]["contract_version"], "0.2.0")

        plan_state_response = self.client.get("/api/plan-state")
        self.assertEqual(plan_state_response.status_code, 200)
        self.assertEqual(plan_state_response.json()["source_of_truth"]["contract"]["contract_version"], "0.2.0")


class PluginPackagingTests(unittest.TestCase):
    def test_repo_local_plugin_manifest_and_marketplace_entry_exist(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        marketplace_path = repo_root / ".agents" / "plugins" / "marketplace.json"
        plugin_manifest_path = repo_root / "plugins" / "data-workbench" / ".codex-plugin" / "plugin.json"

        self.assertTrue(marketplace_path.exists())
        self.assertTrue(plugin_manifest_path.exists())

        marketplace = json.loads(marketplace_path.read_text(encoding="utf-8"))
        plugin_manifest = json.loads(plugin_manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(plugin_manifest["name"], "data-workbench")
        self.assertEqual(plugin_manifest["version"], "0.2.0")
        self.assertEqual(plugin_manifest["skills"], "./skills/")

        entry = next(item for item in marketplace["plugins"] if item["name"] == "data-workbench")
        self.assertEqual(entry["source"]["source"], "local")
        self.assertEqual(entry["source"]["path"], "./plugins/data-workbench")
        self.assertEqual(entry["policy"]["installation"], "AVAILABLE")
        self.assertEqual(entry["policy"]["authentication"], "ON_INSTALL")

    def test_repo_local_plugin_skills_reference_playbooks_and_source_of_truth(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        skill_dir = repo_root / "plugins" / "data-workbench" / "skills"
        expected = {
            "workbench-architect": "docs/agent_playbooks/workbench-architect.md",
            "workbench-scout": "docs/agent_playbooks/workbench-scout.md",
            "workbench-builder": "docs/agent_playbooks/workbench-builder.md",
            "workbench-qa": "docs/agent_playbooks/workbench-qa.md",
        }

        for skill_name, playbook_fragment in expected.items():
            skill_path = skill_dir / skill_name / "SKILL.md"
            self.assertTrue(skill_path.exists(), skill_path)
            content = skill_path.read_text(encoding="utf-8")
            self.assertIn("/api/source-of-truth", content)
            self.assertIn(playbook_fragment, content)
