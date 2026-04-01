from __future__ import annotations

import json
import unittest
from pathlib import Path

import polars as pl

from tests.api_test_case import ApiTestCase


class ApiExecutionTests(ApiTestCase):
    def test_demo_project_bootstrap_save_and_agent_loop(self) -> None:
        example_root = self.root / "demo_dogfood"
        backend_dir = example_root / "backend"
        frontend_dir = example_root / "frontend"
        data_dir = example_root / "data"
        backend_dir.mkdir(parents=True, exist_ok=True)
        frontend_dir.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)

        (backend_dir / "demo_routes.py").write_text(
            """
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class DemoPricingResponse(BaseModel):
    pricing_score: float
    median_home_price: float

@router.get("/api/demo-pricing", response_model=DemoPricingResponse)
def demo_pricing() -> dict[str, float]:
    return {"pricing_score": 87.5, "median_home_price": 742000.0}
""".strip(),
            encoding="utf-8",
        )
        (frontend_dir / "DemoPricingPanel.tsx").write_text(
            """
export async function DemoPricingPanel() {
  const snapshot = await fetch("/api/demo-pricing").then((response) => response.json());
  return `${snapshot.pricingScore} ${snapshot.medianHomePrice}`;
}
""".strip(),
            encoding="utf-8",
        )
        (data_dir / "demo_pricing.csv").write_text(
            "date,pricing_score,median_home_price\n2025-01-01,84.2,731000\n2025-02-01,87.5,742000\n",
            encoding="utf-8",
        )

        profile_response = self.client.get(
            "/api/project/profile",
            params={"root_path": str(example_root), "include_internal": "false"},
        )
        self.assertEqual(profile_response.status_code, 200)
        profile = profile_response.json()["project_profile"]
        api_hint = next(hint for hint in profile["api_contract_hints"] if hint["route"] == "GET /api/demo-pricing")
        ui_hint = next(hint for hint in profile["ui_contract_hints"] if hint["component"] == "DemoPricingPanel")
        asset = next(asset for asset in profile["data_assets"] if asset["path"].endswith("demo_pricing.csv"))

        graph = self.client.get("/api/graph").json()["graph"]
        bootstrap_response = self.client.post(
            "/api/import/project-bootstrap",
            json={
                "graph": graph,
                "root_path": str(example_root),
                "include_internal": False,
                "asset_paths": [asset["path"]],
                "api_hint_ids": [api_hint["id"]],
                "ui_hint_ids": [ui_hint["id"]],
                "import_assets": True,
                "import_api_hints": True,
                "import_ui_hints": True,
            },
        )
        self.assertEqual(bootstrap_response.status_code, 200)
        bootstrapped = bootstrap_response.json()["graph"]

        save_response = self.client.post("/api/graph/save", json={"graph": bootstrapped})
        self.assertEqual(save_response.status_code, 200)
        source_of_truth = save_response.json()["source_of_truth"]
        self.assertEqual(source_of_truth["contract"]["truth_layers"]["plan_state"], "execution")
        saved_graph = save_response.json()["graph"]
        imported_contract = next(
            node for node in saved_graph["nodes"] if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/demo-pricing"
        )
        imported_data = next(
            node for node in saved_graph["nodes"] if node["kind"] == "data" and any(column["name"] == "pricing_score" for column in node.get("columns", []))
        )

        plan_state_response = self.client.put(
            "/api/plan-state",
            json={
                "plan_state": {
                    "decisions": [
                        {
                            "id": "decision.demo_pricing",
                            "title": "Ship demo pricing end-to-end",
                            "kind": "contract",
                            "status": "proposed",
                            "linked_refs": [imported_contract["id"], imported_data["id"]],
                            "summary": "Dogfood the example route and dataset through the execution flow.",
                        }
                    ],
                    "tasks": [],
                    "blockers": [],
                    "acceptance_checks": [],
                    "evidence": [],
                    "attachments": [],
                    "agent_runs": [],
                    "agreement_log": [],
                },
                "expected_revision": save_response.json()["plan_state"]["revision"],
                "updated_by": "architect",
            },
        )
        self.assertEqual(plan_state_response.status_code, 200)

        derive_response = self.client.post(
            "/api/plan-state/derive-tasks",
            json={
                "expected_revision": plan_state_response.json()["plan_state"]["revision"],
                "updated_by": "architect",
            },
        )
        self.assertEqual(derive_response.status_code, 200)
        derived = derive_response.json()["plan_state"]
        self.assertTrue(derived["tasks"])

        workflow_response = self.client.get("/api/agent-contracts/workbench-builder/workflow")
        self.assertEqual(workflow_response.status_code, 200)
        workflow = workflow_response.json()["workflow"]
        self.assertEqual(workflow["starter_run"]["agent_run"]["role"], "builder")

        launch_response = self.client.post(
            "/api/agent-contracts/workbench-builder/launch",
            json={
                "expected_revision": derived["revision"],
                "updated_by": "builder",
            },
        )
        self.assertEqual(launch_response.status_code, 200)
        launch_payload = launch_response.json()
        self.assertIn(launch_payload["launch_mode"], {"created", "resume_existing"})
        self.assertEqual(launch_payload["agent_run"]["role"], "builder")

        brief_response = self.client.get(
            "/api/agent-contracts/workbench-builder/brief",
            params={"run_id": launch_payload["agent_run"]["id"]},
        )
        self.assertEqual(brief_response.status_code, 200)
        self.assertIn("Assigned task", brief_response.json()["brief"]["prompt_markdown"])

    def test_plan_state_endpoint_bridges_graph_work_items_when_no_native_plan_exists(self) -> None:
        graph = self.client.get("/api/graph").json()["graph"]
        target = next(node for node in graph["nodes"] if node["id"] == "data:market_signals")
        target["work_items"] = [
            {
                "title": "Finalize serving bindings",
                "role": "builder",
                "status": "in_progress",
                "summary": "Carry the serving bindings through to the snapshot contract.",
            }
        ]
        save_response = self.client.post("/api/graph/save", json={"graph": graph})
        self.assertEqual(save_response.status_code, 200)

        response = self.client.get("/api/plan-state")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["plan_state"]
        self.assertTrue(payload["tasks"])
        self.assertTrue(payload["decisions"])
        self.assertEqual(payload["tasks"][0]["linked_refs"], ["data:market_signals"])
        self.assertTrue(payload["tasks"][0]["decision_ids"])
        self.assertEqual(payload["decisions"][0]["status"], "in_progress")

    def test_structure_review_and_merge_flow_sync_execution_state(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "structure_execution_sync.md").write_text("GET /api/demo/execution-sync\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/structure_execution_sync.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_demo_execution_sync"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted", "reviewed_by": "qa-manager"},
        )
        self.assertEqual(review_response.status_code, 200)
        review_payload = review_response.json()
        review_task = next(task for task in review_payload["plan_state"]["tasks"] if task["id"].startswith("task.review."))
        review_decision = next(
            decision for decision in review_payload["plan_state"]["decisions"] if decision["id"].startswith("decision.review.")
        )
        self.assertEqual(review_task["status"], "todo")
        self.assertEqual(review_decision["status"], "accepted")
        self.assertTrue(review_payload["source_of_truth"]["top_open_tasks"])

        merge_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/merge",
            json={"merged_by": "qa-manager"},
        )
        self.assertEqual(merge_response.status_code, 200)
        merge_payload = merge_response.json()
        merged_task = next(task for task in merge_payload["plan_state"]["tasks"] if task["id"] == review_task["id"])
        merged_decision = next(
            decision for decision in merge_payload["plan_state"]["decisions"] if decision["id"] == review_decision["id"]
        )
        self.assertEqual(merged_task["status"], "done")
        self.assertEqual(merged_decision["status"], "validated")
        self.assertTrue(merge_payload["source_of_truth"]["recently_completed"])

    def test_structure_rebase_transfers_execution_review_state_to_rebased_bundle(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        review_doc = docs_dir / "rebase_execution.md"
        review_doc.write_text("GET /api/demo/rebase-execution\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/rebase_execution.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]
        patch_id = next(
            patch["id"]
            for patch in bundle["patches"]
            if patch["target_id"] == "contract:api.get__api_demo_rebase_execution"
        )

        review_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/review",
            json={"patch_id": patch_id, "decision": "accepted", "reviewed_by": "review-manager"},
        )
        self.assertEqual(review_response.status_code, 200)
        reviewed_plan_state = review_response.json()["plan_state"]
        original_task = next(task for task in reviewed_plan_state["tasks"] if task["id"].startswith("task.review."))
        original_decision = next(
            decision for decision in reviewed_plan_state["decisions"] if decision["id"].startswith("decision.review.")
        )

        export_response = self.client.get("/api/structure/export")
        self.assertEqual(export_response.status_code, 200)
        import_response = self.client.post(
            "/api/structure/import",
            json={"yaml_text": export_response.text, "updated_by": "rebase-manager"},
        )
        self.assertEqual(import_response.status_code, 200)
        review_doc.write_text(
            "GET /api/demo/rebase-execution\nGET /api/demo/rebase-execution-shadow\n",
            encoding="utf-8",
        )

        rebase_response = self.client.post(
            f"/api/structure/bundles/{bundle['bundle_id']}/rebase",
            json={"preserve_reviews": True, "rebased_by": "review-manager"},
        )
        self.assertEqual(rebase_response.status_code, 200)
        payload = rebase_response.json()
        rebased_bundle = payload["bundle"]
        self.assertNotEqual(rebased_bundle["bundle_id"], bundle["bundle_id"])
        task_ids = {task["id"] for task in payload["plan_state"]["tasks"]}
        decision_ids = {decision["id"] for decision in payload["plan_state"]["decisions"]}
        self.assertNotIn(original_task["id"], task_ids)
        self.assertNotIn(original_decision["id"], decision_ids)
        rebased_bundle_slug = rebased_bundle["bundle_id"].replace(".", "_").replace("-", "_")
        self.assertTrue(
            any(task_id.startswith(f"task.review.{rebased_bundle_slug}") for task_id in task_ids)
        )
        self.assertTrue(
            any(
                entry["kind"] == "rebase_transfer" and bundle["bundle_id"] in entry["summary"]
                for entry in payload["plan_state"]["agreement_log"]
            )
        )

    def test_put_plan_state_rejects_orphan_tasks_and_structural_duplication(self) -> None:
        orphan_task_payload = {
            "decisions": [],
            "tasks": [
                {
                    "id": "task.orphan",
                    "title": "Orphan task",
                    "role": "builder",
                    "status": "todo",
                    "decision_ids": [],
                    "linked_refs": ["data:market_signals"],
                    "summary": "This should fail because it is disconnected from any decision.",
                }
            ],
            "blockers": [],
            "acceptance_checks": [],
            "evidence": [],
            "attachments": [],
            "agent_runs": [],
            "agreement_log": [],
        }
        orphan_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": orphan_task_payload, "expected_revision": 0, "updated_by": "qa"},
        )
        self.assertEqual(orphan_response.status_code, 400)
        self.assertIn("must link to at least one decision", orphan_response.json()["detail"])

        duplicate_structure_payload = self.make_valid_plan_state()
        duplicate_structure_payload["tasks"][0]["columns"] = [{"name": "should_not_be_here"}]
        duplicate_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": duplicate_structure_payload, "expected_revision": 0, "updated_by": "qa"},
        )
        self.assertEqual(duplicate_response.status_code, 400)
        self.assertIn("Extra inputs are not permitted", duplicate_response.json()["detail"])

    def test_plan_state_revision_checks_and_derive_tasks_workflow(self) -> None:
        initial = {
            "decisions": [
                {
                    "id": "decision.new_table",
                    "title": "Introduce pricing snapshot storage",
                    "kind": "data",
                    "status": "proposed",
                    "linked_refs": ["data:market_signals"],
                    "summary": "This starts as a proposed design decision before task derivation.",
                }
            ],
            "tasks": [],
            "blockers": [],
            "acceptance_checks": [],
            "evidence": [],
            "attachments": [],
            "agent_runs": [],
            "agreement_log": [],
        }
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": initial, "expected_revision": 0, "updated_by": "architect"},
        )
        self.assertEqual(save_response.status_code, 200)
        saved = save_response.json()["plan_state"]
        self.assertEqual(saved["revision"], 1)

        stale_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": initial, "expected_revision": 0, "updated_by": "architect"},
        )
        self.assertEqual(stale_response.status_code, 400)
        self.assertIn("revision mismatch", stale_response.json()["detail"])

        derive_response = self.client.post(
            "/api/plan-state/derive-tasks",
            json={"expected_revision": 1, "updated_by": "architect"},
        )
        self.assertEqual(derive_response.status_code, 200)
        derived = derive_response.json()["plan_state"]
        self.assertEqual(derived["revision"], 2)
        self.assertEqual(len(derived["tasks"]), 1)
        self.assertEqual(derived["tasks"][0]["decision_ids"], ["decision.new_table"])

    def test_plan_state_derives_ordered_multi_stage_tasks_with_validation(self) -> None:
        initial = {
            "decisions": [
                {
                    "id": "decision.snapshot_pipeline",
                    "title": "Ship snapshot pipeline",
                    "kind": "contract",
                    "status": "proposed",
                    "linked_refs": ["data:market_signals", "contract:api.market_snapshot"],
                    "acceptance_check_ids": ["check.snapshot.delivery"],
                    "summary": "Carry data into the API contract with explicit proof.",
                }
            ],
            "tasks": [],
            "blockers": [],
            "acceptance_checks": [
                {
                    "id": "check.snapshot.delivery",
                    "label": "Snapshot delivery is validated",
                    "kind": "validation",
                    "linked_refs": ["contract:api.market_snapshot"],
                    "required": True,
                }
            ],
            "evidence": [],
            "attachments": [],
            "agent_runs": [],
            "agreement_log": [],
        }
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": initial, "expected_revision": 0, "updated_by": "architect"},
        )
        self.assertEqual(save_response.status_code, 200)
        revision = save_response.json()["plan_state"]["revision"]

        derive_response = self.client.post(
            "/api/plan-state/derive-tasks",
            json={"expected_revision": revision, "updated_by": "architect"},
        )
        self.assertEqual(derive_response.status_code, 200)
        tasks = derive_response.json()["plan_state"]["tasks"]
        task_ids = {task["id"] for task in tasks}
        self.assertTrue(any(task["id"].endswith(".model") for task in tasks))
        self.assertTrue(any(task["id"].endswith(".deliver") for task in tasks))
        validate_task = next(task for task in tasks if task["id"].endswith(".validate"))
        self.assertEqual(validate_task["role"], "qa")
        self.assertEqual(validate_task["acceptance_check_ids"], ["check.snapshot.delivery"])
        deliver_task = next(task for task in tasks if task["id"].endswith(".deliver"))
        self.assertTrue(all(dependency in task_ids for dependency in deliver_task["depends_on"]))
        self.assertTrue(validate_task["depends_on"])

    def test_agent_run_endpoints_and_source_of_truth_surface_priority_signals(self) -> None:
        plan_state = self.make_valid_plan_state(decision_status="in_progress", task_status="blocked")
        plan_state["blockers"] = [
            {
                "id": "blocker.market_snapshot.confirmation",
                "type": "dependency",
                "status": "open",
                "task_ids": ["task.market_snapshot.delivery"],
                "decision_ids": ["decision.market_snapshot"],
                "linked_refs": ["contract:api.market_snapshot"],
                "summary": "Waiting on upstream binding confirmation.",
                "suggested_resolution": "Confirm the final serving binding with the API owner.",
                "owner": "qa",
            }
        ]
        plan_state["tasks"][0]["blocker_ids"] = ["blocker.market_snapshot.confirmation"]
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": plan_state, "expected_revision": 0, "updated_by": "qa"},
        )
        self.assertEqual(save_response.status_code, 200)
        revision = save_response.json()["plan_state"]["revision"]

        invalid_run_response = self.client.post(
            "/api/agent-runs",
            json={
                "agent_run": {
                    "id": "run.invalid",
                    "role": "builder",
                    "status": "waiting",
                    "task_ids": ["task.market_snapshot.delivery"],
                    "objective": "Finish the snapshot delivery work",
                },
                "expected_revision": revision,
                "updated_by": "qa",
            },
        )
        self.assertEqual(invalid_run_response.status_code, 400)
        self.assertIn("status_reason", invalid_run_response.json()["detail"])

        create_response = self.client.post(
            "/api/agent-runs",
            json={
                "agent_run": {
                    "id": "run.market_snapshot.builder",
                    "role": "builder",
                    "status": "waiting",
                    "status_reason": "Need a confirmed upstream binding before implementation resumes.",
                    "next_action_hint": "Follow up with the API owner and then continue the data wiring.",
                    "blocked_by": ["blocker.market_snapshot.confirmation"],
                    "task_ids": ["task.market_snapshot.delivery"],
                    "objective": "Finish the snapshot delivery work",
                },
                "expected_revision": revision,
                "updated_by": "qa",
            },
        )
        self.assertEqual(create_response.status_code, 200)
        created = create_response.json()["agent_run"]
        self.assertEqual(created["status"], "waiting")
        next_revision = create_response.json()["plan_state"]["revision"]

        event_response = self.client.post(
            "/api/agent-runs/run.market_snapshot.builder/events",
            json={
                "event": {
                    "kind": "note",
                    "summary": "Paused until the upstream binding decision is confirmed.",
                },
                "expected_revision": next_revision,
                "updated_by": "qa",
            },
        )
        self.assertEqual(event_response.status_code, 200)
        self.assertEqual(event_response.json()["agent_run"]["last_summary"], "Paused until the upstream binding decision is confirmed.")

        source_response = self.client.get("/api/source-of-truth")
        self.assertEqual(source_response.status_code, 200)
        source_of_truth = source_response.json()["source_of_truth"]
        self.assertEqual(source_of_truth["contract"]["api_version"], "0.2.0")
        self.assertIn("plan_state", source_of_truth["contract"]["capabilities"])
        self.assertEqual(source_of_truth["contract"]["truth_layers"]["graph"], "structure")
        self.assertEqual(source_of_truth["top_blocker"]["id"], "blocker.market_snapshot.confirmation")
        self.assertEqual(source_of_truth["top_open_tasks"][0]["id"], "task.market_snapshot.delivery")
        self.assertEqual(source_of_truth["highest_risk_decision"]["id"], "decision.market_snapshot")
        self.assertTrue(source_of_truth["plan_state"]["resumable_runs"])
        self.assertTrue(source_of_truth["role_lanes"])
        self.assertEqual(source_of_truth["handoff_queue"][0]["id"], "run.market_snapshot.builder")

    def test_agent_contract_brief_endpoint_returns_run_task_and_priority_context(self) -> None:
        plan_state = self.make_valid_plan_state(decision_status="in_progress", task_status="blocked")
        plan_state["blockers"] = [
            {
                "id": "blocker.market_snapshot.confirmation",
                "type": "dependency",
                "status": "open",
                "task_ids": ["task.market_snapshot.delivery"],
                "decision_ids": ["decision.market_snapshot"],
                "linked_refs": ["contract:api.market_snapshot"],
                "summary": "Waiting on the API owner to confirm the serving binding.",
                "suggested_resolution": "Confirm the binding and resume delivery.",
                "owner": "qa",
            }
        ]
        plan_state["tasks"][0]["blocker_ids"] = ["blocker.market_snapshot.confirmation"]
        plan_state["agent_runs"] = [
            {
                "id": "run.market_snapshot.builder",
                "role": "builder",
                "status": "waiting",
                "status_reason": "Need the API owner to confirm the final binding.",
                "next_action_hint": "Resume the delivery task once the binding is confirmed.",
                "blocked_by": ["blocker.market_snapshot.confirmation"],
                "task_ids": ["task.market_snapshot.delivery"],
                "objective": "Finish the market snapshot delivery task",
                "updated_at": "2026-03-31T12:00:00Z",
            }
        ]
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": plan_state, "expected_revision": 0, "updated_by": "qa"},
        )
        self.assertEqual(save_response.status_code, 200)

        brief_response = self.client.get(
            "/api/agent-contracts/workbench-builder/brief",
            params={"task_id": "task.market_snapshot.delivery", "run_id": "run.market_snapshot.builder"},
        )
        self.assertEqual(brief_response.status_code, 200)
        payload = brief_response.json()
        prompt = payload["brief"]["prompt_markdown"]
        self.assertIn("## Assigned task", prompt)
        self.assertIn("task.market_snapshot.delivery", prompt)
        self.assertIn("## Current run", prompt)
        self.assertIn("Need the API owner to confirm the final binding.", prompt)
        self.assertIn("## Blockers", prompt)
        self.assertIn("Waiting on the API owner to confirm the serving binding.", prompt)
        self.assertEqual(payload["source_of_truth"]["handoff_queue"][0]["id"], "run.market_snapshot.builder")
        self.assertEqual(payload["source_of_truth"]["role_lanes"][0]["role"], "builder")

    def test_agent_workflow_endpoint_recommends_architect_decision_and_task_derivation(self) -> None:
        plan_state = self.make_valid_plan_state(decision_status="proposed", task_status="todo")
        plan_state["tasks"] = []
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": plan_state, "expected_revision": 0, "updated_by": "architect"},
        )
        self.assertEqual(save_response.status_code, 200)

        workflow_response = self.client.get("/api/agent-contracts/workbench-architect/workflow")
        self.assertEqual(workflow_response.status_code, 200)
        workflow = workflow_response.json()["workflow"]
        self.assertEqual(workflow["focus"]["kind"], "decision")
        self.assertEqual(workflow["recommended_decision"]["id"], "decision.market_snapshot")
        action_kinds = {action["kind"] for action in workflow["recommended_actions"]}
        self.assertIn("derive_tasks", action_kinds)
        self.assertEqual(workflow["starter_run"]["agent_run"]["role"], "architect")
        self.assertEqual(workflow["starter_run"]["agent_run"]["status"], "running")

    def test_builder_workflow_launch_creates_then_reuses_resumable_run(self) -> None:
        plan_state = self.make_valid_plan_state(decision_status="in_progress", task_status="todo")
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": plan_state, "expected_revision": 0, "updated_by": "builder"},
        )
        self.assertEqual(save_response.status_code, 200)
        revision = save_response.json()["plan_state"]["revision"]

        workflow_response = self.client.get("/api/agent-contracts/workbench-builder/workflow")
        self.assertEqual(workflow_response.status_code, 200)
        workflow = workflow_response.json()["workflow"]
        self.assertEqual(workflow["recommended_task"]["id"], "task.market_snapshot.delivery")
        self.assertEqual(workflow["starter_run"]["agent_run"]["task_ids"], ["task.market_snapshot.delivery"])

        launch_response = self.client.post(
            "/api/agent-contracts/workbench-builder/launch",
            json={"expected_revision": revision, "updated_by": "builder"},
        )
        self.assertEqual(launch_response.status_code, 200)
        launch_payload = launch_response.json()
        self.assertEqual(launch_payload["launch_mode"], "created")
        self.assertEqual(launch_payload["agent_run"]["role"], "builder")
        self.assertEqual(len(launch_payload["plan_state"]["agent_runs"]), 1)
        next_revision = launch_payload["plan_state"]["revision"]

        second_launch_response = self.client.post(
            "/api/agent-contracts/workbench-builder/launch",
            json={"expected_revision": next_revision, "updated_by": "builder"},
        )
        self.assertEqual(second_launch_response.status_code, 200)
        second_payload = second_launch_response.json()
        self.assertEqual(second_payload["launch_mode"], "resume_existing")
        self.assertEqual(second_payload["agent_run"]["id"], launch_payload["agent_run"]["id"])
        self.assertEqual(len(second_payload["plan_state"]["agent_runs"]), 1)

    def test_scout_and_qa_workflows_pick_review_and_validation_priorities(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "agent_workflow_review.md").write_text("GET /api/demo/agent-workflow-review\n", encoding="utf-8")
        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/agent_workflow_review.md"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle = scan_response.json()["bundle"]

        plan_state = self.make_valid_plan_state(decision_status="accepted", task_status="blocked")
        plan_state["blockers"] = [
            {
                "id": "blocker.market_snapshot.qa",
                "type": "dependency",
                "status": "open",
                "task_ids": ["task.market_snapshot.delivery"],
                "decision_ids": ["decision.market_snapshot"],
                "linked_refs": ["contract:api.market_snapshot"],
                "summary": "Need validation proof before QA can clear this work.",
                "suggested_resolution": "Review the latest execution evidence and confirm the required check.",
                "owner": "qa",
            }
        ]
        plan_state["tasks"][0]["blocker_ids"] = ["blocker.market_snapshot.qa"]
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": plan_state, "expected_revision": 0, "updated_by": "qa"},
        )
        self.assertEqual(save_response.status_code, 200)

        scout_workflow_response = self.client.get("/api/agent-contracts/workbench-scout/workflow")
        self.assertEqual(scout_workflow_response.status_code, 200)
        scout_workflow = scout_workflow_response.json()["workflow"]
        self.assertEqual(scout_workflow["recommended_bundle"]["bundle_id"], bundle["bundle_id"])
        scout_action_kinds = {action["kind"] for action in scout_workflow["recommended_actions"]}
        self.assertIn("inspect_bundle", scout_action_kinds)

        qa_workflow_response = self.client.get("/api/agent-contracts/workbench-qa/workflow")
        self.assertEqual(qa_workflow_response.status_code, 200)
        qa_workflow = qa_workflow_response.json()["workflow"]
        self.assertEqual(qa_workflow["recommended_decision"]["id"], "decision.market_snapshot")
        qa_action_kinds = {action["kind"] for action in qa_workflow["recommended_actions"]}
        self.assertIn("review_bundle", qa_action_kinds)
        self.assertIn("validate_checks", qa_action_kinds)

    def test_agent_workflow_and_launch_accept_scan_context(self) -> None:
        docs_dir = self.root / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        (docs_dir / "scan_context.md").write_text("GET /api/demo/scan-context\n", encoding="utf-8")

        scan_response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "doc_paths": ["docs/scan_context.md"],
                "selected_paths": ["backend"],
                "include_internal": False,
            },
        )
        self.assertEqual(scan_response.status_code, 200)
        bundle_id = scan_response.json()["bundle"]["bundle_id"]

        plan_state = self.make_valid_plan_state(decision_status="accepted", task_status="todo")
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": plan_state, "expected_revision": 0, "updated_by": "scout"},
        )
        self.assertEqual(save_response.status_code, 200)
        revision = save_response.json()["plan_state"]["revision"]

        workflow_response = self.client.get(
            "/api/agent-contracts/workbench-scout/workflow",
            params=[
                ("bundle_id", bundle_id),
                ("root_path", str(self.root)),
                ("doc_paths", "docs/scan_context.md"),
                ("selected_paths", "backend"),
            ],
        )
        self.assertEqual(workflow_response.status_code, 200)
        workflow = workflow_response.json()["workflow"]
        self.assertEqual(workflow["scan_context"]["bundle_id"], bundle_id)
        self.assertEqual(workflow["scan_context"]["doc_paths"], ["docs/scan_context.md"])
        self.assertEqual(workflow["recommended_bundle"]["bundle_id"], bundle_id)

        launch_response = self.client.post(
            "/api/agent-contracts/workbench-scout/launch",
            json={
                "expected_revision": revision,
                "updated_by": "scout",
                "bundle_id": bundle_id,
                "root_path": str(self.root),
                "doc_paths": ["docs/scan_context.md"],
                "selected_paths": ["backend"],
            },
        )
        self.assertEqual(launch_response.status_code, 200)
        payload = launch_response.json()
        self.assertEqual(payload["agent_run"]["bundle_id"], bundle_id)
        self.assertEqual(payload["workflow"]["scan_context"]["selected_paths"], ["backend"])

    def test_agent_run_patch_rejects_stale_revision(self) -> None:
        plan_state = self.make_valid_plan_state(decision_status="in_progress", task_status="todo")
        save_response = self.client.put(
            "/api/plan-state",
            json={"plan_state": plan_state, "expected_revision": 0, "updated_by": "architect"},
        )
        self.assertEqual(save_response.status_code, 200)
        revision = save_response.json()["plan_state"]["revision"]

        create_response = self.client.post(
            "/api/agent-runs",
            json={
                "agent_run": {
                    "id": "run.market_snapshot.builder",
                    "role": "builder",
                    "status": "running",
                    "task_ids": ["task.market_snapshot.delivery"],
                    "objective": "Finish the snapshot delivery work",
                },
                "expected_revision": revision,
                "updated_by": "architect",
            },
        )
        self.assertEqual(create_response.status_code, 200)

        stale_patch_response = self.client.patch(
            "/api/agent-runs/run.market_snapshot.builder",
            json={
                "updates": {"status": "waiting", "status_reason": "Need fresh input.", "next_action_hint": "Resume after review."},
                "expected_revision": revision,
                "updated_by": "architect",
            },
        )
        self.assertEqual(stale_patch_response.status_code, 400)
        self.assertIn("revision mismatch", stale_patch_response.json()["detail"])

    def test_agent_contracts_are_exposed_and_agent_canonical_writes_are_rejected(self) -> None:
        contracts_response = self.client.get("/api/agent-contracts")
        self.assertEqual(contracts_response.status_code, 200)
        contracts = contracts_response.json()["agent_contracts"]
        architect = next(contract for contract in contracts if contract["id"] == "workbench-architect")
        self.assertIn("/api/graph/save", architect["forbidden_actions"])
        self.assertTrue(architect["mission"])
        self.assertTrue(architect["starter_prompt"])
        self.assertTrue(architect["operating_loop"])
        self.assertTrue(architect["playbook_path"].endswith("docs/agent_playbooks/workbench-architect.md"))
        self.assertIn("# Workbench Architect", architect["playbook_markdown"])

        graph = self.client.get("/api/graph").json()["graph"]
        save_response = self.client.post("/api/graph/save", json={"graph": graph, "actor_type": "agent"})
        self.assertEqual(save_response.status_code, 403)
        self.assertIn("Agents may not save canonical structure", save_response.json()["detail"])

        import_response = self.client.post(
            "/api/structure/import",
            json={"spec": graph, "updated_by": "agent", "actor_type": "agent"},
        )
        self.assertEqual(import_response.status_code, 403)
        self.assertIn("Agents may not import canonical structure", import_response.json()["detail"])
