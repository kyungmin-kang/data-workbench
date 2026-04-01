from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

from . import __version__
from .types import (
    DecisionUnit,
    ExecutionTask,
    PlanState,
    build_index,
    identifierify,
    normalize_graph,
    normalize_plan_state,
    node_short_name,
)

REVIEW_DECISION_PREFIX = "decision.review."
REVIEW_TASK_PREFIX = "task.review."
REVIEW_BLOCKER_PREFIX = "blocker.review."
SOURCE_OF_TRUTH_CONTRACT_VERSION = __version__
SOURCE_OF_TRUTH_STABILITY = "beta"
SOURCE_OF_TRUTH_SUPPORTED_ENDPOINTS = [
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
SOURCE_OF_TRUTH_SUPPLEMENTAL_ENDPOINTS = [
    "/api/graph",
    "/api/plans/latest",
    "/api/project/profile",
    "/api/project/profile/jobs",
    "/api/project/profile/assets/jobs",
]


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_or_build_plan_state(
    graph: dict[str, Any],
    persisted_plan_state: dict[str, Any] | None,
) -> dict[str, Any]:
    if persisted_plan_state is not None:
        return normalize_plan_state(persisted_plan_state, graph=graph)
    return build_bridge_plan_state(graph)


def build_bridge_plan_state(graph: dict[str, Any]) -> dict[str, Any]:
    graph = normalize_graph(graph)
    timestamp = graph.get("metadata", {}).get("updated_at", "")
    updated_by = graph.get("metadata", {}).get("updated_by", "user")
    bridge: dict[str, Any] = {
        "revision": 0,
        "updated_at": timestamp,
        "updated_by": updated_by,
        "decisions": [],
        "tasks": [],
        "blockers": [],
        "acceptance_checks": [],
        "evidence": [],
        "attachments": [],
        "agent_runs": [],
        "agreement_log": [],
    }
    decisions_by_id: dict[str, dict[str, Any]] = {}

    for node in graph.get("nodes", []):
        work_items = node.get("work_items", [])
        if not work_items:
            continue
        for index, raw_item in enumerate(work_items, start=1):
            item = dict(raw_item)
            task_id = item.get("id") or f"task.bridge.{node_short_name(node['id'])}.{index}"
            decision_ids = [decision_id for decision_id in item.get("decision_ids", []) if decision_id]
            if not decision_ids and not item.get("exploratory"):
                decision_ids = [f"decision.bridge.{node_short_name(node['id'])}"]
            for decision_id in decision_ids:
                decision = decisions_by_id.get(decision_id)
                if decision is None:
                    decision = DecisionUnit(
                        id=decision_id,
                        title=f"{node['label']} execution",
                        kind=node.get("kind", "general"),
                        status="accepted",
                        linked_refs=[node["id"]],
                        summary=f"Bridge decision created from work items on {node['label']}.",
                        created_at=timestamp,
                        updated_at=timestamp,
                        updated_by=updated_by,
                    ).model_dump(mode="json")
                    decisions_by_id[decision_id] = decision
            bridge["tasks"].append(
                ExecutionTask(
                    id=task_id,
                    title=item.get("title") or f"{node['label']} work item {index}",
                    role=item.get("role") or "builder",
                    status=item.get("status") or node.get("work_status") or "todo",
                    exploratory=bool(item.get("exploratory")),
                    exploration_goal=item.get("exploration_goal", ""),
                    decision_ids=decision_ids,
                    linked_refs=item.get("linked_refs") or [node["id"]],
                    depends_on=item.get("depends_on", []),
                    acceptance_check_ids=item.get("acceptance_check_ids", []),
                    blocker_ids=item.get("blocker_ids", []),
                    summary=item.get("summary") or item.get("title") or "",
                    created_at=timestamp,
                    updated_at=timestamp,
                    completed_at=timestamp if item.get("status") == "done" else "",
                ).model_dump(mode="json")
            )

    bridge["decisions"] = list(decisions_by_id.values())
    for decision in bridge["decisions"]:
        task_statuses = [
            task["status"]
            for task in bridge["tasks"]
            if decision["id"] in task.get("decision_ids", []) and task.get("status") != "cancelled"
        ]
        if not task_statuses:
            decision["status"] = "proposed"
        elif all(status == "done" for status in task_statuses):
            decision["status"] = "validated"
        elif any(status in {"in_progress", "blocked"} for status in task_statuses):
            decision["status"] = "in_progress"
        else:
            decision["status"] = "accepted"
    return normalize_plan_state(bridge, graph=graph)


def derive_tasks_from_plan_state(
    plan_state: dict[str, Any],
    graph: dict[str, Any],
    *,
    updated_by: str = "user",
) -> dict[str, Any]:
    normalized = normalize_plan_state(plan_state, graph=graph)
    tasks = list(normalized.get("tasks", []))
    existing_task_ids = {task.get("id", "") for task in tasks}
    timestamp = utc_timestamp()
    for decision in normalized.get("decisions", []):
        if decision.get("status") == "deprecated":
            continue
        active_tasks = [
            task
            for task in tasks
            if decision["id"] in task.get("decision_ids", []) and task.get("status") != "cancelled"
        ]
        if active_tasks:
            continue
        derived_specs = derive_task_specs_for_decision(decision, graph)
        if not derived_specs:
            derived_specs = [
                {
                    "suffix": "",
                    "title": decision["title"],
                    "role": suggest_role_for_decision(decision, graph),
                    "linked_refs": decision.get("linked_refs", []),
                    "depends_on_suffixes": [],
                    "acceptance_check_ids": decision.get("acceptance_check_ids", []),
                    "summary": decision.get("summary") or f"Derived execution task for {decision['title']}.",
                }
            ]
        suffix_to_task_id: dict[str, str] = {}
        for spec in derived_specs:
            base_id = f"task.{identifierify(decision['id']) or node_short_name(decision['id'])}"
            task_id = f"{base_id}.{spec['suffix']}" if spec["suffix"] else base_id
            counter = 2
            while task_id in existing_task_ids:
                task_id = f"{base_id}.{spec['suffix'] or 'task'}.{counter}"
                counter += 1
            existing_task_ids.add(task_id)
            suffix_to_task_id[spec["suffix"]] = task_id
            tasks.append(
                ExecutionTask(
                    id=task_id,
                    title=spec["title"],
                    role=spec["role"],
                    status="todo",
                    exploratory=False,
                    decision_ids=[decision["id"]],
                    linked_refs=spec["linked_refs"],
                    depends_on=[],
                    acceptance_check_ids=spec["acceptance_check_ids"],
                    evidence_ids=[],
                    blocker_ids=[],
                    summary=spec["summary"],
                    created_at=timestamp,
                    updated_at=timestamp,
                ).model_dump(mode="json")
            )
        if suffix_to_task_id:
            for task in tasks:
                if decision["id"] not in task.get("decision_ids", []):
                    continue
                matching_spec = next((item for item in derived_specs if suffix_to_task_id.get(item["suffix"]) == task["id"]), None)
                if matching_spec is None:
                    continue
                task["depends_on"] = [
                    suffix_to_task_id[suffix]
                    for suffix in matching_spec.get("depends_on_suffixes", [])
                    if suffix in suffix_to_task_id
                ]
    normalized["tasks"] = tasks
    normalized["updated_at"] = timestamp
    normalized["updated_by"] = updated_by
    return normalize_plan_state(normalized, graph=graph)


def derive_task_specs_for_decision(decision: dict[str, Any], graph: dict[str, Any]) -> list[dict[str, Any]]:
    index = build_index(normalize_graph(graph))
    stage_refs: dict[str, list[str]] = defaultdict(list)
    for ref in decision.get("linked_refs", []):
        stage = classify_ref_stage(ref, index)
        if ref not in stage_refs[stage]:
            stage_refs[stage].append(ref)

    ordered_stages = [stage for stage in ("discover", "model", "compute", "deliver") if stage_refs.get(stage)]
    if not ordered_stages:
        ordered_stages = ["plan"]
        stage_refs["plan"] = decision.get("linked_refs", []) or []

    specs: list[dict[str, Any]] = []
    previous_suffix: str | None = None
    for stage in ordered_stages:
        linked_refs = stage_refs.get(stage, []) or decision.get("linked_refs", [])
        suffix = stage
        specs.append(
            {
                "suffix": suffix,
                "title": derive_task_title(stage, decision, linked_refs, index),
                "role": derive_task_role(stage, decision, linked_refs, graph),
                "linked_refs": linked_refs,
                "depends_on_suffixes": [previous_suffix] if previous_suffix else [],
                "acceptance_check_ids": [],
                "summary": derive_task_summary(stage, decision, linked_refs, index),
            }
        )
        previous_suffix = suffix

    required_checks = decision.get("acceptance_check_ids", [])
    if required_checks:
        validation_refs = decision.get("linked_refs", []) or stage_refs.get(previous_suffix or "plan", [])
        specs.append(
            {
                "suffix": "validate",
                "title": f"Validate {decision['title']}",
                "role": "qa",
                "linked_refs": validation_refs,
                "depends_on_suffixes": [previous_suffix] if previous_suffix else [],
                "acceptance_check_ids": required_checks,
                "summary": f"Confirm required acceptance checks for {decision['title']} with structured proof.",
            }
        )
    elif len(specs) == 1:
        specs[0]["acceptance_check_ids"] = decision.get("acceptance_check_ids", [])
    return specs


def classify_ref_stage(ref: str, index: dict[str, Any]) -> str:
    node = index["nodes"].get(ref)
    if node is None and ref in index["field_owner"]:
        node = index["nodes"].get(index["field_owner"][ref])
    if node is None:
        return "plan"
    if node["kind"] == "source":
        return "discover"
    if node["kind"] == "data":
        return "model"
    if node["kind"] == "compute":
        return "compute"
    if node["kind"] == "contract":
        return "deliver"
    return "plan"


def derive_task_title(
    stage: str,
    decision: dict[str, Any],
    linked_refs: list[str],
    index: dict[str, Any],
) -> str:
    stage_prefix = {
        "plan": "Refine",
        "discover": "Confirm inputs for",
        "model": "Shape data for",
        "compute": "Implement compute for",
        "deliver": "Deliver",
        "validate": "Validate",
    }.get(stage, "Execute")
    primary_label = decision["title"]
    for ref in linked_refs:
        node = index["nodes"].get(ref)
        if node is None and ref in index["field_owner"]:
            node = index["nodes"].get(index["field_owner"][ref])
        if node is not None:
            primary_label = f"{decision['title']} ({node['label']})"
            break
    return f"{stage_prefix} {primary_label}"


def derive_task_summary(
    stage: str,
    decision: dict[str, Any],
    linked_refs: list[str],
    index: dict[str, Any],
) -> str:
    node_labels: list[str] = []
    for ref in linked_refs:
        node = index["nodes"].get(ref)
        if node is None and ref in index["field_owner"]:
            node = index["nodes"].get(index["field_owner"][ref])
        if node is not None and node["label"] not in node_labels:
            node_labels.append(node["label"])
    stage_verbs = {
        "plan": "Clarify the exact implementation shape",
        "discover": "Confirm upstream inputs and repo reality",
        "model": "Model storage and schema shape",
        "compute": "Implement the compute or transformation logic",
        "deliver": "Expose the agreed API or UI surface",
        "validate": "Verify completion against required checks",
    }
    scope = ", ".join(node_labels[:3])
    if scope:
        return f"{stage_verbs.get(stage, 'Execute the work')} for {decision['title']} across {scope}."
    return f"{stage_verbs.get(stage, 'Execute the work')} for {decision['title']}."


def derive_task_role(
    stage: str,
    decision: dict[str, Any],
    linked_refs: list[str],
    graph: dict[str, Any],
) -> str:
    if stage == "discover":
        return "scout"
    if stage == "validate":
        return "qa"
    if stage == "plan":
        return "architect" if decision.get("kind") in {"research", "planning"} else suggest_role_for_decision(decision, graph)
    return suggest_role_for_decision({**decision, "linked_refs": linked_refs}, graph)


def suggest_role_for_decision(decision: dict[str, Any], graph: dict[str, Any]) -> str:
    index = build_index(normalize_graph(graph))
    for ref in decision.get("linked_refs", []):
        node = index["nodes"].get(ref)
        if not node and ref in index["field_owner"]:
            node = index["nodes"].get(index["field_owner"][ref])
        if not node:
            continue
        if node["kind"] == "contract" and node.get("extension_type") == "ui":
            return "builder"
        if node["kind"] == "contract":
            return "builder"
        if node["kind"] == "data":
            return "builder"
        if node["kind"] == "compute":
            return "builder"
        if node["kind"] == "source":
            return "scout"
    if decision.get("kind") in {"research", "planning"}:
        return "architect"
    return "builder"


def build_execution_summary(plan_state: dict[str, Any], graph: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_plan_state(plan_state, graph=graph)
    tasks = normalized.get("tasks", [])
    decisions = normalized.get("decisions", [])
    blockers = normalized.get("blockers", [])
    runs = normalized.get("agent_runs", [])

    open_tasks = [task for task in tasks if task.get("status") not in {"done", "cancelled"}]
    blocked_tasks = [task for task in tasks if task.get("status") == "blocked"]
    recently_completed = sorted(
        [task for task in tasks if task.get("status") == "done"],
        key=lambda item: item.get("completed_at") or item.get("updated_at") or "",
        reverse=True,
    )
    resumable_runs = [
        run for run in runs if run.get("status") in {"waiting", "blocked", "failed", "running"}
    ]
    return {
        "revision": normalized.get("revision", 0),
        "updated_at": normalized.get("updated_at", ""),
        "updated_by": normalized.get("updated_by", ""),
        "counts": {
            "decisions": len(decisions),
            "tasks": len(tasks),
            "open_tasks": len(open_tasks),
            "blocked_tasks": len(blocked_tasks),
            "blockers": len([blocker for blocker in blockers if blocker.get("status") != "resolved"]),
            "agent_runs": len(runs),
            "resumable_runs": len(resumable_runs),
            "missing_required_checks": sum(
                len(item.get("missing_required_checks", []))
                for item in rank_decisions(normalized)
            ),
        },
        "open_tasks_by_role": summarize_open_tasks_by_role(normalized),
        "top_open_tasks": rank_open_tasks(normalized)[:5],
        "top_blocker": rank_blockers(normalized)[0] if rank_blockers(normalized) else None,
        "highest_risk_decision": rank_decisions(normalized)[0] if rank_decisions(normalized) else None,
        "critical_path": build_critical_path(normalized)[:6],
        "blocked_work": blocked_tasks[:6],
        "recently_completed": recently_completed[:6],
        "role_lanes": build_role_lanes(normalized),
        "handoff_queue": build_handoff_queue(normalized),
        "resumable_runs": sorted(
            resumable_runs,
            key=lambda item: item.get("updated_at") or item.get("started_at") or "",
            reverse=True,
        )[:6],
        "recent_agent_activity": sorted(
            runs,
            key=lambda item: item.get("updated_at") or item.get("started_at") or "",
            reverse=True,
        )[:5],
        "missing_proof_decisions": [item for item in rank_decisions(normalized) if item.get("missing_required_checks")][:5],
    }


def build_source_of_truth(
    *,
    graph: dict[str, Any],
    structure: dict[str, Any],
    plan_state: dict[str, Any],
    latest_plan: dict[str, Any] | None,
    latest_artifacts: dict[str, Any] | None,
    artifact_storage: dict[str, Any] | None,
    agent_contracts: list[dict[str, object]] | None = None,
) -> dict[str, Any]:
    execution = build_execution_summary(plan_state, graph)
    readiness = structure.get("readiness", {}) if structure else {}
    latest_plan_summary = summarize_latest_plan(latest_plan, latest_artifacts)
    contract_ids = [str(contract.get("id", "")).strip() for contract in (agent_contracts or []) if str(contract.get("id", "")).strip()]
    return {
        "contract": {
            "api_version": __version__,
            "contract_version": SOURCE_OF_TRUTH_CONTRACT_VERSION,
            "stability": SOURCE_OF_TRUTH_STABILITY,
            "capabilities": [
                "source_of_truth",
                "plan_state",
                "agent_contracts",
                "agent_briefs",
                "agent_workflows",
                "structure_scan",
                "structure_review",
                "structure_rebase",
                "structure_merge",
            ],
            "default_runtime_mode": "docker-compose",
            "supported_runtime_modes": ["docker-compose", "local-python"],
            "supported_platforms": ["macos", "linux"],
            "supported_endpoints": SOURCE_OF_TRUTH_SUPPORTED_ENDPOINTS,
            "supplemental_endpoints": SOURCE_OF_TRUTH_SUPPLEMENTAL_ENDPOINTS,
            "truth_layers": {
                "graph": "structure",
                "bundles": "proposal",
                "plan_state": "execution",
            },
            "governance": {
                "proposal_then_approve": True,
                "agents_may_write_canonical_structure": False,
                "canonical_structure_mutations_require_review_merge": True,
                "execution_writes_require_revision_checks": True,
            },
            "agent_contract_ids": contract_ids,
        },
        "graph": {
            "name": graph.get("metadata", {}).get("name", ""),
            "structure_version": graph.get("metadata", {}).get("structure_version", 1),
            "updated_at": graph.get("metadata", {}).get("updated_at", ""),
            "updated_by": graph.get("metadata", {}).get("updated_by", ""),
            "node_count": len(graph.get("nodes", [])),
            "edge_count": len(graph.get("edges", [])),
        },
        "readiness": readiness,
        "latest_plan": latest_plan_summary,
        "artifacts": latest_artifacts or {},
        "artifact_storage": artifact_storage or {},
        "open_bundles": summarize_open_bundles(structure.get("bundles", []), structure.get("structure_version", 1)),
        "plan_state": execution,
        "top_open_tasks": execution["top_open_tasks"],
        "top_blocker": execution["top_blocker"],
        "highest_risk_decision": execution["highest_risk_decision"],
        "critical_path": execution["critical_path"],
        "blocked_work": execution["blocked_work"],
        "recently_completed": execution["recently_completed"],
        "role_lanes": execution["role_lanes"],
        "handoff_queue": execution["handoff_queue"],
        "recent_agent_activity": execution["recent_agent_activity"],
        "agent_contracts": agent_contracts or [],
    }


def apply_patch_review_to_plan_state(
    plan_state: dict[str, Any],
    graph: dict[str, Any],
    *,
    bundle_id: str,
    patch: dict[str, Any],
    decision: str,
    reviewed_by: str,
    note: str = "",
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    return _apply_review_outcome(
        plan_state,
        graph,
        bundle_id=bundle_id,
        record=patch,
        record_kind="patch",
        decision=decision,
        reviewed_by=reviewed_by,
        note=note,
        reviewed_at=reviewed_at or utc_timestamp(),
    )


def apply_contradiction_review_to_plan_state(
    plan_state: dict[str, Any],
    graph: dict[str, Any],
    *,
    bundle_id: str,
    contradiction: dict[str, Any],
    decision: str,
    reviewed_by: str,
    note: str = "",
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    return _apply_review_outcome(
        plan_state,
        graph,
        bundle_id=bundle_id,
        record=contradiction,
        record_kind="contradiction",
        decision=decision,
        reviewed_by=reviewed_by,
        note=note,
        reviewed_at=reviewed_at or utc_timestamp(),
    )


def apply_bundle_merge_to_plan_state(
    plan_state: dict[str, Any],
    graph: dict[str, Any],
    *,
    bundle: dict[str, Any],
    merged_by: str,
    merged_at: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_plan_state(plan_state, graph=graph)
    merged_at = merged_at or utc_timestamp()
    accepted_patches = [patch for patch in bundle.get("patches", []) if patch.get("review_state") == "accepted"]
    for patch in accepted_patches:
        refs = _extract_review_refs(patch, graph)
        slug = identifierify(patch.get("field_id") or patch.get("target_id") or patch.get("node_id") or patch.get("id") or "review")
        expected_decision_id = f"{REVIEW_DECISION_PREFIX}{identifierify(bundle.get('bundle_id', 'bundle'))}.{slug}"
        expected_task_id = f"{REVIEW_TASK_PREFIX}{identifierify(bundle.get('bundle_id', 'bundle'))}.{slug}"
        expected_blocker_id = f"{REVIEW_BLOCKER_PREFIX}{identifierify(bundle.get('bundle_id', 'bundle'))}.{slug}"
        for task in normalized.get("tasks", []):
            if not task.get("id", "").startswith(REVIEW_TASK_PREFIX):
                continue
            if task.get("id") != expected_task_id and not _refs_overlap(refs, task.get("linked_refs", [])):
                continue
            task["status"] = "done"
            task["completed_at"] = merged_at
            task["updated_at"] = merged_at
            task["blocker_ids"] = []
        for decision in normalized.get("decisions", []):
            if not decision.get("id", "").startswith(REVIEW_DECISION_PREFIX):
                continue
            if decision.get("id") != expected_decision_id and not _refs_overlap(refs, decision.get("linked_refs", [])):
                continue
            decision["status"] = "validated"
            decision["updated_at"] = merged_at
            decision["updated_by"] = merged_by
        for blocker in normalized.get("blockers", []):
            if not blocker.get("id", "").startswith(REVIEW_BLOCKER_PREFIX):
                continue
            if blocker.get("id") != expected_blocker_id and not _refs_overlap(refs, blocker.get("linked_refs", [])):
                continue
            blocker["status"] = "resolved"
            blocker["updated_at"] = merged_at
            blocker["resolved_at"] = merged_at
        normalized.setdefault("agreement_log", []).append(
            {
                "id": f"agreement.merge.{identifierify(bundle.get('bundle_id', 'bundle'))}.{identifierify(patch.get('id', 'patch'))}",
                "kind": "merge",
                "summary": f"Merged accepted {patch.get('type', 'patch')} from {bundle.get('bundle_id', 'bundle')}.",
                "actor": merged_by or "user",
                "at": merged_at,
            }
        )
    normalized["updated_at"] = merged_at
    normalized["updated_by"] = merged_by
    return normalize_plan_state(normalized, graph=graph)


def apply_bundle_rebase_to_plan_state(
    plan_state: dict[str, Any],
    graph: dict[str, Any],
    *,
    source_bundle_id: str,
    rebased_bundle: dict[str, Any],
    rebased_by: str,
    rebased_at: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_plan_state(plan_state, graph=graph)
    rebased_at = rebased_at or utc_timestamp()
    target_bundle_id = str(rebased_bundle.get("bundle_id") or "")
    preview = rebased_bundle.get("review", {}).get("last_rebase_summary", {}) or {}
    preserved_units = preview.get("preserved_review_units", []) or []

    decision_map: dict[str, str] = {}
    task_map: dict[str, str] = {}
    blocker_map: dict[str, str] = {}

    for unit in preserved_units:
        source_patch_id = str(unit.get("source_patch_id") or unit.get("patch_id") or "")
        source_review_slug = str(unit.get("source_review_slug") or unit.get("review_slug") or source_patch_id or "")
        target_review_slug = str(unit.get("target_review_slug") or source_review_slug or "")
        if not source_review_slug or not target_review_slug or not target_bundle_id:
            continue
        old_ids = _review_entity_ids(source_bundle_id, source_review_slug)
        new_ids = _review_entity_ids(target_bundle_id, target_review_slug)
        decision_map[old_ids["decision"]] = new_ids["decision"]
        task_map[old_ids["task"]] = new_ids["task"]
        blocker_map[old_ids["blocker"]] = new_ids["blocker"]
        normalized.setdefault("agreement_log", []).append(
            {
                "id": f"agreement.rebase.transfer.{identifierify(source_bundle_id)}.{identifierify(source_patch_id)}",
                "kind": "rebase_transfer",
                "summary": (
                    f"Transferred preserved review execution from {source_bundle_id} to "
                    f"{target_bundle_id} for {unit.get('label') or source_patch_id}."
                ),
                "actor": rebased_by or "user",
                "at": rebased_at,
            }
        )

    _apply_identifier_maps(normalized, decision_map=decision_map, task_map=task_map, blocker_map=blocker_map)

    if source_bundle_id and target_bundle_id and source_bundle_id != target_bundle_id:
        source_slug = identifierify(source_bundle_id)
        unresolved_prefixes = {
            "decision": f"{REVIEW_DECISION_PREFIX}{source_slug}.",
            "task": f"{REVIEW_TASK_PREFIX}{source_slug}.",
            "blocker": f"{REVIEW_BLOCKER_PREFIX}{source_slug}.",
        }
        for item in normalized.get("decisions", []):
            if item.get("id", "").startswith(unresolved_prefixes["decision"]):
                item["status"] = "deprecated"
                item["updated_at"] = rebased_at
                item["updated_by"] = rebased_by or "user"
        for item in normalized.get("tasks", []):
            if item.get("id", "").startswith(unresolved_prefixes["task"]):
                item["status"] = "cancelled"
                item["updated_at"] = rebased_at
                item["completed_at"] = item.get("completed_at") or rebased_at
                item["blocker_ids"] = []
        for item in normalized.get("blockers", []):
            if item.get("id", "").startswith(unresolved_prefixes["blocker"]):
                item["status"] = "resolved"
                item["updated_at"] = rebased_at
                item["resolved_at"] = item.get("resolved_at") or rebased_at
        normalized.setdefault("agreement_log", []).append(
            {
                "id": f"agreement.rebase.supersede.{identifierify(source_bundle_id)}",
                "kind": "rebase_supersede",
                "summary": (
                    f"Superseded review execution from {source_bundle_id} after rebasing to "
                    f"{target_bundle_id or 'the refreshed bundle'}."
                ),
                "actor": rebased_by or "user",
                "at": rebased_at,
            }
        )

    normalized["updated_at"] = rebased_at
    normalized["updated_by"] = rebased_by or "user"
    _recompute_decision_statuses(normalized)
    return normalize_plan_state(normalized, graph=graph)


def summarize_latest_plan(latest_plan: dict[str, Any] | None, latest_artifacts: dict[str, Any] | None) -> dict[str, Any]:
    if not latest_plan:
        return {
            "exists": False,
            "breaking_changes": 0,
            "impacted_apis": 0,
            "artifact_paths": latest_artifacts or {},
        }
    tiers = latest_plan.get("tiers", {})
    tier_1 = tiers.get("tier_1", {})
    tier_2 = tiers.get("tier_2", {})
    return {
        "exists": True,
        "breaking_changes": len(tier_1.get("breaking_changes", [])),
        "impacted_apis": len(tier_2.get("impacted_apis", [])),
        "artifact_paths": latest_artifacts or {},
    }


def summarize_open_bundles(bundles: list[dict[str, Any]], current_version: int) -> list[dict[str, Any]]:
    open_bundles = []
    for bundle in bundles or []:
        merged = bundle.get("merged_at") or bundle.get("review", {}).get("merged_at")
        if merged:
            continue
        open_bundles.append(
            {
                "bundle_id": bundle.get("bundle_id", ""),
                "role": bundle.get("role") or bundle.get("scan", {}).get("role", ""),
                "triage_state": bundle.get("triage_state") or bundle.get("review", {}).get("triage_state", "new"),
                "ready_to_merge": bool(bundle.get("ready_to_merge")),
                "review_required_count": bundle.get("review_required_count", 0),
                "contradiction_count": bundle.get("contradiction_count", 0),
                "stale": (bundle.get("base_structure_version") or 1) < current_version,
            }
        )
    return sorted(
        open_bundles,
        key=lambda item: (
            0 if item["review_required_count"] else 1,
            0 if item["stale"] else 1,
            item["bundle_id"],
        ),
    )[:6]


def rank_open_tasks(plan_state: dict[str, Any]) -> list[dict[str, Any]]:
    blockers = {blocker["id"]: blocker for blocker in plan_state.get("blockers", [])}
    ranked = []
    for task in plan_state.get("tasks", []):
        if task.get("status") in {"done", "cancelled"}:
            continue
        risk = 0
        if task.get("status") == "blocked":
            risk += 5
        risk += len(task.get("blocker_ids", [])) * 2
        risk += len(task.get("depends_on", []))
        risk += len(task.get("acceptance_check_ids", []))
        blocker_summaries = [
            blockers[blocker_id]["summary"]
            for blocker_id in task.get("blocker_ids", [])
            if blocker_id in blockers
        ]
        ranked.append(
            {
                "id": task["id"],
                "title": task["title"],
                "role": task.get("role", ""),
                "status": task.get("status", ""),
                "decision_ids": task.get("decision_ids", []),
                "linked_refs": task.get("linked_refs", []),
                "acceptance_check_ids": task.get("acceptance_check_ids", []),
                "blocker_ids": task.get("blocker_ids", []),
                "summary": task.get("summary", ""),
                "blockers": blocker_summaries,
                "priority_score": risk,
            }
        )
    return sorted(ranked, key=lambda item: (-item["priority_score"], item["title"], item["id"]))


def summarize_open_tasks_by_role(plan_state: dict[str, Any]) -> list[dict[str, Any]]:
    counts: dict[str, int] = defaultdict(int)
    for task in plan_state.get("tasks", []):
        if task.get("status") in {"done", "cancelled"}:
            continue
        counts[task.get("role", "builder") or "builder"] += 1
    return [
        {"role": role, "count": counts[role]}
        for role in sorted(counts)
    ]


def build_role_lanes(plan_state: dict[str, Any]) -> list[dict[str, Any]]:
    ranked = rank_open_tasks(plan_state)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    counts: dict[str, int] = defaultdict(int)
    for task in ranked:
        role = task.get("role", "") or "builder"
        counts[role] += 1
        if len(grouped[role]) < 3:
            grouped[role].append(task)
    return [
        {
            "role": role,
            "open_count": counts[role],
            "tasks": grouped.get(role, []),
        }
        for role in sorted(counts)
    ]


def build_handoff_queue(plan_state: dict[str, Any]) -> list[dict[str, Any]]:
    task_titles = {
        task["id"]: task.get("title", task["id"])
        for task in plan_state.get("tasks", [])
    }
    queue = [
        {
            "id": run["id"],
            "role": run.get("role", ""),
            "status": run.get("status", ""),
            "objective": run.get("objective", ""),
            "status_reason": run.get("status_reason", ""),
            "next_action_hint": run.get("next_action_hint", ""),
            "task_ids": run.get("task_ids", []),
            "task_titles": [task_titles.get(task_id, task_id) for task_id in run.get("task_ids", [])],
            "blocked_by": run.get("blocked_by", []),
            "updated_at": run.get("updated_at", ""),
        }
        for run in plan_state.get("agent_runs", [])
        if run.get("status") in {"waiting", "blocked", "failed"}
    ]
    return sorted(queue, key=lambda item: item.get("updated_at", ""), reverse=True)[:6]


def rank_blockers(plan_state: dict[str, Any]) -> list[dict[str, Any]]:
    ranked = []
    for blocker in plan_state.get("blockers", []):
        if blocker.get("status") == "resolved":
            continue
        ranked.append(
            {
                **blocker,
                "priority_score": len(blocker.get("task_ids", [])) * 2 + len(blocker.get("decision_ids", [])),
            }
        )
    return sorted(ranked, key=lambda item: (-item["priority_score"], item["id"]))


def rank_decisions(plan_state: dict[str, Any]) -> list[dict[str, Any]]:
    tasks_by_decision: dict[str, list[dict[str, Any]]] = defaultdict(list)
    blocker_ids_by_task = {
        task["id"]: set(task.get("blocker_ids", []))
        for task in plan_state.get("tasks", [])
    }
    checks_required = {
        check["id"]: bool(check.get("required", True))
        for check in plan_state.get("acceptance_checks", [])
    }
    evidence_by_id = {item["id"]: item for item in plan_state.get("evidence", [])}
    for task in plan_state.get("tasks", []):
        for decision_id in task.get("decision_ids", []):
            tasks_by_decision[decision_id].append(task)

    ranked = []
    for decision in plan_state.get("decisions", []):
        if decision.get("status") == "deprecated":
            continue
        linked_tasks = tasks_by_decision.get(decision["id"], [])
        missing_checks = [
            check_id
            for check_id in decision.get("acceptance_check_ids", [])
            if checks_required.get(check_id, False)
            and not any(evidence_by_id.get(proof_id, {}).get("check_id") == check_id for proof_id in decision.get("evidence_ids", []))
        ]
        open_blocker_count = sum(len(blocker_ids_by_task.get(task["id"], set())) for task in linked_tasks)
        score = open_blocker_count * 3 + len(missing_checks) * 2 + (0 if linked_tasks else 4)
        ranked.append(
            {
                "id": decision["id"],
                "title": decision["title"],
                "status": decision.get("status", ""),
                "linked_refs": decision.get("linked_refs", []),
                "summary": decision.get("summary", ""),
                "linked_task_count": len(linked_tasks),
                "open_blocker_count": open_blocker_count,
                "missing_required_checks": missing_checks,
                "risk_score": score,
            }
        )
    return sorted(ranked, key=lambda item: (-item["risk_score"], item["title"], item["id"]))


def build_critical_path(plan_state: dict[str, Any]) -> list[dict[str, Any]]:
    open_tasks = {
        task["id"]: task
        for task in plan_state.get("tasks", [])
        if task.get("status") not in {"done", "cancelled"}
    }
    indegree = {task_id: 0 for task_id in open_tasks}
    outgoing: dict[str, list[str]] = defaultdict(list)
    for task_id, task in open_tasks.items():
        for dependency in task.get("depends_on", []):
            if dependency not in open_tasks:
                continue
            outgoing[dependency].append(task_id)
            indegree[task_id] += 1
    queue = deque(sorted(task_id for task_id, degree in indegree.items() if degree == 0))
    ordered: list[str] = []
    while queue:
        current = queue.popleft()
        ordered.append(current)
        for target in sorted(outgoing.get(current, [])):
            indegree[target] -= 1
            if indegree[target] == 0:
                queue.append(target)
    if len(ordered) < len(open_tasks):
        ordered.extend(sorted(task_id for task_id in open_tasks if task_id not in ordered))
    return [
        {
            "id": open_tasks[task_id]["id"],
            "title": open_tasks[task_id]["title"],
            "status": open_tasks[task_id]["status"],
            "depends_on": open_tasks[task_id].get("depends_on", []),
            "role": open_tasks[task_id].get("role", ""),
        }
        for task_id in ordered
    ]


def _apply_review_outcome(
    plan_state: dict[str, Any],
    graph: dict[str, Any],
    *,
    bundle_id: str,
    record: dict[str, Any],
    record_kind: str,
    decision: str,
    reviewed_by: str,
    note: str,
    reviewed_at: str,
) -> dict[str, Any]:
    normalized = normalize_plan_state(plan_state, graph=graph)
    refs = _extract_review_refs(record, graph)
    slug = identifierify(record.get("field_id") or record.get("target_id") or record.get("node_id") or record.get("id") or "review")
    decision_id = f"{REVIEW_DECISION_PREFIX}{identifierify(bundle_id)}.{slug}"
    task_id = f"{REVIEW_TASK_PREFIX}{identifierify(bundle_id)}.{slug}"
    blocker_id = f"{REVIEW_BLOCKER_PREFIX}{identifierify(bundle_id)}.{slug}"

    matched_decisions = _find_entities_by_refs(normalized.get("decisions", []), refs)
    matched_tasks = _find_entities_by_refs(normalized.get("tasks", []), refs)
    matched_blockers = _find_entities_by_refs(normalized.get("blockers", []), refs)

    if not matched_decisions and (decision != "rejected" or record_kind == "contradiction"):
        matched_decisions = [
            _upsert_entity(
                normalized["decisions"],
                {
                    "id": decision_id,
                    "title": _review_title(graph, refs, record, record_kind),
                    "kind": f"review_{record_kind}",
                    "status": "proposed",
                    "linked_refs": refs,
                    "acceptance_check_ids": [],
                    "evidence_ids": [],
                    "summary": _review_summary(record, record_kind, note),
                    "created_at": reviewed_at,
                    "updated_at": reviewed_at,
                    "updated_by": reviewed_by or "user",
                },
            )
        ]
    if decision in {"accepted", "deferred", "rejected"} and not matched_tasks and matched_decisions:
        matched_tasks = [
            _upsert_entity(
                normalized["tasks"],
                {
                    "id": task_id,
                    "title": _review_title(graph, refs, record, record_kind),
                    "role": _review_role(graph, refs),
                    "status": "todo",
                    "exploratory": False,
                    "decision_ids": [item["id"] for item in matched_decisions],
                    "linked_refs": refs,
                    "depends_on": [],
                    "acceptance_check_ids": [],
                    "evidence_ids": [],
                    "blocker_ids": [],
                    "summary": _review_summary(record, record_kind, note),
                    "created_at": reviewed_at,
                    "updated_at": reviewed_at,
                    "completed_at": "",
                },
            )
        ]

    if decision == "accepted":
        for item in matched_decisions:
            if item.get("status") != "validated":
                item["status"] = "accepted"
            item["updated_at"] = reviewed_at
            item["updated_by"] = reviewed_by or "user"
        for item in matched_tasks:
            if item.get("status") not in {"in_progress", "done"}:
                item["status"] = "todo"
            item["updated_at"] = reviewed_at
        _resolve_matching_blockers(matched_blockers, reviewed_at)
        _resolve_matching_blockers(
            [blocker for blocker in normalized.get("blockers", []) if blocker.get("id") == blocker_id],
            reviewed_at,
        )
    elif decision == "deferred":
        blocker = _upsert_review_blocker(
            normalized,
            blocker_id=blocker_id,
            refs=refs,
            matched_tasks=matched_tasks,
            matched_decisions=matched_decisions,
            summary=f"{record_kind.title()} from {bundle_id} is deferred pending more evidence.",
            suggested_resolution=_review_resolution_hint(record, record_kind, accepted=False),
            owner=reviewed_by or "user",
            updated_at=reviewed_at,
        )
        matched_blockers = [*matched_blockers, blocker]
        for item in matched_tasks:
            item["status"] = "blocked"
            if blocker["id"] not in item.get("blocker_ids", []):
                item.setdefault("blocker_ids", []).append(blocker["id"])
            item["updated_at"] = reviewed_at
    elif decision == "rejected":
        if record_kind == "contradiction":
            blocker = _upsert_review_blocker(
                normalized,
                blocker_id=blocker_id,
                refs=refs,
                matched_tasks=matched_tasks,
                matched_decisions=matched_decisions,
                summary=f"Canonical decision remains in place for {bundle_id}; implementation still needs reconciliation.",
                suggested_resolution=_review_resolution_hint(record, record_kind, accepted=False),
                owner=reviewed_by or "user",
                updated_at=reviewed_at,
            )
            matched_blockers = [*matched_blockers, blocker]
            for item in matched_tasks:
                item["status"] = "blocked"
                if blocker["id"] not in item.get("blocker_ids", []):
                    item.setdefault("blocker_ids", []).append(blocker["id"])
                item["updated_at"] = reviewed_at
            for item in matched_decisions:
                if item.get("status") != "validated":
                    item["status"] = "accepted"
                item["updated_at"] = reviewed_at
                item["updated_by"] = reviewed_by or "user"
        else:
            for item in normalized.get("decisions", []):
                if item.get("id") == decision_id or (
                    item.get("id", "").startswith(REVIEW_DECISION_PREFIX) and _refs_overlap(refs, item.get("linked_refs", []))
                ):
                    item["status"] = "deprecated"
                    item["updated_at"] = reviewed_at
                    item["updated_by"] = reviewed_by or "user"
            for item in normalized.get("tasks", []):
                if item.get("id") == task_id or (
                    item.get("id", "").startswith(REVIEW_TASK_PREFIX) and _refs_overlap(refs, item.get("linked_refs", []))
                ):
                    item["status"] = "cancelled"
                    item["updated_at"] = reviewed_at
            _resolve_matching_blockers(matched_blockers, reviewed_at)
            _resolve_matching_blockers(
                [blocker for blocker in normalized.get("blockers", []) if blocker.get("id") == blocker_id],
                reviewed_at,
            )

    normalized.setdefault("agreement_log", []).append(
        {
            "id": f"agreement.{record_kind}.{identifierify(bundle_id)}.{slug}.{decision}",
            "kind": f"{record_kind}_review",
            "summary": f"{decision.title()} {record_kind} review for {bundle_id}: {_review_title(graph, refs, record, record_kind)}.",
            "decision_id": matched_decisions[0]["id"] if matched_decisions else "",
            "task_id": matched_tasks[0]["id"] if matched_tasks else "",
            "actor": reviewed_by or "user",
            "at": reviewed_at,
        }
    )
    normalized["updated_at"] = reviewed_at
    normalized["updated_by"] = reviewed_by or "user"
    _recompute_decision_statuses(normalized)
    return normalize_plan_state(normalized, graph=graph)


def _extract_review_refs(record: dict[str, Any], graph: dict[str, Any]) -> list[str]:
    index = build_index(normalize_graph(graph))
    allowed_refs = {
        *index["nodes"].keys(),
        *index["edges"].keys(),
        *index["field_by_id"].keys(),
    }
    refs: list[str] = []
    for key in ("field_id", "target_id", "node_id"):
        value = str(record.get(key, "") or "").strip()
        if value in index["field_ref_to_id"]:
            value = index["field_ref_to_id"][value]
        if value and value in allowed_refs and value not in refs:
            refs.append(value)
    payload = record.get("payload", {}) or {}
    for key in ("new_binding", "previous_binding", "primary_binding"):
        value = str(payload.get(key, "") or "").strip()
        if value in index["field_ref_to_id"]:
            value = index["field_ref_to_id"][value]
        if value and value in allowed_refs and value not in refs:
            refs.append(value)
    return refs


def _review_title(graph: dict[str, Any], refs: list[str], record: dict[str, Any], record_kind: str) -> str:
    index = build_index(normalize_graph(graph))
    for ref in refs:
        node = index["nodes"].get(ref)
        if node:
            return f"{node['label']} review"
        owner_id = index["field_owner"].get(ref)
        if owner_id:
            field_name = index["field_name_by_id"].get(ref, ref)
            node = index["nodes"].get(owner_id)
            if node:
                return f"{node['label']}.{field_name}"
    return record.get("message") or record.get("id") or f"{record_kind.title()} review"


def _review_summary(record: dict[str, Any], record_kind: str, note: str) -> str:
    base = record.get("message") or record.get("why_this_matters") or f"{record_kind.title()} review item"
    if note.strip():
        return f"{base} Note: {note.strip()}"
    return base


def _review_role(graph: dict[str, Any], refs: list[str]) -> str:
    index = build_index(normalize_graph(graph))
    for ref in refs:
        node = index["nodes"].get(ref)
        if not node and ref in index["field_owner"]:
            node = index["nodes"].get(index["field_owner"][ref])
        if not node:
            continue
        if node["kind"] == "contract" and node.get("extension_type") == "ui":
            return "builder"
        if node["kind"] == "contract":
            return "builder"
        if node["kind"] == "data":
            return "builder"
        if node["kind"] == "compute":
            return "builder"
    return "qa" if any("contradiction" in ref for ref in refs) else "architect"


def _review_resolution_hint(record: dict[str, Any], record_kind: str, accepted: bool) -> str:
    if record_kind == "contradiction":
        return (
            "Confirm whether canonical should change or bring implementation back in line before clearing this blocker."
        )
    if accepted:
        return "Merge the accepted patch through the normal review flow to clear this blocker."
    return "Gather more evidence or revise the patch before moving it forward."


def _find_entities_by_refs(items: list[dict[str, Any]], refs: list[str]) -> list[dict[str, Any]]:
    return [item for item in items if _refs_overlap(refs, item.get("linked_refs", []))]


def _refs_overlap(left: list[str], right: list[str]) -> bool:
    return bool(set(left or []) & set(right or []))


def _upsert_entity(collection: list[dict[str, Any]], payload: dict[str, Any]) -> dict[str, Any]:
    for index, item in enumerate(collection):
        if item.get("id") == payload.get("id"):
            updated = {**item, **payload}
            collection[index] = updated
            return updated
    collection.append(payload)
    return payload


def _review_entity_ids(bundle_id: str, patch_id: str) -> dict[str, str]:
    slug = identifierify(patch_id or "review")
    bundle_slug = identifierify(bundle_id or "bundle")
    return {
        "decision": f"{REVIEW_DECISION_PREFIX}{bundle_slug}.{slug}",
        "task": f"{REVIEW_TASK_PREFIX}{bundle_slug}.{slug}",
        "blocker": f"{REVIEW_BLOCKER_PREFIX}{bundle_slug}.{slug}",
    }


def _apply_identifier_maps(
    plan_state: dict[str, Any],
    *,
    decision_map: dict[str, str],
    task_map: dict[str, str],
    blocker_map: dict[str, str],
) -> None:
    if not decision_map and not task_map and not blocker_map:
        return

    for item in plan_state.get("decisions", []):
        item_id = item.get("id", "")
        if item_id in decision_map:
            item["id"] = decision_map[item_id]

    for item in plan_state.get("tasks", []):
        item_id = item.get("id", "")
        if item_id in task_map:
            item["id"] = task_map[item_id]
        item["decision_ids"] = [decision_map.get(entry, entry) for entry in item.get("decision_ids", [])]
        item["depends_on"] = [task_map.get(entry, entry) for entry in item.get("depends_on", [])]
        item["blocker_ids"] = [blocker_map.get(entry, entry) for entry in item.get("blocker_ids", [])]

    for item in plan_state.get("blockers", []):
        item_id = item.get("id", "")
        if item_id in blocker_map:
            item["id"] = blocker_map[item_id]
        item["task_ids"] = [task_map.get(entry, entry) for entry in item.get("task_ids", [])]
        item["decision_ids"] = [decision_map.get(entry, entry) for entry in item.get("decision_ids", [])]

    for item in plan_state.get("attachments", []):
        item["linked_decision_ids"] = [decision_map.get(entry, entry) for entry in item.get("linked_decision_ids", [])]
        item["linked_task_ids"] = [task_map.get(entry, entry) for entry in item.get("linked_task_ids", [])]

    for item in plan_state.get("agent_runs", []):
        item["task_ids"] = [task_map.get(entry, entry) for entry in item.get("task_ids", [])]
        item["blocked_by"] = [blocker_map.get(entry, entry) for entry in item.get("blocked_by", [])]

    for item in plan_state.get("agreement_log", []):
        decision_id = item.get("decision_id", "")
        task_id = item.get("task_id", "")
        if decision_id in decision_map:
            item["decision_id"] = decision_map[decision_id]
        if task_id in task_map:
            item["task_id"] = task_map[task_id]


def _upsert_review_blocker(
    plan_state: dict[str, Any],
    *,
    blocker_id: str,
    refs: list[str],
    matched_tasks: list[dict[str, Any]],
    matched_decisions: list[dict[str, Any]],
    summary: str,
    suggested_resolution: str,
    owner: str,
    updated_at: str,
) -> dict[str, Any]:
    existing = next((blocker for blocker in plan_state.get("blockers", []) if blocker.get("id") == blocker_id), None)
    payload = {
        "id": blocker_id,
        "type": "review_followup",
        "status": "open",
        "task_ids": [task["id"] for task in matched_tasks],
        "decision_ids": [decision["id"] for decision in matched_decisions],
        "linked_refs": refs,
        "summary": summary,
        "suggested_resolution": suggested_resolution,
        "owner": owner,
        "created_at": existing.get("created_at", updated_at) if existing else updated_at,
        "updated_at": updated_at,
        "resolved_at": "",
    }
    return _upsert_entity(plan_state["blockers"], payload)


def _resolve_matching_blockers(blockers: list[dict[str, Any]], resolved_at: str) -> None:
    for blocker in blockers:
        blocker["status"] = "resolved"
        blocker["updated_at"] = resolved_at
        blocker["resolved_at"] = resolved_at


def _recompute_decision_statuses(plan_state: dict[str, Any]) -> None:
    tasks_by_decision: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in plan_state.get("tasks", []):
        for decision_id in task.get("decision_ids", []):
            tasks_by_decision[decision_id].append(task)
    for decision in plan_state.get("decisions", []):
        if decision.get("status") == "deprecated":
            continue
        linked_tasks = [
            task
            for task in tasks_by_decision.get(decision["id"], [])
            if not task.get("exploratory") and task.get("status") != "cancelled"
        ]
        if not linked_tasks:
            decision["status"] = "proposed"
            continue
        statuses = {task.get("status") for task in linked_tasks}
        if statuses == {"done"}:
            decision["status"] = "validated"
        elif "in_progress" in statuses or "blocked" in statuses:
            decision["status"] = "in_progress"
        else:
            decision["status"] = "accepted"
