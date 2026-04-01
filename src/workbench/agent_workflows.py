from __future__ import annotations

from typing import Any

from .agent_briefs import build_agent_assignment_brief
from .execution import rank_blockers, rank_decisions, rank_open_tasks, utc_timestamp
from .types import identifierify, normalize_plan_state


ACTIVE_RUN_STATUSES = {"planned", "running", "waiting", "blocked", "failed"}
RESUMABLE_RUN_STATUSES = {"running", "waiting", "blocked", "failed"}


def build_agent_workflow(
    *,
    contract: dict[str, Any],
    graph: dict[str, Any],
    plan_state: dict[str, Any],
    source_of_truth: dict[str, Any] | None = None,
    task_id: str = "",
    run_id: str = "",
    bundle_id: str = "",
    root_path: str = "",
    doc_paths: list[str] | None = None,
    selected_paths: list[str] | None = None,
) -> dict[str, Any]:
    normalized = normalize_plan_state(plan_state, graph=graph)
    source_of_truth = source_of_truth or {}
    role = str(contract.get("role") or "builder")

    tasks_by_id = {item.get("id", ""): item for item in normalized.get("tasks", [])}
    decisions_by_id = {item.get("id", ""): item for item in normalized.get("decisions", [])}
    blockers_by_id = {item.get("id", ""): item for item in normalized.get("blockers", [])}
    runs_by_id = {item.get("id", ""): item for item in normalized.get("agent_runs", [])}

    explicit_run = _lookup_or_error(runs_by_id, run_id, "Agent run") if run_id else None
    explicit_task = _lookup_or_error(tasks_by_id, task_id, "Execution task") if task_id else None

    resumable_run = explicit_run or _select_resumable_run(normalized, role, task_id=task_id)
    recommended_task = explicit_task or _select_role_task(normalized, role)
    recommended_decision = _select_role_decision(
        normalized,
        role,
        task=recommended_task,
        explicit_task=explicit_task,
    )
    recommended_blocker = _select_role_blocker(
        normalized,
        role,
        task=recommended_task,
        decision=recommended_decision,
        run=resumable_run,
    )
    recommended_bundle = _select_role_bundle(source_of_truth, role, bundle_id=bundle_id)
    scan_context = {
        "bundle_id": bundle_id or (recommended_bundle or {}).get("bundle_id", ""),
        "root_path": root_path,
        "doc_paths": list(doc_paths or []),
        "selected_paths": list(selected_paths or []),
    }

    focus = _build_focus(
        contract=contract,
        task=recommended_task,
        decision=recommended_decision,
        blocker=recommended_blocker,
        bundle=recommended_bundle,
        run=resumable_run,
    )
    starter_run = _build_starter_run(
        contract=contract,
        plan_state=normalized,
        task=recommended_task,
        decision=recommended_decision,
        blocker=recommended_blocker,
        bundle=recommended_bundle,
        resumable_run=resumable_run,
    )
    brief = build_agent_assignment_brief(
        contract=contract,
        graph=graph,
        plan_state=normalized,
        source_of_truth=source_of_truth,
        task_id=(recommended_task or {}).get("id", ""),
        run_id=(resumable_run or {}).get("id", ""),
    )
    return {
        "contract": contract,
        "role": role,
        "focus": focus,
        "recommended_task": recommended_task,
        "recommended_decision": recommended_decision,
        "recommended_blocker": recommended_blocker,
        "recommended_bundle": recommended_bundle,
        "scan_context": scan_context,
        "resumable_run": resumable_run,
        "starter_run": starter_run,
        "recommended_actions": _build_recommended_actions(
            contract=contract,
            task=recommended_task,
            decision=recommended_decision,
            blocker=recommended_blocker,
            bundle=recommended_bundle,
            resumable_run=resumable_run,
            starter_run=starter_run,
        ),
        "brief": brief,
    }


def launch_agent_workflow_run(
    *,
    workflow: dict[str, Any],
    plan_state: dict[str, Any],
    updated_by: str,
) -> tuple[dict[str, Any], str]:
    normalized = normalize_plan_state(plan_state)
    resumable_run = workflow.get("resumable_run")
    if resumable_run:
        return resumable_run, "resume_existing"

    starter = workflow.get("starter_run") or {}
    agent_run = dict(starter.get("agent_run") or {})
    if not agent_run:
        raise ValueError("No starter run is available for this workflow.")

    timestamp = utc_timestamp()
    if not agent_run.get("id"):
        base = identifierify(agent_run.get("objective") or workflow.get("role") or "agent_run") or "agent_run"
        existing_ids = {item.get("id", "") for item in normalized.get("agent_runs", [])}
        candidate = f"run.{base}"
        counter = 2
        while candidate in existing_ids:
            candidate = f"run.{base}.{counter}"
            counter += 1
        agent_run["id"] = candidate

    agent_run.setdefault("started_at", timestamp)
    agent_run["updated_at"] = timestamp
    if not agent_run.get("events"):
        agent_run["events"] = []
    launch_summary = f"{workflow.get('contract', {}).get('id', workflow.get('role', 'agent'))} workflow launched."
    agent_run["last_summary"] = agent_run.get("last_summary") or launch_summary
    agent_run["events"].append(
        {
            "id": f"{agent_run['id']}.launch.{timestamp.replace(':', '').replace('-', '')}",
            "kind": "launch",
            "summary": launch_summary,
            "created_at": timestamp,
            "created_by": updated_by,
        }
    )
    normalized["agent_runs"] = [*normalized.get("agent_runs", []), agent_run]
    return agent_run, "created"


def _lookup_or_error(items: dict[str, dict[str, Any]], item_id: str, label: str) -> dict[str, Any]:
    item = items.get(item_id)
    if item is None:
        raise ValueError(f"{label} not found: {item_id}")
    return item


def _select_resumable_run(plan_state: dict[str, Any], role: str, *, task_id: str = "") -> dict[str, Any] | None:
    candidates = [
        run
        for run in plan_state.get("agent_runs", [])
        if run.get("role") == role and run.get("status") in RESUMABLE_RUN_STATUSES
    ]
    if task_id:
        task_matches = [run for run in candidates if task_id in (run.get("task_ids", []) or [])]
        if task_matches:
            candidates = task_matches
    if not candidates:
        return None
    status_rank = {"running": 0, "blocked": 1, "waiting": 2, "failed": 3}
    candidates = sorted(
        candidates,
        key=lambda run: run.get("updated_at") or run.get("started_at") or "",
        reverse=True,
    )
    return sorted(candidates, key=lambda run: status_rank.get(run.get("status", ""), 10))[0]


def _select_role_task(plan_state: dict[str, Any], role: str) -> dict[str, Any] | None:
    ranked = rank_open_tasks(plan_state)
    preferred = [task for task in ranked if task.get("role") == role]
    if role == "qa":
        preferred = [
            task
            for task in ranked
            if task.get("role") == "qa" or task.get("status") == "blocked" or (task.get("acceptance_check_ids") or [])
        ] or preferred
    if role == "architect":
        preferred = [task for task in ranked if task.get("role") == "architect"] or preferred
    if role == "scout":
        preferred = [task for task in ranked if task.get("role") == "scout"] or preferred
    return preferred[0] if preferred else None


def _select_role_decision(
    plan_state: dict[str, Any],
    role: str,
    *,
    task: dict[str, Any] | None,
    explicit_task: dict[str, Any] | None,
) -> dict[str, Any] | None:
    decisions_by_id = {item.get("id", ""): item for item in plan_state.get("decisions", [])}
    if task:
        for decision_id in task.get("decision_ids", []) or []:
            if decision_id in decisions_by_id:
                return decisions_by_id[decision_id]

    ranked = rank_decisions(plan_state)
    if role == "architect":
        untasked = [item for item in ranked if not item.get("linked_task_count")]
        if untasked:
            return decisions_by_id.get(untasked[0]["id"])
        for status in ("proposed", "accepted", "in_progress"):
            match = next((item for item in ranked if item.get("status") == status), None)
            if match:
                return decisions_by_id.get(match["id"])
    if role == "qa":
        missing_proof = [item for item in ranked if item.get("missing_required_checks")]
        if missing_proof:
            return decisions_by_id.get(missing_proof[0]["id"])
    if explicit_task is None and ranked:
        return decisions_by_id.get(ranked[0]["id"])
    return None


def _select_role_blocker(
    plan_state: dict[str, Any],
    role: str,
    *,
    task: dict[str, Any] | None,
    decision: dict[str, Any] | None,
    run: dict[str, Any] | None,
) -> dict[str, Any] | None:
    blockers_by_id = {item.get("id", ""): item for item in plan_state.get("blockers", [])}
    task_blockers = [
        blockers_by_id[blocker_id]
        for blocker_id in ((task or {}).get("blocker_ids", []) or [])
        if blocker_id in blockers_by_id and blockers_by_id[blocker_id].get("status") != "resolved"
    ]
    if task_blockers:
        return task_blockers[0]
    run_blockers = [
        blockers_by_id[blocker_id]
        for blocker_id in ((run or {}).get("blocked_by", []) or [])
        if blocker_id in blockers_by_id and blockers_by_id[blocker_id].get("status") != "resolved"
    ]
    if run_blockers:
        return run_blockers[0]
    ranked = rank_blockers(plan_state)
    if role == "qa":
        return ranked[0] if ranked else None
    if role == "scout" and ranked:
        return ranked[0]
    if decision:
        return next(
            (
                blocker
                for blocker in ranked
                if decision.get("id", "") in (blocker.get("decision_ids", []) or [])
            ),
            ranked[0] if ranked else None,
        )
    return None


def _select_role_bundle(source_of_truth: dict[str, Any], role: str, *, bundle_id: str = "") -> dict[str, Any] | None:
    bundles = list(source_of_truth.get("open_bundles", []) or [])
    if not bundles:
        return None
    if bundle_id:
        explicit = next((bundle for bundle in bundles if bundle.get("bundle_id") == bundle_id), None)
        if explicit is not None:
            return explicit
    if role == "scout":
        return bundles[0]
    if role == "qa":
        needs_review = [
            bundle
            for bundle in bundles
            if bundle.get("review_required_count") or bundle.get("contradiction_count") or bundle.get("stale")
        ]
        return needs_review[0] if needs_review else bundles[0]
    return None


def _build_focus(
    *,
    contract: dict[str, Any],
    task: dict[str, Any] | None,
    decision: dict[str, Any] | None,
    blocker: dict[str, Any] | None,
    bundle: dict[str, Any] | None,
    run: dict[str, Any] | None,
) -> dict[str, str]:
    if run:
        return {
            "kind": "run",
            "id": str(run.get("id", "")),
            "title": str(run.get("objective") or run.get("id") or "Resumable run"),
            "subtitle": str(run.get("status_reason") or run.get("status") or ""),
        }
    if task:
        return {
            "kind": "task",
            "id": str(task.get("id", "")),
            "title": str(task.get("title") or task.get("id") or "Execution task"),
            "subtitle": str(task.get("status") or ""),
        }
    if decision:
        return {
            "kind": "decision",
            "id": str(decision.get("id", "")),
            "title": str(decision.get("title") or decision.get("id") or "Decision"),
            "subtitle": str(decision.get("status") or ""),
        }
    if blocker:
        return {
            "kind": "blocker",
            "id": str(blocker.get("id", "")),
            "title": str(blocker.get("summary") or blocker.get("id") or "Blocker"),
            "subtitle": str(blocker.get("status") or ""),
        }
    if bundle:
        return {
            "kind": "bundle",
            "id": str(bundle.get("bundle_id", "")),
            "title": str(bundle.get("bundle_id") or "Review bundle"),
            "subtitle": str(bundle.get("triage_state") or ""),
        }
    return {
        "kind": "contract",
        "id": str(contract.get("id", "")),
        "title": str(contract.get("summary") or contract.get("id") or "Agent workflow"),
        "subtitle": str(contract.get("mission") or ""),
    }


def _build_starter_run(
    *,
    contract: dict[str, Any],
    plan_state: dict[str, Any],
    task: dict[str, Any] | None,
    decision: dict[str, Any] | None,
    blocker: dict[str, Any] | None,
    bundle: dict[str, Any] | None,
    resumable_run: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if resumable_run:
        return {
            "mode": "resume_existing",
            "agent_run": resumable_run,
        }

    role = str(contract.get("role") or "builder")
    objective = (
        (task or {}).get("title")
        or (decision or {}).get("title")
        or (blocker or {}).get("summary")
        or (
            f"Review bundle {(bundle or {}).get('bundle_id', '')}"
            if bundle
            else str(contract.get("mission") or contract.get("summary") or "Advance the next workflow step.")
        )
    )
    if not objective.strip():
        return None
    blocker_ids = list((task or {}).get("blocker_ids", []) or [])
    if blocker and blocker.get("id") and blocker["id"] not in blocker_ids:
        blocker_ids.append(blocker["id"])
    task_ids = list((task or {}).get("id") and [task["id"]] or [])
    status = "running"
    status_reason = ""
    next_action_hint = ""
    if blocker_ids:
        status = "blocked"
        status_reason = (blocker or {}).get("summary") or "Blocked on a structured blocker."
        next_action_hint = (blocker or {}).get("suggested_resolution") or "Resolve the blocker before resuming execution."
    elif role == "architect":
        next_action_hint = "Translate the focus decision into a small, ordered next step and leave a clear handoff."
    elif role == "scout":
        next_action_hint = "Inspect drift, refresh evidence, and return a reviewable finding."
    elif role == "qa":
        next_action_hint = "Validate required proof and contradiction state before calling work safe to merge."
    else:
        next_action_hint = "Advance the assigned task and leave structured evidence or blockers behind."
    return {
        "mode": "create_new",
        "agent_run": {
            "role": role,
            "status": status,
            "status_reason": status_reason,
            "next_action_hint": next_action_hint,
            "blocked_by": blocker_ids,
            "task_ids": task_ids,
            "objective": objective,
            "bundle_id": (bundle or {}).get("bundle_id", ""),
            "branch": "",
            "outputs": [],
            "events": [],
        },
    }


def _build_recommended_actions(
    *,
    contract: dict[str, Any],
    task: dict[str, Any] | None,
    decision: dict[str, Any] | None,
    blocker: dict[str, Any] | None,
    bundle: dict[str, Any] | None,
    resumable_run: dict[str, Any] | None,
    starter_run: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    contract_id = str(contract.get("id", ""))
    role = str(contract.get("role") or "builder")
    actions: list[dict[str, Any]] = [
        {
            "kind": "read_source_of_truth",
            "label": "Read source of truth",
            "reason": "Start from the current agreed graph, execution state, and review context.",
            "route": "/api/source-of-truth",
            "method": "GET",
        }
    ]
    if resumable_run:
        actions.append(
            {
                "kind": "resume_run",
                "label": f"Resume {resumable_run.get('id', 'agent run')}",
                "reason": resumable_run.get("status_reason") or "There is already active context for this role.",
                "route": f"/api/agent-contracts/{contract_id}/launch",
                "method": "POST",
                "params": {"run_id": resumable_run.get("id", "")},
            }
        )
    elif starter_run and starter_run.get("agent_run"):
        params: dict[str, Any] = {}
        if task and task.get("id"):
            params["task_id"] = task["id"]
        actions.append(
            {
                "kind": "start_run",
                "label": "Start guided run",
                "reason": "Create a resumable run object instead of relying on private prompt state.",
                "route": f"/api/agent-contracts/{contract_id}/launch",
                "method": "POST",
                "params": params,
            }
        )

    if role == "architect":
        if decision and not _decision_has_active_tasks(decision, task):
            actions.append(
                {
                    "kind": "derive_tasks",
                    "label": "Derive ordered tasks",
                    "reason": "This decision needs explicit execution steps before builders can act safely.",
                    "route": "/api/plan-state/derive-tasks",
                    "method": "POST",
                }
            )
        elif decision:
            actions.append(
                {
                    "kind": "refine_decision",
                    "label": f"Refine {decision.get('title', decision.get('id', 'decision'))}",
                    "reason": "Keep the decision and task ordering aligned with current evidence.",
                    "route": "/api/plan-state",
                    "method": "PUT",
                }
            )
    if role == "scout":
        if bundle:
            actions.append(
                {
                    "kind": "inspect_bundle",
                    "label": f"Inspect {bundle.get('bundle_id', 'open bundle')}",
                    "reason": "A review bundle already exists and likely contains the highest-signal drift.",
                    "route": f"/api/structure/bundles/{bundle.get('bundle_id', '')}",
                    "method": "GET",
                }
            )
        else:
            actions.append(
                {
                    "kind": "run_scan",
                    "label": "Run structure scan",
                    "reason": "No open review bundle exists, so the next step is to collect fresh evidence.",
                    "route": "/api/structure/scan",
                    "method": "POST",
                }
            )
    if role == "builder":
        if blocker:
            actions.append(
                {
                    "kind": "resolve_blocker",
                    "label": f"Resolve blocker {blocker.get('id', '')}",
                    "reason": blocker.get("suggested_resolution") or blocker.get("summary") or "A blocker is preventing forward progress.",
                    "route": "/api/plan-state",
                    "method": "PUT",
                }
            )
        elif task:
            actions.append(
                {
                    "kind": "advance_task",
                    "label": f"Advance {task.get('title', task.get('id', 'task'))}",
                    "reason": task.get("summary") or "This is the highest-priority builder task right now.",
                    "route": "/api/plan-state",
                    "method": "PUT",
                }
            )
    if role == "qa":
        if bundle:
            actions.append(
                {
                    "kind": "review_bundle",
                    "label": f"Review {bundle.get('bundle_id', 'bundle')}",
                    "reason": "Review state needs to stay aligned with execution proof and contradictions.",
                    "route": f"/api/structure/bundles/{bundle.get('bundle_id', '')}/review-batch",
                    "method": "POST",
                }
            )
        if decision:
            actions.append(
                {
                    "kind": "validate_checks",
                    "label": f"Validate {decision.get('title', decision.get('id', 'decision'))}",
                    "reason": "This decision carries the highest current QA risk or missing proof.",
                    "route": "/api/plan-state",
                    "method": "PUT",
                }
            )

    brief_params: dict[str, str] = {}
    if task and task.get("id"):
        brief_params["task_id"] = task["id"]
    if resumable_run and resumable_run.get("id"):
        brief_params["run_id"] = resumable_run["id"]
    actions.append(
        {
            "kind": "load_brief",
            "label": "Load assignment brief",
            "reason": "Generate a current prompt bundle grounded in the latest execution state.",
            "route": f"/api/agent-contracts/{contract_id}/brief",
            "method": "GET",
            "params": brief_params,
        }
    )
    return actions


def _decision_has_active_tasks(decision: dict[str, Any], task: dict[str, Any] | None) -> bool:
    if task and decision.get("id") in (task.get("decision_ids", []) or []):
        return True
    return decision.get("status") in {"in_progress", "validated"}
