from __future__ import annotations

from typing import Any

from .types import build_index, normalize_graph, normalize_plan_state


def build_agent_assignment_brief(
    *,
    contract: dict[str, Any],
    graph: dict[str, Any],
    plan_state: dict[str, Any],
    source_of_truth: dict[str, Any] | None = None,
    task_id: str = "",
    run_id: str = "",
) -> dict[str, Any]:
    normalized = normalize_plan_state(plan_state, graph=graph)
    source_of_truth = source_of_truth or {}
    index = build_index(normalize_graph(graph))

    tasks_by_id = {item.get("id", ""): item for item in normalized.get("tasks", [])}
    decisions_by_id = {item.get("id", ""): item for item in normalized.get("decisions", [])}
    blockers_by_id = {item.get("id", ""): item for item in normalized.get("blockers", [])}
    checks_by_id = {item.get("id", ""): item for item in normalized.get("acceptance_checks", [])}
    runs_by_id = {item.get("id", ""): item for item in normalized.get("agent_runs", [])}

    run = runs_by_id.get(run_id) if run_id else None
    if run_id and run is None:
        raise ValueError(f"Agent run not found: {run_id}")

    if not task_id and run and run.get("task_ids"):
        task_id = str(run["task_ids"][0])
    task = tasks_by_id.get(task_id) if task_id else None
    if task_id and task is None:
        raise ValueError(f"Execution task not found: {task_id}")

    decision_ids = _ordered_unique(
        [
            *(task.get("decision_ids", []) if task else []),
            *(
                decision["id"]
                for decision in normalized.get("decisions", [])
                if task and set(task.get("linked_refs", [])) & set(decision.get("linked_refs", []))
            ),
        ]
    )
    linked_decisions = [decisions_by_id[decision_id] for decision_id in decision_ids if decision_id in decisions_by_id]

    blocker_ids = _ordered_unique(
        [
            *(task.get("blocker_ids", []) if task else []),
            *(run.get("blocked_by", []) if run else []),
        ]
    )
    linked_blockers = [blockers_by_id[blocker_id] for blocker_id in blocker_ids if blocker_id in blockers_by_id]

    acceptance_check_ids = _ordered_unique(
        [
            *(task.get("acceptance_check_ids", []) if task else []),
            *(check_id for decision in linked_decisions for check_id in decision.get("acceptance_check_ids", [])),
        ]
    )
    acceptance_checks = [checks_by_id[check_id] for check_id in acceptance_check_ids if check_id in checks_by_id]

    linked_refs = _ordered_unique(
        [
            *(task.get("linked_refs", []) if task else []),
            *(ref for decision in linked_decisions for ref in decision.get("linked_refs", [])),
            *(ref for blocker in linked_blockers for ref in blocker.get("linked_refs", [])),
        ]
    )
    ref_context = [_describe_execution_ref(ref, index) for ref in linked_refs[:10]]

    objective = (
        (run or {}).get("objective")
        or (task or {}).get("title")
        or str(contract.get("mission") or contract.get("summary") or "Advance the highest-priority workbench task.")
    )
    top_open_tasks = source_of_truth.get("top_open_tasks", [])[:3]
    top_blocker = source_of_truth.get("top_blocker")
    highest_risk_decision = source_of_truth.get("highest_risk_decision")

    prompt_lines = [
        f"# {contract.get('id', 'agent')} brief",
        "",
        "## Mission",
        str(contract.get("mission") or contract.get("summary") or "Follow the workbench contract."),
        "",
        "## Objective",
        objective,
        "",
    ]
    if task:
        prompt_lines.extend(
            [
                "## Assigned task",
                f"- id: {task.get('id', '')}",
                f"- title: {task.get('title', '')}",
                f"- role: {task.get('role', '')}",
                f"- status: {task.get('status', '')}",
                f"- summary: {task.get('summary', '') or 'No task summary yet.'}",
                "",
            ]
        )
    if run:
        prompt_lines.extend(
            [
                "## Current run",
                f"- id: {run.get('id', '')}",
                f"- status: {run.get('status', '')}",
                f"- reason: {run.get('status_reason', '') or 'No explicit status reason.'}",
                f"- next action: {run.get('next_action_hint', '') or 'No next action hint yet.'}",
                "",
            ]
        )
    prompt_lines.extend(
        [
            "## Linked decisions",
            *(
                [f"- {decision.get('id', '')}: {decision.get('title', '')} ({decision.get('status', '')})" for decision in linked_decisions]
                or ["- None linked yet."]
            ),
            "",
            "## Blockers",
            *(
                [f"- {blocker.get('id', '')}: {blocker.get('summary', '')} [{blocker.get('status', '')}]" for blocker in linked_blockers]
                or ["- None active."]
            ),
            "",
            "## Acceptance checks",
            *(
                [f"- {check.get('id', '')}: {check.get('label', '')}" for check in acceptance_checks]
                or ["- None linked."]
            ),
            "",
            "## Structural refs",
            *([f"- {entry}" for entry in ref_context] or ["- No graph refs linked."]),
            "",
            "## Priority context",
            *(
                [f"- Top task: {item.get('title', '')} [{item.get('status', '')}]" for item in top_open_tasks]
                or ["- No open task ranking available."]
            ),
            f"- Top blocker: {(top_blocker or {}).get('summary', '') or 'None'}",
            f"- Highest-risk decision: {(highest_risk_decision or {}).get('title', '') or 'None'}",
            "",
            "## Operating loop",
            *([f"- {step}" for step in contract.get("operating_loop", [])] or ["- Follow the current execution state."]),
            "",
            "## Handoff requirements",
            *([f"- {step}" for step in contract.get("handoff_requirements", [])] or ["- Leave a concise, useful handoff."]),
            "",
            "## Governance constraints",
            *([f"- Forbidden: {route}" for route in contract.get("forbidden_actions", [])] or ["- Do not mutate canonical structure directly."]),
            "",
            "## Starter prompt",
            str(contract.get("starter_prompt") or ""),
        ]
    )
    prompt_markdown = "\n".join(prompt_lines).strip()
    return {
        "contract": contract,
        "task": task or None,
        "run": run or None,
        "linked_decisions": linked_decisions,
        "linked_blockers": linked_blockers,
        "acceptance_checks": acceptance_checks,
        "linked_refs": linked_refs,
        "ref_context": ref_context,
        "prompt_markdown": prompt_markdown,
        "prompt_text": prompt_markdown,
    }


def _ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _describe_execution_ref(ref: str, index: dict[str, Any]) -> str:
    node = index["nodes"].get(ref)
    if node is not None:
        return f"{ref} ({node.get('label', ref)})"
    owner_id = index["field_owner"].get(ref)
    if owner_id:
        owner = index["nodes"].get(owner_id)
        field_name = index["field_name_by_id"].get(ref, ref)
        if owner is not None:
            return f"{field_name} on {owner.get('label', owner_id)}"
    edge = index["edges"].get(ref)
    if edge is not None:
        return f"{ref} ({edge.get('source', '')} -> {edge.get('target', '')})"
    return ref
