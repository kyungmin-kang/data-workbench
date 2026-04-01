from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import Field, ValidationError

from .types_base import PlanStateValidationError, StrictWorkbenchModel


class AcceptanceCheck(StrictWorkbenchModel):
    id: str
    label: str
    kind: str = "manual"
    linked_refs: list[str] = Field(default_factory=list)
    required: bool = True


class AttachmentRef(StrictWorkbenchModel):
    id: str
    kind: str = "file"
    label: str = ""
    path_or_url: str = ""
    linked_decision_ids: list[str] = Field(default_factory=list)
    linked_task_ids: list[str] = Field(default_factory=list)
    added_at: str = ""
    added_by: str = ""


class EvidenceProof(StrictWorkbenchModel):
    id: str
    check_id: str
    summary: str
    attachment_ids: list[str] = Field(default_factory=list)
    status: Literal["recorded", "verified", "rejected"] = "recorded"
    recorded_by: str = ""
    recorded_at: str = ""


class DecisionUnit(StrictWorkbenchModel):
    id: str
    title: str
    kind: str = "general"
    status: Literal["proposed", "accepted", "in_progress", "validated", "deprecated"] = "proposed"
    linked_refs: list[str] = Field(default_factory=list)
    acceptance_check_ids: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    supersedes_decision_id: str = ""
    locked: bool = False
    summary: str = ""
    created_at: str = ""
    updated_at: str = ""
    updated_by: str = ""


class ExecutionTask(StrictWorkbenchModel):
    id: str
    title: str
    role: str = "builder"
    status: Literal["todo", "in_progress", "blocked", "done", "cancelled"] = "todo"
    exploratory: bool = False
    exploration_goal: str = ""
    decision_ids: list[str] = Field(default_factory=list)
    linked_refs: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    acceptance_check_ids: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    blocker_ids: list[str] = Field(default_factory=list)
    summary: str = ""
    created_at: str = ""
    updated_at: str = ""
    completed_at: str = ""


class Blocker(StrictWorkbenchModel):
    id: str
    type: str = "unknown"
    status: Literal["open", "in_progress", "resolved"] = "open"
    task_ids: list[str] = Field(default_factory=list)
    decision_ids: list[str] = Field(default_factory=list)
    linked_refs: list[str] = Field(default_factory=list)
    summary: str
    suggested_resolution: str = ""
    owner: str = ""
    created_at: str = ""
    updated_at: str = ""
    resolved_at: str = ""


class AgentRunEvent(StrictWorkbenchModel):
    id: str
    kind: str = "note"
    summary: str
    created_at: str = ""
    created_by: str = ""


class AgentRun(StrictWorkbenchModel):
    id: str
    role: str
    status: Literal["planned", "running", "waiting", "blocked", "completed", "failed", "cancelled"] = "planned"
    status_reason: str = ""
    next_action_hint: str = ""
    blocked_by: list[str] = Field(default_factory=list)
    task_ids: list[str] = Field(default_factory=list)
    objective: str = ""
    bundle_id: str = ""
    branch: str = ""
    started_at: str = ""
    updated_at: str = ""
    last_summary: str = ""
    outputs: list[str] = Field(default_factory=list)
    events: list[AgentRunEvent] = Field(default_factory=list)


class AgreementLogEntry(StrictWorkbenchModel):
    id: str
    kind: str = "note"
    summary: str
    decision_id: str = ""
    task_id: str = ""
    actor: str = ""
    at: str = ""


class PlanState(StrictWorkbenchModel):
    revision: int = 0
    updated_at: str = ""
    updated_by: str = ""
    decisions: list[DecisionUnit] = Field(default_factory=list)
    tasks: list[ExecutionTask] = Field(default_factory=list)
    blockers: list[Blocker] = Field(default_factory=list)
    acceptance_checks: list[AcceptanceCheck] = Field(default_factory=list)
    evidence: list[EvidenceProof] = Field(default_factory=list)
    attachments: list[AttachmentRef] = Field(default_factory=list)
    agent_runs: list[AgentRun] = Field(default_factory=list)
    agreement_log: list[AgreementLogEntry] = Field(default_factory=list)


def normalize_plan_state(plan_state: dict[str, Any] | PlanState, graph: Any = None) -> dict[str, Any]:
    model = plan_state if isinstance(plan_state, PlanState) else PlanState.model_validate(plan_state or {})
    normalized = model.model_dump(mode="json")
    _ensure_plan_state_defaults(normalized, graph=graph)
    return normalized


def validate_plan_state(plan_state: dict[str, Any] | PlanState, graph: Any = None) -> dict[str, Any]:
    try:
        return normalize_plan_state(plan_state, graph=graph)
    except ValidationError as error:
        raise PlanStateValidationError(str(error)) from error
    except ValueError as error:
        raise PlanStateValidationError(str(error)) from error


def _ensure_plan_state_defaults(plan_state: dict[str, Any], *, graph: Any = None) -> None:
    from .types import build_index, normalize_graph

    plan_state["revision"] = int(plan_state.get("revision") or 0)
    plan_state.setdefault("updated_at", "")
    plan_state.setdefault("updated_by", "")
    for key in (
        "decisions",
        "tasks",
        "blockers",
        "acceptance_checks",
        "evidence",
        "attachments",
        "agent_runs",
        "agreement_log",
    ):
        plan_state.setdefault(key, [])

    graph_index = build_index(normalize_graph(graph)) if graph is not None else None
    allowed_refs = set()
    if graph_index is not None:
        allowed_refs.update(graph_index["nodes"].keys())
        allowed_refs.update(graph_index["edges"].keys())
        allowed_refs.update(graph_index["field_by_id"].keys())
        allowed_refs.update(graph_index["field_ref_to_id"].keys())

    def normalize_linked_refs(refs: list[str]) -> list[str]:
        normalized_refs: list[str] = []
        for raw_ref in refs or []:
            if not isinstance(raw_ref, str) or not raw_ref.strip():
                continue
            ref = raw_ref.strip()
            if graph_index is not None and ref in graph_index["field_ref_to_id"]:
                ref = graph_index["field_ref_to_id"][ref]
            if graph_index is not None and ref not in allowed_refs:
                raise ValueError(f"Unknown linked_ref: {ref}")
            if ref not in normalized_refs:
                normalized_refs.append(ref)
        return normalized_refs

    def ensure_unique(items: list[dict[str, Any]], label: str) -> set[str]:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for item in items:
            item_id = str(item.get("id", "")).strip()
            if not item_id:
                raise ValueError(f"{label} entries must include an id.")
            if item_id in seen:
                duplicates.add(item_id)
            seen.add(item_id)
        if duplicates:
            raise ValueError(f"Duplicate {label} ids: {sorted(duplicates)}")
        return seen

    decisions = plan_state["decisions"]
    tasks = plan_state["tasks"]
    blockers = plan_state["blockers"]
    checks = plan_state["acceptance_checks"]
    evidence = plan_state["evidence"]
    attachments = plan_state["attachments"]
    agent_runs = plan_state["agent_runs"]
    agreement_log = plan_state["agreement_log"]

    decision_ids = ensure_unique(decisions, "decision")
    task_ids = ensure_unique(tasks, "task")
    blocker_ids = ensure_unique(blockers, "blocker")
    check_ids = ensure_unique(checks, "acceptance check")
    evidence_ids = ensure_unique(evidence, "evidence")
    attachment_ids = ensure_unique(attachments, "attachment")
    ensure_unique(agent_runs, "agent run")
    ensure_unique(agreement_log, "agreement log")

    tasks_by_decision: dict[str, list[dict[str, Any]]] = {decision_id: [] for decision_id in decision_ids}

    for decision in decisions:
        decision["linked_refs"] = normalize_linked_refs(decision.get("linked_refs", []))
        decision["acceptance_check_ids"] = [
            check_id for check_id in decision.get("acceptance_check_ids", []) if check_id in check_ids
        ]
        decision["evidence_ids"] = [proof_id for proof_id in decision.get("evidence_ids", []) if proof_id in evidence_ids]
        if decision.get("supersedes_decision_id") and decision["supersedes_decision_id"] not in decision_ids:
            raise ValueError(f"Decision {decision['id']} supersedes unknown decision {decision['supersedes_decision_id']}.")

    for task in tasks:
        task["linked_refs"] = normalize_linked_refs(task.get("linked_refs", []))
        task["decision_ids"] = [decision_id for decision_id in task.get("decision_ids", []) if decision_id in decision_ids]
        task["depends_on"] = [task_id for task_id in task.get("depends_on", []) if task_id in task_ids]
        task["acceptance_check_ids"] = [check_id for check_id in task.get("acceptance_check_ids", []) if check_id in check_ids]
        task["evidence_ids"] = [proof_id for proof_id in task.get("evidence_ids", []) if proof_id in evidence_ids]
        task["blocker_ids"] = [blocker_id for blocker_id in task.get("blocker_ids", []) if blocker_id in blocker_ids]
        if not task.get("title"):
            raise ValueError(f"Task {task['id']} must include a title.")
        if not task.get("exploratory") and not task["decision_ids"]:
            raise ValueError(f"Task {task['id']} must link to at least one decision or be marked exploratory.")
        if task.get("exploratory") and not task.get("exploration_goal"):
            raise ValueError(f"Exploratory task {task['id']} must include an exploration_goal.")
        if task.get("status") == "done" and task.get("exploratory") and not task["decision_ids"]:
            raise ValueError(
                f"Exploratory task {task['id']} cannot be marked done until it links to a decision or is cancelled."
            )
        if task.get("status") == "blocked" and not task["blocker_ids"]:
            raise ValueError(f"Blocked task {task['id']} must reference at least one structured blocker.")
        for decision_id in task["decision_ids"]:
            tasks_by_decision.setdefault(decision_id, []).append(task)

    active_task_statuses = {"todo", "in_progress", "blocked", "done"}
    for decision in decisions:
        linked_tasks = [
            task
            for task in tasks_by_decision.get(decision["id"], [])
            if not task.get("exploratory") and task.get("status") in active_task_statuses
        ]
        if decision.get("status") in {"accepted", "in_progress", "validated"} and not linked_tasks:
            raise ValueError(
                f"Decision {decision['id']} with status {decision['status']} must have at least one linked active or completed task."
            )

    for blocker in blockers:
        blocker["linked_refs"] = normalize_linked_refs(blocker.get("linked_refs", []))
        blocker["task_ids"] = [task_id for task_id in blocker.get("task_ids", []) if task_id in task_ids]
        blocker["decision_ids"] = [decision_id for decision_id in blocker.get("decision_ids", []) if decision_id in decision_ids]
        if not blocker["task_ids"] and not blocker["decision_ids"]:
            raise ValueError(f"Blocker {blocker['id']} must link to at least one task or decision.")

    for check in checks:
        check["linked_refs"] = normalize_linked_refs(check.get("linked_refs", []))

    evidence_by_id = {item["id"]: item for item in evidence}
    for proof in evidence:
        if proof.get("check_id") not in check_ids:
            raise ValueError(f"Evidence {proof['id']} references unknown acceptance check {proof.get('check_id')}.")
        proof["attachment_ids"] = [attachment_id for attachment_id in proof.get("attachment_ids", []) if attachment_id in attachment_ids]
        if not proof.get("summary", "").strip():
            raise ValueError(f"Evidence {proof['id']} must include a concise summary.")

    checks_by_entity: dict[str, set[str]] = {}
    evidence_by_entity: dict[str, set[str]] = {}
    for decision in decisions:
        checks_by_entity[decision["id"]] = set(decision.get("acceptance_check_ids", []))
        evidence_by_entity[decision["id"]] = set(decision.get("evidence_ids", []))
    for task in tasks:
        checks_by_entity[task["id"]] = set(task.get("acceptance_check_ids", []))
        evidence_by_entity[task["id"]] = set(task.get("evidence_ids", []))
    for entity_id, entity_evidence_ids in evidence_by_entity.items():
        for proof_id in entity_evidence_ids:
            proof = evidence_by_id.get(proof_id)
            if proof is None:
                raise ValueError(f"Entity {entity_id} references unknown evidence {proof_id}.")
            if proof["check_id"] not in checks_by_entity.get(entity_id, set()):
                raise ValueError(
                    f"Evidence {proof_id} must satisfy one of the acceptance checks linked to {entity_id}."
                )

    for attachment in attachments:
        attachment["linked_decision_ids"] = [
            decision_id for decision_id in attachment.get("linked_decision_ids", []) if decision_id in decision_ids
        ]
        attachment["linked_task_ids"] = [task_id for task_id in attachment.get("linked_task_ids", []) if task_id in task_ids]

    for run in agent_runs:
        run["task_ids"] = [task_id for task_id in run.get("task_ids", []) if task_id in task_ids]
        run["blocked_by"] = [blocker_id for blocker_id in run.get("blocked_by", []) if blocker_id in blocker_ids]
        if run.get("status") in {"waiting", "blocked", "failed"}:
            if not str(run.get("status_reason", "")).strip():
                raise ValueError(f"Agent run {run['id']} requires status_reason when status is {run['status']}.")
            if not str(run.get("next_action_hint", "")).strip():
                raise ValueError(f"Agent run {run['id']} requires next_action_hint when status is {run['status']}.")
        ensure_unique(run.get("events", []), f"agent run {run['id']} event")

    for entry in agreement_log:
        if entry.get("decision_id") and entry["decision_id"] not in decision_ids:
            raise ValueError(f"Agreement log {entry['id']} references unknown decision {entry['decision_id']}.")
        if entry.get("task_id") and entry["task_id"] not in task_ids:
            raise ValueError(f"Agreement log {entry['id']} references unknown task {entry['task_id']}.")

    plan_state["decisions"] = sorted(decisions, key=lambda item: item["id"])
    plan_state["tasks"] = sorted(tasks, key=lambda item: item["id"])
    plan_state["blockers"] = sorted(blockers, key=lambda item: item["id"])
    plan_state["acceptance_checks"] = sorted(checks, key=lambda item: item["id"])
    plan_state["evidence"] = sorted(evidence, key=lambda item: item["id"])
    plan_state["attachments"] = sorted(attachments, key=lambda item: item["id"])
    plan_state["agent_runs"] = sorted(agent_runs, key=lambda item: item["id"])
    plan_state["agreement_log"] = sorted(agreement_log, key=lambda item: item["id"])


__all__ = [
    "AcceptanceCheck",
    "AgentRun",
    "AgentRunEvent",
    "AgreementLogEntry",
    "AttachmentRef",
    "Blocker",
    "DecisionUnit",
    "EvidenceProof",
    "ExecutionTask",
    "PlanState",
    "normalize_plan_state",
    "validate_plan_state",
]
