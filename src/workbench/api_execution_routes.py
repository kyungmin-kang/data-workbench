from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from .agent_contracts import get_agent_contract
from .agent_workflows import build_agent_workflow, launch_agent_workflow_run
from .api_helpers import build_execution_payload
from .api_models import (
    AgentRunCreateRequest,
    AgentRunEventRequest,
    AgentRunPatchRequest,
    AgentWorkflowLaunchRequest,
    PlanStateDeriveTasksRequest,
    PlanStateSaveRequest,
)
from .execution import derive_tasks_from_plan_state, utc_timestamp
from .store import load_graph, load_plan_state, save_plan_state
from .types import PlanStateValidationError, identifierify, validate_plan_state


router = APIRouter()


@router.put("/api/plan-state")
def save_plan_state_endpoint(payload: PlanStateSaveRequest) -> dict[str, Any]:
    try:
        graph = load_graph()
        validated = validate_plan_state(payload.plan_state, graph=graph)
        saved = save_plan_state(
            validated,
            graph=graph,
            updated_by=payload.updated_by,
            expected_revision=payload.expected_revision,
        )
        return build_execution_payload(graph, plan_state=saved)
    except (PlanStateValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/plan-state/derive-tasks")
def derive_plan_state_tasks_endpoint(payload: PlanStateDeriveTasksRequest) -> dict[str, Any]:
    try:
        graph = load_graph()
        current = load_plan_state(graph)
        derived = derive_tasks_from_plan_state(current, graph, updated_by=payload.updated_by)
        saved = save_plan_state(
            derived,
            graph=graph,
            updated_by=payload.updated_by,
            expected_revision=payload.expected_revision,
        )
        return build_execution_payload(graph, plan_state=saved)
    except (PlanStateValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/agent-runs")
def create_agent_run_endpoint(payload: AgentRunCreateRequest) -> dict[str, Any]:
    try:
        graph = load_graph()
        current = load_plan_state(graph)
        run = dict(payload.agent_run)
        if not run.get("id"):
            base = identifierify(run.get("objective") or run.get("role") or "agent_run") or "agent_run"
            run["id"] = f"run.{base}.{len(current.get('agent_runs', [])) + 1}"
        run.setdefault("started_at", utc_timestamp())
        run["updated_at"] = utc_timestamp()
        current["agent_runs"] = [*current.get("agent_runs", []), run]
        saved = save_plan_state(
            current,
            graph=graph,
            updated_by=payload.updated_by,
            expected_revision=payload.expected_revision,
        )
        execution_payload = build_execution_payload(graph, plan_state=saved)
        created = next(item for item in saved["agent_runs"] if item["id"] == run["id"])
        return {"agent_run": created, **execution_payload}
    except (PlanStateValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/agent-contracts/{contract_id}/launch")
def launch_agent_contract_endpoint(contract_id: str, payload: AgentWorkflowLaunchRequest) -> dict[str, Any]:
    contract = get_agent_contract(contract_id)
    if contract is None:
        raise HTTPException(status_code=404, detail=f"Agent contract not found: {contract_id}")
    try:
        graph = load_graph()
        current = load_plan_state(graph)
        execution_payload = build_execution_payload(graph, plan_state=current)
        workflow = build_agent_workflow(
            contract=contract,
            graph=graph,
            plan_state=current,
            source_of_truth=execution_payload["source_of_truth"],
            task_id=payload.task_id,
            run_id=payload.run_id,
        )
        agent_run, launch_mode = launch_agent_workflow_run(
            workflow=workflow,
            plan_state=current,
            updated_by=payload.updated_by,
        )
        if launch_mode == "created":
            current["agent_runs"] = [*current.get("agent_runs", []), agent_run]
            saved = save_plan_state(
                current,
                graph=graph,
                updated_by=payload.updated_by,
                expected_revision=payload.expected_revision,
            )
        else:
            saved = current
        refreshed = build_execution_payload(graph, plan_state=saved)
        refreshed_workflow = build_agent_workflow(
            contract=contract,
            graph=graph,
            plan_state=refreshed["plan_state"],
            source_of_truth=refreshed["source_of_truth"],
            task_id=payload.task_id,
            run_id=agent_run.get("id", "") if launch_mode == "created" else payload.run_id,
        )
        return {
            "launch_mode": launch_mode,
            "agent_run": agent_run,
            "workflow": refreshed_workflow,
            **refreshed,
        }
    except (PlanStateValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.patch("/api/agent-runs/{run_id}")
def patch_agent_run_endpoint(run_id: str, payload: AgentRunPatchRequest) -> dict[str, Any]:
    try:
        graph = load_graph()
        current = load_plan_state(graph)
        updated = False
        agent_runs: list[dict[str, Any]] = []
        for run in current.get("agent_runs", []):
            if run.get("id") != run_id:
                agent_runs.append(run)
                continue
            merged = {**run, **payload.updates}
            merged["updated_at"] = utc_timestamp()
            agent_runs.append(merged)
            updated = True
        if not updated:
            raise ValueError(f"Agent run not found: {run_id}")
        current["agent_runs"] = agent_runs
        saved = save_plan_state(
            current,
            graph=graph,
            updated_by=payload.updated_by,
            expected_revision=payload.expected_revision,
        )
        execution_payload = build_execution_payload(graph, plan_state=saved)
        agent_run = next(item for item in saved["agent_runs"] if item["id"] == run_id)
        return {"agent_run": agent_run, **execution_payload}
    except (PlanStateValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/agent-runs/{run_id}/events")
def append_agent_run_event_endpoint(run_id: str, payload: AgentRunEventRequest) -> dict[str, Any]:
    try:
        graph = load_graph()
        current = load_plan_state(graph)
        event = dict(payload.event)
        if not event.get("id"):
            base = identifierify(event.get("kind") or "event") or "event"
            event["id"] = f"{run_id}.{base}.{utc_timestamp().replace(':', '').replace('-', '')}"
        event.setdefault("created_at", utc_timestamp())
        event.setdefault("created_by", payload.updated_by)
        updated = False
        agent_runs: list[dict[str, Any]] = []
        for run in current.get("agent_runs", []):
            if run.get("id") != run_id:
                agent_runs.append(run)
                continue
            merged = dict(run)
            merged["events"] = [*(run.get("events", []) or []), event]
            merged["updated_at"] = utc_timestamp()
            if event.get("summary"):
                merged["last_summary"] = event["summary"]
            agent_runs.append(merged)
            updated = True
        if not updated:
            raise ValueError(f"Agent run not found: {run_id}")
        current["agent_runs"] = agent_runs
        saved = save_plan_state(
            current,
            graph=graph,
            updated_by=payload.updated_by,
            expected_revision=payload.expected_revision,
        )
        execution_payload = build_execution_payload(graph, plan_state=saved)
        agent_run = next(item for item in saved["agent_runs"] if item["id"] == run_id)
        return {"agent_run": agent_run, **execution_payload}
    except (PlanStateValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
