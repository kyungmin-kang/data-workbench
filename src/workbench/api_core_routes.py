from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse

from .agent_briefs import build_agent_assignment_brief
from .agent_contracts import get_agent_contract, list_agent_contracts
from .agent_workflows import build_agent_workflow
from .api_helpers import build_execution_payload, graph_payload
from .store import ROOT_DIR, export_canonical_yaml_text, load_graph


STATIC_DIR = ROOT_DIR / "static"

router = APIRouter()


@router.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/api/graph")
def get_graph() -> dict[str, Any]:
    graph = load_graph()
    return graph_payload(graph)


@router.get("/api/source-of-truth")
def get_source_of_truth() -> dict[str, Any]:
    graph = load_graph()
    return build_execution_payload(graph)


@router.get("/api/agent-contracts")
def get_agent_contracts() -> dict[str, Any]:
    return {"agent_contracts": list_agent_contracts()}


@router.get("/api/agent-contracts/{contract_id}/brief")
def get_agent_contract_brief(contract_id: str, task_id: str = "", run_id: str = "") -> dict[str, Any]:
    contract = get_agent_contract(contract_id)
    if contract is None:
        raise HTTPException(status_code=404, detail=f"Agent contract not found: {contract_id}")
    try:
        graph = load_graph()
        payload = build_execution_payload(graph)
        brief = build_agent_assignment_brief(
            contract=contract,
            graph=graph,
            plan_state=payload["plan_state"],
            source_of_truth=payload["source_of_truth"],
            task_id=task_id,
            run_id=run_id,
        )
        return {"brief": brief, "source_of_truth": payload["source_of_truth"]}
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/api/agent-contracts/{contract_id}/workflow")
def get_agent_contract_workflow(
    contract_id: str,
    task_id: str = "",
    run_id: str = "",
    bundle_id: str = "",
    root_path: str = "",
    doc_paths: list[str] = Query(default_factory=list),
    selected_paths: list[str] = Query(default_factory=list),
) -> dict[str, Any]:
    contract = get_agent_contract(contract_id)
    if contract is None:
        raise HTTPException(status_code=404, detail=f"Agent contract not found: {contract_id}")
    try:
        graph = load_graph()
        payload = build_execution_payload(graph)
        workflow = build_agent_workflow(
            contract=contract,
            graph=graph,
            plan_state=payload["plan_state"],
            source_of_truth=payload["source_of_truth"],
            task_id=task_id,
            run_id=run_id,
            bundle_id=bundle_id,
            root_path=root_path,
            doc_paths=doc_paths,
            selected_paths=selected_paths,
        )
        return {"workflow": workflow, "source_of_truth": payload["source_of_truth"]}
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/api/plan-state")
def get_plan_state_endpoint() -> dict[str, Any]:
    graph = load_graph()
    payload = build_execution_payload(graph)
    return {"plan_state": payload["plan_state"], "source_of_truth": payload["source_of_truth"]}


@router.get("/api/structure/export", response_class=PlainTextResponse)
def export_structure_yaml() -> str:
    return export_canonical_yaml_text()


@router.get("/api/plans/latest")
def get_latest_plan() -> dict[str, Any]:
    from .store import describe_artifact_storage, load_latest_plan, load_latest_plan_artifacts

    return {
        "plan": load_latest_plan(),
        "artifacts": load_latest_plan_artifacts(),
        "artifact_storage": describe_artifact_storage(),
    }
