from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from .api_helpers import analyze_graph, build_execution_payload, graph_payload, reject_agent_canonical_write, resolve_profile_root
from .api_models import (
    AssetImportRequest,
    BulkAssetImportRequest,
    ContractBindingSuggestionRequest,
    GraphSaveRequest,
    GraphValidateRequest,
    OpenAPIImportRequest,
    ProjectBootstrapRequest,
    ProjectHintImportRequest,
)
from .binding_suggestions import collect_contract_field_candidates, suggest_contract_field_source
from .diff import generate_change_plan
from .hint_importer import import_api_hint_into_graph, import_orm_hint_into_graph, import_sql_hint_into_graph, import_ui_hint_into_graph
from .importer import import_asset_into_graph, import_assets_into_graph
from .openapi_importer import import_openapi_into_graph
from .profile import profile_graph
from .project_bootstrap import bootstrap_project_into_graph
from .project_profiler import resolve_project_profile
from .store import describe_artifact_storage, get_root_dir, load_graph, save_graph, write_plan_artifacts
from .types import GraphValidationError, validate_graph


router = APIRouter()


@router.post("/api/import/asset")
def import_asset_endpoint(payload: AssetImportRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "import assets into canonical structure")
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        imported = import_asset_into_graph(graph, payload.import_spec, root_dir, profile_assets=payload.profile_assets)
        updated_graph = imported["graph"]
        analysis = analyze_graph(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": analysis["diagnostics"],
            "validation": analysis["validation"],
            "structure": analysis["structure"],
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/import/assets/bulk")
def import_assets_bulk_endpoint(payload: BulkAssetImportRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "bulk import assets into canonical structure")
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        imported = import_assets_into_graph(graph, payload.import_specs, root_dir, profile_assets=payload.profile_assets)
        updated_graph = imported["graph"]
        analysis = analyze_graph(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": analysis["diagnostics"],
            "validation": analysis["validation"],
            "structure": analysis["structure"],
            "imported": imported["imported"],
            "skipped": imported["skipped"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/import/openapi")
def import_openapi_endpoint(payload: OpenAPIImportRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "import OpenAPI into canonical structure")
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        imported = import_openapi_into_graph(graph, payload.import_spec, root_dir)
        updated_graph = imported["graph"]
        analysis = analyze_graph(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": analysis["diagnostics"],
            "validation": analysis["validation"],
            "structure": analysis["structure"],
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/graph/validate")
def validate_graph_endpoint(payload: GraphValidateRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        analysis = analyze_graph(graph)
        return {
            "graph": graph,
            "diagnostics": analysis["diagnostics"],
            "validation": analysis["validation"],
            "structure": analysis["structure"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/contract/suggestions")
def contract_binding_suggestions_endpoint(payload: ContractBindingSuggestionRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        suggestions = collect_contract_field_candidates(graph, payload.node_id, payload.field_name, limit=payload.limit)
        auto_suggestion = suggest_contract_field_source(graph, payload.node_id, payload.field_name)
        return {
            "suggestions": suggestions,
            "auto_suggestion": auto_suggestion,
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/import/project-hint")
def import_project_hint_endpoint(payload: ProjectHintImportRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "import project hints into canonical structure")
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        project_profile = resolve_project_profile(
            root_dir,
            include_tests=payload.include_tests,
            include_internal=payload.include_internal,
            profile_token=payload.profile_token,
            profiling_mode=payload.profiling_mode,
            exclude_paths=payload.exclude_paths,
            asset_roots=payload.asset_roots,
        )
        if payload.hint_kind == "api":
            hint = next(
                (item for item in project_profile.get("api_contract_hints", []) if item["id"] == payload.hint_id),
                None,
            )
            if hint is None:
                raise ValueError(f"API hint not found: {payload.hint_id}")
            imported = import_api_hint_into_graph(graph, hint)
        elif payload.hint_kind == "ui":
            hint = next(
                (item for item in project_profile.get("ui_contract_hints", []) if item["id"] == payload.hint_id),
                None,
            )
            if hint is None:
                raise ValueError(f"UI hint not found: {payload.hint_id}")
            api_hints_by_route = {
                item["route"]: item
                for item in project_profile.get("api_contract_hints", [])
                if item.get("route")
            }
            imported = import_ui_hint_into_graph(graph, hint, api_hints_by_route=api_hints_by_route)
        elif payload.hint_kind == "sql":
            hint = next(
                (item for item in project_profile.get("sql_structure_hints", []) if item["id"] == payload.hint_id),
                None,
            )
            if hint is None:
                raise ValueError(f"SQL hint not found: {payload.hint_id}")
            imported = import_sql_hint_into_graph(graph, hint)
        elif payload.hint_kind == "orm":
            hint = next(
                (item for item in project_profile.get("orm_structure_hints", []) if item["id"] == payload.hint_id),
                None,
            )
            if hint is None:
                raise ValueError(f"ORM hint not found: {payload.hint_id}")
            imported = import_orm_hint_into_graph(graph, hint)
        else:
            raise ValueError(f"Unsupported hint kind: {payload.hint_kind}")

        updated_graph = imported["graph"]
        analysis = analyze_graph(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": analysis["diagnostics"],
            "validation": analysis["validation"],
            "structure": analysis["structure"],
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/import/project-bootstrap")
def import_project_bootstrap_endpoint(payload: ProjectBootstrapRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "bootstrap canonical structure from project artifacts")
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        project_profile = resolve_project_profile(
            root_dir,
            include_tests=payload.include_tests,
            include_internal=payload.include_internal,
            profile_token=payload.profile_token,
            profiling_mode=payload.profiling_mode,
            exclude_paths=payload.exclude_paths,
            asset_roots=payload.asset_roots,
        )
        imported = bootstrap_project_into_graph(
            graph,
            root_dir,
            project_profile=project_profile,
            include_tests=payload.include_tests,
            include_internal=payload.include_internal,
            asset_paths=payload.asset_paths,
            api_hint_ids=payload.api_hint_ids,
            ui_hint_ids=payload.ui_hint_ids,
            sql_hint_ids=payload.sql_hint_ids,
            orm_hint_ids=payload.orm_hint_ids,
            import_assets=payload.import_assets,
            import_api_hints=payload.import_api_hints,
            import_ui_hints=payload.import_ui_hints,
            import_sql_hints=payload.import_sql_hints,
            import_orm_hints=payload.import_orm_hints,
        )
        updated_graph = imported["graph"]
        analysis = analyze_graph(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": analysis["diagnostics"],
            "validation": analysis["validation"],
            "structure": analysis["structure"],
            "project_profile": imported["project_profile"],
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/graph/save")
def save_graph_endpoint(payload: GraphSaveRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "save canonical structure")
        new_graph = validate_graph(payload.graph)
        analysis = analyze_graph(new_graph)
        if analysis["validation"]["errors"]:
            raise ValueError(analysis["validation"]["errors"][0]["message"])
        old_graph = load_graph()
        new_graph = save_graph(new_graph)
        plan = generate_change_plan(old_graph, new_graph)
        artifacts = write_plan_artifacts(plan)
        execution_payload = build_execution_payload(new_graph)
        return {
            "graph": new_graph,
            "diagnostics": analysis["diagnostics"],
            "validation": analysis["validation"],
            "structure": analysis["structure"],
            "plan": plan,
            "artifacts": artifacts,
            "artifact_storage": describe_artifact_storage(),
            "plan_state": execution_payload["plan_state"],
            "source_of_truth": execution_payload["source_of_truth"],
            "agent_contracts": execution_payload["agent_contracts"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/profile/refresh")
def refresh_profiles() -> dict[str, Any]:
    try:
        graph = load_graph()
        refreshed = profile_graph(graph, get_root_dir())
        refreshed = validate_graph(refreshed)
        refreshed = save_graph(refreshed)
        return graph_payload(refreshed)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
