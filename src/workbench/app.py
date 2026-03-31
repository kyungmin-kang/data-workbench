from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import yaml

from .binding_suggestions import collect_contract_field_candidates, suggest_contract_field_source
from .diagnostics import build_graph_diagnostics
from .diff import generate_change_plan
from .hint_importer import import_api_hint_into_graph, import_ui_hint_into_graph
from .importer import AssetImportSpec, import_asset_into_graph, import_assets_into_graph
from .onboarding_presets import OnboardingPreset, delete_onboarding_preset, load_onboarding_presets, save_onboarding_preset
from .openapi_importer import OpenAPIImportSpec, import_openapi_into_graph
from .project_bootstrap import bootstrap_project_into_graph
from .profile import profile_graph
from .project_profiler import is_ignored_project_dir_name, resolve_project_profile
from .store import (
    ROOT_DIR,
    export_canonical_yaml_text,
    get_root_dir,
    list_bundles,
    load_bundle,
    load_graph,
    load_latest_plan,
    load_latest_plan_artifacts,
    save_graph,
    write_plan_artifacts,
)
from .structure_memory import (
    build_structure_summary,
    import_yaml_spec,
    merge_bundle,
    preview_rebase_bundle,
    rebase_bundle,
    review_bundle_contradiction,
    review_bundle_patch,
    review_bundle_patches,
    scan_structure,
    update_bundle_workflow,
)
from .types import GraphValidationError, validate_graph
from .validation import build_validation_report


STATIC_DIR = ROOT_DIR / "static"


def graph_payload(graph: dict) -> dict:
    validation = build_validation_report(graph)
    diagnostics = build_graph_diagnostics(graph, validation_report=validation)
    return {
        "graph": graph,
        "diagnostics": diagnostics,
        "validation": validation,
        "latest_plan": load_latest_plan(),
        "latest_artifacts": load_latest_plan_artifacts(),
        "structure": build_structure_summary(graph, diagnostics=diagnostics, validation_report=validation),
    }


class GraphSaveRequest(BaseModel):
    graph: dict[str, Any]


class AssetImportRequest(BaseModel):
    graph: dict[str, Any]
    import_spec: AssetImportSpec
    root_path: str | None = None


class OpenAPIImportRequest(BaseModel):
    graph: dict[str, Any]
    import_spec: OpenAPIImportSpec
    root_path: str | None = None


class BulkAssetImportRequest(BaseModel):
    graph: dict[str, Any]
    import_specs: list[AssetImportSpec]
    root_path: str | None = None


class GraphValidateRequest(BaseModel):
    graph: dict[str, Any]


class ContractBindingSuggestionRequest(BaseModel):
    graph: dict[str, Any]
    node_id: str
    field_name: str
    limit: int = 8


class ProjectHintImportRequest(BaseModel):
    graph: dict[str, Any]
    hint_kind: str
    hint_id: str
    profile_token: str | None = None
    root_path: str | None = None
    include_tests: bool = False
    include_internal: bool = True


class ProjectBootstrapRequest(BaseModel):
    graph: dict[str, Any]
    include_tests: bool = False
    include_internal: bool = True
    profile_token: str | None = None
    root_path: str | None = None
    asset_paths: list[str] = []
    api_hint_ids: list[str] = []
    ui_hint_ids: list[str] = []
    import_assets: bool = True
    import_api_hints: bool = True
    import_ui_hints: bool = True


class OnboardingPresetSaveRequest(BaseModel):
    preset: OnboardingPreset


class StructureImportRequest(BaseModel):
    spec: dict[str, Any] | None = None
    yaml_text: str | None = None
    updated_by: str = "user"


class StructureScanRequest(BaseModel):
    role: str = "scout"
    scope: str = "full"
    include_tests: bool = False
    include_internal: bool = True
    profile_token: str | None = None
    force_refresh: bool = False
    root_path: str | None = None
    doc_paths: list[str] = []
    selected_paths: list[str] = []


class StructurePatchReviewRequest(BaseModel):
    patch_id: str
    decision: str
    reviewed_by: str = "user"
    note: str = ""


class StructureBatchReviewRequest(BaseModel):
    patch_ids: list[str] = []
    decision: str
    reviewed_by: str = "user"
    note: str = ""


class StructureContradictionReviewRequest(BaseModel):
    contradiction_id: str
    decision: str
    reviewed_by: str = "user"
    note: str = ""


class StructureBundleWorkflowRequest(BaseModel):
    bundle_owner: str | None = None
    assigned_reviewer: str | None = None
    triage_state: str | None = None
    triage_note: str | None = None
    updated_by: str = "user"
    note: str = ""


class StructureBundleMergeRequest(BaseModel):
    merged_by: str = "user"


class StructureBundleRebaseRequest(BaseModel):
    preserve_reviews: bool = True


app = FastAPI(title="Data Workbench", version="0.2.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/graph")
def get_graph() -> dict[str, Any]:
    graph = load_graph()
    return graph_payload(graph)


@app.get("/api/structure/export", response_class=PlainTextResponse)
def export_structure_yaml() -> str:
    return export_canonical_yaml_text()


@app.post("/api/structure/import")
def import_structure_endpoint(payload: StructureImportRequest) -> dict[str, Any]:
    try:
        if payload.spec is not None:
            spec_payload = payload.spec
        elif payload.yaml_text:
            spec_payload = yaml.safe_load(payload.yaml_text) or {}
        else:
            raise ValueError("Provide either spec or yaml_text for import.")
        return import_yaml_spec(spec_payload, updated_by=payload.updated_by)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.get("/api/structure/bundles")
def get_structure_bundles() -> dict[str, Any]:
    return {"bundles": list_bundles()}


@app.get("/api/structure/bundles/{bundle_id}")
def get_structure_bundle(bundle_id: str) -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail=f"Bundle not found: {bundle_id}")
    return {"bundle": bundle}


@app.post("/api/structure/scan")
def structure_scan_endpoint(payload: StructureScanRequest) -> dict[str, Any]:
    try:
        root_dir = resolve_profile_root(payload.root_path)
        return scan_structure(
            root_dir=root_dir,
            role=payload.role,
            scope=payload.scope,
            include_tests=payload.include_tests,
            include_internal=payload.include_internal,
            profile_token=payload.profile_token,
            force_refresh=payload.force_refresh,
            doc_paths=payload.doc_paths,
            selected_paths=payload.selected_paths,
        )
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/structure/bundles/{bundle_id}/review")
def review_structure_bundle_endpoint(bundle_id: str, payload: StructurePatchReviewRequest) -> dict[str, Any]:
    try:
        return review_bundle_patch(
            bundle_id,
            payload.patch_id,
            payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/structure/bundles/{bundle_id}/review-batch")
def review_structure_bundle_batch_endpoint(bundle_id: str, payload: StructureBatchReviewRequest) -> dict[str, Any]:
    try:
        return review_bundle_patches(
            bundle_id,
            payload.patch_ids,
            payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/structure/bundles/{bundle_id}/review-contradiction")
def review_structure_bundle_contradiction_endpoint(
    bundle_id: str,
    payload: StructureContradictionReviewRequest,
) -> dict[str, Any]:
    try:
        return review_bundle_contradiction(
            bundle_id,
            payload.contradiction_id,
            payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/structure/bundles/{bundle_id}/workflow")
def update_structure_bundle_workflow_endpoint(
    bundle_id: str,
    payload: StructureBundleWorkflowRequest,
) -> dict[str, Any]:
    try:
        return update_bundle_workflow(
            bundle_id,
            bundle_owner=payload.bundle_owner,
            assigned_reviewer=payload.assigned_reviewer,
            triage_state=payload.triage_state,
            triage_note=payload.triage_note,
            updated_by=payload.updated_by,
            note=payload.note,
        )
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.get("/api/structure/bundles/{bundle_id}/rebase-preview")
def preview_structure_bundle_rebase_endpoint(bundle_id: str, preserve_reviews: bool = True) -> dict[str, Any]:
    try:
        return preview_rebase_bundle(bundle_id, preserve_reviews=preserve_reviews)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/structure/bundles/{bundle_id}/merge")
def merge_structure_bundle_endpoint(bundle_id: str, payload: StructureBundleMergeRequest) -> dict[str, Any]:
    try:
        return merge_bundle(bundle_id, merged_by=payload.merged_by)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/structure/bundles/{bundle_id}/rebase")
def rebase_structure_bundle_endpoint(bundle_id: str, payload: StructureBundleRebaseRequest) -> dict[str, Any]:
    try:
        return rebase_bundle(bundle_id, preserve_reviews=payload.preserve_reviews)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.get("/api/plans/latest")
def get_latest_plan() -> dict[str, Any]:
    return {
        "plan": load_latest_plan(),
        "artifacts": load_latest_plan_artifacts(),
    }


def resolve_profile_root(root_path: str | None) -> Path:
    if not root_path:
        return get_root_dir()
    candidate = Path(root_path).expanduser()
    if not candidate.is_absolute():
        candidate = (get_root_dir() / candidate).resolve()
    if not candidate.exists() or not candidate.is_dir():
        raise HTTPException(status_code=400, detail=f"Project root not found: {root_path}")
    return candidate


def search_project_directories(base_path: str | None, query: str, *, limit: int = 40) -> list[dict[str, str]]:
    root_dir = resolve_profile_root(base_path)
    normalized_query = query.strip().lower()
    matches: list[dict[str, str]] = []
    seen: set[str] = set()

    def maybe_add(path: Path) -> None:
        text = str(path)
        if text in seen:
            return
        seen.add(text)
        matches.append({"path": text, "name": path.name or text})

    maybe_add(root_dir)
    if not normalized_query:
        for child in sorted(root_dir.iterdir()):
            if child.is_dir() and not is_ignored_project_dir_name(child.name):
                maybe_add(child)
                if len(matches) >= limit:
                    break
        return matches[:limit]

    stack = [root_dir]
    while stack and len(matches) < limit:
        current = stack.pop()
        try:
            child_dirs = sorted(
                child
                for child in current.iterdir()
                if child.is_dir() and not is_ignored_project_dir_name(child.name)
            )
        except OSError:
            continue
        for child in reversed(child_dirs):
            stack.append(child)
        for path in child_dirs:
            relative = str(path.relative_to(root_dir)).lower()
            if normalized_query not in path.name.lower() and normalized_query not in relative and normalized_query not in str(path).lower():
                continue
            maybe_add(path)
            if len(matches) >= limit:
                break
    return matches[:limit]


@app.get("/api/project/profile")
def get_project_profile(
    include_tests: bool = False,
    include_internal: bool = True,
    root_path: str | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    root_dir = resolve_profile_root(root_path)
    return {
        "project_profile": resolve_project_profile(
            root_dir,
            include_tests=include_tests,
            include_internal=include_internal,
            force_refresh=force_refresh,
        )
    }


@app.get("/api/project/directories")
def get_project_directories(
    query: str = "",
    base_path: str | None = None,
) -> dict[str, Any]:
    return {"directories": search_project_directories(base_path, query)}


@app.get("/api/onboarding/presets")
def get_onboarding_presets_endpoint() -> dict[str, Any]:
    return {"presets": load_onboarding_presets()}


@app.post("/api/onboarding/presets")
def save_onboarding_preset_endpoint(payload: OnboardingPresetSaveRequest) -> dict[str, Any]:
    try:
        saved = save_onboarding_preset(payload.preset)
        return {
            "saved": saved,
            "presets": load_onboarding_presets(),
        }
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.delete("/api/onboarding/presets/{preset_id}")
def delete_onboarding_preset_endpoint(preset_id: str) -> dict[str, Any]:
    deleted = delete_onboarding_preset(preset_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Preset not found: {preset_id}")
    return {
        "deleted": preset_id,
        "presets": load_onboarding_presets(),
    }


@app.post("/api/import/asset")
def import_asset_endpoint(payload: AssetImportRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        imported = import_asset_into_graph(graph, payload.import_spec, root_dir)
        updated_graph = imported["graph"]
        validation = build_validation_report(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": build_graph_diagnostics(updated_graph, validation_report=validation),
            "validation": validation,
            "structure": build_structure_summary(updated_graph, validation_report=validation),
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/import/assets/bulk")
def import_assets_bulk_endpoint(payload: BulkAssetImportRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        imported = import_assets_into_graph(graph, payload.import_specs, root_dir)
        updated_graph = imported["graph"]
        validation = build_validation_report(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": build_graph_diagnostics(updated_graph, validation_report=validation),
            "validation": validation,
            "structure": build_structure_summary(updated_graph, validation_report=validation),
            "imported": imported["imported"],
            "skipped": imported["skipped"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/import/openapi")
def import_openapi_endpoint(payload: OpenAPIImportRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        imported = import_openapi_into_graph(graph, payload.import_spec, root_dir)
        updated_graph = imported["graph"]
        validation = build_validation_report(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": build_graph_diagnostics(updated_graph, validation_report=validation),
            "validation": validation,
            "structure": build_structure_summary(updated_graph, validation_report=validation),
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/graph/validate")
def validate_graph_endpoint(payload: GraphValidateRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        validation = build_validation_report(graph)
        return {
            "graph": graph,
            "diagnostics": build_graph_diagnostics(graph, validation_report=validation),
            "validation": validation,
            "structure": build_structure_summary(graph, validation_report=validation),
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/contract/suggestions")
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


@app.post("/api/import/project-hint")
def import_project_hint_endpoint(payload: ProjectHintImportRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        project_profile = resolve_project_profile(
            root_dir,
            include_tests=payload.include_tests,
            include_internal=payload.include_internal,
            profile_token=payload.profile_token,
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
        else:
            raise ValueError(f"Unsupported hint kind: {payload.hint_kind}")

        updated_graph = imported["graph"]
        validation = build_validation_report(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": build_graph_diagnostics(updated_graph, validation_report=validation),
            "validation": validation,
            "structure": build_structure_summary(updated_graph, validation_report=validation),
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/import/project-bootstrap")
def import_project_bootstrap_endpoint(payload: ProjectBootstrapRequest) -> dict[str, Any]:
    try:
        graph = validate_graph(payload.graph)
        root_dir = resolve_profile_root(payload.root_path)
        project_profile = resolve_project_profile(
            root_dir,
            include_tests=payload.include_tests,
            include_internal=payload.include_internal,
            profile_token=payload.profile_token,
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
            import_assets=payload.import_assets,
            import_api_hints=payload.import_api_hints,
            import_ui_hints=payload.import_ui_hints,
        )
        updated_graph = imported["graph"]
        validation = build_validation_report(updated_graph)
        return {
            "graph": updated_graph,
            "diagnostics": build_graph_diagnostics(updated_graph, validation_report=validation),
            "validation": validation,
            "structure": build_structure_summary(updated_graph, validation_report=validation),
            "project_profile": imported["project_profile"],
            "imported": imported["imported"],
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/graph/save")
def save_graph_endpoint(payload: GraphSaveRequest) -> dict[str, Any]:
    try:
        new_graph = validate_graph(payload.graph)
        validation = build_validation_report(new_graph)
        if validation["errors"]:
            raise ValueError(validation["errors"][0]["message"])
        old_graph = load_graph()
        new_graph = save_graph(new_graph)
        plan = generate_change_plan(old_graph, new_graph)
        artifacts = write_plan_artifacts(plan)
        return {
            "graph": new_graph,
            "diagnostics": build_graph_diagnostics(new_graph, validation_report=validation),
            "validation": validation,
            "structure": build_structure_summary(new_graph, validation_report=validation),
            "plan": plan,
            "artifacts": artifacts,
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/api/profile/refresh")
def refresh_profiles() -> dict[str, Any]:
    try:
        graph = load_graph()
        refreshed = profile_graph(graph, get_root_dir())
        refreshed = validate_graph(refreshed)
        refreshed = save_graph(refreshed)
        return graph_payload(refreshed)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


def main() -> None:
    import os
    import uvicorn

    host = os.environ.get("WORKBENCH_HOST", "0.0.0.0")
    port = int(os.environ.get("WORKBENCH_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
