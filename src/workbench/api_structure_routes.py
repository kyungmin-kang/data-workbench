from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
import yaml

from .api_helpers import (
    build_execution_payload,
    reject_agent_canonical_write,
    resolve_profile_root,
    sync_contradiction_review_with_execution,
    sync_merge_with_execution,
    sync_patch_reviews_with_execution,
    sync_rebase_with_execution,
)
from .api_models import (
    StructureBatchReviewRequest,
    StructureBundleMergeRequest,
    StructureBundleRebaseRequest,
    StructureBundleWorkflowRequest,
    StructureContradictionReviewRequest,
    StructureImportRequest,
    StructurePatchReviewRequest,
    StructureScanRequest,
)
from .store import load_graph
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
from .types import GraphValidationError


router = APIRouter()


@router.post("/api/structure/import")
def import_structure_endpoint(payload: StructureImportRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "import canonical structure")
        if payload.spec is not None:
            spec_payload = payload.spec
        elif payload.yaml_text:
            spec_payload = yaml.safe_load(payload.yaml_text) or {}
        else:
            raise ValueError("Provide either spec or yaml_text for import.")
        return import_yaml_spec(spec_payload, updated_by=payload.updated_by)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/api/structure/bundles")
def get_structure_bundles() -> dict[str, Any]:
    from .store import list_bundles

    return {"bundles": list_bundles()}


@router.get("/api/structure/bundles/{bundle_id}")
def get_structure_bundle(bundle_id: str) -> dict[str, Any]:
    from .store import load_bundle

    bundle = load_bundle(bundle_id)
    if bundle is None:
        raise HTTPException(status_code=404, detail=f"Bundle not found: {bundle_id}")
    return {"bundle": bundle}


@router.post("/api/structure/scan")
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


@router.post("/api/structure/bundles/{bundle_id}/review")
def review_structure_bundle_endpoint(bundle_id: str, payload: StructurePatchReviewRequest) -> dict[str, Any]:
    try:
        result = review_bundle_patch(
            bundle_id,
            payload.patch_id,
            payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        bundle = result["bundle"]
        graph = load_graph()
        plan_state = sync_patch_reviews_with_execution(
            bundle=bundle,
            patch_ids=result.get("updated_patch_ids", []) or [payload.patch_id],
            decision=payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        return {
            **result,
            **build_execution_payload(graph, plan_state=plan_state),
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/structure/bundles/{bundle_id}/review-batch")
def review_structure_bundle_batch_endpoint(bundle_id: str, payload: StructureBatchReviewRequest) -> dict[str, Any]:
    try:
        result = review_bundle_patches(
            bundle_id,
            payload.patch_ids,
            payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        bundle = result["bundle"]
        graph = load_graph()
        plan_state = sync_patch_reviews_with_execution(
            bundle=bundle,
            patch_ids=result.get("updated_patch_ids", []) or payload.patch_ids,
            decision=payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        return {
            **result,
            **build_execution_payload(graph, plan_state=plan_state),
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/structure/bundles/{bundle_id}/review-contradiction")
def review_structure_bundle_contradiction_endpoint(
    bundle_id: str,
    payload: StructureContradictionReviewRequest,
) -> dict[str, Any]:
    try:
        result = review_bundle_contradiction(
            bundle_id,
            payload.contradiction_id,
            payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        bundle = result["bundle"]
        graph = load_graph()
        plan_state = sync_contradiction_review_with_execution(
            bundle=bundle,
            contradiction_id=payload.contradiction_id,
            updated_patch_ids=result.get("updated_patch_ids", []),
            decision=payload.decision,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        return {
            **result,
            **build_execution_payload(graph, plan_state=plan_state),
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/structure/bundles/{bundle_id}/workflow")
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


@router.get("/api/structure/bundles/{bundle_id}/rebase-preview")
def preview_structure_bundle_rebase_endpoint(bundle_id: str, preserve_reviews: bool = True) -> dict[str, Any]:
    try:
        return preview_rebase_bundle(bundle_id, preserve_reviews=preserve_reviews)
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/structure/bundles/{bundle_id}/merge")
def merge_structure_bundle_endpoint(bundle_id: str, payload: StructureBundleMergeRequest) -> dict[str, Any]:
    try:
        reject_agent_canonical_write(payload.actor_type, "merge canonical structure bundles")
        result = merge_bundle(bundle_id, merged_by=payload.merged_by)
        graph = result["graph"]
        structure = result["structure"]
        plan_state = sync_merge_with_execution(
            bundle=result["bundle"],
            graph=graph,
            merged_by=payload.merged_by,
        )
        return {
            **result,
            **build_execution_payload(graph, plan_state=plan_state, structure=structure),
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/api/structure/bundles/{bundle_id}/rebase")
def rebase_structure_bundle_endpoint(bundle_id: str, payload: StructureBundleRebaseRequest) -> dict[str, Any]:
    try:
        result = rebase_bundle(bundle_id, preserve_reviews=payload.preserve_reviews)
        graph = load_graph()
        structure = build_structure_summary(graph)
        plan_state = sync_rebase_with_execution(
            source_bundle_id=bundle_id,
            rebased_bundle=result["bundle"],
            rebased_by=payload.rebased_by,
        )
        return {
            **result,
            **build_execution_payload(graph, plan_state=plan_state, structure=structure),
        }
    except (GraphValidationError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
