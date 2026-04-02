from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from .api_helpers import inspect_project_root, resolve_profile_root, search_project_directories
from .api_models import OnboardingPresetSaveRequest, ProjectAssetProfileJobRequest, ProjectProfileJobRequest
from .onboarding_presets import delete_onboarding_preset, load_onboarding_presets, save_onboarding_preset
from .project_profiler import (
    get_project_asset_profile_job,
    get_project_profile_job,
    resolve_project_profile,
    start_project_asset_profile_job,
    start_project_profile_job,
)


router = APIRouter()


@router.get("/api/project/profile")
def get_project_profile(
    include_tests: bool = False,
    include_internal: bool = True,
    root_path: str | None = None,
    force_refresh: bool = False,
    profiling_mode: str = "metadata_only",
    exclude_paths: list[str] = Query(default_factory=list),
) -> dict[str, Any]:
    root_dir = resolve_profile_root(root_path)
    return {
        "project_profile": resolve_project_profile(
            root_dir,
            include_tests=include_tests,
            include_internal=include_internal,
            force_refresh=force_refresh,
            profiling_mode=profiling_mode,
            exclude_paths=exclude_paths,
        )
    }


@router.post("/api/project/profile/jobs")
def create_project_profile_job(payload: ProjectProfileJobRequest) -> dict[str, Any]:
    root_dir = resolve_profile_root(payload.root_path)
    job = start_project_profile_job(
        root_dir,
        include_tests=payload.include_tests,
        include_internal=payload.include_internal,
        profile_token=payload.profile_token,
        force_refresh=payload.force_refresh,
        profiling_mode=payload.profiling_mode,
        exclude_paths=payload.exclude_paths,
    )
    return {"job": job}


@router.get("/api/project/profile/jobs/{job_id}")
def get_project_profile_job_endpoint(job_id: str) -> dict[str, Any]:
    job = get_project_profile_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Project discovery job not found: {job_id}")
    return {"job": job}


@router.post("/api/project/profile/assets/jobs")
def create_project_asset_profile_job(payload: ProjectAssetProfileJobRequest) -> dict[str, Any]:
    root_dir = resolve_profile_root(payload.root_path)
    job = start_project_asset_profile_job(
        root_dir,
        include_tests=payload.include_tests,
        include_internal=payload.include_internal,
        profile_token=payload.profile_token,
        asset_paths=payload.asset_paths,
        asset_ids=payload.asset_ids,
        exclude_paths=payload.exclude_paths,
    )
    return {"job": job}


@router.get("/api/project/profile/assets/jobs/{job_id}")
def get_project_asset_profile_job_endpoint(job_id: str) -> dict[str, Any]:
    job = get_project_asset_profile_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Project asset profiling job not found: {job_id}")
    return {"job": job}


@router.get("/api/project/directories")
def get_project_directories(
    query: str = "",
    base_path: str | None = None,
) -> dict[str, Any]:
    return {"directories": search_project_directories(base_path, query)}


@router.get("/api/project/root-check")
def get_project_root_check(
    path: str = "",
    include_tests: bool = False,
    include_internal: bool = True,
    profiling_mode: str = "metadata_only",
    exclude_paths: list[str] = Query(default_factory=list),
) -> dict[str, Any]:
    return {
        "root": inspect_project_root(
            path,
            include_tests=include_tests,
            include_internal=include_internal,
            profiling_mode=profiling_mode,
            exclude_paths=exclude_paths,
        )
    }


@router.get("/api/onboarding/presets")
def get_onboarding_presets_endpoint() -> dict[str, Any]:
    return {"presets": load_onboarding_presets()}


@router.post("/api/onboarding/presets")
def save_onboarding_preset_endpoint(payload: OnboardingPresetSaveRequest) -> dict[str, Any]:
    try:
        saved = save_onboarding_preset(payload.preset)
        return {
            "saved": saved,
            "presets": load_onboarding_presets(),
        }
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.delete("/api/onboarding/presets/{preset_id}")
def delete_onboarding_preset_endpoint(preset_id: str) -> dict[str, Any]:
    deleted = delete_onboarding_preset(preset_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Preset not found: {preset_id}")
    return {
        "deleted": preset_id,
        "presets": load_onboarding_presets(),
    }
