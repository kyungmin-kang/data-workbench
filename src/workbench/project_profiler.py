from __future__ import annotations

import ast
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from typing import Any, Callable
import uuid

from .orm_scanner import collect_relationship_targets as collect_orm_relationship_targets
from .orm_scanner import collect_sqlalchemy_class_relations, collect_sqlalchemy_table_relations, merge_relationship_targets, scan_orm_structure_hints
from .profile import build_asset_descriptor, profile_asset
from .project_profiler_planning import summarize_planning_hints
from .project_profiler_support import (
    DEFAULT_IGNORED_PARTS,
    PROJECT_PROFILE_CACHE_VERSION,
    append_hint_evidence_entry,
    build_project_profile_cache_token,
    classify_data_asset_path,
    data_asset_extension_for_format,
    enrich_sql_orm_hint_evidence,
    group_data_assets,
    is_ignored_project_dir_name,
    iter_project_files,
    load_cached_project_profile,
    profile_cache_matches,
    project_profile_exclusion_signature,
    save_cached_project_profile,
    with_project_profile_cache_metadata,
)
from .sql_scanner import scan_sql_structure_hints
from .store import utc_timestamp


IGNORED_PARTS = DEFAULT_IGNORED_PARTS
DATA_SUFFIXES = {".csv", ".gz", ".parquet", ".zip"}
CODE_SUFFIXES = {".py", ".js", ".ts", ".tsx", ".jsx", ".css", ".html", ".sql"}
DOC_SUFFIXES = {".md", ".pdf"}
PYTHON_HINT_SUFFIXES = {".py"}
UI_HINT_SUFFIXES = {".js", ".ts", ".tsx", ".jsx", ".html"}
SQL_HINT_SUFFIXES = {".sql"}
FASTAPI_ROUTE_RE = re.compile(r"@\s*(?:\w+\.)?(get|post|put|patch|delete)\(\s*[\"'](/[^\"']+)[\"']")
FETCH_ROUTE_RE = re.compile(r"fetch\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]")
AXIOS_ROUTE_RE = re.compile(r"axios\.(?:get|post|put|patch|delete)\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]")
JS_ASSIGNMENT_RE = re.compile(r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?);", re.DOTALL)
JS_FUNCTION_RETURN_RE = re.compile(
    r"(?:export\s+)?function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\([^)]*\)\s*\{[\s\S]{0,240}?return\s+(.+?);[\s\S]{0,80}?\}",
    re.DOTALL,
)
JS_ARROW_FUNCTION_RETURN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:async\s*)?\([^)]*\)\s*=>\s*(?:\{[\s\S]{0,240}?return\s+(.+?);[\s\S]{0,80}?\}|(.+?));",
    re.DOTALL,
)
UI_CALL_START_RE = re.compile(r"\b(fetch|axios\.(?:get|post|put|patch|delete))\s*\(")
ASYNC_CALL_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+(fetch|axios\.(?:get|post|put|patch|delete))\s*\(",
    re.MULTILINE,
)
AXIOS_DATA_CALL_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\(\s*await\s+axios\.(?:get|post|put|patch|delete)\s*\(",
    re.MULTILINE,
)
RESPONSE_DATA_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\.data\b",
)
EXPORTED_COMPONENT_RE = re.compile(r"export\s+(?:default\s+)?function\s+([A-Z][A-Za-z0-9_]*)")
FUNCTION_COMPONENT_RE = re.compile(r"\bfunction\s+([A-Z][A-Za-z0-9_]*)\s*\(")
CONST_COMPONENT_RE = re.compile(r"\bconst\s+([A-Z][A-Za-z0-9_]*)\s*=\s*(?:async\s*)?\(")
FETCH_RESPONSE_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+fetch\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]",
)
FETCH_JSON_CHAIN_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+fetch\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`][\s\S]{0,120}?\.json\(\s*\)",
)
JSON_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+([A-Za-z_][A-Za-z0-9_]*)\.json\(\s*\)",
)
AXIOS_RESPONSE_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+axios\.(?:get|post|put|patch|delete)\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]",
)
AXIOS_DATA_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\(\s*await\s+axios\.(?:get|post|put|patch|delete)\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`][\s\S]{0,120}?\)\s*\)\.data",
)
DESTRUCTURE_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s*\{([^}]+)\}\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\b",
)
GENERIC_UI_FIELDS = {"data", "json", "status", "length", "value", "items", "results", "meta"}
MAX_HINT_SCAN_FILE_BYTES = 768 * 1024
GENERATED_HINT_FILE_SUFFIXES = (
    ".min.js",
    ".bundle.js",
    ".chunk.js",
    ".generated.js",
    ".generated.ts",
    ".generated.tsx",
    ".generated.jsx",
    ".generated.py",
)
PROJECT_PROFILE_PROGRESS_REPORT_INTERVAL = 200
PROJECT_PROFILE_JOB_TTL_SECONDS = 60 * 60
PROJECT_PROFILE_JOBS: dict[str, dict[str, Any]] = {}
PROJECT_PROFILE_JOBS_LOCK = threading.Lock()
PROJECT_ASSET_PROFILE_JOBS: dict[str, dict[str, Any]] = {}
PROJECT_ASSET_PROFILE_JOBS_LOCK = threading.Lock()
DEFAULT_PROJECT_PROFILE_MAX_ASSET_BYTES = 16 * 1024 * 1024


def normalize_profiling_mode(value: str | None) -> str:
    return "profile_assets" if str(value or "").strip().lower() == "profile_assets" else "metadata_only"


def profile_project(
    root_dir: Path,
    *,
    include_tests: bool = False,
    include_internal: bool = True,
    profiling_mode: str = "metadata_only",
    exclude_paths: list[str] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict:
    profiling_mode = normalize_profiling_mode(profiling_mode)
    manifests: list[str] = []
    code_files: list[str] = []
    docs: list[str] = []
    data_files: list[Path] = []
    files_scanned = 0

    for path in iter_project_files(
        root_dir,
        include_tests=include_tests,
        include_internal=include_internal,
        exclude_paths=exclude_paths,
        is_test_path=is_test_path,
        is_internal_workbench_path=is_internal_workbench_path,
    ):
        relative = str(path.relative_to(root_dir))
        name = path.name
        suffix = path.suffix.lower()
        files_scanned += 1

        if name in {"pyproject.toml", "package.json", "docker-compose.yml", "Dockerfile", "requirements.txt"}:
            manifests.append(relative)
        if suffix in CODE_SUFFIXES:
            code_files.append(relative)
        if suffix in DOC_SUFFIXES:
            docs.append(relative)
        if suffix in DATA_SUFFIXES:
            data_files.append(path)
        if progress_callback and (
            files_scanned == 1
            or files_scanned % PROJECT_PROFILE_PROGRESS_REPORT_INTERVAL == 0
        ):
            progress_callback(
                {
                    "phase": "walking_files",
                    "message": "Walking the project tree.",
                    "current_path": relative,
                    "files_scanned": files_scanned,
                    "manifests": len(manifests),
                    "code_files": len(code_files),
                    "docs": len(docs),
                    "data_files": len(data_files),
                }
            )

    progress_base = {
        "files_scanned": files_scanned,
        "manifests": len(manifests),
        "code_files": len(code_files),
        "docs": len(docs),
        "data_files": len(data_files),
    }
    if progress_callback:
        progress_callback(
            {
                **progress_base,
                "phase": "profiling_assets",
                "message": (
                    f"Profiling {len(data_files)} detected data assets."
                    if profiling_mode == "profile_assets"
                    else f"Recording metadata for {len(data_files)} detected data assets."
                ),
                "data_assets_processed": 0,
                "data_assets_total": len(data_files),
            }
        )
    data_assets = summarize_data_assets(
        root_dir,
        data_files,
        profiling_mode=profiling_mode,
        progress_callback=progress_callback,
        progress_base=progress_base,
    )
    code_hints, code_hint_stats = summarize_code_hints(
        root_dir,
        code_files,
        progress_callback=progress_callback,
        progress_base=progress_base,
    )
    if progress_callback:
        progress_callback(
            {
                **progress_base,
                "phase": "planning_hints",
                "message": "Mining planning hints from docs and the latest saved plan.",
                "data_assets_total": len(data_assets),
                "skipped_heavy_hint_files": code_hint_stats["skipped_heavy_hint_files"],
            }
        )
    planning_hints = summarize_planning_hints(root_dir)
    notes: list[str] = []
    if code_hint_stats["skipped_heavy_hint_files"]:
        notes.append(
            f"Skipped {code_hint_stats['skipped_heavy_hint_files']} oversized or generated code files during hint discovery."
        )
    if profiling_mode == "metadata_only" and data_assets:
        notes.append("Discovery used metadata-only asset handling. Run explicit asset profiling to calculate row counts and sampled columns.")

    return {
        "root": str(root_dir),
        "summary": {
            "files_scanned": files_scanned,
            "manifests": len(manifests),
            "code_files": len(code_files),
            "docs": len(docs),
            "data_assets": len(data_assets),
            "import_suggestions": len([asset for asset in data_assets if asset.get("suggested_import")]),
            "api_contract_hints": len(code_hints["api_contract_hints"]),
            "ui_contract_hints": len(code_hints["ui_contract_hints"]),
            "sql_structure_hints": len(code_hints["sql_structure_hints"]),
            "orm_structure_hints": len(code_hints["orm_structure_hints"]),
            "planning_api_hints": len(planning_hints["planning_api_hints"]),
            "planning_data_hints": len(planning_hints["planning_data_hints"]),
            "planning_compute_hints": len(planning_hints["planning_compute_hints"]),
            "skipped_heavy_hint_files": code_hint_stats["skipped_heavy_hint_files"],
            "profiling_mode": profiling_mode,
        },
        "manifests": manifests,
        "code_files_sample": code_files[:12],
        "docs_sample": docs[:12],
        "data_assets": data_assets,
        "api_contract_hints": code_hints["api_contract_hints"],
        "ui_contract_hints": code_hints["ui_contract_hints"],
        "sql_structure_hints": code_hints["sql_structure_hints"],
        "orm_structure_hints": code_hints["orm_structure_hints"],
        "planning_api_hints": planning_hints["planning_api_hints"],
        "planning_data_hints": planning_hints["planning_data_hints"],
        "planning_compute_hints": planning_hints["planning_compute_hints"],
        "notes": notes,
    }


def resolve_project_profile(
    root_dir: Path,
    *,
    include_tests: bool = False,
    include_internal: bool = True,
    profile_token: str | None = None,
    force_refresh: bool = False,
    profiling_mode: str = "metadata_only",
    exclude_paths: list[str] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    profiling_mode = normalize_profiling_mode(profiling_mode)
    expected_token = build_project_profile_cache_token(
        root_dir,
        include_tests=include_tests,
        include_internal=include_internal,
        profiling_mode=profiling_mode,
        exclude_paths=exclude_paths,
    )
    candidate_tokens = [token for token in (profile_token, expected_token) if token]
    if not force_refresh:
        for token in candidate_tokens:
            cached = load_cached_project_profile(token)
            if cached and profile_cache_matches(
                cached,
                root_dir=root_dir,
                include_tests=include_tests,
                include_internal=include_internal,
                profiling_mode=profiling_mode,
                exclude_paths=exclude_paths,
            ):
                if progress_callback:
                    progress_callback(
                        {
                            "phase": "loading_cached_profile",
                            "message": "Using the cached discovery snapshot for this root and scope.",
                            "files_scanned": cached.get("summary", {}).get("files_scanned", 0),
                        }
                    )
                return with_project_profile_cache_metadata(cached, cached=True)

    profile = profile_project(
        root_dir,
        include_tests=include_tests,
        include_internal=include_internal,
        profiling_mode=profiling_mode,
        exclude_paths=exclude_paths,
        progress_callback=progress_callback,
    )
    generated_at = utc_timestamp()
    cached_profile = with_project_profile_cache_metadata(
        profile,
        cached=False,
        token=expected_token,
        generated_at=generated_at,
        include_tests=include_tests,
        include_internal=include_internal,
        profiling_mode=profiling_mode,
        exclude_paths=exclude_paths,
    )
    save_cached_project_profile(cached_profile)
    return cached_profile


def summarize_data_assets(
    root_dir: Path,
    data_files: list[Path],
    *,
    profiling_mode: str = "metadata_only",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    progress_base: dict[str, Any] | None = None,
) -> list[dict]:
    profiling_mode = normalize_profiling_mode(profiling_mode)
    grouped_entries = group_data_assets(root_dir, data_files)
    summaries: list[dict] = []
    progress_base = progress_base or {}
    for index, entry in enumerate(grouped_entries, start=1):
        if progress_callback:
            progress_callback(
                {
                    **progress_base,
                    "phase": "profiling_assets",
                    "message": (
                        f"Profiling data asset {index} of {len(grouped_entries)}."
                        if profiling_mode == "profile_assets"
                        else f"Recording metadata for data asset {index} of {len(grouped_entries)}."
                    ),
                    "current_path": str(entry.get("path", "")),
                    "data_assets_processed": index - 1,
                    "data_assets_total": len(grouped_entries),
                }
            )
        summaries.append(summarize_asset_entry(root_dir, entry, profiling_mode=profiling_mode))
    if progress_callback and grouped_entries:
        progress_callback(
            {
                **progress_base,
                "phase": "profiling_assets",
                "message": (
                    f"Profiled {len(grouped_entries)} data assets."
                    if profiling_mode == "profile_assets"
                    else f"Recorded metadata for {len(grouped_entries)} data assets."
                ),
                "data_assets_processed": len(grouped_entries),
                "data_assets_total": len(grouped_entries),
            }
        )
    return sorted(summaries, key=lambda item: item["path"])


def summarize_code_hints(
    root_dir: Path,
    code_files: list[str],
    *,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    progress_base: dict[str, Any] | None = None,
) -> tuple[dict[str, list[dict]], dict[str, int]]:
    api_hints: list[dict] = []
    ui_hints: list[dict] = []
    sql_hints: list[dict] = []
    orm_hints: list[dict] = []
    progress_base = progress_base or {}
    python_files: list[str] = []
    ui_files: list[str] = []
    sql_files: list[str] = []
    skipped_heavy_hint_files = 0
    seen_skipped: set[str] = set()

    for relative_path in code_files:
        suffix = Path(relative_path).suffix.lower()
        if suffix not in PYTHON_HINT_SUFFIXES | UI_HINT_SUFFIXES | SQL_HINT_SUFFIXES:
            continue
        if should_skip_hint_scan(root_dir, relative_path):
            if relative_path not in seen_skipped:
                skipped_heavy_hint_files += 1
                seen_skipped.add(relative_path)
            continue
        if suffix in PYTHON_HINT_SUFFIXES:
            python_files.append(relative_path)
        if suffix in UI_HINT_SUFFIXES:
            ui_files.append(relative_path)
        if suffix in SQL_HINT_SUFFIXES:
            sql_files.append(relative_path)

    if progress_callback:
        progress_callback(
            {
                **progress_base,
                "phase": "indexing_python",
                "message": f"Indexing {len(python_files)} Python modules for route and ORM context.",
                "python_files_total": len(python_files),
                "skipped_heavy_hint_files": skipped_heavy_hint_files,
            }
        )
    python_context = build_python_repo_context(root_dir, python_files)
    files_to_scan = sorted(set(python_files) | set(ui_files) | set(sql_files))

    for index, relative_path in enumerate(files_to_scan, start=1):
        path = root_dir / relative_path
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if progress_callback and (
            index == 1
            or index % 50 == 0
            or index == len(files_to_scan)
        ):
            progress_callback(
                {
                    **progress_base,
                    "phase": "summarizing_code",
                    "message": f"Scanning code hints in {index} of {len(files_to_scan)} files.",
                    "current_path": relative_path,
                    "code_hint_files_processed": index,
                    "code_hint_files_total": len(files_to_scan),
                    "skipped_heavy_hint_files": skipped_heavy_hint_files,
                }
            )

        suffix = path.suffix.lower()
        if suffix in PYTHON_HINT_SUFFIXES:
            api_hints.extend(extract_api_contract_hints(relative_path, text, python_context=python_context))
            orm_hints.extend(
                extract_orm_structure_hints(
                    relative_path,
                    text,
                    python_context=python_context.get(relative_path, {}),
                )
            )
        if suffix in UI_HINT_SUFFIXES:
            ui_hints.extend(extract_ui_contract_hints(relative_path, text))
        if suffix in SQL_HINT_SUFFIXES:
            sql_hints.extend(extract_sql_structure_hints(relative_path, text))

    sql_hints, orm_hints = enrich_sql_orm_hint_evidence(sql_hints, orm_hints)
    return (
        {
            "api_contract_hints": dedupe_hint_list(api_hints, keys=("method", "route", "file")),
            "ui_contract_hints": dedupe_hint_list(ui_hints, keys=("component", "file")),
            "sql_structure_hints": dedupe_hint_list(sql_hints, keys=("relation", "object_type", "file")),
            "orm_structure_hints": dedupe_hint_list(orm_hints, keys=("relation", "object_type", "file")),
        },
        {
            "skipped_heavy_hint_files": skipped_heavy_hint_files,
        },
    )


def should_skip_hint_scan(root_dir: Path, relative_path: str) -> bool:
    name = Path(relative_path).name.lower()
    if any(name.endswith(suffix) for suffix in GENERATED_HINT_FILE_SUFFIXES):
        return True
    try:
        size = (root_dir / relative_path).stat().st_size
    except OSError:
        return False
    return size > MAX_HINT_SCAN_FILE_BYTES


def start_project_profile_job(
    root_dir: Path,
    *,
    include_tests: bool = False,
    include_internal: bool = True,
    profile_token: str | None = None,
    force_refresh: bool = False,
    profiling_mode: str = "metadata_only",
    exclude_paths: list[str] | None = None,
) -> dict[str, Any]:
    profiling_mode = normalize_profiling_mode(profiling_mode)
    job_id = uuid.uuid4().hex[:12]
    job = {
        "job_id": job_id,
        "status": "queued",
        "created_at": utc_timestamp(),
        "started_at": "",
        "completed_at": "",
        "error": "",
        "options": {
            "root_path": str(root_dir),
            "include_tests": include_tests,
            "include_internal": include_internal,
            "profile_token": profile_token or "",
            "force_refresh": force_refresh,
            "profiling_mode": profiling_mode,
            "exclude_paths": list(exclude_paths or []),
        },
        "progress": {
            "phase": "queued",
            "message": "Queued project discovery.",
            "files_scanned": 0,
            "updated_at": utc_timestamp(),
        },
    }
    with PROJECT_PROFILE_JOBS_LOCK:
        prune_project_profile_jobs_locked()
        PROJECT_PROFILE_JOBS[job_id] = job

    def update_progress(update: dict[str, Any]) -> None:
        with PROJECT_PROFILE_JOBS_LOCK:
            current = PROJECT_PROFILE_JOBS.get(job_id)
            if current is None:
                return
            progress = current.setdefault("progress", {})
            progress.update({key: value for key, value in update.items() if value is not None})
            progress["updated_at"] = utc_timestamp()
            if current["status"] == "queued":
                current["status"] = "running"
                current["started_at"] = progress["updated_at"]

    def run_job() -> None:
        update_progress({"phase": "starting", "message": "Starting project discovery."})
        try:
            project_profile = resolve_project_profile(
                root_dir,
                include_tests=include_tests,
                include_internal=include_internal,
                profile_token=profile_token,
                force_refresh=force_refresh,
                profiling_mode=profiling_mode,
                exclude_paths=exclude_paths,
                progress_callback=update_progress,
            )
        except Exception as error:  # pragma: no cover - defensive job wrapper
            with PROJECT_PROFILE_JOBS_LOCK:
                current = PROJECT_PROFILE_JOBS.get(job_id)
                if current is None:
                    return
                current["status"] = "failed"
                current["error"] = str(error)
                current["completed_at"] = utc_timestamp()
                current.setdefault("progress", {}).update(
                    {
                        "phase": "failed",
                        "message": str(error),
                        "updated_at": current["completed_at"],
                    }
                )
            return

        with PROJECT_PROFILE_JOBS_LOCK:
            current = PROJECT_PROFILE_JOBS.get(job_id)
            if current is None:
                return
            current["status"] = "completed"
            current["completed_at"] = utc_timestamp()
            current["project_profile"] = project_profile
            current.setdefault("progress", {}).update(
                {
                    "phase": "completed",
                    "message": "Project discovery is ready.",
                    "updated_at": current["completed_at"],
                }
            )

    thread = threading.Thread(target=run_job, name=f"project-profile-{job_id}", daemon=True)
    thread.start()
    return deepcopy(job)


def get_project_profile_job(job_id: str) -> dict[str, Any] | None:
    with PROJECT_PROFILE_JOBS_LOCK:
        prune_project_profile_jobs_locked()
        job = PROJECT_PROFILE_JOBS.get(job_id)
        return deepcopy(job) if job else None


def start_project_asset_profile_job(
    root_dir: Path,
    *,
    include_tests: bool = False,
    include_internal: bool = True,
    profile_token: str | None = None,
    asset_paths: list[str] | None = None,
    asset_ids: list[str] | None = None,
    exclude_paths: list[str] | None = None,
) -> dict[str, Any]:
    job_id = uuid.uuid4().hex[:12]
    job = {
        "job_id": job_id,
        "status": "queued",
        "created_at": utc_timestamp(),
        "started_at": "",
        "completed_at": "",
        "error": "",
        "options": {
            "root_path": str(root_dir),
            "include_tests": include_tests,
            "include_internal": include_internal,
            "profile_token": profile_token or "",
            "asset_paths": list(asset_paths or []),
            "asset_ids": list(asset_ids or []),
            "exclude_paths": list(exclude_paths or []),
        },
        "progress": {
            "phase": "queued",
            "message": "Queued explicit asset profiling.",
            "assets_processed": 0,
            "assets_total": len(asset_paths or asset_ids or []),
            "updated_at": utc_timestamp(),
        },
        "asset_profiles": [],
    }
    with PROJECT_ASSET_PROFILE_JOBS_LOCK:
        prune_project_asset_profile_jobs_locked()
        PROJECT_ASSET_PROFILE_JOBS[job_id] = job

    def update_progress(update: dict[str, Any]) -> None:
        with PROJECT_ASSET_PROFILE_JOBS_LOCK:
            current = PROJECT_ASSET_PROFILE_JOBS.get(job_id)
            if current is None:
                return
            progress = current.setdefault("progress", {})
            progress.update({key: value for key, value in update.items() if value is not None})
            progress["updated_at"] = utc_timestamp()
            if current["status"] == "queued":
                current["status"] = "running"
                current["started_at"] = progress["updated_at"]

    def run_job() -> None:
        update_progress({"phase": "starting", "message": "Loading cached discovery for selected assets."})
        try:
            project_profile = resolve_project_profile(
                root_dir,
                include_tests=include_tests,
                include_internal=include_internal,
                profile_token=profile_token,
                profiling_mode="metadata_only",
                exclude_paths=exclude_paths,
            )
            selected_entries = select_project_profile_assets(
                project_profile,
                asset_paths=asset_paths or [],
                asset_ids=asset_ids or [],
            )
            if not selected_entries:
                raise ValueError("No matching discovered assets were selected for profiling.")

            asset_profiles: list[dict[str, Any]] = []
            total = len(selected_entries)
            for index, entry in enumerate(selected_entries, start=1):
                update_progress(
                    {
                        "phase": "profiling_asset",
                        "message": f"Profiling selected asset {index} of {total}.",
                        "assets_processed": index - 1,
                        "assets_total": total,
                        "current_path": entry.get("path", ""),
                        "current_asset_id": entry.get("id", ""),
                    }
                )
                asset_profiles.append(profile_asset_entry_isolated(root_dir, entry))

        except Exception as error:  # pragma: no cover - defensive job wrapper
            with PROJECT_ASSET_PROFILE_JOBS_LOCK:
                current = PROJECT_ASSET_PROFILE_JOBS.get(job_id)
                if current is None:
                    return
                current["status"] = "failed"
                current["error"] = str(error)
                current["completed_at"] = utc_timestamp()
                current.setdefault("progress", {}).update(
                    {
                        "phase": "failed",
                        "message": str(error),
                        "updated_at": current["completed_at"],
                    }
                )
            return

        with PROJECT_ASSET_PROFILE_JOBS_LOCK:
            current = PROJECT_ASSET_PROFILE_JOBS.get(job_id)
            if current is None:
                return
            current["status"] = "completed"
            current["completed_at"] = utc_timestamp()
            current["asset_profiles"] = asset_profiles
            current.setdefault("progress", {}).update(
                {
                    "phase": "completed",
                    "message": "Selected asset profiling is ready.",
                    "assets_processed": len(asset_profiles),
                    "assets_total": len(asset_profiles),
                    "updated_at": current["completed_at"],
                }
            )

    thread = threading.Thread(target=run_job, name=f"project-asset-profile-{job_id}", daemon=True)
    thread.start()
    return deepcopy(job)


def get_project_asset_profile_job(job_id: str) -> dict[str, Any] | None:
    with PROJECT_ASSET_PROFILE_JOBS_LOCK:
        prune_project_asset_profile_jobs_locked()
        job = PROJECT_ASSET_PROFILE_JOBS.get(job_id)
        return deepcopy(job) if job else None


def prune_project_profile_jobs_locked() -> None:
    cutoff = time.time() - PROJECT_PROFILE_JOB_TTL_SECONDS
    expired_ids = [
        job_id
        for job_id, job in PROJECT_PROFILE_JOBS.items()
        if job.get("status") in {"completed", "failed"}
        and parse_job_timestamp(job.get("completed_at", "") or job.get("created_at", "")) < cutoff
    ]
    for job_id in expired_ids:
        PROJECT_PROFILE_JOBS.pop(job_id, None)


def prune_project_asset_profile_jobs_locked() -> None:
    cutoff = time.time() - PROJECT_PROFILE_JOB_TTL_SECONDS
    expired_ids = [
        job_id
        for job_id, job in PROJECT_ASSET_PROFILE_JOBS.items()
        if job.get("status") in {"completed", "failed"}
        and parse_job_timestamp(job.get("completed_at", "") or job.get("created_at", "")) < cutoff
    ]
    for job_id in expired_ids:
        PROJECT_ASSET_PROFILE_JOBS.pop(job_id, None)


def parse_job_timestamp(value: str) -> float:
    if not value:
        return 0.0
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def build_python_repo_context(root_dir: Path, code_files: list[str]) -> dict[str, dict[str, Any]]:
    python_files = sorted(relative_path for relative_path in code_files if relative_path.endswith(".py"))
    module_infos: dict[str, dict[str, Any]] = {}
    for relative_path in python_files:
        path = root_dir / relative_path
        try:
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text)
        except (UnicodeDecodeError, SyntaxError):
            continue
        module_name = module_name_from_relative_path(relative_path)
        module_infos[relative_path] = {
            "relative_path": relative_path,
            "module_name": module_name,
            "tree": tree,
            "class_relations": collect_sqlalchemy_class_relations(tree),
            "table_relations": collect_sqlalchemy_table_relations(tree),
            "model_fields": collect_constructor_model_fields(tree),
            "imports": collect_python_import_bindings(tree, module_name),
        }

    contexts: dict[str, dict[str, Any]] = {
        relative_path: {
            "module_name": info["module_name"],
            "class_relations": dict(info["class_relations"]),
            "table_relations": dict(info["table_relations"]),
            "relationship_targets": collect_orm_relationship_targets(
                info["tree"],
                class_relations=info["class_relations"],
                table_relations=info["table_relations"],
            ),
            "helper_relation_templates": {},
            "helper_templates": {},
            "object_templates": {},
            "imports": info["imports"],
        }
        for relative_path, info in module_infos.items()
    }

    for _ in range(4):
        changed = False
        for relative_path in sorted(module_infos):
            info = module_infos[relative_path]
            imported_context = build_imported_python_context(info, contexts)
            class_relations = dict(info["class_relations"])
            class_relations.update(imported_context["class_relations"])
            table_relations = imported_context["table_relations"] | dict(info["table_relations"])
            relationship_targets = merge_relationship_targets(
                imported_context["relationship_targets"],
                collect_orm_relationship_targets(
                    info["tree"],
                    class_relations=class_relations,
                    table_relations=table_relations,
                ),
            )
            helper_relation_templates = build_python_helper_relation_templates(
                info["tree"],
                class_relations,
                initial_templates=imported_context["helper_relation_templates"],
                object_templates=imported_context["object_templates"],
            )
            helper_templates = build_python_helper_templates(
                info["tree"],
                class_relations,
                info["model_fields"],
                helper_relation_templates,
                imported_context["object_templates"],
                initial_templates=imported_context["helper_templates"],
            )
            object_templates = build_python_object_templates(
                info["tree"],
                class_relations,
                helper_relation_templates,
                helper_templates,
                imported_context["object_templates"],
            )
            helper_templates = build_python_helper_templates(
                info["tree"],
                class_relations,
                info["model_fields"],
                helper_relation_templates,
                imported_context["object_templates"] | object_templates,
                initial_templates=imported_context["helper_templates"],
            )
            object_templates = build_python_object_templates(
                info["tree"],
                class_relations,
                helper_relation_templates,
                helper_templates,
                imported_context["object_templates"] | object_templates,
            )
            helper_relation_templates = imported_context["helper_relation_templates"] | helper_relation_templates
            helper_templates = imported_context["helper_templates"] | helper_templates
            object_templates = imported_context["object_templates"] | object_templates
            next_context = {
                "module_name": info["module_name"],
                "class_relations": class_relations,
                "table_relations": table_relations,
                "relationship_targets": relationship_targets,
                "helper_relation_templates": helper_relation_templates,
                "helper_templates": helper_templates,
                "object_templates": object_templates,
                "imports": info["imports"],
            }
            if next_context != contexts.get(relative_path):
                contexts[relative_path] = next_context
                changed = True
        if not changed:
            break
    return contexts


def module_name_from_relative_path(relative_path: str) -> str:
    normalized = relative_path.replace("\\", "/")
    if normalized.endswith("/__init__.py"):
        normalized = normalized[: -len("/__init__.py")]
    elif normalized.endswith(".py"):
        normalized = normalized[:-3]
    return normalized.replace("/", ".")


def collect_python_import_bindings(tree: ast.AST, module_name: str) -> dict[str, dict[str, Any]]:
    symbol_aliases: dict[str, dict[str, str]] = {}
    module_aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                alias_name = alias.asname or alias.name.split(".")[-1]
                module_aliases[alias_name] = alias.name
        elif isinstance(node, ast.ImportFrom):
            target_module = resolve_import_module(module_name, node.module, node.level)
            if not target_module:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                alias_name = alias.asname or alias.name
                symbol_aliases[alias_name] = {"module": target_module, "symbol": alias.name}
    return {"symbol_aliases": symbol_aliases, "module_aliases": module_aliases}


def resolve_import_module(current_module: str, imported_module: str | None, level: int) -> str:
    if level <= 0:
        return imported_module or ""
    current_parts = current_module.split(".") if current_module else []
    base_parts = current_parts[:-level]
    if imported_module:
        base_parts.extend(imported_module.split("."))
    return ".".join(part for part in base_parts if part)


def find_context_by_module_name(
    contexts: dict[str, dict[str, Any]],
    module_name: str,
) -> dict[str, Any] | None:
    for context in contexts.values():
        if context.get("module_name") == module_name:
            return context
    return None


def build_imported_python_context(
    module_info: dict[str, Any],
    contexts: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    imported = {
        "class_relations": {},
        "table_relations": {},
        "relationship_targets": {},
        "helper_relation_templates": {},
        "helper_templates": {},
        "object_templates": {},
    }
    imports = module_info.get("imports", {})
    for alias_name, target in (imports.get("symbol_aliases", {}) or {}).items():
        context = find_context_by_module_name(contexts, target.get("module", ""))
        if not context:
            continue
        symbol_name = target.get("symbol", "")
        relation = context.get("class_relations", {}).get(symbol_name)
        if relation:
            imported["class_relations"][alias_name] = relation
            imported["relationship_targets"] = merge_relationship_targets(
                imported["relationship_targets"],
                context.get("relationship_targets", {}),
            )
        table_relation = context.get("table_relations", {}).get(symbol_name)
        if table_relation:
            imported["table_relations"][alias_name] = table_relation
        helper_relation_template = context.get("helper_relation_templates", {}).get(symbol_name)
        if helper_relation_template:
            imported["helper_relation_templates"][alias_name] = helper_relation_template
        helper_template = context.get("helper_templates", {}).get(symbol_name)
        if helper_template:
            imported["helper_templates"][alias_name] = helper_template
            extend_imported_object_templates(imported, context)
            extend_imported_helper_return_object_context(imported, helper_template, context)
        object_template = context.get("object_templates", {}).get(symbol_name)
        if object_template:
            imported["object_templates"][alias_name] = object_template
            extend_imported_object_templates(imported, context)
        if relation or table_relation or helper_relation_template or helper_template or object_template:
            continue
        submodule_context = find_context_by_module_name(contexts, f"{target.get('module', '')}.{symbol_name}")
        if not submodule_context:
            continue
        extend_imported_module_alias_context(imported, alias_name, submodule_context)
    for alias_name, target_module in (imports.get("module_aliases", {}) or {}).items():
        context = find_context_by_module_name(contexts, target_module)
        if not context:
            continue
        extend_imported_module_alias_context(imported, alias_name, context)
    return imported


def extend_imported_module_alias_context(
    imported: dict[str, dict[str, Any]],
    alias_name: str,
    context: dict[str, Any],
) -> None:
    for symbol_name, relation in (context.get("class_relations", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["class_relations"][f"{alias_name}.{symbol_name}"] = relation
    for symbol_name, relation in (context.get("table_relations", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["table_relations"][f"{alias_name}.{symbol_name}"] = relation
    imported["relationship_targets"] = merge_relationship_targets(
        imported["relationship_targets"],
        context.get("relationship_targets", {}),
    )
    for symbol_name, template in (context.get("helper_relation_templates", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["helper_relation_templates"][f"{alias_name}.{symbol_name}"] = template
    for symbol_name, template in (context.get("helper_templates", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["helper_templates"][f"{alias_name}.{symbol_name}"] = qualify_helper_template_for_module_alias(
            template,
            alias_name,
            context,
        )
    for symbol_name, template in (context.get("object_templates", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["object_templates"][f"{alias_name}.{symbol_name}"] = template


def extend_imported_object_templates(
    imported: dict[str, dict[str, Any]],
    context: dict[str, Any],
) -> None:
    for symbol_name, template in (context.get("object_templates", {}) or {}).items():
        if symbol_name not in imported["object_templates"]:
            imported["object_templates"][symbol_name] = template


def qualify_helper_template_for_module_alias(
    template: dict[str, Any],
    alias_name: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    qualified = deepcopy(template)
    qualify_object_state_for_module_alias(
        qualified.get("return_object", {}),
        alias_name,
        context,
    )
    return qualified


def extend_imported_helper_return_object_context(
    imported: dict[str, dict[str, Any]],
    helper_template: dict[str, Any],
    context: dict[str, Any],
) -> None:
    extend_imported_object_state_context(
        imported,
        helper_template.get("return_object", {}),
        context,
    )


def qualify_object_state_for_module_alias(
    object_state: dict[str, Any],
    alias_name: str,
    context: dict[str, Any],
) -> None:
    if not object_state:
        return
    class_name = object_state.get("class_name", "")
    if class_name and "." not in class_name and class_name in (context.get("object_templates", {}) or {}):
        object_state["class_name"] = f"{alias_name}.{class_name}"
    for nested_state in (object_state.get("object_bindings", {}) or {}).values():
        if isinstance(nested_state, dict):
            qualify_object_state_for_module_alias(nested_state, alias_name, context)


def extend_imported_object_state_context(
    imported: dict[str, dict[str, Any]],
    object_state: dict[str, Any],
    context: dict[str, Any],
) -> None:
    if not object_state:
        return
    class_name = object_state.get("class_name", "")
    if class_name and "." not in class_name:
        object_template = (context.get("object_templates", {}) or {}).get(class_name)
        if object_template and class_name not in imported["object_templates"]:
            imported["object_templates"][class_name] = object_template
    for nested_state in (object_state.get("object_bindings", {}) or {}).values():
        if isinstance(nested_state, dict):
            extend_imported_object_state_context(imported, nested_state, context)


def extract_api_contract_hints(
    relative_path: str,
    text: str,
    *,
    python_context: dict[str, dict[str, Any]] | None = None,
) -> list[dict]:
    if relative_path.endswith(".py"):
        python_hints = extract_api_contract_hints_from_python(
            relative_path,
            text,
            python_context=python_context,
        )
        if python_hints:
            return python_hints

    hints: list[dict] = []
    for method, route in FASTAPI_ROUTE_RE.findall(text):
        normalized_path = normalize_api_hint_path(route)
        route_signature = build_api_route_signature(method.upper(), normalized_path)
        hints.append(
            {
                "id": build_api_hint_id(relative_path, method.upper(), normalized_path),
                "label": route_signature,
                "method": method.upper(),
                "route": route_signature,
                "path": normalized_path,
                "file": relative_path,
                "detected_from": "fastapi_decorator",
                "description": f"Detected from {relative_path}",
                "response_fields": [],
                "response_model": "",
            }
        )
    return hints


def extract_ui_contract_hints(relative_path: str, text: str) -> list[dict]:
    variable_routes, function_routes = collect_ui_route_definitions(text)
    api_routes = sorted(set(collect_ui_call_routes(text, variable_routes, function_routes)))
    if not api_routes:
        return []

    component_names = detect_component_names(relative_path, text)
    route_field_hints = extract_ui_route_field_hints(text, variable_routes, function_routes)
    used_fields = sorted({field for fields in route_field_hints.values() for field in fields})
    hints: list[dict] = []
    for component_name in component_names:
        hints.append(
            {
                "id": build_hint_id("ui", relative_path, component_name),
                "label": component_name,
                "component": component_name,
                "api_routes": api_routes,
                "file": relative_path,
                "detected_from": "ui_fetch_usage",
                "description": f"Detected from {relative_path}",
                "used_fields": used_fields,
                "route_field_hints": route_field_hints,
            }
        )
    return hints


def extract_sql_structure_hints(relative_path: str, text: str) -> list[dict]:
    return scan_sql_structure_hints(relative_path, text)


def extract_orm_structure_hints(
    relative_path: str,
    text: str,
    *,
    python_context: dict[str, Any] | None = None,
) -> list[dict]:
    python_context = python_context or {}
    return scan_orm_structure_hints(
        relative_path,
        text,
        imported_class_relations=python_context.get("class_relations", {}),
        imported_table_relations=python_context.get("table_relations", {}),
        imported_relationship_targets=python_context.get("relationship_targets", {}),
    )


def extract_api_contract_hints_from_python(
    relative_path: str,
    text: str,
    *,
    python_context: dict[str, dict[str, Any]] | None = None,
) -> list[dict]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []

    module_context = (python_context or {}).get(relative_path, {})
    imported_context = (
        build_imported_python_context(module_context, python_context or {})
        if module_context and python_context
        else {"class_relations": {}, "helper_relation_templates": {}, "helper_templates": {}, "object_templates": {}}
    )
    model_fields = collect_constructor_model_fields(tree)
    class_relations = dict(collect_sqlalchemy_class_relations(tree))
    class_relations.update(imported_context.get("class_relations", {}))
    imported_helper_relations = dict(imported_context.get("helper_relation_templates", {}) or {})
    imported_helper_templates = dict(imported_context.get("helper_templates", {}) or {})
    imported_object_templates = dict(imported_context.get("object_templates", {}) or {})
    helper_relation_templates = build_python_helper_relation_templates(
        tree,
        class_relations,
        initial_templates=imported_helper_relations,
        object_templates=imported_object_templates,
    )
    helper_templates = build_python_helper_templates(
        tree,
        class_relations,
        model_fields,
        helper_relation_templates,
        imported_object_templates,
        initial_templates=imported_helper_templates,
    )
    object_templates = build_python_object_templates(
        tree,
        class_relations,
        helper_relation_templates,
        helper_templates,
        imported_object_templates,
    )
    helper_templates = build_python_helper_templates(
        tree,
        class_relations,
        model_fields,
        helper_relation_templates,
        imported_object_templates | object_templates,
        initial_templates=imported_helper_templates,
    )
    object_templates = build_python_object_templates(
        tree,
        class_relations,
        helper_relation_templates,
        helper_templates,
        imported_object_templates | object_templates,
    )
    helper_relation_templates = imported_helper_relations | helper_relation_templates
    helper_templates = imported_helper_templates | helper_templates
    object_templates = imported_object_templates | object_templates
    hints: list[dict] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        decorators = extract_route_decorators(node)
        if not decorators:
            continue

        variable_relations = collect_function_relation_bindings(
            node,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        variable_sources = collect_local_source_assignments(
            node,
            variable_relations,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        dependency_instances = collect_dependency_object_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        object_instances = collect_local_object_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates,
            object_templates,
            initial_assignments=dependency_instances,
        )
        assigned_dicts = collect_local_dict_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        response_field_sources = collect_return_field_mappings(
            node,
            assigned_dicts,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        response_fields = sorted(response_field_sources)
        for decorator in decorators:
            model_name = decorator["response_model"]
            hinted_fields = set(response_fields)
            if model_name:
                hinted_fields.update(model_fields.get(model_name, []))
            normalized_path = normalize_api_hint_path(decorator["path"])
            route_signature = build_api_route_signature(decorator["method"], normalized_path)
            hints.append(
                {
                    "id": build_api_hint_id(relative_path, decorator["method"], normalized_path),
                    "label": route_signature,
                    "method": decorator["method"],
                    "route": route_signature,
                    "path": normalized_path,
                    "file": relative_path,
                    "detected_from": "fastapi_ast",
                    "description": f"Detected from {relative_path}",
                    "response_fields": sorted(hinted_fields),
                    "response_field_sources": [
                        {"name": field_name, "source_fields": response_field_sources.get(field_name, [])}
                        for field_name in sorted(response_field_sources)
                    ],
                    "response_model": model_name or "",
                }
            )
    return hints


def build_python_helper_templates(
    tree: ast.AST,
    class_relations: dict[str, str],
    model_fields: dict[str, list[str]],
    helper_relation_templates: dict[str, dict[str, Any]],
    object_templates: dict[str, dict[str, Any]],
    *,
    initial_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = dict(initial_templates or {})
    templates.update({
        model_name: {
            "params": list(field_names),
            "return_fields": {
                field_name: [{"relation": f"param:{field_name}", "column": field_name}]
                for field_name in field_names
            },
        }
        for model_name, field_names in sorted(model_fields.items())
        if field_names
    })
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if extract_route_decorators(node):
            continue
        parameters = [
            argument.arg
            for argument in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
            if argument.arg not in {"self", "cls"}
        ]
        parameter_relations = {parameter: f"param:{parameter}" for parameter in parameters}
        variable_relations = collect_function_relation_bindings(
            node,
            class_relations,
            initial_bindings=parameter_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        dependency_instances = collect_dependency_object_assignments(
            node,
            variable_relations,
            class_relations,
            {},
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        variable_sources = collect_local_source_assignments(
            node,
            variable_relations,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=dependency_instances,
        )
        object_instances = collect_local_object_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates,
            object_templates,
            initial_assignments=dependency_instances,
        )
        assigned_dicts = collect_local_dict_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        return_fields = collect_return_field_mappings(
            node,
            assigned_dicts,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        return_object = collect_return_object_state(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if return_fields or return_object:
            template = {
                "params": parameters,
                "return_fields": return_fields,
            }
            passthrough_params = collect_passthrough_payload_parameters(node, parameters)
            if passthrough_params:
                template["passthrough_payload_params"] = passthrough_params
            if return_object:
                template["return_object"] = return_object
            templates[node.name] = template
    return templates


def build_python_helper_relation_templates(
    tree: ast.AST,
    class_relations: dict[str, str],
    *,
    initial_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = dict(initial_templates or {})
    for _ in range(3):
        updated = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if extract_route_decorators(node):
                continue
            parameters = [
                argument.arg
                for argument in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
                if argument.arg not in {"self", "cls"}
            ]
            parameter_relations = {parameter: f"param:{parameter}" for parameter in parameters}
            variable_relations = collect_function_relation_bindings(
                node,
                class_relations,
                initial_bindings=parameter_relations,
                helper_relation_templates=templates,
                object_templates=object_templates,
            )
            dependency_instances = collect_dependency_object_assignments(
                node,
                variable_relations,
                class_relations,
                {},
                {},
                helper_relation_templates=templates,
                object_templates=object_templates,
            )
            return_relation = collect_return_relation(
                node,
                variable_relations,
                class_relations,
                helper_relation_templates=templates,
                object_templates=object_templates,
                object_instances=dependency_instances,
            )
            if not return_relation:
                continue
            template = {"params": parameters, "return_relation": return_relation}
            if templates.get(node.name) != template:
                templates[node.name] = template
                updated = True
        if not updated:
            break
    return templates


def build_python_object_templates(
    tree: ast.AST,
    class_relations: dict[str, str],
    helper_relation_templates: dict[str, dict[str, Any]],
    helper_templates: dict[str, dict[str, Any]],
    previous_templates: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = {}
    for class_node in ast.walk(tree):
        if not isinstance(class_node, ast.ClassDef):
            continue
        init_node = next(
            (
                statement
                for statement in class_node.body
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)) and statement.name == "__init__"
            ),
            None,
        )
        init_params = [
            argument.arg
            for argument in [*init_node.args.posonlyargs, *init_node.args.args, *init_node.args.kwonlyargs]
            if init_node and argument.arg not in {"self", "cls"}
        ] if init_node else []
        attribute_bindings = collect_init_attribute_bindings(init_node)
        methods: dict[str, dict[str, Any]] = {}
        current_class_template = {
            "params": init_params,
            "attributes": attribute_bindings,
            "methods": methods,
        }
        for statement in class_node.body:
            if not isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if statement.name == "__init__":
                continue
            template_context = previous_templates | templates | {class_node.name: current_class_template}
            method_params = [
                argument.arg
                for argument in [*statement.args.posonlyargs, *statement.args.args, *statement.args.kwonlyargs]
                if argument.arg not in {"self", "cls"}
            ]
            initial_bindings = {f"self.{attr_name}": placeholder for attr_name, placeholder in attribute_bindings.items()}
            initial_bindings.update({parameter: f"param:{parameter}" for parameter in method_params})
            self_instance = {
                "class_name": class_node.name,
                "relation_bindings": dict(attribute_bindings),
                "source_bindings": {},
                "object_bindings": {
                    attr_name: {"param_object": placeholder.split(":", 1)[1]}
                    for attr_name, placeholder in attribute_bindings.items()
                    if placeholder.startswith("param:")
                },
            }
            variable_relations = collect_function_relation_bindings(
                statement,
                class_relations,
                initial_bindings=initial_bindings,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances={"self": self_instance},
            )
            dependency_instances = collect_dependency_object_assignments(
                statement,
                variable_relations,
                class_relations,
                {},
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
            )
            variable_sources = collect_local_source_assignments(
                statement,
                variable_relations,
                helper_templates,
                initial_assignments=self_instance.get("source_bindings", {}),
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances={"self": self_instance} | dependency_instances,
            )
            object_instances = collect_local_object_assignments(
                statement,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates,
                template_context,
                initial_assignments={"self": self_instance} | dependency_instances,
            )
            assigned_dicts = collect_local_dict_assignments(
                statement,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances=object_instances,
            )
            return_fields = collect_return_field_mappings(
                statement,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances=object_instances,
            )
            return_relation = collect_return_relation(
                statement,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances=object_instances,
            )
            methods[statement.name] = {
                "params": method_params,
                "return_fields": return_fields,
                "return_relation": return_relation,
                "body_node": statement,
            }
            passthrough_params = collect_passthrough_payload_parameters(statement, method_params)
            if passthrough_params:
                methods[statement.name]["passthrough_payload_params"] = passthrough_params
        if attribute_bindings or methods:
            templates[class_node.name] = current_class_template
    return templates


def collect_constructor_model_fields(tree: ast.AST) -> dict[str, list[str]]:
    models: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or not class_looks_like_constructor_model(node):
            continue
        field_names: list[str] = []
        for statement in node.body:
            if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                field_names.append(statement.target.id)
            elif isinstance(statement, ast.Assign):
                for target in statement.targets:
                    if isinstance(target, ast.Name):
                        field_names.append(target.id)
        if field_names:
            models[node.name] = sorted(dict.fromkeys(field_names))
    return models


def class_looks_like_constructor_model(node: ast.ClassDef) -> bool:
    for base in node.bases:
        name = get_ast_name(base)
        if name.split(".")[-1] == "BaseModel":
            return True
    for decorator in node.decorator_list:
        name = get_ast_name(decorator)
        if name.split(".")[-1] == "dataclass":
            return True
    return False


def extract_route_decorators(function_node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[dict[str, str]]:
    decorators: list[dict[str, str]] = []
    for decorator in function_node.decorator_list:
        if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
            continue
        method_name = decorator.func.attr.lower()
        if method_name not in {"get", "post", "put", "patch", "delete"}:
            continue
        if not decorator.args or not isinstance(decorator.args[0], ast.Constant) or not isinstance(decorator.args[0].value, str):
            continue
        path = decorator.args[0].value
        response_model = ""
        for keyword in decorator.keywords:
            if keyword.arg == "response_model":
                response_model = extract_model_name(keyword.value)
                break
        decorators.append({"method": method_name.upper(), "path": path, "response_model": response_model})
    return decorators


def extract_model_name(node: ast.AST | None) -> str:
    if node is None:
        return ""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return extract_model_name(get_subscript_slice(node))
    if isinstance(node, ast.Tuple):
        for element in node.elts:
            model_name = extract_model_name(element)
            if model_name:
                return model_name
    return ""


def get_subscript_slice(node: ast.Subscript) -> ast.AST:
    return node.slice if not isinstance(node.slice, ast.Index) else node.slice.value


def iter_ordered_statement_nodes(node: ast.AST, *, root: ast.AST | None = None) -> Iterable[ast.stmt]:
    root_node = root or node
    if node is not root_node and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return
    if node is not root_node and isinstance(node, ast.stmt):
        yield node
    for child in ast.iter_child_nodes(node):
        if child is not root_node and isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        yield from iter_ordered_statement_nodes(child, root=root_node)


def extract_target_names(node: ast.AST | None) -> list[str]:
    if node is None:
        return []
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        names: list[str] = []
        for element in node.elts:
            names.extend(extract_target_names(element))
        return names
    return []


def unwrap_await(node: ast.AST | None) -> ast.AST | None:
    current = node
    while isinstance(current, ast.Await):
        current = current.value
    return current


def extend_relations_for_comprehensions(
    generators: list[ast.comprehension],
    variable_relations: dict[str, str],
) -> dict[str, str]:
    extended = dict(variable_relations)
    for generator in generators:
        relation = extract_relation_from_expression(generator.iter, extended, {})
        if not relation:
            continue
        for target_name in extract_target_names(generator.target):
            extended[target_name] = relation
    return extended


def is_empty_mapping_expression(node: ast.AST | None) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.Dict):
        return not any(isinstance(key, ast.Constant) and isinstance(key.value, str) for key in node.keys)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict":
        return not node.args and not node.keywords
    return False


def extract_dict_assignment_target(node: ast.AST | None) -> tuple[str, str]:
    if not isinstance(node, ast.Subscript) or not isinstance(node.value, ast.Name):
        return "", ""
    key_node = get_subscript_slice(node)
    if not isinstance(key_node, ast.Constant) or not isinstance(key_node.value, str):
        return "", ""
    return node.value.id, key_node.value


def collect_init_attribute_bindings(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef | None,
) -> dict[str, str]:
    if function_node is None:
        return {}
    bindings: dict[str, str] = {}
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Attribute):
            continue
        target = node.targets[0]
        if not isinstance(target.value, ast.Name) or target.value.id != "self":
            continue
        if not isinstance(node.value, ast.Name):
            continue
        bindings[target.attr] = f"param:{node.value.id}"
    return bindings


def collect_return_relation(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> str:
    relations = {
        extract_relation_from_expression(
            node.value,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        for node in iter_ordered_statement_nodes(function_node, root=function_node)
        if isinstance(node, ast.Return)
    }
    relations.discard("")
    if len(relations) == 1:
        return next(iter(relations))
    return ""


def collect_function_relation_bindings(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    class_relations: dict[str, str],
    *,
    initial_bindings: dict[str, str] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, str]:
    bindings: dict[str, str] = dict(initial_bindings or {})
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            relation = extract_relation_from_expression(
                node.value,
                bindings,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if not relation:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bindings[target.id] = relation
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            relation = (
                extract_relation_from_annotation(node.annotation, class_relations)
                or extract_relation_from_expression(
                    node.value,
                    bindings,
                    class_relations,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
            )
            if relation:
                bindings[node.target.id] = relation
    return bindings


def collect_local_source_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    helper_templates: dict[str, dict[str, Any]],
    *,
    initial_assignments: dict[str, list[dict[str, str]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    assignments: dict[str, list[dict[str, str]]] = {
        name: [dict(source) for source in source_fields]
        for name, source_fields in (initial_assignments or {}).items()
    }
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            source_fields = extract_source_fields_from_expression(
                node.value,
                variable_relations,
                assignments,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if not source_fields:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = [dict(source) for source in source_fields]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            source_fields = extract_source_fields_from_expression(
                node.value,
                variable_relations,
                assignments,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if source_fields:
                assignments[node.target.id] = [dict(source) for source in source_fields]
    return assignments


def collect_local_object_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]],
    object_templates: dict[str, dict[str, Any]],
    *,
    initial_assignments: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    assignments: dict[str, dict[str, Any]] = dict(initial_assignments or {})
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            instance_state = build_object_instance_state(
                node.value,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates,
                object_templates,
                assignments,
            )
            if not instance_state:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = instance_state
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            instance_state = build_object_instance_state(
                node.value,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates,
                object_templates,
                assignments,
            )
            if instance_state:
                assignments[node.target.id] = instance_state
    return assignments


def collect_dependency_object_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    assignments: dict[str, dict[str, Any]] = {}
    positional_args = [*function_node.args.posonlyargs, *function_node.args.args]
    positional_defaults = [None] * (len(positional_args) - len(function_node.args.defaults)) + list(function_node.args.defaults)
    for argument, default in zip(positional_args, positional_defaults):
        if argument.arg in {"self", "cls"}:
            continue
        dependency_hint = default or extract_depends_annotation(argument.annotation)
        instance_state = build_dependency_object_instance_state(
            dependency_hint,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        if instance_state:
            assignments[argument.arg] = instance_state
    for argument, default in zip(function_node.args.kwonlyargs, function_node.args.kw_defaults):
        if argument.arg in {"self", "cls"}:
            continue
        dependency_hint = default or extract_depends_annotation(argument.annotation)
        instance_state = build_dependency_object_instance_state(
            dependency_hint,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        if instance_state:
            assignments[argument.arg] = instance_state
    return assignments


def extract_depends_annotation(node: ast.AST | None) -> ast.AST | None:
    if node is None:
        return None
    if isinstance(node, ast.Call) and get_ast_name(node.func).split(".")[-1] == "Depends":
        return node
    if isinstance(node, ast.Subscript):
        base_name = get_ast_name(node.value).split(".")[-1]
        if base_name == "Annotated":
            slice_node = get_subscript_slice(node)
            for candidate in extract_annotation_elements(slice_node):
                dependency = extract_depends_annotation(candidate)
                if dependency is not None:
                    return dependency
        return extract_depends_annotation(get_subscript_slice(node))
    if isinstance(node, ast.Tuple):
        for element in node.elts:
            dependency = extract_depends_annotation(element)
            if dependency is not None:
                return dependency
    return None


def extract_annotation_elements(node: ast.AST | None) -> list[ast.AST]:
    if node is None:
        return []
    if isinstance(node, ast.Tuple):
        return list(node.elts)
    return [node]


def build_dependency_object_instance_state(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    call = unwrap_await(node)
    if not isinstance(call, ast.Call):
        return {}
    if get_ast_name(call.func).split(".")[-1] != "Depends":
        return {}
    dependency_target: ast.AST | None = None
    if call.args:
        dependency_target = call.args[0]
    else:
        dependency_keyword = next((item for item in call.keywords if item.arg == "dependency"), None)
        dependency_target = dependency_keyword.value if dependency_keyword else None
    if dependency_target is None:
        return {}
    dependency_call = ast.Call(func=dependency_target, args=[], keywords=[])
    return instantiate_helper_object_state(
        dependency_call,
        variable_relations=variable_relations,
        class_relations=class_relations,
        variable_sources=variable_sources,
        helper_templates=helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=None,
    )


def collect_local_dict_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    initial_assignments: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, list[dict[str, str]]]]:
    assignments: dict[str, dict[str, list[dict[str, str]]]] = {
        name: deep_copy_field_mappings(field_mappings)
        for name, field_mappings in (initial_assignments or {}).items()
    }
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            field_mappings = extract_response_field_mapping_from_value(
                node.value,
                assignments,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if field_mappings or is_empty_mapping_expression(node.value):
                        assignments[target.id] = deep_copy_field_mappings(field_mappings)
                    continue
                dict_name, field_name = extract_dict_assignment_target(target)
                if not dict_name or not field_name:
                    continue
                source_fields = extract_source_fields_from_expression(
                    node.value,
                    variable_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
                if not source_fields:
                    continue
                assignments.setdefault(dict_name, {})
                assignments[dict_name][field_name] = [dict(source) for source in source_fields]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            field_mappings = extract_response_field_mapping_from_value(
                node.value,
                assignments,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if field_mappings or is_empty_mapping_expression(node.value):
                assignments[node.target.id] = deep_copy_field_mappings(field_mappings)
        elif isinstance(node, ast.Expr):
            call = unwrap_await(node.value)
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "update"
                and isinstance(call.func.value, ast.Name)
            ):
                dict_name = call.func.value.id
                field_mappings: dict[str, list[dict[str, str]]] = {}
                if call.args:
                    merge_field_mappings(
                        field_mappings,
                        extract_response_field_mapping_from_value(
                            call.args[0],
                            assignments,
                            variable_relations,
                            class_relations,
                            variable_sources,
                            helper_templates,
                            helper_relation_templates=helper_relation_templates,
                            object_templates=object_templates,
                            object_instances=object_instances,
                        ),
                    )
                for keyword in call.keywords:
                    if not keyword.arg:
                        continue
                    field_mappings[keyword.arg] = extract_source_fields_from_expression(
                        keyword.value,
                        variable_relations,
                        variable_sources,
                        helper_templates,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    )
                if field_mappings:
                    assignments.setdefault(dict_name, {})
                    merge_field_mappings(assignments[dict_name], field_mappings)
        elif isinstance(node, ast.AugAssign):
            target = node.target
            if (
                isinstance(target, ast.Name)
                and isinstance(node.op, ast.BitOr)
            ):
                field_mappings = extract_response_field_mapping_from_value(
                    node.value,
                    assignments,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
                if field_mappings:
                    assignments.setdefault(target.id, {})
                    merge_field_mappings(assignments[target.id], field_mappings)
    return assignments


def collect_return_field_mappings(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]],
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    fields: dict[str, list[dict[str, str]]] = {}
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Return):
            merge_field_mappings(
                fields,
                extract_response_field_mapping_from_value(
                    node.value,
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                ),
            )
    return fields


def collect_passthrough_payload_parameters(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    parameters: list[str],
) -> list[str]:
    passthrough: list[str] = []
    parameter_set = set(parameters)
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if not isinstance(node, ast.Return):
            continue
        value = unwrap_await(node.value)
        if isinstance(value, ast.Name) and value.id in parameter_set and value.id not in passthrough:
            passthrough.append(value.id)
    return passthrough


def extract_response_field_mapping_from_value(
    node: ast.AST | None,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]],
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    node = unwrap_await(node)
    if node is None:
        return {}
    if isinstance(node, ast.Dict):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        for key, value in zip(node.keys, node.values):
            if key is None:
                merge_field_mappings(
                    field_mappings,
                    extract_response_field_mapping_from_value(
                        value,
                        assigned_dicts,
                        variable_relations,
                        class_relations,
                        variable_sources,
                        helper_templates,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    ),
                )
                continue
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                continue
            field_mappings[key.value] = extract_source_fields_from_expression(
                value,
                variable_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
        return field_mappings
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        for element in node.elts:
            merge_field_mappings(
                field_mappings,
                extract_response_field_mapping_from_value(
                    element,
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                ),
            )
        return field_mappings
    if isinstance(node, ast.ListComp):
        comprehension_relations = extend_relations_for_comprehensions(node.generators, variable_relations)
        return extract_response_field_mapping_from_value(
            node.elt,
            assigned_dicts,
            comprehension_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
    if isinstance(node, ast.Name):
        return deep_copy_field_mappings(assigned_dicts.get(node.id, {}))
    if isinstance(node, ast.IfExp):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.body,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.orelse,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        return field_mappings
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.left,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.right,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        return field_mappings
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name) and node.func.id == "dict":
            if node.keywords:
                field_mappings: dict[str, list[dict[str, str]]] = {}
                for keyword in node.keywords:
                    if not keyword.arg:
                        continue
                    field_mappings[keyword.arg] = extract_source_fields_from_expression(
                        keyword.value,
                        variable_relations,
                        variable_sources,
                        helper_templates,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    )
                return field_mappings
            if node.args:
                return extract_response_field_mapping_from_value(
                    node.args[0],
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
        if isinstance(node.func, ast.Attribute) and node.func.attr in {"model_dump", "dict"}:
            return extract_response_field_mapping_from_value(
                node.func.value,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
        helper_mapping = instantiate_helper_field_mapping(
            node,
            variable_relations=variable_relations,
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if helper_mapping:
            return helper_mapping
        object_method_mapping = instantiate_object_method_field_mapping(
            node,
            variable_relations=variable_relations,
            class_relations=class_relations,
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if object_method_mapping:
            return object_method_mapping
        for keyword in node.keywords:
            if keyword.arg in {"content", "body"}:
                return extract_response_field_mapping_from_value(
                    keyword.value,
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
        if node.args:
            return extract_response_field_mapping_from_value(
                node.args[0],
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
    return {}


def extract_source_fields_from_expression(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    visited: set[tuple[str, str]] = set()

    def append_source(relation: str, column: str) -> None:
        if not relation or not column:
            return
        key = (relation, column)
        if key in visited:
            return
        visited.add(key)
        collected.append({"relation": relation, "column": column})

    def walk(current: ast.AST | None) -> None:
        current = unwrap_await(current)
        if current is None:
            return
        if isinstance(current, ast.Name):
            for source in variable_sources.get(current.id, []):
                append_source(source.get("relation", ""), source.get("column", ""))
            return
        if isinstance(current, ast.Attribute):
            relation, column = resolve_attribute_source(current, variable_relations)
            if relation and column:
                append_source(relation, column)
                return
            walk(current.value)
            return
        if isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute) and current.func.attr in {"model_dump", "dict"}:
                walk(current.func.value)
                return
            helper_mapping = instantiate_helper_field_mapping(
                current,
                variable_relations=variable_relations,
                variable_sources=variable_sources,
                helper_templates=helper_templates,
                assigned_dicts=assigned_dicts,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if len(helper_mapping) == 1:
                only_sources = next(iter(helper_mapping.values()), [])
                for source in only_sources:
                    append_source(source.get("relation", ""), source.get("column", ""))
                return
            object_method_mapping = instantiate_object_method_field_mapping(
                current,
                variable_relations=variable_relations,
                class_relations={},
                variable_sources=variable_sources,
                helper_templates=helper_templates,
                assigned_dicts=assigned_dicts,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if len(object_method_mapping) == 1:
                only_sources = next(iter(object_method_mapping.values()), [])
                for source in only_sources:
                    append_source(source.get("relation", ""), source.get("column", ""))
                return
            if isinstance(current.func, ast.Attribute):
                walk(current.func.value)
            for argument in current.args:
                walk(argument)
            for keyword in current.keywords:
                walk(keyword.value)
            return
        if isinstance(current, ast.ListComp):
            comprehension_relations = extend_relations_for_comprehensions(current.generators, variable_relations)
            for source in extract_source_fields_from_expression(
                current.elt,
                comprehension_relations,
                variable_sources,
                helper_templates,
                assigned_dicts=assigned_dicts,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ):
                append_source(source.get("relation", ""), source.get("column", ""))
            for generator in current.generators:
                for if_clause in generator.ifs:
                    for source in extract_source_fields_from_expression(
                        if_clause,
                        comprehension_relations,
                        variable_sources,
                        helper_templates,
                        assigned_dicts=assigned_dicts,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    ):
                        append_source(source.get("relation", ""), source.get("column", ""))
            return
        if isinstance(current, ast.BinOp):
            walk(current.left)
            walk(current.right)
            return
        if isinstance(current, ast.UnaryOp):
            walk(current.operand)
            return
        if isinstance(current, ast.BoolOp):
            for value in current.values:
                walk(value)
            return
        if isinstance(current, ast.Compare):
            walk(current.left)
            for comparator in current.comparators:
                walk(comparator)
            return
        if isinstance(current, ast.IfExp):
            walk(current.body)
            walk(current.test)
            walk(current.orelse)
            return
        if isinstance(current, ast.Subscript):
            walk(current.value)
            walk(get_subscript_slice(current))
            return
        if isinstance(current, (ast.Tuple, ast.List, ast.Set)):
            for element in current.elts:
                walk(element)
            return
        if isinstance(current, ast.Dict):
            for value in current.values:
                walk(value)
            return

    walk(node)
    return collected


def instantiate_helper_field_mapping(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    function_name = get_ast_name(node.func)
    if not function_name:
        return {}
    helper = helper_templates.get(function_name)
    if not helper:
        return {}
    parameter_names = helper.get("params", [])
    relation_bindings: dict[str, str] = {}
    source_bindings: dict[str, list[dict[str, str]]] = {}
    payload_bindings: dict[str, dict[str, list[dict[str, str]]]] = {}

    for index, parameter_name in enumerate(parameter_names):
        argument = None
        if index < len(node.args):
            argument = node.args[index]
        else:
            keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            {},
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[parameter_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            source_bindings[parameter_name] = [dict(source) for source in source_fields]
        payload_mapping = extract_response_field_mapping_from_value(
            argument,
            assigned_dicts or {},
            variable_relations,
            {},
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if payload_mapping:
            payload_bindings[parameter_name] = payload_mapping
    resolved = resolve_placeholder_field_mappings(helper.get("return_fields", {}), relation_bindings, source_bindings)
    for parameter_name in helper.get("passthrough_payload_params", []):
        merge_field_mappings(resolved, payload_bindings.get(parameter_name, {}))
    return resolved


def instantiate_helper_object_state(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    function_name = get_ast_name(node.func)
    if not function_name:
        return {}
    helper = helper_templates.get(function_name)
    if not helper:
        return {}
    template_state = helper.get("return_object", {})
    if not template_state:
        return {}
    parameter_names = helper.get("params", [])
    relation_bindings: dict[str, str] = {}
    source_bindings: dict[str, list[dict[str, str]]] = {}
    object_param_bindings: dict[str, dict[str, Any]] = {}
    for index, parameter_name in enumerate(parameter_names):
        argument = None
        if index < len(node.args):
            argument = node.args[index]
        else:
            keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[parameter_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            source_bindings[parameter_name] = [dict(source) for source in source_fields]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            object_param_bindings[parameter_name] = nested_instance
    return resolve_object_state_template(
        template_state,
        relation_bindings,
        source_bindings,
        object_param_bindings,
    )


def resolve_attribute_source(node: ast.Attribute, variable_relations: dict[str, str]) -> tuple[str, str]:
    column_name = node.attr
    base = node.value
    if isinstance(base, ast.Name):
        relation = variable_relations.get(base.id, "")
        return relation, column_name
    if isinstance(base, ast.Attribute):
        relation = extract_relation_from_expression(base, variable_relations, {})
        return relation, column_name
    return "", ""


def resolve_placeholder_field_mappings(
    template_fields: dict[str, list[dict[str, str]]],
    relation_bindings: dict[str, str],
    source_bindings: dict[str, list[dict[str, str]]],
) -> dict[str, list[dict[str, str]]]:
    instantiated: dict[str, list[dict[str, str]]] = {}
    for field_name, source_fields in template_fields.items():
        resolved_sources: list[dict[str, str]] = []
        for source in source_fields:
            relation = source.get("relation", "")
            column = source.get("column", "")
            if relation.startswith("param:"):
                parameter_name = relation.split(":", 1)[1]
                bound_relation = relation_bindings.get(parameter_name, "")
                if bound_relation and column:
                    resolved_sources.append({"relation": bound_relation, "column": column})
                    continue
                for bound_source in source_bindings.get(parameter_name, []):
                    if column and bound_source.get("column") == column:
                        resolved_sources.append(dict(bound_source))
            elif relation and column:
                resolved_sources.append({"relation": relation, "column": column})
        if resolved_sources:
            instantiated[field_name] = dedupe_source_fields(resolved_sources)
    return instantiated


def resolve_placeholder_relations(
    template_relations: dict[str, str],
    relation_bindings: dict[str, str],
) -> dict[str, str]:
    instantiated: dict[str, str] = {}
    for key, relation in template_relations.items():
        if relation.startswith("param:"):
            bound_relation = relation_bindings.get(relation.split(":", 1)[1], "")
            if bound_relation:
                instantiated[key] = bound_relation
            continue
        if relation:
            instantiated[key] = relation
    return instantiated


def resolve_object_state_template(
    template_state: dict[str, Any],
    relation_bindings: dict[str, str],
    source_bindings: dict[str, list[dict[str, str]]],
    object_param_bindings: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not template_state:
        return {}
    if template_state.get("param_object"):
        return deepcopy(object_param_bindings.get(template_state["param_object"], {}))
    resolved = {
        "class_name": template_state.get("class_name", ""),
        "relation_bindings": resolve_placeholder_relations(
            template_state.get("relation_bindings", {}),
            relation_bindings,
        ),
        "source_bindings": resolve_placeholder_field_mappings(
            template_state.get("source_bindings", {}),
            relation_bindings,
            source_bindings,
        ),
        "object_bindings": resolve_placeholder_object_bindings(
            template_state.get("object_bindings", {}),
            relation_bindings,
            source_bindings,
            object_param_bindings,
        ),
    }
    if not resolved["object_bindings"]:
        resolved.pop("object_bindings")
    return resolved


def resolve_placeholder_object_bindings(
    template_bindings: dict[str, dict[str, Any]],
    relation_bindings: dict[str, str],
    source_bindings: dict[str, list[dict[str, str]]],
    object_param_bindings: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    instantiated: dict[str, dict[str, Any]] = {}
    for key, template_state in template_bindings.items():
        resolved = resolve_object_state_template(
            template_state,
            relation_bindings,
            source_bindings,
            object_param_bindings,
        )
        if resolved:
            instantiated[key] = resolved
    return instantiated


def resolve_object_instance_from_expression(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    node = unwrap_await(node)
    if node is None:
        return {}
    if isinstance(node, ast.Name) and object_instances and node.id in object_instances:
        return deepcopy(object_instances[node.id])
    if isinstance(node, ast.Attribute):
        owner_state = resolve_object_instance_from_expression(
            node.value,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if owner_state:
            nested = (owner_state.get("object_bindings", {}) or {}).get(node.attr, {})
            if nested:
                return deepcopy(nested)
        return {}
    if isinstance(node, ast.Call):
        return build_object_instance_state(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates or {},
            object_templates or {},
            object_instances,
        )
    return {}


def build_object_instance_state(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]],
    object_templates: dict[str, dict[str, Any]],
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    node = unwrap_await(node)
    if not isinstance(node, ast.Call):
        return {}
    helper_instance = instantiate_helper_object_state(
        node,
        variable_relations=variable_relations,
        class_relations=class_relations,
        variable_sources=variable_sources,
        helper_templates=helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if helper_instance:
        return helper_instance
    class_name = get_ast_name(node.func)
    if not class_name:
        return {}
    class_template = object_templates.get(class_name)
    if not class_template:
        return {}
    parameter_names = class_template.get("params", [])
    relation_bindings: dict[str, str] = {}
    source_bindings: dict[str, list[dict[str, str]]] = {}
    object_bindings: dict[str, dict[str, Any]] = {}
    for attr_name, placeholder in class_template.get("attributes", {}).items():
        if not placeholder.startswith("param:"):
            continue
        parameter_name = placeholder.split(":", 1)[1]
        argument = None
        if parameter_name in parameter_names:
            parameter_index = parameter_names.index(parameter_name)
            if parameter_index < len(node.args):
                argument = node.args[parameter_index]
        if argument is None:
            keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[attr_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            source_bindings[attr_name] = [dict(source) for source in source_fields]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            object_bindings[attr_name] = nested_instance
        elif (
            isinstance(argument, ast.Name)
            and argument.id not in class_relations
            and argument.id not in variable_relations
            and not source_fields
        ):
            object_bindings[attr_name] = {"param_object": argument.id}
    return {
        "class_name": class_name,
        "relation_bindings": relation_bindings,
        "source_bindings": source_bindings,
        "object_bindings": object_bindings,
    }


def collect_return_object_state(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        instance_state = resolve_object_instance_from_expression(
            node.value,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if instance_state:
            candidates.append(instance_state)
    if len(candidates) != 1:
        return {}
    return deepcopy(candidates[0])


def evaluate_bound_object_method(
    method_template: dict[str, Any],
    call_node: ast.Call,
    instance_state: dict[str, Any],
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
) -> tuple[dict[str, list[dict[str, str]]], str]:
    method_node = method_template.get("body_node")
    if not isinstance(method_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return {}, ""
    method_params = method_template.get("params", [])
    method_relation_bindings: dict[str, str] = {}
    method_source_bindings: dict[str, list[dict[str, str]]] = {}
    method_object_bindings: dict[str, dict[str, Any]] = {}
    method_payload_bindings: dict[str, dict[str, list[dict[str, str]]]] = {}
    for index, parameter_name in enumerate(method_params):
        argument = None
        if index < len(call_node.args):
            argument = call_node.args[index]
        else:
            keyword = next((item for item in call_node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            method_relation_bindings[parameter_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            method_source_bindings[parameter_name] = [dict(source) for source in source_fields]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            method_object_bindings[parameter_name] = nested_instance
        payload_mapping = extract_response_field_mapping_from_value(
            argument,
            assigned_dicts or {},
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if payload_mapping:
            method_payload_bindings[parameter_name] = payload_mapping

    initial_relations = {
        f"self.{attr_name}": relation
        for attr_name, relation in (instance_state.get("relation_bindings", {}) or {}).items()
    }
    initial_relations.update({parameter: f"param:{parameter}" for parameter in method_params})
    initial_relations.update(method_relation_bindings)
    bound_self = deepcopy(instance_state)
    nested_object_instances = {"self": bound_self}
    nested_object_instances.update(method_object_bindings)
    bound_relations = collect_function_relation_bindings(
        method_node,
        class_relations,
        initial_bindings=initial_relations,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=nested_object_instances,
    )
    dependency_instances = collect_dependency_object_assignments(
        method_node,
        bound_relations,
        class_relations,
        method_source_bindings,
        helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
    )
    nested_object_instances.update(dependency_instances)
    local_sources = collect_local_source_assignments(
        method_node,
        bound_relations,
        helper_templates,
        initial_assignments=method_source_bindings,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=nested_object_instances,
    )
    local_objects = collect_local_object_assignments(
        method_node,
        bound_relations,
        class_relations,
        local_sources,
        helper_templates,
        helper_relation_templates or {},
        object_templates or {},
        initial_assignments=nested_object_instances,
    )
    assigned_dicts = collect_local_dict_assignments(
        method_node,
        bound_relations,
        class_relations,
        local_sources,
        helper_templates,
        initial_assignments=method_payload_bindings,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=local_objects,
    )
    return_fields = collect_return_field_mappings(
        method_node,
        assigned_dicts,
        bound_relations,
        class_relations,
        local_sources,
        helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=local_objects,
    )
    for parameter_name in method_template.get("passthrough_payload_params", []):
        merge_field_mappings(return_fields, method_payload_bindings.get(parameter_name, {}))
    return_relation = collect_return_relation(
        method_node,
        bound_relations,
        class_relations,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=local_objects,
    )
    return return_fields, return_relation


def bind_object_method_call_state(
    method_template: dict[str, Any],
    call_node: ast.Call,
    instance_state: dict[str, Any],
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
) -> tuple[dict[str, str], dict[str, list[dict[str, str]]], dict[str, dict[str, Any]]]:
    relation_bindings = dict(instance_state.get("relation_bindings", {}))
    source_bindings = {
        name: [dict(source) for source in source_fields]
        for name, source_fields in (instance_state.get("source_bindings", {}) or {}).items()
    }
    object_bindings = {
        name: deepcopy(state)
        for name, state in (instance_state.get("object_bindings", {}) or {}).items()
    }
    payload_bindings: dict[str, dict[str, list[dict[str, str]]]] = {}
    for index, parameter_name in enumerate(method_template.get("params", [])):
        argument = None
        if index < len(call_node.args):
            argument = call_node.args[index]
        else:
            keyword = next((item for item in call_node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[parameter_name] = relation
        source_fields_for_param = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields_for_param:
            source_bindings[parameter_name] = [dict(source) for source in source_fields_for_param]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            object_bindings[parameter_name] = nested_instance
        payload_mapping = extract_response_field_mapping_from_value(
            argument,
            assigned_dicts or {},
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if payload_mapping:
            payload_bindings[parameter_name] = payload_mapping
    return relation_bindings, source_bindings, object_bindings, payload_bindings


def instantiate_object_method_field_mapping(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str] | None = None,
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    if not isinstance(node.func, ast.Attribute) or not object_templates:
        return {}
    instance_state = resolve_object_instance_from_expression(
        node.func.value,
        variable_relations,
        class_relations or {},
        variable_sources,
        helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if not instance_state:
        return {}
    class_template = object_templates.get(instance_state.get("class_name", ""), {})
    method_template = class_template.get("methods", {}).get(node.func.attr, {})
    if method_template.get("return_fields"):
        relation_bindings, source_bindings, _, payload_bindings = bind_object_method_call_state(
            method_template,
            node,
            instance_state,
            variable_relations=variable_relations,
            class_relations=class_relations or {},
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        resolved = resolve_placeholder_field_mappings(
            method_template.get("return_fields", {}),
            relation_bindings,
            source_bindings,
        )
        for parameter_name in method_template.get("passthrough_payload_params", []):
            merge_field_mappings(resolved, payload_bindings.get(parameter_name, {}))
        runtime_fields, _ = evaluate_bound_object_method(
            method_template,
            node,
            instance_state,
            variable_relations=variable_relations,
            class_relations=class_relations or {},
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
            assigned_dicts=assigned_dicts,
        )
        merge_field_mappings(resolved, runtime_fields)
        return resolved
    return_fields, _ = evaluate_bound_object_method(
        method_template,
        node,
        instance_state,
        variable_relations=variable_relations,
        class_relations=class_relations or {},
        variable_sources=variable_sources,
        helper_templates=helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
        assigned_dicts=assigned_dicts,
    )
    return return_fields


def instantiate_object_method_relation(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str] | None = None,
    variable_sources: dict[str, list[dict[str, str]]] | None = None,
    helper_templates: dict[str, dict[str, Any]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> str:
    if not isinstance(node.func, ast.Attribute) or not object_templates:
        return ""
    instance_state = resolve_object_instance_from_expression(
        node.func.value,
        variable_relations,
        class_relations or {},
        variable_sources or {},
        helper_templates or {},
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if not instance_state:
        return ""
    class_template = object_templates.get(instance_state.get("class_name", ""), {})
    method_template = class_template.get("methods", {}).get(node.func.attr, {})
    return_relation = method_template.get("return_relation", "")
    relation_bindings, _, _, _ = bind_object_method_call_state(
        method_template,
        node,
        instance_state,
        variable_relations=variable_relations,
        class_relations=class_relations or {},
        variable_sources=variable_sources or {},
        helper_templates=helper_templates or {},
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if return_relation.startswith("param:"):
        return relation_bindings.get(return_relation.split(":", 1)[1], "")
    if return_relation:
        return return_relation
    _, resolved_return_relation = evaluate_bound_object_method(
        method_template,
        node,
        instance_state,
        variable_relations=variable_relations,
        class_relations=class_relations or {},
        variable_sources=variable_sources or {},
        helper_templates=helper_templates or {},
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    return resolved_return_relation


def extract_relation_from_annotation(node: ast.AST | None, class_relations: dict[str, str]) -> str:
    if node is None:
        return ""
    if isinstance(node, ast.Subscript):
        return extract_relation_from_annotation(get_subscript_slice(node), class_relations)
    if isinstance(node, ast.Name):
        return class_relations.get(node.id, "")
    if isinstance(node, ast.Attribute):
        return class_relations.get(node.attr, "")
    if isinstance(node, ast.Tuple):
        for element in node.elts:
            relation = extract_relation_from_annotation(element, class_relations)
            if relation:
                return relation
    return ""


def extract_relation_from_expression(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> str:
    node = unwrap_await(node)
    if node is None:
        return ""
    if isinstance(node, ast.Name):
        return variable_relations.get(node.id, class_relations.get(node.id, ""))
    if isinstance(node, ast.Attribute):
        full_name = get_ast_name(node)
        if full_name in variable_relations:
            return variable_relations[full_name]
        if full_name in class_relations:
            return class_relations[full_name]
        if isinstance(node.value, ast.Name) and node.value.id in variable_relations:
            return variable_relations[node.value.id]
        return class_relations.get(node.attr, "")
    if isinstance(node, ast.Call):
        function_name = get_ast_name(node.func)
        if function_name and helper_relation_templates and function_name in helper_relation_templates:
            template = helper_relation_templates.get(function_name, {})
            relation_bindings: dict[str, str] = {}
            for index, parameter_name in enumerate(template.get("params", [])):
                argument = None
                if index < len(node.args):
                    argument = node.args[index]
                else:
                    keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
                    argument = keyword.value if keyword else None
                if argument is None:
                    continue
                relation = extract_relation_from_expression(
                    argument,
                    variable_relations,
                    class_relations,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
                if relation:
                    relation_bindings[parameter_name] = relation
            return_relation = template.get("return_relation", "")
            if return_relation.startswith("param:"):
                return relation_bindings.get(return_relation.split(":", 1)[1], "")
            if return_relation:
                return return_relation
        object_method_relation = instantiate_object_method_relation(
            node,
            variable_relations=variable_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if object_method_relation:
            return object_method_relation
        function_name = get_ast_name(node.func).split(".")[-1]
        if function_name in {"query", "select", "select_from"} and node.args:
            relation = extract_relation_from_expression(
                node.args[0],
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        if function_name == "get" and node.args:
            relation = extract_relation_from_expression(
                node.args[0],
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        if function_name in {
            "all",
            "fetchall",
            "first",
            "one",
            "one_or_none",
            "scalar",
            "scalar_one",
            "scalar_one_or_none",
            "scalars",
            "limit",
            "offset",
            "where",
            "filter",
            "filter_by",
            "order_by",
        } and isinstance(node.func, ast.Attribute):
            relation = extract_relation_from_expression(
                node.func.value,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        relation = extract_relation_from_expression(
            node.func,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            return relation
        for argument in node.args:
            relation = extract_relation_from_expression(
                argument,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        for keyword in node.keywords:
            relation = extract_relation_from_expression(
                keyword.value,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
    return ""


def deep_copy_field_mappings(field_mappings: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, str]]]:
    return {
        field_name: [dict(source) for source in source_fields]
        for field_name, source_fields in field_mappings.items()
    }


def merge_field_mappings(
    target: dict[str, list[dict[str, str]]],
    incoming: dict[str, list[dict[str, str]]],
) -> None:
    for field_name, source_fields in incoming.items():
        existing = target.setdefault(field_name, [])
        seen = {(source["relation"], source["column"]) for source in existing if source.get("relation") and source.get("column")}
        for source in source_fields:
            key = (source.get("relation", ""), source.get("column", ""))
            if not key[0] or not key[1] or key in seen:
                continue
            seen.add(key)
            existing.append(dict(source))


def dedupe_source_fields(source_fields: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for source in source_fields:
        key = (source.get("relation", ""), source.get("column", ""))
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        deduped.append({"relation": key[0], "column": key[1]})
    return deduped


def collect_ui_route_definitions(text: str) -> tuple[dict[str, str], dict[str, str]]:
    variable_routes: dict[str, str] = {}
    function_routes: dict[str, str] = {}
    assignments: dict[str, str] = {}

    for name, expr in JS_ASSIGNMENT_RE.findall(text):
        cleaned = str(expr or "").strip()
        if not cleaned or "=>" in cleaned:
            continue
        assignments[name] = cleaned

    for name, expr in JS_FUNCTION_RETURN_RE.findall(text):
        route = resolve_ui_route_expression(expr, variable_routes, function_routes)
        if route:
            function_routes[name] = route
    for name, block_expr, inline_expr in JS_ARROW_FUNCTION_RETURN_RE.findall(text):
        route = resolve_ui_route_expression(block_expr or inline_expr, variable_routes, function_routes)
        if route:
            function_routes[name] = route

    for _ in range(4):
        changed = False
        for name, expr in assignments.items():
            route = resolve_ui_route_expression(expr, variable_routes, function_routes)
            if route and variable_routes.get(name) != route:
                variable_routes[name] = route
                changed = True
        if not changed:
            break

    return variable_routes, function_routes


def collect_ui_call_routes(
    text: str,
    variable_routes: dict[str, str],
    function_routes: dict[str, str],
) -> list[str]:
    routes: list[str] = []
    seen: set[str] = set()
    for _, expr, _ in iter_ui_call_expressions(text):
        route = resolve_ui_route_expression(expr, variable_routes, function_routes)
        if not route or route in seen:
            continue
        seen.add(route)
        routes.append(route)
    return routes


def iter_ui_call_expressions(text: str) -> list[tuple[str, str, int]]:
    calls: list[tuple[str, str, int]] = []
    for match in UI_CALL_START_RE.finditer(text):
        call_name = match.group(1)
        expr, end_index = parse_js_call_first_argument(text, match.end())
        if not expr:
            continue
        calls.append((call_name, expr, end_index))
    return calls


def parse_js_call_first_argument(text: str, start_index: int) -> tuple[str, int]:
    depth_paren = 0
    depth_brace = 0
    depth_bracket = 0
    quote = ""
    escape = False
    index = start_index
    while index < len(text):
        char = text[index]
        if quote:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = ""
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "(":
            depth_paren += 1
        elif char == ")":
            if depth_paren == 0 and depth_brace == 0 and depth_bracket == 0:
                return text[start_index:index].strip(), index
            if depth_paren > 0:
                depth_paren -= 1
        elif char == "{":
            depth_brace += 1
        elif char == "}":
            if depth_brace > 0:
                depth_brace -= 1
        elif char == "[":
            depth_bracket += 1
        elif char == "]":
            if depth_bracket > 0:
                depth_bracket -= 1
        elif char == "," and depth_paren == 0 and depth_brace == 0 and depth_bracket == 0:
            return text[start_index:index].strip(), index
        index += 1
    return text[start_index:].strip(), len(text)


def resolve_ui_route_expression(
    expression: str,
    variable_routes: dict[str, str],
    function_routes: dict[str, str],
) -> str:
    candidate = str(expression or "").strip()
    if not candidate:
        return ""
    while candidate.startswith("(") and candidate.endswith(")"):
        candidate = candidate[1:-1].strip()
    if candidate.startswith("await "):
        candidate = candidate[6:].strip()

    literal = unwrap_js_string_literal(candidate)
    if literal is not None:
        normalized = normalize_ui_route_candidate(literal)
        if normalized:
            return normalized

    function_call_match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*\([^)]*\)", candidate)
    if function_call_match:
        route = function_routes.get(function_call_match.group(1), "")
        if route:
            return route

    substituted = candidate
    for _ in range(4):
        previous = substituted
        for name, route in function_routes.items():
            substituted = re.sub(
                rf"\b{re.escape(name)}\s*\([^)]*\)",
                route,
                substituted,
            )
        for name, route in variable_routes.items():
            substituted = substituted.replace(f"${{{name}}}", route)
            substituted = re.sub(
                rf"(?<![A-Za-z0-9_$.]){re.escape(name)}(?![A-Za-z0-9_])",
                route,
                substituted,
            )
        if substituted == previous:
            break

    normalized = normalize_ui_route_candidate(substituted)
    if normalized:
        return normalized

    variable_refs = re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", candidate)
    bare_refs = re.findall(r"(?<![A-Za-z0-9_$.])([A-Za-z_][A-Za-z0-9_]*)(?![A-Za-z0-9_])", candidate)
    for name in [*variable_refs, *bare_refs]:
        route = variable_routes.get(name) or function_routes.get(name)
        if route:
            return route
    return ""


def unwrap_js_string_literal(value: str) -> str | None:
    candidate = str(value or "").strip()
    if len(candidate) >= 2 and candidate[0] == candidate[-1] and candidate[0] in {"'", '"', "`"}:
        return candidate[1:-1]
    return None


def normalize_ui_route_candidate(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(/api(?:[^\s\"'`,)]*)?)", text)
    if not match:
        return ""
    route = match.group(1).strip()
    if not route.startswith("/"):
        route = f"/{route.lstrip('/')}"
    return re.sub(r"/{2,}", "/", route)


def extract_ui_route_field_hints(
    text: str,
    variable_routes: dict[str, str] | None = None,
    function_routes: dict[str, str] | None = None,
) -> dict[str, list[str]]:
    variable_routes = variable_routes or {}
    function_routes = function_routes or {}
    route_by_response_var: dict[str, str] = {}
    route_by_data_var: dict[str, str] = {}

    for match in ASYNC_CALL_ASSIGN_RE.finditer(text):
        variable_name = match.group(1)
        route_expr, end_index = parse_js_call_first_argument(text, match.end())
        route = resolve_ui_route_expression(route_expr, variable_routes, function_routes)
        if not route:
            continue
        route_by_response_var[variable_name] = route
        trailing = text[end_index:end_index + 160]
        if ".json(" in trailing:
            route_by_data_var[variable_name] = route

    for match in AXIOS_DATA_CALL_ASSIGN_RE.finditer(text):
        variable_name = match.group(1)
        route_expr, _ = parse_js_call_first_argument(text, match.end())
        route = resolve_ui_route_expression(route_expr, variable_routes, function_routes)
        if route:
            route_by_data_var[variable_name] = route

    for variable_name, response_var in JSON_ASSIGN_RE.findall(text):
        route = route_by_response_var.get(response_var)
        if route:
            route_by_data_var[variable_name] = route
    for variable_name, response_var in RESPONSE_DATA_ASSIGN_RE.findall(text):
        route = route_by_response_var.get(response_var)
        if route:
            route_by_data_var[variable_name] = route

    route_fields: dict[str, set[str]] = defaultdict(set)
    for variable_name, route in route_by_data_var.items():
        route_fields[route].update(extract_fields_for_ui_variable(text, variable_name))

    return {
        route: sorted(field for field in fields if field not in GENERIC_UI_FIELDS)
        for route, fields in route_fields.items()
        if fields
    }


def extract_fields_for_ui_variable(text: str, variable_name: str) -> set[str]:
    fields = set(
        re.findall(rf"\b{re.escape(variable_name)}\??\.([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()", text)
    )
    fields.update(
        re.findall(rf"\b{re.escape(variable_name)}\s*\[\s*[\"']([A-Za-z_][A-Za-z0-9_]*)[\"']\s*\]", text)
    )

    for destructured_fields, source_name in DESTRUCTURE_ASSIGN_RE.findall(text):
        if source_name != variable_name:
            continue
        for entry in destructured_fields.split(","):
            candidate = entry.strip()
            if not candidate:
                continue
            if ":" in candidate:
                candidate = candidate.split(":", 1)[0].strip()
            if candidate.startswith("..."):
                continue
            fields.add(candidate)

    return fields


def get_ast_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = get_ast_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def detect_component_names(relative_path: str, text: str) -> list[str]:
    names = [
        *EXPORTED_COMPONENT_RE.findall(text),
        *FUNCTION_COMPONENT_RE.findall(text),
        *CONST_COMPONENT_RE.findall(text),
    ]
    if names:
        return sorted(dict.fromkeys(names))
    stem = Path(relative_path).stem
    guessed = humanize_asset_name(stem).replace(" ", "")
    return [guessed or "UiContract"]


def dedupe_hint_list(items: list[dict], keys: tuple[str, ...]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[tuple[str, ...]] = set()
    for item in items:
        key = tuple(str(item.get(part, "")) for part in keys)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def build_hint_id(prefix: str, relative_path: str, stable_value: str) -> str:
    path_slug = humanize_asset_name(relative_path).replace(" ", "-").lower()
    value_slug = humanize_asset_name(stable_value).replace(" ", "-").lower()
    return f"{prefix}:{path_slug}:{value_slug}"


def normalize_api_hint_path(path: str) -> str:
    normalized = str(path or "").strip()
    if not normalized:
        normalized = "/"
    if not normalized.startswith("/"):
        normalized = f"/{normalized.lstrip('/')}"
    normalized = re.sub(r"/{2,}", "/", normalized)
    return normalized or "/"


def build_api_route_signature(method: str, path: str) -> str:
    return f"{str(method or '').upper()} {normalize_api_hint_path(path)}"


def build_api_hint_id(relative_path: str, method: str, path: str) -> str:
    normalized_path = normalize_api_hint_path(path)
    readable = slugify_api_hint_readable(f"{str(method or '').lower()} {normalized_path}")
    digest = hashlib.sha1(f"{relative_path}|{str(method or '').upper()}|{normalized_path}".encode("utf-8")).hexdigest()[:8]
    return f"api:{readable}:{digest}"


def slugify_api_hint_readable(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return slug or "route"


def is_test_path(relative_path: str) -> bool:
    normalized = relative_path.replace("\\", "/")
    parts = normalized.split("/")
    filename = parts[-1]
    return "tests" in parts or filename.startswith("test_") or filename.endswith("_test.py")


def is_internal_workbench_path(relative_path: str) -> bool:
    normalized = relative_path.replace("\\", "/")
    return normalized.startswith("src/workbench/") or normalized.startswith("src/data_workbench.egg-info/") or normalized.startswith("static/")


def summarize_asset_entry(
    root_dir: Path,
    entry: dict[str, Any],
    *,
    profiling_mode: str = "metadata_only",
) -> dict:
    profiling_mode = normalize_profiling_mode(profiling_mode)
    path = entry["path"] or ""
    asset = build_asset_descriptor(path, root_dir=root_dir, kind=entry.get("kind"), fmt=entry.get("format"))
    summary = {
        "id": build_profile_asset_id(path, fmt=entry.get("format"), kind=entry.get("kind")),
        "path": path,
        "kind": asset["kind"] if asset else "file",
        "format": asset["format"] if asset else "unknown",
        "profile_status": "schema_only",
        "row_count": None,
        "columns": [],
        "collection_key": entry.get("collection_key", ""),
        "member_count": int(entry.get("member_count", 0) or 0),
        "member_paths_sample": list(entry.get("member_paths_sample", []) or []),
        "group_reason": entry.get("group_reason", ""),
    }
    if asset and asset.get("accessible"):
        skip_reason = should_skip_asset_profiling(asset, profiling_mode=profiling_mode)
        if skip_reason:
            summary["profiling_skipped_reason"] = skip_reason
        else:
            try:
                profile = profile_asset(asset, root_dir)
            except Exception as error:  # noqa: BLE001
                summary["error"] = str(error)
            else:
                summary["profile_status"] = profile["profile_status"]
                summary["row_count"] = profile["row_count"]
                summary["columns"] = [
                    {"name": column["name"], "data_type": column["data_type"]}
                    for column in profile["columns"]
                ]
    elif profiling_mode == "metadata_only":
        summary["profiling_skipped_reason"] = "metadata_only"
    summary["suggested_import"] = build_import_suggestion(summary)
    return summary


def should_skip_asset_profiling(asset: dict[str, Any], *, profiling_mode: str = "metadata_only") -> str | None:
    profiling_mode = normalize_profiling_mode(profiling_mode)
    if profiling_mode != "profile_assets":
        return "metadata_only"
    fmt = str(asset.get("format") or "unknown")
    if fmt in {"parquet", "parquet_collection"} and not project_profile_parquet_profiling_enabled():
        return "parquet_profiling_disabled"

    estimated_bytes = estimate_asset_size_bytes(asset)
    max_bytes = project_profile_max_asset_bytes()
    if estimated_bytes is not None and estimated_bytes > max_bytes:
        return f"asset_too_large:{estimated_bytes}"
    return None


def project_profile_parquet_profiling_enabled() -> bool:
    return (os.environ.get("WORKBENCH_PROJECT_PROFILE_ALLOW_PARQUET") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def project_profile_max_asset_bytes() -> int:
    raw = (os.environ.get("WORKBENCH_PROJECT_PROFILE_MAX_ASSET_BYTES") or "").strip()
    if not raw:
        return DEFAULT_PROJECT_PROFILE_MAX_ASSET_BYTES
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_PROJECT_PROFILE_MAX_ASSET_BYTES


def estimate_asset_size_bytes(asset: dict[str, Any]) -> int | None:
    try:
        if asset.get("kind") in {"glob", "directory"}:
            paths = asset.get("paths", []) or []
            sizes = [path.stat().st_size for path in paths if path.exists()]
            return sum(sizes) if sizes else None
        path = asset.get("path")
        if path and path.exists():
            return path.stat().st_size
    except OSError:
        return None
    return None


def build_import_suggestion(asset_summary: dict) -> dict:
    path = asset_summary["path"]
    label_base = humanize_asset_name(path)
    source_extension_type = "bucket_path" if asset_summary["kind"] == "object_storage" else "disk_path"
    source_origin_kind = "bucket_path" if source_extension_type == "bucket_path" else "disk_path"
    source_provider = "object_storage" if asset_summary["kind"] == "object_storage" else "local"
    return {
        "source_label": f"{label_base} Source",
        "source_extension_type": source_extension_type,
        "source_description": f"Imported from project survey: {path}",
        "source_provider": source_provider,
        "source_refresh": "",
        "source_origin_kind": source_origin_kind,
        "source_origin_value": path,
        "source_series_id": "",
        "raw_asset_label": label_base,
        "raw_asset_kind": asset_summary["kind"],
        "raw_asset_format": asset_summary["format"],
        "raw_asset_value": path,
        "profile_ready": True,
        "data_label": f"{label_base} Raw",
        "data_extension_type": "raw_dataset",
        "data_description": f"Imported from {path}",
        "update_frequency": "",
        "persistence": "cold",
        "persisted": False,
        "schema_columns": [
            {"name": column["name"], "data_type": column["data_type"]}
            for column in asset_summary.get("columns", [])
        ],
    }


def build_profile_asset_id(path: str, *, fmt: str | None = None, kind: str | None = None) -> str:
    payload = json.dumps({"path": path, "format": fmt or "", "kind": kind or ""}, sort_keys=True)
    return f"asset.{uuid.uuid5(uuid.NAMESPACE_URL, payload).hex[:16]}"


def select_project_profile_assets(
    project_profile: dict[str, Any],
    *,
    asset_paths: list[str],
    asset_ids: list[str],
) -> list[dict[str, Any]]:
    assets = list(project_profile.get("data_assets", []) or [])
    if not asset_paths and not asset_ids:
        return []
    by_path = {str(item.get("path", "")): item for item in assets}
    by_id = {str(item.get("id", "")): item for item in assets}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for asset_id in asset_ids:
        item = by_id.get(str(asset_id))
        if item and item.get("id") not in seen:
            ordered.append(item)
            seen.add(str(item.get("id")))
    for asset_path in asset_paths:
        item = by_path.get(str(asset_path))
        if item and item.get("id") not in seen:
            ordered.append(item)
            seen.add(str(item.get("id")))
    return ordered


def profile_asset_entry_isolated(root_dir: Path, entry: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "root_path": str(root_dir),
        "entry": {
            "id": entry.get("id", ""),
            "path": entry.get("path", ""),
            "kind": entry.get("kind"),
            "format": entry.get("format"),
            "collection_key": entry.get("collection_key", ""),
            "member_count": entry.get("member_count", 0),
            "member_paths_sample": entry.get("member_paths_sample", []),
            "group_reason": entry.get("group_reason", ""),
        },
    }
    completed = subprocess.run(
        [sys.executable, "-m", "workbench.project_profile_asset_worker"],
        input=json.dumps(payload),
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "").strip() or "Asset profiling subprocess failed."
        return {
            "id": entry.get("id", build_profile_asset_id(entry.get("path", ""), fmt=entry.get("format"), kind=entry.get("kind"))),
            "path": entry.get("path", ""),
            "kind": entry.get("kind") or "file",
            "format": entry.get("format") or "unknown",
            "profile_status": "schema_only",
            "row_count": None,
            "columns": [],
            "error": message,
            "collection_key": entry.get("collection_key", ""),
            "member_count": int(entry.get("member_count", 0) or 0),
            "member_paths_sample": list(entry.get("member_paths_sample", []) or []),
            "group_reason": entry.get("group_reason", ""),
            "suggested_import": build_import_suggestion(
                {
                    "path": entry.get("path", ""),
                    "kind": entry.get("kind") or "file",
                    "format": entry.get("format") or "unknown",
                    "columns": [],
                }
            ),
        }
    return json.loads(completed.stdout)


def humanize_asset_name(path: str) -> str:
    normalized = path.rstrip("/")
    if "*" in normalized:
        filename = normalized.rsplit("/", 1)[-1]
        filename = filename.replace("*", "").strip("._- ")
        if normalized.endswith("*.parquet"):
            normalized = normalized.rsplit("/", 2)[-2] if "/" in normalized else "parquet collection"
        elif filename:
            if filename.endswith(".csv.gz"):
                normalized = filename[:-7]
            elif filename.endswith(".zip"):
                normalized = filename[:-4]
            else:
                normalized = Path(filename).stem
        else:
            normalized = "collection"
    else:
        filename = normalized.rsplit("/", 1)[-1]
        if filename.endswith(".csv.gz"):
            normalized = filename[:-7]
        elif filename.endswith(".zip"):
            normalized = filename[:-4]
        else:
            normalized = Path(filename).stem
    cleaned = normalized.replace("_", " ").replace("-", " ").strip()
    return cleaned.title() or "Imported Asset"
