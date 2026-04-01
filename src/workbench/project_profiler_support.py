from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable

from .store import get_cache_dir

PROJECT_PROFILE_CACHE_VERSION = "3"
DEFAULT_IGNORED_PARTS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "runtime",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".next",
    "node_modules",
    ".yarn",
    ".cache",
    ".idea",
    ".terraform",
    "dist",
    "build",
    "coverage",
    "out",
    "target",
    "tmp",
    "vendor",
    "site-packages",
}


def get_project_profile_exclusion_roots(root_dir: Path) -> list[Path]:
    raw = (os.environ.get("WORKBENCH_PROJECT_PROFILE_EXCLUDE_PATHS") or "").strip()
    if not raw:
        return []
    roots: list[Path] = []
    seen: set[str] = set()
    for entry in raw.replace("\n", ",").split(","):
        candidate = entry.strip()
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        path = (root_dir / path).resolve() if not path.is_absolute() else path.resolve()
        normalized = str(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        roots.append(path)
    return roots


def project_profile_exclusion_signature(root_dir: Path) -> str:
    roots = get_project_profile_exclusion_roots(root_dir)
    if not roots:
        return ""
    payload = json.dumps(sorted(str(path) for path in roots))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_project_profile_cache_token(
    root_dir: Path,
    *,
    include_tests: bool,
    include_internal: bool,
) -> str:
    payload = json.dumps(
        {
            "root": str(root_dir.resolve()),
            "include_tests": include_tests,
            "include_internal": include_internal,
            "exclude_signature": project_profile_exclusion_signature(root_dir),
            "version": PROJECT_PROFILE_CACHE_VERSION,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def load_cached_project_profile(profile_token: str) -> dict[str, Any] | None:
    path = get_project_profile_cache_path(profile_token)
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def save_cached_project_profile(project_profile: dict[str, Any]) -> None:
    cache = project_profile.get("cache", {})
    token = cache.get("token", "")
    if not token:
        return
    path = get_project_profile_cache_path(token)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(project_profile, file, indent=2, ensure_ascii=True)
        file.write("\n")


def get_project_profile_cache_path(profile_token: str) -> Path:
    return get_cache_dir() / "project_profiles" / f"{profile_token}.json"


def with_project_profile_cache_metadata(
    project_profile: dict[str, Any],
    *,
    cached: bool,
    token: str | None = None,
    generated_at: str | None = None,
    include_tests: bool | None = None,
    include_internal: bool | None = None,
) -> dict[str, Any]:
    profile = deepcopy(project_profile)
    existing_cache = profile.get("cache", {}) if isinstance(profile.get("cache", {}), dict) else {}
    profile["cache"] = {
        "token": token or existing_cache.get("token", ""),
        "generated_at": generated_at or existing_cache.get("generated_at", ""),
        "cached": cached,
        "include_tests": include_tests if include_tests is not None else bool(existing_cache.get("include_tests", False)),
        "include_internal": include_internal if include_internal is not None else bool(existing_cache.get("include_internal", True)),
        "exclude_signature": project_profile_exclusion_signature(Path(profile.get("root") or ".")),
        "version": PROJECT_PROFILE_CACHE_VERSION,
    }
    return profile


def profile_cache_matches(
    project_profile: dict[str, Any],
    *,
    root_dir: Path,
    include_tests: bool,
    include_internal: bool,
) -> bool:
    cache = project_profile.get("cache", {})
    if project_profile.get("root") != str(root_dir):
        return False
    return (
        bool(cache.get("include_tests", False)) == include_tests
        and bool(cache.get("include_internal", True)) == include_internal
        and str(cache.get("exclude_signature", "")) == project_profile_exclusion_signature(root_dir)
    )


def is_ignored_project_dir_name(name: str) -> bool:
    return name in DEFAULT_IGNORED_PARTS or name.endswith(".egg-info")


def iter_project_files(
    root_dir: Path,
    *,
    include_tests: bool,
    include_internal: bool,
    is_test_path: Callable[[str], bool],
    is_internal_workbench_path: Callable[[str], bool],
):
    excluded_roots = get_project_profile_exclusion_roots(root_dir)

    def is_excluded(path: Path) -> bool:
        resolved = path.resolve()
        for excluded_root in excluded_roots:
            try:
                resolved.relative_to(excluded_root)
                return True
            except ValueError:
                continue
        return False

    for current_root, dirnames, filenames in os.walk(root_dir):
        current_root_path = Path(current_root)
        if is_excluded(current_root_path):
            dirnames[:] = []
            continue
        dirnames[:] = sorted(
            dirname
            for dirname in dirnames
            if not is_ignored_project_dir_name(dirname)
            and not is_excluded(current_root_path / dirname)
        )
        for filename in sorted(filenames):
            path = current_root_path / filename
            if is_excluded(path):
                continue
            relative = path.relative_to(root_dir).as_posix()
            if not include_tests and is_test_path(relative):
                continue
            if not include_internal and is_internal_workbench_path(relative):
                continue
            yield path


def group_data_assets(root_dir: Path, data_files: list[Path]) -> list[dict]:
    parquet_by_dir: dict[str, list[str]] = defaultdict(list)
    direct_entries: list[dict[str, str | None]] = []

    for path in data_files:
        relative = path.relative_to(root_dir).as_posix()
        if path.suffix.lower() == ".parquet":
            parquet_by_dir[str(path.relative_to(root_dir).parent).replace("\\", "/")].append(relative)
            continue
        direct_entries.append({"path": relative, "kind": None, "format": None})

    grouped_entries: list[dict[str, str | None]] = []
    for parent, files in sorted(parquet_by_dir.items()):
        if len(files) > 1:
            pattern = f"{parent}/*.parquet" if parent != "." else "*.parquet"
            grouped_entries.append({"path": pattern, "kind": "glob", "format": "parquet_collection"})
            continue
        grouped_entries.append({"path": files[0], "kind": None, "format": None})

    return sorted(direct_entries + grouped_entries, key=lambda item: item["path"] or "")


def enrich_sql_orm_hint_evidence(
    sql_hints: list[dict[str, Any]],
    orm_hints: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    supported_object_types = {"table", "view", "materialized_view"}
    sql_by_relation = {
        hint.get("relation", ""): hint
        for hint in sql_hints
        if hint.get("relation") and hint.get("object_type") in supported_object_types
    }
    orm_by_relation = {
        hint.get("relation", ""): hint
        for hint in orm_hints
        if hint.get("relation") and hint.get("object_type") in supported_object_types
    }

    for relation in sorted(set(sql_by_relation) & set(orm_by_relation)):
        sql_hint = sql_by_relation[relation]
        orm_hint = orm_by_relation[relation]
        shared_fields = {
            field.get("name", "")
            for field in sql_hint.get("fields", [])
            if field.get("name")
        } & {
            field.get("name", "")
            for field in orm_hint.get("fields", [])
            if field.get("name")
        }
        if not shared_fields:
            continue
        append_hint_evidence_entry(sql_hint, "schema_match")
        append_hint_evidence_entry(orm_hint, "schema_match")
        for field in sql_hint.get("fields", []):
            if field.get("name", "") in shared_fields:
                append_hint_evidence_entry(field, "schema_match")
        for field in orm_hint.get("fields", []):
            if field.get("name", "") in shared_fields:
                append_hint_evidence_entry(field, "schema_match")

    return sql_hints, orm_hints


def append_hint_evidence_entry(item: dict[str, Any], evidence: str) -> None:
    if not evidence:
        return
    item["evidence"] = sorted(dict.fromkeys([*(item.get("evidence", []) or []), evidence]))
