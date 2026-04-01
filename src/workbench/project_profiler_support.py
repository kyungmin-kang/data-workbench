from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Callable

from .store import get_cache_dir

PROJECT_PROFILE_CACHE_VERSION = "4"
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

CSV_COLLECTION_TOKEN_RE = re.compile(
    r"(?i)(?:^|[_\-.])(?:part|chunk|shard|segment|batch|partition|slice|page|split|file|dataset)(?:[_\-.]?(?:\d+|[a-z0-9]+))+$"
)
CSV_COLLECTION_DATE_RE = re.compile(
    r"(?i)(?:^|[_\-.])20\d{2}(?:[_\-.]?\d{2}){1,2}(?:$|[_\-.])?"
)
CSV_COLLECTION_COUNTER_RE = re.compile(r"(?i)(?:^|[_\-.])\d{2,}(?:$|[_\-.])?")


def normalize_project_profile_exclude_paths(root_dir: Path, exclude_paths: list[str] | None = None) -> list[Path]:
    raw_entries: list[str] = []
    if exclude_paths:
        raw_entries.extend(exclude_paths)
    else:
        raw = (os.environ.get("WORKBENCH_PROJECT_PROFILE_EXCLUDE_PATHS") or "").strip()
        if raw:
            raw_entries.extend(raw.replace("\n", ",").split(","))

    roots: list[Path] = []
    seen: set[str] = set()
    for entry in raw_entries:
        candidate = str(entry or "").strip()
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


def get_project_profile_exclusion_roots(root_dir: Path, exclude_paths: list[str] | None = None) -> list[Path]:
    return normalize_project_profile_exclude_paths(root_dir, exclude_paths)


def project_profile_exclusion_signature(root_dir: Path, exclude_paths: list[str] | None = None) -> str:
    roots = get_project_profile_exclusion_roots(root_dir, exclude_paths)
    if not roots:
        return ""
    payload = json.dumps(sorted(str(path) for path in roots))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_project_profile_cache_token(
    root_dir: Path,
    *,
    include_tests: bool,
    include_internal: bool,
    profiling_mode: str = "metadata_only",
    exclude_paths: list[str] | None = None,
) -> str:
    payload = json.dumps(
        {
            "root": str(root_dir.resolve()),
            "include_tests": include_tests,
            "include_internal": include_internal,
            "profiling_mode": profiling_mode,
            "exclude_signature": project_profile_exclusion_signature(root_dir, exclude_paths),
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
    profiling_mode: str | None = None,
    exclude_paths: list[str] | None = None,
) -> dict[str, Any]:
    profile = deepcopy(project_profile)
    existing_cache = profile.get("cache", {}) if isinstance(profile.get("cache", {}), dict) else {}
    root_dir = Path(profile.get("root") or ".")
    normalized_excludes = [str(path) for path in normalize_project_profile_exclude_paths(root_dir, exclude_paths)]
    profile["cache"] = {
        "token": token or existing_cache.get("token", ""),
        "generated_at": generated_at or existing_cache.get("generated_at", ""),
        "cached": cached,
        "include_tests": include_tests if include_tests is not None else bool(existing_cache.get("include_tests", False)),
        "include_internal": include_internal if include_internal is not None else bool(existing_cache.get("include_internal", True)),
        "profiling_mode": profiling_mode or str(existing_cache.get("profiling_mode", "metadata_only") or "metadata_only"),
        "exclude_paths": normalized_excludes or list(existing_cache.get("exclude_paths", []) or []),
        "exclude_signature": project_profile_exclusion_signature(root_dir, exclude_paths),
        "version": PROJECT_PROFILE_CACHE_VERSION,
    }
    return profile


def profile_cache_matches(
    project_profile: dict[str, Any],
    *,
    root_dir: Path,
    include_tests: bool,
    include_internal: bool,
    profiling_mode: str = "metadata_only",
    exclude_paths: list[str] | None = None,
) -> bool:
    cache = project_profile.get("cache", {})
    if project_profile.get("root") != str(root_dir):
        return False
    return (
        bool(cache.get("include_tests", False)) == include_tests
        and bool(cache.get("include_internal", True)) == include_internal
        and str(cache.get("profiling_mode", "metadata_only") or "metadata_only") == profiling_mode
        and str(cache.get("exclude_signature", "")) == project_profile_exclusion_signature(root_dir, exclude_paths)
    )


def is_ignored_project_dir_name(name: str) -> bool:
    return name in DEFAULT_IGNORED_PARTS or name.endswith(".egg-info")


def iter_project_files(
    root_dir: Path,
    *,
    include_tests: bool,
    include_internal: bool,
    exclude_paths: list[str] | None = None,
    is_test_path: Callable[[str], bool],
    is_internal_workbench_path: Callable[[str], bool],
):
    excluded_roots = get_project_profile_exclusion_roots(root_dir, exclude_paths)

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
    grouped_collections: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    direct_entries: list[dict[str, Any]] = []

    for path in data_files:
        relative = path.relative_to(root_dir).as_posix()
        fmt, extension = classify_data_asset_path(relative)
        if fmt == "parquet":
            parquet_by_dir[str(path.relative_to(root_dir).parent).replace("\\", "/")].append(relative)
            continue
        collection_key = derive_collection_group_key(relative, fmt)
        if collection_key is not None:
            grouped_collections[collection_key].append(relative)
            continue
        direct_entries.append({"path": relative, "kind": None, "format": fmt, "extension": extension})

    grouped_entries: list[dict[str, Any]] = []
    for parent, files in sorted(parquet_by_dir.items()):
        if len(files) > 1:
            pattern = f"{parent}/*.parquet" if parent != "." else "*.parquet"
            grouped_entries.append(
                {
                    "path": pattern,
                    "kind": "glob",
                    "format": "parquet_collection",
                    "collection_key": f"parquet:{parent}",
                    "member_count": len(files),
                    "member_paths_sample": files[:8],
                    "group_reason": "shared_parent_parquet_directory",
                }
            )
            continue
        grouped_entries.append({"path": files[0], "kind": None, "format": "parquet"})

    for (parent, fmt, normalized_stem), files in sorted(grouped_collections.items()):
        if len(files) < 2:
            grouped_entries.extend(
                {"path": relative, "kind": None, "format": fmt}
                for relative in files
            )
            continue
        extension = data_asset_extension_for_format(fmt)
        pattern_stem = normalized_stem or "*"
        pattern = f"{parent}/{pattern_stem}*{extension}" if parent != "." else f"{pattern_stem}*{extension}"
        grouped_entries.append(
            {
                "path": pattern,
                "kind": "glob",
                "format": f"{fmt}_collection",
                "collection_key": f"{fmt}:{parent}:{normalized_stem}",
                "member_count": len(files),
                "member_paths_sample": files[:8],
                "group_reason": "normalized_partition_stem",
            }
        )

    return sorted(direct_entries + grouped_entries, key=lambda item: item["path"] or "")


def classify_data_asset_path(relative_path: str) -> tuple[str, str]:
    lower_path = relative_path.lower()
    if lower_path.endswith(".csv.gz"):
        return "csv_gz", ".csv.gz"
    if lower_path.endswith(".csv"):
        return "csv", ".csv"
    if lower_path.endswith(".parquet"):
        return "parquet", ".parquet"
    if lower_path.endswith(".zip"):
        return "zip_csv", ".zip"
    return "unknown", Path(relative_path).suffix.lower()


def derive_collection_group_key(relative_path: str, fmt: str) -> tuple[str, str, str] | None:
    if fmt not in {"csv", "csv_gz", "zip_csv"}:
        return None
    path = Path(relative_path)
    normalized_stem = normalize_partition_stem(path.name, data_asset_extension_for_format(fmt))
    if not normalized_stem:
        return None
    parent = str(path.parent).replace("\\", "/")
    parent = parent if parent not in {"", "."} else "."
    return parent, fmt, normalized_stem


def normalize_partition_stem(filename: str, extension: str) -> str:
    lower_name = filename.lower()
    if extension and lower_name.endswith(extension):
        stem = filename[: -len(extension)]
    else:
        stem = Path(filename).stem
    original = stem
    stem = CSV_COLLECTION_DATE_RE.sub("_", stem)
    stem = CSV_COLLECTION_TOKEN_RE.sub("", stem)
    stem = CSV_COLLECTION_COUNTER_RE.sub("", stem)
    stem = re.sub(r"[_\-.]+", "_", stem).strip("_.- ")
    if not stem:
        return ""
    if stem == original and len(re.findall(r"\d{4,}", original)) < 2:
        return ""
    return stem.lower()


def data_asset_extension_for_format(fmt: str) -> str:
    return {
        "csv": ".csv",
        "csv_gz": ".csv.gz",
        "parquet": ".parquet",
        "zip_csv": ".zip",
    }.get(fmt, "")


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
