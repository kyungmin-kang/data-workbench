from __future__ import annotations

import re
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from .openapi_importer import HTTP_METHODS, extract_response_fields
from .types import normalize_graph


DOC_ROUTE_RE = re.compile(r"\b(GET|POST|PUT|PATCH|DELETE)\s+(/[\w\-/{}/]+)")
DATA_PATH_RE = re.compile(r"([A-Za-z0-9_./-]+\.(?:csv|parquet|json|yaml|yml))")


def combine_partial_graph_candidates(partial_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    combined = normalize_graph({"metadata": {"name": "Hybrid Plan"}, "nodes": [], "edges": []})
    for candidate in partial_candidates:
        partial_graph = normalize_graph(candidate["graph"])
        for node in partial_graph.get("nodes", []):
            merge_partial_graph_node(combined, node)
        existing_edges = {edge["id"] for edge in combined.get("edges", [])}
        for edge in partial_graph.get("edges", []):
            if edge["id"] not in existing_edges:
                combined["edges"].append(deepcopy(edge))
                existing_edges.add(edge["id"])
    return normalize_graph(combined)


def merge_partial_graph_node(graph: dict[str, Any], incoming_node: dict[str, Any]) -> None:
    existing_node = next((node for node in graph.get("nodes", []) if node["id"] == incoming_node["id"]), None)
    if existing_node is None:
        graph.setdefault("nodes", []).append(deepcopy(incoming_node))
        return
    existing_node["tags"] = sorted(set([*existing_node.get("tags", []), *incoming_node.get("tags", [])]))
    for key in ("label", "description", "owner"):
        if not existing_node.get(key) and incoming_node.get(key):
            existing_node[key] = incoming_node[key]
    if existing_node["kind"] == "contract":
        contract = existing_node.setdefault("contract", {})
        incoming_contract = incoming_node.get("contract", {})
        for key in ("route", "component", "ui_role"):
            if not contract.get(key) and incoming_contract.get(key):
                contract[key] = incoming_contract[key]
        field_map = {field.get("name"): field for field in contract.get("fields", [])}
        for field in incoming_contract.get("fields", []):
            current = field_map.get(field.get("name"))
            if current is None:
                contract.setdefault("fields", []).append(deepcopy(field))
                field_map[field.get("name")] = contract["fields"][-1]
                continue
            merge_partial_field_like(current, field)
        return

    if not existing_node.get("columns") and incoming_node.get("columns"):
        existing_node["columns"] = []
    column_map = {column.get("name"): column for column in existing_node.get("columns", [])}
    for column in incoming_node.get("columns", []):
        current = column_map.get(column.get("name"))
        if current is None:
            existing_node.setdefault("columns", []).append(deepcopy(column))
            column_map[column.get("name")] = existing_node["columns"][-1]
            continue
        merge_partial_field_like(current, column)
    if existing_node["kind"] == "compute":
        existing_node.setdefault("compute", {})
        incoming_compute = incoming_node.get("compute", {})
        existing_node["compute"]["inputs"] = sorted(
            set([*existing_node["compute"].get("inputs", []), *incoming_compute.get("inputs", [])])
        )
        existing_node["compute"]["outputs"] = sorted(
            set([*existing_node["compute"].get("outputs", []), *incoming_compute.get("outputs", [])])
        )


def merge_partial_field_like(existing_field: dict[str, Any], incoming_field: dict[str, Any]) -> None:
    for key, value in incoming_field.items():
        if key == "history":
            continue
        if key == "sources":
            if not existing_field.get("sources") and value:
                existing_field["sources"] = deepcopy(value)
            continue
        if key == "lineage_inputs":
            if not existing_field.get("lineage_inputs") and value:
                existing_field["lineage_inputs"] = deepcopy(value)
            continue
        if key == "alternatives":
            if not existing_field.get("alternatives") and value:
                existing_field["alternatives"] = list(value)
            continue
        if key == "evidence":
            existing_field["evidence"] = sorted(set([*existing_field.get("evidence", []), *(value or [])]))
            continue
        if is_blank_structure_value(existing_field.get(key)) and not is_blank_structure_value(value):
            existing_field[key] = deepcopy(value)


def is_blank_structure_value(value: Any) -> bool:
    return value in ("", None, [], {})


def extract_sql_relation_tag(node: dict[str, Any]) -> str:
    for tag in node.get("tags", []) or []:
        if tag.startswith("sql_relation:"):
            return tag.split(":", 1)[1]
    return ""


def collect_document_candidates(root_dir: Path, doc_paths: list[str]) -> list[dict[str, Any]]:
    from .structure_markdown import parse_markdown_hybrid_plan

    root_dir = root_dir.resolve()
    candidates: list[dict[str, Any]] = []
    paths: list[Path] = []
    seen_paths: set[str] = set()

    def add_path(path: Path) -> None:
        resolved = path.resolve()
        normalized = str(resolved)
        if normalized in seen_paths:
            return
        seen_paths.add(normalized)
        paths.append(resolved)

    if doc_paths:
        for entry in doc_paths:
            add_path(resolve_doc_path(root_dir, entry))
    else:
        docs_dir = root_dir / "docs"
        if docs_dir.exists():
            for pattern in ("*.md", "*.yaml", "*.yml", "*.json"):
                for path in sorted(docs_dir.rglob(pattern)):
                    add_path(path)
        latest_plan_path = root_dir / "runtime" / "plans" / "latest.plan.md"
        if latest_plan_path.exists():
            add_path(latest_plan_path)
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        if path.suffix.lower() in {".yaml", ".yml", ".json"}:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if isinstance(payload, dict) and "nodes" in payload:
                candidates.append({"type": "partial_graph", "path": str(path), "graph": payload})
                continue
            if isinstance(payload, dict) and isinstance(payload.get("paths"), dict):
                components = payload.get("components", {})
                for path_name, path_item in (payload.get("paths", {}) or {}).items():
                    if not isinstance(path_item, dict):
                        continue
                    for method_name, operation in path_item.items():
                        if method_name.lower() not in HTTP_METHODS or not isinstance(operation, dict):
                            continue
                        route = f"{method_name.upper()} {path_name}"
                        candidates.append(
                            {
                                "type": "api_route",
                                "path": str(path),
                                "route": route,
                                "label": operation.get("summary") or operation.get("operationId") or route,
                                "fields": extract_response_fields(operation, components),
                                "source": "openapi_doc",
                            }
                        )
            continue
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".md":
            partial_graph = parse_markdown_hybrid_plan(path, text)
            if partial_graph:
                candidates.append({"type": "partial_graph", "path": str(path), "graph": partial_graph})
                continue
        for method, route in DOC_ROUTE_RE.findall(text):
            candidates.append(
                {
                    "type": "api_route",
                    "path": str(path),
                    "route": f"{method.upper()} {route}",
                    "label": f"{method.upper()} {route}",
                    "fields": [],
                }
            )
        for asset_path in DATA_PATH_RE.findall(text):
            if not any(asset_path.endswith(ext) for ext in (".csv", ".parquet", ".json")):
                continue
            candidates.append(
                {
                    "type": "data_asset",
                    "path": str(path),
                    "import_spec": {
                        "source_label": Path(asset_path).stem.replace("_", " ").title(),
                        "source_extension_type": "disk_path",
                        "source_description": f"Imported from {path.relative_to(root_dir)}",
                        "source_provider": "local",
                        "source_refresh": "",
                        "source_origin_kind": "disk_path",
                        "source_origin_value": asset_path,
                        "source_series_id": "",
                        "raw_asset_label": Path(asset_path).name,
                        "raw_asset_kind": "file",
                        "raw_asset_format": detect_asset_format(asset_path),
                        "raw_asset_value": asset_path,
                        "profile_ready": False,
                        "data_label": Path(asset_path).stem.replace("_", " ").title(),
                        "data_extension_type": "raw_dataset",
                        "data_description": f"Imported from {path.relative_to(root_dir)}",
                        "update_frequency": "",
                        "persistence": "cold",
                        "persisted": False,
                        "schema_columns": [],
                    },
                }
            )
    return candidates


def resolve_doc_path(root_dir: Path, entry: str) -> Path:
    candidate = Path(entry).expanduser()
    if candidate.is_absolute():
        return candidate
    return (root_dir / candidate).resolve()


def detect_asset_format(asset_path: str) -> str:
    lowered = asset_path.lower()
    if lowered.endswith(".csv.gz"):
        return "csv_gz"
    if lowered.endswith(".zip"):
        return "zip_csv"
    if lowered.endswith(".parquet"):
        return "parquet"
    if "*" in lowered and lowered.endswith(".parquet"):
        return "parquet_collection"
    if lowered.endswith(".csv"):
        return "csv"
    return "unknown"


__all__ = [
    "DATA_PATH_RE",
    "DOC_ROUTE_RE",
    "collect_document_candidates",
    "combine_partial_graph_candidates",
    "detect_asset_format",
    "extract_sql_relation_tag",
    "is_blank_structure_value",
    "merge_partial_field_like",
    "merge_partial_graph_node",
    "resolve_doc_path",
]
