from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from .profile import profile_graph
from .types import slugify, validate_graph


SOURCE_EXTENSION_TYPES = {"provider", "group", "object", "url", "disk_path", "api_endpoint", "bucket_path"}
DATA_EXTENSION_TYPES = {"raw_dataset", "table", "view", "materialized_view", "feature_set"}
EDGE_TYPES = {"contains", "ingests", "produces", "derives", "serves", "binds", "depends_on"}

KIND_X_POSITIONS = {
    "source": 20.0,
    "data": 330.0,
    "compute": 640.0,
    "contract": 960.0,
}


class ImportSchemaColumn(BaseModel):
    name: str
    data_type: str = "unknown"
    description: str = ""
    nullable: bool = True
    notes: str = ""


class AssetImportSpec(BaseModel):
    source_label: str
    source_extension_type: Literal["provider", "group", "object", "url", "disk_path", "api_endpoint", "bucket_path"] = "object"
    source_description: str = ""
    source_provider: str = ""
    source_refresh: str = ""
    source_origin_kind: str = ""
    source_origin_value: str = ""
    source_series_id: str = ""
    raw_asset_label: str = ""
    raw_asset_kind: Literal["file", "object_storage", "glob", "directory"] = "file"
    raw_asset_format: Literal[
        "csv",
        "csv_collection",
        "csv_gz",
        "csv_gz_collection",
        "parquet",
        "parquet_collection",
        "zip_csv",
        "zip_csv_collection",
        "unknown",
    ] = "unknown"
    raw_asset_value: str
    profile_ready: bool = True
    data_label: str
    data_extension_type: Literal["raw_dataset", "table", "view", "materialized_view", "feature_set"] = "raw_dataset"
    data_description: str = ""
    update_frequency: str = ""
    persistence: str = "cold"
    persisted: bool = False
    schema_columns: list[ImportSchemaColumn] = Field(default_factory=list)


def import_asset_into_graph(graph: dict[str, Any], spec: AssetImportSpec | dict[str, Any], root_dir: Path) -> dict[str, Any]:
    parsed = spec if isinstance(spec, AssetImportSpec) else AssetImportSpec.model_validate(spec)
    updated = deepcopy(graph)
    inserted = _insert_asset_nodes(updated, parsed)
    profiled = profile_graph(updated, root_dir)
    validated = validate_graph(profiled)
    return {
        "graph": validated,
        "imported": inserted,
    }


def import_assets_into_graph(
    graph: dict[str, Any], specs: list[AssetImportSpec | dict[str, Any]], root_dir: Path
) -> dict[str, Any]:
    updated = deepcopy(graph)
    imported: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for spec in specs:
        parsed = spec if isinstance(spec, AssetImportSpec) else AssetImportSpec.model_validate(spec)
        existing = _find_existing_import(updated, parsed)
        if existing:
            skipped.append(
                {
                    "reason": "existing_raw_asset",
                    "raw_asset_value": parsed.raw_asset_value,
                    **existing,
                }
            )
            continue
        imported.append(_insert_asset_nodes(updated, parsed))

    profiled = profile_graph(updated, root_dir)
    validated = validate_graph(profiled)
    return {
        "graph": validated,
        "imported": imported,
        "skipped": skipped,
    }


def _insert_asset_nodes(graph: dict[str, Any], parsed: AssetImportSpec) -> dict[str, Any]:
    next_y = _next_y_position(graph)
    source_id = _unique_node_id(graph, "source", parsed.source_label)
    data_id = _unique_node_id(graph, "data", parsed.data_label)
    edge_id = _unique_edge_id(graph, "ingests", source_id, data_id)
    source_origin_kind = parsed.source_origin_kind or _default_origin_kind(parsed)
    source_origin_value = parsed.source_origin_value or _default_origin_value(parsed, source_origin_kind)

    source_node = {
        "id": source_id,
        "kind": "source",
        "extension_type": parsed.source_extension_type,
        "label": parsed.source_label,
        "description": parsed.source_description,
        "tags": [],
        "owner": "",
        "sensitivity": "internal",
        "status": "active",
        "profile_status": "unknown",
        "notes": "",
        "position": {"x": KIND_X_POSITIONS["source"], "y": next_y},
        "columns": [],
        "source": {
            "provider": parsed.source_provider,
            "origin": {"kind": source_origin_kind, "value": source_origin_value},
            "refresh": parsed.source_refresh or parsed.update_frequency,
            "shared_config": {},
            "series_id": parsed.source_series_id,
            "data_dictionaries": [],
            "raw_assets": [
                {
                    "label": parsed.raw_asset_label or parsed.data_label,
                    "kind": parsed.raw_asset_kind,
                    "format": parsed.raw_asset_format,
                    "value": parsed.raw_asset_value,
                    "profile_ready": parsed.profile_ready,
                }
            ],
        },
        "data": {
            "persistence": "",
            "local_path": "",
            "update_frequency": "",
            "persisted": False,
            "row_count": None,
            "sampled": None,
            "profile_target": "",
        },
        "compute": {
            "runtime": "",
            "inputs": [],
            "outputs": [],
            "notes": "",
            "feature_selection": [],
            "column_mappings": [],
        },
        "contract": {"route": "", "component": "", "fields": []},
    }

    data_node = {
        "id": data_id,
        "kind": "data",
        "extension_type": parsed.data_extension_type,
        "label": parsed.data_label,
        "description": parsed.data_description,
        "tags": [],
        "owner": "",
        "sensitivity": "internal",
        "status": "active",
        "profile_status": "schema_only",
        "notes": "",
        "position": {"x": KIND_X_POSITIONS["data"], "y": next_y},
        "columns": [_build_schema_column(column) for column in parsed.schema_columns],
        "source": {
            "provider": "",
            "origin": {"kind": "", "value": ""},
            "refresh": "",
            "shared_config": {},
            "series_id": "",
            "data_dictionaries": [],
            "raw_assets": [],
        },
        "data": {
            "persistence": parsed.persistence,
            "local_path": "",
            "update_frequency": parsed.update_frequency,
            "persisted": parsed.persisted,
            "row_count": None,
            "sampled": False,
            "profile_target": "",
        },
        "compute": {
            "runtime": "",
            "inputs": [],
            "outputs": [],
            "notes": "",
            "feature_selection": [],
            "column_mappings": [],
        },
        "contract": {"route": "", "component": "", "fields": []},
    }

    graph["nodes"].extend([source_node, data_node])
    graph["edges"].append(
        {
            "id": edge_id,
            "type": "ingests",
            "source": source_id,
            "target": data_id,
            "label": "",
            "column_mappings": [],
            "notes": "",
        }
    )
    return {
        "source_node_id": source_id,
        "data_node_id": data_id,
        "edge_id": edge_id,
    }


def _find_existing_import(graph: dict[str, Any], spec: AssetImportSpec) -> dict[str, Any] | None:
    for node in graph.get("nodes", []):
        if node["kind"] != "source":
            continue
        raw_assets = node.get("source", {}).get("raw_assets", [])
        if any(asset.get("value") == spec.raw_asset_value for asset in raw_assets):
            attached_data_ids = sorted(
                edge["target"]
                for edge in graph.get("edges", [])
                if edge["source"] == node["id"] and edge["type"] == "ingests"
            )
            return {
                "source_node_id": node["id"],
                "data_node_ids": attached_data_ids,
            }
    return None


def _build_schema_column(column: ImportSchemaColumn) -> dict[str, Any]:
    return {
        "name": column.name,
        "data_type": column.data_type,
        "description": column.description,
        "nullable": column.nullable,
        "null_pct": None,
        "stats": {},
        "notes": column.notes,
    }


def _default_origin_kind(spec: AssetImportSpec) -> str:
    if spec.source_extension_type == "api_endpoint":
        return "api_endpoint"
    if spec.source_extension_type == "disk_path":
        return "disk_path"
    if spec.source_extension_type == "bucket_path":
        return "bucket_path"
    if spec.source_extension_type == "url":
        return "url"
    return ""


def _default_origin_value(spec: AssetImportSpec, origin_kind: str) -> str:
    if origin_kind == "disk_path" and spec.raw_asset_kind in {"file", "directory", "glob"}:
        return spec.raw_asset_value
    return ""


def _next_y_position(graph: dict[str, Any]) -> float:
    if not graph.get("nodes"):
        return 40.0
    return max(node["position"]["y"] for node in graph["nodes"]) + 140.0


def _unique_node_id(graph: dict[str, Any], kind: str, label: str) -> str:
    base = f"{kind}:{slugify(label) or kind}"
    existing = {node["id"] for node in graph.get("nodes", [])}
    if base not in existing:
        return base
    counter = 2
    while f"{base}-{counter}" in existing:
        counter += 1
    return f"{base}-{counter}"


def _unique_edge_id(graph: dict[str, Any], edge_type: str, source_id: str, target_id: str) -> str:
    base = f"edge:{edge_type}:{slugify(source_id)}:{slugify(target_id)}"
    existing = {edge["id"] for edge in graph.get("edges", [])}
    if base not in existing:
        return base
    counter = 2
    while f"{base}-{counter}" in existing:
        counter += 1
    return f"{base}-{counter}"
