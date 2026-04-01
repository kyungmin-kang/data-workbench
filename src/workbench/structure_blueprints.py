from __future__ import annotations

from typing import Any

from .types import normalize_graph


def build_source_node_from_import_spec(import_spec: dict[str, Any]) -> dict[str, Any]:
    node_id = f"source:{slugify_text(import_spec.get('source_label', 'source'))}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "source",
                    "extension_type": import_spec.get("source_extension_type", "disk_path"),
                    "label": import_spec.get("source_label", "Source"),
                    "description": import_spec.get("source_description", ""),
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": ["imported_asset"],
                    "columns": [],
                    "source": {
                        "provider": import_spec.get("source_provider", "local"),
                        "origin": {
                            "kind": import_spec.get("source_origin_kind", "disk_path"),
                            "value": import_spec.get("source_origin_value", ""),
                        },
                        "refresh": import_spec.get("source_refresh", ""),
                        "shared_config": {},
                        "series_id": import_spec.get("source_series_id", ""),
                        "data_dictionaries": [],
                        "raw_assets": [
                            {
                                "label": import_spec.get("raw_asset_label", "Raw Asset"),
                                "kind": import_spec.get("raw_asset_kind", "file"),
                                "format": import_spec.get("raw_asset_format", "unknown"),
                                "value": import_spec.get("raw_asset_value", ""),
                                "profile_ready": import_spec.get("profile_ready", True),
                            }
                        ],
                    },
                    "data": {},
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_data_node_from_import_spec(import_spec: dict[str, Any]) -> dict[str, Any]:
    node_id = f"data:{slugify_text(import_spec.get('data_label', 'data'))}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "data",
                    "extension_type": import_spec.get("data_extension_type", "raw_dataset"),
                    "label": import_spec.get("data_label", "Data"),
                    "description": import_spec.get("data_description", ""),
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": ["imported_asset"],
                    "columns": [
                        {
                            "name": column.get("name", ""),
                            "data_type": column.get("data_type", "unknown"),
                            "required": column.get("required", False),
                        }
                        for column in import_spec.get("schema_columns", [])
                        if column.get("name")
                    ],
                    "source": {},
                    "data": {
                        "persistence": import_spec.get("persistence", "cold"),
                        "local_path": import_spec.get("raw_asset_value", ""),
                        "update_frequency": import_spec.get("update_frequency", ""),
                        "persisted": import_spec.get("persisted", False),
                        "profile_target": import_spec.get("raw_asset_value", ""),
                    },
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_sql_data_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    relation = hint.get("relation") or hint.get("label") or "sql_relation"
    object_type = hint.get("object_type", "table")
    data_extension_type = {
        "table": "table",
        "view": "view",
        "materialized_view": "materialized_view",
    }.get(object_type, "table")
    persistence = "serving" if object_type in {"view", "materialized_view"} else "hot"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"data:{slugify_text(relation)}",
                    "kind": "data",
                    "extension_type": data_extension_type,
                    "label": humanize_sql_relation_name(relation),
                    "description": hint.get("description", ""),
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"sql_relation:{relation}", f"sql_object:{object_type}"],
                    "columns": [
                        {
                            "name": field.get("name", ""),
                            "data_type": field.get("data_type", "unknown"),
                        }
                        for field in hint.get("fields", [])
                        if field.get("name")
                    ],
                    "source": {},
                    "data": {
                        "persistence": persistence,
                        "local_path": hint.get("file", ""),
                        "update_frequency": "",
                        "persisted": True,
                        "profile_target": hint.get("file", ""),
                    },
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_orm_query_projection_data_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    relation = hint.get("relation") or "query_projection"
    label = humanize_orm_query_projection_name(hint)
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"data:{slugify_text(relation)}",
                    "kind": "data",
                    "extension_type": "feature_set",
                    "label": label,
                    "description": hint.get("description", "") or f"Observed ORM query result for {relation}.",
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"orm_query_projection:{relation}", "orm_query_result"],
                    "columns": [
                        {
                            "name": field.get("name", ""),
                            "data_type": field.get("data_type", "unknown"),
                        }
                        for field in hint.get("fields", [])
                        if field.get("name")
                    ],
                    "source": {},
                    "data": {
                        "persistence": "transient",
                        "local_path": hint.get("file", ""),
                        "update_frequency": "",
                        "persisted": False,
                        "profile_target": hint.get("file", ""),
                    },
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_sql_compute_node_from_hint(hint: dict[str, Any], *, output_node_id: str) -> dict[str, Any]:
    relation = hint.get("relation") or hint.get("label") or "sql_transform"
    label = f"Build {humanize_sql_relation_name(relation)}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"compute:transform.{slugify_text(relation)}",
                    "kind": "compute",
                    "extension_type": "transform",
                    "label": label,
                    "description": hint.get("description", "") or f"Observed SQL transform for {relation}.",
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"sql_relation:{relation}", "sql_transform"],
                    "columns": [
                        {
                            "name": field.get("name", ""),
                            "data_type": field.get("data_type", "unknown"),
                        }
                        for field in hint.get("fields", [])
                        if field.get("name")
                    ],
                    "source": {},
                    "data": {},
                    "compute": {
                        "runtime": "sql",
                        "inputs": [],
                        "outputs": [output_node_id],
                        "notes": f"Observed from {hint.get('file', '')}",
                        "feature_selection": [],
                        "column_mappings": [],
                    },
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_orm_query_projection_compute_node_from_hint(hint: dict[str, Any], *, output_node_id: str) -> dict[str, Any]:
    relation = hint.get("relation") or "query_projection"
    label = f"Build {humanize_orm_query_projection_name(hint)}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"compute:transform.{slugify_text(relation)}",
                    "kind": "compute",
                    "extension_type": "transform",
                    "label": label,
                    "description": hint.get("description", "") or f"Observed ORM query projection for {relation}.",
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"orm_query_projection:{relation}", "orm_transform"],
                    "columns": [
                        {
                            "name": field.get("name", ""),
                            "data_type": field.get("data_type", "unknown"),
                        }
                        for field in hint.get("fields", [])
                        if field.get("name")
                    ],
                    "source": {},
                    "data": {},
                    "compute": {
                        "runtime": "orm",
                        "inputs": [],
                        "outputs": [output_node_id],
                        "notes": f"Observed from {hint.get('file', '')}",
                        "feature_selection": [],
                        "column_mappings": [],
                    },
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_api_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    route = hint.get("route") or hint.get("label") or "GET /unknown"
    node_id = f"contract:api.{slugify_text(route.replace(' ', '_'))}"
    graph = normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "contract",
                    "extension_type": "api",
                    "label": hint.get("label") or route,
                    "description": hint.get("description", ""),
                    "owner": "repo-scan",
                    "columns": [],
                    "source": {},
                    "data": {},
                    "compute": {},
                    "contract": {
                        "route": route,
                        "component": "",
                        "ui_role": "",
                        "fields": [{"name": field_name} for field_name in hint.get("response_fields", []) or []],
                    },
                }
            ],
            "edges": [],
        }
    )
    return graph["nodes"][0]


def build_ui_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    component = hint.get("component") or hint.get("label") or "UiComponent"
    node_id = f"contract:ui.{slugify_text(component)}"
    graph = normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "contract",
                    "extension_type": "ui",
                    "label": hint.get("label") or component,
                    "description": hint.get("description", ""),
                    "owner": "repo-scan",
                    "columns": [],
                    "source": {},
                    "data": {},
                    "compute": {},
                    "contract": {
                        "route": "",
                        "component": component,
                        "ui_role": "component",
                        "fields": [{"name": field_name} for field_name in hint.get("used_fields", []) or []],
                    },
                }
            ],
            "edges": [],
        }
    )
    return graph["nodes"][0]


def collect_existing_field_ids(graph: dict[str, Any]) -> set[str]:
    field_ids: set[str] = set()
    for node in graph.get("nodes", []):
        for column in node.get("columns", []):
            if column.get("id"):
                field_ids.add(column["id"])
        for field in node.get("contract", {}).get("fields", []):
            if field.get("id"):
                field_ids.add(field["id"])
    return field_ids


def humanize_sql_relation_name(relation: str) -> str:
    return relation.split(".")[-1].replace("_", " ").replace("-", " ").title() or relation


def humanize_orm_query_projection_name(hint: dict[str, Any]) -> str:
    query_context = hint.get("query_context", {}) or {}
    function_name = query_context.get("function", "")
    variable_name = query_context.get("variable", "")
    if function_name or variable_name:
        label = " ".join(part for part in [function_name, variable_name] if part)
        return label.replace("_", " ").replace("-", " ").title()
    relation = hint.get("relation") or hint.get("label") or "Query Projection"
    return relation.split(".")[-1].replace("_", " ").replace("-", " ").title()


def slugify_text(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "_" for character in value).strip("_")


__all__ = [
    "build_api_node_from_hint",
    "build_data_node_from_import_spec",
    "build_orm_query_projection_compute_node_from_hint",
    "build_orm_query_projection_data_node_from_hint",
    "build_source_node_from_import_spec",
    "build_sql_compute_node_from_hint",
    "build_sql_data_node_from_hint",
    "build_ui_node_from_hint",
    "collect_existing_field_ids",
    "humanize_orm_query_projection_name",
    "humanize_sql_relation_name",
    "slugify_text",
]
