from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from .binding_suggestions import format_source_ref, suggest_contract_field_source
from .structure_blueprints import (
    build_orm_query_projection_compute_node_from_hint,
    build_orm_query_projection_data_node_from_hint,
    build_source_node_from_import_spec,
    build_sql_compute_node_from_hint,
    build_sql_data_node_from_hint,
)
from .structure_reconciliation import (
    build_edge,
    match_source_node_id,
    match_orm_query_compute_node_id,
    match_orm_query_data_node_id,
    match_sql_compute_node_id,
    match_sql_data_node_id,
)
from .types import slugify, validate_graph


KIND_X_POSITIONS = {
    "source": 20.0,
    "data": 330.0,
    "compute": 640.0,
    "contract": 960.0,
}


def import_api_hint_into_graph(graph: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    updated = deepcopy(graph)
    imported = upsert_api_contract_from_hint(updated, hint)

    validated = validate_graph(updated)
    return {
        "graph": validated,
        "imported": {
            "node_id": imported["node_id"],
            "created": imported["created"],
            "created_field_names": imported["created_field_names"],
            "binding_summary": imported["binding_summary"],
        },
    }


def import_ui_hint_into_graph(
    graph: dict[str, Any], hint: dict[str, Any], api_hints_by_route: dict[str, dict[str, Any]] | None = None
) -> dict[str, Any]:
    updated = deepcopy(graph)
    component_name = hint.get("component") or hint.get("label") or "UiContract"
    ui_node = find_ui_contract_by_component(updated, component_name)
    created_ui = False
    if ui_node is None:
        ui_node = build_empty_contract_node(
            updated,
            extension_type="ui",
            label=hint.get("label") or component_name,
            description=hint.get("description") or f"Imported from {hint.get('file', 'project survey')}",
        )
        ui_node["contract"]["component"] = component_name
        ui_node["notes"] = f"Imported from code hint: {hint.get('file', 'project survey')}"
        updated["nodes"].append(ui_node)
        created_ui = True

    created_api_node_ids: list[str] = []
    updated_api_node_ids: list[str] = []
    created_edge_ids: list[str] = []
    created_field_names: list[str] = []
    bound_api_node_ids: list[str] = []
    binding_summary = {"applied": [], "unresolved": []}
    route_field_hints = hint.get("route_field_hints", {}) or {}
    fallback_used_fields = hint.get("used_fields", []) or []

    for route_path in hint.get("api_routes", []):
        route = f"GET {route_path}"
        api_hint = (api_hints_by_route or {}).get(route)
        if api_hint is not None:
            imported_api = upsert_api_contract_from_hint(updated, api_hint)
            api_node = find_api_contract_by_route(updated, route)
            if imported_api["created"]:
                created_api_node_ids.append(imported_api["node_id"])
            else:
                updated_api_node_ids.append(imported_api["node_id"])
            binding_summary["applied"].extend(imported_api["binding_summary"]["applied"])
            binding_summary["unresolved"].extend(imported_api["binding_summary"]["unresolved"])
        else:
            api_node = find_api_contract_by_route(updated, route)
            if api_node is None:
                api_node = build_empty_contract_node(
                    updated,
                    extension_type="api",
                    label=route,
                    description=f"Imported from UI code hint in {hint.get('file', 'project survey')}",
                )
                api_node["contract"]["route"] = route
                api_node["notes"] = f"Imported from UI code hint: {hint.get('file', 'project survey')}"
                updated["nodes"].append(api_node)
                created_api_node_ids.append(api_node["id"])
        bound_api_node_ids.append(api_node["id"])

        bind_edge = ensure_bind_edge(updated, api_node["id"], ui_node["id"], hint.get("file", "project survey"))
        if bind_edge["created"]:
            created_edge_ids.append(bind_edge["edge_id"])

        route_used_fields = route_field_hints.get(route_path, []) or fallback_used_fields
        if route_used_fields:
            field_result = seed_ui_fields_from_names(updated, ui_node, route_used_fields)
            created_field_names.extend(field_result["created_field_names"])
            binding_summary["applied"].extend(field_result["binding_summary"]["applied"])
            binding_summary["unresolved"].extend(field_result["binding_summary"]["unresolved"])
        else:
            created_field_names.extend(seed_ui_fields_from_api(ui_node, api_node))

    validated = validate_graph(updated)
    return {
        "graph": validated,
        "imported": {
            "node_id": ui_node["id"],
            "created": created_ui,
            "bound_api_node_ids": sorted(set(bound_api_node_ids)),
            "created_api_node_ids": created_api_node_ids,
            "updated_api_node_ids": sorted(set(updated_api_node_ids)),
            "created_edge_ids": created_edge_ids,
            "created_field_names": sorted(set(created_field_names)),
            "binding_summary": {
                "applied": dedupe_binding_entries(binding_summary["applied"]),
                "unresolved": dedupe_binding_entries(binding_summary["unresolved"]),
            },
        },
    }


def import_sql_hint_into_graph(graph: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    updated = deepcopy(graph)
    imported = upsert_sql_structure_from_hint(updated, hint)
    validated = validate_graph(updated)
    return {
        "graph": validated,
        "imported": imported,
    }


def import_orm_hint_into_graph(graph: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    updated = deepcopy(graph)
    if hint.get("object_type") == "query_projection":
        imported = upsert_orm_structure_from_hint(updated, hint)
    else:
        imported = upsert_sql_structure_from_hint(updated, hint)
    validated = validate_graph(updated)
    return {
        "graph": validated,
        "imported": imported,
    }


def upsert_sql_structure_from_hint(graph: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    relation = hint.get("relation", "")
    if not relation:
        raise ValueError("SQL hint is missing relation.")
    object_type = hint.get("object_type", "table")
    created_node_ids: list[str] = []
    updated_node_ids: list[str] = []
    created_edge_ids: list[str] = []

    data_node = _ensure_sql_relation_node(graph, hint, created_node_ids, updated_node_ids)
    source_node = _ensure_hint_file_source_node(graph, hint)
    node_id = data_node["id"]
    if object_type not in {"view", "materialized_view"}:
        if source_node is not None:
            edge_result = _ensure_graph_edge(graph, "ingests", source_node["id"], data_node["id"], hint.get("file", "project survey"))
            if edge_result["created"]:
                created_edge_ids.append(edge_result["edge_id"])
        for upstream_relation in hint.get("upstream_relations", []) or []:
            upstream_node = _ensure_upstream_sql_relation_node(graph, upstream_relation)
            edge_result = _ensure_graph_edge(graph, "depends_on", upstream_node["id"], data_node["id"], hint.get("file", "project survey"))
            if edge_result["created"]:
                created_edge_ids.append(edge_result["edge_id"])
        return {
            "node_id": node_id,
            "created": node_id in created_node_ids,
            "created_node_ids": sorted(set(created_node_ids)),
            "updated_node_ids": sorted(set(updated_node_ids) - set(created_node_ids)),
            "created_edge_ids": sorted(set(created_edge_ids)),
        }

    compute_node = _ensure_sql_compute_node(graph, hint, data_node["id"], created_node_ids, updated_node_ids)
    node_id = compute_node["id"]
    if source_node is not None:
        edge_result = _ensure_graph_edge(graph, "ingests", source_node["id"], compute_node["id"], hint.get("file", "project survey"))
        if edge_result["created"]:
            created_edge_ids.append(edge_result["edge_id"])
    edge_result = _ensure_graph_edge(graph, "produces", compute_node["id"], data_node["id"], hint.get("file", "project survey"))
    if edge_result["created"]:
        created_edge_ids.append(edge_result["edge_id"])
    compute = compute_node.setdefault("compute", {})
    compute["runtime"] = "sql"
    compute["outputs"] = sorted(dict.fromkeys([*(compute.get("outputs") or []), data_node["id"]]))
    for upstream_relation in hint.get("upstream_relations", []) or []:
        upstream_node = _ensure_upstream_sql_relation_node(graph, upstream_relation)
        compute["inputs"] = sorted(dict.fromkeys([*(compute.get("inputs") or []), upstream_node["id"]]))
        edge_result = _ensure_graph_edge(graph, "depends_on", upstream_node["id"], compute_node["id"], hint.get("file", "project survey"))
        if edge_result["created"]:
            created_edge_ids.append(edge_result["edge_id"])
    return {
        "node_id": node_id,
        "created": node_id in created_node_ids,
        "created_node_ids": sorted(set(created_node_ids)),
        "updated_node_ids": sorted(set(updated_node_ids) - set(created_node_ids)),
        "created_edge_ids": sorted(set(created_edge_ids)),
    }


def upsert_orm_structure_from_hint(graph: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    relation = hint.get("relation", "")
    if not relation:
        raise ValueError("ORM hint is missing relation.")
    fields = hint.get("fields", []) or []
    if not fields:
        raise ValueError("ORM query projection hint is missing fields.")
    created_node_ids: list[str] = []
    updated_node_ids: list[str] = []
    created_edge_ids: list[str] = []

    data_node = _ensure_orm_query_data_node(graph, hint, created_node_ids, updated_node_ids)
    compute_node = _ensure_orm_query_compute_node(graph, hint, data_node["id"], created_node_ids, updated_node_ids)
    source_node = _ensure_hint_file_source_node(graph, hint)
    if source_node is not None:
        edge_result = _ensure_graph_edge(graph, "ingests", source_node["id"], compute_node["id"], hint.get("file", "project survey"))
        if edge_result["created"]:
            created_edge_ids.append(edge_result["edge_id"])
    edge_result = _ensure_graph_edge(graph, "produces", compute_node["id"], data_node["id"], hint.get("file", "project survey"))
    if edge_result["created"]:
        created_edge_ids.append(edge_result["edge_id"])
    compute = compute_node.setdefault("compute", {})
    compute["runtime"] = "orm"
    compute["outputs"] = sorted(dict.fromkeys([*(compute.get("outputs") or []), data_node["id"]]))

    for upstream_relation in hint.get("upstream_relations", []) or []:
        upstream_node = _ensure_upstream_sql_relation_node(graph, upstream_relation)
        compute["inputs"] = sorted(dict.fromkeys([*(compute.get("inputs") or []), upstream_node["id"]]))
        edge_result = _ensure_graph_edge(graph, "depends_on", upstream_node["id"], compute_node["id"], hint.get("file", "project survey"))
        if edge_result["created"]:
            created_edge_ids.append(edge_result["edge_id"])
    return {
        "node_id": compute_node["id"],
        "created": compute_node["id"] in created_node_ids,
        "created_node_ids": sorted(set(created_node_ids)),
        "updated_node_ids": sorted(set(updated_node_ids) - set(created_node_ids)),
        "created_edge_ids": sorted(set(created_edge_ids)),
    }


def upsert_api_contract_from_hint(graph: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    route = hint.get("route", "")
    node = find_api_contract_by_route(graph, route)
    created = False
    if node is None:
        node = build_empty_contract_node(
            graph,
            extension_type="api",
            label=hint.get("label") or route or "API Contract",
            description=hint.get("description") or f"Imported from {hint.get('file', 'project survey')}",
        )
        node["contract"]["route"] = route
        node["notes"] = f"Imported from code hint: {hint.get('file', 'project survey')}"
        graph["nodes"].append(node)
        created = True

    response_fields = hint.get("response_fields", []) or []
    binding_summary = {"applied": [], "unresolved": []}
    created_field_names = merge_api_fields_from_hint(graph, node, response_fields, binding_summary)
    return {
        "node_id": node["id"],
        "created": created,
        "created_field_names": created_field_names,
        "binding_summary": {
            "applied": dedupe_binding_entries(binding_summary["applied"]),
            "unresolved": dedupe_binding_entries(binding_summary["unresolved"]),
        },
    }


def merge_api_fields_from_hint(
    graph: dict[str, Any], node: dict[str, Any], field_names: list[str], binding_summary: dict[str, list[dict[str, Any]]]
) -> list[str]:
    created_field_names: list[str] = []
    existing_fields = {field["name"]: field for field in node.get("contract", {}).get("fields", [])}
    if not field_names:
        return created_field_names

    ordered_fields: list[dict[str, Any]] = []
    for field_name in field_names:
        field = deepcopy(existing_fields.get(field_name, {"name": field_name, "sources": []}))
        if field_name not in existing_fields:
            created_field_names.append(field_name)
        if not field.get("sources"):
            suggestion = suggest_contract_field_source(graph, node["id"], field_name)
            if suggestion:
                field["sources"] = [suggestion["source"]]
                binding_summary["applied"].append(
                    {
                        "contract_node_id": node["id"],
                        "field_name": field_name,
                        "source_ref": format_source_ref(suggestion["source"]),
                    }
                )
            else:
                binding_summary["unresolved"].append(
                    {
                        "contract_node_id": node["id"],
                        "field_name": field_name,
                    }
                )
        ordered_fields.append(field)

    for field_name, field in existing_fields.items():
        if field_name not in field_names:
            ordered_fields.append(deepcopy(field))
    node["contract"]["fields"] = ordered_fields
    return created_field_names


def seed_ui_fields_from_names(
    graph: dict[str, Any], ui_node: dict[str, Any], field_names: list[str]
) -> dict[str, Any]:
    created_field_names: list[str] = []
    binding_summary = {"applied": [], "unresolved": []}
    existing_fields = {field["name"]: field for field in ui_node.get("contract", {}).get("fields", [])}

    for field_name in field_names:
        if not field_name:
            continue
        field = existing_fields.get(field_name)
        if field is None:
            field = {"name": field_name, "sources": []}
            ui_node["contract"]["fields"].append(field)
            existing_fields[field_name] = field
            created_field_names.append(field_name)

        if field.get("sources"):
            continue
        suggestion = suggest_contract_field_source(graph, ui_node["id"], field_name)
        if suggestion:
            field["sources"] = [suggestion["source"]]
            binding_summary["applied"].append(
                {
                    "contract_node_id": ui_node["id"],
                    "field_name": field_name,
                    "source_ref": format_source_ref(suggestion["source"]),
                }
            )
        else:
            binding_summary["unresolved"].append(
                {
                    "contract_node_id": ui_node["id"],
                    "field_name": field_name,
                }
            )

    return {
        "created_field_names": created_field_names,
        "binding_summary": binding_summary,
    }


def build_empty_contract_node(
    graph: dict[str, Any], *, extension_type: str, label: str, description: str
) -> dict[str, Any]:
    kind = "contract"
    return {
        "id": make_unique_node_id(graph, kind, extension_type, label),
        "kind": kind,
        "extension_type": extension_type,
        "label": label,
        "description": description,
        "tags": [],
        "owner": "",
        "sensitivity": "internal",
        "status": "active",
        "profile_status": "schema_only",
        "notes": "",
        "position": get_next_node_position(graph, kind),
        "columns": [],
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
        "contract": {
            "route": "",
            "component": "",
            "fields": [],
        },
    }


def ensure_bind_edge(graph: dict[str, Any], source_id: str, target_id: str, hint_file: str) -> dict[str, Any]:
    existing = next(
        (
            edge
            for edge in graph.get("edges", [])
            if edge["type"] == "binds" and edge["source"] == source_id and edge["target"] == target_id
        ),
        None,
    )
    if existing is not None:
        return {"created": False, "edge_id": existing["id"]}

    edge_id = make_unique_edge_id(graph, "binds", source_id, target_id)
    graph["edges"].append(
        {
            "id": edge_id,
            "type": "binds",
            "source": source_id,
            "target": target_id,
            "label": "",
            "column_mappings": [],
            "notes": f"Imported from code hint: {hint_file}",
        }
    )
    return {"created": True, "edge_id": edge_id}


def _ensure_graph_edge(graph: dict[str, Any], edge_type: str, source_id: str, target_id: str, hint_file: str) -> dict[str, Any]:
    existing = next(
        (
            edge
            for edge in graph.get("edges", [])
            if edge["type"] == edge_type and edge["source"] == source_id and edge["target"] == target_id
        ),
        None,
    )
    if existing is not None:
        return {"created": False, "edge_id": existing["id"]}
    edge = build_edge(edge_type, source_id, target_id)
    edge["notes"] = f"Imported from structure hint: {hint_file}"
    graph.setdefault("edges", []).append(edge)
    return {"created": True, "edge_id": edge["id"]}


def _merge_sql_columns(node: dict[str, Any], fields: list[dict[str, Any]]) -> None:
    existing_by_name = {column.get("name"): column for column in node.get("columns", [])}
    for field_hint in fields or []:
        field_name = field_hint.get("name", "")
        if not field_name:
            continue
        if field_name in existing_by_name:
            column = existing_by_name[field_name]
            if column.get("data_type", "unknown") == "unknown" and field_hint.get("data_type"):
                column["data_type"] = field_hint["data_type"]
            for metadata_key in ("primary_key", "foreign_key", "nullable", "index", "unique"):
                if metadata_key in field_hint and metadata_key not in column:
                    column[metadata_key] = field_hint[metadata_key]
            continue
        column = {
            "name": field_name,
            "data_type": field_hint.get("data_type", "unknown"),
        }
        for metadata_key in ("primary_key", "foreign_key", "nullable", "index", "unique"):
            if metadata_key in field_hint:
                column[metadata_key] = field_hint[metadata_key]
        node.setdefault("columns", []).append(column)
        existing_by_name[field_name] = column


def _ensure_upstream_sql_relation_node(
    graph: dict[str, Any],
    relation: str,
    *,
    column_names: list[str] | None = None,
) -> dict[str, Any]:
    hint = {
        "relation": relation,
        "label": relation,
        "object_type": "table",
        "description": "Imported as upstream SQL relation from project discovery.",
        "fields": [{"name": name, "data_type": "unknown"} for name in (column_names or []) if name],
        "file": "",
    }
    created_node_ids: list[str] = []
    updated_node_ids: list[str] = []
    return _ensure_sql_relation_node(graph, hint, created_node_ids, updated_node_ids)


def _ensure_sql_relation_node(
    graph: dict[str, Any],
    hint: dict[str, Any],
    created_node_ids: list[str],
    updated_node_ids: list[str],
) -> dict[str, Any]:
    relation = hint.get("relation") or hint.get("label") or "sql_relation"
    node_id = match_sql_data_node_id(graph, relation)
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None) if node_id else None
    if node is None:
        node = build_sql_data_node_from_hint(hint)
        node.setdefault("notes", f"Imported from structure hint: {hint.get('file', 'project survey')}")
        graph.setdefault("nodes", []).append(node)
        created_node_ids.append(node["id"])
    else:
        updated_node_ids.append(node["id"])
    _merge_sql_columns(node, hint.get("fields", []) or [])
    return node


def _ensure_hint_file_source_node(graph: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    file_path = str(hint.get("file") or "").strip()
    if not file_path:
        return None
    import_spec = {
        "source_label": f"{Path(file_path).name} Source",
        "source_extension_type": "disk_path",
        "source_description": f"Imported from structure hint: {file_path}",
        "source_provider": "local",
        "source_refresh": "",
        "source_origin_kind": "disk_path",
        "source_origin_value": file_path,
        "source_series_id": "",
        "raw_asset_label": Path(file_path).name,
        "raw_asset_kind": "file",
        "raw_asset_format": "unknown",
        "raw_asset_value": file_path,
        "profile_ready": False,
    }
    node_id = match_source_node_id(graph, import_spec)
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None) if node_id else None
    if node is not None:
        return node
    node = build_source_node_from_import_spec(import_spec)
    node["notes"] = f"Imported from structure hint: {file_path}"
    graph.setdefault("nodes", []).append(node)
    return node


def _ensure_sql_compute_node(
    graph: dict[str, Any],
    hint: dict[str, Any],
    output_node_id: str,
    created_node_ids: list[str],
    updated_node_ids: list[str],
) -> dict[str, Any]:
    relation = hint.get("relation") or hint.get("label") or "sql_transform"
    node_id = match_sql_compute_node_id(graph, relation)
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None) if node_id else None
    if node is None:
        node = build_sql_compute_node_from_hint(hint, output_node_id=output_node_id)
        node.setdefault("notes", f"Imported from structure hint: {hint.get('file', 'project survey')}")
        graph.setdefault("nodes", []).append(node)
        created_node_ids.append(node["id"])
    else:
        updated_node_ids.append(node["id"])
    _merge_sql_columns(node, hint.get("fields", []) or [])
    return node


def _ensure_orm_query_data_node(
    graph: dict[str, Any],
    hint: dict[str, Any],
    created_node_ids: list[str],
    updated_node_ids: list[str],
) -> dict[str, Any]:
    relation = hint.get("relation") or "query_projection"
    node_id = match_orm_query_data_node_id(graph, relation)
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None) if node_id else None
    if node is None:
        node = build_orm_query_projection_data_node_from_hint(hint)
        node.setdefault("notes", f"Imported from structure hint: {hint.get('file', 'project survey')}")
        graph.setdefault("nodes", []).append(node)
        created_node_ids.append(node["id"])
    else:
        updated_node_ids.append(node["id"])
    _merge_sql_columns(node, hint.get("fields", []) or [])
    return node


def _ensure_orm_query_compute_node(
    graph: dict[str, Any],
    hint: dict[str, Any],
    output_node_id: str,
    created_node_ids: list[str],
    updated_node_ids: list[str],
) -> dict[str, Any]:
    relation = hint.get("relation") or "query_projection"
    node_id = match_orm_query_compute_node_id(graph, relation)
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None) if node_id else None
    if node is None:
        node = build_orm_query_projection_compute_node_from_hint(hint, output_node_id=output_node_id)
        node.setdefault("notes", f"Imported from structure hint: {hint.get('file', 'project survey')}")
        graph.setdefault("nodes", []).append(node)
        created_node_ids.append(node["id"])
    else:
        updated_node_ids.append(node["id"])
    _merge_sql_columns(node, hint.get("fields", []) or [])
    return node


def seed_ui_fields_from_api(ui_node: dict[str, Any], api_node: dict[str, Any]) -> list[str]:
    created: list[str] = []
    api_fields = api_node.get("contract", {}).get("fields", [])
    existing_field_names = {field["name"] for field in ui_node.get("contract", {}).get("fields", [])}
    route_slug = slugify(api_node.get("contract", {}).get("route", "")) or slugify(api_node["id"])
    single_route = len(api_fields) > 0

    for api_field in api_fields:
        candidate_name = api_field["name"]
        if candidate_name in existing_field_names:
            _ensure_ui_field_source(ui_node, candidate_name, api_node["id"], api_field["name"])
            continue

        unique_name = candidate_name
        if unique_name in existing_field_names:
            unique_name = f"{route_slug}_{candidate_name}"
        if unique_name in existing_field_names:
            counter = 2
            while f"{unique_name}_{counter}" in existing_field_names:
                counter += 1
            unique_name = f"{unique_name}_{counter}"

        ui_node["contract"]["fields"].append(
            {
                "name": unique_name,
                "sources": [{"node_id": api_node["id"], "field": api_field["name"]}],
            }
        )
        existing_field_names.add(unique_name)
        created.append(unique_name)

    return created


def _ensure_ui_field_source(ui_node: dict[str, Any], field_name: str, api_node_id: str, api_field_name: str) -> None:
    field = next((item for item in ui_node.get("contract", {}).get("fields", []) if item["name"] == field_name), None)
    if field is None:
        return
    sources = field.setdefault("sources", [])
    existing = any(source.get("node_id") == api_node_id and source.get("field") == api_field_name for source in sources)
    if not existing:
        sources.append({"node_id": api_node_id, "field": api_field_name})


def find_api_contract_by_route(graph: dict[str, Any], route: str) -> dict[str, Any] | None:
    return next(
        (
            node
            for node in graph.get("nodes", [])
            if node["kind"] == "contract"
            and node["extension_type"] == "api"
            and node.get("contract", {}).get("route") == route
        ),
        None,
    )


def find_ui_contract_by_component(graph: dict[str, Any], component: str) -> dict[str, Any] | None:
    return next(
        (
            node
            for node in graph.get("nodes", [])
            if node["kind"] == "contract"
            and node["extension_type"] == "ui"
            and node.get("contract", {}).get("component") == component
        ),
        None,
    )


def get_next_node_position(graph: dict[str, Any], kind: str) -> dict[str, float]:
    same_kind_nodes = [node for node in graph.get("nodes", []) if node["kind"] == kind]
    next_y = max((node["position"]["y"] for node in same_kind_nodes), default=0.0) + 120.0
    return {"x": KIND_X_POSITIONS.get(kind, 20.0), "y": next_y or 40.0}


def make_unique_node_id(graph: dict[str, Any], kind: str, extension_type: str, label: str) -> str:
    base_slug = slugify(label) or extension_type or kind
    base = f"{kind}:{extension_type}.{base_slug}" if kind == "contract" else f"{kind}:{base_slug}"
    existing = {node["id"] for node in graph.get("nodes", [])}
    if base not in existing:
        return base
    counter = 2
    while f"{base}-{counter}" in existing:
        counter += 1
    return f"{base}-{counter}"


def make_unique_edge_id(graph: dict[str, Any], edge_type: str, source_id: str, target_id: str) -> str:
    base = f"edge:{edge_type}:{slugify(source_id)}:{slugify(target_id)}"
    existing = {edge["id"] for edge in graph.get("edges", [])}
    if base not in existing:
        return base
    counter = 2
    while f"{base}-{counter}" in existing:
        counter += 1
    return f"{base}-{counter}"


def dedupe_binding_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        key = (
            str(entry.get("contract_node_id", "")),
            str(entry.get("field_name", "")),
            str(entry.get("source_ref", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped
