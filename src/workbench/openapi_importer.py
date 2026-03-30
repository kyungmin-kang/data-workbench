from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from .binding_suggestions import (
    collect_contract_field_candidates,
    format_source_ref,
    suggest_contract_field_source,
)
from .types import slugify, validate_graph


HTTP_METHODS = {"get", "post", "put", "patch", "delete", "options", "head"}
SUCCESS_RESPONSE_CODES = ("200", "201", "202", "203", "204", "default")


class OpenAPIImportSpec(BaseModel):
    spec_text: str = ""
    spec_path: str = ""
    owner: str = ""


def import_openapi_into_graph(graph: dict[str, Any], spec: OpenAPIImportSpec | dict[str, Any], root_dir: Path) -> dict[str, Any]:
    parsed = spec if isinstance(spec, OpenAPIImportSpec) else OpenAPIImportSpec.model_validate(spec)
    document = load_openapi_document(parsed, root_dir)
    updated = deepcopy(graph)
    imported_node_ids: list[str] = []
    updated_node_ids: list[str] = []
    binding_summary = {"applied": [], "unresolved": []}
    next_y = _next_y_position(updated)

    title = document.get("info", {}).get("title", "OpenAPI")
    components = document.get("components", {})
    for path_name, path_item in document.get("paths", {}).items():
        if not isinstance(path_item, dict):
            continue
        for method_name, operation in path_item.items():
            if method_name.lower() not in HTTP_METHODS or not isinstance(operation, dict):
                continue

            route = f"{method_name.upper()} {path_name}"
            label = operation.get("summary") or operation.get("operationId") or route
            field_names = extract_response_fields(operation, components)
            owner = parsed.owner or (operation.get("tags") or [""])[0]
            node = _find_contract_by_route(updated, route)
            if node is None:
                node_id = _unique_contract_id(updated, method_name, path_name, label)
                node = _build_contract_node(node_id, label, route, owner, operation, title, next_y)
                updated["nodes"].append(node)
                imported_node_ids.append(node_id)
                next_y += 120.0
            else:
                node_id = node["id"]
                updated_node_ids.append(node_id)
                node["label"] = label
                node["description"] = operation.get("description") or operation.get("summary") or node.get("description") or f"Imported from {title}"
                node["tags"] = operation.get("tags", [])
                if owner:
                    node["owner"] = owner

            existing_fields = {field["name"]: field for field in node.get("contract", {}).get("fields", [])}
            updated_fields = []
            for field_name in field_names:
                field = deepcopy(existing_fields.get(field_name, {"name": field_name, "sources": []}))
                if not field.get("sources"):
                    suggestion = suggest_contract_field_source(updated, node_id, field_name)
                    if suggestion:
                        field["sources"] = [suggestion["source"]]
                        binding_summary["applied"].append(
                            {
                                "contract_node_id": node_id,
                                "field_name": field_name,
                                "source_ref": format_source_ref(suggestion["source"]),
                            }
                        )
                    elif suggestion is None:
                        binding_summary["unresolved"].append(
                            {
                                "contract_node_id": node_id,
                                "field_name": field_name,
                                "candidates": [candidate["ref"] for candidate in collect_binding_candidates(updated, node_id, field_name)[:5]],
                            }
                        )
                updated_fields.append(field)
            node["contract"]["fields"] = updated_fields

    validated = validate_graph(updated)
    return {
        "graph": validated,
        "imported": {
            "contract_node_ids": imported_node_ids,
            "updated_contract_node_ids": updated_node_ids,
            "title": title,
            "binding_summary": binding_summary,
        },
    }


def load_openapi_document(spec: OpenAPIImportSpec, root_dir: Path) -> dict[str, Any]:
    if spec.spec_text.strip():
        payload = spec.spec_text
    elif spec.spec_path.strip():
        payload = (root_dir / spec.spec_path).read_text(encoding="utf-8")
    else:
        raise ValueError("OpenAPI import requires spec_text or spec_path.")

    try:
        document = json.loads(payload)
    except json.JSONDecodeError:
        document = yaml.safe_load(payload)

    if not isinstance(document, dict):
        raise ValueError("OpenAPI document must parse into an object.")
    if "paths" not in document:
        raise ValueError("OpenAPI document is missing paths.")
    return document


def extract_response_fields(operation: dict[str, Any], components: dict[str, Any]) -> list[str]:
    responses = operation.get("responses", {})
    for status_code in SUCCESS_RESPONSE_CODES:
        response = responses.get(status_code)
        if not isinstance(response, dict):
            continue
        schema = pick_response_schema(response)
        if not schema:
            continue
        resolved = resolve_schema(schema, components)
        property_names = extract_schema_properties(resolved, components)
        if property_names:
            return property_names
    return []


def pick_response_schema(response: dict[str, Any]) -> dict[str, Any] | None:
    content = response.get("content", {})
    for content_type, content_spec in content.items():
        if "json" not in content_type.lower():
            continue
        if not isinstance(content_spec, dict):
            continue
        schema = content_spec.get("schema")
        if isinstance(schema, dict):
            return schema
    return None


def resolve_schema(schema: dict[str, Any], components: dict[str, Any]) -> dict[str, Any]:
    if "$ref" in schema:
        return resolve_reference(schema["$ref"], components)
    if "allOf" in schema:
        merged: dict[str, Any] = {"type": "object", "properties": {}}
        for item in schema["allOf"]:
            resolved = resolve_schema(item, components)
            merged["properties"].update(resolved.get("properties", {}))
        return merged
    if schema.get("type") == "array" and isinstance(schema.get("items"), dict):
        return resolve_schema(schema["items"], components)
    return schema


def resolve_reference(reference: str, components: dict[str, Any]) -> dict[str, Any]:
    if not reference.startswith("#/"):
        return {}
    current: Any = {"components": components}
    for part in reference[2:].split("/"):
        if not isinstance(current, dict):
            return {}
        current = current.get(part)
    return current if isinstance(current, dict) else {}


def extract_schema_properties(schema: dict[str, Any], components: dict[str, Any]) -> list[str]:
    resolved = resolve_schema(schema, components)
    properties = resolved.get("properties", {})
    if not isinstance(properties, dict):
        return []
    return list(properties.keys())


def _next_y_position(graph: dict[str, Any]) -> float:
    contract_nodes = [node for node in graph.get("nodes", []) if node["kind"] == "contract"]
    if not contract_nodes:
        return 40.0
    return max(node["position"]["y"] for node in contract_nodes) + 140.0


def _build_contract_node(
    node_id: str,
    label: str,
    route: str,
    owner: str,
    operation: dict[str, Any],
    title: str,
    y_position: float,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "kind": "contract",
        "extension_type": "api",
        "label": label,
        "description": operation.get("description") or operation.get("summary") or f"Imported from {title}",
        "tags": operation.get("tags", []),
        "owner": owner,
        "sensitivity": "internal",
        "status": "active",
        "profile_status": "schema_only",
        "notes": "",
        "position": {"x": 960.0, "y": y_position},
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
            "route": route,
            "component": "",
            "fields": [],
        },
    }


def _find_contract_by_route(graph: dict[str, Any], route: str) -> dict[str, Any] | None:
    for node in graph.get("nodes", []):
        if node["kind"] == "contract" and node["extension_type"] == "api" and node.get("contract", {}).get("route") == route:
            return node
    return None


def collect_binding_candidates(graph: dict[str, Any], contract_node_id: str, field_name: str) -> list[dict[str, Any]]:
    return collect_contract_field_candidates(graph, contract_node_id, field_name)


def _unique_contract_id(graph: dict[str, Any], method_name: str, path_name: str, label: str) -> str:
    route_slug = slugify(f"{method_name}-{path_name}") or slugify(label) or "api"
    base = f"contract:api.{route_slug}"
    existing = {node["id"] for node in graph.get("nodes", [])}
    if base not in existing:
        return base
    counter = 2
    while f"{base}-{counter}" in existing:
        counter += 1
    return f"{base}-{counter}"
