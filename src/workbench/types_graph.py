from __future__ import annotations

from copy import deepcopy
from typing import Any

from pydantic import ValidationError

from .types_base import GraphValidationError


def slugify(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "-" for character in value).strip("-")


def identifierify(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "_" for character in value).strip("_")


def node_short_name(node_id: str) -> str:
    if ":" in node_id:
        return identifierify(node_id.split(":", 1)[1]) or identifierify(node_id)
    return identifierify(node_id)


def column_ref(node_id: str, column_name: str) -> str:
    return f"{node_id}.{column_name}"


def make_field_id(node_id: str, field_name: str, existing_ids: set[str]) -> str:
    base = f"field.{node_short_name(node_id)}.{identifierify(field_name) or 'field'}"
    candidate = base
    counter = 2
    while candidate in existing_ids:
        candidate = f"{base}.{counter}"
        counter += 1
    existing_ids.add(candidate)
    return candidate


def field_name_for_node(node: dict[str, Any], field_id: str) -> str | None:
    for column in node.get("columns", []):
        if column.get("id") == field_id:
            return column.get("name")
    for field in node.get("contract", {}).get("fields", []):
        if field.get("id") == field_id:
            return field.get("name")
    return None


def deep_sort(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: deep_sort(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [deep_sort(item) for item in sorted(value, key=_sort_key)]
    return value


def _sort_key(value: Any) -> Any:
    if isinstance(value, dict):
        if "id" in value:
            return value["id"]
        if "name" in value:
            return value["name"]
        return repr(sorted(value.items()))
    return repr(value)


def normalize_graph(graph: dict[str, Any] | Any) -> dict[str, Any]:
    from .types import GraphSpec

    model = graph if isinstance(graph, GraphSpec) else GraphSpec.model_validate(graph)
    normalized = model.model_dump(mode="json")
    normalized["nodes"] = sorted(normalized["nodes"], key=lambda item: item["id"])
    normalized["edges"] = sorted(normalized["edges"], key=lambda item: item["id"])
    _ensure_graph_defaults(normalized)
    return normalized


def validate_graph(graph: dict[str, Any] | Any) -> dict[str, Any]:
    try:
        return normalize_graph(graph)
    except ValidationError as error:
        raise GraphValidationError(str(error)) from error
    except ValueError as error:
        raise GraphValidationError(str(error)) from error


def _ensure_graph_defaults(graph: dict[str, Any]) -> None:
    from .types import (
        CONSUMER_CONTRACT_TYPES,
        ComputeConfig,
        ContractConfig,
        DataConfig,
        GraphWorkItemBridge,
        SourceConfig,
        _bridge_role_from_kind,
        _bridge_title_from_text,
    )

    metadata = graph.setdefault("metadata", {})
    metadata.setdefault("name", "Data Workbench Graph")
    metadata.setdefault("description", "")
    metadata.setdefault("updated_at", "")
    metadata.setdefault("updated_by", "user")
    metadata["structure_version"] = int(metadata.get("structure_version") or 1)

    existing_ids: set[str] = set()
    for node in graph.get("nodes", []):
        node.setdefault("state", "confirmed")
        node.setdefault("verification_state", "confirmed" if node["state"] == "confirmed" else "observed")
        node.setdefault("confidence", "high" if node["state"] == "confirmed" else "medium")
        node.setdefault("evidence", [])
        node.setdefault("last_verified_by", "user" if node["state"] == "confirmed" else "")
        node.setdefault("last_verified_at", graph["metadata"].get("updated_at", ""))
        node.setdefault("history", [])
        node.setdefault("removed", False)
        node.setdefault("removed_at", "")
        node.setdefault("removed_by", "")
        node.setdefault("work_items", [])
        node.setdefault("work_status", "todo")
        node.setdefault("position", {"x": 0, "y": 0})
        node.setdefault("columns", [])
        node.setdefault("source", deepcopy(SourceConfig().model_dump(mode="json")))
        node.setdefault("data", deepcopy(DataConfig().model_dump(mode="json")))
        node.setdefault("compute", deepcopy(ComputeConfig().model_dump(mode="json")))
        node.setdefault("contract", deepcopy(ContractConfig().model_dump(mode="json")))
        normalized_work_items: list[dict[str, Any]] = []
        for index, raw_item in enumerate(node.get("work_items", []), start=1):
            item = GraphWorkItemBridge.model_validate(raw_item).model_dump(mode="json")
            item.setdefault("id", f"bridge.{node_short_name(node['id'])}.task_{index}")
            item.setdefault("title", _bridge_title_from_text(item.get("summary", ""), fallback=f"{node['label']} task {index}"))
            item.setdefault("role", _bridge_role_from_kind(item.get("legacy_kind", "")))
            item.setdefault("status", node.get("work_status") or "todo")
            item["linked_refs"] = [ref for ref in item.get("linked_refs", []) if isinstance(ref, str) and ref.strip()]
            if not item["linked_refs"]:
                item["linked_refs"] = [node["id"]]
            item["decision_ids"] = [ref for ref in item.get("decision_ids", []) if isinstance(ref, str) and ref.strip()]
            item["depends_on"] = [ref for ref in item.get("depends_on", []) if isinstance(ref, str) and ref.strip()]
            item["acceptance_check_ids"] = [ref for ref in item.get("acceptance_check_ids", []) if isinstance(ref, str) and ref.strip()]
            item["blocker_ids"] = [ref for ref in item.get("blocker_ids", []) if isinstance(ref, str) and ref.strip()]
            normalized_work_items.append(item)
        node["work_items"] = normalized_work_items

        default_required = node["kind"] == "compute"
        for column in node.get("columns", []):
            _ensure_field_defaults(
                column,
                node_id=node["id"],
                existing_ids=existing_ids,
                default_required=default_required,
                default_state=node["state"],
                default_verified=node["verification_state"],
                default_confidence=node["confidence"],
                default_evidence=node["evidence"],
            )
            column.setdefault("lineage_inputs", [])

        default_field_required = node["kind"] == "contract" and node.get("extension_type") in CONSUMER_CONTRACT_TYPES
        for field in node.get("contract", {}).get("fields", []):
            _ensure_field_defaults(
                field,
                node_id=node["id"],
                existing_ids=existing_ids,
                default_required=default_field_required,
                default_state=node["state"],
                default_verified=node["verification_state"],
                default_confidence=node["confidence"],
                default_evidence=node["evidence"],
            )
            field.setdefault("primary_binding", "")
            field.setdefault("alternatives", [])
            field.setdefault("sources", [])
            field.setdefault("binding_locked", False)

    field_maps = _build_field_maps(graph)
    _normalize_field_refs(graph, field_maps)
    _normalize_edge_mappings(graph, field_maps)
    graph["nodes"] = sorted(graph.get("nodes", []), key=lambda item: item["id"])
    for node in graph["nodes"]:
        node["columns"] = sorted(node.get("columns", []), key=lambda item: item.get("id", item.get("name", "")))
        node["contract"]["fields"] = sorted(
            node.get("contract", {}).get("fields", []),
            key=lambda item: item.get("id", item.get("name", "")),
        )
        node["compute"]["feature_selection"] = sorted(
            node.get("compute", {}).get("feature_selection", []),
            key=lambda item: (
                item.get("order") if item.get("order") is not None else 10_000,
                item.get("field_id", ""),
                item.get("column_ref", ""),
            ),
        )
    graph["edges"] = sorted(graph.get("edges", []), key=lambda item: item["id"])


def _ensure_field_defaults(
    field: dict[str, Any],
    *,
    node_id: str,
    existing_ids: set[str],
    default_required: bool,
    default_state: str,
    default_verified: str,
    default_confidence: str,
    default_evidence: list[str],
) -> None:
    if not field.get("id"):
        field["id"] = make_field_id(node_id, field.get("name", ""), existing_ids)
    existing_ids.add(field["id"])
    if "description" not in field:
        field["description"] = ""
    if field.get("required") is None:
        field["required"] = default_required
    if not field.get("state"):
        field["state"] = default_state
    if not field.get("verification_state"):
        field["verification_state"] = default_verified
    if not field.get("confidence"):
        field["confidence"] = default_confidence
    if not field.get("evidence"):
        field["evidence"] = list(default_evidence)
    if "last_verified_by" not in field:
        field["last_verified_by"] = ""
    if "last_verified_at" not in field:
        field["last_verified_at"] = ""
    if "history" not in field:
        field["history"] = []
    if "removed" not in field:
        field["removed"] = False
    if "removed_at" not in field:
        field["removed_at"] = ""
    if "removed_by" not in field:
        field["removed_by"] = ""
    if "derived_from" not in field:
        field["derived_from"] = []
    if "replaces" not in field:
        field["replaces"] = []


def _build_field_maps(graph: dict[str, Any]) -> dict[str, Any]:
    by_id: dict[str, dict[str, Any]] = {}
    by_ref: dict[str, str] = {}
    owners: dict[str, str] = {}
    for node in graph.get("nodes", []):
        for column in node.get("columns", []):
            by_id[column["id"]] = column
            by_ref[column_ref(node["id"], column["name"])] = column["id"]
            owners[column["id"]] = node["id"]
        for field in node.get("contract", {}).get("fields", []):
            by_id[field["id"]] = field
            by_ref[column_ref(node["id"], field["name"])] = field["id"]
            owners[field["id"]] = node["id"]
    return {"by_id": by_id, "by_ref": by_ref, "owners": owners}


def _normalize_field_refs(graph: dict[str, Any], field_maps: dict[str, Any]) -> None:
    from .types import CONSUMER_CONTRACT_TYPES

    incoming_binding_by_target: dict[str, list[str]] = {}
    for edge in graph.get("edges", []):
        for mapping in edge.get("column_mappings", []):
            source_ref = mapping.get("source_field_id") or (
                column_ref(edge["source"], mapping.get("source_column", "")) if mapping.get("source_column") else ""
            )
            target_ref = mapping.get("target_field_id") or (
                column_ref(edge["target"], mapping.get("target_column", "")) if mapping.get("target_column") else ""
            )
            source_field_id = resolve_field_reference(source_ref, field_maps)
            target_field_id = resolve_field_reference(target_ref, field_maps)
            if source_field_id:
                mapping["source_field_id"] = source_field_id
            if target_field_id:
                mapping["target_field_id"] = target_field_id
            if source_field_id and target_field_id:
                incoming_binding_by_target.setdefault(target_field_id, [])
                if source_field_id not in incoming_binding_by_target[target_field_id]:
                    incoming_binding_by_target[target_field_id].append(source_field_id)

    for node in graph.get("nodes", []):
        for mapping in node.get("compute", {}).get("column_mappings", []):
            mapping["source_field_id"] = resolve_field_reference(mapping.get("source_field_id") or mapping.get("source", ""), field_maps)
            mapping["target_field_id"] = resolve_field_reference(mapping.get("target_field_id") or mapping.get("target", ""), field_maps)

        for feature in node.get("compute", {}).get("feature_selection", []):
            field_id = resolve_field_reference(feature.get("field_id") or feature.get("column_ref", ""), field_maps)
            feature["field_id"] = field_id
            if field_id:
                feature["column_ref"] = display_ref_for_field_id(field_id, graph, field_maps)

        for field in node.get("contract", {}).get("fields", []):
            explicitly_unbound = (
                field.get("binding_locked")
                or (isinstance(field.get("sources"), list) and not field.get("sources") and bool(field.get("primary_binding") or field.get("alternatives")))
            )
            if explicitly_unbound:
                field["binding_locked"] = True
                field["primary_binding"] = ""
                field["alternatives"] = []
            elif field.get("primary_binding") or field.get("alternatives") or field.get("sources"):
                field["binding_locked"] = False
            if not field.get("primary_binding") and field.get("sources"):
                field["primary_binding"] = resolve_field_reference(_legacy_source_to_ref(field["sources"][0]), field_maps)
            if not field.get("alternatives") and len(field.get("sources", [])) > 1:
                field["alternatives"] = [
                    resolve_field_reference(_legacy_source_to_ref(source), field_maps)
                    for source in field.get("sources", [])[1:]
                ]
            field["alternatives"] = [ref for ref in (resolve_field_reference(reference, field_maps) for reference in field.get("alternatives", [])) if ref]
            field["sources"] = [
                source
                for source in [
                    binding_ref_to_legacy_source(reference, graph, field_maps)
                    for reference in [field.get("primary_binding", ""), *field.get("alternatives", [])]
                ]
                if source
            ]
            if not field.get("primary_binding") and not explicitly_unbound:
                suggestion = suggest_binding_reference_for_field(graph, node, field["name"], field_maps)
                if suggestion:
                    field["primary_binding"] = suggestion
                    field["binding_locked"] = False
                    source = binding_ref_to_legacy_source(suggestion, graph, field_maps)
                    field["sources"] = [source] if source else []
            if not field.get("primary_binding") and not explicitly_unbound and field.get("id") in incoming_binding_by_target:
                inferred_sources = incoming_binding_by_target[field["id"]]
                field["primary_binding"] = inferred_sources[0]
                field["binding_locked"] = False
                field["alternatives"] = [ref for ref in inferred_sources[1:] if ref not in field.get("alternatives", [])]
                field["sources"] = [
                    source
                    for source in [
                        binding_ref_to_legacy_source(reference, graph, field_maps)
                        for reference in [field.get("primary_binding", ""), *field.get("alternatives", [])]
                    ]
                    if source
                ]
            if field.get("required") is None:
                field["required"] = node["kind"] == "contract" and node.get("extension_type") in CONSUMER_CONTRACT_TYPES

        for column in node.get("columns", []):
            normalized_inputs: list[dict[str, Any]] = []
            for input_ref in column.get("lineage_inputs", []):
                if isinstance(input_ref, str):
                    field_id = resolve_field_reference(input_ref, field_maps)
                    if field_id:
                        normalized_inputs.append({"field_id": field_id, "role": ""})
                    continue
                field_id = resolve_field_reference(input_ref.get("field_id", ""), field_maps)
                if field_id:
                    normalized_inputs.append({"field_id": field_id, "role": input_ref.get("role", "")})
            if not normalized_inputs and node["kind"] == "compute":
                normalized_inputs = [
                    {"field_id": mapping["source_field_id"], "role": "mapped_input"}
                    for mapping in node.get("compute", {}).get("column_mappings", [])
                    if mapping.get("target_field_id") == column.get("id") and mapping.get("source_field_id")
                ]
            if not normalized_inputs and node["kind"] == "compute":
                normalized_inputs = [
                    {"field_id": feature["field_id"], "role": "feature_input"}
                    for feature in node.get("compute", {}).get("feature_selection", [])
                    if feature.get("field_id")
                ]
            column["lineage_inputs"] = normalized_inputs
            if column.get("required") is None:
                column["required"] = node["kind"] == "compute"


def _normalize_edge_mappings(graph: dict[str, Any], field_maps: dict[str, Any]) -> None:
    for edge in graph.get("edges", []):
        edge.setdefault("state", "confirmed")
        edge.setdefault("confidence", "high")
        edge.setdefault("evidence", [])
        edge.setdefault("removed", False)
        edge.setdefault("history", [])
        for mapping in edge.get("column_mappings", []):
            source_ref = column_ref(edge["source"], mapping.get("source_column", "")) if mapping.get("source_column") else ""
            target_ref = column_ref(edge["target"], mapping.get("target_column", "")) if mapping.get("target_column") else ""
            mapping["source_field_id"] = resolve_field_reference(mapping.get("source_field_id") or source_ref, field_maps)
            mapping["target_field_id"] = resolve_field_reference(mapping.get("target_field_id") or target_ref, field_maps)


def _legacy_source_to_ref(source: dict[str, Any]) -> str:
    if source.get("column"):
        return column_ref(source["node_id"], source["column"])
    if source.get("field"):
        return column_ref(source["node_id"], source["field"])
    return source.get("node_id", "")


def build_index(graph: dict[str, Any]) -> dict[str, Any]:
    _ensure_graph_defaults(graph)
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}
    outgoing: dict[str, list[dict[str, Any]]] = {node_id: [] for node_id in nodes}
    incoming: dict[str, list[dict[str, Any]]] = {node_id: [] for node_id in nodes}
    field_by_id: dict[str, dict[str, Any]] = {}
    field_owner: dict[str, str] = {}
    field_name_by_id: dict[str, str] = {}
    field_ref_to_id: dict[str, str] = {}
    active_field_ids: set[str] = set()
    for edge in graph["edges"]:
        outgoing[edge["source"]].append(edge)
        incoming[edge["target"]].append(edge)
    for node in graph.get("nodes", []):
        for column in node.get("columns", []):
            field_by_id[column["id"]] = column
            field_owner[column["id"]] = node["id"]
            field_name_by_id[column["id"]] = column.get("name", "")
            field_ref_to_id[column_ref(node["id"], column["name"])] = column["id"]
            if not column.get("removed"):
                active_field_ids.add(column["id"])
        for field in node.get("contract", {}).get("fields", []):
            field_by_id[field["id"]] = field
            field_owner[field["id"]] = node["id"]
            field_name_by_id[field["id"]] = field.get("name", "")
            field_ref_to_id[column_ref(node["id"], field["name"])] = field["id"]
            if not field.get("removed"):
                active_field_ids.add(field["id"])
    return {
        "nodes": nodes,
        "edges": edges,
        "outgoing": outgoing,
        "incoming": incoming,
        "field_by_id": field_by_id,
        "field_owner": field_owner,
        "field_name_by_id": field_name_by_id,
        "field_ref_to_id": field_ref_to_id,
        "active_field_ids": active_field_ids,
    }


def suggest_binding_reference_for_field(graph: dict[str, Any], node: dict[str, Any], field_name: str, field_maps_or_index: dict[str, Any]) -> str:
    normalized = identifierify(field_name)
    if not normalized:
        return ""
    for candidate in graph.get("nodes", []):
        if candidate["id"] == node["id"] or candidate.get("removed"):
            continue
        if node["kind"] == "contract" and node.get("extension_type") == "ui":
            if candidate["kind"] != "contract" or candidate.get("extension_type") != "api":
                continue
            for field in candidate.get("contract", {}).get("fields", []):
                if identifierify(field.get("name", "")) == normalized:
                    return field["id"]
            continue
        if node["kind"] == "contract" and node.get("extension_type") == "api":
            if candidate["kind"] not in {"data", "compute"}:
                continue
            for column in candidate.get("columns", []):
                if identifierify(column.get("name", "")) == normalized:
                    return column["id"]
    return ""


def resolve_field_reference(reference: str, field_maps_or_index: dict[str, Any]) -> str:
    if not reference:
        return ""
    if "field_by_id" in field_maps_or_index and reference in field_maps_or_index["field_by_id"]:
        return reference
    if "by_id" in field_maps_or_index and reference in field_maps_or_index["by_id"]:
        return reference
    field_ref_to_id = field_maps_or_index.get("field_ref_to_id") or field_maps_or_index.get("by_ref") or {}
    return field_ref_to_id.get(reference, "")


def display_ref_for_field_id(field_id: str, graph: dict[str, Any], field_maps_or_index: dict[str, Any] | None = None) -> str:
    if not field_id:
        return ""
    if field_maps_or_index and "owners" in field_maps_or_index and "by_id" in field_maps_or_index:
        node_id = field_maps_or_index["owners"].get(field_id, "")
        field_name = field_maps_or_index["by_id"].get(field_id, {}).get("name", "")
    else:
        index = field_maps_or_index if field_maps_or_index and "field_owner" in field_maps_or_index else build_index(graph)
        node_id = index["field_owner"].get(field_id, "")
        field_name = index["field_name_by_id"].get(field_id, "")
    if node_id and field_name:
        return column_ref(node_id, field_name)
    return field_id


def binding_ref_to_legacy_source(reference: str, graph: dict[str, Any], field_maps_or_index: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if not reference:
        return None
    if field_maps_or_index and "owners" in field_maps_or_index and "by_id" in field_maps_or_index:
        field_id = resolve_field_reference(reference, field_maps_or_index)
        node_id = field_maps_or_index["owners"].get(field_id, "")
        field_name = field_maps_or_index["by_id"].get(field_id, {}).get("name", "")
        node = next((item for item in graph.get("nodes", []) if item["id"] == node_id), None)
    else:
        index = field_maps_or_index if field_maps_or_index and "field_owner" in field_maps_or_index else build_index(graph)
        field_id = resolve_field_reference(reference, index)
        node_id = index["field_owner"].get(field_id, "")
        field_name = index["field_name_by_id"].get(field_id, "")
        node = index["nodes"].get(node_id)
    if not field_id or not node_id or not field_name or not node:
        return None
    if node["kind"] == "contract":
        return {"node_id": node_id, "field": field_name}
    return {"node_id": node_id, "column": field_name}


def find_column(node: dict[str, Any], column_name: str) -> dict[str, Any] | None:
    for column in node.get("columns", []):
        if column["name"] == column_name:
            return column
    return None


def find_field_by_id(graph: dict[str, Any], field_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    index = build_index(graph)
    node_id = index["field_owner"].get(field_id)
    if not node_id:
        return None, None
    return index["nodes"].get(node_id), index["field_by_id"].get(field_id)


def active_nodes(graph: dict[str, Any]) -> list[dict[str, Any]]:
    return [node for node in graph.get("nodes", []) if not node.get("removed")]


__all__ = [
    "active_nodes",
    "binding_ref_to_legacy_source",
    "build_index",
    "column_ref",
    "deep_sort",
    "display_ref_for_field_id",
    "field_name_for_node",
    "find_column",
    "find_field_by_id",
    "identifierify",
    "make_field_id",
    "node_short_name",
    "normalize_graph",
    "resolve_field_reference",
    "slugify",
    "suggest_binding_reference_for_field",
    "validate_graph",
]
