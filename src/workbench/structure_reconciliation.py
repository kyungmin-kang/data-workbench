from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any, Callable

from .diagnostics import describe_downstream_component_impacts, infer_ui_roles
from .store import utc_timestamp
from .structure_blueprints import collect_existing_field_ids, slugify_text
from .types import (
    PATCH_TYPE_PRECEDENCE,
    build_index,
    column_ref,
    display_ref_for_field_id,
    find_column,
    make_field_id,
)


CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}


def ensure_contract_field(
    graph: dict[str, Any],
    node: dict[str, Any],
    field_name: str,
    *,
    role: str,
    evidence: list[str],
    patches: list[dict[str, Any]],
) -> dict[str, Any]:
    for field in node.get("contract", {}).get("fields", []):
        if field["name"] == field_name:
            return field
    existing_ids = collect_existing_field_ids(graph)
    field = {
        "id": make_field_id(node["id"], field_name, existing_ids),
        "name": field_name,
        "required": True,
        "state": "observed",
        "verification_state": "observed",
        "confidence": role_default_confidence(role),
        "evidence": evidence,
        "primary_binding": "",
        "alternatives": [],
        "sources": [],
        "history": [],
    }
    node.setdefault("contract", {}).setdefault("fields", []).append(field)
    patches.append(
        build_patch(
            "add_field",
            target_id=field["id"],
            node_id=node["id"],
            field_id=field["id"],
            role=role,
            evidence=evidence,
            payload={"field": field},
        )
    )
    return field


def resolve_legacy_source_reference(graph: dict[str, Any], reference: str) -> dict[str, Any] | None:
    if not reference:
        return None
    for node in graph.get("nodes", []):
        for column in node.get("columns", []):
            if column.get("id") == reference or column_ref(node["id"], column.get("name", "")) == reference:
                return {"node_id": node["id"], "column": column.get("name", "")}
        for field in node.get("contract", {}).get("fields", []):
            if field.get("id") == reference or column_ref(node["id"], field.get("name", "")) == reference:
                return {"node_id": node["id"], "field": field.get("name", "")}
    return None


def reconcile_field_binding(
    graph: dict[str, Any],
    node_id: str,
    field_name: str,
    suggested_binding: str,
    *,
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
    canonical_index: dict[str, Any],
    evidence: list[str] | None = None,
) -> None:
    node = next(candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id)
    field = next(candidate for candidate in node.get("contract", {}).get("fields", []) if candidate["name"] == field_name)
    current_binding = field.get("primary_binding", "")
    canonical_field_id = canonical_index["field_ref_to_id"].get(column_ref(node_id, field_name))
    canonical_field = canonical_index["field_by_id"].get(canonical_field_id, {})
    canonical_binding = canonical_field.get("primary_binding", "") if canonical_field else ""
    binding_evidence = evidence or ["schema_match"]

    if not current_binding and not canonical_binding:
        field["primary_binding"] = suggested_binding
        field["sources"] = [resolve_legacy_source_reference(graph, suggested_binding)] if suggested_binding else []
        field["sources"] = [source for source in field["sources"] if source]
        field["state"] = "observed"
        field["confidence"] = role_default_confidence(role)
        field["verification_state"] = "observed"
        patches.append(
            build_patch(
                "add_binding",
                target_id=field["id"],
                node_id=node_id,
                field_id=field["id"],
                role=role,
                evidence=binding_evidence,
                payload={"field_id": field["id"], "primary_binding": suggested_binding, "alternatives": []},
            )
        )
        return

    winner_binding = canonical_binding or current_binding
    if winner_binding and winner_binding != suggested_binding:
        contradiction = build_binding_contradiction(
            graph,
            node_id=node_id,
            field=field,
            winner_binding=winner_binding,
            suggested_binding=suggested_binding,
        )
        contradictions.append(contradiction)
        impacts.extend(
            {
                "target_id": field["id"],
                "message": impact,
                "significant": is_significant_impact(field, canonical_field, impact),
            }
            for impact in contradiction.get("downstream_impacts", [])
        )
        patches.append(
            build_patch(
                "change_binding",
                target_id=field["id"],
                node_id=node_id,
                field_id=field["id"],
                role=role,
                evidence=binding_evidence,
                payload={
                    "field_id": field["id"],
                    "previous_binding": winner_binding,
                    "new_binding": suggested_binding,
                },
            )
        )
        if role == "scout":
            field["confidence"] = lower_confidence(field.get("confidence", "medium"))
        return

    if suggested_binding and current_binding != suggested_binding:
        field["primary_binding"] = suggested_binding
        field["sources"] = [resolve_legacy_source_reference(graph, suggested_binding)] if suggested_binding else []
        field["sources"] = [source for source in field["sources"] if source]


def suggest_explicit_binding_for_contract_field(
    graph: dict[str, Any],
    source_fields: list[dict[str, Any]],
    *,
    role: str,
    patches: list[dict[str, Any]],
    ensure_relation_node: Callable[..., str],
) -> str:
    if len(source_fields) != 1:
        return ""
    source_field = source_fields[0]
    relation = source_field.get("relation", "")
    column_name = source_field.get("column", "")
    if not relation or not column_name:
        return ""
    upstream_node_id = ensure_relation_node(
        graph,
        relation=relation,
        role=role,
        patches=patches,
        column_names=[column_name],
    )
    upstream_node = next(candidate for candidate in graph["nodes"] if candidate["id"] == upstream_node_id)
    source_column = find_column(upstream_node, column_name)
    return source_column["id"] if source_column else ""


def ensure_edge(
    graph: dict[str, Any],
    *,
    edge_type: str,
    source_id: str,
    target_id: str,
    patches: list[dict[str, Any]],
    role: str,
    evidence: list[str],
) -> None:
    node_ids = {node.get("id") for node in graph.get("nodes", [])}
    if source_id not in node_ids or target_id not in node_ids:
        return
    if any(edge["type"] == edge_type and edge["source"] == source_id and edge["target"] == target_id for edge in graph.get("edges", [])):
        return
    edge = build_edge(edge_type, source_id, target_id)
    graph.setdefault("edges", []).append(edge)
    patches.append(
        build_patch(
            "add_edge",
            target_id=edge["id"],
            edge_id=edge["id"],
            role=role,
            evidence=evidence,
            payload={"edge": edge},
        )
    )


def build_edge(edge_type: str, source_id: str, target_id: str) -> dict[str, Any]:
    return {
        "id": f"edge.{edge_type}.{slugify_text(source_id)}.{slugify_text(target_id)}",
        "type": edge_type,
        "source": source_id,
        "target": target_id,
        "label": "",
        "column_mappings": [],
        "notes": "",
        "state": "observed",
        "confidence": "medium",
        "evidence": ["static_analysis"],
        "removed": False,
        "history": [],
    }


def build_patch(
    patch_type: str,
    *,
    target_id: str,
    role: str,
    evidence: list[str],
    confidence: str | None = None,
    node_id: str = "",
    field_id: str = "",
    edge_id: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = payload or {}
    fingerprint = hashlib.sha1(
        json.dumps(
            {
                "type": patch_type,
                "target_id": target_id,
                "node_id": node_id,
                "field_id": field_id,
                "edge_id": edge_id,
                "payload": payload,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:10]
    return {
        "id": f"patch.{patch_type}.{target_id.replace(':', '_').replace('.', '_')}.{fingerprint}",
        "type": patch_type,
        "target_id": target_id,
        "node_id": node_id,
        "field_id": field_id,
        "edge_id": edge_id,
        "confidence": confidence or role_default_confidence(role),
        "evidence": evidence,
        "review_state": "pending",
        "payload": payload,
    }


def build_binding_contradiction(
    graph: dict[str, Any],
    *,
    node_id: str,
    field: dict[str, Any],
    winner_binding: str,
    suggested_binding: str,
) -> dict[str, Any]:
    contradiction_id = f"contradiction.{field['id']}.{hashlib.sha1(f'{winner_binding}|{suggested_binding}'.encode('utf-8')).hexdigest()[:8]}"
    impacts = describe_downstream_component_impacts(build_index(graph), node_id, field["name"], infer_ui_roles(graph))
    existing_display = display_ref_for_field_id(winner_binding, graph)
    suggested_display = display_ref_for_field_id(suggested_binding, graph)
    return {
        "id": contradiction_id,
        "target_id": field["id"],
        "node_id": node_id,
        "field_id": field["id"],
        "kind": "binding_mismatch",
        "severity": "high" if impacts else "medium",
        "existing_belief": {"primary_binding": winner_binding},
        "new_evidence": {"primary_binding": suggested_binding},
        "affected_refs": [field["id"], winner_binding, suggested_binding],
        "confidence_delta": "down",
        "downstream_impacts": impacts,
        "evidence_sources": ["canonical", "implementation"],
        "message": f"{field['name']} maps to {existing_display or winner_binding}, but scan suggests {suggested_display or suggested_binding}.",
        "why_this_matters": impacts[0] if impacts else "Column-level structure no longer matches the latest observed implementation.",
        "review_required": True,
    }


def apply_patch_to_graph(
    graph: dict[str, Any],
    patch: dict[str, Any],
    *,
    merged_by: str,
    strict: bool = False,
) -> tuple[bool, str]:
    patch_type = patch.get("type")
    payload = patch.get("payload", {})
    if patch_type == "add_node":
        node = payload.get("node")
        if not node:
            return False, "Accepted add_node patch is missing node payload."
        if not any(candidate["id"] == node["id"] for candidate in graph.get("nodes", [])):
            graph["nodes"].append(deepcopy(node))
        return True, ""
    if patch_type == "add_edge":
        edge = payload.get("edge")
        if not edge:
            return False, "Accepted add_edge patch is missing edge payload."
        node_ids = {node.get("id") for node in graph.get("nodes", [])}
        if edge.get("source") not in node_ids or edge.get("target") not in node_ids:
            return False, f"Accepted add_edge patch {patch['id']} depends on nodes that are not present in the merge graph."
        if not any(candidate["id"] == edge["id"] for candidate in graph.get("edges", [])):
            graph["edges"].append(deepcopy(edge))
        return True, ""
    if patch_type == "add_field":
        node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == patch.get("node_id")), None)
        field = payload.get("field")
        if not node or not field:
            return False, f"Accepted add_field patch {patch['id']} targets a node or field that is missing from the merge graph."
        target_list = node["contract"]["fields"] if node["kind"] == "contract" else node["columns"]
        if any(candidate["id"] == field["id"] for candidate in target_list):
            return True, ""
        name_conflict = next((candidate for candidate in target_list if candidate.get("name") == field.get("name")), None)
        if name_conflict is not None:
            return False, (
                f"Accepted add_field patch {patch['id']} would duplicate "
                f"{patch.get('node_id', '')}.{field.get('name', '')} in canonical YAML."
            )
        if not any(candidate["id"] == field["id"] for candidate in target_list):
            target_list.append(deepcopy(field))
        return True, ""
    if patch_type == "add_binding":
        return update_field_binding(
            graph,
            patch.get("field_id", ""),
            payload.get("primary_binding", ""),
            payload.get("alternatives", []),
            merged_by,
            strict=strict,
        )
    if patch_type == "change_binding":
        return update_field_binding(
            graph,
            patch.get("field_id", ""),
            payload.get("new_binding", ""),
            payload.get("alternatives", []),
            merged_by,
            previous_binding=payload.get("previous_binding", ""),
            strict=strict,
        )
    if patch_type == "confidence_change":
        update_field_confidence(graph, patch.get("field_id", ""), payload.get("confidence", ""), merged_by, explicit_override=True)
        return True, ""
    if patch_type == "remove_field":
        return mark_field_removed(graph, patch.get("field_id", ""), merged_by)
    if patch_type == "remove_node":
        return mark_node_removed(graph, patch.get("node_id", ""), merged_by)
    if patch_type == "remove_edge":
        edge = next((candidate for candidate in graph.get("edges", []) if candidate["id"] == patch.get("edge_id")), None)
        if edge is None:
            return False, f"Accepted remove_edge patch {patch['id']} targets an edge that is missing from canonical YAML."
        edge["removed"] = True
        edge["history"] = [*edge.get("history", []), {"change": "remove_edge", "at": utc_timestamp(), "by": merged_by}]
        return True, ""
    if patch_type == "contradiction":
        return True, ""
    return False, f"Unsupported merge patch type: {patch_type}"


def update_field_binding(
    graph: dict[str, Any],
    field_id: str,
    primary_binding: str,
    alternatives: list[str],
    merged_by: str,
    *,
    previous_binding: str = "",
    strict: bool = False,
) -> tuple[bool, str]:
    node, field = resolve_field(graph, field_id)
    if not node or not field:
        return False, f"Accepted binding patch targets a field that is missing from canonical YAML ({field_id})."
    current = field.get("primary_binding", "")
    if previous_binding and current != previous_binding:
        return False, (
            f"Accepted binding patch for {field_id} expected {previous_binding or 'unbound'}, "
            f"but canonical YAML currently has {current or 'unbound'}."
        )
    previous = field.get("primary_binding", previous_binding)
    field["primary_binding"] = primary_binding
    field["alternatives"] = alternatives or []
    if node["kind"] == "contract":
        field["sources"] = [
            source
            for source in [
                resolve_legacy_source_reference(graph, reference)
                for reference in [primary_binding, *field.get("alternatives", [])]
            ]
            if source
        ]
    else:
        field["lineage_inputs"] = [
            {"field_id": reference, "role": ""}
            for reference in [primary_binding, *field.get("alternatives", [])]
            if reference
        ]
    if previous != primary_binding:
        field.setdefault("history", []).append(
            {
                "change": "change_binding",
                "from_value": previous,
                "to_value": primary_binding,
                "at": utc_timestamp(),
                "by": merged_by,
            }
        )
    return True, ""


def update_field_confidence(
    graph: dict[str, Any],
    field_id: str,
    confidence: str,
    merged_by: str,
    *,
    explicit_override: bool = False,
) -> None:
    _, field = resolve_field(graph, field_id)
    if not field or not confidence:
        return
    previous = field.get("confidence", "medium")
    if previous == confidence:
        return
    if not explicit_override and CONFIDENCE_RANK.get(confidence, 0) < CONFIDENCE_RANK.get(previous, 0):
        return
    field["confidence"] = confidence
    field.setdefault("history", []).append(
        {
            "change": "confidence_change",
            "from_value": previous,
            "to_value": confidence,
            "at": utc_timestamp(),
            "by": merged_by,
        }
    )


def mark_field_removed(graph: dict[str, Any], field_id: str, merged_by: str) -> tuple[bool, str]:
    _, field = resolve_field(graph, field_id)
    if not field:
        return False, f"Accepted remove_field patch targets a field that is missing from canonical YAML ({field_id})."
    field["removed"] = True
    field["removed_at"] = utc_timestamp()
    field["removed_by"] = merged_by
    field.setdefault("history", []).append(
        {
            "change": "remove_field",
            "from_value": field.get("name", ""),
            "to_value": "",
            "at": field["removed_at"],
            "by": merged_by,
        }
    )
    return True, ""


def mark_node_removed(graph: dict[str, Any], node_id: str, merged_by: str) -> tuple[bool, str]:
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None)
    if not node:
        return False, f"Accepted remove_node patch targets a node that is missing from canonical YAML ({node_id})."
    node["removed"] = True
    node["removed_at"] = utc_timestamp()
    node["removed_by"] = merged_by
    node.setdefault("history", []).append(
        {
            "change": "remove_node",
            "from_value": node.get("label", node_id),
            "to_value": "",
            "at": node["removed_at"],
            "by": merged_by,
        }
    )
    return True, ""


def resolve_field(graph: dict[str, Any], field_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    index = build_index(graph)
    node_id = index["field_owner"].get(field_id)
    if not node_id:
        return None, None
    node = index["nodes"].get(node_id)
    if not node:
        return None, None
    for column in node.get("columns", []):
        if column["id"] == field_id:
            return node, column
    for field in node.get("contract", {}).get("fields", []):
        if field["id"] == field_id:
            return node, field
    return None, None


def suggest_binding_for_contract_field(graph: dict[str, Any], node_id: str, field_name: str) -> str:
    field_normalized = normalize_token(field_name)
    for node in graph.get("nodes", []):
        if node["id"] == node_id:
            continue
        if node["kind"] in {"data", "compute"}:
            for column in node.get("columns", []):
                if normalize_token(column["name"]) == field_normalized:
                    return column["id"]
        if node["kind"] == "contract" and node.get("extension_type") == "api":
            for field in node.get("contract", {}).get("fields", []):
                if normalize_token(field["name"]) == field_normalized:
                    return field["id"]
    return ""


def match_source_node_id(graph: dict[str, Any], import_spec: dict[str, Any]) -> str:
    origin = import_spec.get("source_origin_value") or import_spec.get("raw_asset_value") or ""
    for node in graph.get("nodes", []):
        if node["kind"] != "source" or node.get("removed"):
            continue
        raw_values = [asset.get("value", "") for asset in node.get("source", {}).get("raw_assets", [])]
        if origin and (node.get("source", {}).get("origin", {}).get("value") == origin or origin in raw_values):
            return node["id"]
        if node.get("label") == import_spec.get("source_label"):
            return node["id"]
    return ""


def match_data_node_id(graph: dict[str, Any], import_spec: dict[str, Any]) -> str:
    profile_target = import_spec.get("raw_asset_value", "")
    for node in graph.get("nodes", []):
        if node["kind"] != "data" or node.get("removed"):
            continue
        if profile_target and profile_target in {
            node.get("data", {}).get("profile_target", ""),
            node.get("data", {}).get("local_path", ""),
        }:
            return node["id"]
        if node.get("label") == import_spec.get("data_label"):
            return node["id"]
    return ""


def match_sql_data_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"sql_relation:{relation}"
    normalized_relation = slugify_text(relation)
    for node in graph.get("nodes", []):
        if node["kind"] != "data" or node.get("removed"):
            continue
        if tag in (node.get("tags") or []):
            return node["id"]
        if slugify_text(node.get("id", "").split(":", 1)[-1]) == normalized_relation:
            return node["id"]
    return ""


def match_sql_compute_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"sql_relation:{relation}"
    normalized_relation = slugify_text(relation)
    for node in graph.get("nodes", []):
        if node["kind"] != "compute" or node.get("extension_type") != "transform" or node.get("removed"):
            continue
        if tag in (node.get("tags") or []):
            return node["id"]
        if slugify_text(node.get("id", "").split(".", 1)[-1]) == normalized_relation:
            return node["id"]
    return ""


def match_orm_query_data_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"orm_query_projection:{relation}"
    normalized_relation = slugify_text(relation)
    for node in graph.get("nodes", []):
        if node["kind"] != "data" or node.get("removed"):
            continue
        if tag in (node.get("tags") or []):
            return node["id"]
        if slugify_text(node.get("id", "").split(":", 1)[-1]) == normalized_relation:
            return node["id"]
    return ""


def match_orm_query_compute_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"orm_query_projection:{relation}"
    normalized_relation = slugify_text(relation)
    for node in graph.get("nodes", []):
        if node["kind"] != "compute" or node.get("extension_type") != "transform" or node.get("removed"):
            continue
        if tag in (node.get("tags") or []):
            return node["id"]
        if slugify_text(node.get("id", "").split(".", 1)[-1]) == normalized_relation:
            return node["id"]
    return ""


def match_api_node_id(graph: dict[str, Any], route: str, label: str) -> str:
    for node in graph.get("nodes", []):
        if node["kind"] == "contract" and node.get("extension_type") == "api" and node.get("contract", {}).get("route") == route:
            return node["id"]
    normalized = slugify_text(route or label)
    for node in graph.get("nodes", []):
        if node["kind"] == "contract" and node.get("extension_type") == "api" and slugify_text(node["label"]) == normalized:
            return node["id"]
    return ""


def match_ui_node_id(graph: dict[str, Any], component: str) -> str:
    for node in graph.get("nodes", []):
        if node["kind"] != "contract" or node.get("extension_type") != "ui":
            continue
        if node.get("contract", {}).get("component") == component or node.get("label") == component:
            return node["id"]
    return ""


def build_missing_node_impacts(index: dict[str, Any], node_id: str) -> list[str]:
    node = index["nodes"].get(node_id)
    if not node:
        return []
    if node["kind"] == "contract" and node.get("extension_type") == "api":
        labels = []
        ui_roles = infer_ui_roles({"nodes": list(index["nodes"].values()), "edges": list(index["edges"].values())})
        for field in node.get("contract", {}).get("fields", []):
            labels.extend(describe_downstream_component_impacts(index, node_id, field["name"], ui_roles))
        return sorted(set(labels))
    return []


def role_default_confidence(role: str) -> str:
    return "medium" if role == "scout" else "high"


def lower_confidence(confidence: str) -> str:
    if confidence == "high":
        return "medium"
    if confidence == "medium":
        return "low"
    return "low"


def is_significant_impact(field: dict[str, Any], canonical_field: dict[str, Any], impact: str) -> bool:
    return bool(field.get("required", False) or canonical_field.get("required", False) or impact)


def dedupe_patch_rows(patches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for patch in patches:
        key = (patch.get("target_id", ""), patch.get("type", ""))
        current = deduped.get(key)
        if current is None:
            deduped[key] = patch
            continue
        if PATCH_TYPE_PRECEDENCE.get(patch.get("type", ""), 999) <= PATCH_TYPE_PRECEDENCE.get(current.get("type", ""), 999):
            deduped[key] = patch
    return sorted(
        deduped.values(),
        key=lambda item: (
            item.get("target_id", ""),
            PATCH_TYPE_PRECEDENCE.get(item.get("type", ""), 999),
            json.dumps(item.get("payload", {}), sort_keys=True),
        ),
    )


def dedupe_contradictions(contradictions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for contradiction in contradictions:
        deduped.setdefault(contradiction["id"], contradiction)
    return sorted(deduped.values(), key=lambda item: item["id"])


def dedupe_impacts(impacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for impact in impacts:
        if not impact.get("significant", False):
            continue
        key = (impact.get("target_id", ""), impact.get("message", ""))
        deduped.setdefault(key, impact)
    return [deduped[key] for key in sorted(deduped)]


def normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
