from __future__ import annotations

from copy import deepcopy
from typing import Any

from .structure_blueprints import collect_existing_field_ids
from .structure_candidates import extract_sql_relation_tag, merge_partial_field_like
from .structure_reconciliation import (
    build_edge,
    build_patch,
    match_api_node_id,
    match_sql_compute_node_id,
    match_sql_data_node_id,
    match_ui_node_id,
    resolve_legacy_source_reference,
)
from .types import (
    active_nodes,
    build_index,
    column_ref,
    make_field_id,
    normalize_graph,
    resolve_field_reference,
)

def apply_partial_graph_candidate(
    observed_graph: dict[str, Any],
    *,
    candidate: dict[str, Any],
    patches: list[dict[str, Any]],
    role: str,
) -> None:
    partial_graph = normalize_graph(candidate["graph"])
    partial_index = build_index(partial_graph)
    plan_node_id_map = {
        node["id"]: match_partial_graph_node_id(observed_graph, node) or node["id"]
        for node in active_nodes(partial_graph)
    }
    for node in active_nodes(partial_graph):
        matched_node_id = plan_node_id_map.get(node["id"], node["id"])
        if not any(candidate_node["id"] == matched_node_id for candidate_node in observed_graph.get("nodes", [])):
            node_copy = build_plan_node_copy(
                node,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            observed_graph["nodes"].append(node_copy)
            patches.append(
                build_patch(
                    "add_node",
                    target_id=node_copy["id"],
                    node_id=node_copy["id"],
                    role=role,
                    evidence=["plan_yaml"],
                    payload={"node": node_copy},
                    confidence="low",
                )
            )
            continue
        observed_node = next(candidate_node for candidate_node in observed_graph["nodes"] if candidate_node["id"] == matched_node_id)
        ensure_plan_node_fields(
            observed_graph,
            observed_node,
            node,
            plan_node_id_map=plan_node_id_map,
            partial_index=partial_index,
            patches=patches,
            role=role,
        )

    observed_index = build_index(observed_graph)
    for edge in partial_graph.get("edges", []):
        source_id = plan_node_id_map.get(edge.get("source", ""), edge.get("source", ""))
        target_id = plan_node_id_map.get(edge.get("target", ""), edge.get("target", ""))
        if not source_id or not target_id:
            continue
        edge_copy = deepcopy(edge)
        edge_copy["source"] = source_id
        edge_copy["target"] = target_id
        edge_copy["id"] = build_edge(edge_copy.get("type", ""), source_id, target_id)["id"]
        if edge_copy["id"] in observed_index["edges"]:
            continue
        if source_id not in observed_index["nodes"] or target_id not in observed_index["nodes"]:
            continue
        edge_copy["state"] = "proposed"
        edge_copy["confidence"] = "low"
        edge_copy["evidence"] = sorted(set([*edge_copy.get("evidence", []), "plan_yaml"]))
        edge_copy["removed"] = False
        edge_copy["history"] = []
        observed_graph["edges"].append(edge_copy)
        patches.append(
            build_patch(
                "add_edge",
                target_id=edge_copy["id"],
                edge_id=edge_copy["id"],
                role=role,
                evidence=["plan_yaml"],
                payload={"edge": edge_copy},
                confidence="low",
            )
        )


def match_partial_graph_node_id(graph: dict[str, Any], node: dict[str, Any]) -> str:
    if any(candidate["id"] == node["id"] for candidate in graph.get("nodes", [])):
        return node["id"]
    if node["kind"] == "contract" and node.get("extension_type") == "api":
        return match_api_node_id(graph, node.get("contract", {}).get("route", ""), node.get("label", ""))
    if node["kind"] == "contract" and node.get("extension_type") == "ui":
        return match_ui_node_id(graph, node.get("contract", {}).get("component") or node.get("label", ""))
    relation = extract_sql_relation_tag(node)
    if node["kind"] == "data" and relation:
        return match_sql_data_node_id(graph, relation)
    if node["kind"] == "compute" and relation:
        return match_sql_compute_node_id(graph, relation)
    if node["kind"] == "data":
        plan_path = node.get("data", {}).get("profile_target") or node.get("data", {}).get("local_path") or ""
        if plan_path:
            for candidate in graph.get("nodes", []):
                if candidate["kind"] != "data" or candidate.get("removed"):
                    continue
                candidate_path = candidate.get("data", {}).get("profile_target") or candidate.get("data", {}).get("local_path") or ""
                if candidate_path == plan_path:
                    return candidate["id"]
    return ""


def build_plan_node_copy(
    node: dict[str, Any],
    *,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> dict[str, Any]:
    node_copy = deepcopy(node)
    node_copy["state"] = "proposed"
    node_copy["verification_state"] = "observed"
    node_copy["confidence"] = "low"
    node_copy["evidence"] = sorted(set([*node_copy.get("evidence", []), "plan_yaml"]))
    node_copy["last_verified_by"] = ""
    node_copy["last_verified_at"] = ""
    node_copy["history"] = []
    node_copy["removed"] = False
    node_copy["removed_at"] = ""
    node_copy["removed_by"] = ""
    if node_copy["kind"] == "compute":
        node_copy.setdefault("compute", {})
        node_copy["compute"]["inputs"] = remap_plan_node_ids(
            node_copy.get("compute", {}).get("inputs", []),
            plan_node_id_map,
        )
        node_copy["compute"]["outputs"] = remap_plan_node_ids(
            node_copy.get("compute", {}).get("outputs", []),
            plan_node_id_map,
        )
    if node_copy["kind"] == "contract":
        node_copy.setdefault("contract", {})
        node_copy["contract"]["fields"] = [
            build_plan_field_copy(
                node_copy["id"],
                field,
                observed_graph=None,
                is_contract=True,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            for field in node_copy.get("contract", {}).get("fields", [])
        ]
    else:
        node_copy["columns"] = [
            build_plan_field_copy(
                node_copy["id"],
                column,
                observed_graph=None,
                is_contract=False,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            for column in node_copy.get("columns", [])
        ]
    return node_copy


def build_plan_field_copy(
    node_id: str,
    field: dict[str, Any],
    *,
    observed_graph: dict[str, Any] | None,
    is_contract: bool,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> dict[str, Any]:
    field_copy = deepcopy(field)
    if observed_graph is not None:
        existing_ids = collect_existing_field_ids(observed_graph)
        if not field_copy.get("id") or field_copy["id"] in existing_ids:
            field_copy["id"] = make_field_id(node_id, field_copy.get("name", "field"), existing_ids)
    field_copy["state"] = "proposed"
    field_copy["verification_state"] = "observed"
    field_copy["confidence"] = "low"
    field_copy["evidence"] = sorted(set([*field_copy.get("evidence", []), "plan_yaml"]))
    field_copy["last_verified_by"] = ""
    field_copy["last_verified_at"] = ""
    field_copy["history"] = []
    field_copy["removed"] = False
    field_copy["removed_at"] = ""
    field_copy["removed_by"] = ""
    if is_contract:
        field_copy["primary_binding"] = remap_plan_field_reference(
            field_copy.get("primary_binding", ""),
            plan_node_id_map,
            partial_index,
        )
        field_copy["alternatives"] = [
            reference
            for reference in (
                remap_plan_field_reference(reference, plan_node_id_map, partial_index)
                for reference in field_copy.get("alternatives", [])
            )
            if reference
        ]
        field_copy["sources"] = remap_plan_contract_sources(field_copy.get("sources", []), plan_node_id_map)
    else:
        field_copy["lineage_inputs"] = remap_plan_lineage_inputs(
            field_copy.get("lineage_inputs", []),
            plan_node_id_map,
            partial_index,
        )
    return field_copy


def remap_plan_node_ids(node_ids: list[str], plan_node_id_map: dict[str, str]) -> list[str]:
    return sorted(
        {
            plan_node_id_map.get(node_id, node_id)
            for node_id in node_ids or []
            if plan_node_id_map.get(node_id, node_id)
        }
    )


def remap_plan_field_reference(
    reference: str,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> str:
    if not reference:
        return ""
    resolved_field_id = resolve_field_reference(reference, partial_index)
    if resolved_field_id:
        owner_id = partial_index.get("field_owner", {}).get(resolved_field_id, "")
        field_name = partial_index.get("field_name_by_id", {}).get(resolved_field_id, "")
        mapped_owner_id = plan_node_id_map.get(owner_id, owner_id)
        if mapped_owner_id and field_name:
            return column_ref(mapped_owner_id, field_name)
    if "." in reference:
        owner_ref, field_name = reference.rsplit(".", 1)
        mapped_owner_id = plan_node_id_map.get(owner_ref, owner_ref)
        if mapped_owner_id and field_name:
            return column_ref(mapped_owner_id, field_name)
    return plan_node_id_map.get(reference, reference)


def remap_plan_contract_sources(
    sources: list[dict[str, Any]],
    plan_node_id_map: dict[str, str],
) -> list[dict[str, Any]]:
    remapped: list[dict[str, Any]] = []
    for source in sources or []:
        node_id = plan_node_id_map.get(source.get("node_id", ""), source.get("node_id", ""))
        if not node_id:
            continue
        remapped_source = {"node_id": node_id}
        if source.get("column"):
            remapped_source["column"] = source["column"]
        if source.get("field"):
            remapped_source["field"] = source["field"]
        remapped.append(remapped_source)
    return remapped


def remap_plan_lineage_inputs(
    lineage_inputs: list[dict[str, Any]] | list[str],
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> list[dict[str, Any]]:
    remapped: list[dict[str, Any]] = []
    for item in lineage_inputs or []:
        if isinstance(item, str):
            reference = remap_plan_field_reference(item, plan_node_id_map, partial_index)
            if reference:
                remapped.append({"field_id": reference, "role": ""})
            continue
        reference = remap_plan_field_reference(item.get("field_id", ""), plan_node_id_map, partial_index)
        if not reference:
            continue
        remapped.append({"field_id": reference, "role": item.get("role", "")})
    return remapped


def plan_sources_to_binding_refs(sources: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for source in sources or []:
        if source.get("column"):
            refs.append(column_ref(source.get("node_id", ""), source.get("column", "")))
        elif source.get("field"):
            refs.append(column_ref(source.get("node_id", ""), source.get("field", "")))
        elif source.get("node_id"):
            refs.append(source["node_id"])
    return [reference for reference in refs if reference]


def materialize_plan_contract_binding(graph: dict[str, Any], field: dict[str, Any]) -> None:
    binding_refs = [reference for reference in [field.get("primary_binding", ""), *field.get("alternatives", [])] if reference]
    if not binding_refs:
        binding_refs = plan_sources_to_binding_refs(field.get("sources", []))
    if not binding_refs:
        return
    graph_index = build_index(graph)
    resolved_refs = [resolve_field_reference(reference, graph_index) or reference for reference in binding_refs]
    field["primary_binding"] = resolved_refs[0]
    field["alternatives"] = [reference for reference in resolved_refs[1:] if reference != resolved_refs[0]]
    field["sources"] = [
        source
        for source in (
            resolve_legacy_source_reference(graph, reference)
            for reference in [field.get("primary_binding", ""), *field.get("alternatives", [])]
        )
        if source
    ]


def maybe_add_plan_binding_patch(
    graph: dict[str, Any],
    *,
    node_id: str,
    field: dict[str, Any],
    had_binding: bool,
    patches: list[dict[str, Any]],
    role: str,
) -> None:
    if had_binding:
        return
    materialize_plan_contract_binding(graph, field)
    if not field.get("primary_binding"):
        return
    field["state"] = "observed"
    field["verification_state"] = "observed"
    field["confidence"] = "low"
    patches.append(
        build_patch(
            "add_binding",
            target_id=field["id"],
            node_id=node_id,
            field_id=field["id"],
            role=role,
            evidence=["plan_yaml"],
            confidence="low",
            payload={
                "field_id": field["id"],
                "primary_binding": field.get("primary_binding", ""),
                "alternatives": field.get("alternatives", []),
            },
        )
    )


def ensure_plan_node_fields(
    graph: dict[str, Any],
    observed_node: dict[str, Any],
    plan_node: dict[str, Any],
    *,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
    patches: list[dict[str, Any]],
    role: str,
) -> None:
    observed_node["tags"] = sorted(set([*observed_node.get("tags", []), *plan_node.get("tags", [])]))
    observed_node["evidence"] = sorted(set([*observed_node.get("evidence", []), "plan_yaml"]))
    for key in ("description", "owner"):
        if not observed_node.get(key) and plan_node.get(key):
            observed_node[key] = plan_node[key]
    if observed_node["kind"] == "contract":
        existing_fields = {field.get("name"): field for field in observed_node.get("contract", {}).get("fields", [])}
        for field in plan_node.get("contract", {}).get("fields", []):
            field_name = field.get("name", "")
            if not field_name:
                continue
            current = existing_fields.get(field_name)
            if current is None:
                field_copy = build_plan_field_copy(
                    observed_node["id"],
                    field,
                    observed_graph=graph,
                    is_contract=True,
                    plan_node_id_map=plan_node_id_map,
                    partial_index=partial_index,
                )
                observed_node.setdefault("contract", {}).setdefault("fields", []).append(field_copy)
                patches.append(
                    build_patch(
                        "add_field",
                        target_id=field_copy["id"],
                        node_id=observed_node["id"],
                        field_id=field_copy["id"],
                        role=role,
                        evidence=["plan_yaml"],
                        payload={"field": field_copy},
                        confidence="low",
                    )
                )
                continue
            had_binding = bool(current.get("primary_binding") or current.get("sources"))
            field_intent = build_plan_field_copy(
                observed_node["id"],
                field,
                observed_graph=None,
                is_contract=True,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            merge_partial_field_like(current, field_intent)
            maybe_add_plan_binding_patch(
                graph,
                node_id=observed_node["id"],
                field=current,
                had_binding=had_binding,
                patches=patches,
                role=role,
            )
        return

    existing_columns = {column.get("name"): column for column in observed_node.get("columns", [])}
    for column in plan_node.get("columns", []):
        column_name = column.get("name", "")
        if not column_name:
            continue
        current = existing_columns.get(column_name)
        if current is None:
            column_copy = build_plan_field_copy(
                observed_node["id"],
                column,
                observed_graph=graph,
                is_contract=False,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            observed_node.setdefault("columns", []).append(column_copy)
            patches.append(
                build_patch(
                    "add_field",
                    target_id=column_copy["id"],
                    node_id=observed_node["id"],
                    field_id=column_copy["id"],
                    role=role,
                    evidence=["plan_yaml"],
                    payload={"field": column_copy},
                    confidence="low",
                )
            )
            continue
        column_intent = build_plan_field_copy(
            observed_node["id"],
            column,
            observed_graph=None,
            is_contract=False,
            plan_node_id_map=plan_node_id_map,
            partial_index=partial_index,
        )
        merge_partial_field_like(current, column_intent)
    if observed_node["kind"] == "compute":
        observed_node.setdefault("compute", {})
        plan_compute = plan_node.get("compute", {})
        observed_node["compute"]["inputs"] = sorted(
            set(
                [
                    *observed_node["compute"].get("inputs", []),
                    *remap_plan_node_ids(plan_compute.get("inputs", []), plan_node_id_map),
                ]
            )
        )
        observed_node["compute"]["outputs"] = sorted(
            set(
                [
                    *observed_node["compute"].get("outputs", []),
                    *remap_plan_node_ids(plan_compute.get("outputs", []), plan_node_id_map),
                ]
            )
        )


