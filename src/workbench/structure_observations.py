from __future__ import annotations

from copy import deepcopy
from typing import Any

from .structure_blueprints import (
    build_api_node_from_hint,
    build_data_node_from_import_spec,
    build_orm_query_projection_compute_node_from_hint,
    build_orm_query_projection_data_node_from_hint,
    build_source_node_from_import_spec,
    build_sql_compute_node_from_hint,
    build_sql_data_node_from_hint,
    build_ui_node_from_hint,
    collect_existing_field_ids,
)
from .structure_hybrid_support import apply_hint_signal_to_node, hint_observation_evidence
from .structure_reconciliation import (
    CONFIDENCE_RANK,
    build_edge,
    build_patch,
    ensure_contract_field,
    ensure_edge,
    match_api_node_id,
    match_data_node_id,
    match_orm_query_compute_node_id,
    match_orm_query_data_node_id,
    match_source_node_id,
    match_sql_compute_node_id,
    match_sql_data_node_id,
    match_ui_node_id,
    reconcile_field_binding,
    role_default_confidence,
    suggest_binding_for_contract_field,
    suggest_explicit_binding_for_contract_field,
)
from .types import build_index, deep_sort, find_column, make_field_id, normalize_graph

def apply_asset_observation(
    observed_graph: dict[str, Any],
    *,
    canonical_index: dict[str, Any],
    import_spec: dict[str, Any],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
) -> None:
    source_id = match_source_node_id(observed_graph, import_spec)
    data_id = match_data_node_id(observed_graph, import_spec)
    if not source_id:
        source_node = build_source_node_from_import_spec(import_spec)
        source_node["state"] = "observed"
        source_node["confidence"] = role_default_confidence(role)
        source_node["verification_state"] = "observed"
        source_node["evidence"] = ["static_analysis"]
        observed_graph["nodes"].append(source_node)
        source_id = source_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=source_id,
                node_id=source_id,
                role=role,
                evidence=["static_analysis"],
                payload={"node": source_node},
            )
        )
    if not data_id:
        data_node = build_data_node_from_import_spec(import_spec)
        data_node["state"] = "observed"
        data_node["confidence"] = role_default_confidence(role)
        data_node["verification_state"] = "observed"
        data_node["evidence"] = ["static_analysis"]
        observed_graph["nodes"].append(data_node)
        data_id = data_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=data_id,
                node_id=data_id,
                role=role,
                evidence=["static_analysis"],
                payload={"node": data_node},
            )
        )
        patches.append(
            build_patch(
                "add_edge",
                target_id=f"edge.ingests.{source_id}.{data_id}",
                node_id=data_id,
                role=role,
                evidence=["static_analysis"],
                payload={"edge": build_edge("ingests", source_id, data_id)},
            )
        )
        observed_graph["edges"].append(build_edge("ingests", source_id, data_id))


def apply_sql_structure_observation(
    observed_graph: dict[str, Any],
    *,
    canonical_index: dict[str, Any],
    hint: dict[str, Any],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
) -> None:
    relation = hint.get("relation", "")
    object_type = hint.get("object_type", "table")
    fields = hint.get("fields", []) or []
    upstream_relations = hint.get("upstream_relations", []) or []
    hint_evidence = hint_observation_evidence(hint)
    data_node_id = match_sql_data_node_id(observed_graph, relation)
    if not data_node_id:
        data_node = build_sql_data_node_from_hint(hint)
        data_node["state"] = "observed"
        data_node["verification_state"] = "observed"
        data_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        data_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(data_node)
        data_node_id = data_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=data_node_id,
                node_id=data_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": data_node},
            )
        )
    data_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == data_node_id)
    apply_hint_signal_to_node(data_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        data_node,
        fields,
        role=role,
        evidence=hint_evidence,
        patches=patches,
    )

    data_field_map = {column.get("name"): column for column in data_node.get("columns", [])}
    for field_hint in fields:
        data_column = data_field_map.get(field_hint.get("name", ""))
        if not data_column:
            continue
        lineage_refs = []
        lineage_role = "foreign_key" if object_type == "table" else "sql_input"
        for source_field in field_hint.get("source_fields", []) or []:
            upstream_node_id = ensure_sql_relation_node(
                observed_graph,
                relation=source_field.get("relation", ""),
                role=role,
                patches=patches,
                column_names=[source_field.get("column", "")],
            )
            upstream_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == upstream_node_id)
            source_column = find_column(upstream_node, source_field.get("column", ""))
            if source_column:
                lineage_refs.append({"field_id": source_column["id"], "role": lineage_role})
        if lineage_refs:
            data_column["lineage_inputs"] = lineage_refs
        if field_hint.get("unresolved_sources"):
            data_column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))

    if object_type not in {"view", "materialized_view"}:
        for upstream_relation in upstream_relations:
            upstream_node_id = ensure_sql_relation_node(
                observed_graph,
                relation=upstream_relation,
                role=role,
                patches=patches,
            )
            ensure_edge(
                observed_graph,
                edge_type="depends_on",
                source_id=upstream_node_id,
                target_id=data_node_id,
                patches=patches,
                role=role,
                evidence=hint_evidence,
            )

        return

    compute_node_id = match_sql_compute_node_id(observed_graph, relation)
    if not compute_node_id:
        compute_node = build_sql_compute_node_from_hint(hint, output_node_id=data_node_id)
        compute_node["state"] = "observed"
        compute_node["verification_state"] = "observed"
        compute_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        compute_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(compute_node)
        compute_node_id = compute_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=compute_node_id,
                node_id=compute_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": compute_node},
            )
        )
    compute_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == compute_node_id)
    apply_hint_signal_to_node(compute_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        compute_node,
        fields,
        role=role,
        evidence=hint_evidence,
        patches=patches,
    )
    compute_node.setdefault("compute", {})["runtime"] = "sql"
    compute_node["compute"]["outputs"] = sorted(dict.fromkeys([*compute_node["compute"].get("outputs", []), data_node_id]))

    ensure_edge(
        observed_graph,
        edge_type="produces",
        source_id=compute_node_id,
        target_id=data_node_id,
        patches=patches,
        role=role,
        evidence=hint_evidence,
    )
    field_map = {column.get("name"): column for column in compute_node.get("columns", [])}
    for upstream_relation in upstream_relations:
        upstream_node_id = ensure_sql_relation_node(
            observed_graph,
            relation=upstream_relation,
            role=role,
            patches=patches,
        )
        compute_node["compute"]["inputs"] = sorted(dict.fromkeys([*compute_node["compute"].get("inputs", []), upstream_node_id]))
        ensure_edge(
            observed_graph,
            edge_type="depends_on",
            source_id=upstream_node_id,
            target_id=compute_node_id,
            patches=patches,
            role=role,
            evidence=hint_evidence,
        )

    for field_hint in fields:
        compute_column = field_map.get(field_hint.get("name", ""))
        if not compute_column:
            continue
        lineage_refs = []
        for source_field in field_hint.get("source_fields", []) or []:
            upstream_node_id = ensure_sql_relation_node(
                observed_graph,
                relation=source_field.get("relation", ""),
                role=role,
                patches=patches,
                column_names=[source_field.get("column", "")],
            )
            upstream_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == upstream_node_id)
            source_column = find_column(upstream_node, source_field.get("column", ""))
            if source_column:
                lineage_refs.append({"field_id": source_column["id"], "role": "sql_input"})
        if lineage_refs:
            compute_column["lineage_inputs"] = lineage_refs


def apply_orm_query_projection_observation(
    observed_graph: dict[str, Any],
    *,
    canonical_index: dict[str, Any],
    hint: dict[str, Any],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
) -> None:
    relation = hint.get("relation", "")
    fields = hint.get("fields", []) or []
    upstream_relations = hint.get("upstream_relations", []) or []
    if not relation or not fields:
        return
    hint_evidence = hint_observation_evidence(hint)

    data_node_id = match_orm_query_data_node_id(observed_graph, relation)
    if not data_node_id:
        data_node = build_orm_query_projection_data_node_from_hint(hint)
        data_node["state"] = "observed"
        data_node["verification_state"] = "observed"
        data_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        data_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(data_node)
        data_node_id = data_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=data_node_id,
                node_id=data_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": data_node},
            )
        )
    data_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == data_node_id)
    apply_hint_signal_to_node(data_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        data_node,
        fields,
        role=role,
        evidence=hint_evidence,
        patches=patches,
    )

    compute_node_id = match_orm_query_compute_node_id(observed_graph, relation)
    if not compute_node_id:
        compute_node = build_orm_query_projection_compute_node_from_hint(hint, output_node_id=data_node_id)
        compute_node["state"] = "observed"
        compute_node["verification_state"] = "observed"
        compute_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        compute_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(compute_node)
        compute_node_id = compute_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=compute_node_id,
                node_id=compute_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": compute_node},
            )
        )
    compute_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == compute_node_id)
    apply_hint_signal_to_node(compute_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        compute_node,
        fields,
        role=role,
        evidence=hint_evidence,
        patches=patches,
    )
    compute_node.setdefault("compute", {})["runtime"] = "orm"
    compute_node["compute"]["outputs"] = sorted(dict.fromkeys([*compute_node["compute"].get("outputs", []), data_node_id]))

    ensure_edge(
        observed_graph,
        edge_type="produces",
        source_id=compute_node_id,
        target_id=data_node_id,
        patches=patches,
        role=role,
        evidence=hint_evidence,
    )

    for upstream_relation in upstream_relations:
        upstream_node_id = ensure_sql_relation_node(
            observed_graph,
            relation=upstream_relation,
            role=role,
            patches=patches,
        )
        compute_node["compute"]["inputs"] = sorted(dict.fromkeys([*compute_node["compute"].get("inputs", []), upstream_node_id]))
        ensure_edge(
            observed_graph,
            edge_type="depends_on",
            source_id=upstream_node_id,
            target_id=compute_node_id,
            patches=patches,
            role=role,
            evidence=hint_evidence,
        )

    compute_field_map = {column.get("name"): column for column in compute_node.get("columns", [])}
    data_field_map = {column.get("name"): column for column in data_node.get("columns", [])}
    for field_hint in fields:
        compute_column = compute_field_map.get(field_hint.get("name", ""))
        data_column = data_field_map.get(field_hint.get("name", ""))
        lineage_refs: list[dict[str, Any]] = []
        for source_field in field_hint.get("source_fields", []) or []:
            upstream_node_id = ensure_sql_relation_node(
                observed_graph,
                relation=source_field.get("relation", ""),
                role=role,
                patches=patches,
                column_names=[source_field.get("column", "")],
            )
            upstream_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == upstream_node_id)
            source_column = find_column(upstream_node, source_field.get("column", ""))
            if source_column:
                lineage_refs.append({"field_id": source_column["id"], "role": "orm_input"})
        if lineage_refs:
            if compute_column is not None:
                compute_column["lineage_inputs"] = lineage_refs
            if data_column is not None:
                data_column["lineage_inputs"] = deepcopy(lineage_refs)
        if field_hint.get("unresolved_sources"):
            if compute_column is not None:
                compute_column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))
            if data_column is not None:
                data_column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))


def ensure_sql_columns(
    graph: dict[str, Any],
    node: dict[str, Any],
    fields: list[dict[str, Any]],
    *,
    role: str,
    evidence: list[str],
    patches: list[dict[str, Any]],
) -> None:
    existing_by_name = {column.get("name"): column for column in node.get("columns", [])}
    existing_ids = collect_existing_field_ids(graph)
    for field_hint in fields:
        field_name = field_hint.get("name", "")
        if not field_name:
            continue
        if field_name in existing_by_name:
            column = existing_by_name[field_name]
            if column.get("data_type", "unknown") == "unknown" and field_hint.get("data_type"):
                column["data_type"] = field_hint["data_type"]
            if field_hint.get("confidence") and CONFIDENCE_RANK.get(field_hint["confidence"], 0) != CONFIDENCE_RANK.get(column.get("confidence", role_default_confidence(role)), 0):
                column["confidence"] = field_hint["confidence"]
            if field_hint.get("evidence"):
                column["evidence"] = sorted(dict.fromkeys([*column.get("evidence", []), *field_hint.get("evidence", [])]))
            if "unresolved_sources" in field_hint:
                column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))
            for metadata_key in ("primary_key", "foreign_key", "nullable", "index", "unique"):
                if metadata_key not in field_hint:
                    continue
                new_value = field_hint[metadata_key]
                current_value = column.get(metadata_key)
                if metadata_key == "nullable":
                    if current_value is None or (current_value is True and new_value is False):
                        column[metadata_key] = new_value
                elif metadata_key in {"primary_key", "index", "unique"}:
                    if new_value and not current_value:
                        column[metadata_key] = new_value
                elif new_value and current_value in {"", None}:
                    column[metadata_key] = new_value
            continue
        column = {
            "id": make_field_id(node["id"], field_name, existing_ids),
            "name": field_name,
            "data_type": field_hint.get("data_type", "unknown"),
            "state": "observed",
            "verification_state": "observed",
            "confidence": field_hint.get("confidence", role_default_confidence(role)),
            "evidence": sorted(dict.fromkeys([*evidence, *(field_hint.get("evidence", []) or [])])),
            "required": node["kind"] == "compute",
            "lineage_inputs": [],
            "history": [],
        }
        for metadata_key in ("primary_key", "foreign_key", "nullable", "index", "unique"):
            if metadata_key in field_hint:
                column[metadata_key] = field_hint[metadata_key]
        if "unresolved_sources" in field_hint:
            column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))
        node.setdefault("columns", []).append(column)
        existing_by_name[field_name] = column
        patches.append(
            build_patch(
                "add_field",
                target_id=column["id"],
                node_id=node["id"],
                field_id=column["id"],
                role=role,
                evidence=evidence,
                payload={"field": column},
            )
        )


def ensure_sql_relation_node(
    graph: dict[str, Any],
    *,
    relation: str,
    role: str,
    patches: list[dict[str, Any]],
    column_names: list[str] | None = None,
) -> str:
    node_id = match_sql_data_node_id(graph, relation)
    if not node_id:
        node = build_sql_data_node_from_hint(
            {
                "relation": relation,
                "label": relation,
                "object_type": "table",
                "description": "Observed as SQL input relation.",
                "fields": [{"name": name, "data_type": "unknown"} for name in (column_names or []) if name],
            }
        )
        node["state"] = "observed"
        node["verification_state"] = "observed"
        node["confidence"] = role_default_confidence(role)
        node["evidence"] = ["static_analysis"]
        graph["nodes"].append(node)
        node_id = node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=node_id,
                node_id=node_id,
                role=role,
                evidence=["static_analysis"],
                payload={"node": node},
            )
        )
    if column_names:
        node = next(candidate for candidate in graph["nodes"] if candidate["id"] == node_id)
        ensure_sql_columns(
            graph,
            node,
            [{"name": name, "data_type": "unknown"} for name in column_names if name],
            role=role,
            evidence=["static_analysis"],
            patches=patches,
        )
    return node_id


def apply_api_hint_observation(
    observed_graph: dict[str, Any],
    *,
    canonical_index: dict[str, Any],
    hint: dict[str, Any],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
) -> None:
    node_id = match_api_node_id(observed_graph, hint.get("route", ""), hint.get("label", ""))
    if not node_id:
        node = build_api_node_from_hint(hint)
        node["state"] = "observed"
        node["verification_state"] = "observed"
        node["confidence"] = role_default_confidence(role)
        node["evidence"] = ["static_analysis"]
        observed_graph["nodes"].append(node)
        node_id = node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=node_id,
                node_id=node_id,
                role=role,
                evidence=["static_analysis"],
                payload={"node": node},
            )
        )

    node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == node_id)
    field_sources_by_name = {
        item.get("name", ""): item.get("source_fields", []) or []
        for item in hint.get("response_field_sources", []) or []
        if item.get("name")
    }
    for field_name in hint.get("response_fields", []) or []:
        ensure_contract_field(
            observed_graph,
            node,
            field_name,
            role=role,
            evidence=["static_analysis"],
            patches=patches,
        )
        explicit_suggestion = suggest_explicit_binding_for_contract_field(
            observed_graph,
            field_sources_by_name.get(field_name, []),
            role=role,
            patches=patches,
            ensure_relation_node=ensure_sql_relation_node,
        )
        suggestion = explicit_suggestion or suggest_binding_for_contract_field(observed_graph, node["id"], field_name)
        if suggestion:
            reconcile_field_binding(
                observed_graph,
                node["id"],
                field_name,
                suggestion,
                patches=patches,
                contradictions=contradictions,
                impacts=impacts,
                role=role,
                canonical_index=canonical_index,
                evidence=["code_reference"] if explicit_suggestion else ["schema_match"],
            )


def apply_ui_hint_observation(
    observed_graph: dict[str, Any],
    *,
    canonical_index: dict[str, Any],
    hint: dict[str, Any],
    api_hints_by_route: dict[str, dict[str, Any]],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
) -> None:
    component = hint.get("component") or hint.get("label") or "UiContract"
    node_id = match_ui_node_id(observed_graph, component)
    if not node_id:
        node = build_ui_node_from_hint(hint)
        node["state"] = "observed"
        node["verification_state"] = "observed"
        node["confidence"] = role_default_confidence(role)
        node["evidence"] = ["static_analysis"]
        observed_graph["nodes"].append(node)
        node_id = node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=node_id,
                node_id=node_id,
                role=role,
                evidence=["static_analysis"],
                payload={"node": node},
            )
        )
    node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == node_id)
    route_field_hints = hint.get("route_field_hints", {}) or {}
    fields = hint.get("used_fields", []) or []
    for field_name in fields:
        ensure_contract_field(observed_graph, node, field_name, role=role, evidence=["static_analysis"], patches=patches)
        suggestion = suggest_binding_for_contract_field(observed_graph, node["id"], field_name)
        if suggestion:
            reconcile_field_binding(
                observed_graph,
                node["id"],
                field_name,
                suggestion,
                patches=patches,
                contradictions=contradictions,
                impacts=impacts,
                role=role,
                canonical_index=canonical_index,
            )

    for route_path in hint.get("api_routes", []) or []:
        route = f"GET {route_path}"
        api_node_id = match_api_node_id(observed_graph, route, route)
        if not api_node_id:
            api_hint = api_hints_by_route.get(route, {})
            api_node = build_api_node_from_hint(api_hint or {"route": route, "label": route})
            api_node["state"] = "observed"
            api_node["verification_state"] = "observed"
            api_node["confidence"] = role_default_confidence(role)
            api_node["evidence"] = ["static_analysis"]
            observed_graph["nodes"].append(api_node)
            api_node_id = api_node["id"]
            patches.append(
                build_patch(
                    "add_node",
                    target_id=api_node_id,
                    node_id=api_node_id,
                    role=role,
                    evidence=["static_analysis"],
                    payload={"node": api_node},
                )
            )
        ensure_edge(
            observed_graph,
            edge_type="contains" if node.get("contract", {}).get("ui_role") in {"screen", "container"} else "binds",
            source_id=api_node_id,
            target_id=node["id"],
            patches=patches,
            role=role,
            evidence=["static_analysis"],
        )

        for field_name in route_field_hints.get(route_path, []):
            ensure_contract_field(observed_graph, node, field_name, role=role, evidence=["static_analysis"], patches=patches)


def apply_document_candidate(
    observed_graph: dict[str, Any],
    *,
    canonical_index: dict[str, Any],
    candidate: dict[str, Any],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
) -> None:
    if candidate["type"] == "api_route":
        hint = {"route": candidate["route"], "label": candidate["label"], "response_fields": candidate.get("fields", [])}
        apply_api_hint_observation(
            observed_graph,
            canonical_index=canonical_index,
            hint=hint,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )
        return
    if candidate["type"] == "data_asset":
        spec = candidate["import_spec"]
        apply_asset_observation(
            observed_graph,
            canonical_index=canonical_index,
            import_spec=spec,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )
        return
    if candidate["type"] == "partial_graph":
        partial_graph = normalize_graph(candidate["graph"])
        partial_index = build_index(partial_graph)
        for node in partial_graph.get("nodes", []):
            if node["id"] not in build_index(observed_graph)["nodes"]:
                node_copy = deepcopy(node)
                node_copy["state"] = "proposed"
                node_copy["confidence"] = "low"
                node_copy["verification_state"] = "observed"
                node_copy["evidence"] = ["user_defined"]
                observed_graph["nodes"].append(node_copy)
                patches.append(
                    build_patch(
                        "add_node",
                        target_id=node_copy["id"],
                        node_id=node_copy["id"],
                        role=role,
                        evidence=["user_defined"],
                        payload={"node": node_copy},
                    )
                )
        for edge in partial_graph.get("edges", []):
            if edge["id"] not in build_index(observed_graph)["edges"]:
                observed_graph["edges"].append(deepcopy(edge))
                patches.append(
                    build_patch(
                        "add_edge",
                        target_id=edge["id"],
                        edge_id=edge["id"],
                        role=role,
                        evidence=["user_defined"],
                        payload={"edge": edge},
                    )
                )


