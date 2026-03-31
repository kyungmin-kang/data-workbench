from __future__ import annotations

from typing import Any

from .diff import analyze_contracts
from .types import build_index, display_ref_for_field_id, find_column, resolve_field_reference


def build_validation_report(graph: dict[str, Any]) -> dict[str, Any]:
    index = build_index(graph)
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    contract_diagnostics = analyze_contracts(graph)
    for node_id, details in contract_diagnostics.items():
        for message in details.get("missing_dependencies", []):
            errors.append(_issue("error", "contract_dependency", message, node_id=node_id))
        for field_name in details.get("unused_fields", []):
            warnings.append(
                _issue(
                    "warning",
                    "unused_contract_field",
                    f"{node_id}.{field_name}: field is not consumed downstream.",
                    node_id=node_id,
                )
            )

    for node in graph.get("nodes", []):
        node_id = node["id"]
        incoming = index["incoming"].get(node_id, [])
        outgoing = index["outgoing"].get(node_id, [])
        if node.get("removed"):
            continue

        if not incoming and not outgoing:
            warnings.append(
                _issue("warning", "orphan_node", f"{node_id}: node has no incoming or outgoing edges.", node_id=node_id)
            )

        if node["kind"] == "contract":
            errors.extend(_validate_contract_node(node))
        if node["kind"] == "compute":
            errors.extend(_validate_compute_node(node, index))
        if node["kind"] == "data" and not node.get("data", {}).get("persistence"):
            errors.append(
                _issue(
                    "error",
                    "missing_persistence_role",
                    f"{node_id}: data persistence role is missing.",
                    node_id=node_id,
                )
            )

    for edge in graph.get("edges", []):
        errors.extend(_validate_edge(edge, index))

    return {
        "errors": _dedupe_issues(errors),
        "warnings": _dedupe_issues(warnings),
        "summary": {
            "errors": len(_dedupe_issues(errors)),
            "warnings": len(_dedupe_issues(warnings)),
        },
    }


def _validate_contract_node(node: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    field_names = [field["name"] for field in node.get("contract", {}).get("fields", [])]
    duplicates = sorted({field_name for field_name in field_names if field_names.count(field_name) > 1})
    for duplicate in duplicates:
        issues.append(
            _issue(
                "error",
                "duplicate_contract_field",
                f"{node['id']}.{duplicate}: duplicate contract field name.",
                node_id=node["id"],
                column_ref=f"{node['id']}.{duplicate}",
            )
        )
    for field in node.get("contract", {}).get("fields", []):
        if field.get("removed"):
            continue
        if not field.get("required", True):
            continue
        primary_binding = field.get("primary_binding") or ""
        if not primary_binding:
            issues.append(
                _issue(
                    "error",
                    "missing_required_binding",
                    f"{node['id']}.{field['name']}: required contract field is missing an upstream binding.",
                    node_id=node["id"],
                    column_ref=field.get("id") or f"{node['id']}.{field['name']}",
                )
            )
    return issues


def _validate_compute_node(node: dict[str, Any], index: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for input_node_id in node.get("compute", {}).get("inputs", []):
        if input_node_id not in index["nodes"]:
            issues.append(
                _issue(
                    "error",
                    "missing_compute_input",
                    f"{node['id']}: compute input missing ({input_node_id}).",
                    node_id=node["id"],
                )
            )
    for output_node_id in node.get("compute", {}).get("outputs", []):
        if output_node_id not in index["nodes"]:
            issues.append(
                _issue(
                    "error",
                    "missing_compute_output",
                    f"{node['id']}: compute output missing ({output_node_id}).",
                    node_id=node["id"],
                )
            )

    feature_refs = [feature.get("column_ref", "") for feature in node.get("compute", {}).get("feature_selection", []) if feature.get("column_ref")]
    duplicates = sorted({feature_ref for feature_ref in feature_refs if feature_refs.count(feature_ref) > 1})
    for duplicate in duplicates:
        issues.append(
            _issue(
                "error",
                "duplicate_feature_ref",
                f"{node['id']}: duplicate feature reference ({duplicate}).",
                node_id=node["id"],
                column_ref=duplicate,
            )
        )

    for feature in node.get("compute", {}).get("feature_selection", []):
        reference = feature.get("field_id") or feature.get("column_ref", "")
        if not reference:
            continue
        validation_message = _validate_ref(reference, index)
        if validation_message:
            issues.append(
                _issue(
                    "error",
                    "invalid_feature_ref",
                    f"{node['id']}: {validation_message}",
                    node_id=node["id"],
                    column_ref=reference,
                )
            )
    for column in node.get("columns", []):
        if column.get("removed") or not column.get("required", True):
            continue
        lineage_inputs = column.get("lineage_inputs", [])
        if not lineage_inputs:
            issues.append(
                _issue(
                    "error",
                    "missing_compute_lineage",
                    f"{node['id']}.{column['name']}: required compute output is missing lineage inputs.",
                    node_id=node["id"],
                    column_ref=column.get("id") or f"{node['id']}.{column['name']}",
                )
            )
            continue
        for lineage in lineage_inputs:
            validation_message = _validate_ref(lineage.get("field_id", ""), index)
            if validation_message:
                issues.append(
                    _issue(
                        "error",
                        "invalid_lineage_input",
                        f"{node['id']}.{column['name']}: {validation_message}",
                        node_id=node["id"],
                        column_ref=column.get("id") or f"{node['id']}.{column['name']}",
                    )
                )
    return issues


def _validate_edge(edge: dict[str, Any], index: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    source_node = index["nodes"].get(edge["source"])
    target_node = index["nodes"].get(edge["target"])
    if not source_node or not target_node:
        return issues

    for mapping in edge.get("column_mappings", []):
        source_column = mapping.get("source_column", "")
        target_column = mapping.get("target_column", "")
        if source_column and _node_requires_declared_bindings(source_node) and not _node_has_binding_name(source_node, source_column):
            issues.append(
                _issue(
                    "error",
                    "invalid_edge_source_mapping",
                    f"{edge['id']}: source mapping missing ({edge['source']}.{source_column}).",
                    edge_id=edge["id"],
                    column_ref=f"{edge['source']}.{source_column}",
                )
            )
        if target_column and _node_requires_declared_bindings(target_node) and not _node_has_binding_name(target_node, target_column):
            issues.append(
                _issue(
                    "error",
                    "invalid_edge_target_mapping",
                    f"{edge['id']}: target mapping missing ({edge['target']}.{target_column}).",
                    edge_id=edge["id"],
                    column_ref=f"{edge['target']}.{target_column}",
                )
            )
    return issues


def _validate_ref(reference: str, index: dict[str, Any]) -> str | None:
    field_id = resolve_field_reference(reference, index)
    if not field_id:
        if "." not in reference:
            return f"reference missing or unknown ({reference})."
        node_id, binding_name = reference.rsplit(".", 1)
        node = index["nodes"].get(node_id)
        if not node:
            return f"upstream node missing ({node_id})."
        if not _node_has_binding_name(node, binding_name):
            return f"upstream binding missing ({node_id}.{binding_name})."
        return None
    node_id = index["field_owner"].get(field_id, "")
    if not node_id:
        return f"upstream binding missing ({reference})."
    return None


def _node_has_binding_name(node: dict[str, Any], binding_name: str) -> bool:
    if node["kind"] == "contract":
        return any(field["name"] == binding_name for field in node.get("contract", {}).get("fields", []))
    if node["kind"] == "compute":
        return binding_name in _compute_binding_names(node)
    return find_column(node, binding_name) is not None


def _node_requires_declared_bindings(node: dict[str, Any]) -> bool:
    return node["kind"] in {"data", "contract", "compute"}


def _compute_binding_names(node: dict[str, Any]) -> set[str]:
    names = {
        column["name"]
        for column in node.get("columns", [])
        if column.get("name")
    }
    for mapping in node.get("compute", {}).get("column_mappings", []):
        for key in ("source", "target"):
            reference = mapping.get(key, "")
            if "." in reference:
                names.add(reference.rsplit(".", 1)[-1])
    for feature in node.get("compute", {}).get("feature_selection", []):
        reference = feature.get("column_ref", "")
        if "." in reference:
            names.add(reference.rsplit(".", 1)[-1])
    return names


def _issue(
    severity: str,
    category: str,
    message: str,
    *,
    node_id: str | None = None,
    edge_id: str | None = None,
    column_ref: str | None = None,
) -> dict[str, Any]:
    issue = {
        "severity": severity,
        "category": category,
        "message": message,
    }
    if node_id:
        issue["node_id"] = node_id
    if edge_id:
        issue["edge_id"] = edge_id
    if column_ref:
        issue["column_ref"] = column_ref
    return issue


def _dedupe_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for issue in issues:
        key = (
            issue.get("severity"),
            issue.get("category"),
            issue.get("message"),
            issue.get("node_id"),
            issue.get("edge_id"),
            issue.get("column_ref"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return deduped
