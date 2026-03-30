from __future__ import annotations

from collections import defaultdict
from typing import Any

from .diff import analyze_contracts
from .types import build_index, display_ref_for_field_id, find_column, resolve_field_reference


HEALTH_ORDER = {"healthy": 0, "warning": 1, "broken": 2}
UI_ROLE_COMPONENT = "component"
UI_IMPACT_LIMIT = 3


def build_graph_diagnostics(
    graph: dict[str, Any],
    *,
    validation_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    index = build_index(graph)
    validation_report = validation_report or {"errors": [], "warnings": [], "summary": {}}
    contract_diagnostics = analyze_contracts(graph)
    ui_roles = infer_ui_roles(graph)
    issues_by_node, issues_by_binding = index_validation_issues(validation_report)

    nodes: dict[str, dict[str, Any]] = {}
    contract_nodes: dict[str, dict[str, Any]] = {}
    broken_rows: list[dict[str, Any]] = []
    warning_rows: list[dict[str, Any]] = []

    for node in graph.get("nodes", []):
        summary = {
            "id": node["id"],
            "label": node.get("label", node["id"]),
            "kind": node["kind"],
            "extension_type": node.get("extension_type", ""),
            "ui_role": ui_roles.get(node["id"], ""),
            "upstream_count": len({edge["source"] for edge in index["incoming"].get(node["id"], [])}),
            "downstream_count": len({edge["target"] for edge in index["outgoing"].get(node["id"], [])}),
            "node_issues": [],
            "bindings": {},
            "health": "healthy",
            "state": node.get("state", "confirmed"),
            "confidence": node.get("confidence", "high"),
        }

        binding_entries: list[dict[str, Any]] = []
        if node["kind"] == "contract":
            contract_summary = build_contract_node_summary(
                node,
                index=index,
                contract_diagnostics=contract_diagnostics.get(node["id"], {}),
                issues_by_binding=issues_by_binding,
                issues_by_node=issues_by_node,
                ui_roles=ui_roles,
            )
            summary["bindings"] = contract_summary["bindings"]
            summary["node_issues"] = contract_summary["node_issues"]
            summary["health"] = contract_summary["health"]
            contract_nodes[node["id"]] = contract_summary
            binding_entries = list(contract_summary["bindings"].values())
        elif node["kind"] == "data":
            binding_entries = build_data_column_summaries(node, index=index, issues_by_binding=issues_by_binding)
            summary["bindings"] = {entry["name"]: entry for entry in binding_entries}
            summary["node_issues"] = summarize_node_validation_issues(node["id"], issues_by_node)
            summary["health"] = rollup_health(binding_entries, summary["node_issues"])
        else:
            summary["node_issues"] = summarize_node_validation_issues(node["id"], issues_by_node)
            summary["health"] = rollup_health([], summary["node_issues"])

        nodes[node["id"]] = summary

        for issue in binding_entries:
            if issue["health"] == "broken":
                broken_rows.extend(build_severity_rows(node, issue, severity="broken"))
            elif issue["health"] == "warning":
                warning_rows.extend(build_severity_rows(node, issue, severity="warning"))
        if summary["health"] != "healthy":
            node_level_rows = build_node_issue_rows(node, summary["node_issues"])
            if summary["health"] == "broken":
                broken_rows.extend(node_level_rows)
            else:
                warning_rows.extend(node_level_rows)

    broken_rows = dedupe_severity_rows(broken_rows)
    warning_rows = dedupe_severity_rows(warning_rows)
    health_counts = {"healthy": 0, "warning": 0, "broken": 0}
    for details in nodes.values():
        health_counts[details["health"]] += 1

    return {
        "contracts": contract_nodes,
        "nodes": nodes,
        "summary": {
            "node_health": health_counts,
            "validation_errors": len(validation_report.get("errors", [])),
            "validation_warnings": len(validation_report.get("warnings", [])),
        },
        "severity_tiles": {
            "contract_breaks": {
                "icon": "🚨",
                "label": "Contract Breaks",
                "count": len(broken_rows),
                "rows": group_severity_rows(broken_rows),
            },
            "warnings": {
                "icon": "⚠️",
                "label": "Warnings",
                "count": len(warning_rows),
                "rows": group_severity_rows(warning_rows),
            },
            "informational": {
                "icon": "ℹ️",
                "label": "Informational",
                "count": 0,
                "rows": [],
            },
        },
    }


def infer_ui_roles(graph: dict[str, Any]) -> dict[str, str]:
    index = build_index(graph)
    roles: dict[str, str] = {}
    for node in graph.get("nodes", []):
        if node["kind"] != "contract" or node.get("extension_type") != "ui":
            continue
        explicit = node.get("contract", {}).get("ui_role")
        if explicit:
            roles[node["id"]] = explicit
            continue

        parent_ids = [
            edge["source"]
            for edge in index["incoming"].get(node["id"], [])
            if edge["type"] == "contains"
            and index["nodes"].get(edge["source"], {}).get("kind") == "contract"
            and index["nodes"].get(edge["source"], {}).get("extension_type") == "ui"
        ]
        child_ids = [
            edge["target"]
            for edge in index["outgoing"].get(node["id"], [])
            if edge["type"] == "contains"
            and index["nodes"].get(edge["target"], {}).get("kind") == "contract"
            and index["nodes"].get(edge["target"], {}).get("extension_type") == "ui"
        ]
        if child_ids:
            roles[node["id"]] = "screen" if not parent_ids else "container"
        else:
            roles[node["id"]] = UI_ROLE_COMPONENT
    return roles


def index_validation_issues(validation_report: dict[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    by_node: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_binding: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for issue in validation_report.get("errors", []) + validation_report.get("warnings", []):
        node_id = issue.get("node_id")
        if node_id:
            by_node[node_id].append(issue)
        column_ref = issue.get("column_ref")
        if column_ref:
            by_binding[column_ref].append(issue)
    return dict(by_node), dict(by_binding)


def build_contract_node_summary(
    node: dict[str, Any],
    *,
    index: dict[str, Any],
    contract_diagnostics: dict[str, list[str]],
    issues_by_binding: dict[str, list[dict[str, Any]]],
    issues_by_node: dict[str, list[dict[str, Any]]],
    ui_roles: dict[str, str],
) -> dict[str, Any]:
    bindings: dict[str, dict[str, Any]] = {}
    for field in node.get("contract", {}).get("fields", []):
        binding = build_contract_field_summary(
            node,
            field,
            index=index,
            contract_diagnostics=contract_diagnostics,
            issues_by_binding=issues_by_binding,
            ui_roles=ui_roles,
        )
        bindings[field["name"]] = binding

    node_issues = summarize_node_validation_issues(node["id"], issues_by_node)
    return {
        "id": node["id"],
        "missing_dependencies": contract_diagnostics.get("missing_dependencies", []),
        "unused_fields": contract_diagnostics.get("unused_fields", []),
        "breakage_warnings": contract_diagnostics.get("breakage_warnings", []),
        "bindings": bindings,
        "node_issues": node_issues,
        "health": rollup_health(list(bindings.values()), node_issues),
    }


def build_contract_field_summary(
    node: dict[str, Any],
    field: dict[str, Any],
    *,
    index: dict[str, Any],
    contract_diagnostics: dict[str, list[str]],
    issues_by_binding: dict[str, list[dict[str, Any]]],
    ui_roles: dict[str, str],
) -> dict[str, Any]:
    ref = field.get("id") or f"{node['id']}.{field['name']}"
    primary_binding = format_binding_ref(graph_index=index, reference=field.get("primary_binding", ""), fallback_sources=field.get("sources", []))
    alternative_bindings = [
        format_binding_ref(graph_index=index, reference=reference)
        for reference in field.get("alternatives", [])
        if reference
    ]
    direct_issues: list[dict[str, Any]] = []

    if field.get("required", True) and not field.get("primary_binding") and not field.get("sources"):
        direct_issues.append(
            {
                "severity": "broken",
                "message": "No upstream source is defined.",
                "technical_message": f"{field['name']}: no upstream source is defined.",
            }
        )
    else:
        binding_refs = [field.get("primary_binding", ""), *field.get("alternatives", [])]
        legacy_sources = field.get("sources", [])
        sources = binding_refs if any(binding_refs) else legacy_sources
        for source in sources:
            message = validate_field_source(index, node, field["name"], source)
            if message:
                direct_issues.append(
                    {
                        "severity": "broken",
                        "message": message.split(": ", 1)[-1],
                        "technical_message": message,
                    }
                )

    if node.get("extension_type") == "api" and field["name"] in set(contract_diagnostics.get("unused_fields", [])):
        direct_issues.append(
            {
                "severity": "warning",
                "message": "Field is not consumed downstream.",
                "technical_message": f"{field['name']}: field is not consumed downstream.",
            }
        )

    for issue in issues_by_binding.get(ref, []):
        direct_issues.append(
            {
                "severity": "broken" if issue.get("severity") == "error" else "warning",
                "message": issue.get("message", ""),
                "technical_message": issue.get("message", ""),
            }
        )

    direct_issues = dedupe_issue_summaries(direct_issues)
    health = rollup_health([], direct_issues)
    impacts = []
    if health == "broken":
        impacts = describe_downstream_component_impacts(index, node["id"], field["name"], ui_roles)
    why_this_matters = impacts[0] if impacts else ""
    upstream_count, downstream_count = count_contract_field_neighbors(index, node["id"], field)

    return {
        "id": field.get("id", ""),
        "name": field["name"],
        "type": "field",
        "health": health,
        "upstream_count": upstream_count,
        "downstream_count": downstream_count,
        "primary_binding": primary_binding,
        "alternative_bindings": alternative_bindings,
        "alternative_count": len(alternative_bindings),
        "broken": not field.get("primary_binding") and not field.get("sources"),
        "issues": direct_issues,
        "why_this_matters": why_this_matters,
        "downstream_impacts": impacts,
        "state": field.get("state", "confirmed"),
        "confidence": field.get("confidence", "high"),
        "verification_state": field.get("verification_state", "confirmed"),
        "required": field.get("required", True),
    }


def build_data_column_summaries(
    node: dict[str, Any],
    *,
    index: dict[str, Any],
    issues_by_binding: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for column in node.get("columns", []):
        ref = column.get("id") or f"{node['id']}.{column['name']}"
        issues = [
            {
                "severity": "broken" if issue.get("severity") == "error" else "warning",
                "message": issue.get("message", ""),
            }
            for issue in issues_by_binding.get(ref, [])
        ]
        upstream_count, downstream_count = count_data_column_neighbors(index, node["id"], column["name"])
        summaries.append(
            {
                "name": column["name"],
                "type": "column",
                "health": rollup_health([], issues),
                "upstream_count": upstream_count,
                "downstream_count": downstream_count,
                "issues": dedupe_issue_summaries(issues),
                "id": column.get("id", ""),
                "state": column.get("state", "confirmed"),
                "confidence": column.get("confidence", "high"),
                "verification_state": column.get("verification_state", "confirmed"),
                "required": column.get("required", False),
            }
        )
    return summaries


def count_data_column_neighbors(index: dict[str, Any], node_id: str, column_name: str) -> tuple[int, int]:
    upstream = {
        edge["source"]
        for edge in index["incoming"].get(node_id, [])
        if any(mapping.get("target_column") == column_name for mapping in edge.get("column_mappings", []))
    }
    downstream = {
        edge["target"]
        for edge in index["outgoing"].get(node_id, [])
        if any(mapping.get("source_column") == column_name for mapping in edge.get("column_mappings", []))
    }
    return len(upstream), len(downstream)


def count_contract_field_neighbors(index: dict[str, Any], node_id: str, field: dict[str, Any]) -> tuple[int, int]:
    upstream = set()
    for source_ref in [field.get("primary_binding", ""), *field.get("alternatives", [])]:
        field_id = resolve_field_reference(source_ref, index)
        owner = index["field_owner"].get(field_id, "")
        if owner:
            upstream.add(owner)
    if not upstream:
        upstream = {
            source.get("node_id")
            for source in field.get("sources", [])
            if source.get("node_id")
        }
    target_ref = f"{node_id}.{field['name']}"
    downstream = set()
    for node in index["nodes"].values():
        if node.get("kind") != "contract":
            continue
        for candidate in node.get("contract", {}).get("fields", []):
            if any(
                format_single_source_ref(source) == target_ref
                for source in candidate.get("sources", [])
            ) or resolve_field_reference(candidate.get("primary_binding", ""), index) == field.get("id"):
                downstream.add(node["id"])
    return len(upstream), len(downstream)


def summarize_node_validation_issues(
    node_id: str,
    issues_by_node: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    return dedupe_issue_summaries(
        [
            {
                "severity": "broken" if issue.get("severity") == "error" else "warning",
                "message": issue.get("message", ""),
            }
            for issue in issues_by_node.get(node_id, [])
            if not issue.get("column_ref") or issue.get("category") == "duplicate_contract_field"
        ]
    )


def rollup_health(
    bindings: list[dict[str, Any]],
    issues: list[dict[str, Any]],
) -> str:
    max_health = "healthy"
    for binding in bindings:
        max_health = max_severity(max_health, binding.get("health", "healthy"))
    for issue in issues:
        max_health = max_severity(max_health, issue.get("severity", "healthy"))
    return max_health


def max_severity(left: str, right: str) -> str:
    return left if HEALTH_ORDER.get(left, 0) >= HEALTH_ORDER.get(right, 0) else right


def format_source_ref(sources: list[dict[str, Any]]) -> str:
    if not sources:
        return ""
    return format_single_source_ref(sources[0])


def format_single_source_ref(source: dict[str, Any]) -> str:
    if source.get("column"):
        return f"{source['node_id']}.{source['column']}"
    if source.get("field"):
        return f"{source['node_id']}.{source['field']}"
    return source.get("node_id", "")


def validate_field_source(
    index: dict[str, Any],
    contract_node: dict[str, Any],
    field_name: str,
    source: dict[str, Any] | str,
) -> str | None:
    if isinstance(source, str):
        field_id = resolve_field_reference(source, index)
        if not field_id:
            return f"{contract_node['id']}.{field_name}: upstream binding missing ({source})."
        node_id = index["field_owner"].get(field_id, "")
        field_display = display_ref_for_field_id(field_id, {"nodes": list(index["nodes"].values()), "edges": list(index["edges"].values())}, index)
        if not node_id:
            return f"{contract_node['id']}.{field_name}: upstream binding missing ({source})."
        return None
    node_id = source.get("node_id")
    if node_id not in index["nodes"]:
        return f"{contract_node['id']}.{field_name}: upstream node missing ({node_id})."
    upstream_node = index["nodes"][node_id]
    column_name = source.get("column")
    if column_name and not find_column(upstream_node, column_name):
        return f"{contract_node['id']}.{field_name}: upstream column missing ({node_id}.{column_name})."
    upstream_field = source.get("field")
    if upstream_field:
        field_names = {field["name"] for field in upstream_node.get("contract", {}).get("fields", [])}
        if upstream_field not in field_names:
            return f"{contract_node['id']}.{field_name}: upstream field missing ({node_id}.{upstream_field})."
    return None


def describe_downstream_component_impacts(
    index: dict[str, Any],
    node_id: str,
    field_name: str,
    ui_roles: dict[str, str],
) -> list[str]:
    component_labels: list[str] = []
    seen_labels: set[str] = set()
    target_ref = f"{node_id}.{field_name}"
    target_field_id = resolve_field_reference(target_ref, index)

    for node in index["nodes"].values():
        if node["kind"] != "contract" or node.get("extension_type") != "ui":
            continue
        for field in node.get("contract", {}).get("fields", []):
            sources = field.get("sources", [])
            if not any(format_single_source_ref(source) == target_ref for source in sources) and resolve_field_reference(field.get("primary_binding", ""), index) != target_field_id:
                continue
            for label in resolve_impacted_component_labels(index, node["id"], ui_roles):
                if label not in seen_labels:
                    seen_labels.add(label)
                    component_labels.append(label)

    if not component_labels:
        return []
    if len(component_labels) == 1:
        return [f"{component_labels[0]} will not render"]
    if len(component_labels) <= UI_IMPACT_LIMIT:
        joined = ", ".join(component_labels[:-1]) + f", and {component_labels[-1]}"
        return [f"{joined} will not render"]
    visible = ", ".join(component_labels[:UI_IMPACT_LIMIT])
    remainder = len(component_labels) - UI_IMPACT_LIMIT
    return [f"{visible}, and {remainder} more components will not render"]


def resolve_impacted_component_labels(
    index: dict[str, Any],
    node_id: str,
    ui_roles: dict[str, str],
) -> list[str]:
    role = ui_roles.get(node_id, "")
    node = index["nodes"].get(node_id)
    if not node:
        return []
    if role == UI_ROLE_COMPONENT:
        return [node.get("label", node_id)]

    labels: list[str] = []
    queue = [node_id]
    seen: set[str] = set()
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        current_node = index["nodes"].get(current)
        if not current_node:
            continue
        current_role = ui_roles.get(current, "")
        if current_role == UI_ROLE_COMPONENT:
            labels.append(current_node.get("label", current))
            continue
        for edge in index["outgoing"].get(current, []):
            if edge["type"] != "contains":
                continue
            target_node = index["nodes"].get(edge["target"])
            if target_node and target_node.get("kind") == "contract" and target_node.get("extension_type") == "ui":
                queue.append(target_node["id"])

    return labels or [node.get("label", node_id)]


def format_binding_ref(
    *,
    graph_index: dict[str, Any],
    reference: str,
    fallback_sources: list[dict[str, Any]] | None = None,
) -> str:
    if reference:
        return display_ref_for_field_id(reference, {"nodes": list(graph_index["nodes"].values()), "edges": list(graph_index["edges"].values())}, graph_index)
    if fallback_sources:
        return format_source_ref(fallback_sources)
    return ""


def build_severity_rows(node: dict[str, Any], binding: dict[str, Any], *, severity: str) -> list[dict[str, Any]]:
    rows = []
    issues = binding.get("issues", [])
    if not issues and binding.get("why_this_matters"):
        issues = [{"message": binding["why_this_matters"]}]
    for issue in issues:
        message = issue.get("message") or ""
        why_this_matters = binding.get("why_this_matters") or ""
        rows.append(
            {
                "node_id": node["id"],
                "node_label": node.get("label", node["id"]),
                "binding_name": binding.get("name", ""),
                "severity": severity,
                "message": message,
                "why_this_matters": why_this_matters,
            }
        )
    return rows


def build_node_issue_rows(node: dict[str, Any], issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "node_id": node["id"],
            "node_label": node.get("label", node["id"]),
            "binding_name": "",
            "severity": issue.get("severity", "warning"),
            "message": issue.get("message", ""),
            "why_this_matters": "",
        }
        for issue in issues
    ]


def dedupe_issue_summaries(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for issue in issues:
        key = (issue.get("severity", ""), issue.get("message", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return deduped


def dedupe_severity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (
            row.get("node_id", ""),
            row.get("binding_name", ""),
            row.get("message", ""),
            row.get("why_this_matters", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def group_severity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row["node_id"]
        if key not in grouped:
            grouped[key] = {
                "node_id": row["node_id"],
                "node_label": row["node_label"],
                "issues": [],
            }
        grouped[key]["issues"].append(
            {
                "binding_name": row.get("binding_name", ""),
                "message": row.get("message", ""),
                "why_this_matters": row.get("why_this_matters", ""),
            }
        )
    return list(grouped.values())
