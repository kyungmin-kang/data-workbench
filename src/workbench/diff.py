from __future__ import annotations

from collections import deque
from copy import deepcopy
from typing import Any

from .types import build_index, column_ref, deep_sort, display_ref_for_field_id, find_column, resolve_field_reference, slugify


def analyze_contracts(graph: dict[str, Any]) -> dict[str, dict[str, list[str]]]:
    index = build_index(graph)
    diagnostics: dict[str, dict[str, list[str]]] = {}
    for node in graph["nodes"]:
        if node["kind"] != "contract":
            continue

        missing_dependencies: list[str] = []
        unused_fields: list[str] = []
        breakage_warnings: list[str] = []
        contract_fields = node["contract"].get("fields", [])
        for field in contract_fields:
            field_name = field["name"]
            binding_refs = [field.get("primary_binding", ""), *field.get("alternatives", [])]
            sources = [ref for ref in binding_refs if ref] or field.get("sources", [])
            if not sources and field.get("required", True):
                missing_dependencies.append(f"{field_name}: no upstream source is defined.")
                continue
            for source in sources:
                message = _validate_field_source(index, node, field_name, source)
                if message:
                    missing_dependencies.append(message)
                    breakage_warnings.append(f"{field_name}: upstream dependency is missing.")

        if node["extension_type"] == "api":
            bound_ui_fields = _bound_ui_fields(index, node["id"])
            for field in contract_fields:
                if field["name"] not in bound_ui_fields:
                    unused_fields.append(field["name"])

        diagnostics[node["id"]] = {
            "missing_dependencies": sorted(set(missing_dependencies)),
            "unused_fields": sorted(set(unused_fields)),
            "breakage_warnings": sorted(set(breakage_warnings)),
        }
    return diagnostics


def generate_change_plan(old_graph: dict[str, Any], new_graph: dict[str, Any]) -> dict[str, Any]:
    old_index = build_index(old_graph)
    new_index = build_index(new_graph)
    contract_diagnostics = analyze_contracts(new_graph)

    diff = {
        "added_nodes": sorted(set(new_index["nodes"]) - set(old_index["nodes"])),
        "removed_nodes": sorted(set(old_index["nodes"]) - set(new_index["nodes"])),
        "changed_nodes": [],
        "added_columns": [],
        "removed_columns": [],
        "changed_contract_fields": [],
        "changed_feature_selection": [],
        "changed_source_dictionaries": [],
        "changed_source_raw_assets": [],
    }

    changed_node_ids: set[str] = set(diff["added_nodes"]) | set(diff["removed_nodes"])

    for node_id in sorted(set(old_index["nodes"]) & set(new_index["nodes"])):
        old_node = old_index["nodes"][node_id]
        new_node = new_index["nodes"][node_id]
        if _node_without_columns(old_node) != _node_without_columns(new_node):
            diff["changed_nodes"].append(node_id)
            changed_node_ids.add(node_id)

        old_columns = {column["name"]: column for column in old_node.get("columns", [])}
        new_columns = {column["name"]: column for column in new_node.get("columns", [])}
        for column_name in sorted(set(new_columns) - set(old_columns)):
            diff["added_columns"].append(column_ref(node_id, column_name))
            changed_node_ids.add(node_id)
        for column_name in sorted(set(old_columns) - set(new_columns)):
            diff["removed_columns"].append(column_ref(node_id, column_name))
            changed_node_ids.add(node_id)
        for column_name in sorted(set(old_columns) & set(new_columns)):
            if deep_sort(old_columns[column_name]) != deep_sort(new_columns[column_name]):
                diff["changed_nodes"].append(node_id)
                changed_node_ids.add(node_id)

        if old_node["kind"] == "contract" and new_node["kind"] == "contract":
            if deep_sort(old_node["contract"].get("fields", [])) != deep_sort(
                new_node["contract"].get("fields", [])
            ):
                diff["changed_contract_fields"].append(node_id)
                changed_node_ids.add(node_id)

        if old_node["kind"] == "compute" and new_node["kind"] == "compute":
            if deep_sort(old_node["compute"].get("feature_selection", [])) != deep_sort(
                new_node["compute"].get("feature_selection", [])
            ):
                diff["changed_feature_selection"].append(node_id)
                changed_node_ids.add(node_id)

        if old_node["kind"] == "source" and new_node["kind"] == "source":
            if deep_sort(old_node["source"].get("data_dictionaries", [])) != deep_sort(
                new_node["source"].get("data_dictionaries", [])
            ):
                diff["changed_source_dictionaries"].append(node_id)
                changed_node_ids.add(node_id)
            if deep_sort(old_node["source"].get("raw_assets", [])) != deep_sort(
                new_node["source"].get("raw_assets", [])
            ):
                diff["changed_source_raw_assets"].append(node_id)
                changed_node_ids.add(node_id)

    downstream_ids = _traverse(new_index, changed_node_ids, direction="downstream")
    upstream_ids = _traverse(new_index, changed_node_ids, direction="upstream")

    impacted_datasets = sorted(
        node_id
        for node_id in downstream_ids | changed_node_ids
        if node_id in new_index["nodes"] and new_index["nodes"][node_id]["kind"] == "data"
    )
    impacted_apis = sorted(
        node_id
        for node_id in downstream_ids | changed_node_ids
        if node_id in new_index["nodes"]
        and new_index["nodes"][node_id]["kind"] == "contract"
        and new_index["nodes"][node_id]["extension_type"] == "api"
    )

    tier_1 = {
        "breaking_changes": sorted(
            [f"Removed node: {node_id}" for node_id in diff["removed_nodes"]]
            + [
                f"Contract field changed: {node_id}"
                for node_id in diff["changed_contract_fields"]
            ]
        ),
        "removed_columns": sorted(diff["removed_columns"]),
        "contract_violations": sorted(
            violation
            for node_id, details in contract_diagnostics.items()
            for violation in details["missing_dependencies"] + details["breakage_warnings"]
        ),
    }
    tier_2 = {
        "impacted_datasets": impacted_datasets,
        "impacted_apis": impacted_apis,
    }
    tier_3 = {
        "upstream_ripple_effects": sorted(
            node_id
            for node_id in upstream_ids
            if node_id not in changed_node_ids and node_id in new_index["nodes"]
        ),
        "potential_file_targets": sorted(
            {
                "specs/workbench.graph.json",
                "specs/structure/spec.yaml",
                *(_file_targets_for_node(new_index["nodes"][node_id]) for node_id in changed_node_ids if node_id in new_index["nodes"]),
                *(_file_targets_for_node(new_index["nodes"][node_id]) for node_id in impacted_apis if node_id in new_index["nodes"]),
            }
        ),
    }

    return {
        "diff": diff,
        "tiers": {
            "tier_1": tier_1,
            "tier_2": tier_2,
            "tier_3": tier_3,
        },
        "markdown": render_plan_markdown({"tier_1": tier_1, "tier_2": tier_2, "tier_3": tier_3}),
        "contract_diagnostics": contract_diagnostics,
    }


def render_plan_markdown(tiers: dict[str, dict[str, list[str]]]) -> str:
    lines = [
        "# Save Plan",
        "",
        "## Tier 1 (must-read)",
        _render_section("Breaking changes", tiers["tier_1"]["breaking_changes"]),
        _render_section("Removed columns", tiers["tier_1"]["removed_columns"]),
        _render_section("Contract violations", tiers["tier_1"]["contract_violations"]),
        "",
        "## Tier 2 (important)",
        _render_section("Impacted datasets", tiers["tier_2"]["impacted_datasets"]),
        _render_section("Impacted APIs", tiers["tier_2"]["impacted_apis"]),
        "",
        "## Tier 3 (informational)",
        _render_section("Upstream ripple effects", tiers["tier_3"]["upstream_ripple_effects"]),
        _render_section("Potential file targets", tiers["tier_3"]["potential_file_targets"]),
    ]
    return "\n".join(lines).strip() + "\n"


def _render_section(title: str, items: list[str]) -> str:
    lines = [f"### {title}"]
    if not items:
        lines.append("- ✓ No issues")
    else:
        lines.extend(f"- {item}" for item in items)
    return "\n".join(lines)


def _validate_field_source(
    index: dict[str, Any],
    contract_node: dict[str, Any],
    field_name: str,
    source: dict[str, Any] | str,
) -> str | None:
    if isinstance(source, str):
        field_id = resolve_field_reference(source, index)
        if not field_id:
            return f"{contract_node['id']}.{field_name}: upstream binding missing ({source})."
        return None
    node_id = source.get("node_id")
    if node_id not in index["nodes"]:
        return f"{contract_node['id']}.{field_name}: upstream node missing ({node_id})."
    upstream_node = index["nodes"][node_id]
    column_name = source.get("column")
    if column_name:
        if not find_column(upstream_node, column_name):
            return (
                f"{contract_node['id']}.{field_name}: upstream column missing "
                f"({node_id}.{column_name})."
            )
    upstream_field = source.get("field")
    if upstream_field:
        field_names = {field["name"] for field in upstream_node.get("contract", {}).get("fields", [])}
        if upstream_field not in field_names:
            return (
                f"{contract_node['id']}.{field_name}: upstream field missing "
                f"({node_id}.{upstream_field})."
            )
    return None


def _bound_ui_fields(index: dict[str, Any], api_node_id: str) -> set[str]:
    bound_fields: set[str] = set()
    api_field_ids = {
        field["id"]
        for field in index["nodes"].get(api_node_id, {}).get("contract", {}).get("fields", [])
    }
    for node in index["nodes"].values():
        if node["kind"] != "contract" or node["extension_type"] != "ui":
            continue
        for field in node["contract"].get("fields", []):
            if resolve_field_reference(field.get("primary_binding", ""), index) in api_field_ids:
                source_ref = display_ref_for_field_id(field.get("primary_binding", ""), {"nodes": list(index["nodes"].values()), "edges": list(index["edges"].values())}, index)
                if "." in source_ref:
                    bound_fields.add(source_ref.rsplit(".", 1)[-1])
            for source in field.get("sources", []):
                if source.get("node_id") == api_node_id and source.get("field"):
                    bound_fields.add(source["field"])
    return bound_fields


def _node_without_columns(node: dict[str, Any]) -> dict[str, Any]:
    stripped = deepcopy(node)
    stripped["columns"] = [{"name": column["name"]} for column in node.get("columns", [])]
    return deep_sort(stripped)


def _traverse(index: dict[str, Any], seed: set[str], direction: str) -> set[str]:
    if not seed:
        return set()
    visited = set(seed)
    queue = deque(seed)
    while queue:
        current = queue.popleft()
        edges = index["outgoing"][current] if direction == "downstream" else index["incoming"][current]
        for edge in edges:
            neighbor = edge["target"] if direction == "downstream" else edge["source"]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    return visited


def _file_targets_for_node(node: dict[str, Any]) -> str:
    slug = slugify(node["id"])
    if node["kind"] == "source":
        return f"specs/sources/{slug}.json"
    if node["kind"] == "data":
        return f"specs/data/{slug}.json"
    if node["kind"] == "compute" and node["extension_type"] == "transform":
        return f"pipelines/transforms/{slug}.py"
    if node["kind"] == "compute" and node["extension_type"] == "model":
        return f"pipelines/models/{slug}.py"
    if node["kind"] == "contract" and node["extension_type"] == "api":
        return f"contracts/api/{slug}.json"
    return f"contracts/ui/{slug}.json"
