from __future__ import annotations

from pathlib import Path
from typing import Any


def summarize_planning_hints(root_dir: Path) -> dict[str, list[dict[str, Any]]]:
    from .structure_candidates import collect_document_candidates, combine_partial_graph_candidates, extract_sql_relation_tag

    doc_candidates = collect_document_candidates(root_dir, [])
    api_hints: list[dict[str, Any]] = []
    data_hints: list[dict[str, Any]] = []
    compute_hints: list[dict[str, Any]] = []

    for candidate in doc_candidates:
        if candidate.get("type") != "api_route":
            continue
        route = candidate.get("route", "")
        if not route:
            continue
        fields = candidate.get("fields", []) or []
        api_hints.append(
            {
                "id": _build_hint_id("plan-api", candidate.get("path", ""), route),
                "label": candidate.get("label", route),
                "route": route,
                "file": candidate.get("path", ""),
                "detected_from": candidate.get("source", "doc_spec"),
                "response_fields": list(fields),
                "required_fields": list(fields),
                "response_field_sources": [],
            }
        )

    partial_candidates = [candidate for candidate in doc_candidates if candidate.get("type") == "partial_graph"]
    if partial_candidates:
        plan_graph = combine_partial_graph_candidates(partial_candidates)
        for node in plan_graph.get("nodes", []):
            if node.get("removed"):
                continue
            if node["kind"] == "contract" and node.get("extension_type") == "api":
                route = node.get("contract", {}).get("route", "")
                if not route:
                    continue
                fields = [field for field in node.get("contract", {}).get("fields", []) if not field.get("removed")]
                api_hints.append(
                    {
                        "id": node["id"],
                        "label": node.get("label", route),
                        "route": route,
                        "file": "",
                        "detected_from": "plan_structure",
                        "response_fields": [field.get("name", "") for field in fields if field.get("name")],
                        "required_fields": [
                            field.get("name", "")
                            for field in fields
                            if field.get("name") and field.get("required", True)
                        ],
                        "response_field_sources": [
                            {
                                "name": field.get("name", ""),
                                "source_fields": [
                                    {
                                        "node_id": source.get("node_id", ""),
                                        "relation": extract_sql_relation_tag(
                                            next(
                                                (
                                                    candidate_node
                                                    for candidate_node in plan_graph.get("nodes", [])
                                                    if candidate_node["id"] == source.get("node_id", "")
                                                ),
                                                {},
                                            )
                                        ),
                                        "column": source.get("column", ""),
                                        "field": source.get("field", ""),
                                    }
                                    for source in field.get("sources", [])
                                ],
                            }
                            for field in fields
                            if field.get("name")
                        ],
                    }
                )
                continue
            if node["kind"] == "data":
                relation = extract_sql_relation_tag(node)
                if not relation:
                    continue
                data_hints.append(
                    {
                        "id": node["id"],
                        "label": node.get("label", relation),
                        "relation": relation,
                        "object_type": node.get("extension_type", "table"),
                        "detected_from": "plan_structure",
                        "fields": [
                            {
                                "name": column.get("name", ""),
                                "data_type": column.get("data_type", "unknown"),
                                "required": column.get("required", True),
                                "source_fields": resolve_plan_source_fields(plan_graph, column.get("lineage_inputs", [])),
                            }
                            for column in node.get("columns", [])
                            if column.get("name") and not column.get("removed")
                        ],
                    }
                )
                continue
            if node["kind"] != "compute":
                continue
            relation = extract_sql_relation_tag(node) or node.get("label", node["id"])
            compute_hints.append(
                {
                    "id": node["id"],
                    "label": node.get("label", relation),
                    "relation": relation,
                    "extension_type": node.get("extension_type", ""),
                    "detected_from": "plan_structure",
                    "inputs": list(node.get("compute", {}).get("inputs", [])),
                    "outputs": list(node.get("compute", {}).get("outputs", [])),
                    "fields": [
                        {
                            "name": column.get("name", ""),
                            "data_type": column.get("data_type", "unknown"),
                            "required": column.get("required", True),
                            "source_fields": resolve_plan_source_fields(plan_graph, column.get("lineage_inputs", [])),
                        }
                        for column in node.get("columns", [])
                        if column.get("name") and not column.get("removed")
                    ],
                }
            )

    return {
        "planning_api_hints": _dedupe_hint_list(api_hints, keys=("route", "detected_from")),
        "planning_data_hints": _dedupe_hint_list(data_hints, keys=("relation", "object_type")),
        "planning_compute_hints": _dedupe_hint_list(compute_hints, keys=("relation", "extension_type")),
    }


def resolve_plan_source_fields(plan_graph: dict[str, Any], lineage_inputs: list[dict[str, Any]]) -> list[dict[str, str]]:
    source_fields: list[dict[str, str]] = []
    field_lookup: dict[str, tuple[str, str]] = {}
    for node in plan_graph.get("nodes", []):
        for column in node.get("columns", []):
            if column.get("id"):
                field_lookup[column["id"]] = (node["id"], column.get("name", ""))
        for field in node.get("contract", {}).get("fields", []):
            if field.get("id"):
                field_lookup[field["id"]] = (node["id"], field.get("name", ""))

    for lineage in lineage_inputs or []:
        field_ref = lineage.get("field_id", "") if isinstance(lineage, dict) else str(lineage or "")
        if not field_ref:
            continue
        node_id = ""
        field_name = ""
        if field_ref in field_lookup:
            node_id, field_name = field_lookup[field_ref]
        elif "." in field_ref:
            node_id, field_name = field_ref.rsplit(".", 1)
        if not node_id or not field_name:
            continue
        source_node = next((candidate for candidate in plan_graph.get("nodes", []) if candidate["id"] == node_id), {})
        source_fields.append(
            {
                "node_id": node_id,
                "relation": extract_sql_relation_from_node(source_node),
                "column": field_name if source_node.get("kind") != "contract" else "",
                "field": field_name if source_node.get("kind") == "contract" else "",
            }
        )
    return source_fields


def extract_sql_relation_from_node(node: dict[str, Any]) -> str:
    for tag in node.get("tags", []) or []:
        if tag.startswith("sql_relation:"):
            return tag.split(":", 1)[1]
    return ""


def _dedupe_hint_list(items: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for item in items:
        key = tuple(str(item.get(part, "")) for part in keys)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _build_hint_id(prefix: str, relative_path: str, stable_value: str) -> str:
    path_slug = _humanize_asset_name(relative_path).replace(" ", "-").lower()
    value_slug = _humanize_asset_name(stable_value).replace(" ", "-").lower()
    return f"{prefix}:{path_slug}:{value_slug}"


def _humanize_asset_name(path: str) -> str:
    normalized = path.rstrip("/")
    if normalized.endswith("*.parquet"):
        normalized = normalized.rsplit("/", 2)[-2] if "/" in normalized else "parquet collection"
    else:
        filename = normalized.rsplit("/", 1)[-1]
        if filename.endswith(".csv.gz"):
            normalized = filename[:-7]
        elif filename.endswith(".zip"):
            normalized = filename[:-4]
        else:
            normalized = Path(filename).stem
    cleaned = normalized.replace("_", " ").replace("-", " ").strip()
    return cleaned.title() or "Imported Asset"
