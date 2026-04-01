from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .structure_blueprints import (
    build_api_node_from_hint,
    build_sql_data_node_from_hint,
    build_ui_node_from_hint,
    collect_existing_field_ids,
    slugify_text,
)
from .structure_candidates import merge_partial_graph_node
from .types import column_ref, make_field_id, normalize_graph


MARKDOWN_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
MARKDOWN_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
MARKDOWN_ROUTE_LINE_RE = re.compile(r"^(GET|POST|PUT|PATCH|DELETE)\s+(/[\w\-/{}/.]+)$", re.IGNORECASE)
MARKDOWN_API_HEADING_RE = re.compile(
    r"^(?:api|route|endpoint)\s*:?\s*(GET|POST|PUT|PATCH|DELETE)\s+(/[\w\-/{}/.]+)$",
    re.IGNORECASE,
)
MARKDOWN_UI_HEADING_RE = re.compile(r"^(?:ui|component)\s*:?\s*(.+)$", re.IGNORECASE)
MARKDOWN_DATA_HEADING_RE = re.compile(
    r"^(?P<prefix>data|dataset|table|view|materialized view|materialized_view|relation)\s*:?\s*(?P<value>.+)$",
    re.IGNORECASE,
)
MARKDOWN_COMPUTE_HEADING_RE = re.compile(r"^(?P<prefix>compute|transform|model)\s*:?\s*(?P<value>.+)$", re.IGNORECASE)
MARKDOWN_LIST_PREFIX_RE = re.compile(r"^(?:[-*+]\s+|\d+\.\s+|\[[ xX]\]\s+)")


def parse_markdown_hybrid_plan(path: Path, text: str) -> dict[str, Any] | None:
    sanitized_text = MARKDOWN_CODE_FENCE_RE.sub("\n", text)
    sections = split_markdown_sections(sanitized_text)
    if not sections:
        return None

    graph = {"metadata": {"name": f"Hybrid Plan {path.name}"}, "nodes": [], "edges": []}
    found_structured_section = False
    compute_links: list[dict[str, Any]] = []

    for section in sections:
        descriptor = parse_markdown_section_descriptor(section["title"], section["body"])
        if descriptor is None:
            continue
        found_structured_section = True

        if descriptor["kind"] == "api":
            node = build_api_node_from_hint(
                {
                    "route": descriptor["route"],
                    "label": descriptor["label"],
                    "response_fields": [field["name"] for field in descriptor["fields"]],
                }
            )
            field_by_name = {field.get("name"): field for field in node.get("contract", {}).get("fields", [])}
            for field_spec in descriptor["fields"]:
                field = field_by_name.get(field_spec["name"])
                if field is None:
                    continue
                if field_spec["required"] is not None:
                    field["required"] = field_spec["required"]
                if field_spec["sources"]:
                    field["sources"] = []
                    for source in field_spec["sources"]:
                        ensure_markdown_relation_column(
                            graph,
                            source.get("relation", ""),
                            source.get("node_id", ""),
                            source.get("column", ""),
                        )
                        field["sources"].append(
                            {
                                "node_id": source.get("node_id", ""),
                                "column": source.get("column", ""),
                            }
                        )
            merge_partial_graph_node(graph, node)
            continue

        if descriptor["kind"] == "ui":
            node = build_ui_node_from_hint(
                {
                    "component": descriptor["component"],
                    "label": descriptor["label"],
                    "used_fields": [field["name"] for field in descriptor["fields"]],
                }
            )
            field_by_name = {field.get("name"): field for field in node.get("contract", {}).get("fields", [])}
            for field_spec in descriptor["fields"]:
                field = field_by_name.get(field_spec["name"])
                if field is not None and field_spec["required"] is not None:
                    field["required"] = field_spec["required"]
            merge_partial_graph_node(graph, node)
            continue

        if descriptor["kind"] == "data":
            node = build_sql_data_node_from_hint(
                {
                    "relation": descriptor["relation"],
                    "object_type": descriptor["object_type"],
                    "fields": [
                        {
                            "name": field["name"],
                            "data_type": field.get("data_type", "unknown"),
                        }
                        for field in descriptor["fields"]
                    ],
                }
            )
            column_by_name = {column.get("name"): column for column in node.get("columns", [])}
            for field_spec in descriptor["fields"]:
                column = column_by_name.get(field_spec["name"])
                if column is None:
                    continue
                lineage_inputs: list[dict[str, Any]] = []
                for source in field_spec["sources"]:
                    ensure_markdown_relation_column(
                        graph,
                        source.get("relation", ""),
                        source.get("node_id", ""),
                        source.get("column", ""),
                    )
                    if source.get("node_id") and source.get("column"):
                        lineage_inputs.append(
                            {
                                "field_id": column_ref(source["node_id"], source["column"]),
                                "role": "sql_input",
                            }
                        )
                if lineage_inputs:
                    column["lineage_inputs"] = lineage_inputs
            merge_partial_graph_node(graph, node)
            continue

        if descriptor["kind"] == "compute":
            relation = descriptor["relation"] or descriptor["label"]
            node = normalize_graph(
                {
                    "metadata": {"name": "Hybrid Plan"},
                    "nodes": [
                        {
                            "id": f"compute:{descriptor['extension_type']}.{slugify_text(relation)}",
                            "kind": "compute",
                            "extension_type": descriptor["extension_type"],
                            "label": descriptor["label"],
                            "description": "",
                            "tags": [f"sql_relation:{relation}", "plan_markdown"],
                            "columns": [
                                {
                                    "name": field["name"],
                                    "data_type": field.get("data_type", "unknown"),
                                    "required": True,
                                    "lineage_inputs": [
                                        {
                                            "field_id": column_ref(source["node_id"], source["column"]),
                                            "role": "sql_input",
                                        }
                                        for source in field["sources"]
                                        if source.get("node_id") and source.get("column")
                                    ],
                                }
                                for field in descriptor["fields"]
                            ],
                            "source": {},
                            "data": {},
                            "compute": {
                                "runtime": "sql" if descriptor["extension_type"] == "transform" else descriptor["extension_type"],
                                "inputs": [],
                                "outputs": [],
                                "notes": "",
                                "feature_selection": [],
                                "column_mappings": [],
                            },
                            "contract": {},
                        }
                    ],
                    "edges": [],
                }
            )["nodes"][0]
            column_by_name = {column.get("name"): column for column in node.get("columns", [])}
            for field_spec in descriptor["fields"]:
                for source in field_spec["sources"]:
                    ensure_markdown_relation_column(
                        graph,
                        source.get("relation", ""),
                        source.get("node_id", ""),
                        source.get("column", ""),
                    )
                column = column_by_name.get(field_spec["name"])
                if column is None:
                    continue
                column["lineage_inputs"] = [
                    {
                        "field_id": column_ref(source["node_id"], source["column"]),
                        "role": "sql_input",
                    }
                    for source in field_spec["sources"]
                    if source.get("node_id") and source.get("column")
                ]
            merge_partial_graph_node(graph, node)
            compute_links.append(
                {
                    "node_id": node["id"],
                    "inputs": descriptor["inputs"],
                    "outputs": descriptor["outputs"],
                }
            )

    if not found_structured_section or not graph.get("nodes"):
        return None

    for link in compute_links:
        compute_node = next((node for node in graph.get("nodes", []) if node["id"] == link["node_id"]), None)
        if compute_node is None:
            continue
        compute_inputs: list[str] = []
        compute_outputs: list[str] = []
        for raw_input in link["inputs"]:
            reference = parse_markdown_node_reference(raw_input)
            if reference is None:
                continue
            target_id = ensure_markdown_relation_node(graph, reference.get("relation", ""), reference.get("node_id", ""))
            if target_id:
                compute_inputs.append(target_id)
        for raw_output in link["outputs"]:
            reference = parse_markdown_node_reference(raw_output)
            if reference is None:
                continue
            target_id = ensure_markdown_relation_node(graph, reference.get("relation", ""), reference.get("node_id", ""))
            if target_id:
                compute_outputs.append(target_id)
        compute_node.setdefault("compute", {})["inputs"] = sorted(set(compute_inputs))
        compute_node["compute"]["outputs"] = sorted(set(compute_outputs))
        for upstream_node_id in compute_node["compute"]["inputs"]:
            ensure_markdown_edge(graph, "depends_on", upstream_node_id, compute_node["id"])
        for output_node_id in compute_node["compute"]["outputs"]:
            ensure_markdown_edge(graph, "produces", compute_node["id"], output_node_id)

    return normalize_graph(graph)


def split_markdown_sections(text: str) -> list[dict[str, Any]]:
    matches = list(MARKDOWN_HEADING_RE.finditer(text))
    sections: list[dict[str, Any]] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        sections.append({"level": len(match.group(1)), "title": normalize_markdown_value(match.group(2)), "body": text[start:end].strip()})
    return sections


def parse_markdown_section_descriptor(title: str, body: str) -> dict[str, Any] | None:
    api_match = MARKDOWN_API_HEADING_RE.match(title)
    if api_match:
        route = f"{api_match.group(1).upper()} {api_match.group(2)}"
        return {"kind": "api", "route": route, "label": route, "fields": extract_markdown_field_specs(body, contract_kind="contract")}

    route = extract_markdown_route(title) or extract_markdown_route(body)
    if route:
        return {"kind": "api", "route": route, "label": route, "fields": extract_markdown_field_specs(body, contract_kind="contract")}

    ui_match = MARKDOWN_UI_HEADING_RE.match(title)
    if ui_match:
        component = normalize_markdown_value(ui_match.group(1))
        return {"kind": "ui", "component": component, "label": component, "fields": extract_markdown_field_specs(body, contract_kind="contract")}

    data_match = MARKDOWN_DATA_HEADING_RE.match(title)
    if data_match:
        prefix = data_match.group("prefix").lower().replace("_", " ")
        relation = normalize_markdown_value(data_match.group("value"))
        object_type = "materialized_view" if "materialized" in prefix else "view" if prefix == "view" else "table"
        return {"kind": "data", "relation": relation, "object_type": object_type, "fields": extract_markdown_field_specs(body, contract_kind="data")}

    compute_match = MARKDOWN_COMPUTE_HEADING_RE.match(title)
    if compute_match:
        prefix = compute_match.group("prefix").lower()
        label = normalize_markdown_value(compute_match.group("value"))
        outputs = extract_markdown_reference_values(body, ("output", "outputs", "produces", "writes", "targets", "emits"))
        inferred_relation = label
        if outputs:
            reference = parse_markdown_node_reference(outputs[0])
            if reference and reference.get("relation"):
                inferred_relation = reference["relation"]
        return {
            "kind": "compute",
            "label": label,
            "relation": inferred_relation,
            "extension_type": "model" if prefix == "model" else "transform",
            "fields": extract_markdown_field_specs(body, contract_kind="data"),
            "inputs": extract_markdown_reference_values(body, ("input", "inputs", "reads", "sources", "depends on", "depends_on", "upstream", "upstreams", "consumes", "requires")),
            "outputs": outputs,
        }

    return None


def extract_markdown_route(text: str) -> str:
    for raw_line in text.splitlines():
        line = normalize_markdown_value(MARKDOWN_LIST_PREFIX_RE.sub("", raw_line.strip()))
        match = MARKDOWN_ROUTE_LINE_RE.match(line)
        if match:
            return f"{match.group(1).upper()} {match.group(2)}"
    return ""


def extract_markdown_field_specs(body: str, *, contract_kind: str) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    prefixes = (
        "response schema",
        "response",
        "response fields",
        "output fields",
        "output columns",
        "input fields",
        "input columns",
        "schema",
        "returns",
        "exposes",
        "required fields",
        "fields",
        "columns",
        "bindings",
        "lineage",
    )
    skipped_prefixes = ("inputs", "outputs", "produces", "writes", "reads", "sources", "depends on", "upstream")
    for raw_line in body.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        content = normalize_markdown_value(MARKDOWN_LIST_PREFIX_RE.sub("", stripped))
        lowered = content.lower()
        if any(lowered.startswith(f"{prefix}:") for prefix in skipped_prefixes):
            continue
        items: list[str] = []
        matched_prefix = next((prefix for prefix in prefixes if lowered.startswith(f"{prefix}:")), "")
        if matched_prefix:
            items = split_markdown_inline_items(content.split(":", 1)[1])
        elif MARKDOWN_LIST_PREFIX_RE.match(stripped):
            items = [content]
        for item in items:
            spec = parse_markdown_field_spec(item, contract_kind=contract_kind)
            if spec is None or spec["name"] in seen_names:
                continue
            seen_names.add(spec["name"])
            specs.append(spec)
    return specs


def extract_markdown_reference_values(body: str, labels: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for raw_line in body.splitlines():
        content = normalize_markdown_value(MARKDOWN_LIST_PREFIX_RE.sub("", raw_line.strip()))
        if not content:
            continue
        lowered = content.lower()
        for label in labels:
            if not lowered.startswith(f"{label}:"):
                continue
            for item in split_markdown_inline_items(content.split(":", 1)[1]):
                if item and item not in seen:
                    seen.add(item)
                    values.append(item)
    return values


def parse_markdown_field_spec(value: str, *, contract_kind: str) -> dict[str, Any] | None:
    text = normalize_markdown_value(value)
    if not text or text.lower() in {"none", "n/a"}:
        return None

    binding_text = ""
    marker = next((item for item in ("<-", "->") if item in text), "")
    if marker:
        left, right = [item.strip() for item in text.split(marker, 1)]
        text = left
        binding_text = right
    else:
        binding_match = re.split(
            r"\s+(?:from|maps?\s+to|bound\s+to|binds\s+to|sourced\s+from|source(?:d)?\s+from|using)\s+",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        if len(binding_match) == 2:
            text, binding_text = [item.strip() for item in binding_match]

    lowered = text.lower()
    required = None
    if "optional" in lowered:
        required = False
    elif "required" in lowered:
        required = True
    elif contract_kind == "contract":
        required = True

    text = re.sub(r"\[(?:required|optional)\]|\((?:required|optional)\)|\brequired\b|\boptional\b", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"^(?:field|column|property)\s+", "", text, flags=re.IGNORECASE).strip()
    type_match = re.match(r"^(?P<name>[A-Za-z_][\w.-]*)\s*\((?P<data_type>[A-Za-z0-9_<>\[\]|?.-]+)\)$", text)
    if type_match is None:
        type_match = re.match(r"^(?P<name>[A-Za-z_][\w.-]*)\s*:\s*(?P<data_type>[A-Za-z0-9_<>\[\]|?.-]+)$", text)
    if type_match is None:
        type_match = re.match(r"^(?P<name>[A-Za-z_][\w.-]*)\s*=\s*(?P<data_type>[A-Za-z0-9_<>\[\]|?.-]+)$", text)

    if type_match is not None:
        name = type_match.group("name")
        data_type = type_match.group("data_type")
    else:
        name_match = re.match(r"^(?P<name>[A-Za-z_][\w.-]*)$", text)
        if name_match is None:
            return None
        name = name_match.group("name")
        data_type = "unknown"

    sources = [source for source in (parse_markdown_binding_source(item) for item in split_markdown_inline_items(binding_text)) if source is not None]
    return {"name": name, "required": required, "data_type": data_type, "sources": sources}


def split_markdown_inline_items(value: str) -> list[str]:
    if not value:
        return []
    return [normalize_markdown_value(item) for item in re.split(r"[;,]|\s+\band\b\s+", value, flags=re.IGNORECASE) if normalize_markdown_value(item)]


def parse_markdown_binding_source(value: str) -> dict[str, str] | None:
    text = normalize_markdown_value(value)
    if not text or text.lower() in {"none", "n/a"}:
        return None
    if text.startswith("sql:"):
        text = text[4:]
    text = re.sub(
        r"^(?:from|source|sources|binds?\s+to|maps?\s+to|using|table|view|dataset|relation|data|column|field)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    if "->" in text:
        text = normalize_markdown_value(text.split("->", 1)[1])
    text = text.replace("::", ".").replace("#", ".")
    if text.startswith("column_ref(") and text.endswith(")"):
        inner = text[len("column_ref(") : -1]
        node_id, _, column_name = inner.partition(",")
        node_id = normalize_markdown_value(node_id)
        column_name = normalize_markdown_value(column_name)
        if node_id and column_name:
            return {"node_id": node_id, "column": column_name}
        return None
    if ":" in text.split(".", 1)[0]:
        node_id, separator, column_name = text.rpartition(".")
        if separator and node_id and column_name:
            return {"node_id": node_id, "column": column_name}
        return {"node_id": text}
    relation, separator, column_name = text.rpartition(".")
    if separator and relation and column_name:
        return {"relation": relation, "node_id": f"data:{slugify_text(relation)}", "column": column_name}
    return None


def parse_markdown_node_reference(value: str) -> dict[str, str] | None:
    text = normalize_markdown_value(value)
    if not text or text.lower() in {"none", "n/a"}:
        return None
    if text.startswith("sql:"):
        text = text[4:]
    text = re.sub(
        r"^(?:table|view|dataset|relation|data|compute|transform|model|output|outputs|input|inputs)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    if "->" in text:
        text = normalize_markdown_value(text.split("->", 1)[-1])
    text = text.replace("::", ".").replace("#", ".")
    if ":" in text.split(".", 1)[0]:
        return {"node_id": text}
    return {"relation": text, "node_id": f"data:{slugify_text(text)}"}


def normalize_markdown_value(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized.startswith("`") and normalized.endswith("`"):
        normalized = normalized[1:-1]
    return normalized.strip().strip("`").strip()


def ensure_markdown_relation_node(graph: dict[str, Any], relation: str, node_id: str, *, object_type: str = "table") -> str:
    if node_id and any(node.get("id") == node_id for node in graph.get("nodes", [])):
        return node_id
    if relation:
        candidate = build_sql_data_node_from_hint({"relation": relation, "object_type": object_type, "fields": []})
        merge_partial_graph_node(graph, candidate)
        return candidate["id"]
    if not node_id:
        return ""
    candidate = normalize_graph(
        {
            "metadata": {"name": "Hybrid Plan"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "data",
                    "extension_type": "table",
                    "label": node_id.split(":", 1)[-1].replace("_", " ").title(),
                    "columns": [],
                    "source": {},
                    "data": {"persistence": "hot", "persisted": True},
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]
    merge_partial_graph_node(graph, candidate)
    return candidate["id"]


def ensure_markdown_relation_column(graph: dict[str, Any], relation: str, node_id: str, column_name: str) -> None:
    if not column_name:
        ensure_markdown_relation_node(graph, relation, node_id)
        return
    resolved_node_id = ensure_markdown_relation_node(graph, relation, node_id)
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == resolved_node_id), None)
    if node is None:
        return
    if any(column.get("name") == column_name for column in node.get("columns", [])):
        return
    existing_ids = collect_existing_field_ids(graph)
    node.setdefault("columns", []).append({"id": make_field_id(node["id"], column_name, existing_ids), "name": column_name, "data_type": "unknown"})


def ensure_markdown_edge(graph: dict[str, Any], edge_type: str, source_id: str, target_id: str) -> None:
    if not source_id or not target_id:
        return
    if not any(node.get("id") == source_id for node in graph.get("nodes", [])):
        return
    if not any(node.get("id") == target_id for node in graph.get("nodes", [])):
        return
    edge_id = f"edge.{edge_type}.{slugify_text(source_id)}.{slugify_text(target_id)}"
    if any(edge.get("id") == edge_id for edge in graph.get("edges", [])):
        return
    graph.setdefault("edges", []).append(
        {
            "id": edge_id,
            "type": edge_type,
            "source": source_id,
            "target": target_id,
            "state": "proposed",
            "confidence": "low",
            "evidence": ["plan_markdown"],
        }
    )


__all__ = [
    "parse_markdown_hybrid_plan",
]
