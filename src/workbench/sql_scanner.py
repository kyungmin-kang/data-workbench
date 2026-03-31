from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


SQL_IDENTIFIER_PART_PATTERN = r'(?:[A-Za-z_][A-Za-z0-9_]*|"[^"]+"|`[^`]+`|\[[^\]]+\])'
SQL_IDENTIFIER_PATTERN = rf"{SQL_IDENTIFIER_PART_PATTERN}(?:\s*\.\s*{SQL_IDENTIFIER_PART_PATTERN})*"
SQL_CREATE_TABLE_RE = re.compile(
    rf"^create\s+table\s+(?:if\s+not\s+exists\s+)?(?P<name>{SQL_IDENTIFIER_PATTERN})\s*\((?P<body>.*)\)$",
    re.IGNORECASE | re.DOTALL,
)
SQL_CREATE_VIEW_RE = re.compile(
    rf"^create\s+(?:or\s+replace\s+)?(?P<kind>materialized\s+view|view)\s+(?:if\s+not\s+exists\s+)?(?P<name>{SQL_IDENTIFIER_PATTERN})\s+as\s+(?P<select>.+)$",
    re.IGNORECASE | re.DOTALL,
)
SQL_INLINE_REFERENCE_RE = re.compile(
    rf"\breferences\s+(?P<relation>{SQL_IDENTIFIER_PATTERN})\s*(?:\(\s*(?P<column>{SQL_IDENTIFIER_PART_PATTERN})\s*\))?",
    re.IGNORECASE,
)
SQL_TABLE_FK_RE = re.compile(
    rf"foreign\s+key\s*\((?P<local>{SQL_IDENTIFIER_PART_PATTERN})\)\s+references\s+(?P<relation>{SQL_IDENTIFIER_PATTERN})\s*\((?P<column>{SQL_IDENTIFIER_PART_PATTERN})\)",
    re.IGNORECASE,
)
QUALIFIED_COLUMN_RE = re.compile(rf"({SQL_IDENTIFIER_PART_PATTERN})\s*\.\s*({SQL_IDENTIFIER_PART_PATTERN})")
BARE_IDENTIFIER_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
QUOTED_IDENTIFIER_RE = re.compile(r'"([^"]+)"|`([^`]+)`|\[([^\]]+)\]')
FUNCTION_NAME_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")
TRAILING_ALIAS_RE = re.compile(rf"\bas\s+({SQL_IDENTIFIER_PART_PATTERN})\s*$", re.IGNORECASE)
TRAILING_IDENTIFIER_RE = re.compile(rf"({SQL_IDENTIFIER_PART_PATTERN})\s*$")
RAW_IDENTIFIER_RE = re.compile(rf"^{SQL_IDENTIFIER_PATTERN}$")
QUERY_CLAUSE_KEYWORDS = (
    "where",
    "group by",
    "having",
    "order by",
    "limit",
    "qualify",
    "window",
    "union",
    "except",
    "intersect",
)
JOIN_KEYWORDS = (
    "join",
    "left join",
    "left outer join",
    "right join",
    "right outer join",
    "inner join",
    "full join",
    "full outer join",
    "cross join",
)
PROJECTION_RESERVED_WORDS = {
    "all",
    "and",
    "as",
    "asc",
    "by",
    "case",
    "cast",
    "coalesce",
    "count",
    "desc",
    "distinct",
    "else",
    "end",
    "false",
    "from",
    "if",
    "in",
    "is",
    "join",
    "left",
    "max",
    "min",
    "not",
    "null",
    "on",
    "order",
    "or",
    "over",
    "partition",
    "right",
    "row_number",
    "select",
    "sum",
    "then",
    "true",
    "when",
}
SET_OPERATION_KEYWORDS = ("union all", "union", "except", "intersect")


@dataclass
class RelationBinding:
    relation: str
    fields_by_name: dict[str, dict[str, Any]] = field(default_factory=dict)
    upstream_relations: set[str] = field(default_factory=set)


@dataclass
class QueryAnalysis:
    fields: list[dict[str, Any]]
    upstream_relations: list[str]


def scan_sql_structure_hints(relative_path: str, text: str) -> list[dict[str, Any]]:
    statements = split_sql_statements(strip_sql_comments(text))
    known_relations: dict[str, list[dict[str, Any]]] = {}
    hints: list[dict[str, Any]] = []

    for statement in statements:
        normalized_statement = statement.strip().rstrip(";").strip()
        if not normalized_statement:
            continue
        table_match = SQL_CREATE_TABLE_RE.match(normalized_statement)
        if table_match:
            relation = normalize_sql_identifier(table_match.group("name") or "")
            fields = parse_sql_table_columns(table_match.group("body") or "")
            if relation:
                known_relations[relation] = [dict(field) for field in fields]
                hints.append(
                    {
                        "id": build_hint_id("sql", relative_path, relation),
                        "label": relation,
                        "relation": relation,
                        "object_type": "table",
                        "file": relative_path,
                        "detected_from": "sql_create_table",
                        "description": f"Detected from {relative_path}",
                        "fields": fields,
                        "upstream_relations": sorted(
                            {
                                source_field["relation"]
                                for field in fields
                                for source_field in field.get("source_fields", [])
                                if source_field.get("relation")
                            }
                        ),
                        "confidence": "high",
                        "evidence": ["static_analysis", "code_reference"],
                    }
                )
            continue

        view_match = SQL_CREATE_VIEW_RE.match(normalized_statement)
        if not view_match:
            continue
        relation = normalize_sql_identifier(view_match.group("name") or "")
        select_sql = view_match.group("select") or ""
        object_type = "materialized_view" if "materialized" in (view_match.group("kind") or "").lower() else "view"
        analysis = analyze_select_query(select_sql, known_relations)
        if not relation:
            continue
        known_relations[relation] = [dict(field) for field in analysis.fields]
        hints.append(
            {
                "id": build_hint_id("sql", relative_path, relation),
                "label": relation,
                "relation": relation,
                "object_type": object_type,
                "file": relative_path,
                "detected_from": "sql_create_view",
                "description": f"Detected from {relative_path}",
                "fields": analysis.fields,
                "upstream_relations": analysis.upstream_relations,
                "confidence": aggregate_confidence(analysis.fields, default="medium"),
                "evidence": ["static_analysis", "code_reference"],
            }
        )

    return hints


def parse_sql_table_columns(body: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    fields_by_name: dict[str, dict[str, Any]] = {}
    for entry in split_sql_top_level(body):
        stripped = entry.strip().strip(",")
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered.startswith(("primary key", "constraint", "unique", "index", "key", "check")):
            continue
        if lowered.startswith("foreign key"):
            match = SQL_TABLE_FK_RE.search(stripped)
            if not match:
                continue
            field = fields_by_name.get(normalize_sql_identifier(match.group("local") or ""))
            if not field:
                continue
            relation = normalize_sql_identifier(match.group("relation") or "")
            column = normalize_sql_identifier(match.group("column") or "")
            if relation and column:
                add_source_field(field, relation, column)
                field["foreign_key"] = f"{relation}.{column}"
            continue

        column_identifier, cursor = consume_sql_identifier(stripped)
        if not column_identifier:
            continue
        column_name = normalize_sql_identifier(column_identifier)
        if not column_name:
            continue
        data_type_token, _ = consume_sql_type_token(stripped, cursor)
        if not data_type_token:
            continue
        data_type = normalize_sql_type(data_type_token)
        field = {
            "name": column_name,
            "data_type": data_type or "unknown",
            "source_fields": [],
            "confidence": "high",
            "evidence": ["static_analysis", "code_reference"],
        }
        if "primary key" in lowered:
            field["primary_key"] = True
        if "not null" in lowered:
            field["nullable"] = False
        elif " null" in lowered:
            field["nullable"] = True
        if " unique" in f" {lowered} ":
            field["unique"] = True
        if " index" in f" {lowered} ":
            field["index"] = True

        inline_reference = SQL_INLINE_REFERENCE_RE.search(stripped)
        if inline_reference:
            relation = normalize_sql_identifier(inline_reference.group("relation") or "")
            column = normalize_sql_identifier(inline_reference.group("column") or "")
            if relation:
                add_source_field(field, relation, column)
                if column:
                    field["foreign_key"] = f"{relation}.{column}"

        fields.append(field)
        fields_by_name[column_name] = field
    return fields


def analyze_select_query(
    select_sql: str,
    known_relations: dict[str, list[dict[str, Any]]],
    *,
    projection_names: list[str] | None = None,
) -> QueryAnalysis:
    stripped_sql = strip_wrapping_parentheses(select_sql.strip().rstrip(";").strip())
    set_operation_branches = split_top_level_set_operations(stripped_sql)
    if len(set_operation_branches) > 1:
        analyses = [
            analyze_select_query(branch, known_relations, projection_names=projection_names)
            for branch in set_operation_branches
        ]
        return merge_set_operation_analyses(analyses)
    cte_bindings, main_sql = parse_cte_bindings(stripped_sql, known_relations)
    if not main_sql:
        return QueryAnalysis(fields=[], upstream_relations=[])
    select_start = find_top_level_keyword(main_sql, "select")
    if select_start == -1:
        return QueryAnalysis(fields=[], upstream_relations=[])
    from_index = find_top_level_keyword(main_sql, "from", start=select_start + len("select"))
    if from_index == -1:
        select_list = main_sql[select_start + len("select") :]
        relation_bindings: dict[str, RelationBinding] = {}
    else:
        select_list = main_sql[select_start + len("select") : from_index]
        clause_end = len(main_sql)
        for keyword in QUERY_CLAUSE_KEYWORDS:
            index = find_top_level_keyword(main_sql, keyword, start=from_index + len("from"))
            if index != -1:
                clause_end = min(clause_end, index)
        relation_bindings = parse_relation_bindings(
            main_sql[from_index + len("from") : clause_end],
            known_relations,
            cte_bindings,
        )

    projections: list[dict[str, Any]] = []
    select_body = drop_select_modifiers(select_list)
    for index, expression in enumerate(split_sql_top_level(select_body)):
        projections.extend(
            parse_projection_expression(
                expression,
                relation_bindings,
                fallback_name=projection_names[index] if projection_names and index < len(projection_names) else "",
            )
        )

    return QueryAnalysis(
        fields=projections,
        upstream_relations=sorted(
            {
                relation
                for binding in relation_bindings.values()
                for relation in binding.upstream_relations
                if relation
            }
        ),
    )


def parse_cte_bindings(
    sql: str,
    known_relations: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, RelationBinding], str]:
    stripped = sql.lstrip()
    if not stripped[:4].lower() == "with":
        return {}, sql
    cursor = 4
    if stripped[cursor:].lstrip().lower().startswith("recursive"):
        cursor = len(stripped) - len(stripped[cursor:].lstrip()) + cursor + len("recursive")
    bindings: dict[str, RelationBinding] = {}

    while cursor < len(stripped):
        cursor = skip_sql_whitespace(stripped, cursor)
        cte_name_raw, consumed = consume_sql_identifier(stripped[cursor:])
        if not cte_name_raw:
            return bindings, stripped[cursor:].lstrip()
        cte_name = normalize_sql_identifier(cte_name_raw)
        cursor += consumed
        cursor = skip_sql_whitespace(stripped, cursor)
        column_aliases: list[str] = []
        if cursor < len(stripped) and stripped[cursor] == "(":
            alias_list_end = find_matching_paren(stripped, cursor)
            if alias_list_end == -1:
                return bindings, stripped[cursor:].lstrip()
            column_aliases = extract_cte_column_aliases(stripped[cursor + 1 : alias_list_end])
            cursor = alias_list_end + 1
            cursor = skip_sql_whitespace(stripped, cursor)
        if not stripped[cursor : cursor + 2].lower() == "as":
            return bindings, stripped[cursor:].lstrip()
        cursor += 2
        cursor = skip_sql_whitespace(stripped, cursor)
        if cursor >= len(stripped) or stripped[cursor] != "(":
            return bindings, stripped[cursor:].lstrip()
        end_index = find_matching_paren(stripped, cursor)
        if end_index == -1:
            return bindings, stripped[cursor:].lstrip()
        analysis = analyze_select_query(stripped[cursor + 1 : end_index], known_relations, projection_names=column_aliases or None)
        cte_fields = apply_cte_column_aliases(analysis.fields, column_aliases)
        bindings[cte_name] = RelationBinding(
            relation=cte_name,
            fields_by_name={field["name"]: dict(field) for field in cte_fields if field.get("name")},
            upstream_relations=set(analysis.upstream_relations),
        )
        cursor = skip_sql_whitespace(stripped, end_index + 1)
        if cursor < len(stripped) and stripped[cursor] == ",":
            cursor += 1
            continue
        return bindings, stripped[cursor:].lstrip()
    return bindings, ""


def parse_relation_bindings(
    from_clause: str,
    known_relations: dict[str, list[dict[str, Any]]],
    cte_bindings: dict[str, RelationBinding],
) -> dict[str, RelationBinding]:
    bindings: dict[str, RelationBinding] = {}
    segments = split_relation_segments(from_clause)
    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue
        binding, alias = parse_relation_segment(segment, known_relations, cte_bindings)
        if not binding or not alias:
            continue
        bindings[alias] = binding
        if binding.relation:
            short_name = normalize_sql_identifier(binding.relation.split(".")[-1])
            if short_name and short_name not in bindings:
                bindings[short_name] = binding
    return bindings


def split_relation_segments(from_clause: str) -> list[str]:
    positions = [
        index
        for keyword in ("from", *JOIN_KEYWORDS)
        if (index := find_top_level_keyword(f"from {from_clause}", keyword)) != -1
    ]
    if not positions:
        return [from_clause]
    sql = f"from {from_clause}"
    ordered = sorted(set(positions))
    segments: list[str] = []
    for idx, start in enumerate(ordered):
        end = ordered[idx + 1] if idx + 1 < len(ordered) else len(sql)
        keyword = next(keyword for keyword in ("from", *JOIN_KEYWORDS) if sql[start :].lower().startswith(keyword))
        segment_body = sql[start + len(keyword) : end].strip()
        if segment_body:
            segments.append(segment_body)
    return segments


def parse_relation_segment(
    segment: str,
    known_relations: dict[str, list[dict[str, Any]]],
    cte_bindings: dict[str, RelationBinding],
) -> tuple[RelationBinding | None, str]:
    relation_part = segment
    for stop_keyword in (" on ", " using "):
        index = find_top_level_fragment(relation_part, stop_keyword)
        if index != -1:
            relation_part = relation_part[:index]
            break
    relation_part = relation_part.strip()
    if not relation_part:
        return None, ""
    if relation_part.startswith("("):
        end_index = find_matching_paren(relation_part, 0)
        if end_index == -1:
            return None, ""
        inner_query = relation_part[1:end_index]
        alias = extract_relation_alias(relation_part[end_index + 1 :])
        if not alias:
            return None, ""
        analysis = analyze_select_query(inner_query, known_relations)
        return (
            RelationBinding(
                relation=alias,
                fields_by_name={field["name"]: dict(field) for field in analysis.fields if field.get("name")},
                upstream_relations=set(analysis.upstream_relations),
            ),
            alias,
        )

    relation_name_raw, consumed = consume_sql_identifier(relation_part)
    if not relation_name_raw:
        return None, ""
    relation_name = normalize_sql_identifier(relation_name_raw)
    alias = extract_relation_alias(relation_part[consumed:]) or normalize_sql_identifier(relation_name.split(".")[-1])
    if not relation_name:
        return None, ""
    if relation_name in cte_bindings:
        return cte_bindings[relation_name], alias
    relation_fields = known_relations.get(relation_name, [])
    return (
        RelationBinding(
            relation=relation_name,
            fields_by_name={field["name"]: dict(field) for field in relation_fields if field.get("name")},
            upstream_relations={relation_name},
        ),
        alias,
    )


def extract_relation_alias(remainder: str) -> str:
    stripped = remainder.strip()
    if not stripped:
        return ""
    lowered = stripped.lower()
    if lowered.startswith("as "):
        identifier, _ = consume_sql_identifier(stripped[3:])
        return normalize_sql_identifier(identifier)
    if lowered.startswith("lateral "):
        identifier, _ = consume_sql_identifier(stripped[8:])
        return normalize_sql_identifier(identifier)
    identifier, _ = consume_sql_identifier(stripped)
    return normalize_sql_identifier(identifier)


def parse_projection_expression(
    expression: str,
    relation_bindings: dict[str, RelationBinding],
    *,
    fallback_name: str = "",
) -> list[dict[str, Any]]:
    stripped = expression.strip()
    if not stripped:
        return []
    wildcard_fields = expand_wildcard_projection(stripped, relation_bindings)
    if wildcard_fields:
        return wildcard_fields

    output_name = fallback_name or infer_projection_name(stripped)
    if not output_name:
        return []

    source_fields, unresolved_sources = resolve_expression_sources(
        stripped,
        output_name=output_name,
        relation_bindings=relation_bindings,
    )
    binding_field = resolve_projection_binding_field(stripped, relation_bindings)
    data_type = infer_projection_type(stripped)
    if binding_field and data_type == "unknown":
        data_type = binding_field.get("data_type", "unknown")
    inferred_confidence = projection_confidence(
        expression=stripped,
        source_fields=source_fields,
        unresolved_sources=unresolved_sources,
    )
    if binding_field and not source_fields and not unresolved_sources:
        confidence = binding_field.get("confidence", "medium")
    elif binding_field:
        confidence = merge_projection_confidence(
            inferred_confidence,
            binding_field.get("confidence", inferred_confidence),
        )
    else:
        confidence = inferred_confidence
    field: dict[str, Any] = {
        "name": output_name,
        "data_type": data_type,
        "source_fields": source_fields,
        "confidence": confidence,
        "evidence": ["static_analysis", "code_reference"],
    }
    if unresolved_sources:
        field["unresolved_sources"] = unresolved_sources
    return [field]


def expand_wildcard_projection(
    expression: str,
    relation_bindings: dict[str, RelationBinding],
) -> list[dict[str, Any]]:
    stripped = expression.strip()
    if stripped == "*":
        aliases = []
        seen_relations: set[str] = set()
        for alias, binding in relation_bindings.items():
            if alias != normalize_sql_identifier(binding.relation.split(".")[-1]):
                if binding.relation in seen_relations:
                    continue
                seen_relations.add(binding.relation)
                aliases.append(alias)
        expanded: list[dict[str, Any]] = []
        for alias in aliases:
            expanded.extend(expand_wildcard_projection(f"{alias}.*", relation_bindings))
        return expanded
    if not stripped.endswith(".*"):
        return []
    alias = normalize_sql_identifier(stripped[:-2])
    binding = relation_bindings.get(alias)
    if not binding:
        return []
    fields: list[dict[str, Any]] = []
    for field_name, source_field in binding.fields_by_name.items():
        fields.append(
            {
                "name": field_name,
                "data_type": source_field.get("data_type", "unknown"),
                "source_fields": resolve_binding_field_sources(binding, field_name),
                "confidence": "medium",
                "evidence": ["static_analysis", "code_reference"],
            }
        )
    return fields


def resolve_projection_binding_field(
    expression: str,
    relation_bindings: dict[str, RelationBinding],
) -> dict[str, Any] | None:
    stripped = strip_projection_alias(expression)
    qualified_match = QUALIFIED_COLUMN_RE.fullmatch(stripped)
    if qualified_match:
        alias = normalize_sql_identifier(qualified_match.group(1))
        column = normalize_sql_identifier(qualified_match.group(2))
        binding = relation_bindings.get(alias)
        if binding:
            return binding.fields_by_name.get(column)
        return None
    normalized = normalize_sql_identifier(stripped)
    if not normalized or "." in normalized:
        return None
    matching_fields = [
        binding.fields_by_name[normalized]
        for binding in relation_bindings.values()
        if normalized in binding.fields_by_name
    ]
    if len(matching_fields) == 1:
        return matching_fields[0]
    return None


def resolve_expression_sources(
    expression: str,
    *,
    output_name: str,
    relation_bindings: dict[str, RelationBinding],
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    source_fields: list[dict[str, str]] = []
    unresolved_sources: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    qualified_spans: list[tuple[int, int]] = []
    expression_without_alias = strip_projection_alias(expression)

    def add_source(relation: str, column: str) -> None:
        key = (relation, column)
        if not relation or not column or key in seen:
            return
        seen.add(key)
        source_fields.append({"relation": relation, "column": column})

    for match in QUALIFIED_COLUMN_RE.finditer(expression_without_alias):
        alias = normalize_sql_identifier(match.group(1))
        column = normalize_sql_identifier(match.group(2))
        qualified_spans.append(match.span())
        binding = relation_bindings.get(alias)
        if not binding:
            unresolved_sources.append({"token": match.group(0), "reason": "unknown_relation_alias"})
            continue
        binding_field = binding.fields_by_name.get(column)
        resolved_sources = resolve_binding_field_sources(binding, column)
        if resolved_sources:
            for item in resolved_sources:
                add_source(item["relation"], item["column"])
        elif binding_field:
            unresolved_sources.extend(
                dict(item)
                for item in binding_field.get("unresolved_sources", [])
            )
        elif binding.relation and "." in binding.relation:
            add_source(binding.relation, column)
        else:
            unresolved_sources.append({"token": match.group(0), "reason": "unresolved_projection"})

    bare_expression = remove_qualified_segments(expression_without_alias, qualified_spans)
    bare_expression = strip_sql_string_literals(bare_expression)
    bare_identifiers = [
        token
        for token in extract_bare_identifier_tokens(bare_expression)
        if token.lower() not in PROJECTION_RESERVED_WORDS and not token.isdigit()
    ]
    function_names = {name.lower() for name in FUNCTION_NAME_RE.findall(bare_expression)}
    for token in bare_identifiers:
        if token.lower() in function_names:
            continue
        resolved_sources, reason = resolve_unqualified_column(token, relation_bindings)
        if resolved_sources:
            for item in resolved_sources:
                add_source(item["relation"], item["column"])
            continue
        if reason == "known_binding_no_lineage":
            continue
        if is_literal_expression(expression):
            continue
        unresolved_entry = {"token": token, "reason": reason}
        if reason == "ambiguous_unqualified_column":
            unresolved_entry["candidates"] = sorted(
                {
                    binding.relation
                    for binding in relation_bindings.values()
                    if token in binding.fields_by_name or not binding.fields_by_name
                }
            )
        unresolved_sources.append(unresolved_entry)

    return source_fields, unresolved_sources


def resolve_binding_field_sources(binding: RelationBinding, column: str) -> list[dict[str, str]]:
    field = binding.fields_by_name.get(column)
    if field:
        if field.get("source_fields"):
            return dedupe_source_fields(field["source_fields"])
        if binding.relation and "." in binding.relation:
            return [{"relation": binding.relation, "column": column}]
        return []
    if binding.relation and "." in binding.relation:
        return [{"relation": binding.relation, "column": column}]
    return []


def resolve_unqualified_column(
    column: str,
    relation_bindings: dict[str, RelationBinding],
) -> tuple[list[dict[str, str]], str]:
    candidates: list[RelationBinding] = []
    seen_relations: set[str] = set()
    for binding in relation_bindings.values():
        relation_key = binding.relation
        if relation_key in seen_relations:
            continue
        seen_relations.add(relation_key)
        if not binding.fields_by_name or column in binding.fields_by_name:
            candidates.append(binding)
    if not candidates:
        return [], "unknown_unqualified_column"
    if len(candidates) > 1:
        concrete = [binding for binding in candidates if column in binding.fields_by_name]
        if len(concrete) == 1:
            resolved_sources = resolve_binding_field_sources(concrete[0], column)
            return resolved_sources, "known_binding_no_lineage" if column in concrete[0].fields_by_name and not resolved_sources else ""
        return [], "ambiguous_unqualified_column"
    resolved_sources = resolve_binding_field_sources(candidates[0], column)
    return resolved_sources, "known_binding_no_lineage" if column in candidates[0].fields_by_name and not resolved_sources else ""


def projection_confidence(
    *,
    expression: str,
    source_fields: list[dict[str, str]],
    unresolved_sources: list[dict[str, Any]],
) -> str:
    normalized = expression.strip().lower()
    if unresolved_sources:
        if source_fields and any(token in normalized for token in ("(", " case ", "+", "-", "*", "/", " over ")):
            return "medium"
        return "low"
    if not source_fields:
        return "medium" if is_literal_expression(expression) else "low"
    if any(token in normalized for token in ("(", " case ", "+", "-", "*", "/", " over ")):
        return "medium"
    return "high"


def infer_projection_type(expression: str) -> str:
    lowered = expression.lower()
    if "::" in expression:
        cast_target = normalize_sql_type(expression.rsplit("::", 1)[-1].split()[0])
        if cast_target:
            return cast_target
    cast_match = re.search(r"\bcast\s*\(.+?\s+as\s+([A-Za-z_][A-Za-z0-9_]*)\)", expression, re.IGNORECASE)
    if cast_match:
        cast_target = normalize_sql_type(cast_match.group(1))
        if cast_target:
            return cast_target
    if any(token in lowered for token in ("count(",)):
        return "integer"
    if any(token in lowered for token in ("sum(", "avg(", "numeric", "decimal", "/", "*", "+", "-")):
        return "float"
    return "unknown"


def infer_projection_name(expression: str) -> str:
    match = TRAILING_ALIAS_RE.search(expression)
    if match:
        return normalize_sql_identifier(match.group(1))
    stripped = expression.strip()
    simple_qualified = QUALIFIED_COLUMN_RE.fullmatch(stripped)
    if simple_qualified:
        return normalize_sql_identifier(simple_qualified.group(2))
    trailing = TRAILING_IDENTIFIER_RE.search(stripped)
    if trailing and "(" in stripped:
        return normalize_sql_identifier(trailing.group(1))
    if RAW_IDENTIFIER_RE.fullmatch(stripped):
        return normalize_sql_identifier(stripped)
    return ""


def aggregate_confidence(fields: list[dict[str, Any]], *, default: str) -> str:
    if not fields:
        return default
    if any(field.get("confidence") == "low" for field in fields):
        return "low"
    if any(field.get("confidence") == "medium" for field in fields):
        return "medium"
    return "high"


def merge_projection_confidence(inferred_confidence: str, binding_confidence: str) -> str:
    rank = {"low": 0, "medium": 1, "high": 2}
    if rank.get(binding_confidence, 1) < rank.get(inferred_confidence, 1):
        return binding_confidence
    return inferred_confidence


def add_source_field(field: dict[str, Any], relation: str, column: str) -> None:
    source_fields = field.setdefault("source_fields", [])
    key = (relation, column)
    existing = {(item.get("relation", ""), item.get("column", "")) for item in source_fields}
    if relation and key not in existing:
        source_fields.append({"relation": relation, "column": column})


def dedupe_source_fields(source_fields: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for source in source_fields:
        key = (source.get("relation", ""), source.get("column", ""))
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        deduped.append({"relation": key[0], "column": key[1]})
    return deduped


def build_hint_id(prefix: str, relative_path: str, stable_value: str) -> str:
    path_slug = humanize_asset_name(relative_path).replace(" ", "-").lower()
    value_slug = humanize_asset_name(stable_value).replace(" ", "-").lower()
    return f"{prefix}:{path_slug}:{value_slug}"


def humanize_asset_name(path: str) -> str:
    normalized = path.rstrip("/")
    filename = normalized.rsplit("/", 1)[-1]
    return re.sub(r"[_\-]+", " ", filename).strip() or "Sql"


def normalize_sql_identifier(identifier: str) -> str:
    parts = split_sql_identifier_parts(identifier)
    if not parts:
        normalized = identifier.strip().strip(",").strip().strip('"').strip("`").strip("'").strip("[]")
        return normalized.lower()
    return ".".join(normalize_sql_identifier_part(part) for part in parts if part)


def normalize_sql_type(type_name: str) -> str:
    normalized = type_name.strip().strip(",").lower()
    if normalized.startswith(("varchar", "char", "text", "string")):
        return "string"
    if normalized.startswith(("int", "bigint", "smallint", "serial")):
        return "integer"
    if normalized.startswith(("numeric", "decimal", "float", "double", "real")):
        return "float"
    if normalized.startswith(("bool", "boolean")):
        return "boolean"
    if normalized.startswith(("date",)):
        return "date"
    if normalized.startswith(("timestamp", "datetime")):
        return "datetime"
    if normalized.startswith(("json", "jsonb")):
        return "json"
    if normalized.startswith(("array",)):
        return "array"
    return normalized


def strip_wrapping_parentheses(sql: str) -> str:
    stripped = sql.strip()
    while stripped.startswith("(") and stripped.endswith(")"):
        end_index = find_matching_paren(stripped, 0)
        if end_index != len(stripped) - 1:
            break
        stripped = stripped[1:-1].strip()
    return stripped


def strip_sql_comments(text: str) -> str:
    output: list[str] = []
    index = 0
    in_single = False
    in_double = False
    while index < len(text):
        char = text[index]
        next_two = text[index : index + 2]
        if not in_double and char == "'" and (index == 0 or text[index - 1] != "\\"):
            in_single = not in_single
            output.append(char)
            index += 1
            continue
        if not in_single and char == '"' and (index == 0 or text[index - 1] != "\\"):
            in_double = not in_double
            output.append(char)
            index += 1
            continue
        if not in_single and not in_double and next_two == "--":
            newline_index = text.find("\n", index)
            index = len(text) if newline_index == -1 else newline_index
            continue
        if not in_single and not in_double and next_two == "/*":
            end_index = text.find("*/", index + 2)
            index = len(text) if end_index == -1 else end_index + 2
            continue
        output.append(char)
        index += 1
    return "".join(output)


def split_sql_statements(text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    for char in text:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif char == ";" and depth == 0:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                continue
        current.append(char)
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def split_sql_top_level(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    for char in text:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif char == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
        current.append(char)
    trailing = "".join(current).strip()
    if trailing:
        parts.append(trailing)
    return parts


def drop_select_modifiers(select_list: str) -> str:
    stripped = select_list.strip()
    lowered = stripped.lower()
    for keyword in ("distinct", "all"):
        if lowered.startswith(f"{keyword} "):
            return stripped[len(keyword) :].strip()
    return stripped


def find_top_level_keyword(text: str, keyword: str, *, start: int = 0) -> int:
    keyword_lower = keyword.lower()
    depth = 0
    in_single = False
    in_double = False
    index = start
    while index < len(text):
        char = text[index]
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif depth == 0 and text[index : index + len(keyword_lower)].lower() == keyword_lower:
                before = text[index - 1] if index > 0 else " "
                after = text[index + len(keyword_lower)] if index + len(keyword_lower) < len(text) else " "
                if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
                    return index
        index += 1
    return -1


def find_top_level_fragment(text: str, fragment: str) -> int:
    fragment_lower = fragment.lower()
    depth = 0
    in_single = False
    in_double = False
    for index, char in enumerate(text):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif depth == 0 and text[index : index + len(fragment_lower)].lower() == fragment_lower:
                return index
    return -1


def find_matching_paren(text: str, start_index: int) -> int:
    depth = 0
    in_single = False
    in_double = False
    for index in range(start_index, len(text)):
        char = text[index]
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return index
    return -1


def remove_qualified_segments(expression: str, spans: list[tuple[int, int]]) -> str:
    if not spans:
        return expression
    output: list[str] = []
    cursor = 0
    for start, end in spans:
        if start > cursor:
            output.append(expression[cursor:start])
        output.append(" " * (end - start))
        cursor = end
    if cursor < len(expression):
        output.append(expression[cursor:])
    return "".join(output)


def skip_sql_whitespace(text: str, start: int) -> int:
    cursor = start
    while cursor < len(text) and text[cursor].isspace():
        cursor += 1
    return cursor


def is_literal_expression(expression: str) -> bool:
    stripped = expression.strip().lower()
    if not stripped:
        return True
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", stripped):
        return True
    if stripped in {"true", "false", "null"}:
        return True
    if (stripped.startswith("'") and stripped.endswith("'")) or (stripped.startswith('"') and stripped.endswith('"')):
        return True
    return False


def strip_projection_alias(expression: str) -> str:
    stripped = expression.strip()
    alias_match = TRAILING_ALIAS_RE.search(stripped)
    if alias_match:
        return stripped[: alias_match.start()].rstrip()
    trailing_match = TRAILING_IDENTIFIER_RE.search(stripped)
    if trailing_match and "(" in stripped:
        candidate = stripped[: trailing_match.start()].rstrip()
        if candidate:
            return candidate
    return stripped


def extract_cte_column_aliases(alias_list_sql: str) -> list[str]:
    aliases: list[str] = []
    for alias_expression in split_sql_top_level(alias_list_sql):
        alias_identifier, _ = consume_sql_identifier(alias_expression.strip())
        alias_name = normalize_sql_identifier(alias_identifier)
        if alias_name:
            aliases.append(alias_name)
    return aliases


def apply_cte_column_aliases(fields: list[dict[str, Any]], column_aliases: list[str]) -> list[dict[str, Any]]:
    if not column_aliases:
        return [dict(field) for field in fields]
    aliased_fields: list[dict[str, Any]] = []
    for index, alias_name in enumerate(column_aliases):
        base_field = fields[index] if index < len(fields) else None
        if base_field is None:
            aliased_fields.append(
                {
                    "name": alias_name,
                    "data_type": "unknown",
                    "source_fields": [],
                    "confidence": "low",
                    "evidence": ["static_analysis", "code_reference"],
                    "unresolved_sources": [{"token": alias_name, "reason": "cte_column_alias_without_source"}],
                }
            )
            continue
        aliased_fields.append({**base_field, "name": alias_name})
    return aliased_fields


def split_top_level_set_operations(sql: str) -> list[str]:
    branches: list[str] = []
    cursor = 0
    while cursor < len(sql):
        next_index = -1
        matched_keyword = ""
        for keyword in SET_OPERATION_KEYWORDS:
            index = find_top_level_keyword(sql, keyword, start=cursor)
            if index == -1:
                continue
            if next_index == -1 or index < next_index or (index == next_index and len(keyword) > len(matched_keyword)):
                next_index = index
                matched_keyword = keyword
        if next_index == -1:
            branches.append(sql[cursor:].strip())
            break
        branches.append(sql[cursor:next_index].strip())
        cursor = next_index + len(matched_keyword)
    return [branch for branch in branches if branch]


def merge_set_operation_analyses(analyses: list[QueryAnalysis]) -> QueryAnalysis:
    if not analyses:
        return QueryAnalysis(fields=[], upstream_relations=[])
    merged_fields: list[dict[str, Any]] = []
    max_length = max(len(analysis.fields) for analysis in analyses)
    for index in range(max_length):
        candidates = [analysis.fields[index] for analysis in analyses if index < len(analysis.fields)]
        if not candidates:
            continue
        output_name = candidates[0].get("name", "")
        source_fields = dedupe_source_fields(
            [
                source
                for candidate in candidates
                for source in candidate.get("source_fields", [])
            ]
        )
        unresolved_sources = sorted(
            {
                (item.get("token", ""), item.get("reason", ""), tuple(item.get("candidates", [])))
                for candidate in candidates
                for item in candidate.get("unresolved_sources", [])
            }
        )
        field: dict[str, Any] = {
            "name": output_name,
            "data_type": merge_set_operation_data_type(candidates),
            "source_fields": source_fields,
            "confidence": merge_set_operation_confidence(candidates),
            "evidence": ["static_analysis", "code_reference"],
        }
        if unresolved_sources:
            field["unresolved_sources"] = [
                {
                    **({"token": token} if token else {}),
                    **({"reason": reason} if reason else {}),
                    **({"candidates": list(candidates_list)} if candidates_list else {}),
                }
                for token, reason, candidates_list in unresolved_sources
            ]
        merged_fields.append(field)
    return QueryAnalysis(
        fields=merged_fields,
        upstream_relations=sorted(
            {
                relation
                for analysis in analyses
                for relation in analysis.upstream_relations
            }
        ),
    )


def merge_set_operation_data_type(candidates: list[dict[str, Any]]) -> str:
    data_types = {candidate.get("data_type", "unknown") for candidate in candidates if candidate.get("data_type")}
    if len(data_types) == 1:
        return next(iter(data_types))
    if "unknown" in data_types and len(data_types) == 2:
        return next(item for item in data_types if item != "unknown")
    return "unknown"


def merge_set_operation_confidence(candidates: list[dict[str, Any]]) -> str:
    if any(candidate.get("unresolved_sources") for candidate in candidates):
        return "low"
    if len(candidates) > 1:
        return "medium"
    return candidates[0].get("confidence", "medium")


def extract_bare_identifier_tokens(expression: str) -> list[str]:
    tokens = [normalize_sql_identifier(token) for token in BARE_IDENTIFIER_RE.findall(expression)]
    tokens.extend(
        normalize_sql_identifier(match.group(1) or match.group(2) or match.group(3) or "")
        for match in QUOTED_IDENTIFIER_RE.finditer(expression)
    )
    ordered: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token and token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered


def strip_sql_string_literals(expression: str) -> str:
    output: list[str] = []
    index = 0
    in_single = False
    while index < len(expression):
        char = expression[index]
        if char == "'" and not in_single:
            in_single = True
            output.append(" ")
            index += 1
            continue
        if char == "'" and in_single:
            if index + 1 < len(expression) and expression[index + 1] == "'":
                output.extend((" ", " "))
                index += 2
                continue
            in_single = False
            output.append(" ")
            index += 1
            continue
        if in_single:
            output.append(" ")
            index += 1
            continue
        output.append(char)
        index += 1
    return "".join(output)


def split_sql_identifier_parts(identifier: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_double = False
    in_backtick = False
    in_bracket = False
    for char in identifier.strip():
        if char == '"' and not in_backtick and not in_bracket:
            in_double = not in_double
            current.append(char)
            continue
        if char == "`" and not in_double and not in_bracket:
            in_backtick = not in_backtick
            current.append(char)
            continue
        if char == "[" and not in_double and not in_backtick:
            in_bracket = True
            current.append(char)
            continue
        if char == "]" and in_bracket:
            in_bracket = False
            current.append(char)
            continue
        if char == "." and not in_double and not in_backtick and not in_bracket:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(char)
    trailing = "".join(current).strip()
    if trailing:
        parts.append(trailing)
    return parts


def normalize_sql_identifier_part(part: str) -> str:
    cleaned = part.strip().strip(",")
    if cleaned.startswith('"') and cleaned.endswith('"'):
        cleaned = cleaned[1:-1]
    elif cleaned.startswith("`") and cleaned.endswith("`"):
        cleaned = cleaned[1:-1]
    elif cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    return cleaned.strip().lower()


def consume_sql_identifier(text: str, start: int = 0) -> tuple[str, int]:
    cursor = skip_sql_whitespace(text, start)
    parts: list[str] = []
    while cursor < len(text):
        part, cursor = consume_sql_identifier_part(text, cursor)
        if not part:
            break
        parts.append(part)
        cursor = skip_sql_whitespace(text, cursor)
        if cursor < len(text) and text[cursor] == ".":
            cursor += 1
            continue
        break
    return ".".join(parts), cursor


def consume_sql_identifier_part(text: str, start: int = 0) -> tuple[str, int]:
    cursor = skip_sql_whitespace(text, start)
    if cursor >= len(text):
        return "", cursor
    char = text[cursor]
    if char == '"':
        end = text.find('"', cursor + 1)
        return (text[cursor : end + 1], end + 1) if end != -1 else ("", cursor)
    if char == "`":
        end = text.find("`", cursor + 1)
        return (text[cursor : end + 1], end + 1) if end != -1 else ("", cursor)
    if char == "[":
        end = text.find("]", cursor + 1)
        return (text[cursor : end + 1], end + 1) if end != -1 else ("", cursor)
    match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[cursor:])
    if not match:
        return "", cursor
    return match.group(0), cursor + match.end()


def consume_sql_type_token(text: str, start: int = 0) -> tuple[str, int]:
    cursor = skip_sql_whitespace(text, start)
    begin = cursor
    depth = 0
    while cursor < len(text):
        char = text[cursor]
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                break
            depth -= 1
        elif depth == 0 and char.isspace():
            break
        cursor += 1
    token = text[begin:cursor]
    trailing = text[cursor:].lstrip().lower()
    if token.lower() == "double" and trailing.startswith("precision"):
        cursor += len(text[cursor:]) - len(text[cursor:].lstrip())
        cursor += len("precision")
        token = f"{token} precision"
    return token, cursor
