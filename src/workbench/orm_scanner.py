from __future__ import annotations

import ast
import re
from typing import Any


QUERY_RESULT_METHODS = {
    "all",
    "first",
    "one",
    "one_or_none",
    "scalar",
    "scalar_one",
    "scalar_one_or_none",
    "scalars",
}
CHAIN_METHODS = {"limit", "offset", "where", "filter", "filter_by", "order_by", "join", "outerjoin"}
EXECUTE_WRAPPER_METHODS = {"execute", "scalars", "scalar", "scalar_one", "scalar_one_or_none"}
LOADER_RELATION_METHODS = {"joinedload", "selectinload", "subqueryload", "contains_eager"}
QUERY_PROJECTION_METHODS = {"with_entities", "with_only_columns", "add_columns"}


def scan_orm_structure_hints(
    relative_path: str,
    text: str,
    *,
    imported_class_relations: dict[str, str] | None = None,
    imported_table_relations: dict[str, str] | None = None,
    imported_relationship_targets: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []

    local_class_relations = collect_sqlalchemy_class_relations(tree)
    class_relations = {**(imported_class_relations or {}), **local_class_relations}
    table_hints = extract_sqlalchemy_table_hints(tree, relative_path)
    local_table_relations = {hint["symbol"]: hint["relation"] for hint in table_hints if hint.get("symbol")}
    table_relations = {**(imported_table_relations or {}), **local_table_relations}
    relationship_targets = merge_relationship_targets(
        imported_relationship_targets or {},
        collect_relationship_targets(
            tree,
            class_relations=class_relations,
            table_relations=table_relations,
        ),
    )
    relation_fields: dict[str, list[dict[str, Any]]] = {
        hint["relation"]: [dict(field) for field in hint.get("fields", [])]
        for hint in table_hints
        if hint.get("relation")
    }

    hints: list[dict[str, Any]] = [
        {key: value for key, value in hint.items() if key != "symbol"}
        for hint in table_hints
    ]

    for hint in extract_sqlalchemy_model_hints(
        tree,
        relative_path=relative_path,
        class_relations=class_relations,
        table_relations=table_relations,
    ):
        relation_fields[hint["relation"]] = [
            *[dict(field) for field in hint.get("fields", [])],
            *[dict(field) for field in hint.get("computed_fields", [])],
        ]
        hints.append(hint)

    hints.extend(
        extract_query_projection_hints(
            tree,
            relative_path=relative_path,
            class_relations=class_relations,
            relation_fields=relation_fields,
            table_relations=table_relations,
            relationship_targets=relationship_targets,
        )
    )
    return hints


def extract_sqlalchemy_table_hints(tree: ast.AST, relative_path: str) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        field_target = node.targets[0].id
        if not isinstance(node.value, ast.Call) or get_ast_name(node.value.func).split(".")[-1] != "Table":
            continue
        if not node.value.args or not isinstance(node.value.args[0], ast.Constant) or not isinstance(node.value.args[0].value, str):
            continue
        relation_name = node.value.args[0].value
        schema_name = extract_sqlalchemy_schema_from_keywords(node.value)
        qualified_relation = qualify_relation_name(relation_name, schema_name=schema_name)
        fields: list[dict[str, Any]] = []
        upstream_relations: set[str] = set()
        for candidate in node.value.args[1:]:
            field_hint = extract_table_column_hint(candidate, schema_name=schema_name)
            if not field_hint:
                continue
            fields.append(field_hint)
            for source_field in field_hint.get("source_fields", []):
                if source_field.get("relation"):
                    upstream_relations.add(source_field["relation"])
        hints.append(
            {
                "symbol": field_target,
                "id": build_hint_id("orm", relative_path, qualified_relation),
                "label": qualified_relation,
                "relation": qualified_relation,
                "object_type": "table",
                "file": relative_path,
                "detected_from": "sqlalchemy_table",
                "description": f"Detected from {relative_path}",
                "fields": fields,
                "upstream_relations": sorted(upstream_relations),
                "confidence": "high",
                "evidence": ["static_analysis", "code_reference"],
            }
        )
    return hints


def extract_sqlalchemy_model_hints(
    tree: ast.AST,
    *,
    relative_path: str,
    class_relations: dict[str, str],
    table_relations: dict[str, str],
) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        seeded_relation = class_relations.get(node.name, "")
        schema_name, relation_name = split_qualified_relation(seeded_relation)
        fields: list[dict[str, Any]] = []
        upstream_relations: set[str] = set()
        relationship_details: list[dict[str, Any]] = []
        computed_fields: list[dict[str, Any]] = []

        for statement in node.body:
            if isinstance(statement, ast.Assign):
                target_names = [target.id for target in statement.targets if isinstance(target, ast.Name)]
                if "__tablename__" in target_names and isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
                    relation_name = statement.value.value
                    continue
                if "__table_args__" in target_names:
                    schema_name = extract_sqlalchemy_schema(statement.value)
                    continue
                field_name = target_names[0] if target_names else ""
                relationship_hint = extract_relationship_details(
                    statement.value,
                    class_relations=class_relations,
                    table_relations=table_relations,
                    schema_name=schema_name,
                )
                if relationship_hint:
                    relationship_details.append(relationship_hint)
                    upstream_relations.update(relationship_hint.get("relations", []))
                    continue
                field_hint = extract_sqlalchemy_field_hint(field_name, statement.value, schema_name=schema_name)
                if field_hint:
                    fields.append(field_hint)
                    upstream_relations.update(
                        source_field["relation"]
                        for source_field in field_hint.get("source_fields", [])
                        if source_field.get("relation")
                    )
            elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                if statement.target.id == "__tablename__" and isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
                    relation_name = statement.value.value
                    continue
                if statement.target.id == "__table_args__":
                    schema_name = extract_sqlalchemy_schema(statement.value)
                    continue
                relationship_hint = extract_relationship_details(
                    statement.value,
                    class_relations=class_relations,
                    table_relations=table_relations,
                    schema_name=schema_name,
                )
                if relationship_hint:
                    relationship_details.append(relationship_hint)
                    upstream_relations.update(relationship_hint.get("relations", []))
                    continue
                field_hint = extract_sqlalchemy_field_hint(
                    statement.target.id,
                    statement.value,
                    annotation=statement.annotation,
                    schema_name=schema_name,
                )
                if field_hint:
                    fields.append(field_hint)
                    upstream_relations.update(
                        source_field["relation"]
                        for source_field in field_hint.get("source_fields", [])
                        if source_field.get("relation")
                    )
            elif isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                computed_field = extract_computed_field_hint(
                    statement,
                    seeded_relation or qualify_relation_name(relation_name, schema_name=schema_name),
                    owner_class_name=node.name,
                )
                if computed_field:
                    computed_fields.append(computed_field)

        if not relation_name:
            continue
        qualified_relation = qualify_relation_name(relation_name, schema_name=schema_name)
        hint: dict[str, Any] = {
            "id": build_hint_id("orm", relative_path, qualified_relation),
            "label": qualified_relation,
            "relation": qualified_relation,
            "object_type": "table",
            "file": relative_path,
            "detected_from": "sqlalchemy_model",
            "description": f"Detected from {relative_path}",
            "fields": fields,
            "upstream_relations": sorted(upstream_relations),
            "confidence": "high",
            "evidence": ["static_analysis", "code_reference"],
        }
        if relationship_details:
            hint["relationships"] = relationship_details
        if computed_fields:
            hint["computed_fields"] = dedupe_field_hints_by_name(computed_fields)
        hints.append(hint)
    return hints


def collect_relationship_targets(
    tree: ast.AST,
    *,
    class_relations: dict[str, str],
    table_relations: dict[str, str],
) -> dict[str, dict[str, str]]:
    targets: dict[str, dict[str, str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        base_relation = class_relations.get(node.name, "")
        if not base_relation:
            continue
        attribute_targets: dict[str, str] = {}
        schema_name, _ = split_qualified_relation(base_relation)
        for statement in node.body:
            field_name = ""
            value: ast.AST | None = None
            if isinstance(statement, ast.Assign):
                target_names = [target.id for target in statement.targets if isinstance(target, ast.Name)]
                field_name = target_names[0] if target_names else ""
                value = statement.value
            elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                field_name = statement.target.id
                value = statement.value
            if not field_name:
                continue
            relationship_hint = extract_relationship_details(
                value,
                class_relations=class_relations,
                table_relations=table_relations,
                schema_name=schema_name,
            )
            if not relationship_hint:
                continue
            target_relation = relationship_hint.get("target_relation", "")
            if target_relation:
                attribute_targets[field_name] = target_relation
        if attribute_targets:
            targets[base_relation] = attribute_targets
    return targets


def extract_query_projection_hints(
    tree: ast.AST,
    *,
    relative_path: str,
    class_relations: dict[str, str],
    relation_fields: dict[str, list[dict[str, Any]]],
    table_relations: dict[str, str],
    relationship_targets: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    for function_node in ast.walk(tree):
        if not isinstance(function_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        function_relations = class_relations | collect_local_relation_aliases(function_node, class_relations)
        local_value_bindings: dict[str, ast.AST] = {}
        for statement in iter_ordered_statement_nodes(function_node):
            assignment_target = extract_assignment_target_name(statement)
            value = extract_assignment_value(statement)
            projection = parse_query_projection(
                value,
                class_relations=function_relations,
                relation_fields=relation_fields,
                table_relations=table_relations,
                relationship_targets=relationship_targets,
                local_value_bindings=local_value_bindings,
            )
            if projection and assignment_target:
                hint_relation = f"query.{module_slug(relative_path)}.{function_node.name}.{assignment_target}"
                hints.append(
                    {
                        "id": build_hint_id("orm", relative_path, hint_relation),
                        "label": hint_relation,
                        "relation": hint_relation,
                        "object_type": "query_projection",
                        "file": relative_path,
                        "detected_from": "sqlalchemy_query_projection",
                        "description": f"Detected query projection in {function_node.name}",
                        "fields": projection["fields"],
                        "upstream_relations": projection["upstream_relations"],
                        "confidence": projection["confidence"],
                        "evidence": ["static_analysis", "code_reference"],
                        "query_context": {
                            "function": function_node.name,
                            "variable": assignment_target,
                            "returns_mapping": projection["returns_mapping"],
                        },
                    }
                )
            if assignment_target and value is not None:
                local_value_bindings[assignment_target] = value
    return hints


def parse_query_projection(
    node: ast.AST | None,
    *,
    class_relations: dict[str, str],
    relation_fields: dict[str, list[dict[str, Any]]],
    table_relations: dict[str, str],
    relationship_targets: dict[str, dict[str, str]],
    local_value_bindings: dict[str, ast.AST] | None = None,
) -> dict[str, Any] | None:
    local_value_bindings = local_value_bindings or {}
    current = resolve_local_query_value(node, local_value_bindings)
    if not isinstance(current, ast.Call):
        return None

    returns_mapping = False
    upstream_relations: set[str] = set()
    loader_projection_fields: list[dict[str, Any]] = []
    explicit_projection_fields: list[dict[str, Any]] = []
    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
        current = resolve_local_query_value(current, local_value_bindings)
        method_name = current.func.attr
        if method_name == "options":
            option_fields, option_relations = extract_loader_option_hints(
                current,
                class_relations=class_relations,
                relation_fields=relation_fields,
                relationship_targets=relationship_targets,
            )
            loader_projection_fields.extend(option_fields)
            upstream_relations.update(option_relations)
            current = resolve_local_query_value(current.func.value, local_value_bindings)
            continue
        if method_name == "mappings":
            returns_mapping = True
            current = resolve_local_query_value(current.func.value, local_value_bindings)
            continue
        if method_name in QUERY_PROJECTION_METHODS:
            for argument in current.args:
                projection = parse_query_projection_argument(
                    argument,
                    class_relations=class_relations,
                    relation_fields=relation_fields,
                    relationship_targets=relationship_targets,
                )
                if projection:
                    explicit_projection_fields.append(projection)
                    upstream_relations.update(
                        source_field["relation"]
                        for source_field in projection.get("source_fields", [])
                        if source_field.get("relation")
                    )
                    upstream_relations.update(projection.get("upstream_relations", []))
                    continue
                relation = extract_relation_reference(argument, class_relations) or extract_table_reference(argument, table_relations)
                if relation:
                    upstream_relations.add(relation)
            current = resolve_local_query_value(current.func.value, local_value_bindings)
            continue
        if method_name in EXECUTE_WRAPPER_METHODS and current.args:
            execute_target = resolve_local_query_value(current.args[0], local_value_bindings)
            if isinstance(execute_target, ast.Call):
                current = execute_target
                continue
        if method_name in EXECUTE_WRAPPER_METHODS:
            current = resolve_local_query_value(current.func.value, local_value_bindings)
            continue
        if method_name in QUERY_RESULT_METHODS or method_name in CHAIN_METHODS:
            for argument in current.args:
                relation = extract_relation_reference(argument, class_relations) or extract_table_reference(argument, table_relations)
                if relation:
                    upstream_relations.add(relation)
            for keyword in current.keywords:
                relation = extract_relation_reference(keyword.value, class_relations) or extract_table_reference(keyword.value, table_relations)
                if relation:
                    upstream_relations.add(relation)
            current = resolve_local_query_value(current.func.value, local_value_bindings)
            continue
        break

    current = resolve_local_query_value(current, local_value_bindings)
    select_call = None
    if isinstance(current, ast.Call) and isinstance(current.func, ast.Name) and current.func.id == "select":
        select_call = current
    elif isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute) and current.func.attr == "query":
        select_call = current
    elif (
        isinstance(current, ast.Call)
        and isinstance(current.func, ast.Attribute)
        and current.func.attr == "execute"
        and current.args
        and isinstance(current.args[0], ast.Call)
    ):
        select_call = current.args[0]

    if not isinstance(select_call, ast.Call):
        return None
    select_call = resolve_local_query_value(select_call, local_value_bindings)
    while isinstance(select_call, ast.Call) and isinstance(select_call.func, ast.Attribute) and select_call.func.attr in CHAIN_METHODS:
        for argument in select_call.args:
            relation = extract_relation_reference(argument, class_relations) or extract_table_reference(argument, table_relations)
            if relation:
                upstream_relations.add(relation)
        for keyword in select_call.keywords:
            relation = extract_relation_reference(keyword.value, class_relations) or extract_table_reference(keyword.value, table_relations)
            if relation:
                upstream_relations.add(relation)
        select_call = resolve_local_query_value(select_call.func.value, local_value_bindings)

    fields: list[dict[str, Any]] = []
    if explicit_projection_fields:
        fields.extend(dedupe_field_hints_by_name(explicit_projection_fields))
    else:
        for argument in select_call.args:
            projection = parse_query_projection_argument(
                argument,
                class_relations=class_relations,
                relation_fields=relation_fields,
                relationship_targets=relationship_targets,
            )
            if projection:
                fields.append(projection)
                upstream_relations.update(
                    source_field["relation"]
                    for source_field in projection.get("source_fields", [])
                    if source_field.get("relation")
                )
                upstream_relations.update(projection.get("upstream_relations", []))
                continue
            relation = extract_relation_reference(argument, class_relations)
            if relation:
                upstream_relations.add(relation)

    if isinstance(select_call.func, ast.Attribute) and select_call.func.attr == "query":
        for argument in select_call.args:
            relation = extract_relation_reference(argument, class_relations)
            if relation:
                upstream_relations.add(relation)

    for keyword in select_call.keywords:
        relation = extract_relation_reference(keyword.value, class_relations) or extract_table_reference(keyword.value, table_relations)
        if relation:
            upstream_relations.add(relation)

    if not fields and loader_projection_fields and any(extract_relation_reference(argument, class_relations) for argument in select_call.args):
        fields = dedupe_field_hints_by_name(loader_projection_fields)
    if not fields:
        return None
    confidence = "low" if any(field.get("confidence") == "low" for field in fields) else "medium"
    if all(field.get("confidence") == "high" for field in fields):
        confidence = "high"
    return {
        "fields": fields,
        "upstream_relations": sorted(upstream_relations),
        "confidence": confidence,
        "returns_mapping": returns_mapping,
    }


def parse_query_projection_argument(
    node: ast.AST,
    *,
    class_relations: dict[str, str],
    relation_fields: dict[str, list[dict[str, Any]]],
    relationship_targets: dict[str, dict[str, str]],
) -> dict[str, Any] | None:
    label_name = extract_label_name(node)
    base_node = unwrap_projection_node(node)
    source_fields = extract_projection_source_fields(
        base_node,
        class_relations=class_relations,
        relation_fields=relation_fields,
    )
    unresolved_sources: list[dict[str, Any]] = []
    candidate_relations: list[str] = []
    if not source_fields:
        unresolved_sources, candidate_relations = extract_projection_unresolved_sources(
            base_node,
            class_relations=class_relations,
            relationship_targets=relationship_targets,
        )
    if not source_fields and not unresolved_sources:
        return None
    output_name = label_name or infer_projection_output_name(base_node)
    if not output_name:
        return None
    data_type = infer_projection_data_type(base_node, relation_fields, class_relations=class_relations)
    confidence = "low" if unresolved_sources else ("medium" if isinstance(base_node, ast.Call) else "high")
    field_hint = {
        "name": output_name,
        "data_type": data_type,
        "source_fields": source_fields,
        "confidence": confidence,
        "evidence": ["static_analysis", "code_reference"],
    }
    if unresolved_sources:
        field_hint["unresolved_sources"] = unresolved_sources
    if candidate_relations:
        field_hint["upstream_relations"] = candidate_relations
    return field_hint


def extract_loader_option_hints(
    options_call: ast.Call,
    *,
    class_relations: dict[str, str],
    relation_fields: dict[str, list[dict[str, Any]]],
    relationship_targets: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], set[str]]:
    fields: list[dict[str, Any]] = []
    upstream_relations: set[str] = set()
    stack = [argument for argument in options_call.args if isinstance(argument, ast.Call)]
    while stack:
        current = stack.pop()
        function_name = get_ast_name(current.func).split(".")[-1]
        if function_name == "load_only":
            for argument in current.args:
                projection = parse_query_projection_argument(
                    argument,
                    class_relations=class_relations,
                    relation_fields=relation_fields,
                    relationship_targets=relationship_targets,
                )
                if projection:
                    fields.append(projection)
                    upstream_relations.update(
                        source_field["relation"]
                        for source_field in projection.get("source_fields", [])
                        if source_field.get("relation")
                    )
                    upstream_relations.update(projection.get("upstream_relations", []))
            continue
        if function_name in LOADER_RELATION_METHODS:
            for argument in current.args:
                relation = resolve_loader_relation(
                    argument,
                    class_relations=class_relations,
                    relationship_targets=relationship_targets,
                )
                if relation:
                    upstream_relations.add(relation)
        for argument in current.args:
            if isinstance(argument, ast.Call):
                stack.append(argument)
        if isinstance(current.func, ast.Attribute) and isinstance(current.func.value, ast.Call):
            stack.append(current.func.value)
    return dedupe_field_hints_by_name(fields), upstream_relations


def resolve_loader_relation(
    node: ast.AST | None,
    *,
    class_relations: dict[str, str],
    relationship_targets: dict[str, dict[str, str]],
) -> str:
    if isinstance(node, ast.Attribute):
        base_relation = extract_relation_reference(node.value, class_relations)
        if base_relation:
            return relationship_targets.get(base_relation, {}).get(node.attr, base_relation)
    return extract_relation_reference(node, class_relations)


def lookup_relation_field_sources(
    relation_fields: dict[str, list[dict[str, Any]]],
    relation: str,
    field_name: str,
) -> list[dict[str, str]]:
    for field in relation_fields.get(relation, []):
        if field.get("name") != field_name:
            continue
        if field.get("source_fields"):
            return [
                {"relation": source["relation"], "column": source["column"]}
                for source in field.get("source_fields", [])
                if source.get("relation") and source.get("column")
            ]
        return [{"relation": relation, "column": field_name}]
    return []


def extract_label_name(node: ast.AST) -> str:
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "label"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return node.args[0].value
    return ""


def unwrap_projection_node(node: ast.AST) -> ast.AST:
    current = node
    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute) and current.func.attr in {"label", "desc", "asc"}:
        current = current.func.value
    return current


def extract_projection_source_fields(
    node: ast.AST,
    *,
    class_relations: dict[str, str],
    relation_fields: dict[str, list[dict[str, Any]]],
) -> list[dict[str, str]]:
    sources: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def append_source(relation: str, column: str) -> None:
        key = (relation, column)
        if not relation or not column or key in seen:
            return
        seen.add(key)
        sources.append({"relation": relation, "column": column})

    def walk(current: ast.AST | None) -> None:
        if current is None:
            return
        if isinstance(current, ast.Attribute):
            relation = extract_relation_reference(current.value, class_relations)
            if relation:
                resolved_sources = lookup_relation_field_sources(relation_fields, relation, current.attr)
                if resolved_sources:
                    for source in resolved_sources:
                        append_source(source["relation"], source["column"])
                    return
                append_source(relation, current.attr)
                return
            walk(current.value)
            return
        if isinstance(current, ast.Call):
            walk(current.func)
            for argument in current.args:
                walk(argument)
            for keyword in current.keywords:
                walk(keyword.value)
            return
        if isinstance(current, ast.BinOp):
            walk(current.left)
            walk(current.right)
            return
        if isinstance(current, ast.UnaryOp):
            walk(current.operand)
            return
        if isinstance(current, ast.Compare):
            walk(current.left)
            for comparator in current.comparators:
                walk(comparator)
            return
        if isinstance(current, ast.Subscript):
            walk(current.value)
            walk(get_subscript_slice(current))
            return
        if isinstance(current, (ast.Tuple, ast.List, ast.Set)):
            for element in current.elts:
                walk(element)

    walk(node)
    return sources


def infer_projection_output_name(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def infer_projection_data_type(
    node: ast.AST,
    relation_fields: dict[str, list[dict[str, Any]]],
    *,
    class_relations: dict[str, str],
) -> str:
    if isinstance(node, ast.Attribute):
        relation = extract_relation_reference(node.value, class_relations)
        if relation:
            for field in relation_fields.get(relation, []):
                if field.get("name") == node.attr:
                    return field.get("data_type", "unknown")
        return "unknown"
    if isinstance(node, ast.Call):
        function_name = get_ast_name(node.func).split(".")[-1].lower()
        if function_name in {"count"}:
            return "integer"
        if function_name in {"sum", "avg"}:
            return "float"
    return "unknown"


def extract_projection_unresolved_sources(
    node: ast.AST,
    *,
    class_relations: dict[str, str],
    relationship_targets: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], list[str]]:
    candidate_relations = sorted(collect_projection_candidate_relations(node, class_relations, relationship_targets))
    if isinstance(node, ast.Attribute):
        base_relation = extract_relation_reference(node.value, class_relations)
        if base_relation:
            target_relation = relationship_targets.get(base_relation, {}).get(node.attr, "")
            if target_relation:
                return (
                    [
                        {
                            "token": get_ast_name(node) or node.attr,
                            "reason": "relationship_attribute_projection",
                            "candidates": [target_relation],
                        }
                    ],
                    [target_relation],
                )
            return ([{"token": get_ast_name(node) or node.attr, "reason": "unknown_relation_attribute"}], candidate_relations)
    if isinstance(node, ast.Call):
        function_name = get_ast_name(node.func).split(".")[-1]
        if function_name in {"text", "literal_column", "column"} and node.args:
            token = ""
            first_arg = node.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                token = first_arg.value
            return ([{"token": token or function_name or "expression", "reason": "raw_sql_projection"}], candidate_relations)
        return ([{"token": function_name or infer_projection_output_name(node) or "expression", "reason": "unsupported_projection_source"}], candidate_relations)
    if isinstance(node, ast.Name):
        return ([{"token": node.id, "reason": "unknown_projection_symbol"}], candidate_relations)
    return [], candidate_relations


def collect_projection_candidate_relations(
    node: ast.AST,
    class_relations: dict[str, str],
    relationship_targets: dict[str, dict[str, str]],
) -> set[str]:
    candidates: set[str] = set()
    for current in ast.walk(node):
        if not isinstance(current, ast.Attribute):
            continue
        base_relation = extract_relation_reference(current.value, class_relations)
        if not base_relation:
            continue
        target_relation = relationship_targets.get(base_relation, {}).get(current.attr, "")
        if target_relation:
            candidates.add(target_relation)
    return candidates


def extract_relationship_details(
    value: ast.AST | None,
    *,
    class_relations: dict[str, str],
    table_relations: dict[str, str],
    schema_name: str = "",
) -> dict[str, Any] | None:
    if not isinstance(value, ast.Call):
        return None
    function_name = get_ast_name(value.func).split(".")[-1]
    if function_name not in {"relationship", "dynamic_loader", "association_proxy"}:
        return None
    relations: list[str] = []
    target_relation = ""
    if value.args:
        target_relation = extract_relation_reference(value.args[0], class_relations)
        if target_relation:
            relations.append(target_relation)
    secondary_relation = ""
    for keyword in value.keywords:
        if keyword.arg == "secondary":
            secondary_relation = extract_table_reference(keyword.value, table_relations, schema_name=schema_name)
            if secondary_relation:
                relations.append(secondary_relation)
    if not relations:
        return None
    return {
        "target_relation": target_relation,
        "secondary_relation": secondary_relation,
        "relations": sorted(dict.fromkeys(relations)),
    }


def extract_computed_field_hint(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    relation: str,
    *,
    owner_class_name: str = "",
) -> dict[str, Any] | None:
    decorator_names = {get_ast_name(decorator) for decorator in function_node.decorator_list}
    decorator_suffixes = {name.split(".")[-1] for name in decorator_names}
    if not (decorator_suffixes & {"hybrid_property", "property"} or "expression" in decorator_suffixes):
        return None
    source_fields = extract_computed_return_source_fields(
        function_node,
        relation,
        owner_class_name=owner_class_name,
    )
    if not source_fields:
        return None
    return {
        "name": function_node.name,
        "data_type": "unknown",
        "source_fields": source_fields,
        "confidence": "medium" if "expression" in decorator_suffixes else "low",
        "evidence": ["static_analysis", "code_reference"],
        "computed": True,
    }


def extract_computed_return_source_fields(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    relation: str,
    *,
    owner_class_name: str = "",
) -> list[dict[str, str]]:
    fields: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    allowed_bases = {"self", "cls"}
    if owner_class_name:
        allowed_bases.add(owner_class_name)
    for node in ast.walk(function_node):
        if not isinstance(node, ast.Return):
            continue
        for candidate in ast.walk(node.value):
            if not isinstance(candidate, ast.Attribute):
                continue
            if isinstance(candidate.value, ast.Name) and candidate.value.id in allowed_bases:
                key = (relation, candidate.attr)
                if key in seen:
                    continue
                seen.add(key)
                fields.append({"relation": relation, "column": candidate.attr})
    return fields


def extract_table_column_hint(node: ast.AST, *, schema_name: str = "") -> dict[str, Any] | None:
    if not isinstance(node, ast.Call) or get_ast_name(node.func).split(".")[-1] != "Column":
        return None
    if not node.args or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
        return None
    field_name = node.args[0].value
    candidate_type_nodes = list(node.args[1:])
    candidate_type_nodes.extend(keyword.value for keyword in node.keywords if keyword.arg in {"type_", "type"})
    data_type = "unknown"
    for candidate in candidate_type_nodes:
        inferred = infer_sqlalchemy_type(candidate)
        if inferred:
            data_type = inferred
            break

    source_fields = extract_sqlalchemy_source_fields(node, schema_name=schema_name)
    field_hint: dict[str, Any] = {
        "name": field_name,
        "data_type": data_type,
        "source_fields": source_fields,
        "confidence": "high",
        "evidence": ["static_analysis", "code_reference"],
    }
    for keyword in node.keywords:
        if keyword.arg == "primary_key":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                field_hint["primary_key"] = flag
    if source_fields:
        field_hint["foreign_key"] = ",".join(
            f"{source['relation']}.{source['column']}" if source.get("column") else source["relation"]
            for source in source_fields
            if source.get("relation")
        )
    return field_hint

def collect_local_relation_aliases(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    class_relations: dict[str, str],
) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for statement in iter_ordered_statement_nodes(function_node):
        value: ast.AST | None = None
        target_name = extract_assignment_target_name(statement)
        if not target_name:
            continue
        if isinstance(statement, (ast.Assign, ast.AnnAssign)):
            value = statement.value
        relation = ""
        if isinstance(value, ast.Call) and get_ast_name(value.func).split(".")[-1] == "aliased" and value.args:
            relation = extract_relation_reference(value.args[0], class_relations | aliases)
        else:
            relation = extract_relation_reference(value, class_relations | aliases)
        if relation:
            aliases[target_name] = relation
    return aliases


def resolve_local_query_value(
    node: ast.AST | None,
    local_value_bindings: dict[str, ast.AST],
) -> ast.AST | None:
    current = node
    visited: set[str] = set()
    while isinstance(current, ast.Name) and current.id in local_value_bindings and current.id not in visited:
        visited.add(current.id)
        current = local_value_bindings[current.id]
    return current


def dedupe_field_hints_by_name(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for field in fields:
        field_name = field.get("name", "")
        if not field_name:
            continue
        current = merged.get(field_name)
        if current is None:
            merged[field_name] = {
                **field,
                "source_fields": [
                    {"relation": source["relation"], "column": source["column"]}
                    for source in field.get("source_fields", [])
                    if source.get("relation") and source.get("column")
                ],
            }
            continue
        current_sources = {
            (source.get("relation", ""), source.get("column", ""))
            for source in current.get("source_fields", [])
        }
        for source in field.get("source_fields", []):
            key = (source.get("relation", ""), source.get("column", ""))
            if key[0] and key[1] and key not in current_sources:
                current_sources.add(key)
                current.setdefault("source_fields", []).append({"relation": key[0], "column": key[1]})
        current_unresolved = {
            (
                item.get("token", ""),
                item.get("reason", ""),
                tuple(item.get("candidates", [])),
            )
            for item in current.get("unresolved_sources", [])
        }
        for item in field.get("unresolved_sources", []):
            key = (
                item.get("token", ""),
                item.get("reason", ""),
                tuple(item.get("candidates", [])),
            )
            if key in current_unresolved:
                continue
            current_unresolved.add(key)
            current.setdefault("unresolved_sources", []).append(
                {
                    **({"token": key[0]} if key[0] else {}),
                    **({"reason": key[1]} if key[1] else {}),
                    **({"candidates": list(key[2])} if key[2] else {}),
                }
            )
        if current.get("data_type") in {"", "unknown"} and field.get("data_type"):
            current["data_type"] = field["data_type"]
        current_relations = set(current.get("upstream_relations", []))
        current_relations.update(field.get("upstream_relations", []))
        if current_relations:
            current["upstream_relations"] = sorted(current_relations)
        if field.get("confidence") == "low" or current.get("confidence") == "low" or current.get("unresolved_sources"):
            current["confidence"] = "low"
        elif field.get("confidence") == "medium" or current.get("confidence") == "medium":
            current["confidence"] = "medium"
        else:
            current["confidence"] = "high"
        current["evidence"] = sorted(set([*current.get("evidence", []), *(field.get("evidence", []) or [])]))
    return list(merged.values())


def collect_sqlalchemy_table_relations(tree: ast.AST) -> dict[str, str]:
    relations: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        if not isinstance(node.value, ast.Call) or get_ast_name(node.value.func).split(".")[-1] != "Table":
            continue
        if not node.value.args or not isinstance(node.value.args[0], ast.Constant) or not isinstance(node.value.args[0].value, str):
            continue
        schema_name = extract_sqlalchemy_schema_from_keywords(node.value)
        relations[node.targets[0].id] = qualify_relation_name(node.value.args[0].value, schema_name=schema_name)
    return relations


def merge_relationship_targets(
    left: dict[str, dict[str, str]],
    right: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    merged = {
        relation: dict(targets)
        for relation, targets in left.items()
    }
    for relation, targets in right.items():
        merged.setdefault(relation, {}).update(targets)
    return merged


def collect_sqlalchemy_class_relations(tree: ast.AST) -> dict[str, str]:
    relations: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        relation_name = ""
        schema_name = ""
        for statement in node.body:
            if isinstance(statement, ast.Assign):
                target_names = [target.id for target in statement.targets if isinstance(target, ast.Name)]
                if "__tablename__" in target_names and isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
                    relation_name = statement.value.value
                elif "__table_args__" in target_names:
                    schema_name = extract_sqlalchemy_schema(statement.value)
            elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                if statement.target.id == "__tablename__" and isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
                    relation_name = statement.value.value
                elif statement.target.id == "__table_args__":
                    schema_name = extract_sqlalchemy_schema(statement.value)
        if relation_name:
            relations[node.name] = qualify_relation_name(relation_name, schema_name=schema_name)
    return relations


def extract_sqlalchemy_schema(node: ast.AST | None) -> str:
    if node is None:
        return ""
    if isinstance(node, ast.Dict):
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant) and key.value == "schema" and isinstance(value, ast.Constant) and isinstance(value.value, str):
                return value.value
        return ""
    if isinstance(node, (ast.Tuple, ast.List)):
        for element in reversed(node.elts):
            schema_name = extract_sqlalchemy_schema(element)
            if schema_name:
                return schema_name
        return ""
    if isinstance(node, ast.Call) and get_ast_name(node.func).split(".")[-1] == "dict":
        for keyword in node.keywords:
            if keyword.arg == "schema" and isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                return keyword.value.value
    return ""


def extract_sqlalchemy_schema_from_keywords(node: ast.Call) -> str:
    for keyword in node.keywords:
        if keyword.arg == "schema" and isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
            return keyword.value.value
    return ""


def extract_sqlalchemy_field_hint(
    field_name: str,
    value: ast.AST | None,
    *,
    annotation: ast.AST | None = None,
    schema_name: str = "",
) -> dict[str, Any] | None:
    if not field_name or field_name.startswith("__") or value is None:
        return None
    if not isinstance(value, ast.Call):
        return None
    function_name = get_ast_name(value.func).split(".")[-1]
    if function_name in {"relationship", "dynamic_loader", "association_proxy"}:
        return None
    if function_name not in {"Column", "mapped_column"}:
        return None

    candidate_type_nodes = list(value.args)
    if candidate_type_nodes and isinstance(candidate_type_nodes[0], ast.Constant) and isinstance(candidate_type_nodes[0].value, str):
        candidate_type_nodes = candidate_type_nodes[1:]
    candidate_type_nodes.extend(keyword.value for keyword in value.keywords if keyword.arg in {"type_", "type"})

    data_type = "unknown"
    for candidate in candidate_type_nodes:
        inferred = infer_sqlalchemy_type(candidate)
        if inferred:
            data_type = inferred
            break
    if data_type == "unknown" and annotation is not None:
        inferred = infer_sqlalchemy_type(annotation)
        if inferred:
            data_type = inferred

    source_fields = extract_sqlalchemy_source_fields(value, schema_name=schema_name)
    field_hint: dict[str, Any] = {
        "name": field_name,
        "data_type": data_type,
        "source_fields": source_fields,
        "confidence": "high",
        "evidence": ["static_analysis", "code_reference"],
    }
    for keyword in value.keywords:
        if keyword.arg == "primary_key":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                field_hint["primary_key"] = flag
        elif keyword.arg == "nullable":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                field_hint["nullable"] = flag
        elif keyword.arg == "index":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                field_hint["index"] = flag
        elif keyword.arg == "unique":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                field_hint["unique"] = flag
    if source_fields:
        field_hint["foreign_key"] = ",".join(
            f"{source['relation']}.{source['column']}" if source.get("column") else source["relation"]
            for source in source_fields
            if source.get("relation")
        )
    return field_hint


def extract_sqlalchemy_source_fields(call_node: ast.Call, *, schema_name: str = "") -> list[dict[str, str]]:
    source_fields: list[dict[str, str]] = []
    for candidate in [*call_node.args, *(keyword.value for keyword in call_node.keywords)]:
        relation, column = extract_foreign_key_reference(candidate, schema_name=schema_name)
        if relation:
            source_fields.append({"relation": relation, "column": column})
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for source in source_fields:
        key = (source.get("relation", ""), source.get("column", ""))
        if key[0] and key not in seen:
            seen.add(key)
            deduped.append(source)
    return deduped


def extract_foreign_key_reference(node: ast.AST | None, *, schema_name: str = "") -> tuple[str, str]:
    if not isinstance(node, ast.Call):
        return "", ""
    function_name = get_ast_name(node.func).split(".")[-1]
    if function_name != "ForeignKey" or not node.args:
        return "", ""
    target = node.args[0]
    if not isinstance(target, ast.Constant) or not isinstance(target.value, str):
        return "", ""
    return qualify_foreign_relation(target.value, schema_name=schema_name)


def qualify_foreign_relation(reference: str, *, schema_name: str = "") -> tuple[str, str]:
    normalized = reference.strip().strip('"').strip("'")
    parts = [part for part in normalized.split(".") if part]
    if len(parts) >= 3:
        return ".".join(parts[:-1]), parts[-1]
    if len(parts) == 2:
        relation = f"{schema_name}.{parts[0]}" if schema_name else parts[0]
        return relation, parts[1]
    relation = f"{schema_name}.{parts[0]}" if schema_name else parts[0]
    return relation, ""


def infer_sqlalchemy_type(node: ast.AST | None) -> str:
    if node is None:
        return ""
    if isinstance(node, ast.Subscript):
        base_name = get_ast_name(node.value).split(".")[-1]
        if base_name in {"Mapped", "Optional", "list", "List", "Sequence"}:
            return infer_sqlalchemy_type(get_subscript_slice(node))
    if isinstance(node, ast.Tuple):
        for element in node.elts:
            inferred = infer_sqlalchemy_type(element)
            if inferred:
                return inferred
        return ""
    if isinstance(node, ast.Call):
        function_name = get_ast_name(node.func).split(".")[-1]
        if function_name == "ForeignKey":
            return ""
        return map_sqlalchemy_type_name(function_name)
    if isinstance(node, ast.Name):
        return map_sqlalchemy_type_name(node.id)
    if isinstance(node, ast.Attribute):
        return map_sqlalchemy_type_name(node.attr)
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool):
            return "boolean"
        if isinstance(node.value, int):
            return "integer"
        if isinstance(node.value, float):
            return "float"
        if isinstance(node.value, str):
            return "string"
    return ""


def map_sqlalchemy_type_name(type_name: str) -> str:
    normalized = type_name.lower()
    if normalized in {"integer", "int", "biginteger", "smallinteger"}:
        return "integer"
    if normalized in {"numeric", "decimal", "float", "real", "double", "doubleprecision"}:
        return "float"
    if normalized in {"string", "text", "unicode", "varchar", "char"}:
        return "string"
    if normalized in {"boolean", "bool"}:
        return "boolean"
    if normalized in {"date"}:
        return "date"
    if normalized in {"datetime", "timestamp"}:
        return "datetime"
    if normalized in {"json", "jsonb"}:
        return "json"
    if normalized in {"array"}:
        return "array"
    return ""


def extract_relation_reference(node: ast.AST | None, class_relations: dict[str, str]) -> str:
    if node is None:
        return ""
    dotted_name = get_ast_name(node)
    if dotted_name and dotted_name in class_relations:
        return class_relations[dotted_name]
    if isinstance(node, ast.Name):
        return class_relations.get(node.id, "")
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return class_relations.get(node.value, node.value if "." in node.value else "")
    if isinstance(node, ast.Attribute):
        return class_relations.get(node.attr, extract_relation_reference(node.value, class_relations))
    if isinstance(node, ast.Subscript):
        return extract_relation_reference(node.value, class_relations)
    return ""


def extract_table_reference(node: ast.AST | None, table_relations: dict[str, str], *, schema_name: str = "") -> str:
    if node is None:
        return ""
    dotted_name = get_ast_name(node)
    if dotted_name and dotted_name in table_relations:
        return table_relations[dotted_name]
    if isinstance(node, ast.Name):
        return table_relations.get(node.id, "")
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        value = node.value
        if value in table_relations:
            return table_relations[value]
        return qualify_relation_name(value, schema_name=schema_name)
    if isinstance(node, ast.Attribute):
        return table_relations.get(node.attr, "")
    return ""


def iter_ordered_statement_nodes(function_node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.stmt]:
    statements: list[ast.stmt] = []
    for node in ast.walk(function_node):
        if isinstance(node, ast.stmt) and not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            statements.append(node)
    return sorted(statements, key=lambda statement: getattr(statement, "lineno", 0))


def extract_assignment_target_name(node: ast.stmt) -> str:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name):
                return target.id
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return node.target.id
    return ""


def extract_assignment_value(node: ast.stmt) -> ast.AST | None:
    if isinstance(node, (ast.Assign, ast.AnnAssign)):
        return node.value
    return None


def split_qualified_relation(relation: str) -> tuple[str, str]:
    if "." not in relation:
        return "", relation
    schema_name, relation_name = relation.rsplit(".", 1)
    return schema_name, relation_name


def qualify_relation_name(relation_name: str, *, schema_name: str = "") -> str:
    return f"{schema_name}.{relation_name}" if schema_name and "." not in relation_name else relation_name


def extract_bool_constant(node: ast.AST | None) -> bool | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return node.value
    return None


def get_subscript_slice(node: ast.Subscript) -> ast.AST:
    return node.slice if not isinstance(node.slice, ast.Index) else node.slice.value


def get_ast_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = get_ast_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def build_hint_id(prefix: str, relative_path: str, stable_value: str) -> str:
    path_slug = humanize_asset_name(relative_path).replace(" ", "-").lower()
    value_slug = humanize_asset_name(stable_value).replace(" ", "-").lower()
    return f"{prefix}:{path_slug}:{value_slug}"


def humanize_asset_name(path: str) -> str:
    normalized = path.rstrip("/")
    filename = normalized.rsplit("/", 1)[-1]
    return re.sub(r"[_\-]+", " ", filename).strip() or "Orm"


def module_slug(relative_path: str) -> str:
    stem = relative_path.replace("\\", "/").replace("/", "_")
    return stem[:-3] if stem.endswith(".py") else stem
