from __future__ import annotations

import ast
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
import re

from .orm_scanner import collect_relationship_targets as collect_orm_relationship_targets
from .orm_scanner import collect_sqlalchemy_table_relations, merge_relationship_targets, scan_orm_structure_hints
from .profile import build_asset_descriptor, profile_asset
from .sql_scanner import scan_sql_structure_hints


IGNORED_PARTS = {".git", ".venv", "__pycache__", "runtime", ".pytest_cache"}
DATA_SUFFIXES = {".csv", ".gz", ".parquet", ".zip"}
CODE_SUFFIXES = {".py", ".js", ".ts", ".tsx", ".jsx", ".css", ".html", ".sql"}
DOC_SUFFIXES = {".md", ".pdf"}
FASTAPI_ROUTE_RE = re.compile(r"@\s*(?:\w+\.)?(get|post|put|patch|delete)\(\s*[\"'](/[^\"']+)[\"']")
FETCH_ROUTE_RE = re.compile(r"fetch\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]")
AXIOS_ROUTE_RE = re.compile(r"axios\.(?:get|post|put|patch|delete)\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]")
SQL_CREATE_TABLE_RE = re.compile(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?(?P<name>[A-Za-z_][\w.]*)\s*\((?P<body>.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
SQL_CREATE_VIEW_RE = re.compile(
    r"create\s+(?:or\s+replace\s+)?(?P<kind>materialized\s+view|view)\s+(?:if\s+not\s+exists\s+)?(?P<name>[A-Za-z_][\w.]*)\s+as\s+(?P<select>select\b.*?);",
    re.IGNORECASE | re.DOTALL,
)
SQL_RELATION_RE = re.compile(
    r"\b(from|join)\s+([A-Za-z_][\w.]*)(?:\s+(?:as\s+)?([A-Za-z_][\w]*))?",
    re.IGNORECASE,
)
SQL_BARE_IDENTIFIER_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
EXPORTED_COMPONENT_RE = re.compile(r"export\s+(?:default\s+)?function\s+([A-Z][A-Za-z0-9_]*)")
FUNCTION_COMPONENT_RE = re.compile(r"\bfunction\s+([A-Z][A-Za-z0-9_]*)\s*\(")
CONST_COMPONENT_RE = re.compile(r"\bconst\s+([A-Z][A-Za-z0-9_]*)\s*=\s*(?:async\s*)?\(")
FETCH_RESPONSE_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+fetch\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]",
)
FETCH_JSON_CHAIN_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+fetch\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`][\s\S]{0,120}?\.json\(\s*\)",
)
JSON_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+([A-Za-z_][A-Za-z0-9_]*)\.json\(\s*\)",
)
AXIOS_RESPONSE_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*await\s+axios\.(?:get|post|put|patch|delete)\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`]",
)
AXIOS_DATA_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\(\s*await\s+axios\.(?:get|post|put|patch|delete)\(\s*[\"'`](\/api\/[^\"'`]+)[\"'`][\s\S]{0,120}?\)\s*\)\.data",
)
DESTRUCTURE_ASSIGN_RE = re.compile(
    r"(?:const|let|var)\s*\{([^}]+)\}\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\b",
)
GENERIC_UI_FIELDS = {"data", "json", "status", "length", "value", "items", "results", "meta"}


def profile_project(root_dir: Path, *, include_tests: bool = False, include_internal: bool = True) -> dict:
    manifests: list[str] = []
    code_files: list[str] = []
    docs: list[str] = []
    data_files: list[Path] = []

    for path in iter_project_files(root_dir, include_tests=include_tests, include_internal=include_internal):
        relative = str(path.relative_to(root_dir))
        name = path.name
        suffix = path.suffix.lower()

        if name in {"pyproject.toml", "package.json", "docker-compose.yml", "Dockerfile", "requirements.txt"}:
            manifests.append(relative)
        if suffix in CODE_SUFFIXES:
            code_files.append(relative)
        if suffix in DOC_SUFFIXES:
            docs.append(relative)
        if suffix in DATA_SUFFIXES:
            data_files.append(path)

    data_assets = summarize_data_assets(root_dir, data_files)
    code_hints = summarize_code_hints(root_dir, code_files)
    planning_hints = summarize_planning_hints(root_dir)

    return {
        "root": str(root_dir),
        "summary": {
            "manifests": len(manifests),
            "code_files": len(code_files),
            "docs": len(docs),
            "data_assets": len(data_assets),
            "import_suggestions": len([asset for asset in data_assets if asset.get("suggested_import")]),
            "api_contract_hints": len(code_hints["api_contract_hints"]),
            "ui_contract_hints": len(code_hints["ui_contract_hints"]),
            "sql_structure_hints": len(code_hints["sql_structure_hints"]),
            "orm_structure_hints": len(code_hints["orm_structure_hints"]),
            "planning_api_hints": len(planning_hints["planning_api_hints"]),
            "planning_data_hints": len(planning_hints["planning_data_hints"]),
            "planning_compute_hints": len(planning_hints["planning_compute_hints"]),
        },
        "manifests": manifests,
        "code_files_sample": code_files[:12],
        "docs_sample": docs[:12],
        "data_assets": data_assets[:12],
        "api_contract_hints": code_hints["api_contract_hints"][:20],
        "ui_contract_hints": code_hints["ui_contract_hints"][:20],
        "sql_structure_hints": code_hints["sql_structure_hints"][:20],
        "orm_structure_hints": code_hints["orm_structure_hints"][:20],
        "planning_api_hints": planning_hints["planning_api_hints"][:20],
        "planning_data_hints": planning_hints["planning_data_hints"][:20],
        "planning_compute_hints": planning_hints["planning_compute_hints"][:20],
    }


def iter_project_files(root_dir: Path, *, include_tests: bool, include_internal: bool):
    for path in root_dir.rglob("*"):
        if not path.is_file():
            continue
        if any(part in IGNORED_PARTS for part in path.parts):
            continue
        relative = path.relative_to(root_dir).as_posix()
        if not include_tests and is_test_path(relative):
            continue
        if not include_internal and is_internal_workbench_path(relative):
            continue
        yield path


def summarize_data_assets(root_dir: Path, data_files: list[Path]) -> list[dict]:
    grouped_entries = group_data_assets(root_dir, data_files)
    summaries = [summarize_asset_entry(root_dir, entry) for entry in grouped_entries]
    return sorted(summaries, key=lambda item: item["path"])


def summarize_code_hints(root_dir: Path, code_files: list[str]) -> dict[str, list[dict]]:
    api_hints: list[dict] = []
    ui_hints: list[dict] = []
    sql_hints: list[dict] = []
    orm_hints: list[dict] = []
    python_context = build_python_repo_context(root_dir, code_files)

    for relative_path in code_files:
        path = root_dir / relative_path
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        api_hints.extend(extract_api_contract_hints(relative_path, text, python_context=python_context))
        ui_hints.extend(extract_ui_contract_hints(relative_path, text))
        if relative_path.endswith(".py"):
            orm_hints.extend(
                extract_orm_structure_hints(
                    relative_path,
                    text,
                    python_context=python_context.get(relative_path, {}),
                )
            )
        if relative_path.endswith(".sql"):
            sql_hints.extend(extract_sql_structure_hints(relative_path, text))

    sql_hints, orm_hints = enrich_sql_orm_hint_evidence(sql_hints, orm_hints)
    return {
        "api_contract_hints": dedupe_hint_list(api_hints, keys=("method", "route", "file")),
        "ui_contract_hints": dedupe_hint_list(ui_hints, keys=("component", "file")),
        "sql_structure_hints": dedupe_hint_list(sql_hints, keys=("relation", "object_type", "file")),
        "orm_structure_hints": dedupe_hint_list(orm_hints, keys=("relation", "object_type", "file")),
    }


def summarize_planning_hints(root_dir: Path) -> dict[str, list[dict]]:
    from .structure_memory import collect_document_candidates, combine_partial_graph_candidates, extract_sql_relation_tag

    doc_candidates = collect_document_candidates(root_dir, [])
    api_hints: list[dict] = []
    data_hints: list[dict] = []
    compute_hints: list[dict] = []

    for candidate in doc_candidates:
        if candidate.get("type") != "api_route":
            continue
        route = candidate.get("route", "")
        if not route:
            continue
        fields = candidate.get("fields", []) or []
        api_hints.append(
            {
                "id": build_hint_id("plan-api", candidate.get("path", ""), route),
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
                        "required_fields": [field.get("name", "") for field in fields if field.get("name") and field.get("required", True)],
                        "response_field_sources": [
                            {
                                "name": field.get("name", ""),
                                "source_fields": [
                                    {
                                        "node_id": source.get("node_id", ""),
                                        "relation": extract_sql_relation_tag(
                                            next(
                                                (candidate_node for candidate_node in plan_graph.get("nodes", []) if candidate_node["id"] == source.get("node_id", "")),
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
        "planning_api_hints": dedupe_hint_list(api_hints, keys=("route", "detected_from")),
        "planning_data_hints": dedupe_hint_list(data_hints, keys=("relation", "object_type")),
        "planning_compute_hints": dedupe_hint_list(compute_hints, keys=("relation", "extension_type")),
    }


def resolve_plan_source_fields(plan_graph: dict[str, Any], lineage_inputs: list[dict]) -> list[dict]:
    source_fields: list[dict] = []
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


def extract_sql_relation_from_node(node: dict) -> str:
    for tag in node.get("tags", []) or []:
        if tag.startswith("sql_relation:"):
            return tag.split(":", 1)[1]
    return ""


def group_data_assets(root_dir: Path, data_files: list[Path]) -> list[dict]:
    parquet_by_dir: dict[str, list[str]] = defaultdict(list)
    direct_entries: list[dict[str, str | None]] = []

    for path in data_files:
        relative = path.relative_to(root_dir).as_posix()
        if path.suffix.lower() == ".parquet":
            parquet_by_dir[str(path.relative_to(root_dir).parent).replace("\\", "/")].append(relative)
            continue
        direct_entries.append({"path": relative, "kind": None, "format": None})

    grouped_entries: list[dict[str, str | None]] = []
    for parent, files in sorted(parquet_by_dir.items()):
        if len(files) > 1:
            pattern = f"{parent}/*.parquet" if parent != "." else "*.parquet"
            grouped_entries.append({"path": pattern, "kind": "glob", "format": "parquet_collection"})
            continue
        grouped_entries.append({"path": files[0], "kind": None, "format": None})

    return sorted(direct_entries + grouped_entries, key=lambda item: item["path"] or "")


def enrich_sql_orm_hint_evidence(
    sql_hints: list[dict[str, Any]],
    orm_hints: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    supported_object_types = {"table", "view", "materialized_view"}
    sql_by_relation = {
        hint.get("relation", ""): hint
        for hint in sql_hints
        if hint.get("relation") and hint.get("object_type") in supported_object_types
    }
    orm_by_relation = {
        hint.get("relation", ""): hint
        for hint in orm_hints
        if hint.get("relation") and hint.get("object_type") in supported_object_types
    }

    for relation in sorted(set(sql_by_relation) & set(orm_by_relation)):
        sql_hint = sql_by_relation[relation]
        orm_hint = orm_by_relation[relation]
        shared_fields = {
            field.get("name", "")
            for field in sql_hint.get("fields", [])
            if field.get("name")
        } & {
            field.get("name", "")
            for field in orm_hint.get("fields", [])
            if field.get("name")
        }
        if not shared_fields:
            continue
        append_hint_evidence_entry(sql_hint, "schema_match")
        append_hint_evidence_entry(orm_hint, "schema_match")
        for field in sql_hint.get("fields", []):
            if field.get("name", "") in shared_fields:
                append_hint_evidence_entry(field, "schema_match")
        for field in orm_hint.get("fields", []):
            if field.get("name", "") in shared_fields:
                append_hint_evidence_entry(field, "schema_match")

    return sql_hints, orm_hints


def append_hint_evidence_entry(item: dict[str, Any], evidence: str) -> None:
    if not evidence:
        return
    item["evidence"] = sorted(dict.fromkeys([*(item.get("evidence", []) or []), evidence]))


def build_python_repo_context(root_dir: Path, code_files: list[str]) -> dict[str, dict[str, Any]]:
    python_files = sorted(relative_path for relative_path in code_files if relative_path.endswith(".py"))
    module_infos: dict[str, dict[str, Any]] = {}
    for relative_path in python_files:
        path = root_dir / relative_path
        try:
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text)
        except (UnicodeDecodeError, SyntaxError):
            continue
        module_name = module_name_from_relative_path(relative_path)
        module_infos[relative_path] = {
            "relative_path": relative_path,
            "module_name": module_name,
            "tree": tree,
            "class_relations": collect_sqlalchemy_class_relations(tree),
            "table_relations": collect_sqlalchemy_table_relations(tree),
            "model_fields": collect_constructor_model_fields(tree),
            "imports": collect_python_import_bindings(tree, module_name),
        }

    contexts: dict[str, dict[str, Any]] = {
        relative_path: {
            "module_name": info["module_name"],
            "class_relations": dict(info["class_relations"]),
            "table_relations": dict(info["table_relations"]),
            "relationship_targets": collect_orm_relationship_targets(
                info["tree"],
                class_relations=info["class_relations"],
                table_relations=info["table_relations"],
            ),
            "helper_relation_templates": {},
            "helper_templates": {},
            "object_templates": {},
            "imports": info["imports"],
        }
        for relative_path, info in module_infos.items()
    }

    for _ in range(4):
        changed = False
        for relative_path in sorted(module_infos):
            info = module_infos[relative_path]
            imported_context = build_imported_python_context(info, contexts)
            class_relations = dict(info["class_relations"])
            class_relations.update(imported_context["class_relations"])
            table_relations = imported_context["table_relations"] | dict(info["table_relations"])
            relationship_targets = merge_relationship_targets(
                imported_context["relationship_targets"],
                collect_orm_relationship_targets(
                    info["tree"],
                    class_relations=class_relations,
                    table_relations=table_relations,
                ),
            )
            helper_relation_templates = build_python_helper_relation_templates(
                info["tree"],
                class_relations,
                initial_templates=imported_context["helper_relation_templates"],
                object_templates=imported_context["object_templates"],
            )
            helper_templates = build_python_helper_templates(
                info["tree"],
                class_relations,
                info["model_fields"],
                helper_relation_templates,
                imported_context["object_templates"],
                initial_templates=imported_context["helper_templates"],
            )
            object_templates = build_python_object_templates(
                info["tree"],
                class_relations,
                helper_relation_templates,
                helper_templates,
                imported_context["object_templates"],
            )
            helper_templates = build_python_helper_templates(
                info["tree"],
                class_relations,
                info["model_fields"],
                helper_relation_templates,
                imported_context["object_templates"] | object_templates,
                initial_templates=imported_context["helper_templates"],
            )
            object_templates = build_python_object_templates(
                info["tree"],
                class_relations,
                helper_relation_templates,
                helper_templates,
                imported_context["object_templates"] | object_templates,
            )
            helper_relation_templates = imported_context["helper_relation_templates"] | helper_relation_templates
            helper_templates = imported_context["helper_templates"] | helper_templates
            object_templates = imported_context["object_templates"] | object_templates
            next_context = {
                "module_name": info["module_name"],
                "class_relations": class_relations,
                "table_relations": table_relations,
                "relationship_targets": relationship_targets,
                "helper_relation_templates": helper_relation_templates,
                "helper_templates": helper_templates,
                "object_templates": object_templates,
                "imports": info["imports"],
            }
            if next_context != contexts.get(relative_path):
                contexts[relative_path] = next_context
                changed = True
        if not changed:
            break
    return contexts


def module_name_from_relative_path(relative_path: str) -> str:
    normalized = relative_path.replace("\\", "/")
    if normalized.endswith("/__init__.py"):
        normalized = normalized[: -len("/__init__.py")]
    elif normalized.endswith(".py"):
        normalized = normalized[:-3]
    return normalized.replace("/", ".")


def collect_python_import_bindings(tree: ast.AST, module_name: str) -> dict[str, dict[str, Any]]:
    symbol_aliases: dict[str, dict[str, str]] = {}
    module_aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                alias_name = alias.asname or alias.name.split(".")[-1]
                module_aliases[alias_name] = alias.name
        elif isinstance(node, ast.ImportFrom):
            target_module = resolve_import_module(module_name, node.module, node.level)
            if not target_module:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                alias_name = alias.asname or alias.name
                symbol_aliases[alias_name] = {"module": target_module, "symbol": alias.name}
    return {"symbol_aliases": symbol_aliases, "module_aliases": module_aliases}


def resolve_import_module(current_module: str, imported_module: str | None, level: int) -> str:
    if level <= 0:
        return imported_module or ""
    current_parts = current_module.split(".") if current_module else []
    base_parts = current_parts[:-level]
    if imported_module:
        base_parts.extend(imported_module.split("."))
    return ".".join(part for part in base_parts if part)


def find_context_by_module_name(
    contexts: dict[str, dict[str, Any]],
    module_name: str,
) -> dict[str, Any] | None:
    for context in contexts.values():
        if context.get("module_name") == module_name:
            return context
    return None


def build_imported_python_context(
    module_info: dict[str, Any],
    contexts: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    imported = {
        "class_relations": {},
        "table_relations": {},
        "relationship_targets": {},
        "helper_relation_templates": {},
        "helper_templates": {},
        "object_templates": {},
    }
    imports = module_info.get("imports", {})
    for alias_name, target in (imports.get("symbol_aliases", {}) or {}).items():
        context = find_context_by_module_name(contexts, target.get("module", ""))
        if not context:
            continue
        symbol_name = target.get("symbol", "")
        relation = context.get("class_relations", {}).get(symbol_name)
        if relation:
            imported["class_relations"][alias_name] = relation
            imported["relationship_targets"] = merge_relationship_targets(
                imported["relationship_targets"],
                context.get("relationship_targets", {}),
            )
        table_relation = context.get("table_relations", {}).get(symbol_name)
        if table_relation:
            imported["table_relations"][alias_name] = table_relation
        helper_relation_template = context.get("helper_relation_templates", {}).get(symbol_name)
        if helper_relation_template:
            imported["helper_relation_templates"][alias_name] = helper_relation_template
        helper_template = context.get("helper_templates", {}).get(symbol_name)
        if helper_template:
            imported["helper_templates"][alias_name] = helper_template
            extend_imported_object_templates(imported, context)
            extend_imported_helper_return_object_context(imported, helper_template, context)
        object_template = context.get("object_templates", {}).get(symbol_name)
        if object_template:
            imported["object_templates"][alias_name] = object_template
            extend_imported_object_templates(imported, context)
        if relation or table_relation or helper_relation_template or helper_template or object_template:
            continue
        submodule_context = find_context_by_module_name(contexts, f"{target.get('module', '')}.{symbol_name}")
        if not submodule_context:
            continue
        extend_imported_module_alias_context(imported, alias_name, submodule_context)
    for alias_name, target_module in (imports.get("module_aliases", {}) or {}).items():
        context = find_context_by_module_name(contexts, target_module)
        if not context:
            continue
        extend_imported_module_alias_context(imported, alias_name, context)
    return imported


def extend_imported_module_alias_context(
    imported: dict[str, dict[str, Any]],
    alias_name: str,
    context: dict[str, Any],
) -> None:
    for symbol_name, relation in (context.get("class_relations", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["class_relations"][f"{alias_name}.{symbol_name}"] = relation
    for symbol_name, relation in (context.get("table_relations", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["table_relations"][f"{alias_name}.{symbol_name}"] = relation
    imported["relationship_targets"] = merge_relationship_targets(
        imported["relationship_targets"],
        context.get("relationship_targets", {}),
    )
    for symbol_name, template in (context.get("helper_relation_templates", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["helper_relation_templates"][f"{alias_name}.{symbol_name}"] = template
    for symbol_name, template in (context.get("helper_templates", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["helper_templates"][f"{alias_name}.{symbol_name}"] = qualify_helper_template_for_module_alias(
            template,
            alias_name,
            context,
        )
    for symbol_name, template in (context.get("object_templates", {}) or {}).items():
        if "." in symbol_name:
            continue
        imported["object_templates"][f"{alias_name}.{symbol_name}"] = template


def extend_imported_object_templates(
    imported: dict[str, dict[str, Any]],
    context: dict[str, Any],
) -> None:
    for symbol_name, template in (context.get("object_templates", {}) or {}).items():
        if symbol_name not in imported["object_templates"]:
            imported["object_templates"][symbol_name] = template


def qualify_helper_template_for_module_alias(
    template: dict[str, Any],
    alias_name: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    qualified = deepcopy(template)
    qualify_object_state_for_module_alias(
        qualified.get("return_object", {}),
        alias_name,
        context,
    )
    return qualified


def extend_imported_helper_return_object_context(
    imported: dict[str, dict[str, Any]],
    helper_template: dict[str, Any],
    context: dict[str, Any],
) -> None:
    extend_imported_object_state_context(
        imported,
        helper_template.get("return_object", {}),
        context,
    )


def qualify_object_state_for_module_alias(
    object_state: dict[str, Any],
    alias_name: str,
    context: dict[str, Any],
) -> None:
    if not object_state:
        return
    class_name = object_state.get("class_name", "")
    if class_name and "." not in class_name and class_name in (context.get("object_templates", {}) or {}):
        object_state["class_name"] = f"{alias_name}.{class_name}"
    for nested_state in (object_state.get("object_bindings", {}) or {}).values():
        if isinstance(nested_state, dict):
            qualify_object_state_for_module_alias(nested_state, alias_name, context)


def extend_imported_object_state_context(
    imported: dict[str, dict[str, Any]],
    object_state: dict[str, Any],
    context: dict[str, Any],
) -> None:
    if not object_state:
        return
    class_name = object_state.get("class_name", "")
    if class_name and "." not in class_name:
        object_template = (context.get("object_templates", {}) or {}).get(class_name)
        if object_template and class_name not in imported["object_templates"]:
            imported["object_templates"][class_name] = object_template
    for nested_state in (object_state.get("object_bindings", {}) or {}).values():
        if isinstance(nested_state, dict):
            extend_imported_object_state_context(imported, nested_state, context)


def extract_api_contract_hints(
    relative_path: str,
    text: str,
    *,
    python_context: dict[str, dict[str, Any]] | None = None,
) -> list[dict]:
    if relative_path.endswith(".py"):
        python_hints = extract_api_contract_hints_from_python(
            relative_path,
            text,
            python_context=python_context,
        )
        if python_hints:
            return python_hints

    hints: list[dict] = []
    for method, route in FASTAPI_ROUTE_RE.findall(text):
        route_signature = f"{method.upper()} {route}"
        hints.append(
            {
                "id": build_hint_id("api", relative_path, route_signature),
                "label": route_signature,
                "method": method.upper(),
                "route": route_signature,
                "path": route,
                "file": relative_path,
                "detected_from": "fastapi_decorator",
                "description": f"Detected from {relative_path}",
                "response_fields": [],
                "response_model": "",
            }
        )
    return hints


def extract_ui_contract_hints(relative_path: str, text: str) -> list[dict]:
    api_routes = sorted(set(FETCH_ROUTE_RE.findall(text)) | set(AXIOS_ROUTE_RE.findall(text)))
    if not api_routes:
        return []

    component_names = detect_component_names(relative_path, text)
    route_field_hints = extract_ui_route_field_hints(text)
    used_fields = sorted({field for fields in route_field_hints.values() for field in fields})
    hints: list[dict] = []
    for component_name in component_names:
        hints.append(
            {
                "id": build_hint_id("ui", relative_path, component_name),
                "label": component_name,
                "component": component_name,
                "api_routes": api_routes,
                "file": relative_path,
                "detected_from": "ui_fetch_usage",
                "description": f"Detected from {relative_path}",
                "used_fields": used_fields,
                "route_field_hints": route_field_hints,
            }
        )
    return hints


def extract_sql_structure_hints(relative_path: str, text: str) -> list[dict]:
    return scan_sql_structure_hints(relative_path, text)


def extract_orm_structure_hints(
    relative_path: str,
    text: str,
    *,
    python_context: dict[str, Any] | None = None,
) -> list[dict]:
    python_context = python_context or {}
    return scan_orm_structure_hints(
        relative_path,
        text,
        imported_class_relations=python_context.get("class_relations", {}),
        imported_table_relations=python_context.get("table_relations", {}),
        imported_relationship_targets=python_context.get("relationship_targets", {}),
    )


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
            relations[node.name] = f"{schema_name}.{relation_name}" if schema_name else relation_name
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


def extract_sqlalchemy_field_hint(
    field_name: str,
    value: ast.AST | None,
    *,
    annotation: ast.AST | None = None,
    schema_name: str = "",
) -> dict | None:
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
    candidate_type_nodes.extend(keyword.value for keyword in value.keywords if keyword.arg in {"type_", "type"} and keyword.value is not None)

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
    column_hint: dict[str, object] = {
        "name": field_name,
        "data_type": data_type,
        "source_fields": source_fields,
    }
    for keyword in value.keywords:
        if keyword.arg == "primary_key":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                column_hint["primary_key"] = flag
        elif keyword.arg == "nullable":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                column_hint["nullable"] = flag
        elif keyword.arg == "index":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                column_hint["index"] = flag
        elif keyword.arg == "unique":
            flag = extract_bool_constant(keyword.value)
            if flag is not None:
                column_hint["unique"] = flag

    if source_fields:
        column_hint["foreign_key"] = ",".join(
            f"{source['relation']}.{source['column']}" if source.get("column") else source["relation"]
            for source in source_fields
            if source.get("relation")
        )

    return column_hint


def extract_sqlalchemy_relationship_relation(
    value: ast.AST | None,
    *,
    class_relations: dict[str, str],
    schema_name: str = "",
) -> str:
    if not isinstance(value, ast.Call):
        return ""
    function_name = get_ast_name(value.func).split(".")[-1]
    if function_name not in {"relationship", "dynamic_loader", "association_proxy"}:
        return ""
    if not value.args:
        return ""
    target = value.args[0]
    if isinstance(target, ast.Constant) and isinstance(target.value, str):
        raw_target = target.value
    else:
        raw_target = get_ast_name(target).split(".")[-1]
    if not raw_target:
        return ""
    if raw_target in class_relations:
        return class_relations[raw_target]
    if "." in raw_target:
        return qualify_foreign_relation(raw_target, schema_name=schema_name)[0]
    return ""


def extract_sqlalchemy_source_fields(call_node: ast.Call, *, schema_name: str = "") -> list[dict]:
    source_fields: list[dict] = []
    for candidate in [*call_node.args, *(keyword.value for keyword in call_node.keywords)]:
        relation, column = extract_foreign_key_reference(candidate, schema_name=schema_name)
        if not relation:
            continue
        source_fields.append({"relation": relation, "column": column})
    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for source in source_fields:
        key = (source["relation"], source.get("column", ""))
        if key in seen:
            continue
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
    if not normalized:
        return "", ""
    parts = [part for part in normalized.split(".") if part]
    if len(parts) >= 3:
        return ".".join(parts[:-1]), parts[-1]
    if len(parts) == 2:
        relation = f"{schema_name}.{parts[0]}" if schema_name else parts[0]
        return relation, parts[1]
    relation = f"{schema_name}.{parts[0]}" if schema_name else parts[0]
    return relation, ""


def extract_bool_constant(node: ast.AST | None) -> bool | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return node.value
    return None


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


def extract_api_contract_hints_from_python(
    relative_path: str,
    text: str,
    *,
    python_context: dict[str, dict[str, Any]] | None = None,
) -> list[dict]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []

    module_context = (python_context or {}).get(relative_path, {})
    imported_context = (
        build_imported_python_context(module_context, python_context or {})
        if module_context and python_context
        else {"class_relations": {}, "helper_relation_templates": {}, "helper_templates": {}, "object_templates": {}}
    )
    model_fields = collect_constructor_model_fields(tree)
    class_relations = dict(collect_sqlalchemy_class_relations(tree))
    class_relations.update(imported_context.get("class_relations", {}))
    imported_helper_relations = dict(imported_context.get("helper_relation_templates", {}) or {})
    imported_helper_templates = dict(imported_context.get("helper_templates", {}) or {})
    imported_object_templates = dict(imported_context.get("object_templates", {}) or {})
    helper_relation_templates = build_python_helper_relation_templates(
        tree,
        class_relations,
        initial_templates=imported_helper_relations,
        object_templates=imported_object_templates,
    )
    helper_templates = build_python_helper_templates(
        tree,
        class_relations,
        model_fields,
        helper_relation_templates,
        imported_object_templates,
        initial_templates=imported_helper_templates,
    )
    object_templates = build_python_object_templates(
        tree,
        class_relations,
        helper_relation_templates,
        helper_templates,
        imported_object_templates,
    )
    helper_templates = build_python_helper_templates(
        tree,
        class_relations,
        model_fields,
        helper_relation_templates,
        imported_object_templates | object_templates,
        initial_templates=imported_helper_templates,
    )
    object_templates = build_python_object_templates(
        tree,
        class_relations,
        helper_relation_templates,
        helper_templates,
        imported_object_templates | object_templates,
    )
    helper_relation_templates = imported_helper_relations | helper_relation_templates
    helper_templates = imported_helper_templates | helper_templates
    object_templates = imported_object_templates | object_templates
    hints: list[dict] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        decorators = extract_route_decorators(node)
        if not decorators:
            continue

        variable_relations = collect_function_relation_bindings(
            node,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        variable_sources = collect_local_source_assignments(
            node,
            variable_relations,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        dependency_instances = collect_dependency_object_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        object_instances = collect_local_object_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates,
            object_templates,
            initial_assignments=dependency_instances,
        )
        assigned_dicts = collect_local_dict_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        response_field_sources = collect_return_field_mappings(
            node,
            assigned_dicts,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        response_fields = sorted(response_field_sources)
        for decorator in decorators:
            model_name = decorator["response_model"]
            hinted_fields = set(response_fields)
            if model_name:
                hinted_fields.update(model_fields.get(model_name, []))
            route_signature = f"{decorator['method']} {decorator['path']}"
            hints.append(
                {
                    "id": build_hint_id("api", relative_path, route_signature),
                    "label": route_signature,
                    "method": decorator["method"],
                    "route": route_signature,
                    "path": decorator["path"],
                    "file": relative_path,
                    "detected_from": "fastapi_ast",
                    "description": f"Detected from {relative_path}",
                    "response_fields": sorted(hinted_fields),
                    "response_field_sources": [
                        {"name": field_name, "source_fields": response_field_sources.get(field_name, [])}
                        for field_name in sorted(response_field_sources)
                    ],
                    "response_model": model_name or "",
                }
            )
    return hints


def build_python_helper_templates(
    tree: ast.AST,
    class_relations: dict[str, str],
    model_fields: dict[str, list[str]],
    helper_relation_templates: dict[str, dict[str, Any]],
    object_templates: dict[str, dict[str, Any]],
    *,
    initial_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = dict(initial_templates or {})
    templates.update({
        model_name: {
            "params": list(field_names),
            "return_fields": {
                field_name: [{"relation": f"param:{field_name}", "column": field_name}]
                for field_name in field_names
            },
        }
        for model_name, field_names in sorted(model_fields.items())
        if field_names
    })
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if extract_route_decorators(node):
            continue
        parameters = [
            argument.arg
            for argument in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
            if argument.arg not in {"self", "cls"}
        ]
        parameter_relations = {parameter: f"param:{parameter}" for parameter in parameters}
        variable_relations = collect_function_relation_bindings(
            node,
            class_relations,
            initial_bindings=parameter_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        dependency_instances = collect_dependency_object_assignments(
            node,
            variable_relations,
            class_relations,
            {},
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        variable_sources = collect_local_source_assignments(
            node,
            variable_relations,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=dependency_instances,
        )
        object_instances = collect_local_object_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates,
            object_templates,
            initial_assignments=dependency_instances,
        )
        assigned_dicts = collect_local_dict_assignments(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        return_fields = collect_return_field_mappings(
            node,
            assigned_dicts,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        return_object = collect_return_object_state(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if return_fields or return_object:
            template = {
                "params": parameters,
                "return_fields": return_fields,
            }
            passthrough_params = collect_passthrough_payload_parameters(node, parameters)
            if passthrough_params:
                template["passthrough_payload_params"] = passthrough_params
            if return_object:
                template["return_object"] = return_object
            templates[node.name] = template
    return templates


def build_python_helper_relation_templates(
    tree: ast.AST,
    class_relations: dict[str, str],
    *,
    initial_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = dict(initial_templates or {})
    for _ in range(3):
        updated = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if extract_route_decorators(node):
                continue
            parameters = [
                argument.arg
                for argument in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
                if argument.arg not in {"self", "cls"}
            ]
            parameter_relations = {parameter: f"param:{parameter}" for parameter in parameters}
            variable_relations = collect_function_relation_bindings(
                node,
                class_relations,
                initial_bindings=parameter_relations,
                helper_relation_templates=templates,
                object_templates=object_templates,
            )
            dependency_instances = collect_dependency_object_assignments(
                node,
                variable_relations,
                class_relations,
                {},
                {},
                helper_relation_templates=templates,
                object_templates=object_templates,
            )
            return_relation = collect_return_relation(
                node,
                variable_relations,
                class_relations,
                helper_relation_templates=templates,
                object_templates=object_templates,
                object_instances=dependency_instances,
            )
            if not return_relation:
                continue
            template = {"params": parameters, "return_relation": return_relation}
            if templates.get(node.name) != template:
                templates[node.name] = template
                updated = True
        if not updated:
            break
    return templates


def build_python_object_templates(
    tree: ast.AST,
    class_relations: dict[str, str],
    helper_relation_templates: dict[str, dict[str, Any]],
    helper_templates: dict[str, dict[str, Any]],
    previous_templates: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = {}
    for class_node in ast.walk(tree):
        if not isinstance(class_node, ast.ClassDef):
            continue
        init_node = next(
            (
                statement
                for statement in class_node.body
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)) and statement.name == "__init__"
            ),
            None,
        )
        init_params = [
            argument.arg
            for argument in [*init_node.args.posonlyargs, *init_node.args.args, *init_node.args.kwonlyargs]
            if init_node and argument.arg not in {"self", "cls"}
        ] if init_node else []
        attribute_bindings = collect_init_attribute_bindings(init_node)
        methods: dict[str, dict[str, Any]] = {}
        current_class_template = {
            "params": init_params,
            "attributes": attribute_bindings,
            "methods": methods,
        }
        for statement in class_node.body:
            if not isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if statement.name == "__init__":
                continue
            template_context = previous_templates | templates | {class_node.name: current_class_template}
            method_params = [
                argument.arg
                for argument in [*statement.args.posonlyargs, *statement.args.args, *statement.args.kwonlyargs]
                if argument.arg not in {"self", "cls"}
            ]
            initial_bindings = {f"self.{attr_name}": placeholder for attr_name, placeholder in attribute_bindings.items()}
            initial_bindings.update({parameter: f"param:{parameter}" for parameter in method_params})
            self_instance = {
                "class_name": class_node.name,
                "relation_bindings": dict(attribute_bindings),
                "source_bindings": {},
                "object_bindings": {
                    attr_name: {"param_object": placeholder.split(":", 1)[1]}
                    for attr_name, placeholder in attribute_bindings.items()
                    if placeholder.startswith("param:")
                },
            }
            variable_relations = collect_function_relation_bindings(
                statement,
                class_relations,
                initial_bindings=initial_bindings,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances={"self": self_instance},
            )
            dependency_instances = collect_dependency_object_assignments(
                statement,
                variable_relations,
                class_relations,
                {},
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
            )
            variable_sources = collect_local_source_assignments(
                statement,
                variable_relations,
                helper_templates,
                initial_assignments=self_instance.get("source_bindings", {}),
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances={"self": self_instance} | dependency_instances,
            )
            object_instances = collect_local_object_assignments(
                statement,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates,
                template_context,
                initial_assignments={"self": self_instance} | dependency_instances,
            )
            assigned_dicts = collect_local_dict_assignments(
                statement,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances=object_instances,
            )
            return_fields = collect_return_field_mappings(
                statement,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances=object_instances,
            )
            return_relation = collect_return_relation(
                statement,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=template_context,
                object_instances=object_instances,
            )
            methods[statement.name] = {
                "params": method_params,
                "return_fields": return_fields,
                "return_relation": return_relation,
                "body_node": statement,
            }
            passthrough_params = collect_passthrough_payload_parameters(statement, method_params)
            if passthrough_params:
                methods[statement.name]["passthrough_payload_params"] = passthrough_params
        if attribute_bindings or methods:
            templates[class_node.name] = current_class_template
    return templates


def collect_constructor_model_fields(tree: ast.AST) -> dict[str, list[str]]:
    models: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or not class_looks_like_constructor_model(node):
            continue
        field_names: list[str] = []
        for statement in node.body:
            if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                field_names.append(statement.target.id)
            elif isinstance(statement, ast.Assign):
                for target in statement.targets:
                    if isinstance(target, ast.Name):
                        field_names.append(target.id)
        if field_names:
            models[node.name] = sorted(dict.fromkeys(field_names))
    return models


def class_looks_like_constructor_model(node: ast.ClassDef) -> bool:
    for base in node.bases:
        name = get_ast_name(base)
        if name.split(".")[-1] == "BaseModel":
            return True
    for decorator in node.decorator_list:
        name = get_ast_name(decorator)
        if name.split(".")[-1] == "dataclass":
            return True
    return False


def extract_route_decorators(function_node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[dict[str, str]]:
    decorators: list[dict[str, str]] = []
    for decorator in function_node.decorator_list:
        if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
            continue
        method_name = decorator.func.attr.lower()
        if method_name not in {"get", "post", "put", "patch", "delete"}:
            continue
        if not decorator.args or not isinstance(decorator.args[0], ast.Constant) or not isinstance(decorator.args[0].value, str):
            continue
        path = decorator.args[0].value
        response_model = ""
        for keyword in decorator.keywords:
            if keyword.arg == "response_model":
                response_model = extract_model_name(keyword.value)
                break
        decorators.append({"method": method_name.upper(), "path": path, "response_model": response_model})
    return decorators


def extract_model_name(node: ast.AST | None) -> str:
    if node is None:
        return ""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return extract_model_name(get_subscript_slice(node))
    if isinstance(node, ast.Tuple):
        for element in node.elts:
            model_name = extract_model_name(element)
            if model_name:
                return model_name
    return ""


def get_subscript_slice(node: ast.Subscript) -> ast.AST:
    return node.slice if not isinstance(node.slice, ast.Index) else node.slice.value


def iter_ordered_statement_nodes(node: ast.AST, *, root: ast.AST | None = None) -> Iterable[ast.stmt]:
    root_node = root or node
    if node is not root_node and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return
    if node is not root_node and isinstance(node, ast.stmt):
        yield node
    for child in ast.iter_child_nodes(node):
        if child is not root_node and isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        yield from iter_ordered_statement_nodes(child, root=root_node)


def extract_target_names(node: ast.AST | None) -> list[str]:
    if node is None:
        return []
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        names: list[str] = []
        for element in node.elts:
            names.extend(extract_target_names(element))
        return names
    return []


def unwrap_await(node: ast.AST | None) -> ast.AST | None:
    current = node
    while isinstance(current, ast.Await):
        current = current.value
    return current


def extend_relations_for_comprehensions(
    generators: list[ast.comprehension],
    variable_relations: dict[str, str],
) -> dict[str, str]:
    extended = dict(variable_relations)
    for generator in generators:
        relation = extract_relation_from_expression(generator.iter, extended, {})
        if not relation:
            continue
        for target_name in extract_target_names(generator.target):
            extended[target_name] = relation
    return extended


def is_empty_mapping_expression(node: ast.AST | None) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.Dict):
        return not any(isinstance(key, ast.Constant) and isinstance(key.value, str) for key in node.keys)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict":
        return not node.args and not node.keywords
    return False


def extract_dict_assignment_target(node: ast.AST | None) -> tuple[str, str]:
    if not isinstance(node, ast.Subscript) or not isinstance(node.value, ast.Name):
        return "", ""
    key_node = get_subscript_slice(node)
    if not isinstance(key_node, ast.Constant) or not isinstance(key_node.value, str):
        return "", ""
    return node.value.id, key_node.value


def collect_init_attribute_bindings(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef | None,
) -> dict[str, str]:
    if function_node is None:
        return {}
    bindings: dict[str, str] = {}
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Attribute):
            continue
        target = node.targets[0]
        if not isinstance(target.value, ast.Name) or target.value.id != "self":
            continue
        if not isinstance(node.value, ast.Name):
            continue
        bindings[target.attr] = f"param:{node.value.id}"
    return bindings


def collect_return_relation(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> str:
    relations = {
        extract_relation_from_expression(
            node.value,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        for node in iter_ordered_statement_nodes(function_node, root=function_node)
        if isinstance(node, ast.Return)
    }
    relations.discard("")
    if len(relations) == 1:
        return next(iter(relations))
    return ""


def collect_function_relation_bindings(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    class_relations: dict[str, str],
    *,
    initial_bindings: dict[str, str] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, str]:
    bindings: dict[str, str] = dict(initial_bindings or {})
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            relation = extract_relation_from_expression(
                node.value,
                bindings,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if not relation:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bindings[target.id] = relation
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            relation = (
                extract_relation_from_annotation(node.annotation, class_relations)
                or extract_relation_from_expression(
                    node.value,
                    bindings,
                    class_relations,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
            )
            if relation:
                bindings[node.target.id] = relation
    return bindings


def collect_local_source_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    helper_templates: dict[str, dict[str, Any]],
    *,
    initial_assignments: dict[str, list[dict[str, str]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    assignments: dict[str, list[dict[str, str]]] = {
        name: [dict(source) for source in source_fields]
        for name, source_fields in (initial_assignments or {}).items()
    }
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            source_fields = extract_source_fields_from_expression(
                node.value,
                variable_relations,
                assignments,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if not source_fields:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = [dict(source) for source in source_fields]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            source_fields = extract_source_fields_from_expression(
                node.value,
                variable_relations,
                assignments,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if source_fields:
                assignments[node.target.id] = [dict(source) for source in source_fields]
    return assignments


def collect_local_object_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]],
    object_templates: dict[str, dict[str, Any]],
    *,
    initial_assignments: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    assignments: dict[str, dict[str, Any]] = dict(initial_assignments or {})
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            instance_state = build_object_instance_state(
                node.value,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates,
                object_templates,
                assignments,
            )
            if not instance_state:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = instance_state
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            instance_state = build_object_instance_state(
                node.value,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates,
                object_templates,
                assignments,
            )
            if instance_state:
                assignments[node.target.id] = instance_state
    return assignments


def collect_dependency_object_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    assignments: dict[str, dict[str, Any]] = {}
    positional_args = [*function_node.args.posonlyargs, *function_node.args.args]
    positional_defaults = [None] * (len(positional_args) - len(function_node.args.defaults)) + list(function_node.args.defaults)
    for argument, default in zip(positional_args, positional_defaults):
        if argument.arg in {"self", "cls"}:
            continue
        dependency_hint = default or extract_depends_annotation(argument.annotation)
        instance_state = build_dependency_object_instance_state(
            dependency_hint,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        if instance_state:
            assignments[argument.arg] = instance_state
    for argument, default in zip(function_node.args.kwonlyargs, function_node.args.kw_defaults):
        if argument.arg in {"self", "cls"}:
            continue
        dependency_hint = default or extract_depends_annotation(argument.annotation)
        instance_state = build_dependency_object_instance_state(
            dependency_hint,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
        )
        if instance_state:
            assignments[argument.arg] = instance_state
    return assignments


def extract_depends_annotation(node: ast.AST | None) -> ast.AST | None:
    if node is None:
        return None
    if isinstance(node, ast.Call) and get_ast_name(node.func).split(".")[-1] == "Depends":
        return node
    if isinstance(node, ast.Subscript):
        base_name = get_ast_name(node.value).split(".")[-1]
        if base_name == "Annotated":
            slice_node = get_subscript_slice(node)
            for candidate in extract_annotation_elements(slice_node):
                dependency = extract_depends_annotation(candidate)
                if dependency is not None:
                    return dependency
        return extract_depends_annotation(get_subscript_slice(node))
    if isinstance(node, ast.Tuple):
        for element in node.elts:
            dependency = extract_depends_annotation(element)
            if dependency is not None:
                return dependency
    return None


def extract_annotation_elements(node: ast.AST | None) -> list[ast.AST]:
    if node is None:
        return []
    if isinstance(node, ast.Tuple):
        return list(node.elts)
    return [node]


def build_dependency_object_instance_state(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    call = unwrap_await(node)
    if not isinstance(call, ast.Call):
        return {}
    if get_ast_name(call.func).split(".")[-1] != "Depends":
        return {}
    dependency_target: ast.AST | None = None
    if call.args:
        dependency_target = call.args[0]
    else:
        dependency_keyword = next((item for item in call.keywords if item.arg == "dependency"), None)
        dependency_target = dependency_keyword.value if dependency_keyword else None
    if dependency_target is None:
        return {}
    dependency_call = ast.Call(func=dependency_target, args=[], keywords=[])
    return instantiate_helper_object_state(
        dependency_call,
        variable_relations=variable_relations,
        class_relations=class_relations,
        variable_sources=variable_sources,
        helper_templates=helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=None,
    )


def collect_local_dict_assignments(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    initial_assignments: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, list[dict[str, str]]]]:
    assignments: dict[str, dict[str, list[dict[str, str]]]] = {
        name: deep_copy_field_mappings(field_mappings)
        for name, field_mappings in (initial_assignments or {}).items()
    }
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Assign):
            field_mappings = extract_response_field_mapping_from_value(
                node.value,
                assignments,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if field_mappings or is_empty_mapping_expression(node.value):
                        assignments[target.id] = deep_copy_field_mappings(field_mappings)
                    continue
                dict_name, field_name = extract_dict_assignment_target(target)
                if not dict_name or not field_name:
                    continue
                source_fields = extract_source_fields_from_expression(
                    node.value,
                    variable_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
                if not source_fields:
                    continue
                assignments.setdefault(dict_name, {})
                assignments[dict_name][field_name] = [dict(source) for source in source_fields]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            field_mappings = extract_response_field_mapping_from_value(
                node.value,
                assignments,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if field_mappings or is_empty_mapping_expression(node.value):
                assignments[node.target.id] = deep_copy_field_mappings(field_mappings)
        elif isinstance(node, ast.Expr):
            call = unwrap_await(node.value)
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "update"
                and isinstance(call.func.value, ast.Name)
            ):
                dict_name = call.func.value.id
                field_mappings: dict[str, list[dict[str, str]]] = {}
                if call.args:
                    merge_field_mappings(
                        field_mappings,
                        extract_response_field_mapping_from_value(
                            call.args[0],
                            assignments,
                            variable_relations,
                            class_relations,
                            variable_sources,
                            helper_templates,
                            helper_relation_templates=helper_relation_templates,
                            object_templates=object_templates,
                            object_instances=object_instances,
                        ),
                    )
                for keyword in call.keywords:
                    if not keyword.arg:
                        continue
                    field_mappings[keyword.arg] = extract_source_fields_from_expression(
                        keyword.value,
                        variable_relations,
                        variable_sources,
                        helper_templates,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    )
                if field_mappings:
                    assignments.setdefault(dict_name, {})
                    merge_field_mappings(assignments[dict_name], field_mappings)
        elif isinstance(node, ast.AugAssign):
            target = node.target
            if (
                isinstance(target, ast.Name)
                and isinstance(node.op, ast.BitOr)
            ):
                field_mappings = extract_response_field_mapping_from_value(
                    node.value,
                    assignments,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
                if field_mappings:
                    assignments.setdefault(target.id, {})
                    merge_field_mappings(assignments[target.id], field_mappings)
    return assignments


def collect_return_field_mappings(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]],
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    fields: dict[str, list[dict[str, str]]] = {}
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if isinstance(node, ast.Return):
            merge_field_mappings(
                fields,
                extract_response_field_mapping_from_value(
                    node.value,
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                ),
            )
    return fields


def collect_passthrough_payload_parameters(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    parameters: list[str],
) -> list[str]:
    passthrough: list[str] = []
    parameter_set = set(parameters)
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if not isinstance(node, ast.Return):
            continue
        value = unwrap_await(node.value)
        if isinstance(value, ast.Name) and value.id in parameter_set and value.id not in passthrough:
            passthrough.append(value.id)
    return passthrough


def extract_response_field_mapping_from_value(
    node: ast.AST | None,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]],
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    node = unwrap_await(node)
    if node is None:
        return {}
    if isinstance(node, ast.Dict):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        for key, value in zip(node.keys, node.values):
            if key is None:
                merge_field_mappings(
                    field_mappings,
                    extract_response_field_mapping_from_value(
                        value,
                        assigned_dicts,
                        variable_relations,
                        class_relations,
                        variable_sources,
                        helper_templates,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    ),
                )
                continue
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                continue
            field_mappings[key.value] = extract_source_fields_from_expression(
                value,
                variable_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
        return field_mappings
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        for element in node.elts:
            merge_field_mappings(
                field_mappings,
                extract_response_field_mapping_from_value(
                    element,
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                ),
            )
        return field_mappings
    if isinstance(node, ast.ListComp):
        comprehension_relations = extend_relations_for_comprehensions(node.generators, variable_relations)
        return extract_response_field_mapping_from_value(
            node.elt,
            assigned_dicts,
            comprehension_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
    if isinstance(node, ast.Name):
        return deep_copy_field_mappings(assigned_dicts.get(node.id, {}))
    if isinstance(node, ast.IfExp):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.body,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.orelse,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        return field_mappings
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        field_mappings: dict[str, list[dict[str, str]]] = {}
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.left,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        merge_field_mappings(
            field_mappings,
            extract_response_field_mapping_from_value(
                node.right,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ),
        )
        return field_mappings
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name) and node.func.id == "dict":
            if node.keywords:
                field_mappings: dict[str, list[dict[str, str]]] = {}
                for keyword in node.keywords:
                    if not keyword.arg:
                        continue
                    field_mappings[keyword.arg] = extract_source_fields_from_expression(
                        keyword.value,
                        variable_relations,
                        variable_sources,
                        helper_templates,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    )
                return field_mappings
            if node.args:
                return extract_response_field_mapping_from_value(
                    node.args[0],
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
        if isinstance(node.func, ast.Attribute) and node.func.attr in {"model_dump", "dict"}:
            return extract_response_field_mapping_from_value(
                node.func.value,
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
        helper_mapping = instantiate_helper_field_mapping(
            node,
            variable_relations=variable_relations,
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if helper_mapping:
            return helper_mapping
        object_method_mapping = instantiate_object_method_field_mapping(
            node,
            variable_relations=variable_relations,
            class_relations=class_relations,
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if object_method_mapping:
            return object_method_mapping
        for keyword in node.keywords:
            if keyword.arg in {"content", "body"}:
                return extract_response_field_mapping_from_value(
                    keyword.value,
                    assigned_dicts,
                    variable_relations,
                    class_relations,
                    variable_sources,
                    helper_templates,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
        if node.args:
            return extract_response_field_mapping_from_value(
                node.args[0],
                assigned_dicts,
                variable_relations,
                class_relations,
                variable_sources,
                helper_templates,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
    return {}


def extract_source_fields_from_expression(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    visited: set[tuple[str, str]] = set()

    def append_source(relation: str, column: str) -> None:
        if not relation or not column:
            return
        key = (relation, column)
        if key in visited:
            return
        visited.add(key)
        collected.append({"relation": relation, "column": column})

    def walk(current: ast.AST | None) -> None:
        current = unwrap_await(current)
        if current is None:
            return
        if isinstance(current, ast.Name):
            for source in variable_sources.get(current.id, []):
                append_source(source.get("relation", ""), source.get("column", ""))
            return
        if isinstance(current, ast.Attribute):
            relation, column = resolve_attribute_source(current, variable_relations)
            if relation and column:
                append_source(relation, column)
                return
            walk(current.value)
            return
        if isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute) and current.func.attr in {"model_dump", "dict"}:
                walk(current.func.value)
                return
            helper_mapping = instantiate_helper_field_mapping(
                current,
                variable_relations=variable_relations,
                variable_sources=variable_sources,
                helper_templates=helper_templates,
                assigned_dicts=assigned_dicts,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if len(helper_mapping) == 1:
                only_sources = next(iter(helper_mapping.values()), [])
                for source in only_sources:
                    append_source(source.get("relation", ""), source.get("column", ""))
                return
            object_method_mapping = instantiate_object_method_field_mapping(
                current,
                variable_relations=variable_relations,
                class_relations={},
                variable_sources=variable_sources,
                helper_templates=helper_templates,
                assigned_dicts=assigned_dicts,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if len(object_method_mapping) == 1:
                only_sources = next(iter(object_method_mapping.values()), [])
                for source in only_sources:
                    append_source(source.get("relation", ""), source.get("column", ""))
                return
            if isinstance(current.func, ast.Attribute):
                walk(current.func.value)
            for argument in current.args:
                walk(argument)
            for keyword in current.keywords:
                walk(keyword.value)
            return
        if isinstance(current, ast.ListComp):
            comprehension_relations = extend_relations_for_comprehensions(current.generators, variable_relations)
            for source in extract_source_fields_from_expression(
                current.elt,
                comprehension_relations,
                variable_sources,
                helper_templates,
                assigned_dicts=assigned_dicts,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            ):
                append_source(source.get("relation", ""), source.get("column", ""))
            for generator in current.generators:
                for if_clause in generator.ifs:
                    for source in extract_source_fields_from_expression(
                        if_clause,
                        comprehension_relations,
                        variable_sources,
                        helper_templates,
                        assigned_dicts=assigned_dicts,
                        helper_relation_templates=helper_relation_templates,
                        object_templates=object_templates,
                        object_instances=object_instances,
                    ):
                        append_source(source.get("relation", ""), source.get("column", ""))
            return
        if isinstance(current, ast.BinOp):
            walk(current.left)
            walk(current.right)
            return
        if isinstance(current, ast.UnaryOp):
            walk(current.operand)
            return
        if isinstance(current, ast.BoolOp):
            for value in current.values:
                walk(value)
            return
        if isinstance(current, ast.Compare):
            walk(current.left)
            for comparator in current.comparators:
                walk(comparator)
            return
        if isinstance(current, ast.IfExp):
            walk(current.body)
            walk(current.test)
            walk(current.orelse)
            return
        if isinstance(current, ast.Subscript):
            walk(current.value)
            walk(get_subscript_slice(current))
            return
        if isinstance(current, (ast.Tuple, ast.List, ast.Set)):
            for element in current.elts:
                walk(element)
            return
        if isinstance(current, ast.Dict):
            for value in current.values:
                walk(value)
            return

    walk(node)
    return collected


def instantiate_helper_field_mapping(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    function_name = get_ast_name(node.func)
    if not function_name:
        return {}
    helper = helper_templates.get(function_name)
    if not helper:
        return {}
    parameter_names = helper.get("params", [])
    relation_bindings: dict[str, str] = {}
    source_bindings: dict[str, list[dict[str, str]]] = {}
    payload_bindings: dict[str, dict[str, list[dict[str, str]]]] = {}

    for index, parameter_name in enumerate(parameter_names):
        argument = None
        if index < len(node.args):
            argument = node.args[index]
        else:
            keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            {},
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[parameter_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            source_bindings[parameter_name] = [dict(source) for source in source_fields]
        payload_mapping = extract_response_field_mapping_from_value(
            argument,
            assigned_dicts or {},
            variable_relations,
            {},
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if payload_mapping:
            payload_bindings[parameter_name] = payload_mapping
    resolved = resolve_placeholder_field_mappings(helper.get("return_fields", {}), relation_bindings, source_bindings)
    for parameter_name in helper.get("passthrough_payload_params", []):
        merge_field_mappings(resolved, payload_bindings.get(parameter_name, {}))
    return resolved


def instantiate_helper_object_state(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    function_name = get_ast_name(node.func)
    if not function_name:
        return {}
    helper = helper_templates.get(function_name)
    if not helper:
        return {}
    template_state = helper.get("return_object", {})
    if not template_state:
        return {}
    parameter_names = helper.get("params", [])
    relation_bindings: dict[str, str] = {}
    source_bindings: dict[str, list[dict[str, str]]] = {}
    object_param_bindings: dict[str, dict[str, Any]] = {}
    for index, parameter_name in enumerate(parameter_names):
        argument = None
        if index < len(node.args):
            argument = node.args[index]
        else:
            keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[parameter_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            source_bindings[parameter_name] = [dict(source) for source in source_fields]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            object_param_bindings[parameter_name] = nested_instance
    return resolve_object_state_template(
        template_state,
        relation_bindings,
        source_bindings,
        object_param_bindings,
    )


def resolve_attribute_source(node: ast.Attribute, variable_relations: dict[str, str]) -> tuple[str, str]:
    column_name = node.attr
    base = node.value
    if isinstance(base, ast.Name):
        relation = variable_relations.get(base.id, "")
        return relation, column_name
    if isinstance(base, ast.Attribute):
        relation = extract_relation_from_expression(base, variable_relations, {})
        return relation, column_name
    return "", ""


def resolve_placeholder_field_mappings(
    template_fields: dict[str, list[dict[str, str]]],
    relation_bindings: dict[str, str],
    source_bindings: dict[str, list[dict[str, str]]],
) -> dict[str, list[dict[str, str]]]:
    instantiated: dict[str, list[dict[str, str]]] = {}
    for field_name, source_fields in template_fields.items():
        resolved_sources: list[dict[str, str]] = []
        for source in source_fields:
            relation = source.get("relation", "")
            column = source.get("column", "")
            if relation.startswith("param:"):
                parameter_name = relation.split(":", 1)[1]
                bound_relation = relation_bindings.get(parameter_name, "")
                if bound_relation and column:
                    resolved_sources.append({"relation": bound_relation, "column": column})
                    continue
                for bound_source in source_bindings.get(parameter_name, []):
                    if column and bound_source.get("column") == column:
                        resolved_sources.append(dict(bound_source))
            elif relation and column:
                resolved_sources.append({"relation": relation, "column": column})
        if resolved_sources:
            instantiated[field_name] = dedupe_source_fields(resolved_sources)
    return instantiated


def resolve_placeholder_relations(
    template_relations: dict[str, str],
    relation_bindings: dict[str, str],
) -> dict[str, str]:
    instantiated: dict[str, str] = {}
    for key, relation in template_relations.items():
        if relation.startswith("param:"):
            bound_relation = relation_bindings.get(relation.split(":", 1)[1], "")
            if bound_relation:
                instantiated[key] = bound_relation
            continue
        if relation:
            instantiated[key] = relation
    return instantiated


def resolve_object_state_template(
    template_state: dict[str, Any],
    relation_bindings: dict[str, str],
    source_bindings: dict[str, list[dict[str, str]]],
    object_param_bindings: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not template_state:
        return {}
    if template_state.get("param_object"):
        return deepcopy(object_param_bindings.get(template_state["param_object"], {}))
    resolved = {
        "class_name": template_state.get("class_name", ""),
        "relation_bindings": resolve_placeholder_relations(
            template_state.get("relation_bindings", {}),
            relation_bindings,
        ),
        "source_bindings": resolve_placeholder_field_mappings(
            template_state.get("source_bindings", {}),
            relation_bindings,
            source_bindings,
        ),
        "object_bindings": resolve_placeholder_object_bindings(
            template_state.get("object_bindings", {}),
            relation_bindings,
            source_bindings,
            object_param_bindings,
        ),
    }
    if not resolved["object_bindings"]:
        resolved.pop("object_bindings")
    return resolved


def resolve_placeholder_object_bindings(
    template_bindings: dict[str, dict[str, Any]],
    relation_bindings: dict[str, str],
    source_bindings: dict[str, list[dict[str, str]]],
    object_param_bindings: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    instantiated: dict[str, dict[str, Any]] = {}
    for key, template_state in template_bindings.items():
        resolved = resolve_object_state_template(
            template_state,
            relation_bindings,
            source_bindings,
            object_param_bindings,
        )
        if resolved:
            instantiated[key] = resolved
    return instantiated


def resolve_object_instance_from_expression(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    node = unwrap_await(node)
    if node is None:
        return {}
    if isinstance(node, ast.Name) and object_instances and node.id in object_instances:
        return deepcopy(object_instances[node.id])
    if isinstance(node, ast.Attribute):
        owner_state = resolve_object_instance_from_expression(
            node.value,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if owner_state:
            nested = (owner_state.get("object_bindings", {}) or {}).get(node.attr, {})
            if nested:
                return deepcopy(nested)
        return {}
    if isinstance(node, ast.Call):
        return build_object_instance_state(
            node,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates or {},
            object_templates or {},
            object_instances,
        )
    return {}


def build_object_instance_state(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]],
    object_templates: dict[str, dict[str, Any]],
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    node = unwrap_await(node)
    if not isinstance(node, ast.Call):
        return {}
    helper_instance = instantiate_helper_object_state(
        node,
        variable_relations=variable_relations,
        class_relations=class_relations,
        variable_sources=variable_sources,
        helper_templates=helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if helper_instance:
        return helper_instance
    class_name = get_ast_name(node.func)
    if not class_name:
        return {}
    class_template = object_templates.get(class_name)
    if not class_template:
        return {}
    parameter_names = class_template.get("params", [])
    relation_bindings: dict[str, str] = {}
    source_bindings: dict[str, list[dict[str, str]]] = {}
    object_bindings: dict[str, dict[str, Any]] = {}
    for attr_name, placeholder in class_template.get("attributes", {}).items():
        if not placeholder.startswith("param:"):
            continue
        parameter_name = placeholder.split(":", 1)[1]
        argument = None
        if parameter_name in parameter_names:
            parameter_index = parameter_names.index(parameter_name)
            if parameter_index < len(node.args):
                argument = node.args[parameter_index]
        if argument is None:
            keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[attr_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            source_bindings[attr_name] = [dict(source) for source in source_fields]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            object_bindings[attr_name] = nested_instance
        elif (
            isinstance(argument, ast.Name)
            and argument.id not in class_relations
            and argument.id not in variable_relations
            and not source_fields
        ):
            object_bindings[attr_name] = {"param_object": argument.id}
    return {
        "class_name": class_name,
        "relation_bindings": relation_bindings,
        "source_bindings": source_bindings,
        "object_bindings": object_bindings,
    }


def collect_return_object_state(
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for node in iter_ordered_statement_nodes(function_node, root=function_node):
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        instance_state = resolve_object_instance_from_expression(
            node.value,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if instance_state:
            candidates.append(instance_state)
    if len(candidates) != 1:
        return {}
    return deepcopy(candidates[0])


def evaluate_bound_object_method(
    method_template: dict[str, Any],
    call_node: ast.Call,
    instance_state: dict[str, Any],
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
) -> tuple[dict[str, list[dict[str, str]]], str]:
    method_node = method_template.get("body_node")
    if not isinstance(method_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return {}, ""
    method_params = method_template.get("params", [])
    method_relation_bindings: dict[str, str] = {}
    method_source_bindings: dict[str, list[dict[str, str]]] = {}
    method_object_bindings: dict[str, dict[str, Any]] = {}
    method_payload_bindings: dict[str, dict[str, list[dict[str, str]]]] = {}
    for index, parameter_name in enumerate(method_params):
        argument = None
        if index < len(call_node.args):
            argument = call_node.args[index]
        else:
            keyword = next((item for item in call_node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            method_relation_bindings[parameter_name] = relation
        source_fields = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields:
            method_source_bindings[parameter_name] = [dict(source) for source in source_fields]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            method_object_bindings[parameter_name] = nested_instance
        payload_mapping = extract_response_field_mapping_from_value(
            argument,
            assigned_dicts or {},
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if payload_mapping:
            method_payload_bindings[parameter_name] = payload_mapping

    initial_relations = {
        f"self.{attr_name}": relation
        for attr_name, relation in (instance_state.get("relation_bindings", {}) or {}).items()
    }
    initial_relations.update({parameter: f"param:{parameter}" for parameter in method_params})
    initial_relations.update(method_relation_bindings)
    bound_self = deepcopy(instance_state)
    nested_object_instances = {"self": bound_self}
    nested_object_instances.update(method_object_bindings)
    bound_relations = collect_function_relation_bindings(
        method_node,
        class_relations,
        initial_bindings=initial_relations,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=nested_object_instances,
    )
    dependency_instances = collect_dependency_object_assignments(
        method_node,
        bound_relations,
        class_relations,
        method_source_bindings,
        helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
    )
    nested_object_instances.update(dependency_instances)
    local_sources = collect_local_source_assignments(
        method_node,
        bound_relations,
        helper_templates,
        initial_assignments=method_source_bindings,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=nested_object_instances,
    )
    local_objects = collect_local_object_assignments(
        method_node,
        bound_relations,
        class_relations,
        local_sources,
        helper_templates,
        helper_relation_templates or {},
        object_templates or {},
        initial_assignments=nested_object_instances,
    )
    assigned_dicts = collect_local_dict_assignments(
        method_node,
        bound_relations,
        class_relations,
        local_sources,
        helper_templates,
        initial_assignments=method_payload_bindings,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=local_objects,
    )
    return_fields = collect_return_field_mappings(
        method_node,
        assigned_dicts,
        bound_relations,
        class_relations,
        local_sources,
        helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=local_objects,
    )
    for parameter_name in method_template.get("passthrough_payload_params", []):
        merge_field_mappings(return_fields, method_payload_bindings.get(parameter_name, {}))
    return_relation = collect_return_relation(
        method_node,
        bound_relations,
        class_relations,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=local_objects,
    )
    return return_fields, return_relation


def bind_object_method_call_state(
    method_template: dict[str, Any],
    call_node: ast.Call,
    instance_state: dict[str, Any],
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
) -> tuple[dict[str, str], dict[str, list[dict[str, str]]], dict[str, dict[str, Any]]]:
    relation_bindings = dict(instance_state.get("relation_bindings", {}))
    source_bindings = {
        name: [dict(source) for source in source_fields]
        for name, source_fields in (instance_state.get("source_bindings", {}) or {}).items()
    }
    object_bindings = {
        name: deepcopy(state)
        for name, state in (instance_state.get("object_bindings", {}) or {}).items()
    }
    payload_bindings: dict[str, dict[str, list[dict[str, str]]]] = {}
    for index, parameter_name in enumerate(method_template.get("params", [])):
        argument = None
        if index < len(call_node.args):
            argument = call_node.args[index]
        else:
            keyword = next((item for item in call_node.keywords if item.arg == parameter_name), None)
            argument = keyword.value if keyword else None
        if argument is None:
            continue
        relation = extract_relation_from_expression(
            argument,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            relation_bindings[parameter_name] = relation
        source_fields_for_param = extract_source_fields_from_expression(
            argument,
            variable_relations,
            variable_sources,
            helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if source_fields_for_param:
            source_bindings[parameter_name] = [dict(source) for source in source_fields_for_param]
        nested_instance = resolve_object_instance_from_expression(
            argument,
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if nested_instance:
            object_bindings[parameter_name] = nested_instance
        payload_mapping = extract_response_field_mapping_from_value(
            argument,
            assigned_dicts or {},
            variable_relations,
            class_relations,
            variable_sources,
            helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if payload_mapping:
            payload_bindings[parameter_name] = payload_mapping
    return relation_bindings, source_bindings, object_bindings, payload_bindings


def instantiate_object_method_field_mapping(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str] | None = None,
    variable_sources: dict[str, list[dict[str, str]]],
    helper_templates: dict[str, dict[str, Any]],
    assigned_dicts: dict[str, dict[str, list[dict[str, str]]]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    if not isinstance(node.func, ast.Attribute) or not object_templates:
        return {}
    instance_state = resolve_object_instance_from_expression(
        node.func.value,
        variable_relations,
        class_relations or {},
        variable_sources,
        helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if not instance_state:
        return {}
    class_template = object_templates.get(instance_state.get("class_name", ""), {})
    method_template = class_template.get("methods", {}).get(node.func.attr, {})
    if method_template.get("return_fields"):
        relation_bindings, source_bindings, _, payload_bindings = bind_object_method_call_state(
            method_template,
            node,
            instance_state,
            variable_relations=variable_relations,
            class_relations=class_relations or {},
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            assigned_dicts=assigned_dicts,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        resolved = resolve_placeholder_field_mappings(
            method_template.get("return_fields", {}),
            relation_bindings,
            source_bindings,
        )
        for parameter_name in method_template.get("passthrough_payload_params", []):
            merge_field_mappings(resolved, payload_bindings.get(parameter_name, {}))
        runtime_fields, _ = evaluate_bound_object_method(
            method_template,
            node,
            instance_state,
            variable_relations=variable_relations,
            class_relations=class_relations or {},
            variable_sources=variable_sources,
            helper_templates=helper_templates,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
            assigned_dicts=assigned_dicts,
        )
        merge_field_mappings(resolved, runtime_fields)
        return resolved
    return_fields, _ = evaluate_bound_object_method(
        method_template,
        node,
        instance_state,
        variable_relations=variable_relations,
        class_relations=class_relations or {},
        variable_sources=variable_sources,
        helper_templates=helper_templates,
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
        assigned_dicts=assigned_dicts,
    )
    return return_fields


def instantiate_object_method_relation(
    node: ast.Call,
    *,
    variable_relations: dict[str, str],
    class_relations: dict[str, str] | None = None,
    variable_sources: dict[str, list[dict[str, str]]] | None = None,
    helper_templates: dict[str, dict[str, Any]] | None = None,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> str:
    if not isinstance(node.func, ast.Attribute) or not object_templates:
        return ""
    instance_state = resolve_object_instance_from_expression(
        node.func.value,
        variable_relations,
        class_relations or {},
        variable_sources or {},
        helper_templates or {},
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if not instance_state:
        return ""
    class_template = object_templates.get(instance_state.get("class_name", ""), {})
    method_template = class_template.get("methods", {}).get(node.func.attr, {})
    return_relation = method_template.get("return_relation", "")
    relation_bindings, _, _, _ = bind_object_method_call_state(
        method_template,
        node,
        instance_state,
        variable_relations=variable_relations,
        class_relations=class_relations or {},
        variable_sources=variable_sources or {},
        helper_templates=helper_templates or {},
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    if return_relation.startswith("param:"):
        return relation_bindings.get(return_relation.split(":", 1)[1], "")
    if return_relation:
        return return_relation
    _, resolved_return_relation = evaluate_bound_object_method(
        method_template,
        node,
        instance_state,
        variable_relations=variable_relations,
        class_relations=class_relations or {},
        variable_sources=variable_sources or {},
        helper_templates=helper_templates or {},
        helper_relation_templates=helper_relation_templates,
        object_templates=object_templates,
        object_instances=object_instances,
    )
    return resolved_return_relation


def extract_relation_from_annotation(node: ast.AST | None, class_relations: dict[str, str]) -> str:
    if node is None:
        return ""
    if isinstance(node, ast.Subscript):
        return extract_relation_from_annotation(get_subscript_slice(node), class_relations)
    if isinstance(node, ast.Name):
        return class_relations.get(node.id, "")
    if isinstance(node, ast.Attribute):
        return class_relations.get(node.attr, "")
    if isinstance(node, ast.Tuple):
        for element in node.elts:
            relation = extract_relation_from_annotation(element, class_relations)
            if relation:
                return relation
    return ""


def extract_relation_from_expression(
    node: ast.AST | None,
    variable_relations: dict[str, str],
    class_relations: dict[str, str],
    *,
    helper_relation_templates: dict[str, dict[str, Any]] | None = None,
    object_templates: dict[str, dict[str, Any]] | None = None,
    object_instances: dict[str, dict[str, Any]] | None = None,
) -> str:
    node = unwrap_await(node)
    if node is None:
        return ""
    if isinstance(node, ast.Name):
        return variable_relations.get(node.id, class_relations.get(node.id, ""))
    if isinstance(node, ast.Attribute):
        full_name = get_ast_name(node)
        if full_name in variable_relations:
            return variable_relations[full_name]
        if full_name in class_relations:
            return class_relations[full_name]
        if isinstance(node.value, ast.Name) and node.value.id in variable_relations:
            return variable_relations[node.value.id]
        return class_relations.get(node.attr, "")
    if isinstance(node, ast.Call):
        function_name = get_ast_name(node.func)
        if function_name and helper_relation_templates and function_name in helper_relation_templates:
            template = helper_relation_templates.get(function_name, {})
            relation_bindings: dict[str, str] = {}
            for index, parameter_name in enumerate(template.get("params", [])):
                argument = None
                if index < len(node.args):
                    argument = node.args[index]
                else:
                    keyword = next((item for item in node.keywords if item.arg == parameter_name), None)
                    argument = keyword.value if keyword else None
                if argument is None:
                    continue
                relation = extract_relation_from_expression(
                    argument,
                    variable_relations,
                    class_relations,
                    helper_relation_templates=helper_relation_templates,
                    object_templates=object_templates,
                    object_instances=object_instances,
                )
                if relation:
                    relation_bindings[parameter_name] = relation
            return_relation = template.get("return_relation", "")
            if return_relation.startswith("param:"):
                return relation_bindings.get(return_relation.split(":", 1)[1], "")
            if return_relation:
                return return_relation
        object_method_relation = instantiate_object_method_relation(
            node,
            variable_relations=variable_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if object_method_relation:
            return object_method_relation
        function_name = get_ast_name(node.func).split(".")[-1]
        if function_name in {"query", "select", "select_from"} and node.args:
            relation = extract_relation_from_expression(
                node.args[0],
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        if function_name == "get" and node.args:
            relation = extract_relation_from_expression(
                node.args[0],
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        if function_name in {
            "all",
            "fetchall",
            "first",
            "one",
            "one_or_none",
            "scalar",
            "scalar_one",
            "scalar_one_or_none",
            "scalars",
            "limit",
            "offset",
            "where",
            "filter",
            "filter_by",
            "order_by",
        } and isinstance(node.func, ast.Attribute):
            relation = extract_relation_from_expression(
                node.func.value,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        relation = extract_relation_from_expression(
            node.func,
            variable_relations,
            class_relations,
            helper_relation_templates=helper_relation_templates,
            object_templates=object_templates,
            object_instances=object_instances,
        )
        if relation:
            return relation
        for argument in node.args:
            relation = extract_relation_from_expression(
                argument,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
        for keyword in node.keywords:
            relation = extract_relation_from_expression(
                keyword.value,
                variable_relations,
                class_relations,
                helper_relation_templates=helper_relation_templates,
                object_templates=object_templates,
                object_instances=object_instances,
            )
            if relation:
                return relation
    return ""


def deep_copy_field_mappings(field_mappings: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, str]]]:
    return {
        field_name: [dict(source) for source in source_fields]
        for field_name, source_fields in field_mappings.items()
    }


def merge_field_mappings(
    target: dict[str, list[dict[str, str]]],
    incoming: dict[str, list[dict[str, str]]],
) -> None:
    for field_name, source_fields in incoming.items():
        existing = target.setdefault(field_name, [])
        seen = {(source["relation"], source["column"]) for source in existing if source.get("relation") and source.get("column")}
        for source in source_fields:
            key = (source.get("relation", ""), source.get("column", ""))
            if not key[0] or not key[1] or key in seen:
                continue
            seen.add(key)
            existing.append(dict(source))


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


def extract_ui_route_field_hints(text: str) -> dict[str, list[str]]:
    route_by_response_var: dict[str, str] = {}
    route_by_data_var: dict[str, str] = {}

    for variable_name, route in FETCH_RESPONSE_ASSIGN_RE.findall(text):
        route_by_response_var[variable_name] = route
    for variable_name, route in AXIOS_RESPONSE_ASSIGN_RE.findall(text):
        route_by_response_var[variable_name] = route
    for variable_name, route in FETCH_JSON_CHAIN_ASSIGN_RE.findall(text):
        route_by_data_var[variable_name] = route
    for variable_name, route in AXIOS_DATA_ASSIGN_RE.findall(text):
        route_by_data_var[variable_name] = route
    for variable_name, response_var in JSON_ASSIGN_RE.findall(text):
        route = route_by_response_var.get(response_var)
        if route:
            route_by_data_var[variable_name] = route

    route_fields: dict[str, set[str]] = defaultdict(set)
    for variable_name, route in route_by_data_var.items():
        route_fields[route].update(extract_fields_for_ui_variable(text, variable_name))

    return {
        route: sorted(field for field in fields if field not in GENERIC_UI_FIELDS)
        for route, fields in route_fields.items()
        if fields
    }


def extract_fields_for_ui_variable(text: str, variable_name: str) -> set[str]:
    fields = set(
        re.findall(rf"\b{re.escape(variable_name)}\??\.([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()", text)
    )
    fields.update(
        re.findall(rf"\b{re.escape(variable_name)}\s*\[\s*[\"']([A-Za-z_][A-Za-z0-9_]*)[\"']\s*\]", text)
    )

    for destructured_fields, source_name in DESTRUCTURE_ASSIGN_RE.findall(text):
        if source_name != variable_name:
            continue
        for entry in destructured_fields.split(","):
            candidate = entry.strip()
            if not candidate:
                continue
            if ":" in candidate:
                candidate = candidate.split(":", 1)[0].strip()
            if candidate.startswith("..."):
                continue
            fields.add(candidate)

    return fields


def get_ast_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = get_ast_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def detect_component_names(relative_path: str, text: str) -> list[str]:
    names = [
        *EXPORTED_COMPONENT_RE.findall(text),
        *FUNCTION_COMPONENT_RE.findall(text),
        *CONST_COMPONENT_RE.findall(text),
    ]
    if names:
        return sorted(dict.fromkeys(names))
    stem = Path(relative_path).stem
    guessed = humanize_asset_name(stem).replace(" ", "")
    return [guessed or "UiContract"]


def dedupe_hint_list(items: list[dict], keys: tuple[str, ...]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[tuple[str, ...]] = set()
    for item in items:
        key = tuple(str(item.get(part, "")) for part in keys)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def build_hint_id(prefix: str, relative_path: str, stable_value: str) -> str:
    path_slug = humanize_asset_name(relative_path).replace(" ", "-").lower()
    value_slug = humanize_asset_name(stable_value).replace(" ", "-").lower()
    return f"{prefix}:{path_slug}:{value_slug}"


def is_test_path(relative_path: str) -> bool:
    normalized = relative_path.replace("\\", "/")
    parts = normalized.split("/")
    filename = parts[-1]
    return "tests" in parts or filename.startswith("test_") or filename.endswith("_test.py")


def is_internal_workbench_path(relative_path: str) -> bool:
    normalized = relative_path.replace("\\", "/")
    return normalized.startswith("src/workbench/") or normalized.startswith("src/data_workbench.egg-info/") or normalized.startswith("static/")


def summarize_asset_entry(root_dir: Path, entry: dict[str, str | None]) -> dict:
    path = entry["path"] or ""
    asset = build_asset_descriptor(path, root_dir=root_dir, kind=entry.get("kind"), fmt=entry.get("format"))
    summary = {
        "path": path,
        "kind": asset["kind"] if asset else "file",
        "format": asset["format"] if asset else "unknown",
        "profile_status": "schema_only",
        "row_count": None,
        "columns": [],
    }
    if asset and asset.get("accessible"):
        try:
            profile = profile_asset(asset, root_dir)
        except Exception as error:  # noqa: BLE001
            summary["error"] = str(error)
        else:
            summary["profile_status"] = profile["profile_status"]
            summary["row_count"] = profile["row_count"]
            summary["columns"] = [
                {"name": column["name"], "data_type": column["data_type"]}
                for column in profile["columns"]
            ]
    summary["suggested_import"] = build_import_suggestion(summary)
    return summary


def build_import_suggestion(asset_summary: dict) -> dict:
    path = asset_summary["path"]
    label_base = humanize_asset_name(path)
    source_extension_type = "bucket_path" if asset_summary["kind"] == "object_storage" else "disk_path"
    source_origin_kind = "bucket_path" if source_extension_type == "bucket_path" else "disk_path"
    source_provider = "object_storage" if asset_summary["kind"] == "object_storage" else "local"
    return {
        "source_label": f"{label_base} Source",
        "source_extension_type": source_extension_type,
        "source_description": f"Imported from project survey: {path}",
        "source_provider": source_provider,
        "source_refresh": "",
        "source_origin_kind": source_origin_kind,
        "source_origin_value": path,
        "source_series_id": "",
        "raw_asset_label": label_base,
        "raw_asset_kind": asset_summary["kind"],
        "raw_asset_format": asset_summary["format"],
        "raw_asset_value": path,
        "profile_ready": True,
        "data_label": f"{label_base} Raw",
        "data_extension_type": "raw_dataset",
        "data_description": f"Imported from {path}",
        "update_frequency": "",
        "persistence": "cold",
        "persisted": False,
        "schema_columns": [
            {"name": column["name"], "data_type": column["data_type"]}
            for column in asset_summary.get("columns", [])
        ],
    }


def humanize_asset_name(path: str) -> str:
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


def parse_sql_table_columns(body: str) -> list[dict]:
    fields: list[dict] = []
    for entry in split_sql_top_level(body):
        stripped = entry.strip().strip(",")
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered.startswith(("primary key", "foreign key", "constraint", "unique", "index", "key", "check")):
            continue
        parts = stripped.split()
        if len(parts) < 2:
            continue
        column_name = normalize_sql_identifier(parts[0])
        if not column_name:
            continue
        data_type = parts[1].rstrip(",")
        fields.append({"name": column_name, "data_type": data_type.lower(), "source_fields": []})
    return fields


def parse_sql_select_projection(select_sql: str) -> tuple[list[dict], list[str]]:
    lowered = select_sql.lower()
    if "select" not in lowered or "from" not in lowered:
        return [], []
    select_start = lowered.find("select") + len("select")
    from_index = find_top_level_keyword(select_sql, "from", start=select_start)
    if from_index == -1:
        return [], []
    select_list = select_sql[select_start:from_index]
    relation_aliases = extract_sql_relation_aliases(select_sql[from_index:])
    upstream_relations = sorted(dict.fromkeys(relation_aliases.values()))
    projections: list[dict] = []
    for expression in split_sql_top_level(select_list):
        parsed = parse_sql_projection_expression(expression, relation_aliases, upstream_relations)
        if parsed:
            projections.append(parsed)
    return projections, upstream_relations


def extract_sql_relation_aliases(sql_tail: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for _kind, relation, alias in SQL_RELATION_RE.findall(sql_tail):
        normalized_relation = normalize_sql_identifier(relation)
        if not normalized_relation:
            continue
        aliases[normalized_relation.split(".")[-1]] = normalized_relation
        if alias:
            aliases[normalize_sql_identifier(alias)] = normalized_relation
    return aliases


def parse_sql_projection_expression(expression: str, relation_aliases: dict[str, str], upstream_relations: list[str]) -> dict | None:
    stripped = expression.strip()
    if not stripped:
        return None
    output_name = infer_sql_projection_name(stripped)
    if not output_name or output_name == "*":
        return None

    source_fields = []
    for relation_alias, column_name in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)", stripped):
        relation = relation_aliases.get(normalize_sql_identifier(relation_alias), normalize_sql_identifier(relation_alias))
        source_fields.append({"relation": relation, "column": normalize_sql_identifier(column_name)})

    if not source_fields and len(upstream_relations) == 1:
        relation = upstream_relations[0]
        for token in SQL_BARE_IDENTIFIER_RE.findall(stripped):
            normalized = normalize_sql_identifier(token)
            if (
                normalized
                and normalized not in {output_name, "select", "from", "as", "and", "or", "when", "then", "else", "end", "null", "case", "sum", "avg", "count", "min", "max", "coalesce", "cast", "distinct"}
                and not normalized.isdigit()
            ):
                source_fields.append({"relation": relation, "column": normalized})

    deduped_sources = []
    seen: set[tuple[str, str]] = set()
    for source in source_fields:
        key = (source["relation"], source["column"])
        if key in seen:
            continue
        seen.add(key)
        deduped_sources.append(source)

    return {
        "name": output_name,
        "data_type": "unknown",
        "source_fields": deduped_sources,
    }


def infer_sql_projection_name(expression: str) -> str:
    match = re.search(r"\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", expression, re.IGNORECASE)
    if match:
        return normalize_sql_identifier(match.group(1))
    stripped = expression.strip()
    if "." in stripped and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", stripped):
        return normalize_sql_identifier(stripped.split(".")[-1])
    trailing = re.search(r"\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", stripped)
    if trailing and "(" in stripped:
        return normalize_sql_identifier(trailing.group(1))
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", stripped):
        return normalize_sql_identifier(stripped)
    return ""


def split_sql_top_level(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    previous = ""
    for character in text:
        if character == "'" and not in_double and previous != "\\":
            in_single = not in_single
        elif character == '"' and not in_single and previous != "\\":
            in_double = not in_double
        elif not in_single and not in_double:
            if character == "(":
                depth += 1
            elif character == ")":
                depth = max(0, depth - 1)
            elif character == "," and depth == 0:
                part = "".join(current).strip()
                if part:
                    parts.append(part)
                current = []
                previous = character
                continue
        current.append(character)
        previous = character
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def find_top_level_keyword(text: str, keyword: str, *, start: int = 0) -> int:
    lowered = text.lower()
    keyword = keyword.lower()
    depth = 0
    in_single = False
    in_double = False
    previous = ""
    for index in range(start, len(text)):
        character = text[index]
        if character == "'" and not in_double and previous != "\\":
            in_single = not in_single
        elif character == '"' and not in_single and previous != "\\":
            in_double = not in_double
        elif not in_single and not in_double:
            if character == "(":
                depth += 1
            elif character == ")":
                depth = max(0, depth - 1)
            elif depth == 0 and lowered.startswith(keyword, index):
                before = lowered[index - 1] if index > 0 else " "
                after_index = index + len(keyword)
                after = lowered[after_index] if after_index < len(lowered) else " "
                if not before.isalnum() and not after.isalnum():
                    return index
        previous = character
    return -1


def normalize_sql_identifier(value: str) -> str:
    return value.strip().strip('"').strip("`").strip("[]")
