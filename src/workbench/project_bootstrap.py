from __future__ import annotations

from typing import Any

from .hint_importer import import_api_hint_into_graph, import_orm_hint_into_graph, import_sql_hint_into_graph, import_ui_hint_into_graph
from .importer import import_assets_into_graph
from .types import validate_graph


def bootstrap_project_into_graph(
    graph: dict[str, Any],
    root_dir,
    *,
    project_profile: dict[str, Any] | None = None,
    include_tests: bool = False,
    include_internal: bool = True,
    asset_paths: list[str] | None = None,
    api_hint_ids: list[str] | None = None,
    ui_hint_ids: list[str] | None = None,
    sql_hint_ids: list[str] | None = None,
    orm_hint_ids: list[str] | None = None,
    import_assets: bool = True,
    import_api_hints: bool = True,
    import_ui_hints: bool = True,
    import_sql_hints: bool = True,
    import_orm_hints: bool = True,
) -> dict[str, Any]:
    if project_profile is None:
        from .project_profiler import resolve_project_profile

        project_profile = resolve_project_profile(
            root_dir,
            include_tests=include_tests,
            include_internal=include_internal,
        )
    updated = validate_graph(graph)

    summary: dict[str, Any] = {
        "asset_imported": [],
        "asset_skipped": [],
        "api_created": [],
        "api_updated": [],
        "api_created_fields": [],
        "ui_created": [],
        "ui_updated": [],
        "ui_created_fields": [],
        "sql_created": [],
        "sql_updated": [],
        "orm_created": [],
        "orm_updated": [],
        "created_edges": [],
        "binding_summary": {"applied": [], "unresolved": []},
        "selection": {
            "asset_paths": [],
            "api_hint_ids": [],
            "ui_hint_ids": [],
            "sql_hint_ids": [],
            "orm_hint_ids": [],
        },
    }

    if import_assets:
        selected_asset_summaries = _select_asset_summaries(project_profile, asset_paths)
        summary["selection"]["asset_paths"] = [asset["path"] for asset in selected_asset_summaries]
        asset_specs = [asset["suggested_import"] for asset in selected_asset_summaries if asset.get("suggested_import")]
        if asset_specs:
            imported_assets = import_assets_into_graph(updated, asset_specs, root_dir, profile_assets=False)
            updated = imported_assets["graph"]
            summary["asset_imported"] = imported_assets["imported"]
            summary["asset_skipped"] = imported_assets["skipped"]

    selected_api_hints = _select_hints(project_profile.get("api_contract_hints", []), api_hint_ids, import_api_hints)
    summary["selection"]["api_hint_ids"] = [hint["id"] for hint in selected_api_hints]
    for hint in selected_api_hints:
        imported_api = import_api_hint_into_graph(updated, hint)
        updated = imported_api["graph"]
        if imported_api["imported"]["created"]:
            summary["api_created"].append(imported_api["imported"]["node_id"])
        else:
            summary["api_updated"].append(imported_api["imported"]["node_id"])
        summary["api_created_fields"].extend(imported_api["imported"].get("created_field_names", []))
        _extend_binding_summary(summary["binding_summary"], imported_api["imported"].get("binding_summary", {}))

    api_hints_by_route = {
        hint["route"]: hint
        for hint in project_profile.get("api_contract_hints", [])
        if hint.get("route")
    }
    selected_ui_hints = _select_hints(project_profile.get("ui_contract_hints", []), ui_hint_ids, import_ui_hints)
    summary["selection"]["ui_hint_ids"] = [hint["id"] for hint in selected_ui_hints]
    for hint in selected_ui_hints:
        imported_ui = import_ui_hint_into_graph(updated, hint, api_hints_by_route=api_hints_by_route)
        updated = imported_ui["graph"]
        if imported_ui["imported"]["created"]:
            summary["ui_created"].append(imported_ui["imported"]["node_id"])
        else:
            summary["ui_updated"].append(imported_ui["imported"]["node_id"])
        summary["api_created"].extend(imported_ui["imported"].get("created_api_node_ids", []))
        summary["api_updated"].extend(imported_ui["imported"].get("updated_api_node_ids", []))
        summary["ui_created_fields"].extend(imported_ui["imported"].get("created_field_names", []))
        summary["created_edges"].extend(imported_ui["imported"].get("created_edge_ids", []))
        _extend_binding_summary(summary["binding_summary"], imported_ui["imported"].get("binding_summary", {}))

    selected_sql_hints = _select_hints(project_profile.get("sql_structure_hints", []), sql_hint_ids, import_sql_hints)
    summary["selection"]["sql_hint_ids"] = [hint["id"] for hint in selected_sql_hints]
    for hint in selected_sql_hints:
        imported_sql = import_sql_hint_into_graph(updated, hint)
        updated = imported_sql["graph"]
        if imported_sql["imported"]["created"]:
            summary["sql_created"].extend(imported_sql["imported"].get("created_node_ids", []) or [imported_sql["imported"]["node_id"]])
        else:
            summary["sql_updated"].extend(imported_sql["imported"].get("updated_node_ids", []) or [imported_sql["imported"]["node_id"]])
        summary["created_edges"].extend(imported_sql["imported"].get("created_edge_ids", []))

    selected_orm_hints = _select_hints(project_profile.get("orm_structure_hints", []), orm_hint_ids, import_orm_hints)
    summary["selection"]["orm_hint_ids"] = [hint["id"] for hint in selected_orm_hints]
    for hint in selected_orm_hints:
        imported_orm = import_orm_hint_into_graph(updated, hint)
        updated = imported_orm["graph"]
        if imported_orm["imported"]["created"]:
            summary["orm_created"].extend(imported_orm["imported"].get("created_node_ids", []) or [imported_orm["imported"]["node_id"]])
        else:
            summary["orm_updated"].extend(imported_orm["imported"].get("updated_node_ids", []) or [imported_orm["imported"]["node_id"]])
        summary["created_edges"].extend(imported_orm["imported"].get("created_edge_ids", []))

    summary["asset_imported"] = sorted(summary["asset_imported"], key=lambda item: item.get("data_node_id", ""))
    summary["asset_skipped"] = sorted(summary["asset_skipped"], key=lambda item: item.get("source_node_id", ""))
    summary["api_created"] = sorted(set(summary["api_created"]))
    summary["api_updated"] = sorted(set(summary["api_updated"]) - set(summary["api_created"]))
    summary["api_created_fields"] = sorted(set(summary["api_created_fields"]))
    summary["ui_created"] = sorted(set(summary["ui_created"]))
    summary["ui_updated"] = sorted(set(summary["ui_updated"]) - set(summary["ui_created"]))
    summary["ui_created_fields"] = sorted(set(summary["ui_created_fields"]))
    summary["sql_created"] = sorted(set(summary["sql_created"]))
    summary["sql_updated"] = sorted(set(summary["sql_updated"]) - set(summary["sql_created"]))
    summary["orm_created"] = sorted(set(summary["orm_created"]))
    summary["orm_updated"] = sorted(set(summary["orm_updated"]) - set(summary["orm_created"]))
    summary["created_edges"] = sorted(set(summary["created_edges"]))
    summary["binding_summary"]["applied"] = _dedupe_binding_entries(summary["binding_summary"]["applied"])
    summary["binding_summary"]["unresolved"] = _dedupe_binding_entries(summary["binding_summary"]["unresolved"])

    return {
        "graph": updated,
        "project_profile": project_profile,
        "imported": summary,
    }


def _select_asset_summaries(project_profile: dict[str, Any], asset_paths: list[str] | None) -> list[dict[str, Any]]:
    assets = [asset for asset in project_profile.get("data_assets", []) if asset.get("suggested_import")]
    if asset_paths:
        selected = {path for path in asset_paths}
        return [asset for asset in assets if asset["path"] in selected]
    return assets


def _select_hints(hints: list[dict[str, Any]], hint_ids: list[str] | None, enabled: bool) -> list[dict[str, Any]]:
    if not enabled:
        return []
    if hint_ids:
        selected = {hint_id for hint_id in hint_ids}
        return [hint for hint in hints if hint["id"] in selected]
    return list(hints)


def _extend_binding_summary(target: dict[str, list[dict[str, Any]]], source: dict[str, list[dict[str, Any]]]) -> None:
    target["applied"].extend(source.get("applied", []))
    target["unresolved"].extend(source.get("unresolved", []))


def _dedupe_binding_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        key = (
            str(entry.get("contract_node_id", "")),
            str(entry.get("field_name", "")),
            str(entry.get("source_ref", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped
