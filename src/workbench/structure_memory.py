from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from .diagnostics import build_graph_diagnostics, describe_downstream_component_impacts, infer_ui_roles
from .openapi_importer import HTTP_METHODS, extract_response_fields
from .project_profiler import profile_project
from .store import list_bundles, load_bundle, load_graph, save_bundle, save_graph, utc_timestamp
from .types import (
    OBSERVED_PRECEDENCE,
    PATCH_TYPE_PRECEDENCE,
    ReadinessReport,
    ScanBundle,
    active_nodes,
    build_index,
    column_ref,
    deep_sort,
    display_ref_for_field_id,
    find_column,
    make_field_id,
    normalize_graph,
    resolve_field_reference,
)
from .validation import build_validation_report


DOC_ROUTE_RE = re.compile(r"\b(GET|POST|PUT|PATCH|DELETE)\s+(/[\w\-/{}/]+)")
DATA_PATH_RE = re.compile(r"([A-Za-z0-9_./-]+\.(?:csv|parquet|json|yaml|yml))")
SCANNER_VERSIONS = {
    "assets": "1",
    "python_fastapi": "1",
    "ui_contracts": "1",
    "docs": "2",
}
CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}


def build_structure_summary(
    graph: dict[str, Any],
    *,
    diagnostics: dict[str, Any] | None = None,
    validation_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validation_report = validation_report or build_validation_report(graph)
    diagnostics = diagnostics or build_graph_diagnostics(graph, validation_report=validation_report)
    return {
        "canonical_spec_path": "specs/structure/spec.yaml",
        "legacy_spec_path": "specs/workbench.graph.json",
        "structure_version": graph.get("metadata", {}).get("structure_version", 1),
        "updated_at": graph.get("metadata", {}).get("updated_at", ""),
        "updated_by": graph.get("metadata", {}).get("updated_by", ""),
        "readiness": build_readiness_report(graph, diagnostics=diagnostics, validation_report=validation_report),
        "bundles": list_bundles(),
    }


def build_readiness_report(
    graph: dict[str, Any],
    *,
    diagnostics: dict[str, Any] | None = None,
    validation_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validation_report = validation_report or build_validation_report(graph)
    diagnostics = diagnostics or build_graph_diagnostics(graph, validation_report=validation_report)
    issues: list[dict[str, Any]] = []
    required_binding_failures = 0
    missing_persistence = 0
    missing_compute_ios = 0

    for node in active_nodes(graph):
        node_diagnostics = diagnostics.get("nodes", {}).get(node["id"], {})
        if node["kind"] == "data" and not node.get("data", {}).get("persistence"):
            missing_persistence += 1
            issues.append(
                {
                    "level": "tier_1",
                    "target_id": node["id"],
                    "message": f"{node['label']}: persistence role is missing.",
                    "why_this_matters": "Data readiness is incomplete.",
                }
            )
        if node["kind"] == "compute":
            if not node.get("compute", {}).get("inputs") or not node.get("compute", {}).get("outputs"):
                missing_compute_ios += 1
                issues.append(
                    {
                        "level": "tier_1",
                        "target_id": node["id"],
                        "message": f"{node['label']}: compute inputs or outputs are incomplete.",
                        "why_this_matters": "Compute structure is not ready to build.",
                    }
                )
            for column in node.get("columns", []):
                if column.get("removed") or not column.get("required", True):
                    continue
                if not column.get("lineage_inputs"):
                    required_binding_failures += 1
                    issues.append(
                        {
                            "level": "tier_1",
                            "target_id": column["id"],
                            "message": f"{column['name']}: required compute output is missing lineage inputs.",
                            "why_this_matters": "Downstream contracts cannot trust this derived output.",
                        }
                    )
        if node["kind"] == "contract":
            for field in node.get("contract", {}).get("fields", []):
                if field.get("removed") or not field.get("required", True):
                    continue
                binding_diagnostics = node_diagnostics.get("bindings", {}).get(field["name"], {})
                if binding_diagnostics.get("health") == "broken" or not field.get("primary_binding"):
                    required_binding_failures += 1
                    issues.append(
                        {
                            "level": "tier_1",
                            "target_id": field["id"],
                            "message": f"{field['name']}: required contract field is missing a valid upstream binding.",
                            "why_this_matters": binding_diagnostics.get("why_this_matters", "Required contract delivery is incomplete."),
                        }
                    )

    warning_count = len(validation_report.get("warnings", []))
    tier_1_count = len([issue for issue in issues if issue["level"] == "tier_1"]) + len(validation_report.get("errors", []))
    if tier_1_count:
        status = "Not Ready"
    elif warning_count:
        status = "Partially Ready"
    else:
        status = "Ready to Build"

    return ReadinessReport.model_validate(
        {
            "status": status,
            "summary": {
                "tier_1_issues": tier_1_count,
                "warnings": warning_count,
                "required_binding_failures": required_binding_failures,
                "missing_persistence": missing_persistence,
                "missing_compute_ios": missing_compute_ios,
            },
            "issues": issues,
        }
    ).model_dump(mode="json")


def scan_structure(
    *,
    root_dir: Path,
    role: str,
    scope: str = "full",
    include_tests: bool = False,
    include_internal: bool = True,
    doc_paths: list[str] | None = None,
    selected_paths: list[str] | None = None,
) -> dict[str, Any]:
    canonical_graph = load_graph()
    profile = profile_project(root_dir, include_tests=include_tests, include_internal=include_internal)
    doc_candidates = collect_document_candidates(root_dir, doc_paths or [])
    fingerprint = build_scan_fingerprint(
        canonical_graph,
        profile=profile,
        role=role,
        scope=scope,
        doc_candidates=doc_candidates,
        root_dir=root_dir,
        selected_paths=selected_paths or [],
    )
    bundle_id = f"bundle.{fingerprint[:12]}"
    existing_bundle = load_bundle(bundle_id)
    if existing_bundle is not None:
        return {
            "bundle": existing_bundle,
            "structure": build_structure_summary(canonical_graph),
            "project_profile": profile,
        }

    observed_graph, patches, contradictions, impacts = build_observed_bundle_content(
        canonical_graph,
        profile=profile,
        role=role,
        root_dir=root_dir,
        scope=scope,
        doc_candidates=doc_candidates,
        selected_paths=selected_paths or [],
    )
    reconciliation = build_reconciliation_summary(
        canonical_graph,
        profile=profile,
        doc_candidates=doc_candidates,
        patches=patches,
        contradictions=contradictions,
        impacts=impacts,
        scope=scope,
    )
    validation = build_validation_report(observed_graph)
    diagnostics = build_graph_diagnostics(observed_graph, validation_report=validation)
    readiness = build_readiness_report(observed_graph, diagnostics=diagnostics, validation_report=validation)
    bundle = {
        "bundle_id": bundle_id,
        "base_structure_version": canonical_graph.get("metadata", {}).get("structure_version", 1),
        "scan": {
            "bundle_id": bundle_id,
            "role": role,
            "scope": scope,
            "root_path": str(root_dir),
            "doc_paths": [str(path) for path in doc_paths or []],
            "fingerprint": fingerprint,
            "scanner_versions": SCANNER_VERSIONS,
            "base_structure_version": canonical_graph.get("metadata", {}).get("structure_version", 1),
            "created_at": utc_timestamp(),
        },
        "observed": observed_graph,
        "patches": patches,
        "contradictions": contradictions,
        "impacts": impacts,
        "reconciliation": reconciliation,
        "review": {
            "accepted_patch_ids": [],
            "rejected_patch_ids": [],
            "deferred_patch_ids": [],
            "merged_at": "",
            "merged_by": "",
        },
        "readiness": readiness,
    }
    normalized_bundle = save_bundle(bundle)
    return {
        "bundle": normalized_bundle,
        "structure": build_structure_summary(canonical_graph),
        "project_profile": profile,
    }


def review_bundle_patch(bundle_id: str, patch_id: str, decision: str) -> dict[str, Any]:
    return review_bundle_patches(bundle_id, [patch_id], decision)


def review_bundle_patches(bundle_id: str, patch_ids: list[str], decision: str) -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    if decision not in {"accepted", "rejected", "deferred"}:
        raise ValueError(f"Unsupported review decision: {decision}")
    normalized_ids = [patch_id for patch_id in dict.fromkeys(patch_ids) if patch_id]
    if not normalized_ids:
        raise ValueError("Provide at least one patch id for review.")
    requested_ids = set(normalized_ids)
    found_ids: set[str] = set()
    for patch in bundle.get("patches", []):
        if patch["id"] not in requested_ids:
            continue
        patch["review_state"] = decision
        found_ids.add(patch["id"])
    missing = requested_ids - found_ids
    if missing:
        raise ValueError(f"Patch not found: {sorted(missing)[0]}")
    review = bundle.setdefault("review", {})
    for key in ("accepted_patch_ids", "rejected_patch_ids", "deferred_patch_ids"):
        review.setdefault(key, [])
        review[key] = [value for value in review[key] if value not in requested_ids]
    review[f"{decision}_patch_ids"] = [*review[f"{decision}_patch_ids"], *normalized_ids]
    normalized = save_bundle(bundle)
    return {"bundle": normalized, "updated_patch_ids": normalized_ids}


def merge_bundle(bundle_id: str, *, merged_by: str = "user") -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    canonical_graph = load_graph()
    base_version = bundle.get("base_structure_version", 1)
    if canonical_graph.get("metadata", {}).get("structure_version", 1) != base_version:
        raise ValueError("Bundle is stale and must be regenerated before merge.")

    merged_graph = deepcopy(canonical_graph)
    accepted_patches = [patch for patch in bundle.get("patches", []) if patch.get("review_state") == "accepted"]
    for patch in sorted(
        accepted_patches,
        key=lambda item: (
            PATCH_TYPE_PRECEDENCE.get(item.get("type", ""), 999),
            item.get("target_id", ""),
            item.get("id", ""),
        ),
    ):
        apply_patch_to_graph(merged_graph, patch, merged_by=merged_by)

    merged_graph = normalize_graph(merged_graph)
    merged_graph = save_graph(merged_graph, updated_by=merged_by, increment_version=True)
    bundle.setdefault("review", {})["merged_at"] = utc_timestamp()
    bundle.setdefault("review", {})["merged_by"] = merged_by
    normalized_bundle = save_bundle(bundle)
    validation = build_validation_report(merged_graph)
    diagnostics = build_graph_diagnostics(merged_graph, validation_report=validation)
    return {
        "graph": merged_graph,
        "diagnostics": diagnostics,
        "validation": validation,
        "bundle": normalized_bundle,
        "structure": build_structure_summary(merged_graph, diagnostics=diagnostics, validation_report=validation),
    }


def import_yaml_spec(spec_payload: dict[str, Any], *, updated_by: str = "user") -> dict[str, Any]:
    graph = normalize_graph(spec_payload)
    graph = save_graph(graph, updated_by=updated_by, increment_version=True)
    validation = build_validation_report(graph)
    diagnostics = build_graph_diagnostics(graph, validation_report=validation)
    return {
        "graph": graph,
        "diagnostics": diagnostics,
        "validation": validation,
        "structure": build_structure_summary(graph, diagnostics=diagnostics, validation_report=validation),
    }


def build_observed_bundle_content(
    canonical_graph: dict[str, Any],
    *,
    profile: dict[str, Any],
    role: str,
    root_dir: Path,
    scope: str,
    doc_candidates: list[dict[str, Any]],
    selected_paths: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    observed_graph = deepcopy(canonical_graph)
    patches: list[dict[str, Any]] = []
    contradictions: list[dict[str, Any]] = []
    impacts: list[dict[str, Any]] = []
    canonical_index = build_index(canonical_graph)

    api_hints = profile.get("api_contract_hints", [])
    ui_hints = profile.get("ui_contract_hints", [])
    sql_hints = profile.get("sql_structure_hints", [])
    orm_hints = profile.get("orm_structure_hints", [])
    data_assets = profile.get("data_assets", [])
    allowed_paths = set(selected_paths or [])

    for asset in data_assets:
        if allowed_paths and asset.get("path") not in allowed_paths:
            continue
        import_spec = asset.get("suggested_import")
        if not import_spec:
            continue
        apply_asset_observation(
            observed_graph,
            canonical_index=canonical_index,
            import_spec=import_spec,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )

    for hint in sql_hints:
        apply_sql_structure_observation(
            observed_graph,
            canonical_index=canonical_index,
            hint=hint,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )

    for hint in orm_hints:
        apply_sql_structure_observation(
            observed_graph,
            canonical_index=canonical_index,
            hint=hint,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )

    api_by_route = {hint.get("route"): hint for hint in api_hints if hint.get("route")}
    for hint in api_hints:
        apply_api_hint_observation(
            observed_graph,
            canonical_index=canonical_index,
            hint=hint,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )

    for hint in ui_hints:
        apply_ui_hint_observation(
            observed_graph,
            canonical_index=canonical_index,
            hint=hint,
            api_hints_by_route=api_by_route,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )

    for candidate in doc_candidates:
        apply_document_candidate(
            observed_graph,
            canonical_index=canonical_index,
            candidate=candidate,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )

    detect_missing_confirmed_structure(
        canonical_graph,
        observed_graph,
        contradictions=contradictions,
        impacts=impacts,
        scope=scope,
    )

    deduped_patches = dedupe_patch_rows(patches)
    deduped_contradictions = dedupe_contradictions(contradictions)
    deduped_impacts = dedupe_impacts(impacts)
    observed_graph = normalize_graph(observed_graph)
    return observed_graph, deduped_patches, deduped_contradictions, deduped_impacts


def build_reconciliation_summary(
    canonical_graph: dict[str, Any],
    *,
    profile: dict[str, Any],
    doc_candidates: list[dict[str, Any]],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    scope: str,
) -> dict[str, Any]:
    canonical_index = build_index(canonical_graph)
    planned_missing: list[dict[str, Any]] = []
    observed_untracked: list[dict[str, Any]] = []
    implemented_differently: list[dict[str, Any]] = []
    uncertain_matches: list[dict[str, Any]] = []

    observed_routes = collect_observed_routes(profile, doc_candidates)
    observed_components = collect_observed_ui_components(profile)
    observed_assets = collect_observed_asset_paths(profile, doc_candidates)

    if scope == "full":
        for node in active_nodes(canonical_graph):
            if node.get("state") not in {"confirmed", "proposed"}:
                continue
            if node["kind"] == "contract" and node.get("extension_type") == "api":
                route = node.get("contract", {}).get("route", "")
                if route and route not in observed_routes:
                    node_impacts = build_missing_node_impacts(canonical_index, node["id"])
                    planned_missing.append(
                        {
                            "target_id": node["id"],
                            "label": node.get("label", route),
                            "message": f"{route} is planned but was not observed in repo or docs.",
                            "why_this_matters": node_impacts[0] if node_impacts else "Planned API structure is still missing from implementation evidence.",
                            "source": "canonical",
                            "significant": bool(node_impacts or node.get("state") == "confirmed"),
                        }
                    )
            elif node["kind"] == "contract" and node.get("extension_type") == "ui":
                component = node.get("contract", {}).get("component", "")
                if component and component not in observed_components:
                    planned_missing.append(
                        {
                            "target_id": node["id"],
                            "label": node.get("label", component),
                            "message": f"{component} is planned but was not observed in repo usage.",
                            "why_this_matters": "The planned UI contract is not yet grounded in implementation evidence.",
                            "source": "canonical",
                            "significant": node.get("state") == "confirmed",
                        }
                    )
            elif node["kind"] == "data":
                data_path = node.get("data", {}).get("profile_target") or node.get("data", {}).get("local_path") or ""
                if data_path and data_path not in observed_assets:
                    planned_missing.append(
                        {
                            "target_id": node["id"],
                            "label": node.get("label", data_path),
                            "message": f"{data_path} is planned but was not observed in scanned assets.",
                            "why_this_matters": "Expected backend data inputs are still missing from the current repo or docs.",
                            "source": "canonical",
                            "significant": bool(node.get("columns")),
                        }
                    )

    canonical_routes = {
        node.get("contract", {}).get("route", "")
        for node in active_nodes(canonical_graph)
        if node["kind"] == "contract" and node.get("extension_type") == "api"
    }
    canonical_components = {
        node.get("contract", {}).get("component", "")
        for node in active_nodes(canonical_graph)
        if node["kind"] == "contract" and node.get("extension_type") == "ui"
    }
    canonical_assets = {
        node.get("data", {}).get("profile_target") or node.get("data", {}).get("local_path") or ""
        for node in active_nodes(canonical_graph)
        if node["kind"] == "data"
    }

    for route, details in observed_routes.items():
        if route in canonical_routes:
            continue
        observed_untracked.append(
            {
                "target_id": details.get("target_id", route),
                "label": details.get("label", route),
                "message": f"{route} was discovered from {details.get('source', 'scan')} but is not in canonical YAML.",
                "why_this_matters": "New API structure needs review before it becomes trusted canonical memory.",
                "source": details.get("source", "scan"),
                "significant": True,
            }
        )

    for component, details in observed_components.items():
        if component in canonical_components:
            continue
        observed_untracked.append(
            {
                "target_id": details.get("target_id", component),
                "label": details.get("label", component),
                "message": f"{component} was discovered from repo usage but is not in canonical YAML.",
                "why_this_matters": "UI consumption has drifted beyond the current canonical structure memory.",
                "source": details.get("source", "repo"),
                "significant": bool(details.get("api_routes")),
            }
        )

    for asset_path, details in observed_assets.items():
        if asset_path in canonical_assets:
            continue
        observed_untracked.append(
            {
                "target_id": details.get("target_id", asset_path),
                "label": details.get("label", asset_path),
                "message": f"{asset_path} was discovered from {details.get('source', 'scan')} but is not in canonical YAML.",
                "why_this_matters": "Data ingestion may have started outside the currently trusted structure memory.",
                "source": details.get("source", "scan"),
                "significant": True,
            }
        )

    canonical_node_ids = {node["id"] for node in active_nodes(canonical_graph)}
    for patch in patches:
        if patch.get("type") != "add_node":
            continue
        node_payload = patch.get("payload", {}).get("node", {})
        node_id = node_payload.get("id", "") or patch.get("node_id", "")
        if not node_id or node_id in canonical_node_ids:
            continue
        if node_payload.get("kind") == "contract":
            continue
        observed_untracked.append(
            {
                "target_id": node_id,
                "label": node_payload.get("label", node_id),
                "message": f"{node_payload.get('label', node_id)} was observed from repo structure but is not in canonical YAML.",
                "why_this_matters": "Backend structure has grown beyond the currently trusted structure memory.",
                "source": (node_payload.get("tags") or ["scan"])[0],
                "significant": node_payload.get("kind") in {"data", "compute"},
            }
        )

    seen_differences: set[str] = set()
    for contradiction in contradictions:
        key = contradiction.get("field_id") or contradiction.get("target_id") or contradiction.get("id", "")
        if key in seen_differences:
            continue
        seen_differences.add(key)
        implemented_differently.append(
            {
                "target_id": contradiction.get("target_id", ""),
                "field_id": contradiction.get("field_id", ""),
                "label": contradiction.get("message", "") or contradiction.get("target_id", "Contradiction"),
                "message": contradiction.get("message", "") or "Observed structure conflicts with the canonical structure memory.",
                "why_this_matters": contradiction.get("why_this_matters", "") or next(iter(contradiction.get("downstream_impacts", []) or []), ""),
                "source": "contradiction",
                "significant": True,
            }
        )

    contradiction_targets = {
        contradiction.get("field_id") or contradiction.get("target_id", "")
        for contradiction in contradictions
    }
    for patch in patches:
        if patch.get("type") == "change_binding" and patch.get("target_id", "") not in contradiction_targets:
            implemented_differently.append(
                {
                    "target_id": patch.get("target_id", ""),
                    "field_id": patch.get("field_id", ""),
                    "label": patch.get("target_id", ""),
                    "message": summarize_patch_for_reconciliation(patch),
                    "why_this_matters": first_impact_for_target(impacts, patch.get("target_id", "")),
                    "source": "patch",
                    "significant": True,
                }
            )
        if patch.get("confidence") == "low" or (
            patch.get("review_state") == "pending"
            and patch.get("confidence") == "medium"
            and patch.get("type") in {"add_field", "add_binding", "change_binding"}
        ):
            uncertain_matches.append(
                {
                    "target_id": patch.get("target_id", ""),
                    "field_id": patch.get("field_id", ""),
                    "label": patch.get("target_id", ""),
                    "message": summarize_patch_for_reconciliation(patch),
                    "why_this_matters": first_impact_for_target(impacts, patch.get("target_id", "")),
                    "source": "scan",
                    "significant": patch.get("confidence") == "low" and bool(first_impact_for_target(impacts, patch.get("target_id", ""))),
                }
            )

    planned_missing = dedupe_reconciliation_items(planned_missing)
    observed_untracked = dedupe_reconciliation_items(observed_untracked)
    implemented_differently = dedupe_reconciliation_items(implemented_differently)
    uncertain_matches = dedupe_reconciliation_items(uncertain_matches)

    return {
        "summary": {
            "planned_missing": len(planned_missing),
            "observed_untracked": len(observed_untracked),
            "implemented_differently": len(implemented_differently),
            "uncertain_matches": len(uncertain_matches),
        },
        "planned_missing": planned_missing,
        "observed_untracked": observed_untracked,
        "implemented_differently": implemented_differently,
        "uncertain_matches": uncertain_matches,
    }


def collect_observed_routes(profile: dict[str, Any], doc_candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    routes: dict[str, dict[str, Any]] = {}
    for hint in profile.get("api_contract_hints", []) or []:
        route = hint.get("route", "")
        if not route:
            continue
        routes.setdefault(
            route,
            {
                "target_id": f"contract:api.{slugify_text(route.replace(' ', '_'))}",
                "label": hint.get("label", route),
                "source": "repo",
                "fields": hint.get("response_fields", []),
            },
        )
    for candidate in doc_candidates:
        if candidate.get("type") != "api_route":
            continue
        route = candidate.get("route", "")
        if not route:
            continue
        routes.setdefault(
            route,
            {
                "target_id": f"contract:api.{slugify_text(route.replace(' ', '_'))}",
                "label": candidate.get("label", route),
                "source": "docs",
                "fields": candidate.get("fields", []),
            },
        )
    return routes


def collect_observed_ui_components(profile: dict[str, Any]) -> dict[str, dict[str, Any]]:
    components: dict[str, dict[str, Any]] = {}
    for hint in profile.get("ui_contract_hints", []) or []:
        component = hint.get("component", "")
        if not component:
            continue
        components.setdefault(
            component,
            {
                "target_id": f"contract:ui.{slugify_text(component)}",
                "label": hint.get("label", component),
                "source": "repo",
                "api_routes": hint.get("api_routes", []),
            },
        )
    return components


def collect_observed_asset_paths(profile: dict[str, Any], doc_candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    assets: dict[str, dict[str, Any]] = {}
    for asset in profile.get("data_assets", []) or []:
        path = asset.get("suggested_import", {}).get("raw_asset_value") or asset.get("path", "")
        if not path:
            continue
        assets.setdefault(
            path,
            {
                "target_id": f"data:{slugify_text(Path(path).stem or path)}",
                "label": asset.get("label", path),
                "source": "repo",
            },
        )
    for candidate in doc_candidates:
        if candidate.get("type") != "data_asset":
            continue
        path = candidate.get("import_spec", {}).get("raw_asset_value") or ""
        if not path:
            continue
        assets.setdefault(
            path,
            {
                "target_id": f"data:{slugify_text(Path(path).stem or path)}",
                "label": candidate.get("import_spec", {}).get("data_label", path),
                "source": "docs",
            },
        )
    return assets


def dedupe_reconciliation_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        key = (item.get("target_id", ""), item.get("message", ""))
        current = deduped.get(key)
        if current is None or (item.get("significant") and not current.get("significant")):
            deduped[key] = item
    return sorted(
        deduped.values(),
        key=lambda item: (
            item.get("target_id", ""),
            item.get("field_id", ""),
            item.get("label", ""),
            item.get("message", ""),
        ),
    )


def summarize_patch_for_reconciliation(patch: dict[str, Any]) -> str:
    payload = patch.get("payload", {}) or {}
    if patch.get("type") in {"add_binding", "change_binding"}:
        previous = payload.get("previous_binding", "") or "unbound"
        current = payload.get("new_binding", "") or payload.get("primary_binding", "") or "unbound"
        return f"{previous} -> {current}"
    if patch.get("type") in {"add_field", "remove_field"}:
        return payload.get("field", {}).get("name") or patch.get("target_id", "")
    if patch.get("type") in {"add_node", "remove_node"}:
        return payload.get("node", {}).get("label") or patch.get("target_id", "")
    return payload.get("message", "") or patch.get("target_id", "") or patch.get("type", "")


def first_impact_for_target(impacts: list[dict[str, Any]], target_id: str) -> str:
    for impact in impacts:
        if impact.get("target_id", "") == target_id and impact.get("message"):
            return impact["message"]
    return ""


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
    data_node_id = match_sql_data_node_id(observed_graph, relation)
    if not data_node_id:
        data_node = build_sql_data_node_from_hint(hint)
        data_node["state"] = "observed"
        data_node["verification_state"] = "observed"
        data_node["confidence"] = role_default_confidence(role)
        data_node["evidence"] = ["static_analysis"]
        observed_graph["nodes"].append(data_node)
        data_node_id = data_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=data_node_id,
                node_id=data_node_id,
                role=role,
                evidence=["static_analysis"],
                payload={"node": data_node},
            )
        )
    data_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == data_node_id)
    ensure_sql_columns(
        observed_graph,
        data_node,
        fields,
        role=role,
        evidence=["static_analysis"],
        patches=patches,
    )

    data_field_map = {column.get("name"): column for column in data_node.get("columns", [])}
    for field_hint in fields:
        data_column = data_field_map.get(field_hint.get("name", ""))
        if not data_column:
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
                lineage_refs.append({"field_id": source_column["id"], "role": "foreign_key"})
        if lineage_refs:
            data_column["lineage_inputs"] = lineage_refs

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
                evidence=["static_analysis"],
            )

        return

    compute_node_id = match_sql_compute_node_id(observed_graph, relation)
    if not compute_node_id:
        compute_node = build_sql_compute_node_from_hint(hint, output_node_id=data_node_id)
        compute_node["state"] = "observed"
        compute_node["verification_state"] = "observed"
        compute_node["confidence"] = role_default_confidence(role)
        compute_node["evidence"] = ["static_analysis"]
        observed_graph["nodes"].append(compute_node)
        compute_node_id = compute_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=compute_node_id,
                node_id=compute_node_id,
                role=role,
                evidence=["static_analysis"],
                payload={"node": compute_node},
            )
        )
    compute_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == compute_node_id)
    ensure_sql_columns(
        observed_graph,
        compute_node,
        fields,
        role=role,
        evidence=["static_analysis"],
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
        evidence=["static_analysis"],
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
            evidence=["static_analysis"],
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
            "confidence": role_default_confidence(role),
            "evidence": evidence,
            "required": node["kind"] == "compute",
            "lineage_inputs": [],
            "history": [],
        }
        for metadata_key in ("primary_key", "foreign_key", "nullable", "index", "unique"):
            if metadata_key in field_hint:
                column[metadata_key] = field_hint[metadata_key]
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


def detect_missing_confirmed_structure(
    canonical_graph: dict[str, Any],
    observed_graph: dict[str, Any],
    *,
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    scope: str,
) -> None:
    canonical_index = build_index(canonical_graph)
    observed_index = build_index(observed_graph)
    observed_api_routes = {
        node.get("contract", {}).get("route")
        for node in observed_graph.get("nodes", [])
        if node["kind"] == "contract" and node.get("extension_type") == "api"
    }
    for node in canonical_graph.get("nodes", []):
        if node.get("removed") or node.get("state") != "confirmed":
            continue
        if node["kind"] == "contract" and node.get("extension_type") == "api":
            route = node.get("contract", {}).get("route")
            if route and route not in observed_api_routes and scope == "full":
                contradiction_id = f"contradiction.{node['id']}.missing_route"
                contradictions.append(
                    {
                        "id": contradiction_id,
                        "target_id": node["id"],
                        "node_id": node["id"],
                        "field_id": "",
                        "existing_belief": {"route": route},
                        "new_evidence": {"missing_in_scan": True},
                        "affected_refs": [node["id"]],
                        "confidence_delta": "down",
                        "downstream_impacts": build_missing_node_impacts(canonical_index, node["id"]),
                        "message": f"{route} was not observed during the current full scan.",
                        "why_this_matters": next(iter(build_missing_node_impacts(canonical_index, node["id"])), "Confirmed API structure may be missing from the implementation."),
                        "review_required": True,
                    }
                )
                impacts.extend(
                    {
                        "target_id": node["id"],
                        "message": impact,
                        "significant": True,
                    }
                    for impact in build_missing_node_impacts(canonical_index, node["id"])
                )


def collect_document_candidates(root_dir: Path, doc_paths: list[str]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    paths: list[Path] = []
    if doc_paths:
        paths.extend(resolve_doc_path(root_dir, entry) for entry in doc_paths)
    else:
        docs_dir = root_dir / "docs"
        if docs_dir.exists():
            paths.extend(sorted(docs_dir.rglob("*.md")))
            paths.extend(sorted(docs_dir.rglob("*.yaml")))
            paths.extend(sorted(docs_dir.rglob("*.yml")))
            paths.extend(sorted(docs_dir.rglob("*.json")))
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        if path.suffix.lower() in {".yaml", ".yml", ".json"}:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if isinstance(payload, dict) and "nodes" in payload:
                candidates.append({"type": "partial_graph", "path": str(path), "graph": payload})
                continue
            if isinstance(payload, dict) and isinstance(payload.get("paths"), dict):
                components = payload.get("components", {})
                for path_name, path_item in (payload.get("paths", {}) or {}).items():
                    if not isinstance(path_item, dict):
                        continue
                    for method_name, operation in path_item.items():
                        if method_name.lower() not in HTTP_METHODS or not isinstance(operation, dict):
                            continue
                        route = f"{method_name.upper()} {path_name}"
                        candidates.append(
                            {
                                "type": "api_route",
                                "path": str(path),
                                "route": route,
                                "label": operation.get("summary") or operation.get("operationId") or route,
                                "fields": extract_response_fields(operation, components),
                                "source": "openapi_doc",
                            }
                        )
            continue
        text = path.read_text(encoding="utf-8")
        for method, route in DOC_ROUTE_RE.findall(text):
            candidates.append(
                {
                    "type": "api_route",
                    "path": str(path),
                    "route": f"{method.upper()} {route}",
                    "label": f"{method.upper()} {route}",
                    "fields": [],
                }
            )
        for asset_path in DATA_PATH_RE.findall(text):
            if not any(asset_path.endswith(ext) for ext in (".csv", ".parquet", ".json")):
                continue
            candidates.append(
                {
                    "type": "data_asset",
                    "path": str(path),
                    "import_spec": {
                        "source_label": Path(asset_path).stem.replace("_", " ").title(),
                        "source_extension_type": "disk_path",
                        "source_description": f"Imported from {path.relative_to(root_dir)}",
                        "source_provider": "local",
                        "source_refresh": "",
                        "source_origin_kind": "disk_path",
                        "source_origin_value": asset_path,
                        "source_series_id": "",
                        "raw_asset_label": Path(asset_path).name,
                        "raw_asset_kind": "file",
                        "raw_asset_format": detect_asset_format(asset_path),
                        "raw_asset_value": asset_path,
                        "profile_ready": False,
                        "data_label": Path(asset_path).stem.replace("_", " ").title(),
                        "data_extension_type": "raw_dataset",
                        "data_description": f"Imported from {path.relative_to(root_dir)}",
                        "update_frequency": "",
                        "persistence": "cold",
                        "persisted": False,
                        "schema_columns": [],
                    },
                }
            )
    return candidates


def resolve_doc_path(root_dir: Path, entry: str) -> Path:
    candidate = Path(entry).expanduser()
    if candidate.is_absolute():
        return candidate
    return (root_dir / candidate).resolve()


def build_scan_fingerprint(
    canonical_graph: dict[str, Any],
    *,
    profile: dict[str, Any],
    role: str,
    scope: str,
    doc_candidates: list[dict[str, Any]],
    root_dir: Path,
    selected_paths: list[str],
) -> str:
    payload = {
        "base_structure_version": canonical_graph.get("metadata", {}).get("structure_version", 1),
        "role": role,
        "scope": scope,
        "root_dir": str(root_dir),
        "profile": profile.get("summary", {}),
        "data_assets": deep_sort(
            [
                {
                    "path": item.get("path", ""),
                    "format": item.get("format", ""),
                    "label": item.get("label", ""),
                    "raw_asset_value": item.get("suggested_import", {}).get("raw_asset_value", ""),
                }
                for item in profile.get("data_assets", [])
            ]
        ),
        "api_hints": deep_sort(
            [
                {
                    "route": item.get("route", ""),
                    "response_fields": item.get("response_fields", []),
                    "response_field_sources": item.get("response_field_sources", []),
                    "response_model": item.get("response_model", ""),
                    "detected_from": item.get("detected_from", ""),
                }
                for item in profile.get("api_contract_hints", [])
            ]
        ),
        "ui_hints": deep_sort(
            [
                {
                    "id": item.get("id", ""),
                    "api_routes": item.get("api_routes", []),
                    "used_fields": item.get("used_fields", []),
                    "route_field_hints": item.get("route_field_hints", {}),
                }
                for item in profile.get("ui_contract_hints", [])
            ]
        ),
        "sql_hints": deep_sort(
            [
                {
                    "relation": item.get("relation", ""),
                    "object_type": item.get("object_type", ""),
                    "fields": item.get("fields", []),
                    "upstream_relations": item.get("upstream_relations", []),
                    "file": item.get("file", ""),
                }
                for item in profile.get("sql_structure_hints", [])
            ]
        ),
        "orm_hints": deep_sort(
            [
                {
                    "relation": item.get("relation", ""),
                    "object_type": item.get("object_type", ""),
                    "fields": item.get("fields", []),
                    "upstream_relations": item.get("upstream_relations", []),
                    "file": item.get("file", ""),
                }
                for item in profile.get("orm_structure_hints", [])
            ]
        ),
        "doc_candidates": deep_sort(doc_candidates),
        "selected_paths": sorted(selected_paths),
        "scanners": SCANNER_VERSIONS,
    }
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_source_node_from_import_spec(import_spec: dict[str, Any]) -> dict[str, Any]:
    node_id = f"source:{slugify_text(import_spec.get('source_label', 'source'))}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "source",
                    "extension_type": import_spec.get("source_extension_type", "object"),
                    "label": import_spec.get("source_label", "Source"),
                    "description": import_spec.get("source_description", ""),
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "columns": [],
                    "source": {
                        "provider": import_spec.get("source_provider", ""),
                        "origin": {
                            "kind": import_spec.get("source_origin_kind", ""),
                            "value": import_spec.get("source_origin_value", ""),
                        },
                        "refresh": import_spec.get("source_refresh", ""),
                        "shared_config": {},
                        "series_id": import_spec.get("source_series_id", ""),
                        "data_dictionaries": [],
                        "raw_assets": [
                            {
                                "label": import_spec.get("raw_asset_label", ""),
                                "kind": import_spec.get("raw_asset_kind", "file"),
                                "format": import_spec.get("raw_asset_format", "unknown"),
                                "value": import_spec.get("raw_asset_value", ""),
                                "profile_ready": import_spec.get("profile_ready", True),
                            }
                        ],
                    },
                    "data": {},
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_data_node_from_import_spec(import_spec: dict[str, Any]) -> dict[str, Any]:
    node_id = f"data:{slugify_text(import_spec.get('data_label', 'data'))}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "data",
                    "extension_type": import_spec.get("data_extension_type", "raw_dataset"),
                    "label": import_spec.get("data_label", "Data"),
                    "description": import_spec.get("data_description", ""),
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "columns": [
                        {
                            "name": column.get("name", ""),
                            "data_type": column.get("data_type", "unknown"),
                        }
                        for column in import_spec.get("schema_columns", [])
                    ],
                    "source": {},
                    "data": {
                        "persistence": import_spec.get("persistence", ""),
                        "local_path": import_spec.get("raw_asset_value", ""),
                        "update_frequency": import_spec.get("update_frequency", ""),
                        "persisted": import_spec.get("persisted", False),
                        "profile_target": import_spec.get("raw_asset_value", ""),
                    },
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_sql_data_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    relation = hint.get("relation") or hint.get("label") or "sql_relation"
    object_type = hint.get("object_type", "table")
    data_extension_type = {
        "table": "table",
        "view": "view",
        "materialized_view": "materialized_view",
    }.get(object_type, "table")
    persistence = "serving" if object_type in {"view", "materialized_view"} else "hot"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"data:{slugify_text(relation)}",
                    "kind": "data",
                    "extension_type": data_extension_type,
                    "label": humanize_sql_relation_name(relation),
                    "description": hint.get("description", ""),
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"sql_relation:{relation}", f"sql_object:{object_type}"],
                    "columns": [
                        {
                            "name": field.get("name", ""),
                            "data_type": field.get("data_type", "unknown"),
                        }
                        for field in hint.get("fields", [])
                        if field.get("name")
                    ],
                    "source": {},
                    "data": {
                        "persistence": persistence,
                        "local_path": hint.get("file", ""),
                        "update_frequency": "",
                        "persisted": True,
                        "profile_target": hint.get("file", ""),
                    },
                    "compute": {},
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_sql_compute_node_from_hint(hint: dict[str, Any], *, output_node_id: str) -> dict[str, Any]:
    relation = hint.get("relation") or hint.get("label") or "sql_transform"
    label = f"Build {humanize_sql_relation_name(relation)}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"compute:transform.{slugify_text(relation)}",
                    "kind": "compute",
                    "extension_type": "transform",
                    "label": label,
                    "description": hint.get("description", "") or f"Observed SQL transform for {relation}.",
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"sql_relation:{relation}", "sql_transform"],
                    "columns": [
                        {
                            "name": field.get("name", ""),
                            "data_type": field.get("data_type", "unknown"),
                        }
                        for field in hint.get("fields", [])
                        if field.get("name")
                    ],
                    "source": {},
                    "data": {},
                    "compute": {
                        "runtime": "sql",
                        "inputs": [],
                        "outputs": [output_node_id],
                        "notes": f"Observed from {hint.get('file', '')}",
                        "feature_selection": [],
                        "column_mappings": [],
                    },
                    "contract": {},
                }
            ],
            "edges": [],
        }
    )["nodes"][0]


def build_api_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    route = hint.get("route") or hint.get("label") or "GET /unknown"
    node_id = f"contract:api.{slugify_text(route.replace(' ', '_'))}"
    graph = normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "contract",
                    "extension_type": "api",
                    "label": hint.get("label") or route,
                    "description": hint.get("description", ""),
                    "owner": "repo-scan",
                    "columns": [],
                    "source": {},
                    "data": {},
                    "compute": {},
                    "contract": {
                        "route": route,
                        "component": "",
                        "ui_role": "",
                        "fields": [{"name": field_name} for field_name in hint.get("response_fields", []) or []],
                    },
                }
            ],
            "edges": [],
        }
    )
    return graph["nodes"][0]


def build_ui_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    component = hint.get("component") or hint.get("label") or "UiComponent"
    node_id = f"contract:ui.{slugify_text(component)}"
    graph = normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": node_id,
                    "kind": "contract",
                    "extension_type": "ui",
                    "label": hint.get("label") or component,
                    "description": hint.get("description", ""),
                    "owner": "repo-scan",
                    "columns": [],
                    "source": {},
                    "data": {},
                    "compute": {},
                    "contract": {
                        "route": "",
                        "component": component,
                        "ui_role": "component",
                        "fields": [{"name": field_name} for field_name in hint.get("used_fields", []) or []],
                    },
                }
            ],
            "edges": [],
        }
    )
    return graph["nodes"][0]


def ensure_contract_field(
    graph: dict[str, Any],
    node: dict[str, Any],
    field_name: str,
    *,
    role: str,
    evidence: list[str],
    patches: list[dict[str, Any]],
) -> dict[str, Any]:
    for field in node.get("contract", {}).get("fields", []):
        if field["name"] == field_name:
            return field
    existing_ids = collect_existing_field_ids(graph)
    field = {
        "id": make_field_id(node["id"], field_name, existing_ids),
        "name": field_name,
        "required": True,
        "state": "observed",
        "verification_state": "observed",
        "confidence": role_default_confidence(role),
        "evidence": evidence,
        "primary_binding": "",
        "alternatives": [],
        "sources": [],
        "history": [],
    }
    node.setdefault("contract", {}).setdefault("fields", []).append(field)
    patches.append(
        build_patch(
            "add_field",
            target_id=field["id"],
            node_id=node["id"],
            field_id=field["id"],
            role=role,
            evidence=evidence,
            payload={"field": field},
        )
    )
    return field


def collect_existing_field_ids(graph: dict[str, Any]) -> set[str]:
    field_ids: set[str] = set()
    for node in graph.get("nodes", []):
        for column in node.get("columns", []):
            if column.get("id"):
                field_ids.add(column["id"])
        for field in node.get("contract", {}).get("fields", []):
            if field.get("id"):
                field_ids.add(field["id"])
    return field_ids


def resolve_legacy_source_reference(graph: dict[str, Any], reference: str) -> dict[str, Any] | None:
    if not reference:
        return None
    for node in graph.get("nodes", []):
        for column in node.get("columns", []):
            if column.get("id") == reference or column_ref(node["id"], column.get("name", "")) == reference:
                return {"node_id": node["id"], "column": column.get("name", "")}
        for field in node.get("contract", {}).get("fields", []):
            if field.get("id") == reference or column_ref(node["id"], field.get("name", "")) == reference:
                return {"node_id": node["id"], "field": field.get("name", "")}
    return None


def reconcile_field_binding(
    graph: dict[str, Any],
    node_id: str,
    field_name: str,
    suggested_binding: str,
    *,
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    role: str,
    canonical_index: dict[str, Any],
    evidence: list[str] | None = None,
) -> None:
    node = next(candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id)
    field = next(candidate for candidate in node.get("contract", {}).get("fields", []) if candidate["name"] == field_name)
    current_binding = field.get("primary_binding", "")
    canonical_field_id = canonical_index["field_ref_to_id"].get(column_ref(node_id, field_name))
    canonical_field = canonical_index["field_by_id"].get(canonical_field_id, {})
    canonical_binding = canonical_field.get("primary_binding", "") if canonical_field else ""
    binding_evidence = evidence or ["schema_match"]

    if not current_binding and not canonical_binding:
        field["primary_binding"] = suggested_binding
        field["sources"] = [resolve_legacy_source_reference(graph, suggested_binding)] if suggested_binding else []
        field["sources"] = [source for source in field["sources"] if source]
        field["state"] = "observed"
        field["confidence"] = role_default_confidence(role)
        field["verification_state"] = "observed"
        patches.append(
            build_patch(
                "add_binding",
                target_id=field["id"],
                node_id=node_id,
                field_id=field["id"],
                role=role,
                evidence=binding_evidence,
                payload={"field_id": field["id"], "primary_binding": suggested_binding, "alternatives": []},
            )
        )
        return

    winner_binding = canonical_binding or current_binding
    if winner_binding and winner_binding != suggested_binding:
        contradiction = build_binding_contradiction(
            graph,
            node_id=node_id,
            field=field,
            winner_binding=winner_binding,
            suggested_binding=suggested_binding,
        )
        contradictions.append(contradiction)
        impacts.extend(
            {
                "target_id": field["id"],
                "message": impact,
                "significant": is_significant_impact(field, canonical_field, impact),
            }
            for impact in contradiction.get("downstream_impacts", [])
        )
        patches.append(
            build_patch(
                "change_binding",
                target_id=field["id"],
                node_id=node_id,
                field_id=field["id"],
                role=role,
                evidence=binding_evidence,
                payload={
                    "field_id": field["id"],
                    "previous_binding": winner_binding,
                    "new_binding": suggested_binding,
                },
            )
        )
        if role == "scout":
            field["confidence"] = lower_confidence(field.get("confidence", "medium"))
        return

    if suggested_binding and current_binding != suggested_binding:
        field["primary_binding"] = suggested_binding
        field["sources"] = [resolve_legacy_source_reference(graph, suggested_binding)] if suggested_binding else []
        field["sources"] = [source for source in field["sources"] if source]


def suggest_explicit_binding_for_contract_field(
    graph: dict[str, Any],
    source_fields: list[dict[str, Any]],
    *,
    role: str,
    patches: list[dict[str, Any]],
) -> str:
    if len(source_fields) != 1:
        return ""
    source_field = source_fields[0]
    relation = source_field.get("relation", "")
    column_name = source_field.get("column", "")
    if not relation or not column_name:
        return ""
    upstream_node_id = ensure_sql_relation_node(
        graph,
        relation=relation,
        role=role,
        patches=patches,
        column_names=[column_name],
    )
    upstream_node = next(candidate for candidate in graph["nodes"] if candidate["id"] == upstream_node_id)
    source_column = find_column(upstream_node, column_name)
    return source_column["id"] if source_column else ""


def ensure_edge(
    graph: dict[str, Any],
    *,
    edge_type: str,
    source_id: str,
    target_id: str,
    patches: list[dict[str, Any]],
    role: str,
    evidence: list[str],
) -> None:
    node_ids = {node.get("id") for node in graph.get("nodes", [])}
    if source_id not in node_ids or target_id not in node_ids:
        return
    if any(edge["type"] == edge_type and edge["source"] == source_id and edge["target"] == target_id for edge in graph.get("edges", [])):
        return
    edge = build_edge(edge_type, source_id, target_id)
    graph.setdefault("edges", []).append(edge)
    patches.append(
        build_patch(
            "add_edge",
            target_id=edge["id"],
            edge_id=edge["id"],
            role=role,
            evidence=evidence,
            payload={"edge": edge},
        )
    )


def build_edge(edge_type: str, source_id: str, target_id: str) -> dict[str, Any]:
    return {
        "id": f"edge.{edge_type}.{slugify_text(source_id)}.{slugify_text(target_id)}",
        "type": edge_type,
        "source": source_id,
        "target": target_id,
        "label": "",
        "column_mappings": [],
        "notes": "",
        "state": "observed",
        "confidence": "medium",
        "evidence": ["static_analysis"],
        "removed": False,
        "history": [],
    }


def build_patch(
    patch_type: str,
    *,
    target_id: str,
    role: str,
    evidence: list[str],
    node_id: str = "",
    field_id: str = "",
    edge_id: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = payload or {}
    fingerprint = hashlib.sha1(
        json.dumps(
            {
                "type": patch_type,
                "target_id": target_id,
                "node_id": node_id,
                "field_id": field_id,
                "edge_id": edge_id,
                "payload": payload,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:10]
    return {
        "id": f"patch.{patch_type}.{target_id.replace(':', '_').replace('.', '_')}.{fingerprint}",
        "type": patch_type,
        "target_id": target_id,
        "node_id": node_id,
        "field_id": field_id,
        "edge_id": edge_id,
        "confidence": role_default_confidence(role),
        "evidence": evidence,
        "review_state": "pending",
        "payload": payload,
    }


def build_binding_contradiction(
    graph: dict[str, Any],
    *,
    node_id: str,
    field: dict[str, Any],
    winner_binding: str,
    suggested_binding: str,
) -> dict[str, Any]:
    contradiction_id = f"contradiction.{field['id']}.{hashlib.sha1(f'{winner_binding}|{suggested_binding}'.encode('utf-8')).hexdigest()[:8]}"
    impacts = describe_downstream_component_impacts(build_index(graph), node_id, field["name"], infer_ui_roles(graph))
    existing_display = display_ref_for_field_id(winner_binding, graph)
    suggested_display = display_ref_for_field_id(suggested_binding, graph)
    return {
        "id": contradiction_id,
        "target_id": field["id"],
        "node_id": node_id,
        "field_id": field["id"],
        "existing_belief": {"primary_binding": winner_binding},
        "new_evidence": {"primary_binding": suggested_binding},
        "affected_refs": [field["id"], winner_binding, suggested_binding],
        "confidence_delta": "down",
        "downstream_impacts": impacts,
        "message": f"{field['name']} maps to {existing_display or winner_binding}, but scan suggests {suggested_display or suggested_binding}.",
        "why_this_matters": impacts[0] if impacts else "Column-level structure no longer matches the latest observed implementation.",
        "review_required": True,
    }


def apply_patch_to_graph(graph: dict[str, Any], patch: dict[str, Any], *, merged_by: str) -> None:
    patch_type = patch.get("type")
    payload = patch.get("payload", {})
    if patch_type == "add_node":
        node = payload.get("node")
        if node and not any(candidate["id"] == node["id"] for candidate in graph.get("nodes", [])):
            graph["nodes"].append(deepcopy(node))
        return
    if patch_type == "add_edge":
        edge = payload.get("edge")
        if edge and not any(candidate["id"] == edge["id"] for candidate in graph.get("edges", [])):
            graph["edges"].append(deepcopy(edge))
        return
    if patch_type == "add_field":
        node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == patch.get("node_id")), None)
        field = payload.get("field")
        if not node or not field:
            return
        target_list = node["contract"]["fields"] if node["kind"] == "contract" else node["columns"]
        if not any(candidate["id"] == field["id"] for candidate in target_list):
            target_list.append(deepcopy(field))
        return
    if patch_type == "add_binding":
        update_field_binding(graph, patch.get("field_id", ""), payload.get("primary_binding", ""), payload.get("alternatives", []), merged_by)
        return
    if patch_type == "change_binding":
        update_field_binding(graph, patch.get("field_id", ""), payload.get("new_binding", ""), payload.get("alternatives", []), merged_by, previous_binding=payload.get("previous_binding", ""))
        return
    if patch_type == "confidence_change":
        update_field_confidence(graph, patch.get("field_id", ""), payload.get("confidence", ""), merged_by, explicit_override=True)
        return
    if patch_type == "remove_field":
        mark_field_removed(graph, patch.get("field_id", ""), merged_by)
        return
    if patch_type == "remove_node":
        mark_node_removed(graph, patch.get("node_id", ""), merged_by)
        return
    if patch_type == "remove_edge":
        edge = next((candidate for candidate in graph.get("edges", []) if candidate["id"] == patch.get("edge_id")), None)
        if edge:
            edge["removed"] = True
            edge["history"] = [*edge.get("history", []), {"change": "remove_edge", "at": utc_timestamp(), "by": merged_by}]
        return
    if patch_type == "contradiction":
        return


def update_field_binding(
    graph: dict[str, Any],
    field_id: str,
    primary_binding: str,
    alternatives: list[str],
    merged_by: str,
    *,
    previous_binding: str = "",
) -> None:
    node, field = resolve_field(graph, field_id)
    if not node or not field:
        return
    previous = field.get("primary_binding", previous_binding)
    field["primary_binding"] = primary_binding
    field["alternatives"] = alternatives or []
    if node["kind"] == "contract":
        field["sources"] = [
            source
            for source in [
                resolve_legacy_source_reference(graph, reference)
                for reference in [primary_binding, *field.get("alternatives", [])]
            ]
            if source
        ]
    else:
        field["lineage_inputs"] = [
            {"field_id": reference, "role": ""}
            for reference in [primary_binding, *field.get("alternatives", [])]
            if reference
        ]
    if previous != primary_binding:
        field.setdefault("history", []).append(
            {
                "change": "change_binding",
                "from_value": previous,
                "to_value": primary_binding,
                "at": utc_timestamp(),
                "by": merged_by,
            }
        )


def update_field_confidence(
    graph: dict[str, Any],
    field_id: str,
    confidence: str,
    merged_by: str,
    *,
    explicit_override: bool = False,
) -> None:
    _, field = resolve_field(graph, field_id)
    if not field or not confidence:
        return
    previous = field.get("confidence", "medium")
    if previous == confidence:
        return
    if not explicit_override and CONFIDENCE_RANK.get(confidence, 0) < CONFIDENCE_RANK.get(previous, 0):
        return
    field["confidence"] = confidence
    field.setdefault("history", []).append(
        {
            "change": "confidence_change",
            "from_value": previous,
            "to_value": confidence,
            "at": utc_timestamp(),
            "by": merged_by,
        }
    )


def mark_field_removed(graph: dict[str, Any], field_id: str, merged_by: str) -> None:
    _, field = resolve_field(graph, field_id)
    if not field:
        return
    field["removed"] = True
    field["removed_at"] = utc_timestamp()
    field["removed_by"] = merged_by
    field.setdefault("history", []).append(
        {
            "change": "remove_field",
            "from_value": field.get("name", ""),
            "to_value": "",
            "at": field["removed_at"],
            "by": merged_by,
        }
    )


def mark_node_removed(graph: dict[str, Any], node_id: str, merged_by: str) -> None:
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None)
    if not node:
        return
    node["removed"] = True
    node["removed_at"] = utc_timestamp()
    node["removed_by"] = merged_by
    node.setdefault("history", []).append(
        {
            "change": "remove_node",
            "from_value": node.get("label", node_id),
            "to_value": "",
            "at": node["removed_at"],
            "by": merged_by,
        }
    )


def resolve_field(graph: dict[str, Any], field_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    index = build_index(graph)
    node_id = index["field_owner"].get(field_id)
    if not node_id:
        return None, None
    node = index["nodes"].get(node_id)
    if not node:
        return None, None
    for column in node.get("columns", []):
        if column["id"] == field_id:
            return node, column
    for field in node.get("contract", {}).get("fields", []):
        if field["id"] == field_id:
            return node, field
    return None, None


def suggest_binding_for_contract_field(graph: dict[str, Any], node_id: str, field_name: str) -> str:
    field_normalized = normalize_token(field_name)
    for node in graph.get("nodes", []):
        if node["id"] == node_id:
            continue
        if node["kind"] in {"data", "compute"}:
            for column in node.get("columns", []):
                if normalize_token(column["name"]) == field_normalized:
                    return column["id"]
        if node["kind"] == "contract" and node.get("extension_type") == "api":
            for field in node.get("contract", {}).get("fields", []):
                if normalize_token(field["name"]) == field_normalized:
                    return field["id"]
    return ""


def match_source_node_id(graph: dict[str, Any], import_spec: dict[str, Any]) -> str:
    origin = import_spec.get("source_origin_value") or import_spec.get("raw_asset_value") or ""
    for node in graph.get("nodes", []):
        if node["kind"] != "source" or node.get("removed"):
            continue
        raw_values = [asset.get("value", "") for asset in node.get("source", {}).get("raw_assets", [])]
        if origin and (node.get("source", {}).get("origin", {}).get("value") == origin or origin in raw_values):
            return node["id"]
        if node.get("label") == import_spec.get("source_label"):
            return node["id"]
    return ""


def match_data_node_id(graph: dict[str, Any], import_spec: dict[str, Any]) -> str:
    profile_target = import_spec.get("raw_asset_value", "")
    for node in graph.get("nodes", []):
        if node["kind"] != "data" or node.get("removed"):
            continue
        if profile_target and profile_target in {
            node.get("data", {}).get("profile_target", ""),
            node.get("data", {}).get("local_path", ""),
        }:
            return node["id"]
        if node.get("label") == import_spec.get("data_label"):
            return node["id"]
    return ""


def match_sql_data_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"sql_relation:{relation}"
    normalized_relation = slugify_text(relation)
    for node in graph.get("nodes", []):
        if node["kind"] != "data" or node.get("removed"):
            continue
        if tag in (node.get("tags") or []):
            return node["id"]
        if slugify_text(node.get("id", "").split(":", 1)[-1]) == normalized_relation:
            return node["id"]
    return ""


def match_sql_compute_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"sql_relation:{relation}"
    normalized_relation = slugify_text(relation)
    for node in graph.get("nodes", []):
        if node["kind"] != "compute" or node.get("extension_type") != "transform" or node.get("removed"):
            continue
        if tag in (node.get("tags") or []):
            return node["id"]
        if slugify_text(node.get("id", "").split(".", 1)[-1]) == normalized_relation:
            return node["id"]
    return ""


def match_api_node_id(graph: dict[str, Any], route: str, label: str) -> str:
    for node in graph.get("nodes", []):
        if node["kind"] == "contract" and node.get("extension_type") == "api" and node.get("contract", {}).get("route") == route:
            return node["id"]
    normalized = slugify_text(route or label)
    for node in graph.get("nodes", []):
        if node["kind"] == "contract" and node.get("extension_type") == "api" and slugify_text(node["label"]) == normalized:
            return node["id"]
    return ""


def match_ui_node_id(graph: dict[str, Any], component: str) -> str:
    for node in graph.get("nodes", []):
        if node["kind"] != "contract" or node.get("extension_type") != "ui":
            continue
        if node.get("contract", {}).get("component") == component or node.get("label") == component:
            return node["id"]
    return ""


def humanize_sql_relation_name(relation: str) -> str:
    return relation.split(".")[-1].replace("_", " ").replace("-", " ").title() or relation


def build_missing_node_impacts(index: dict[str, Any], node_id: str) -> list[str]:
    node = index["nodes"].get(node_id)
    if not node:
        return []
    if node["kind"] == "contract" and node.get("extension_type") == "api":
        labels = []
        ui_roles = infer_ui_roles({"nodes": list(index["nodes"].values()), "edges": list(index["edges"].values())})
        for field in node.get("contract", {}).get("fields", []):
            labels.extend(describe_downstream_component_impacts(index, node_id, field["name"], ui_roles))
        return sorted(set(labels))
    return []


def role_default_confidence(role: str) -> str:
    return "medium" if role == "scout" else "high"


def lower_confidence(confidence: str) -> str:
    if confidence == "high":
        return "medium"
    if confidence == "medium":
        return "low"
    return "low"


def is_significant_impact(field: dict[str, Any], canonical_field: dict[str, Any], impact: str) -> bool:
    return bool(
        field.get("required", False)
        or canonical_field.get("required", False)
        or impact
    )


def dedupe_patch_rows(patches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for patch in patches:
        key = (patch.get("target_id", ""), patch.get("type", ""))
        current = deduped.get(key)
        if current is None:
            deduped[key] = patch
            continue
        if PATCH_TYPE_PRECEDENCE.get(patch.get("type", ""), 999) <= PATCH_TYPE_PRECEDENCE.get(current.get("type", ""), 999):
            deduped[key] = patch
    return sorted(
        deduped.values(),
        key=lambda item: (
            item.get("target_id", ""),
            PATCH_TYPE_PRECEDENCE.get(item.get("type", ""), 999),
            json.dumps(item.get("payload", {}), sort_keys=True),
        ),
    )


def dedupe_contradictions(contradictions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for contradiction in contradictions:
        deduped.setdefault(contradiction["id"], contradiction)
    return sorted(deduped.values(), key=lambda item: item["id"])


def dedupe_impacts(impacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for impact in impacts:
        if not impact.get("significant", False):
            continue
        key = (impact.get("target_id", ""), impact.get("message", ""))
        deduped.setdefault(key, impact)
    return [deduped[key] for key in sorted(deduped)]


def slugify_text(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "_" for character in value).strip("_")


def normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def detect_asset_format(asset_path: str) -> str:
    lowered = asset_path.lower()
    if lowered.endswith(".csv.gz"):
        return "csv_gz"
    if lowered.endswith(".zip"):
        return "zip_csv"
    if lowered.endswith(".parquet"):
        return "parquet"
    if "*" in lowered and lowered.endswith(".parquet"):
        return "parquet_collection"
    if lowered.endswith(".csv"):
        return "csv"
    return "unknown"
