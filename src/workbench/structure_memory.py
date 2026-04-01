from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from .diagnostics import build_graph_diagnostics, infer_ui_roles
from .structure_blueprints import slugify_text
from .structure_observations import (
    apply_api_hint_observation,
    apply_asset_observation,
    apply_document_candidate,
    apply_orm_query_projection_observation,
    apply_sql_structure_observation,
    apply_ui_hint_observation,
)
from .project_profiler import resolve_project_profile
from .structure_plan_candidates import apply_partial_graph_candidate, plan_sources_to_binding_refs
from .structure_hybrid_support import (
    build_contradiction_clusters,
    build_contradiction_summary,
    build_hybrid_contradiction,
    build_hybrid_impact_graph,
    collect_api_field_evidence,
    collect_data_relation_evidence,
    collect_observed_asset_paths,
    collect_observed_routes,
    collect_observed_ui_components,
    collect_ui_field_evidence,
    dedupe_reconciliation_items,
    find_related_patch_ids_for_targets,
    first_impact_for_target,
    normalize_lineage_keys,
    normalize_reference_key,
    summarize_downstream_breakage,
    summarize_patch_for_reconciliation,
)
from .structure_reconciliation import (
    apply_patch_to_graph,
    build_missing_node_impacts,
    build_patch,
    dedupe_contradictions,
    dedupe_impacts,
    dedupe_patch_rows,
    lower_confidence,
    match_api_node_id,
    match_sql_compute_node_id,
    match_sql_data_node_id,
    match_ui_node_id,
    normalize_token,
    resolve_field,
)
from .structure_candidates import (
    collect_document_candidates,
    combine_partial_graph_candidates,
    extract_sql_relation_tag,
    merge_partial_graph_node,
)
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
    identifierify,
    normalize_graph,
)
from .validation import build_validation_report

SCANNER_VERSIONS = {
    "assets": "1",
    "python_fastapi": "1",
    "ui_contracts": "1",
    "docs": "2",
    "sql": "3",
    "orm": "3",
}


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
    profile_token: str | None = None,
    force_refresh: bool = False,
    doc_paths: list[str] | None = None,
    selected_paths: list[str] | None = None,
) -> dict[str, Any]:
    canonical_graph = load_graph()
    profile = resolve_project_profile(
        root_dir,
        include_tests=include_tests,
        include_internal=include_internal,
        profile_token=profile_token,
        force_refresh=force_refresh,
    )
    doc_candidates = collect_document_candidates(root_dir, doc_paths or [])
    fingerprint = build_scan_fingerprint(
        canonical_graph,
        profile=profile,
        role=role,
        scope=scope,
        include_tests=include_tests,
        include_internal=include_internal,
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

    observed_graph, patches, contradictions, impacts, hybrid_context = build_observed_bundle_content(
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
        hybrid_context=hybrid_context,
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
            "selected_paths": [str(path) for path in selected_paths or []],
            "include_tests": include_tests,
            "include_internal": include_internal,
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
            "bundle_owner": "",
            "assigned_reviewer": "",
            "triage_state": "new",
            "triage_note": "",
            "workflow_history": [],
            "last_reviewed_at": "",
            "last_reviewed_by": "",
            "last_review_note": "",
            "merged_at": "",
            "merged_by": "",
            "merge_status": "",
            "rebase_required": False,
            "merge_patch_ids": [],
            "merge_blockers": [],
            "merge_plan": {},
            "last_rebase_summary": {},
        },
        "readiness": readiness,
    }
    sync_bundle_review_summary(bundle)
    sync_bundle_reconciliation(bundle)
    normalized_bundle = save_bundle(bundle)
    return {
        "bundle": normalized_bundle,
        "structure": build_structure_summary(canonical_graph),
        "project_profile": profile,
    }


def review_bundle_patch(
    bundle_id: str,
    patch_id: str,
    decision: str,
    *,
    reviewed_by: str = "user",
    note: str = "",
) -> dict[str, Any]:
    return review_bundle_patches(bundle_id, [patch_id], decision, reviewed_by=reviewed_by, note=note)


def append_review_event(
    record: dict[str, Any],
    *,
    decision: str,
    reviewed_by: str,
    note: str = "",
    reviewed_at: str,
    related_patch_ids: list[str] | None = None,
) -> None:
    actor = reviewed_by or "user"
    review_note = note.strip()
    event = {
        "decision": decision,
        "reviewed_by": actor,
        "reviewed_at": reviewed_at,
        "note": review_note,
    }
    if related_patch_ids:
        event["related_patch_ids"] = sorted(dict.fromkeys(related_patch_ids))
    record["review_state"] = decision
    record["reviewed_by"] = actor
    record["reviewed_at"] = reviewed_at
    record["review_note"] = review_note
    record.setdefault("review_history", []).append(event)


def append_workflow_event(
    review: dict[str, Any],
    *,
    updated_by: str,
    updated_at: str,
    note: str,
    changes: dict[str, Any],
) -> None:
    if not changes and not note.strip():
        return
    review.setdefault("workflow_history", []).append(
        {
            "updated_by": updated_by or "user",
            "updated_at": updated_at,
            "note": note.strip(),
            "changes": deepcopy(changes),
        }
    )


def copy_bundle_workflow_state(source_bundle: dict[str, Any], target_bundle: dict[str, Any]) -> None:
    source_review = source_bundle.get("review", {})
    target_review = target_bundle.setdefault("review", {})
    for key, default in (
        ("bundle_owner", ""),
        ("assigned_reviewer", ""),
        ("triage_state", "new"),
        ("triage_note", ""),
    ):
        target_review[key] = source_review.get(key, default) or default
    target_review["workflow_history"] = deepcopy(source_review.get("workflow_history", []))


def apply_patch_review_decision(
    bundle: dict[str, Any],
    patch_ids: list[str],
    decision: str,
    *,
    reviewed_by: str,
    note: str = "",
    reviewed_at: str,
) -> list[str]:
    requested_ids = {patch_id for patch_id in patch_ids if patch_id}
    found_ids: list[str] = []
    for patch in bundle.get("patches", []):
        if patch["id"] not in requested_ids:
            continue
        append_review_event(
            patch,
            decision=decision,
            reviewed_by=reviewed_by,
            note=note,
            reviewed_at=reviewed_at,
        )
        found_ids.append(patch["id"])
    return found_ids


def sync_bundle_review_summary(
    bundle: dict[str, Any],
    *,
    reviewed_by: str = "",
    note: str = "",
    reviewed_at: str = "",
) -> None:
    review = bundle.setdefault("review", {})
    canonical_graph = load_graph()
    for decision in ("accepted", "rejected", "deferred"):
        review[f"{decision}_patch_ids"] = sorted(
            patch["id"]
            for patch in bundle.get("patches", [])
            if patch.get("review_state") == decision
        )
    if reviewed_at:
        review["last_reviewed_at"] = reviewed_at
        review["last_reviewed_by"] = reviewed_by or "user"
        review["last_review_note"] = note.strip()
    if review.get("superseded_by_bundle_id"):
        review["merge_status"] = "superseded"
        review["rebase_required"] = False
        review["merge_blockers"] = [
            {
                "type": "superseded_bundle",
                "reason": (
                    f"Bundle has been superseded by {review['superseded_by_bundle_id']} "
                    "and should not be merged."
                ),
            }
        ]
        review["merge_patch_ids"] = [
            patch["id"]
            for patch in sorted_accepted_patches(bundle.get("patches", []))
        ]
        review["merge_plan"] = build_merge_plan_artifact(bundle, canonical_graph=canonical_graph)
        return
    staleness = build_bundle_staleness(bundle, canonical_graph=canonical_graph)
    if staleness["is_stale"]:
        review["merge_status"] = "stale"
        review["rebase_required"] = True
        review["merge_blockers"] = staleness["merge_blockers"]
    else:
        if review.get("merge_status") == "stale":
            review["merge_status"] = ""
        review["rebase_required"] = False
        review["merge_blockers"] = []
    review["merge_patch_ids"] = [
        patch["id"]
        for patch in sorted_accepted_patches(bundle.get("patches", []))
    ]
    merge_plan = build_merge_plan_artifact(bundle, canonical_graph=canonical_graph)
    review["merge_plan"] = merge_plan
    if merge_plan.get("status") == "blocked":
        review["merge_status"] = "blocked"
        review["merge_blockers"] = merge_plan.get("blockers", [])
    elif review.get("merge_status") == "blocked":
        review["merge_status"] = ""
        review["merge_blockers"] = []


def sync_bundle_reconciliation(bundle: dict[str, Any]) -> None:
    reconciliation = bundle.setdefault("reconciliation", {})
    field_matrix = dedupe_field_matrix_rows(reconciliation.get("field_matrix", []))
    reconciliation["field_matrix"] = field_matrix
    reconciliation["field_matrix_summary"] = build_field_matrix_summary(field_matrix)
    reconciliation["contradiction_summary"] = build_contradiction_summary(bundle.get("contradictions", []))
    contradiction_clusters = build_contradiction_clusters(
        bundle.get("contradictions", []),
        bundle.get("patches", []),
    )
    reconciliation["contradiction_clusters"] = contradiction_clusters
    reconciliation["contradiction_cluster_summary"] = build_contradiction_cluster_summary(contradiction_clusters)
    reconciliation["downstream_breakage"] = summarize_downstream_breakage(
        bundle.get("contradictions", []),
        bundle.get("impacts", []),
    )


def find_related_bundle_patch_ids(bundle: dict[str, Any], contradiction: dict[str, Any]) -> list[str]:
    return find_related_patch_ids_for_targets(
        bundle.get("patches", []),
        target_ids=[
            contradiction.get("field_id", ""),
            contradiction.get("target_id", ""),
        ],
        node_ids=[contradiction.get("node_id", "")],
    )


def review_bundle_patches(
    bundle_id: str,
    patch_ids: list[str],
    decision: str,
    *,
    reviewed_by: str = "user",
    note: str = "",
) -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    if decision not in {"accepted", "rejected", "deferred"}:
        raise ValueError(f"Unsupported review decision: {decision}")
    normalized_ids = [patch_id for patch_id in dict.fromkeys(patch_ids) if patch_id]
    if not normalized_ids:
        raise ValueError("Provide at least one patch id for review.")
    reviewed_at = utc_timestamp()
    found_ids = apply_patch_review_decision(
        bundle,
        normalized_ids,
        decision,
        reviewed_by=reviewed_by,
        note=note,
        reviewed_at=reviewed_at,
    )
    missing = set(normalized_ids) - set(found_ids)
    if missing:
        raise ValueError(f"Patch not found: {sorted(missing)[0]}")
    sync_bundle_review_summary(
        bundle,
        reviewed_by=reviewed_by,
        note=note,
        reviewed_at=reviewed_at,
    )
    review = bundle.setdefault("review", {})
    if review.get("triage_state", "new") == "new":
        review["triage_state"] = "in_review"
    sync_bundle_reconciliation(bundle)
    normalized = save_bundle(bundle)
    return {"bundle": normalized, "updated_patch_ids": normalized_ids}


def review_bundle_contradiction(
    bundle_id: str,
    contradiction_id: str,
    decision: str,
    *,
    reviewed_by: str = "user",
    note: str = "",
) -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    if decision not in {"accepted", "rejected", "deferred"}:
        raise ValueError(f"Unsupported contradiction decision: {decision}")
    contradiction = next((item for item in bundle.get("contradictions", []) if item.get("id") == contradiction_id), None)
    if not contradiction:
        raise ValueError(f"Contradiction not found: {contradiction_id}")
    related_patch_ids = find_related_bundle_patch_ids(bundle, contradiction)
    reviewed_at = utc_timestamp()
    updated_patch_ids: list[str] = []
    if related_patch_ids:
        updated_patch_ids = apply_patch_review_decision(
            bundle,
            related_patch_ids,
            decision,
            reviewed_by=reviewed_by,
            note=note,
            reviewed_at=reviewed_at,
        )
    append_review_event(
        contradiction,
        decision=decision,
        reviewed_by=reviewed_by,
        note=note,
        reviewed_at=reviewed_at,
        related_patch_ids=related_patch_ids,
    )
    contradiction["review_required"] = decision == "deferred"
    sync_bundle_review_summary(
        bundle,
        reviewed_by=reviewed_by,
        note=note,
        reviewed_at=reviewed_at,
    )
    review = bundle.setdefault("review", {})
    if review.get("triage_state", "new") == "new":
        review["triage_state"] = "in_review"
    sync_bundle_reconciliation(bundle)
    normalized = save_bundle(bundle)
    return {
        "bundle": normalized,
        "updated_patch_ids": updated_patch_ids,
        "updated_contradiction_id": contradiction_id,
    }


def update_bundle_workflow(
    bundle_id: str,
    *,
    bundle_owner: str | None = None,
    assigned_reviewer: str | None = None,
    triage_state: str | None = None,
    triage_note: str | None = None,
    updated_by: str = "user",
    note: str = "",
) -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    review = bundle.setdefault("review", {})
    normalized_changes: dict[str, Any] = {}
    if triage_state is not None and triage_state not in {"new", "in_review", "blocked", "resolved"}:
        raise ValueError(f"Unsupported triage state: {triage_state}")
    for key, value in (
        ("bundle_owner", bundle_owner),
        ("assigned_reviewer", assigned_reviewer),
    ):
        if value is None:
            continue
        normalized_value = value.strip()
        if review.get(key, "") == normalized_value:
            continue
        review[key] = normalized_value
        normalized_changes[key] = normalized_value
    if triage_state is not None and review.get("triage_state", "new") != triage_state:
        review["triage_state"] = triage_state
        normalized_changes["triage_state"] = triage_state
    if triage_note is not None:
        normalized_note = triage_note.strip()
        if review.get("triage_note", "") != normalized_note:
            review["triage_note"] = normalized_note
            normalized_changes["triage_note"] = normalized_note
    updated_at = utc_timestamp()
    append_workflow_event(
        review,
        updated_by=updated_by,
        updated_at=updated_at,
        note=note or (triage_note or ""),
        changes=normalized_changes,
    )
    normalized = save_bundle(bundle)
    return {
        "bundle": normalized,
        "updated_fields": sorted(normalized_changes.keys()),
    }


def sorted_accepted_patches(patches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    accepted_patches = [patch for patch in patches if patch.get("review_state") == "accepted"]
    return sorted(
        accepted_patches,
        key=lambda item: (
            PATCH_TYPE_PRECEDENCE.get(item.get("type", ""), 999),
            item.get("target_id", ""),
            item.get("id", ""),
        ),
    )


def preview_merge_step(graph: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    payload = patch.get("payload", {}) or {}
    patch_type = patch.get("type", "")
    step = {
        "patch_id": patch.get("id", ""),
        "type": patch_type,
        "target_id": patch.get("target_id", ""),
        "node_id": patch.get("node_id", ""),
        "field_id": patch.get("field_id", ""),
        "summary": summarize_patch_for_reconciliation(patch),
        "status": "ready",
        "reason": "",
    }

    if patch_type == "add_node":
        node = payload.get("node")
        if not node:
            step["status"] = "blocked"
            step["reason"] = "Accepted add_node patch is missing node payload."
        elif any(candidate["id"] == node["id"] for candidate in graph.get("nodes", [])):
            step["status"] = "noop"
            step["reason"] = f"Node {node['id']} already exists in canonical YAML."
        return step

    if patch_type == "add_edge":
        edge = payload.get("edge")
        if not edge:
            step["status"] = "blocked"
            step["reason"] = "Accepted add_edge patch is missing edge payload."
            return step
        node_ids = {node.get("id") for node in graph.get("nodes", [])}
        if edge.get("source") not in node_ids or edge.get("target") not in node_ids:
            step["status"] = "blocked"
            step["reason"] = f"Accepted add_edge patch {patch['id']} depends on nodes that are not present in the merge graph."
        elif any(candidate["id"] == edge["id"] for candidate in graph.get("edges", [])):
            step["status"] = "noop"
            step["reason"] = f"Edge {edge['id']} already exists in canonical YAML."
        return step

    if patch_type == "add_field":
        node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == patch.get("node_id")), None)
        field = payload.get("field")
        if not node or not field:
            step["status"] = "blocked"
            step["reason"] = f"Accepted add_field patch {patch['id']} targets a node or field that is missing from the merge graph."
            return step
        target_list = node["contract"]["fields"] if node["kind"] == "contract" else node["columns"]
        if any(candidate["id"] == field["id"] for candidate in target_list):
            step["status"] = "noop"
            step["reason"] = f"Field {field['id']} already exists in canonical YAML."
            return step
        name_conflict = next((candidate for candidate in target_list if candidate.get("name") == field.get("name")), None)
        if name_conflict is not None:
            step["status"] = "blocked"
            step["reason"] = (
                f"Accepted add_field patch {patch['id']} would duplicate "
                f"{patch.get('node_id', '')}.{field.get('name', '')} in canonical YAML."
            )
        return step

    if patch_type in {"add_binding", "change_binding"}:
        node, field = resolve_field(graph, patch.get("field_id", ""))
        next_binding = payload.get("new_binding", "") or payload.get("primary_binding", "")
        next_alternatives = sorted(dict.fromkeys(payload.get("alternatives", []) or []))
        previous_binding = payload.get("previous_binding", "")
        if not node or not field:
            step["status"] = "blocked"
            step["reason"] = f"Accepted binding patch targets a field that is missing from canonical YAML ({patch.get('field_id', '')})."
            return step
        current_binding = field.get("primary_binding", "")
        current_alternatives = sorted(dict.fromkeys(field.get("alternatives", []) or []))
        if previous_binding and current_binding != previous_binding:
            step["status"] = "blocked"
            step["reason"] = (
                f"Accepted binding patch for {patch.get('field_id', '')} expected {previous_binding or 'unbound'}, "
                f"but canonical YAML currently has {current_binding or 'unbound'}."
            )
            return step
        if current_binding == next_binding and current_alternatives == next_alternatives:
            step["status"] = "noop"
            step["reason"] = f"Binding for {patch.get('field_id', '')} already matches canonical YAML."
        return step

    if patch_type == "confidence_change":
        _, field = resolve_field(graph, patch.get("field_id", ""))
        if not field:
            step["status"] = "blocked"
            step["reason"] = f"Accepted confidence patch targets a field that is missing from canonical YAML ({patch.get('field_id', '')})."
            return step
        if field.get("confidence", "medium") == payload.get("confidence", ""):
            step["status"] = "noop"
            step["reason"] = f"Confidence for {patch.get('field_id', '')} already matches canonical YAML."
        return step

    if patch_type == "remove_field":
        _, field = resolve_field(graph, patch.get("field_id", ""))
        if not field:
            step["status"] = "blocked"
            step["reason"] = f"Accepted remove_field patch targets a field that is missing from canonical YAML ({patch.get('field_id', '')})."
        elif field.get("removed"):
            step["status"] = "noop"
            step["reason"] = f"Field {patch.get('field_id', '')} is already removed in canonical YAML."
        return step

    if patch_type == "remove_node":
        node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == patch.get("node_id")), None)
        if not node:
            step["status"] = "blocked"
            step["reason"] = f"Accepted remove_node patch targets a node that is missing from canonical YAML ({patch.get('node_id', '')})."
        elif node.get("removed"):
            step["status"] = "noop"
            step["reason"] = f"Node {patch.get('node_id', '')} is already removed in canonical YAML."
        return step

    if patch_type == "remove_edge":
        edge = next((candidate for candidate in graph.get("edges", []) if candidate["id"] == patch.get("edge_id")), None)
        if edge is None:
            step["status"] = "blocked"
            step["reason"] = f"Accepted remove_edge patch targets an edge that is missing from canonical YAML ({patch.get('edge_id', '')})."
        elif edge.get("removed"):
            step["status"] = "noop"
            step["reason"] = f"Edge {patch.get('edge_id', '')} is already removed in canonical YAML."
        return step

    if patch_type == "contradiction":
        step["status"] = "noop"
        step["reason"] = "Contradiction records do not mutate canonical YAML during merge."
    return step


def prepare_merge_graph(
    canonical_graph: dict[str, Any],
    accepted_patches: list[dict[str, Any]],
    *,
    merged_by: str,
) -> dict[str, Any]:
    working_graph = deepcopy(canonical_graph)
    blockers: list[dict[str, Any]] = []
    merge_patch_ids: list[str] = []
    steps: list[dict[str, Any]] = []
    applied_count = 0
    noop_count = 0
    for patch in accepted_patches:
        merge_patch_ids.append(patch["id"])
        step = preview_merge_step(working_graph, patch)
        if step["status"] == "noop":
            noop_count += 1
            steps.append(step)
            continue
        if step["status"] == "blocked":
            blockers.append(
                {
                    "patch_id": patch["id"],
                    "target_id": patch.get("target_id", ""),
                    "type": patch.get("type", ""),
                    "reason": step["reason"] or f"Accepted patch {patch['id']} could not be merged deterministically.",
                }
            )
            steps.append(step)
            continue
        applied, reason = apply_patch_to_graph(working_graph, patch, merged_by=merged_by, strict=True)
        if applied:
            applied_count += 1
            steps.append(step)
            continue
        step["status"] = "blocked"
        step["reason"] = reason or f"Accepted patch {patch['id']} could not be merged deterministically."
        blockers.append(
            {
                "patch_id": patch["id"],
                "target_id": patch.get("target_id", ""),
                "type": patch.get("type", ""),
                "reason": step["reason"],
            }
        )
        steps.append(step)
    return {
        "graph": working_graph,
        "merge_patch_ids": merge_patch_ids,
        "blockers": blockers,
        "steps": steps,
        "applied_count": applied_count,
        "noop_count": noop_count,
    }


def build_merge_plan_artifact(
    bundle: dict[str, Any],
    *,
    canonical_graph: dict[str, Any] | None = None,
    merged_by: str = "merge-preview",
) -> dict[str, Any]:
    canonical_graph = canonical_graph or load_graph()
    current_version = int(canonical_graph.get("metadata", {}).get("structure_version", 1) or 1)
    accepted_patches = sorted_accepted_patches(bundle.get("patches", []))
    patch_ids = [patch["id"] for patch in accepted_patches]
    review = bundle.get("review", {})
    if review.get("superseded_by_bundle_id"):
        blockers = [
            {
                "type": "superseded_bundle",
                "reason": (
                    f"Bundle has been superseded by {review['superseded_by_bundle_id']} "
                    "and should not be merged."
                ),
            }
        ]
        return {
            "base_version": int(bundle.get("base_structure_version", 1) or 1),
            "current_version": current_version,
            "accepted_patch_count": len(accepted_patches),
            "merge_patch_ids": patch_ids,
            "step_count": 0,
            "applied_step_count": 0,
            "noop_count": 0,
            "blocked_step_count": len(blockers),
            "mergeable": False,
            "status": "superseded",
            "blockers": blockers,
            "steps": [],
        }
    staleness = build_bundle_staleness(bundle, canonical_graph=canonical_graph)
    if staleness["is_stale"]:
        return {
            "base_version": staleness["base_version"],
            "current_version": staleness["current_version"],
            "accepted_patch_count": len(accepted_patches),
            "merge_patch_ids": patch_ids,
            "step_count": 0,
            "applied_step_count": 0,
            "noop_count": 0,
            "blocked_step_count": len(staleness["merge_blockers"]),
            "mergeable": False,
            "status": "stale",
            "blockers": staleness["merge_blockers"],
            "steps": [],
        }
    merge_plan = prepare_merge_graph(canonical_graph, accepted_patches, merged_by=merged_by)
    status = "blocked" if merge_plan["blockers"] else "ready" if accepted_patches else "empty"
    return {
        "base_version": int(bundle.get("base_structure_version", 1) or 1),
        "current_version": current_version,
        "accepted_patch_count": len(accepted_patches),
        "merge_patch_ids": patch_ids,
        "step_count": len(merge_plan["steps"]),
        "applied_step_count": merge_plan["applied_count"],
        "noop_count": merge_plan["noop_count"],
        "blocked_step_count": len(merge_plan["blockers"]),
        "mergeable": bool(accepted_patches) and not merge_plan["blockers"],
        "status": status,
        "blockers": merge_plan["blockers"],
        "steps": merge_plan["steps"],
    }


def build_contradiction_cluster_summary(clusters: list[dict[str, Any]]) -> dict[str, Any]:
    resolution_state_counts = {"pending": 0, "accepted": 0, "rejected": 0, "deferred": 0, "mixed": 0}
    open_count = 0
    resolved_count = 0
    high_risk_open_count = 0
    for cluster in clusters:
        state = cluster.get("resolution_state", "") or "pending"
        resolution_state_counts.setdefault(state, 0)
        resolution_state_counts[state] += 1
        if cluster.get("resolved", False):
            resolved_count += 1
        else:
            open_count += 1
            if cluster.get("highest_severity", "") == "high":
                high_risk_open_count += 1
    return {
        "count": len(clusters),
        "open_count": open_count,
        "resolved_count": resolved_count,
        "high_risk_open_count": high_risk_open_count,
        "resolution_state_counts": resolution_state_counts,
    }


def build_bundle_staleness(bundle: dict[str, Any], *, canonical_graph: dict[str, Any] | None = None) -> dict[str, Any]:
    canonical_graph = canonical_graph or load_graph()
    base_version = int(bundle.get("base_structure_version", 1) or 1)
    current_version = int(canonical_graph.get("metadata", {}).get("structure_version", 1) or 1)
    if current_version == base_version:
        return {
            "is_stale": False,
            "base_version": base_version,
            "current_version": current_version,
            "merge_blockers": [],
        }
    return {
        "is_stale": True,
        "base_version": base_version,
        "current_version": current_version,
        "merge_blockers": [
            {
                "type": "stale_bundle",
                "reason": (
                    f"Bundle targets structure version {base_version}, "
                    f"but canonical YAML is now version {current_version}."
                ),
            }
        ],
    }


def merge_bundle(bundle_id: str, *, merged_by: str = "user") -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    canonical_graph = load_graph()
    review = bundle.setdefault("review", {})
    if review.get("superseded_by_bundle_id"):
        review["merge_status"] = "superseded"
        review["rebase_required"] = False
        review["merge_blockers"] = [
            {
                "type": "superseded_bundle",
                "reason": (
                    f"Bundle has been superseded by {review['superseded_by_bundle_id']} "
                    "and should not be merged."
                ),
            }
        ]
        review["merge_patch_ids"] = [
            patch["id"]
            for patch in sorted_accepted_patches(bundle.get("patches", []))
        ]
        review["merge_plan"] = build_merge_plan_artifact(bundle, canonical_graph=canonical_graph, merged_by=merged_by)
        save_bundle(bundle)
        raise ValueError(review["merge_blockers"][0]["reason"])
    staleness = build_bundle_staleness(bundle, canonical_graph=canonical_graph)
    if staleness["is_stale"]:
        review["merge_status"] = "stale"
        review["rebase_required"] = True
        review["merge_blockers"] = staleness["merge_blockers"]
        review["merge_patch_ids"] = [
            patch["id"]
            for patch in sorted_accepted_patches(bundle.get("patches", []))
        ]
        review["merge_plan"] = build_merge_plan_artifact(bundle, canonical_graph=canonical_graph, merged_by=merged_by)
        save_bundle(bundle)
        raise ValueError("Bundle is stale and must be regenerated before merge.")

    accepted_patches = sorted_accepted_patches(bundle.get("patches", []))
    merge_plan = prepare_merge_graph(canonical_graph, accepted_patches, merged_by=merged_by)
    review["merge_patch_ids"] = merge_plan["merge_patch_ids"]
    review["merge_plan"] = {
        "base_version": int(bundle.get("base_structure_version", 1) or 1),
        "current_version": int(canonical_graph.get("metadata", {}).get("structure_version", 1) or 1),
        "accepted_patch_count": len(accepted_patches),
        "merge_patch_ids": merge_plan["merge_patch_ids"],
        "step_count": len(merge_plan["steps"]),
        "applied_step_count": merge_plan["applied_count"],
        "noop_count": merge_plan["noop_count"],
        "blocked_step_count": len(merge_plan["blockers"]),
        "mergeable": bool(accepted_patches) and not merge_plan["blockers"],
        "status": "blocked" if merge_plan["blockers"] else "ready" if accepted_patches else "empty",
        "blockers": merge_plan["blockers"],
        "steps": merge_plan["steps"],
    }
    if merge_plan["blockers"]:
        review["merge_status"] = "blocked"
        review["rebase_required"] = False
        review["merge_blockers"] = merge_plan["blockers"]
        save_bundle(bundle)
        raise ValueError(merge_plan["blockers"][0]["reason"])

    merged_graph = merge_plan["graph"]
    merged_graph = normalize_graph(merged_graph)
    merged_graph = save_graph(merged_graph, updated_by=merged_by, increment_version=True)
    review["merged_at"] = utc_timestamp()
    review["merged_by"] = merged_by
    review["merge_status"] = "merged"
    review["rebase_required"] = False
    review["merge_blockers"] = []
    review["triage_state"] = "resolved"
    review["merge_plan"] = {
        **review.get("merge_plan", {}),
        "mergeable": False,
        "status": "merged",
        "merged_by": merged_by,
        "merged_structure_version": int(merged_graph.get("metadata", {}).get("structure_version", 1) or 1),
    }
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


def collect_bundle_review_targets(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    observed_graph = bundle.get("observed") or {"metadata": {"name": "Observed"}, "nodes": [], "edges": []}
    index = build_index(observed_graph)
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for contradiction in bundle.get("contradictions", []):
        if contradiction.get("review_required") is False:
            continue
        target_id = contradiction.get("field_id") or contradiction.get("target_id") or contradiction.get("id", "")
        if not target_id:
            continue
        label = display_ref_for_field_id(target_id, observed_graph, index) if target_id.startswith("field.") else target_id
        key = ("contradiction", label)
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "kind": "contradiction",
                "label": label,
                "message": contradiction.get("message", "") or label,
                "severity": contradiction.get("severity", "") or "medium",
            }
        )
    for patch in bundle.get("patches", []):
        if patch.get("review_state") != "pending":
            continue
        target_id = patch.get("field_id") or patch.get("target_id") or patch.get("node_id") or patch.get("id", "")
        if not target_id:
            continue
        label = display_ref_for_field_id(target_id, observed_graph, index) if target_id.startswith("field.") else target_id
        key = ("patch", label)
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "kind": "patch",
                "label": label,
                "message": summarize_patch_for_reconciliation(patch),
                "severity": "high" if patch.get("type") in {"change_binding", "remove_field", "remove_binding"} else "medium",
            }
        )
    return sorted(items, key=lambda item: (item.get("kind", ""), item.get("label", "")))


def build_rebase_preview_summary(
    source_bundle: dict[str, Any],
    preview_bundle: dict[str, Any],
    *,
    transferred_review_count: int = 0,
    preserved_review_states: dict[str, int] | None = None,
    dropped_review_states: dict[str, int] | None = None,
    preserved_review_units: list[dict[str, Any]] | None = None,
    dropped_review_units: list[dict[str, Any]] | None = None,
    current_version: int | None = None,
) -> dict[str, Any]:
    preview_review = preview_bundle.get("review", {})
    preview_pending_count = sum(1 for patch in preview_bundle.get("patches", []) if patch.get("review_state") == "pending")
    preview_accepted_count = sum(1 for patch in preview_bundle.get("patches", []) if patch.get("review_state") == "accepted")
    preview_deferred_count = sum(1 for patch in preview_bundle.get("patches", []) if patch.get("review_state") == "deferred")
    preview_review_required_count = sum(1 for item in preview_bundle.get("contradictions", []) if item.get("review_required", True))
    changed_targets = collect_bundle_review_targets(preview_bundle)
    dropped_review_states = dropped_review_states or {"accepted": 0, "rejected": 0, "deferred": 0}
    preserved_review_states = preserved_review_states or {"accepted": 0, "rejected": 0, "deferred": 0}
    return {
        "base_version": int(source_bundle.get("base_structure_version", 1) or 1),
        "current_version": current_version if current_version is not None else int(load_graph().get("metadata", {}).get("structure_version", 1) or 1),
        "preview_bundle_id": preview_bundle.get("bundle_id", ""),
        "transferred_review_count": transferred_review_count,
        "preserved_review_states": preserved_review_states,
        "preserved_review_units": (preserved_review_units or [])[:8],
        "dropped_review_count": sum(dropped_review_states.values()),
        "dropped_review_states": dropped_review_states,
        "dropped_review_units": (dropped_review_units or [])[:8],
        "pending_patch_count": preview_pending_count,
        "accepted_patch_count": preview_accepted_count,
        "deferred_patch_count": preview_deferred_count,
        "review_required_count": preview_review_required_count,
        "changed_target_count": len(changed_targets),
        "merge_plan_status": preview_review.get("merge_plan", {}).get("status", ""),
        "merge_plan_noop_count": int(preview_review.get("merge_plan", {}).get("noop_count", 0) or 0),
        "merge_plan_blocked_step_count": int(preview_review.get("merge_plan", {}).get("blocked_step_count", 0) or 0),
        "ready_to_merge": (
            bool(preview_review.get("merge_patch_ids"))
            and preview_pending_count == 0
            and preview_review_required_count == 0
            and preview_review.get("merge_plan", {}).get("status", "") in {"ready", "empty"}
        ),
        "changed_targets": changed_targets[:8],
    }


def preview_rebase_bundle(bundle_id: str, *, preserve_reviews: bool = True) -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    scan = bundle.get("scan", {})
    root_dir = Path(scan.get("root_path") or ".").expanduser()
    if not root_dir.is_absolute():
        root_dir = root_dir.resolve()
    if not root_dir.exists() or not root_dir.is_dir():
        raise ValueError(f"Project root not found for rebasing bundle: {root_dir}")

    result = scan_structure(
        root_dir=root_dir,
        role=scan.get("role", "scout"),
        scope=scan.get("scope", "full"),
        include_tests=bool(scan.get("include_tests", False)),
        include_internal=bool(scan.get("include_internal", True)),
        doc_paths=scan.get("doc_paths", []),
        selected_paths=scan.get("selected_paths", []),
    )
    preview_bundle = result["bundle"]
    transferred_review_count = 0
    preserved_review_states = {"accepted": 0, "rejected": 0, "deferred": 0}
    dropped_review_states = {"accepted": 0, "rejected": 0, "deferred": 0}
    preserved_review_units: list[dict[str, Any]] = []
    dropped_review_units: list[dict[str, Any]] = []
    if preserve_reviews:
        transfer = transfer_bundle_review_states(bundle, deepcopy(preview_bundle))
        preview_bundle = transfer["bundle"]
        transferred_review_count = transfer["transferred_review_count"]
        preserved_review_states = transfer["preserved_review_states"]
        dropped_review_states = transfer["dropped_review_states"]
        preserved_review_units = transfer["preserved_review_units"]
        dropped_review_units = transfer["dropped_review_units"]
    preview = build_rebase_preview_summary(
        bundle,
        preview_bundle,
        transferred_review_count=transferred_review_count,
        preserved_review_states=preserved_review_states,
        dropped_review_states=dropped_review_states,
        preserved_review_units=preserved_review_units,
        dropped_review_units=dropped_review_units,
    )
    return {"preview": preview}


def rebase_bundle(bundle_id: str, *, preserve_reviews: bool = True) -> dict[str, Any]:
    bundle = load_bundle(bundle_id)
    if not bundle:
        raise ValueError(f"Bundle not found: {bundle_id}")
    scan = bundle.get("scan", {})
    root_dir = Path(scan.get("root_path") or ".").expanduser()
    if not root_dir.is_absolute():
        root_dir = root_dir.resolve()
    if not root_dir.exists() or not root_dir.is_dir():
        raise ValueError(f"Project root not found for rebasing bundle: {root_dir}")

    result = scan_structure(
        root_dir=root_dir,
        role=scan.get("role", "scout"),
        scope=scan.get("scope", "full"),
        include_tests=bool(scan.get("include_tests", False)),
        include_internal=bool(scan.get("include_internal", True)),
        doc_paths=scan.get("doc_paths", []),
        selected_paths=scan.get("selected_paths", []),
    )
    rebased_bundle = result["bundle"]
    transferred_review_count = 0
    preserved_review_states = {"accepted": 0, "rejected": 0, "deferred": 0}
    dropped_review_states = {"accepted": 0, "rejected": 0, "deferred": 0}
    preserved_review_units: list[dict[str, Any]] = []
    dropped_review_units: list[dict[str, Any]] = []

    if preserve_reviews:
        transfer = transfer_bundle_review_states(bundle, rebased_bundle)
        rebased_bundle = transfer["bundle"]
        transferred_review_count = transfer["transferred_review_count"]
        preserved_review_states = transfer["preserved_review_states"]
        dropped_review_states = transfer["dropped_review_states"]
        preserved_review_units = transfer["preserved_review_units"]
        dropped_review_units = transfer["dropped_review_units"]
        if transfer["updated"]:
            rebased_bundle = save_bundle(rebased_bundle)
            result["bundle"] = rebased_bundle
            result["structure"] = build_structure_summary(load_graph())

    rebased_bundle.setdefault("review", {})["last_rebase_summary"] = build_rebase_preview_summary(
        bundle,
        rebased_bundle,
        transferred_review_count=transferred_review_count,
        preserved_review_states=preserved_review_states,
        dropped_review_states=dropped_review_states,
        preserved_review_units=preserved_review_units,
        dropped_review_units=dropped_review_units,
    )
    rebased_at = utc_timestamp()
    rebased_review = rebased_bundle.setdefault("review", {})
    rebased_review["rebased_from_bundle_id"] = bundle_id
    rebased_review["superseded_by_bundle_id"] = ""
    rebased_review["superseded_at"] = ""
    rebased_review["superseded_by"] = ""
    rebased_review["last_rebase_summary"]["rebased_from_bundle_id"] = bundle_id
    rebased_review["last_rebase_summary"]["rebased_at"] = rebased_at
    sync_bundle_review_summary(rebased_bundle)
    sync_bundle_reconciliation(rebased_bundle)
    rebased_bundle = save_bundle(rebased_bundle)
    result["bundle"] = rebased_bundle
    result["structure"] = build_structure_summary(load_graph())

    if rebased_bundle["bundle_id"] != bundle_id:
        source_review = bundle.setdefault("review", {})
        source_review["superseded_by_bundle_id"] = rebased_bundle["bundle_id"]
        source_review["superseded_at"] = rebased_at
        source_review["superseded_by"] = "rebase"
        source_review["merge_status"] = "superseded"
        source_review["rebase_required"] = False
        source_review["merge_blockers"] = [
            {
                "type": "superseded_bundle",
                "reason": (
                    f"Bundle has been superseded by {rebased_bundle['bundle_id']} "
                    "and should not be merged."
                ),
            }
        ]
        source_review["last_rebase_summary"] = deepcopy(rebased_review["last_rebase_summary"])
        save_bundle(bundle)

    return {
        **result,
        "bundle": rebased_bundle,
        "rebased_from_bundle_id": bundle_id,
        "transferred_review_count": transferred_review_count,
        "preserved_review_states": preserved_review_states,
        "dropped_review_count": sum(dropped_review_states.values()),
        "dropped_review_states": dropped_review_states,
    }


def transfer_bundle_review_states(source_bundle: dict[str, Any], target_bundle: dict[str, Any]) -> dict[str, Any]:
    decisions_by_key: dict[str, str] = {}
    source_patch_descriptions: dict[str, dict[str, Any]] = {}
    source_reviewed_count = {"accepted": 0, "rejected": 0, "deferred": 0}
    for patch in source_bundle.get("patches", []):
        decision = patch.get("review_state", "pending")
        if decision == "pending":
            continue
        if decision in source_reviewed_count:
            source_reviewed_count[decision] += 1
        key = build_patch_review_transfer_key(source_bundle, patch)
        if key and key not in decisions_by_key:
            decisions_by_key[key] = decision
            source_patch_descriptions[key] = describe_patch_review_unit(source_bundle, patch)

    transferred_review_count = 0
    preserved_review_states = {"accepted": 0, "rejected": 0, "deferred": 0}
    dropped_review_states = {"accepted": 0, "rejected": 0, "deferred": 0}
    preserved_review_units: list[dict[str, Any]] = []
    preserved_keys: set[str] = set()
    updated = False
    for patch in target_bundle.get("patches", []):
        if patch.get("review_state") != "pending":
            continue
        key = build_patch_review_transfer_key(target_bundle, patch)
        decision = decisions_by_key.get(key, "")
        if decision not in preserved_review_states:
            continue
        patch["review_state"] = decision
        preserved_review_states[decision] += 1
        transferred_review_count += 1
        updated = True
        if key in source_patch_descriptions and key not in preserved_keys:
            preserved_keys.add(key)
            preserved_review_units.append(
                {
                    **source_patch_descriptions[key],
                    "source_patch_id": source_patch_descriptions[key].get("patch_id", ""),
                    "source_review_slug": source_patch_descriptions[key].get("review_slug", ""),
                    "target_patch_id": patch.get("id", ""),
                    "target_review_slug": identifierify(
                        patch.get("field_id") or patch.get("target_id") or patch.get("node_id") or patch.get("id") or "review"
                    ),
                    "decision": decision,
                }
            )

    review = target_bundle.setdefault("review", {})
    review["merge_status"] = ""
    review["rebase_required"] = False
    review["merge_blockers"] = []
    review["rebased_from_bundle_id"] = source_bundle.get("bundle_id", "")
    review["superseded_by_bundle_id"] = ""
    review["superseded_at"] = ""
    review["superseded_by"] = ""
    copy_bundle_workflow_state(source_bundle, target_bundle)
    sync_bundle_review_summary(target_bundle)
    sync_bundle_reconciliation(target_bundle)
    for decision, count in source_reviewed_count.items():
        dropped_review_states[decision] = max(count - preserved_review_states[decision], 0)
    dropped_review_units = [
        {
            **description,
            "decision": decisions_by_key.get(key, ""),
        }
        for key, description in source_patch_descriptions.items()
        if key not in preserved_keys
    ]
    return {
        "bundle": target_bundle,
        "transferred_review_count": transferred_review_count,
        "preserved_review_states": preserved_review_states,
        "dropped_review_states": dropped_review_states,
        "preserved_review_units": preserved_review_units,
        "dropped_review_units": dropped_review_units,
        "updated": updated,
    }


def build_patch_review_transfer_key(bundle: dict[str, Any], patch: dict[str, Any]) -> str:
    observed_graph = bundle.get("observed") or {"metadata": {"name": "Observed"}, "nodes": [], "edges": []}
    index = build_index(observed_graph)
    contexts = [(observed_graph, index)]
    payload = patch.get("payload", {}) or {}
    patch_type = patch.get("type", "")
    node = resolve_patch_node(index, patch, payload)
    node_key = review_key_for_node(node)

    if patch_type in {"add_node", "remove_node"}:
        return json.dumps(
            {
                "type": patch_type,
                "node": node_key,
                "label": (payload.get("node", {}) or {}).get("label", node.get("label", "") if node else ""),
            },
            sort_keys=True,
        )

    if patch_type in {"add_field", "remove_field", "confidence_change"}:
        return json.dumps(
            {
                "type": patch_type,
                "node": node_key,
                "field": resolve_patch_field_name(index, patch, payload),
                "confidence": payload.get("confidence", ""),
            },
            sort_keys=True,
        )

    if patch_type in {"add_binding", "change_binding", "remove_binding"}:
        current_binding = normalize_reference_key(
            payload.get("new_binding", "") or payload.get("primary_binding", ""),
            contexts,
        )
        previous_binding = normalize_reference_key(payload.get("previous_binding", ""), contexts)
        return json.dumps(
            {
                "type": patch_type,
                "node": node_key,
                "field": resolve_patch_field_name(index, patch, payload),
                "current_binding": current_binding,
                "previous_binding": previous_binding,
            },
            sort_keys=True,
        )

    if patch_type in {"add_edge", "remove_edge"}:
        edge = payload.get("edge") or index.get("edges", {}).get(patch.get("edge_id", ""))
        if edge:
            mappings = sorted(
                (
                    normalize_reference_key(
                        mapping.get("source_field_id")
                        or column_ref(edge.get("source", ""), mapping.get("source_column", "")),
                        contexts,
                    ),
                    normalize_reference_key(
                        mapping.get("target_field_id")
                        or column_ref(edge.get("target", ""), mapping.get("target_column", "")),
                        contexts,
                    ),
                )
                for mapping in edge.get("column_mappings", []) or []
            )
            return json.dumps(
                {
                    "type": patch_type,
                    "edge_type": edge.get("type", ""),
                    "source": review_key_for_node(index.get("nodes", {}).get(edge.get("source", ""))),
                    "target": review_key_for_node(index.get("nodes", {}).get(edge.get("target", ""))),
                    "mappings": mappings,
                },
                sort_keys=True,
            )

    return json.dumps(
        {
            "type": patch_type,
            "target_id": patch.get("target_id", ""),
            "node": node_key,
            "field": resolve_patch_field_name(index, patch, payload),
        },
        sort_keys=True,
    )


def resolve_patch_node(index: dict[str, Any], patch: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any] | None:
    node_id = patch.get("node_id", "")
    if not node_id and patch.get("field_id"):
        node_id = index.get("field_owner", {}).get(patch.get("field_id", ""), "")
    if node_id:
        node = index.get("nodes", {}).get(node_id)
        if node:
            return node
    if payload.get("node"):
        return payload["node"]
    if payload.get("edge"):
        return index.get("nodes", {}).get(payload["edge"].get("target", ""))
    return None


def review_key_for_node(node: dict[str, Any] | None) -> str:
    if not node:
        return ""
    if node.get("kind") == "contract" and node.get("extension_type") == "api":
        return f"api:{node.get('contract', {}).get('route', '') or node.get('label', '')}"
    if node.get("kind") == "contract" and node.get("extension_type") == "ui":
        return f"ui:{node.get('contract', {}).get('component', '') or node.get('label', '')}"
    relation = extract_sql_relation_tag(node)
    if relation:
        return f"{node.get('kind', 'node')}:{relation}"
    data_path = node.get("data", {}).get("profile_target") or node.get("data", {}).get("local_path") or ""
    if data_path:
        return f"path:{data_path}"
    return node.get("id", "")


def resolve_patch_field_name(index: dict[str, Any], patch: dict[str, Any], payload: dict[str, Any]) -> str:
    field = payload.get("field", {}) or {}
    if field.get("name"):
        return field["name"]
    return index.get("field_name_by_id", {}).get(patch.get("field_id", ""), "")


def describe_patch_review_unit(bundle: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    observed_graph = bundle.get("observed") or {"metadata": {"name": "Observed"}, "nodes": [], "edges": []}
    index = build_index(observed_graph)
    payload = patch.get("payload", {}) or {}
    node = resolve_patch_node(index, patch, payload)
    field_name = resolve_patch_field_name(index, patch, payload)
    label = node.get("label", "") if node else ""
    if field_name:
        label = f"{label}.{field_name}" if label else field_name
    if not label and patch.get("field_id", ""):
        label = display_ref_for_field_id(patch["field_id"], observed_graph, index)
    if not label and patch.get("target_id", ""):
        label = patch["target_id"]
    if not label:
        label = patch.get("id", "")
    return {
        "patch_id": patch.get("id", ""),
        "target_id": patch.get("target_id", ""),
        "field_id": patch.get("field_id", ""),
        "node_id": patch.get("node_id", ""),
        "review_slug": identifierify(
            patch.get("field_id") or patch.get("target_id") or patch.get("node_id") or patch.get("id") or "review"
        ),
        "label": label,
        "type": patch.get("type", ""),
        "summary": summarize_patch_for_reconciliation(patch),
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
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
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
    implementation_doc_candidates = [candidate for candidate in doc_candidates if candidate.get("type") != "partial_graph"]

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
        object_type = hint.get("object_type")
        if object_type in {"table", "view", "materialized_view"}:
            apply_sql_structure_observation(
                observed_graph,
                canonical_index=canonical_index,
                hint=hint,
                patches=patches,
                contradictions=contradictions,
                impacts=impacts,
                role=role,
            )
            continue
        if object_type == "query_projection":
            apply_orm_query_projection_observation(
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

    for candidate in implementation_doc_candidates:
        apply_document_candidate(
            observed_graph,
            canonical_index=canonical_index,
            candidate=candidate,
            patches=patches,
            contradictions=contradictions,
            impacts=impacts,
            role=role,
        )

    for candidate in doc_candidates:
        if candidate.get("type") != "partial_graph":
            continue
        apply_partial_graph_candidate(
            observed_graph,
            candidate=candidate,
            patches=patches,
            role=role,
        )

    hybrid_context = build_hybrid_plan_comparison(
        canonical_graph,
        profile=profile,
        doc_candidates=doc_candidates,
    )
    contradictions.extend(hybrid_context.get("contradictions", []))
    impacts.extend(hybrid_context.get("impacts", []))

    detect_missing_confirmed_structure(
        canonical_graph,
        observed_routes=collect_observed_routes(profile, implementation_doc_candidates),
        contradictions=contradictions,
        impacts=impacts,
        scope=scope,
    )

    deduped_patches = dedupe_patch_rows(patches)
    deduped_contradictions = dedupe_contradictions(contradictions)
    deduped_impacts = dedupe_impacts(impacts)
    observed_graph = normalize_graph(observed_graph)
    hybrid_context["contradictions"] = dedupe_contradictions(hybrid_context.get("contradictions", []))
    hybrid_context["impacts"] = dedupe_impacts(hybrid_context.get("impacts", []))
    return observed_graph, deduped_patches, deduped_contradictions, deduped_impacts, hybrid_context


def build_reconciliation_summary(
    canonical_graph: dict[str, Any],
    *,
    profile: dict[str, Any],
    doc_candidates: list[dict[str, Any]],
    patches: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    scope: str,
    hybrid_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_index = build_index(canonical_graph)
    hybrid_context = hybrid_context or build_empty_hybrid_context()
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

    planned_missing.extend(hybrid_context.get("planned_missing", []))
    observed_untracked.extend(hybrid_context.get("observed_untracked", []))
    uncertain_matches.extend(hybrid_context.get("uncertain_matches", []))

    planned_missing = dedupe_reconciliation_items(planned_missing)
    observed_untracked = dedupe_reconciliation_items(observed_untracked)
    implemented_differently = dedupe_reconciliation_items(implemented_differently)
    uncertain_matches = dedupe_reconciliation_items(uncertain_matches)
    contradiction_clusters = build_contradiction_clusters(contradictions, patches)

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
        "comparison": hybrid_context.get("summary", {}),
        "field_matrix": dedupe_field_matrix_rows(hybrid_context.get("field_matrix", [])),
        "field_matrix_summary": build_field_matrix_summary(hybrid_context.get("field_matrix", [])),
        "contradiction_summary": build_contradiction_summary(contradictions),
        "contradiction_clusters": contradiction_clusters,
        "contradiction_cluster_summary": build_contradiction_cluster_summary(contradiction_clusters),
        "downstream_breakage": summarize_downstream_breakage(contradictions, impacts),
    }


def build_empty_hybrid_context() -> dict[str, Any]:
    return {
        "summary": {
            "plan_candidates": 0,
            "plan_paths": [],
            "planned_nodes": 0,
            "matched_nodes": 0,
            "planned_fields": 0,
            "matched_fields": 0,
            "planned_bindings": 0,
            "matched_bindings": 0,
            "binding_mismatches": 0,
            "column_mismatches": 0,
            "missing_fields": 0,
            "unplanned_fields": 0,
        },
        "planned_missing": [],
        "observed_untracked": [],
        "uncertain_matches": [],
        "field_matrix": [],
        "field_matrix_summary": {
            "count": 0,
            "review_required_count": 0,
            "unresolved_count": 0,
            "implemented_differently_count": 0,
            "status_counts": {},
        },
        "contradictions": [],
        "impacts": [],
    }


def field_matrix_status_priority(status: str) -> int:
    return {
        "binding_mismatch": 0,
        "lineage_mismatch": 1,
        "column_type_mismatch": 2,
        "planned_missing": 3,
        "observed_unplanned": 4,
        "uncertain": 5,
        "matched": 6,
    }.get(status, 999)


def build_field_matrix_row(
    *,
    scope: str,
    node_id: str,
    node_ref: str,
    field_name: str,
    target_id: str,
    field_id: str = "",
    status: str,
    planned: bool,
    observed: bool,
    why_this_matters: str,
    evidence_sources: list[str] | set[str] | None = None,
    issues: list[str] | None = None,
    planned_binding: str = "",
    observed_bindings: list[str] | set[str] | None = None,
    planned_type: str = "",
    observed_type: str = "",
    planned_lineage: list[str] | set[str] | None = None,
    observed_lineage: list[str] | set[str] | None = None,
) -> dict[str, Any]:
    row_issues = sorted(dict.fromkeys(issues or ([] if status == "matched" else [status])))
    return {
        "row_id": f"{scope}:{slugify_text(node_id or node_ref or scope)}:{slugify_text(field_name or 'field')}",
        "scope": scope,
        "node_id": node_id,
        "node_ref": node_ref,
        "target_id": target_id or field_id or node_id,
        "field_id": field_id or "",
        "field_name": field_name,
        "subject": ".".join(value for value in (node_ref, field_name) if value),
        "status": status,
        "issues": row_issues,
        "planned": bool(planned),
        "observed": bool(observed),
        "review_required": status != "matched",
        "why_this_matters": why_this_matters,
        "evidence_sources": sorted(dict.fromkeys(evidence_sources or [])),
        "planned_binding": planned_binding,
        "observed_bindings": sorted(dict.fromkeys(observed_bindings or [])),
        "planned_type": planned_type,
        "observed_type": observed_type,
        "planned_lineage": sorted(dict.fromkeys(planned_lineage or [])),
        "observed_lineage": sorted(dict.fromkeys(observed_lineage or [])),
    }


def dedupe_field_matrix_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = row.get("row_id", "")
        if not row_id:
            continue
        current = deduped.get(row_id)
        if current is None:
            deduped[row_id] = deepcopy(row)
            continue
        if field_matrix_status_priority(row.get("status", "")) < field_matrix_status_priority(current.get("status", "")):
            preferred = row
            fallback = current
        else:
            preferred = current
            fallback = row
        merged = deepcopy(preferred)
        merged["issues"] = sorted(dict.fromkeys([*preferred.get("issues", []), *fallback.get("issues", [])]))
        merged["evidence_sources"] = sorted(
            dict.fromkeys([*preferred.get("evidence_sources", []), *fallback.get("evidence_sources", [])])
        )
        merged["observed_bindings"] = sorted(
            dict.fromkeys([*preferred.get("observed_bindings", []), *fallback.get("observed_bindings", [])])
        )
        merged["planned_lineage"] = sorted(
            dict.fromkeys([*preferred.get("planned_lineage", []), *fallback.get("planned_lineage", [])])
        )
        merged["observed_lineage"] = sorted(
            dict.fromkeys([*preferred.get("observed_lineage", []), *fallback.get("observed_lineage", [])])
        )
        merged["review_required"] = bool(preferred.get("review_required") or fallback.get("review_required"))
        for key in (
            "target_id",
            "field_id",
            "node_id",
            "node_ref",
            "field_name",
            "subject",
            "planned_binding",
            "planned_type",
            "observed_type",
            "why_this_matters",
        ):
            if not merged.get(key) and fallback.get(key):
                merged[key] = fallback[key]
        deduped[row_id] = merged
    return sorted(
        deduped.values(),
        key=lambda item: (
            item.get("scope", ""),
            item.get("node_ref", ""),
            item.get("field_name", ""),
            item.get("row_id", ""),
        ),
    )


def build_field_matrix_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_rows = dedupe_field_matrix_rows(rows)
    status_counts = {
        "matched": 0,
        "planned_missing": 0,
        "observed_unplanned": 0,
        "binding_mismatch": 0,
        "lineage_mismatch": 0,
        "column_type_mismatch": 0,
        "uncertain": 0,
    }
    review_required_count = 0
    unresolved_count = 0
    implemented_differently_count = 0
    for row in normalized_rows:
        status = row.get("status", "") or "matched"
        status_counts.setdefault(status, 0)
        status_counts[status] += 1
        if row.get("review_required", False):
            review_required_count += 1
        if status != "matched":
            unresolved_count += 1
        if status in {"binding_mismatch", "lineage_mismatch", "column_type_mismatch"}:
            implemented_differently_count += 1
    return {
        "count": len(normalized_rows),
        "review_required_count": review_required_count,
        "unresolved_count": unresolved_count,
        "implemented_differently_count": implemented_differently_count,
        "status_counts": status_counts,
    }


def resolve_hybrid_field_target_id(
    canonical_index: dict[str, Any],
    *,
    node_id: str,
    field_name: str,
    explicit_field_id: str = "",
    is_contract: bool,
) -> str:
    if explicit_field_id:
        return explicit_field_id
    node = canonical_index.get("nodes", {}).get(node_id, {})
    if node:
        if is_contract:
            for field in node.get("contract", {}).get("fields", []):
                if field.get("name") == field_name and field.get("id"):
                    return field["id"]
        else:
            for column in node.get("columns", []):
                if column.get("name") == field_name and column.get("id"):
                    return column["id"]
    return column_ref(node_id, field_name)


def normalize_planned_contract_bindings(
    field: dict[str, Any],
    reference_contexts: list[tuple[dict[str, Any], dict[str, Any]]],
) -> list[str]:
    binding_refs = [reference for reference in [field.get("primary_binding", ""), *field.get("alternatives", [])] if reference]
    if not binding_refs:
        binding_refs = plan_sources_to_binding_refs(field.get("sources", []))
    normalized: list[str] = []
    for reference in binding_refs:
        resolved = normalize_reference_key(reference, reference_contexts)
        if resolved and resolved not in normalized:
            normalized.append(resolved)
    return normalized


def build_field_matrix_from_hybrid(
    canonical_graph: dict[str, Any],
    plan_graph: dict[str, Any],
    *,
    api_evidence: dict[str, dict[str, Any]],
    ui_evidence: dict[str, dict[str, Any]],
    relation_evidence: dict[str, dict[str, Any]],
    reference_contexts: list[tuple[dict[str, Any], dict[str, Any]]],
) -> list[dict[str, Any]]:
    canonical_index = build_index(canonical_graph)
    rows: list[dict[str, Any]] = []
    planned_api_routes: set[str] = set()
    planned_ui_components: set[str] = set()
    planned_relations: set[str] = set()

    for node in active_nodes(plan_graph):
        if node["kind"] == "contract" and node.get("extension_type") == "api":
            route = node.get("contract", {}).get("route", "")
            if not route:
                continue
            planned_api_routes.add(route)
            evidence = api_evidence.get(route)
            plan_fields = {field["name"]: field for field in node.get("contract", {}).get("fields", []) if not field.get("removed")}
            if not evidence:
                for field_name, field in sorted(plan_fields.items()):
                    field_target_id = resolve_hybrid_field_target_id(
                        canonical_index,
                        node_id=node["id"],
                        field_name=field_name,
                        explicit_field_id=field.get("id", ""),
                        is_contract=True,
                    )
                    rows.append(
                        build_field_matrix_row(
                            scope="api",
                            node_id=node["id"],
                            node_ref=route,
                            field_name=field_name,
                            target_id=field_target_id,
                            field_id=field.get("id", ""),
                            status="planned_missing",
                            planned=True,
                            observed=False,
                            why_this_matters="Column-level contract memory is missing a planned response field.",
                            evidence_sources=["plan_yaml"],
                        )
                    )
                continue
            impl_field_names = set(evidence.get("fields", set()))
            for field_name, field in sorted(plan_fields.items()):
                field_target_id = resolve_hybrid_field_target_id(
                    canonical_index,
                    node_id=node["id"],
                    field_name=field_name,
                    explicit_field_id=field.get("id", ""),
                    is_contract=True,
                )
                if field_name not in impl_field_names:
                    rows.append(
                        build_field_matrix_row(
                            scope="api",
                            node_id=node["id"],
                            node_ref=route,
                            field_name=field_name,
                            target_id=field_target_id,
                            field_id=field.get("id", ""),
                            status="planned_missing",
                            planned=True,
                            observed=False,
                            why_this_matters="Column-level contract memory is missing a planned response field.",
                            evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                        )
                    )
                    continue
                planned_bindings = normalize_planned_contract_bindings(field, reference_contexts)
                observed_bindings = sorted(evidence.get("field_bindings", {}).get(field_name, set()))
                if planned_bindings and not observed_bindings:
                    rows.append(
                        build_field_matrix_row(
                            scope="api",
                            node_id=node["id"],
                            node_ref=route,
                            field_name=field_name,
                            target_id=field_target_id,
                            field_id=field.get("id", ""),
                            status="uncertain",
                            planned=True,
                            observed=True,
                            why_this_matters="Review is required before trusting this contract binding.",
                            evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                            planned_binding=planned_bindings[0],
                        )
                    )
                    continue
                if planned_bindings and not set(planned_bindings).intersection(observed_bindings):
                    rows.append(
                        build_field_matrix_row(
                            scope="api",
                            node_id=node["id"],
                            node_ref=route,
                            field_name=field_name,
                            target_id=field_target_id,
                            field_id=field.get("id", ""),
                            status="binding_mismatch",
                            planned=True,
                            observed=True,
                            why_this_matters="Planned and implemented contract bindings disagree at the column level.",
                            evidence_sources=["implementation", "plan_yaml", *sorted(evidence.get("sources", []))],
                            planned_binding=planned_bindings[0],
                            observed_bindings=observed_bindings,
                        )
                    )
                    continue
                rows.append(
                    build_field_matrix_row(
                        scope="api",
                        node_id=node["id"],
                        node_ref=route,
                        field_name=field_name,
                        target_id=field_target_id,
                        field_id=field.get("id", ""),
                        status="matched",
                        planned=True,
                        observed=True,
                        why_this_matters="Planned and implemented API field evidence currently agree.",
                        evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                        planned_binding=planned_bindings[0] if planned_bindings else "",
                        observed_bindings=observed_bindings,
                    )
                )
            for field_name in sorted(impl_field_names - set(plan_fields)):
                field_target_id = resolve_hybrid_field_target_id(
                    canonical_index,
                    node_id=node["id"],
                    field_name=field_name,
                    is_contract=True,
                )
                rows.append(
                    build_field_matrix_row(
                        scope="api",
                        node_id=node["id"],
                        node_ref=route,
                        field_name=field_name,
                        target_id=field_target_id,
                        status="observed_unplanned",
                        planned=False,
                        observed=True,
                        why_this_matters="Implementation has drifted beyond the documented contract plan.",
                        evidence_sources=["implementation", *sorted(evidence.get("sources", []))],
                        observed_bindings=sorted(evidence.get("field_bindings", {}).get(field_name, set())),
                    )
                )
            continue

        if node["kind"] == "contract" and node.get("extension_type") == "ui":
            component = node.get("contract", {}).get("component") or node.get("label", "")
            if not component:
                continue
            planned_ui_components.add(component)
            evidence = ui_evidence.get(component)
            plan_fields = {field["name"]: field for field in node.get("contract", {}).get("fields", []) if not field.get("removed")}
            if not evidence:
                for field_name, field in sorted(plan_fields.items()):
                    field_target_id = resolve_hybrid_field_target_id(
                        canonical_index,
                        node_id=node["id"],
                        field_name=field_name,
                        explicit_field_id=field.get("id", ""),
                        is_contract=True,
                    )
                    rows.append(
                        build_field_matrix_row(
                            scope="ui",
                            node_id=node["id"],
                            node_ref=component,
                            field_name=field_name,
                            target_id=field_target_id,
                            field_id=field.get("id", ""),
                            status="planned_missing",
                            planned=True,
                            observed=False,
                            why_this_matters="A planned UI field is missing from the current implementation evidence.",
                            evidence_sources=["plan_yaml"],
                        )
                    )
                continue
            impl_field_names = set(evidence.get("fields", set()))
            for field_name, field in sorted(plan_fields.items()):
                field_target_id = resolve_hybrid_field_target_id(
                    canonical_index,
                    node_id=node["id"],
                    field_name=field_name,
                    explicit_field_id=field.get("id", ""),
                    is_contract=True,
                )
                if field_name in impl_field_names:
                    rows.append(
                        build_field_matrix_row(
                            scope="ui",
                            node_id=node["id"],
                            node_ref=component,
                            field_name=field_name,
                            target_id=field_target_id,
                            field_id=field.get("id", ""),
                            status="matched",
                            planned=True,
                            observed=True,
                            why_this_matters="Planned and implemented UI field usage currently agree.",
                            evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                        )
                    )
                    continue
                rows.append(
                    build_field_matrix_row(
                        scope="ui",
                        node_id=node["id"],
                        node_ref=component,
                        field_name=field_name,
                        target_id=field_target_id,
                        field_id=field.get("id", ""),
                        status="planned_missing",
                        planned=True,
                        observed=False,
                        why_this_matters="A planned UI field is missing from the current implementation evidence.",
                        evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                    )
                )
            for field_name in sorted(impl_field_names - set(plan_fields)):
                field_target_id = resolve_hybrid_field_target_id(
                    canonical_index,
                    node_id=node["id"],
                    field_name=field_name,
                    is_contract=True,
                )
                rows.append(
                    build_field_matrix_row(
                        scope="ui",
                        node_id=node["id"],
                        node_ref=component,
                        field_name=field_name,
                        target_id=field_target_id,
                        status="observed_unplanned",
                        planned=False,
                        observed=True,
                        why_this_matters="UI consumption has drifted beyond the documented plan.",
                        evidence_sources=["implementation", *sorted(evidence.get("sources", []))],
                    )
                )
            continue

        if node["kind"] not in {"data", "compute"}:
            continue
        relation = extract_sql_relation_tag(node)
        if not relation:
            continue
        planned_relations.add(relation)
        evidence = relation_evidence.get(relation)
        plan_columns = {column["name"]: column for column in node.get("columns", []) if not column.get("removed")}
        if not evidence:
            for column_name, column in sorted(plan_columns.items()):
                field_target_id = resolve_hybrid_field_target_id(
                    canonical_index,
                    node_id=node["id"],
                    field_name=column_name,
                    explicit_field_id=column.get("id", ""),
                    is_contract=False,
                )
                rows.append(
                    build_field_matrix_row(
                        scope=node["kind"],
                        node_id=node["id"],
                        node_ref=relation,
                        field_name=column_name,
                        target_id=field_target_id,
                        field_id=column.get("id", ""),
                        status="planned_missing",
                        planned=True,
                        observed=False,
                        why_this_matters="Column-level backend structure differs from the documented plan.",
                        evidence_sources=["plan_yaml"],
                        planned_type=column.get("data_type", ""),
                    )
                )
            continue
        impl_field_names = set(evidence.get("fields", set()))
        for column_name, column in sorted(plan_columns.items()):
            field_target_id = resolve_hybrid_field_target_id(
                canonical_index,
                node_id=node["id"],
                field_name=column_name,
                explicit_field_id=column.get("id", ""),
                is_contract=False,
            )
            if column_name not in impl_field_names:
                rows.append(
                    build_field_matrix_row(
                        scope=node["kind"],
                        node_id=node["id"],
                        node_ref=relation,
                        field_name=column_name,
                        target_id=field_target_id,
                        field_id=column.get("id", ""),
                        status="planned_missing",
                        planned=True,
                        observed=False,
                        why_this_matters="Column-level backend structure differs from the documented plan.",
                        evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                        planned_type=column.get("data_type", ""),
                    )
                )
                continue
            planned_type = column.get("data_type", "unknown")
            observed_type = evidence.get("data_types", {}).get(column_name, "unknown")
            planned_lineage = normalize_lineage_keys(column.get("lineage_inputs", []), reference_contexts)
            observed_lineage = sorted(evidence.get("field_sources", {}).get(column_name, set()))
            if planned_type not in {"", "unknown"} and observed_type not in {"", "unknown"} and planned_type != observed_type:
                rows.append(
                    build_field_matrix_row(
                        scope=node["kind"],
                        node_id=node["id"],
                        node_ref=relation,
                        field_name=column_name,
                        target_id=field_target_id,
                        field_id=column.get("id", ""),
                        status="column_type_mismatch",
                        planned=True,
                        observed=True,
                        why_this_matters="Column data types differ between the plan/spec artifact and implementation evidence.",
                        evidence_sources=["implementation", "plan_yaml", *sorted(evidence.get("sources", []))],
                        planned_type=planned_type,
                        observed_type=observed_type,
                        planned_lineage=planned_lineage,
                        observed_lineage=observed_lineage,
                    )
                )
                continue
            if planned_lineage and not observed_lineage:
                rows.append(
                    build_field_matrix_row(
                        scope=node["kind"],
                        node_id=node["id"],
                        node_ref=relation,
                        field_name=column_name,
                        target_id=field_target_id,
                        field_id=column.get("id", ""),
                        status="uncertain",
                        planned=True,
                        observed=True,
                        why_this_matters="Review is required before trusting this lineage mapping.",
                        evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                        planned_type=planned_type,
                        observed_type=observed_type,
                        planned_lineage=planned_lineage,
                    )
                )
                continue
            if planned_lineage and observed_lineage and set(planned_lineage) != set(observed_lineage):
                rows.append(
                    build_field_matrix_row(
                        scope=node["kind"],
                        node_id=node["id"],
                        node_ref=relation,
                        field_name=column_name,
                        target_id=field_target_id,
                        field_id=column.get("id", ""),
                        status="lineage_mismatch",
                        planned=True,
                        observed=True,
                        why_this_matters="Column lineage differs between the plan/spec artifact and implementation evidence.",
                        evidence_sources=["implementation", "plan_yaml", *sorted(evidence.get("sources", []))],
                        planned_type=planned_type,
                        observed_type=observed_type,
                        planned_lineage=planned_lineage,
                        observed_lineage=observed_lineage,
                    )
                )
                continue
            rows.append(
                build_field_matrix_row(
                    scope=node["kind"],
                    node_id=node["id"],
                    node_ref=relation,
                    field_name=column_name,
                    target_id=field_target_id,
                    field_id=column.get("id", ""),
                    status="matched",
                    planned=True,
                    observed=True,
                    why_this_matters="Planned and implemented backend field evidence currently agree.",
                    evidence_sources=["plan_yaml", *sorted(evidence.get("sources", []))],
                    planned_type=planned_type,
                    observed_type=observed_type,
                    planned_lineage=planned_lineage,
                    observed_lineage=observed_lineage,
                )
            )
        for column_name in sorted(impl_field_names - set(plan_columns)):
            field_target_id = resolve_hybrid_field_target_id(
                canonical_index,
                node_id=node["id"],
                field_name=column_name,
                is_contract=False,
            )
            rows.append(
                build_field_matrix_row(
                    scope=node["kind"],
                    node_id=node["id"],
                    node_ref=relation,
                    field_name=column_name,
                    target_id=field_target_id,
                    status="observed_unplanned",
                    planned=False,
                    observed=True,
                    why_this_matters="Implementation structure has drifted beyond the documented backend plan.",
                    evidence_sources=["implementation", *sorted(evidence.get("sources", []))],
                    observed_type=evidence.get("data_types", {}).get(column_name, ""),
                    observed_lineage=sorted(evidence.get("field_sources", {}).get(column_name, set())),
                )
            )

    for route, evidence in sorted(api_evidence.items()):
        if route in planned_api_routes:
            continue
        node_id = match_api_node_id(canonical_graph, route, route) or f"contract:api.{slugify_text(route.replace(' ', '_'))}"
        for field_name in sorted(evidence.get("fields", set())):
            field_target_id = resolve_hybrid_field_target_id(
                canonical_index,
                node_id=node_id,
                field_name=field_name,
                is_contract=True,
            )
            rows.append(
                build_field_matrix_row(
                    scope="api",
                    node_id=node_id,
                    node_ref=route,
                    field_name=field_name,
                    target_id=field_target_id,
                    status="observed_unplanned",
                    planned=False,
                    observed=True,
                    why_this_matters="Implementation has introduced API fields beyond the documented plan.",
                    evidence_sources=["implementation", *sorted(evidence.get("sources", []))],
                    observed_bindings=sorted(evidence.get("field_bindings", {}).get(field_name, set())),
                )
            )
    for component, evidence in sorted(ui_evidence.items()):
        if component in planned_ui_components:
            continue
        node_id = match_ui_node_id(canonical_graph, component) or f"contract:ui.{slugify_text(component)}"
        for field_name in sorted(evidence.get("fields", set())):
            field_target_id = resolve_hybrid_field_target_id(
                canonical_index,
                node_id=node_id,
                field_name=field_name,
                is_contract=True,
            )
            rows.append(
                build_field_matrix_row(
                    scope="ui",
                    node_id=node_id,
                    node_ref=component,
                    field_name=field_name,
                    target_id=field_target_id,
                    status="observed_unplanned",
                    planned=False,
                    observed=True,
                    why_this_matters="UI usage has introduced fields beyond the documented plan.",
                    evidence_sources=["implementation", *sorted(evidence.get("sources", []))],
                )
            )
    for relation, evidence in sorted(relation_evidence.items()):
        if relation in planned_relations:
            continue
        node_id = match_sql_data_node_id(canonical_graph, relation) or f"data:{slugify_text(relation)}"
        for field_name in sorted(evidence.get("fields", set())):
            field_target_id = resolve_hybrid_field_target_id(
                canonical_index,
                node_id=node_id,
                field_name=field_name,
                is_contract=False,
            )
            rows.append(
                build_field_matrix_row(
                    scope="data",
                    node_id=node_id,
                    node_ref=relation,
                    field_name=field_name,
                    target_id=field_target_id,
                    status="observed_unplanned",
                    planned=False,
                    observed=True,
                    why_this_matters="Implementation structure has introduced backend fields beyond the documented plan.",
                    evidence_sources=["implementation", *sorted(evidence.get("sources", []))],
                    observed_type=evidence.get("data_types", {}).get(field_name, ""),
                    observed_lineage=sorted(evidence.get("field_sources", {}).get(field_name, set())),
                )
            )

    return dedupe_field_matrix_rows(rows)

def build_hybrid_plan_comparison(
    canonical_graph: dict[str, Any],
    *,
    profile: dict[str, Any],
    doc_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    context = build_empty_hybrid_context()
    partial_candidates = [candidate for candidate in doc_candidates if candidate.get("type") == "partial_graph"]
    if not partial_candidates:
        return context

    plan_graph = combine_partial_graph_candidates(partial_candidates)
    if not plan_graph:
        return context

    context["summary"]["plan_candidates"] = len(partial_candidates)
    context["summary"]["plan_paths"] = sorted(candidate.get("path", "") for candidate in partial_candidates if candidate.get("path"))
    canonical_index = build_index(canonical_graph)
    reference_contexts = [
        (plan_graph, build_index(plan_graph)),
        (canonical_graph, canonical_index),
    ]
    impact_graph = build_hybrid_impact_graph(canonical_graph, plan_graph)
    impact_index = build_index(impact_graph)
    ui_roles = infer_ui_roles(impact_graph)
    implementation_doc_candidates = [candidate for candidate in doc_candidates if candidate.get("type") != "partial_graph"]
    api_evidence = collect_api_field_evidence(profile, implementation_doc_candidates)
    ui_evidence = collect_ui_field_evidence(profile)
    relation_evidence = collect_data_relation_evidence(profile)

    for node in active_nodes(plan_graph):
        context["summary"]["planned_nodes"] += 1
        if node["kind"] == "contract" and node.get("extension_type") == "api":
            route = node.get("contract", {}).get("route", "")
            plan_fields = {field["name"]: field for field in node.get("contract", {}).get("fields", []) if not field.get("removed")}
            context["summary"]["planned_fields"] += len(plan_fields)
            evidence = api_evidence.get(route)
            if not evidence:
                context["summary"]["missing_fields"] += len(plan_fields)
                context["planned_missing"].append(
                    {
                        "target_id": node["id"],
                        "label": node.get("label", route),
                        "message": f"{route} is documented in plan/spec artifacts but was not observed in implementation evidence.",
                        "why_this_matters": "The planned API route is not yet grounded in repo or doc implementation evidence.",
                        "source": "plan_yaml",
                        "significant": True,
                    }
                )
                continue
            context["summary"]["matched_nodes"] += 1
            impl_field_names = set(evidence["fields"])
            for field_name, field in plan_fields.items():
                field_target_id = resolve_hybrid_field_target_id(
                    canonical_index,
                    node_id=node["id"],
                    field_name=field_name,
                    explicit_field_id=field.get("id", ""),
                    is_contract=True,
                )
                if field_name not in impl_field_names:
                    context["summary"]["missing_fields"] += 1
                    context["planned_missing"].append(
                        {
                            "target_id": field_target_id,
                            "field_id": field.get("id", ""),
                            "label": field_name,
                            "message": f"{route}.{field_name} is planned but was not observed in implementation evidence.",
                            "why_this_matters": "Column-level contract memory is missing a planned response field.",
                            "source": "plan_yaml",
                            "significant": True,
                        }
                    )
                    continue
                context["summary"]["matched_fields"] += 1
                plan_bindings = normalize_planned_contract_bindings(field, reference_contexts)
                if not plan_bindings:
                    continue
                context["summary"]["planned_bindings"] += 1
                impl_bindings = evidence["field_bindings"].get(field_name, set())
                if not impl_bindings:
                    context["uncertain_matches"].append(
                        {
                            "target_id": field_target_id,
                            "field_id": field.get("id", ""),
                            "label": field_name,
                            "message": f"{route}.{field_name} has a planned binding, but implementation evidence did not expose a deterministic source.",
                            "why_this_matters": "Review is required before trusting this contract binding.",
                            "source": "plan_yaml",
                            "significant": True,
                        }
                    )
                    continue
                if set(plan_bindings).intersection(impl_bindings):
                    context["summary"]["matched_bindings"] += 1
                    continue
                contradiction = build_hybrid_contradiction(
                    impact_index,
                    ui_roles,
                    node_id=node["id"],
                    field_id=field_target_id,
                    field_name=field_name,
                    kind="hybrid_binding_mismatch",
                    existing_belief={"implementation_bindings": sorted(impl_bindings)},
                    new_evidence={"planned_bindings": plan_bindings},
                    message=(
                        f"{route}.{field_name} is planned to bind to {', '.join(plan_bindings)}, "
                        f"but implementation evidence points to {', '.join(sorted(impl_bindings))}."
                    ),
                    default_why="Planned and implemented contract bindings disagree at the column level.",
                )
                context["summary"]["binding_mismatches"] += 1
                context["contradictions"].append(contradiction)
                context["impacts"].extend(
                    {
                        "target_id": contradiction.get("field_id") or contradiction.get("target_id", ""),
                        "message": impact,
                        "significant": True,
                    }
                    for impact in contradiction.get("downstream_impacts", [])
                )
            for extra_field in sorted(impl_field_names - set(plan_fields)):
                context["summary"]["unplanned_fields"] += 1
                context["observed_untracked"].append(
                    {
                        "target_id": resolve_hybrid_field_target_id(
                            canonical_index,
                            node_id=node["id"],
                            field_name=extra_field,
                            is_contract=True,
                        ),
                        "label": extra_field,
                        "message": f"{route}.{extra_field} was observed in implementation evidence but is not present in the plan/spec artifact.",
                        "why_this_matters": "Implementation has drifted beyond the documented contract plan.",
                        "source": "implementation",
                        "significant": True,
                    }
                )
            continue

        if node["kind"] == "contract" and node.get("extension_type") == "ui":
            component = node.get("contract", {}).get("component") or node.get("label", "")
            plan_fields = {field["name"]: field for field in node.get("contract", {}).get("fields", []) if not field.get("removed")}
            context["summary"]["planned_fields"] += len(plan_fields)
            evidence = ui_evidence.get(component)
            if not evidence:
                context["summary"]["missing_fields"] += len(plan_fields)
                context["planned_missing"].append(
                    {
                        "target_id": node["id"],
                        "label": node.get("label", component),
                        "message": f"{component} is documented in plan/spec artifacts but was not observed in implementation usage.",
                        "why_this_matters": "The UI plan is not yet reflected in current repo evidence.",
                        "source": "plan_yaml",
                        "significant": True,
                    }
                )
                continue
            context["summary"]["matched_nodes"] += 1
            impl_field_names = set(evidence["fields"])
            for field_name, field in plan_fields.items():
                field_target_id = resolve_hybrid_field_target_id(
                    canonical_index,
                    node_id=node["id"],
                    field_name=field_name,
                    explicit_field_id=field.get("id", ""),
                    is_contract=True,
                )
                if field_name in impl_field_names:
                    context["summary"]["matched_fields"] += 1
                    continue
                context["summary"]["missing_fields"] += 1
                context["planned_missing"].append(
                    {
                        "target_id": field_target_id,
                        "field_id": field.get("id", ""),
                        "label": field_name,
                        "message": f"{component}.{field_name} is planned but was not observed in implementation usage.",
                        "why_this_matters": "A planned UI field is missing from the current implementation evidence.",
                        "source": "plan_yaml",
                        "significant": True,
                    }
                )
            for extra_field in sorted(impl_field_names - set(plan_fields)):
                context["summary"]["unplanned_fields"] += 1
                context["observed_untracked"].append(
                    {
                        "target_id": resolve_hybrid_field_target_id(
                            canonical_index,
                            node_id=node["id"],
                            field_name=extra_field,
                            is_contract=True,
                        ),
                        "label": extra_field,
                        "message": f"{component}.{extra_field} was observed in implementation usage but is not present in the plan/spec artifact.",
                        "why_this_matters": "UI consumption has drifted beyond the documented plan.",
                        "source": "implementation",
                        "significant": True,
                    }
                )
            continue

        if node["kind"] not in {"data", "compute"}:
            continue
        relation = extract_sql_relation_tag(node)
        plan_columns = {column["name"]: column for column in node.get("columns", []) if not column.get("removed")}
        context["summary"]["planned_fields"] += len(plan_columns)
        if not relation:
            continue
        evidence = relation_evidence.get(relation)
        if not evidence:
            context["summary"]["missing_fields"] += len(plan_columns)
            context["planned_missing"].append(
                {
                    "target_id": node["id"],
                    "label": node.get("label", relation),
                    "message": f"{relation} is documented in plan/spec artifacts but was not observed in implementation structure hints.",
                    "why_this_matters": "The planned backend relation is missing from implementation evidence.",
                    "source": "plan_yaml",
                    "significant": True,
                }
            )
            continue
        context["summary"]["matched_nodes"] += 1
        impl_field_names = set(evidence["fields"])
        for column_name, column in plan_columns.items():
            field_target_id = resolve_hybrid_field_target_id(
                canonical_index,
                node_id=node["id"],
                field_name=column_name,
                explicit_field_id=column.get("id", ""),
                is_contract=False,
            )
            if column_name not in impl_field_names:
                context["summary"]["missing_fields"] += 1
                context["planned_missing"].append(
                    {
                        "target_id": field_target_id,
                        "field_id": column.get("id", ""),
                        "label": column_name,
                        "message": f"{relation}.{column_name} is planned but was not observed in implementation structure hints.",
                        "why_this_matters": "Column-level backend structure differs from the documented plan.",
                        "source": "plan_yaml",
                        "significant": True,
                    }
                )
                continue
            context["summary"]["matched_fields"] += 1
            planned_type = column.get("data_type", "unknown")
            observed_type = evidence["data_types"].get(column_name, "unknown")
            if planned_type not in {"", "unknown"} and observed_type not in {"", "unknown"} and planned_type != observed_type:
                contradiction = build_hybrid_contradiction(
                    impact_index,
                    ui_roles,
                    node_id=node["id"],
                    field_id=field_target_id,
                    field_name=column_name,
                    kind="hybrid_column_type_mismatch",
                    existing_belief={"implementation_type": observed_type},
                    new_evidence={"planned_type": planned_type},
                    message=(
                        f"{relation}.{column_name} is planned as {planned_type}, "
                        f"but implementation evidence reports {observed_type}."
                    ),
                    default_why="Column data types differ between the plan/spec artifact and implementation evidence.",
                )
                context["summary"]["column_mismatches"] += 1
                context["contradictions"].append(contradiction)
            planned_lineage = normalize_lineage_keys(column.get("lineage_inputs", []), reference_contexts)
            observed_lineage = evidence["field_sources"].get(column_name, set())
            if planned_lineage and not observed_lineage:
                context["uncertain_matches"].append(
                    {
                        "target_id": field_target_id,
                        "field_id": column.get("id", ""),
                        "label": column_name,
                        "message": f"{relation}.{column_name} has planned lineage, but implementation evidence did not expose deterministic upstream columns.",
                        "why_this_matters": "Review is required before trusting this lineage mapping.",
                        "source": "plan_yaml",
                        "significant": True,
                    }
                )
            elif planned_lineage and observed_lineage and planned_lineage != observed_lineage:
                contradiction = build_hybrid_contradiction(
                    impact_index,
                    ui_roles,
                    node_id=node["id"],
                    field_id=field_target_id,
                    field_name=column_name,
                    kind="hybrid_lineage_mismatch",
                    existing_belief={"implementation_lineage": sorted(observed_lineage)},
                    new_evidence={"planned_lineage": sorted(planned_lineage)},
                    message=(
                        f"{relation}.{column_name} is planned from {', '.join(sorted(planned_lineage))}, "
                        f"but implementation evidence shows {', '.join(sorted(observed_lineage))}."
                    ),
                    default_why="Column lineage differs between the plan/spec artifact and implementation evidence.",
                )
                context["summary"]["column_mismatches"] += 1
                context["contradictions"].append(contradiction)
        for extra_field in sorted(impl_field_names - set(plan_columns)):
            context["summary"]["unplanned_fields"] += 1
            context["observed_untracked"].append(
                {
                    "target_id": resolve_hybrid_field_target_id(
                        canonical_index,
                        node_id=node["id"],
                        field_name=extra_field,
                        is_contract=False,
                    ),
                    "label": extra_field,
                    "message": f"{relation}.{extra_field} was observed in implementation structure hints but is not present in the plan/spec artifact.",
                    "why_this_matters": "Implementation structure has drifted beyond the documented backend plan.",
                    "source": "implementation",
                    "significant": True,
                }
            )

    context["impacts"] = dedupe_impacts(context.get("impacts", []))
    context["contradictions"] = dedupe_contradictions(context.get("contradictions", []))
    context["field_matrix"] = build_field_matrix_from_hybrid(
        canonical_graph,
        plan_graph,
        api_evidence=api_evidence,
        ui_evidence=ui_evidence,
        relation_evidence=relation_evidence,
        reference_contexts=reference_contexts,
    )
    context["field_matrix_summary"] = build_field_matrix_summary(context.get("field_matrix", []))
    return context

def detect_missing_confirmed_structure(
    canonical_graph: dict[str, Any],
    *,
    observed_routes: dict[str, dict[str, Any]],
    contradictions: list[dict[str, Any]],
    impacts: list[dict[str, Any]],
    scope: str,
) -> None:
    canonical_index = build_index(canonical_graph)
    observed_api_routes = set(observed_routes)
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
                        "kind": "missing_confirmed_route",
                        "severity": "high",
                        "existing_belief": {"route": route},
                        "new_evidence": {"missing_in_scan": True},
                        "affected_refs": [node["id"]],
                        "confidence_delta": "down",
                        "downstream_impacts": build_missing_node_impacts(canonical_index, node["id"]),
                        "evidence_sources": ["canonical", "implementation"],
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


def build_scan_fingerprint(
    canonical_graph: dict[str, Any],
    *,
    profile: dict[str, Any],
    role: str,
    scope: str,
    include_tests: bool,
    include_internal: bool,
    doc_candidates: list[dict[str, Any]],
    root_dir: Path,
    selected_paths: list[str],
) -> str:
    payload = {
        "base_structure_version": canonical_graph.get("metadata", {}).get("structure_version", 1),
        "role": role,
        "scope": scope,
        "include_tests": include_tests,
        "include_internal": include_internal,
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
