from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
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
MARKDOWN_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
MARKDOWN_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
MARKDOWN_ROUTE_LINE_RE = re.compile(r"^(GET|POST|PUT|PATCH|DELETE)\s+(/[\w\-/{}/.]+)$", re.IGNORECASE)
MARKDOWN_API_HEADING_RE = re.compile(r"^(?:api|route|endpoint)\s*:?\s*(GET|POST|PUT|PATCH|DELETE)\s+(/[\w\-/{}/.]+)$", re.IGNORECASE)
MARKDOWN_UI_HEADING_RE = re.compile(r"^(?:ui|component)\s*:?\s*(.+)$", re.IGNORECASE)
MARKDOWN_DATA_HEADING_RE = re.compile(
    r"^(?P<prefix>data|dataset|table|view|materialized view|materialized_view|relation)\s*:?\s*(?P<value>.+)$",
    re.IGNORECASE,
)
MARKDOWN_COMPUTE_HEADING_RE = re.compile(r"^(?P<prefix>compute|transform|model)\s*:?\s*(?P<value>.+)$", re.IGNORECASE)
MARKDOWN_LIST_PREFIX_RE = re.compile(r"^(?:[-*+]\s+|\d+\.\s+|\[[ xX]\]\s+)")
SCANNER_VERSIONS = {
    "assets": "1",
    "python_fastapi": "1",
    "ui_contracts": "1",
    "docs": "2",
    "sql": "3",
    "orm": "3",
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

def apply_partial_graph_candidate(
    observed_graph: dict[str, Any],
    *,
    candidate: dict[str, Any],
    patches: list[dict[str, Any]],
    role: str,
) -> None:
    partial_graph = normalize_graph(candidate["graph"])
    partial_index = build_index(partial_graph)
    plan_node_id_map = {
        node["id"]: match_partial_graph_node_id(observed_graph, node) or node["id"]
        for node in active_nodes(partial_graph)
    }
    for node in active_nodes(partial_graph):
        matched_node_id = plan_node_id_map.get(node["id"], node["id"])
        if not any(candidate_node["id"] == matched_node_id for candidate_node in observed_graph.get("nodes", [])):
            node_copy = build_plan_node_copy(
                node,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            observed_graph["nodes"].append(node_copy)
            patches.append(
                build_patch(
                    "add_node",
                    target_id=node_copy["id"],
                    node_id=node_copy["id"],
                    role=role,
                    evidence=["plan_yaml"],
                    payload={"node": node_copy},
                    confidence="low",
                )
            )
            continue
        observed_node = next(candidate_node for candidate_node in observed_graph["nodes"] if candidate_node["id"] == matched_node_id)
        ensure_plan_node_fields(
            observed_graph,
            observed_node,
            node,
            plan_node_id_map=plan_node_id_map,
            partial_index=partial_index,
            patches=patches,
            role=role,
        )

    observed_index = build_index(observed_graph)
    for edge in partial_graph.get("edges", []):
        source_id = plan_node_id_map.get(edge.get("source", ""), edge.get("source", ""))
        target_id = plan_node_id_map.get(edge.get("target", ""), edge.get("target", ""))
        if not source_id or not target_id:
            continue
        edge_copy = deepcopy(edge)
        edge_copy["source"] = source_id
        edge_copy["target"] = target_id
        edge_copy["id"] = build_edge(edge_copy.get("type", ""), source_id, target_id)["id"]
        if edge_copy["id"] in observed_index["edges"]:
            continue
        if source_id not in observed_index["nodes"] or target_id not in observed_index["nodes"]:
            continue
        edge_copy["state"] = "proposed"
        edge_copy["confidence"] = "low"
        edge_copy["evidence"] = sorted(set([*edge_copy.get("evidence", []), "plan_yaml"]))
        edge_copy["removed"] = False
        edge_copy["history"] = []
        observed_graph["edges"].append(edge_copy)
        patches.append(
            build_patch(
                "add_edge",
                target_id=edge_copy["id"],
                edge_id=edge_copy["id"],
                role=role,
                evidence=["plan_yaml"],
                payload={"edge": edge_copy},
                confidence="low",
            )
        )


def match_partial_graph_node_id(graph: dict[str, Any], node: dict[str, Any]) -> str:
    if any(candidate["id"] == node["id"] for candidate in graph.get("nodes", [])):
        return node["id"]
    if node["kind"] == "contract" and node.get("extension_type") == "api":
        return match_api_node_id(graph, node.get("contract", {}).get("route", ""), node.get("label", ""))
    if node["kind"] == "contract" and node.get("extension_type") == "ui":
        return match_ui_node_id(graph, node.get("contract", {}).get("component") or node.get("label", ""))
    relation = extract_sql_relation_tag(node)
    if node["kind"] == "data" and relation:
        return match_sql_data_node_id(graph, relation)
    if node["kind"] == "compute" and relation:
        return match_sql_compute_node_id(graph, relation)
    if node["kind"] == "data":
        plan_path = node.get("data", {}).get("profile_target") or node.get("data", {}).get("local_path") or ""
        if plan_path:
            for candidate in graph.get("nodes", []):
                if candidate["kind"] != "data" or candidate.get("removed"):
                    continue
                candidate_path = candidate.get("data", {}).get("profile_target") or candidate.get("data", {}).get("local_path") or ""
                if candidate_path == plan_path:
                    return candidate["id"]
    return ""


def build_plan_node_copy(
    node: dict[str, Any],
    *,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> dict[str, Any]:
    node_copy = deepcopy(node)
    node_copy["state"] = "proposed"
    node_copy["verification_state"] = "observed"
    node_copy["confidence"] = "low"
    node_copy["evidence"] = sorted(set([*node_copy.get("evidence", []), "plan_yaml"]))
    node_copy["last_verified_by"] = ""
    node_copy["last_verified_at"] = ""
    node_copy["history"] = []
    node_copy["removed"] = False
    node_copy["removed_at"] = ""
    node_copy["removed_by"] = ""
    if node_copy["kind"] == "compute":
        node_copy.setdefault("compute", {})
        node_copy["compute"]["inputs"] = remap_plan_node_ids(
            node_copy.get("compute", {}).get("inputs", []),
            plan_node_id_map,
        )
        node_copy["compute"]["outputs"] = remap_plan_node_ids(
            node_copy.get("compute", {}).get("outputs", []),
            plan_node_id_map,
        )
    if node_copy["kind"] == "contract":
        node_copy.setdefault("contract", {})
        node_copy["contract"]["fields"] = [
            build_plan_field_copy(
                node_copy["id"],
                field,
                observed_graph=None,
                is_contract=True,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            for field in node_copy.get("contract", {}).get("fields", [])
        ]
    else:
        node_copy["columns"] = [
            build_plan_field_copy(
                node_copy["id"],
                column,
                observed_graph=None,
                is_contract=False,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            for column in node_copy.get("columns", [])
        ]
    return node_copy


def build_plan_field_copy(
    node_id: str,
    field: dict[str, Any],
    *,
    observed_graph: dict[str, Any] | None,
    is_contract: bool,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> dict[str, Any]:
    field_copy = deepcopy(field)
    if observed_graph is not None:
        existing_ids = collect_existing_field_ids(observed_graph)
        if not field_copy.get("id") or field_copy["id"] in existing_ids:
            field_copy["id"] = make_field_id(node_id, field_copy.get("name", "field"), existing_ids)
    field_copy["state"] = "proposed"
    field_copy["verification_state"] = "observed"
    field_copy["confidence"] = "low"
    field_copy["evidence"] = sorted(set([*field_copy.get("evidence", []), "plan_yaml"]))
    field_copy["last_verified_by"] = ""
    field_copy["last_verified_at"] = ""
    field_copy["history"] = []
    field_copy["removed"] = False
    field_copy["removed_at"] = ""
    field_copy["removed_by"] = ""
    if is_contract:
        field_copy["primary_binding"] = remap_plan_field_reference(
            field_copy.get("primary_binding", ""),
            plan_node_id_map,
            partial_index,
        )
        field_copy["alternatives"] = [
            reference
            for reference in (
                remap_plan_field_reference(reference, plan_node_id_map, partial_index)
                for reference in field_copy.get("alternatives", [])
            )
            if reference
        ]
        field_copy["sources"] = remap_plan_contract_sources(field_copy.get("sources", []), plan_node_id_map)
    else:
        field_copy["lineage_inputs"] = remap_plan_lineage_inputs(
            field_copy.get("lineage_inputs", []),
            plan_node_id_map,
            partial_index,
        )
    return field_copy


def remap_plan_node_ids(node_ids: list[str], plan_node_id_map: dict[str, str]) -> list[str]:
    return sorted(
        {
            plan_node_id_map.get(node_id, node_id)
            for node_id in node_ids or []
            if plan_node_id_map.get(node_id, node_id)
        }
    )


def remap_plan_field_reference(
    reference: str,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> str:
    if not reference:
        return ""
    resolved_field_id = resolve_field_reference(reference, partial_index)
    if resolved_field_id:
        owner_id = partial_index.get("field_owner", {}).get(resolved_field_id, "")
        field_name = partial_index.get("field_name_by_id", {}).get(resolved_field_id, "")
        mapped_owner_id = plan_node_id_map.get(owner_id, owner_id)
        if mapped_owner_id and field_name:
            return column_ref(mapped_owner_id, field_name)
    if "." in reference:
        owner_ref, field_name = reference.rsplit(".", 1)
        mapped_owner_id = plan_node_id_map.get(owner_ref, owner_ref)
        if mapped_owner_id and field_name:
            return column_ref(mapped_owner_id, field_name)
    return plan_node_id_map.get(reference, reference)


def remap_plan_contract_sources(
    sources: list[dict[str, Any]],
    plan_node_id_map: dict[str, str],
) -> list[dict[str, Any]]:
    remapped: list[dict[str, Any]] = []
    for source in sources or []:
        node_id = plan_node_id_map.get(source.get("node_id", ""), source.get("node_id", ""))
        if not node_id:
            continue
        remapped_source = {"node_id": node_id}
        if source.get("column"):
            remapped_source["column"] = source["column"]
        if source.get("field"):
            remapped_source["field"] = source["field"]
        remapped.append(remapped_source)
    return remapped


def remap_plan_lineage_inputs(
    lineage_inputs: list[dict[str, Any]] | list[str],
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
) -> list[dict[str, Any]]:
    remapped: list[dict[str, Any]] = []
    for item in lineage_inputs or []:
        if isinstance(item, str):
            reference = remap_plan_field_reference(item, plan_node_id_map, partial_index)
            if reference:
                remapped.append({"field_id": reference, "role": ""})
            continue
        reference = remap_plan_field_reference(item.get("field_id", ""), plan_node_id_map, partial_index)
        if not reference:
            continue
        remapped.append({"field_id": reference, "role": item.get("role", "")})
    return remapped


def plan_sources_to_binding_refs(sources: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for source in sources or []:
        if source.get("column"):
            refs.append(column_ref(source.get("node_id", ""), source.get("column", "")))
        elif source.get("field"):
            refs.append(column_ref(source.get("node_id", ""), source.get("field", "")))
        elif source.get("node_id"):
            refs.append(source["node_id"])
    return [reference for reference in refs if reference]


def materialize_plan_contract_binding(graph: dict[str, Any], field: dict[str, Any]) -> None:
    binding_refs = [reference for reference in [field.get("primary_binding", ""), *field.get("alternatives", [])] if reference]
    if not binding_refs:
        binding_refs = plan_sources_to_binding_refs(field.get("sources", []))
    if not binding_refs:
        return
    graph_index = build_index(graph)
    resolved_refs = [resolve_field_reference(reference, graph_index) or reference for reference in binding_refs]
    field["primary_binding"] = resolved_refs[0]
    field["alternatives"] = [reference for reference in resolved_refs[1:] if reference != resolved_refs[0]]
    field["sources"] = [
        source
        for source in (
            resolve_legacy_source_reference(graph, reference)
            for reference in [field.get("primary_binding", ""), *field.get("alternatives", [])]
        )
        if source
    ]


def maybe_add_plan_binding_patch(
    graph: dict[str, Any],
    *,
    node_id: str,
    field: dict[str, Any],
    had_binding: bool,
    patches: list[dict[str, Any]],
    role: str,
) -> None:
    if had_binding:
        return
    materialize_plan_contract_binding(graph, field)
    if not field.get("primary_binding"):
        return
    field["state"] = "observed"
    field["verification_state"] = "observed"
    field["confidence"] = "low"
    patches.append(
        build_patch(
            "add_binding",
            target_id=field["id"],
            node_id=node_id,
            field_id=field["id"],
            role=role,
            evidence=["plan_yaml"],
            confidence="low",
            payload={
                "field_id": field["id"],
                "primary_binding": field.get("primary_binding", ""),
                "alternatives": field.get("alternatives", []),
            },
        )
    )


def ensure_plan_node_fields(
    graph: dict[str, Any],
    observed_node: dict[str, Any],
    plan_node: dict[str, Any],
    *,
    plan_node_id_map: dict[str, str],
    partial_index: dict[str, Any],
    patches: list[dict[str, Any]],
    role: str,
) -> None:
    observed_node["tags"] = sorted(set([*observed_node.get("tags", []), *plan_node.get("tags", [])]))
    observed_node["evidence"] = sorted(set([*observed_node.get("evidence", []), "plan_yaml"]))
    for key in ("description", "owner"):
        if not observed_node.get(key) and plan_node.get(key):
            observed_node[key] = plan_node[key]
    if observed_node["kind"] == "contract":
        existing_fields = {field.get("name"): field for field in observed_node.get("contract", {}).get("fields", [])}
        for field in plan_node.get("contract", {}).get("fields", []):
            field_name = field.get("name", "")
            if not field_name:
                continue
            current = existing_fields.get(field_name)
            if current is None:
                field_copy = build_plan_field_copy(
                    observed_node["id"],
                    field,
                    observed_graph=graph,
                    is_contract=True,
                    plan_node_id_map=plan_node_id_map,
                    partial_index=partial_index,
                )
                observed_node.setdefault("contract", {}).setdefault("fields", []).append(field_copy)
                patches.append(
                    build_patch(
                        "add_field",
                        target_id=field_copy["id"],
                        node_id=observed_node["id"],
                        field_id=field_copy["id"],
                        role=role,
                        evidence=["plan_yaml"],
                        payload={"field": field_copy},
                        confidence="low",
                    )
                )
                continue
            had_binding = bool(current.get("primary_binding") or current.get("sources"))
            field_intent = build_plan_field_copy(
                observed_node["id"],
                field,
                observed_graph=None,
                is_contract=True,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            merge_partial_field_like(current, field_intent)
            maybe_add_plan_binding_patch(
                graph,
                node_id=observed_node["id"],
                field=current,
                had_binding=had_binding,
                patches=patches,
                role=role,
            )
        return

    existing_columns = {column.get("name"): column for column in observed_node.get("columns", [])}
    for column in plan_node.get("columns", []):
        column_name = column.get("name", "")
        if not column_name:
            continue
        current = existing_columns.get(column_name)
        if current is None:
            column_copy = build_plan_field_copy(
                observed_node["id"],
                column,
                observed_graph=graph,
                is_contract=False,
                plan_node_id_map=plan_node_id_map,
                partial_index=partial_index,
            )
            observed_node.setdefault("columns", []).append(column_copy)
            patches.append(
                build_patch(
                    "add_field",
                    target_id=column_copy["id"],
                    node_id=observed_node["id"],
                    field_id=column_copy["id"],
                    role=role,
                    evidence=["plan_yaml"],
                    payload={"field": column_copy},
                    confidence="low",
                )
            )
            continue
        column_intent = build_plan_field_copy(
            observed_node["id"],
            column,
            observed_graph=None,
            is_contract=False,
            plan_node_id_map=plan_node_id_map,
            partial_index=partial_index,
        )
        merge_partial_field_like(current, column_intent)
    if observed_node["kind"] == "compute":
        observed_node.setdefault("compute", {})
        plan_compute = plan_node.get("compute", {})
        observed_node["compute"]["inputs"] = sorted(
            set(
                [
                    *observed_node["compute"].get("inputs", []),
                    *remap_plan_node_ids(plan_compute.get("inputs", []), plan_node_id_map),
                ]
            )
        )
        observed_node["compute"]["outputs"] = sorted(
            set(
                [
                    *observed_node["compute"].get("outputs", []),
                    *remap_plan_node_ids(plan_compute.get("outputs", []), plan_node_id_map),
                ]
            )
        )


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


def combine_partial_graph_candidates(partial_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    combined = normalize_graph({"metadata": {"name": "Hybrid Plan"}, "nodes": [], "edges": []})
    for candidate in partial_candidates:
        partial_graph = normalize_graph(candidate["graph"])
        for node in partial_graph.get("nodes", []):
            merge_partial_graph_node(combined, node)
        existing_edges = {edge["id"] for edge in combined.get("edges", [])}
        for edge in partial_graph.get("edges", []):
            if edge["id"] not in existing_edges:
                combined["edges"].append(deepcopy(edge))
                existing_edges.add(edge["id"])
    return normalize_graph(combined)


def merge_partial_graph_node(graph: dict[str, Any], incoming_node: dict[str, Any]) -> None:
    existing_node = next((node for node in graph.get("nodes", []) if node["id"] == incoming_node["id"]), None)
    if existing_node is None:
        graph.setdefault("nodes", []).append(deepcopy(incoming_node))
        return
    existing_node["tags"] = sorted(set([*existing_node.get("tags", []), *incoming_node.get("tags", [])]))
    for key in ("label", "description", "owner"):
        if not existing_node.get(key) and incoming_node.get(key):
            existing_node[key] = incoming_node[key]
    if existing_node["kind"] == "contract":
        contract = existing_node.setdefault("contract", {})
        incoming_contract = incoming_node.get("contract", {})
        for key in ("route", "component", "ui_role"):
            if not contract.get(key) and incoming_contract.get(key):
                contract[key] = incoming_contract[key]
        field_map = {field.get("name"): field for field in contract.get("fields", [])}
        for field in incoming_contract.get("fields", []):
            current = field_map.get(field.get("name"))
            if current is None:
                contract.setdefault("fields", []).append(deepcopy(field))
                field_map[field.get("name")] = contract["fields"][-1]
                continue
            merge_partial_field_like(current, field)
        return

    if not existing_node.get("columns") and incoming_node.get("columns"):
        existing_node["columns"] = []
    column_map = {column.get("name"): column for column in existing_node.get("columns", [])}
    for column in incoming_node.get("columns", []):
        current = column_map.get(column.get("name"))
        if current is None:
            existing_node.setdefault("columns", []).append(deepcopy(column))
            column_map[column.get("name")] = existing_node["columns"][-1]
            continue
        merge_partial_field_like(current, column)
    if existing_node["kind"] == "compute":
        existing_node.setdefault("compute", {})
        incoming_compute = incoming_node.get("compute", {})
        existing_node["compute"]["inputs"] = sorted(set([*existing_node["compute"].get("inputs", []), *incoming_compute.get("inputs", [])]))
        existing_node["compute"]["outputs"] = sorted(set([*existing_node["compute"].get("outputs", []), *incoming_compute.get("outputs", [])]))


def merge_partial_field_like(existing_field: dict[str, Any], incoming_field: dict[str, Any]) -> None:
    for key, value in incoming_field.items():
        if key == "history":
            continue
        if key == "sources":
            if not existing_field.get("sources") and value:
                existing_field["sources"] = deepcopy(value)
            continue
        if key == "lineage_inputs":
            if not existing_field.get("lineage_inputs") and value:
                existing_field["lineage_inputs"] = deepcopy(value)
            continue
        if key == "alternatives":
            if not existing_field.get("alternatives") and value:
                existing_field["alternatives"] = list(value)
            continue
        if key == "evidence":
            existing_field["evidence"] = sorted(set([*existing_field.get("evidence", []), *(value or [])]))
            continue
        if is_blank_structure_value(existing_field.get(key)) and not is_blank_structure_value(value):
            existing_field[key] = deepcopy(value)


def is_blank_structure_value(value: Any) -> bool:
    return value in ("", None, [], {})


def collect_api_field_evidence(profile: dict[str, Any], doc_candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    for hint in profile.get("api_contract_hints", []) or []:
        route = hint.get("route", "")
        if not route:
            continue
        item = evidence.setdefault(route, {"fields": set(), "field_bindings": defaultdict(set), "sources": set()})
        item["sources"].add("repo")
        item["fields"].update(field_name for field_name in hint.get("response_fields", []) or [] if field_name)
        for source_hint in hint.get("response_field_sources", []) or []:
            field_name = source_hint.get("name", "")
            if not field_name:
                continue
            for source_field in source_hint.get("source_fields", []) or []:
                normalized = normalize_source_field_key(source_field)
                if normalized:
                    item["field_bindings"][field_name].add(normalized)
    for candidate in doc_candidates:
        if candidate.get("type") != "api_route":
            continue
        route = candidate.get("route", "")
        if not route:
            continue
        item = evidence.setdefault(route, {"fields": set(), "field_bindings": defaultdict(set), "sources": set()})
        item["sources"].add("docs")
        item["fields"].update(field_name for field_name in candidate.get("fields", []) or [] if field_name)
    return evidence


def collect_ui_field_evidence(profile: dict[str, Any]) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    for hint in profile.get("ui_contract_hints", []) or []:
        component = hint.get("component") or hint.get("label") or ""
        if not component:
            continue
        item = evidence.setdefault(component, {"fields": set(), "api_routes": set(), "sources": set()})
        item["sources"].add("repo")
        item["fields"].update(field_name for field_name in hint.get("used_fields", []) or [] if field_name)
        item["api_routes"].update(route for route in hint.get("api_routes", []) or [] if route)
    return evidence


def collect_data_relation_evidence(profile: dict[str, Any]) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    for hint in [*(profile.get("sql_structure_hints", []) or []), *(profile.get("orm_structure_hints", []) or [])]:
        relation = hint.get("relation", "")
        if not relation:
            continue
        item = evidence.setdefault(
            relation,
            {
                "fields": set(),
                "field_sources": defaultdict(set),
                "data_types": {},
                "object_types": set(),
                "upstream_relations": set(),
                "sources": set(),
            },
        )
        item["sources"].add("repo")
        if hint.get("object_type"):
            item["object_types"].add(hint["object_type"])
        item["upstream_relations"].update(relation_name for relation_name in hint.get("upstream_relations", []) or [] if relation_name)
        for field in hint.get("fields", []) or []:
            field_name = field.get("name", "")
            if not field_name:
                continue
            item["fields"].add(field_name)
            if field.get("data_type") and item["data_types"].get(field_name) in {None, "", "unknown"}:
                item["data_types"][field_name] = field["data_type"]
            for source_field in field.get("source_fields", []) or []:
                normalized = normalize_source_field_key(source_field)
                if normalized:
                    item["field_sources"][field_name].add(normalized)
    return evidence


def build_hybrid_impact_graph(canonical_graph: dict[str, Any], plan_graph: dict[str, Any]) -> dict[str, Any]:
    combined = normalize_graph({"metadata": {"name": "Hybrid Impact Graph"}, "nodes": [], "edges": []})
    existing_node_ids: set[str] = set()
    existing_edge_ids: set[str] = set()
    for graph in (canonical_graph, plan_graph):
        for node in graph.get("nodes", []):
            if node["id"] not in existing_node_ids:
                combined["nodes"].append(deepcopy(node))
                existing_node_ids.add(node["id"])
        for edge in graph.get("edges", []):
            if edge["id"] not in existing_edge_ids:
                combined["edges"].append(deepcopy(edge))
                existing_edge_ids.add(edge["id"])
    return normalize_graph(combined)


def build_hybrid_contradiction(
    impact_index: dict[str, Any],
    ui_roles: dict[str, str],
    *,
    node_id: str,
    field_id: str,
    field_name: str,
    kind: str,
    existing_belief: dict[str, Any],
    new_evidence: dict[str, Any],
    message: str,
    default_why: str,
) -> dict[str, Any]:
    impacts = describe_downstream_component_impacts(impact_index, node_id, field_name, ui_roles)
    fingerprint = hashlib.sha1(
        json.dumps(
            {
                "node_id": node_id,
                "field_id": field_id,
                "kind": kind,
                "existing_belief": existing_belief,
                "new_evidence": new_evidence,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:8]
    return {
        "id": f"contradiction.{field_id or slugify_text(node_id)}.{fingerprint}",
        "target_id": field_id or node_id,
        "node_id": node_id,
        "field_id": field_id,
        "kind": kind,
        "severity": "high" if impacts else "medium",
        "existing_belief": existing_belief,
        "new_evidence": new_evidence,
        "affected_refs": [value for value in [field_id, node_id] if value],
        "confidence_delta": "down",
        "downstream_impacts": impacts,
        "evidence_sources": ["plan_yaml", "implementation"],
        "message": message,
        "why_this_matters": impacts[0] if impacts else default_why,
        "review_required": True,
    }


def normalize_reference_key(reference: str, reference_contexts: list[tuple[dict[str, Any], dict[str, Any]]]) -> str:
    if not reference:
        return ""
    for graph, index in reference_contexts:
        resolved = resolve_field_reference(reference, index)
        if not resolved:
            continue
        owner_id = index["field_owner"].get(resolved, "")
        field_name = index["field_name_by_id"].get(resolved, "")
        node = index["nodes"].get(owner_id, {})
        relation = extract_sql_relation_tag(node)
        data_path = node.get("data", {}).get("profile_target") or node.get("data", {}).get("local_path") or ""
        if relation:
            return f"sql:{relation}.{field_name}"
        if data_path:
            return f"path:{data_path}.{field_name}"
        return column_ref(owner_id, field_name)
    return reference


def normalize_lineage_keys(
    lineage_inputs: list[dict[str, Any]],
    reference_contexts: list[tuple[dict[str, Any], dict[str, Any]]],
) -> set[str]:
    keys: set[str] = set()
    for lineage in lineage_inputs or []:
        normalized = normalize_reference_key(lineage.get("field_id", ""), reference_contexts)
        if normalized:
            keys.add(normalized)
    return keys


def normalize_source_field_key(source_field: dict[str, Any]) -> str:
    relation = source_field.get("relation", "")
    column = source_field.get("column", "")
    if relation and column:
        return f"sql:{relation}.{column}"
    if relation:
        return f"sql:{relation}"
    return ""


def extract_sql_relation_tag(node: dict[str, Any]) -> str:
    for tag in node.get("tags", []) or []:
        if tag.startswith("sql_relation:"):
            return tag.split(":", 1)[1]
    return ""


def hint_observation_evidence(hint: dict[str, Any]) -> list[str]:
    evidence = ["static_analysis", *(hint.get("evidence", []) or [])]
    return sorted(dict.fromkeys(item for item in evidence if item))


def apply_hint_signal_to_node(node: dict[str, Any], hint: dict[str, Any], *, role: str) -> None:
    hint_confidence = hint.get("confidence", "")
    if hint_confidence and CONFIDENCE_RANK.get(hint_confidence, 0) != CONFIDENCE_RANK.get(node.get("confidence", role_default_confidence(role)), 0):
        node["confidence"] = hint_confidence
    hint_evidence = hint_observation_evidence(hint)
    if hint_evidence:
        node["evidence"] = sorted(dict.fromkeys([*node.get("evidence", []), *hint_evidence]))


def summarize_downstream_breakage(contradictions: list[dict[str, Any]], impacts: list[dict[str, Any]]) -> dict[str, Any]:
    items: dict[tuple[str, str], dict[str, Any]] = {}
    for contradiction in contradictions:
        for message in contradiction.get("downstream_impacts", []) or []:
            target_id = contradiction.get("field_id") or contradiction.get("target_id", "")
            source = contradiction.get("kind", "contradiction")
            key = (target_id, message)
            items.setdefault(
                key,
                {
                    "target_id": target_id,
                    "message": message,
                    "source": source,
                },
            )
    for impact in impacts:
        if not impact.get("significant", False):
            continue
        target_id = impact.get("target_id", "")
        key = (target_id, impact.get("message", ""))
        items.setdefault(
            key,
            {
                "target_id": target_id,
                "message": impact.get("message", ""),
                "source": "impact",
            },
        )
    ordered_items = [items[key] for key in sorted(items)]
    by_source: dict[str, int] = {}
    by_target: dict[str, dict[str, Any]] = {}
    for item in ordered_items:
        source = item.get("source", "") or "impact"
        target_id = item.get("target_id", "")
        by_source[source] = by_source.get(source, 0) + 1
        target_summary = by_target.setdefault(
            target_id,
            {"target_id": target_id, "count": 0, "sources": set(), "messages": []},
        )
        target_summary["count"] += 1
        target_summary["sources"].add(source)
        if item.get("message", "") not in target_summary["messages"]:
            target_summary["messages"].append(item.get("message", ""))
    ordered_sources = [
        {"source": source, "count": by_source[source]}
        for source in sorted(by_source)
    ]
    ordered_targets = [
        {
            "target_id": summary["target_id"],
            "count": summary["count"],
            "sources": sorted(summary["sources"]),
            "messages": summary["messages"][:5],
        }
        for summary in sorted(
            by_target.values(),
            key=lambda item: (-item["count"], item["target_id"]),
        )
    ]
    return {
        "count": len(ordered_items),
        "items": ordered_items,
        "by_source": ordered_sources,
        "targets": ordered_targets,
    }


def find_related_patch_ids_for_targets(
    patches: list[dict[str, Any]],
    *,
    target_ids: list[str] | set[str] | None = None,
    node_ids: list[str] | set[str] | None = None,
) -> list[str]:
    target_refs = {value for value in (target_ids or []) if value}
    node_refs = {value for value in (node_ids or []) if value}
    related_ids: list[str] = []
    for patch in patches:
        patch_targets = {patch.get("field_id", ""), patch.get("target_id", ""), patch.get("node_id", "")}
        if target_refs.intersection(patch_targets) or node_refs.intersection(patch_targets):
            related_ids.append(patch["id"])
    return sorted(dict.fromkeys(related_ids))


def resolve_contradiction_cluster_state(
    review_state_counts: dict[str, int],
    patch_state_counts: dict[str, int],
    *,
    review_required_count: int,
) -> str:
    if review_state_counts.get("deferred", 0) or patch_state_counts.get("deferred", 0):
        return "deferred"
    if review_required_count or review_state_counts.get("pending", 0) or patch_state_counts.get("pending", 0):
        return "pending"
    accepted_count = review_state_counts.get("accepted", 0) + patch_state_counts.get("accepted", 0)
    rejected_count = review_state_counts.get("rejected", 0) + patch_state_counts.get("rejected", 0)
    if accepted_count and rejected_count:
        return "mixed"
    if accepted_count:
        return "accepted"
    if rejected_count:
        return "rejected"
    return "pending"


def build_contradiction_clusters(
    contradictions: list[dict[str, Any]],
    patches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    clusters: dict[tuple[str, str], dict[str, Any]] = {}
    for contradiction in contradictions:
        target_id = contradiction.get("field_id") or contradiction.get("target_id") or contradiction.get("node_id") or contradiction.get("id", "")
        kind = contradiction.get("kind", "") or "unknown"
        key = (target_id, kind)
        cluster = clusters.setdefault(
            key,
            {
                "cluster_id": f"cluster.{slugify_text(target_id or kind)}.{slugify_text(kind)}",
                "target_id": target_id,
                "node_id": contradiction.get("node_id", ""),
                "field_id": contradiction.get("field_id", ""),
                "kind": kind,
                "count": 0,
                "review_required_count": 0,
                "highest_severity": "low",
                "contradiction_ids": [],
                "messages": [],
                "downstream_impacts": [],
                "evidence_sources": [],
                "review_state_counts": {"pending": 0, "accepted": 0, "rejected": 0, "deferred": 0},
            },
        )
        cluster["count"] += 1
        if contradiction.get("review_required", True):
            cluster["review_required_count"] += 1
        severity = contradiction.get("severity", "") or "medium"
        if severity == "high" or (
            severity == "medium" and cluster["highest_severity"] == "low"
        ):
            cluster["highest_severity"] = severity
        if contradiction.get("id") and contradiction["id"] not in cluster["contradiction_ids"]:
            cluster["contradiction_ids"].append(contradiction["id"])
        if contradiction.get("message") and contradiction["message"] not in cluster["messages"]:
            cluster["messages"].append(contradiction["message"])
        for impact in contradiction.get("downstream_impacts", []) or []:
            if impact not in cluster["downstream_impacts"]:
                cluster["downstream_impacts"].append(impact)
        for source in contradiction.get("evidence_sources", []) or []:
            if source not in cluster["evidence_sources"]:
                cluster["evidence_sources"].append(source)
        review_state = contradiction.get("review_state", "pending")
        cluster["review_state_counts"].setdefault(review_state, 0)
        cluster["review_state_counts"][review_state] += 1

    ordered_clusters: list[dict[str, Any]] = []
    for cluster in clusters.values():
        related_patch_ids = find_related_patch_ids_for_targets(
            patches,
            target_ids=[cluster.get("target_id", ""), cluster.get("field_id", "")],
            node_ids=[cluster.get("node_id", "")],
        )
        patch_state_counts = {"pending": 0, "accepted": 0, "rejected": 0, "deferred": 0}
        for patch in patches:
            if patch.get("id") not in related_patch_ids:
                continue
            patch_state = patch.get("review_state", "pending")
            patch_state_counts.setdefault(patch_state, 0)
            patch_state_counts[patch_state] += 1
        cluster["related_patch_ids"] = related_patch_ids
        cluster["patch_state_counts"] = patch_state_counts
        cluster["resolution_state"] = resolve_contradiction_cluster_state(
            cluster["review_state_counts"],
            patch_state_counts,
            review_required_count=cluster["review_required_count"],
        )
        cluster["resolved"] = cluster["resolution_state"] in {"accepted", "rejected"}
        cluster["contradiction_ids"] = sorted(cluster["contradiction_ids"])
        cluster["messages"] = sorted(cluster["messages"])
        cluster["downstream_impacts"] = sorted(cluster["downstream_impacts"])
        cluster["evidence_sources"] = sorted(cluster["evidence_sources"])
        ordered_clusters.append(cluster)
    return sorted(
        ordered_clusters,
        key=lambda item: (
            item.get("target_id", ""),
            item.get("kind", ""),
            item.get("cluster_id", ""),
        ),
    )


def build_contradiction_summary(contradictions: list[dict[str, Any]]) -> dict[str, Any]:
    severity_counts = {"high": 0, "medium": 0, "low": 0}
    kind_counts: dict[str, dict[str, Any]] = {}
    target_counts: dict[str, dict[str, Any]] = {}
    review_required_count = 0
    for contradiction in contradictions:
        severity = contradiction.get("severity", "") or "medium"
        if severity not in severity_counts:
            severity_counts[severity] = 0
        severity_counts[severity] += 1
        if contradiction.get("review_required", True):
            review_required_count += 1
        kind = contradiction.get("kind", "") or "unknown"
        kind_summary = kind_counts.setdefault(
            kind,
            {
                "kind": kind,
                "count": 0,
                "review_required_count": 0,
                "high_severity_count": 0,
                "targets": set(),
            },
        )
        kind_summary["count"] += 1
        if contradiction.get("review_required", True):
            kind_summary["review_required_count"] += 1
        if severity == "high":
            kind_summary["high_severity_count"] += 1
        target_id = contradiction.get("field_id") or contradiction.get("target_id", "")
        if target_id:
            kind_summary["targets"].add(target_id)
            target_summary = target_counts.setdefault(
                target_id,
                {
                    "target_id": target_id,
                    "count": 0,
                    "review_required_count": 0,
                    "highest_severity": "low",
                    "kinds": set(),
                },
            )
            target_summary["count"] += 1
            if contradiction.get("review_required", True):
                target_summary["review_required_count"] += 1
            if severity == "high" or (
                severity == "medium" and target_summary["highest_severity"] == "low"
            ):
                target_summary["highest_severity"] = severity
            target_summary["kinds"].add(kind)

    ordered_kinds = [
        {
            "kind": summary["kind"],
            "count": summary["count"],
            "review_required_count": summary["review_required_count"],
            "high_severity_count": summary["high_severity_count"],
            "targets": sorted(summary["targets"]),
        }
        for summary in sorted(
            kind_counts.values(),
            key=lambda item: (-item["count"], item["kind"]),
        )
    ]
    ordered_targets = [
        {
            "target_id": summary["target_id"],
            "count": summary["count"],
            "review_required_count": summary["review_required_count"],
            "highest_severity": summary["highest_severity"],
            "kinds": sorted(summary["kinds"]),
        }
        for summary in sorted(
            target_counts.values(),
            key=lambda item: (-item["count"], item["target_id"]),
        )
    ]
    return {
        "count": len(contradictions),
        "review_required_count": review_required_count,
        "severity_counts": severity_counts,
        "kinds": ordered_kinds,
        "targets": ordered_targets,
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
    hint_evidence = hint_observation_evidence(hint)
    data_node_id = match_sql_data_node_id(observed_graph, relation)
    if not data_node_id:
        data_node = build_sql_data_node_from_hint(hint)
        data_node["state"] = "observed"
        data_node["verification_state"] = "observed"
        data_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        data_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(data_node)
        data_node_id = data_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=data_node_id,
                node_id=data_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": data_node},
            )
        )
    data_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == data_node_id)
    apply_hint_signal_to_node(data_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        data_node,
        fields,
        role=role,
        evidence=hint_evidence,
        patches=patches,
    )

    data_field_map = {column.get("name"): column for column in data_node.get("columns", [])}
    for field_hint in fields:
        data_column = data_field_map.get(field_hint.get("name", ""))
        if not data_column:
            continue
        lineage_refs = []
        lineage_role = "foreign_key" if object_type == "table" else "sql_input"
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
                lineage_refs.append({"field_id": source_column["id"], "role": lineage_role})
        if lineage_refs:
            data_column["lineage_inputs"] = lineage_refs
        if field_hint.get("unresolved_sources"):
            data_column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))

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
                evidence=hint_evidence,
            )

        return

    compute_node_id = match_sql_compute_node_id(observed_graph, relation)
    if not compute_node_id:
        compute_node = build_sql_compute_node_from_hint(hint, output_node_id=data_node_id)
        compute_node["state"] = "observed"
        compute_node["verification_state"] = "observed"
        compute_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        compute_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(compute_node)
        compute_node_id = compute_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=compute_node_id,
                node_id=compute_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": compute_node},
            )
        )
    compute_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == compute_node_id)
    apply_hint_signal_to_node(compute_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        compute_node,
        fields,
        role=role,
        evidence=hint_evidence,
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
        evidence=hint_evidence,
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
            evidence=hint_evidence,
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


def apply_orm_query_projection_observation(
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
    fields = hint.get("fields", []) or []
    upstream_relations = hint.get("upstream_relations", []) or []
    if not relation or not fields:
        return
    hint_evidence = hint_observation_evidence(hint)

    data_node_id = match_orm_query_data_node_id(observed_graph, relation)
    if not data_node_id:
        data_node = build_orm_query_projection_data_node_from_hint(hint)
        data_node["state"] = "observed"
        data_node["verification_state"] = "observed"
        data_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        data_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(data_node)
        data_node_id = data_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=data_node_id,
                node_id=data_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": data_node},
            )
        )
    data_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == data_node_id)
    apply_hint_signal_to_node(data_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        data_node,
        fields,
        role=role,
        evidence=hint_evidence,
        patches=patches,
    )

    compute_node_id = match_orm_query_compute_node_id(observed_graph, relation)
    if not compute_node_id:
        compute_node = build_orm_query_projection_compute_node_from_hint(hint, output_node_id=data_node_id)
        compute_node["state"] = "observed"
        compute_node["verification_state"] = "observed"
        compute_node["confidence"] = hint.get("confidence", role_default_confidence(role))
        compute_node["evidence"] = hint_evidence
        observed_graph["nodes"].append(compute_node)
        compute_node_id = compute_node["id"]
        patches.append(
            build_patch(
                "add_node",
                target_id=compute_node_id,
                node_id=compute_node_id,
                role=role,
                evidence=hint_evidence,
                payload={"node": compute_node},
            )
        )
    compute_node = next(candidate for candidate in observed_graph["nodes"] if candidate["id"] == compute_node_id)
    apply_hint_signal_to_node(compute_node, hint, role=role)
    ensure_sql_columns(
        observed_graph,
        compute_node,
        fields,
        role=role,
        evidence=hint_evidence,
        patches=patches,
    )
    compute_node.setdefault("compute", {})["runtime"] = "orm"
    compute_node["compute"]["outputs"] = sorted(dict.fromkeys([*compute_node["compute"].get("outputs", []), data_node_id]))

    ensure_edge(
        observed_graph,
        edge_type="produces",
        source_id=compute_node_id,
        target_id=data_node_id,
        patches=patches,
        role=role,
        evidence=hint_evidence,
    )

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
            evidence=hint_evidence,
        )

    compute_field_map = {column.get("name"): column for column in compute_node.get("columns", [])}
    data_field_map = {column.get("name"): column for column in data_node.get("columns", [])}
    for field_hint in fields:
        compute_column = compute_field_map.get(field_hint.get("name", ""))
        data_column = data_field_map.get(field_hint.get("name", ""))
        lineage_refs: list[dict[str, Any]] = []
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
                lineage_refs.append({"field_id": source_column["id"], "role": "orm_input"})
        if lineage_refs:
            if compute_column is not None:
                compute_column["lineage_inputs"] = lineage_refs
            if data_column is not None:
                data_column["lineage_inputs"] = deepcopy(lineage_refs)
        if field_hint.get("unresolved_sources"):
            if compute_column is not None:
                compute_column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))
            if data_column is not None:
                data_column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))


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
            if field_hint.get("confidence") and CONFIDENCE_RANK.get(field_hint["confidence"], 0) != CONFIDENCE_RANK.get(column.get("confidence", role_default_confidence(role)), 0):
                column["confidence"] = field_hint["confidence"]
            if field_hint.get("evidence"):
                column["evidence"] = sorted(dict.fromkeys([*column.get("evidence", []), *field_hint.get("evidence", [])]))
            if "unresolved_sources" in field_hint:
                column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))
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
            "confidence": field_hint.get("confidence", role_default_confidence(role)),
            "evidence": sorted(dict.fromkeys([*evidence, *(field_hint.get("evidence", []) or [])])),
            "required": node["kind"] == "compute",
            "lineage_inputs": [],
            "history": [],
        }
        for metadata_key in ("primary_key", "foreign_key", "nullable", "index", "unique"):
            if metadata_key in field_hint:
                column[metadata_key] = field_hint[metadata_key]
        if "unresolved_sources" in field_hint:
            column["unresolved_sources"] = deep_sort(field_hint.get("unresolved_sources", []))
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
        sections.append(
            {
                "level": len(match.group(1)),
                "title": normalize_markdown_value(match.group(2)),
                "body": text[start:end].strip(),
            }
        )
    return sections


def parse_markdown_section_descriptor(title: str, body: str) -> dict[str, Any] | None:
    api_match = MARKDOWN_API_HEADING_RE.match(title)
    if api_match:
        route = f"{api_match.group(1).upper()} {api_match.group(2)}"
        return {
            "kind": "api",
            "route": route,
            "label": route,
            "fields": extract_markdown_field_specs(body, contract_kind="contract"),
        }

    route = extract_markdown_route(title) or extract_markdown_route(body)
    if route:
        return {
            "kind": "api",
            "route": route,
            "label": route,
            "fields": extract_markdown_field_specs(body, contract_kind="contract"),
        }

    ui_match = MARKDOWN_UI_HEADING_RE.match(title)
    if ui_match:
        component = normalize_markdown_value(ui_match.group(1))
        return {
            "kind": "ui",
            "component": component,
            "label": component,
            "fields": extract_markdown_field_specs(body, contract_kind="contract"),
        }

    data_match = MARKDOWN_DATA_HEADING_RE.match(title)
    if data_match:
        prefix = data_match.group("prefix").lower().replace("_", " ")
        relation = normalize_markdown_value(data_match.group("value"))
        object_type = "materialized_view" if "materialized" in prefix else "view" if prefix == "view" else "table"
        return {
            "kind": "data",
            "relation": relation,
            "object_type": object_type,
            "fields": extract_markdown_field_specs(body, contract_kind="data"),
        }

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
    skipped_prefixes = (
        "inputs",
        "outputs",
        "produces",
        "writes",
        "reads",
        "sources",
        "depends on",
        "upstream",
    )
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
    arrow_markers = ("<-", "->")
    marker = next((item for item in arrow_markers if item in text), "")
    if marker:
        left, right = [item.strip() for item in text.split(marker, 1)]
        if marker == "->":
            text = left
            binding_text = right
        else:
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

    sources = [
        source
        for source in (parse_markdown_binding_source(item) for item in split_markdown_inline_items(binding_text))
        if source is not None
    ]
    return {"name": name, "required": required, "data_type": data_type, "sources": sources}


def split_markdown_inline_items(value: str) -> list[str]:
    if not value:
        return []
    return [
        normalize_markdown_value(item)
        for item in re.split(r"[;,]|\s+\band\b\s+", value, flags=re.IGNORECASE)
        if normalize_markdown_value(item)
    ]


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
        return {
            "relation": relation,
            "node_id": f"data:{slugify_text(relation)}",
            "column": column_name,
        }
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


def ensure_markdown_relation_node(
    graph: dict[str, Any],
    relation: str,
    node_id: str,
    *,
    object_type: str = "table",
) -> str:
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
    node.setdefault("columns", []).append(
        {
            "id": make_field_id(node["id"], column_name, existing_ids),
            "name": column_name,
            "data_type": "unknown",
        }
    )


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


def collect_document_candidates(root_dir: Path, doc_paths: list[str]) -> list[dict[str, Any]]:
    root_dir = root_dir.resolve()
    candidates: list[dict[str, Any]] = []
    paths: list[Path] = []
    seen_paths: set[str] = set()

    def add_path(path: Path) -> None:
        resolved = path.resolve()
        normalized = str(resolved)
        if normalized in seen_paths:
            return
        seen_paths.add(normalized)
        paths.append(resolved)

    if doc_paths:
        for entry in doc_paths:
            add_path(resolve_doc_path(root_dir, entry))
    else:
        docs_dir = root_dir / "docs"
        if docs_dir.exists():
            for pattern in ("*.md", "*.yaml", "*.yml", "*.json"):
                for path in sorted(docs_dir.rglob(pattern)):
                    add_path(path)
        latest_plan_path = root_dir / "runtime" / "plans" / "latest.plan.md"
        if latest_plan_path.exists():
            add_path(latest_plan_path)
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
        if path.suffix.lower() == ".md":
            partial_graph = parse_markdown_hybrid_plan(path, text)
            if partial_graph:
                candidates.append({"type": "partial_graph", "path": str(path), "graph": partial_graph})
                continue
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


def build_orm_query_projection_data_node_from_hint(hint: dict[str, Any]) -> dict[str, Any]:
    relation = hint.get("relation") or "query_projection"
    label = humanize_orm_query_projection_name(hint)
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"data:{slugify_text(relation)}",
                    "kind": "data",
                    "extension_type": "feature_set",
                    "label": label,
                    "description": hint.get("description", "") or f"Observed ORM query result for {relation}.",
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"orm_query_projection:{relation}", "orm_query_result"],
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
                        "persistence": "transient",
                        "local_path": hint.get("file", ""),
                        "update_frequency": "",
                        "persisted": False,
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


def build_orm_query_projection_compute_node_from_hint(hint: dict[str, Any], *, output_node_id: str) -> dict[str, Any]:
    relation = hint.get("relation") or "query_projection"
    label = f"Build {humanize_orm_query_projection_name(hint)}"
    return normalize_graph(
        {
            "metadata": {"name": "Observed"},
            "nodes": [
                {
                    "id": f"compute:transform.{slugify_text(relation)}",
                    "kind": "compute",
                    "extension_type": "transform",
                    "label": label,
                    "description": hint.get("description", "") or f"Observed ORM query projection for {relation}.",
                    "owner": "repo-scan",
                    "profile_status": "schema_only",
                    "tags": [f"orm_query_projection:{relation}", "orm_transform"],
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
                        "runtime": "orm",
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
    confidence: str | None = None,
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
        "confidence": confidence or role_default_confidence(role),
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
        "kind": "binding_mismatch",
        "severity": "high" if impacts else "medium",
        "existing_belief": {"primary_binding": winner_binding},
        "new_evidence": {"primary_binding": suggested_binding},
        "affected_refs": [field["id"], winner_binding, suggested_binding],
        "confidence_delta": "down",
        "downstream_impacts": impacts,
        "evidence_sources": ["canonical", "implementation"],
        "message": f"{field['name']} maps to {existing_display or winner_binding}, but scan suggests {suggested_display or suggested_binding}.",
        "why_this_matters": impacts[0] if impacts else "Column-level structure no longer matches the latest observed implementation.",
        "review_required": True,
    }


def apply_patch_to_graph(
    graph: dict[str, Any],
    patch: dict[str, Any],
    *,
    merged_by: str,
    strict: bool = False,
) -> tuple[bool, str]:
    patch_type = patch.get("type")
    payload = patch.get("payload", {})
    if patch_type == "add_node":
        node = payload.get("node")
        if not node:
            return False, "Accepted add_node patch is missing node payload."
        if not any(candidate["id"] == node["id"] for candidate in graph.get("nodes", [])):
            graph["nodes"].append(deepcopy(node))
        return True, ""
    if patch_type == "add_edge":
        edge = payload.get("edge")
        if not edge:
            return False, "Accepted add_edge patch is missing edge payload."
        node_ids = {node.get("id") for node in graph.get("nodes", [])}
        if edge.get("source") not in node_ids or edge.get("target") not in node_ids:
            return False, f"Accepted add_edge patch {patch['id']} depends on nodes that are not present in the merge graph."
        if not any(candidate["id"] == edge["id"] for candidate in graph.get("edges", [])):
            graph["edges"].append(deepcopy(edge))
        return True, ""
    if patch_type == "add_field":
        node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == patch.get("node_id")), None)
        field = payload.get("field")
        if not node or not field:
            return False, f"Accepted add_field patch {patch['id']} targets a node or field that is missing from the merge graph."
        target_list = node["contract"]["fields"] if node["kind"] == "contract" else node["columns"]
        if any(candidate["id"] == field["id"] for candidate in target_list):
            return True, ""
        name_conflict = next((candidate for candidate in target_list if candidate.get("name") == field.get("name")), None)
        if name_conflict is not None:
            return False, (
                f"Accepted add_field patch {patch['id']} would duplicate "
                f"{patch.get('node_id', '')}.{field.get('name', '')} in canonical YAML."
            )
        if not any(candidate["id"] == field["id"] for candidate in target_list):
            target_list.append(deepcopy(field))
        return True, ""
    if patch_type == "add_binding":
        return update_field_binding(
            graph,
            patch.get("field_id", ""),
            payload.get("primary_binding", ""),
            payload.get("alternatives", []),
            merged_by,
            strict=strict,
        )
    if patch_type == "change_binding":
        return update_field_binding(
            graph,
            patch.get("field_id", ""),
            payload.get("new_binding", ""),
            payload.get("alternatives", []),
            merged_by,
            previous_binding=payload.get("previous_binding", ""),
            strict=strict,
        )
    if patch_type == "confidence_change":
        update_field_confidence(graph, patch.get("field_id", ""), payload.get("confidence", ""), merged_by, explicit_override=True)
        return True, ""
    if patch_type == "remove_field":
        return mark_field_removed(graph, patch.get("field_id", ""), merged_by)
    if patch_type == "remove_node":
        return mark_node_removed(graph, patch.get("node_id", ""), merged_by)
    if patch_type == "remove_edge":
        edge = next((candidate for candidate in graph.get("edges", []) if candidate["id"] == patch.get("edge_id")), None)
        if edge is None:
            return False, f"Accepted remove_edge patch {patch['id']} targets an edge that is missing from canonical YAML."
        edge["removed"] = True
        edge["history"] = [*edge.get("history", []), {"change": "remove_edge", "at": utc_timestamp(), "by": merged_by}]
        return True, ""
    if patch_type == "contradiction":
        return True, ""
    return False, f"Unsupported merge patch type: {patch_type}"


def update_field_binding(
    graph: dict[str, Any],
    field_id: str,
    primary_binding: str,
    alternatives: list[str],
    merged_by: str,
    *,
    previous_binding: str = "",
    strict: bool = False,
) -> tuple[bool, str]:
    node, field = resolve_field(graph, field_id)
    if not node or not field:
        return False, f"Accepted binding patch targets a field that is missing from canonical YAML ({field_id})."
    current = field.get("primary_binding", "")
    if previous_binding and current != previous_binding:
        return False, (
            f"Accepted binding patch for {field_id} expected {previous_binding or 'unbound'}, "
            f"but canonical YAML currently has {current or 'unbound'}."
        )
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
    return True, ""


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


def mark_field_removed(graph: dict[str, Any], field_id: str, merged_by: str) -> tuple[bool, str]:
    _, field = resolve_field(graph, field_id)
    if not field:
        return False, f"Accepted remove_field patch targets a field that is missing from canonical YAML ({field_id})."
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
    return True, ""


def mark_node_removed(graph: dict[str, Any], node_id: str, merged_by: str) -> tuple[bool, str]:
    node = next((candidate for candidate in graph.get("nodes", []) if candidate["id"] == node_id), None)
    if not node:
        return False, f"Accepted remove_node patch targets a node that is missing from canonical YAML ({node_id})."
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
    return True, ""


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


def match_orm_query_data_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"orm_query_projection:{relation}"
    normalized_relation = slugify_text(relation)
    for node in graph.get("nodes", []):
        if node["kind"] != "data" or node.get("removed"):
            continue
        if tag in (node.get("tags") or []):
            return node["id"]
        if slugify_text(node.get("id", "").split(":", 1)[-1]) == normalized_relation:
            return node["id"]
    return ""


def match_orm_query_compute_node_id(graph: dict[str, Any], relation: str) -> str:
    tag = f"orm_query_projection:{relation}"
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


def humanize_orm_query_projection_name(hint: dict[str, Any]) -> str:
    query_context = hint.get("query_context", {}) or {}
    function_name = query_context.get("function", "")
    variable_name = query_context.get("variable", "")
    if function_name or variable_name:
        label = " ".join(part for part in [function_name, variable_name] if part)
        return label.replace("_", " ").replace("-", " ").title()
    relation = hint.get("relation") or hint.get("label") or "Query Projection"
    return relation.split(".")[-1].replace("_", " ").replace("-", " ").title()


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
