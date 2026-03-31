from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .types import PATCH_TYPE_PRECEDENCE, ScanBundle, normalize_graph


ROOT_DIR = Path(__file__).resolve().parents[2]


def get_root_dir() -> Path:
    override = os.environ.get("WORKBENCH_ROOT_DIR")
    return Path(override) if override else ROOT_DIR


def get_structure_dir() -> Path:
    return get_root_dir() / "specs" / "structure"


def get_spec_path() -> Path:
    return get_structure_dir() / "spec.yaml"


def get_legacy_spec_path() -> Path:
    return get_root_dir() / "specs" / "workbench.graph.json"


def get_bundles_dir() -> Path:
    return get_structure_dir() / "bundles"


def get_plans_dir() -> Path:
    return get_root_dir() / "runtime" / "plans"


def get_cache_dir() -> Path:
    return get_root_dir() / "runtime" / "cache"


def get_onboarding_presets_path() -> Path:
    return get_root_dir() / "specs" / "onboarding_presets.json"


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_graph(path: Path | None = None) -> dict[str, Any]:
    path = path or get_spec_path()
    if path.suffix in {".yaml", ".yml"} and path.exists():
        with path.open(encoding="utf-8") as file:
            payload = yaml.safe_load(file) or {}
        legacy_path = get_legacy_spec_path()
        if legacy_path.exists():
            with legacy_path.open(encoding="utf-8") as file:
                legacy_payload = json.load(file)
            _merge_legacy_runtime_hints(payload, legacy_payload)
        return normalize_graph(payload)

    if path.exists():
        with path.open(encoding="utf-8") as file:
            return normalize_graph(json.load(file))

    legacy_path = get_legacy_spec_path()
    if legacy_path.exists():
        with legacy_path.open(encoding="utf-8") as file:
            graph = normalize_graph(json.load(file))
        save_graph(graph, updated_by="migration", increment_version=False)
        return graph
    return normalize_graph({})


def save_graph(
    graph: dict[str, Any],
    path: Path | None = None,
    *,
    updated_by: str = "user",
    increment_version: bool = True,
) -> dict[str, Any]:
    path = path or get_spec_path()
    graph = normalize_graph(graph)
    previous_version = int(graph.get("metadata", {}).get("structure_version") or 1)
    graph["metadata"]["updated_at"] = utc_timestamp()
    graph["metadata"]["updated_by"] = updated_by
    if increment_version:
        graph["metadata"]["structure_version"] = previous_version + 1
    else:
        graph["metadata"]["structure_version"] = previous_version

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(
            canonical_yaml_payload(graph),
            file,
            sort_keys=False,
            allow_unicode=False,
            width=120,
        )

    legacy_path = get_legacy_spec_path()
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    with legacy_path.open("w", encoding="utf-8") as file:
        json.dump(graph, file, indent=2, ensure_ascii=True)
        file.write("\n")
    return graph


def canonical_yaml_payload(graph: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(graph))
    for node in payload.get("nodes", []):
        for field in node.get("contract", {}).get("fields", []):
            field.pop("sources", None)
        for feature in node.get("compute", {}).get("feature_selection", []):
            feature.pop("column_ref", None)
        node["columns"] = _stable_sorted(node.get("columns", []))
        node["contract"]["fields"] = _stable_sorted(node.get("contract", {}).get("fields", []))
        node["compute"]["feature_selection"] = sorted(
            node.get("compute", {}).get("feature_selection", []),
            key=lambda item: (
                item.get("order") if item.get("order") is not None else 10_000,
                item.get("field_id", ""),
                item.get("status", ""),
            ),
        )
    payload["nodes"] = _stable_sorted(payload.get("nodes", []))
    payload["edges"] = _stable_sorted(payload.get("edges", []))
    return payload


def write_plan_artifacts(plan: dict[str, Any]) -> dict[str, str]:
    plans_dir = get_plans_dir()
    root_dir = get_root_dir()
    plans_dir.mkdir(parents=True, exist_ok=True)
    timestamp = utc_timestamp().replace(":", "-")
    latest_json_path = plans_dir / "latest.plan.json"
    latest_markdown_path = plans_dir / "latest.plan.md"
    timestamped_json_path = plans_dir / f"{timestamp}.plan.json"
    timestamped_markdown_path = plans_dir / f"{timestamp}.plan.md"

    for json_path in (latest_json_path, timestamped_json_path):
        with json_path.open("w", encoding="utf-8") as file:
            json.dump(plan, file, indent=2, ensure_ascii=True)
            file.write("\n")
    for markdown_path in (latest_markdown_path, timestamped_markdown_path):
        with markdown_path.open("w", encoding="utf-8") as file:
            file.write(plan["markdown"])

    return {
        "latest_json": str(latest_json_path.relative_to(root_dir)),
        "latest_markdown": str(latest_markdown_path.relative_to(root_dir)),
        "timestamped_json": str(timestamped_json_path.relative_to(root_dir)),
        "timestamped_markdown": str(timestamped_markdown_path.relative_to(root_dir)),
    }


def load_latest_plan() -> dict[str, Any] | None:
    latest_json_path = get_plans_dir() / "latest.plan.json"
    if not latest_json_path.exists():
        return None
    with latest_json_path.open(encoding="utf-8") as file:
        return json.load(file)


def save_bundle(bundle: dict[str, Any] | ScanBundle) -> dict[str, Any]:
    normalized = ScanBundle.model_validate(bundle).model_dump(mode="json")
    review = normalized.setdefault("review", {})
    for key in ("accepted_patch_ids", "rejected_patch_ids", "deferred_patch_ids"):
        review[key] = sorted(dict.fromkeys(review.get(key, [])))
    normalized["patches"] = sorted(
        normalized.get("patches", []),
        key=lambda item: (
            item.get("target_id", ""),
            PATCH_TYPE_PRECEDENCE.get(item.get("type", ""), 999),
            json.dumps(item.get("payload", {}), sort_keys=True),
        ),
    )
    normalized["contradictions"] = sorted(
        normalized.get("contradictions", []),
        key=lambda item: (item.get("target_id", ""), item.get("id", "")),
    )
    normalized["impacts"] = sorted(
        normalized.get("impacts", []),
        key=lambda item: (item.get("target_id", ""), item.get("message", "")),
    )
    normalized["reconciliation"] = _normalize_reconciliation(normalized.get("reconciliation", {}))
    bundles_dir = get_bundles_dir()
    bundles_dir.mkdir(parents=True, exist_ok=True)
    path = bundles_dir / f"{normalized['bundle_id']}.yaml"
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(normalized, file, sort_keys=False, allow_unicode=False, width=120)
    return normalized


def load_bundle(bundle_id: str) -> dict[str, Any] | None:
    path = get_bundles_dir() / f"{bundle_id}.yaml"
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as file:
        payload = yaml.safe_load(file) or {}
    return ScanBundle.model_validate(payload).model_dump(mode="json")


def list_bundles() -> list[dict[str, Any]]:
    bundles_dir = get_bundles_dir()
    if not bundles_dir.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(bundles_dir.glob("*.yaml")):
        with path.open(encoding="utf-8") as file:
            payload = yaml.safe_load(file) or {}
        bundle = ScanBundle.model_validate(payload).model_dump(mode="json")
        review = bundle.get("review", {})
        items.append(
            {
                "bundle_id": bundle["bundle_id"],
                "role": bundle.get("scan", {}).get("role", "scout"),
                "scope": bundle.get("scan", {}).get("scope", "full"),
                "base_structure_version": bundle.get("base_structure_version", 1),
                "created_at": bundle.get("scan", {}).get("created_at", ""),
                "patch_count": len(bundle.get("patches", [])),
                "pending_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "pending"),
                "accepted_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "accepted"),
                "rejected_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "rejected"),
                "deferred_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "deferred"),
                "contradiction_count": len(bundle.get("contradictions", [])),
                "review_required_count": sum(1 for item in bundle.get("contradictions", []) if item.get("review_required", True)),
                "high_severity_contradiction_count": bundle.get("reconciliation", {}).get("contradiction_summary", {}).get("severity_counts", {}).get("high", 0),
                "contradiction_cluster_count": len(bundle.get("reconciliation", {}).get("contradiction_clusters", [])),
                "open_contradiction_cluster_count": bundle.get("reconciliation", {}).get("contradiction_cluster_summary", {}).get("open_count", 0),
                "resolved_contradiction_cluster_count": bundle.get("reconciliation", {}).get("contradiction_cluster_summary", {}).get("resolved_count", 0),
                "mixed_contradiction_cluster_count": bundle.get("reconciliation", {}).get("contradiction_cluster_summary", {}).get("resolution_state_counts", {}).get("mixed", 0),
                "field_matrix_review_required_count": bundle.get("reconciliation", {}).get("field_matrix_summary", {}).get("review_required_count", 0),
                "bundle_owner": review.get("bundle_owner", ""),
                "assigned_reviewer": review.get("assigned_reviewer", ""),
                "triage_state": review.get("triage_state", "new"),
                "triage_note": review.get("triage_note", ""),
                "last_reviewed_at": review.get("last_reviewed_at", ""),
                "last_reviewed_by": review.get("last_reviewed_by", ""),
                "merged_at": review.get("merged_at", ""),
                "merged_by": review.get("merged_by", ""),
                "merge_status": review.get("merge_status", ""),
                "rebase_required": bool(review.get("rebase_required", False)),
                "rebased_from_bundle_id": review.get("rebased_from_bundle_id", ""),
                "superseded_by_bundle_id": review.get("superseded_by_bundle_id", ""),
                "merge_plan_status": review.get("merge_plan", {}).get("status", ""),
                "merge_plan_noop_count": review.get("merge_plan", {}).get("noop_count", 0),
                "merge_plan_blocked_step_count": review.get("merge_plan", {}).get("blocked_step_count", 0),
                "ready_to_merge": bool(review.get("merge_patch_ids"))
                and sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "pending") == 0
                and sum(1 for item in bundle.get("contradictions", []) if item.get("review_required", True)) == 0
                and not bool(review.get("rebase_required", False))
                and not bool(review.get("superseded_by_bundle_id", ""))
                and review.get("merge_plan", {}).get("status", "") in {"ready", "empty"},
                "readiness_status": bundle.get("readiness", {}).get("status", "Not Ready"),
                "planned_missing_count": bundle.get("reconciliation", {}).get("summary", {}).get("planned_missing", 0),
                "observed_untracked_count": bundle.get("reconciliation", {}).get("summary", {}).get("observed_untracked", 0),
                "implemented_differently_count": bundle.get("reconciliation", {}).get("summary", {}).get("implemented_differently", 0),
                "uncertain_matches_count": bundle.get("reconciliation", {}).get("summary", {}).get("uncertain_matches", 0),
                "binding_mismatch_count": bundle.get("reconciliation", {}).get("comparison", {}).get("binding_mismatches", 0),
                "column_mismatch_count": bundle.get("reconciliation", {}).get("comparison", {}).get("column_mismatches", 0),
                "downstream_breakage_count": bundle.get("reconciliation", {}).get("downstream_breakage", {}).get("count", 0),
            }
        )
    return items


def export_canonical_yaml_text() -> str:
    graph = load_graph()
    return yaml.safe_dump(canonical_yaml_payload(graph), sort_keys=False, allow_unicode=False, width=120)


def _stable_sorted(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: item.get("id", item.get("name", "")))


def _normalize_reconciliation(reconciliation: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(reconciliation, dict):
        return {}
    normalized = json.loads(json.dumps(reconciliation))
    summary = normalized.get("summary", {})
    normalized["summary"] = {
        "planned_missing": int(summary.get("planned_missing", 0) or 0),
        "observed_untracked": int(summary.get("observed_untracked", 0) or 0),
        "implemented_differently": int(summary.get("implemented_differently", 0) or 0),
        "uncertain_matches": int(summary.get("uncertain_matches", 0) or 0),
    }
    comparison = normalized.get("comparison", {})
    normalized["comparison"] = {
        "plan_candidates": int(comparison.get("plan_candidates", 0) or 0),
        "plan_paths": sorted(comparison.get("plan_paths", []) or []),
        "planned_nodes": int(comparison.get("planned_nodes", 0) or 0),
        "matched_nodes": int(comparison.get("matched_nodes", 0) or 0),
        "planned_fields": int(comparison.get("planned_fields", 0) or 0),
        "matched_fields": int(comparison.get("matched_fields", 0) or 0),
        "planned_bindings": int(comparison.get("planned_bindings", 0) or 0),
        "matched_bindings": int(comparison.get("matched_bindings", 0) or 0),
        "binding_mismatches": int(comparison.get("binding_mismatches", 0) or 0),
        "column_mismatches": int(comparison.get("column_mismatches", 0) or 0),
        "missing_fields": int(comparison.get("missing_fields", 0) or 0),
        "unplanned_fields": int(comparison.get("unplanned_fields", 0) or 0),
    }
    field_matrix_summary = normalized.get("field_matrix_summary", {})
    field_matrix_status_counts = field_matrix_summary.get("status_counts", {}) if isinstance(field_matrix_summary, dict) else {}
    normalized["field_matrix_summary"] = {
        "count": int(field_matrix_summary.get("count", 0) or 0),
        "review_required_count": int(field_matrix_summary.get("review_required_count", 0) or 0),
        "unresolved_count": int(field_matrix_summary.get("unresolved_count", 0) or 0),
        "implemented_differently_count": int(field_matrix_summary.get("implemented_differently_count", 0) or 0),
        "status_counts": {
            "matched": int(field_matrix_status_counts.get("matched", 0) or 0),
            "planned_missing": int(field_matrix_status_counts.get("planned_missing", 0) or 0),
            "observed_unplanned": int(field_matrix_status_counts.get("observed_unplanned", 0) or 0),
            "binding_mismatch": int(field_matrix_status_counts.get("binding_mismatch", 0) or 0),
            "lineage_mismatch": int(field_matrix_status_counts.get("lineage_mismatch", 0) or 0),
            "column_type_mismatch": int(field_matrix_status_counts.get("column_type_mismatch", 0) or 0),
            "uncertain": int(field_matrix_status_counts.get("uncertain", 0) or 0),
        },
    }
    normalized["field_matrix"] = sorted(
        normalized.get("field_matrix", []),
        key=lambda item: (
            item.get("scope", ""),
            item.get("node_ref", ""),
            item.get("field_name", ""),
            item.get("row_id", ""),
        ),
    )
    contradiction_summary = normalized.get("contradiction_summary", {})
    severity_counts = contradiction_summary.get("severity_counts", {}) if isinstance(contradiction_summary, dict) else {}
    normalized["contradiction_summary"] = {
        "count": int(contradiction_summary.get("count", 0) or 0),
        "review_required_count": int(contradiction_summary.get("review_required_count", 0) or 0),
        "severity_counts": {
            "high": int(severity_counts.get("high", 0) or 0),
            "medium": int(severity_counts.get("medium", 0) or 0),
            "low": int(severity_counts.get("low", 0) or 0),
        },
        "kinds": sorted(
            contradiction_summary.get("kinds", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("kind", ""),
            ),
        ),
        "targets": sorted(
            contradiction_summary.get("targets", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("target_id", ""),
            ),
        ),
    }
    normalized["contradiction_clusters"] = sorted(
        normalized.get("contradiction_clusters", []),
        key=lambda item: (
            item.get("target_id", ""),
            item.get("kind", ""),
            item.get("cluster_id", ""),
        ),
    )
    contradiction_cluster_summary = normalized.get("contradiction_cluster_summary", {})
    cluster_state_counts = contradiction_cluster_summary.get("resolution_state_counts", {}) if isinstance(contradiction_cluster_summary, dict) else {}
    normalized["contradiction_cluster_summary"] = {
        "count": int(contradiction_cluster_summary.get("count", 0) or 0),
        "open_count": int(contradiction_cluster_summary.get("open_count", 0) or 0),
        "resolved_count": int(contradiction_cluster_summary.get("resolved_count", 0) or 0),
        "high_risk_open_count": int(contradiction_cluster_summary.get("high_risk_open_count", 0) or 0),
        "resolution_state_counts": {
            "pending": int(cluster_state_counts.get("pending", 0) or 0),
            "accepted": int(cluster_state_counts.get("accepted", 0) or 0),
            "rejected": int(cluster_state_counts.get("rejected", 0) or 0),
            "deferred": int(cluster_state_counts.get("deferred", 0) or 0),
            "mixed": int(cluster_state_counts.get("mixed", 0) or 0),
        },
    }
    downstream_breakage = normalized.get("downstream_breakage", {})
    normalized["downstream_breakage"] = {
        "count": int(downstream_breakage.get("count", 0) or 0),
        "items": sorted(
            downstream_breakage.get("items", []),
            key=lambda item: (
                item.get("target_id", ""),
                item.get("message", ""),
                item.get("source", ""),
            ),
        ),
        "by_source": sorted(
            downstream_breakage.get("by_source", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("source", ""),
            ),
        ),
        "targets": sorted(
            downstream_breakage.get("targets", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("target_id", ""),
            ),
        ),
    }
    for key in ("planned_missing", "observed_untracked", "implemented_differently", "uncertain_matches"):
        normalized[key] = sorted(
            normalized.get(key, []),
            key=lambda item: (
                item.get("target_id", ""),
                item.get("field_id", ""),
                item.get("label", ""),
                item.get("message", ""),
            ),
        )
    return normalized


def _merge_legacy_runtime_hints(payload: dict[str, Any], legacy_payload: dict[str, Any]) -> None:
    legacy_nodes = {node.get("id"): node for node in legacy_payload.get("nodes", [])}
    legacy_edges = {edge.get("id"): edge for edge in legacy_payload.get("edges", [])}

    for node in payload.get("nodes", []):
        legacy_node = legacy_nodes.get(node.get("id"))
        if not legacy_node:
            continue

        _merge_compute_feature_selection(node, legacy_node)
        _merge_columns(node, legacy_node)
        _merge_contract_fields(node, legacy_node)

    for edge in payload.get("edges", []):
        legacy_edge = legacy_edges.get(edge.get("id"))
        if not legacy_edge:
            continue
        legacy_mappings = legacy_edge.get("column_mappings", [])
        mappings = edge.get("column_mappings", [])
        for index, mapping in enumerate(mappings):
            if index >= len(legacy_mappings):
                break
            legacy_mapping = legacy_mappings[index]
            if not mapping.get("source_field_id") and legacy_mapping.get("source_field_id"):
                mapping["source_field_id"] = legacy_mapping["source_field_id"]
            if not mapping.get("target_field_id") and legacy_mapping.get("target_field_id"):
                mapping["target_field_id"] = legacy_mapping["target_field_id"]


def _merge_columns(node: dict[str, Any], legacy_node: dict[str, Any]) -> None:
    legacy_columns = {column.get("name"): column for column in legacy_node.get("columns", [])}
    for column in node.get("columns", []):
        legacy_column = legacy_columns.get(column.get("name"))
        if not legacy_column:
            continue
        if not column.get("lineage_inputs") and legacy_column.get("lineage_inputs"):
            column["lineage_inputs"] = legacy_column.get("lineage_inputs", [])
        if not column.get("id") and legacy_column.get("id"):
            column["id"] = legacy_column["id"]


def _merge_compute_feature_selection(node: dict[str, Any], legacy_node: dict[str, Any]) -> None:
    features = node.get("compute", {}).get("feature_selection", [])
    legacy_features = legacy_node.get("compute", {}).get("feature_selection", [])
    for index, feature in enumerate(features):
        if index >= len(legacy_features):
            break
        legacy_feature = legacy_features[index]
        if not feature.get("field_id") and legacy_feature.get("field_id"):
            feature["field_id"] = legacy_feature["field_id"]
        if not feature.get("column_ref") and legacy_feature.get("column_ref"):
            feature["column_ref"] = legacy_feature["column_ref"]


def _merge_contract_fields(node: dict[str, Any], legacy_node: dict[str, Any]) -> None:
    legacy_fields = {field.get("name"): field for field in legacy_node.get("contract", {}).get("fields", [])}
    for field in node.get("contract", {}).get("fields", []):
        legacy_field = legacy_fields.get(field.get("name"))
        if not legacy_field:
            continue
        if not field.get("id") and legacy_field.get("id"):
            field["id"] = legacy_field["id"]
        if not field.get("primary_binding") and legacy_field.get("primary_binding"):
            field["primary_binding"] = legacy_field["primary_binding"]
        if not field.get("alternatives") and legacy_field.get("alternatives"):
            field["alternatives"] = legacy_field.get("alternatives", [])
        if not field.get("sources") and legacy_field.get("sources"):
            field["sources"] = legacy_field.get("sources", [])
