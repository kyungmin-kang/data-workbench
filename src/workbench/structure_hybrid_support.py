from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any

from .diagnostics import describe_downstream_component_impacts
from .structure_blueprints import slugify_text
from .structure_candidates import extract_sql_relation_tag
from .structure_reconciliation import CONFIDENCE_RANK, role_default_confidence
from .types import column_ref, normalize_graph, resolve_field_reference


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
