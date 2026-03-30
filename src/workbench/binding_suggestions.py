from __future__ import annotations

import re
from typing import Any


CAMEL_CASE_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def collect_contract_field_candidates(
    graph: dict[str, Any], contract_node_id: str, field_name: str, *, limit: int = 8
) -> list[dict[str, Any]]:
    contract_node = next((node for node in graph.get("nodes", []) if node["id"] == contract_node_id), None)
    if not contract_node or contract_node["kind"] != "contract":
        return []

    if contract_node["extension_type"] == "api":
        candidates = _collect_api_field_candidates(graph, contract_node_id, field_name)
    else:
        candidates = _collect_ui_field_candidates(graph, contract_node_id, field_name)
    return candidates[:limit]


def suggest_contract_field_source(graph: dict[str, Any], contract_node_id: str, field_name: str) -> dict[str, Any] | None:
    candidates = collect_contract_field_candidates(graph, contract_node_id, field_name, limit=2)
    if not candidates:
        return None
    if len(candidates) == 1 or candidates[0]["score"] > candidates[1]["score"]:
        return candidates[0]
    return None


def format_source_ref(source: dict[str, Any]) -> str:
    if source.get("column"):
        return f"{source['node_id']}.{source['column']}"
    if source.get("field"):
        return f"{source['node_id']}.{source['field']}"
    return source.get("node_id", "")


def normalize_binding_name(value: str) -> str:
    split = CAMEL_CASE_BOUNDARY_RE.sub("_", value or "")
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", split).strip("_").lower()
    return normalized


def _collect_api_field_candidates(graph: dict[str, Any], contract_node_id: str, field_name: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for node in graph.get("nodes", []):
        if node["id"] == contract_node_id or node["kind"] not in {"data", "compute"}:
            continue
        for column in node.get("columns", []):
            column_name = column["name"]
            base_score = _binding_name_score(field_name, column_name)
            if base_score <= 0:
                continue
            score = base_score + _api_node_score(node)
            candidates.append(
                {
                    "ref": f"{node['id']}.{column_name}",
                    "score": score,
                    "reason": _api_candidate_reason(node),
                    "source": {"node_id": node["id"], "column": column_name},
                }
            )
    return sorted(candidates, key=lambda item: (-item["score"], item["ref"]))


def _collect_ui_field_candidates(graph: dict[str, Any], contract_node_id: str, field_name: str) -> list[dict[str, Any]]:
    bound_api_ids = {
        edge["source"]
        for edge in graph.get("edges", [])
        if edge["type"] == "binds" and edge["target"] == contract_node_id
    }
    candidates: list[dict[str, Any]] = []
    for node in graph.get("nodes", []):
        if node["kind"] != "contract" or node["extension_type"] != "api":
            continue
        route = node.get("contract", {}).get("route", "")
        for field in node.get("contract", {}).get("fields", []):
            field_candidate = field["name"]
            base_score = _binding_name_score(field_name, field_candidate)
            if base_score <= 0:
                continue
            if (
                normalize_binding_name(field_name) != normalize_binding_name(field_candidate)
                and _binding_token_overlap(field_name, field_candidate) < 2
            ):
                continue
            score = base_score + 40
            if node["id"] in bound_api_ids:
                score += 60
            if route:
                score += 5
            candidates.append(
                {
                    "ref": f"{node['id']}.{field_candidate}",
                    "score": score,
                    "reason": "Existing API contract field" + (" on a bound API edge" if node["id"] in bound_api_ids else ""),
                    "source": {"node_id": node["id"], "field": field_candidate},
                }
            )
    return sorted(candidates, key=lambda item: (-item["score"], item["ref"]))


def _binding_name_score(left: str, right: str) -> int:
    if left == right:
        return 220

    normalized_left = normalize_binding_name(left)
    normalized_right = normalize_binding_name(right)
    if not normalized_left or not normalized_right:
        return 0
    if normalized_left == normalized_right:
        return 200

    left_tokens = [token for token in normalized_left.split("_") if token]
    right_tokens = [token for token in normalized_right.split("_") if token]
    overlap = len(set(left_tokens) & set(right_tokens))
    if overlap == 0:
        return 0

    shared_prefix_bonus = 0
    if left_tokens and right_tokens and left_tokens[-1] == right_tokens[-1]:
        shared_prefix_bonus += 10
    if left_tokens and right_tokens and left_tokens[0] == right_tokens[0]:
        shared_prefix_bonus += 10

    symmetric_diff = len(set(left_tokens) ^ set(right_tokens))
    return max(40, 120 + overlap * 25 + shared_prefix_bonus - symmetric_diff * 8)


def _binding_token_overlap(left: str, right: str) -> int:
    normalized_left = normalize_binding_name(left)
    normalized_right = normalize_binding_name(right)
    if not normalized_left or not normalized_right:
        return 0
    return len(set(normalized_left.split("_")) & set(normalized_right.split("_")))


def _api_node_score(node: dict[str, Any]) -> int:
    score = 0
    if node["kind"] == "data":
        score += 40
        if node.get("data", {}).get("persisted"):
            score += 20
        if node.get("data", {}).get("persistence") == "hot":
            score += 20
        if node.get("extension_type") in {"table", "view", "materialized_view"}:
            score += 10
        if node.get("extension_type") == "raw_dataset":
            score -= 10
    if node["kind"] == "compute":
        score += 25
        if node.get("extension_type") == "model":
            score += 15
    return score


def _api_candidate_reason(node: dict[str, Any]) -> str:
    if node["kind"] == "data":
        persistence = node.get("data", {}).get("persistence") or "unknown"
        return f"Data column from {persistence} tier"
    return f"Compute output from {node.get('extension_type') or 'compute'}"
