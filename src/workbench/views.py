from __future__ import annotations

from typing import Any

from .types import build_index


VIEW_DEFINITIONS = {
    "data": {"name": "Data View"},
    "contract": {"name": "Contract View"},
    "ui": {"name": "UI Dependency View"},
    "impact": {"name": "Impact View"},
}


def project_graph(graph: dict[str, Any], view_name: str) -> dict[str, Any]:
    if view_name not in VIEW_DEFINITIONS:
        raise ValueError(f"Unknown view: {view_name}")

    index = build_index(graph)
    included_ids = {
        "data": _project_data_ids(index),
        "contract": _project_contract_ids(index),
        "ui": _project_ui_ids(index),
        "impact": set(index["nodes"]),
    }[view_name]

    return {
        "view": view_name,
        "nodes": [node for node in graph["nodes"] if node["id"] in included_ids],
        "edges": [
            edge
            for edge in graph["edges"]
            if edge["source"] in included_ids and edge["target"] in included_ids
        ],
    }


def _project_data_ids(index: dict[str, Any]) -> set[str]:
    return {
        node_id
        for node_id, node in index["nodes"].items()
        if node["kind"] in {"source", "data", "compute"}
    }


def _project_contract_ids(index: dict[str, Any]) -> set[str]:
    api_ids = {
        node_id
        for node_id, node in index["nodes"].items()
        if node["kind"] == "contract" and node["extension_type"] == "api"
    }
    selected = set(api_ids)
    queue = list(api_ids)
    while queue:
        current = queue.pop(0)
        for edge in index["incoming"][current]:
            source_id = edge["source"]
            if source_id not in selected:
                selected.add(source_id)
                queue.append(source_id)
    return selected


def _project_ui_ids(index: dict[str, Any]) -> set[str]:
    selected = {
        node_id
        for node_id, node in index["nodes"].items()
        if node["kind"] == "contract" or node["kind"] in {"source", "data", "compute"}
    }
    return _closure(index, selected)


def _closure(index: dict[str, Any], seed_ids: set[str]) -> set[str]:
    queue = list(seed_ids)
    visited = set(seed_ids)
    while queue:
        current = queue.pop(0)
        neighbors = [
            edge["target"] for edge in index["outgoing"][current]
        ] + [edge["source"] for edge in index["incoming"][current]]
        for neighbor in neighbors:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    return visited
