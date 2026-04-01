from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import HTTPException

from .agent_contracts import list_agent_contracts
from .diagnostics import build_graph_diagnostics
from .execution import (
    apply_bundle_merge_to_plan_state,
    apply_bundle_rebase_to_plan_state,
    apply_contradiction_review_to_plan_state,
    apply_patch_review_to_plan_state,
    build_source_of_truth,
    utc_timestamp,
)
from .project_profiler import is_ignored_project_dir_name
from .store import (
    describe_artifact_storage,
    get_root_dir,
    load_graph,
    load_latest_plan,
    load_latest_plan_artifacts,
    load_plan_state,
    save_plan_state,
)
from .structure_memory import build_structure_summary
from .validation import build_validation_report


def analyze_graph(graph: dict[str, Any]) -> dict[str, Any]:
    validation = build_validation_report(graph)
    diagnostics = build_graph_diagnostics(graph, validation_report=validation)
    structure = build_structure_summary(graph, diagnostics=diagnostics, validation_report=validation)
    return {
        "validation": validation,
        "diagnostics": diagnostics,
        "structure": structure,
    }


def graph_payload(graph: dict[str, Any]) -> dict[str, Any]:
    analysis = analyze_graph(graph)
    artifact_storage = describe_artifact_storage()
    latest_plan = load_latest_plan()
    latest_artifacts = load_latest_plan_artifacts()
    plan_state = load_plan_state(graph)
    agent_contracts = list_agent_contracts()
    return {
        "graph": graph,
        "diagnostics": analysis["diagnostics"],
        "validation": analysis["validation"],
        "latest_plan": latest_plan,
        "latest_artifacts": latest_artifacts,
        "artifact_storage": artifact_storage,
        "structure": analysis["structure"],
        "plan_state": plan_state,
        "agent_contracts": agent_contracts,
        "source_of_truth": build_source_of_truth(
            graph=graph,
            structure=analysis["structure"],
            plan_state=plan_state,
            latest_plan=latest_plan,
            latest_artifacts=latest_artifacts,
            artifact_storage=artifact_storage,
            agent_contracts=agent_contracts,
        ),
    }


def build_execution_payload(
    graph: dict[str, Any],
    *,
    plan_state: dict[str, Any] | None = None,
    structure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    analysis = None
    if structure is None:
        analysis = analyze_graph(graph)
        structure = analysis["structure"]
    artifact_storage = describe_artifact_storage()
    latest_plan = load_latest_plan()
    latest_artifacts = load_latest_plan_artifacts()
    normalized_plan_state = plan_state or load_plan_state(graph)
    agent_contracts = list_agent_contracts()
    return {
        "plan_state": normalized_plan_state,
        "agent_contracts": agent_contracts,
        "source_of_truth": build_source_of_truth(
            graph=graph,
            structure=structure,
            plan_state=normalized_plan_state,
            latest_plan=latest_plan,
            latest_artifacts=latest_artifacts,
            artifact_storage=artifact_storage,
            agent_contracts=agent_contracts,
        ),
    }


def reject_agent_canonical_write(actor_type: str, action_label: str) -> None:
    if actor_type == "agent":
        raise HTTPException(
            status_code=403,
            detail=(
                f"Agents may not {action_label}. Use plan_state, scans, and review proposals instead of "
                "mutating canonical structure directly."
            ),
        )


def sync_patch_reviews_with_execution(
    *,
    bundle: dict[str, Any],
    patch_ids: list[str],
    decision: str,
    reviewed_by: str,
    note: str = "",
) -> dict[str, Any]:
    normalized_ids = [patch_id for patch_id in dict.fromkeys(patch_ids) if patch_id]
    graph = load_graph()
    current = load_plan_state(graph)
    if not normalized_ids:
        return current
    reviewed_at = utc_timestamp()
    updated = current
    patches_by_id = {patch.get("id"): patch for patch in bundle.get("patches", [])}
    for patch_id in normalized_ids:
        patch = patches_by_id.get(patch_id)
        if patch is None:
            continue
        updated = apply_patch_review_to_plan_state(
            updated,
            graph,
            bundle_id=bundle.get("bundle_id", ""),
            patch=patch,
            decision=decision,
            reviewed_by=reviewed_by,
            note=note,
            reviewed_at=reviewed_at,
        )
    return save_plan_state(
        updated,
        graph=graph,
        updated_by=reviewed_by,
        expected_revision=int(current.get("revision") or 0),
    )


def sync_contradiction_review_with_execution(
    *,
    bundle: dict[str, Any],
    contradiction_id: str,
    updated_patch_ids: list[str],
    decision: str,
    reviewed_by: str,
    note: str = "",
) -> dict[str, Any]:
    graph = load_graph()
    current = load_plan_state(graph)
    reviewed_at = utc_timestamp()
    updated = current
    contradiction = next(
        (item for item in bundle.get("contradictions", []) if item.get("id") == contradiction_id),
        None,
    )
    if contradiction is None:
        return current
    patches_by_id = {patch.get("id"): patch for patch in bundle.get("patches", [])}
    for patch_id in updated_patch_ids:
        patch = patches_by_id.get(patch_id)
        if patch is None:
            continue
        updated = apply_patch_review_to_plan_state(
            updated,
            graph,
            bundle_id=bundle.get("bundle_id", ""),
            patch=patch,
            decision=decision,
            reviewed_by=reviewed_by,
            note=note,
            reviewed_at=reviewed_at,
        )
    updated = apply_contradiction_review_to_plan_state(
        updated,
        graph,
        bundle_id=bundle.get("bundle_id", ""),
        contradiction=contradiction,
        decision=decision,
        reviewed_by=reviewed_by,
        note=note,
        reviewed_at=reviewed_at,
    )
    return save_plan_state(
        updated,
        graph=graph,
        updated_by=reviewed_by,
        expected_revision=int(current.get("revision") or 0),
    )


def sync_merge_with_execution(
    *,
    bundle: dict[str, Any],
    graph: dict[str, Any],
    merged_by: str,
) -> dict[str, Any]:
    current = load_plan_state(graph)
    updated = apply_bundle_merge_to_plan_state(
        current,
        graph,
        bundle=bundle,
        merged_by=merged_by,
        merged_at=bundle.get("review", {}).get("merged_at") or utc_timestamp(),
    )
    return save_plan_state(
        updated,
        graph=graph,
        updated_by=merged_by,
        expected_revision=int(current.get("revision") or 0),
    )


def sync_rebase_with_execution(
    *,
    source_bundle_id: str,
    rebased_bundle: dict[str, Any],
    rebased_by: str,
) -> dict[str, Any]:
    graph = load_graph()
    current = load_plan_state(graph)
    updated = apply_bundle_rebase_to_plan_state(
        current,
        graph,
        source_bundle_id=source_bundle_id,
        rebased_bundle=rebased_bundle,
        rebased_by=rebased_by,
        rebased_at=rebased_bundle.get("review", {}).get("last_rebase_summary", {}).get("rebased_at") or utc_timestamp(),
    )
    return save_plan_state(
        updated,
        graph=graph,
        updated_by=rebased_by,
        expected_revision=int(current.get("revision") or 0),
    )


def resolve_profile_root(root_path: str | None) -> Path:
    if not root_path:
        return get_root_dir()
    candidate = Path(root_path).expanduser()
    if not candidate.is_absolute():
        candidate = (get_root_dir() / candidate).resolve()
    if not candidate.exists() or not candidate.is_dir():
        raise HTTPException(status_code=400, detail=f"Project root not found: {root_path}")
    return candidate


def search_project_directories(base_path: str | None, query: str, *, limit: int = 40) -> list[dict[str, str]]:
    root_dir = resolve_profile_root(base_path)
    normalized_query = query.strip().lower()
    matches: list[dict[str, str]] = []
    seen: set[str] = set()

    def maybe_add(path: Path) -> None:
        text = str(path)
        if text in seen:
            return
        seen.add(text)
        matches.append({"path": text, "name": path.name or text})

    maybe_add(root_dir)
    if not normalized_query:
        for child in sorted(root_dir.iterdir()):
            if child.is_dir() and not is_ignored_project_dir_name(child.name):
                maybe_add(child)
                if len(matches) >= limit:
                    break
        return matches[:limit]

    stack = [root_dir]
    while stack and len(matches) < limit:
        current = stack.pop()
        try:
            child_dirs = sorted(
                child
                for child in current.iterdir()
                if child.is_dir() and not is_ignored_project_dir_name(child.name)
            )
        except OSError:
            continue
        for child in reversed(child_dirs):
            stack.append(child)
        for path in child_dirs:
            relative = str(path.relative_to(root_dir)).lower()
            if normalized_query not in path.name.lower() and normalized_query not in relative and normalized_query not in str(path).lower():
                continue
            maybe_add(path)
            if len(matches) >= limit:
                break
    return matches[:limit]
