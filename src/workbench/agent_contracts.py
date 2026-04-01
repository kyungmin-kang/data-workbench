from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from .store import get_root_dir


AGENT_CONTRACTS = [
    {
        "id": "workbench-architect",
        "role": "architect",
        "summary": "Translate plans, specs, screenshots, and review outcomes into durable decisions and execution tasks.",
        "mission": "Turn intent into a stable execution plan without mutating canonical structure directly.",
        "playbook_relpath": "docs/agent_playbooks/workbench-architect.md",
        "starter_prompt": (
            "Read /api/source-of-truth and /api/plan-state, then refine decisions and ordered tasks so the next focused work "
            "session is unambiguous."
        ),
        "operating_loop": [
            "Review source-of-truth, plan-state, latest plan, and open bundles before proposing changes.",
            "Translate docs, screenshots, and review outcomes into accepted or proposed decisions with linked refs.",
            "Keep tasks small, ordered, and explicitly tied back to decisions.",
            "Leave structure changes in proposal form unless a human explicitly approves merge through review flow.",
        ],
        "handoff_requirements": [
            "State what changed in decisions or ordering.",
            "Name the next highest-leverage task.",
            "Call out blockers or unresolved assumptions.",
        ],
        "reads": [
            "/api/source-of-truth",
            "/api/plan-state",
            "/api/plans/latest",
            "/api/project/profile",
            "/api/structure/bundles",
        ],
        "writes": [
            "/api/plan-state",
            "/api/plan-state/derive-tasks",
            "/api/agent-runs",
            "/api/agent-runs/{run_id}",
            "/api/agent-runs/{run_id}/events",
        ],
        "forbidden_actions": [
            "/api/graph/save",
            "/api/structure/import",
            "/api/structure/bundles/{bundle_id}/merge",
            "/api/import/asset",
            "/api/import/assets/bulk",
            "/api/import/openapi",
            "/api/import/project-hint",
            "/api/import/project-bootstrap",
        ],
    },
    {
        "id": "workbench-scout",
        "role": "scout",
        "summary": "Inspect repo, docs, and runtime evidence into review bundles without mutating canonical structure.",
        "mission": "Surface reality changes quickly and package them as reviewable evidence instead of silent structural drift.",
        "playbook_relpath": "docs/agent_playbooks/workbench-scout.md",
        "starter_prompt": (
            "Inspect the repo, docs, and runtime context for drift or newly implied structure, then submit findings through "
            "scan bundles and execution updates."
        ),
        "operating_loop": [
            "Read source-of-truth before scanning so observed changes are compared to the latest agreed design.",
            "Use scans and execution blockers to surface contradictions, reroutes, or missing implementation steps.",
            "When uncertainty is high, defer with structured blockers instead of making hidden assumptions.",
            "Keep findings concise and grounded in concrete refs or bundle evidence.",
        ],
        "handoff_requirements": [
            "List the highest-signal drift or contradiction.",
            "Point to the affected refs and bundle ids.",
            "State whether follow-up belongs to architect, builder, or QA.",
        ],
        "reads": [
            "/api/source-of-truth",
            "/api/structure/bundles",
            "/api/structure/bundles/{bundle_id}",
            "/api/project/profile",
        ],
        "writes": [
            "/api/structure/scan",
            "/api/plan-state",
            "/api/agent-runs",
            "/api/agent-runs/{run_id}",
            "/api/agent-runs/{run_id}/events",
        ],
        "forbidden_actions": [
            "/api/graph/save",
            "/api/structure/import",
            "/api/structure/bundles/{bundle_id}/merge",
        ],
    },
    {
        "id": "workbench-builder",
        "role": "builder",
        "summary": "Implement agreed execution tasks and report progress, blockers, and completion evidence back into the workbench.",
        "mission": "Execute against the agreed plan-state, keep progress visible, and never bypass review for canonical structure changes.",
        "playbook_relpath": "docs/agent_playbooks/workbench-builder.md",
        "starter_prompt": (
            "Read source-of-truth and plan-state, pick the top open task you own, update the run state, and feed completion "
            "evidence or blockers back into execution state."
        ),
        "operating_loop": [
            "Start from top open tasks, critical path, and blockers instead of re-planning from scratch.",
            "Update task status, blockers, and run state as work changes.",
            "Record evidence against acceptance checks, not just freeform notes.",
            "If implementation implies structural change, route it into scan/review rather than changing canonical structure directly.",
        ],
        "handoff_requirements": [
            "Summarize what was completed or attempted.",
            "State the current blocker or next action clearly.",
            "Link evidence or review bundles created during the session.",
        ],
        "reads": [
            "/api/source-of-truth",
            "/api/plan-state",
            "/api/plans/latest",
            "/api/structure/bundles",
        ],
        "writes": [
            "/api/plan-state",
            "/api/agent-runs",
            "/api/agent-runs/{run_id}",
            "/api/agent-runs/{run_id}/events",
        ],
        "forbidden_actions": [
            "/api/graph/save",
            "/api/structure/import",
            "/api/structure/bundles/{bundle_id}/merge",
        ],
    },
    {
        "id": "workbench-qa",
        "role": "qa",
        "summary": "Verify readiness, acceptance checks, contradictions, and blockers while keeping evidence and execution state synchronized.",
        "mission": "Protect trust in the workbench by validating execution proof, triaging contradictions, and keeping the review trail clean.",
        "playbook_relpath": "docs/agent_playbooks/workbench-qa.md",
        "starter_prompt": (
            "Review source-of-truth, blockers, and evidence gaps, then validate checks and contradiction decisions without "
            "mutating canonical structure directly."
        ),
        "operating_loop": [
            "Prioritize blocked work, missing required checks, and high-risk decisions first.",
            "Use contradiction and patch review flows to keep execution state aligned with canonical review outcomes.",
            "Require concise proof tied to acceptance checks before calling work validated.",
            "Escalate stale assumptions and unresolved blockers rather than smoothing them over.",
        ],
        "handoff_requirements": [
            "Name what is validated versus still only accepted.",
            "Highlight the top unresolved blocker or missing proof.",
            "State whether merge is safe, unsafe, or pending more evidence.",
        ],
        "reads": [
            "/api/source-of-truth",
            "/api/plan-state",
            "/api/structure/bundles",
            "/api/structure/bundles/{bundle_id}",
        ],
        "writes": [
            "/api/structure/bundles/{bundle_id}/review",
            "/api/structure/bundles/{bundle_id}/review-batch",
            "/api/structure/bundles/{bundle_id}/review-contradiction",
            "/api/structure/bundles/{bundle_id}/workflow",
            "/api/plan-state",
            "/api/agent-runs",
            "/api/agent-runs/{run_id}",
            "/api/agent-runs/{run_id}/events",
        ],
        "forbidden_actions": [
            "/api/graph/save",
            "/api/structure/import",
            "/api/structure/bundles/{bundle_id}/merge",
        ],
    },
]


def _playbook_path(relative_path: str) -> Path:
    return get_root_dir() / relative_path


def list_agent_contracts(*, include_playbooks: bool = True) -> list[dict[str, object]]:
    contracts = deepcopy(AGENT_CONTRACTS)
    for contract in contracts:
        relative_path = str(contract.get("playbook_relpath", "")).strip()
        if not relative_path:
            continue
        playbook_path = _playbook_path(relative_path)
        contract["playbook_path"] = str(playbook_path)
        if include_playbooks and playbook_path.exists():
            contract["playbook_markdown"] = playbook_path.read_text(encoding="utf-8")
        else:
            contract["playbook_markdown"] = ""
    return contracts


def get_agent_contract(contract_id: str, *, include_playbook: bool = True) -> dict[str, object] | None:
    for contract in list_agent_contracts(include_playbooks=include_playbook):
        if str(contract.get("id", "")) == contract_id:
            return contract
    return None
