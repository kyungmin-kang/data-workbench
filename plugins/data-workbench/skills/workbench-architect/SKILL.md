# workbench-architect

Use this skill when the goal is to turn plans, specs, screenshots, and review outcomes into durable decisions and focused execution tasks.

Read first:

- `GET /api/source-of-truth`
- `GET /api/plan-state`
- `GET /api/agent-contracts/workbench-architect/workflow`
- `docs/agent_playbooks/workbench-architect.md`

Default loop:

1. Read `/api/source-of-truth` first and anchor on the top open tasks, top blocker, highest-risk decision, and critical path.
2. Read `/api/plan-state` and check whether the decision/task graph already covers the requested change.
3. Use the architect workflow endpoint to refresh the recommended focus before editing execution state.
4. Update decisions and tasks through `PUT /api/plan-state` or `POST /api/plan-state/derive-tasks`.
5. If structure must change, route that through scan and review instead of mutating canonical structure directly.

Guardrails:

- Do not call `/api/graph/save` or merge endpoints as part of normal skill use.
- Keep every non-exploratory task linked to a decision.
- Prefer small, ordered tasks that fit one focused work session.

Repo playbook:

- `../../../../docs/agent_playbooks/workbench-architect.md`
