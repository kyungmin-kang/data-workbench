# workbench-builder

Use this skill when the goal is to execute the next agreed task, keep progress visible, and feed proof or blockers back into the workbench.

Read first:

- `GET /api/source-of-truth`
- `GET /api/plan-state`
- `GET /api/agent-contracts/workbench-builder/workflow`
- `docs/agent_playbooks/workbench-builder.md`

Default loop:

1. Start from `/api/source-of-truth` and take the top open task or workflow-selected task.
2. Launch or resume a run through `POST /api/agent-contracts/workbench-builder/launch`.
3. Record progress with `PATCH /api/agent-runs/{run_id}` and `POST /api/agent-runs/{run_id}/events`.
4. Update tasks, blockers, and evidence through `PUT /api/plan-state`.
5. If implementation implies structure drift, open a scan/review proposal instead of changing canonical structure directly.

Guardrails:

- Tie evidence to explicit acceptance checks.
- Keep handoff state current with `status_reason`, `next_action_hint`, and blockers.
- Never bypass review to mutate the canonical graph.

Repo playbook:

- `../../../../docs/agent_playbooks/workbench-builder.md`
