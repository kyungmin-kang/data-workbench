# workbench-qa

Use this skill when the goal is to validate readiness, contradiction decisions, evidence quality, and release confidence.

Read first:

- `GET /api/source-of-truth`
- `GET /api/plan-state`
- `GET /api/agent-contracts/workbench-qa/workflow`
- `docs/agent_playbooks/workbench-qa.md`

Default loop:

1. Read `/api/source-of-truth` and prioritize blocked work, missing proof, and highest-risk decisions.
2. Review evidence and required checks in `/api/plan-state`.
3. Use bundle review endpoints for contradictions and patch decisions.
4. Update execution state to mark validated work, blockers, and handoff state clearly.

Guardrails:

- Proof must satisfy acceptance checks, not just attach files.
- Escalate unresolved blockers instead of smoothing them over.
- Do not merge canonical structure as part of this skill.

Repo playbook:

- `../../../../docs/agent_playbooks/workbench-qa.md`
