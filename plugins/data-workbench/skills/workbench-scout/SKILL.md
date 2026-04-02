# workbench-scout

Use this skill when the goal is to inspect the repo, docs, and runtime evidence and package drift as reviewable findings.

Read first:

- `GET /api/source-of-truth`
- `GET /api/agent-contracts/workbench-scout/workflow`
- `docs/agent_playbooks/workbench-scout.md`

Default loop:

1. Read `/api/source-of-truth` to understand the current agreed graph, execution pressure, and open bundles.
2. During onboarding or large-repo discovery, treat `GET /api/project/profile` as the fast baseline rather than the finished answer.
3. Inspect selected files, docs, routes, relations, and raw-file origins that the baseline suggests but does not fully connect.
4. Run a structure scan with `POST /api/structure/scan`.
5. Use bundle review and execution blockers to surface what changed and what needs follow-up.
6. If needed, update execution state with blockers or notes through `PUT /api/plan-state`.

Guardrails:

- Do not mutate canonical structure directly.
- Prefer explicit contradictions and blockers over hidden assumptions.
- Keep findings tied to refs, bundle ids, and concrete evidence.
- Do not assume a successful scan means the model is complete.

Repo playbook:

- `../../../../docs/agent_playbooks/workbench-scout.md`
