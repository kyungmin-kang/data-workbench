# Workbench v1 Operator Guide

## Purpose

The workbench is designed to stay useful as a standalone app and become more powerful when agents use the same state.

- `graph` is the structural source of truth.
- structure bundles and reviews are proposal truth.
- `plan_state` is execution truth for decisions, tasks, blockers, evidence, and agent runs.

For the contributor-ready release track:

- Docker Compose is the default operator path.
- local Python setup is the contributor path.
- the onboarding demo lives in [`examples/onboarding_wizard_demo/README.md`](../examples/onboarding_wizard_demo/README.md).

## Human-only loop

1. Review the graph and latest plan to confirm the intended structure.
2. Use the Execution State panel to create or refine decisions, tasks, blockers, checks, and evidence.
3. Save execution state so priority signals, role lanes, and handoff views stay current.
4. Use structure scans and the review inbox when implementation or docs imply structural drift.
5. Merge accepted structure changes only through review and merge flows.

## Agent-enhanced loop

1. Start from `/api/source-of-truth` for the current priorities, blockers, and role lanes.
2. Use `/api/agent-contracts` plus `/api/agent-contracts/{contract_id}/brief` to generate a focused assignment brief.
3. Read and write only through execution endpoints, scan endpoints, and review endpoints.
4. Keep task status, blockers, evidence, and run handoff state visible in the workbench.
5. Never mutate canonical structure directly from an agent; route structural change through review bundles.

## Recommended startup

### Docker-first

```bash
docker compose up --build
```

### Local contributor

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[e2e,persistence]"
PYTHONPATH=src python -m workbench.app
```

## Execution panel quick actions

- Decisions can spawn linked tasks quickly.
- Tasks can be started, blocked, completed, turned into blockers, or turned into tracked agent runs.
- Acceptance checks can spawn proof drafts.
- Runs can be resumed, blocked, handed off, completed, and turned into copyable agent briefs.

## Governance rules

- Canonical structure changes only through review and merge.
- Agents may propose and update execution state, but may not merge canonical structure.
- All execution writes use revision checks.
- Evidence should prove specific acceptance checks, not act as an unstructured dump.
- Blockers should stay structured so handoff and triage remain clear.

## Recommended day-to-day rhythm

- Human or architect agent keeps decisions and task ordering clean.
- Builder agents work from top open tasks, role lanes, and briefs.
- QA validates required checks and closes blockers with structured proof.
- Scout agents scan for implementation drift and open reviewable contradictions instead of rewriting structure.
