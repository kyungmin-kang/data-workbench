# Draft Release Notes: 0.2.0

## Status

This document is the prepared release note draft for the first formal `0.2.0` GitHub release.

The repo may be shared publicly before that tag as a preview while the core workflows are dogfooded in real use.

## What this release is

The goal of this release is not to present a fully polished end-user product. The goal is to make the repo:

- useful in real projects right now
- safe to share publicly
- understandable enough that other people can run it and contribute

This release is Docker-first, standalone-first, and agent-enhanced rather than agent-dependent.

## Core model

The current truth model is now explicit and stable for this release:

- `graph` = structural truth
- bundles and review = proposal truth
- `plan_state` = execution truth

The workbench remains usable without agents. Agents operate against the same durable state humans can inspect and edit.

## Highlights

- standalone-first graph, review, and execution workflow in one app
- project discovery and onboarding bootstrap for assets, API hints, UI hints, SQL hints, and ORM hints
- deterministic save-to-plan flow with latest plan artifacts
- structure scan, contradiction review, rebase, and merge workflow
- execution-state model for decisions, tasks, blockers, evidence, and agent runs
- agent contracts, briefs, workflow recommendations, and guided launch support
- Docker Compose as the primary public runtime path
- repo-local Codex plugin scaffold for dogfooding the agent flow

## Stable in this release

These are the areas we are explicitly treating as supported for the planned `0.2.0` release:

- Docker Compose startup as the default runtime path
- local Python startup as the contributor path
- `/api/source-of-truth`
- `/api/plan-state`
- `/api/plan-state/derive-tasks`
- `/api/agent-contracts`
- `/api/agent-contracts/{id}/brief`
- `/api/agent-contracts/{id}/workflow`
- `/api/agent-contracts/{id}/launch`
- agent-run endpoints
- structure scan, bundle review, rebase-preview, rebase, and merge endpoints
- onboarding wizard demo flow under `examples/onboarding_wizard_demo/`

Supported local platforms for this release:

- macOS
- Linux

Windows may work through Docker, but it is not yet part of the first-class tested platform set for `0.2.0`.

## Experimental or intentionally light in this release

These areas exist and may already be useful, but they should still be treated as lighter-weight or dogfood-oriented:

- repo-local Codex plugin packaging and skill scaffold
- broader contributor ergonomics outside the documented Docker-first and local Python paths
- persistence behavior beyond the documented mirror/local workflows
- UI polish beyond what is needed for contributor clarity
- any workflow that depends on platforms we do not yet test as first-class, such as Windows

## Governance and trust model

- agents may update execution state, create runs, and participate in review
- agents may not mutate canonical structure directly
- canonical structure changes require review and merge
- execution writes use revision checks
- all agent actions should remain visible in the app

## Good first contribution areas

- UI polish that does not break the truth-layer model
- better onboarding and discovery ergonomics
- stronger browser E2E coverage for public flows
- focused refactors that keep large files shrinking instead of growing
- improvements to docs, examples, and operator guidance

## Suggested first run

1. `cp .env.example .env`
2. `docker compose up --build`
3. Open `http://127.0.0.1:8000`
4. Run the onboarding demo from `examples/onboarding_wizard_demo/`
5. Bootstrap the graph
6. Save the graph and inspect the latest plan
7. Add or derive execution tasks
8. Run a structure scan and review any proposed changes

## Related docs

- [`README.md`](../README.md)
- [`docs/architecture.md`](architecture.md)
- [`docs/workbench_v1_operator_guide.md`](workbench_v1_operator_guide.md)
- [`docs/onboarding_wizard_quickstart.md`](onboarding_wizard_quickstart.md)
- [`docs/release_checklist_0_2_0.md`](release_checklist_0_2_0.md)
