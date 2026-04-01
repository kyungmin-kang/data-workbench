# Architecture

## Goal

This document describes the current module boundaries for the contributor-ready release track.

The short version:

- `graph` is structural truth
- bundles and review are proposal truth
- `plan_state` is execution truth

## Runtime Modes

- Docker Compose is the primary public runtime path.
- Local Python setup is the contributor path.
- `supported_platforms` in the public contract refers to first-class tested platforms, not every environment where Docker may happen to run.
- Windows may work through Docker, but macOS and Linux are the platforms currently treated as officially tested and supported.

## Public Contract

The contributor-ready release track treats `/api/source-of-truth` as the frozen read contract.

It now advertises:

- `api_version`
- `contract_version`
- `stability`
- `default_runtime_mode`
- `supported_runtime_modes`
- `supported_platforms`
- `supported_endpoints`
- `supplemental_endpoints`
- `truth_layers`
- `governance`
- `agent_contract_ids`

Primary supported endpoints for this contract:

- `/api/source-of-truth`
- `/api/plan-state`
- `/api/plan-state/derive-tasks`
- `/api/agent-contracts`
- `/api/agent-contracts/{id}/brief`
- `/api/agent-contracts/{id}/workflow`
- `/api/agent-contracts/{id}/launch`
- `/api/agent-runs`
- `/api/agent-runs/{run_id}`
- `/api/agent-runs/{run_id}/events`
- structure scan, bundle review, rebase-preview, rebase, and merge endpoints

## Backend Shape

### Entry and route composition

- `src/workbench/app.py`
  - FastAPI entrypoint only
  - static mounting
  - router composition

- `src/workbench/api_core_routes.py`
  - index
  - healthcheck
  - graph, source-of-truth, latest plan
  - agent contract read endpoints

- `src/workbench/api_execution_routes.py`
  - `plan_state`
  - agent runs
  - task derivation

- `src/workbench/api_structure_routes.py`
  - structure scan
  - review
  - rebase
  - merge

- `src/workbench/api_project_routes.py`
  - project profile and scanning jobs

- `src/workbench/api_authoring_routes.py`
  - imports, authoring helpers, and graph authoring endpoints

### Core backend domains

- `src/workbench/store.py`
  - local and mirrored persistence
  - canonical graph storage
  - plan-state storage
  - artifact storage metadata

- `src/workbench/execution.py`
  - execution summaries
  - source-of-truth assembly
  - plan-state review sync logic

- `src/workbench/agent_contracts.py`
  - supported agent role definitions

- `src/workbench/agent_briefs.py`
  - role/task/run-specific brief generation

- `src/workbench/agent_workflows.py`
  - role-specific workflow recommendations and launch prep

- `src/workbench/structure_memory.py`
  - scan, review, merge, and hybrid orchestration

- `src/workbench/structure_blueprints.py`
- `src/workbench/structure_candidates.py`
- `src/workbench/structure_markdown.py`
- `src/workbench/structure_observations.py`
- `src/workbench/structure_plan_candidates.py`
- `src/workbench/structure_reconciliation.py`
  - focused structure parsing and reconciliation helpers

- `src/workbench/project_profiler.py`
  - remaining project discovery orchestration
  - should continue shrinking as focused hint extractors move out

- `src/workbench/project_profiler_planning.py`
  - planning-doc and intended-structure extraction helpers

- `src/workbench/types.py`
- `src/workbench/types_base.py`
- `src/workbench/types_execution.py`
- `src/workbench/types_graph.py`
  - shared type definitions and validation

## Frontend Shape

- `static/js/app.js`
  - shell bootstrap and shared state wiring

- `static/js/graph-editing.js`
  - graph edit actions and in-canvas authoring behavior

- `static/js/graph-shell.js`
  - graph rendering, layout, navigation, and interaction shell

- `static/js/app-runtime-support.js`
  - shared formatting, parsing, runtime helpers, and authoring utility functions

- `static/js/project-profile-panel.js`
  - onboarding, discovery, and project profile UI

- `static/js/execution-panel.js`
  - execution UI controller and actions

- `static/js/execution-render.js`
  - execution rendering

- `static/js/execution-briefs.js`
  - agent brief UI helpers

- `static/js/execution-workflows.js`
  - workflow loading and launch helpers

## Test Structure

- `tests/api_test_case.py`
  - shared API test base

- `tests/workbench_test_support.py`
  - shared temp-root setup and reusable test factories

- `tests/test_api*.py`
  - API behavior grouped by subsystem

- `tests/test_e2e_browser.py`
  - browser E2E coverage

- `tests/test_persistence_integration.py`
  - mirrored persistence integration coverage

## Maintenance Rules

- Keep `app.py` thin.
- Keep `static/js/app.js` as bootstrap/composition only.
- Keep `src/workbench/structure_memory.py` and `src/workbench/project_profiler.py` focused on orchestration rather than low-level helper accumulation.
- Prefer adding tests through shared factories and helpers instead of duplicating root setup and fixture dictionaries.
- Treat `/api/source-of-truth` as the stable agent and operator read surface for this release.

## Codex Plugin Scaffold

The repo-local Codex packaging lives here:

- marketplace: `.agents/plugins/marketplace.json`
- plugin root: `plugins/data-workbench/`
- plugin skills mirror the built-in workbench roles and point back to `docs/agent_playbooks/`
