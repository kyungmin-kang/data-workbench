# Data Workbench

Internal, spec-first data workbench for modeling sources, datasets, transforms, models, API contracts, and UI dependencies in one graph.

## What is implemented

- One canonical graph spec with 4 base node types: `source`, `data`, `compute`, and `contract`
- Multiple filtered graph views: Data, Contract, UI Dependency, and Impact
- Column-level lineage via edge mappings and contract field bindings
- Quick profiling for accessible datasets via `polars`
- `save -> plan` flow that writes deterministic machine-readable and human-readable diff artifacts
- Browser UI for graph browsing, note editing, node dragging, view switching, profile refresh, and plan inspection
- Docker Compose setup with app, worker, Postgres, and MinIO services

## Run locally

```bash
python -m venv .venv
python -m pip install -e .
PYTHONPATH=src python -m workbench.app
```

Then open [http://localhost:8000](http://localhost:8000).

## Run with Docker Compose

```bash
docker compose up --build
```

The app is available at `http://localhost:8000`.

Compose now runs the app and worker in `mirror` persistence mode:

- canonical graph state, bundles, plans, and onboarding presets are mirrored into Postgres
- local YAML and JSON artifacts are still written under `specs/` and `runtime/` for easy inspection
- plan artifacts, bundle YAML, graph snapshots, and preset payloads are also mirrored into MinIO object storage

## Discovery workflow

Project discovery now caches a full scan per root and scope combination. The onboarding wizard reuses that cached profile for hint import and bootstrap, and `Rescan project` forces a fresh crawl when you want to invalidate the cache after repo changes.

## Test

```bash
PYTHONPATH=src python -m unittest discover -s tests
```

## Browser E2E

The browser coverage is optional and now works with the current interpreter, a repo-local virtualenv, or an explicit override.

```bash
python -m pip install -e ".[e2e]"
python -m playwright install chromium
PYTHONPATH=src python -m unittest discover -s tests -p 'test_e2e_browser.py'
```

Optional overrides:

- `WORKBENCH_E2E_PYTHON` to point the harness at a specific Python executable
- `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` to point Playwright at a system Chromium or Chrome install
- `PLAYWRIGHT_CHROMIUM_CHANNEL` to launch a branded browser channel such as `chrome` or `msedge`

## CI

GitHub Actions now runs three lanes on every push and pull request:

- the default unittest suite plus a Python compile check
- a dedicated browser E2E job on Ubuntu with Playwright Chromium installed
- a persistence integration job that brings up Postgres and MinIO, then exercises the real persistence backends end-to-end

## Why these dependencies

The workbench now uses a small high-leverage dependency set:

- `FastAPI` for a cleaner local API surface and better future extensibility
- `Pydantic` for typed graph/spec validation instead of hand-rolled schema checks
- `polars` for faster dataset profiling and a stronger path toward larger data assets
- `uvicorn` for a durable local app server
