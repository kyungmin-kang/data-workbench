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
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
python -m workbench.app
```

Then open [http://localhost:8000](http://localhost:8000).

## Run with Docker Compose

```bash
docker compose up --build
```

The app is available at `http://localhost:8000`.

## Test

```bash
python3 -m unittest discover -s tests
```

## Why these dependencies

The workbench now uses a small high-leverage dependency set:

- `FastAPI` for a cleaner local API surface and better future extensibility
- `Pydantic` for typed graph/spec validation instead of hand-rolled schema checks
- `polars` for faster dataset profiling and a stronger path toward larger data assets
- `uvicorn` for a durable local app server
