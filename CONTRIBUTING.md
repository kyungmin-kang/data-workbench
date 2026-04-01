# Contributing

## Project Status

Keep the workbench easy to run, easy to understand, and safe to change.

The repo is currently in a public-preview stage on the way to a formal `0.2.0` release. That means:

- real use and real feedback matter more than broad polish
- small focused fixes are more helpful than speculative redesigns
- we want contributions that strengthen the existing truth model and operator flow

## Goal

This repo is currently optimized for:

- Docker-first local use
- contributor-friendly local development on macOS and Linux
- a standalone app that is optionally agent-enhanced

Windows contributors are still welcome, and Docker may work there, but macOS and Linux are the first-class tested platforms for the current preview.

## Quickstart

### Docker-first

```bash
cp .env.example .env
docker compose up --build
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

### Local contributor path

```bash
./scripts/bootstrap_venv.sh
. .venv/bin/activate
PYTHONPATH=src python -m workbench.app
```

The bootstrap script enforces Python 3.11+ so local setup does not silently fall back to an unsupported interpreter.

## Working Agreement

- Keep `graph` as structural truth.
- Keep structure bundles and review as proposal truth.
- Keep `plan_state` as execution truth.
- Do not add new behavior back into orchestration hotspots when a focused module is a better home.
- Do not let agents mutate canonical structure directly. Structural changes still flow through review and merge.

## Where New Code Should Go

- API route registration stays thin in `src/workbench/app.py`.
- New API behavior belongs in the appropriate `api_*_routes.py` module or a focused helper module.
- Frontend shell/bootstrap belongs in `static/js/app.js`; feature logic should live in focused JS modules.
- Structure reconciliation and parsing work should go into focused `structure_*` modules instead of expanding `structure_memory.py`.
- Project analysis and hint extraction should go into focused profiler modules instead of expanding `project_profiler.py`.

See [`docs/architecture.md`](docs/architecture.md) for the current module map.

## Tests

Run the default suite:

```bash
PYTHONPATH=src python -m unittest discover -s tests
```

Run browser E2E:

```bash
python -m pip install -e ".[e2e]"
PLAYWRIGHT_BROWSERS_PATH=.playwright-browsers python -m playwright install chromium
PYTHONPATH=src python -m unittest discover -s tests -p 'test_e2e_browser.py'
```

Run persistence integration:

```bash
python -m pip install -e ".[persistence]"
PYTHONPATH=src WORKBENCH_RUN_PERSISTENCE_INTEGRATION=1 python -m unittest discover -s tests -p 'test_persistence_integration.py'
```

## Pull Requests

- Keep changes scoped.
- Add or update tests for behavioral changes.
- Update docs when setup, contracts, or operator workflow changes.
- Call out any intentional follow-up work clearly.
- If you are unsure whether something fits the preview goal, open an issue first and describe the user-facing problem you are trying to improve.

## Good First Areas

- UI polish in focused frontend modules
- docs and walkthrough improvements
- targeted test coverage additions
- profiler and reconciliation helper extraction
- example/demo project improvements
