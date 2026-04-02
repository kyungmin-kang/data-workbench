# Data Workbench

A graphical source of truth for data structure and flow across assets, transforms, APIs, and UI consumers. Data Workbench helps humans and agents coordinate implementation against the same model, compare intended versus observed changes, and keep execution work tied to the architecture it is supposed to implement.

It extends the usefulness of an ER diagram by linking structure, execution, review, and implementation evidence in one place. It can be used as a standalone workbench or as the shared control plane for development with agents.

## What the workbench does

- provides a graphical map of sources, data assets, compute steps, APIs, and UI dependencies
- treats that map as shared truth for both humans and agents
- imports hints from project scans, planning docs, SQL, ORM code, and API code
- saves deterministic latest-plan artifacts after graph changes
- reconciles intended structure versus observed implementation through bundles, contradictions, review, rebase, and merge
- tracks decisions, tasks, blockers, evidence, and agent runs in `plan_state`
- exposes agent-friendly assignment briefs and workflow recommendations without making agents mandatory

The core truth model is:

- `graph` = structural truth
- bundles and review = proposal truth
- `plan_state` = execution truth

## Preview status

This repo is being shared publicly now as a **usable preview** ahead of the formal `0.2.0` GitHub release.

Right now, the expectation is:

- core workflows should already be usable in real projects
- the repo should already be understandable and contributor-friendly
- a short dogfood period will confirm the intended loop before the formal release tag
- smaller ergonomics and edge-case fixes may still land quickly during preview

## Quickstart

### Docker-first

This is the default path for running the project from a clean clone.

```bash
cp .env.example .env
docker compose up --build
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

Included services:

- app
- worker
- Postgres
- MinIO

The default compose setup runs in `mirror` persistence mode:

- local artifacts still land under `specs/` and `runtime/`
- canonical graph state and execution state can be mirrored to Postgres
- plan artifacts and bundle artifacts can be mirrored to MinIO

### Local contributor path

Preferred bootstrap:

```bash
./scripts/bootstrap_venv.sh
. .venv/bin/activate
```

The bootstrap script auto-selects Python 3.11+ and refuses older interpreters.

Manual path if you prefer:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[e2e,persistence]"
PYTHONPATH=src python -m workbench.app
```

## Using the workbench against another repo

When you launch the app from the `data-workbench` repo, it stores canonical state under that repo by default. For real dogfooding, point the workbench state at a separate workspace root and point onboarding or profiling at the target project root.

Recommended sidecar workspace:

```bash
WORKBENCH_ROOT_DIR=/home/you/projects/RealEstate/.data-workbench \
PYTHONPATH=src python -m workbench.app
```

Then use the target repo as the onboarding/profile `root_path`, for example:

```text
/home/you/projects/RealEstate
```

This keeps the workbench's own `specs/` and `runtime/` state out of the `data-workbench` repo while still letting the app scan and model the external project.

If you want the modeled project to own its workbench state directly, set:

```bash
WORKBENCH_ROOT_DIR=/home/you/projects/RealEstate
```

That will write `specs/` and `runtime/` inside the target repo itself.

For large repos, project profiling now stays conservative by default:

- parquet and parquet-collection assets are discovered, but not deeply profiled during the initial project survey
- oversized local data assets are left as schema-only discovery entries instead of being eagerly read

If you explicitly want deep parquet profiling during discovery, opt in with:

```bash
WORKBENCH_PROJECT_PROFILE_ALLOW_PARQUET=1
```

If a repo includes giant raw-data or ingestion trees that you do not want to scan during the first pass, exclude them explicitly:

```bash
WORKBENCH_PROJECT_PROFILE_EXCLUDE_PATHS=raw,warehouse,tmp
```

## Demo walkthrough

Use the onboarding demo under [`examples/onboarding_wizard_demo/README.md`](examples/onboarding_wizard_demo/README.md).

Recommended first loop:

1. Start the app with Docker Compose.
2. Run onboarding against `examples/onboarding_wizard_demo/`.
3. Bootstrap the graph and save it.
4. Inspect the latest plan.
5. Add or derive execution tasks.
6. Run a structure scan and review any proposed changes.

The onboarding-specific walkthrough is in [`docs/onboarding_wizard_quickstart.md`](docs/onboarding_wizard_quickstart.md).

## Recommended large-repo onboarding loop

For large or messy repos, treat onboarding as a joint human+agent modeling pass:

1. Run metadata-first discovery to get a fast baseline.
2. Bootstrap the highest-signal findings first:
   - selected assets
   - API/UI contracts
   - SQL/ORM structure
3. Keep the imported origin and raw-file context when discovery can identify a defining file.
4. Save the graph and inspect what is still missing or too noisy.
5. Then use a scout or architect/scout agent pass to inspect selected objects, files, docs, and routes and help complete the model.

The important default is: discovery is the fast first pass, not the final answer. Agents should help complete and review the model, not just rerun discovery and stop.

## What to expect in preview

- The Docker-first path is the main supported way to run the workbench right now.
- The standalone app is the primary product; agent support is additive.
- The API and truth-layer model are intentionally stabilizing now, not being reinvented during preview.
- Contributions, issues, and sharp-edge reports are welcome, especially around real operator workflows.
- For large repos, start with a serving-relevant subroot if a full-project profile is too heavy, then widen the model in later passes.

## Agent-enhanced use

The workbench is usable without agents. When you do use agents, the intended read path is:

- `/api/source-of-truth`
- `/api/agent-contracts`
- `/api/agent-contracts/{id}/brief`
- `/api/agent-contracts/{id}/workflow`
- `/api/agent-contracts/{id}/launch`

Agents should update execution state, open scans, and participate in review, but they should not mutate canonical structure directly.

For onboarding and large-repo discovery, the intended agent behavior is collaborative:

- let deterministic discovery create the cheap baseline
- inspect selected files, relations, docs, and routes to fill in what the baseline missed
- package missing structure as reviewable proposals, notes, blockers, or follow-up tasks
- do not assume the first scan is complete just because it ran successfully

See [`docs/workbench_v1_operator_guide.md`](docs/workbench_v1_operator_guide.md) for the day-to-day human and agent operating loop.

## Current preview support surface

These are the backend surfaces we currently expect to carry into the formal `0.2.0` release:

- `/api/source-of-truth`
- `/api/plan-state`
- `/api/agent-contracts`
- `/api/agent-contracts/{id}/brief`
- `/api/agent-contracts/{id}/workflow`
- `/api/agent-contracts/{id}/launch`
- structure scan, review, rebase, and merge endpoints

Current supported runtime modes:

- Docker Compose: primary
- local Python: contributor path

Current first-class tested local platforms:

- macOS
- Linux

Windows may work through Docker Desktop, but it is not yet an officially tested or supported path for this preview. The `supported_platforms` contract field should be read as the first-class platforms we currently test and stand behind.

## Optional Codex plugin scaffold

For local dogfooding with Codex, this repo now includes a minimal plugin scaffold:

- marketplace: [`.agents/plugins/marketplace.json`](.agents/plugins/marketplace.json)
- plugin manifest: [`plugins/data-workbench/.codex-plugin/plugin.json`](plugins/data-workbench/.codex-plugin/plugin.json)
- skills:
  - [`plugins/data-workbench/skills/workbench-architect/SKILL.md`](plugins/data-workbench/skills/workbench-architect/SKILL.md)
  - [`plugins/data-workbench/skills/workbench-scout/SKILL.md`](plugins/data-workbench/skills/workbench-scout/SKILL.md)
  - [`plugins/data-workbench/skills/workbench-builder/SKILL.md`](plugins/data-workbench/skills/workbench-builder/SKILL.md)
  - [`plugins/data-workbench/skills/workbench-qa/SKILL.md`](plugins/data-workbench/skills/workbench-qa/SKILL.md)

This plugin track is optional during preview. The app remains fully usable without it.

## Tests

Default suite:

```bash
PYTHONPATH=src python -m unittest discover -s tests
```

Browser E2E:

```bash
python -m pip install -e ".[e2e]"
PLAYWRIGHT_BROWSERS_PATH=.playwright-browsers python -m playwright install chromium
PYTHONPATH=src python -m unittest discover -s tests -p 'test_e2e_browser.py'
```

Persistence integration:

```bash
python -m pip install -e ".[persistence]"
PYTHONPATH=src WORKBENCH_RUN_PERSISTENCE_INTEGRATION=1 python -m unittest discover -s tests -p 'test_persistence_integration.py'
```

## CI

GitHub Actions currently runs:

- unit tests plus Python compile checks
- Docker smoke
- browser E2E with Playwright Chromium
- persistence integration against Postgres and MinIO

## Docs

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/workbench_v1_operator_guide.md`](docs/workbench_v1_operator_guide.md)
- [`docs/onboarding_wizard_quickstart.md`](docs/onboarding_wizard_quickstart.md)
- [`docs/release_notes_0_2_0.md`](docs/release_notes_0_2_0.md)
- [`docs/release_checklist_0_2_0.md`](docs/release_checklist_0_2_0.md)
- [`docs/shipping_plan.md`](docs/shipping_plan.md)
- [`docs/shipping_tasks.md`](docs/shipping_tasks.md)

The release notes and checklist docs describe the planned formal `0.2.0` release after the current preview dogfood period.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).
