# Onboarding Wizard Quickstart

## Docker-first path

1. Start the workbench from the repo root:
   - `docker compose up --build`
2. Open `http://127.0.0.1:8000`.
3. In `Project Survey`, stay on `Step 1. Scope`.
4. Leave `include workbench internals` off for a cleaner demo, then click `Run survey`.
5. Use the demo files under `examples/onboarding_wizard_demo/`:
   - API route: `GET /api/demo-pricing`
   - UI consumer: `DemoPricingPanel`
   - Data asset: `examples/onboarding_wizard_demo/data/demo_pricing.csv`
6. Walk the wizard:
   - Step 2: select the CSV asset
   - Step 3: select the API and UI hints
   - Step 4: click `Bootstrap graph`
7. Save the graph, inspect the latest plan, and create or derive execution work in the Execution State panel.
8. Save a reusable onboarding preset back in `Step 1`.

## Local contributor path

If you are not using Docker:

1. `python -m venv .venv`
2. `. .venv/bin/activate`
3. `python -m pip install -e ".[e2e,persistence]"`
4. `PYTHONPATH=src python -m workbench.app`
