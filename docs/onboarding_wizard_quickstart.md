# Onboarding Wizard Quickstart

1. Start the app:
   - `PYTHONPATH=src .venv/bin/python -m workbench.app`
2. Open `http://127.0.0.1:8000`.
3. In `Project Survey`, stay on `Step 1. Scope`.
4. Leave `include workbench internals` off for a cleaner demo, then click `Run survey`.
5. Use the demo files under `examples/onboarding_wizard_demo/`:
   - API route: `GET /api/demo-pricing`
   - UI consumer: `DemoPricingPanel`
   - Data asset: `examples/onboarding_wizard_demo/data/demo_pricing.csv`
6. Walk the wizard:
   - Step 2: select the CSV asset
   - Step 3: select the API/UI hints
   - Step 4: click `Bootstrap graph`
7. Save a reusable onboarding preset back in `Step 1`.
