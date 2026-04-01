# Onboarding Wizard Demo

This example exists to exercise the public dogfood flow:

- discovery
- onboarding bootstrap
- execution-state authoring
- structure review

## Included assets

- `backend/demo_routes.py`
  - exposes `GET /api/demo-pricing`
- `frontend/DemoPricingPanel.tsx`
  - shows a UI consumer for the demo route
- `data/demo_pricing.csv`
  - provides a simple tabular asset for onboarding hints

## Recommended walkthrough

1. Start the workbench with Docker Compose from the repo root.
2. Open `http://127.0.0.1:8000`.
3. In the onboarding wizard, point discovery at this example directory.
4. Import the CSV, API route, and UI consumer hints.
5. Bootstrap the graph.
6. Save the graph and inspect the latest plan.
7. Add or derive execution tasks in the Execution State panel.
8. Run a structure scan against this example and review any proposed changes.
