# Release Checklist: 0.2.0

## Goal

Ship a Docker-first, contributor-ready GitHub release that is already usable in real projects and safe to hand to outside contributors after the current public-preview dogfood period.

## Scope

This checklist is for the `0.2.0` contributor-ready share. It does not try to cover hosted deployment, multi-user support, or post-release roadmap work.

## Summary

Release is ready when:

- public docs are accurate
- versioning is aligned
- the documented startup paths work
- the frozen contract surface is still true
- unit, browser, and persistence checks are green
- the demo walkthrough matches the app

This checklist is meant for the move from public preview to the formal `0.2.0` release tag.

## Version alignment

- [ ] `pyproject.toml` version is `0.2.0`
- [ ] `src/workbench/__init__.py` version is `0.2.0`
- [ ] FastAPI app version in `src/workbench/app.py` resolves from `workbench.__version__`
- [ ] `/api/source-of-truth` reports `api_version = 0.2.0`
- [ ] `/api/source-of-truth` reports `contract_version = 0.2.0`
- [ ] plugin manifest version in `plugins/data-workbench/.codex-plugin/plugin.json` is `0.2.0`

## Public docs

- [ ] `README.md` reflects Docker-first quickstart
- [ ] `README.md` reflects the frozen public API surface
- [ ] `docs/architecture.md` reflects current module boundaries and truth layers
- [ ] `docs/onboarding_wizard_quickstart.md` matches the demo flow
- [ ] `examples/onboarding_wizard_demo/README.md` matches the current app walkthrough
- [ ] `docs/release_notes_0_2_0.md` reflects what is stable versus experimental
- [ ] `CONTRIBUTING.md` is present and current
- [ ] `LICENSE` is present and public

## Runtime and smoke paths

- [ ] `docker compose config`
- [ ] `docker compose up --build`
- [ ] Docker app responds on `/healthz`
- [ ] Docker app responds on `/api/source-of-truth`
- [ ] local contributor path starts with `PYTHONPATH=src python -m workbench.app`
- [ ] local contributor path responds on `/healthz`
- [ ] local contributor path responds on `/api/source-of-truth`

## Test gates

- [ ] `PYTHONPATH=src python -m unittest discover -s tests`
- [ ] browser E2E suite passes
- [ ] persistence integration suite passes when enabled
- [ ] release docs parity tests pass
- [ ] contract tests pass

## Demo and workflow gates

- [ ] onboarding demo can be discovered and bootstrapped
- [ ] graph can be saved and latest plan artifacts are produced
- [ ] execution tasks can be created or derived
- [ ] structure scan and review loop works
- [ ] agent workflow brief and guided launch loop works

## Public positioning check

- [ ] release notes clearly say this is contributor-ready, not mass-market polished
- [ ] docs clearly say macOS and Linux are the first-class tested platforms
- [ ] docs clearly say Windows via Docker may work but is not yet officially supported
- [ ] Docker Compose is documented as the primary runtime path
- [ ] repo-local Codex plugin scaffold is clearly labeled optional

## Tag prep

- [ ] working tree reviewed for intentional changes
- [ ] unsupported claims removed from docs
- [ ] stale or duplicate docs removed or updated
- [ ] final smoke run performed after any last-minute edits
- [ ] release notes ready to use as the GitHub release body
