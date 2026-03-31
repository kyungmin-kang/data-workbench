2026-03-30

- Inspected current SQL/ORM hint extraction and bundle merge flow.
- Extracted owned scanner modules for SQL and ORM coverage to keep profiler integration thin.
- Added richer SQL lineage for joins, aliases, CTEs, derived projections, wildcard expansion, and ambiguity surfacing.
- Added richer ORM coverage for SQLAlchemy tables, association tables, relationship metadata, and query projection hints.
- Updated bundle merge to preserve field-level confidence/evidence and skip non-materialized ORM query hints during observed graph application.
- Verified targeted SQL/ORM slices and the full suite with `PYTHONPATH=src python -m pytest -q` (`83 passed, 3 skipped`).
- Extended SQL coverage for quoted identifiers, nested subquery inputs, `UNION` set operations, and windowed projections with deterministic unresolved handling.
- Extended ORM coverage for local `aliased(...)` bindings, loader-option projections via `load_only(...)`, eager relation hints via `joinedload(...)`, and hybrid expression-backed fields.
- Added mixed SQL/ORM idempotence fixtures and re-verified the full suite with `PYTHONPATH=src python -m pytest -q` (`94 passed, 3 skipped`).
- Extended SQL CTE handling for explicit column alias lists, propagated confidence/data types through transformed CTE outputs, and stripped string literals from source-token resolution to avoid false lineage.
- Extended ORM query-result inference for local `stmt/query` variables and explicit selected-column chains such as `with_entities(...)`, while keeping query projection hints deterministic.
- Added focused regressions for CTE alias/literal handling and query-variable result inference, then re-verified the full suite with `PYTHONPATH=src python -m pytest -q` (`118 passed, 3 skipped`).
- Tightened backend-side SQL/ORM evidence by marking cross-confirmed SQL/ORM relations and fields with `schema_match`, keeping confidence/evidence explicit instead of implicit.
- Materialized ORM query projections into stable observed compute/data nodes with deterministic field IDs and lineage back to source relation fields, so query-result structure is no longer dropped as table-only hints.
- Added regressions for schema-match evidence, observed ORM query-result nodes, and mixed SQL/ORM idempotence, then re-verified targeted slices plus the full suite with `PYTHONPATH=src python -m pytest -q` (`129 passed, 3 skipped`).
- Extended ORM scan context across imported modules so query files can resolve imported model classes, association tables, and relationship-target hints instead of only same-file symbols.
- Surfaced unresolved ORM projection fields explicitly with low confidence and `unresolved_sources` markers for raw SQL and unsupported result expressions, rather than silently dropping those selected columns.
- Added repo-shaped regressions for imported-module ORM queries, unresolved projection surfacing, and SQL/ORM-backed downstream binding breakage, then re-verified `tests/test_sql_orm_coverage.py`, targeted integration slices, and the full suite with `PYTHONPATH=src python -m pytest -q` (`134 passed, 3 skipped`).
