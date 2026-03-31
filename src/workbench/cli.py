from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

from .store import export_canonical_yaml_text, get_root_dir, list_bundles, load_bundle
from .structure_memory import (
    import_yaml_spec,
    merge_bundle,
    preview_rebase_bundle,
    rebase_bundle,
    review_bundle_patch,
    scan_structure,
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1
    try:
        result = handler(args)
    except Exception as error:  # pragma: no cover - surfaced to the user
        print(str(error), file=sys.stderr)
        return 1
    if result is not None:
        print(result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="data-workbench-structure", description="YAML-centered structure memory CLI")
    subparsers = parser.add_subparsers(dest="command")

    export_parser = subparsers.add_parser("export", help="Export the canonical structure YAML")
    export_parser.add_argument("--format", choices=["yaml"], default="yaml")
    export_parser.add_argument("--output", default="", help="Optional output file path")
    export_parser.set_defaults(handler=handle_export)

    import_parser = subparsers.add_parser("import", help="Import YAML or inspect a repo/docs into a review bundle")
    import_parser.add_argument("target", help="Path to a YAML spec, repo root, or doc file")
    import_parser.add_argument("--updated-by", default="user")
    import_parser.add_argument("--agent", choices=["scout", "recorder"], default="scout")
    import_parser.add_argument("--scope", choices=["changed", "paths", "full"], default="full")
    import_parser.add_argument("--include-tests", action="store_true")
    import_parser.add_argument("--include-internal", action="store_true")
    import_parser.add_argument("--doc-path", action="append", dest="doc_paths", default=[])
    import_parser.add_argument("--selected-path", action="append", dest="selected_paths", default=[])
    import_parser.set_defaults(handler=handle_import_like)

    sync_parser = subparsers.add_parser("sync", help="Run an incremental repo/doc scan into a review bundle")
    sync_parser.add_argument("target", nargs="?", default=".")
    sync_parser.add_argument("--agent", choices=["scout", "recorder"], default="scout")
    sync_parser.add_argument("--scope", choices=["changed", "paths", "full"], default="changed")
    sync_parser.add_argument("--include-tests", action="store_true")
    sync_parser.add_argument("--include-internal", action="store_true")
    sync_parser.add_argument("--doc-path", action="append", dest="doc_paths", default=[])
    sync_parser.add_argument("--selected-path", action="append", dest="selected_paths", default=[])
    sync_parser.set_defaults(handler=handle_import_like)

    bundles_parser = subparsers.add_parser("bundles", help="List saved review bundles")
    bundles_parser.set_defaults(handler=handle_bundles)

    bundle_parser = subparsers.add_parser("bundle", help="Show a saved review bundle")
    bundle_parser.add_argument("bundle_id")
    bundle_parser.set_defaults(handler=handle_bundle)

    review_parser = subparsers.add_parser("review", help="Accept, reject, or defer a patch in a bundle")
    review_parser.add_argument("bundle_id")
    review_parser.add_argument("patch_id")
    review_parser.add_argument("decision", choices=["accepted", "rejected", "deferred"])
    review_parser.set_defaults(handler=handle_review)

    merge_parser = subparsers.add_parser("merge", help="Merge accepted patches from a bundle into canonical YAML")
    merge_parser.add_argument("bundle_id")
    merge_parser.add_argument("--merged-by", default="user")
    merge_parser.set_defaults(handler=handle_merge)

    rebase_parser = subparsers.add_parser("rebase", help="Regenerate a bundle against the latest canonical YAML")
    rebase_parser.add_argument("bundle_id")
    rebase_parser.add_argument("--preserve-reviews", dest="preserve_reviews", action="store_true", default=True)
    rebase_parser.add_argument("--no-preserve-reviews", dest="preserve_reviews", action="store_false")
    rebase_parser.set_defaults(handler=handle_rebase)

    rebase_preview_parser = subparsers.add_parser("rebase-preview", help="Preview how a stale bundle would rebase")
    rebase_preview_parser.add_argument("bundle_id")
    rebase_preview_parser.add_argument("--preserve-reviews", dest="preserve_reviews", action="store_true", default=True)
    rebase_preview_parser.add_argument("--no-preserve-reviews", dest="preserve_reviews", action="store_false")
    rebase_preview_parser.set_defaults(handler=handle_rebase_preview)

    return parser


def handle_export(args: argparse.Namespace) -> str | None:
    yaml_text = export_canonical_yaml_text()
    if args.output:
        path = Path(args.output).expanduser()
        if not path.is_absolute():
            path = (get_root_dir() / path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml_text, encoding="utf-8")
        return f"Wrote YAML to {path}"
    return yaml_text


def handle_import_like(args: argparse.Namespace) -> str:
    target = Path(args.target).expanduser()
    if not target.is_absolute():
        target = (get_root_dir() / target).resolve()
    if target.is_file() and target.suffix.lower() in {".yaml", ".yml"}:
        payload = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
        result = import_yaml_spec(payload, updated_by=getattr(args, "updated_by", "user"))
        return json.dumps(
            {
                "graph_name": result["graph"]["metadata"]["name"],
                "structure_version": result["structure"]["structure_version"],
                "updated_by": result["structure"]["updated_by"],
            },
            indent=2,
        )

    if target.is_file():
        result = scan_structure(
            root_dir=target.parent,
            role=args.agent,
            scope=args.scope,
            include_tests=args.include_tests,
            include_internal=args.include_internal,
            doc_paths=[str(target)],
            selected_paths=args.selected_paths,
        )
        return json.dumps(
            {
                "bundle_id": result["bundle"]["bundle_id"],
                "patch_count": len(result["bundle"]["patches"]),
                "role": result["bundle"]["scan"]["role"],
                "scope": result["bundle"]["scan"]["scope"],
                "planned_missing": result["bundle"]["reconciliation"].get("summary", {}).get("planned_missing", 0),
                "observed_untracked": result["bundle"]["reconciliation"].get("summary", {}).get("observed_untracked", 0),
                "implemented_differently": result["bundle"]["reconciliation"].get("summary", {}).get("implemented_differently", 0),
                "uncertain_matches": result["bundle"]["reconciliation"].get("summary", {}).get("uncertain_matches", 0),
                "binding_mismatches": result["bundle"]["reconciliation"].get("comparison", {}).get("binding_mismatches", 0),
                "column_mismatches": result["bundle"]["reconciliation"].get("comparison", {}).get("column_mismatches", 0),
                "field_matrix_review_required_count": result["bundle"]["reconciliation"].get("field_matrix_summary", {}).get("review_required_count", 0),
                "contradiction_cluster_count": len(result["bundle"]["reconciliation"].get("contradiction_clusters", [])),
                "open_contradiction_clusters": result["bundle"]["reconciliation"].get("contradiction_cluster_summary", {}).get("open_count", 0),
                "resolved_contradiction_clusters": result["bundle"]["reconciliation"].get("contradiction_cluster_summary", {}).get("resolved_count", 0),
                "mixed_contradiction_clusters": result["bundle"]["reconciliation"].get("contradiction_cluster_summary", {}).get("resolution_state_counts", {}).get("mixed", 0),
                "high_severity_contradictions": result["bundle"]["reconciliation"].get("contradiction_summary", {}).get("severity_counts", {}).get("high", 0),
                "downstream_breakage_count": result["bundle"]["reconciliation"].get("downstream_breakage", {}).get("count", 0),
                "merge_plan_status": result["bundle"].get("review", {}).get("merge_plan", {}).get("status", ""),
                "merge_plan_noop_count": result["bundle"].get("review", {}).get("merge_plan", {}).get("noop_count", 0),
            },
            indent=2,
        )

    result = scan_structure(
        root_dir=target,
        role=args.agent,
        scope=args.scope,
        include_tests=args.include_tests,
        include_internal=args.include_internal,
        doc_paths=args.doc_paths,
        selected_paths=args.selected_paths,
    )
    return json.dumps(
        {
            "bundle_id": result["bundle"]["bundle_id"],
            "patch_count": len(result["bundle"]["patches"]),
            "role": result["bundle"]["scan"]["role"],
            "scope": result["bundle"]["scan"]["scope"],
            "planned_missing": result["bundle"]["reconciliation"].get("summary", {}).get("planned_missing", 0),
            "observed_untracked": result["bundle"]["reconciliation"].get("summary", {}).get("observed_untracked", 0),
            "implemented_differently": result["bundle"]["reconciliation"].get("summary", {}).get("implemented_differently", 0),
            "uncertain_matches": result["bundle"]["reconciliation"].get("summary", {}).get("uncertain_matches", 0),
            "binding_mismatches": result["bundle"]["reconciliation"].get("comparison", {}).get("binding_mismatches", 0),
            "column_mismatches": result["bundle"]["reconciliation"].get("comparison", {}).get("column_mismatches", 0),
            "field_matrix_review_required_count": result["bundle"]["reconciliation"].get("field_matrix_summary", {}).get("review_required_count", 0),
            "contradiction_cluster_count": len(result["bundle"]["reconciliation"].get("contradiction_clusters", [])),
            "open_contradiction_clusters": result["bundle"]["reconciliation"].get("contradiction_cluster_summary", {}).get("open_count", 0),
            "resolved_contradiction_clusters": result["bundle"]["reconciliation"].get("contradiction_cluster_summary", {}).get("resolved_count", 0),
            "mixed_contradiction_clusters": result["bundle"]["reconciliation"].get("contradiction_cluster_summary", {}).get("resolution_state_counts", {}).get("mixed", 0),
            "high_severity_contradictions": result["bundle"]["reconciliation"].get("contradiction_summary", {}).get("severity_counts", {}).get("high", 0),
            "downstream_breakage_count": result["bundle"]["reconciliation"].get("downstream_breakage", {}).get("count", 0),
            "merge_plan_status": result["bundle"].get("review", {}).get("merge_plan", {}).get("status", ""),
            "merge_plan_noop_count": result["bundle"].get("review", {}).get("merge_plan", {}).get("noop_count", 0),
        },
        indent=2,
    )


def handle_bundles(args: argparse.Namespace) -> str:
    return json.dumps({"bundles": list_bundles()}, indent=2)


def handle_bundle(args: argparse.Namespace) -> str:
    bundle = load_bundle(args.bundle_id)
    if bundle is None:
        raise ValueError(f"Bundle not found: {args.bundle_id}")
    return yaml.safe_dump(bundle, sort_keys=False, allow_unicode=False, width=120)


def handle_review(args: argparse.Namespace) -> str:
    result = review_bundle_patch(args.bundle_id, args.patch_id, args.decision)
    return json.dumps(
        {
            "bundle_id": result["bundle"]["bundle_id"],
            "patch_id": args.patch_id,
            "decision": args.decision,
        },
        indent=2,
    )


def handle_merge(args: argparse.Namespace) -> str:
    result = merge_bundle(args.bundle_id, merged_by=args.merged_by)
    return json.dumps(
        {
            "bundle_id": result["bundle"]["bundle_id"],
            "structure_version": result["structure"]["structure_version"],
            "updated_by": result["structure"]["updated_by"],
        },
        indent=2,
    )


def handle_rebase(args: argparse.Namespace) -> str:
    result = rebase_bundle(args.bundle_id, preserve_reviews=args.preserve_reviews)
    return json.dumps(
        {
            "bundle_id": result["bundle"]["bundle_id"],
            "rebased_from_bundle_id": result["rebased_from_bundle_id"],
            "transferred_review_count": result["transferred_review_count"],
            "preserved_review_states": result["preserved_review_states"],
            "dropped_review_count": result["dropped_review_count"],
            "dropped_review_states": result["dropped_review_states"],
            "merge_plan_status": result["bundle"].get("review", {}).get("merge_plan", {}).get("status", ""),
            "merge_plan_noop_count": result["bundle"].get("review", {}).get("merge_plan", {}).get("noop_count", 0),
        },
        indent=2,
    )


def handle_rebase_preview(args: argparse.Namespace) -> str:
    result = preview_rebase_bundle(args.bundle_id, preserve_reviews=args.preserve_reviews)
    return json.dumps(result["preview"], indent=2)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
