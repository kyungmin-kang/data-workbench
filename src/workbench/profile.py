from __future__ import annotations

import gzip
import io
from copy import deepcopy
from pathlib import Path
from zipfile import ZipFile

import polars as pl

from .types import build_index


SAMPLED_FILE_SIZE_BYTES = 2_000_000
SAMPLED_ROW_LIMIT = 500
SUPPORTED_EXTENSIONS = {".csv", ".gz", ".parquet", ".zip"}


def profile_graph(graph: dict, root_dir: Path) -> dict:
    updated = deepcopy(graph)
    index = build_index(updated)
    for node in updated["nodes"]:
        if node["kind"] != "data":
            continue
        asset = resolve_profile_asset(node, index, root_dir)
        if asset:
            node["data"]["profile_target"] = asset["profile_target"]
        if asset and asset.get("accessible"):
            profile = profile_asset(asset, root_dir)
            if profile:
                node["columns"] = merge_profile_into_columns(node.get("columns", []), profile["columns"])
                node["profile_status"] = profile["profile_status"]
                node["data"]["row_count"] = profile["row_count"]
                node["data"]["sampled"] = profile["profile_status"] == "sampled_profile"
                node["data"]["profile_target"] = profile["profile_target"]
                continue
        node["profile_status"] = "schema_only"
        node["columns"] = clear_profile_metrics(node.get("columns", []))
        node["data"]["row_count"] = None
        node["data"]["sampled"] = False
    return updated


def resolve_profile_asset(node: dict, index: dict, root_dir: Path) -> dict | None:
    fallback: dict | None = None

    local_path = node.get("data", {}).get("local_path")
    if local_path:
        descriptor = build_asset_descriptor(local_path, root_dir=root_dir)
        if descriptor and descriptor.get("accessible"):
            return descriptor
        fallback = fallback or descriptor

    for edge in index["incoming"].get(node["id"], []):
        source_node = index["nodes"][edge["source"]]
        if source_node["kind"] != "source":
            continue
        for raw_asset in source_node.get("source", {}).get("raw_assets", []):
            if not raw_asset.get("profile_ready", True):
                continue
            descriptor = build_asset_descriptor(
                raw_asset.get("value", ""),
                kind=raw_asset.get("kind"),
                fmt=raw_asset.get("format"),
                root_dir=root_dir,
                label=raw_asset.get("label", ""),
            )
            if descriptor and descriptor.get("accessible"):
                return descriptor
            fallback = fallback or descriptor
        origin = source_node.get("source", {}).get("origin", {})
        if origin.get("kind") == "disk_path" and origin.get("value"):
            descriptor = build_asset_descriptor(origin["value"], root_dir=root_dir)
            if descriptor and descriptor.get("accessible"):
                return descriptor
            fallback = fallback or descriptor
    return fallback


def build_asset_descriptor(
    value: str,
    *,
    kind: str | None = None,
    fmt: str | None = None,
    root_dir: Path,
    label: str = "",
) -> dict | None:
    if not value:
        return None

    normalized_kind = None if kind in {None, "", "unknown"} else kind
    normalized_format = None if fmt in {None, "", "unknown"} else fmt
    inferred_kind = normalized_kind or infer_asset_kind(value)
    inferred_format = normalized_format or infer_asset_format(value, inferred_kind)
    if inferred_kind == "object_storage":
        return {
            "label": label,
            "kind": inferred_kind,
            "format": inferred_format,
            "value": value,
            "accessible": False,
            "profile_target": value,
        }

    if inferred_kind == "glob":
        matches = sorted(root_dir.glob(value))
        return {
            "label": label,
            "kind": inferred_kind,
            "format": inferred_format,
            "value": value,
            "paths": matches,
            "accessible": bool(matches),
            "profile_target": value,
        }

    path = root_dir / value
    if inferred_kind == "directory":
        matches = sorted(path.glob("*.parquet"))
        return {
            "label": label,
            "kind": inferred_kind,
            "format": inferred_format,
            "value": value,
            "paths": matches,
            "accessible": path.exists() and bool(matches),
            "profile_target": value,
        }

    return {
        "label": label,
        "kind": inferred_kind,
        "format": inferred_format,
        "value": value,
        "path": path,
        "accessible": path.exists(),
        "profile_target": value,
    }


def profile_asset(asset: dict, root_dir: Path) -> dict | None:
    fmt = asset.get("format", "unknown")
    if fmt == "csv":
        return profile_frame(read_csv_asset(asset), asset["profile_target"], asset)
    if fmt == "csv_gz":
        return profile_frame(read_gzip_csv_asset(asset), asset["profile_target"], asset)
    if fmt == "zip_csv":
        return profile_frame(read_zip_csv_asset(asset), asset["profile_target"], asset)
    if fmt == "parquet":
        return profile_frame(pl.read_parquet(asset["path"]), asset["profile_target"], asset)
    if fmt == "parquet_collection":
        return profile_frame(read_parquet_collection(asset), asset["profile_target"], asset)
    return None


def profile_csv(path: Path) -> dict:
    asset = build_asset_descriptor(str(path), root_dir=Path("."), kind="file", fmt=infer_asset_format(str(path), "file"))
    assert asset is not None
    asset["path"] = path
    asset["accessible"] = path.exists()
    asset["profile_target"] = str(path)
    profile = profile_asset(asset, Path(".")) if asset["accessible"] else None
    if profile is None:
        raise ValueError(f"Unable to profile asset at {path}")
    return profile


def profile_frame(frame: pl.DataFrame, profile_target: str, asset: dict) -> dict:
    if frame.columns:
        non_empty_row = None
        for column_name in frame.columns:
            expression = pl.col(column_name).is_not_null()
            non_empty_row = expression if non_empty_row is None else (non_empty_row | expression)
        frame = frame.filter(non_empty_row)
    row_count = frame.height
    sampled = asset_is_large(asset, row_count)
    sample_frame = frame.head(SAMPLED_ROW_LIMIT) if sampled else frame
    return {
        "row_count": row_count,
        "profile_status": "sampled_profile" if sampled else "profiled",
        "profile_target": profile_target,
        "columns": [_profile_series(sample_frame.get_column(column_name), sample_frame.height) for column_name in sample_frame.columns],
    }


def read_csv_asset(asset: dict) -> pl.DataFrame:
    return pl.read_csv(asset["path"], try_parse_dates=True)


def read_gzip_csv_asset(asset: dict) -> pl.DataFrame:
    with gzip.open(asset["path"], "rb") as handle:
        return pl.read_csv(io.BytesIO(handle.read()), try_parse_dates=True)


def read_zip_csv_asset(asset: dict) -> pl.DataFrame:
    with ZipFile(asset["path"]) as archive:
        csv_names = sorted(name for name in archive.namelist() if name.lower().endswith(".csv"))
        if not csv_names:
            raise ValueError(f"No CSV file found inside archive {asset['path']}")
        with archive.open(csv_names[0]) as handle:
            return pl.read_csv(io.BytesIO(handle.read()), try_parse_dates=True)


def read_parquet_collection(asset: dict) -> pl.DataFrame:
    paths = asset.get("paths", [])
    if not paths:
        raise ValueError(f"No parquet files found for {asset['profile_target']}")
    frames = [pl.read_parquet(path) for path in paths]
    return pl.concat(frames, how="diagonal_relaxed") if len(frames) > 1 else frames[0]


def merge_profile_into_columns(existing_columns: list[dict], profiled_columns: list[dict]) -> list[dict]:
    existing = {column["name"]: deepcopy(column) for column in existing_columns}
    merged: list[dict] = []
    for profiled_column in profiled_columns:
        column = existing.get(profiled_column["name"], {"name": profiled_column["name"]})
        column["data_type"] = profiled_column["data_type"]
        column["nullable"] = profiled_column["nullable"]
        column["null_pct"] = profiled_column["null_pct"]
        column["stats"] = profiled_column["stats"]
        merged.append(column)
    return merged


def clear_profile_metrics(columns: list[dict]) -> list[dict]:
    cleared: list[dict] = []
    for existing_column in columns:
        column = deepcopy(existing_column)
        column["null_pct"] = None
        column["stats"] = {}
        cleared.append(column)
    return cleared


def _profile_series(series: pl.Series, sample_row_count: int) -> dict:
    non_null_series = series.drop_nulls()
    inferred_type = infer_column_type(series.dtype)
    stats = build_stats(non_null_series, inferred_type)
    null_count = sample_row_count - non_null_series.len()
    return {
        "name": series.name,
        "data_type": inferred_type,
        "nullable": null_count > 0,
        "null_pct": round((null_count / sample_row_count) * 100, 2) if sample_row_count else None,
        "stats": stats,
    }


def infer_column_type(dtype: pl.DataType) -> str:
    if dtype.is_integer():
        return "integer"
    if dtype.is_float():
        return "float"
    if dtype == pl.Boolean:
        return "boolean"
    if dtype in {pl.Date, pl.Datetime, pl.Time}:
        return "date"
    return "string"


def build_stats(series: pl.Series, inferred_type: str) -> dict:
    if series.is_empty():
        return {}
    if inferred_type in {"integer", "float"}:
        numeric_series = series.cast(pl.Float64)
        return {
            "mean": round(float(numeric_series.mean()), 4),
            "std": round(float(numeric_series.std() or 0.0), 4),
            "min": round(float(numeric_series.min()), 4),
            "max": round(float(numeric_series.max()), 4),
        }

    value_counts = (
        pl.DataFrame({"value": series.cast(pl.Utf8)})
        .group_by("value")
        .len()
        .sort("len", descending=True)
        .head(3)
    )
    top_values = [(row[0], int(row[1])) for row in value_counts.iter_rows()]
    most_common_value, count = top_values[0]
    return {
        "mode": most_common_value,
        "mode_count": count,
        "top_values": top_values,
    }


def infer_asset_kind(value: str) -> str:
    if value.startswith(("s3://", "minio://", "gs://")):
        return "object_storage"
    if "*" in value or "?" in value or "[" in value:
        return "glob"
    path = Path(value)
    if value.endswith("/") or path.suffix == "":
        return "directory"
    return "file"


def infer_asset_format(value: str, kind: str) -> str:
    lower_value = value.lower()
    if kind in {"glob", "directory"}:
        return "parquet_collection" if "parquet" in lower_value or kind == "directory" else "unknown"
    if lower_value.endswith(".csv"):
        return "csv"
    if lower_value.endswith(".csv.gz") or lower_value.endswith(".gz"):
        return "csv_gz"
    if lower_value.endswith(".parquet"):
        return "parquet"
    if lower_value.endswith(".zip"):
        return "zip_csv"
    return "unknown"


def asset_is_large(asset: dict, row_count: int) -> bool:
    if row_count > SAMPLED_ROW_LIMIT:
        return True
    if asset.get("kind") in {"glob", "directory"}:
        return len(asset.get("paths", [])) > 1
    path = asset.get("path")
    if path and path.exists():
        return path.stat().st_size > SAMPLED_FILE_SIZE_BYTES
    return False
