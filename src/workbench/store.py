from __future__ import annotations

import io
import json
import importlib
import os
from functools import lru_cache
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .types import PATCH_TYPE_PRECEDENCE, ScanBundle, normalize_graph


ROOT_DIR = Path(__file__).resolve().parents[2]
GRAPH_JSON_KEY = "graphs/current.json"
GRAPH_YAML_KEY = "graphs/current.yaml"
LATEST_PLAN_JSON_KEY = "plans/latest.plan.json"
LATEST_PLAN_MARKDOWN_KEY = "plans/latest.plan.md"
LATEST_PLAN_ARTIFACTS_KEY = "plans/latest.artifacts.json"
ONBOARDING_PRESETS_KEY = "presets/onboarding.json"
BUNDLE_PREFIX = "bundles/"


class PostgresDocumentStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        try:
            self.psycopg = importlib.import_module("psycopg")
        except ModuleNotFoundError as error:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Postgres persistence requires psycopg. Install the project with the 'persistence' extra."
            ) from error
        self._schema_ready = False

    def _connect(self):
        return self.psycopg.connect(self.dsn)

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workbench_documents (
                        key TEXT PRIMARY KEY,
                        content TEXT NOT NULL,
                        content_type TEXT NOT NULL DEFAULT 'text/plain',
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
        self._schema_ready = True

    def read_text(self, key: str) -> str | None:
        self._ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT content FROM workbench_documents WHERE key = %s", (key,))
                row = cursor.fetchone()
        if not row:
            return None
        return str(row[0])

    def write_text(self, key: str, content: str, *, content_type: str = "text/plain") -> None:
        self._ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO workbench_documents (key, content, content_type, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (key)
                    DO UPDATE SET
                        content = EXCLUDED.content,
                        content_type = EXCLUDED.content_type,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (key, content, content_type),
                )

    def list_documents(self, prefix: str) -> list[tuple[str, str]]:
        self._ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT key, content FROM workbench_documents WHERE key LIKE %s ORDER BY key",
                    (f"{prefix}%",),
                )
                rows = cursor.fetchall() or []
        return [(str(row[0]), str(row[1])) for row in rows]


class MinioObjectStore:
    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool,
    ) -> None:
        try:
            minio_module = importlib.import_module("minio")
            error_module = importlib.import_module("minio.error")
        except ModuleNotFoundError as error:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "MinIO persistence requires the minio package. Install the project with the 'persistence' extra."
            ) from error
        self.client = minio_module.Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.bucket_name = bucket_name
        self.s3_error = error_module.S3Error
        self._bucket_ready = False

    def _ensure_bucket(self) -> None:
        if self._bucket_ready:
            return
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
        self._bucket_ready = True

    def read_text(self, key: str) -> str | None:
        self._ensure_bucket()
        response = None
        try:
            response = self.client.get_object(self.bucket_name, key)
            return response.read().decode("utf-8")
        except self.s3_error as error:
            if getattr(error, "code", "") in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
                return None
            raise
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def write_text(self, key: str, content: str, *, content_type: str = "text/plain") -> None:
        self._ensure_bucket()
        payload = content.encode("utf-8")
        self.client.put_object(
            self.bucket_name,
            key,
            io.BytesIO(payload),
            length=len(payload),
            content_type=content_type,
        )

    def list_documents(self, prefix: str) -> list[tuple[str, str]]:
        self._ensure_bucket()
        items: list[tuple[str, str]] = []
        for entry in self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True):
            content = self.read_text(entry.object_name)
            if content is not None:
                items.append((entry.object_name, content))
        return items


def get_root_dir() -> Path:
    override = os.environ.get("WORKBENCH_ROOT_DIR")
    return Path(override) if override else ROOT_DIR


def get_structure_dir() -> Path:
    return get_root_dir() / "specs" / "structure"


def get_spec_path() -> Path:
    return get_structure_dir() / "spec.yaml"


def get_legacy_spec_path() -> Path:
    return get_root_dir() / "specs" / "workbench.graph.json"


def get_bundles_dir() -> Path:
    return get_structure_dir() / "bundles"


def get_plans_dir() -> Path:
    return get_root_dir() / "runtime" / "plans"


def get_cache_dir() -> Path:
    return get_root_dir() / "runtime" / "cache"


def get_onboarding_presets_path() -> Path:
    return get_root_dir() / "specs" / "onboarding_presets.json"


def get_persistence_backend() -> str:
    backend = (os.environ.get("WORKBENCH_PERSISTENCE_BACKEND") or "local").strip().lower()
    if backend not in {"local", "postgres", "mirror"}:
        return "local"
    return backend


def local_persistence_enabled() -> bool:
    return get_persistence_backend() in {"local", "mirror"}


def postgres_persistence_enabled() -> bool:
    return get_persistence_backend() in {"postgres", "mirror"}


def object_store_enabled() -> bool:
    backend = (os.environ.get("WORKBENCH_OBJECT_STORE_BACKEND") or "").strip().lower()
    return backend in {"minio", "mirror"}


def get_persistence_prefix() -> str:
    return (os.environ.get("WORKBENCH_PERSISTENCE_PREFIX") or "").strip().strip("/")


def storage_key(key: str) -> str:
    normalized = key.lstrip("/")
    prefix = get_persistence_prefix()
    return f"{prefix}/{normalized}" if prefix else normalized


def strip_storage_prefix(key: str) -> str:
    normalized = key.lstrip("/")
    prefix = get_persistence_prefix()
    marker = f"{prefix}/"
    if prefix and normalized.startswith(marker):
        return normalized[len(marker):]
    return normalized


@lru_cache(maxsize=1)
def _get_postgres_document_store() -> PostgresDocumentStore:
    dsn = (os.environ.get("WORKBENCH_POSTGRES_DSN") or "").strip()
    if not dsn:
        raise RuntimeError("WORKBENCH_POSTGRES_DSN is required when Postgres persistence is enabled.")
    return PostgresDocumentStore(dsn)


@lru_cache(maxsize=1)
def _get_object_store() -> MinioObjectStore:
    endpoint = (os.environ.get("WORKBENCH_MINIO_ENDPOINT") or "").strip()
    access_key = (os.environ.get("WORKBENCH_MINIO_ACCESS_KEY") or "").strip()
    secret_key = (os.environ.get("WORKBENCH_MINIO_SECRET_KEY") or "").strip()
    bucket_name = (os.environ.get("WORKBENCH_MINIO_BUCKET") or "").strip()
    secure = (os.environ.get("WORKBENCH_MINIO_SECURE") or "0").strip() not in {"0", "false", "False"}
    if not endpoint or not access_key or not secret_key or not bucket_name:
        raise RuntimeError(
            "WORKBENCH_MINIO_ENDPOINT, WORKBENCH_MINIO_ACCESS_KEY, WORKBENCH_MINIO_SECRET_KEY, and WORKBENCH_MINIO_BUCKET are required when object storage is enabled."
        )
    return MinioObjectStore(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        secure=secure,
    )


def object_store_uri(key: str) -> str:
    bucket_name = (os.environ.get("WORKBENCH_MINIO_BUCKET") or "").strip() or "workbench"
    return f"minio://{bucket_name}/{storage_key(key)}"


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_graph(path: Path | None = None) -> dict[str, Any]:
    if path is not None:
        return _load_graph_local(path)
    if postgres_persistence_enabled():
        graph = _load_graph_postgres()
        if graph is not None:
            return graph
    if object_store_enabled():
        graph = _load_graph_object_store()
        if graph is not None:
            return graph
    graph = _load_graph_local()
    if postgres_persistence_enabled() and _local_graph_exists():
        _persist_graph_postgres(graph)
    if object_store_enabled() and _local_graph_exists():
        _persist_graph_object_store(graph)
    return graph


def _load_graph_local(path: Path | None = None) -> dict[str, Any]:
    path = path or get_spec_path()
    if path.suffix in {".yaml", ".yml"} and path.exists():
        with path.open(encoding="utf-8") as file:
            payload = yaml.safe_load(file) or {}
        legacy_path = get_legacy_spec_path()
        if legacy_path.exists():
            with legacy_path.open(encoding="utf-8") as file:
                legacy_payload = json.load(file)
            _merge_legacy_runtime_hints(payload, legacy_payload)
        return normalize_graph(payload)

    if path.exists():
        with path.open(encoding="utf-8") as file:
            return normalize_graph(json.load(file))

    legacy_path = get_legacy_spec_path()
    if legacy_path.exists():
        with legacy_path.open(encoding="utf-8") as file:
            graph = normalize_graph(json.load(file))
        save_graph(graph, updated_by="migration", increment_version=False)
        return graph
    return normalize_graph({})


def _load_graph_postgres() -> dict[str, Any] | None:
    store = _get_postgres_document_store()
    json_text = store.read_text(storage_key(GRAPH_JSON_KEY))
    if json_text:
        return normalize_graph(json.loads(json_text))
    yaml_text = store.read_text(storage_key(GRAPH_YAML_KEY))
    if yaml_text:
        return normalize_graph(yaml.safe_load(yaml_text) or {})
    return None


def _load_graph_object_store() -> dict[str, Any] | None:
    store = _get_object_store()
    json_text = store.read_text(storage_key(GRAPH_JSON_KEY))
    if json_text:
        return normalize_graph(json.loads(json_text))
    yaml_text = store.read_text(storage_key(GRAPH_YAML_KEY))
    if yaml_text:
        return normalize_graph(yaml.safe_load(yaml_text) or {})
    return None


def save_graph(
    graph: dict[str, Any],
    path: Path | None = None,
    *,
    updated_by: str = "user",
    increment_version: bool = True,
) -> dict[str, Any]:
    graph = normalize_graph(graph)
    previous_version = int(graph.get("metadata", {}).get("structure_version") or 1)
    graph["metadata"]["updated_at"] = utc_timestamp()
    graph["metadata"]["updated_by"] = updated_by
    if increment_version:
        graph["metadata"]["structure_version"] = previous_version + 1
    else:
        graph["metadata"]["structure_version"] = previous_version

    target_path = path or get_spec_path()
    if path is not None or local_persistence_enabled():
        _write_graph_local(graph, target_path)
    if path is None and postgres_persistence_enabled():
        _persist_graph_postgres(graph)
    if path is None and object_store_enabled():
        _persist_graph_object_store(graph)
    return graph


def canonical_yaml_payload(graph: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(graph))
    for node in payload.get("nodes", []):
        for field in node.get("contract", {}).get("fields", []):
            field.pop("sources", None)
        for feature in node.get("compute", {}).get("feature_selection", []):
            feature.pop("column_ref", None)
        node["columns"] = _stable_sorted(node.get("columns", []))
        node["contract"]["fields"] = _stable_sorted(node.get("contract", {}).get("fields", []))
        node["compute"]["feature_selection"] = sorted(
            node.get("compute", {}).get("feature_selection", []),
            key=lambda item: (
                item.get("order") if item.get("order") is not None else 10_000,
                item.get("field_id", ""),
                item.get("status", ""),
            ),
        )
    payload["nodes"] = _stable_sorted(payload.get("nodes", []))
    payload["edges"] = _stable_sorted(payload.get("edges", []))
    return payload


def write_plan_artifacts(plan: dict[str, Any]) -> dict[str, str]:
    timestamp = utc_timestamp().replace(":", "-")
    artifacts: dict[str, str] = {}
    if local_persistence_enabled():
        local_artifacts = _plan_artifact_paths(timestamp)
        _write_plan_artifacts_local(plan, local_artifacts)
        artifacts.update(local_artifacts)
    if postgres_persistence_enabled():
        _write_plan_artifacts_postgres(plan, timestamp)
    if object_store_enabled():
        _write_plan_artifacts_object_store(plan, timestamp)
        artifacts.update(_plan_artifact_object_paths(timestamp))
    _write_latest_plan_artifact_metadata(artifacts)
    return artifacts


def _write_plan_artifacts_local(plan: dict[str, Any], artifacts: dict[str, str]) -> None:
    plans_dir = get_plans_dir()
    plans_dir.mkdir(parents=True, exist_ok=True)
    latest_json_path = plans_dir / "latest.plan.json"
    latest_markdown_path = plans_dir / "latest.plan.md"
    timestamped_json_path = get_root_dir() / artifacts["timestamped_json"]
    timestamped_markdown_path = get_root_dir() / artifacts["timestamped_markdown"]

    for json_path in (latest_json_path, timestamped_json_path):
        with json_path.open("w", encoding="utf-8") as file:
            json.dump(plan, file, indent=2, ensure_ascii=True)
            file.write("\n")
    for markdown_path in (latest_markdown_path, timestamped_markdown_path):
        with markdown_path.open("w", encoding="utf-8") as file:
            file.write(plan["markdown"])

    return None


def _write_plan_artifacts_postgres(plan: dict[str, Any], timestamp: str) -> None:
    store = _get_postgres_document_store()
    json_text = json.dumps(plan, indent=2, ensure_ascii=True) + "\n"
    markdown_text = plan["markdown"]
    store.write_text(storage_key(LATEST_PLAN_JSON_KEY), json_text, content_type="application/json")
    store.write_text(storage_key(LATEST_PLAN_MARKDOWN_KEY), markdown_text, content_type="text/markdown")
    store.write_text(storage_key(f"plans/{timestamp}.plan.json"), json_text, content_type="application/json")
    store.write_text(storage_key(f"plans/{timestamp}.plan.md"), markdown_text, content_type="text/markdown")


def _write_plan_artifacts_object_store(plan: dict[str, Any], timestamp: str) -> None:
    store = _get_object_store()
    json_text = json.dumps(plan, indent=2, ensure_ascii=True) + "\n"
    markdown_text = plan["markdown"]
    store.write_text(storage_key(LATEST_PLAN_JSON_KEY), json_text, content_type="application/json")
    store.write_text(storage_key(LATEST_PLAN_MARKDOWN_KEY), markdown_text, content_type="text/markdown")
    store.write_text(storage_key(f"plans/{timestamp}.plan.json"), json_text, content_type="application/json")
    store.write_text(storage_key(f"plans/{timestamp}.plan.md"), markdown_text, content_type="text/markdown")


def _plan_artifact_paths(timestamp: str) -> dict[str, str]:
    return {
        "latest_json": "runtime/plans/latest.plan.json",
        "latest_markdown": "runtime/plans/latest.plan.md",
        "timestamped_json": f"runtime/plans/{timestamp}.plan.json",
        "timestamped_markdown": f"runtime/plans/{timestamp}.plan.md",
    }


def _plan_artifact_object_paths(timestamp: str) -> dict[str, str]:
    return {
        "remote_latest_json": object_store_uri(LATEST_PLAN_JSON_KEY),
        "remote_latest_markdown": object_store_uri(LATEST_PLAN_MARKDOWN_KEY),
        "remote_timestamped_json": object_store_uri(f"plans/{timestamp}.plan.json"),
        "remote_timestamped_markdown": object_store_uri(f"plans/{timestamp}.plan.md"),
    }


def _write_latest_plan_artifact_metadata(artifacts: dict[str, str]) -> None:
    metadata_text = json.dumps(artifacts, indent=2, ensure_ascii=True) + "\n"
    if local_persistence_enabled():
        path = get_plans_dir() / "latest.artifacts.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as file:
            file.write(metadata_text)
    if postgres_persistence_enabled():
        _get_postgres_document_store().write_text(
            storage_key(LATEST_PLAN_ARTIFACTS_KEY),
            metadata_text,
            content_type="application/json",
        )
    if object_store_enabled():
        _get_object_store().write_text(
            storage_key(LATEST_PLAN_ARTIFACTS_KEY),
            metadata_text,
            content_type="application/json",
        )


def load_latest_plan_artifacts() -> dict[str, str] | None:
    if postgres_persistence_enabled():
        store = _get_postgres_document_store()
        metadata_text = store.read_text(storage_key(LATEST_PLAN_ARTIFACTS_KEY))
        metadata = _parse_latest_plan_artifacts(metadata_text)
        if metadata is not None:
            return metadata
    if object_store_enabled():
        metadata_text = _get_object_store().read_text(storage_key(LATEST_PLAN_ARTIFACTS_KEY))
        metadata = _parse_latest_plan_artifacts(metadata_text)
        if metadata is not None:
            return metadata
    return _load_latest_plan_artifacts_local()


def _load_latest_plan_artifacts_local() -> dict[str, str] | None:
    path = get_plans_dir() / "latest.artifacts.json"
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    artifacts = {
        str(key): str(value)
        for key, value in payload.items()
        if isinstance(key, str) and isinstance(value, str) and value
    }
    return artifacts or None


def _parse_latest_plan_artifacts(payload_text: str | None) -> dict[str, str] | None:
    if not payload_text:
        return None
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    artifacts = {
        str(key): str(value)
        for key, value in payload.items()
        if isinstance(key, str) and isinstance(value, str) and value
    }
    return artifacts or None


def load_latest_plan() -> dict[str, Any] | None:
    if postgres_persistence_enabled():
        store = _get_postgres_document_store()
        latest_text = store.read_text(storage_key(LATEST_PLAN_JSON_KEY))
        if latest_text:
            return json.loads(latest_text)
    if object_store_enabled():
        latest_text = _get_object_store().read_text(storage_key(LATEST_PLAN_JSON_KEY))
        if latest_text:
            return json.loads(latest_text)
    return _load_latest_plan_local()


def _load_latest_plan_local() -> dict[str, Any] | None:
    latest_json_path = get_plans_dir() / "latest.plan.json"
    if not latest_json_path.exists():
        return None
    with latest_json_path.open(encoding="utf-8") as file:
        return json.load(file)


def save_bundle(bundle: dict[str, Any] | ScanBundle) -> dict[str, Any]:
    normalized = ScanBundle.model_validate(bundle).model_dump(mode="json")
    review = normalized.setdefault("review", {})
    for key in ("accepted_patch_ids", "rejected_patch_ids", "deferred_patch_ids"):
        review[key] = sorted(dict.fromkeys(review.get(key, [])))
    normalized["patches"] = sorted(
        normalized.get("patches", []),
        key=lambda item: (
            item.get("target_id", ""),
            PATCH_TYPE_PRECEDENCE.get(item.get("type", ""), 999),
            json.dumps(item.get("payload", {}), sort_keys=True),
        ),
    )
    normalized["contradictions"] = sorted(
        normalized.get("contradictions", []),
        key=lambda item: (item.get("target_id", ""), item.get("id", "")),
    )
    normalized["impacts"] = sorted(
        normalized.get("impacts", []),
        key=lambda item: (item.get("target_id", ""), item.get("message", "")),
    )
    normalized["reconciliation"] = _normalize_reconciliation(normalized.get("reconciliation", {}))
    if local_persistence_enabled():
        _save_bundle_local(normalized)
    if postgres_persistence_enabled():
        _save_bundle_postgres(normalized)
    if object_store_enabled():
        _save_bundle_object_store(normalized)
    return normalized


def load_bundle(bundle_id: str) -> dict[str, Any] | None:
    if postgres_persistence_enabled():
        bundle = _load_bundle_postgres(bundle_id)
        if bundle is not None:
            return bundle
    if object_store_enabled():
        bundle = _load_bundle_object_store(bundle_id)
        if bundle is not None:
            return bundle
    return _load_bundle_local(bundle_id)


def _load_bundle_local(bundle_id: str) -> dict[str, Any] | None:
    path = get_bundles_dir() / f"{bundle_id}.yaml"
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as file:
        payload = yaml.safe_load(file) or {}
    return ScanBundle.model_validate(payload).model_dump(mode="json")


def list_bundles() -> list[dict[str, Any]]:
    if postgres_persistence_enabled():
        items = _list_bundles_postgres()
        if items:
            return items
    if object_store_enabled():
        items = _list_bundles_object_store()
        if items:
            return items
    return _list_bundles_local()


def _list_bundles_local() -> list[dict[str, Any]]:
    bundles_dir = get_bundles_dir()
    if not bundles_dir.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(bundles_dir.glob("*.yaml")):
        with path.open(encoding="utf-8") as file:
            payload = yaml.safe_load(file) or {}
        items.append(_summarize_bundle_listing(payload))
    return items


def export_canonical_yaml_text() -> str:
    graph = load_graph()
    return _canonical_yaml_text(graph)


def _local_graph_exists() -> bool:
    return get_spec_path().exists() or get_legacy_spec_path().exists()


def _canonical_yaml_text(graph: dict[str, Any]) -> str:
    return yaml.safe_dump(canonical_yaml_payload(graph), sort_keys=False, allow_unicode=False, width=120)


def _write_graph_local(graph: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        file.write(_canonical_yaml_text(graph))
    legacy_path = get_legacy_spec_path()
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    with legacy_path.open("w", encoding="utf-8") as file:
        json.dump(graph, file, indent=2, ensure_ascii=True)
        file.write("\n")


def _persist_graph_postgres(graph: dict[str, Any]) -> None:
    store = _get_postgres_document_store()
    store.write_text(storage_key(GRAPH_JSON_KEY), json.dumps(graph, indent=2, ensure_ascii=True) + "\n", content_type="application/json")
    store.write_text(storage_key(GRAPH_YAML_KEY), _canonical_yaml_text(graph), content_type="application/yaml")


def _persist_graph_object_store(graph: dict[str, Any]) -> None:
    store = _get_object_store()
    store.write_text(storage_key(GRAPH_JSON_KEY), json.dumps(graph, indent=2, ensure_ascii=True) + "\n", content_type="application/json")
    store.write_text(storage_key(GRAPH_YAML_KEY), _canonical_yaml_text(graph), content_type="application/yaml")


def _save_bundle_local(bundle: dict[str, Any]) -> None:
    bundles_dir = get_bundles_dir()
    bundles_dir.mkdir(parents=True, exist_ok=True)
    path = bundles_dir / f"{bundle['bundle_id']}.yaml"
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(bundle, file, sort_keys=False, allow_unicode=False, width=120)


def _save_bundle_postgres(bundle: dict[str, Any]) -> None:
    store = _get_postgres_document_store()
    store.write_text(
        storage_key(f"{BUNDLE_PREFIX}{bundle['bundle_id']}.yaml"),
        yaml.safe_dump(bundle, sort_keys=False, allow_unicode=False, width=120),
        content_type="application/yaml",
    )


def _load_bundle_postgres(bundle_id: str) -> dict[str, Any] | None:
    store = _get_postgres_document_store()
    payload_text = store.read_text(storage_key(f"{BUNDLE_PREFIX}{bundle_id}.yaml"))
    if not payload_text:
        return None
    payload = yaml.safe_load(payload_text) or {}
    return ScanBundle.model_validate(payload).model_dump(mode="json")


def _list_bundles_postgres() -> list[dict[str, Any]]:
    store = _get_postgres_document_store()
    items: list[dict[str, Any]] = []
    for _, payload_text in store.list_documents(storage_key(BUNDLE_PREFIX)):
        payload = yaml.safe_load(payload_text) or {}
        items.append(_summarize_bundle_listing(payload))
    return items


def _save_bundle_object_store(bundle: dict[str, Any]) -> None:
    store = _get_object_store()
    store.write_text(
        storage_key(f"{BUNDLE_PREFIX}{bundle['bundle_id']}.yaml"),
        yaml.safe_dump(bundle, sort_keys=False, allow_unicode=False, width=120),
        content_type="application/yaml",
    )


def _load_bundle_object_store(bundle_id: str) -> dict[str, Any] | None:
    store = _get_object_store()
    payload_text = store.read_text(storage_key(f"{BUNDLE_PREFIX}{bundle_id}.yaml"))
    if not payload_text:
        return None
    payload = yaml.safe_load(payload_text) or {}
    return ScanBundle.model_validate(payload).model_dump(mode="json")


def _list_bundles_object_store() -> list[dict[str, Any]]:
    store = _get_object_store()
    items: list[dict[str, Any]] = []
    for _, payload_text in store.list_documents(storage_key(BUNDLE_PREFIX)):
        payload = yaml.safe_load(payload_text) or {}
        items.append(_summarize_bundle_listing(payload))
    return items


def _summarize_bundle_listing(payload: dict[str, Any]) -> dict[str, Any]:
    bundle = ScanBundle.model_validate(payload).model_dump(mode="json")
    review = bundle.get("review", {})
    return {
        "bundle_id": bundle["bundle_id"],
        "role": bundle.get("scan", {}).get("role", "scout"),
        "scope": bundle.get("scan", {}).get("scope", "full"),
        "base_structure_version": bundle.get("base_structure_version", 1),
        "created_at": bundle.get("scan", {}).get("created_at", ""),
        "patch_count": len(bundle.get("patches", [])),
        "pending_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "pending"),
        "accepted_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "accepted"),
        "rejected_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "rejected"),
        "deferred_count": sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "deferred"),
        "contradiction_count": len(bundle.get("contradictions", [])),
        "review_required_count": sum(1 for item in bundle.get("contradictions", []) if item.get("review_required", True)),
        "high_severity_contradiction_count": bundle.get("reconciliation", {}).get("contradiction_summary", {}).get("severity_counts", {}).get("high", 0),
        "contradiction_cluster_count": len(bundle.get("reconciliation", {}).get("contradiction_clusters", [])),
        "open_contradiction_cluster_count": bundle.get("reconciliation", {}).get("contradiction_cluster_summary", {}).get("open_count", 0),
        "resolved_contradiction_cluster_count": bundle.get("reconciliation", {}).get("contradiction_cluster_summary", {}).get("resolved_count", 0),
        "mixed_contradiction_cluster_count": bundle.get("reconciliation", {}).get("contradiction_cluster_summary", {}).get("resolution_state_counts", {}).get("mixed", 0),
        "field_matrix_review_required_count": bundle.get("reconciliation", {}).get("field_matrix_summary", {}).get("review_required_count", 0),
        "bundle_owner": review.get("bundle_owner", ""),
        "assigned_reviewer": review.get("assigned_reviewer", ""),
        "triage_state": review.get("triage_state", "new"),
        "triage_note": review.get("triage_note", ""),
        "last_reviewed_at": review.get("last_reviewed_at", ""),
        "last_reviewed_by": review.get("last_reviewed_by", ""),
        "merged_at": review.get("merged_at", ""),
        "merged_by": review.get("merged_by", ""),
        "merge_status": review.get("merge_status", ""),
        "rebase_required": bool(review.get("rebase_required", False)),
        "rebased_from_bundle_id": review.get("rebased_from_bundle_id", ""),
        "superseded_by_bundle_id": review.get("superseded_by_bundle_id", ""),
        "merge_plan_status": review.get("merge_plan", {}).get("status", ""),
        "merge_plan_noop_count": review.get("merge_plan", {}).get("noop_count", 0),
        "merge_plan_blocked_step_count": review.get("merge_plan", {}).get("blocked_step_count", 0),
        "ready_to_merge": bool(review.get("merge_patch_ids"))
        and sum(1 for patch in bundle.get("patches", []) if patch.get("review_state") == "pending") == 0
        and sum(1 for item in bundle.get("contradictions", []) if item.get("review_required", True)) == 0
        and not bool(review.get("rebase_required", False))
        and not bool(review.get("superseded_by_bundle_id", ""))
        and review.get("merge_plan", {}).get("status", "") in {"ready", "empty"},
        "readiness_status": bundle.get("readiness", {}).get("status", "Not Ready"),
        "planned_missing_count": bundle.get("reconciliation", {}).get("summary", {}).get("planned_missing", 0),
        "observed_untracked_count": bundle.get("reconciliation", {}).get("summary", {}).get("observed_untracked", 0),
        "implemented_differently_count": bundle.get("reconciliation", {}).get("summary", {}).get("implemented_differently", 0),
        "uncertain_matches_count": bundle.get("reconciliation", {}).get("summary", {}).get("uncertain_matches", 0),
        "binding_mismatch_count": bundle.get("reconciliation", {}).get("comparison", {}).get("binding_mismatches", 0),
        "column_mismatch_count": bundle.get("reconciliation", {}).get("comparison", {}).get("column_mismatches", 0),
        "downstream_breakage_count": bundle.get("reconciliation", {}).get("downstream_breakage", {}).get("count", 0),
    }


def load_onboarding_presets_payload() -> list[dict[str, Any]]:
    if postgres_persistence_enabled():
        store = _get_postgres_document_store()
        payload_text = store.read_text(storage_key(ONBOARDING_PRESETS_KEY))
        if payload_text:
            payload = json.loads(payload_text)
            return payload if isinstance(payload, list) else []
    if object_store_enabled():
        payload_text = _get_object_store().read_text(storage_key(ONBOARDING_PRESETS_KEY))
        if payload_text:
            payload = json.loads(payload_text)
            return payload if isinstance(payload, list) else []
    return _load_onboarding_presets_local()


def save_onboarding_presets_payload(presets: list[dict[str, Any]]) -> None:
    if local_persistence_enabled():
        _save_onboarding_presets_local(presets)
    if postgres_persistence_enabled():
        store = _get_postgres_document_store()
        store.write_text(
            storage_key(ONBOARDING_PRESETS_KEY),
            json.dumps(presets, indent=2, ensure_ascii=True) + "\n",
            content_type="application/json",
        )
    if object_store_enabled():
        _get_object_store().write_text(
            storage_key(ONBOARDING_PRESETS_KEY),
            json.dumps(presets, indent=2, ensure_ascii=True) + "\n",
            content_type="application/json",
        )


def _load_onboarding_presets_local() -> list[dict[str, Any]]:
    path = get_onboarding_presets_path()
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as file:
        data = json.load(file)
    return data if isinstance(data, list) else []


def _save_onboarding_presets_local(presets: list[dict[str, Any]]) -> None:
    path = get_onboarding_presets_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(presets, file, indent=2, ensure_ascii=True)
        file.write("\n")


def _stable_sorted(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: item.get("id", item.get("name", "")))


def _normalize_reconciliation(reconciliation: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(reconciliation, dict):
        return {}
    normalized = json.loads(json.dumps(reconciliation))
    summary = normalized.get("summary", {})
    normalized["summary"] = {
        "planned_missing": int(summary.get("planned_missing", 0) or 0),
        "observed_untracked": int(summary.get("observed_untracked", 0) or 0),
        "implemented_differently": int(summary.get("implemented_differently", 0) or 0),
        "uncertain_matches": int(summary.get("uncertain_matches", 0) or 0),
    }
    comparison = normalized.get("comparison", {})
    normalized["comparison"] = {
        "plan_candidates": int(comparison.get("plan_candidates", 0) or 0),
        "plan_paths": sorted(comparison.get("plan_paths", []) or []),
        "planned_nodes": int(comparison.get("planned_nodes", 0) or 0),
        "matched_nodes": int(comparison.get("matched_nodes", 0) or 0),
        "planned_fields": int(comparison.get("planned_fields", 0) or 0),
        "matched_fields": int(comparison.get("matched_fields", 0) or 0),
        "planned_bindings": int(comparison.get("planned_bindings", 0) or 0),
        "matched_bindings": int(comparison.get("matched_bindings", 0) or 0),
        "binding_mismatches": int(comparison.get("binding_mismatches", 0) or 0),
        "column_mismatches": int(comparison.get("column_mismatches", 0) or 0),
        "missing_fields": int(comparison.get("missing_fields", 0) or 0),
        "unplanned_fields": int(comparison.get("unplanned_fields", 0) or 0),
    }
    field_matrix_summary = normalized.get("field_matrix_summary", {})
    field_matrix_status_counts = field_matrix_summary.get("status_counts", {}) if isinstance(field_matrix_summary, dict) else {}
    normalized["field_matrix_summary"] = {
        "count": int(field_matrix_summary.get("count", 0) or 0),
        "review_required_count": int(field_matrix_summary.get("review_required_count", 0) or 0),
        "unresolved_count": int(field_matrix_summary.get("unresolved_count", 0) or 0),
        "implemented_differently_count": int(field_matrix_summary.get("implemented_differently_count", 0) or 0),
        "status_counts": {
            "matched": int(field_matrix_status_counts.get("matched", 0) or 0),
            "planned_missing": int(field_matrix_status_counts.get("planned_missing", 0) or 0),
            "observed_unplanned": int(field_matrix_status_counts.get("observed_unplanned", 0) or 0),
            "binding_mismatch": int(field_matrix_status_counts.get("binding_mismatch", 0) or 0),
            "lineage_mismatch": int(field_matrix_status_counts.get("lineage_mismatch", 0) or 0),
            "column_type_mismatch": int(field_matrix_status_counts.get("column_type_mismatch", 0) or 0),
            "uncertain": int(field_matrix_status_counts.get("uncertain", 0) or 0),
        },
    }
    normalized["field_matrix"] = sorted(
        normalized.get("field_matrix", []),
        key=lambda item: (
            item.get("scope", ""),
            item.get("node_ref", ""),
            item.get("field_name", ""),
            item.get("row_id", ""),
        ),
    )
    contradiction_summary = normalized.get("contradiction_summary", {})
    severity_counts = contradiction_summary.get("severity_counts", {}) if isinstance(contradiction_summary, dict) else {}
    normalized["contradiction_summary"] = {
        "count": int(contradiction_summary.get("count", 0) or 0),
        "review_required_count": int(contradiction_summary.get("review_required_count", 0) or 0),
        "severity_counts": {
            "high": int(severity_counts.get("high", 0) or 0),
            "medium": int(severity_counts.get("medium", 0) or 0),
            "low": int(severity_counts.get("low", 0) or 0),
        },
        "kinds": sorted(
            contradiction_summary.get("kinds", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("kind", ""),
            ),
        ),
        "targets": sorted(
            contradiction_summary.get("targets", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("target_id", ""),
            ),
        ),
    }
    normalized["contradiction_clusters"] = sorted(
        normalized.get("contradiction_clusters", []),
        key=lambda item: (
            item.get("target_id", ""),
            item.get("kind", ""),
            item.get("cluster_id", ""),
        ),
    )
    contradiction_cluster_summary = normalized.get("contradiction_cluster_summary", {})
    cluster_state_counts = contradiction_cluster_summary.get("resolution_state_counts", {}) if isinstance(contradiction_cluster_summary, dict) else {}
    normalized["contradiction_cluster_summary"] = {
        "count": int(contradiction_cluster_summary.get("count", 0) or 0),
        "open_count": int(contradiction_cluster_summary.get("open_count", 0) or 0),
        "resolved_count": int(contradiction_cluster_summary.get("resolved_count", 0) or 0),
        "high_risk_open_count": int(contradiction_cluster_summary.get("high_risk_open_count", 0) or 0),
        "resolution_state_counts": {
            "pending": int(cluster_state_counts.get("pending", 0) or 0),
            "accepted": int(cluster_state_counts.get("accepted", 0) or 0),
            "rejected": int(cluster_state_counts.get("rejected", 0) or 0),
            "deferred": int(cluster_state_counts.get("deferred", 0) or 0),
            "mixed": int(cluster_state_counts.get("mixed", 0) or 0),
        },
    }
    downstream_breakage = normalized.get("downstream_breakage", {})
    normalized["downstream_breakage"] = {
        "count": int(downstream_breakage.get("count", 0) or 0),
        "items": sorted(
            downstream_breakage.get("items", []),
            key=lambda item: (
                item.get("target_id", ""),
                item.get("message", ""),
                item.get("source", ""),
            ),
        ),
        "by_source": sorted(
            downstream_breakage.get("by_source", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("source", ""),
            ),
        ),
        "targets": sorted(
            downstream_breakage.get("targets", []),
            key=lambda item: (
                -int(item.get("count", 0) or 0),
                item.get("target_id", ""),
            ),
        ),
    }
    for key in ("planned_missing", "observed_untracked", "implemented_differently", "uncertain_matches"):
        normalized[key] = sorted(
            normalized.get(key, []),
            key=lambda item: (
                item.get("target_id", ""),
                item.get("field_id", ""),
                item.get("label", ""),
                item.get("message", ""),
            ),
        )
    return normalized


def _merge_legacy_runtime_hints(payload: dict[str, Any], legacy_payload: dict[str, Any]) -> None:
    legacy_nodes = {node.get("id"): node for node in legacy_payload.get("nodes", [])}
    legacy_edges = {edge.get("id"): edge for edge in legacy_payload.get("edges", [])}

    for node in payload.get("nodes", []):
        legacy_node = legacy_nodes.get(node.get("id"))
        if not legacy_node:
            continue

        _merge_compute_feature_selection(node, legacy_node)
        _merge_columns(node, legacy_node)
        _merge_contract_fields(node, legacy_node)

    for edge in payload.get("edges", []):
        legacy_edge = legacy_edges.get(edge.get("id"))
        if not legacy_edge:
            continue
        legacy_mappings = legacy_edge.get("column_mappings", [])
        mappings = edge.get("column_mappings", [])
        for index, mapping in enumerate(mappings):
            if index >= len(legacy_mappings):
                break
            legacy_mapping = legacy_mappings[index]
            if not mapping.get("source_field_id") and legacy_mapping.get("source_field_id"):
                mapping["source_field_id"] = legacy_mapping["source_field_id"]
            if not mapping.get("target_field_id") and legacy_mapping.get("target_field_id"):
                mapping["target_field_id"] = legacy_mapping["target_field_id"]


def _merge_columns(node: dict[str, Any], legacy_node: dict[str, Any]) -> None:
    legacy_columns = {column.get("name"): column for column in legacy_node.get("columns", [])}
    for column in node.get("columns", []):
        legacy_column = legacy_columns.get(column.get("name"))
        if not legacy_column:
            continue
        if not column.get("lineage_inputs") and legacy_column.get("lineage_inputs"):
            column["lineage_inputs"] = legacy_column.get("lineage_inputs", [])
        if not column.get("id") and legacy_column.get("id"):
            column["id"] = legacy_column["id"]


def _merge_compute_feature_selection(node: dict[str, Any], legacy_node: dict[str, Any]) -> None:
    features = node.get("compute", {}).get("feature_selection", [])
    legacy_features = legacy_node.get("compute", {}).get("feature_selection", [])
    for index, feature in enumerate(features):
        if index >= len(legacy_features):
            break
        legacy_feature = legacy_features[index]
        if not feature.get("field_id") and legacy_feature.get("field_id"):
            feature["field_id"] = legacy_feature["field_id"]
        if not feature.get("column_ref") and legacy_feature.get("column_ref"):
            feature["column_ref"] = legacy_feature["column_ref"]


def _merge_contract_fields(node: dict[str, Any], legacy_node: dict[str, Any]) -> None:
    legacy_fields = {field.get("name"): field for field in legacy_node.get("contract", {}).get("fields", [])}
    for field in node.get("contract", {}).get("fields", []):
        legacy_field = legacy_fields.get(field.get("name"))
        if not legacy_field:
            continue
        if not field.get("id") and legacy_field.get("id"):
            field["id"] = legacy_field["id"]
        if not field.get("primary_binding") and legacy_field.get("primary_binding"):
            field["primary_binding"] = legacy_field["primary_binding"]
        if not field.get("alternatives") and legacy_field.get("alternatives"):
            field["alternatives"] = legacy_field.get("alternatives", [])
        if not field.get("sources") and legacy_field.get("sources"):
            field["sources"] = legacy_field.get("sources", [])
