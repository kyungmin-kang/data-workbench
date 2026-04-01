from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, model_validator

from .types_base import GraphValidationError, PlanStateValidationError, StrictWorkbenchModel, WorkbenchModel
from .types_execution import (
    AcceptanceCheck,
    AgentRun,
    AgentRunEvent,
    AgreementLogEntry,
    AttachmentRef,
    Blocker,
    DecisionUnit,
    EvidenceProof,
    ExecutionTask,
    PlanState,
    normalize_plan_state,
    validate_plan_state,
)
from .types_graph import (
    active_nodes,
    binding_ref_to_legacy_source,
    build_index,
    column_ref,
    deep_sort,
    display_ref_for_field_id,
    field_name_for_node,
    find_column,
    find_field_by_id,
    identifierify,
    make_field_id,
    node_short_name,
    normalize_graph,
    resolve_field_reference,
    slugify,
    suggest_binding_reference_for_field,
    validate_graph,
)


BASE_NODE_TYPES = {
    "source": {
        "provider",
        "group",
        "object",
        "url",
        "disk_path",
        "api_endpoint",
        "bucket_path",
    },
    "data": {
        "raw_dataset",
        "table",
        "view",
        "materialized_view",
        "feature_set",
    },
    "compute": {"transform", "model"},
    "contract": {"api", "ui"},
}

EDGE_TYPES = {
    "contains",
    "ingests",
    "produces",
    "derives",
    "serves",
    "binds",
    "depends_on",
}

PROFILE_STATUSES = {"schema_only", "profiled", "sampled_profile", "unknown"}
STRUCTURE_STATES = {"confirmed", "observed", "proposed"}
VERIFICATION_STATES = {"observed", "confirmed", "contradicted"}
CONFIDENCE_LEVELS = {"high", "medium", "low"}
PATCH_TYPES = {
    "add_node",
    "remove_node",
    "add_field",
    "remove_field",
    "add_edge",
    "remove_edge",
    "add_binding",
    "remove_binding",
    "change_binding",
    "confidence_change",
    "contradiction",
}

COMMON_NODE_DEFAULTS = {
    "description": "",
    "tags": [],
    "owner": "",
    "sensitivity": "internal",
    "status": "active",
    "profile_status": "unknown",
    "notes": "",
}

PATCH_TYPE_PRECEDENCE = {
    "add_node": 10,
    "add_edge": 20,
    "add_field": 30,
    "add_binding": 40,
    "change_binding": 50,
    "confidence_change": 60,
    "contradiction": 70,
    "remove_field": 80,
    "remove_edge": 90,
    "remove_node": 100,
}

OBSERVED_PRECEDENCE = {
    "confirmed": 1,
    "user_defined": 2,
    "plan_yaml": 2,
    "recorder": 3,
    "scout": 4,
    "heuristic": 5,
}

CONSUMER_CONTRACT_TYPES = {"api", "ui"}
TRACELESS_FIELD_KINDS = {"source"}


class Position(WorkbenchModel):
    x: float = 0
    y: float = 0


class HistoryEntry(WorkbenchModel):
    change: str = ""
    from_value: Any = None
    to_value: Any = None
    at: str = ""
    by: str = ""
    note: str = ""
    derived_from: list[str] = Field(default_factory=list)
    replaces: list[str] = Field(default_factory=list)


class LineageInput(WorkbenchModel):
    field_id: str = ""
    role: str = ""


class SourceOrigin(WorkbenchModel):
    kind: str = ""
    value: str = ""


class DataDictionaryAttachment(WorkbenchModel):
    label: str = ""
    kind: Literal["link", "file"] = "link"
    value: str = ""


class RawAssetAttachment(WorkbenchModel):
    label: str = ""
    kind: Literal["file", "object_storage", "glob", "directory"] = "file"
    format: Literal["csv", "csv_gz", "parquet", "parquet_collection", "zip_csv", "unknown"] = "unknown"
    value: str = ""
    profile_ready: bool = True


class BaseFieldSpec(WorkbenchModel):
    id: str = ""
    name: str
    description: str = ""
    required: bool | None = None
    state: Literal["confirmed", "observed", "proposed"] = "confirmed"
    verification_state: Literal["observed", "confirmed", "contradicted"] = "confirmed"
    confidence: Literal["high", "medium", "low"] = "high"
    evidence: list[str] = Field(default_factory=list)
    last_verified_by: str = ""
    last_verified_at: str = ""
    history: list[HistoryEntry] = Field(default_factory=list)
    removed: bool = False
    removed_at: str = ""
    removed_by: str = ""
    derived_from: list[str] = Field(default_factory=list)
    replaces: list[str] = Field(default_factory=list)


class ColumnSpec(BaseFieldSpec):
    data_type: str = "unknown"
    nullable: bool = True
    null_pct: float | None = None
    stats: dict[str, Any] = Field(default_factory=dict)
    notes: str = ""
    lineage_inputs: list[LineageInput] = Field(default_factory=list)


class SourceConfig(WorkbenchModel):
    provider: str = ""
    origin: SourceOrigin = Field(default_factory=SourceOrigin)
    refresh: str = ""
    shared_config: dict[str, Any] = Field(default_factory=dict)
    series_id: str = ""
    data_dictionaries: list[DataDictionaryAttachment] = Field(default_factory=list)
    raw_assets: list[RawAssetAttachment] = Field(default_factory=list)


class DataConfig(WorkbenchModel):
    persistence: str = ""
    local_path: str = ""
    update_frequency: str = ""
    persisted: bool = False
    row_count: int | None = None
    sampled: bool | None = None
    profile_target: str = ""


class ComputeFieldMapping(WorkbenchModel):
    source_field_id: str = ""
    target_field_id: str = ""
    source: str = ""
    target: str = ""


class FeatureSelection(WorkbenchModel):
    field_id: str = ""
    column_ref: str = ""
    status: Literal["selected", "candidate", "rejected", "deferred"] = "candidate"
    persisted: bool = False
    stage: str = ""
    category: str = ""
    labels: list[str] = Field(default_factory=list)
    order: int | None = None
    confidence: Literal["high", "medium", "low"] = "medium"
    evidence: list[str] = Field(default_factory=list)


class ComputeConfig(WorkbenchModel):
    runtime: str = ""
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    notes: str = ""
    feature_selection: list[FeatureSelection] = Field(default_factory=list)
    column_mappings: list[ComputeFieldMapping] = Field(default_factory=list)


class ContractFieldSource(WorkbenchModel):
    node_id: str = ""
    column: str | None = None
    field: str | None = None


class ContractField(BaseFieldSpec):
    primary_binding: str = ""
    alternatives: list[str] = Field(default_factory=list)
    sources: list[ContractFieldSource] = Field(default_factory=list)
    binding_locked: bool = False


class ContractConfig(WorkbenchModel):
    route: str = ""
    component: str = ""
    ui_role: str = ""
    fields: list[ContractField] = Field(default_factory=list)


def _bridge_title_from_text(text: str, fallback: str = "Work item") -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return fallback
    if len(cleaned) <= 72:
        return cleaned
    return f"{cleaned[:69].rstrip()}..."


def _bridge_role_from_kind(kind: str) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized == "bug":
        return "qa"
    if normalized == "story":
        return "architect"
    if normalized == "note":
        return "architect"
    return "builder"


class GraphWorkItemBridge(WorkbenchModel):
    id: str = ""
    title: str = ""
    role: str = "builder"
    status: Literal["todo", "in_progress", "blocked", "done", "cancelled"] = "todo"
    linked_refs: list[str] = Field(default_factory=list)
    decision_ids: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    acceptance_check_ids: list[str] = Field(default_factory=list)
    blocker_ids: list[str] = Field(default_factory=list)
    summary: str = ""
    exploratory: bool = False
    exploration_goal: str = ""
    legacy_kind: str = ""

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_fields(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        legacy_kind = str(normalized.pop("kind", "") or normalized.get("legacy_kind", "")).strip().lower()
        legacy_text = str(normalized.pop("text", "") or normalized.get("summary", "")).strip()
        if legacy_kind and not normalized.get("legacy_kind"):
            normalized["legacy_kind"] = legacy_kind
        if legacy_text:
            normalized.setdefault("summary", legacy_text)
            normalized.setdefault(
                "title",
                _bridge_title_from_text(legacy_text, fallback=legacy_kind.title() if legacy_kind else "Work item"),
            )
        normalized.setdefault("role", _bridge_role_from_kind(legacy_kind))
        normalized.setdefault("status", "todo")
        return normalized


class NodeSpec(WorkbenchModel):
    id: str
    kind: Literal["source", "data", "compute", "contract"]
    extension_type: str
    label: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    owner: str = ""
    sensitivity: str = "internal"
    status: str = "active"
    work_status: str = "todo"
    profile_status: Literal["schema_only", "profiled", "sampled_profile", "unknown"] = "unknown"
    notes: str = ""
    position: Position = Field(default_factory=Position)
    columns: list[ColumnSpec] = Field(default_factory=list)
    source: SourceConfig = Field(default_factory=SourceConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    contract: ContractConfig = Field(default_factory=ContractConfig)
    work_items: list[GraphWorkItemBridge] = Field(default_factory=list)
    state: Literal["confirmed", "observed", "proposed"] = "confirmed"
    verification_state: Literal["observed", "confirmed", "contradicted"] = "confirmed"
    confidence: Literal["high", "medium", "low"] = "high"
    evidence: list[str] = Field(default_factory=list)
    last_verified_by: str = ""
    last_verified_at: str = ""
    history: list[HistoryEntry] = Field(default_factory=list)
    removed: bool = False
    removed_at: str = ""
    removed_by: str = ""

    @model_validator(mode="after")
    def validate_extension_type(self) -> "NodeSpec":
        if self.extension_type not in BASE_NODE_TYPES[self.kind]:
            raise ValueError(
                f"Invalid extension_type for {self.id}: {self.extension_type} (kind={self.kind})"
            )
        return self


class EdgeColumnMapping(WorkbenchModel):
    source_column: str = ""
    target_column: str = ""
    source_field_id: str = ""
    target_field_id: str = ""


class EdgeSpec(WorkbenchModel):
    id: str
    type: Literal["contains", "ingests", "produces", "derives", "serves", "binds", "depends_on"]
    source: str
    target: str
    label: str = ""
    column_mappings: list[EdgeColumnMapping] = Field(default_factory=list)
    notes: str = ""
    state: Literal["confirmed", "observed", "proposed"] = "confirmed"
    confidence: Literal["high", "medium", "low"] = "high"
    evidence: list[str] = Field(default_factory=list)
    removed: bool = False
    history: list[HistoryEntry] = Field(default_factory=list)


class GraphMetadata(WorkbenchModel):
    name: str = "Data Workbench Graph"
    description: str = ""
    updated_at: str = ""
    updated_by: str = "user"
    structure_version: int = 1


class GraphSpec(WorkbenchModel):
    version: str = "2.0"
    metadata: GraphMetadata = Field(default_factory=GraphMetadata)
    nodes: list[NodeSpec] = Field(default_factory=list)
    edges: list[EdgeSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_relationships(self) -> "GraphSpec":
        node_ids = [node.id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            duplicates = {node_id for node_id in node_ids if node_ids.count(node_id) > 1}
            raise ValueError(f"Duplicate node ids: {sorted(duplicates)}")

        edge_ids = [edge.id for edge in self.edges]
        if len(edge_ids) != len(set(edge_ids)):
            duplicates = {edge_id for edge_id in edge_ids if edge_ids.count(edge_id) > 1}
            raise ValueError(f"Duplicate edge ids: {sorted(duplicates)}")

        node_id_set = set(node_ids)
        for edge in self.edges:
            if edge.source not in node_id_set:
                raise ValueError(f"Edge {edge.id} source not found: {edge.source}")
            if edge.target not in node_id_set:
                raise ValueError(f"Edge {edge.id} target not found: {edge.target}")
        return self


class StructuralPatch(WorkbenchModel):
    id: str
    type: str
    target_id: str = ""
    node_id: str = ""
    field_id: str = ""
    edge_id: str = ""
    confidence: Literal["high", "medium", "low"] = "medium"
    evidence: list[str] = Field(default_factory=list)
    review_state: Literal["pending", "accepted", "rejected", "deferred"] = "pending"
    reviewed_at: str = ""
    reviewed_by: str = ""
    review_note: str = ""
    review_history: list[dict[str, Any]] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_patch_type(self) -> "StructuralPatch":
        if self.type not in PATCH_TYPES:
            raise ValueError(f"Unsupported patch type: {self.type}")
        return self


class ContradictionRecord(WorkbenchModel):
    id: str
    target_id: str = ""
    node_id: str = ""
    field_id: str = ""
    kind: str = ""
    severity: str = ""
    existing_belief: dict[str, Any] = Field(default_factory=dict)
    new_evidence: dict[str, Any] = Field(default_factory=dict)
    affected_refs: list[str] = Field(default_factory=list)
    confidence_delta: str = ""
    downstream_impacts: list[str] = Field(default_factory=list)
    evidence_sources: list[str] = Field(default_factory=list)
    message: str = ""
    why_this_matters: str = ""
    review_required: bool = True
    review_state: Literal["pending", "accepted", "rejected", "deferred"] = "pending"
    reviewed_at: str = ""
    reviewed_by: str = ""
    review_note: str = ""
    review_history: list[dict[str, Any]] = Field(default_factory=list)


class ScanMetadata(WorkbenchModel):
    bundle_id: str = ""
    role: Literal["scout", "recorder"] = "scout"
    scope: str = "full"
    root_path: str = ""
    doc_paths: list[str] = Field(default_factory=list)
    selected_paths: list[str] = Field(default_factory=list)
    include_tests: bool = False
    include_internal: bool = True
    fingerprint: str = ""
    scanner_versions: dict[str, str] = Field(default_factory=dict)
    base_structure_version: int = 1
    created_at: str = ""


class ReviewSummary(WorkbenchModel):
    accepted_patch_ids: list[str] = Field(default_factory=list)
    rejected_patch_ids: list[str] = Field(default_factory=list)
    deferred_patch_ids: list[str] = Field(default_factory=list)
    bundle_owner: str = ""
    assigned_reviewer: str = ""
    triage_state: Literal["new", "in_review", "blocked", "resolved"] = "new"
    triage_note: str = ""
    workflow_history: list[dict[str, Any]] = Field(default_factory=list)
    last_reviewed_at: str = ""
    last_reviewed_by: str = ""
    last_review_note: str = ""
    merged_at: str = ""
    merged_by: str = ""
    merge_status: str = ""
    rebase_required: bool = False
    merge_patch_ids: list[str] = Field(default_factory=list)
    merge_blockers: list[dict[str, Any]] = Field(default_factory=list)
    merge_plan: dict[str, Any] = Field(default_factory=dict)
    rebased_from_bundle_id: str = ""
    superseded_by_bundle_id: str = ""
    superseded_at: str = ""
    superseded_by: str = ""
    last_rebase_summary: dict[str, Any] = Field(default_factory=dict)


class ReadinessIssue(WorkbenchModel):
    level: Literal["tier_1", "tier_2", "info"] = "tier_2"
    target_id: str = ""
    message: str = ""
    why_this_matters: str = ""


class ReadinessReport(WorkbenchModel):
    status: Literal["Not Ready", "Partially Ready", "Ready to Build"] = "Not Ready"
    summary: dict[str, Any] = Field(default_factory=dict)
    issues: list[ReadinessIssue] = Field(default_factory=list)


class ScanBundle(WorkbenchModel):
    bundle_id: str
    base_structure_version: int = 1
    scan: ScanMetadata = Field(default_factory=ScanMetadata)
    observed: dict[str, Any] = Field(default_factory=dict)
    patches: list[StructuralPatch] = Field(default_factory=list)
    contradictions: list[ContradictionRecord] = Field(default_factory=list)
    impacts: list[dict[str, Any]] = Field(default_factory=list)
    reconciliation: dict[str, Any] = Field(default_factory=dict)
    review: ReviewSummary = Field(default_factory=ReviewSummary)
    readiness: ReadinessReport = Field(default_factory=ReadinessReport)

