from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from .importer import AssetImportSpec
from .onboarding_presets import OnboardingPreset
from .openapi_importer import OpenAPIImportSpec


class GraphSaveRequest(BaseModel):
    graph: dict[str, Any]
    actor_type: Literal["human", "agent"] = "human"


class PlanStateSaveRequest(BaseModel):
    plan_state: dict[str, Any]
    expected_revision: int
    updated_by: str = "user"


class PlanStateDeriveTasksRequest(BaseModel):
    expected_revision: int
    updated_by: str = "user"


class AgentRunCreateRequest(BaseModel):
    agent_run: dict[str, Any]
    expected_revision: int
    updated_by: str = "user"


class AgentRunPatchRequest(BaseModel):
    updates: dict[str, Any]
    expected_revision: int
    updated_by: str = "user"


class AgentRunEventRequest(BaseModel):
    event: dict[str, Any]
    expected_revision: int
    updated_by: str = "user"


class AgentWorkflowLaunchRequest(BaseModel):
    expected_revision: int
    updated_by: str = "user"
    task_id: str = ""
    run_id: str = ""


class AssetImportRequest(BaseModel):
    graph: dict[str, Any]
    import_spec: AssetImportSpec
    root_path: str | None = None
    actor_type: Literal["human", "agent"] = "human"


class OpenAPIImportRequest(BaseModel):
    graph: dict[str, Any]
    import_spec: OpenAPIImportSpec
    root_path: str | None = None
    actor_type: Literal["human", "agent"] = "human"


class BulkAssetImportRequest(BaseModel):
    graph: dict[str, Any]
    import_specs: list[AssetImportSpec]
    root_path: str | None = None
    actor_type: Literal["human", "agent"] = "human"


class GraphValidateRequest(BaseModel):
    graph: dict[str, Any]


class ContractBindingSuggestionRequest(BaseModel):
    graph: dict[str, Any]
    node_id: str
    field_name: str
    limit: int = 8


class ProjectHintImportRequest(BaseModel):
    graph: dict[str, Any]
    hint_kind: str
    hint_id: str
    profile_token: str | None = None
    root_path: str | None = None
    include_tests: bool = False
    include_internal: bool = True
    actor_type: Literal["human", "agent"] = "human"


class ProjectBootstrapRequest(BaseModel):
    graph: dict[str, Any]
    include_tests: bool = False
    include_internal: bool = True
    profile_token: str | None = None
    root_path: str | None = None
    asset_paths: list[str] = []
    api_hint_ids: list[str] = []
    ui_hint_ids: list[str] = []
    import_assets: bool = True
    import_api_hints: bool = True
    import_ui_hints: bool = True
    actor_type: Literal["human", "agent"] = "human"


class OnboardingPresetSaveRequest(BaseModel):
    preset: OnboardingPreset


class ProjectProfileJobRequest(BaseModel):
    include_tests: bool = False
    include_internal: bool = True
    root_path: str | None = None
    force_refresh: bool = False
    profile_token: str | None = None


class StructureImportRequest(BaseModel):
    spec: dict[str, Any] | None = None
    yaml_text: str | None = None
    updated_by: str = "user"
    actor_type: Literal["human", "agent"] = "human"


class StructureScanRequest(BaseModel):
    role: str = "scout"
    scope: str = "full"
    include_tests: bool = False
    include_internal: bool = True
    profile_token: str | None = None
    force_refresh: bool = False
    root_path: str | None = None
    doc_paths: list[str] = []
    selected_paths: list[str] = []


class StructurePatchReviewRequest(BaseModel):
    patch_id: str
    decision: str
    reviewed_by: str = "user"
    note: str = ""


class StructureBatchReviewRequest(BaseModel):
    patch_ids: list[str] = []
    decision: str
    reviewed_by: str = "user"
    note: str = ""


class StructureContradictionReviewRequest(BaseModel):
    contradiction_id: str
    decision: str
    reviewed_by: str = "user"
    note: str = ""


class StructureBundleWorkflowRequest(BaseModel):
    bundle_owner: str | None = None
    assigned_reviewer: str | None = None
    triage_state: str | None = None
    triage_note: str | None = None
    updated_by: str = "user"
    note: str = ""


class StructureBundleMergeRequest(BaseModel):
    merged_by: str = "user"
    actor_type: Literal["human", "agent"] = "human"


class StructureBundleRebaseRequest(BaseModel):
    preserve_reviews: bool = True
    rebased_by: str = "user"
