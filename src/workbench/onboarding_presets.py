from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .store import load_onboarding_presets_payload, save_onboarding_presets_payload, utc_timestamp
from .types import slugify


class OnboardingBootstrapOptions(BaseModel):
    assets: bool = True
    apiHints: bool = True
    uiHints: bool = True
    sqlHints: bool = True
    ormHints: bool = True


class OnboardingPreset(BaseModel):
    id: str = ""
    name: str
    description: str = ""
    root: str = ""
    include_tests: bool = False
    include_internal: bool = True
    exclude_paths: list[str] = Field(default_factory=list)
    asset_roots: list[str] = Field(default_factory=list)
    profiling_mode: str = "metadata_only"
    agent_enrich_after_scan: bool = False
    bootstrap_options: OnboardingBootstrapOptions = Field(default_factory=OnboardingBootstrapOptions)
    selected_asset_paths: list[str] = Field(default_factory=list)
    selected_api_hint_ids: list[str] = Field(default_factory=list)
    selected_ui_hint_ids: list[str] = Field(default_factory=list)
    selected_sql_hint_ids: list[str] = Field(default_factory=list)
    selected_orm_hint_ids: list[str] = Field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


def load_onboarding_presets() -> list[dict[str, Any]]:
    data = load_onboarding_presets_payload()
    if not isinstance(data, list):
        return []
    presets = [OnboardingPreset.model_validate(item).model_dump(mode="json") for item in data]
    return sorted(presets, key=lambda item: item["name"].lower())


def save_onboarding_preset(preset: OnboardingPreset | dict[str, Any]) -> dict[str, Any]:
    parsed = preset if isinstance(preset, OnboardingPreset) else OnboardingPreset.model_validate(preset)
    existing = load_onboarding_presets()
    now = utc_timestamp()

    if not parsed.id:
        parsed.id = build_unique_preset_id(existing, parsed.name)
        parsed.created_at = now
    else:
        existing_match = next((item for item in existing if item["id"] == parsed.id), None)
        parsed.created_at = parsed.created_at or (existing_match or {}).get("created_at", now)
    parsed.updated_at = now

    updated = [item for item in existing if item["id"] != parsed.id]
    updated.append(parsed.model_dump(mode="json"))
    write_onboarding_presets(updated)
    return parsed.model_dump(mode="json")


def delete_onboarding_preset(preset_id: str) -> bool:
    existing = load_onboarding_presets()
    updated = [item for item in existing if item["id"] != preset_id]
    if len(updated) == len(existing):
        return False
    write_onboarding_presets(updated)
    return True


def write_onboarding_presets(presets: list[dict[str, Any]]) -> None:
    ordered = sorted(
        [OnboardingPreset.model_validate(item).model_dump(mode="json") for item in presets],
        key=lambda item: item["name"].lower(),
    )
    save_onboarding_presets_payload(ordered)


def build_unique_preset_id(existing: list[dict[str, Any]], name: str) -> str:
    base = slugify(name) or "preset"
    preset_id = f"preset:{base}"
    existing_ids = {item["id"] for item in existing}
    if preset_id not in existing_ids:
        return preset_id
    counter = 2
    while f"{preset_id}-{counter}" in existing_ids:
        counter += 1
    return f"{preset_id}-{counter}"
