from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from .store import get_onboarding_presets_path, utc_timestamp
from .types import slugify


class OnboardingBootstrapOptions(BaseModel):
    assets: bool = True
    apiHints: bool = True
    uiHints: bool = True


class OnboardingPreset(BaseModel):
    id: str = ""
    name: str
    description: str = ""
    root: str = ""
    include_tests: bool = False
    include_internal: bool = True
    bootstrap_options: OnboardingBootstrapOptions = Field(default_factory=OnboardingBootstrapOptions)
    selected_asset_paths: list[str] = Field(default_factory=list)
    selected_api_hint_ids: list[str] = Field(default_factory=list)
    selected_ui_hint_ids: list[str] = Field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


def load_onboarding_presets() -> list[dict[str, Any]]:
    path = get_onboarding_presets_path()
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as file:
        data = json.load(file)
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
    path = get_onboarding_presets_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(
        [OnboardingPreset.model_validate(item).model_dump(mode="json") for item in presets],
        key=lambda item: item["name"].lower(),
    )
    with path.open("w", encoding="utf-8") as file:
        json.dump(ordered, file, indent=2, ensure_ascii=True)
        file.write("\n")


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
