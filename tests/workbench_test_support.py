from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterable

from workbench.store import ROOT_DIR


ROOT_COPY_DIRS = ("data", "specs", "src", "static", "plugins")
ROOT_COPY_FILES = ("pyproject.toml", "docker-compose.yml", "Dockerfile", ".env.example")


def populate_temp_workbench_root(
    destination: Path,
    *,
    extra_files: Iterable[str] = (),
) -> None:
    for directory in ROOT_COPY_DIRS:
        shutil.copytree(ROOT_DIR / directory, destination / directory)
    agent_playbooks = ROOT_DIR / "docs" / "agent_playbooks"
    if agent_playbooks.exists():
        (destination / "docs").mkdir(parents=True, exist_ok=True)
        shutil.copytree(agent_playbooks, destination / "docs" / "agent_playbooks")
    agent_plugins = ROOT_DIR / ".agents"
    if agent_plugins.exists():
        shutil.copytree(agent_plugins, destination / ".agents")
    for filename in (*ROOT_COPY_FILES, *tuple(extra_files)):
        source = ROOT_DIR / filename
        if source.exists():
            shutil.copy2(source, destination / filename)
    (destination / "runtime" / "plans").mkdir(parents=True, exist_ok=True)
    (destination / "runtime" / "cache").mkdir(parents=True, exist_ok=True)


def make_valid_plan_state(
    *,
    decision_status: str = "accepted",
    task_status: str = "todo",
) -> dict[str, Any]:
    return {
        "revision": 0,
        "decisions": [
            {
                "id": "decision.market_snapshot",
                "title": "Keep market snapshot contract aligned",
                "kind": "contract",
                "status": decision_status,
                "linked_refs": ["contract:api.market_snapshot"],
                "acceptance_check_ids": ["check.market_snapshot.delivery"],
                "summary": "Maintain the canonical market snapshot contract and its delivery path.",
            }
        ],
        "tasks": [
            {
                "id": "task.market_snapshot.delivery",
                "title": "Implement market snapshot delivery",
                "role": "builder",
                "status": task_status,
                "decision_ids": ["decision.market_snapshot"],
                "linked_refs": ["contract:api.market_snapshot"],
                "acceptance_check_ids": ["check.market_snapshot.delivery"],
                "summary": "One focused task for the contract delivery work.",
            }
        ],
        "blockers": [],
        "acceptance_checks": [
            {
                "id": "check.market_snapshot.delivery",
                "label": "Snapshot contract fields resolve with valid bindings",
                "kind": "binding",
                "linked_refs": ["contract:api.market_snapshot"],
                "required": True,
            }
        ],
        "evidence": [],
        "attachments": [],
        "agent_runs": [],
        "agreement_log": [],
    }


class WorkbenchTempRootMixin:
    temp_dir: tempfile.TemporaryDirectory[str]
    root: Path
    previous_root: str | None

    def setUpWorkbenchRoot(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        populate_temp_workbench_root(self.root)
        self.previous_root = os.environ.get("WORKBENCH_ROOT_DIR")
        os.environ["WORKBENCH_ROOT_DIR"] = str(self.root)

    def tearDownWorkbenchRoot(self) -> None:
        if self.previous_root is None:
            os.environ.pop("WORKBENCH_ROOT_DIR", None)
        else:
            os.environ["WORKBENCH_ROOT_DIR"] = self.previous_root
        self.temp_dir.cleanup()
