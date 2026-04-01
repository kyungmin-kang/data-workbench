from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class GraphValidationError(ValueError):
    """Raised when a graph spec is invalid."""


class PlanStateValidationError(ValueError):
    """Raised when an execution plan payload is invalid."""


class WorkbenchModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class StrictWorkbenchModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


__all__ = [
    "GraphValidationError",
    "PlanStateValidationError",
    "StrictWorkbenchModel",
    "WorkbenchModel",
]
