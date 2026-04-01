from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from . import __version__
from .api_authoring_routes import router as authoring_router
from .api_core_routes import STATIC_DIR, router as core_router
from .api_execution_routes import router as execution_router
from .api_project_routes import router as project_router
from .api_structure_routes import router as structure_router


app = FastAPI(title="Data Workbench", version=__version__)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.include_router(core_router)
app.include_router(execution_router)
app.include_router(structure_router)
app.include_router(project_router)
app.include_router(authoring_router)


def main() -> None:
    import os
    import uvicorn

    host = os.environ.get("WORKBENCH_HOST", "0.0.0.0")
    port = int(os.environ.get("WORKBENCH_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
