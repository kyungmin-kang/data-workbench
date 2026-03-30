from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from .profile import profile_graph
from .store import get_cache_dir, get_root_dir, load_graph, save_graph
from .types import validate_graph


def write_heartbeat(payload: dict) -> None:
    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    heartbeat_path = cache_dir / "worker-heartbeat.json"
    with heartbeat_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=True)
        file.write("\n")


def main() -> None:
    import os

    interval_seconds = int(os.environ.get("WORKBENCH_WORKER_INTERVAL", "30"))
    auto_profile = os.environ.get("WORKBENCH_AUTO_PROFILE", "0") == "1"

    while True:
        heartbeat = {
            "status": "ok",
            "auto_profile": auto_profile,
            "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        if auto_profile:
            graph = load_graph()
            refreshed = validate_graph(profile_graph(graph, get_root_dir()))
            save_graph(refreshed)
            heartbeat["profiled"] = True
        write_heartbeat(heartbeat)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
