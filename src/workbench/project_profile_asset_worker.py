from __future__ import annotations

import json
import sys
from pathlib import Path

from .project_profiler import summarize_asset_entry


def main() -> int:
    raw = sys.stdin.read()
    payload = json.loads(raw or "{}")
    root_path = str(payload.get("root_path") or ".")
    entry = payload.get("entry") or {}
    result = summarize_asset_entry(Path(root_path), entry, profiling_mode="profile_assets")
    sys.stdout.write(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
