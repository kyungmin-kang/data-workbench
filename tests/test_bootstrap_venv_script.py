from __future__ import annotations

import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_venv.sh"


def make_fake_python(directory: Path, name: str, version: str) -> Path:
    path = directory / name
    path.write_text(
        textwrap.dedent(
            f"""\
            #!/bin/sh
            if [ "$1" = "-c" ]; then
              echo "{version}"
              exit 0
            fi
            echo "unexpected invocation" >&2
            exit 1
            """
        ),
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


class BootstrapVenvScriptTests(unittest.TestCase):
    def test_dry_run_prefers_supported_python_over_older_python3(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            make_fake_python(temp_path, "python3", "3.8")
            chosen = make_fake_python(temp_path, "python3.11", "3.11")
            env = os.environ.copy()
            env["PATH"] = f"{temp_path}:/usr/bin:/bin"
            result = subprocess.run(
                ["/bin/bash", str(SCRIPT_PATH), "--dry-run"],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(str(chosen), result.stdout)
            self.assertIn("(3.11)", result.stdout)

    def test_dry_run_rejects_older_python_when_no_supported_candidate_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            old_python = make_fake_python(temp_path, "python3", "3.8")
            env = os.environ.copy()
            env["PATH"] = f"{temp_path}:/usr/bin:/bin"
            result = subprocess.run(
                ["/bin/bash", str(SCRIPT_PATH), "--dry-run"],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Python 3.11+ is required", result.stderr)
            self.assertIn(str(old_python), result.stderr)

    def test_explicit_python_flag_rejects_old_interpreter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            old_python = make_fake_python(temp_path, "python3.8", "3.8")
            result = subprocess.run(
                ["/bin/bash", str(SCRIPT_PATH), "--dry-run", "--python", str(old_python)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Python 3.11+ is required", result.stderr)


if __name__ == "__main__":
    unittest.main()
