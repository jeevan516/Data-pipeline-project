"""The package modules can be run directly as files, e.g. with VS Code's Run button."""

import subprocess
import sys
from pathlib import Path

import pytest

PACKAGE_DIR = Path(__file__).resolve().parents[1] / "src" / "data_pipeline"


@pytest.mark.parametrize("module", ["cli.py", "transform.py", "download.py"])
def test_module_runs_as_a_file(module: str) -> None:
    result = subprocess.run(
        [sys.executable, str(PACKAGE_DIR / module), "--help"],
        capture_output=True,
        text=True,
        cwd=PACKAGE_DIR,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "Usage:" in result.stdout

