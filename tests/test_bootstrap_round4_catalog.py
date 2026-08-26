from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
HARNESS = REPO / "tests" / "bootstrap_round4_catalog_harness.sh"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_bootstrap_resolves_round4s_catalog_the_way_lifecycle_does():
    """Pin bootstrap.sh's half of the Round 4 catalog contract.

    server/lifecycle.py:_round4_catalog is covered directly in test_lifecycle.py,
    but bootstrap.sh has to reach the same answer *before* provisioning spends
    anything, and it is shell. The harness extracts that block out of bootstrap.sh
    verbatim and runs it against a stub `databricks`, so the two halves cannot
    drift apart silently. It prints one line per case and exits non-zero on any
    failure; the output is attached here so a failure names the case.
    """
    result = subprocess.run(
        ["bash", str(HARNESS)],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "all Round 4 catalog resolution cases pass" in result.stdout
