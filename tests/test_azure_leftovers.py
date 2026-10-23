"""scripts/release_bar/azure_leftovers.py: Round 5's PgBouncer left on is a leftover, as a copy
server left standing is."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import Any

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "release_bar" / "azure_leftovers.py"
spec = importlib.util.spec_from_file_location("azure_leftovers", SCRIPT)
assert spec and spec.loader
azure_leftovers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(azure_leftovers)


class Arm:
    def __init__(self, pgbouncer: str) -> None:
        self.pgbouncer = pgbouncer

    def get_server(self, name: str) -> Any:
        return None

    def get_configuration(self, server: str, name: str) -> str:
        assert (server, name) == ("psql-test-r5", "pgbouncer.enabled")
        return self.pgbouncer


def manifest(*, round5: bool) -> Any:
    return SimpleNamespace(
        run_id="ad-test", azure=SimpleNamespace(round4=None, round6=None),
        round5_azure=SimpleNamespace(server_name="psql-test-r5") if round5 else None)


def test_a_pgbouncer_left_on_is_reported_and_one_switched_off_is_not() -> None:
    assert azure_leftovers.leftovers(Arm("true"), manifest(round5=True)) == [
        "PgBouncer still on at psql-test-r5"]
    assert azure_leftovers.leftovers(Arm("false"), manifest(round5=True)) == []
    # No Round 5 sealed: nothing of its to look at.
    assert azure_leftovers.leftovers(Arm("true"), manifest(round5=False)) == []
