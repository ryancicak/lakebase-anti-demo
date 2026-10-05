"""Cleanup reads a Round 5 journal setup never made as empty, until Round 5 is sealed.

rc22's install stopped before it made the coordination database (2026-10-05). Its cleanup
asked that database for the journal, waited two minutes for it to come up, and refused, so
the installation could not be uninstalled. Only a Round 5 bout writes the journal, and none
can run before Round 5 is sealed.
"""

from __future__ import annotations

from types import SimpleNamespace

import psycopg
import pytest

from server import lifecycle

_ENDPOINT = "projects/anti-demo-test-coord/branches/production/endpoints/primary"


class _Cursor:
    def __init__(self, server: _Coordination, database: str) -> None:
        self._server = server
        self._database = database
        self._found: list[tuple] = []

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    async def execute(self, query, params=None) -> None:
        text = str(query)
        if "pg_database" in text:
            self._found = [(1,)] if params[0] in self._server.databases else []
            return
        if self._server.journal is None:
            raise psycopg.errors.UndefinedTable('relation "round5_creation_journal" does not exist')
        self._found = list(self._server.journal)

    async def fetchone(self):
        return self._found[0] if self._found else None

    async def fetchall(self):
        return self._found


class _Connection:
    def __init__(self, server: _Coordination, database: str) -> None:
        self._server = server
        self._database = database

    async def __aenter__(self) -> _Connection:
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    def cursor(self) -> _Cursor:
        return _Cursor(self._server, self._database)


class _Coordination:
    """The coordination endpoint: which databases it has, and its journal's rows if any."""

    def __init__(self, *, databases: set[str], journal: list[tuple] | None) -> None:
        self.databases = {"postgres", *databases}
        self.journal = journal
        self.connections: list[str] = []

    async def connect(self, material, *, autocommit: bool = False) -> _Connection:
        self.connections.append(material.database)
        if material.database not in self.databases:
            # What `_connect` raises once its two minutes are up: Lakebase answers a
            # database that does not exist without a SQLSTATE, so it is waited out.
            raise RuntimeError(
                f"PostgreSQL setup connection to {material.host} did not become ready "
                "within 120 seconds"
            )
        return _Connection(self, material.database)


def _manifest(*, round5_ready: bool) -> SimpleNamespace:
    return SimpleNamespace(
        manifest_version=7,
        round5_ready=round5_ready,
        databricks=SimpleNamespace(profile="profile", database="anti_demo", user="installer"),
    )


def _bind(monkeypatch, coordination: _Coordination) -> None:
    def databricks_json(_profile, _group, command, *_args):
        if command == "get-endpoint":
            return {"name": _ENDPOINT, "status": {"hosts": {"host": "coord.example.invalid"}}}
        return {"token": "test-token"}

    monkeypatch.setattr(lifecycle, "_coordination_endpoint_name", lambda manifest: _ENDPOINT)
    monkeypatch.setattr(lifecycle, "_databricks_json", databricks_json)
    monkeypatch.setattr(lifecycle, "_connect", coordination.connect)


def test_a_coordination_database_never_made_has_no_journal(monkeypatch, capsys) -> None:
    coordination = _Coordination(databases=set(), journal=None)
    _bind(monkeypatch, coordination)

    assert lifecycle._round5_active_journal_addons(_manifest(round5_ready=False)) == []
    # Asked of the database every endpoint has, and the missing one is never waited for.
    assert coordination.connections == ["postgres"]
    assert (
        "ABSENT Round 5 journal database anti_demo: setup stopped before making it, and "
        "Round 5 was never sealed, so no Round 5 bout ran here and there is nothing to "
        "reconcile"
    ) in capsys.readouterr().out


def test_an_install_that_stopped_before_its_seals_has_no_journal(monkeypatch) -> None:
    # rc22's own shape: setup stops before sealing anything, so its manifest is still version 1.
    coordination = _Coordination(databases=set(), journal=None)
    _bind(monkeypatch, coordination)
    manifest = _manifest(round5_ready=False)
    manifest.manifest_version = 1

    assert lifecycle._round5_active_journal_addons(manifest) == []
    assert coordination.connections == ["postgres"]


def test_a_journal_table_never_made_is_empty_too(monkeypatch, capsys) -> None:
    coordination = _Coordination(databases={"anti_demo"}, journal=None)
    _bind(monkeypatch, coordination)

    assert lifecycle._round5_active_journal_addons(_manifest(round5_ready=False)) == []
    assert "ABSENT Round 5 journal table" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("databases", "missing"),
    [
        pytest.param(set(), "database anti_demo", id="no-database"),
        pytest.param({"anti_demo"}, "table", id="no-table"),
    ],
)
def test_a_sealed_round5_with_no_journal_is_still_refused(monkeypatch, databases, missing) -> None:
    coordination = _Coordination(databases=databases, journal=None)
    _bind(monkeypatch, coordination)

    with pytest.raises(RuntimeError) as refusal:
        lifecycle._round5_active_journal_addons(_manifest(round5_ready=True))
    assert str(refusal.value) == f"Round 5 journal {missing} is missing from the sealed baseline"


def test_a_journal_that_exists_is_read_as_before(monkeypatch) -> None:
    coordination = _Coordination(databases={"anti_demo"}, journal=[("bout-1", 7, 2, "created")])
    _bind(monkeypatch, coordination)

    assert lifecycle._round5_active_journal_addons(_manifest(round5_ready=True)) == [
        "bout-1:7:2:created"
    ]
    assert coordination.connections == ["postgres", "anti_demo"]
