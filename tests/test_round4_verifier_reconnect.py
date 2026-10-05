"""Round 4's verifier reads through a lost connection on a new one.

2026-10-02, rc9's release bar: a Round 4 Prepare against RDS failed with AdminShutdown
("terminating connection due to administrator command") six seconds after its head-start
connection to Lakebase opened. Round 4's Lakebase compute suspends after a minute with no
query, an open but idle connection doesn't keep it awake, and Prepare checks the Delta source
and the parked lanes before it reads.
"""

from __future__ import annotations

import inspect

import psycopg
import pytest
from psycopg import sql

from server.model_score import ModelScoreRow
from server.round4_race import Round4RaceError
from server.round4_race_live import GlueWriterLane, LakebaseSyncLane, PostgresReader

ROW = ("entity-1", 0.75, "model-v1", "nonce-1")
BASELINE = ModelScoreRow(
    entity_id="entity-1", score=0.75, model_version="model-v1", proof_nonce="nonce-1"
)
SHUT_DOWN = psycopg.errors.AdminShutdown("terminating connection due to administrator command")


class _Cursor:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, query, params) -> None:
        self._connection.executed.append(params)
        if self._connection.fail is not None:
            raise self._connection.fail

    async def fetchall(self) -> list[tuple]:
        return list(self._connection.rows)


class _Connection:
    def __init__(self, *, rows=(ROW,), fail: Exception | None = None) -> None:
        self.rows = rows
        self.fail = fail
        self.executed: list[tuple] = []
        self.closed = False

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    async def close(self) -> None:
        self.closed = True


class _Reconnect:
    def __init__(self, *results) -> None:
        self.results = list(results)
        self.calls = 0

    async def __call__(self) -> _Connection:
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _reader(connection: _Connection, reconnect=None) -> PostgresReader:
    return PostgresReader(connection, sql.Identifier("scores", "model_scores"), reconnect=reconnect)


@pytest.mark.parametrize(
    "lost",
    [
        SHUT_DOWN,
        psycopg.OperationalError("the connection is closed"),
        psycopg.InterfaceError("the connection is lost"),
    ],
    ids=["admin-shutdown", "closed", "interface"],
)
async def test_a_read_whose_connection_was_lost_reads_again_on_a_new_one(lost) -> None:
    first = _Connection(fail=lost)
    second = _Connection()
    reconnect = _Reconnect(second)

    row = await _reader(first, reconnect).read("entity-1")

    assert row == BASELINE
    assert first.closed
    assert reconnect.calls == 1
    assert second.executed == [("entity-1",)]


async def test_without_a_way_to_reconnect_the_lost_connection_is_raised() -> None:
    with pytest.raises(psycopg.errors.AdminShutdown):
        await _reader(_Connection(fail=SHUT_DOWN)).read("entity-1")


async def test_a_query_error_is_not_a_lost_connection() -> None:
    reconnect = _Reconnect(_Connection())

    with pytest.raises(psycopg.errors.UndefinedTable):
        await _reader(
            _Connection(fail=psycopg.errors.UndefinedTable("relation does not exist")), reconnect
        ).read("entity-1")

    assert reconnect.calls == 0


async def test_one_new_connection_per_read_and_a_second_loss_is_raised() -> None:
    reconnect = _Reconnect(_Connection(fail=SHUT_DOWN))

    with pytest.raises(psycopg.errors.AdminShutdown):
        await _reader(_Connection(fail=SHUT_DOWN), reconnect).read("entity-1")

    assert reconnect.calls == 1


async def test_a_reconnect_that_fails_is_raised_and_the_next_read_tries_again() -> None:
    """The race polls on: one read's failed reconnect leaves the next read to reconnect."""

    reconnect = _Reconnect(psycopg.OperationalError("connection refused"), _Connection())
    reader = _reader(_Connection(fail=SHUT_DOWN), reconnect)

    with pytest.raises(psycopg.OperationalError):
        await reader.read("entity-1")
    assert await reader.read("entity-1") == BASELINE
    assert reconnect.calls == 2


async def test_two_rows_for_one_key_is_still_refused_after_a_reconnect() -> None:
    reconnect = _Reconnect(_Connection(rows=(ROW, ROW)))

    with pytest.raises(Round4RaceError):
        await _reader(_Connection(fail=SHUT_DOWN), reconnect).read("entity-1")


@pytest.mark.parametrize("lane", [LakebaseSyncLane, GlueWriterLane])
def test_both_lanes_give_their_verifier_a_way_to_reconnect(lane) -> None:
    source = inspect.getsource(lane.open_reader)
    assert "reconnect=self._connect_verifier" in source
    assert "await self._connect_verifier()" in source
