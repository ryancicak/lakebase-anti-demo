"""Real ephemeral-PostgreSQL race of a stale ring release against a phase transition.

A transition keeps the lease id and fence token, so only ``release``'s phase
predicate tells a pre-towel copy from the towel's row. These tests hold one
statement's transaction open on the ring row, start the other on a second
connection so it queues on the row lock, and only then let the first commit.
Under READ COMMITTED the queued UPDATE re-checks its WHERE clause against the
committed row, so exactly one of the two wins and the loser changes nothing.

Every statement is the production SQL of ``LakebaseBoutLeaseStore``; the only
test seam is a connector that defers one connection's commit.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest

from server.coordination import LakebaseBoutLeaseStore, LeaseLostError
from server.models import BoutOperator, SessionState
from tests.test_round5_accept_bell_with_leases_transaction_pg import (
    _fake_workspace_client,
    _free_port,
    _local_pg_connector,
)

pytestmark = pytest.mark.skipif(
    shutil.which("initdb") is None or shutil.which("pg_ctl") is None,
    reason="requires a local PostgreSQL (initdb/pg_ctl on PATH)",
)

RING_KEY = "main"
OPERATOR = BoutOperator(display_name="Operator", email="operator@example.com", subject="op")
TTL = timedelta(seconds=300)


@pytest.fixture(scope="module")
def pg_dsn():
    tmp = Path(tempfile.mkdtemp(prefix="ringphasepg-"))
    data = tmp / "data"
    subprocess.run(
        ["initdb", "-D", str(data), "-U", "postgres", "--auth=trust", "--no-sync"],
        check=True,
        capture_output=True,
    )
    port = _free_port()
    subprocess.run(
        [
            "pg_ctl",
            "-D",
            str(data),
            "-l",
            str(tmp / "log"),
            "-o",
            f"-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories={tmp}",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    dsn = {"host": "127.0.0.1", "port": port, "dbname": "postgres", "user": "postgres"}
    deadline = time.monotonic() + 20
    while True:
        try:
            with psycopg.connect(**dsn, connect_timeout=3) as conn:
                conn.execute("SELECT 1")
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)
    try:
        yield dsn
    finally:
        subprocess.run(
            ["pg_ctl", "-D", str(data), "-m", "immediate", "stop"], capture_output=True
        )
        shutil.rmtree(tmp, ignore_errors=True)


class _DeferredCommit:
    """One connection whose commit waits until ``proceed``, after its statement ran."""

    def __init__(self, connection: Any, executed: asyncio.Event, proceed: asyncio.Event):
        self._connection = connection
        self._executed = executed
        self._proceed = proceed

    async def __aenter__(self) -> _DeferredCommit:
        await self._connection.__aenter__()
        return self

    def cursor(self) -> Any:
        return self._connection.cursor()

    async def __aexit__(self, *exc: object) -> Any:
        self._executed.set()
        await self._proceed.wait()
        return await self._connection.__aexit__(*exc)


def _store(dsn: dict, connector) -> LakebaseBoutLeaseStore:
    return LakebaseBoutLeaseStore(
        endpoint_name="test-endpoint",
        database=dsn["dbname"],
        host=dsn["host"],
        port=dsn["port"],
        user=dsn["user"],
        ring_key=RING_KEY,
        connector=connector,
        workspace_client=_fake_workspace_client(),
    )


def _deferring(executed: asyncio.Event, proceed: asyncio.Event):
    async def connect(**kwargs: Any) -> _DeferredCommit:
        kwargs["autocommit"] = False
        return _DeferredCommit(await _local_pg_connector(**kwargs), executed, proceed)

    return connect


async def _committed_lease(dsn: dict):
    store = _store(dsn, _local_pg_connector)
    await store.initialize()
    while (held := await store.current()) is not None:
        await store.release(held)
    lease = await store.claim(
        session_id="session-a",
        operator=OPERATOR,
        phase="run_committed",
        session_state=SessionState.RUNNING,
        round_id="wake_idle_app",
        round_title="Wake an idle app",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        ttl=TTL,
    )
    return store, lease


async def _queued_on_the_ring_row(dsn: dict, statement_marker: str) -> None:
    """Wait until another backend runs ``statement_marker`` and waits on a row lock."""

    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
    try:
        for _ in range(1000):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND query LIKE %s",
                    (f"%{statement_marker}%",),
                )
                row = await cur.fetchone()
            if row is not None and row[0] >= 1:
                return
            await asyncio.sleep(0.005)
        raise AssertionError(f"no backend queued on the ring row for {statement_marker!r}")
    finally:
        await conn.close()


async def test_pg_a_stale_release_queued_behind_a_towel_transition_frees_nothing(
    pg_dsn,
) -> None:
    plain, committed = await _committed_lease(pg_dsn)
    executed, proceed = asyncio.Event(), asyncio.Event()
    towel_store = _store(pg_dsn, _deferring(executed, proceed))

    transition = asyncio.create_task(
        towel_store.transition(
            committed,
            operator=OPERATOR,
            expected_phase="run_committed",
            phase="towel_cleanup",
            session_state=SessionState.RUNNING,
            ttl=TTL,
        )
    )
    try:
        await asyncio.wait_for(executed.wait(), timeout=10)
        # The towel's UPDATE holds the row, uncommitted. The pre-towel copy's
        # release now queues behind it on a second connection.
        release = asyncio.create_task(plain.release(committed))
        await _queued_on_the_ring_row(pg_dsn, "SET lease_id = NULL")
        assert not release.done()
    finally:
        proceed.set()
    towel = await asyncio.wait_for(transition, timeout=10)

    assert await asyncio.wait_for(release, timeout=10) is False
    held = await plain.current()
    assert held is not None
    assert held.phase == "towel_cleanup"
    assert (held.lease_id, held.fencing_token) == (committed.lease_id, committed.fencing_token)
    assert (towel.lease_id, towel.fencing_token, towel.phase) == (
        held.lease_id,
        held.fencing_token,
        held.phase,
    )
    # The towel's own copy still releases its row.
    assert await plain.release(towel) is True
    assert await plain.current() is None


async def test_pg_a_towel_transition_queued_behind_the_release_finds_the_row_gone(
    pg_dsn,
) -> None:
    plain, committed = await _committed_lease(pg_dsn)
    executed, proceed = asyncio.Event(), asyncio.Event()
    releasing_store = _store(pg_dsn, _deferring(executed, proceed))

    release = asyncio.create_task(releasing_store.release(committed))
    try:
        await asyncio.wait_for(executed.wait(), timeout=10)
        transition = asyncio.create_task(
            plain.transition(
                committed,
                operator=OPERATOR,
                expected_phase="run_committed",
                phase="towel_cleanup",
                session_state=SessionState.RUNNING,
                ttl=TTL,
            )
        )
        await _queued_on_the_ring_row(pg_dsn, "SET phase = %s")
        assert not transition.done()
    finally:
        proceed.set()

    assert await asyncio.wait_for(release, timeout=10) is True
    with pytest.raises(LeaseLostError):
        await asyncio.wait_for(transition, timeout=10)
    assert await plain.current() is None
