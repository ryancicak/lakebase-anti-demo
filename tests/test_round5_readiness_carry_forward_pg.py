"""Real ephemeral-PostgreSQL round trip for ``StartupReadinessStore.carry_ready_forward``.

The statement moves Round 5's READY stamp up to the ring's current generation
without taking the ring. The fenced re-stamp it replaces read as CLEANUP IN
PROGRESS on an idle round (release bar, 2026-10-01 01:20:27Z). Its guards are what
keep it from advertising a round that is in use: the row must be READY, sealed with
the same manifest and behind the ring, and no lease may be held. So they run here
against the production DDL, with the ring lease table created through
``LakebaseBoutLeaseStore.initialize()`` and the readiness table through
``StartupReadinessStore.initialize()``. Every claim and release goes through the
real CAS statements.
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from server.coordination import COORDINATION_SCHEMA, ROUND5_RING_KEY, LakebaseBoutLeaseStore
from server.models import BoutOperator, SessionState
from server.readiness import StartupReadinessStore

SEAL = "a" * 64
OPERATOR = BoutOperator(display_name="Operator", email="operator@example.com")

pytestmark = pytest.mark.skipif(
    shutil.which("initdb") is None or shutil.which("pg_ctl") is None,
    reason="requires a local PostgreSQL (initdb/pg_ctl on PATH)",
)


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


@pytest.fixture(scope="module")
def pg_dsn():
    tmp = Path(tempfile.mkdtemp(prefix="r5readypg-"))
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


async def _local_pg_connector(**kwargs: Any) -> Any:
    """The ephemeral cluster has no TLS listener; drop only the transport kwarg."""

    kwargs.pop("sslmode", None)
    return await psycopg.AsyncConnection.connect(**kwargs)


def _fake_workspace_client() -> Any:
    """The cluster runs ``--auth=trust``, so the minted password is never checked."""

    return SimpleNamespace(
        postgres=SimpleNamespace(
            generate_database_credential=lambda _name: SimpleNamespace(
                token="ephemeral-pg-test-token"
            )
        )
    )


async def _stores(dsn: dict) -> tuple[Any, LakebaseBoutLeaseStore, StartupReadinessStore]:
    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=False)
    async with conn.cursor() as cur:
        await cur.execute(f"DROP SCHEMA IF EXISTS {COORDINATION_SCHEMA} CASCADE")
    await conn.commit()
    leases = LakebaseBoutLeaseStore(
        endpoint_name="test-endpoint",
        database=dsn["dbname"],
        host=dsn["host"],
        port=dsn["port"],
        user=dsn["user"],
        ring_key=ROUND5_RING_KEY,
        connector=_local_pg_connector,
        workspace_client=_fake_workspace_client(),
    )
    await leases.initialize()

    async def run(operation):
        async with conn.cursor() as cur:
            try:
                result = await operation(cur)
            except Exception:
                await conn.rollback()
                raise
        await conn.commit()
        return result

    readiness = StartupReadinessStore(run, ring_key=ROUND5_RING_KEY)
    await readiness.initialize()
    return conn, leases, readiness


async def _claim(leases: LakebaseBoutLeaseStore, *, phase: str, session_id: str, ttl=None):
    return await leases.claim(
        session_id=session_id,
        operator=OPERATOR,
        phase=phase,
        session_state=SessionState.CHECKING,
        round_id="survive_connection_spike",
        round_title="Ready a pooled application path",
        competitor_id="rds_postgres",
        competitor_name="RDS PostgreSQL",
        ttl=ttl or timedelta(minutes=5),
    )


async def _stamp(
    leases: LakebaseBoutLeaseStore,
    readiness: StartupReadinessStore,
    *,
    state: str = "ready",
    seal: str = SEAL,
) -> int:
    """Stamp the row the way the gate does: under a startup lease, then release."""

    lease = await _claim(leases, phase="startup_cleanup", session_id="startup-cleanup-run")
    await readiness.write(lease, manifest_seal=seal, state=state, detail=None)
    assert await leases.release(lease) is True
    return lease.fencing_token


async def _a_bout_comes_and_goes(leases: LakebaseBoutLeaseStore) -> None:
    bout = await _claim(leases, phase="checking", session_id="finished-bout")
    assert await leases.release(bout) is True


def test_a_ready_stamp_catches_up_with_the_ring_without_a_claim(pg_dsn) -> None:
    async def scenario() -> None:
        conn, leases, readiness = await _stores(pg_dsn)
        try:
            stamped = await _stamp(leases, readiness)
            await _a_bout_comes_and_goes(leases)
            generation = await readiness.ring_generation()
            assert (await readiness.read()).fencing_token == stamped < generation

            assert await readiness.carry_ready_forward(manifest_seal=SEAL) is True

            # The ring itself did not move: nothing claimed it to write the stamp.
            assert await readiness.ring_generation() == generation
            row = await readiness.read()
            assert (row.state, row.manifest_seal, row.fencing_token) == ("ready", SEAL, generation)
            assert await leases.current() is None
            # Once caught up, there is nothing left to carry.
            assert await readiness.carry_ready_forward(manifest_seal=SEAL) is False
        finally:
            await conn.close()

    asyncio.run(scenario())


def test_a_held_ring_is_never_stamped_ready(pg_dsn) -> None:
    async def scenario() -> None:
        conn, leases, readiness = await _stores(pg_dsn)
        try:
            stamped = await _stamp(leases, readiness)
            bout = await _claim(leases, phase="checking", session_id="live-bout")

            assert await readiness.carry_ready_forward(manifest_seal=SEAL) is False
            assert (await readiness.read()).fencing_token == stamped

            assert await leases.release(bout) is True
            assert await readiness.carry_ready_forward(manifest_seal=SEAL) is True
            assert (await readiness.read()).fencing_token == bout.fencing_token
        finally:
            await conn.close()

    asyncio.run(scenario())


def test_an_expired_lease_reads_as_an_idle_ring(pg_dsn) -> None:
    async def scenario() -> None:
        conn, leases, readiness = await _stores(pg_dsn)
        try:
            await _stamp(leases, readiness)
            # A holder that died without releasing: the same rule current() and
            # claim() use, so a lapsed lease neither blocks this nor is overlooked.
            lapsed = await _claim(
                leases, phase="checking", session_id="lapsed-bout", ttl=timedelta(seconds=1)
            )
            await asyncio.sleep(1.5)
            assert await leases.current() is None

            assert await readiness.carry_ready_forward(manifest_seal=SEAL) is True
            assert (await readiness.read()).fencing_token == lapsed.fencing_token
        finally:
            await conn.close()

    asyncio.run(scenario())


def test_only_a_ready_row_under_the_same_seal_moves(pg_dsn) -> None:
    async def scenario() -> None:
        conn, leases, readiness = await _stores(pg_dsn)
        try:
            blocked = await _stamp(leases, readiness, state="blocked")
            await _a_bout_comes_and_goes(leases)
            assert await readiness.carry_ready_forward(manifest_seal=SEAL) is False
            assert (await readiness.read()).fencing_token == blocked

            ready = await _stamp(leases, readiness)
            await _a_bout_comes_and_goes(leases)
            assert await readiness.carry_ready_forward(manifest_seal="b" * 64) is False
            row = await readiness.read()
            assert (row.state, row.fencing_token) == ("ready", ready)
        finally:
            await conn.close()

    asyncio.run(scenario())
