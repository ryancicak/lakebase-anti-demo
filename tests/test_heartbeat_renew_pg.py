"""Real ephemeral-PostgreSQL proof that a lapsed ring row is never a confirmed release.

Only the coordinator knows when its row lapsed: a clock that jumps there leaves this
process's proof of the lease, and the lease's own ``expires_at``, in the future. If
``release`` still cleared that row, the manager would take the lapse for the bout giving
its ring up and open the ring here while the bout's run still finishes. Every statement
is the production SQL of ``LakebaseBoutLeaseStore``; the lapse is written straight into
the row on the database's clock.

The same coordinator also carries the next bout of a card whose Round 1 idle watch
outlived that release: the watch timing out or turning ready afterwards leaves the new lease
alone, and what its control planes answer afterwards confirms nothing in the next bout's
cooldown.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from server.coordination import COORDINATION_TABLE, LakebaseBoutLeaseStore
from server.manager import InvalidStateError
from server.models import SessionState
from tests.test_heartbeat_renew import (
    OTHER,
    OWNER,
    _a_stale_answer_meets_the_next_bouts_cooldown,
    _failing_round_one,
    _finishing,
    _rearmed_as_the_idle_watch_times_out,
    _rearmed_as_the_idle_watch_turns_ready,
    _round_six,
    _short_leases,
)
from tests.test_round5_accept_bell_with_leases_transaction_pg import _free_port, _lease_store

pytestmark = pytest.mark.skipif(
    shutil.which("initdb") is None or shutil.which("pg_ctl") is None,
    reason="requires a local PostgreSQL (initdb/pg_ctl on PATH)",
)

RING_KEY = "main"


@pytest.fixture(scope="module")
def pg_dsn():
    tmp = Path(tempfile.mkdtemp(prefix="ringlapsepg-"))
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
        subprocess.run(["pg_ctl", "-D", str(data), "-m", "immediate", "stop"], capture_output=True)
        shutil.rmtree(tmp, ignore_errors=True)


async def _empty_ring(dsn: dict) -> LakebaseBoutLeaseStore:
    store = await _lease_store(dsn, RING_KEY)
    while (held := await store.current()) is not None:
        await store.release(held)
    return store


async def _lapse(dsn: dict) -> None:
    """Expire the held ring row on the database's clock, leaving every other column."""

    async with await psycopg.AsyncConnection.connect(**dsn, autocommit=True) as conn:
        cursor = await conn.execute(
            f"UPDATE {COORDINATION_TABLE} "
            "SET expires_at = clock_timestamp() - interval '1 second' "
            "WHERE ring_key = %s AND lease_id IS NOT NULL",
            (RING_KEY,),
        )
        assert cursor.rowcount == 1


async def _finishing_on(dsn: dict):
    """``_finishing`` on the real coordinator, with leases that local PostgreSQL renews
    comfortably even on a loaded machine."""

    store = await _empty_ring(dsn)
    manager, record, lease, finishes, run = await _finishing(store)
    _short_leases(manager, heartbeat=0.25, ttl=3.0)
    return store, manager, record, lease, finishes, run


async def test_pg_a_release_never_confirms_a_row_lapsed_on_the_coordinator_clock(
    pg_dsn,
) -> None:
    store = await _empty_ring(pg_dsn)
    lease = await store.claim(
        session_id="session-a",
        operator=OWNER,
        phase="run_committed",
        session_state=SessionState.RUNNING,
        round_id="analyze_live_orders",
        round_title="Analyze live orders",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        ttl=timedelta(minutes=5),
    )
    await _lapse(pg_dsn)
    assert lease.expires_at > datetime.now(UTC)

    assert await store.release(lease) is False
    assert await store.current() is None
    # The row it leaves behind holds nothing: the next claim takes the next fence.
    successor = await store.claim(
        session_id="session-b",
        operator=OTHER,
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id="analyze_live_orders",
        round_title="Analyze live orders",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        ttl=timedelta(minutes=5),
    )
    assert successor.fencing_token == lease.fencing_token + 1
    assert await store.release(successor) is True
    assert await store.current() is None


async def test_pg_a_terminal_settle_that_finds_the_row_lapsed_keeps_the_ring_closed(
    pg_dsn,
) -> None:
    """The bout still finishes with its own outcome, and its run still finishing keeps
    the ring closed here."""

    store, manager, record, lease, finishes, run = await _finishing_on(pg_dsn)
    await _lapse(pg_dsn)
    assert lease.expires_at > datetime.now(UTC)
    assert record.lease_proof is not None and record.lease_proof[1] > time.monotonic()
    message = "The AWS lane's verifier lost its connection"
    try:
        await manager._finish_live_orders_failure(record, message)
        failed = await manager.get(record.snapshot.id)
        assert failed.state == SessionState.FAILED
        assert failed.failure == message
        assert record.lease is None
        assert record.ring_lease_lost is True
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
        assert await store.current() is None
    finally:
        finishes.set()
    await run
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def test_pg_a_bout_release_that_finds_the_row_lapsed_keeps_the_ring_closed(
    pg_dsn,
) -> None:
    store, manager, record, lease, finishes, run = await _finishing_on(pg_dsn)
    await _lapse(pg_dsn)
    assert lease.expires_at > datetime.now(UTC)
    assert record.lease_proof is not None and record.lease_proof[1] > time.monotonic()
    try:
        assert await manager._release_bout(record) is True
        assert record.lease is None
        assert record.ring_lease_lost is True
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
        assert await store.current() is None
    finally:
        finishes.set()
    await run
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


@pytest.mark.parametrize("path", ["terminal settle", "release_bout"])
async def test_pg_a_release_the_coordinator_confirms_leaves_the_ring_open(
    pg_dsn, path: str
) -> None:
    """The control: in time, the release is the bout giving its ring up, so the run
    still finishing after it closes nothing here."""

    store, manager, record, _lease, finishes, run = await _finishing_on(pg_dsn)
    try:
        if path == "terminal settle":
            await manager._finish_live_orders_failure(record, "The AWS lane's verifier failed")
        else:
            assert await manager._release_bout(record) is True
        assert record.lease is None
        assert record.ring_lease_lost is False
        assert await store.current() is None
        later = await manager.create(_round_six())
        assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
        assert not run.done()
    finally:
        finishes.set()
    await run
    await manager.close()


@pytest.mark.parametrize("lapsed", [True, False], ids=["lost release", "confirmed release"])
async def test_pg_an_idle_watch_timing_out_after_its_card_rearmed_leaves_the_new_lease_alone(
    pg_dsn, lapsed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _empty_ring(pg_dsn)
    manager, record = await _failing_round_one(store)
    _short_leases(manager, heartbeat=0.25, ttl=3.0)
    if lapsed:
        await _lapse(pg_dsn)
    rearmed, lost = await _rearmed_as_the_idle_watch_times_out(manager, record, monkeypatch)
    assert lost is lapsed
    await asyncio.sleep(manager._active_lease_ttl + 0.5)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    held = await store.current()
    assert held is not None and manager._same_exact_lease(held, rearmed)
    assert record.ring_lease_lost is False
    await manager.close()


@pytest.mark.parametrize("lapsed", [True, False], ids=["lost release", "confirmed release"])
async def test_pg_an_idle_watch_turning_ready_after_its_card_rearmed_releases_nothing(
    pg_dsn, lapsed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _empty_ring(pg_dsn)
    manager, record = await _failing_round_one(store)
    _short_leases(manager, heartbeat=0.25, ttl=3.0)
    if lapsed:
        await _lapse(pg_dsn)
    rearmed, lost = await _rearmed_as_the_idle_watch_turns_ready(manager, record, monkeypatch)
    assert lost is lapsed
    await asyncio.sleep(manager._active_lease_ttl + 0.5)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    held = await store.current()
    assert held is not None and manager._same_exact_lease(held, rearmed)
    assert record.ring_lease_lost is False
    await manager.close()


async def test_pg_a_stale_idle_watch_cannot_confirm_or_release_the_next_bouts_cooldown(
    pg_dsn,
) -> None:
    await _a_stale_answer_meets_the_next_bouts_cooldown(await _empty_ring(pg_dsn))
