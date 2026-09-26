"""Ephemeral real-PostgreSQL round-trip for the no-bell authority transitions.

Env-gated on a local PostgreSQL (initdb/pg_ctl on PATH) so it is skipped in
environments without one, but exercised locally. It drives the DURABLE
``LakebaseRound5WarmStore`` against real JSONB so the exact transitions -- claim
retained on expired->CLEANING, idempotent begin_cleanup, finish to N+1, the
``cleanup_started`` event, and the episodic ``requires_cleaned_bout`` parse
(explicit-false round-trip and mixed-version infer-when-absent) -- are validated
against the real database, not an in-memory fake cursor.

Stale-owner exception taxonomy (documented per blocker 9): a bell on an expired
claim raises ``WarmClaimUnavailableError``; a lost coordinator fence/owner raises
``WarmFenceLostError``; a CAS that lost a concurrent race raises
``WarmStoreConflictError``. Production callers catch all three (the supervised
loop's ``except (WarmCoordinatorHeldError, WarmStoreConflictError,
WarmFenceLostError)`` and the manager bell handler's broad ``except Exception``).
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import tempfile
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from server.coordination import COORDINATION_SCHEMA
from server.round5_warm import (
    LakebaseRound5WarmStore,
    Round5BoutClaim,
    Round5Variant,
    Round5WarmSlot,
    Round5WarmState,
    round5_warm_migration_statements,
)

DIGEST = "a" * 64

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
    tmp = Path(tempfile.mkdtemp(prefix="r5nobellpg-"))
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


async def _store(dsn):
    """A LakebaseRound5WarmStore backed by a real async psycopg connection."""

    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=False)
    # Fresh coordination schema + the two warm tables the transitions touch.
    async with conn.cursor() as cur:
        await cur.execute(f"DROP SCHEMA IF EXISTS {COORDINATION_SCHEMA} CASCADE")
        await cur.execute(f"CREATE SCHEMA {COORDINATION_SCHEMA}")
        for statement in round5_warm_migration_statements():
            await cur.execute(statement)
    await conn.commit()

    async def run(op):
        async with conn.cursor() as cur:
            try:
                result = await op(cur)
            except Exception:
                await conn.rollback()
                raise
        await conn.commit()
        return result

    return conn, LakebaseRound5WarmStore(run)


def _claimed_slot(now: datetime) -> Round5WarmSlot:
    claim = Round5BoutClaim(
        claim_id="claim-pg",
        bell_id="bell-pg",
        session_id="session-pg",
        bout_id="bout-pg",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
        claimed_at=now,
        claim_expires_at=now + timedelta(seconds=180),
        capsule_generation=1,
        lakebase_job_id=DIGEST,
        competitor_job_id="b" * 64,
        warm_attempt_token="attempt-pg",
    )
    return Round5WarmSlot(
        installation_id="install-pg",
        generation=1,
        state=Round5WarmState.CLAIMED,
        revision=1,
        coordinator_fence=1,
        process_epoch="process-pg",
        broker_epoch="broker-pg",
        warm_contract_sha256=DIGEST,
        warming_started_at=now,
        claim=claim,
    )


async def test_pg_expired_claim_fences_into_cleaning_and_finishes_next_gen(pg_dsn):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    conn, store = await _store(pg_dsn)
    try:
        seeded = _claimed_slot(now)
        # Seed the CLAIMED row through the store's own durable insert path.
        await store._persist(None, seeded, "claim_created", now=now)

        claimed = await store.read("install-pg")
        assert claimed is not None and claimed.state == Round5WarmState.CLAIMED

        # Expired -> CLEANING with the claim RETAINED (never READY/WARMING).
        fenced = await store.release_expired_claim(
            claimed,
            now=now + timedelta(seconds=181),
            still_fresh=False,
        )
        assert fenced.state == Round5WarmState.CLEANING
        assert fenced.claim is not None and fenced.claim.claim_id == "claim-pg"
        # Exact job IDs preserved through the durable transition.
        assert fenced.claim.lakebase_job_id == DIGEST
        assert fenced.claim.competitor_job_id == "b" * 64

        persisted = await store.read("install-pg")
        assert persisted is not None and persisted.state == Round5WarmState.CLEANING
        assert persisted.claim is not None

        events = await store.events("install-pg")
        assert any(e.event_type == "cleanup_started" for e in events)

        # begin_cleanup is idempotent on an already-CLEANING slot with the same claim.
        again = await store.begin_cleanup(
            persisted, claim_id="claim-pg", now=now + timedelta(seconds=182)
        )
        assert again.state == Round5WarmState.CLEANING
        cleanup_started_count = sum(
            1 for e in await store.events("install-pg") if e.event_type == "cleanup_started"
        )
        assert cleanup_started_count == 1

        # Finish -> WARMING generation N+1, with the (episodic) lineage flag set.
        warmed = await store.finish_cleanup_and_rewarm(
            persisted, claim_id="claim-pg", now=now + timedelta(seconds=183)
        )
        assert warmed.state == Round5WarmState.WARMING
        assert warmed.generation == 2
        assert warmed.requires_cleaned_bout is True
        assert warmed.cleaned_bout_id == "bout-pg"

        final = await store.read("install-pg")
        assert final is not None and final.generation == 2
        assert final.state == Round5WarmState.WARMING
    finally:
        await conn.close()


async def test_pg_requires_cleaned_bout_parse_is_episodic_and_mixed_version(pg_dsn):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    conn, store = await _store(pg_dsn)
    try:
        # A WARMING slot that has CLEARED the episodic flag but retains the audit id.
        cleared = Round5WarmSlot(
            installation_id="install-pg-audit",
            generation=3,
            state=Round5WarmState.WARMING,
            revision=1,
            coordinator_fence=1,
            process_epoch="process-pg",
            broker_epoch="broker-pg",
            warm_contract_sha256=DIGEST,
            warming_started_at=now,
            cleaned_bout_id="bout-audit",
            requires_cleaned_bout=False,
        )
        await store._persist(None, cleared, "warm_started", now=now)
        roundtrip = await store.read("install-pg-audit")
        assert roundtrip is not None
        # Explicit false survives the JSONB round-trip even with the audit id kept.
        assert roundtrip.requires_cleaned_bout is False
        assert roundtrip.cleaned_bout_id == "bout-audit"

        # Mixed-version: a legacy row whose payload predates the episodic flag
        # (key absent) infers True from the retained cleaned_bout_id.
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE {COORDINATION_SCHEMA}.round5_warm_slot_v4 "
                "SET payload = payload - 'requires_cleaned_bout' "
                "WHERE installation_id = %s",
                ("install-pg-audit",),
            )
        await conn.commit()
        legacy = await store.read("install-pg-audit")
        assert legacy is not None
        assert legacy.requires_cleaned_bout is True
    finally:
        await conn.close()


async def test_pg_claim_expiry_bell_predicate_executes_with_boundaries(pg_dsn):
    """Validate the accept_bell_with_leases claim-expiry SQL predicate on real PG.

    The atomic bell guard rejects a claim whose expiry is <= the DB clock via
    ``(payload->'claim'->>'claim_expires_at')::timestamptz > clock_timestamp()``.
    Here we prove that exact JSONB->timestamptz predicate truly executes and has
    the right boundary semantics (equality/-1us/+1us) and against the live DB clock.
    """

    now = datetime(2026, 9, 15, tzinfo=UTC)
    conn, store = await _store(pg_dsn)
    try:
        seeded = _claimed_slot(now)  # claim_expires_at = now + 180s
        await store._persist(None, seeded, "claim_created", now=now)
        expiry = seeded.claim.claim_expires_at
        table = f"{COORDINATION_SCHEMA}.round5_warm_slot_v4"
        predicate = (
            f"SELECT (payload->'claim'->>'claim_expires_at')::timestamptz > %s::timestamptz "
            f"FROM {table} WHERE installation_id = %s"
        )

        async with conn.cursor() as cur:
            # Equality: expiry > expiry is FALSE (equality counts as expired).
            await cur.execute(predicate, (expiry, "install-pg"))
            assert (await cur.fetchone())[0] is False
            # -1us before expiry: expiry > (expiry-1us) is TRUE (still valid).
            await cur.execute(predicate, (expiry - timedelta(microseconds=1), "install-pg"))
            assert (await cur.fetchone())[0] is True
            # +1us past expiry: expiry > (expiry+1us) is FALSE (expired).
            await cur.execute(predicate, (expiry + timedelta(microseconds=1), "install-pg"))
            assert (await cur.fetchone())[0] is False

            # Against the LIVE DB clock (clock_timestamp()), exactly as production.
            live = (
                f"SELECT (payload->'claim'->>'claim_expires_at')::timestamptz "
                f"> clock_timestamp() FROM {table} WHERE installation_id = %s"
            )
            # Seeded expiry is far in the past (2026-09-15 + 180s), so it is expired.
            await cur.execute(live, ("install-pg",))
            assert (await cur.fetchone())[0] is False

            # A future-dated claim is NOT expired against the live clock.
            future = _claimed_slot(datetime.now(UTC) + timedelta(hours=1))
            future = replace(future, installation_id="install-pg-future")
            await store._persist(None, future, "claim_created", now=now)
            await cur.execute(live, ("install-pg-future",))
            assert (await cur.fetchone())[0] is True
    finally:
        await conn.close()
