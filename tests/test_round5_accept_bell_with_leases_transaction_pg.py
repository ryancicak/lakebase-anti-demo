"""Real ephemeral-PostgreSQL round-trip for the production ``accept_bell_with_leases``
four-row transaction (blocker: the only prior coverage of this SQL was
``test_durable_bell_transaction_inserts_lakebase_release_outbox``, an
``inspect.getsource`` string check, plus a hand-extracted copy of the claim-expiry
predicate in ``test_round5_nobell_authority_pg.py``. Neither ever executed the real
CTE against a real database.

This module drives ``LakebaseRound5WarmStore.accept_bell_with_leases`` -- the exact
method the production ``Round5WarmCoordinator.accept_bell_with_leases`` calls -- with
a real ``psycopg`` async connection against a throwaway ``initdb``/``pg_ctl``
PostgreSQL. Every table it touches is migrated with the exact production DDL:

* ``round5_control_migration_statements()`` (the resident-control outbox + runner
  event tables), and
* ``round5_warm_migration_statements()`` (the warm slot + warm event tables),

plus the ring-lease table, created through the real
``LakebaseBoutLeaseStore.initialize()`` (not a hand-copied ``CREATE TABLE``).

The two ring rows are armed through the real production ``claim()``/``transition()``
CAS statements (not seeded by direct INSERT), the STAGE control-outbox row is
written through the real ``LakebaseRound5ControlStore.enqueue()``, and its
``payload->'binding'`` is required by the production guard to be byte-equal to the
RELEASE event's ``binding.wire_value()`` -- both come from the *same*
``Round5ControlBinding`` instance here, exactly as production shares one binding
across STAGE and RELEASE for a job (see ``Round5ResidentTransport.stage``/
``.release``, which take the identical ``binding`` argument for both calls).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import socket
import subprocess
import tempfile
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest

from server.coordination import (
    COORDINATION_SCHEMA,
    COORDINATION_TABLE,
    LakebaseBoutLeaseStore,
    round_ring_key,
)
from server.models import BoutOperator, SessionState
from server.round5_control import (
    ROUND5_CONTROL_OUTBOX_TABLE,
    LakebaseRound5ControlStore,
    Round5ControlBinding,
    Round5ControlEvent,
    Round5ControlKind,
    canonical_json,
    round5_control_migration_statements,
)
from server.round5_warm import (
    ROUND5_WARM_EVENT_TABLE,
    ROUND5_WARM_SLOT_TABLE,
    LakebaseRound5WarmStore,
    Round5BoutClaim,
    Round5Variant,
    Round5WarmSlot,
    Round5WarmState,
    WarmFenceLostError,
    WarmStoreConflictError,
    round5_warm_migration_statements,
)

LAKEBASE_JOB_DIGEST = "a" * 64
COMPETITOR_JOB_DIGEST = "b" * 64
RUNNER_HARNESS_DIGEST = "c" * 64
ROUND_ID = "survive_connection_spike"

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
    tmp = Path(tempfile.mkdtemp(prefix="r5bellpg-"))
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


async def _migrate_schema(dsn: dict) -> None:
    """Apply BOTH production migration sets against a fresh schema."""

    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=False)
    try:
        async with conn.cursor() as cur:
            await cur.execute(f"DROP SCHEMA IF EXISTS {COORDINATION_SCHEMA} CASCADE")
            await cur.execute(f"CREATE SCHEMA {COORDINATION_SCHEMA}")
            for statement in round5_warm_migration_statements():
                await cur.execute(statement)
            for statement in round5_control_migration_statements():
                await cur.execute(statement)
        await conn.commit()
    finally:
        await conn.close()


async def _warm_store(dsn: dict) -> tuple[Any, LakebaseRound5WarmStore]:
    """A LakebaseRound5WarmStore backed by its own real async psycopg connection."""

    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=False)

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


def _fake_workspace_client() -> Any:
    """``LakebaseBoutLeaseStore`` mints a Databricks OAuth token per attempt; the
    ephemeral cluster runs ``--auth=trust`` so the password value is never
    actually checked, and this stub never touches the real Databricks SDK."""

    from types import SimpleNamespace

    return SimpleNamespace(
        postgres=SimpleNamespace(
            generate_database_credential=lambda _name: SimpleNamespace(
                token="ephemeral-pg-test-token"
            )
        )
    )


async def _local_pg_connector(**kwargs: Any) -> Any:
    """``LakebaseBoutLeaseStore._run`` hardcodes ``sslmode="require"`` for the
    real Lakebase TLS endpoint. The ephemeral ``initdb`` cluster has no TLS
    listener at all, so this drops that one transport-level kwarg before
    delegating to the real ``psycopg.AsyncConnection.connect`` -- every SQL
    statement ``claim()``/``transition()``/``initialize()`` issue is untouched."""

    kwargs.pop("sslmode", None)
    return await psycopg.AsyncConnection.connect(**kwargs)


async def _lease_store(dsn: dict, ring_key: str) -> LakebaseBoutLeaseStore:
    store = LakebaseBoutLeaseStore(
        endpoint_name="test-endpoint",
        database=dsn["dbname"],
        host=dsn["host"],
        port=dsn["port"],
        user=dsn["user"],
        ring_key=ring_key,
        connector=_local_pg_connector,
        workspace_client=_fake_workspace_client(),
    )
    await store.initialize()
    return store


async def _armed_lease(store: LakebaseBoutLeaseStore, *, session_id: str):
    """Drive the real CAS claim()/transition() path to an ARMED ring row."""

    operator = BoutOperator(display_name="Operator", email="operator@example.com")
    checking = await store.claim(
        session_id=session_id,
        operator=operator,
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id=ROUND_ID,
        round_title="Survive a Connection Spike",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        ttl=timedelta(seconds=300),
    )
    return await store.transition(
        checking,
        operator=operator,
        expected_phase="checking",
        phase="armed",
        session_state=SessionState.ARMED,
        ttl=timedelta(seconds=300),
    )


def _claimed_slot(
    *,
    installation_id: str,
    claim_id: str,
    bell_id: str,
    bout_id: str,
    warm_attempt_token: str,
    claim_expires_at: datetime,
    now: datetime,
) -> Round5WarmSlot:
    claim = Round5BoutClaim(
        claim_id=claim_id,
        bell_id=bell_id,
        session_id="session-" + claim_id,
        bout_id=bout_id,
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
        claimed_at=now,
        claim_expires_at=claim_expires_at,
        capsule_generation=1,
        lakebase_job_id=LAKEBASE_JOB_DIGEST,
        competitor_job_id=COMPETITOR_JOB_DIGEST,
        warm_attempt_token=warm_attempt_token,
    )
    return Round5WarmSlot(
        installation_id=installation_id,
        generation=1,
        state=Round5WarmState.CLAIMED,
        revision=1,
        coordinator_fence=1,
        process_epoch="process-pg",
        broker_epoch="broker-pg",
        warm_contract_sha256=LAKEBASE_JOB_DIGEST,
        warming_started_at=now,
        claim=claim,
    )


def _request_and_digest() -> tuple[dict[str, object], str]:
    body = {"kind": "resident_launch_request", "target": "lakebase"}
    digest = hashlib.sha256(canonical_json(body)).hexdigest()
    return {**body, "prepared_request_digest": digest}, digest


def _binding(slot: Round5WarmSlot, *, bell_id: str, request_sha256: str) -> Round5ControlBinding:
    assert slot.claim is not None
    return Round5ControlBinding(
        installation_id=slot.installation_id,
        lane_id="lakebase",
        generation=slot.generation,
        warm_attempt_token=slot.claim.warm_attempt_token,
        claim_id=slot.claim.claim_id,
        bout_id=slot.claim.bout_id,
        bell_id=bell_id,
        fence=slot.claim.bout_fence,
        job_id=slot.claim.lakebase_job_id,
        runner_boot_id="boot-lakebase",
        runner_process_boot_id="process-boot-lakebase",
        runner_harness_sha256=RUNNER_HARNESS_DIGEST,
        request_sha256=request_sha256,
    )


async def _seed_stage_event(
    run,
    binding: Round5ControlBinding,
    *,
    request: dict[str, object],
    staged_at: datetime,
) -> Round5ControlEvent:
    """Insert the STAGE outbox row through the real production ``enqueue()``."""

    stage_event = Round5ControlEvent.create(
        binding=binding,
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": request},
        created_at=staged_at,
    )
    await LakebaseRound5ControlStore(run).enqueue(stage_event)
    return stage_event


def _release_event(binding: Round5ControlBinding, *, bell_at_utc: datetime) -> Round5ControlEvent:
    return Round5ControlEvent.create(
        binding=binding,
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        payload={},
        created_at=bell_at_utc,
    )


async def _scenario(
    dsn: dict,
    *,
    installation_id: str,
    claim_expires_at: datetime,
) -> dict[str, Any]:
    """Build one full, real bell-ready fixture: two armed ring rows, a CLAIMED warm
    slot, and a STAGE outbox row whose binding matches the RELEASE event we hand
    back for the caller to feed into ``accept_bell_with_leases``."""

    await _migrate_schema(dsn)

    claim_id = f"claim-{installation_id}"
    bell_id = f"bell-{installation_id}"
    bout_id = f"bout-{installation_id}"
    warm_attempt_token = f"attempt-{installation_id}"
    now = datetime(2026, 9, 15, tzinfo=UTC)

    main_ring_key = round_ring_key(installation_id, ROUND_ID)
    cleanup_ring_key = round_ring_key(installation_id, ROUND_ID, cleanup=True)
    main_lease_store = await _lease_store(dsn, main_ring_key)
    cleanup_lease_store = await _lease_store(dsn, cleanup_ring_key)
    main_lease = await _armed_lease(main_lease_store, session_id="session-main")
    cleanup_lease = await _armed_lease(cleanup_lease_store, session_id="session-cleanup")

    conn, warm_store = await _warm_store(dsn)
    slot = _claimed_slot(
        installation_id=installation_id,
        claim_id=claim_id,
        bell_id=bell_id,
        bout_id=bout_id,
        warm_attempt_token=warm_attempt_token,
        claim_expires_at=claim_expires_at,
        now=now,
    )
    await warm_store._persist(None, slot, "claim_created", now=now)

    request, request_sha256 = _request_and_digest()
    binding = _binding(slot, bell_id=bell_id, request_sha256=request_sha256)
    await _seed_stage_event(
        warm_store._run,
        binding,
        request=request,
        staged_at=now,
    )

    return {
        "installation_id": installation_id,
        "claim_id": claim_id,
        "bell_id": bell_id,
        "slot": slot,
        "conn": conn,
        "warm_store": warm_store,
        "main_ring_key": main_ring_key,
        "cleanup_ring_key": cleanup_ring_key,
        "main_lease_store": main_lease_store,
        "cleanup_lease_store": cleanup_lease_store,
        "main_lease": main_lease,
        "cleanup_lease": cleanup_lease,
        "binding": binding,
    }


async def _read_ring_row(dsn: dict, ring_key: str) -> tuple[Any, ...] | None:
    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT phase, session_state, fencing_token, lease_id::text, "
                f"session_id, owner_subject, expires_at "
                f"FROM {COORDINATION_TABLE} WHERE ring_key = %s",
                (ring_key,),
            )
            return await cur.fetchone()
    finally:
        await conn.close()


async def _read_warm_slot_row(dsn: dict, installation_id: str) -> tuple[Any, ...] | None:
    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT state, generation, revision, payload "
                f"FROM {ROUND5_WARM_SLOT_TABLE} WHERE installation_id = %s",
                (installation_id,),
            )
            return await cur.fetchone()
    finally:
        await conn.close()


async def _count_warm_events(dsn: dict, installation_id: str, event_type: str) -> int:
    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM {ROUND5_WARM_EVENT_TABLE} "
                f"WHERE installation_id = %s AND event_type = %s",
                (installation_id, event_type),
            )
            row = await cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        await conn.close()


async def _read_release_outbox_row(dsn: dict, job_id: str) -> tuple[Any, ...] | None:
    conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT kind, sequence, payload FROM {ROUND5_CONTROL_OUTBOX_TABLE} "
                f"WHERE job_id = %s AND sequence = 2",
                (job_id,),
            )
            return await cur.fetchone()
    finally:
        await conn.close()


async def _close_scenario(built: dict[str, Any]) -> None:
    await built["conn"].close()


# ---------------------------------------------------------------------------
# 1. Success: the full four-row transaction commits atomically.
# ---------------------------------------------------------------------------


async def test_pg_accept_bell_with_leases_commits_all_touched_rows(pg_dsn) -> None:
    installation_id = "install-bell-success"
    # The production guard checks claim expiry against the LIVE DB clock
    # (``clock_timestamp()``), not the synthetic ``claimed_at`` bookkeeping
    # timestamp used elsewhere in the seed -- so this must be real-clock-relative.
    built = await _scenario(
        pg_dsn,
        installation_id=installation_id,
        claim_expires_at=datetime.now(UTC) + timedelta(seconds=180),
    )
    try:
        bell_at_utc = datetime.now(UTC)
        release_event = _release_event(built["binding"], bell_at_utc=bell_at_utc)

        (
            updated,
            main_updated_at,
            main_expires_at,
            cleanup_updated_at,
            cleanup_expires_at,
        ) = await built["warm_store"].accept_bell_with_leases(
            built["slot"],
            claim_id=built["claim_id"],
            bell_id=built["bell_id"],
            bell_at_utc=bell_at_utc,
            main_ring_key=built["main_ring_key"],
            main_lease=built["main_lease"],
            cleanup_ring_key=built["cleanup_ring_key"],
            cleanup_lease=built["cleanup_lease"],
            ttl=timedelta(seconds=600),
            release_event=release_event,
        )

        assert updated.state == Round5WarmState.RUNNING
        assert updated.revision == built["slot"].revision + 1
        assert updated.bell_id == built["bell_id"]
        assert main_updated_at is not None and main_expires_at is not None
        assert cleanup_updated_at is not None and cleanup_expires_at is not None

        # -- Every touched table, read back from a FRESH connection. --
        main_row = await _read_ring_row(pg_dsn, built["main_ring_key"])
        cleanup_row = await _read_ring_row(pg_dsn, built["cleanup_ring_key"])
        assert main_row is not None and cleanup_row is not None
        assert main_row[0] == "run_committed" and main_row[1] == "running"
        assert cleanup_row[0] == "run_committed" and cleanup_row[1] == "running"
        # Fencing tokens and lease identities are UNCHANGED by the bell -- only
        # phase/session_state/timestamps move.
        assert main_row[2] == built["main_lease"].fencing_token
        assert cleanup_row[2] == built["cleanup_lease"].fencing_token

        warm_row = await _read_warm_slot_row(pg_dsn, installation_id)
        assert warm_row is not None
        assert warm_row[0] == "running"
        assert warm_row[1] == 1  # generation unchanged
        assert warm_row[2] == built["slot"].revision + 1
        payload = warm_row[3] if isinstance(warm_row[3], dict) else json.loads(warm_row[3])
        assert payload["bell_id"] == built["bell_id"]

        assert await _count_warm_events(pg_dsn, installation_id, "bell_accepted") == 1

        outbox_row = await _read_release_outbox_row(pg_dsn, LAKEBASE_JOB_DIGEST)
        assert outbox_row is not None
        assert outbox_row[0] == "release"
        assert outbox_row[1] == 2
        stored_binding = (
            outbox_row[2]["binding"]
            if isinstance(outbox_row[2], dict)
            else json.loads(outbox_row[2])["binding"]
        )
        assert stored_binding == release_event.binding.wire_value()
    finally:
        await _close_scenario(built)


# ---------------------------------------------------------------------------
# 2. Claim-expiry boundary against the REAL, live DB clock (not a bound
#    parameter): equality counts as expired, and so does one microsecond past
#    it. A generous future margin (the success test above, +180s) is the
#    complementary "clearly still valid" case.
#
#    A literal "-1us before expiry is still valid" case cannot be asserted
#    deterministically here: the production guard compares against
#    ``clock_timestamp()`` at COMMIT time, which cannot be pinned to
#    microsecond precision across a network round trip without mocking the
#    database's clock (Postgres offers no session-level clock override for
#    ``clock_timestamp()``, unlike ``now()``/``statement_timestamp()``). The
#    two cases below are the deterministic, non-flaky edges that boundary
#    implies: real time only ever advances, so a claim set to expire AT or
#    BEFORE a just-captured reference instant is *always* observed expired by
#    the time the guard actually runs.
# ---------------------------------------------------------------------------


async def _reference_clock_timestamp(pg_dsn) -> datetime:
    conn = await psycopg.AsyncConnection.connect(**pg_dsn, autocommit=True)
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT clock_timestamp()")
            row = await cur.fetchone()
            assert row is not None
            return row[0]
    finally:
        await conn.close()


async def _expect_bell_rejected_with_no_partial_state(pg_dsn, built: dict[str, Any]) -> None:
    bell_at_utc = datetime.now(UTC)
    release_event = _release_event(built["binding"], bell_at_utc=bell_at_utc)

    with pytest.raises(WarmFenceLostError, match="rolled back"):
        await built["warm_store"].accept_bell_with_leases(
            built["slot"],
            claim_id=built["claim_id"],
            bell_id=built["bell_id"],
            bell_at_utc=bell_at_utc,
            main_ring_key=built["main_ring_key"],
            main_lease=built["main_lease"],
            cleanup_ring_key=built["cleanup_ring_key"],
            cleanup_lease=built["cleanup_lease"],
            ttl=timedelta(seconds=600),
            release_event=release_event,
        )

    # No partial mutation anywhere: ring rows still ARMED, warm slot still
    # CLAIMED at the original revision, no bell event, no RELEASE outbox row.
    main_row = await _read_ring_row(pg_dsn, built["main_ring_key"])
    cleanup_row = await _read_ring_row(pg_dsn, built["cleanup_ring_key"])
    assert main_row is not None and main_row[0] == "armed"
    assert cleanup_row is not None and cleanup_row[0] == "armed"
    warm_row = await _read_warm_slot_row(pg_dsn, built["installation_id"])
    assert warm_row is not None
    assert warm_row[0] == "claimed"
    assert warm_row[2] == built["slot"].revision
    assert await _count_warm_events(pg_dsn, built["installation_id"], "bell_accepted") == 0
    assert await _read_release_outbox_row(pg_dsn, LAKEBASE_JOB_DIGEST) is None


async def test_pg_accept_bell_with_leases_rejects_at_expiry_equality_against_live_clock(
    pg_dsn,
) -> None:
    installation_id = "install-bell-expiry-eq"
    reference = await _reference_clock_timestamp(pg_dsn)
    built = await _scenario(
        pg_dsn,
        installation_id=installation_id,
        claim_expires_at=reference,
    )
    try:
        await _expect_bell_rejected_with_no_partial_state(pg_dsn, built)
    finally:
        await _close_scenario(built)


async def test_pg_accept_bell_with_leases_rejects_one_microsecond_past_expiry(pg_dsn) -> None:
    installation_id = "install-bell-expiry-1us"
    reference = await _reference_clock_timestamp(pg_dsn)
    built = await _scenario(
        pg_dsn,
        installation_id=installation_id,
        claim_expires_at=reference - timedelta(microseconds=1),
    )
    try:
        await _expect_bell_rejected_with_no_partial_state(pg_dsn, built)
    finally:
        await _close_scenario(built)


# ---------------------------------------------------------------------------
# 3. Concurrent accept-bell vs. expiry/begin-cleanup: two REAL, independent
#    connections race the SAME pre-race warm-slot snapshot through the SAME
#    Postgres. PostgreSQL's row-level locking on the single-statement CTEs
#    (each op is exactly one ``FOR UPDATE``-gated statement, executed then
#    immediately committed) serializes the two operations; whichever commits
#    first advances ``revision``, and the other's compare-and-swap on the
#    stale, pre-race revision then matches ZERO rows and fails atomically --
#    never a partial mix of the two outcomes.
# ---------------------------------------------------------------------------


async def _race(
    pg_dsn,
    installation_id: str,
    other_op,
) -> tuple[Any, Any]:
    built = await _scenario(
        pg_dsn,
        installation_id=installation_id,
        claim_expires_at=datetime.now(UTC) + timedelta(seconds=300),
    )
    try:
        second_conn, second_store = await _warm_store(pg_dsn)
        try:
            bell_at_utc = datetime.now(UTC)
            release_event = _release_event(built["binding"], bell_at_utc=bell_at_utc)

            async def bell():
                return await built["warm_store"].accept_bell_with_leases(
                    built["slot"],
                    claim_id=built["claim_id"],
                    bell_id=built["bell_id"],
                    bell_at_utc=bell_at_utc,
                    main_ring_key=built["main_ring_key"],
                    main_lease=built["main_lease"],
                    cleanup_ring_key=built["cleanup_ring_key"],
                    cleanup_lease=built["cleanup_lease"],
                    ttl=timedelta(seconds=600),
                    release_event=release_event,
                )

            bell_result, other_result = await asyncio.gather(
                bell(),
                other_op(second_store, built["slot"]),
                return_exceptions=True,
            )
            return bell_result, other_result
        finally:
            await second_conn.close()
    finally:
        await _close_scenario(built)


def _exactly_one_exception(a: Any, b: Any) -> tuple[Any, Any]:
    a_failed = isinstance(a, BaseException)
    b_failed = isinstance(b, BaseException)
    assert a_failed != b_failed, (a, b)
    return (a, b) if a_failed else (b, a)


async def test_pg_concurrent_accept_bell_versus_expiry_is_atomic(pg_dsn) -> None:
    installation_id = "install-bell-race-expiry"

    async def expire(store, slot):
        return await store.release_expired_claim(
            slot,
            now=slot.claim.claim_expires_at + timedelta(seconds=1),
            still_fresh=False,
        )

    bell_result, expire_result = await _race(pg_dsn, installation_id, expire)
    failure, success = _exactly_one_exception(bell_result, expire_result)
    assert isinstance(failure, (WarmFenceLostError, WarmStoreConflictError))

    warm_row = await _read_warm_slot_row(pg_dsn, installation_id)
    assert warm_row is not None
    main_row = await _read_ring_row(pg_dsn, round_ring_key(installation_id, ROUND_ID))
    assert main_row is not None
    if isinstance(bell_result, BaseException):
        # Expiry won: claim retained under CLEANING, ring rows NEVER touched.
        assert warm_row[0] == "cleaning"
        assert main_row[0] == "armed"
        assert await _count_warm_events(pg_dsn, installation_id, "bell_accepted") == 0
        assert await _read_release_outbox_row(pg_dsn, LAKEBASE_JOB_DIGEST) is None
    else:
        # Bell won: fully committed RUNNING state, expiry's stale CAS refused.
        assert warm_row[0] == "running"
        assert main_row[0] == "run_committed"
        assert await _read_release_outbox_row(pg_dsn, LAKEBASE_JOB_DIGEST) is not None


async def test_pg_concurrent_accept_bell_versus_begin_cleanup_is_atomic(pg_dsn) -> None:
    installation_id = "install-bell-race-cleanup"

    async def begin_cleanup(store, slot):
        assert slot.claim is not None
        return await store.begin_cleanup(
            slot,
            claim_id=slot.claim.claim_id,
            now=datetime.now(UTC),
        )

    bell_result, cleanup_result = await _race(pg_dsn, installation_id, begin_cleanup)
    failure, success = _exactly_one_exception(bell_result, cleanup_result)
    assert isinstance(failure, (WarmFenceLostError, WarmStoreConflictError))

    warm_row = await _read_warm_slot_row(pg_dsn, installation_id)
    assert warm_row is not None
    main_row = await _read_ring_row(pg_dsn, round_ring_key(installation_id, ROUND_ID))
    assert main_row is not None
    if isinstance(bell_result, BaseException):
        assert warm_row[0] == "cleaning"
        assert main_row[0] == "armed"
        assert await _count_warm_events(pg_dsn, installation_id, "bell_accepted") == 0
        assert await _read_release_outbox_row(pg_dsn, LAKEBASE_JOB_DIGEST) is None
    else:
        assert warm_row[0] == "running"
        assert main_row[0] == "run_committed"
        assert await _read_release_outbox_row(pg_dsn, LAKEBASE_JOB_DIGEST) is not None


# ---------------------------------------------------------------------------
# 4. Byte-equivalence sanity: a STAGE row whose binding differs from the
#    RELEASE event's (even by one field) must NOT satisfy the guard -- the
#    guard is a JSONB EQUALITY, not a partial/job_id-only match.
# ---------------------------------------------------------------------------


async def test_pg_accept_bell_with_leases_rejects_when_stage_binding_diverges(pg_dsn) -> None:
    installation_id = "install-bell-binding-mismatch"
    built = await _scenario(
        pg_dsn,
        installation_id=installation_id,
        claim_expires_at=datetime.now(UTC) + timedelta(seconds=180),
    )
    try:
        bell_at_utc = datetime.now(UTC)
        # A RELEASE event built from a DIFFERENT (but individually-valid) binding
        # -- e.g. a different runner_boot_id, as would happen if a resident
        # restarted between STAGE and the bell -- must not satisfy the STAGE
        # EXISTS check, even though every claim/fence/job_id field still matches.
        diverged_binding = replace(built["binding"], runner_boot_id="boot-lakebase-restarted")
        release_event = _release_event(diverged_binding, bell_at_utc=bell_at_utc)

        with pytest.raises(WarmFenceLostError, match="rolled back"):
            await built["warm_store"].accept_bell_with_leases(
                built["slot"],
                claim_id=built["claim_id"],
                bell_id=built["bell_id"],
                bell_at_utc=bell_at_utc,
                main_ring_key=built["main_ring_key"],
                main_lease=built["main_lease"],
                cleanup_ring_key=built["cleanup_ring_key"],
                cleanup_lease=built["cleanup_lease"],
                ttl=timedelta(seconds=600),
                release_event=release_event,
            )

        warm_row = await _read_warm_slot_row(pg_dsn, installation_id)
        assert warm_row is not None and warm_row[0] == "claimed"
        assert await _read_release_outbox_row(pg_dsn, LAKEBASE_JOB_DIGEST) is None
    finally:
        await _close_scenario(built)
