"""REAL PostgreSQL regression tests for the Round 5 journal lifecycle CHECK.

The 2026-09-23 live arm regression: the arm/bell path wrote a lifecycle_state the
deployed ``round5_creation_journal_lifecycle_state_check`` constraint did not
admit, so PostgreSQL rejected every arm insert.  Every in-memory journal test
passed because a fake dict-journal has no CHECK.

Approach A emits only already-allowed states, so no migration is needed.  These
tests run against a REAL ephemeral PostgreSQL so the schema itself is under test:

* the code lifecycle enum and the DDL allow-list never drift (pure, no PG);
* the journal CHECK admits every :class:`LifecycleState` value and REJECTS any
  other (a constraint-enforcing journal, unlike the fakes that let this ship);
* the REAL journal store's guarded commit -- the exact SQL the manager ARM/bell
  path runs -- durably commits a ``create_intent`` row;
* ``admitted_lifecycle_states`` reads the live CHECK so readiness (req #3) can
  refuse ring readiness on a code<->live-DB mismatch.
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest

from server.connection_spike_journal import (
    ROUND5_CREATION_JOURNAL_TABLE,
    CreationScope,
    JournalEvent,
    LifecycleState,
    ResourceSpec,
    Round5CreationCoordinator,
)
from server.connection_spike_live import LakebaseCreationJournalStore
from server.coordination import COORDINATION_TABLE, RING_KEY
from server.lifecycle import (
    ROUND5_JOURNAL_LIFECYCLE_STATES,
    _round5_journal_lifecycle_check_predicate,
)

pytestmark = pytest.mark.skipif(
    shutil.which("initdb") is None or shutil.which("pg_ctl") is None,
    reason="requires a local PostgreSQL (initdb/pg_ctl on PATH)",
)

_JOURNAL_COLUMNS_DDL = """
    event_id bigserial PRIMARY KEY,
    bout_id text NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    ordinal integer NOT NULL CHECK (ordinal > 0),
    resource_kind text NOT NULL CHECK (resource_kind <> ''),
    deterministic_name text,
    client_token text,
    provider_id text,
    lifecycle_state text NOT NULL CHECK ({check}),
    metadata jsonb NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
    runtime_seal_sha256 char(64) NOT NULL,
    intent_at timestamptz NOT NULL,
    occurred_at timestamptz NOT NULL,
    completed_at timestamptz,
    error text,
    CHECK (deterministic_name IS NOT NULL OR client_token IS NOT NULL)
"""


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


@pytest.fixture(scope="module")
def pg_dsn():
    tmp = Path(tempfile.mkdtemp(prefix="r5pg-"))
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


def _reset_schema(conn: psycopg.Connection) -> None:
    conn.execute("DROP SCHEMA IF EXISTS anti_demo_coordination CASCADE")
    conn.execute("CREATE SCHEMA anti_demo_coordination")
    conn.commit()


def _create_journal(conn: psycopg.Connection, *, check: str | None = None) -> None:
    predicate = check or _round5_journal_lifecycle_check_predicate()
    conn.execute(
        f"CREATE TABLE {ROUND5_CREATION_JOURNAL_TABLE} "
        f"({_JOURNAL_COLUMNS_DDL.format(check=predicate)})"
    )
    conn.commit()


def _insert_state(conn: psycopg.Connection, state: str, ordinal: int) -> None:
    conn.execute(
        f"""
        INSERT INTO {ROUND5_CREATION_JOURNAL_TABLE} (
            bout_id, fencing_token, ordinal, resource_kind, deterministic_name,
            lifecycle_state, metadata, runtime_seal_sha256, intent_at, occurred_at,
            completed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
        """,
        (
            "bout-pg",
            7,
            ordinal,
            "rds_proxy",
            f"r5-proxy-{ordinal}",
            state,
            "{}",
            "d" * 64,
            datetime.now(UTC),
            datetime.now(UTC),
            None if state in {"create_intent", "delete_intent"} else datetime.now(UTC),
        ),
    )
    conn.commit()


# --------------------------------------------------------------------------- #
# Pure: the code enum and the DDL allow-list must never drift (schema<->enum)
# --------------------------------------------------------------------------- #


def test_ddl_allowlist_matches_the_lifecycle_enum_exactly() -> None:
    assert set(ROUND5_JOURNAL_LIFECYCLE_STATES) == {s.value for s in LifecycleState}
    predicate = _round5_journal_lifecycle_check_predicate()
    for state in LifecycleState:
        assert f"'{state.value}'" in predicate


# --------------------------------------------------------------------------- #
# Constraint-enforcing journal on real PostgreSQL (the fakes never did this)
# --------------------------------------------------------------------------- #


def test_journal_check_admits_every_code_state_and_rejects_others(pg_dsn) -> None:
    with psycopg.connect(**pg_dsn) as conn:
        _reset_schema(conn)
        _create_journal(conn)  # fresh DDL from the single-source predicate

        # Every state the code emits commits.
        for index, state in enumerate(sorted(ROUND5_JOURNAL_LIFECYCLE_STATES), start=1):
            _insert_state(conn, state, ordinal=index)

        # A state the code does NOT emit (e.g. the reverted pending_launch, or any
        # typo) is rejected by the real CHECK -- proving the constraint is real.
        for forbidden in ("pending_launch", "bogus_state"):
            with pytest.raises(psycopg.errors.CheckViolation):
                _insert_state(conn, forbidden, ordinal=99)
            conn.rollback()


def test_admitted_lifecycle_states_reads_the_live_check(pg_dsn) -> None:
    # Req #3: readiness reads the live CHECK to refuse readiness on drift.
    async def read(dsn: dict) -> frozenset[str]:
        async def run(fn):
            conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
            try:
                async with conn.cursor() as cursor:
                    return await fn(cursor)
            finally:
                await conn.close()

        store = LakebaseCreationJournalStore(run, authority_ring_key=RING_KEY)
        return await store.admitted_lifecycle_states()

    import asyncio

    with psycopg.connect(**pg_dsn) as conn:
        _reset_schema(conn)
        _create_journal(conn)  # full allow-list
    admitted = asyncio.run(read(pg_dsn))
    assert admitted == set(ROUND5_JOURNAL_LIFECYCLE_STATES)

    # A narrower CHECK (missing create_intent) is detected as a drift: the states
    # the code emits are NOT all admitted.
    narrow = "lifecycle_state IN ('created', 'deleted')"
    with psycopg.connect(**pg_dsn) as conn:
        _reset_schema(conn)
        _create_journal(conn, check=narrow)
    admitted_narrow = asyncio.run(read(pg_dsn))
    assert "create_intent" not in admitted_narrow
    assert {s.value for s in LifecycleState} - admitted_narrow  # non-empty -> refuse


# --------------------------------------------------------------------------- #
# Real journal store ARM/bell path: the exact guarded SQL the manager runs
# --------------------------------------------------------------------------- #


def _create_ring_lease(conn: psycopg.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE {COORDINATION_TABLE} (
            ring_key text NOT NULL,
            session_id text,
            fencing_token bigint,
            lease_id text,
            expires_at timestamptz
        )
        """
    )
    conn.commit()


def _insert_active_lease(conn: psycopg.Connection, *, session_id: str, fence: int) -> None:
    conn.execute(
        f"INSERT INTO {COORDINATION_TABLE} "
        "(ring_key, session_id, fencing_token, lease_id, expires_at) "
        "VALUES (%s, %s, %s, %s, %s)",
        (RING_KEY, session_id, fence, str(uuid4()), datetime.now(UTC) + timedelta(hours=1)),
    )
    conn.commit()


class _Fence:
    async def assert_current(self, _scope) -> None:
        return None


def _proxy_spec() -> ResourceSpec:
    return ResourceSpec(
        1, "rds_proxy", deterministic_name="r5-proxy", metadata={"tags": {"owner": "x"}}
    )


async def _arm_precommit(dsn: dict) -> JournalEvent:
    """Drive the REAL LakebaseCreationJournalStore commit the ARM/bell path uses."""

    async def run(fn):
        conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
        try:
            async with conn.cursor() as cursor:
                return await fn(cursor)
        finally:
            await conn.close()

    store = LakebaseCreationJournalStore(run, authority_ring_key=RING_KEY)
    coordinator = Round5CreationCoordinator(
        journal=store,
        fence=_Fence(),
        adapters={"rds_proxy": SimpleNamespace(create=None, inspect=None, delete=None)},
    )
    scope = CreationScope("bout-arm", 7, "d" * 64)
    # Approach A: the bell path commits CREATE_INTENT (an admitted state) through
    # the guarded lease-fence -- exactly what the manager's precommit_launch_intent
    # does via the orchestrator.
    return await coordinator.precommit_intent(scope, _proxy_spec())


async def test_real_journal_arm_commits_create_intent(pg_dsn) -> None:
    with psycopg.connect(**pg_dsn) as conn:
        _reset_schema(conn)
        _create_ring_lease(conn)
        _insert_active_lease(conn, session_id="bout-arm", fence=7)
        _create_journal(conn)  # current allow-list

    # READY -> claim(lease) -> real-journal bell precommit succeeds, and a durable
    # create_intent row exists (written through the guarded lease-fence).
    event = await _arm_precommit(pg_dsn)
    assert event.lifecycle_state is LifecycleState.CREATE_INTENT

    with psycopg.connect(**pg_dsn) as conn:
        row = conn.execute(
            f"SELECT lifecycle_state FROM {ROUND5_CREATION_JOURNAL_TABLE} "
            "WHERE bout_id = %s AND ordinal = %s",
            ("bout-arm", 1),
        ).fetchone()
    assert row is not None and row[0] == "create_intent"
