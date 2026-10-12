"""REAL PostgreSQL regressions for Round 4 rebase fencing and boot privileges.

The append-only event_id CAS is not atomic under READ COMMITTED: an uncommitted
owed INSERT can allocate a lower identity that a concurrent satisfy cannot see,
so a stale satisfy can commit a newer row and leave the later owed event
satisfied. These tests use a real ephemeral PostgreSQL. They skip when initdb
and pg_ctl are not on PATH.
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from server import pipeline_power
from server.coordination import COORDINATION_SCHEMA, CoordinationObjectsMissingError
from server.pipeline_power import (
    _INTENT_FIELDS,
    _INTENT_REBASE_OWED,
    PIPELINE_POWER_SEQUENCE,
    PIPELINE_POWER_TABLE,
    ROUND4_REBASE_SEQUENCE,
    ROUND4_REBASE_TABLE,
    DurablePipelinePowerStore,
    _pair_generation,
    install_pipeline_power_store,
    mint_power_pair_key,
    reset_power_start_generation,
)

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
    tmp = Path(tempfile.mkdtemp(prefix="r4rebase-pg-"))
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
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)
    try:
        yield dsn
    finally:
        subprocess.run(
            ["pg_ctl", "-D", str(data), "-m", "immediate", "stop"],
            capture_output=True,
        )
        shutil.rmtree(tmp, ignore_errors=True)


def _runner(dsn: dict):
    async def run(operation):
        conn = await psycopg.AsyncConnection.connect(**dsn, autocommit=True)
        try:
            async with conn.cursor() as cursor:
                return await operation(cursor)
        finally:
            await conn.close()

    return run


def _held_runner(connection: psycopg.AsyncConnection):
    async def run(operation):
        async with connection.cursor() as cursor:
            return await operation(cursor)

    return run


def _ensure_tables(dsn: dict) -> DurablePipelinePowerStore:
    store = DurablePipelinePowerStore(_runner(dsn))
    asyncio.run(store.initialize())
    return store


def test_uncommitted_owed_insert_fences_stale_satisfy(pg_dsn):
    """owed2 holds the pipeline lock until commit; stale satisfy cannot sneak past."""

    store = _ensure_tables(pg_dsn)
    pipeline_id = "pipeline-lock-1"
    owed1 = asyncio.run(store.append_rebase(pipeline_id, _INTENT_REBASE_OWED))

    async def interleave() -> None:
        held = await psycopg.AsyncConnection.connect(**pg_dsn, autocommit=False)
        waiter = await psycopg.AsyncConnection.connect(**pg_dsn, autocommit=False)
        try:
            owed2_store = DurablePipelinePowerStore(_held_runner(held))
            satisfy_store = DurablePipelinePowerStore(_held_runner(waiter))
            owed2 = await owed2_store.append_rebase(pipeline_id, _INTENT_REBASE_OWED)
            assert owed2 > owed1
            pending = asyncio.create_task(
                satisfy_store.satisfy_rebase(pipeline_id, owed1)
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not pending.done():
                async with await psycopg.AsyncConnection.connect(
                    **pg_dsn, autocommit=True
                ) as probe:
                    async with probe.cursor() as cursor:
                        await cursor.execute(
                            "SELECT count(*) FROM pg_locks "
                            "WHERE locktype = 'advisory' AND NOT granted"
                        )
                        row = await cursor.fetchone()
                if row is not None and int(row[0]) > 0:
                    break
                await asyncio.sleep(0.05)
            assert not pending.done(), "stale satisfy must wait on the uncommitted owed"
            await held.commit()
            inserted = await pending
            assert inserted is None
            await waiter.commit()
            latest = await store.latest_rebase(pipeline_id)
            assert latest is not None
            assert latest["event_id"] == owed2
            assert latest["intent"] == _INTENT_REBASE_OWED
        finally:
            await held.close()
            await waiter.close()

    asyncio.run(interleave())


def test_the_generation_floor_is_the_newest_reservation_on_real_postgres(pg_dsn, monkeypatch):
    """One keyed row: the newest reservation for this pipeline, whatever committed after it."""

    store = _ensure_tables(pg_dsn)
    pipeline_id = "pipeline-floor-1"

    def key(generation: int) -> str:
        return f"ad-test-001:{generation}:{generation:032x}"

    def append(intent: str, pipeline: str, generation: int) -> None:
        stamp_field, actor_field = _INTENT_FIELDS[intent]
        asyncio.run(
            store.append(
                {
                    "intent": intent,
                    "pipeline_id": pipeline,
                    "run_id": key(generation),
                    stamp_field: datetime.now(UTC).isoformat(),
                    actor_field: "pg-test",
                }
            )
        )

    for generation in (1, 2, 3):
        append("generation_reserved", pipeline_id, generation)
        append("resuming", pipeline_id, generation)
    append("stop_owed", pipeline_id, 1)
    append("generation_reserved", "pipeline-floor-other", 99)

    assert asyncio.run(store.newest_reserved_pair_key(pipeline_id)) == key(3)
    assert asyncio.run(store.newest_reserved_pair_key("pipeline-floor-empty")) == ""

    install_pipeline_power_store(store)
    try:
        reset_power_start_generation()
        monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 1)
        minted = asyncio.run(mint_power_pair_key("ad-test-001", pipeline_id))
    finally:
        install_pipeline_power_store(None)
    assert _pair_generation(minted) == 4
    assert asyncio.run(store.newest_reserved_pair_key(pipeline_id)) == minted


def test_select_only_rebase_table_fails_initialize(pg_dsn):
    _ensure_tables(pg_dsn)
    role = "round4_rebase_select_only"
    with psycopg.connect(**pg_dsn, autocommit=True) as conn:
        conn.execute(f"DROP ROLE IF EXISTS {role}")
        conn.execute(f"CREATE ROLE {role} LOGIN")
        conn.execute(f"GRANT USAGE ON SCHEMA {COORDINATION_SCHEMA} TO {role}")
        conn.execute(f"GRANT SELECT ON TABLE {ROUND4_REBASE_TABLE} TO {role}")
        conn.execute(
            f"GRANT SELECT, INSERT ON TABLE {PIPELINE_POWER_TABLE} TO {role}"
        )
        conn.execute(
            f"GRANT USAGE ON SEQUENCE {COORDINATION_SCHEMA}.{ROUND4_REBASE_SEQUENCE} "
            f"TO {role}"
        )
        conn.execute(
            f"GRANT USAGE ON SEQUENCE {COORDINATION_SCHEMA}.{PIPELINE_POWER_SEQUENCE} "
            f"TO {role}"
        )
    limited = dict(pg_dsn, user=role)
    store = DurablePipelinePowerStore(_runner(limited))
    with pytest.raises(CoordinationObjectsMissingError, match="INSERT"):
        asyncio.run(store.initialize())


def test_missing_rebase_sequence_usage_fails_initialize(pg_dsn):
    _ensure_tables(pg_dsn)
    role = "round4_rebase_no_seq"
    with psycopg.connect(**pg_dsn, autocommit=True) as conn:
        conn.execute(f"DROP ROLE IF EXISTS {role}")
        conn.execute(f"CREATE ROLE {role} LOGIN")
        conn.execute(f"GRANT USAGE ON SCHEMA {COORDINATION_SCHEMA} TO {role}")
        conn.execute(
            f"GRANT SELECT, INSERT ON TABLE {ROUND4_REBASE_TABLE} TO {role}"
        )
        conn.execute(
            f"GRANT SELECT, INSERT ON TABLE {PIPELINE_POWER_TABLE} TO {role}"
        )
        conn.execute(
            f"GRANT USAGE ON SEQUENCE {COORDINATION_SCHEMA}.{PIPELINE_POWER_SEQUENCE} "
            f"TO {role}"
        )
    limited = dict(pg_dsn, user=role)
    store = DurablePipelinePowerStore(_runner(limited))
    with pytest.raises(CoordinationObjectsMissingError, match="USAGE"):
        asyncio.run(store.initialize())


def test_missing_rebase_table_initialize_names_coordination_upgrade(pg_dsn):
    _ensure_tables(pg_dsn)
    role = "round4_rebase_nocreate"
    with psycopg.connect(**pg_dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE {ROUND4_REBASE_TABLE}")
        conn.execute(f"DROP ROLE IF EXISTS {role}")
        conn.execute(f"CREATE ROLE {role} LOGIN")
        conn.execute(f"GRANT USAGE ON SCHEMA {COORDINATION_SCHEMA} TO {role}")
        conn.execute(
            f"GRANT SELECT, INSERT ON TABLE {PIPELINE_POWER_TABLE} TO {role}"
        )
        conn.execute(
            f"GRANT USAGE ON SEQUENCE {COORDINATION_SCHEMA}.{PIPELINE_POWER_SEQUENCE} "
            f"TO {role}"
        )
    store = DurablePipelinePowerStore(_runner(dict(pg_dsn, user=role)))
    with pytest.raises(CoordinationObjectsMissingError) as caught:
        asyncio.run(store.initialize())
    message = str(caught.value)
    assert "round4_source_rebase" in message
    assert "coordination-upgrade" in message
    assert "resets database lanes" in message
    assert "cursor.ddl" not in message
    assert "'antidemo renew' provisions" not in message

