"""Round 5 remediation (B): prove the bout-receipt least-privilege design against
a REAL PostgreSQL.

The deployed app connects as a limited coordination login. It must be able to
record declarations (INSERT), overlay cleanup state, and read the history back --
without holding table-wide UPDATE, which would let it rewrite the immutable
declaration/terminal rows. sql/round5_receipt_least_privilege.sql confines the one
mutable write to a SECURITY DEFINER function owned by the privileged owner and
grants the app role only USAGE / SELECT / INSERT / EXECUTE.

This spins up an ephemeral local Postgres (like test_round5_journal_schema_pg.py),
applies the migration exactly as the privileged setup path would, then runs the
scenario AS THE APP LOGIN inside a rolled-back transaction and verifies that the
historical immutable fields cannot be rewritten.
"""

from __future__ import annotations

import json
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from server.receipts import DurableReceiptStore, derive_receipt
from tests.test_receipts import failed_then_verified_round_one_bouts

pytestmark = pytest.mark.skipif(
    shutil.which("initdb") is None or shutil.which("pg_ctl") is None,
    reason="requires a local PostgreSQL (initdb/pg_ctl on PATH)",
)

MIGRATION = (
    pathlib.Path(__file__).resolve().parents[1] / "sql" / "round5_receipt_least_privilege.sql"
)
APP_ROLE = "r5_coord_app"
TABLE = "anti_demo_coordination.bout_receipt"
FUNC_SIG = (
    "anti_demo_coordination.bout_receipt_cleanup_upsert_v1"
    "(text,text,text,text,text,timestamptz,jsonb)"
)
FUNC_CALL = (
    "SELECT anti_demo_coordination.bout_receipt_cleanup_upsert_v1"
    "(%s,%s,%s,%s,%s,%s,%s::jsonb)"
)
INSERT_DECL = f"""
    INSERT INTO {TABLE}
        (session_id, round_id, sealing_event, receipt, run_id, outcome, sealed_at, document)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (session_id, round_id, sealing_event) DO NOTHING
"""


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


@pytest.fixture(scope="module")
def pg():
    tmp = Path(tempfile.mkdtemp(prefix="r5pg-recpt-"))
    data = tmp / "data"
    subprocess.run(
        ["initdb", "-D", str(data), "-U", "postgres", "--auth=trust", "--no-sync"],
        check=True, capture_output=True,
    )
    port = _free_port()
    subprocess.run(
        ["pg_ctl", "-D", str(data), "-l", str(tmp / "log"), "-o",
         f"-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories={tmp}",
         "-w", "start"],
        check=True, capture_output=True,
    )
    owner = {"host": "127.0.0.1", "port": port, "dbname": "postgres", "user": "postgres"}
    deadline = time.monotonic() + 20
    while True:
        try:
            with psycopg.connect(**owner, connect_timeout=3) as conn:
                conn.execute("SELECT 1")
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)
    try:
        # Privileged setup path: create the app login, then apply the migration
        # exactly as the owner would (psql :"app_role" -> a quoted identifier).
        with psycopg.connect(**owner, autocommit=True) as conn:
            conn.execute(f'CREATE ROLE {APP_ROLE} LOGIN')
            migration = MIGRATION.read_text().replace(':"app_role"', f'"{APP_ROLE}"')
            conn.execute(migration)
        app = dict(owner, user=APP_ROLE)
        yield owner, app
    finally:
        subprocess.run(["pg_ctl", "-D", str(data), "-m", "immediate", "stop"], capture_output=True)
        shutil.rmtree(tmp, ignore_errors=True)


def _doc(cleanup_failure=None) -> str:
    return json.dumps({"receipt": {"cleanup_failure": cleanup_failure}}, sort_keys=True)


def test_app_role_has_select_insert_execute_but_not_update(pg):
    owner, _ = pg
    with psycopg.connect(**owner) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT has_table_privilege('{APP_ROLE}', '{TABLE}', 'SELECT')")
        assert cur.fetchone()[0] is True
        cur.execute(f"SELECT has_table_privilege('{APP_ROLE}', '{TABLE}', 'INSERT')")
        assert cur.fetchone()[0] is True
        cur.execute(f"SELECT has_table_privilege('{APP_ROLE}', '{TABLE}', 'UPDATE')")
        assert cur.fetchone()[0] is False, "app role must NOT hold table-wide UPDATE"
        cur.execute(f"SELECT has_function_privilege('{APP_ROLE}', '{FUNC_SIG}', 'EXECUTE')")
        assert cur.fetchone()[0] is True


def test_app_credential_rolled_back_transaction_preserves_immutable_declaration(pg):
    _, app = pg
    now = datetime.now(UTC)
    sess, rnd = "sess-immutable", "survive_connection_spike"
    decl_doc = _doc()
    with psycopg.connect(**app) as conn:
        conn.autocommit = False
        cur = conn.cursor()

        # (a) durable declaration -- the immutable record.
        cur.execute(
            INSERT_DECL,
            (sess, rnd, "declared", "receipt-A", "run-1", "verified", now, decl_doc),
        )
        assert cur.rowcount == 1
        # (b) repeat is a no-op (append-only DO NOTHING), original untouched.
        cur.execute(
            INSERT_DECL,
            (sess, rnd, "declared", "receipt-B", "run-1", "verified", now, _doc("x")),
        )
        assert cur.rowcount == 0

        # (c) cleanup overlay written ONLY through the definer function.
        cur.execute(
            FUNC_CALL,
            (sess, rnd, "receipt-cleanup", "run-1", "cleaning", now, _doc("still-retrying")),
        )
        assert cur.fetchone()[0] is True
        # a superseding cleanup with a changed failure moves it forward...
        cur.execute(
            FUNC_CALL,
            (
                sess, rnd, "receipt-cleanup", "run-1", "cleaned",
                now + timedelta(seconds=1), _doc(None),
            ),
        )
        assert cur.fetchone()[0] is True
        # ...but an older sealed_at is refused by the guard (no backward move).
        cur.execute(
            FUNC_CALL,
            (
                sess, rnd, "receipt-STALE", "run-1", "cleaned",
                now - timedelta(hours=1), _doc("regressed"),
            ),
        )
        assert cur.fetchone()[0] is False

        # (d) readback: exactly the immutable declaration + one cleanup overlay.
        cur.execute(
            f"SELECT sealing_event, receipt FROM {TABLE} "
            "WHERE session_id=%s ORDER BY sealing_event",
            (sess,),
        )
        rows = dict(cur.fetchall())
        assert rows == {"declared": "receipt-A", "cleanup_update": "receipt-cleanup"}

        # (e) the attack: a direct UPDATE of the immutable declaration must be
        #     refused by Postgres (no UPDATE privilege). Use a savepoint so the
        #     denial does not abort the surrounding transaction's readback.
        cur.execute("SAVEPOINT atk")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cur.execute(
                f"UPDATE {TABLE} SET receipt='HACKED' "
                "WHERE session_id=%s AND sealing_event='declared'",
                (sess,),
            )
        cur.execute("ROLLBACK TO SAVEPOINT atk")

        # historical immutable fields are exactly as first written.
        cur.execute(
            f"SELECT receipt, run_id, outcome, document FROM {TABLE} "
            "WHERE session_id=%s AND sealing_event='declared'",
            (sess,),
        )
        receipt, run_id, outcome, document = cur.fetchone()
        assert (receipt, run_id, outcome) == ("receipt-A", "run-1", "verified")
        assert document == json.loads(decl_doc)

        conn.rollback()  # the whole scenario is rolled back; nothing persists.

    # nothing leaked past the rolled-back transaction.
    with psycopg.connect(**app) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT count(*) FROM {TABLE} WHERE session_id=%s", (sess,))
        assert cur.fetchone()[0] == 0


def test_definer_function_cannot_touch_a_declaration_row(pg):
    # The function fixes sealing_event='cleanup_update', so even though it runs as
    # the privileged owner it can only ever write the overlay row -- a declaration
    # (different sealing_event => different PK) is unreachable through it.
    _, app = pg
    now = datetime.now(UTC)
    sess, rnd = "sess-func-scope", "survive_connection_spike"
    with psycopg.connect(**app) as conn:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(
            INSERT_DECL,
            (sess, rnd, "declared", "keep-me", "run-1", "verified", now, _doc()),
        )
        cur.execute(FUNC_CALL, (sess, rnd, "overlay", "run-1", "cleaned", now, _doc("f")))
        cur.execute(
            f"SELECT sealing_event FROM {TABLE} WHERE session_id=%s ORDER BY sealing_event",
            (sess,),
        )
        assert [r[0] for r in cur.fetchall()] == ["cleanup_update", "declared"]
        cur.execute(
            f"SELECT receipt FROM {TABLE} WHERE session_id=%s AND sealing_event='declared'",
            (sess,),
        )
        assert cur.fetchone()[0] == "keep-me"  # untouched by the cleanup upsert
        conn.rollback()


async def test_app_role_keeps_a_rearmed_bouts_declaration_over_its_failed_first_bout(pg):
    """A re-armed card's verified bout survives the failed bout before it, without UPDATE.

    A FAILED card in any round but Round 5 can be armed again, so one session seals
    `run_finished` twice. As the app login, both rows have to land and the store's own
    read has to answer with the bout that verified; repeating either write is a no-op.
    """

    failed, verified = await failed_then_verified_round_one_bouts()
    stopped_short = derive_receipt(failed, "run_finished")
    declared = derive_receipt(verified, "run_finished")
    _, app = pg
    async with await psycopg.AsyncConnection.connect(**app) as conn:

        async def run(operation):
            async with conn.cursor() as cursor:
                return await operation(cursor)

        store = DurableReceiptStore(run)
        for receipt, snapshot in ((stopped_short, failed), (declared, verified)) * 2:
            await store.append(receipt, snapshot)

        found = await store.load()
        assert [(item.session_id, item.outcome) for item in found] == [(verified.id, "declared")]
        assert found[0].sealed_at == declared.sealed_at
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT outcome FROM {TABLE} WHERE session_id=%s ORDER BY outcome",
                (verified.id,),
            )
            assert [row[0] for row in await cur.fetchall()] == ["declared", "stopped_short"]
        await conn.rollback()


async def test_app_role_keeps_each_pre_arm_attempt_of_one_card(pg):
    """Two attempts cancelled before ARMED have no `armed_at`, and both rows still land."""

    failed, _ = await failed_then_verified_round_one_bouts()
    failed.armed_at = None
    failed.run_started_at = None
    first = failed.model_copy(deep=True, update={"receipt_bout_id": "attempt-one"})
    second = failed.model_copy(
        deep=True,
        update={
            "receipt_bout_id": "attempt-two",
            "updated_at": failed.updated_at + timedelta(minutes=1),
        },
    )
    _, app = pg
    async with await psycopg.AsyncConnection.connect(**app) as conn:

        async def run(operation):
            async with conn.cursor() as cursor:
                return await operation(cursor)

        store = DurableReceiptStore(run)
        for snapshot in (first, second) * 2:
            await store.append(derive_receipt(snapshot, "session_cancelled"), snapshot)

        found = await store.load()
        assert [(item.session_id, item.sealed_at) for item in found] == [
            (second.id, second.updated_at)
        ]
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM {TABLE} WHERE session_id=%s", (second.id,)
            )
            assert (await cur.fetchone())[0] == 2
        await conn.rollback()


async def test_app_role_keeps_an_accepted_run_over_a_later_no_bell_cancel(pg):
    """Execute run-over-cancel receipt ranking in PostgreSQL, not a fake cursor."""

    failed, _ = await failed_then_verified_round_one_bouts()
    failed.receipt_bout_id = "accepted-run"
    failed.run_started_at = failed.updated_at - timedelta(seconds=1)
    stopped = derive_receipt(failed, "run_finished")
    cancelled = failed.model_copy(
        deep=True,
        update={
            "receipt_bout_id": "later-no-bell-cancel",
            "run_started_at": None,
            "updated_at": failed.updated_at + timedelta(minutes=1),
            "failure": "The later attempt was cancelled before the bell.",
        },
    )
    no_bell = derive_receipt(cancelled, "session_cancelled")
    _, app = pg
    async with await psycopg.AsyncConnection.connect(**app) as conn:

        async def run(operation):
            async with conn.cursor() as cursor:
                return await operation(cursor)

        store = DurableReceiptStore(run)
        await store.append(stopped, failed)
        await store.append(no_bell, cancelled)

        found = await store.load()
        assert [(item.sealing_event, item.sealed_at) for item in found] == [
            ("run_finished", stopped.sealed_at)
        ]
        await conn.rollback()


@pytest.mark.parametrize("terminal", ["towel_finished", "session_failed"])
async def test_app_role_keeps_a_towel_or_a_failure_over_a_later_no_bell_cancel(pg, terminal):
    """In PostgreSQL, a no-bell cancel hides no terminal outcome, as it hides no run (v1.2.1)."""

    failed, _ = await failed_then_verified_round_one_bouts()
    failed.receipt_bout_id = "accepted-bout"
    failed.run_started_at = failed.updated_at - timedelta(seconds=1)
    ended = derive_receipt(failed, terminal)
    cancelled = failed.model_copy(
        deep=True,
        update={
            "receipt_bout_id": "later-no-bell-cancel",
            "run_started_at": None,
            "updated_at": failed.updated_at + timedelta(minutes=1),
            "failure": "The later attempt was cancelled before the bell.",
        },
    )
    no_bell = derive_receipt(cancelled, "session_cancelled")
    _, app = pg
    async with await psycopg.AsyncConnection.connect(**app) as conn:

        async def run(operation):
            async with conn.cursor() as cursor:
                return await operation(cursor)

        store = DurableReceiptStore(run)
        await store.append(ended, failed)
        await store.append(no_bell, cancelled)

        found = await store.load()
        assert [(item.sealing_event, item.sealed_at) for item in found] == [
            (terminal, ended.sealed_at)
        ]
        await conn.rollback()
