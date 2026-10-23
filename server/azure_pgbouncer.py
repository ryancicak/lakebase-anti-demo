"""Round 5's Azure lane setup: turn on Flexible Server's built-in PgBouncer, from a database-only
start, until port 6432 commits the exact transaction (design section 6.5).

The mapping is Lakebase's included pool (no timed mutation) against AWS's new RDS Proxy (three)
against Azure's built-in PgBouncer, which is **off by default**. Turning it on is Azure's setup.
Phase 0 (2026-10-18, D2ds_v5) measured what that setup is:

* **Two timed mutations, not one.** ``pgbouncer.max_client_conn`` can't be set while PgBouncer is
  off (``ServerParameterToCMSBlockedUpdateForDisabledPgBouncer``), so the switch comes first and
  the raise second. The default is 5000, and **exactly 10000 admits 9,999 clients**: something on
  Azure's side holds a slot. 10100 admitted all 10,000. That value is the competitor's documented
  range (1-50000), and the card states it.
* **The clock stops on the data plane.** 6432 committed the gate transaction 8.3-9.0 s after the
  switch request, while ARM still reported the operation in progress. The raise took 2.2-6.4 s.
  ARM's status lags by 30-60 s either way, so it decides nothing here.
* **The anti-cheat proof can't read the setting it guards.** While PgBouncer is off,
  ``max_client_conn`` does not exist, yet a value set while on survives an off/on cycle. A bout
  could start pre-staged and no read would show it. So the baseline reset lowers it to the default
  *before* switching off, and records that it did: the Azure counterpart of
  ``create_db_proxy_pre_bell``.
* **The admin console is closed** (``SHOW POOLS``: ``FATAL: not allowed``), so the observer's
  witness is ``pg_stat_activity`` on 5432.

Synchronous control-plane calls run through ``asyncio.to_thread``; the gate is async psycopg with
TLS ``verify-full`` against the sealed Azure roots, as the runners connect.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from .azure_arm import ArmClient

ENABLED = "pgbouncer.enabled"
MAX_CLIENT_CONN = "pgbouncer.max_client_conn"
#: Azure's default. The baseline reset restores it before switching off.
DEFAULT_MAX_CLIENT_CONN = "5000"
#: Admits the round's 10,000 clients plus the slot Azure holds (Phase 0: 10000 admitted 9,999).
ROUND5_MAX_CLIENT_CONN = "10100"
POOLED_PORT = 6432
#: How long the gate waits for 6432 to commit, and the reset for it to refuse: the round's own
#: setup deadline (`connection_spike.SETUP_DEADLINE_SECONDS`; AWS's Proxy builds can take eleven
#: minutes). An on-demand backup holds Azure's parameter writes until it ends: on 2026-10-20 the
#: bell's switch waited out a 6-minute backup, and the 300 s bound this replaced failed a bout whose
#: Azure lane was only slow. The wait is Azure's, on its own clock.
GATE_BOUND_SECONDS = 30 * 60.0

Connector = Callable[..., Awaitable[Any]]


class PgBouncerSetupError(RuntimeError):
    """The pooled path could not be made, or the baseline could not be proved. No host in it."""


@dataclass(frozen=True)
class PooledPathTimings:
    """What the bell's Azure setup took, each stamp from the request it measures."""

    switch_request_to_gate_seconds: float
    raise_seconds: float
    request_to_ready_seconds: float
    max_client_conn: str


@dataclass
class PooledPathStart:
    """The switch, sent: ARM's write in flight, and when it was asked for.

    The bell's two halves (`AzurePgBouncer.begin_pooled_path` and `finish_pooled_path`) are apart
    so the orchestrator can stage the competitor's runner between them, as AWS stages it once
    CreateDBProxy has returned: the resident prepares its workers while PgBouncer comes up.
    """

    started: float
    write: asyncio.Future[Any]
    #: The monotonic ns at which the switch's ARM call began, in the thread that makes it: the
    #: contract's request-boundary stamp (AWS's ``create_db_proxy_requested_ns``).
    requested_ns: list[int]


@dataclass(frozen=True)
class BaselineProof:
    enabled: str | None
    max_client_conn_visible: bool
    pooled_port_refused: bool
    reset_max_client_conn: bool


class AzurePgBouncer:
    def __init__(
        self,
        arm: ArmClient,
        *,
        server: str,
        host: str,
        database: str,
        user: str,
        password: Callable[[], str],
        ca_file: str,
        connector: Connector = psycopg.AsyncConnection.connect,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        gate_bound_seconds: float = GATE_BOUND_SECONDS,
        probe_interval_seconds: float = 0.5,
    ) -> None:
        self._arm = arm
        self._server = server
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._ca_file = ca_file
        self._connector = connector
        self._sleep = sleep
        self._clock = clock
        self._gate_bound = gate_bound_seconds
        self._probe_interval = probe_interval_seconds

    async def _pooled(self) -> Any:
        return await self._connector(
            host=self._host, port=POOLED_PORT, dbname=self._database, user=self._user,
            password=self._password(), sslmode="verify-full", sslrootcert=self._ca_file,
            connect_timeout=10, autocommit=True, application_name="lakebase-anti-demo-round-5")

    async def _commit_once(self) -> bool:
        """Client 1's gate: one TLS verify-full + SCRAM login on 6432 and one committed write."""

        try:
            connection = await self._pooled()
        except psycopg.OperationalError:
            return False
        try:
            async with connection.transaction():
                await connection.execute("select 1")
            return True
        except psycopg.Error:
            return False
        finally:
            await connection.close()

    async def _refused(self) -> bool:
        try:
            connection = await self._pooled()
        except psycopg.OperationalError:
            return True
        await connection.close()
        return False

    async def make_pooled_path(self) -> PooledPathTimings:
        """The Azure lane's timed setup after the bell. Raises PgBouncerSetupError on a timeout."""

        return await self.finish_pooled_path(self.begin_pooled_path())

    def begin_pooled_path(self, *, executor: Any = None) -> PooledPathStart:
        """Send the switch, the bell's first timed mutation, and return at once.

        `executor` is a worker made before the bell, so none is created between it and the
        request (AWS warms its CreateDBProxy worker for the same reason).
        """

        requested: list[int] = []

        def switch() -> None:
            requested.append(time.monotonic_ns())
            self._arm.set_configuration(self._server, ENABLED, "true")

        write = asyncio.get_running_loop().run_in_executor(executor, switch)
        return PooledPathStart(started=self._clock(), write=write, requested_ns=requested)

    async def finish_pooled_path(self, start: PooledPathStart) -> PooledPathTimings:
        """The gate on 6432, then the raise. Raises PgBouncerSetupError on a timeout."""

        started, switch = start.started, start.write
        gate_at: float | None = None
        while self._clock() - started < self._gate_bound:
            if await self._commit_once():
                gate_at = self._clock() - started
                break
            await self._sleep(self._probe_interval)
        await switch  # the write must have landed before the raise is accepted
        if gate_at is None:
            raise PgBouncerSetupError("Azure's PgBouncer did not answer on its pooled port")
        raised = self._clock()
        await asyncio.to_thread(self._arm.set_configuration, self._server, MAX_CLIENT_CONN,
                                ROUND5_MAX_CLIENT_CONN)
        raise_seconds = self._clock() - raised
        value = await asyncio.to_thread(self._arm.get_configuration, self._server, MAX_CLIENT_CONN)
        if value != ROUND5_MAX_CLIENT_CONN:
            raise PgBouncerSetupError("Azure's PgBouncer did not take the round's client limit")
        return PooledPathTimings(switch_request_to_gate_seconds=gate_at,
                                 raise_seconds=raise_seconds,
                                 request_to_ready_seconds=self._clock() - started,
                                 max_client_conn=value)

    async def reset_baseline(self) -> BaselineProof:
        """Back to the database-only start: default limit first, then off, then 6432 refuses."""

        enabled = await asyncio.to_thread(self._arm.get_configuration, self._server, ENABLED)
        reset = False
        if str(enabled).lower() == "true":
            current = await asyncio.to_thread(self._arm.get_configuration, self._server,
                                              MAX_CLIENT_CONN)
            if current != DEFAULT_MAX_CLIENT_CONN:
                await asyncio.to_thread(self._arm.set_configuration, self._server,
                                        MAX_CLIENT_CONN, DEFAULT_MAX_CLIENT_CONN)
                reset = True
            await asyncio.to_thread(self._arm.set_configuration, self._server, ENABLED, "false")
        started = self._clock()
        while not await self._refused():
            if self._clock() - started > self._gate_bound:
                raise PgBouncerSetupError("Azure's PgBouncer still answers after switching off")
            await self._sleep(self._probe_interval)
        proof = await self.prove_database_only_start()
        return BaselineProof(enabled=proof.enabled,
                             max_client_conn_visible=proof.max_client_conn_visible,
                             pooled_port_refused=proof.pooled_port_refused,
                             reset_max_client_conn=reset)

    async def prove_database_only_start(self) -> BaselineProof:
        """Before the bell: PgBouncer off, its limit invisible, and 6432 refusing."""

        enabled = await asyncio.to_thread(self._arm.get_configuration, self._server, ENABLED)
        visible = await asyncio.to_thread(self._arm.get_configuration, self._server,
                                          MAX_CLIENT_CONN) is not None
        refused = await self._refused()
        if str(enabled).lower() != "false" or visible or not refused:
            raise PgBouncerSetupError("Azure's PgBouncer is not at the database-only start")
        return BaselineProof(enabled=enabled, max_client_conn_visible=visible,
                             pooled_port_refused=refused, reset_max_client_conn=False)
