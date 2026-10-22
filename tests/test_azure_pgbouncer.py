"""Round 5's Azure setup (server/azure_pgbouncer.py) against a fake that behaves as Phase 0 did.

The fake's rules are Azure's: the client limit does not exist while PgBouncer is off and can't be
set then; it survives an off/on cycle; the pooled port opens a few probes after the switch request
(before ARM would say so) and refuses once switched off.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import psycopg
import pytest

from server.azure_arm import ArmError
from server.azure_pgbouncer import (
    DEFAULT_MAX_CLIENT_CONN,
    ROUND5_MAX_CLIENT_CONN,
    AzurePgBouncer,
    PgBouncerSetupError,
)


class FakeAzure:
    def __init__(self, *, opens_after_probes: int = 3, survived_limit: str | None = None) -> None:
        self.enabled = False
        self.limit = survived_limit or DEFAULT_MAX_CLIENT_CONN
        self.opens_after = opens_after_probes
        self.probes_since_switch = 0
        self.writes: list[tuple[str, str]] = []

    # ARM, synchronous like the real client.
    def get_configuration(self, server: str, name: str) -> str | None:
        if name == "pgbouncer.enabled":
            return "true" if self.enabled else "false"
        return self.limit if self.enabled else None  # ConfigurationNotExists while off

    def set_configuration(self, server: str, name: str, value: str) -> None:
        if name == "pgbouncer.max_client_conn" and not self.enabled:
            raise ArmError("blocked", code="ServerParameterToCMSBlockedUpdateForDisabledPgBouncer")
        self.writes.append((name, value))
        if name == "pgbouncer.enabled":
            self.enabled = value == "true"
            self.probes_since_switch = 0
        else:
            self.limit = value

    # The pooled port.
    async def connect(self, **kwargs: Any) -> Any:
        assert kwargs["sslmode"] == "verify-full" and kwargs["port"] == 6432
        self.probes_since_switch += 1
        if not self.enabled or self.probes_since_switch <= self.opens_after:
            raise psycopg.OperationalError("connection refused")
        return FakeConnection()


class FakeConnection:
    @contextlib.asynccontextmanager
    async def transaction(self):  # type: ignore[no-untyped-def]
        yield

    async def execute(self, statement: str) -> None:
        return None

    async def close(self) -> None:
        return None


def bouncer(azure: FakeAzure, **kwargs: Any) -> AzurePgBouncer:
    async def no_sleep(_: float) -> None:
        # Yield, as asyncio.sleep does: the switch runs in a worker thread the loop must schedule.
        await asyncio.sleep(0)

    return AzurePgBouncer(azure, server="r5", host="r5.postgres.example.test",  # type: ignore[arg-type]
                          database="postgres", user="antidemo_admin", password=lambda: "pw",
                          ca_file="/tmp/roots.pem", connector=azure.connect, sleep=no_sleep,
                          **kwargs)


async def test_the_pooled_path_is_switch_then_raise_then_a_committing_gate() -> None:
    azure = FakeAzure()
    timings = await bouncer(azure).make_pooled_path()
    assert azure.writes == [("pgbouncer.enabled", "true"),
                            ("pgbouncer.max_client_conn", ROUND5_MAX_CLIENT_CONN)]
    assert timings.max_client_conn == ROUND5_MAX_CLIENT_CONN == "10100"


async def test_the_bell_half_returns_at_once_and_stamps_its_request_in_the_calling_thread() -> None:
    # The orchestrator stages the competitor's runner between the halves, as AWS stages it once
    # CreateDBProxy returns.
    import concurrent.futures
    import threading
    import time

    azure = FakeAzure()
    switched_in: list[str] = []
    original = azure.set_configuration

    def recording(server: str, name: str, value: str) -> None:
        switched_in.append(threading.current_thread().name)
        original(server, name, value)

    azure.set_configuration = recording  # type: ignore[method-assign]
    with concurrent.futures.ThreadPoolExecutor(1, thread_name_prefix="r5-switch") as worker:
        before = time.monotonic_ns()
        start = bouncer(azure).begin_pooled_path(executor=worker)
        timings = await bouncer(azure).finish_pooled_path(start)
    (requested,) = start.requested_ns
    assert before <= requested
    assert switched_in[0].startswith("r5-switch")
    assert timings.max_client_conn == ROUND5_MAX_CLIENT_CONN


async def test_a_switch_queued_behind_a_backup_is_waited_out_on_azures_clock() -> None:
    # An on-demand backup holds Azure's parameter writes until it ends: on 2026-10-20 the bell's
    # switch waited out a 6-minute backup (the release bar's backup step), and a 300 s gate failed
    # a bout whose Azure lane was only slow. The gate waits as long as the round's setup deadline.
    from server.azure_pgbouncer import GATE_BOUND_SECONDS
    from server.connection_spike import SETUP_DEADLINE_SECONDS

    assert GATE_BOUND_SECONDS == SETUP_DEADLINE_SECONDS

    def queued_behind_a_backup(**kwargs: Any) -> AzurePgBouncer:
        now = [0.0]
        azure = FakeAzure()

        async def after_the_backup(**connect: Any) -> Any:
            if now[0] < 370.0:  # the switch applies once the backup ends
                raise psycopg.OperationalError("connection refused")
            return await azure.connect(**connect)

        async def tick(seconds: float) -> None:
            now[0] += seconds
            await asyncio.sleep(0)

        return AzurePgBouncer(azure, server="r5", host="r5.postgres.example.test",  # type: ignore[arg-type]
                              database="postgres", user="antidemo_admin", password=lambda: "pw",
                              ca_file="/tmp/roots.pem", connector=after_the_backup, sleep=tick,
                              clock=lambda: now[0], **kwargs)

    timings = await queued_behind_a_backup().make_pooled_path()
    assert timings.switch_request_to_gate_seconds >= 370.0
    assert timings.max_client_conn == ROUND5_MAX_CLIENT_CONN
    with pytest.raises(PgBouncerSetupError, match="did not answer"):
        await queued_behind_a_backup(gate_bound_seconds=300.0).make_pooled_path()


async def test_a_port_that_never_opens_is_a_named_failure_not_a_hang() -> None:
    azure = FakeAzure(opens_after_probes=10**9)
    with pytest.raises(PgBouncerSetupError, match="did not answer"):
        await bouncer(azure, gate_bound_seconds=0).make_pooled_path()


async def test_the_reset_lowers_a_surviving_limit_before_switching_off() -> None:
    azure = FakeAzure()
    await bouncer(azure).make_pooled_path()
    azure.writes.clear()
    proof = await bouncer(azure).reset_baseline()
    assert azure.writes == [("pgbouncer.max_client_conn", DEFAULT_MAX_CLIENT_CONN),
                            ("pgbouncer.enabled", "false")]
    assert proof.reset_max_client_conn and proof.pooled_port_refused
    assert proof.enabled == "false" and not proof.max_client_conn_visible
    # Switched on again later, it starts from Azure's default, not pre-staged.
    azure.set_configuration("r5", "pgbouncer.enabled", "true")
    assert azure.get_configuration("r5", "pgbouncer.max_client_conn") == DEFAULT_MAX_CLIENT_CONN


async def test_the_database_only_start_refuses_a_bouncer_left_on() -> None:
    azure = FakeAzure(opens_after_probes=0)
    azure.enabled = True
    with pytest.raises(PgBouncerSetupError, match="database-only start"):
        await bouncer(azure).prove_database_only_start()


async def test_the_database_only_start_holds_when_off_invisible_and_refusing() -> None:
    proof = await bouncer(FakeAzure()).prove_database_only_start()
    assert (proof.enabled, proof.max_client_conn_visible, proof.pooled_port_refused) == (
        "false", False, True)
