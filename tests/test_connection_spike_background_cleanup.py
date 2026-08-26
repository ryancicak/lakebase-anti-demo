from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import server.connection_spike_live as live
from server.connection_spike_journal import CreationScope, ResourceObservation
from server.connection_spike_live import (
    ConnectionSpikeCleanupError,
    LiveConnectionSpikeEngine,
    LiveConnectionSpikeSetupOrchestrator,
)


@pytest.mark.asyncio
async def test_engine_returns_verified_result_before_starting_slow_cleanup(monkeypatch) -> None:
    expected = object()

    class Adapter:
        config = SimpleNamespace(
            targets=(
                SimpleNamespace(lane_id="lakebase"),
                SimpleNamespace(lane_id="competitor"),
            )
        )

        async def execute(self, run_id, schedule, *, targets=None):
            del run_id, schedule, targets
            return {}

    orchestrator = SimpleNamespace()
    engine = LiveConnectionSpikeEngine(Adapter(), setup_orchestrator=orchestrator)
    engine._setup_result = SimpleNamespace(bout_id="bout-proof")
    engine._runtime_targets = lambda: None
    arm = await engine.check()
    monkeypatch.setattr(live, "_finalize_raw_result", lambda supplied_arm, raw: expected)

    assert await engine.run(arm) is expected
    assert engine._setup_result.bout_id == "bout-proof"


@pytest.mark.asyncio
async def test_stop_starts_cleanup_once_and_exposes_two_wait_boundaries() -> None:
    calls: list[tuple[str, str]] = []

    class Adapter:
        async def cancel(self, run_id):
            calls.append(("cancel", run_id))

    class Orchestrator:
        accepted = False

        async def begin_cleanup(self, bout_id):
            calls.append(("begin", bout_id))

        def proxy_delete_accepted(self, bout_id):
            calls.append(("accepted", bout_id))
            return self.accepted

        async def wait_for_proxy_delete_accepted(self, bout_id):
            calls.append(("wait-accepted", bout_id))

        async def wait_for_cleanup_complete(self, bout_id):
            calls.append(("wait-complete", bout_id))

    orchestrator = Orchestrator()
    engine = LiveConnectionSpikeEngine(Adapter(), setup_orchestrator=orchestrator)
    arm = object()
    engine._armed = arm
    engine._active_run_id = "runner-one"
    engine._setup_result = SimpleNamespace(bout_id="bout-one")

    await asyncio.gather(
        engine.stop_and_begin_cleanup(arm),
        engine.stop_and_begin_cleanup(arm),
    )
    assert calls == [("cancel", "runner-one"), ("begin", "bout-one")]
    assert engine._setup_result is None
    assert engine.proxy_delete_accepted() is False
    await engine.wait_for_proxy_delete_accepted()
    await engine.wait_for_cleanup_complete()
    assert calls[-3:] == [
        ("accepted", "bout-one"),
        ("wait-accepted", "bout-one"),
        ("wait-complete", "bout-one"),
    ]


@pytest.mark.asyncio
async def test_proxy_delete_acceptance_precedes_exact_absence() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._proxy_delete_accepted = {}
    orchestrator._sleep = asyncio.sleep
    orchestrator.config = SimpleNamespace(poll_interval_seconds=0.001)
    delete_returned = asyncio.Event()
    allow_absence = asyncio.Event()

    async def call(operation, **kwargs):
        del operation
        assert kwargs == {"DBProxyName": "round5-proxy"}
        delete_returned.set()
        return {}

    async def inspect(*args, **kwargs):
        del args, kwargs
        await allow_absence.wait()
        return None

    orchestrator._call = call
    orchestrator._inspect_proxy = inspect
    clients = SimpleNamespace(rds=SimpleNamespace(delete_db_proxy=object()))
    observed = ResourceObservation(
        resource_kind="rds_proxy",
        provider_id="arn:aws:rds:us-west-2:123456789012:db-proxy:prx-1",
        deterministic_name="round5-proxy",
    )

    cleanup = asyncio.create_task(
        orchestrator._delete_proxy(clients, observed, bout_id="bout-proxy")
    )
    await delete_returned.wait()
    await asyncio.sleep(0)
    assert orchestrator.proxy_delete_accepted("bout-proxy") is True
    assert cleanup.done() is False

    allow_absence.set()
    await cleanup


@pytest.mark.asyncio
async def test_cancelled_cleanup_starter_does_not_cancel_exact_cleanup() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._cleanup_start_lock = asyncio.Lock()
    orchestrator._cleanup_tasks = {}
    orchestrator._proxy_delete_accepted = {}
    orchestrator._coordinators = {}
    orchestrator._scopes = {}
    settled = asyncio.Event()
    release = asyncio.Event()

    async def settle(_bout_id):
        settled.set()
        await release.wait()

    orchestrator._settle_commands = settle
    starter = asyncio.create_task(orchestrator.begin_cleanup("bout-cancelled-caller"))
    await settled.wait()
    starter.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await starter

    for _ in range(10):
        if "bout-cancelled-caller" in orchestrator._cleanup_tasks:
            break
        await asyncio.sleep(0)
    await orchestrator.wait_for_cleanup_complete("bout-cancelled-caller")
    assert orchestrator.proxy_delete_accepted("bout-cancelled-caller") is True


@pytest.mark.asyncio
async def test_fresh_bout_guard_is_read_only_until_old_journals_are_empty() -> None:
    class Journal:
        unresolved = ("old-bout",)

        async def unresolved_bout_ids(self):
            return self.unresolved

    class Fence:
        authorities: list[CreationScope] = []

        async def assert_current(self, authority):
            self.authorities.append(authority)

    journal = Journal()
    fence = Fence()
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(baseline_sha256="a" * 64)
    orchestrator._journal = journal
    orchestrator._fence = fence
    orchestrator._lock = asyncio.Lock()

    with pytest.raises(ConnectionSpikeCleanupError, match="prior cleanup"):
        await orchestrator.assert_no_unresolved_bouts("new-bout", 17)
    assert fence.authorities == [CreationScope("new-bout", 17, "a" * 64)]
    assert journal.unresolved == ("old-bout",)

    journal.unresolved = ()
    await orchestrator.assert_no_unresolved_bouts("new-bout", 17)
    assert await orchestrator.unresolved_bout_ids() == ()
