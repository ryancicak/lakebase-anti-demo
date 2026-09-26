"""Round 5 timed CreateDBProxy dispatch honesty (bell/proxy defect #3).

The single scored mutation of the competitor lane -- CreateDBProxy -- must be
issued on a DEDICATED worker whose request-boundary timestamp is taken inside the
worker immediately before the SDK call.  The previous code stamped the boundary on
the event loop and then submitted the call through ``asyncio.to_thread`` (the
shared default ``ThreadPoolExecutor``); under saturation that both hid the
scheduling delay and delayed the dispatch itself.
"""

from __future__ import annotations

import asyncio
import itertools
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from server.connection_spike_live import LiveConnectionSpikeSetupOrchestrator


class _RecordingRds:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_db_proxy(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        return {"DBProxies": [{"DBProxyName": kwargs.get("DBProxyName")}]}


_EXECUTORS: list[ThreadPoolExecutor] = []


@pytest.fixture(autouse=True)
def _shutdown_reused_executors():
    yield
    while _EXECUTORS:
        _EXECUTORS.pop().shutdown(wait=False)


def _fake_self() -> SimpleNamespace:
    counter = itertools.count(1_000)
    ns = SimpleNamespace(_monotonic_ns=lambda: next(counter), _createproxy_executor=None)

    def _ensure() -> ThreadPoolExecutor:
        # Req #7: the orchestrator REUSES one worker (warmed before the scored
        # window), never creating a ThreadPoolExecutor inside it.
        executor = LiveConnectionSpikeSetupOrchestrator._ensure_createproxy_executor(ns)
        if executor not in _EXECUTORS:
            _EXECUTORS.append(executor)
        return executor

    ns._ensure_createproxy_executor = _ensure
    return ns


async def test_dispatch_reuses_one_worker_across_calls() -> None:
    # Req #7: two dispatches must share the same reused executor (no per-call
    # ThreadPoolExecutor creation inside the scored window).
    resources = SimpleNamespace(proxy_create_requested_ns=None)
    clients = SimpleNamespace(rds=_RecordingRds())
    fake = _fake_self()
    await LiveConnectionSpikeSetupOrchestrator._dispatch_create_db_proxy(
        fake, clients, resources, {"DBProxyName": "p1"}
    )
    first = fake._createproxy_executor
    await LiveConnectionSpikeSetupOrchestrator._dispatch_create_db_proxy(
        fake, clients, resources, {"DBProxyName": "p2"}
    )
    assert fake._createproxy_executor is first is not None


async def test_request_ns_is_stamped_inside_the_worker() -> None:
    resources = SimpleNamespace(proxy_create_requested_ns=None)
    clients = SimpleNamespace(rds=_RecordingRds())
    fake = _fake_self()

    result = await LiveConnectionSpikeSetupOrchestrator._dispatch_create_db_proxy(
        fake, clients, resources, {"DBProxyName": "anti-demo-bout-xyz"}
    )

    # The SDK call ran and the request boundary was stamped (inside the worker).
    assert result["DBProxies"][0]["DBProxyName"] == "anti-demo-bout-xyz"
    assert resources.proxy_create_requested_ns is not None
    assert clients.rds.calls == [{"DBProxyName": "anti-demo-bout-xyz"}]


async def test_dedicated_worker_dispatches_while_default_executor_is_saturated() -> None:
    loop = asyncio.get_running_loop()
    # Give the loop a tiny default executor and fill it, so that anything routed
    # through asyncio.to_thread() would block behind `release`.
    saturating = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(saturating)
    release = threading.Event()
    blockers = [
        asyncio.ensure_future(loop.run_in_executor(None, release.wait)) for _ in range(3)
    ]
    await asyncio.sleep(0.01)  # let the single worker pick up one blocker

    resources = SimpleNamespace(proxy_create_requested_ns=None)
    clients = SimpleNamespace(rds=_RecordingRds())
    fake = _fake_self()

    try:
        # The dedicated executor must let this complete promptly even though the
        # default executor is fully occupied and its queue is backed up.
        result = await asyncio.wait_for(
            LiveConnectionSpikeSetupOrchestrator._dispatch_create_db_proxy(
                fake, clients, resources, {"DBProxyName": "p"}
            ),
            timeout=2.0,
        )
        assert result["DBProxies"][0]["DBProxyName"] == "p"
        assert resources.proxy_create_requested_ns is not None
    finally:
        release.set()
        await asyncio.gather(*blockers, return_exceptions=True)
        saturating.shutdown(wait=True)


async def test_worker_cancellation_still_awaits_the_shielded_sdk_call() -> None:
    # A cancellation between submit and completion must not leave the SDK call
    # dangling: the shield re-awaits the worker before propagating cancellation.
    started = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    class _SlowRds:
        def create_db_proxy(self, **kwargs: object) -> dict[str, object]:
            started.set()
            release.wait(timeout=5)
            completed.set()
            return {"DBProxies": [{"DBProxyName": kwargs.get("DBProxyName")}]}

    resources = SimpleNamespace(proxy_create_requested_ns=None)
    clients = SimpleNamespace(rds=_SlowRds())
    fake = _fake_self()

    task = asyncio.ensure_future(
        LiveConnectionSpikeSetupOrchestrator._dispatch_create_db_proxy(
            fake, clients, resources, {"DBProxyName": "p"}
        )
    )
    await asyncio.to_thread(started.wait, 2.0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    # The shielded worker was awaited to completion rather than abandoned.
    assert completed.is_set()
