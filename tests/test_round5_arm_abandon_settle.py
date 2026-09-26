"""Abandoned ARM must settle the staged resident, or the next warm latches BLOCKED.

ARM stages the Lakebase resident (``prepare`` -> ``transport.stage``) before the
bell. If the arm is then abandoned -- its TTL expires, the operator cancels it,
or the bell is refused -- that staged resident is left occupying the runner. The
next warm generation's ``stage_resident_generation`` -> ``wait_agent_ready`` then
observes a resident that cannot re-attest a fresh process for the new attempt,
raises "resident readiness binding changed", and the warm provider classifies
that as ``warm_baseline_unexpected`` -- a *terminal* BLOCKED slot that only a new
coordinator epoch plus a clean resident can escape. That is exactly how the live
ring reached ``round5_warm_state=blocked`` at generation 10.

``LiveConnectionSpikeEngine.settle_abandoned_arm`` closes the gap: it issues a
durable CANCEL for every binding staged at ARM and awaits SETTLED, returning the
resident to idle so the next warm attempt re-attests cleanly. These tests pin
both halves: the settle actually cancels and clears the staged bindings, and a
warm attempt that follows a settled abandon reaches AGENT_READY instead of the
binding-changed error that latches BLOCKED.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from server.connection_spike_live import LiveConnectionSpikeAdapter, LiveConnectionSpikeEngine
from server.round5_control import (
    InMemoryRound5ControlStore,
    Round5ControlBinding,
    Round5ControlDispatcher,
    Round5ControlKind,
    Round5ResidentTransport,
    Round5RunnerEvent,
    Round5RunnerEventKind,
)


def _request() -> dict[str, object]:
    """A canonical resident request whose digest matches its bytes."""

    request: dict[str, object] = {"protocol": "round5-fanin-v2"}
    request["prepared_request_digest"] = hashlib.sha256(
        json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return request


def _arm_binding(*, job_id: str, request: dict[str, object]) -> Round5ControlBinding:
    """A claim-bound binding, the shape ``prepare`` records at ARM."""

    return Round5ControlBinding(
        installation_id="installation-one",
        lane_id="lakebase",
        generation=10,
        warm_attempt_token="attempt-arm",
        claim_id="claim-one",
        bout_id="bout-one",
        bell_id="bell-one",
        fence=7,
        job_id=job_id,
        runner_boot_id="runner-boot-one",
        runner_process_boot_id="process-arm",
        runner_harness_sha256="b" * 64,
        request_sha256=str(request["prepared_request_digest"]),
    )


def _warm_binding(*, job_id: str, token: str, request: dict[str, object]) -> Round5ControlBinding:
    """The unattested, un-claimed binding ``stage_resident_generation`` waits on."""

    return Round5ControlBinding(
        installation_id="installation-one",
        lane_id="lakebase",
        generation=10,
        warm_attempt_token=token,
        claim_id=None,
        bout_id=None,
        bell_id=None,
        fence=0,
        job_id=job_id,
        runner_boot_id="runner-boot-one",
        runner_process_boot_id="unattested",
        runner_harness_sha256="b" * 64,
        request_sha256=str(request["prepared_request_digest"]),
    )


class _ResidentSim:
    """An in-memory stand-in for the resident runner's control responses.

    The one property under test: a STAGE that is never CANCELled leaves the
    resident *occupied*, and an occupied resident cannot re-attest a fresh
    process for a subsequent warm PRELOAD -- it answers AGENT_READY with an
    ``unattested`` process, which is exactly what ``wait_agent_ready`` rejects.
    A CANCEL clears the occupation so the next PRELOAD re-attests cleanly.
    """

    def __init__(self, store: InMemoryRound5ControlStore) -> None:
        self.store = store
        self.occupied = False
        self._pid = 1000
        self._seq: dict[str, int] = {}

    def _next(self, job_id: str) -> int:
        self._seq[job_id] = self._seq.get(job_id, 0) + 1
        return self._seq[job_id]

    async def __call__(self, event) -> None:
        if event.kind == Round5ControlKind.PRELOAD:
            await self._agent_ready(event.binding)
        elif event.kind == Round5ControlKind.STAGE:
            self.occupied = True
            await self._prepared(event.binding)
        elif event.kind == Round5ControlKind.CANCEL:
            self.occupied = False
            await self._settled(event.binding)

    async def _agent_ready(self, binding: Round5ControlBinding) -> None:
        self._pid += 1
        process = "unattested" if self.occupied else f"process-{self._pid}"
        attested = replace(binding, runner_process_boot_id=process)
        event_id = f"ready-{binding.job_id}-{self._seq.get(binding.job_id, 0)}".ljust(64, "0")
        await self.store.append_runner_event(
            Round5RunnerEvent(
                event_id=event_id[:64],
                binding=attested,
                sequence=self._next(binding.job_id),
                kind=Round5RunnerEventKind.AGENT_READY,
                occurred_at=datetime.now(UTC),
                payload={
                    "worker_count": 4,
                    "worker_ready_indexes": [0, 1, 2, 3],
                    "warm_attempt_token": binding.warm_attempt_token,
                    "runner_boot_id": binding.runner_boot_id,
                    "runner_process_boot_id": process,
                    "process_pid": self._pid,
                    "runner_harness_sha256": binding.runner_harness_sha256,
                },
            )
        )

    async def _prepared(self, binding: Round5ControlBinding) -> None:
        await self.store.append_runner_event(
            Round5RunnerEvent(
                event_id=f"prep-{binding.job_id}".ljust(64, "0")[:64],
                binding=binding,
                sequence=self._next(binding.job_id),
                kind=Round5RunnerEventKind.PREPARED,
                occurred_at=datetime.now(UTC),
                payload={
                    "state": "prepared",
                    "worker_ready_count": 4,
                    "request_sha256": binding.request_sha256,
                },
            )
        )

    async def _settled(self, binding: Round5ControlBinding) -> None:
        await self.store.append_runner_event(
            Round5RunnerEvent(
                event_id=f"settled-{binding.job_id}".ljust(64, "0")[:64],
                binding=binding,
                sequence=self._next(binding.job_id),
                kind=Round5RunnerEventKind.SETTLED,
                occurred_at=datetime.now(UTC),
                payload={},
            )
        )


def _transport(store: InMemoryRound5ControlStore, sim: _ResidentSim) -> Round5ResidentTransport:
    dispatcher = Round5ControlDispatcher(store, sim, sleep=lambda _delay: asyncio.sleep(0))
    return Round5ResidentTransport(store, dispatcher, sleep=lambda _delay: asyncio.sleep(0))


async def test_abandoned_arm_without_settle_latches_the_next_warm_blocked() -> None:
    """The regression: a staged-but-uncancelled arm poisons the next warm attempt.

    This is the pre-fix behaviour and the direct cause of the live BLOCKED
    generation 10 -- proven here so the settle test below is meaningful.
    """

    store = InMemoryRound5ControlStore()
    sim = _ResidentSim(store)
    transport = _transport(store, sim)
    request = _request()

    # ARM stages the resident; it is now occupied and never cancelled.
    await transport.stage(binding=_arm_binding(job_id="b" * 64, request=request), request=request)
    assert sim.occupied is True

    # The next warm generation preloads and waits for a fresh attestation.
    warm = _warm_binding(job_id="c" * 64, token="attempt-rewarm", request=request)
    not_before = datetime.now(UTC) - timedelta(seconds=1)
    await transport.preload(binding=warm, request=request)
    with pytest.raises(RuntimeError, match="readiness binding changed"):
        await asyncio.wait_for(
            transport.wait_agent_ready(warm, not_before=not_before),
            timeout=2,
        )


async def test_settled_abandoned_arm_lets_the_next_warm_reach_agent_ready() -> None:
    """The fix: once the abandoned arm is settled, the next warm attests cleanly."""

    store = InMemoryRound5ControlStore()
    sim = _ResidentSim(store)
    transport = _transport(store, sim)
    request = _request()

    arm = _arm_binding(job_id="b" * 64, request=request)
    await transport.stage(binding=arm, request=request)
    assert sim.occupied is True

    # settle_abandoned_arm delivers exactly this CANCEL for each staged binding.
    await transport.cancel(binding=arm, await_settlement=True)
    assert sim.occupied is False

    warm = _warm_binding(job_id="d" * 64, token="attempt-rewarm", request=request)
    not_before = datetime.now(UTC) - timedelta(seconds=1)
    await transport.preload(binding=warm, request=request)
    ready = await asyncio.wait_for(
        transport.wait_agent_ready(warm, not_before=not_before),
        timeout=2,
    )
    assert ready["worker_ready_indexes"] == [0, 1, 2, 3]
    assert ready["runner_process_boot_id"] not in {"", "unattested"}


def _adapter(transport: Round5ResidentTransport) -> LiveConnectionSpikeAdapter:
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._resident_transport = transport
    adapter._resident_settlement_debt = {}
    adapter._resident_release_events = {}
    return adapter


async def test_settle_abandoned_arm_cancels_and_clears_every_staged_binding() -> None:
    """Engine-level: settle_abandoned_arm cancels each lane and clears the map."""

    store = InMemoryRound5ControlStore()
    sim = _ResidentSim(store)
    transport = _transport(store, sim)
    request = _request()

    lakebase = replace(_arm_binding(job_id="b" * 64, request=request), lane_id="lakebase")
    competitor = replace(_arm_binding(job_id="e" * 64, request=request), lane_id="competitor")

    adapter = _adapter(transport)
    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._resident_bindings = {"lakebase": lakebase, "competitor": competitor}
    engine._active_run_ids = {}
    engine._lane_adapters = {"lakebase": adapter, "competitor": adapter}

    await asyncio.wait_for(engine.settle_abandoned_arm(), timeout=2)

    # Both staged residents are cancelled and forgotten.
    assert engine._resident_bindings == {}
    assert adapter._resident_settlement_debt == {}
    cancels = {
        event.job_id
        for event, _published in store.outbox.values()
        if event.kind == Round5ControlKind.CANCEL
    }
    assert cancels == {"b" * 64, "e" * 64}

    # Idempotent: a second settle is a clean no-op.
    await asyncio.wait_for(engine.settle_abandoned_arm(), timeout=2)
    assert engine._resident_bindings == {}
