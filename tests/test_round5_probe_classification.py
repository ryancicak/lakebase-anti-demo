"""Probe and warm errors that are transient take strikes, not rewarms.

The engine's own STALE/ABSENT ``RetryableWarmError`` used to become ``False`` (an
attested identity change) inside ``validate_ready``, and so did a probe read whose
``psycopg.OperationalError`` survived ``_run``'s retry, so READY demoted to WARMING on
the first miss. During a warm, the same error was classified ``warm_baseline_unexpected``.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import psycopg
import pytest
from databricks.sdk import errors as databricks_errors

from server import round5_warm
from server.connection_spike_live import LiveConnectionSpikeEngine, LiveRound5WarmProvider
from server.round5_control import (
    InMemoryRound5ControlStore,
    Round5ControlBinding,
    Round5ResidentTransport,
    Round5RunnerEvent,
    Round5RunnerEventKind,
)
from server.round5_warm import (
    MAX_PROVENANCE_PROBE_FAILURES,
    BlockedWarmError,
    RetryableWarmError,
    Round5WarmState,
)
from tests.test_round5_warm import DIGEST, Clock, Provider, coordinator, warm_ready

INSTALLATION = "install-one"
LANES = ("lakebase", "competitor")
PIDS = {"lakebase": 101, "competitor": 102}


class _ProbeEngine:
    def __init__(self, exc: BaseException | None = None, result: bool = True) -> None:
        self._exc = exc
        self._result = result
        self._armed = object()

    async def validate_ready_provenance(self, shared_receipt, warm_attempt_token) -> bool:
        if self._exc is not None:
            raise self._exc
        return self._result


def _provider(engine: _ProbeEngine) -> LiveRound5WarmProvider:
    provider = LiveRound5WarmProvider(
        manifest=cast(Any, object()),
        engine_factory=lambda _competitor: cast(Any, engine),
    )
    provider._engines = {"aurora_serverless_v2": cast(Any, engine)}
    return provider


_SLOT = SimpleNamespace(shared_receipt=object(), warm_attempt_token="attempt")


@pytest.mark.parametrize(
    "code", ["runner_attestation_stale", "runner_attestation_transport_absent"]
)
async def test_a_stale_or_absent_probe_is_a_strike_not_an_identity_change(code: str) -> None:
    provider = _provider(_ProbeEngine(RetryableWarmError(code)))
    with pytest.raises(RetryableWarmError) as raised:
        await provider.validate_ready(_SLOT, None)
    assert raised.value.code == code


async def test_a_surviving_operational_error_on_the_probe_is_a_strike() -> None:
    provider = _provider(_ProbeEngine(psycopg.OperationalError("connection refused")))
    with pytest.raises(RetryableWarmError) as raised:
        await provider.validate_ready(_SLOT, None)
    assert raised.value.code == "runner_provenance_probe_retryable"


async def test_an_attested_identity_change_still_returns_false() -> None:
    provider = _provider(_ProbeEngine(result=False))
    assert await provider.validate_ready(_SLOT, None) is False


async def test_an_unexpected_probe_error_still_returns_false() -> None:
    provider = _provider(_ProbeEngine(ValueError("unexpected")))
    assert await provider.validate_ready(_SLOT, None) is False


class _WarmEngine:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def warm(self, generation: int, warm_attempt_token: str) -> object:
        raise self._exc


async def _prepare_with(exc: BaseException) -> object:
    provider = LiveRound5WarmProvider(
        manifest=cast(Any, object()),
        engine_factory=lambda _competitor: cast(Any, _WarmEngine(exc)),
    )
    return await provider.prepare(
        generation=8,
        coordinator_fence=1,
        process_epoch="proc",
        broker_epoch="broker",
        warm_attempt_token="attempt",
        requires_cleaned_bout=False,
    )


async def test_a_surviving_operational_error_during_a_warm_is_retryable() -> None:
    with pytest.raises(RetryableWarmError) as raised:
        await _prepare_with(psycopg.OperationalError("server closed the connection"))
    assert raised.value.code == "warm_provider_retryable"


async def test_query_canceled_counts_as_the_operational_family() -> None:
    with pytest.raises(RetryableWarmError):
        await _prepare_with(psycopg.errors.QueryCanceled("canceling statement"))


async def test_an_unexpected_warm_error_still_fails_closed() -> None:
    with pytest.raises(BlockedWarmError) as raised:
        await _prepare_with(ValueError("unexpected"))
    assert raised.value.code == "warm_baseline_unexpected"


# The coordination store's credential mint raises these when the workspace is briefly
# unavailable; the store itself retries exactly this set.
_TRANSIENT_DATABRICKS = [
    pytest.param(databricks_errors.InternalError("upstream request failed"), id="internal"),
    pytest.param(
        databricks_errors.TemporarilyUnavailable("try again later"), id="temporarily-unavailable"
    ),
    pytest.param(databricks_errors.DeadlineExceeded("deadline exceeded"), id="deadline"),
    pytest.param(databricks_errors.ResourceExhausted("quota"), id="resource-exhausted"),
]
# A denial or a malformed request is an answer, not an outage.
_DATABRICKS_ANSWERS = [
    pytest.param(databricks_errors.PermissionDenied("no grant"), id="permission-denied"),
    pytest.param(databricks_errors.Unauthenticated("expired principal"), id="unauthenticated"),
    pytest.param(databricks_errors.BadRequest("bad request"), id="bad-request"),
    pytest.param(databricks_errors.NotFound("no such endpoint"), id="not-found"),
]


@pytest.mark.parametrize("error", _TRANSIENT_DATABRICKS)
async def test_a_transient_databricks_error_on_the_probe_is_a_strike(error: Exception) -> None:
    provider = _provider(_ProbeEngine(error))
    with pytest.raises(RetryableWarmError) as raised:
        await provider.validate_ready(_SLOT, None)
    assert raised.value.code == "runner_provenance_probe_retryable"


@pytest.mark.parametrize("error", _TRANSIENT_DATABRICKS)
async def test_a_transient_databricks_error_during_a_warm_is_retryable(error: Exception) -> None:
    with pytest.raises(RetryableWarmError) as raised:
        await _prepare_with(error)
    assert raised.value.code == "warm_provider_retryable"


async def test_a_wrapped_transient_databricks_error_is_retryable() -> None:
    wrapped = RuntimeError("coordination read failed")
    wrapped.__cause__ = databricks_errors.TemporarilyUnavailable("try again later")
    with pytest.raises(RetryableWarmError):
        await _prepare_with(wrapped)


@pytest.mark.parametrize("error", _DATABRICKS_ANSWERS)
async def test_a_databricks_answer_still_fails_closed(error: Exception) -> None:
    assert await _provider(_ProbeEngine(error)).validate_ready(_SLOT, None) is False
    with pytest.raises(BlockedWarmError) as raised:
        await _prepare_with(error)
    assert raised.value.code == "warm_baseline_unexpected"


# ---------------------------------------------------------------------------
# A READY coordinator probing through the live provider and a real engine
# ---------------------------------------------------------------------------


class _NoopDispatcher:
    async def start(self) -> None:
        return None

    def wake(self) -> None:
        return None

    def hold_release(self, _event_id: str) -> None:
        return None

    def allow_release(self, _event_id: str) -> None:
        return None

    def discard_release(self, _event_id: str) -> None:
        return None


class _UnreachableControlStore(InMemoryRound5ControlStore):
    """The coordination read as ``_run`` leaves it once both of its attempts failed."""

    async def latest_resident_attestation(self, *_args: object, **_kwargs: object) -> None:
        raise psycopg.OperationalError("connection to server failed: Connection refused")


def _resident_engine(store: InMemoryRound5ControlStore) -> LiveConnectionSpikeEngine:
    """A real engine whose in-process runner identity matches the warmed receipts."""

    transport = Round5ResidentTransport(store, _NoopDispatcher())
    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._armed = SimpleNamespace(
        preflights={
            lane: SimpleNamespace(
                boot_id=f"boot-{lane}", runner_harness_sha256=DIGEST, model_sha256=DIGEST
            )
            for lane in LANES
        }
    )
    engine._lane_adapters = {
        lane: SimpleNamespace(
            _resident_process_boot_id=f"process-{lane}",
            _resident_process_pid=PIDS[lane],
            _resident_transport=transport,
            config=SimpleNamespace(resident_installation_id=INSTALLATION),
        )
        for lane in LANES
    }
    engine.authority_guard = None
    return engine


def _heartbeat(lane: str, token: str, *, age_seconds: float) -> Round5RunnerEvent:
    binding = Round5ControlBinding(
        installation_id=INSTALLATION,
        lane_id=lane,
        generation=1,
        warm_attempt_token=token,
        claim_id=None,
        bout_id=None,
        bell_id=None,
        fence=0,
        job_id=hashlib.sha256(f"job-{lane}-{token}".encode()).hexdigest(),
        runner_boot_id=f"boot-{lane}",
        runner_process_boot_id=f"process-{lane}",
        runner_harness_sha256=DIGEST,
        request_sha256="c" * 64,
    )
    return Round5RunnerEvent(
        event_id=hashlib.sha256(f"hb-{lane}-{token}".encode()).hexdigest(),
        binding=binding,
        sequence=2,
        kind=Round5RunnerEventKind.HEARTBEAT,
        occurred_at=datetime.now(UTC) - timedelta(seconds=age_seconds),
        payload={
            "worker_ready_indexes": [0, 1, 2, 3],
            "runner_boot_id": f"boot-{lane}",
            "runner_process_boot_id": f"process-{lane}",
            "process_pid": PIDS[lane],
            "runner_harness_sha256": DIGEST,
        },
    )


async def _ready_probing_through(control_store_for) -> tuple[Any, LiveRound5WarmProvider]:
    clock = Clock()
    fake = Provider(clock)
    manager = coordinator(clock, fake)
    await warm_ready(manager, fake)
    ready = await manager.store.read(INSTALLATION)
    assert ready is not None and ready.state == Round5WarmState.READY and manager.ring_ready
    engine = _resident_engine(await control_store_for(ready.warm_attempt_token))
    provider = LiveRound5WarmProvider(
        SimpleNamespace(), lambda _competitor: SimpleNamespace(_setup_orchestrator=None)
    )
    provider._engines = {"aurora_serverless_v2": engine, "rds_postgres": engine}
    provider.authority_guard = manager._authority_guard
    manager.provider = provider
    clock.advance(1)
    return manager, provider


async def _stale_lakebase_heartbeat(token: str) -> InMemoryRound5ControlStore:
    store = InMemoryRound5ControlStore()
    await store.append_runner_event(_heartbeat("lakebase", token, age_seconds=16))
    await store.append_runner_event(_heartbeat("competitor", token, age_seconds=1))
    return store


async def _unreachable_coordination(_token: str) -> InMemoryRound5ControlStore:
    return _UnreachableControlStore()


@pytest.mark.parametrize(
    "control_store_for",
    [_stale_lakebase_heartbeat, _unreachable_coordination],
    ids=["stale-heartbeat", "unreachable-coordination"],
)
async def test_a_transient_ready_probe_miss_keeps_the_ring_ready_on_one_strike(
    control_store_for,
) -> None:
    manager, provider = await _ready_probing_through(control_store_for)
    engines = dict(provider._engines)
    try:
        delay = await manager.run_one_cycle()
        slot = await manager.store.read(INSTALLATION)
        assert slot is not None and slot.state == Round5WarmState.READY
        assert delay > 0
        assert manager._provenance_probe_failures == 1
        assert manager._identity_reestablish_failures == 0
        assert provider._engines == engines
        assert manager.ring_ready
    finally:
        await manager.close()


def _strikes(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.getMessage().startswith("round5_ready_probe_strike")
    ]


async def test_ready_probe_strikes_are_logged_without_flooding(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    clock = Clock()
    fake = Provider(clock)
    manager = coordinator(clock, fake)
    await warm_ready(manager, fake)
    unreachable = RetryableWarmError("runner_provenance_probe_retryable")
    unreachable.__cause__ = psycopg.OperationalError("connection refused")
    fake.validate_error = unreachable
    for _ in range(MAX_PROVENANCE_PROBE_FAILURES):
        clock.advance(1)
        await manager.run_one_cycle()

    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state == Round5WarmState.WARMING
    first, last = _strikes(caplog)
    assert first.levelno == last.levelno == logging.WARNING
    assert first.exc_info is None and last.exc_info is None
    first_message = first.getMessage()
    assert "code=runner_provenance_probe_retryable" in first_message
    assert f"strikes=1/{MAX_PROVENANCE_PROBE_FAILURES}" in first_message
    assert "cause=OperationalError" in first_message
    assert "action=retry" in first_message
    last_message = last.getMessage()
    assert (
        f"strikes={MAX_PROVENANCE_PROBE_FAILURES}/{MAX_PROVENANCE_PROBE_FAILURES}"
        in last_message
    )
    assert "action=rewarm" in last_message
    assert f"suppressed={MAX_PROVENANCE_PROBE_FAILURES - 2}" in last_message
    await manager.close()


async def test_a_strike_after_a_quiet_interval_is_logged_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    clock = Clock()
    fake = Provider(clock)
    manager = coordinator(clock, fake)
    await warm_ready(manager, fake)
    stale = RetryableWarmError("runner_attestation_stale")
    fake.validate_error = stale
    clock.advance(1)
    await manager.run_one_cycle()
    fake.validate_error = None
    quiet = 0.0
    while quiet <= round5_warm.PROVENANCE_PROBE_STRIKE_LOG_INTERVAL_SECONDS:
        clock.advance(5)
        quiet += 5
        await manager.run_one_cycle()
    fake.validate_error = stale
    clock.advance(1)
    await manager.run_one_cycle()

    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state == Round5WarmState.READY
    strikes = [record.getMessage() for record in _strikes(caplog)]
    assert len(strikes) == 2
    for message in strikes:
        assert "code=runner_attestation_stale" in message
        assert f"strikes=1/{MAX_PROVENANCE_PROBE_FAILURES}" in message
        assert "cause=none" in message
        assert "suppressed=0" in message
    await manager.close()
