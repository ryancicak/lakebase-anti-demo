"""Restart-safe repayment of Round 4's inherited pipeline stop debt."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

import app as app_module
from server import pipeline_power
from server.api import _fight_card_round_status
from server.coordination import InMemoryBoutLeaseStore, round_ring_key
from server.model_score_live import PipelineSignals
from server.models import (
    BoutOperator,
    BoutStatus,
    CompetitorId,
    RoundId,
    SessionState,
)
from server.round4_stop_recovery import (
    RECOVERY_PHASE,
    InheritedRound4StopRecovery,
)

INSTALLATION_ID = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"
PIPELINE_ID = "pipeline-1"


@pytest.fixture(autouse=True)
def _reset_pipeline_power_globals():
    pipeline_power.install_pipeline_power_store(None)
    yield
    pipeline_power.install_pipeline_power_store(None)


def _manifest() -> SimpleNamespace:
    return SimpleNamespace(
        manifest_version=7,
        installation_id=INSTALLATION_ID,
        run_id="run-1",
        owner="owner@example.com",
        round4=SimpleNamespace(
            pipeline_id=PIPELINE_ID,
            synced_table_id="storage.round4.model_scores",
            app_service_principal_client_id="app-client",
        ),
        databricks=SimpleNamespace(profile="demo", user="app-client"),
    )


def _owed(due_at: datetime) -> dict[str, str]:
    return {
        "intent": "stop_owed",
        "pipeline_id": PIPELINE_ID,
        "run_id": "run-1",
        "owed_at": due_at.astimezone(UTC).isoformat(),
        "owed_by": "app-client",
    }


def _resuming(at: datetime) -> dict[str, str]:
    return {
        "intent": "resuming",
        "pipeline_id": PIPELINE_ID,
        "run_id": "run-1",
        "resumed_at": at.astimezone(UTC).isoformat(),
        "resumed_by": "app-client",
    }


HEALTHY = PipelineSignals(
    pipeline_state="RUNNING",
    update_state="RUNNING",
    synced_table_state="SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
    continuous_reported=True,
)
PROVEN_STOPPED = PipelineSignals(
    pipeline_state="IDLE",
    update_state="CANCELED",
    synced_table_state="SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
    continuous_reported=False,
)


class Clock:
    def __init__(self, now: datetime) -> None:
        self.value = now
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += timedelta(seconds=seconds)
        await asyncio.sleep(0)


class PowerStore:
    def __init__(self, *records: dict[str, str], append_failures: int = 0) -> None:
        self.records = [dict(record) for record in records]
        self.append_failures = append_failures
        self.append_attempts = 0
        self.latest_reads = 0
        self.closed = False
        self.events: list[str] = []

    async def latest(self, pipeline_id: str):
        assert not self.closed
        assert pipeline_id == PIPELINE_ID
        self.latest_reads += 1
        self.events.append("latest")
        return dict(self.records[-1]) if self.records else None

    async def append(self, record):
        assert not self.closed
        self.append_attempts += 1
        if self.append_failures:
            self.append_failures -= 1
            raise ConnectionError("coordination transport reset")
        self.records.append(dict(record))

    async def close(self) -> None:
        self.closed = True


def _round4_store(root: InMemoryBoutLeaseStore) -> InMemoryBoutLeaseStore:
    return root.for_ring_key(
        round_ring_key(INSTALLATION_ID, RoundId.PUT_MODEL_SCORE_IN_APP.value)
    )


def _recovery(
    clock: Clock,
    root: InMemoryBoutLeaseStore,
    power: PowerStore,
    inherited: dict[str, str],
    *,
    signals,
    stop_pipeline,
    sleep=None,
    retry_base_seconds: float = 0,
) -> InheritedRound4StopRecovery:
    async def read_signals() -> PipelineSignals:
        return signals() if callable(signals) else signals

    return InheritedRound4StopRecovery(
        _manifest(),
        _round4_store(root),
        power,  # type: ignore[arg-type]
        inherited,
        read_signals=read_signals,
        stop_pipeline=stop_pipeline,
        now=clock.now,
        sleep=sleep or clock.sleep,
        retry_base_seconds=retry_base_seconds,
        retry_max_seconds=max(retry_base_seconds, 0.01),
    )


async def test_overdue_inherited_debt_stops_exactly_once_and_persists_repayment() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    stop_calls = 0

    async def stop_pipeline() -> dict[str, str]:
        nonlocal stop_calls
        stop_calls += 1
        held = await _round4_store(root).current()
        assert held is not None and held.phase == RECOVERY_PHASE
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    pipeline_power.install_owed_stop_snapshot(inherited)
    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=HEALTHY,
        stop_pipeline=stop_pipeline,
    )

    await recovery.run()

    assert stop_calls == 1
    assert power.records[-1]["intent"] == "stopped"
    assert power.records[-1]["pipeline_id"] == PIPELINE_ID
    assert pipeline_power.owed_stop_notice(now=clock.now) is None
    assert recovery.status.state == "settled"
    assert await _round4_store(root).current() is None


async def test_future_debt_waits_for_the_exact_due_time_before_stopping() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    due = clock.now() + timedelta(minutes=20)
    inherited = _owed(due)
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    stop_times: list[datetime] = []

    async def stop_pipeline() -> dict[str, str]:
        stop_times.append(clock.now())
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=HEALTHY,
        stop_pipeline=stop_pipeline,
    )
    await recovery.run()

    assert clock.sleeps[0] == pytest.approx(20 * 60)
    assert stop_times == [due]
    assert power.records[-1]["intent"] == "stopped"


async def test_active_round4_bout_defers_and_its_new_redo_window_supersedes_old_debt() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    old = _owed(clock.now() - timedelta(hours=1))
    newer_due = clock.now() + timedelta(minutes=20)
    power = PowerStore(old)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    ring = _round4_store(root)
    active = await ring.claim(
        session_id="real-bout",
        operator=BoutOperator(display_name="Presenter", subject="presenter"),
        phase="running",
        session_state=SessionState.RUNNING,
        round_id=RoundId.PUT_MODEL_SCORE_IN_APP.value,
        round_title="Round 4",
        competitor_id=CompetitorId.AURORA_SERVERLESS_V2.value,
        competitor_name="Aurora",
        ttl=timedelta(minutes=10),
    )
    future_wait_started = asyncio.Event()
    hold_future_wait = asyncio.Event()
    sleeps = 0
    stop_calls = 0

    async def controlled_sleep(seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 1:
            power.records.append(_owed(newer_due))
            assert await ring.release(active) is True
            return
        assert seconds == pytest.approx(20 * 60)
        future_wait_started.set()
        await hold_future_wait.wait()

    async def stop_pipeline() -> dict[str, str]:
        nonlocal stop_calls
        stop_calls += 1
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    pipeline_power.install_owed_stop_snapshot(old)
    recovery = _recovery(
        clock,
        root,
        power,
        old,
        signals=HEALTHY,
        stop_pipeline=stop_pipeline,
        sleep=controlled_sleep,
        retry_base_seconds=0.01,
    )
    task = asyncio.create_task(recovery.run())
    await asyncio.wait_for(future_wait_started.wait(), timeout=1)

    assert stop_calls == 0
    notice = pipeline_power.owed_stop_notice(now=lambda: newer_due - timedelta(seconds=1))
    assert notice is None
    assert recovery.status.next_attempt_seconds == pytest.approx(20 * 60)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await ring.current() is None


async def test_claim_then_latest_reread_are_both_load_bearing_against_a_race() -> None:
    """Removing either the claim or its following latest read makes this stop stale."""

    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    inner = _round4_store(root)
    events: list[str] = []
    stop_calls = 0

    class RacingRing:
        mode = inner.mode
        ring_key = inner.ring_key

        async def claim(self, **kwargs):
            events.append("claim")
            # A newer arm lands after startup's stale pre-check but before the
            # recovery owns the ring. Only a latest read *after* this hook sees it.
            power.records.append(_resuming(clock.now()))
            return await inner.claim(**kwargs)

        async def release(self, lease):
            events.append("release")
            return await inner.release(lease)

    original_latest = power.latest

    async def latest(pipeline_id: str):
        events.append("latest")
        return await original_latest(pipeline_id)

    power.latest = latest  # type: ignore[method-assign]

    async def stop_pipeline() -> dict[str, str]:
        nonlocal stop_calls
        stop_calls += 1
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    recovery = InheritedRound4StopRecovery(
        _manifest(),
        RacingRing(),  # type: ignore[arg-type]
        power,  # type: ignore[arg-type]
        inherited,
        read_signals=lambda: asyncio.sleep(0, result=HEALTHY),
        stop_pipeline=stop_pipeline,
        now=clock.now,
        sleep=clock.sleep,
        retry_base_seconds=0,
        retry_max_seconds=0,
    )
    pipeline_power.install_owed_stop_snapshot(inherited)
    await recovery.run()

    # The first post-claim read sees the racing resume and returns before live
    # signals, so the immediate-pre-mutation read is intentionally not reached.
    assert events == ["claim", "latest", "release"]
    assert stop_calls == 0
    assert power.records[-1]["intent"] == "resuming"
    assert pipeline_power.owed_stop_notice(now=clock.now) is None


async def test_latest_is_rechecked_after_slow_signals_before_any_stop() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    stop_calls = 0

    async def racing_signals() -> PipelineSignals:
        # The first post-claim read has already returned. A newer intent lands
        # during the control-plane reads and must still beat stale recovery.
        power.records.append(_resuming(clock.now()))
        return HEALTHY

    async def stop_pipeline() -> dict[str, str]:
        nonlocal stop_calls
        stop_calls += 1
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    recovery = InheritedRound4StopRecovery(
        _manifest(),
        _round4_store(root),
        power,  # type: ignore[arg-type]
        inherited,
        read_signals=racing_signals,
        stop_pipeline=stop_pipeline,
        now=clock.now,
        sleep=clock.sleep,
        retry_base_seconds=0,
        retry_max_seconds=0,
    )
    await recovery.run()

    assert power.latest_reads == 2
    assert stop_calls == 0
    assert power.records[-1]["intent"] == "resuming"


async def test_two_replicas_share_the_fence_and_only_one_issues_stop() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    stop_entered = asyncio.Event()
    allow_stop = asyncio.Event()
    current_signals = HEALTHY
    stop_calls = 0

    def signals() -> PipelineSignals:
        return current_signals

    async def stop_pipeline() -> dict[str, str]:
        nonlocal stop_calls, current_signals
        stop_calls += 1
        stop_entered.set()
        await allow_stop.wait()
        current_signals = PROVEN_STOPPED
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    async def yielding_sleep(_seconds: float) -> None:
        await asyncio.sleep(0.001)

    first = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=signals,
        stop_pipeline=stop_pipeline,
        sleep=yielding_sleep,
        retry_base_seconds=0.001,
    )
    second = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=signals,
        stop_pipeline=stop_pipeline,
        sleep=yielding_sleep,
        retry_base_seconds=0.001,
    )
    first_task = asyncio.create_task(first.run())
    await asyncio.wait_for(stop_entered.wait(), timeout=1)
    second_task = asyncio.create_task(second.run())
    await asyncio.sleep(0.01)
    assert second.status.state == "retrying"
    assert stop_calls == 1

    allow_stop.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=1)

    assert stop_calls == 1
    assert power.records[-1]["intent"] == "stopped"
    assert first.status.state == second.status.state == "settled"


async def test_operator_stopped_pipeline_is_recorded_without_a_second_mutation() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)

    async def unsafe_second_stop() -> dict[str, str]:
        pytest.fail("proven operator stop must not issue another mutation")

    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=PROVEN_STOPPED,
        stop_pipeline=unsafe_second_stop,
    )
    await recovery.run()

    assert power.records[-1]["intent"] == "stopped"
    assert power.records[-1]["stopped_by"] == (
        "operator stop confirmed by startup recovery"
    )
    assert recovery.status.state == "settled"


@pytest.mark.parametrize(
    "signals",
    [
        PipelineSignals(
            pipeline_state="IDLE",
            update_state="",
            synced_table_state="SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE",
            continuous_reported=False,
        ),
        PipelineSignals(
            pipeline_state="IDLE",
            update_state="FAILED",
            synced_table_state="SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
            continuous_reported=False,
        ),
    ],
    ids=["ambiguous-idle", "failed-update"],
)
async def test_ambiguous_or_failed_inactive_pipeline_never_masks_failure(
    signals: PipelineSignals,
) -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    pipeline_power.install_owed_stop_snapshot(inherited)

    async def stop_pipeline() -> dict[str, str]:
        pytest.fail("inactive ambiguous or failed pipeline must not be mutated")

    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=signals,
        stop_pipeline=stop_pipeline,
    )
    await recovery.run()

    assert power.records == [inherited]
    assert recovery.status.state == "given_up"
    assert "not a proven deliberate stop" in (recovery.status.detail or "")
    assert pipeline_power.owed_stop_notice(now=clock.now) is not None
    assert await _round4_store(root).current() is None


async def test_transient_store_failure_retries_without_hiding_or_repeating_stop() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited, append_failures=1)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    current_signals = HEALTHY
    stop_calls = 0
    debt_visible_during_retry: list[bool] = []

    def signals() -> PipelineSignals:
        return current_signals

    async def stop_pipeline() -> dict[str, str]:
        nonlocal stop_calls, current_signals
        stop_calls += 1
        current_signals = PROVEN_STOPPED
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    async def retry_sleep(_seconds: float) -> None:
        debt_visible_during_retry.append(
            pipeline_power.owed_stop_notice(now=clock.now) is not None
        )
        await asyncio.sleep(0)

    pipeline_power.install_owed_stop_snapshot(inherited)
    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=signals,
        stop_pipeline=stop_pipeline,
        sleep=retry_sleep,
        retry_base_seconds=0,
    )
    await recovery.run()

    assert stop_calls == 1
    assert power.append_attempts == 2
    assert debt_visible_during_retry == [True]
    assert power.records[-1]["intent"] == "stopped"
    assert pipeline_power.owed_stop_notice(now=clock.now) is None
    assert await _round4_store(root).current() is None


async def test_transient_stop_failure_retries_while_debt_stays_visible() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    stop_calls = 0
    debt_visible_during_retry: list[bool] = []

    async def stop_pipeline() -> dict[str, str]:
        nonlocal stop_calls
        stop_calls += 1
        if stop_calls == 1:
            raise ConnectionError("workspace transport reset")
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    async def retry_sleep(_seconds: float) -> None:
        debt_visible_during_retry.append(
            pipeline_power.owed_stop_notice(now=clock.now) is not None
        )
        await asyncio.sleep(0)

    pipeline_power.install_owed_stop_snapshot(inherited)
    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=HEALTHY,
        stop_pipeline=stop_pipeline,
        sleep=retry_sleep,
        retry_base_seconds=0,
    )
    await recovery.run()

    assert stop_calls == 2
    assert debt_visible_during_retry == [True]
    assert power.records[-1]["intent"] == "stopped"
    assert pipeline_power.owed_stop_notice(now=clock.now) is None
    assert await _round4_store(root).current() is None


async def test_failed_durable_append_does_not_clear_inherited_health_snapshot(
    monkeypatch,
) -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))

    class RefusingStore:
        async def append(self, _record) -> None:
            raise ConnectionError("coordination transport reset")

    monkeypatch.setattr(pipeline_power, "_durable_store", RefusingStore())
    pipeline_power.install_owed_stop_snapshot(inherited)
    stopped = pipeline_power.stopped_record(_manifest(), now=clock.now)

    assert await pipeline_power.record_power_request(stopped) is False
    assert pipeline_power.owed_stop_notice(now=clock.now) is not None


async def test_shutdown_cancels_recovery_releases_lease_then_stores_can_close() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    read_started = asyncio.Event()
    hold_read = asyncio.Event()

    async def blocked_read() -> PipelineSignals:
        read_started.set()
        await hold_read.wait()
        return HEALTHY

    recovery = InheritedRound4StopRecovery(
        _manifest(),
        _round4_store(root),
        power,  # type: ignore[arg-type]
        inherited,
        read_signals=blocked_read,
        stop_pipeline=lambda: asyncio.sleep(
            0,
            result=pipeline_power.stopped_record(_manifest(), now=clock.now),
        ),
        now=clock.now,
        sleep=clock.sleep,
        retry_base_seconds=0,
        retry_max_seconds=0,
    )
    task = asyncio.create_task(recovery.run())
    await asyncio.wait_for(read_started.wait(), timeout=1)
    held = await _round4_store(root).current()
    assert held is not None and recovery.lease_held is True

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert recovery.lease_held is False
    assert await _round4_store(root).current() is None
    await power.close()
    await root.close()
    assert power.closed is True


async def test_round4_recovery_ring_does_not_block_any_other_round() -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    stop_started = asyncio.Event()
    hold_stop = asyncio.Event()

    async def blocked_stop() -> dict[str, str]:
        stop_started.set()
        await hold_stop.wait()
        return pipeline_power.stopped_record(_manifest(), now=clock.now)

    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=HEALTHY,
        stop_pipeline=blocked_stop,
    )
    task = asyncio.create_task(recovery.run())
    await asyncio.wait_for(stop_started.wait(), timeout=1)
    round4 = await _round4_store(root).current()
    assert round4 is not None and round4.phase == RECOVERY_PHASE

    for round_id in RoundId:
        if round_id == RoundId.PUT_MODEL_SCORE_IN_APP:
            continue
        store = root.for_ring_key(round_ring_key(INSTALLATION_ID, round_id.value))
        lease = await store.claim(
            session_id=f"bout-{round_id.value}",
            operator=BoutOperator(display_name="Other presenter", subject="other"),
            phase="checking",
            session_state=SessionState.CHECKING,
            round_id=round_id.value,
            round_title=round_id.value,
            competitor_id=CompetitorId.AURORA_SERVERLESS_V2.value,
            competitor_name="Aurora",
            ttl=timedelta(minutes=1),
        )
        assert await store.release(lease) is True

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await _round4_store(root).current() is None


def _readyz_on_healthy_runtime(monkeypatch) -> dict:
    state = app_module.app.state
    monkeypatch.setattr(state, "coordination_mode", "lakebase", raising=False)
    monkeypatch.setattr(state, "readiness_verified", True, raising=False)
    monkeypatch.setattr(state, "credential_sentry", None, raising=False)
    monkeypatch.setattr(state, "restart_history", None, raising=False)
    monkeypatch.setattr(state, "startup_reap", None, raising=False)
    monkeypatch.setattr(
        state,
        "readiness_gate",
        SimpleNamespace(
            status=SimpleNamespace(
                ring_ready=True,
                maintenance_state="ready",
                maintenance_detail=None,
            ),
            recovery=None,
            round5_recovery=None,
        ),
        raising=False,
    )
    monkeypatch.setattr(app_module, "_apply_manifest_lifecycle", lambda _payload: None)
    return json.loads(app_module._readiness_response().body)


async def test_readyz_debt_transitions_true_to_false_only_after_durable_recovery(
    monkeypatch,
) -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    pipeline_power.install_owed_stop_snapshot(inherited)

    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=PROVEN_STOPPED,
        stop_pipeline=lambda: asyncio.sleep(0, result={}),
    )
    monkeypatch.setattr(
        app_module.app.state,
        "round4_stop_recovery",
        recovery,
        raising=False,
    )
    before = _readyz_on_healthy_runtime(monkeypatch)
    await recovery.run()
    after = _readyz_on_healthy_runtime(monkeypatch)

    assert before["round4_stop_owed"] is True
    assert before["round4_stop_owed_since"] == inherited["owed_at"]
    assert after["round4_stop_owed"] is False
    assert after["round4_stop_owed_since"] is None
    assert after["round4_stop_recovery_state"] == "settled"
    assert after["round4_stop_recovery_lease_held"] is False


async def test_permanent_recovery_refusal_is_visible_in_readyz(monkeypatch) -> None:
    clock = Clock(datetime(2026, 9, 3, 18, 0, tzinfo=UTC))
    inherited = _owed(clock.now() - timedelta(hours=1))
    power = PowerStore(inherited)
    root = InMemoryBoutLeaseStore(clock=clock.now)
    pipeline_power.install_owed_stop_snapshot(inherited)
    ambiguous = PipelineSignals(
        pipeline_state="IDLE",
        update_state="",
        synced_table_state="SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE",
        continuous_reported=False,
    )
    recovery = _recovery(
        clock,
        root,
        power,
        inherited,
        signals=ambiguous,
        stop_pipeline=lambda: asyncio.sleep(0, result={}),
    )
    await recovery.run()
    monkeypatch.setattr(
        app_module.app.state,
        "round4_stop_recovery",
        recovery,
        raising=False,
    )

    payload = _readyz_on_healthy_runtime(monkeypatch)

    assert payload["round4_stop_recovery_state"] == "given_up"
    assert payload["round4_stop_recovery_error"] == (
        "Round4StopRecoveryRefusedError"
    )
    assert "NOT RETRYING" in payload["round4_stop_recovery_detail"]
    assert payload["round4_stop_owed"] is True
    # Spend recovery does not falsely take the other five rounds or the serving
    # process out of rotation.
    assert payload["ring_ready"] is True
    assert payload["status"] == "ready"


def test_fight_card_calls_recovery_cleanup_only_while_lease_is_reported() -> None:
    active = BoutStatus(
        scope="round",
        round_id=RoundId.PUT_MODEL_SCORE_IN_APP,
        active=True,
        can_start=False,
        phase=RECOVERY_PHASE,
        state=SessionState.RUNNING,
    )
    ready = BoutStatus(
        scope="round",
        round_id=RoundId.PUT_MODEL_SCORE_IN_APP,
        active=False,
        can_start=True,
    )
    availability = SimpleNamespace(availability="ready", availability_reason_code=None)

    recovering = _fight_card_round_status(
        RoundId.PUT_MODEL_SCORE_IN_APP,
        availability,
        active,
    )
    released = _fight_card_round_status(
        RoundId.PUT_MODEL_SCORE_IN_APP,
        availability,
        ready,
    )

    assert recovering.state == "cleanup_in_progress"
    assert recovering.active_phase == RECOVERY_PHASE
    assert recovering.can_start is False
    assert released.state == "ready"
    assert released.can_start is True


async def test_runtime_shutdown_awaits_recovery_before_manager_and_stores(
    monkeypatch,
) -> None:
    events: list[str] = []
    recovery_started = asyncio.Event()

    async def recovery_task_body() -> None:
        recovery_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            events.append("recovery-finished")

    recovery_task = asyncio.create_task(
        recovery_task_body(),
        name="round4-inherited-pipeline-stop-recovery",
    )
    await recovery_started.wait()
    posted_task = asyncio.create_task(asyncio.Event().wait())

    class Manager:
        async def close(self) -> None:
            events.append("manager-closed")

    class Store:
        def __init__(self, name: str) -> None:
            self.name = name

        async def close(self) -> None:
            events.append(f"{self.name}-closed")

    monkeypatch.setattr(app_module, "unregister_serving_process", lambda _record: None)
    monkeypatch.setattr(app_module.app.state, "run_manager", Manager(), raising=False)
    monkeypatch.setattr(
        app_module.app.state,
        "round4_stop_recovery",
        SimpleNamespace(),
        raising=False,
    )
    runtime = app_module._Runtime(
        lease_store=Store("lease"),
        round5_lease_store=Store("round5"),
        cost_ledger_store=Store("cost"),
        readiness_task=None,
        posted_usage_task=posted_task,
        process_record=None,
        round4_stop_recovery_task=recovery_task,
    )

    await app_module._close_runtime(app_module.app, runtime)

    assert recovery_task.done() and recovery_task.cancelled()
    assert events[0] == "recovery-finished"
    assert events.index("recovery-finished") < events.index("manager-closed")
    assert events.index("recovery-finished") < events.index("lease-closed")
    assert events[-3:] == ["round5-closed", "cost-closed", "lease-closed"]
