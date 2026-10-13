"""A held row is judged against its own phase's renew cadence.

Round 4 stop recovery renews every 30 s, so measuring it against the process-wide
15 s interval called a healthy owner "LEASE LIKELY STALE" for half of every cycle.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from server.coordination import (
    LEASE_HEARTBEAT_SECONDS_ENV,
    BoutLease,
    InMemoryBoutLeaseStore,
    diagnose_held_lease,
)
from server.manager import InvalidStateError, RunManager
from server.models import BoutOperator, CompetitorId, Corner, RoundId, SessionCreate, SessionState
from server.round4_stop_recovery import RECOVERY_LEASE_TTL, RECOVERY_PHASE
from tests.test_coordination import MutableClock

OWNER = BoutOperator(display_name="Owner", subject="owner-subject")
START = datetime(2026, 10, 8, 15, 0, tzinfo=UTC)


async def _held(phase: str, *, ttl: timedelta) -> tuple[BoutLease, MutableClock]:
    clock = MutableClock(START)
    store = InMemoryBoutLeaseStore(clock=clock)
    lease = await store.claim(
        session_id=f"{phase}-run",
        operator=BoutOperator(display_name="Backstage maintenance", subject="maintenance:app"),
        phase=phase,
        session_state=SessionState.RUNNING,
        round_id="put_model_score_in_app",
        round_title="held",
        competitor_id="all",
        competitor_name="held",
        ttl=ttl,
    )
    return lease, clock


async def test_round4_stop_recovery_inside_its_30s_cadence_is_not_stale() -> None:
    lease, _clock = await _held(RECOVERY_PHASE, ttl=RECOVERY_LEASE_TTL)
    diagnosis = diagnose_held_lease(lease, now=START + timedelta(seconds=25))
    assert diagnosis.likely_stale is False
    assert diagnosis.heartbeat_interval_seconds == 30


async def test_round4_stop_recovery_silent_past_its_cadence_still_reads_stale() -> None:
    lease, _clock = await _held(RECOVERY_PHASE, ttl=RECOVERY_LEASE_TTL)
    assert diagnose_held_lease(lease, now=START + timedelta(seconds=31)).likely_stale is True


async def test_startup_cleanup_keeps_its_own_cadence_when_the_global_one_is_lowered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(LEASE_HEARTBEAT_SECONDS_ENV, "5")
    lease, _clock = await _held("startup_cleanup", ttl=timedelta(seconds=60))
    assert diagnose_held_lease(lease, now=START + timedelta(seconds=10)).likely_stale is False
    assert diagnose_held_lease(lease, now=START + timedelta(seconds=16)).likely_stale is True


async def test_an_explicit_cadence_is_not_raised_to_the_phase_floor() -> None:
    lease, _clock = await _held(RECOVERY_PHASE, ttl=timedelta(seconds=30))
    diagnosis = diagnose_held_lease(
        lease,
        now=START + timedelta(seconds=20),
        heartbeat_seconds=10,
    )
    assert diagnosis.heartbeat_interval_seconds == 10
    assert diagnosis.likely_stale is True


async def test_a_phase_without_its_own_cadence_keeps_the_global_interval() -> None:
    lease, _clock = await _held("running", ttl=timedelta(seconds=90))
    diagnosis = diagnose_held_lease(lease, now=START + timedelta(seconds=20))
    assert diagnosis.likely_stale is True
    assert diagnosis.heartbeat_interval_seconds == 15


async def test_round4_prepare_beside_a_healthy_stop_recovery_does_not_say_stale() -> None:
    clock = MutableClock(START)
    store = InMemoryBoutLeaseStore(clock=clock)
    await store.claim(
        session_id="round4-stop-recovery-run",
        operator=BoutOperator(display_name="Backstage maintenance", subject="maintenance:app"),
        phase=RECOVERY_PHASE,
        session_state=SessionState.RUNNING,
        round_id="put_model_score_in_app",
        round_title="Round 4 pipeline stop recovery",
        competitor_id="sealed_pipeline",
        competitor_name="Sealed Managed Sync pipeline",
        ttl=RECOVERY_LEASE_TTL,
    )
    clock.now += timedelta(seconds=25)
    manager = RunManager(model_score_factory=lambda _c: object(), lease_store=store)
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="data_analyst",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.PUT_MODEL_SCORE_IN_APP,
        )
    )
    with pytest.raises(InvalidStateError) as refused:
        await manager.start_arm(created.id, OWNER)
    text = str(refused.value)
    assert "BOUT IN PROGRESS" in text
    assert "LEASE LIKELY STALE" not in text
    assert "RING UNLOCKS IN 575S" in text
    await manager.close()
