"""A stale process-local veto must never hold Round 5 closed while its warm pool is ready.

Live 2026-09-25 22:18Z: a Round 5 Prepare was refused on a READY ring, and for the next
eleven minutes -- until the process restarted -- the fight card said UNAVAILABLE ("This
round is unavailable right now.") while ``/readyz`` reported the warm slot ready and
claimable with no claim held. Nothing on the fight-card polling path could lift the
manager's own gate: an unexpired-looking local lease, or the durable-cleanup latch that
only ``/api/catalog`` or a restart ever cleared.

These tests drive the real ``RunManager``, warm coordinator and ASGI routes from the
pre-deploy acceptance harness. The durable atomic claim still decides who may arm; these
only prove that the fight card stops reporting a veto nothing durable backs.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from server.api import _fight_card_round_status
from server.coordination import BoutLease
from server.models import (
    Availability,
    BoutOperator,
    BoutStatus,
    FightCardState,
    RoundId,
    SessionState,
)
from tests.test_round5_pre_deploy_acceptance import (
    _asgi,
    _BlockingPlan,
    _coordinator,
    _EngineProvider,
    _make_manager,
    _session_body,
    _warm_to_ready,
)
from tests.test_round5_warm import Clock

_BASE = "http://anti-demo.test"


@pytest.fixture(autouse=True)
def _local_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)


def _round_five_card(board: dict) -> dict:
    return board["rounds"]["survive_connection_spike"]


def _lease(session_id: str, *, expires_at: datetime) -> BoutLease:
    started = expires_at - timedelta(minutes=2)
    return BoutLease(
        lease_id="lease-left-by-a-refused-prepare",
        fencing_token=41,
        session_id=session_id,
        operator=BoutOperator(display_name="Local operator"),
        owner_subject="local:local operator",
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE.value,
        round_title="Ready a pooled application path",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        started_at=started,
        updated_at=started,
        expires_at=expires_at,
    )


async def test_an_expired_local_lease_cannot_hold_the_fight_card_closed() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    manager = _make_manager(coordinator, clock, round_isolation=True)
    try:
        await _warm_to_ready(coordinator, provider)
        transport = ASGITransport(app=_asgi(manager))
        async with AsyncClient(transport=transport, base_url=_BASE) as client:
            created = await client.post("/api/sessions", json=_session_body())
            assert created.status_code == 201, created.text
            record = manager._records[created.json()["id"]]
            now = datetime.now(UTC)

            # A lease that is still live keeps closing the local gate, as designed.
            live = _lease(record.snapshot.id, expires_at=now + timedelta(seconds=60))
            record.round5_lease = live
            assert coordinator.ring_ready is True
            assert manager.round5_ring_ready is False
            board = (await client.get("/api/bout/all")).json()
            assert _round_five_card(board)["can_start"] is False

            # The same lease past its expiry is no authority anywhere; the card reopens.
            record.round5_lease = replace(live, expires_at=now - timedelta(seconds=1))
            assert manager.round5_ring_ready is True
            card = _round_five_card((await client.get("/api/bout/all")).json())
            assert card["state"] == FightCardState.READY.value
            assert card["can_start"] is True
    finally:
        await manager.close()
        await coordinator.close()


async def test_the_fight_card_lifts_the_cleanup_latch_once_the_cleanup_ring_is_empty() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    manager = _make_manager(coordinator, clock, round_isolation=True)
    try:
        await _warm_to_ready(coordinator, provider)
        # The latch a finished cleanup can leave behind when nobody polls /api/catalog.
        manager._round5_durable_cleanup_blocked = True
        assert await manager._round5_cleanup_store().current() is None
        assert manager.round5_ring_ready is False
        transport = ASGITransport(app=_asgi(manager))
        async with AsyncClient(transport=transport, base_url=_BASE) as client:
            # One fight-card read observes the empty cleanup ring and lifts the latch;
            # the next poll reports the round startable without a catalog request.
            await client.get("/api/bout/all")
            assert manager._round5_durable_cleanup_blocked is False
            card = _round_five_card((await client.get("/api/bout/all")).json())
            assert card["state"] == FightCardState.READY.value
            assert card["can_start"] is True
    finally:
        await manager.close()
        await coordinator.close()


def _ready_catalog_entry() -> SimpleNamespace:
    return SimpleNamespace(
        availability=Availability.READY,
        availability_reason=None,
        availability_reason_code=None,
        availability_headline=None,
    )


def _gate_refusal(detail: str) -> BoutStatus:
    return BoutStatus(
        scope="round",
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        active=False,
        can_start=False,
        ring_ready=False,
        maintenance_state="maintenance",
        maintenance_detail=detail,
    )


def test_a_round5_gate_refusal_reads_temporarily_unavailable_with_its_reason() -> None:
    reason = (
        "ROUND 5 NOT STARTABLE · STAGE REWARMING · GENERATION 57 · CAN_START FALSE · "
        "WAIT FOR READY / RING_READY TRUE"
    )
    status = _fight_card_round_status(
        RoundId.SURVIVE_CONNECTION_SPIKE, _ready_catalog_entry(), _gate_refusal(reason)
    )
    assert status.state == FightCardState.TEMPORARILY_UNAVAILABLE
    assert status.can_start is False
    assert status.detail == reason


def test_a_terminal_round5_gate_refusal_stays_unavailable() -> None:
    reason = (
        "ROUND 5 NOT STARTABLE · STAGE TERMINAL-BLOCKED · GENERATION 57 · CAN_START FALSE · "
        "WAIT FOR READY / RING_READY TRUE"
    )
    status = _fight_card_round_status(
        RoundId.SURVIVE_CONNECTION_SPIKE, _ready_catalog_entry(), _gate_refusal(reason)
    )
    assert status.state == FightCardState.UNAVAILABLE
    assert status.detail == reason
