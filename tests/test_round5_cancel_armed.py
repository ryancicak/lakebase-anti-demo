"""The owner can take back an armed Round 5 fight card without waiting out its window.

Live 2026-09-26: ``cancel-arm`` refused every Round 5 card ("Only a Round 1 start-state
check can be cancelled") and the ready screen offered only the bell, so a presenter who
prepared the wrong matchup held Round 5 for the whole three-minute armed window plus
cleanup (209 s measured). The cancel now runs the exact no-bell cleanup the window's
expiry runs -- CLEANING with the claim retained, provider settle, rewarm to N+1 -- only
without the wait. The armed TTL is left at its production value here, so nothing but the
cancel can explain the recovery.
"""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from server.models import SessionState
from server.round5_warm import Round5WarmState
from tests.test_round5_pre_deploy_acceptance import (
    _arm_via_http,
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
_INSTALLATION = "install-acceptance"


@pytest.fixture(autouse=True)
def _fast_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "0.02")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "0.05")


async def _generation_after(coordinator, generation: int) -> None:
    for _ in range(400):
        slot = await coordinator.store.read(_INSTALLATION)
        if slot is not None and slot.generation == generation + 1:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("cleanup never reached the next generation")


async def test_cancelling_an_armed_round5_card_reopens_the_ring_without_the_window() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    first_generation = (await coordinator.store.read(_INSTALLATION)).generation
    manager = _make_manager(coordinator, clock)
    assert manager._armed_ttl == 180
    try:
        transport = ASGITransport(app=_asgi(manager))
        async with AsyncClient(transport=transport, base_url=_BASE) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)

            cancelled = await client.post(f"/api/sessions/{session_id}/cancel-arm")
            assert cancelled.status_code == 200, cancelled.text
            assert cancelled.json()["state"] == SessionState.FAILED.value
            assert cancelled.json()["run_started_at"] is None
            # The window timer is gone; nothing else will fire for this card.
            assert manager._records[session_id].armed_expiry_task is None

            # A second click is harmless.
            again = await client.post(f"/api/sessions/{session_id}/cancel-arm")
            assert again.status_code == 200, again.text
            # The released card can never be rung.
            rung = await client.post(f"/api/sessions/{session_id}/run")
            assert rung.status_code == 409

            await _generation_after(coordinator, first_generation)
            assert provider.engines[0].settle_abandoned_calls >= 1
            await coordinator.run_one_cycle()  # WARMING(N+1) -> READY(N+1)
            ready = await coordinator.store.read(_INSTALLATION)
            assert ready is not None and ready.state == Round5WarmState.READY
            assert ready.claim is None

            board = (await client.get("/api/bout/all")).json()
            assert board["rounds"]["survive_connection_spike"]["can_start"] is True
            second = await client.post("/api/sessions", json=_session_body("rds_postgres"))
            await _arm_via_http(client, second.json()["id"])
    finally:
        await manager.close()
        await coordinator.close()


async def test_a_round5_card_cannot_be_cancelled_after_its_bell() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock)
    try:
        transport = ASGITransport(app=_asgi(manager))
        async with AsyncClient(transport=transport, base_url=_BASE) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            rung = await client.post(f"/api/sessions/{session_id}/run")
            assert rung.status_code == 200, rung.text

            refused = await client.post(f"/api/sessions/{session_id}/cancel-arm")
            assert refused.status_code == 409
            assert "has not rung" in refused.json()["detail"]
            snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
            assert snapshot["run_started_at"] is not None
            assert snapshot["state"] != SessionState.FAILED.value
    finally:
        await manager.close()
        await coordinator.close()
