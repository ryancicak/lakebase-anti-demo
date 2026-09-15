"""Lakebase's 10,000 starts when Lakebase is ready, not when the AWS path finishes.

This is the behaviour Ryan asked for and repeated until it was unmistakable: ring the bell, and the
lane that is ready goes. Lakebase verifies its included pooled path in about three and a half
seconds; the AWS path spends eleven or twelve minutes building an RDS Proxy. A round that holds
Lakebase's ramp until both setups stop makes that Proxy build a precondition for Lakebase's number,
which is the opposite of the finding the round exists to show, and on screen it is eleven minutes of
nothing.

The tests below drive the engine through a stand-in orchestrator that reports one lane ready and
then pauses, which is the shape of a real bout: one lane verified, the other still provisioning.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from server.connection_spike_live import (
    ConnectionSpikeSetupLaneStop,
    ConnectionSpikeTarget,
    LiveConnectionSpikeEngine,
)

ACCOUNT = "123456789012"


def target(lane_id: str, *, competitor_id: str = "") -> ConnectionSpikeTarget:
    return ConnectionSpikeTarget(
        lane_id=lane_id,
        secret_arn=(
            "" if lane_id == "lakebase" else f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:x"
        ),
        endpoint_host=f"{lane_id}-pooled.example.test",
        credential_host=f"{lane_id}-direct.example.test",
        competitor_id=competitor_id,
        competitor_target_id="sealed-instance" if competitor_id else "",
        competitor_resource_id="db-SEALED" if competitor_id else "",
        credential_sha256="c" * 64,
        observer_credential_sha256="d" * 64,
    )


class Adapter:
    """Records which lanes were dispatched, in order, and when."""

    def __init__(self) -> None:
        self.dispatched: list[str] = []
        self.config = SimpleNamespace(
            targets=(target("lakebase"), target("competitor", competitor_id="rds_postgres")),
            runner_instance_type="c7i.2xlarge",
            trust_bundle_sha256="a" * 64,
        )

    async def preflight_capacity(self, run_id, **digests):
        del run_id, digests
        return SimpleNamespace(sufficient=True, failures=())

    async def execute(self, run_id, request, *, targets=None):
        del run_id, targets
        lanes = [str(entry["lane_id"]) for entry in request["targets"]]
        assert len(lanes) == 1, "each dispatch carries exactly one lane"
        self.dispatched.extend(lanes)
        return {}


class Orchestrator:
    """Reports Lakebase ready, then waits, which is what a real Proxy build looks like."""

    def __init__(self) -> None:
        self.lakebase_reported = asyncio.Event()
        self.release_competitor = asyncio.Event()

    async def prepare(self, bout_id, fencing_token):
        del bout_id, fencing_token

    async def setup(self, bout_id, fencing_token, on_progress=None, on_lane_ready=None):
        del fencing_token, on_progress
        assert on_lane_ready is not None, "the engine must ask to be told when a lane is ready"
        lakebase = ConnectionSpikeSetupLaneStop(
            lane_id="lakebase",
            launched_ns=1_000,
            stopped_ns=3_400_000_000,
            credential_sha256="c" * 64,
            endpoint_host="lakebase-pooled.example.test",
        )
        await on_lane_ready(lakebase)
        self.lakebase_reported.set()
        # The AWS path is still building. Nothing about Lakebase's ramp may depend on this.
        await self.release_competitor.wait()
        competitor = ConnectionSpikeSetupLaneStop(
            lane_id="competitor",
            launched_ns=1_000,
            stopped_ns=870_000_000_000,
            credential_sha256="c" * 64,
            endpoint_host="competitor-pooled.example.test",
            secret_arn=f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:x",
        )
        await on_lane_ready(competitor)
        return SimpleNamespace(bout_id=bout_id, lakebase=lakebase, competitor=competitor)

    async def assert_no_unresolved_bouts(self, *args, **kwargs):
        del args, kwargs


async def test_lakebase_ramps_while_the_aws_path_is_still_provisioning() -> None:
    """The whole point. Lakebase must be dispatched before the competitor's setup finishes."""

    adapter = Adapter()
    orchestrator = Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    engine._armed = await engine.check()

    setup = asyncio.create_task(engine.setup("bout-per-lane", 7))
    await asyncio.wait_for(orchestrator.lakebase_reported.wait(), timeout=2)
    # Give the launched ramp a turn to reach the adapter.
    for _ in range(50):
        if adapter.dispatched:
            break
        await asyncio.sleep(0)

    assert adapter.dispatched == ["lakebase"], (
        "Lakebase's 10,000 must start at its own setup stop, while the AWS path is still "
        f"building its Proxy; dispatched={adapter.dispatched}"
    )
    assert not orchestrator.release_competitor.is_set()

    orchestrator.release_competitor.set()
    await asyncio.wait_for(setup, timeout=2)
    for _ in range(50):
        if len(adapter.dispatched) > 1:
            break
        await asyncio.sleep(0)
    assert adapter.dispatched == ["lakebase", "competitor"]


async def test_the_engine_asks_to_be_told_when_a_lane_is_ready() -> None:
    """A setup never asked cannot start a lane early, so the request itself is the contract."""

    adapter = Adapter()
    orchestrator = Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    engine._armed = await engine.check()
    setup = asyncio.create_task(engine.setup("bout-asks", 7))
    await asyncio.wait_for(orchestrator.lakebase_reported.wait(), timeout=2)
    orchestrator.release_competitor.set()
    await asyncio.wait_for(setup, timeout=2)


async def test_a_lane_with_no_arm_defers_instead_of_failing() -> None:
    """Starting early is an optimisation and must never be why a round cannot ring.

    A bout armed at the bell rather than at the arm has no arm while setup runs, so no lane can be
    dispatched early. That has to be a slower round, not a failed one.
    """

    adapter = Adapter()
    orchestrator = Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    setup = asyncio.create_task(engine.setup("bout-unarmed", 7))
    await asyncio.wait_for(orchestrator.lakebase_reported.wait(), timeout=2)
    orchestrator.release_competitor.set()
    await asyncio.wait_for(setup, timeout=2)
    assert adapter.dispatched == [], "no arm means nothing is dispatched early"


@pytest.mark.parametrize("lane_id", ["lakebase", "competitor"])
async def test_each_lane_is_bound_to_the_endpoint_its_own_setup_produced(lane_id: str) -> None:
    """A lane's ramp must use the endpoint that lane's setup created, not the sealed one.

    For the competitor that endpoint is the per-bout RDS Proxy, which does not exist until setup
    builds it, so binding to the sealed direct host would ramp against the wrong thing entirely.
    """

    adapter = Adapter()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=Orchestrator())
    stop = ConnectionSpikeSetupLaneStop(
        lane_id=lane_id,
        launched_ns=1_000,
        stopped_ns=2_000_000,
        credential_sha256="e" * 64,
        endpoint_host=f"{lane_id}-from-setup.example.test",
        secret_arn=(
            "" if lane_id == "lakebase" else f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:x"
        ),
    )
    bound = engine._runtime_target_for(stop)
    assert bound is not None
    assert bound.endpoint_host == f"{lane_id}-from-setup.example.test"
    assert bound.credential_sha256 == "e" * 64
    # Sealed once at install time and identical in every bout, so it comes from the configured
    # lane rather than from this bout's stop.
    assert bound.observer_credential_sha256 == "d" * 64
    assert bound.credential_host == f"{lane_id}-direct.example.test"
