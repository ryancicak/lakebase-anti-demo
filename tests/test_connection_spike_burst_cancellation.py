"""The bound on Round 5's *burst* cancellation teardown.

The setup path was bounded first, and the burst path was left behind with two
defects that compound. Its cancellation handlers awaited ``_cancel_and_settle``
under a bare ``asyncio.shield`` with no ceiling at all, and inside that method
the ``cancel_command`` call sat *outside* the ``asyncio.timeout`` that appeared
to cover it -- so the one call that can wedge was the one call the deadline
could not reach, and the block read as protection while providing none.

Every stall test here hangs against the unbounded version: the wait is the
thing under test, so a fake that merely returned slowly would prove nothing.
Each one wedges a real worker thread the way an unreachable SSM endpoint does,
and always releases it, so the loop's executor can still shut down.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from types import SimpleNamespace

import boto3
import pytest
from botocore.stub import Stubber
from test_connection_spike_live import (
    INSTANCE_ID,
    FakeCloudWatch,
    FakeEc2,
    FakeRds,
    FakeSessionFactory,
    FakeSts,
    live_config,
)

from server.connection_spike import build_schedule
from server.connection_spike_live import (
    ConnectionSpikeCleanupError,
    LiveConnectionSpikeAdapter,
    _serialize_schedule,
)

REGION = "us-west-2"
BOUND = 0.1
# Generous next to BOUND, unreachable if the wait is unbounded.
PATIENCE = 3.0
RUN_ID = "burst-run"


class _Wedge:
    """A worker thread that blocks the way an unreachable endpoint does."""

    def __init__(self, *, wedged: bool = True) -> None:
        self.entered = threading.Event()
        self.released = threading.Event()
        self.calls: list[dict[str, object]] = []
        if not wedged:
            self.released.set()

    def __call__(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        self.entered.set()
        self.released.wait(30)
        return {}

    def release(self) -> None:
        self.released.set()


class _Ssm:
    """A runner whose invocation never settles and whose cancel can wedge."""

    def __init__(self, *, wedge_cancel: bool = True, settles: bool = False) -> None:
        self.sent = threading.Event()
        self.send_calls: list[dict[str, object]] = []
        self.cancel = _Wedge(wedged=wedge_cancel)
        self._settles = settles
        self.cancelled = False

    def send_command(self, **kwargs: object) -> dict[str, object]:
        self.send_calls.append(kwargs)
        self.sent.set()
        # SSM command IDs are UUIDs; anything else would be rejected by the
        # service model, so the fake uses a real one even though it is a fake.
        return {"Command": {"CommandId": "11111111-2222-4333-8444-555555555555"}}

    def get_command_invocation(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        if self.cancelled and self._settles:
            return {
                "Status": "Cancelled",
                "StandardOutputContent": (
                    f"CLEANUP_CONFIRMED:{RUN_ID}\nRUNNER_FLOCK_RELEASED:{RUN_ID}\n"
                ),
            }
        return {"Status": "InProgress", "StandardOutputContent": ""}

    def cancel_command(self, **kwargs: object) -> dict[str, object]:
        self.cancelled = True
        return self.cancel(**kwargs)

    def describe_instance_information(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        return {
            "InstanceInformationList": [
                {
                    "InstanceId": INSTANCE_ID,
                    "PingStatus": "Online",
                    "PlatformType": "Linux",
                }
            ]
        }


async def _until(predicate, description: str) -> None:
    deadline = time.monotonic() + PATIENCE
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(0.005)


def _adapter(ssm: _Ssm, *, bound: float = BOUND) -> LiveConnectionSpikeAdapter:
    """A real adapter whose only fakes are the assumed AWS clients."""

    async def poll(_delay: float) -> None:
        await asyncio.sleep(0.001)

    return LiveConnectionSpikeAdapter(
        live_config(),
        session_factory=FakeSessionFactory(FakeSts(), ssm),
        sleep=poll,
        cancel_teardown_timeout_seconds=bound,
    )


def _schedule() -> list[dict[str, object]]:
    return _serialize_schedule(
        SimpleNamespace(  # type: ignore[arg-type]
            schedule=build_schedule(("lakebase", "competitor"), scheduled_at_ns=0)
        )
    )


async def test_a_wedged_cancel_command_cannot_stall_the_burst_cancellation() -> None:
    """The case nothing covered: cancel_command itself never returns.

    Hangs without the bound. The burst handler awaited ``_cancel_and_settle``
    under a bare shield, and that method's first act was an unbounded
    ``to_thread`` onto ``cancel_command``, so Ctrl-C during a bout held the
    shutdown open for as long as SSM stayed unreachable.
    """

    ssm = _Ssm()
    adapter = _adapter(ssm)
    task = asyncio.create_task(adapter.execute(RUN_ID, _schedule()))
    try:
        await _until(ssm.sent.is_set, "the runner command to be sent")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)
        # Abandoned, not skipped: the cancel really was issued.
        await _until(lambda: bool(ssm.cancel.calls), "the cancellation to be issued")
    finally:
        ssm.cancel.release()
        await asyncio.sleep(0.05)

    assert all(set(call) == {"CommandId", "InstanceIds"} for call in ssm.cancel.calls)


async def test_the_settlement_bound_now_covers_the_cancel_command() -> None:
    """Fix two on its own, with the caller's bound deliberately out of the way.

    ``_cancel_and_settle`` is called directly, so nothing outside it can rescue
    the wait. Against the previous body this never returns: ``cancel_command``
    ran before ``asyncio.timeout`` was entered, so the deadline covered only the
    polling loop that a wedged cancel never reached. The settlement timeout is a
    sealed 10s constant, so the config is stubbed down to keep the test quick --
    the placement is what is under test, not the value.
    """

    ssm = _Ssm()
    adapter = _adapter(ssm)
    adapter.config = SimpleNamespace(  # type: ignore[assignment]
        settlement_timeout_seconds=BOUND,
        runner_instance_id=INSTANCE_ID,
        poll_interval_seconds=0.01,
    )
    active = SimpleNamespace(
        run_id=RUN_ID,
        command_id="11111111-2222-4333-8444-555555555555",
        clients=SimpleNamespace(ssm=ssm),
    )

    started = time.monotonic()
    try:
        with pytest.raises(ConnectionSpikeCleanupError, match="did not confirm cleanup"):
            await asyncio.wait_for(
                adapter._cancel_and_settle(active),  # type: ignore[arg-type]
                timeout=PATIENCE,
            )
    finally:
        ssm.cancel.release()
        await asyncio.sleep(0.05)

    assert ssm.cancel.entered.is_set(), "the wedge must have been reached"
    assert time.monotonic() - started < PATIENCE


async def test_a_wedged_cancel_cannot_stall_a_cancellation_during_dispatch() -> None:
    """The other handler: cancelled while the send is still in flight.

    That path re-awaits ``send_task`` to learn the command id -- deliberately,
    because a command that was dispatched and then forgotten is the worst
    outcome -- and then tears down. Hangs without the bound for the same reason
    as the post-dispatch handler.
    """

    ssm = _Ssm()
    send_gate = threading.Event()
    real_send = ssm.send_command

    def gated_send(**kwargs: object) -> dict[str, object]:
        send_gate.wait(30)
        return real_send(**kwargs)

    ssm.send_command = gated_send  # type: ignore[method-assign]
    adapter = _adapter(ssm)
    task = asyncio.create_task(adapter.execute(RUN_ID, _schedule()))
    try:
        # Cancel while the dispatch itself is still on the worker thread.
        await _until(lambda: adapter._pending is not None, "the command to be dispatched")
        task.cancel()
        send_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)
        await _until(lambda: bool(ssm.cancel.calls), "the cancellation to be issued")
    finally:
        send_gate.set()
        ssm.cancel.release()
        await asyncio.sleep(0.05)


async def test_the_orphan_risk_log_names_the_state_that_may_still_be_held(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No spend leaks here, but held state does, and it has to be findable."""

    ssm = _Ssm()
    adapter = _adapter(ssm)
    caplog.set_level(logging.ERROR, logger="server.safe_change")
    task = asyncio.create_task(adapter.execute(RUN_ID, _schedule()))
    try:
        await _until(ssm.sent.is_set, "the runner command to be sent")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)
    finally:
        ssm.cancel.release()
        await asyncio.sleep(0.05)

    orphan = next(
        record.getMessage()
        for record in caplog.records
        if "ORPHAN RISK" in record.getMessage()
    )
    assert "in-flight SSM command 11111111-2222-4333-8444-555555555555" in orphan
    assert f"runner instance {INSTANCE_ID}" in orphan
    assert f"runner flock for {RUN_ID}" in orphan
    assert f"ring and scoped Round 5 leases held for {RUN_ID}" in orphan
    # The bound is what is being reported, not a generic failure.
    assert f"{BOUND:.1f}s" in orphan


async def test_the_default_bound_matches_the_sibling_rounds() -> None:
    from server.safe_change import DEFAULT_CANCEL_TEARDOWN_SECONDS

    adapter = LiveConnectionSpikeAdapter(live_config())

    assert adapter.cancel_teardown_timeout_seconds == DEFAULT_CANCEL_TEARDOWN_SECONDS
    assert DEFAULT_CANCEL_TEARDOWN_SECONDS == 5.0
    with pytest.raises(ValueError, match="must be positive"):
        LiveConnectionSpikeAdapter(live_config(), cancel_teardown_timeout_seconds=0)


async def test_a_cancellation_that_settles_promptly_is_confirmed_not_abandoned(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The bound must not turn a healthy teardown into a reported orphan."""

    ssm = _Ssm(wedge_cancel=False, settles=True)
    adapter = _adapter(ssm, bound=PATIENCE)
    caplog.set_level(logging.ERROR, logger="server.safe_change")
    task = asyncio.create_task(adapter.execute(RUN_ID, _schedule()))
    await _until(ssm.sent.is_set, "the runner command to be sent")
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)

    assert len(ssm.cancel.calls) == 1
    assert not [r for r in caplog.records if "ORPHAN RISK" in r.getMessage()]
    assert adapter._active is None


async def test_burst_cancellation_uses_the_exact_ssm_call_shapes() -> None:
    """Stubber, not a fake: botocore validates what a hand-rolled fake skips."""

    session = boto3.Session(
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    ssm = session.client("ssm")
    adapter = _adapter(_Ssm())
    command_id = "11111111-2222-4333-8444-555555555555"
    stubber = Stubber(ssm)
    stubber.add_response(
        "cancel_command",
        {},
        {"CommandId": command_id, "InstanceIds": [INSTANCE_ID]},
    )
    stubber.add_response(
        "get_command_invocation",
        {
            "Status": "Cancelled",
            "StandardOutputContent": (
                f"CLEANUP_CONFIRMED:{RUN_ID}\nRUNNER_FLOCK_RELEASED:{RUN_ID}\n"
            ),
        },
        {"CommandId": command_id, "InstanceId": INSTANCE_ID},
    )
    active = SimpleNamespace(
        run_id=RUN_ID,
        command_id=command_id,
        clients=SimpleNamespace(ssm=ssm),
    )

    with stubber:
        await adapter._cancel_and_settle(active)  # type: ignore[arg-type]

    stubber.assert_no_pending_responses()


async def test_the_unassumed_client_fakes_still_serve_the_normal_burst_path() -> None:
    """Guards the harness itself: these fakes must exercise the real preflight."""

    ssm = _Ssm()
    factory = FakeSessionFactory(FakeSts(), ssm)
    adapter = LiveConnectionSpikeAdapter(live_config(), session_factory=factory)

    await adapter.check()

    assert factory.client_origins[0] == ("ambient", "sts")
    assert isinstance(factory.rds, FakeRds)
    assert isinstance(factory.cloudwatch, FakeCloudWatch)
    assert isinstance(factory.ec2, FakeEc2)
