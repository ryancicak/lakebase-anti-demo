"""Round 5's control transport on Azure: the App's send; the runner's identity, secret and queue.

SQS FIFO's guarantees, in Service Bus's names: a job's events in order (its session, `<lane>-<job>`,
SQS's message group), a resend dropped (the message ID is the event's ID), and an acknowledgement
that is the end of a message. Nothing here reaches Azure: IMDS, Key Vault, Service Bus and the SDK
are fakes. The live proof is the runners' own (FINDINGS).
"""

from __future__ import annotations

import asyncio
import io
import json
import urllib.error
from types import SimpleNamespace
from typing import Any

import pytest

from runner import azure_io
from server.azure_round5_transport import (
    ServiceBusPublishError,
    message_properties,
    service_bus_publisher,
)

QUEUES = {"lakebase": "r5-runner-0", "competitor": "r5-runner-1"}


class Event:
    lane_id, job_id, event_id = "competitor", "job-7", "event-42"

    @staticmethod
    def encoded_body() -> str:
        return "H4sIAAAA-body"


class Response(io.BytesIO):
    def __init__(self, body: bytes = b"", status: int = 201) -> None:
        super().__init__(body)
        self.status = status

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def test_the_app_sends_each_event_to_its_lanes_queue_in_its_jobs_session() -> None:
    sent: list[Any] = []

    def opener(request: Any, timeout: float) -> Response:
        sent.append(request)
        return Response()

    publish = service_bus_publisher("sb-test", QUEUES, lambda: "app-token", opener=opener)
    asyncio.run(publish(Event()))
    (request,) = sent
    assert request.full_url == "https://sb-test.servicebus.windows.net/r5-runner-1/messages"
    assert request.get_method() == "POST"
    assert request.data == b"H4sIAAAA-body"
    assert request.headers["Authorization"] == "Bearer app-token"
    assert json.loads(request.headers["Brokerproperties"]) == {
        "SessionId": "competitor-job-7", "MessageId": "event-42"}
    assert message_properties(Event()) == {"SessionId": "competitor-job-7",
                                           "MessageId": "event-42"}


def test_a_refused_send_is_the_dispatchers_to_retry() -> None:
    def opener(request: Any, timeout: float) -> Response:
        raise urllib.error.HTTPError(request.full_url, 401, "Unauthorized", None, None)

    publish = service_bus_publisher("sb-test", QUEUES, lambda: "t", opener=opener)
    with pytest.raises(ServiceBusPublishError, match="HTTP 401"):
        asyncio.run(publish(Event()))


def test_the_transport_needs_both_lanes_queues() -> None:
    with pytest.raises(ValueError, match="both lanes"):
        service_bus_publisher("sb-test", {"lakebase": "q"}, lambda: "t")


def test_the_runners_identity_is_asked_of_imds_once_per_resource_until_near_its_expiry() -> None:
    asked: list[str] = []
    now = [1_000.0]

    def opener(request: Any, timeout: float) -> Response:
        asked.append(request.full_url)
        assert request.headers["Metadata"] == "true"
        return Response(json.dumps({"access_token": f"token-{len(asked)}",
                                    "expires_on": str(int(now[0]) + 3600)}).encode())

    credential = azure_io.ImdsCredential(opener=opener, clock=lambda: now[0])
    assert credential.get_token("https://servicebus.azure.net/.default").token == "token-1"
    assert credential.get_token("https://servicebus.azure.net/.default").token == "token-1"
    assert "resource=https%3A%2F%2Fservicebus.azure.net" in asked[0]
    now[0] += 3600 - 200  # inside the refresh margin
    assert credential.get_token("https://servicebus.azure.net/.default").token == "token-2"
    with pytest.raises(ValueError):
        credential.get_token("a", "b")


def test_the_control_secret_reads_as_none_until_the_installer_writes_it() -> None:
    token = SimpleNamespace(get_token=lambda scope: SimpleNamespace(token=f"kv:{scope}"))

    def present(request: Any, timeout: float) -> Response:
        assert request.full_url == (
            "https://kv-test.vault.azure.net/secrets/round5-lakebase-control?api-version=7.4")
        assert request.headers["Authorization"] == "Bearer kv:https://vault.azure.net/.default"
        return Response(json.dumps({"value": "postgresql://control"}).encode())

    def absent(request: Any, timeout: float) -> Response:
        raise urllib.error.HTTPError(request.full_url, 404, "SecretNotFound", None, None)

    vault = "https://kv-test.vault.azure.net/"
    assert azure_io.key_vault_secret(vault, "round5-lakebase-control", token,
                                     opener=present) == "postgresql://control"
    assert azure_io.key_vault_secret(vault, "round5-lakebase-control", token,
                                     opener=absent) is None


class Timeout(Exception):
    pass


class Locked(Exception):
    pass


class Dropped(Exception):
    """The SDK's ServiceBusConnectionError: a connection lost in passing."""


class Refused(Exception):
    """The SDK's ServiceBusAuthorizationError: the identity lacks the queue's role."""


class LockLost(Exception):
    pass


class Receiver:
    def __init__(self, sdk: Sdk, session_id: str, messages: list[str]) -> None:
        self.sdk, self.messages = sdk, list(messages)
        self.session = SimpleNamespace(session_id=session_id)
        self.completed: list[str] = []
        self.dead_lettered: list[tuple[str, str]] = []

    def __enter__(self) -> Receiver:
        return self

    def __exit__(self, *_: Any) -> None:
        # Service Bus offers a let-go session again while it still has events.
        self.sdk.released.append(self.session.session_id)
        if self.messages:
            self.sdk.sessions.append((self.session.session_id, self.messages))

    def receive_messages(self, max_message_count: int, max_wait_time: float) -> list[str]:
        assert max_message_count == 1
        self.sdk.waits.append(("receive", max_wait_time))
        if self.sdk.receive_failures:
            raise self.sdk.receive_failures.pop(0)
        return [self.messages.pop(0)] if self.messages else []

    def complete_message(self, message: str) -> None:
        if self.sdk.settle_failures:
            raise self.sdk.settle_failures.pop(0)
        self.completed.append(message)

    def dead_letter_message(self, message: str, *, reason: str, error_description: str) -> None:
        self.dead_lettered.append((message, reason))


class Sdk:
    """The parts of azure.servicebus the queue uses, with sessions queued up in order."""

    NEXT_AVAILABLE_SESSION = object()

    def __init__(self, sessions: list[tuple[str, list[str]]]) -> None:
        self.sessions, self.released, self.receivers = list(sessions), [], []
        self.renewed: list[tuple[str, float]] = []
        self.waits: list[tuple[str, float]] = []
        self.accept_failures: list[Exception] = []
        self.receive_failures: list[Exception] = []
        self.settle_failures: list[Exception] = []
        self.exceptions = SimpleNamespace(
            OperationTimeoutError=Timeout, SessionCannotBeLockedError=Locked,
            ServiceBusConnectionError=Dropped, ServiceBusCommunicationError=Dropped,
            ServiceBusServerBusyError=Dropped, SessionLockLostError=LockLost)
        sdk = self

        class AutoLockRenewer:
            def __init__(self, *, max_lock_renewal_duration: float) -> None:
                sdk.renewal_bound = max_lock_renewal_duration

            def register(self, receiver: Receiver, renewable: Any, *,
                         max_lock_renewal_duration: float) -> None:
                assert renewable is receiver.session
                sdk.renewed.append((renewable.session_id, max_lock_renewal_duration))

            def close(self) -> None:
                sdk.renewer_closed = True

        self.AutoLockRenewer = AutoLockRenewer

        class Client:
            def __init__(self, *, fully_qualified_namespace: str, credential: Any,
                         retry_total: int) -> None:
                sdk.namespace, sdk.retry_total = fully_qualified_namespace, retry_total

            def get_queue_receiver(self, queue: str, *, session_id: Any, max_wait_time: float,
                                   prefetch_count: int) -> Receiver:
                assert session_id is Sdk.NEXT_AVAILABLE_SESSION and prefetch_count == 0
                sdk.waits.append(("accept", max_wait_time))
                if sdk.accept_failures:
                    raise sdk.accept_failures.pop(0)
                if not sdk.sessions:
                    raise Timeout()
                receiver = Receiver(sdk, *sdk.sessions.pop(0))
                sdk.receivers.append(receiver)
                return receiver

            def close(self) -> None:
                sdk.closed = True

        self.ServiceBusClient = Client


def test_a_jobs_events_are_read_in_its_session_and_acknowledged_one_by_one() -> None:
    sdk = Sdk([("lakebase-job-1", ["stage", "release"]), ("lakebase-job-2", ["preload"])])
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=sdk)
    assert sdk.namespace == "sb-test.servicebus.windows.net"
    (stage,) = queue.receive(1.0)
    assert (stage.body, stage.session_id) == ("stage", "lakebase-job-1")
    stage.acknowledge()
    # A job's next event, already there, is read in the session held: no new session is taken.
    (release,) = queue.receive(1.0)
    assert (release.body, len(sdk.receivers)) == ("release", 1)
    release.acknowledge()
    assert sdk.receivers[0].completed == ["stage", "release"]
    # Nothing more in it: the session is let go, and the next job's is taken on the next receive.
    assert queue.receive(1.0) == [] and sdk.released == ["lakebase-job-1"]
    (preload,) = queue.receive(1.0)
    assert (preload.body, preload.session_id) == ("preload", "lakebase-job-2")
    # Each held session's lock is kept renewed while the agent works: it completes a message only
    # once done, SQS gave it 720 s, and a Service Bus lock lasts 300 s.
    assert [session for session, _ in sdk.renewed] == ["lakebase-job-1", "lakebase-job-2"]
    assert all(bound >= 720 for _, bound in sdk.renewed)


def test_a_left_event_comes_back_after_its_visibility_timeout_and_its_job_waits_behind_it() -> None:
    # SQS FIFO: a message received and not deleted holds its group until its visibility timeout
    # passes, then is received again; other groups flow meanwhile. The agent leaves an event it
    # could not finish (RESIDENT_CONTROL_TRANSIENT) and relies on exactly that.
    now = [0.0]
    sdk = Sdk([("lakebase-job-1", ["stage", "release"]), ("lakebase-job-2", ["preload"])])
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=sdk,
                                            clock=lambda: now[0])
    (stage,) = queue.receive(1.0)
    stage.leave(720.0)
    # Another job's events are not held up...
    (preload,) = queue.receive(1.0)
    assert (preload.body, preload.session_id) == ("preload", "lakebase-job-2")
    preload.acknowledge()
    # ...but job 1's release does not overtake its stage, however often the agent asks.
    for _ in range(3):
        now[0] += 1.0
        assert queue.receive(1.0) == []
    first = sdk.receivers[0]
    assert first.messages == ["release"] and "lakebase-job-1" not in sdk.released
    now[0] = 720.0
    (again,) = queue.receive(1.0)
    assert (again.body, again.session_id) == ("stage", "lakebase-job-1")
    again.acknowledge()
    assert first.completed == ["stage"] and "lakebase-job-1" in sdk.released
    (release,) = queue.receive(1.0)
    assert release.body == "release"
    release.acknowledge()


def test_an_event_received_three_times_and_left_is_dead_lettered() -> None:
    # The AWS queues' redrive, maxReceiveCount 3: moved once the third receive's visibility
    # timeout passes, with the job held behind it until then. Its later events then flow.
    now = [0.0]
    sdk = Sdk([("competitor-job-3", ["cancel", "after"])])
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-1", object(), sdk=sdk,
                                            clock=lambda: now[0])
    (cancel,) = queue.receive(1.0)
    for receives in (2, 3):
        cancel.leave(720.0)
        now[0] += 720.0
        (cancel,) = queue.receive(1.0)
        assert cancel.body == "cancel", receives
    cancel.leave(720.0)
    first = sdk.receivers[0]
    now[0] += 719.0
    assert queue.receive(1.0) == [] and first.dead_lettered == []
    now[0] += 1.0
    (after,) = queue.receive(1.0)
    assert first.dead_lettered == [("cancel", "MaxReceivesExceeded")] and first.completed == []
    assert after.body == "after"
    queue.close()
    assert sdk.renewer_closed and sdk.closed


def test_the_client_retries_nothing_so_an_idle_receive_takes_the_agents_one_second() -> None:
    # The SDK's default (three retries, with backoff) turns one empty session accept into ~10 s,
    # and the agent's heartbeats, 2 s apart and stale after 15, would starve.
    sdk = Sdk([])
    azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=sdk)
    assert sdk.retry_total == 0


def test_a_failure_in_passing_is_an_empty_receive_and_a_refusal_is_not() -> None:
    sdk = Sdk([("lakebase-job-1", ["stage"])])
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=sdk,
                                            sleep=lambda _: None)
    sdk.accept_failures.append(Dropped())
    assert queue.receive(1.0) == []
    # A receive on the session that fails in passing lets the session go; the next takes it again.
    sdk.receive_failures.append(LockLost())
    assert queue.receive(1.0) == [] and sdk.released == ["lakebase-job-1"]
    (stage,) = queue.receive(1.0)
    assert stage.body == "stage"
    # The identity lacking the queue's role is not a quiet queue: it ends the agent, and systemd's
    # restarts show it (an AWS agent's AccessDenied does the same).
    sdk.accept_failures.append(Refused())
    queue.release()
    with pytest.raises(Refused):
        queue.receive(1.0)


def test_settling_is_tried_three_times_over_failures_in_passing() -> None:
    slept: list[float] = []
    sdk = Sdk([("lakebase-job-1", ["stage", "release"])])
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=sdk,
                                            sleep=slept.append)
    (stage,) = queue.receive(1.0)
    sdk.settle_failures.extend([Dropped(), Dropped()])
    stage.acknowledge()
    assert sdk.receivers[0].completed == ["stage"] and slept == [0.5, 1.0]
    (release,) = queue.receive(1.0)
    sdk.settle_failures.extend([Dropped(), Dropped(), Dropped()])
    with pytest.raises(Dropped):
        release.acknowledge()
    sdk.settle_failures.append(LockLost())  # not in passing: never sent again
    with pytest.raises(LockLost):
        release.acknowledge()


def test_a_session_held_since_its_last_event_is_read_for_one_second_however_long_the_wait() -> None:
    # A re-warm's preload, then the arm's stage seconds later, each in its job's own session. Read
    # for the pump's 20 s after the preload, the preload's session would have held up the stage.
    sdk = Sdk([("lakebase-warm-1", ["preload"]), ("lakebase-job-2", ["stage"])])
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=sdk)
    (preload,) = queue.receive(20.0)
    preload.acknowledge()
    assert queue.receive(20.0) == [] and sdk.released == ["lakebase-warm-1"]
    (stage,) = queue.receive(20.0)
    assert stage.body == "stage"
    # A session just taken has an event waiting, and is read for the whole wait.
    assert sdk.waits == [("accept", 20.0), ("receive", 20.0), ("receive", 1.0),
                         ("accept", 20.0), ("receive", 20.0)]


def test_no_session_with_events_is_an_empty_receive() -> None:
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=Sdk([]))
    assert queue.receive(1.0) == []
    queue.close()


class SlowQueue:
    """A ServiceBusControlQueue whose receives take as long as Service Bus's did at worst."""

    def __init__(self, events: list[str], *, receive_seconds: float) -> None:
        import threading

        self.events, self.receive_seconds = list(events), receive_seconds
        self.settled: list[tuple[str, object]] = []
        self.receiving = threading.Event()

    def receive(self, wait_seconds: float) -> list[Any]:
        import time

        self.receiving.set()
        time.sleep(self.receive_seconds)
        if not self.events:
            return []
        body = self.events.pop(0)
        return [azure_io.ControlMessage(
            body=body, session_id=f"lakebase-{body}",
            _settle=lambda: self.settled.append((body, "acknowledged")),
            _leave=lambda timeout: self.settled.append((body, timeout)))]


def test_the_agents_receive_never_waits_on_a_slow_service_bus() -> None:
    # Service Bus's session accepts went from 2 s to as much as 35 s at times on 2026-10-21; the
    # agent's heartbeats (2 s apart, stale after 15 s) waited on them, and the App re-warmed
    # Round 5 for a runner it took for lost. The pump does the waiting now.
    import time

    slow = SlowQueue(["stage", "release"], receive_seconds=0.3)
    pumped = azure_io.PumpedControlQueue(slow, wait_seconds=0.3)
    try:
        started = time.monotonic()
        assert pumped.receive(0.05) == []  # the agent's long poll ends on time; it heartbeats
        assert time.monotonic() - started < 0.25
        (stage,) = _received(pumped)
        assert stage.body == "stage"
        # One event at a time: the next isn't received until the agent decides on this one.
        time.sleep(0.5)
        assert slow.events == ["release"]
        stage.acknowledge()  # returns at once; the pump settles it before receiving again
        (release,) = _received(pumped)
        assert release.body == "release" and slow.settled == [("stage", "acknowledged")]
        release.leave(720.0)
        _received(pumped, expect_none=True)
        assert slow.settled[-1] == ("release", 720.0)
    finally:
        pumped.close()


def test_an_empty_receive_that_returns_at_once_is_paced_not_spun() -> None:
    # A receive that fails fast in passing (a dropped link) came back at once; the pump went
    # straight back to it, in a spin. An empty receive takes at least the agent's 1 s.
    import time

    receives: list[float] = []

    class Empty:
        def receive(self, wait_seconds: float) -> list[Any]:
            receives.append(time.monotonic())
            return []

    pumped = azure_io.PumpedControlQueue(Empty(), wait_seconds=0.01)
    try:
        time.sleep(1.5)
    finally:
        pumped.close()
    assert 1 <= len(receives) <= 2


def test_a_refusal_on_the_pump_reaches_the_agent_and_ends_it() -> None:
    class Refusing:
        def receive(self, wait_seconds: float) -> list[Any]:
            raise Refused("the identity lacks the queue's role")

    pumped = azure_io.PumpedControlQueue(Refusing(), wait_seconds=0.01)
    with pytest.raises(Refused):
        _received(pumped)


def _received(pumped: Any, *, expect_none: bool = False) -> list[Any]:
    import time

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        got = pumped.receive(0.05)
        if got:
            assert not expect_none
            return got
    assert expect_none, "the pump handed nothing over"
    return []


def test_a_slow_service_bus_call_is_journaled_by_name_and_a_quick_one_is_not() -> None:
    # The first Azure bout's Lakebase runner acted on a RELEASE 22 s after it was sent, and printed
    # nothing to say where the time went (2026-10-20).
    now, lines = [0.0], []
    # An idle accept takes about a second more than its wait (2.05 s for the agent's 1 s, the
    # second live bout): ordinary, so never journaled.
    durations = [21.9, 2.05, 21.9]
    sdk = Sdk([("lakebase-job-1", ["release"])])
    accept = sdk.ServiceBusClient.get_queue_receiver

    def timed_accept(self: Any, *args: Any, **kwargs: Any) -> Receiver:
        now[0] += durations.pop(0)
        return accept(self, *args, **kwargs)

    sdk.ServiceBusClient.get_queue_receiver = timed_accept  # type: ignore[method-assign]
    queue = azure_io.ServiceBusControlQueue("sb-test", "r5-runner-0", object(), sdk=sdk,
                                            clock=lambda: now[0], report=lines.append)
    (release,) = queue.receive(1.0)
    release.acknowledge()
    assert lines == ["SERVICE_BUS_SLOW:accept:21.900"]
    # The held session is read once more and let go; then two accepts find no session. Those are
    # timed too: each is the agent's whole receive.
    assert [queue.receive(1.0) for _ in range(3)] == [[], [], []]
    assert lines[1:] == ["SERVICE_BUS_SLOW:accept:21.900"] and durations == []
