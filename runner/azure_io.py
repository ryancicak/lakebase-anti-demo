"""The runner's Azure I/O: its managed identity, its control secret, and its control queue.

The AWS runner reads its lane's control DSN from Secrets Manager and its control events from an SQS
FIFO queue (boto3). On Azure the same two reads are Key Vault and a Service Bus session queue, both
as the VM's own managed identity:

- **The identity** comes from IMDS, at a link-local address no other host reaches. It is an
  azure-core credential (`get_token`), so the Service Bus SDK takes it as is, and the runner needs
  neither azure-identity nor its MSAL and cryptography.
- **The secret** is one HTTPS read. A secret with no value yet reads as None: the installer writes
  it, and an agent started first waits for it, as the AWS agent waits out ResourceNotFound.
- **The queue** carries SQS FIFO's guarantees: one session per job (the App sends with session ID
  `<lane>-<job>`, SQS's message group), so a job's events arrive in order; the queue's duplicate
  detection on the event ID; and a dead-letter queue after three deliveries. A session is read
  while its events keep coming and let go at the first receive that finds none within the
  agent's 1 s, so a job's next event (the bell's, seconds or minutes after its stage) comes with
  the next session taken, as a FIFO consumer's next poll takes whichever message group is ready.
- **An event the agent leaves undeleted** (it could not finish it: a refused control connection,
  say) comes back once SQS's visibility timeout passes, and its job's later events wait behind it,
  as a FIFO message group waits on its message in flight. Other jobs' events flow meanwhile.
"""

from __future__ import annotations

import contextlib
import json
import queue as queue_module
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

IMDS_TOKEN_URL = "http://169.254.169.254/metadata/identity/oauth2/token"
KEY_VAULT_RESOURCE = "https://vault.azure.net"
SERVICE_BUS_RESOURCE = "https://servicebus.azure.net"
KEY_VAULT_API = "7.4"
#: A token this close to expiring is fetched again; IMDS's are good for hours.
TOKEN_REFRESH_MARGIN_SECONDS = 300.0
#: How long a held session's lock is kept renewed. The agent completes a message only once it is
#: done with it (SQS gave it a 720 s visibility timeout), and a Service Bus lock lasts 300 s. It
#: covers an event left undeleted until it is dead-lettered (MAX_RECEIVES x 720 s).
LOCK_RENEWAL_SECONDS = 3600.0
#: How many times an event is received and left before it is dead-lettered: the AWS edition's
#: control queues' maxReceiveCount.
MAX_RECEIVES = 3
#: The SDK sends a failed call again three times, with backoff, by default. A session accept that
#: finds no session with events fails that way, so every idle receive would take about 10 s where
#: the agent long-polls for 1 s, and its heartbeats (2 s apart, stale after 15 s) would starve. So
#: the client retries nothing; an accept or receive that fails in passing is an empty receive, as
#: boto3 rides out a failed long poll, and settling is tried this many times.
SETTLE_ATTEMPTS = 3
SETTLE_RETRY_SECONDS = 0.5
#: A Service Bus call this much slower than the wait it was given is written to the journal,
#: named. The first Azure bout's Lakebase runner acted on a RELEASE 22 s after it was sent
#: (2026-10-20), and printed nothing to say where those 22 s went. An idle session accept takes
#: about a second more than its wait (2.05 s for the agent's 1 s, the second bout), so the
#: allowance is over the wait, not from zero.
SLOW_CALL_SECONDS = 2.0
#: How long the pump's own receives wait (`PumpedControlQueue`). Each receive the queue makes
#: while idle is a session accept, each on a new AMQP connection (azure-servicebus 7.15 shares
#: none): at the agent's 1 s that was one every two seconds all night. Off the agent's loop, the
#: wait can be long, and the connections ten times fewer.
PUMP_WAIT_SECONDS = 20.0
#: The least an empty receive on the pump takes: the agent's own long poll.
PUMP_MIN_IDLE_SECONDS = 1.0
#: How long a session held since its last event is read for the job's next one before it is let
#: go: the agent's own long poll, however long the receive may wait. No other job's event can be
#: taken while it is held, and read for the pump's 20 s, a re-warm's session would have held up
#: the arm's stage, sent seconds later in a session of its own. A session just taken has an event
#: waiting, and is read for the whole wait.
HELD_SESSION_WAIT_SECONDS = 1.0

Opener = Callable[..., Any]


def report_slow(line: str) -> None:
    print(line, file=sys.stderr, flush=True)


@dataclass(frozen=True)
class AccessToken:
    """azure-core's AccessToken shape: the token and its expiry in epoch seconds."""

    token: str
    expires_on: int


class ImdsCredential:
    """The VM's system-assigned managed identity, as an azure-core credential."""

    def __init__(self, *, opener: Opener = urllib.request.urlopen,
                 clock: Callable[[], float] = time.time) -> None:
        self._open = opener
        self._clock = clock
        self._tokens: dict[str, AccessToken] = {}

    def get_token(self, *scopes: str, **_kwargs: Any) -> AccessToken:
        if len(scopes) != 1:
            raise ValueError("IMDS issues a token for exactly one resource")
        resource = scopes[0].removesuffix("/.default")
        held = self._tokens.get(resource)
        if held is not None and held.expires_on - self._clock() > TOKEN_REFRESH_MARGIN_SECONDS:
            return held
        query = urllib.parse.urlencode({"api-version": "2018-02-01", "resource": resource})
        request = urllib.request.Request(f"{IMDS_TOKEN_URL}?{query}", headers={"Metadata": "true"})
        with self._open(request, timeout=10) as response:
            body = json.loads(response.read())
        token = AccessToken(str(body["access_token"]), int(body["expires_on"]))
        self._tokens[resource] = token
        return token


def key_vault_secret(vault_uri: str, name: str, credential: ImdsCredential, *,
                     opener: Opener = urllib.request.urlopen) -> str | None:
    """The secret's current value, or None while it has no value yet."""

    token = credential.get_token(f"{KEY_VAULT_RESOURCE}/.default").token
    url = (f"{vault_uri.rstrip('/')}/secrets/{urllib.parse.quote(name, safe='')}"
           f"?api-version={KEY_VAULT_API}")
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with opener(request, timeout=10) as response:
            return str(json.loads(response.read()).get("value") or "")
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


@dataclass
class ControlMessage:
    """One control event as received: its body, and the two ways the agent is done with it."""

    body: str
    session_id: str
    _settle: Callable[[], None]
    _leave: Callable[[float], None]

    def acknowledge(self) -> None:
        """SQS's DeleteMessage: the event is done and is never delivered again."""

        self._settle()

    def leave(self, visibility_timeout: float) -> None:
        """Not deleted: handed out again once `visibility_timeout` passes, or dead-lettered."""

        self._leave(visibility_timeout)


@dataclass
class _HeldBack:
    """A session held aside behind the event the agent left in it."""

    receiver: Any
    message: Any
    receives: int
    due: float


class ServiceBusControlQueue:
    """A lane's control queue: SQS FIFO's ReceiveMessage and DeleteMessage, on sessions.

    `receive` returns at most one message, as the AWS agent asks SQS for one. A left event whose
    visibility timeout is over comes first; then the session being read; when that has nothing
    within the wait, it is released and the next session with messages is taken, as a FIFO consumer
    moves to the next message group. A session held back is never offered to this receiver again
    while it is held, so none of its later events can overtake the one left in it.
    """

    def __init__(self, namespace: str, queue: str, credential: Any, *, sdk: Any = None,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep,
                 report: Callable[[str], None] = report_slow) -> None:
        if sdk is None:
            # The runner's venv carries it (requirements-round5-azure.txt); the App never imports
            # this module.
            import azure.servicebus as sdk
            import azure.servicebus.exceptions  # noqa: F401 - `sdk.exceptions` below
        self._sdk = sdk
        self._clock = clock
        self._sleep = sleep
        self._report = report
        errors = sdk.exceptions
        #: What fails in passing: the connection, the service, a lock that lapsed. Refusals (the
        #: identity's role, a missing queue) are not here: they end the agent, and systemd's
        #: restarts show them, as an AWS agent's AccessDenied does.
        self._passing = (errors.ServiceBusConnectionError, errors.ServiceBusCommunicationError,
                         errors.ServiceBusServerBusyError, errors.OperationTimeoutError)
        self._client = sdk.ServiceBusClient(
            fully_qualified_namespace=f"{namespace}.servicebus.windows.net",
            credential=credential, retry_total=0)
        self._queue = queue
        self._receiver: Any = None
        self._held_back: list[_HeldBack] = []
        self._renewer = sdk.AutoLockRenewer(max_lock_renewal_duration=LOCK_RENEWAL_SECONDS)

    @contextlib.contextmanager
    def _timed(self, name: str, *, wait_seconds: float = 0.0) -> Iterator[None]:
        """Journal the call inside, named, when it takes `SLOW_CALL_SECONDS` over its wait."""

        started = self._clock()
        try:
            yield
        finally:
            seconds = self._clock() - started
            if seconds >= wait_seconds + SLOW_CALL_SECONDS:
                self._report(f"SERVICE_BUS_SLOW:{name}:{seconds:.3f}")

    def _next_session(self, wait_seconds: float) -> Any:
        errors = self._sdk.exceptions
        try:
            with self._timed("accept", wait_seconds=wait_seconds):
                receiver = self._client.get_queue_receiver(
                    self._queue, session_id=self._sdk.NEXT_AVAILABLE_SESSION,
                    max_wait_time=wait_seconds, prefetch_count=0)
                receiver.__enter__()
        except (*self._passing, errors.SessionCannotBeLockedError):
            # No session has messages within the wait (or another receiver holds the one that
            # does, or the accept failed in passing): an empty receive, as SQS's long poll that
            # returns nothing.
            return None
        # Held while the agent works on its message, which it completes only once done.
        self._renewer.register(receiver, receiver.session,
                               max_lock_renewal_duration=LOCK_RENEWAL_SECONDS)
        return receiver

    def receive(self, wait_seconds: float) -> list[ControlMessage]:
        now = self._clock()
        for held in list(self._held_back):
            if held.due > now:
                continue
            self._held_back.remove(held)
            if held.receives < MAX_RECEIVES:
                return [self._handed(held.receiver, held.message, held.receives + 1)]
            # Received its last time and left again: SQS's redrive moves it once this visibility
            # timeout passes, and the job's later events come with the next session taken.
            self._settled(lambda held=held: held.receiver.dead_letter_message(
                held.message, reason="MaxReceivesExceeded",
                error_description=f"received {held.receives} times and never deleted"))
            self._let_go(held.receiver)
        taken = self._receiver is None
        if taken:
            self._receiver = self._next_session(wait_seconds)
            if self._receiver is None:
                return []
        receiver = self._receiver
        wait = wait_seconds if taken else min(wait_seconds, HELD_SESSION_WAIT_SECONDS)
        try:
            with self._timed("receive", wait_seconds=wait):
                messages = receiver.receive_messages(max_message_count=1, max_wait_time=wait)
        except (*self._passing, self._sdk.exceptions.SessionLockLostError):
            messages = []  # the session is taken again on the next receive
        if not messages:
            self.release()
            return []
        return [self._handed(receiver, messages[0], 1)]

    def _settled(self, settle: Callable[[], None]) -> None:
        with self._timed("settle"):
            for attempt in range(SETTLE_ATTEMPTS):
                try:
                    settle()
                    return
                except self._passing:
                    if attempt == SETTLE_ATTEMPTS - 1:
                        raise
                    self._sleep(SETTLE_RETRY_SECONDS * 2 ** attempt)

    def _handed(self, receiver: Any, message: Any, receives: int) -> ControlMessage:
        def settle() -> None:
            self._settled(lambda: receiver.complete_message(message))
            self._done_with(receiver)

        def leave(visibility_timeout: float) -> None:
            if receiver is self._receiver:
                self._receiver = None  # held back, not read: the next receive takes another session
            self._held_back.append(
                _HeldBack(receiver, message, receives, self._clock() + visibility_timeout))

        return ControlMessage(body=str(message), session_id=str(receiver.session.session_id),
                              _settle=settle, _leave=leave)

    def _done_with(self, receiver: Any) -> None:
        # A session held back has nothing ahead of its later events once the left one is settled:
        # let it go, and they come with the next session taken.
        if receiver is not self._receiver:
            self._let_go(receiver)

    def _let_go(self, receiver: Any) -> None:
        try:
            with self._timed("close"):
                receiver.__exit__(None, None, None)
        except self._passing:
            pass  # a link already broken: its lock lapses on its own

    def release(self) -> None:
        """Let the session go, so its next events can be taken by whichever receive comes."""

        receiver, self._receiver = self._receiver, None
        if receiver is not None:
            self._let_go(receiver)

    def close(self) -> None:
        # A left event's session let go here hands it to the next runner to start, as a restarted
        # AWS agent finds an in-flight message once its visibility timeout passes.
        self.release()
        held_back, self._held_back = self._held_back, []
        for held in held_back:
            self._let_go(held.receiver)
        self._renewer.close()
        self._client.close()


class PumpedControlQueue:
    """`ServiceBusControlQueue` on a thread of its own, so the agent's loop never waits on it.

    The resident agent receives, settles and heartbeats on one loop, and its heartbeats (2 s apart,
    stale after 15 s) wait while it receives. Service Bus's session accepts went from 2 s to as
    much as 35 s at times on 2026-10-21, on both runners at once, and a settle took 15.7 s. The App
    took the stalled heartbeats for a lost runner and re-warmed Round 5, and a bout's release waited
    22 s on one.

    So the queue's calls run on a pump thread. The agent's `receive` waits only its own wait for
    what the pump has handed over, and its acknowledge or leave is the pump's next job. The pump
    hands over one event and does nothing else until the agent decides on it, so the queue's
    ordering and held-back sessions are exactly as before, and no SDK object is used by two threads
    at once. An event settled after the agent moved on is safe: a redelivery is one the agent has
    already consumed, by its event ID. A refusal the queue raises reaches the agent at its next
    receive, and ends it as before.
    """

    def __init__(self, queue: Any, *, wait_seconds: float = PUMP_WAIT_SECONDS,
                 start: bool = True) -> None:
        self._queue = queue
        self._wait = wait_seconds
        self._handed: queue_module.Queue[Any] = queue_module.Queue(maxsize=1)
        self._decisions: queue_module.Queue[tuple[str, float]] = queue_module.Queue(maxsize=1)
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._pump, name="service-bus-pump", daemon=True)
        if start:
            self._thread.start()

    def _pump(self) -> None:
        while not self._stopped.is_set():
            started = time.monotonic()
            try:
                messages = self._queue.receive(self._wait)
                for message in messages:
                    self._handed.put(ControlMessage(
                        body=message.body, session_id=message.session_id,
                        _settle=lambda: self._decisions.put(("acknowledge", 0.0)),
                        _leave=lambda timeout: self._decisions.put(("leave", timeout))))
                    decision, timeout = self._decisions.get()
                    if decision == "acknowledge":
                        message.acknowledge()
                    else:
                        message.leave(timeout)
            except BaseException as error:  # noqa: BLE001 - handed to the agent, which ends
                self._handed.put(error)
                return
            if not messages:
                # An empty receive that came back at once (a link dropped in passing, say) is paced
                # as the agent's own long poll is, never retried in a spin.
                rest = PUMP_MIN_IDLE_SECONDS - (time.monotonic() - started)
                if rest > 0:
                    self._stopped.wait(rest)

    def receive(self, wait_seconds: float) -> list[ControlMessage]:
        try:
            item = self._handed.get(timeout=wait_seconds)
        except queue_module.Empty:
            return []
        if isinstance(item, BaseException):
            raise item
        return [item]

    def close(self) -> None:
        # The pump may be inside a receive; it is a daemon, and ends with the agent's process.
        self._stopped.set()


def pumped_control_queue(namespace: str, queue: str, credential: Any) -> PumpedControlQueue:
    """The runner's control queue: `ServiceBusControlQueue`, pumped off the agent's loop."""

    return PumpedControlQueue(ServiceBusControlQueue(namespace, queue, credential))
