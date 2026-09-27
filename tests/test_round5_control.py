from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import inspect
import json
import logging
import queue
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from runner import connection_spike_runner as runner
from server import lifecycle
from server.connection_spike_live import (
    LiveConnectionSpikeAdapter,
    LiveConnectionSpikeEngine,
)
from server.round5_control import (
    ROUND5_ORPHANED_RELEASE_TTL_SECONDS,
    InMemoryRound5ControlStore,
    LakebaseRound5ControlStore,
    Round5ControlBinding,
    Round5ControlDispatcher,
    Round5ControlEvent,
    Round5ControlKind,
    Round5ResidentTransport,
    Round5RunnerEvent,
    Round5RunnerEventKind,
    _verified_runner_event,
)
from server.round5_warm import LakebaseRound5WarmStore, Round5WarmCoordinator


def canonical_request(**values: object) -> dict[str, object]:
    request = {"protocol": runner.fanin.PROTOCOL, **values}
    request["prepared_request_digest"] = hashlib.sha256(
        json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return request


def binding(
    *,
    lane_id: str = "lakebase",
    job_id: str = "a" * 64,
    claim_bound: bool = True,
    process_boot_id: str = "process-current",
    request: dict[str, object] | None = None,
) -> Round5ControlBinding:
    request = request or canonical_request()
    return Round5ControlBinding(
        installation_id="installation-one",
        lane_id=lane_id,
        generation=9,
        warm_attempt_token="attempt-nine",
        claim_id="claim-one" if claim_bound else None,
        bout_id="bout-one" if claim_bound else None,
        bell_id="bell-one" if claim_bound else None,
        fence=7 if claim_bound else 0,
        job_id=job_id,
        runner_boot_id="runner-boot-one",
        runner_process_boot_id=process_boot_id,
        runner_harness_sha256="b" * 64,
        request_sha256=str(request["prepared_request_digest"]),
    )


class DurableControlDb:
    """Stateful cursor fake for the durable outbox's PostgreSQL statements."""

    CONTROL_COLUMNS = {
        "round5_control_outbox_v3": {
            "event_id",
            "installation_id",
            "lane_id",
            "generation",
            "warm_attempt_token",
            "job_id",
            "sequence",
            "kind",
            "payload",
            "created_at",
            "published_at",
        },
        "round5_runner_event_v3": {
            "event_id",
            "installation_id",
            "lane_id",
            "generation",
            "warm_attempt_token",
            "job_id",
            "sequence",
            "kind",
            "binding",
            "payload",
            "occurred_at",
        },
    }

    def __init__(self) -> None:
        self.outbox: dict[str, dict[str, object]] = {}
        self.transactions: list[list[tuple[str, object]]] = []
        self.revalidation_started: asyncio.Event | None = None
        self.finish_revalidation: asyncio.Event | None = None
        self.fail_expiry = False

    def seed(
        self,
        event: Round5ControlEvent,
        *,
        published_at: datetime | None = None,
    ) -> None:
        self.outbox[event.event_id] = {
            "event": event,
            "published_at": published_at,
        }

    async def run(self, operation):
        transaction: list[tuple[str, object]] = []
        self.transactions.append(transaction)
        return await operation(self.Cursor(self, transaction))

    class Cursor:
        def __init__(
            self,
            database: DurableControlDb,
            transaction: list[tuple[str, object]],
        ) -> None:
            self.database = database
            self.transaction = transaction
            self.rows: list[tuple[object, ...]] = []

        async def execute(self, statement, parameters=None):
            text = str(statement)
            normalized = " ".join(text.split())
            self.transaction.append((normalized, parameters))
            self.rows = []

            if "SELECT 1 FROM pg_catalog.pg_namespace" in text:
                self.rows = [(1,)]
                return
            if "FROM pg_catalog.pg_class" in text:
                self.rows = [(str(name),) for name in parameters[1]]
                return
            if "FROM information_schema.columns" in text:
                self.rows = [
                    (table, column)
                    for table, columns in DurableControlDb.CONTROL_COLUMNS.items()
                    for column in columns
                ]
                return
            if "UPDATE" in text and " AS release" in text and " AS cancel" in text:
                release_kind, cancel_kind = parameters
                for release_row in self.database.outbox.values():
                    release = release_row["event"]
                    if (
                        release.kind != release_kind
                        or release_row["published_at"] is not None
                    ):
                        continue
                    matching = next(
                        (
                            candidate["event"]
                            for candidate in self.database.outbox.values()
                            if candidate["event"].kind == cancel_kind
                            and candidate["event"].binding == release.binding
                        ),
                        None,
                    )
                    if matching is not None:
                        release_row["published_at"] = matching.created_at
                return
            if normalized.startswith("INSERT INTO") and "round5_control_outbox_v3" in text:
                (
                    event_id,
                    _installation_id,
                    _lane_id,
                    _generation,
                    _warm_attempt_token,
                    _job_id,
                    _sequence,
                    _kind,
                    payload,
                    _created_at,
                ) = parameters
                event = Round5ControlEvent.from_wire(json.loads(payload))
                existing = self.database.outbox.get(event_id)
                if existing is None:
                    self.database.seed(event)
                elif existing["event"] != event:
                    self.rows = []
                    return
                self.rows = [(event_id,)]
                return
            if (
                normalized.startswith("UPDATE")
                and "SET published_at = COALESCE(published_at, %s)" in text
                and "WHERE job_id = %s" in text
            ):
                published_at, job_id, release_kind = parameters
                for row in self.database.outbox.values():
                    event = row["event"]
                    if (
                        event.job_id == job_id
                        and event.kind == release_kind
                        and row["published_at"] is None
                    ):
                        row["published_at"] = published_at
                return
            if normalized.startswith("SELECT payload") and "published_at IS NULL" in text:
                release_kind, allowed_release_ids, limit = parameters
                allowed = frozenset(allowed_release_ids)
                events = [
                    row["event"]
                    for row in self.database.outbox.values()
                    if row["published_at"] is None
                    and (
                        row["event"].kind != release_kind
                        or row["event"].event_id in allowed
                    )
                ]
                events.sort(
                    key=lambda event: (
                        event.installation_id,
                        event.lane_id,
                        event.generation,
                        event.binding.warm_attempt_token,
                        event.job_id,
                        event.sequence,
                        event.created_at,
                        event.event_id,
                    )
                )
                self.rows = [(event.wire_value(),) for event in events[:limit]]
                return
            if "SELECT EXISTS" in text and "release.event_id = %s" in text:
                if self.database.revalidation_started is not None:
                    self.database.revalidation_started.set()
                if self.database.finish_revalidation is not None:
                    await self.database.finish_revalidation.wait()
                event_id, release_kind, cancel_kind = parameters
                row = self.database.outbox.get(event_id)
                dispatchable = False
                if row is not None:
                    release = row["event"]
                    dispatchable = (
                        release.kind == release_kind
                        and row["published_at"] is None
                        and not any(
                            candidate["event"].kind == cancel_kind
                            and candidate["event"].binding == release.binding
                            for candidate in self.database.outbox.values()
                        )
                    )
                self.rows = [(dispatchable,)]
                return
            if (
                normalized.startswith("UPDATE")
                and "created_at <= %s" in text
                and "RETURNING event_id" in text
            ):
                if self.database.fail_expiry:
                    raise RuntimeError("coordination unavailable")
                expired_at, release_kind, created_before, protected_ids = parameters
                protected = frozenset(protected_ids)
                expired: list[str] = []
                for event_id, row in self.database.outbox.items():
                    event = row["event"]
                    if (
                        event.kind == release_kind
                        and row["published_at"] is None
                        and event.created_at <= created_before
                        and event_id not in protected
                    ):
                        row["published_at"] = expired_at
                        expired.append(event_id)
                self.rows = [(event_id,) for event_id in expired]
                return
            if (
                normalized.startswith("UPDATE")
                and "SET published_at = COALESCE(published_at, %s)" in text
                and "WHERE event_id = %s" in text
            ):
                published_at, event_id = parameters
                row = self.database.outbox.get(event_id)
                if row is not None and row["published_at"] is None:
                    row["published_at"] = published_at
                return
            raise AssertionError(f"unexpected durable control SQL: {normalized}")

        async def fetchone(self):
            return self.rows[0] if self.rows else None

        async def fetchall(self):
            return self.rows


async def test_transactional_outbox_preserves_stage_release_fifo_order() -> None:
    store = InMemoryRound5ControlStore()
    sent: list[Round5ControlEvent] = []
    sent_all = asyncio.Event()

    async def send(event: Round5ControlEvent) -> None:
        sent.append(event)
        if len(sent) >= 2:
            sent_all.set()

    dispatcher = Round5ControlDispatcher(store, send)
    transport = Round5ResidentTransport(store, dispatcher)
    request = canonical_request()
    event_binding = binding(lane_id="competitor", request=request)
    operation = asyncio.create_task(
        transport.stage_and_release(
            binding=event_binding,
            request=request,
        )
    )
    await asyncio.sleep(0)
    assert store.outbox
    assert not operation.done()
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="9" * 64,
            binding=event_binding,
            sequence=1,
            kind=Round5RunnerEventKind.PREPARED,
            occurred_at=datetime.now(UTC),
            payload={
                "state": "prepared",
                "worker_ready_count": 4,
                "request_sha256": event_binding.request_sha256,
            },
        )
    )
    await asyncio.wait_for(operation, timeout=1)
    await asyncio.wait_for(sent_all.wait(), timeout=1)
    assert [event.kind for event in sent] == [
        Round5ControlKind.STAGE,
        Round5ControlKind.RELEASE,
    ]
    assert [event.sequence for event in sent] == [1, 2]
    await dispatcher.close()


async def test_durable_release_cannot_publish_before_its_process_gate_opens() -> None:
    store = InMemoryRound5ControlStore()
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
    )
    event = Round5ControlEvent.create(
        binding=binding(),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        payload={"bell_id": "bell-one"},
    )
    await store.enqueue(event)

    assert await dispatcher.publish_once() == 0
    assert sent == []
    assert store.outbox[event.event_id][1] is None

    dispatcher.allow_release(event.event_id)
    assert await dispatcher.publish_once() == 1
    assert sent == [event]
    assert store.outbox[event.event_id][1] is not None


async def test_multiple_enqueues_initialize_legacy_repair_once() -> None:
    class CountingStore(InMemoryRound5ControlStore):
        def __init__(self) -> None:
            super().__init__()
            self.initialize_calls = 0

        async def initialize(self) -> None:
            self.initialize_calls += 1

    store = CountingStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(store, dispatcher)
    request = canonical_request()

    await asyncio.gather(
        *(
            transport.preload(
                binding=binding(
                    job_id=f"{index:064x}",
                    claim_bound=False,
                    request=request,
                ),
                request=request,
            )
            for index in range(8)
        )
    )

    assert store.initialize_calls == 1
    await dispatcher.close()


async def test_cancel_discards_an_allowed_release_after_send_failure() -> None:
    class RecordingStore(InMemoryRound5ControlStore):
        def __init__(self) -> None:
            super().__init__()
            self.allowed_scans: list[frozenset[str]] = []

        async def pending(
            self,
            limit: int = 32,
            *,
            allowed_release_ids=(),
        ) -> tuple[Round5ControlEvent, ...]:
            self.allowed_scans.append(frozenset(allowed_release_ids))
            return await super().pending(
                limit,
                allowed_release_ids=allowed_release_ids,
            )

    store = RecordingStore()

    async def send(event: Round5ControlEvent) -> None:
        if event.kind == Round5ControlKind.RELEASE:
            raise RuntimeError("injected release delivery failure")

    dispatcher = Round5ControlDispatcher(store, send)
    transport = Round5ResidentTransport(store, dispatcher)
    event_binding = binding()
    release = Round5ControlEvent.create(
        binding=event_binding,
        sequence=2,
        kind=Round5ControlKind.RELEASE,
    )
    await store.enqueue(release)
    dispatcher.allow_release(release.event_id)

    with pytest.raises(RuntimeError, match="injected release delivery failure"):
        await dispatcher.publish_once()
    assert release.event_id in dispatcher._allowed_releases

    await transport.cancel(binding=event_binding, await_settlement=False)

    assert release.event_id not in dispatcher._allowed_releases
    await dispatcher.publish_once()
    assert release.event_id not in store.allowed_scans[-1]
    await dispatcher.close()


async def test_cancel_during_release_send_is_durably_rejected_before_runner_launch() -> None:
    store = InMemoryRound5ControlStore()
    send_started = asyncio.Event()
    unblock_send = asyncio.Event()
    launched: list[Round5ControlEvent] = []
    rejected: list[Round5ControlEvent] = []

    async def send(event: Round5ControlEvent) -> None:
        if event.kind != Round5ControlKind.RELEASE:
            return
        send_started.set()
        await unblock_send.wait()
        # SQS send cannot be revoked after it starts. The resident's durable
        # disposition fence applies this same test when the body arrives.
        if await store.release_dispatchable(event.event_id):
            launched.append(event)
        else:
            rejected.append(event)

    dispatcher = Round5ControlDispatcher(store, send)
    event_binding = binding()
    release = Round5ControlEvent.create(
        binding=event_binding,
        sequence=2,
        kind=Round5ControlKind.RELEASE,
    )
    await store.enqueue(release)
    dispatcher.allow_release(release.event_id)

    publishing = asyncio.create_task(dispatcher.publish_once())
    await asyncio.wait_for(send_started.wait(), timeout=1)
    cancel = Round5ControlEvent.create(
        binding=event_binding,
        sequence=3,
        kind=Round5ControlKind.CANCEL,
    )
    await store.enqueue(cancel)
    dispatcher.discard_release(release.event_id)
    unblock_send.set()

    assert await asyncio.wait_for(publishing, timeout=1) == 1
    assert launched == []
    assert rejected == [release]
    assert not await store.release_dispatchable(release.event_id)


async def test_cancel_supersedes_gated_release_without_outbox_debt() -> None:
    store = InMemoryRound5ControlStore()
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
    )

    for index in range(33):
        event_binding = binding(job_id=f"{index:064x}")
        await store.enqueue(
            Round5ControlEvent.create(
                binding=event_binding,
                sequence=2,
                kind=Round5ControlKind.RELEASE,
            )
        )
        await store.enqueue(
            Round5ControlEvent.create(
                binding=event_binding,
                sequence=3,
                kind=Round5ControlKind.CANCEL,
            )
        )

    deliverable = Round5ControlEvent.create(
        binding=binding(job_id="f" * 64),
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": canonical_request()},
    )
    await store.enqueue(deliverable)

    assert all(event.kind != Round5ControlKind.RELEASE for event in await store.pending())
    assert await dispatcher.publish_once() == 32
    assert await dispatcher.publish_once() == 2
    assert deliverable in sent
    assert not await store.pending()


async def test_unpublished_releases_from_crashed_bells_do_not_starve_current_generation() -> None:
    store = InMemoryRound5ControlStore()
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
    )

    leftover: list[Round5ControlEvent] = []
    for bell in range(16):
        for lane_id, lane_nibble in (("lakebase", "0"), ("competitor", "1")):
            event_binding = binding(
                lane_id=lane_id,
                job_id=f"{bell:02x}{lane_nibble}{'a' * 61}",
            )
            event = Round5ControlEvent.create(
                binding=event_binding,
                sequence=2,
                kind=Round5ControlKind.RELEASE,
            )
            leftover.append(event)
            await store.enqueue(event)

    assert len(leftover) == 32
    request = canonical_request()
    preload = Round5ControlEvent.create(
        binding=binding(job_id="f" * 64, claim_bound=False, request=request),
        sequence=1,
        kind=Round5ControlKind.PRELOAD,
        payload={"request": request},
    )
    release = Round5ControlEvent.create(
        binding=binding(job_id="f" * 64, request=request),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
    )
    await store.enqueue(preload)
    await store.enqueue(release)

    assert await dispatcher.publish_once() == 1
    assert sent == [preload]
    assert all(store.outbox[event.event_id][1] is None for event in leftover)
    assert store.outbox[release.event_id][1] is None

    dispatcher.allow_release(release.event_id)
    assert await dispatcher.publish_once() == 1
    assert sent == [preload, release]
    assert store.outbox[release.event_id][1] is not None
    assert all(store.outbox[event.event_id][1] is None for event in leftover)


async def test_orphaned_release_is_tombstoned_after_bout_deadline() -> None:
    created_at = datetime(2026, 9, 18, tzinfo=UTC)
    now = created_at + timedelta(seconds=ROUND5_ORPHANED_RELEASE_TTL_SECONDS + 1)
    store = InMemoryRound5ControlStore()
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
        now=lambda: now,
    )
    release = Round5ControlEvent.create(
        binding=binding(),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=created_at,
    )
    await store.enqueue(release)

    assert await dispatcher.publish_once() == 0
    assert store.outbox[release.event_id][1] == now
    assert not await store.pending(allowed_release_ids={release.event_id})

    # A late process-local bell cannot resurrect a durable crashed-bell tombstone.
    dispatcher.allow_release(release.event_id)
    assert await dispatcher.publish_once() == 0
    assert sent == []


async def test_live_held_release_survives_janitor_until_owner_is_lost() -> None:
    created_at = datetime(2026, 9, 18, tzinfo=UTC)
    clock = {
        "now": created_at + timedelta(seconds=ROUND5_ORPHANED_RELEASE_TTL_SECONDS + 1)
    }
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(
        store,
        lambda _event: asyncio.sleep(0),
        now=lambda: clock["now"],
    )
    release = Round5ControlEvent.create(
        binding=binding(),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=created_at,
    )
    await store.enqueue(release)
    dispatcher.hold_release(release.event_id)

    assert await dispatcher.publish_once() == 0
    assert store.outbox[release.event_id][1] is None

    # Losing the process-local owner converts the same old row into crash debt;
    # the next bounded janitor pass tombstones it.
    dispatcher.discard_release(release.event_id)
    clock["now"] += timedelta(seconds=31)
    assert await dispatcher.publish_once() == 0
    assert store.outbox[release.event_id][1] == clock["now"]


async def test_orphan_reap_failure_does_not_block_delivery_or_retry_at_10hz(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class ReapFailingStore(InMemoryRound5ControlStore):
        def __init__(self) -> None:
            super().__init__()
            self.reap_calls = 0

        async def expire_orphaned_releases(self, **unused) -> tuple[str, ...]:
            self.reap_calls += 1
            raise RuntimeError("coordination unavailable")

    now = datetime(2026, 9, 18, tzinfo=UTC)
    store = ReapFailingStore()
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
        now=lambda: now,
    )
    request = canonical_request()
    stage = Round5ControlEvent.create(
        binding=binding(request=request),
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": request},
    )
    await store.enqueue(stage)
    caplog.set_level(logging.WARNING, logger="server.round5_control")

    assert await dispatcher.publish_once() == 1
    assert await dispatcher.publish_once() == 0
    assert sent == [stage]
    assert store.reap_calls == 1
    assert "round5_control_orphan_reap_failed" in caplog.text


async def test_release_wake_during_outbox_scan_is_not_lost() -> None:
    dispatcher = Round5ControlDispatcher(
        InMemoryRound5ControlStore(),
        lambda _event: asyncio.sleep(0),
    )
    first_scan = asyncio.Event()
    finish_first_scan = asyncio.Event()
    second_scan = asyncio.Event()
    scans = 0

    async def scan() -> int:
        nonlocal scans
        scans += 1
        if scans == 1:
            first_scan.set()
            await finish_first_scan.wait()
        else:
            second_scan.set()
            dispatcher._closed = True
        return 0

    dispatcher.publish_once = scan
    task = asyncio.create_task(dispatcher.run())
    await asyncio.wait_for(first_scan.wait(), timeout=1)
    dispatcher.wake()
    finish_first_scan.set()

    await asyncio.wait_for(second_scan.wait(), timeout=0.05)
    await asyncio.wait_for(task, timeout=1)


async def test_lakebase_bell_binding_opens_durable_release_only_after_t0() -> None:
    store = InMemoryRound5ControlStore()
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
    )
    transport = Round5ResidentTransport(store, dispatcher)
    engine = object.__new__(LiveConnectionSpikeEngine)
    event_binding = binding()
    engine._resident_bindings = {"lakebase": event_binding}
    engine._durable_lakebase_release = None
    engine._lane_adapters = {"lakebase": SimpleNamespace(_resident_transport=transport)}
    event = engine.lakebase_release_event(datetime.now(UTC))
    await store.enqueue(event)

    assert await dispatcher.publish_once() == 0
    engine.bind_bell(
        SimpleNamespace(
            bell_id=event_binding.bell_id,
            claim_id=event_binding.claim_id,
            t0_monotonic_ns=123,
        )
    )
    assert engine._bell_t0_ns == 123
    assert await dispatcher.publish_once() == 1
    assert sent == [event]


async def test_persistent_delivery_failure_reports_only_sanitized_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = InMemoryRound5ControlStore()
    request = canonical_request()
    event = Round5ControlEvent.create(
        binding=binding(claim_bound=False, request=request),
        sequence=1,
        kind=Round5ControlKind.PRELOAD,
        payload={"request": request},
    )
    await store.enqueue(event)
    reasons: list[str] = []

    async def fail(_event: Round5ControlEvent) -> None:
        raise RuntimeError("secret queue and credential detail")

    dispatcher = Round5ControlDispatcher(
        store,
        fail,
        on_persistent_failure=lambda code: asyncio.sleep(
            0,
            result=reasons.append(code),
        ),
        persistent_failure_threshold=2,
        # This test targets sanitization of the persistent-failure reason, not the
        # anti-flicker time window; keep it count-based so two real-row send failures
        # trip the withdrawal. (The window is covered by a dedicated test.)
        persistent_failure_window_seconds=0.0,
    )
    caplog.set_level(logging.WARNING, logger="server.round5_control")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await dispatcher.publish_once()

    assert reasons == ["resident_control_delivery_failed"]
    assert "secret queue and credential detail" not in caplog.text


async def test_outbox_dispatch_failure_is_visible_without_leaking_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A REAL outbox row whose delivery (_send) fails is a genuine publish failure
    # (visible, sanitized). A failed empty-outbox SCAN is deliberately NOT a delivery
    # failure (Fix E) and is covered separately, so this test enqueues a real row and
    # fails the send.
    store = InMemoryRound5ControlStore()
    request = canonical_request()
    event = Round5ControlEvent.create(
        binding=binding(claim_bound=False, request=request),
        sequence=1,
        kind=Round5ControlKind.PRELOAD,
        payload={"request": request},
    )
    await store.enqueue(event)

    async def fail(_event: Round5ControlEvent) -> None:
        raise RuntimeError("secret-provider-detail")

    dispatcher = Round5ControlDispatcher(store, fail)
    caplog.set_level(logging.WARNING, logger="server.round5_control")

    with pytest.raises(RuntimeError):
        await dispatcher.publish_once()

    assert "round5_control_outbox_publish_failed consecutive_failures=1" in caplog.text
    assert "secret-provider-detail" not in caplog.text


async def test_validate_ready_provenance_tri_state_precedence() -> None:
    # B1: exercise the REAL LiveConnectionSpikeEngine.validate_ready_provenance against
    # a boundary transport returning each ResidentLiveness state, and assert the mapping
    # + precedence: all CURRENT -> True; any STALE/ABSENT -> RetryableWarmError (strike);
    # any IDENTITY_CHANGED -> False (demote); IDENTITY_CHANGED beats STALE (identity wins).
    from types import SimpleNamespace

    from server.connection_spike_live import LiveConnectionSpikeEngine
    from server.round5_control import ResidentLiveness
    from server.round5_warm import RetryableWarmError

    def receipt(lane: str) -> SimpleNamespace:
        return SimpleNamespace(
            boot_id=f"boot-{lane}",
            process_boot_id=f"pboot-{lane}",
            process_pid=100,
            loaded_harness_sha256="h",
            capacity_model_sha256="m",
        )

    shared = SimpleNamespace(
        lakebase_runner=receipt("lakebase"),
        competitor_runner=receipt("competitor"),
    )

    def make_engine(states: dict[str, ResidentLiveness]) -> LiveConnectionSpikeEngine:
        class _Transport:
            async def resident_liveness(self, **kw: object) -> ResidentLiveness:
                return states[kw["lane_id"]]

        engine = object.__new__(LiveConnectionSpikeEngine)
        engine._armed = SimpleNamespace(
            preflights={
                lane: SimpleNamespace(
                    boot_id=f"boot-{lane}", runner_harness_sha256="h", model_sha256="m"
                )
                for lane in ("lakebase", "competitor")
            }
        )
        engine._lane_adapters = {
            lane: SimpleNamespace(
                _resident_process_boot_id=f"pboot-{lane}",
                _resident_process_pid=100,
                _resident_transport=_Transport(),
                config=SimpleNamespace(resident_installation_id="inst"),
            )
            for lane in ("lakebase", "competitor")
        }
        return engine

    both_current = {"lakebase": ResidentLiveness.CURRENT, "competitor": ResidentLiveness.CURRENT}
    assert await make_engine(both_current).validate_ready_provenance(shared, "tok") is True

    with pytest.raises(RetryableWarmError):
        await make_engine(
            {"lakebase": ResidentLiveness.CURRENT, "competitor": ResidentLiveness.STALE}
        ).validate_ready_provenance(shared, "tok")
    with pytest.raises(RetryableWarmError):
        await make_engine(
            {"lakebase": ResidentLiveness.ABSENT, "competitor": ResidentLiveness.CURRENT}
        ).validate_ready_provenance(shared, "tok")

    assert (
        await make_engine(
            {"lakebase": ResidentLiveness.IDENTITY_CHANGED, "competitor": ResidentLiveness.CURRENT}
        ).validate_ready_provenance(shared, "tok")
        is False
    )
    # Precedence: an attested identity change on ANY lane wins over a stale lane.
    assert (
        await make_engine(
            {"lakebase": ResidentLiveness.IDENTITY_CHANGED, "competitor": ResidentLiveness.STALE}
        ).validate_ready_provenance(shared, "tok")
        is False
    )


async def test_outbox_scan_failure_does_not_increment_delivery_streak() -> None:
    # B5: a repeatedly-failing outbox SCAN (pending() raises) logs a warning, returns 0,
    # and does NOT increment the delivery-failure streak or invalidate readiness -- there
    # is no real outbox row that failed to deliver. Mutation: counting a scan failure as
    # a delivery failure trips the persistent-failure withdrawal (idle TU flicker).
    class ScanFailStore(InMemoryRound5ControlStore):
        async def pending(self, limit: int = 32, *, allowed_release_ids=()):
            raise RuntimeError("transient-scan-blip")

    invalidated: list[str] = []

    async def on_fail(code: str) -> None:
        invalidated.append(code)

    dispatcher = Round5ControlDispatcher(
        ScanFailStore(),
        lambda _event: asyncio.sleep(0),
        on_persistent_failure=on_fail,
        persistent_failure_threshold=1,
        persistent_failure_window_seconds=0.0,
    )
    for _ in range(5):
        assert await dispatcher.publish_once() == 0
    assert dispatcher._consecutive_failures == 0
    assert invalidated == []


async def test_round5_structured_start_surfaces_revision_and_error() -> None:
    # B4: the machine-readable Round 5 start status exposes the durable warm revision and
    # last error code so a soak can track idle keep-alive health from stable fields.
    from server.manager import RunManager

    status = RunManager._round5_structured_start(
        {
            "round5_start_stage": "rewarming",
            "round5_warm_generation": 28,
            "round5_warm_revision": 58482,
            "round5_warm_last_error_code": "warm_fence_contention",
        }
    )
    assert status is not None
    assert status.generation == 28
    assert status.revision == 58482
    assert status.last_error_code == "warm_fence_contention"
    # Absent/blank values normalize to None rather than leaking a falsy placeholder.
    cleared = RunManager._round5_structured_start(
        {"round5_start_stage": "ready", "round5_warm_revision": None}
    )
    assert cleared is not None
    assert cleared.revision is None
    assert cleared.last_error_code is None


async def test_cleanup_overlay_read_exception_does_not_latch_blocked() -> None:
    # B3/D: a single failed read of the cleanup-fence lease (a transient store blip) must
    # NOT latch _round5_durable_cleanup_blocked or return a Temporarily Unavailable
    # overlay; it returns None (unknown, warm status governs) so the fight card stays
    # startable across board polls. Mutation: latching blocked + returning a TU overlay
    # on the read exception fails this test.
    from server.manager import RunManager

    manager = object.__new__(RunManager)
    manager._round5_durable_cleanup_blocked = False

    class _FailingCleanupStore:
        async def current(self) -> object:
            raise RuntimeError("transient-lease-read-blip")

    manager._round5_cleanup_store = lambda: _FailingCleanupStore()

    overlay = await manager.round5_durable_cleanup_overlay()

    assert overlay is None
    assert manager._round5_durable_cleanup_blocked is False


async def test_outbox_persistent_failure_requires_sustained_window_not_burst() -> None:
    # Fix B/E: withdrawing Round 5 readiness for an outbox failure requires the failure
    # to PERSIST across a real wall-clock window, not just a sub-second burst at the
    # ~10Hz idle poll rate. Mutation: dropping the window (count-only) fires the
    # readiness withdrawal on the burst and reintroduces the idle "Temporarily
    # Unavailable" flicker from a transient store blip.
    invalidated: list[str] = []
    clk = {"t": datetime(2026, 1, 1, tzinfo=UTC)}

    async def on_fail(code: str) -> None:
        invalidated.append(code)

    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(
        store,
        lambda _event: asyncio.sleep(0),
        on_persistent_failure=on_fail,
        persistent_failure_threshold=3,
        persistent_failure_window_seconds=8.0,
        now=lambda: clk["t"],
    )

    # A sub-second burst well past the COUNT threshold must NOT withdraw readiness,
    # because the failure has not persisted across the wall-clock WINDOW.
    for _ in range(12):
        clk["t"] += timedelta(seconds=0.05)
        await dispatcher._record_delivery_failure()
    assert invalidated == []

    # Once the failure has persisted past the window, it withdraws (fails closed).
    clk["t"] += timedelta(seconds=9)
    await dispatcher._record_delivery_failure()
    assert invalidated == ["resident_control_delivery_failed"]

    # Recovery resets the window so a later isolated blip starts fresh.
    dispatcher._record_delivery_success()
    assert dispatcher._first_failure_at is None


async def test_injected_fifteen_second_preparation_finishes_before_stage_returns() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(
        store,
        dispatcher,
        sleep=lambda _delay: asyncio.sleep(0),
    )
    request = canonical_request()
    event_binding = binding(request=request)
    staged = asyncio.create_task(transport.stage(binding=event_binding, request=request))
    await asyncio.sleep(0)
    assert store.outbox
    assert not staged.done()
    started = datetime(2026, 9, 16, tzinfo=UTC)
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="8" * 64,
            binding=event_binding,
            sequence=1,
            kind=Round5RunnerEventKind.PREPARED,
            occurred_at=started + timedelta(seconds=15),
            payload={
                "state": "prepared",
                "worker_ready_count": 4,
                "request_sha256": event_binding.request_sha256,
            },
        )
    )
    await asyncio.wait_for(staged, timeout=1)
    assert (store.events[event_binding.job_id][0].occurred_at - started).total_seconds() == 15


async def test_competitor_can_prepare_exact_request_while_release_stays_closed() -> None:
    store = InMemoryRound5ControlStore()
    sent: list[Round5ControlEvent] = []
    clock = {"now": datetime.now(UTC)}
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
        now=lambda: clock["now"],
    )
    transport = Round5ResidentTransport(
        store,
        dispatcher,
        sleep=lambda _delay: asyncio.sleep(0),
    )
    request = canonical_request(
        resident_generation=9,
        targets=[
            {
                "lane_id": "competitor",
                "endpoint_host": "late-bound.proxy.example.test",
            }
        ],
    )
    event_binding = binding(
        lane_id="competitor",
        job_id="2" * 64,
        request=request,
    )
    staged = asyncio.create_task(
        transport.stage_for_release(
            binding=event_binding,
            request=request,
        )
    )
    await asyncio.sleep(0)
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="3" * 64,
            binding=event_binding,
            sequence=1,
            kind=Round5RunnerEventKind.PREPARED,
            occurred_at=datetime.now(UTC),
            payload={
                "state": "prepared",
                "worker_ready_count": 4,
                "request_sha256": event_binding.request_sha256,
            },
        )
    )
    release = await asyncio.wait_for(staged, timeout=1)
    for _ in range(20):
        if sent:
            break
        await asyncio.sleep(0)

    assert [event.kind for event in sent] == [Round5ControlKind.STAGE]
    assert release.kind == Round5ControlKind.RELEASE
    assert store.outbox[release.event_id][1] is None
    await dispatcher.close()

    # A live competitor gate can remain closed beyond the ordinary bout
    # deadline while RDS Proxy becomes available. stage_for_release wires the
    # row into dispatcher ownership, so age alone cannot tombstone it.
    clock["now"] = release.created_at + timedelta(
        seconds=ROUND5_ORPHANED_RELEASE_TTL_SECONDS + 1
    )
    assert await dispatcher.publish_once() == 0
    assert store.outbox[release.event_id][1] is None


async def test_conflicting_logical_event_reuse_is_rejected() -> None:
    store = InMemoryRound5ControlStore()
    request = canonical_request()
    event_binding = binding(request=request)
    created_at = datetime.now(UTC)
    first = Round5ControlEvent.create(
        binding=event_binding,
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": request},
        created_at=created_at,
    )
    await store.enqueue(first)
    conflicting_request = canonical_request(extra="changed")
    conflicting_binding = binding(request=conflicting_request)
    conflicting = Round5ControlEvent.create(
        binding=conflicting_binding,
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": conflicting_request},
        created_at=created_at,
    )
    with pytest.raises(ValueError, match="logical identity conflict"):
        await store.enqueue(conflicting)


def test_event_identity_hashes_every_canonical_binding_field() -> None:
    request = canonical_request()
    original = binding(request=request)
    created_at = datetime(2026, 9, 16, tzinfo=UTC)
    first = Round5ControlEvent.create(
        binding=original,
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": request},
        created_at=created_at,
    )
    mutations = {
        "installation_id": "installation-two",
        "lane_id": "competitor",
        "generation": 10,
        "warm_attempt_token": "attempt-ten",
        "claim_id": "claim-two",
        "bout_id": "bout-two",
        "bell_id": "bell-two",
        "fence": 8,
        "job_id": "c" * 64,
        "runner_boot_id": "runner-boot-two",
        "runner_process_boot_id": "process-two",
        "runner_harness_sha256": "d" * 64,
    }
    for field, value in mutations.items():
        changed = Round5ControlBinding(**{**original.wire_value(), field: value})
        event = Round5ControlEvent.create(
            binding=changed,
            sequence=1,
            kind=Round5ControlKind.STAGE,
            payload={"request": request},
            created_at=created_at,
        )
        assert event.event_id != first.event_id, field
    changed_request = canonical_request(extra="different")
    changed_binding = Round5ControlBinding(
        **{
            **original.wire_value(),
            "request_sha256": changed_request["prepared_request_digest"],
        }
    )
    changed = Round5ControlEvent.create(
        binding=changed_binding,
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": changed_request},
        created_at=created_at,
    )
    assert changed.event_id != first.event_id


def test_persisted_runner_event_hash_is_verified_before_result_consumption() -> None:
    event_binding = binding()
    occurred_at = datetime(2026, 9, 16, tzinfo=UTC)
    payload = {"winner": "lakebase", "verified": True}
    identity = {
        "binding": event_binding.wire_value(),
        "sequence": 4,
        "kind": "result",
        "occurred_at": occurred_at.isoformat(),
        "payload": payload,
    }
    event_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    verified = _verified_runner_event(
        event_id=event_id,
        binding=event_binding.wire_value(),
        sequence=4,
        kind="result",
        occurred_at=occurred_at,
        payload=payload,
    )
    assert verified.event_id == event_id

    with pytest.raises(ValueError, match="identity changed"):
        _verified_runner_event(
            event_id=event_id,
            binding=event_binding.wire_value(),
            sequence=4,
            kind="result",
            occurred_at=occurred_at,
            payload={**payload, "winner": "competitor"},
        )


def test_resident_never_recomputes_and_blesses_mutated_request_digest() -> None:
    request = canonical_request()
    event_binding = binding(request=request)
    event = Round5ControlEvent.create(
        binding=event_binding,
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": request},
    )
    assert runner._decode_resident_control(event.encoded_body())["event_id"] == event.event_id

    wire = event.wire_value()
    wire["payload"]["request"]["mutated"] = True
    identity = {key: value for key, value in wire.items() if key != "event_id"}
    wire["event_id"] = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    encoded = base64.urlsafe_b64encode(
        gzip.compress(
            json.dumps(wire, sort_keys=True, separators=(",", ":")).encode(),
            mtime=0,
        )
    ).decode()
    with pytest.raises(
        runner.RunnerContractError,
        match="resident_stage_request_digest_invalid",
    ):
        runner._decode_resident_control(encoded)


async def test_durable_dispatcher_initializes_once_and_repairs_only_matching_legacy_debt() -> None:
    database = DurableControlDb()
    created_at = datetime(2026, 9, 18, 12, tzinfo=UTC)
    eligible_binding = binding(job_id="1" * 64)
    eligible_release = Round5ControlEvent.create(
        binding=eligible_binding,
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=created_at,
    )
    eligible_cancel = Round5ControlEvent.create(
        binding=eligible_binding,
        sequence=3,
        kind=Round5ControlKind.CANCEL,
        created_at=created_at + timedelta(seconds=1),
    )
    unmatched_release = Round5ControlEvent.create(
        binding=binding(job_id="2" * 64),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=created_at,
    )
    database.seed(eligible_release)
    database.seed(eligible_cancel)
    database.seed(unmatched_release)

    store = LakebaseRound5ControlStore(database.run)
    dispatcher = Round5ControlDispatcher(
        store,
        lambda _event: asyncio.sleep(0),
        now=lambda: created_at,
    )
    first, second = await asyncio.gather(dispatcher.start(), dispatcher.start())
    assert first is second
    await dispatcher.close()

    repair_statements = [
        (sql, params)
        for transaction in database.transactions
        for sql, params in transaction
        if "round5_control_outbox_v3 AS release" in sql
    ]
    assert len(repair_statements) == 1
    repair_sql, repair_params = repair_statements[0]
    assert repair_params == (
        Round5ControlKind.RELEASE.value,
        Round5ControlKind.CANCEL.value,
    )
    for identity_column in (
        "installation_id",
        "lane_id",
        "generation",
        "warm_attempt_token",
        "job_id",
    ):
        assert f"cancel.{identity_column} = release.{identity_column}" in repair_sql
    assert database.outbox[eligible_release.event_id]["published_at"] == (
        eligible_cancel.created_at
    )
    assert database.outbox[unmatched_release.event_id]["published_at"] is None


async def test_durable_cancel_insert_and_release_tombstone_share_one_transaction() -> None:
    database = DurableControlDb()
    store = LakebaseRound5ControlStore(database.run)
    event_binding = binding(job_id="3" * 64)
    release = Round5ControlEvent.create(
        binding=event_binding,
        sequence=2,
        kind=Round5ControlKind.RELEASE,
    )
    cancel = Round5ControlEvent.create(
        binding=event_binding,
        sequence=3,
        kind=Round5ControlKind.CANCEL,
        created_at=release.created_at + timedelta(seconds=1),
    )

    await store.enqueue(release)
    await store.enqueue(cancel)

    assert database.outbox[release.event_id]["published_at"] == cancel.created_at
    assert database.outbox[cancel.event_id]["published_at"] is None
    cancel_transaction = database.transactions[-1]
    assert len(cancel_transaction) == 2
    insert_sql, insert_params = cancel_transaction[0]
    suppress_sql, suppress_params = cancel_transaction[1]
    assert "INSERT INTO anti_demo_coordination.round5_control_outbox_v3" in insert_sql
    assert insert_params[0] == cancel.event_id
    assert insert_params[7] == Round5ControlKind.CANCEL.value
    assert "SET published_at = COALESCE(published_at, %s)" in suppress_sql
    assert "WHERE job_id = %s" in suppress_sql
    assert suppress_params == (
        cancel.created_at,
        event_binding.job_id,
        Round5ControlKind.RELEASE.value,
    )


async def test_durable_pending_adapts_empty_and_allowed_release_arrays() -> None:
    database = DurableControlDb()
    store = LakebaseRound5ControlStore(database.run)
    request = canonical_request()
    stage = Round5ControlEvent.create(
        binding=binding(job_id="4" * 64, request=request),
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": request},
    )
    release = Round5ControlEvent.create(
        binding=binding(job_id="5" * 64),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
    )
    database.seed(stage)
    database.seed(release)

    assert await store.pending() == (stage,)
    empty_sql, empty_params = database.transactions[-1][0]
    assert "event_id = ANY(%s)" in empty_sql
    assert empty_params == (Round5ControlKind.RELEASE.value, [], 32)

    allowed = await store.pending(allowed_release_ids={release.event_id})
    assert {event.event_id for event in allowed} == {
        stage.event_id,
        release.event_id,
    }
    allowed_sql, allowed_params = database.transactions[-1][0]
    assert "event_id = ANY(%s)" in allowed_sql
    assert allowed_params == (
        Round5ControlKind.RELEASE.value,
        [release.event_id],
        32,
    )


async def test_durable_pending_filters_32_crashed_bells_before_limit() -> None:
    database = DurableControlDb()
    store = LakebaseRound5ControlStore(database.run)
    for bell in range(16):
        for lane_id, nibble in (("lakebase", "0"), ("competitor", "1")):
            database.seed(
                Round5ControlEvent.create(
                    binding=binding(
                        lane_id=lane_id,
                        job_id=f"{bell:02x}{nibble}{'a' * 61}",
                    ),
                    sequence=2,
                    kind=Round5ControlKind.RELEASE,
                )
            )
    request = canonical_request()
    current = Round5ControlEvent.create(
        binding=binding(
            job_id="e" * 64,
            claim_bound=False,
            request=request,
        ),
        sequence=1,
        kind=Round5ControlKind.PRELOAD,
        payload={"request": request},
    )
    current_release = Round5ControlEvent.create(
        binding=binding(job_id="f" * 64),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
    )
    database.seed(current)
    database.seed(current_release)

    assert await store.pending(limit=32) == (current,)
    pending_sql, pending_params = database.transactions[-1][0]
    assert pending_sql.index("kind <> %s") < pending_sql.index("LIMIT %s")
    assert pending_params == (Round5ControlKind.RELEASE.value, [], 32)

    opened = await store.pending(
        limit=32,
        allowed_release_ids={current_release.event_id},
    )
    assert {event.event_id for event in opened} == {
        current.event_id,
        current_release.event_id,
    }


async def test_durable_pre_send_revalidation_observes_interleaved_cancel() -> None:
    database = DurableControlDb()
    database.revalidation_started = asyncio.Event()
    database.finish_revalidation = asyncio.Event()
    store = LakebaseRound5ControlStore(database.run)
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
    )
    event_binding = binding(job_id="6" * 64)
    release = Round5ControlEvent.create(
        binding=event_binding,
        sequence=2,
        kind=Round5ControlKind.RELEASE,
    )
    await store.enqueue(release)
    dispatcher.allow_release(release.event_id)

    publishing = asyncio.create_task(dispatcher.publish_once())
    await asyncio.wait_for(database.revalidation_started.wait(), timeout=1)
    cancel = Round5ControlEvent.create(
        binding=event_binding,
        sequence=3,
        kind=Round5ControlKind.CANCEL,
        created_at=release.created_at + timedelta(seconds=1),
    )
    await store.enqueue(cancel)
    database.finish_revalidation.set()

    assert await asyncio.wait_for(publishing, timeout=1) == 0
    assert sent == []
    assert release.event_id not in dispatcher._allowed_releases
    assert database.outbox[release.event_id]["published_at"] == cancel.created_at
    revalidation_sql = next(
        sql
        for transaction in database.transactions
        for sql, _params in transaction
        if "SELECT EXISTS" in sql
    )
    assert "release.published_at IS NULL" in revalidation_sql
    assert "NOT EXISTS" in revalidation_sql


async def test_durable_orphan_janitor_expires_old_debt_but_protects_live_gate() -> None:
    database = DurableControlDb()
    store = LakebaseRound5ControlStore(database.run)
    created_at = datetime(2026, 9, 18, 12, tzinfo=UTC)
    expired_at = created_at + timedelta(seconds=ROUND5_ORPHANED_RELEASE_TTL_SECONDS + 1)
    orphan = Round5ControlEvent.create(
        binding=binding(job_id="7" * 64),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=created_at,
    )
    protected = Round5ControlEvent.create(
        binding=binding(job_id="8" * 64),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=created_at,
    )
    fresh = Round5ControlEvent.create(
        binding=binding(job_id="9" * 64),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=expired_at,
    )
    for event in (orphan, protected, fresh):
        database.seed(event)

    dispatcher = Round5ControlDispatcher(
        store,
        lambda _event: asyncio.sleep(0),
        now=lambda: expired_at,
    )
    dispatcher.hold_release(protected.event_id)

    assert await dispatcher.publish_once() == 0
    assert database.outbox[orphan.event_id]["published_at"] == expired_at
    assert database.outbox[protected.event_id]["published_at"] is None
    assert database.outbox[fresh.event_id]["published_at"] is None
    expiry_sql, expiry_params = next(
        (sql, params)
        for transaction in database.transactions
        for sql, params in transaction
        if "created_at <= %s" in sql
    )
    assert "created_at <= %s" in expiry_sql
    assert "NOT (event_id = ANY(%s))" in expiry_sql
    assert expiry_params == (
        expired_at,
        Round5ControlKind.RELEASE.value,
        expired_at - timedelta(seconds=ROUND5_ORPHANED_RELEASE_TTL_SECONDS),
        [protected.event_id],
    )


async def test_durable_orphan_reap_failure_backs_off_without_blocking_delivery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    database = DurableControlDb()
    database.fail_expiry = True
    store = LakebaseRound5ControlStore(database.run)
    now = datetime(2026, 9, 18, 12, tzinfo=UTC)
    request = canonical_request()
    stage = Round5ControlEvent.create(
        binding=binding(job_id="b" * 64, request=request),
        sequence=1,
        kind=Round5ControlKind.STAGE,
        payload={"request": request},
    )
    database.seed(stage)
    sent: list[Round5ControlEvent] = []
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
        now=lambda: now,
    )
    caplog.set_level(logging.WARNING, logger="server.round5_control")

    assert await dispatcher.publish_once() == 1
    assert await dispatcher.publish_once() == 0

    expiry_statements = [
        sql
        for transaction in database.transactions
        for sql, _params in transaction
        if "created_at <= %s" in sql
    ]
    assert len(expiry_statements) == 1
    assert sent == [stage]
    assert "round5_control_orphan_reap_failed" in caplog.text


async def test_runtime_control_initialization_executes_zero_ddl() -> None:
    statements: list[str] = []

    class Cursor:
        rows: list[tuple[object, ...]] = []

        async def execute(self, statement, unused_parameters=None):
            statements.append(str(statement))
            if "SELECT 1 FROM pg_catalog.pg_namespace" in str(statement):
                self.rows = [(1,)]
            elif "information_schema.columns" in str(statement):
                columns = {
                    "round5_control_outbox_v3": {
                        "event_id",
                        "installation_id",
                        "lane_id",
                        "generation",
                        "warm_attempt_token",
                        "job_id",
                        "sequence",
                        "kind",
                        "payload",
                        "created_at",
                        "published_at",
                    },
                    "round5_runner_event_v3": {
                        "event_id",
                        "installation_id",
                        "lane_id",
                        "generation",
                        "warm_attempt_token",
                        "job_id",
                        "sequence",
                        "kind",
                        "binding",
                        "payload",
                        "occurred_at",
                    },
                }
                self.rows = [
                    (table, column) for table, names in columns.items() for column in names
                ]
            else:
                self.rows = [
                    ("round5_control_outbox_v3",),
                    ("round5_runner_event_v3",),
                ]

        async def fetchone(self):
            return self.rows[0] if self.rows else None

        async def fetchall(self):
            return self.rows

    async def run(callback):
        return await callback(Cursor())

    await LakebaseRound5ControlStore(run).initialize()
    assert statements
    assert all(
        token not in statement.upper()
        for statement in statements
        for token in ("CREATE ", "ALTER ", "DROP ", "TRUNCATE ")
    )


async def test_lakebase_store_revalidates_and_expires_releases_durably() -> None:
    statements: list[tuple[str, object]] = []

    class Cursor:
        rows: list[tuple[object, ...]] = []

        async def execute(self, statement, parameters=None):
            text = str(statement)
            statements.append((text, parameters))
            if "SELECT EXISTS" in text:
                self.rows = [(False,)]
            elif "RETURNING event_id" in text:
                self.rows = [("a" * 64,), ("b" * 64,)]
            else:
                self.rows = []

        async def fetchone(self):
            return self.rows[0] if self.rows else None

        async def fetchall(self):
            return self.rows

    async def run(callback):
        return await callback(Cursor())

    store = LakebaseRound5ControlStore(run)
    assert not await store.release_dispatchable("c" * 64)
    now = datetime(2026, 9, 18, 12, tzinfo=UTC)
    expired = await store.expire_orphaned_releases(
        created_before=now - timedelta(minutes=12),
        expired_at=now,
        protected_release_ids={"d" * 64},
    )

    assert expired == ("a" * 64, "b" * 64)
    revalidation_sql, revalidation_params = statements[0]
    assert "release.published_at IS NULL" in revalidation_sql
    assert "NOT EXISTS" in revalidation_sql
    assert revalidation_params == (
        "c" * 64,
        Round5ControlKind.RELEASE.value,
        Round5ControlKind.CANCEL.value,
    )
    expiry_sql, expiry_params = statements[1]
    assert "created_at <= %s" in expiry_sql
    assert "NOT (event_id = ANY(%s))" in expiry_sql
    assert expiry_params[-1] == ["d" * 64]


async def test_runtime_warm_initialization_also_executes_zero_ddl() -> None:
    statements: list[str] = []

    class Cursor:
        rows: list[tuple[object, ...]] = []

        async def execute(self, statement, parameters=None):
            text = str(statement)
            statements.append(text)
            if "SELECT 1 FROM pg_catalog.pg_namespace" in text:
                self.rows = [(1,)]
            elif "information_schema.columns" in text:
                control_columns = {
                    "round5_control_outbox_v3": {
                        "event_id",
                        "installation_id",
                        "lane_id",
                        "generation",
                        "warm_attempt_token",
                        "job_id",
                        "sequence",
                        "kind",
                        "payload",
                        "created_at",
                        "published_at",
                    },
                    "round5_runner_event_v3": {
                        "event_id",
                        "installation_id",
                        "lane_id",
                        "generation",
                        "warm_attempt_token",
                        "job_id",
                        "sequence",
                        "kind",
                        "binding",
                        "payload",
                        "occurred_at",
                    },
                }
                self.rows = [
                    (table, column)
                    for table, columns in control_columns.items()
                    for column in columns
                ]
            else:
                names = list(parameters[1])
                self.rows = [(name,) for name in names]

        async def fetchone(self):
            return self.rows[0] if self.rows else None

        async def fetchall(self):
            return self.rows

    async def run(callback):
        return await callback(Cursor())

    await LakebaseRound5WarmStore(run).initialize()
    assert statements
    assert all(
        token not in statement.upper()
        for statement in statements
        for token in ("CREATE ", "ALTER ", "DROP ", "TRUNCATE ")
    )


async def test_resident_result_and_progress_return_without_ssm() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(store, dispatcher, sleep=lambda _delay: asyncio.sleep(0))
    event_binding = binding()
    progress = {
        "protocol": runner.fanin.PROTOCOL,
        "schema_version": runner.fanin.SCHEMA_VERSION,
        "sequence": 1,
        "lane_id": "lakebase",
        "phase": "ramping",
        "initiated_clients": 4,
    }
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="b" * 64,
            binding=event_binding,
            sequence=1,
            kind=Round5RunnerEventKind.PROGRESS,
            occurred_at=datetime.now(UTC),
            payload=progress,
        )
    )
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="c" * 64,
            binding=event_binding,
            sequence=2,
            kind=Round5RunnerEventKind.RESULT,
            occurred_at=datetime.now(UTC),
            payload={"raw": "result"},
        )
    )
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="d" * 64,
            binding=event_binding,
            sequence=3,
            kind=Round5RunnerEventKind.SETTLED,
            occurred_at=datetime.now(UTC),
            payload={"state": "completed"},
        )
    )
    observed: list[dict[str, object]] = []
    result = await transport.result(
        event_binding,
        on_progress=lambda value: asyncio.sleep(0, result=observed.append(dict(value))),
    )
    assert observed == [progress]
    assert result == {"raw": "result"}


async def test_cancel_waits_for_durable_resident_settlement() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(
        store,
        dispatcher,
        sleep=lambda _delay: asyncio.sleep(0),
    )
    event_binding = binding()
    cancellation = asyncio.create_task(transport.cancel(binding=event_binding))
    await asyncio.sleep(0)
    assert not cancellation.done()
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="e" * 64,
            binding=event_binding,
            sequence=1,
            kind=Round5RunnerEventKind.SETTLED,
            occurred_at=datetime.now(UTC),
            payload={"state": "cancelled"},
        )
    )
    await asyncio.wait_for(cancellation, timeout=1)


async def test_permanent_quarantine_fails_transport_wait_immediately() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(
        store,
        dispatcher,
        sleep=lambda _delay: asyncio.sleep(0),
    )
    event_binding = binding()
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="7" * 64,
            binding=event_binding,
            sequence=1,
            kind=Round5RunnerEventKind.QUARANTINED,
            occurred_at=datetime.now(UTC),
            payload={"code": "resident_control_sequence_invalid"},
        )
    )
    with pytest.raises(RuntimeError, match="resident_control_sequence_invalid"):
        await transport.result(event_binding, on_progress=None)


@pytest.mark.parametrize(
    ("code", "quoted"),
    [
        ("resident_fanin_client_errors", "resident_fanin_client_errors"),
        ("fan-in stopped: 7 clients refused by host-a", "resident_runner_failed"),
        (None, "resident_runner_failed"),
    ],
)
async def test_a_runner_reported_failure_reaches_the_operator_log_as_a_bounded_code(
    code: str | None,
    quoted: str,
) -> None:
    """2026-09-26: a lane failed at 8,000 held clients and the log said only
    "RuntimeError <- CancelledError", because a bare RuntimeError is never quoted."""

    from server.manager import operator_diagnosis
    from server.round5_control import Round5ResidentRunnerError

    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(store, dispatcher, sleep=lambda _delay: asyncio.sleep(0))
    event_binding = binding()
    for sequence, kind, payload in (
        (1, Round5RunnerEventKind.FAILED, {} if code is None else {"code": code}),
        (2, Round5RunnerEventKind.SETTLED, {"state": "failed"}),
    ):
        await store.append_runner_event(
            Round5RunnerEvent(
                event_id=str(sequence) * 64,
                binding=event_binding,
                sequence=sequence,
                kind=kind,
                occurred_at=datetime.now(UTC),
                payload=payload,
            )
        )

    with pytest.raises(Round5ResidentRunnerError) as raised:
        await transport.result(event_binding, on_progress=None)

    assert str(raised.value) == quoted
    assert quoted in operator_diagnosis(raised.value)
    assert "host-a" not in operator_diagnosis(raised.value)


async def test_restart_reconciliation_uses_resident_registry_not_ssm() -> None:
    calls: list[str] = []

    class Transport:
        async def settle_registry_job(self, job_id):
            calls.append(job_id)

    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._resident_transport = Transport()
    await adapter.cancel_job("a" * 64)
    assert calls == ["a" * 64]


async def test_prebell_restart_settles_claim_jobs_before_absence_proof() -> None:
    calls: list[tuple[str, str]] = []

    class Adapter:
        def __init__(self, lane_id):
            self.lane_id = lane_id

        async def cancel_job(self, job_id):
            calls.append((self.lane_id, job_id))

    class Orchestrator:
        async def prove_bout_absent(self, bout_id):
            assert calls == [
                ("lakebase", "a" * 64),
                ("competitor", "b" * 64),
            ]
            calls.append(("prove_absent", bout_id))

    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._lane_adapters = {
        "lakebase": Adapter("lakebase"),
        "competitor": Adapter("competitor"),
    }
    engine._setup_orchestrator = Orchestrator()
    engine._warm_attempt_token = ""
    engine._bound_claim = None
    engine._cleanup_bout_id = None
    claim = SimpleNamespace(
        lakebase_job_id="a" * 64,
        competitor_job_id="b" * 64,
        capsule_generation=7,
        warm_attempt_token="attempt-prebell-restart",
        bout_id="bout-prebell-restart",
    )

    await engine.reconcile_abandoned_claim(claim)

    assert calls[-1] == ("prove_absent", "bout-prebell-restart")


async def test_reused_engine_second_abandon_supersedes_first_cleaned_bout() -> None:
    """Engine identity reuse across generations (the live Case A no-bell wedge): the
    coordinator reuses ONE warm engine, so a COMPLETED predecessor cleanup leaves
    ``_cleanup_bout_id`` retained. A subsequent no-bell abandon for a DIFFERENT claim
    on the SAME engine must settle (retain its own bout) rather than refuse with
    ConnectionSpikeCleanupError "Round 5 engine already carries another cleanup bout"
    (which converged to BlockedWarmError and looped cleanup_reconcile_blocked live).

    Real LiveConnectionSpikeEngine (only the lane adapters / setup orchestrator are
    controlled boundary fakes). Mutation: without the supersede reset, the terminal
    retain_cleaned_bout for bout B raises here because _cleanup_bout_id still holds A.
    """

    class Adapter:
        def __init__(self, lane_id: str) -> None:
            self.lane_id = lane_id
            self.cancelled: list[str] = []

        async def cancel_job(self, job_id: str) -> None:
            self.cancelled.append(job_id)

    class Orchestrator:
        def __init__(self) -> None:
            self.proved: list[str] = []

        async def prove_bout_absent(self, bout_id: str) -> None:
            self.proved.append(bout_id)

    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._lane_adapters = {"lakebase": Adapter("lakebase"), "competitor": Adapter("competitor")}
    engine._setup_orchestrator = Orchestrator()
    engine._warm_attempt_token = ""
    engine._bound_claim = None
    engine._cleanup_bout_id = None
    engine._cleanup_bout_required = False

    claim_a = SimpleNamespace(
        lakebase_job_id="a" * 64,
        competitor_job_id="b" * 64,
        capsule_generation=7,
        warm_attempt_token="attempt-reuse-a",
        bout_id="bout-reuse-a",
    )
    await engine.reconcile_abandoned_claim(claim_a)
    # Production leaves the completed bout A retained on this reused engine.
    assert engine._cleanup_bout_id == "bout-reuse-a"

    # SAME engine instance, a DIFFERENT claim/bout B (the second successive cleanup).
    claim_b = SimpleNamespace(
        lakebase_job_id="c" * 64,
        competitor_job_id="d" * 64,
        capsule_generation=8,
        warm_attempt_token="attempt-reuse-b",
        bout_id="bout-reuse-b",
    )
    # Between generations the coordinator RE-WARMS this reused engine, so its resident
    # warm attempt becomes the new generation's; simulate that re-warm. The retained
    # _cleanup_bout_id lineage from bout A intentionally persists across it -- that is
    # exactly the engine-identity reuse under test.
    engine._warm_attempt_token = claim_b.warm_attempt_token
    assert engine._cleanup_bout_id == "bout-reuse-a"
    # Must NOT raise "engine already carries another cleanup bout": it settles to B.
    await engine.reconcile_abandoned_claim(claim_b)
    assert engine._cleanup_bout_id == "bout-reuse-b"
    assert engine._setup_orchestrator.proved == ["bout-reuse-a", "bout-reuse-b"]


async def test_local_observer_cancellation_retains_resident_settlement_debt() -> None:
    release = asyncio.Event()

    class Transport:
        async def result(self, unused_binding, *, on_progress):
            del on_progress
            await release.wait()
            return {}

    request = canonical_request(
        resident_generation=9,
        targets=[{"lane_id": "lakebase"}],
    )
    event_binding = binding(request=request)
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._prepared_clients = SimpleNamespace(expires_at=datetime.now(UTC) + timedelta(hours=1))
    adapter._resident_transport = Transport()
    adapter._dispatch_lock = asyncio.Lock()
    adapter._settlement_debt = {}
    adapter._resident_settlement_debt = {}
    task = asyncio.create_task(
        adapter.execute_prepared(
            event_binding.job_id,
            request,
            resident_binding=event_binding,
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert adapter.settlement_pending(event_binding.job_id)


async def test_historical_agent_ready_cannot_satisfy_new_warm_attempt() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(store, dispatcher, sleep=lambda _delay: asyncio.sleep(0))
    request = canonical_request()
    expected = binding(
        claim_bound=False,
        process_boot_id="unattested",
        request=request,
    )
    historical = Round5ControlBinding(
        **{
            **expected.wire_value(),
            "warm_attempt_token": "attempt-eight",
        }
    )
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="f" * 64,
            binding=historical,
            sequence=1,
            kind=Round5RunnerEventKind.AGENT_READY,
            occurred_at=datetime.now(UTC),
            payload={
                "worker_count": 4,
                "worker_ready_indexes": [0, 1, 2, 3],
                "warm_attempt_token": "attempt-eight",
                "runner_boot_id": expected.runner_boot_id,
                "runner_process_boot_id": "process-old",
                "process_pid": 101,
                "runner_harness_sha256": expected.runner_harness_sha256,
            },
        )
    )
    with pytest.raises(RuntimeError, match="readiness binding changed"):
        await asyncio.wait_for(
            transport.wait_agent_ready(expected),
            timeout=1,
        )


async def test_agent_ready_requires_attested_process_and_all_worker_indexes() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(store, dispatcher, sleep=lambda _delay: asyncio.sleep(0))
    request = canonical_request()
    expected = binding(
        claim_bound=False,
        process_boot_id="unattested",
        request=request,
    )
    attested = Round5ControlBinding(
        **{
            **expected.wire_value(),
            "runner_process_boot_id": "process-current",
        }
    )
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="1" * 64,
            binding=attested,
            sequence=1,
            kind=Round5RunnerEventKind.AGENT_READY,
            occurred_at=datetime.now(UTC),
            payload={
                "worker_count": 4,
                "worker_ready_indexes": [0, 1, 2, 3],
                "warm_attempt_token": expected.warm_attempt_token,
                "runner_boot_id": expected.runner_boot_id,
                "runner_process_boot_id": "process-current",
                "process_pid": 102,
                "runner_harness_sha256": expected.runner_harness_sha256,
            },
        )
    )
    ready = await transport.wait_agent_ready(expected)
    assert ready["runner_process_boot_id"] == "process-current"


async def test_same_attempt_restart_requires_post_check_agent_ready() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(store, dispatcher, sleep=lambda _delay: asyncio.sleep(0))
    request = canonical_request()
    expected = binding(
        claim_bound=False,
        process_boot_id="unattested",
        request=request,
    )
    before = datetime(2026, 9, 16, tzinfo=UTC)

    def ready_event(sequence: int, process: str, occurred_at: datetime):
        attested = Round5ControlBinding(
            **{
                **expected.wire_value(),
                "runner_process_boot_id": process,
            }
        )
        return Round5RunnerEvent(
            event_id=f"{sequence}" * 64,
            binding=attested,
            sequence=sequence,
            kind=Round5RunnerEventKind.AGENT_READY,
            occurred_at=occurred_at,
            payload={
                "worker_count": 4,
                "worker_ready_indexes": [0, 1, 2, 3],
                "warm_attempt_token": expected.warm_attempt_token,
                "runner_boot_id": expected.runner_boot_id,
                "runner_process_boot_id": process,
                "process_pid": 103 + sequence,
                "runner_harness_sha256": expected.runner_harness_sha256,
            },
        )

    await store.append_runner_event(ready_event(1, "process-old", before))
    waiting = asyncio.create_task(
        transport.wait_agent_ready(
            expected,
            not_before=before + timedelta(seconds=1),
        )
    )
    await asyncio.sleep(0)
    assert not waiting.done()
    await store.append_runner_event(ready_event(2, "process-new", before + timedelta(seconds=2)))
    ready = await asyncio.wait_for(waiting, timeout=1)
    assert ready["runner_process_boot_id"] == "process-new"


async def test_live_resident_freshness_uses_pid_heartbeat_and_loaded_harness() -> None:
    store = InMemoryRound5ControlStore()
    dispatcher = Round5ControlDispatcher(store, lambda _event: asyncio.sleep(0))
    transport = Round5ResidentTransport(store, dispatcher)
    request = canonical_request()
    event_binding = binding(
        claim_bound=False,
        process_boot_id="process-live",
        request=request,
    )
    now = datetime.now(UTC)
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id="6" * 64,
            binding=event_binding,
            sequence=1,
            kind=Round5RunnerEventKind.HEARTBEAT,
            occurred_at=now,
            payload={
                "worker_ready_indexes": [0, 1, 2, 3],
                "runner_boot_id": event_binding.runner_boot_id,
                "runner_process_boot_id": "process-live",
                "process_pid": 4242,
                "runner_harness_sha256": event_binding.runner_harness_sha256,
            },
        )
    )
    assert await transport.resident_is_current(
        installation_id=event_binding.installation_id,
        lane_id=event_binding.lane_id,
        warm_attempt_token=event_binding.warm_attempt_token,
        runner_boot_id=event_binding.runner_boot_id,
        process_boot_id="process-live",
        process_pid=4242,
        harness_sha256=event_binding.runner_harness_sha256,
        now=now,
    )
    assert not await transport.resident_is_current(
        installation_id=event_binding.installation_id,
        lane_id=event_binding.lane_id,
        warm_attempt_token=event_binding.warm_attempt_token,
        runner_boot_id=event_binding.runner_boot_id,
        process_boot_id="process-stale",
        process_pid=4242,
        harness_sha256=event_binding.runner_harness_sha256,
        now=now,
    )


def test_execute_prepared_contains_no_post_bell_ssm_startup() -> None:
    source = inspect.getsource(runner._resident_agent)
    adapter_source = inspect.getsource(
        __import__(
            "server.connection_spike_live",
            fromlist=["LiveConnectionSpikeAdapter"],
        ).LiveConnectionSpikeAdapter.execute_prepared
    )
    assert "_send_command" not in adapter_source
    assert "_execute_reserved" not in adapter_source
    assert "run_lane_v3" not in adapter_source
    assert "ResidentShardPool" in source


def test_preloaded_worker_does_nothing_before_authenticated_request() -> None:
    received = queue.Queue()
    request_queue = queue.Queue()
    preload_ready_queue = queue.Queue()
    called = threading.Event()
    original = runner._fanin_worker_process

    def invoked(*args):
        received.put(args[0])
        called.set()

    runner._fanin_worker_process = invoked
    thread = threading.Thread(
        target=runner._resident_worker_entry,
        args=(
            request_queue,
            preload_ready_queue,
            0,
            object(),
            object(),
            object(),
            object(),
            object(),
            object(),
            object(),
            object(),
            object(),
            object(),
        ),
    )
    try:
        thread.start()
        assert not called.wait(0.02)
        ready = preload_ready_queue.get(timeout=1)
        assert ready[0:2] == ("worker_ready", 0)
        assert ready[2] > 0
        assert ready[3] == runner.LOADED_RUNNER_HARNESS_SHA256
        request_queue.put({"protocol": runner.fanin.PROTOCOL})
        assert called.wait(1)
        assert received.get_nowait() == {"protocol": runner.fanin.PROTOCOL}
    finally:
        runner._fanin_worker_process = original
        thread.join(1)


def test_terraform_declares_two_static_fifo_control_queues_with_bounded_actions() -> None:
    root = Path(__file__).resolve().parents[1]
    runner_hcl = (root / "infra/aws/round5_runner.tf").read_text()
    control_hcl = (root / "infra/aws/round5_control.tf").read_text()

    assert runner_hcl.count('resource "aws_sqs_queue"') == 4
    assert runner_hcl.count("fifo_queue") >= 4
    assert runner_hcl.count("redrive_policy") == 2
    assert runner_hcl.count("redrive_allow_policy") == 2
    assert "sqs:ReceiveMessage" in runner_hcl
    assert "sqs:DeleteMessage" in runner_hcl
    assert "sqs:*" not in runner_hcl
    assert "sqs:SendMessage" in control_hcl
    assert "sqs:*" not in control_hcl
    app_source = (root / "app.py").read_text()
    assert 'MessageGroupId=f"{event.lane_id}-{event.job_id}"' in app_source


def test_first_activity_milestones_are_emitted_at_real_socket_and_auth_edges() -> None:
    from runner import round5_fanin

    source = inspect.getsource(round5_fanin._open_client)
    socket_milestone = source.index('"milestone": "first_socket_initiated"')
    socket_open = source.index("loop.create_connection")
    auth_wait = source.index("await client.authenticated")
    auth_milestone = source.index('"milestone": "first_client_authenticated"')
    assert socket_milestone < socket_open < auth_wait < auth_milestone
    assert "// 1_000" not in source


def test_fourth_commit_and_cancellation_have_one_parent_decision_edge() -> None:
    source = inspect.getsource(runner._execute_sharded_fanin)
    commit = source.index('await await_stage("hold_committed")')
    cancellation = source.index(
        "if cancelled.is_set() or cancel_event.is_set()",
        commit,
    )
    sampling = source.index("sample_release_event.set()", cancellation)
    assert "await " not in source[cancellation:sampling]


def test_durable_bell_transaction_inserts_lakebase_release_outbox() -> None:
    source = inspect.getsource(LakebaseRound5WarmStore.accept_bell_with_leases)
    assert "ROUND5_CONTROL_OUTBOX_TABLE" in source
    assert "Round5ControlKind.RELEASE" in source
    assert "release_event.binding.job_id" in source
    assert "CROSS JOIN released" in source


def test_atomic_claim_uses_only_durable_claimability_predicate() -> None:
    source = inspect.getsource(Round5WarmCoordinator.claim)
    assert "_claimable" in source
    assert "provider." not in source
    assert "validate_ready" not in source
    ring_ready = inspect.getsource(Round5WarmCoordinator.ring_ready.fget)
    assert "_claimable" in ring_ready


def test_resident_service_is_installed_once_not_started_during_warming() -> None:
    install_source = inspect.getsource(lifecycle._configure_round5_runner)
    warm_source = inspect.getsource(LiveConnectionSpikeEngine.warm)
    assert "systemctl enable --now" in install_source
    assert "--resident-agent" in install_source
    assert "send_command" not in warm_source
    assert "start_resident_agent" not in warm_source


def test_runner_rotation_stops_before_replace_and_restarts_new_attested_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    harness = "b" * 64
    assets = {"connection_spike_runner.py": "a" * 64}

    class Session:
        region_name = "us-west-2"

        def client(self, name):
            assert name == "ssm"
            return object()

    def command(unused_ssm, *, commands, **unused):
        joined = "\n".join(commands)
        if "systemctl stop" in joined:
            order.append("stop")
            assert "resident-lakebase-active.json" in joined
            return "OLD_PID=17\nOLD_PROCESS_BOOT=process-old\n"
        if "enable --now" in joined:
            order.append("restart")
            assert order == ["stop", "install", "trust", "verify", "restart"]
            return f"NEW_PID=22\nNEW_PROCESS_BOOT=process-new\nLOADED_HARNESS={harness}\n"
        order.append("trust")
        return ""

    monkeypatch.setattr(lifecycle, "_run_round5_ssm_command", command)
    monkeypatch.setattr(
        lifecycle,
        "_install_round5_runner_assets",
        lambda *unused, **kwargs: order.append("install"),
    )
    monkeypatch.setattr(
        lifecycle,
        "_round5_runner_asset_checksums",
        lambda *unused, **kwargs: (
            order.append("verify") or assets,
            harness,
            "c" * 64,
        ),
    )
    import server.connection_spike_live as live

    monkeypatch.setattr(live, "runner_asset_sha256s", lambda: assets)
    trust = lifecycle._configure_round5_runner(
        Session(),
        runner_instance_id="i-0123456789abcdef0",
        expected_harness_sha256=harness,
        resident_lane_id="lakebase",
        resident_control_queue_url=(
            "https://sqs.us-west-2.amazonaws.com/123456789012/lakebase.fifo"
        ),
        resident_control_secret_arn=(
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:control"
        ),
    )
    assert trust == "c" * 64
    assert order == ["stop", "install", "trust", "verify", "restart"]


def test_resident_agent_checks_twelve_thousand_ports_before_worker_spawn() -> None:
    source = inspect.getsource(runner._resident_agent)
    capacity = source.index("fanin.TARGET_CLIENTS_PER_LANE")
    reserve = source.index("fanin.EPHEMERAL_PORT_RESERVE_PER_LANE", capacity)
    refusal = source.index('"ephemeral_port_reserve_exhausted"', reserve)
    spawn = source.index("ResidentShardPool.start()", refusal)
    assert capacity < reserve < refusal < spawn


def test_stale_control_messages_are_quarantined_then_acknowledged() -> None:
    source = inspect.getsource(runner._resident_agent)
    quarantine = source.index("RESIDENT_CONTROL_QUARANTINED")
    finalizer = source.index("finally:", quarantine)
    delete = source.index("sqs.delete_message", finalizer)
    assert quarantine < finalizer < delete


def test_transient_database_and_filesystem_failures_remain_retryable() -> None:
    assert runner._resident_control_failure_is_permanent(runner.RunnerContractError("bad_binding"))
    assert not runner._resident_control_failure_is_permanent(
        OSError("temporary filesystem failure")
    )
    assert not runner._resident_control_failure_is_permanent(
        runner.psycopg.OperationalError("temporary database failure")
    )
