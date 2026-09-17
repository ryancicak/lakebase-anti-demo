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
    class FailingStore(InMemoryRound5ControlStore):
        async def pending(self, limit: int = 32) -> tuple[Round5ControlEvent, ...]:
            raise RuntimeError("secret-provider-detail")

    dispatcher = Round5ControlDispatcher(FailingStore(), lambda _event: asyncio.sleep(0))
    caplog.set_level(logging.WARNING, logger="server.round5_control")

    await dispatcher.start()
    await asyncio.sleep(0)
    await dispatcher.close()

    assert "round5_control_outbox_publish_failed consecutive_failures=1" in caplog.text
    assert "secret-provider-detail" not in caplog.text


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
    dispatcher = Round5ControlDispatcher(
        store,
        lambda event: asyncio.sleep(0, result=sent.append(event)),
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


async def test_restart_reconciliation_uses_resident_registry_not_ssm() -> None:
    calls: list[str] = []

    class Transport:
        async def settle_registry_job(self, job_id):
            calls.append(job_id)

    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._resident_transport = Transport()
    await adapter.cancel_job("a" * 64)
    assert calls == ["a" * 64]


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
