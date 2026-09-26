from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from runner import connection_spike_runner as runner
from server import connection_spike_live as live
from server.connection_spike_journal import (
    CreationScope,
    LifecycleState,
    ResourceSpec,
    Round5CreationCoordinator,
)
from server.connection_spike_live import (
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeSetupLaneStop,
    LiveConnectionSpikeAdapter,
    LiveConnectionSpikeEngine,
)
from server.manager import RunManager
from server.models import (
    BoutOperator,
    CompetitorId,
    Corner,
    LaneState,
    RoundFiveRuntimeLaneSnapshot,
    RoundFiveRuntimeSnapshot,
    RoundFiveSetupLaneSnapshot,
    RoundFiveSetupSnapshot,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.round5_warm import BellContext, Round5Variant
from tests.test_connection_fanin import finalize as finalize_fanin_lane
from tests.test_connection_fanin import raw_lane as raw_fanin_lane


def test_v2_and_v3_setup_protocols_require_exact_schema_pairings() -> None:
    lanes = {
        lane_id: RoundFiveSetupLaneSnapshot(id=lane_id, name=lane_id)
        for lane_id in ("lakebase", "competitor")
    }
    assert RoundFiveSetupSnapshot(lanes=lanes).schema_version == 4
    assert RoundFiveSetupSnapshot(
        protocol="round5-fanin-v2",
        schema_version=2,
        lanes=lanes,
    ).protocol == "round5-fanin-v2"
    for protocol, schema in (
        ("round5-fanin-v2", 3),
        ("round5-fanin-v3", 2),
        ("round5-fanin-v4", 3),
    ):
        with pytest.raises(ValueError, match="do not pair"):
            RoundFiveSetupSnapshot(
                protocol=protocol,
                schema_version=schema,
                lanes=lanes,
            )


def test_manager_rejects_verified_flag_and_10k_without_complete_v3_gates() -> None:
    raw = SimpleNamespace(
        lane_id="lakebase",
        initiated_clients=10_000,
        authenticated_clients=10_000,
        held_clients_at_gate=10_000,
        verified=True,
        gates=SimpleNamespace(passed=True),
    )
    evidence = RunManager._round_five_evidence(raw)
    assert not RunManager._round_five_lane_valid(raw, evidence)


async def test_lakebase_eligibility_schedules_dispatch_before_progress_io() -> None:
    orchestrator = object.__new__(live.LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        lakebase_credential_sha256="a" * 64,
        lakebase_pooled_host="pooled.example.test",
    )
    ticks = iter((1_000_000_001, 1_000_000_002))
    orchestrator._monotonic_ns = lambda: next(ticks)
    order: list[str] = []

    async def lane_ready(stop: ConnectionSpikeSetupLaneStop) -> None:
        assert stop.endpoint_host == "pooled.example.test"
        order.append("dispatch_scheduled")

    async def progress(value: object) -> None:
        del value
        order.append("progress_published")

    await orchestrator._setup_lakebase(
        "bout",
        SimpleNamespace(),
        SimpleNamespace(wait=lambda: asyncio.sleep(0)),
        [1_000_000_000],
        progress,
        lane_ready,
    )

    assert order == ["dispatch_scheduled", "progress_published"]


async def test_create_proxy_is_first_timed_aws_mutation_and_dispatch_waits_for_gate() -> None:
    orchestrator = object.__new__(live.LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="rds_postgres",
        competitor_credential_id="rds",
        competitor_direct_host="direct.example.test",
        competitor_credential_sha256="b" * 64,
    )
    orchestrator._monotonic_ns = lambda: 1_000_000_001
    order: list[str] = []

    class Coordinator:
        async def create_resource(self, scope, spec) -> None:
            del scope
            order.append(f"mutate:{spec.resource_kind}")

        async def complete_prestaged(self, scope, spec, *, intent) -> None:
            # The proxy CREATE_INTENT is pre-committed before the bell T0; the
            # timed path issues the mutation with no coordination I/O in front.
            del scope, intent
            order.append(f"mutate:{spec.resource_kind}")

    async def report(callback, lane_id, phase, status, **kwargs) -> None:
        del callback, lane_id, status, kwargs
        order.append(f"progress:{phase}")

    async def available(clients, resources, **kwargs):
        del clients, resources, kwargs
        return "available"

    async def journal(*args) -> None:
        del args
        order.append("journal_exact")

    async def topology(*args) -> None:
        del args
        order.append("proxy_gate_exact")

    async def lane_ready(stop: ConnectionSpikeSetupLaneStop) -> None:
        del stop
        order.append("dispatch_scheduled")

    async def lane_stage(stop: ConnectionSpikeSetupLaneStop) -> None:
        assert stop.endpoint_host == "proxy.example.test"
        order.append("resident_prepared")

    orchestrator._report = report
    orchestrator._wait_proxy_available = available
    orchestrator._verify_journaled_resources = journal
    orchestrator._verify_proxy_topology = topology
    specs = (
        SimpleNamespace(resource_kind="rds_proxy"),
        SimpleNamespace(resource_kind="proxy_target_group"),
        SimpleNamespace(resource_kind="proxy_target"),
    )
    resources = SimpleNamespace(
        names=SimpleNamespace(proxy_name="proxy"),
        proxy_endpoint="proxy.example.test",
        secret_arn="secret-ref",
    )

    await orchestrator._setup_competitor(
        "bout",
        SimpleNamespace(),
        SimpleNamespace(rds=SimpleNamespace(), ssm=SimpleNamespace()),
        Coordinator(),
        specs,
        resources,
        SimpleNamespace(wait=lambda: asyncio.sleep(0)),
        [1_000_000_000],
        None,
        lane_ready,
        lane_stage,
        None,
        SimpleNamespace(ordinal=1, resource_kind="rds_proxy"),
    )

    assert order[0] == "mutate:rds_proxy"
    assert order.index("resident_prepared") < order.index("proxy_gate_exact")
    assert order.index("proxy_gate_exact") < order.index("dispatch_scheduled")
    assert not any("verifying_transaction" in item for item in order)


class _SlowJournal:
    """In-memory journal whose every round trip costs real wall time, standing in
    for the deployed cold-TLS coordination connect that the pre-stage fix moves
    off the timed path. Records the monotonic instant of each op so a test can
    assert none landed inside the [T0, boto3-request] window."""

    def __init__(self, clock, *, latency_s: float = 0.05) -> None:
        self._clock = clock
        self._latency_s = latency_s
        self.committed: list = []
        self.op_ns: list[tuple[str, int]] = []

    async def commit(self, event, *, authority_scope=None) -> None:
        del authority_scope
        await asyncio.sleep(self._latency_s)  # a fresh coordination connect
        self.committed.append(event)
        self.op_ns.append((f"commit:{event.lifecycle_state.value}", self._clock()))

    async def events(self, scope):
        del scope
        await asyncio.sleep(self._latency_s)
        self.op_ns.append(("events", self._clock()))
        return tuple(self.committed)

    async def scopes(self, bout_id):
        del bout_id
        return ()


class _SlowFence:
    def __init__(self, clock, *, latency_s: float = 0.05) -> None:
        self._clock = clock
        self._latency_s = latency_s
        self.op_ns: list[int] = []

    async def assert_current(self, scope: CreationScope) -> None:
        del scope
        await asyncio.sleep(self._latency_s)
        self.op_ns.append(self._clock())


def _proxy_stamp_orchestrator():
    """A real orchestrator wired for the production CreateDBProxy stamp path with
    a fake boto3 rds client. ``_create_proxy`` (the shipped adapter body) stamps
    ``resources.proxy_create_requested_ns`` immediately before the boto3 call."""

    orchestrator = object.__new__(live.LiveConnectionSpikeSetupOrchestrator)
    orchestrator._monotonic_ns = time.monotonic_ns
    # Req #7: the reused CreateDBProxy worker slot (created lazily via
    # _ensure_createproxy_executor; __init__ is bypassed by object.__new__).
    orchestrator._createproxy_executor = None
    orchestrator.config = SimpleNamespace(
        region="us-west-2",
        expected_account_id="123456789012",
        proxy_subnet_ids=("subnet-aaaa", "subnet-bbbb"),
    )
    boto = SimpleNamespace(request_ns=None)

    def create_db_proxy(**kwargs):
        boto.request_ns = time.monotonic_ns()
        boto.kwargs = kwargs
        return {}

    def describe_db_proxies(**kwargs):
        del kwargs
        return {
            "DBProxies": [
                {
                    "Endpoint": "proxy.internal",
                    "DBProxyArn": (
                        "arn:aws:rds:us-west-2:123456789012:db-proxy:pb-1"
                    ),
                }
            ]
        }

    clients = SimpleNamespace(
        rds=SimpleNamespace(
            create_db_proxy=create_db_proxy,
            describe_db_proxies=describe_db_proxies,
        )
    )
    resources = SimpleNamespace(
        names=SimpleNamespace(proxy_name="pb-1", token="tok"),
        secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:s",
        proxy_role_arn="arn:aws:iam::123456789012:role/r",
        proxy_security_group_id="sg-1",
        proxy_endpoint="",
        proxy_create_requested_ns=None,
    )
    return orchestrator, clients, resources, boto


def _proxy_spec():
    return ResourceSpec(
        1,
        "rds_proxy",
        "pb-1",
        metadata={"tags": {"anti-demo-bout-id": "bout-stamp"}},
    )


async def test_production_stamp_path_keeps_createdbproxy_under_100ms_despite_slow_journal() -> None:
    """FINDING 1: the REAL Round5CreationCoordinator + REAL _create_proxy stamp
    path. Injected 50 ms-per-op journal/fence latency (five ops on the old path)
    is pre-committed BEFORE the bell T0, so the direct boto3 CreateDBProxy request
    lands <=100 ms after T0 without hiding any coordination latency -- and the
    stamp is at the true request boundary, in the same monotonic domain as T0."""

    orchestrator, clients, resources, boto = _proxy_stamp_orchestrator()
    clock = orchestrator._monotonic_ns
    journal = _SlowJournal(clock, latency_s=0.05)
    fence = _SlowFence(clock, latency_s=0.05)

    class ProxyAdapter:
        async def create(self, spec):
            return await orchestrator._create_proxy(clients, resources, spec)

    coordinator = Round5CreationCoordinator(
        journal=journal, fence=fence, adapters={"rds_proxy": ProxyAdapter()}
    )
    scope = CreationScope("bout-stamp", 7, "d" * 64)
    spec = _proxy_spec()

    # --- pre-bell (untimed): durable intent + fence, the slow coordination hop ---
    intent = await coordinator.precommit_intent(scope, spec)
    assert intent.lifecycle_state is LifecycleState.CREATE_INTENT
    assert journal.committed[0].lifecycle_state is LifecycleState.CREATE_INTENT

    # --- authoritative bell T0 captured AFTER the coordination hop ---
    t0_ns = orchestrator._monotonic_ns()

    # --- post-gate timed path: direct mutation, no coordination I/O in front ---
    completed = await coordinator.complete_prestaged(scope, spec, intent=intent)
    assert completed.lifecycle_state is LifecycleState.CREATED

    # Stamp exists, in the same monotonic domain as T0, at the true boundary.
    assert resources.proxy_create_requested_ns is not None
    assert resources.proxy_create_requested_ns >= t0_ns
    assert boto.request_ns is not None
    assert boto.request_ns >= resources.proxy_create_requested_ns

    # The scored bell->CreateDBProxy delta is tiny despite 250 ms of coordination
    # latency, because all of it happened before T0.
    delta_ms = (resources.proxy_create_requested_ns - t0_ns) / 1_000_000
    assert delta_ms <= 100.0, delta_ms

    # No journal/fence op landed inside the [T0, boto3-request] window: the only
    # coordination between T0 and the mutation is nothing at all.
    window = range(t0_ns, boto.request_ns + 1)
    assert not [op for op, ns in journal.op_ns if ns in window]
    assert not [ns for ns in fence.op_ns if ns in window]
    # Durability preserved: CREATED committed only AFTER the boto3 request.
    created_commit_ns = [ns for op, ns in journal.op_ns if op == "commit:created"][0]
    assert created_commit_ns > boto.request_ns


async def test_old_inline_create_resource_would_blow_the_100ms_window() -> None:
    """Contrast (does NOT mock create_resource away): running the SAME real
    coordinator via the OLD inline create_resource AFTER T0 charges the slow
    journal/fence hop to the window, so the stamp lands well past 100 ms. Proves
    the gate is not hollow and that the pre-stage placement is what fixes it."""

    orchestrator, clients, resources, boto = _proxy_stamp_orchestrator()
    clock = orchestrator._monotonic_ns
    journal = _SlowJournal(clock, latency_s=0.05)
    fence = _SlowFence(clock, latency_s=0.05)

    class ProxyAdapter:
        async def create(self, spec):
            return await orchestrator._create_proxy(clients, resources, spec)

    coordinator = Round5CreationCoordinator(
        journal=journal, fence=fence, adapters={"rds_proxy": ProxyAdapter()}
    )
    scope = CreationScope("bout-stamp", 7, "d" * 64)
    spec = _proxy_spec()

    t0_ns = orchestrator._monotonic_ns()
    await coordinator.create_resource(scope, spec)  # old path: intent + fences after T0

    delta_ms = (resources.proxy_create_requested_ns - t0_ns) / 1_000_000
    # >= 3 coordination ops (events + intent commit + fence) each 50 ms.
    assert delta_ms > 100.0, delta_ms


async def test_lane_failure_cancels_sibling_without_waiting_for_lane_order() -> None:
    adapter = SimpleNamespace(
        config=SimpleNamespace(
            targets=(
                SimpleNamespace(lane_id="lakebase"),
                SimpleNamespace(lane_id="competitor"),
            )
        ),
        cancel=lambda run_id: asyncio.sleep(0),
    )
    engine = LiveConnectionSpikeEngine(adapter)
    arm = object()
    engine._armed = arm
    sibling_cancelled = asyncio.Event()

    async def fail() -> object:
        raise ConnectionSpikeLiveOperationError("lane failed")

    async def sibling() -> object:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    engine._lane_bursts = {
        "lakebase": asyncio.create_task(sibling()),
        "competitor": asyncio.create_task(fail()),
    }

    with pytest.raises(ConnectionSpikeLiveOperationError, match="lane failed"):
        await engine.run(arm)
    assert sibling_cancelled.is_set()


def test_9999_cannot_stop_a_v3_bell_clock() -> None:
    with pytest.raises(ValueError, match="exactly 10,000"):
        RoundFiveRuntimeLaneSnapshot(
            id="lakebase",
            phase="holding",
            elapsed_at_snapshot_ms=3_000,
            bell_to_10000_observed_ms=3_000,
            held_clients=9_999,
            status="not exact",
        )


def test_2895_cannot_be_a_verified_v3_result() -> None:
    with pytest.raises(ValueError, match="exact 10,000-client stop"):
        RoundFiveRuntimeLaneSnapshot(
            id="lakebase",
            phase="verified",
            elapsed_at_snapshot_ms=43_948,
            bell_to_10000_observed_ms=None,
            clients_initiated=2_932,
            clients_authenticated=2_895,
            held_clients=2_895,
            sampled_queries_succeeded=16,
            status="false verified latch",
        )


def test_2895_result_is_rejected_even_when_runner_labels_it_verified() -> None:
    partial = SimpleNamespace(
        lane_id="lakebase",
        initiated_clients=2_932,
        authenticated_clients=2_895,
        held_clients_at_gate=2_895,
        terminal_failures=0,
        failure_codes={},
        retries=0,
        disconnected_during_hold=0,
        sampled_queries_succeeded=16,
        verified=True,
        gates=SimpleNamespace(passed=True),
    )

    evidence = RunManager._round_five_evidence(partial)

    assert evidence["schema_version"] == live.FANIN_SCHEMA_VERSION
    assert evidence["protocol"] == live.FANIN_PROTOCOL
    assert "safety_evidence_version" in evidence
    assert "hard_safety_verified" in evidence
    assert "telemetry_advisories" in evidence
    assert RunManager._round_five_lane_valid(partial, evidence) is False


def test_job_control_cancels_by_logical_job_id_and_reports_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    job_id = "a" * 64
    job, owner, lock = runner._claim_job(
        {
            "job_id": job_id,
            "prepared_request_digest": "b" * 64,
        }
    )
    assert owner
    runner._release_job_lock(lock)
    runner._atomic_job_write(
        job,
        "latest_progress",
        json.dumps(
            {
                "sequence": 4,
                "lane_id": "lakebase",
                "held_clients": 7_500,
            }
        ),
    )

    encoded = base64.urlsafe_b64encode(
        gzip.compress(
            json.dumps(
                {
                    "protocol": runner.JOB_PROTOCOL,
                    "schema_version": 3,
                    "action": "cancel_job",
                    "job_id": job_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            mtime=0,
        )
    ).decode()
    request = runner._decode_job_control(encoded)
    assert runner._job_control(request) == 0

    output = capsys.readouterr().out
    status = json.loads(output.split("JOB_STATUS:", 1)[1])
    assert status["job_id"] == job_id
    assert status["latest_progress"]["held_clients"] == 7_500
    assert (job / "cancel_requested").read_text(encoding="utf-8") == "true"


def test_runtime_clock_uses_one_server_bell_origin() -> None:
    bell_at = datetime.now(UTC)
    lane = RoundFiveRuntimeLaneSnapshot(
        id="competitor",
        phase="provisioning_proxy",
        elapsed_at_snapshot_ms=0,
        status="Creating Proxy",
    )
    assert bell_at + timedelta(milliseconds=lane.elapsed_at_snapshot_ms) == bell_at


async def test_adapter_reads_sequenced_progress_from_the_persistent_job_registry() -> None:
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter.config = SimpleNamespace(command_timeout_seconds=10, poll_interval_seconds=0)
    adapter._sleep = lambda _delay: asyncio.sleep(0)
    invocations = iter(
        (
            {"Status": "InProgress", "StandardOutputContent": ""},
            {"Status": "Success", "StandardOutputContent": ""},
        )
    )

    async def invocation(active):
        del active
        return next(invocations)

    async def status(ssm, *, job_id):
        del ssm
        return {
            "protocol": "round5-job-v3",
            "job_id": job_id,
            "latest_progress": {
                "sequence": 7,
                "protocol": live.FANIN_PROTOCOL,
                "schema_version": live.FANIN_SCHEMA_VERSION,
                "lane_id": "lakebase",
                "phase": "holding",
                "initiated_clients": 10_000,
                "authenticated_clients": 10_000,
                "held_clients": 10_000,
                "peak_held_clients": 10_000,
                "terminal_failures": 0,
                "elapsed_ms": 3_500,
                "time_to_target_ms": 3_500,
            },
        }

    adapter._get_invocation = invocation
    adapter._query_job_status = status
    updates = []

    async def on_progress(update):
        updates.append(update)

    result = await adapter._wait_for_terminal(
        SimpleNamespace(job_id="a" * 64, clients=SimpleNamespace(ssm=object())),
        on_progress=on_progress,
    )

    assert result["Status"] == "Success"
    assert len(updates) == 1
    assert updates[0].held_clients == 10_000


async def test_ten_minute_proxy_wait_retains_the_720_second_dispatch_margin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    origin = datetime(2026, 9, 15, tzinfo=UTC)

    class ClockDatetime(datetime):
        current = origin

        @classmethod
        def now(cls, tz=None):
            value = cls.current
            return value if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(live, "datetime", ClockDatetime)
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._capsule_refresh_lock = asyncio.Lock()
    adapter._prepared_clients = SimpleNamespace(
        expires_at=origin + timedelta(minutes=15)
    )
    minted = []

    async def mint(context_id):
        minted.append(context_id)
        return SimpleNamespace(
            expires_at=ClockDatetime.current + timedelta(minutes=15)
        )

    adapter._assumed_clients = mint

    await adapter.ensure_dispatch_capsule("proxy-start")
    assert minted == []
    ClockDatetime.current = origin + timedelta(minutes=10)
    expires_at = await adapter.ensure_dispatch_capsule("proxy-ten-minutes")
    assert minted == ["proxy-ten-minutes"]
    assert (
        expires_at - ClockDatetime.current
    ).total_seconds() > live.DISPATCH_CAPSULE_SAFETY_SECONDS
    await adapter.ensure_dispatch_capsule(
        "dispatch",
        refresh_lead_seconds=0,
    )
    assert minted == ["proxy-ten-minutes"]


async def test_durable_result_chunks_reassemble_without_original_stdout() -> None:
    encoded = "A" * 17_123
    digest = hashlib.sha256(encoded.encode("ascii")).hexdigest()
    chunk_size = runner.JOB_RESULT_CHUNK_CHARS
    chunks = [
        encoded[index : index + chunk_size]
        for index in range(0, len(encoded), chunk_size)
    ]
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter.config = SimpleNamespace(runner_instance_id="i-0123456789abcdef0")
    adapter._sleep = lambda _delay: asyncio.sleep(0)

    async def send_control(ssm, *, action, job_id, chunk_index=None):
        del ssm, action, job_id
        return f"chunk-{chunk_index}"

    class Ssm:
        def get_command_invocation(self, **kwargs):
            index = int(str(kwargs["CommandId"]).removeprefix("chunk-"))
            document = {
                "protocol": runner.JOB_PROTOCOL,
                "schema_version": 3,
                "job_id": "a" * 64,
                "chunk_index": index,
                "chunk_count": len(chunks),
                "result_sha256": digest,
                "payload": chunks[index],
            }
            return {
                "Status": "Success",
                "StandardOutputContent": (
                    runner.JOB_RESULT_PREFIX
                    + json.dumps(document, sort_keys=True, separators=(",", ":"))
                ),
            }

    adapter._send_job_control = send_control
    rebuilt = await adapter._query_job_result(
        Ssm(),
        job_id="a" * 64,
        status={
            "state": "completed",
            "settled": True,
            "result_available": True,
            "result_chunks": len(chunks),
            "result_size": len(encoded),
            "result_sha256": digest,
        },
    )

    assert rebuilt == encoded


async def test_generation_five_status_visibility_race_keeps_the_lane_alive() -> None:
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter.config = SimpleNamespace(
        runner_instance_id="i-0123456789abcdef0",
    )
    adapter._sleep = lambda _delay: asyncio.sleep(0)

    class InvocationNotVisibleYet(Exception):
        response = {"Error": {"Code": "InvocationDoesNotExist"}}

    calls = 0

    class Ssm:
        def get_command_invocation(self, **kwargs):
            nonlocal calls
            del kwargs
            calls += 1
            if calls == 1:
                raise InvocationNotVisibleYet
            return {
                "Status": "Success",
                "StandardOutputContent": (
                    'JOB_STATUS:{"protocol":"round5-job-v3",'
                    '"schema_version":3,"job_id":"'
                    + "a" * 64
                    + '","state":"running","result_available":false,'
                    '"latest_progress":null}'
                ),
            }

    async def send_control(ssm, *, action, job_id):
        del ssm, action, job_id
        return "command-status"

    adapter._send_job_control = send_control

    status = await adapter._query_job_status(
        Ssm(),
        job_id="a" * 64,
    )

    assert calls == 2
    assert status["state"] == "running"


async def test_primary_adapter_error_settles_job_before_provider_cleanup() -> None:
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter.config = SimpleNamespace(command_timeout_seconds=600)
    adapter._pending = None
    adapter._active = None
    adapter._settlement_debt = {}
    adapter._validate_run_id = lambda _run_id: None
    adapter._validate_request = lambda _run_id, _request: None
    order: list[str] = []

    class PrimaryObserverError(RuntimeError):
        pass

    class SecondaryCleanupError(RuntimeError):
        pass

    async def send_command(ssm, request, *, timeout_seconds):
        del ssm, request, timeout_seconds
        return "command-run"

    async def wait_for_terminal(active, *, timeout_seconds, on_progress):
        del active, timeout_seconds, on_progress
        raise PrimaryObserverError("status observer failed")

    async def settle(active):
        del active
        order.append("job_settled")
        raise SecondaryCleanupError("secondary cleanup detail")

    adapter._send_command = send_command
    adapter._wait_for_terminal = wait_for_terminal
    adapter._cancel_and_settle = settle
    clients = SimpleNamespace(ssm=object())

    with pytest.raises(PrimaryObserverError, match="status observer failed"):
        await adapter._execute_reserved(
            "job-run",
            {
                "action": "run_lane_v3",
                "job_id": "a" * 64,
            },
            prepared_clients=clients,
        )

    assert order == ["job_settled"]
    assert adapter._active is None


async def test_setup_cleanup_waits_for_lane_settlement_before_provider_cleanup() -> None:
    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._setup_task = None
    engine._cleanup_start_lock = asyncio.Lock()
    engine._cleanup_bout_id = None
    engine._setup_result = None
    engine._resident_bindings = {}
    engine._active_run_ids = {"lakebase": "job-one"}
    order: list[str] = []

    async def active_lane() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            order.append("job_settled")
            engine._active_run_ids.clear()
            raise

    lane_task = asyncio.create_task(active_lane())
    engine._lane_bursts = {"lakebase": lane_task}

    class Setup:
        async def begin_cleanup(self, bout_id):
            assert bout_id == "bout-one"
            order.append("provider_cleanup")

    engine._setup_orchestrator = Setup()
    engine._lane_adapters = {}
    await asyncio.sleep(0)

    await engine._stop_setup_and_begin_cleanup_once("bout-one")

    assert order == ["job_settled", "provider_cleanup"]
    assert engine._cleanup_bout_id == "bout-one"


async def test_abandoned_arm_settles_prepared_resident_before_rewarm() -> None:
    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._setup_task = None
    engine._cleanup_start_lock = asyncio.Lock()
    engine._cleanup_bout_id = None
    engine._setup_result = None
    engine._lane_bursts = {}
    engine._active_run_ids = {}
    binding = SimpleNamespace(job_id="prepared-job")
    engine._resident_bindings = {"lakebase": binding}
    order: list[str] = []

    class LaneAdapter:
        async def cancel_resident(self, *, binding: object) -> None:
            assert binding is engine._resident_bindings["lakebase"]
            order.append("prepared_resident_settled")

    class Setup:
        async def begin_cleanup(self, bout_id: str) -> None:
            assert bout_id == "bout-one"
            order.append("provider_cleanup")

    engine._lane_adapters = {"lakebase": LaneAdapter()}
    engine._setup_orchestrator = Setup()

    await engine._stop_setup_and_begin_cleanup_once("bout-one")

    assert order == ["prepared_resident_settled", "provider_cleanup"]
    assert engine._resident_bindings == {}
    assert engine._cleanup_bout_id == "bout-one"


async def test_abandoned_arm_timeout_defers_resident_but_starts_provider_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(live, "ROUND5_ABANDONED_ARM_SETTLEMENT_SECONDS", 0.01)
    caplog.set_level("ERROR", logger="server.connection_spike_live")
    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._setup_task = None
    engine._cleanup_start_lock = asyncio.Lock()
    engine._cleanup_bout_id = None
    engine._setup_result = None
    engine._lane_bursts = {}
    binding = SimpleNamespace(job_id="prepared-job")
    engine._resident_bindings = {"lakebase": binding}
    engine._active_run_ids = {"lakebase": binding.job_id}
    order: list[str] = []

    class Transport:
        async def cancel(
            self,
            *,
            binding: object,
            await_settlement: bool,
        ) -> None:
            assert await_settlement is True
            await asyncio.Event().wait()

    class Setup:
        async def begin_cleanup(self, bout_id: str) -> None:
            assert bout_id == "bout-one"
            order.append("provider_cleanup")

    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._resident_transport = Transport()
    adapter._resident_settlement_debt = {}
    adapter._resident_release_events = {}
    engine._lane_adapters = {"lakebase": adapter}
    engine._setup_orchestrator = Setup()

    await asyncio.wait_for(
        engine._stop_setup_and_begin_cleanup_once("bout-one"),
        timeout=0.2,
    )

    assert order == ["provider_cleanup"]
    assert engine._resident_bindings == {"lakebase": binding}
    assert engine._active_run_ids == {"lakebase": binding.job_id}
    assert adapter._resident_settlement_debt == {"prepared-job": binding}
    assert engine._cleanup_bout_id == "bout-one"
    assert "round5_abandoned_arm_resident_settlement_deferred" in caplog.text


async def test_duplicate_lane_callback_is_refused_before_a_second_dispatch() -> None:
    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._armed = object()
    existing = asyncio.create_task(asyncio.sleep(0))
    engine._lane_bursts = {"lakebase": existing}
    stop = ConnectionSpikeSetupLaneStop(
        lane_id="lakebase",
        launched_ns=1,
        stopped_ns=2,
        credential_sha256="a" * 64,
        endpoint_host="pooled.example.test",
    )

    with pytest.raises(
        ConnectionSpikeLiveOperationError,
        match="already dispatched",
    ):
        await engine._start_lane_burst(stop)

    await existing
    assert engine._lane_bursts == {"lakebase": existing}


async def test_asymmetric_failure_preserves_verified_lane_evidence() -> None:
    manager = RunManager()
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    record = await manager._record(created.id)
    record.snapshot.state = SessionState.RUNNING
    record.snapshot.run_started_at = datetime.now(UTC)
    record.snapshot.round5_runtime = RoundFiveRuntimeSnapshot(
        warm_generation=1,
        bell_id="bell-asymmetric",
        revision=1,
        state="running",
        bell_at_utc=datetime.now(UTC),
        lanes={
            "lakebase": RoundFiveRuntimeLaneSnapshot(
                id="lakebase",
                phase="verified",
                elapsed_at_snapshot_ms=10,
                bell_to_10000_observed_ms=10,
                clients_initiated=10_000,
                clients_authenticated=10_000,
                held_clients=10_000,
                peak_held_clients=10_000,
                sampled_queries_succeeded=64,
                status="exact Lakebase proof",
            ),
            "competitor": RoundFiveRuntimeLaneSnapshot(
                id="competitor",
                phase="provisioning_proxy",
                elapsed_at_snapshot_ms=10,
                status="provisioning",
            ),
        },
    )
    lakebase = record.snapshot.lanes["lakebase"]
    lakebase.state = LaneState.VERIFIED
    lakebase.status = "exact Lakebase proof"
    lakebase.evidence = {"held_clients_at_gate": 10_000}
    lakebase.error = None

    await manager._finish_connection_spike_failure(
        record,
        "Aurora dispatch failed",
        cleanup_verified=True,
    )
    failed = await manager.get(created.id)

    assert failed.lanes["lakebase"].state == LaneState.VERIFIED
    assert failed.lanes["lakebase"].status == "exact Lakebase proof"
    assert failed.lanes["lakebase"].evidence == {
        "held_clients_at_gate": 10_000
    }


async def test_global_failure_after_two_verified_lanes_is_terminal_once() -> None:
    manager = RunManager()
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    record = await manager._record(created.id)
    now = datetime.now(UTC)
    record.snapshot.state = SessionState.RUNNING
    record.snapshot.run_started_at = now
    record.snapshot.comparison = None
    record.snapshot.round5_runtime = RoundFiveRuntimeSnapshot(
        warm_generation=1,
        bell_id="bell-global-failure",
        revision=7,
        state="running",
        bell_at_utc=now,
        lanes={
            lane_id: RoundFiveRuntimeLaneSnapshot(
                id=lane_id,
                phase="verified",
                elapsed_at_snapshot_ms=10 + index,
                bell_to_10000_observed_ms=10 + index,
                clients_initiated=10_000,
                clients_authenticated=10_000,
                held_clients=10_000,
                peak_held_clients=10_000,
                sampled_queries_succeeded=64,
                status="independently verified",
            )
            for index, lane_id in enumerate(("lakebase", "competitor"))
        },
    )
    for lane in record.snapshot.lanes.values():
        lane.state = LaneState.VERIFIED
        lane.status = "independently verified"
        lane.evidence = {"held_clients_at_gate": 10_000}
    await manager._finish_connection_spike_failure(
        record,
        "Global launch skew invalid",
        cleanup_verified=True,
    )
    event_count = len(record.event_log.events)
    await manager._finish_connection_spike_failure(
        record,
        "late duplicate",
        cleanup_verified=True,
    )
    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    assert failed.comparison is None
    assert all(lane.state == LaneState.VERIFIED for lane in failed.lanes.values())
    assert failed.round5_runtime is not None
    assert failed.round5_runtime.state == "failed"
    assert {
        lane.phase for lane in failed.round5_runtime.lanes.values()
    } == {"verified"}
    assert len(record.event_log.events) == event_count
    await manager.close()


async def test_cleanup_is_not_retryable_until_a_cleanup_attempt_fails() -> None:
    manager = RunManager()
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    record = await manager._record(created.id)

    async def leave_cleanup_in_progress(_record):
        return None

    manager._begin_connection_spike_cleanup_handoff = leave_cleanup_in_progress
    await manager._finish_connection_spike_failure(
        record,
        "Aurora dispatch failed",
        cleanup_verified=False,
    )
    in_progress = await manager.get(created.id)
    assert in_progress.round5_setup is not None
    assert in_progress.round5_setup.cleanup_retryable is False
    assert in_progress.round5_setup.state.value == "failed"

    await manager._mark_connection_spike_cleanup_pending(record)
    failed_cleanup = await manager.get(created.id)
    assert failed_cleanup.round5_setup is not None
    assert failed_cleanup.round5_setup.cleanup_retryable is True
    assert failed_cleanup.round5_setup.state.value == "cleanup_failed"
    await manager.close()


async def test_cleanup_retry_with_disappeared_lease_returns_to_retryable_state() -> None:
    manager = RunManager()
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    record = await manager._record(created.id)
    record.snapshot.state = SessionState.FAILED
    record.snapshot.failure = "bout failed"
    record.round5_lease = None

    class Engine:
        async def reconcile_failed_cleanup(self, bout_id, fencing_token):
            raise AssertionError((bout_id, fencing_token))

    assert not await manager._retry_connection_spike_cleanup(record, Engine())
    snapshot = await manager.get(created.id)
    assert snapshot.state == SessionState.FAILED
    assert snapshot.round5_setup is not None
    assert snapshot.round5_setup.cleanup_retryable is True
    assert snapshot.round5_setup.cleanup_failure is not None
    await manager.close()


async def test_run_returns_one_bell_and_two_advancing_clocks_without_provider_progress() -> None:
    release = asyncio.Event()
    setup_entered = asyncio.Event()
    cost_open_entered = asyncio.Event()
    release_cost_open = asyncio.Event()

    class Engine:
        has_timed_setup = True
        _armed = object()

        def bind_claim(self, claim) -> None:
            assert claim.lakebase_job_id != claim.competitor_job_id

        async def prepare(self, bout_id, fencing_token) -> None:
            assert bout_id and fencing_token > 0

        async def precommit_launch_intent(self, bout_id, fencing_token) -> None:
            # Required bell seam (req #6): manager calls it directly before T0.
            assert bout_id and fencing_token > 0

        async def setup(
            self,
            bout_id,
            fencing_token,
            on_setup_progress,
            on_lane_progress,
            on_lane_result,
        ):
            del (
                bout_id,
                fencing_token,
                on_setup_progress,
                on_lane_progress,
                on_lane_result,
            )
            setup_entered.set()
            await release.wait()

        async def cancel_setup_and_settle(self, bout_id) -> None:
            del bout_id
            release.set()

    engine = Engine()
    claim = SimpleNamespace(
        claim_id="claim-one",
        lakebase_job_id="a" * 64,
        competitor_job_id="b" * 64,
    )
    warm_slot = SimpleNamespace(claim=claim)
    capsule = SimpleNamespace(
        variant_contexts={
            Round5Variant.AURORA: engine,
            Round5Variant.RDS: engine,
        }
    )

    class Warm:
        ring_ready = True
        store = SimpleNamespace()

        async def claim(self, **kwargs):
            del kwargs
            return warm_slot, capsule

        def release_claim_active(self, claim_id):
            del claim_id

        async def accept_bell(self, claim_id):
            assert claim_id == "claim-one"
            return BellContext(
                bell_id="bell-one",
                claim_id=claim_id,
                warm_generation=4,
                bout_id="bout-one",
                bout_fence=1,
                bell_at_utc=datetime(2026, 9, 15, tzinfo=UTC),
                t0_monotonic_ns=1_000_000_000,
            )

    clock_value = 1_050_000_000
    manager = RunManager(
        connection_spike_factory=lambda competitor: engine,
        round5_warm_coordinator=Warm(),
        clock_ns=lambda: clock_value,
    )

    async def blocking_cost_open(*_args, **_kwargs):
        cost_open_entered.set()
        await release_cost_open.wait()
        return None

    manager._open_cost_bout = blocking_cost_open
    operator = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    await manager.start_arm(created.id, operator)
    for _ in range(100):
        armed = await manager.get(created.id)
        if armed.state == SessionState.ARMED:
            break
        await asyncio.sleep(0)
    assert armed.state == SessionState.ARMED

    running = await asyncio.wait_for(
        manager.start_run(created.id, operator),
        timeout=1,
    )

    assert running.state == SessionState.RUNNING
    assert running.round5_runtime is not None
    assert running.round5_runtime.bell_id == "bell-one"
    assert all(
        lane.elapsed_at_snapshot_ms == 0
        for lane in running.round5_runtime.lanes.values()
    )
    assert running.round5_clock_projection is not None
    assert all(
        elapsed >= 50
        for elapsed in running.round5_clock_projection.elapsed_ms.values()
    )
    running_revision = running.round5_runtime.revision
    projected_once = await manager.get(created.id)
    projected_twice = await manager.get(created.id)
    assert projected_once.round5_runtime is not None
    assert projected_twice.round5_runtime is not None
    assert projected_once.round5_runtime.revision == running_revision
    assert projected_twice.round5_runtime.revision == running_revision
    duplicate = await manager.start_run(created.id, operator)
    assert duplicate.round5_runtime is not None
    assert duplicate.round5_runtime.bell_id == "bell-one"
    assert duplicate.round5_runtime.revision == running_revision
    await asyncio.wait_for(setup_entered.wait(), timeout=1)
    await asyncio.wait_for(cost_open_entered.wait(), timeout=1)
    assert not release_cost_open.is_set()
    release_cost_open.set()
    release.set()
    await asyncio.sleep(0)
    await manager.close()


async def test_progress_terminal_inversion_is_ordered_and_late_callback_is_ignored() -> None:
    class Engine:
        has_timed_setup = True
        _armed = object()

        def bind_claim(self, claim) -> None:
            assert claim.claim_id == "claim-order"

        async def prepare(self, bout_id, fencing_token) -> None:
            assert bout_id and fencing_token > 0

        async def precommit_launch_intent(self, bout_id, fencing_token) -> None:
            # Required bell seam (req #6): manager calls it directly before T0.
            assert bout_id and fencing_token > 0

        async def setup(
            self,
            bout_id,
            fencing_token,
            on_setup_progress,
            on_lane_progress,
            on_lane_result,
        ):
            del bout_id, fencing_token, on_setup_progress
            await on_lane_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="ramping",
                    initiated_clients=5_000,
                    authenticated_clients=4_990,
                    held_clients=4_990,
                    peak_held_clients=4_990,
                    sampled_queries_succeeded=0,
                    time_to_target_ms=None,
                    elapsed_ms=10_000.0,
                    sequence=1,
                )
            )
            await on_lane_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="holding",
                    initiated_clients=10_000,
                    authenticated_clients=10_000,
                    held_clients=10_000,
                    peak_held_clients=10_000,
                    sampled_queries_succeeded=64,
                    time_to_target_ms=20_000.0,
                    elapsed_ms=20_000.0,
                    sequence=2,
                )
            )
            await on_lane_result(finalize_fanin_lane(raw_fanin_lane("lakebase")))

            late_release = asyncio.Event()

            async def late_progress() -> None:
                await late_release.wait()
                await on_lane_progress(
                    SimpleNamespace(
                        lane_id="lakebase",
                        phase="holding",
                        initiated_clients=10_000,
                        authenticated_clients=10_000,
                        held_clients=10_000,
                        peak_held_clients=10_000,
                        sampled_queries_succeeded=64,
                        time_to_target_ms=20_000.0,
                        elapsed_ms=21_000.0,
                        sequence=3,
                    )
                )

            late_task = asyncio.create_task(late_progress())
            try:
                await on_lane_result(
                    SimpleNamespace(
                        lane_id="competitor",
                        initiated_clients=7_162,
                        authenticated_clients=7_134,
                        cancelled_clients=28,
                        held_clients_at_gate=7_134,
                        terminal_failures=0,
                        sampled_queries_succeeded=32,
                        verified=True,
                        gates=SimpleNamespace(passed=True, telemetry=True),
                    )
                )
            finally:
                late_release.set()
                await late_task

        async def cancel_setup_and_settle(self, bout_id) -> None:
            del bout_id

    engine = Engine()
    claim = SimpleNamespace(
        claim_id="claim-order",
        lakebase_job_id="c" * 64,
        competitor_job_id="d" * 64,
    )
    warm_slot = SimpleNamespace(claim=claim)
    capsule = SimpleNamespace(
        variant_contexts={
            Round5Variant.AURORA: engine,
            Round5Variant.RDS: engine,
        }
    )

    class Warm:
        ring_ready = True
        store = SimpleNamespace()

        async def claim(self, **kwargs):
            del kwargs
            return warm_slot, capsule

        def release_claim_active(self, claim_id):
            del claim_id

        async def accept_bell(self, claim_id):
            return BellContext(
                bell_id="bell-order",
                claim_id=claim_id,
                warm_generation=9,
                bout_id="bout-order",
                bout_fence=1,
                bell_at_utc=datetime(2026, 9, 16, tzinfo=UTC),
                t0_monotonic_ns=1_000_000_000,
            )

    manager = RunManager(
        connection_spike_factory=lambda competitor: engine,
        round5_warm_coordinator=Warm(),
        clock_ns=lambda: 11_000_000_000,
    )
    operator = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    await manager.start_arm(created.id, operator)
    for _ in range(100):
        if (await manager.get(created.id)).state == SessionState.ARMED:
            break
        await asyncio.sleep(0)
    await manager.start_run(created.id, operator)
    for _ in range(100):
        failed = await manager.get(created.id)
        if failed.state == SessionState.FAILED:
            break
        await asyncio.sleep(0)
    assert failed.state == SessionState.FAILED
    assert failed.round5_runtime is not None
    assert failed.lanes["lakebase"].state == LaneState.VERIFIED
    assert failed.round5_runtime.lanes["lakebase"].phase == "verified"
    assert failed.round5_runtime.lanes["lakebase"].held_clients == 10_000
    assert (
        failed.round5_runtime.lanes["lakebase"].bell_to_10000_observed_ms
        is not None
    )
    assert failed.round5_runtime.lanes["lakebase"].progress_revision == 2
    assert failed.lanes["competitor"].state == LaneState.FAILED

    record = manager._records[created.id]
    snapshots = [
        event.payload.get("session")
        for event in record.event_log.events
        if isinstance(event.payload.get("session"), dict)
        and event.payload["session"].get("round5_runtime") is not None
    ]
    revisions = [
        int(snapshot["round5_runtime"]["revision"])
        for snapshot in snapshots
    ]
    assert revisions == sorted(revisions)
    terminal_events = [
        event
        for event in record.event_log.events
        if event.event in {"run_finished", "session_failed", "towel_started"}
    ]
    assert [event.event for event in terminal_events] == ["session_failed"]
    terminal_revision = int(
        terminal_events[0].payload["session"]["round5_runtime"]["revision"]
    )
    assert terminal_revision > 2
    assert all(
        snapshot["round5_runtime"]["state"] != "running"
        for snapshot in snapshots
        if int(snapshot["round5_runtime"]["revision"]) >= terminal_revision
    )
    event_count = len(record.event_log.events)
    repeated_towel = await manager.start_towel(created.id, operator)
    assert repeated_towel.state == SessionState.FAILED
    assert repeated_towel.towel is None
    assert repeated_towel.round5_runtime is not None
    assert repeated_towel.round5_runtime.revision == failed.round5_runtime.revision
    assert len(record.event_log.events) == event_count
    lease = record.lease
    if lease is not None:
        await manager._handle_lost_lease(record, lease)
    after_lease_loss = await manager.get(created.id)
    assert after_lease_loss.state == SessionState.FAILED
    assert after_lease_loss.round5_runtime is not None
    assert after_lease_loss.round5_runtime.revision == failed.round5_runtime.revision
    assert [
        event.event
        for event in record.event_log.events
        if event.event in {"run_finished", "session_failed", "towel_started"}
    ] == ["session_failed"]
    await manager.close()


async def test_v3_towel_wins_lock_before_natural_setup_return() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class Engine:
        has_timed_setup = True
        _armed = object()

        def bind_claim(self, claim) -> None:
            assert claim.claim_id == "claim-towel-first"

        async def prepare(self, bout_id, fencing_token) -> None:
            assert bout_id and fencing_token > 0

        async def precommit_launch_intent(self, bout_id, fencing_token) -> None:
            # Required bell seam (req #6): manager calls it directly before T0.
            assert bout_id and fencing_token > 0

        async def setup(
            self,
            bout_id,
            fencing_token,
            on_setup_progress,
            on_lane_progress,
            on_lane_result,
        ):
            del (
                bout_id,
                fencing_token,
                on_setup_progress,
                on_lane_progress,
                on_lane_result,
            )
            entered.set()
            await release.wait()

        async def cancel_setup_and_settle(self, bout_id) -> None:
            del bout_id
            release.set()

    engine = Engine()
    claim = SimpleNamespace(
        claim_id="claim-towel-first",
        lakebase_job_id="e" * 64,
        competitor_job_id="f" * 64,
    )
    warm_slot = SimpleNamespace(claim=claim)
    capsule = SimpleNamespace(
        variant_contexts={
            Round5Variant.AURORA: engine,
            Round5Variant.RDS: engine,
        }
    )

    class Warm:
        ring_ready = True
        store = SimpleNamespace()

        async def claim(self, **kwargs):
            del kwargs
            return warm_slot, capsule

        def release_claim_active(self, claim_id):
            del claim_id

        async def accept_bell(self, claim_id):
            return BellContext(
                bell_id="bell-towel-first",
                claim_id=claim_id,
                warm_generation=9,
                bout_id="bout-towel-first",
                bout_fence=1,
                bell_at_utc=datetime(2026, 9, 16, tzinfo=UTC),
                t0_monotonic_ns=1_000_000_000,
            )

    manager = RunManager(
        connection_spike_factory=lambda competitor: engine,
        round5_warm_coordinator=Warm(),
        clock_ns=lambda: 11_000_000_000,
    )
    operator = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    await manager.start_arm(created.id, operator)
    for _ in range(100):
        if (await manager.get(created.id)).state == SessionState.ARMED:
            break
        await asyncio.sleep(0)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(entered.wait(), timeout=1)

    towelled = await manager.start_towel(created.id, operator)
    assert towelled.state == SessionState.TOWELLED
    assert towelled.round5_runtime is not None
    towel_revision = towelled.round5_runtime.revision
    release.set()
    record = manager._records[created.id]
    assert record.task is not None
    await asyncio.gather(record.task, return_exceptions=True)
    canonical = await manager.get(created.id)

    assert canonical.state == SessionState.TOWELLED
    assert canonical.round5_runtime is not None
    assert canonical.round5_runtime.state == "towelled"
    assert canonical.round5_runtime.revision >= towel_revision
    assert [
        event.event
        for event in record.event_log.events
        if event.event in {"run_finished", "session_failed", "towel_started"}
    ] == ["towel_started"]
    await manager.close()


def _real_arm_prepare_engine(transport: object) -> LiveConnectionSpikeEngine:
    """A real LiveConnectionSpikeEngine wired for its ARM ``prepare`` seam only.

    Exercises the production code path the acceptance harness's fake no-op
    ``prepare`` cannot reach: the O(1) coordination-only orchestrator bind plus
    the bounded Lakebase resident rebind. Only the two pure request/binding
    helpers are stubbed (they need a full sealed FanInArm otherwise); the timed
    behaviour under test -- the bounded ``transport.stage`` -- is the real code.
    """

    class _Orchestrator:
        async def prepare(self, bout_id: str, fencing_token: int) -> None:
            # Production orchestrator prepare is coordination-only and O(1); it
            # binds the already-warmed context to the bout without slow AWS work.
            assert bout_id and fencing_token > 0

    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._setup_orchestrator = _Orchestrator()
    engine._bound_claim = object()
    engine._armed = object()
    engine._adapter = SimpleNamespace(
        config=SimpleNamespace(targets=(SimpleNamespace(lane_id="lakebase"),))
    )
    engine._job_ids = {"lakebase": "job-lakebase"}
    engine._lane_adapters = {
        "lakebase": SimpleNamespace(_resident_transport=transport)
    }
    engine._resident_bindings = {}
    # The two pure helpers need a fully sealed arm to build a real request; stub
    # them so the test isolates the bounded-staging behaviour on the real seam.
    engine._fanin_request = lambda *args, **kwargs: {"request": "lakebase"}
    engine._resident_binding = lambda lane_id, request: SimpleNamespace(lane_id=lane_id)
    return engine


async def test_real_arm_prepare_is_bounded_when_the_ring_is_not_warm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real ARM must fail fast, not block on the 720s bout deadline.

    A READY warm slot has already staged both resident pools backstage, so the
    ARM rebind is O(1)-shaped. If the resident never acknowledges PREPARED, ARM
    must surface a bounded failure within the ARM budget instead of hanging for
    the full bout-execution deadline. This runs the *real*
    ``LiveConnectionSpikeEngine.prepare``.
    """

    class _NeverPreparedTransport:
        def __init__(self) -> None:
            self.stage_calls = 0

        async def stage(self, *, binding: object, request: object) -> None:
            self.stage_calls += 1
            await asyncio.Event().wait()  # resident never publishes PREPARED

    transport = _NeverPreparedTransport()
    engine = _real_arm_prepare_engine(transport)
    monkeypatch.setattr(live, "ROUND5_ARM_STAGE_DEADLINE_SECONDS", 0.2)

    with pytest.raises(ConnectionSpikeLiveOperationError, match="bounded staging budget"):
        # The outer wait_for is only a test-safety net: if the production bound
        # regressed we fail here rather than hanging the suite.
        await asyncio.wait_for(engine.prepare("bout-arm", 7), timeout=5)
    assert transport.stage_calls == 1
    # The lane binding is recorded before staging so a later cancellation can
    # still find and settle the resident, even on a bounded ARM failure.
    assert "lakebase" in engine._resident_bindings


async def test_real_arm_prepare_returns_promptly_against_a_warm_resident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Against a warm-staged resident the real ARM rebind completes immediately."""

    class _WarmTransport:
        def __init__(self) -> None:
            self.stage_calls = 0

        async def stage(self, *, binding: object, request: object) -> None:
            self.stage_calls += 1  # warm pool acknowledges PREPARED at once

    transport = _WarmTransport()
    engine = _real_arm_prepare_engine(transport)
    monkeypatch.setattr(live, "ROUND5_ARM_STAGE_DEADLINE_SECONDS", 0.2)

    await asyncio.wait_for(engine.prepare("bout-arm", 7), timeout=5)
    assert transport.stage_calls == 1
    assert "lakebase" in engine._resident_bindings
