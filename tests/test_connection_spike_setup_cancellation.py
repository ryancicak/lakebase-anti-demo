"""The bound on Round 5's cancellation teardown.

Round 5 already issued teardown when a timed setup was cancelled, and already
did it fire-and-forget.  What it had no ceiling on was the *wait*: both the
drain of the cancelled lanes and ``begin_cleanup``'s command settlement reach
AWS through ``_call``, which re-awaits its shielded worker thread rather than
abandoning a mutation mid-flight.  A wedged SSM or RDS endpoint therefore held
a Ctrl-C open indefinitely, and a shutdown that looks hung gets killed, which
loses the teardown *and* the log line naming what leaked.

Every stall test here hangs against the unbounded version: the wait is the
thing under test, so a fake that merely returns slowly would prove nothing.
Each one wedges a real worker thread the way an unreachable endpoint does, and
always releases it, so the loop's executor can still shut down.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from types import SimpleNamespace

import boto3
import pytest
from botocore.stub import Stubber

from server.connection_spike_journal import (
    CreationScope,
    JournalEvent,
    LifecycleState,
    ResourceObservation,
    ResourceSpec,
    Round5CreationCoordinator,
)
from server.connection_spike_live import (
    ConnectionSpikeSetupConfig,
    LiveConnectionSpikeSetupOrchestrator,
)

ACCOUNT = "123456789012"
REGION = "us-west-2"
BOUND = 0.1
# Generous next to BOUND, unreachable if the wait is unbounded.
PATIENCE = 3.0


def _config(**overrides: object) -> ConnectionSpikeSetupConfig:
    config = ConnectionSpikeSetupConfig(
        region=REGION,
        expected_account_id=ACCOUNT,
        baseline_control_role_arn=f"arn:aws:iam::{ACCOUNT}:role/baseline-control",
        runner_instance_id="i-0123456789abcdef0",
        vpc_id="vpc-sealed",
        proxy_subnet_ids=("subnet-a", "subnet-b"),
        lakebase_direct_host="lakebase-direct.test",
        lakebase_pooled_host="lakebase-pooled.test",
        competitor_id="rds_postgres",
        competitor_target_id="rds-source",
        competitor_resource_id="db-RESOURCE",
        competitor_direct_host="rds-direct.test",
        competitor_security_group_id="sg-rds",
        runner_security_group_id="sg-runner",
        proxy_service_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-service",
        proxy_service_policy_name="proxy-service-secrets",
        aurora_proxy_secret_arn=(
            f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:aurora-proxy"
        ),
        rds_proxy_secret_arn=f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:rds-proxy",
        deterministic_name_prefix="anti-demo-r5",
        ownership_tags=(("Owner", "anti-demo"), ("owner", "anti-demo")),
        trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
        trust_bundle_sha256="b" * 64,
        runner_public_key_sha256="c" * 64,
        baseline_sha256="d" * 64,
        lakebase_credential_sha256="e" * 64,
        competitor_credential_sha256="f" * 64,
    )
    return replace(config, **overrides) if overrides else config


class _Fence:
    async def assert_current(self, scope: CreationScope) -> None:
        del scope


class _RecordingJournal:
    """An in-memory stand-in for the durable append-only journal."""

    def __init__(self) -> None:
        self.committed: list[JournalEvent] = []

    async def commit(self, event, *, authority_scope=None) -> None:
        del authority_scope
        self.committed.append(event)

    async def events(self, scope):
        del scope
        return tuple(self.committed)

    async def scopes(self, bout_id):
        del bout_id
        return ()


class _EmptyJournal:
    async def commit(self, event, *, authority_scope=None) -> None:
        del event, authority_scope

    async def events(self, scope):
        del scope
        return ()

    async def scopes(self, bout_id):
        del bout_id
        return ()


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


async def _until(predicate, description: str) -> None:
    deadline = time.monotonic() + PATIENCE
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(0.005)


class _Ssm:
    """A runner endpoint whose invocations never reach a terminal state.

    ``cancel_on_send`` cancels the caller from inside the second ``SendCommand``
    worker thread, which is the one moment worth testing: SSM already holds both
    commands and the process holds neither identifier. ``hold_send`` then keeps
    that second send unresolved, so the identifier never arrives at all.
    """

    def __init__(
        self,
        *,
        terminal_lanes: frozenset[str] = frozenset(),
        wedge_cancel: bool = True,
        cancel_on_send: Callable[[], None] | None = None,
        hold_send: bool = False,
    ) -> None:
        self.requests: dict[str, dict[str, object]] = {}
        self.cancel = _Wedge(wedged=wedge_cancel)
        self.send = _Wedge(wedged=hold_send)
        self._terminal_lanes = terminal_lanes
        self._cancel_on_send = cancel_on_send
        self._send_lock = threading.Lock()

    def send_command(self, **kwargs: object) -> dict[str, object]:
        import base64
        import gzip

        parameters = kwargs["Parameters"]
        assert isinstance(parameters, dict)
        encoded = str(parameters["commands"][0]).rsplit(" ", 1)[1]
        request = json.loads(gzip.decompress(base64.urlsafe_b64decode(encoded)))
        with self._send_lock:
            command_id = f"command-{len(self.requests) + 1}"
            self.requests[command_id] = request
            reached = len(self.requests)
        if self._cancel_on_send is not None and reached == 2:
            # Ordered by the callback, not by a sleep: the callback runs on the
            # event loop and only releases this thread once the cancellation has
            # been requested, so the send is guaranteed to still be unresolved.
            self._cancel_on_send()
            self.send.released.wait(30)
        return {"Command": {"CommandId": command_id}}

    def get_command_invocation(self, **kwargs: object) -> dict[str, object]:
        request = self.requests[str(kwargs["CommandId"])]
        if request["lane_id"] not in self._terminal_lanes:
            return {"Status": "Pending", "StandardOutputContent": ""}
        receipt = {
            "protocol": "connection-spike-setup-v1",
            "action": request["action"],
            "bout_id": request["bout_id"],
            "lane_id": request["lane_id"],
            "nonce": request["nonce"],
            "status": "verified",
        }
        return {
            "Status": "Success",
            "StandardOutputContent": (
                f"SETUP_RESULT:{json.dumps(receipt, separators=(',', ':'))}\n"
                f"SETUP_SETTLED:{request['nonce']}\n"
                f"RUNNER_FLOCK_RELEASED:{request['bout_id']}\n"
            ),
        }

    def cancel_command(self, **kwargs: object) -> dict[str, object]:
        return self.cancel(**kwargs)


def _orchestrator(
    monkeypatch: pytest.MonkeyPatch,
    *,
    ssm: object,
    rds: object | None = None,
    create_hook=None,
    bound: float = BOUND,
) -> tuple[LiveConnectionSpikeSetupOrchestrator, list[str]]:
    """A real orchestrator whose only fakes are the AWS clients and the journal."""

    config = _config()
    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        config,
        journal=_EmptyJournal(),
        fence=_Fence(),
        fresh_lakebase_host=_pooled_host,
        sleep=lambda _delay: asyncio.sleep(0.001),
        cancel_teardown_timeout_seconds=bound,
    )
    clients = SimpleNamespace(
        ssm=ssm,
        rds=rds if rds is not None else SimpleNamespace(),
        ec2=SimpleNamespace(),
        iam=SimpleNamespace(),
        secretsmanager=SimpleNamespace(),
    )
    created: list[str] = []
    real_coordinator = orchestrator._coordinator

    class Coordinator:
        def __init__(self, resources) -> None:
            self.resources = resources

        async def create_resource(self, scope, spec):
            del scope
            created.append(spec.resource_kind)
            if spec.resource_kind == "proxy_security_group":
                self.resources.proxy_security_group_id = "sg-round5-proxy"
            if create_hook is not None:
                await create_hook(orchestrator, clients, spec)
            return SimpleNamespace(provider_id=f"provider-{spec.ordinal}")

        async def seal(self, scope):
            del scope
            return SimpleNamespace()

        async def reconcile_incomplete(self, scope):
            del scope
            return SimpleNamespace(complete=True)

    def coordinator(scope, supplied_clients, resources):
        _unused, specs = real_coordinator(scope, supplied_clients, resources)
        return Coordinator(resources), specs

    async def noop(*args: object, **kwargs: object) -> None:
        del args, kwargs

    monkeypatch.setattr(orchestrator, "_assumed_clients", lambda _bout: _value(clients))
    monkeypatch.setattr(orchestrator, "_coordinator", coordinator)
    monkeypatch.setattr(orchestrator, "_preflight_baseline", noop)
    monkeypatch.setattr(orchestrator, "_wait_proxy_available", noop)
    monkeypatch.setattr(orchestrator, "_verify_proxy_topology", noop)
    monkeypatch.setattr(orchestrator, "_verify_journaled_resources", noop)
    return orchestrator, created


async def _value(item: object) -> object:
    return item


async def _pooled_host() -> str:
    return "lakebase-pooled.test"


async def test_an_unresponsive_ssm_endpoint_cannot_stall_the_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hangs without the bound: both lanes park in an uncancellable settle."""

    ssm = _Ssm()
    orchestrator, _created = _orchestrator(monkeypatch, ssm=ssm)
    task = asyncio.create_task(orchestrator.setup("bout-ssm-wedged", 11))
    try:
        await _until(lambda: len(ssm.requests) == 2, "both runner commands to be sent")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)
        # The settlement really was issued and then abandoned, not skipped.
        await _until(lambda: bool(ssm.cancel.calls), "the settlement to be issued")
    finally:
        ssm.cancel.release()
        await asyncio.sleep(0.05)

    assert all(set(call) == {"CommandId", "InstanceIds"} for call in ssm.cancel.calls)


async def test_a_wedged_proxy_delete_cannot_stall_the_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hangs without the bound: the RDS control plane wedges the same way SSM does."""

    wedge = _Wedge()
    ssm = _Ssm(terminal_lanes=frozenset({"lakebase"}))
    rds = SimpleNamespace(create_db_proxy=wedge)

    async def create_hook(orchestrator, clients, spec):
        if spec.resource_kind == "rds_proxy":
            await orchestrator._call(
                clients.rds.create_db_proxy,
                DBProxyName=spec.deterministic_name,
            )

    orchestrator, created = _orchestrator(
        monkeypatch,
        ssm=ssm,
        rds=rds,
        create_hook=create_hook,
    )
    task = asyncio.create_task(orchestrator.setup("bout-proxy-wedged", 11))
    try:
        await _until(wedge.entered.is_set, "the proxy creation to wedge")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)
    finally:
        wedge.release()
        await asyncio.sleep(0.05)

    assert created[-1] == "rds_proxy"


async def test_the_cancellation_propagates_when_the_teardown_itself_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken teardown must not replace the CancelledError with its own failure."""

    # Nothing here is wedged, and the bound is generous: the point is which
    # exception survives a *responsive* but broken teardown, not the deadline.
    ssm = _Ssm(terminal_lanes=frozenset({"lakebase", "rds"}), wedge_cancel=False)
    orchestrator, _created = _orchestrator(monkeypatch, ssm=ssm, bound=PATIENCE)
    settled = asyncio.Event()
    holding = asyncio.Event()

    async def exploding_settle(bout_id: str) -> None:
        del bout_id
        settled.set()
        raise RuntimeError("the settlement path is broken")

    async def hold(*args: object, **kwargs: object) -> None:
        del args, kwargs
        holding.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(orchestrator, "_settle_commands", exploding_settle)
    monkeypatch.setattr(orchestrator, "_verify_proxy_topology", hold)
    task = asyncio.create_task(orchestrator.setup("bout-teardown-raises", 11))
    await holding.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)
    assert task.cancelled() is True
    # The broken teardown was reached and its failure did not become the result.
    await _until(settled.is_set, "the failing settlement to run")


@pytest.mark.parametrize("hold_send", [False, True], ids=["send_resolves", "send_wedged"])
async def test_the_orphan_risk_log_names_every_resource_that_may_have_survived(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    hold_send: bool,
) -> None:
    """The whole reason not to wait longer: someone has to be able to find these.

    Cancellation is delivered from inside the second ``SendCommand``, so at the
    instant the handler runs SSM holds two commands and this process holds
    neither identifier. That used to be a race the assertions below lost
    occasionally; it is now the guaranteed starting state, because it is the
    state the report has to survive. Both commands genuinely reached SSM in both
    arms, so both must be accounted for either way: by identifier once the send
    resolves, and by lane when it never does.
    """

    loop = asyncio.get_running_loop()
    holder: dict[str, asyncio.Task[object]] = {}
    requested = threading.Event()

    def cancel_from_send() -> None:
        def _cancel() -> None:
            holder["task"].cancel()
            requested.set()

        loop.call_soon_threadsafe(_cancel)
        requested.wait(PATIENCE)

    ssm = _Ssm(cancel_on_send=cancel_from_send, hold_send=hold_send)
    orchestrator, _created = _orchestrator(monkeypatch, ssm=ssm)
    names = orchestrator.names_for_bout("anti-demo-r5", "bout-named-orphans")
    caplog.set_level(logging.ERROR, logger="server.safe_change")
    task = asyncio.create_task(orchestrator.setup("bout-named-orphans", 11))
    holder["task"] = task
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=PATIENCE)
    finally:
        ssm.send.release()
        ssm.cancel.release()

    # This was `await asyncio.sleep(0.05)`, and the sleep was measured before it
    # was replaced rather than after: with the wait removed entirely the report
    # is already present on every one of ten runs, because the teardown that
    # writes it runs inside the bound and therefore finishes before the `await`
    # above returns. So the sleep was never load-bearing and this is hardening,
    # not a repair -- it is recorded that way so nobody later reads it as the
    # fix for a flake it did not cause.
    #
    # Kept as a wait rather than deleted because the ordering it depends on is
    # real and unstated: if the teardown ever lands after the cancellation
    # instead of within it, the `next()` below raises a bare `StopIteration`
    # with no message attached, which is the least debuggable way for a public
    # CI run to go red. Waiting on the report itself costs nothing when it is
    # already there and names the problem when it is not.
    await _until(
        lambda: any("ORPHAN RISK" in record.getMessage() for record in caplog.records),
        "the orphan risk report to be logged",
    )

    orphan = next(
        record.getMessage()
        for record in caplog.records
        if "ORPHAN RISK" in record.getMessage()
    )
    stem = names.proxy_name
    # Everything a human needs to find and delete these by hand.
    assert f"rds_proxy {stem}" in orphan
    assert f"proxy_security_group {names.proxy_security_group_name}" in orphan
    assert f"proxy_target {stem}-target" in orphan
    assert f"proxy_target_group {stem}-target-group" in orphan
    for rule in ("proxy-ingress", "proxy-egress", "runner-egress", "rds-ingress"):
        assert f"{stem}-{rule}" in orphan
    assert "proxy target rds_postgres rds-source" in orphan
    assert "observed security group sg-round5-proxy" in orphan
    assert "in-flight SSM commands on i-0123456789abcdef0" in orphan
    assert len(ssm.requests) == 2
    for command_id, request in ssm.requests.items():
        lane = f"{request['lane_id']}:{request['action']}"
        if f"{lane}={command_id}" in orphan:
            continue
        # The identifier never reached this process. The lane must still be
        # named as a command of unknown fate rather than silently omitted: an
        # unnamed command holds the runner's flock and wedges the next bout.
        assert "SSM commands of unknown fate on i-0123456789abcdef0" in orphan
        assert lane in orphan
    assert "bout-named-orphans" in orphan
    # The bound is what is being reported, not a generic failure.
    assert f"{BOUND:.1f}s" in orphan


async def test_the_default_bound_matches_the_sibling_rounds() -> None:
    from server.safe_change import DEFAULT_CANCEL_TEARDOWN_SECONDS

    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        _config(),
        journal=_EmptyJournal(),
        fence=_Fence(),
        fresh_lakebase_host=_pooled_host,
    )

    assert orchestrator.cancel_teardown_timeout_seconds == DEFAULT_CANCEL_TEARDOWN_SECONDS
    assert DEFAULT_CANCEL_TEARDOWN_SECONDS == 5.0
    with pytest.raises(ValueError, match="must be positive"):
        LiveConnectionSpikeSetupOrchestrator(
            _config(),
            journal=_EmptyJournal(),
            fence=_Fence(),
            fresh_lakebase_host=_pooled_host,
            cancel_teardown_timeout_seconds=0,
        )


async def test_a_successful_setup_never_reaches_the_cancellation_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The new code must be unreachable except on cancellation."""

    ssm = _Ssm(terminal_lanes=frozenset({"lakebase", "rds"}))
    orchestrator, created = _orchestrator(monkeypatch, ssm=ssm)
    abandoned: list[str] = []
    monkeypatch.setattr(
        orchestrator,
        "_abandon_setup",
        lambda bout_id, lanes: abandoned.append(bout_id),
    )

    result = await orchestrator.setup("bout-successful", 11)

    assert result.bout_id == "bout-successful"
    assert abandoned == []
    assert ssm.cancel.calls == []
    assert orchestrator._cleanup_tasks == {}
    assert created == [
        "proxy_security_group",
        "proxy_default_egress",
        "proxy_ingress",
        "proxy_egress",
        "runner_egress",
        "rds_ingress",
        "rds_proxy",
        "proxy_target_group",
        "proxy_target",
    ]


async def test_a_failed_setup_still_settles_and_reconciles_without_the_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The non-cancellation failure path keeps its own ordering exactly."""

    ssm = _Ssm(terminal_lanes=frozenset({"lakebase", "rds"}), wedge_cancel=False)
    orchestrator, _created = _orchestrator(monkeypatch, ssm=ssm)
    order: list[str] = []

    async def failing_host() -> str:
        raise RuntimeError("the sealed pooled host is unreachable")

    async def settle(bout_id: str) -> None:
        order.append(f"settle:{bout_id}")

    monkeypatch.setattr(orchestrator, "_fresh_lakebase_host", failing_host)
    monkeypatch.setattr(orchestrator, "_settle_commands", settle)
    monkeypatch.setattr(
        orchestrator,
        "_abandon_setup",
        lambda bout_id, lanes: order.append("abandon"),
    )
    real_coordinator = orchestrator._coordinator

    def coordinator(scope, clients, resources):
        value, specs = real_coordinator(scope, clients, resources)

        async def reconcile_incomplete(supplied_scope):
            order.append(f"reconcile:{supplied_scope.bout_id}")
            return SimpleNamespace(complete=True)

        value.reconcile_incomplete = reconcile_incomplete
        return value, specs

    monkeypatch.setattr(orchestrator, "_coordinator", coordinator)

    with pytest.raises(RuntimeError, match="unreachable"):
        await orchestrator.setup("bout-failed", 11)

    assert order == ["settle:bout-failed", "reconcile:bout-failed"]
    assert orchestrator._cleanup_tasks == {}


async def test_abort_tears_resources_down_in_exact_reverse_order() -> None:
    """The invariant the bound must not disturb: ordinals unwind 9 to 1."""

    journal = _RecordingJournal()
    deletions: list[str] = []
    kinds = (
        "proxy_security_group",
        "proxy_default_egress",
        "proxy_ingress",
        "proxy_egress",
        "runner_egress",
        "rds_ingress",
        "rds_proxy",
        "proxy_target_group",
        "proxy_target",
    )
    specs = tuple(
        ResourceSpec(index, kind, f"anti-demo-r5-{kind}")
        for index, kind in enumerate(kinds, 1)
    )
    live: dict[str, ResourceObservation] = {}

    def adapter(kind: str):
        class Adapter:
            async def create(self, spec):
                observed = ResourceObservation(
                    resource_kind=spec.resource_kind,
                    provider_id=f"provider-{spec.ordinal}",
                    deterministic_name=spec.deterministic_name,
                )
                live[spec.deterministic_name] = observed
                return observed

            async def inspect(self, spec, *, provider_id):
                del provider_id
                return live.get(spec.deterministic_name)

            async def delete(self, resource):
                deletions.append(resource.resource_kind)
                live.pop(resource.deterministic_name, None)

        del kind
        return Adapter()

    coordinator = Round5CreationCoordinator(
        journal=journal,
        fence=_Fence(),
        adapters={kind: adapter(kind) for kind in kinds},
    )
    scope = CreationScope("bout-reverse-order", 11, "d" * 64)
    receipt = await coordinator.create_resources(scope, specs)

    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._cleanup_start_lock = asyncio.Lock()
    orchestrator._cleanup_tasks = {}
    orchestrator._proxy_delete_accepted = {}
    orchestrator._coordinators = {"bout-reverse-order": coordinator}
    orchestrator._scopes = {"bout-reverse-order": scope}
    orchestrator._receipts = {"bout-reverse-order": receipt}
    orchestrator._results = {}
    orchestrator._settle_commands = lambda _bout_id: _value(None)

    await orchestrator.begin_cleanup("bout-reverse-order")
    await orchestrator.wait_for_cleanup_complete("bout-reverse-order")

    assert deletions == list(reversed(kinds))
    assert live == {}
    assert orchestrator.proxy_delete_accepted("bout-reverse-order") is True
    # Every delete is durably intended before it happens and confirmed after.
    intents = [
        event.ordinal
        for event in journal.committed
        if event.lifecycle_state is LifecycleState.DELETE_INTENT
    ]
    assert intents == list(range(9, 0, -1))


async def test_settlement_uses_the_exact_ssm_cancellation_call_shape() -> None:
    """Stubber, not a fake: botocore validates the parameters a fake would skip."""

    session = boto3.Session(
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    ssm = session.client("ssm")
    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        _config(),
        journal=_EmptyJournal(),
        fence=_Fence(),
        fresh_lakebase_host=_pooled_host,
        sleep=lambda _delay: asyncio.sleep(0),
    )
    nonce = "a" * 64
    # SSM command IDs are UUIDs and botocore enforces the length; a hand-rolled
    # fake would have accepted "command-1" and proved nothing.
    command_id = "11111111-2222-4333-8444-555555555555"
    stubber = Stubber(ssm)
    stubber.add_response(
        "cancel_command",
        {},
        {"CommandId": command_id, "InstanceIds": ["i-0123456789abcdef0"]},
    )
    stubber.add_response(
        "get_command_invocation",
        {
            "Status": "Success",
            "StandardOutputContent": (
                f"SETUP_SETTLED:{nonce}\nRUNNER_FLOCK_RELEASED:bout-stubbed\n"
            ),
        },
        {"CommandId": command_id, "InstanceId": "i-0123456789abcdef0"},
    )
    active = SimpleNamespace(
        bout_id="bout-stubbed",
        lane_id="lakebase",
        action="verify",
        command_id=command_id,
        ssm=ssm,
    )

    with stubber:
        await orchestrator._cancel_setup_command(active, nonce)

    stubber.assert_no_pending_responses()


async def test_proxy_deletion_uses_the_exact_rds_call_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one resource that costs real money, checked against the real model."""

    session = boto3.Session(
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    rds = session.client("rds")
    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        _config(),
        journal=_EmptyJournal(),
        fence=_Fence(),
        fresh_lakebase_host=_pooled_host,
    )
    monkeypatch.setattr(
        orchestrator,
        "_inspect_proxy",
        lambda *args, **kwargs: _value(None),
    )
    observed = ResourceObservation(
        resource_kind="rds_proxy",
        provider_id=f"arn:aws:rds:{REGION}:{ACCOUNT}:db-proxy:prx-1",
        deterministic_name="anti-demo-r5-abcdef0123456789-proxy",
    )
    stubber = Stubber(rds)
    stubber.add_response(
        "delete_db_proxy",
        {},
        {"DBProxyName": "anti-demo-r5-abcdef0123456789-proxy"},
    )

    with stubber:
        await orchestrator._delete_proxy(
            SimpleNamespace(rds=rds),
            observed,
            bout_id="bout-stubbed-proxy",
        )

    stubber.assert_no_pending_responses()
    assert orchestrator.proxy_delete_accepted("bout-stubbed-proxy") is True
