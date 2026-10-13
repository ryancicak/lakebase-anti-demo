"""B12 / G09: a Lakebase delete outlives its caller, so settling must wait for it.

`DatabricksRestRunner` runs each request in a worker thread that a timeout or a
cancel cannot stop. The Lakebase adapters had no settle hook, so a cancelled
startup cleanup could never prove its DELETE was done and left ``main`` to expire
while that DELETE could still land under a successor.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import json
import logging
import threading
import time
from types import SimpleNamespace

import pytest
import requests
from databricks.sdk._base_client import _BaseClient
from databricks.sdk.clock import Clock
from databricks.sdk.errors import (
    BadRequest,
    InternalError,
    NotFound,
    PermissionDenied,
    Unauthenticated,
)

from server.readiness import ShowtimeReadinessGate
from server.recovery_live import LakebaseRecoveryAdapter
from server.safe_change import SafeChangeProvider
from server.safe_change_live import (
    ControlPlaneCommandError,
    DatabricksRestRunner,
    LakebaseSafeChangeAdapter,
    SafeChangeControlPlaneError,
)
from tests.test_lakebase_transient_retry import (
    RECOVERY_AT,
    ScriptedWorkspace,
    adapter_on,
    rc14_answer,
    recovery_plan,
    scripted_runner,
)
from tests.test_readiness import DurableFakeLeaseStore, FakeReadinessStore, manifest
from tests.test_safe_change_live import LAKEBASE_SOURCE, FakeDatabricksRunner, quiet_report
from tests.test_safe_change_live import plan as safe_change_plan
from tests.test_startup_cleanup_cancel import Done

BRANCH = "/api/2.0/postgres/projects/ad-test-001/branches/recovery-ad-test-001"


def blocking_runner(
    *,
    returned: threading.Event | None = None,
    failure: Exception | None = None,
) -> tuple[DatabricksRestRunner, threading.Event, threading.Event]:
    """A real runner whose wire call blocks its worker thread until released."""

    entered = threading.Event()
    release = threading.Event()

    def api(_profile, _method, _path, body=None):
        del body
        entered.set()
        release.wait(5)
        if returned is not None:
            returned.set()
        if failure is not None:
            raise failure
        return {}

    runner = DatabricksRestRunner(workspace_client=object())
    runner._api = api
    return runner, entered, release


async def test_a_timed_out_delete_stays_pending_until_its_thread_returns() -> None:
    runner, _entered, release = blocking_runner()
    with pytest.raises(ControlPlaneCommandError, match="timed out"):
        await runner.run("DELETE", BRANCH, timeout_seconds=0.05)
    settle = asyncio.create_task(runner.settle_pending_requests(5))
    await asyncio.sleep(0.05)
    assert not settle.done()
    release.set()
    await asyncio.wait_for(settle, 2)


async def test_a_delete_still_running_past_the_bound_is_unsettled() -> None:
    runner, _entered, release = blocking_runner()
    with pytest.raises(ControlPlaneCommandError):
        await runner.run("DELETE", BRANCH, timeout_seconds=0.05)
    try:
        with pytest.raises(ControlPlaneCommandError, match="still in flight"):
            await runner.settle_pending_requests(0.05)
        with pytest.raises(ControlPlaneCommandError, match="still in flight"):
            await runner.settle_pending_requests(0.05)
    finally:
        release.set()
    await runner.settle_pending_requests(2)


async def test_a_slow_read_holds_nothing() -> None:
    runner, entered, release = blocking_runner()
    read = asyncio.create_task(runner.json("GET", BRANCH, timeout_seconds=5))
    assert await asyncio.to_thread(entered.wait, 2)
    try:
        await asyncio.wait_for(runner.settle_pending_requests(5), 0.5)
    finally:
        release.set()
    await read


async def test_a_slow_credential_request_holds_nothing() -> None:
    runner, entered, release = blocking_runner()
    credential = asyncio.create_task(
        runner.json(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": "e"},
            timeout_seconds=5,
        )
    )
    assert await asyncio.to_thread(entered.wait, 2)
    try:
        await asyncio.wait_for(runner.settle_pending_requests(5), 0.5)
    finally:
        release.set()
    await credential


async def test_an_abandoned_delete_that_fails_later_is_logged(caplog) -> None:
    runner, _entered, release = blocking_runner(failure=InternalError("backend gone"))
    with pytest.raises(ControlPlaneCommandError, match="timed out"):
        await runner.run("DELETE", BRANCH, timeout_seconds=0.05)
    with caplog.at_level(logging.WARNING, logger="server.safe_change_live"):
        release.set()
        await runner.settle_pending_requests(2)
        await asyncio.sleep(0)
    late = [record.getMessage() for record in caplog.records if "gave up" in record.getMessage()]
    assert len(late) == 1
    assert f"DELETE {BRANCH}" in late[0] and "backend gone" in late[0]


async def test_a_cancel_landing_as_a_mutation_fails_still_logs_it_once(caplog) -> None:
    runner, entered, release = blocking_runner(failure=InternalError("backend gone"))
    original = runner._request_done
    caller: asyncio.Task | None = None

    def done_then_cancel(*args) -> None:
        original(*args)
        assert caller is not None
        caller.cancel()

    runner._request_done = done_then_cancel
    caller = asyncio.create_task(runner.run("DELETE", BRANCH, timeout_seconds=5))
    assert await asyncio.to_thread(entered.wait, 2)
    with caplog.at_level(logging.WARNING, logger="server.safe_change_live"):
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await caller
    late = [record.getMessage() for record in caplog.records if "gave up" in record.getMessage()]
    assert len(late) == 1 and f"DELETE {BRANCH}" in late[0]


async def test_an_abandoned_recovery_create_that_fails_later_leaves_no_unhandled_error() -> None:
    runner, _entered, _release = blocking_runner()
    recovery = LakebaseRecoveryAdapter(adapter_on(runner))
    unhandled: list[dict] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    failing = asyncio.Event()

    async def create() -> None:
        await failing.wait()
        raise ControlPlaneCommandError("create refused")

    try:
        caller = asyncio.create_task(recovery._run_mutation(create()))
        await asyncio.sleep(0)
        caller.cancel()
        await asyncio.gather(caller, return_exceptions=True)
        failing.set()
        with pytest.raises(ControlPlaneCommandError):
            await recovery.settle_pending_mutations()
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)
    assert unhandled == []


async def test_an_abandoned_read_that_fails_later_leaves_no_unretrieved_error() -> None:
    returned = threading.Event()
    runner, _entered, release = blocking_runner(
        failure=InternalError("backend gone"), returned=returned
    )
    unretrieved: list[dict] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: unretrieved.append(context))
    try:
        with pytest.raises(ControlPlaneCommandError, match="timed out"):
            await runner.json("GET", BRANCH, timeout_seconds=0.05)
        release.set()
        assert await asyncio.to_thread(returned.wait, 2)
        await asyncio.sleep(0.05)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)
    assert unretrieved == []


async def test_both_lakebase_adapters_settle_on_their_runner() -> None:
    runner, entered, release = blocking_runner()
    adapter = adapter_on(runner)
    recovery = LakebaseRecoveryAdapter(adapter)
    cancelled = asyncio.create_task(runner.run("DELETE", BRANCH, timeout_seconds=5))
    assert await asyncio.to_thread(entered.wait, 2)
    cancelled.cancel()
    await asyncio.gather(cancelled, return_exceptions=True)
    settles = [
        asyncio.create_task(adapter.settle_pending_mutations()),
        asyncio.create_task(recovery.settle_pending_mutations()),
    ]
    await asyncio.sleep(0.05)
    assert not any(settle.done() for settle in settles)
    release.set()
    await asyncio.wait_for(asyncio.gather(*settles), 2)


async def test_the_recovery_settle_shares_one_bound_across_both_waits() -> None:
    runner, _entered, release = blocking_runner()
    adapter = adapter_on(runner)
    adapter.config = dataclasses.replace(adapter.config, control_timeout_seconds=0.6)
    recovery = LakebaseRecoveryAdapter(adapter)
    with pytest.raises(ControlPlaneCommandError, match="timed out"):
        await runner.run("DELETE", BRANCH, timeout_seconds=0.05)
    runner_settles: list[float] = []
    settle_runner = runner.settle_pending_requests

    async def recorded_settle(timeout_seconds: float) -> None:
        runner_settles.append(time.monotonic())
        await settle_runner(timeout_seconds)

    runner.settle_pending_requests = recorded_settle
    recovery._pending_mutations.add(asyncio.create_task(asyncio.sleep(0.2)))
    started = time.monotonic()
    try:
        with pytest.raises((SafeChangeControlPlaneError, ControlPlaneCommandError)):
            await recovery.settle_pending_mutations()
        # The runner was reached, and inside the same 0.6 s: two bounds would take 0.8 s.
        assert len(runner_settles) == 1
        assert time.monotonic() - started < 0.75
    finally:
        release.set()
    await runner.settle_pending_requests(2)


async def test_a_failed_tracked_create_still_waits_for_the_runner() -> None:
    runner, _entered, release = blocking_runner()
    recovery = LakebaseRecoveryAdapter(adapter_on(runner))
    with pytest.raises(ControlPlaneCommandError, match="timed out"):
        await runner.run("DELETE", BRANCH, timeout_seconds=0.05)

    async def failed_create() -> None:
        raise RuntimeError("create refused")

    failed = asyncio.create_task(failed_create())
    await asyncio.gather(failed, return_exceptions=True)
    recovery._pending_mutations.add(failed)
    settle = asyncio.create_task(recovery.settle_pending_mutations())
    await asyncio.sleep(0.05)
    assert not settle.done()
    release.set()
    with pytest.raises(RuntimeError, match="create refused"):
        await asyncio.wait_for(settle, 2)


class LakebaseDeleteCleanup:
    def __init__(self, adapter: LakebaseSafeChangeAdapter) -> None:
        self.lakebase = adapter
        self.competitors: dict[str, object] = {}

    async def reset_all(self) -> None:
        await self.lakebase._runner.run("DELETE", BRANCH, timeout_seconds=5)


class LakebaseRecoveryDeleteCleanup:
    def __init__(self, runner: DatabricksRestRunner) -> None:
        self.runner = runner
        self.lakebase = LakebaseRecoveryAdapter(adapter_on(runner))
        self.competitors: dict[str, object] = {}

    async def reset_all(self) -> None:
        await self.runner.run("DELETE", BRANCH, timeout_seconds=5)


class IdleLakebaseCleanup:
    def __init__(self, runner: DatabricksRestRunner) -> None:
        self.lakebase = adapter_on(runner)
        self.competitors: dict[str, object] = {}

    async def reset_all(self) -> None:
        return None


class ReleaseRecordingLeaseStore(DurableFakeLeaseStore):
    def __init__(self, delete_returned: threading.Event) -> None:
        super().__init__()
        self.delete_returned = delete_returned
        self.release_saw_delete_running: list[bool] = []

    async def release(self, lease):
        self.release_saw_delete_running.append(not self.delete_returned.is_set())
        return await super().release(lease)


async def test_cancelled_startup_cleanup_holds_main_until_the_lakebase_delete_returns() -> None:
    returned = threading.Event()
    runner, entered, release = blocking_runner(returned=returned)
    leases = ReleaseRecordingLeaseStore(returned)
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: LakebaseDeleteCleanup(adapter_on(runner)),
        recovery_factory=lambda _manifest: Done(),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
        heartbeat_seconds=0.01,
        lease_seconds=1,
    )
    run = asyncio.create_task(gate._lead_cleanup())
    assert await asyncio.to_thread(entered.wait, 2)
    run.cancel()
    await asyncio.sleep(0.1)
    assert not run.done()
    assert leases.release_saw_delete_running == []
    release.set()
    await asyncio.gather(run, return_exceptions=True)
    assert leases.release_saw_delete_running == [False]
    assert await leases.current() is None


async def test_cancelled_startup_cleanup_waits_for_the_recovery_runner_too() -> None:
    returned = threading.Event()
    safe_runner, _safe_entered, safe_release = blocking_runner()
    recovery_runner, entered, release = blocking_runner(returned=returned)
    leases = ReleaseRecordingLeaseStore(returned)
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: IdleLakebaseCleanup(safe_runner),
        recovery_factory=lambda _manifest: LakebaseRecoveryDeleteCleanup(recovery_runner),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
        heartbeat_seconds=0.01,
        lease_seconds=1,
    )
    run = asyncio.create_task(gate._lead_cleanup())
    assert await asyncio.to_thread(entered.wait, 2)
    run.cancel()
    await asyncio.sleep(0.1)
    assert not run.done()
    assert leases.release_saw_delete_running == []
    release.set()
    safe_release.set()
    await asyncio.gather(run, return_exceptions=True)
    assert leases.release_saw_delete_running == [False]


class AcceptedButSlowDelete(FakeDatabricksRunner):
    """Accepts a branch DELETE at once but keeps the branch until the test removes it."""

    def __init__(self, *, hold_request: bool = False) -> None:
        super().__init__()
        self.accepted = asyncio.Event()
        self.hold_request = hold_request

    async def run(self, method, path, *, body=None, timeout_seconds: float) -> None:
        del body, timeout_seconds
        self.commands.append([method, path])
        self.accepted.set()
        if self.hold_request:
            await asyncio.Event().wait()

    def finish_delete(self, branch_name: str) -> None:
        self.branches.pop(branch_name)
        for endpoint in list(self.endpoints):
            if endpoint.startswith(f"{branch_name}/endpoints/"):
                self.endpoints.pop(endpoint)


async def _cancel_once_accepted(runner: AcceptedButSlowDelete, delete) -> None:
    deleting = asyncio.create_task(delete)
    await asyncio.wait_for(runner.accepted.wait(), 2)
    if not runner.hold_request:
        await asyncio.sleep(0.05)
    assert not deleting.done()
    deleting.cancel()
    await asyncio.gather(deleting, return_exceptions=True)


async def test_a_branch_delete_cancelled_before_absence_settles_only_once_it_is_gone() -> None:
    runner = AcceptedButSlowDelete()
    adapter = adapter_on(runner)
    adapter._sleep = asyncio.sleep
    lakebase_plan = safe_change_plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    await adapter.preflight(lakebase_plan)
    artifact = await adapter.create_isolated(lakebase_plan, quiet_report)
    await _cancel_once_accepted(
        runner, adapter.delete_isolated(lakebase_plan, artifact, quiet_report)
    )
    settle = asyncio.create_task(adapter.settle_pending_mutations())
    await asyncio.sleep(0.1)
    assert not settle.done()
    runner.finish_delete(str(artifact.metadata["branch_name"]))
    await asyncio.wait_for(settle, 2)


async def test_a_recovery_delete_cancelled_before_absence_settles_only_once_it_is_gone() -> None:
    runner = AcceptedButSlowDelete()
    adapter = adapter_on(runner)
    adapter._sleep = asyncio.sleep
    recovery = LakebaseRecoveryAdapter(adapter)
    artifact = await recovery.create_recovery(recovery_plan(), RECOVERY_AT, quiet_report)
    await _cancel_once_accepted(
        runner, recovery.delete_recovery(recovery_plan(), artifact, quiet_report)
    )
    settle = asyncio.create_task(recovery.settle_pending_mutations())
    await asyncio.sleep(0.1)
    assert not settle.done()
    runner.finish_delete(str(artifact.metadata["branch_name"]))
    await asyncio.wait_for(settle, 2)


async def test_a_branch_delete_cancelled_mid_request_settles_only_once_it_is_gone() -> None:
    runner = AcceptedButSlowDelete(hold_request=True)
    adapter = adapter_on(runner)
    adapter._sleep = asyncio.sleep
    lakebase_plan = safe_change_plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    await adapter.preflight(lakebase_plan)
    artifact = await adapter.create_isolated(lakebase_plan, quiet_report)
    await _cancel_once_accepted(
        runner, adapter.delete_isolated(lakebase_plan, artifact, quiet_report)
    )
    settle = asyncio.create_task(adapter.settle_pending_mutations())
    await asyncio.sleep(0.1)
    assert not settle.done()
    runner.finish_delete(str(artifact.metadata["branch_name"]))
    await asyncio.wait_for(settle, 2)


async def test_a_recovery_delete_cancelled_mid_request_settles_only_once_it_is_gone() -> None:
    runner = AcceptedButSlowDelete(hold_request=True)
    adapter = adapter_on(runner)
    adapter._sleep = asyncio.sleep
    recovery = LakebaseRecoveryAdapter(adapter)
    artifact = await recovery.create_recovery(recovery_plan(), RECOVERY_AT, quiet_report)
    await _cancel_once_accepted(
        runner, recovery.delete_recovery(recovery_plan(), artifact, quiet_report)
    )
    settle = asyncio.create_task(recovery.settle_pending_mutations())
    await asyncio.sleep(0.1)
    assert not settle.done()
    runner.finish_delete(str(artifact.metadata["branch_name"]))
    await asyncio.wait_for(settle, 2)


async def test_a_branch_that_never_leaves_fails_settle_at_the_control_bound() -> None:
    runner = AcceptedButSlowDelete()
    adapter = adapter_on(runner)
    adapter._sleep = asyncio.sleep
    lakebase_plan = safe_change_plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    await adapter.preflight(lakebase_plan)
    artifact = await adapter.create_isolated(lakebase_plan, quiet_report)
    await _cancel_once_accepted(
        runner, adapter.delete_isolated(lakebase_plan, artifact, quiet_report)
    )
    adapter.config = dataclasses.replace(adapter.config, control_timeout_seconds=0.2)
    started = time.monotonic()
    with pytest.raises(SafeChangeControlPlaneError, match="still unconfirmed"):
        await adapter.settle_pending_mutations()
    assert time.monotonic() - started < 1.0


async def test_a_recovery_create_failing_as_its_caller_is_cancelled_still_fails_settle() -> None:
    runner, _entered, _release = blocking_runner()
    recovery = LakebaseRecoveryAdapter(adapter_on(runner))
    caller: asyncio.Task | None = None

    async def create_failing_as_cancelled() -> None:
        assert caller is not None
        caller.cancel()
        raise ControlPlaneCommandError("create refused")

    caller = asyncio.create_task(recovery._run_mutation(create_failing_as_cancelled()))
    with pytest.raises(asyncio.CancelledError):
        await caller
    with pytest.raises(ControlPlaneCommandError, match="create refused"):
        await recovery.settle_pending_mutations()


class RefusedDelete(AcceptedButSlowDelete):
    """Answers a branch DELETE with ``cause``, leaving the branch where it is."""

    def __init__(self, cause: BaseException, *, transient: bool = False) -> None:
        super().__init__()
        self.cause = cause
        self.transient = transient

    async def run(self, method, path, *, body=None, timeout_seconds: float) -> None:
        if method != "DELETE":
            return await super().run(method, path, body=body, timeout_seconds=timeout_seconds)
        self.commands.append([method, path])
        raise ControlPlaneCommandError(
            "Databricks control-plane request failed", transient=self.transient
        ) from self.cause


async def _refused_branch_delete(cause: BaseException) -> LakebaseSafeChangeAdapter:
    runner = RefusedDelete(cause)
    adapter = adapter_on(runner)
    adapter._sleep = asyncio.sleep
    lakebase_plan = safe_change_plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    await adapter.preflight(lakebase_plan)
    artifact = await adapter.create_isolated(lakebase_plan, quiet_report)
    with pytest.raises(ControlPlaneCommandError):
        await adapter.delete_isolated(lakebase_plan, artifact, quiet_report)
    return adapter


REFUSED_ANSWERS = [
    BadRequest("malformed"),
    Unauthenticated("no credential"),
    PermissionDenied("not allowed"),
]


@pytest.mark.parametrize("answer", REFUSED_ANSWERS, ids=lambda answer: type(answer).__name__)
async def test_a_refused_delete_stays_recorded_when_hidden_retries_are_possible(answer) -> None:
    adapter = await _refused_branch_delete(answer)
    assert len(adapter._deletes_awaiting_absence) == 1


async def test_a_timed_out_delete_stays_recorded_for_settle() -> None:
    adapter = await _refused_branch_delete(TimeoutError())
    assert len(adapter._deletes_awaiting_absence) == 1


@pytest.mark.parametrize("answer", REFUSED_ANSWERS, ids=lambda answer: type(answer).__name__)
async def test_a_recovery_delete_refusal_stays_recorded_when_retries_are_possible(answer) -> None:
    runner = RefusedDelete(answer)
    adapter = adapter_on(runner)
    adapter._sleep = asyncio.sleep
    recovery = LakebaseRecoveryAdapter(adapter)
    artifact = await recovery.create_recovery(recovery_plan(), RECOVERY_AT, quiet_report)
    with pytest.raises(ControlPlaneCommandError):
        await recovery.delete_recovery(recovery_plan(), artifact, quiet_report)
    assert len(adapter._deletes_awaiting_absence) == 1


async def test_not_found_proves_delete_settled_on_both_paths() -> None:
    adapter = await _refused_branch_delete(NotFound("already gone"))
    assert adapter._deletes_awaiting_absence == {}
    await asyncio.wait_for(adapter.settle_pending_mutations(), 1)

    runner = RefusedDelete(NotFound("already gone"))
    adapter = adapter_on(runner)
    recovery = LakebaseRecoveryAdapter(adapter)
    artifact = await recovery.create_recovery(recovery_plan(), RECOVERY_AT, quiet_report)
    with pytest.raises(ControlPlaneCommandError):
        await recovery.delete_recovery(recovery_plan(), artifact, quiet_report)
    assert adapter._deletes_awaiting_absence == {}
    await asyncio.wait_for(recovery.settle_pending_mutations(), 1)


async def test_a_stuck_mutation_with_no_delete_pending_is_not_reported_as_a_delete() -> None:
    runner, _entered, release = blocking_runner()
    adapter = adapter_on(runner)
    adapter.config = dataclasses.replace(adapter.config, control_timeout_seconds=0.1)
    with pytest.raises(ControlPlaneCommandError, match="timed out"):
        await runner.run("POST", BRANCH, timeout_seconds=0.05)
    try:
        with pytest.raises(SafeChangeControlPlaneError, match="still in flight"):
            await adapter.settle_pending_mutations()
    finally:
        release.set()
    await runner.settle_pending_requests(2)


async def test_a_ambiguous_delete_failure_stays_recorded_for_settle() -> None:
    adapter = await _refused_branch_delete(InternalError("backend gone"))
    assert len(adapter._deletes_awaiting_absence) == 1


async def _absent() -> bool:
    return False


async def test_a_refusal_after_a_delete_asked_again_stays_recorded_for_settle() -> None:
    workspace = ScriptedWorkspace(rc14_answer(), PermissionDenied("not allowed"))
    runner, _ = scripted_runner(workspace)
    adapter = adapter_on(runner)
    with pytest.raises(ControlPlaneCommandError):
        await adapter._send_branch_delete("recovery-ad-test-001", _absent, "still exists")
    assert len(workspace.requests) == 2
    assert list(adapter._deletes_awaiting_absence) == ["recovery-ad-test-001"]


async def test_a_refusal_on_the_only_visible_attempt_stays_recorded() -> None:
    workspace = ScriptedWorkspace(PermissionDenied("not allowed"))
    runner, _ = scripted_runner(workspace)
    adapter = adapter_on(runner)
    with pytest.raises(ControlPlaneCommandError):
        await adapter._send_branch_delete("recovery-ad-test-001", _absent, "still exists")
    assert list(adapter._deletes_awaiting_absence) == ["recovery-ad-test-001"]


async def test_a_bad_request_saying_try_again_later_stays_recorded_for_settle() -> None:
    adapter = adapter_on(RefusedDelete(BadRequest("Please try again later"), transient=True))
    with pytest.raises(ControlPlaneCommandError):
        await adapter._send_branch_delete("recovery-ad-test-001", _absent, "still exists")
    assert list(adapter._deletes_awaiting_absence) == ["recovery-ad-test-001"]


class InstantSdkClock(Clock):
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class ScriptedSdkSession:
    def __init__(self, *answers) -> None:
        self.answers = list(answers)
        self.sent: list[str] = []

    def request(self, method, _url, **_kwargs):
        self.sent.append(method)
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def sdk_answer(status: int, error_code: str, message: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps({"error_code": error_code, "message": message}).encode()
    response.headers["Content-Type"] = "application/json"
    response.url = "https://workspace.invalid/api/2.0/postgres/recovery-ad-test-001"
    response.request = requests.Request("DELETE", response.url).prepare()
    return response


async def test_an_sdk_resent_delete_refused_as_invalid_state_stays_recorded() -> None:
    base = _BaseClient(header_factory=lambda: {}, clock=InstantSdkClock())
    session = ScriptedSdkSession(
        requests.ReadTimeout("read timed out"),
        sdk_answer(400, "INVALID_STATE", "Branch is being deleted"),
    )
    base._session = session

    class ApiClient:
        def do(self, method, path, body=None):
            return base.do(method, f"https://workspace.invalid{path}", body=body)

    runner = DatabricksRestRunner(
        workspace_client=SimpleNamespace(api_client=ApiClient()),
    )
    adapter = adapter_on(runner)
    with pytest.raises(ControlPlaneCommandError):
        await adapter._send_branch_delete("recovery-ad-test-001", _absent, "still exists")

    assert session.sent == ["DELETE", "DELETE"]
    assert list(adapter._deletes_awaiting_absence) == ["recovery-ad-test-001"]
