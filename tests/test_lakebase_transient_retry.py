"""Databricks' "try again later" is asked again, and a create it refused is never made twice.

2026-10-03, rc14. Round 3's Lakebase lane asked for its recovery branch six seconds after the
bell, and the control plane answered INTERNAL_ERROR "The request could not be processed. Please
try again later". The lane failed on that first answer, so the bout ended "could not verify" and
the release run needed another full run. A lookup, a credential or a DELETE is safe to ask again
as it stands. A create is not: the refused POST may have made the branch, and a second POST would
then be refused as "already exists". So before a create is asked again, the lane checks whether
the resource is there as it asked for it.

2026-10-04, rc17. The same POST was refused five times in a row, past the 15 s the retries then
stopped at, and the lane failed with 13 minutes of its bound left. It is now asked again until
the lane's bound from the bell, the round's one maximum, ends it.

Both of those refusals turned out to be one Lakebase refuses every time, a recovery point on a
whole minute (tests/test_lakebase_whole_minute_recovery.py). The retries stand for the answers
that really are transient.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from databricks.sdk.errors import (
    Aborted,
    BadRequest,
    DatabricksError,
    DeadlineExceeded,
    InternalError,
    NotFound,
    PermissionDenied,
    TemporarilyUnavailable,
    TooManyRequests,
)

from server.bout_limit import BOUT_TIME_LIMIT_SECONDS
from server.models import CompetitorId
from server.recovery import RecoveryPlan, deterministic_recovery_artifact_id
from server.recovery_live import LakebaseRecoveryAdapter
from server.safe_change import SafeChangeProvider
from server.safe_change_live import (
    LAKEBASE_API_ROOT,
    LAKEBASE_TRANSIENT_RETRY_SECONDS,
    ControlPlaneCommandError,
    DatabricksRestRunner,
    LakebaseSafeChangeAdapter,
    LakebaseSafeChangeConfig,
    SafeChangeControlPlaneError,
    _control_plane_failure,
    _lakebase_create_path,
    create_lakebase_resource,
    lakebase_resource_path,
    lakebase_transient_pauses,
)
from tests.test_recovery import build_engine as build_recovery_engine
from tests.test_safe_change_live import (
    LAKEBASE_SOURCE,
    OWNER,
    REGION,
    RUN_ID,
    FakeDatabricksRunner,
    RecordingConnector,
    plan,
    quiet_report,
    scope,
)

PROJECT = "projects/ad-test-001"
RECOVERY_AT = datetime(2026, 10, 3, 17, 45, 55, tzinfo=UTC)


def rc14_answer() -> InternalError:
    return InternalError(
        "The request could not be processed. Please try again later. [TraceId: 00-rc14]",
        error_code="INTERNAL_ERROR",
    )


# --- which answers mean "ask again" -------------------------------------------------------------


@pytest.mark.parametrize(
    "answer",
    [
        rc14_answer(),
        TemporarilyUnavailable("service is overloaded"),
        TooManyRequests("rate limited"),
        DeadlineExceeded("deadline exceeded"),
        Aborted("aborted"),
        DatabricksError("quota", error_code="RESOURCE_EXHAUSTED"),
        DatabricksError("Something failed. Please try again later.", error_code="UNKNOWN"),
    ],
    ids=["internal", "unavailable", "rate-limited", "deadline", "aborted", "exhausted", "words"],
)
def test_an_answer_that_the_request_was_not_served_is_transient(answer) -> None:
    assert _control_plane_failure(answer).transient is True


@pytest.mark.parametrize(
    "answer",
    [
        BadRequest('branch already exists; branch_name:"recovery-ad-test-003"'),
        PermissionDenied("assign the user 'Can Use' for Database project abc"),
        NotFound("branch id not found"),
    ],
    ids=["already-exists", "permission", "absent"],
)
def test_a_refusal_is_not_transient(answer) -> None:
    assert _control_plane_failure(answer).transient is False


# --- the runner asks lookups, credentials and deletes again --------------------------------------


class ScriptedWorkspace:
    """A workspace that gives each request the next scripted answer."""

    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.requests: list[tuple[str, str]] = []

    def do(self, method: str, path: str, *, body=None):
        del body
        self.requests.append((method, path))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def scripted_runner(workspace: ScriptedWorkspace) -> tuple[DatabricksRestRunner, list[float]]:
    pauses: list[float] = []

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    runner = DatabricksRestRunner(
        workspace_client=SimpleNamespace(api_client=workspace), sleep=sleep
    )
    return runner, pauses


BRANCH = f"{PROJECT}/branches/recovery-ad-test-001"


async def test_a_lookup_answered_try_again_later_is_asked_again(caplog) -> None:
    workspace = ScriptedWorkspace(rc14_answer(), rc14_answer(), {"name": BRANCH})
    runner, pauses = scripted_runner(workspace)

    with caplog.at_level("WARNING", logger="server.safe_change_live"):
        answer = await runner.json("GET", lakebase_resource_path(BRANCH), timeout_seconds=5.0)

    assert answer == {"name": BRANCH}
    assert len(workspace.requests) == 3
    assert pauses == list(LAKEBASE_TRANSIENT_RETRY_SECONDS[:2])
    assert sum(
        "again later; asking again in" in record.getMessage() for record in caplog.records
    ) == 2


def test_the_pauses_grow_to_15_seconds_and_stop_at_the_rounds_maximum() -> None:
    pauses = list(lakebase_transient_pauses())

    assert pauses[:5] == [1.0, 2.0, 4.0, 8.0, 15.0]
    assert set(pauses[4:]) == {15.0}
    assert sum(pauses) <= BOUT_TIME_LIMIT_SECONDS < sum(pauses) + 15.0


async def test_a_lookup_that_stays_unserved_fails_after_the_last_pause() -> None:
    schedule = list(lakebase_transient_pauses())
    workspace = ScriptedWorkspace(*(rc14_answer() for _ in range(len(schedule) + 1)))
    runner, pauses = scripted_runner(workspace)

    with pytest.raises(ControlPlaneCommandError) as raised:
        await runner.json("GET", lakebase_resource_path(BRANCH), timeout_seconds=5.0)

    assert raised.value.transient is True
    assert len(workspace.requests) == len(schedule) + 1
    assert pauses == schedule


async def test_a_refusal_is_not_asked_again() -> None:
    workspace = ScriptedWorkspace(PermissionDenied("no"), {"name": BRANCH})
    runner, pauses = scripted_runner(workspace)

    with pytest.raises(ControlPlaneCommandError, match="refused"):
        await runner.json("GET", lakebase_resource_path(BRANCH), timeout_seconds=5.0)

    assert len(workspace.requests) == 1
    assert pauses == []


async def test_a_credential_post_is_asked_again() -> None:
    workspace = ScriptedWorkspace(rc14_answer(), {"token": "fresh"})
    runner, _ = scripted_runner(workspace)

    answer = await runner.json(
        "POST", f"{LAKEBASE_API_ROOT}/credentials", body={"endpoint": "e"}, timeout_seconds=5.0
    )

    assert answer == {"token": "fresh"}
    assert len(workspace.requests) == 2


async def test_the_runner_leaves_a_create_to_the_lane_that_can_check_for_it() -> None:
    path = _lakebase_create_path(PROJECT, "branches", "branch_id", "recovery-ad-test-001")
    workspace = ScriptedWorkspace(rc14_answer(), {})
    runner, pauses = scripted_runner(workspace)

    with pytest.raises(ControlPlaneCommandError) as raised:
        await runner.json("POST", path, body={}, timeout_seconds=5.0)

    assert raised.value.transient is True
    assert workspace.requests == [("POST", path)]
    assert pauses == []


async def test_a_delete_asked_again_that_finds_the_branch_gone_is_done() -> None:
    """The first DELETE was served though answered as not: the branch is gone, as asked."""

    workspace = ScriptedWorkspace(rc14_answer(), NotFound("branch id not found"))
    runner, _ = scripted_runner(workspace)

    await runner.run("DELETE", lakebase_resource_path(BRANCH), timeout_seconds=5.0)

    assert len(workspace.requests) == 2


async def test_a_first_delete_of_an_absent_branch_still_says_so() -> None:
    workspace = ScriptedWorkspace(NotFound("branch id not found"))
    runner, _ = scripted_runner(workspace)

    with pytest.raises(ControlPlaneCommandError) as raised:
        await runner.run("DELETE", lakebase_resource_path(BRANCH), timeout_seconds=5.0)

    assert raised.value.not_found is True


# --- a create is asked again only when it did not happen -----------------------------------------


class ScriptedPosts:
    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.posts = 0

    async def json(self, method: str, path: str, *, body=None, timeout_seconds: float):
        del path, body, timeout_seconds
        assert method == "POST"
        self.posts += 1
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


async def test_a_create_that_stays_unserved_fails_after_the_last_pause() -> None:
    schedule = list(lakebase_transient_pauses())
    runner = ScriptedPosts(
        *(_control_plane_failure(rc14_answer()) for _ in range(len(schedule) + 1))
    )
    pauses: list[float] = []
    checks: list[str] = []

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    async def created() -> bool:
        checks.append("checked")
        return False

    with pytest.raises(ControlPlaneCommandError) as raised:
        await create_lakebase_resource(
            runner, "/p?branch_id=b", body={}, timeout_seconds=5.0, created=created, sleep=sleep
        )

    assert raised.value.transient is True
    assert runner.posts == len(schedule) + 1
    assert pauses == schedule
    assert len(checks) == len(schedule)


async def test_rc17_a_create_refused_past_the_old_15_seconds_is_still_asked_again() -> None:
    # Five refusals were the old schedule's last answer; rc17's lane failed on the fifth.
    runner = ScriptedPosts(*(_control_plane_failure(rc14_answer()) for _ in range(6)), {})
    pauses: list[float] = []

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    async def created() -> bool:
        return False

    await create_lakebase_resource(
        runner, "/p?branch_id=b", body={}, timeout_seconds=5.0, created=created, sleep=sleep
    )

    assert runner.posts == 7
    assert pauses == [1.0, 2.0, 4.0, 8.0, 15.0, 15.0]


async def test_a_create_refused_until_the_bound_stops_the_lane_there_and_does_not_fail_it() -> None:
    """The lane's bound decides it, as it would a lane still running: the other lane wins."""

    engine, lakebase, _, _, _ = build_recovery_engine()
    engine.run_timeout_seconds = 0.2
    forever = ScriptedPosts(*(_control_plane_failure(rc14_answer()) for _ in range(10_000)))

    async def created() -> bool:
        return False

    async def create_recovery(plan, recovery_at, report):
        del plan, recovery_at, report
        await create_lakebase_resource(
            forever,
            "/p?branch_id=b",
            body={},
            timeout_seconds=5.0,
            created=created,
            sleep=lambda _seconds: asyncio.sleep(0.005),
        )
        raise AssertionError("Databricks never served the create")

    lakebase.create_recovery = create_recovery  # type: ignore[method-assign]
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm)

    stopped = result.lanes["lakebase"]
    assert stopped.timed_out is True
    assert stopped.error is not None and stopped.error.startswith("Recovery lane exceeded")
    assert result.lanes["competitor"].ok is True
    # Asked again past where the old schedule gave up, until the bound.
    assert forever.posts > len(LAKEBASE_TRANSIENT_RETRY_SECONDS) + 1


async def test_a_refused_create_is_not_asked_again() -> None:
    runner = ScriptedPosts(_control_plane_failure(BadRequest("branch already exists")))
    pauses: list[float] = []

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    async def created() -> bool:
        raise AssertionError("a refusal is not checked for")

    with pytest.raises(ControlPlaneCommandError, match="refused"):
        await create_lakebase_resource(
            runner, "/p?branch_id=b", body={}, timeout_seconds=5.0, created=created, sleep=sleep
        )

    assert runner.posts == 1
    assert pauses == []


# --- both rounds' Lakebase lanes, against a control plane that answers like rc14's ------------


class RefusingControlPlane(FakeDatabricksRunner):
    """Answers the first ``refusals`` creates of ``collection`` with rc14's INTERNAL_ERROR,
    having first made the resource when ``made``. ``primary=False`` leaves a new branch
    without its native endpoint, so the lane has to create one."""

    def __init__(
        self, *, collection: str, refusals: int = 1, made: bool, primary: bool = True
    ) -> None:
        super().__init__()
        self.collection = collection
        self.refusals = refusals
        self.made = made
        self.primary = primary
        self.posts: list[str] = []

    async def json(self, method, path, *, body=None, timeout_seconds: float):
        creating = method == "POST" and "?" in path
        if creating:
            self.posts.append(path)
        refused = creating and self.refusals > 0 and f"/{self.collection}?" in path
        if refused:
            self.refusals -= 1
        if not refused or self.made:
            answer = await super().json(
                method, path, body=body, timeout_seconds=timeout_seconds
            )
            if creating and not self.primary and "/branches?" in path:
                self.endpoints.pop(f"{answer['name']}/endpoints/primary")
        if refused:
            raise _control_plane_failure(rc14_answer())
        return answer


def adapter_on(
    runner: FakeDatabricksRunner, pauses: list[float] | None = None
) -> LakebaseSafeChangeAdapter:
    async def sleep(seconds: float) -> None:
        if pauses is not None:
            pauses.append(seconds)

    return LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile="test-profile",
            source_endpoint=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
            expected_region=REGION,
            poll_interval_seconds=0.01,
        ),
        runner=runner,
        connector=RecordingConnector(),
        sleep=sleep,
    )


def recovery_plan() -> RecoveryPlan:
    return RecoveryPlan(
        lane_id=SafeChangeProvider.LAKEBASE.value,
        name=SafeChangeProvider.LAKEBASE.value,
        provider=SafeChangeProvider.LAKEBASE,
        source_id=LAKEBASE_SOURCE,
        artifact_id=deterministic_recovery_artifact_id(RUN_ID, SafeChangeProvider.LAKEBASE),
        scope=scope(),
    )


def recovery_branch_path() -> str:
    return _lakebase_create_path(PROJECT, "branches", "branch_id", recovery_plan().artifact_id)


async def recover(runner: FakeDatabricksRunner, pauses: list[float] | None = None):
    recovery = LakebaseRecoveryAdapter(adapter_on(runner, pauses))
    return await recovery.create_recovery(recovery_plan(), RECOVERY_AT, quiet_report)


async def test_round3_asks_again_for_the_recovery_branch_rc14_was_refused() -> None:
    runner = RefusingControlPlane(collection="branches", made=False)
    pauses: list[float] = []

    artifact = await recover(runner, pauses)

    assert artifact.state == "READY/ACTIVE"
    assert artifact.owner == OWNER
    assert runner.posts == [recovery_branch_path(), recovery_branch_path()]
    assert pauses[0] == LAKEBASE_TRANSIENT_RETRY_SECONDS[0]
    branch = runner.branches[str(artifact.metadata["branch_name"])]
    assert branch["spec"]["source_branch_time"] == RECOVERY_AT.isoformat()


async def test_round3_asks_again_for_the_recovery_branch_rc17_was_refused() -> None:
    runner = RefusingControlPlane(collection="branches", refusals=6, made=False)
    pauses: list[float] = []

    artifact = await recover(runner, pauses)

    assert artifact.state == "READY/ACTIVE"
    assert runner.posts == [recovery_branch_path()] * 7
    assert pauses[:6] == [1.0, 2.0, 4.0, 8.0, 15.0, 15.0]
    branch = runner.branches[str(artifact.metadata["branch_name"])]
    assert branch["spec"]["source_branch_time"] == RECOVERY_AT.isoformat()


class SdkControlPlane:
    """``FakeDatabricksRunner``'s control plane behind the real runner, answering with the
    SDK's own errors: rc14's INTERNAL_ERROR on the first ``refusals`` branch creates, and
    ``NotFound`` for an absent resource."""

    def __init__(self, *, refusals: int) -> None:
        self.plane = FakeDatabricksRunner()
        self.refusals = refusals
        self.requests: list[tuple[str, str]] = []

    def do(self, method: str, path: str, *, body=None):
        # The runner calls this from a worker thread, which has no event loop of its own.
        self.requests.append((method, path))
        if method == "POST" and "/branches?" in path and self.refusals > 0:
            self.refusals -= 1
            raise rc14_answer()
        call = self.plane.run if method == "DELETE" else self.plane.json
        try:
            return asyncio.run(call(method, path, body=body, timeout_seconds=30.0))
        except ControlPlaneCommandError as error:
            if error.not_found:
                raise NotFound("not found") from error
            raise


async def test_rc14_through_the_real_runner_the_lane_recovers() -> None:
    workspace = SdkControlPlane(refusals=1)
    runner = DatabricksRestRunner(workspace_client=SimpleNamespace(api_client=workspace))

    artifact = await recover(runner)  # type: ignore[arg-type]

    assert artifact.state == "READY/ACTIVE"
    branch_posts = [path for method, path in workspace.requests if method == "POST"]
    assert branch_posts == [recovery_branch_path(), recovery_branch_path()]


async def test_round3_keeps_a_recovery_branch_the_refused_post_made(caplog) -> None:
    runner = RefusingControlPlane(collection="branches", made=True)

    with caplog.at_level("WARNING", logger="server.safe_change_live"):
        artifact = await recover(runner)

    assert artifact.state == "READY/ACTIVE"
    assert runner.posts == [recovery_branch_path()]
    assert any(
        "on the POST it answered as failed" in record.getMessage() for record in caplog.records
    )


async def test_round3_refuses_a_branch_of_that_name_at_another_recovery_point() -> None:
    runner = RefusingControlPlane(collection="branches", made=False)
    name = f"{PROJECT}/branches/{recovery_plan().artifact_id}"
    runner.branches[name] = {
        "name": name,
        "spec": {
            "source_branch": f"{PROJECT}/branches/production",
            "source_branch_time": "2026-10-03T16:00:00Z",
        },
        "status": {"current_state": "READY", "source_branch": f"{PROJECT}/branches/production"},
    }

    with pytest.raises(SafeChangeControlPlaneError, match="another recovery point"):
        await recover(runner)

    assert runner.posts == [recovery_branch_path()]


async def test_round3_recognizes_its_recovery_point_in_another_spelling() -> None:
    runner = RefusingControlPlane(collection="branches", made=False)
    name = f"{PROJECT}/branches/{recovery_plan().artifact_id}"
    runner.branches[name] = {
        "name": name,
        "spec": {
            "source_branch": f"{PROJECT}/branches/production",
            "source_branch_time": "2026-10-03T17:45:55Z",
        },
        "status": {"current_state": "READY", "source_branch": f"{PROJECT}/branches/production"},
    }
    runner.endpoints[f"{name}/endpoints/primary"] = {
        "name": f"{name}/endpoints/primary",
        "status": {
            "current_state": "ACTIVE",
            "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
            "hosts": {"host": "isolated.database.us-west-2.cloud.databricks.com"},
        },
    }

    artifact = await recover(runner)

    assert artifact.state == "READY/ACTIVE"
    assert runner.posts == [recovery_branch_path()]


async def test_round3_asks_again_for_its_recovery_endpoint() -> None:
    runner = RefusingControlPlane(collection="endpoints", made=False, primary=False)

    artifact = await recover(runner)

    assert artifact.state == "READY/ACTIVE"
    endpoint_posts = [path for path in runner.posts if "/endpoints?" in path]
    assert len(endpoint_posts) == 2


async def test_round2_asks_again_for_its_branch() -> None:
    runner = RefusingControlPlane(collection="branches", made=False)
    lane = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)

    artifact = await adapter_on(runner).create_isolated(lane, quiet_report)

    assert artifact.state == "READY/ACTIVE"
    branch_path = _lakebase_create_path(PROJECT, "branches", "branch_id", lane.artifact_id)
    assert runner.posts == [branch_path, branch_path]


async def test_round2_keeps_a_branch_the_refused_post_made() -> None:
    runner = RefusingControlPlane(collection="branches", made=True)
    lane = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)

    artifact = await adapter_on(runner).create_isolated(lane, quiet_report)

    assert artifact.state == "READY/ACTIVE"
    assert len(runner.posts) == 1


async def test_round2_refuses_a_branch_of_that_name_from_another_source() -> None:
    runner = RefusingControlPlane(collection="branches", made=False)
    lane = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    name = f"{PROJECT}/branches/{lane.artifact_id}"
    runner.branches[name] = {
        "name": name,
        "status": {
            "current_state": "READY",
            "source_branch": f"{PROJECT}/branches/not-production",
        },
    }

    with pytest.raises(SafeChangeControlPlaneError, match="not branched from the source"):
        await adapter_on(runner).create_isolated(lane, quiet_report)

    assert len(runner.posts) == 1


@pytest.mark.parametrize("made", [False, True], ids=["not-made", "made"])
async def test_round2_creates_its_endpoint_once(made: bool) -> None:
    runner = RefusingControlPlane(collection="endpoints", made=made, primary=False)
    lane = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)

    artifact = await adapter_on(runner).create_isolated(lane, quiet_report)

    assert artifact.metadata["ownership_marker_valid"] is True
    endpoint_posts = [path for path in runner.posts if "/endpoints?" in path]
    assert len(endpoint_posts) == (1 if made else 2)
