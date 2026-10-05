"""A dropped Lakebase branch DELETE is sent again, and a refused request names its answer.

2026-10-02. rc10's release bar lost a Round 3 bout because the Lakebase lane's branch POST was
refused at the bell, and the log said only "Databricks control-plane request was refused". A probe
on the R6 test installation then showed that Lakebase accepts a DELETE sent while a branch is
still initializing and never acts on it: the branch stayed five minutes after a DELETE that
answered done, and the next create of that name was refused with "branch already exists".
Rounds 2 and 3 reuse one branch name per installation.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from databricks.sdk.errors import BadRequest, NotFound

from server.safe_change_live import (
    LAKEBASE_DELETE_REASK_SECONDS,
    ControlPlaneCommandError,
    DatabricksRestRunner,
    SafeChangeControlPlaneError,
    lakebase_resource_path,
    wait_lakebase_branch_gone,
)

BRANCH = "projects/ad-test/branches/recovery-ad-test-003"
CONFIG = SimpleNamespace(
    poll_timeout_seconds=300.0, poll_interval_seconds=2.0, control_timeout_seconds=30.0
)


class _Branch:
    """A branch that ignores DELETEs until the ``honored``-th one."""

    def __init__(self, *, honored: int, resend_error: Exception | None = None) -> None:
        self.honored = honored
        self.resend_error = resend_error
        # The caller has already sent the first DELETE, before the wait starts.
        self.deletes = 1
        self.present = honored > 1
        self.paths: list[str] = []

    async def run(self, method, path, *, body=None, timeout_seconds: float) -> None:
        assert (method, body, timeout_seconds) == ("DELETE", None, 30.0)
        self.paths.append(path)
        self.deletes += 1
        if self.resend_error is not None:
            self.present = False
            raise self.resend_error
        if self.deletes >= self.honored:
            self.present = False


async def _wait(branch: _Branch) -> dict[str, float]:
    now = {"t": 0.0}

    async def sleep(seconds: float) -> None:
        now["t"] += seconds

    async def still_present() -> bool:
        return branch.present

    await wait_lakebase_branch_gone(
        branch,
        BRANCH,
        still_present,
        clock=lambda: now["t"],
        sleep=sleep,
        config=CONFIG,
        still_exists="the branch is still there",
    )
    return now


async def test_a_dropped_delete_is_sent_again_until_the_branch_goes() -> None:
    branch = _Branch(honored=2)

    now = await _wait(branch)

    assert branch.deletes == 2
    assert branch.paths == [lakebase_resource_path(BRANCH)]
    assert LAKEBASE_DELETE_REASK_SECONDS <= now["t"] < CONFIG.poll_timeout_seconds


async def test_each_resend_is_in_the_operator_log(caplog) -> None:
    """INFO never reaches the Apps log, so a release run could not show the resend firing."""

    with caplog.at_level("WARNING", logger="server.safe_change_live"):
        await _wait(_Branch(honored=2))

    assert [record.getMessage() for record in caplog.records] == [
        f"Lakebase branch {BRANCH} is still present 10s after its DELETE; sending it again"
    ]


async def test_a_delete_that_landed_the_first_time_is_not_sent_again() -> None:
    branch = _Branch(honored=1)

    await _wait(branch)

    assert branch.paths == []


async def test_a_branch_that_never_goes_still_fails_at_the_deadline_after_asking_again() -> None:
    branch = _Branch(honored=10**9)

    with pytest.raises(SafeChangeControlPlaneError, match="the branch is still there"):
        await _wait(branch)

    assert len(branch.paths) >= CONFIG.poll_timeout_seconds // LAKEBASE_DELETE_REASK_SECONDS - 1


@pytest.mark.parametrize(
    "answer",
    [
        ControlPlaneCommandError("missing", not_found=True),
        ControlPlaneCommandError("Databricks control-plane request was refused"),
    ],
    ids=["already-gone", "not-accepted"],
)
async def test_a_resend_the_control_plane_turns_down_keeps_waiting(answer) -> None:
    branch = _Branch(honored=10**9, resend_error=answer)

    await _wait(branch)

    assert branch.deletes == 2


def _runner_answering(error: Exception) -> DatabricksRestRunner:
    def do(method: str, path: str, *, body=None):
        del method, path, body
        raise error

    return DatabricksRestRunner(
        workspace_client=SimpleNamespace(api_client=SimpleNamespace(do=do))
    )


async def test_a_refused_request_logs_the_workspaces_own_answer(caplog) -> None:
    path = "/api/2.0/postgres/projects/ad-test/branches?branch_id=recovery-ad-test-003"
    runner = _runner_answering(
        BadRequest(
            'branch already exists; branch_name:"recovery-ad-test-003" [TraceId: 8ac35a6a]',
            error_code="BAD_REQUEST",
        )
    )

    with caplog.at_level("WARNING", logger="server.safe_change_live"):
        with pytest.raises(ControlPlaneCommandError, match="request was refused"):
            await runner.json("POST", path, body={}, timeout_seconds=5.0)

    logged = [record.getMessage() for record in caplog.records]
    assert any(
        f"POST {path}" in line and "BAD_REQUEST" in line and "branch already exists" in line
        for line in logged
    ), logged


def test_both_rounds_wait_for_their_branch_through_the_resend() -> None:
    import inspect

    from server.recovery_live import LakebaseRecoveryAdapter
    from server.safe_change_live import LakebaseSafeChangeAdapter

    deletes = (LakebaseRecoveryAdapter.delete_recovery, LakebaseSafeChangeAdapter.delete_isolated)
    for delete in deletes:
        assert "await wait_lakebase_branch_gone(" in inspect.getsource(delete), delete.__qualname__


async def test_an_absence_probe_logs_nothing(caplog) -> None:
    runner = _runner_answering(NotFound("Branch 'recovery-ad-test-003' not found"))

    with caplog.at_level("WARNING", logger="server.safe_change_live"):
        with pytest.raises(ControlPlaneCommandError):
            await runner.json("GET", lakebase_resource_path(BRANCH), timeout_seconds=5.0)

    assert caplog.records == []
