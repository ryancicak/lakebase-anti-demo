"""Round 3 never asks Lakebase for a recovery branch at a whole minute.

2026-10-04, rc17. Round 3's Lakebase lane asked for its recovery branch at 20:32:00Z and was
answered INTERNAL_ERROR "Please try again later" five times running. It was not a passing
hiccup: Lakebase refuses every create-branch whose `source_branch_time` has no seconds and no
fraction, and accepts the same call a millisecond later.
The recovery point is the full second before the delete, so about one bout in 60 lands on a
minute, and each of the four that did in the release runs lost its Lakebase lane: rc10's at
09:49:00Z, rc13's at 02:46:00Z (hidden by a towel), rc14's at 17:46:00Z and rc17's. Asking
again could never help, so the lane asks for the instant a millisecond later instead.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from server.recovery_live import LakebaseRecoveryAdapter, lakebase_source_branch_time
from server.safe_change_live import _control_plane_failure
from tests.test_lakebase_transient_retry import adapter_on, rc14_answer, recovery_plan
from tests.test_safe_change_live import FakeDatabricksRunner, quiet_report

RC17_RECOVERY_AT = datetime(2026, 10, 4, 20, 32, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("recovery_at", "asked"),
    [
        (RC17_RECOVERY_AT, "2026-10-04T20:32:00.001000+00:00"),
        (datetime(2026, 10, 3, 17, 46, 0, tzinfo=UTC), "2026-10-03T17:46:00.001000+00:00"),
        # Any other second is asked as it is: only a whole minute is refused.
        (datetime(2026, 10, 4, 20, 32, 5, tzinfo=UTC), "2026-10-04T20:32:05+00:00"),
        (datetime(2026, 10, 4, 20, 31, 59, tzinfo=UTC), "2026-10-04T20:31:59+00:00"),
    ],
)
def test_only_a_whole_minute_is_asked_a_millisecond_later(recovery_at: datetime, asked: str):
    assert lakebase_source_branch_time(recovery_at) == asked


def test_the_millisecond_stays_inside_the_second_before_the_delete() -> None:
    asked = datetime.fromisoformat(lakebase_source_branch_time(RC17_RECOVERY_AT))
    # The delete is observed at least a full second after the recovery point.
    assert RC17_RECOVERY_AT < asked < RC17_RECOVERY_AT + timedelta(seconds=1)


class WholeMinuteRefusingControlPlane(FakeDatabricksRunner):
    """Lakebase as the release runs met it: a branch create at a whole minute is answered
    INTERNAL_ERROR every time, and one a millisecond later is made."""

    def __init__(self) -> None:
        super().__init__()
        self.branch_times: list[str] = []

    async def json(self, method, path, *, body=None, timeout_seconds: float):
        if method == "POST" and "/branches?" in path:
            asked = str(((body or {}).get("spec") or {}).get("source_branch_time") or "")
            self.branch_times.append(asked)
            moment = datetime.fromisoformat(asked.replace("Z", "+00:00"))
            if moment.second == 0 and moment.microsecond == 0:
                raise _control_plane_failure(rc14_answer())
        return await super().json(method, path, body=body, timeout_seconds=timeout_seconds)


async def test_rc17s_whole_minute_bout_gets_its_recovery_branch_on_the_first_ask() -> None:
    runner = WholeMinuteRefusingControlPlane()
    pauses: list[float] = []
    recovery = LakebaseRecoveryAdapter(adapter_on(runner, pauses))

    artifact = await recovery.create_recovery(recovery_plan(), RC17_RECOVERY_AT, quiet_report)

    assert artifact.state == "READY/ACTIVE"
    assert runner.branch_times == ["2026-10-04T20:32:00.001000+00:00"]
    branch = runner.branches[str(artifact.metadata["branch_name"])]
    assert branch["spec"]["source_branch_time"] == "2026-10-04T20:32:00.001000+00:00"


async def test_the_evidence_names_the_instant_lakebase_was_asked_for() -> None:
    recovery = LakebaseRecoveryAdapter(adapter_on(WholeMinuteRefusingControlPlane()))

    evidence = await recovery.wait_recovery_point(recovery_plan(), RC17_RECOVERY_AT, quiet_report)

    assert evidence == {"source_branch_time": "2026-10-04T20:32:00.001000+00:00"}
