"""Which installation's receipts an operator is allowed to see.

An installation owns real cloud resources. When one is torn down and replaced, the
replacement gets a new ``run_id`` and the old one's endpoints, instances and
clusters stop existing. Its receipts are still on disk, and showing them beside
today's would present results from infrastructure that is gone as though they were
this demo's. These tests pin the rule that stops that.

They also pin the property the summary is built on: a receipt outlives the process
that wrote it, so a round completed before a restart is still there afterwards.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api import router
from server.models import RoundId
from server.receipts import (
    BoutReceipt,
    Installation,
    LaneReceipt,
    belongs_to_installation,
    derive_receipt,
    load_receipts,
    write_receipt,
)
from tests.test_receipts import verified_round_one_snapshot

THIS_RUN = "ad-20260820-1446-abcd"
# The installation before it. Its AWS resources were destroyed on 2026-08-20.
PRIOR_RUN = "ad-20260819-0009-dcba"
INSTALLED_AT = datetime(2026, 8, 20, 14, 46, 33, 556294, tzinfo=UTC)

INSTALLATION = Installation(run_id=THIS_RUN, created_at=INSTALLED_AT)


def receipt(
    *,
    run_id: str | None,
    sealed_at: datetime,
    identifier: str = "AAAA1111",
) -> BoutReceipt:
    return BoutReceipt(
        receipt=identifier,
        session_id=f"{identifier.lower()}-0000-0000-0000-000000000000",
        round_id=RoundId.WAKE_IDLE_APP,
        round_title="WAKE THIS IDLE APP",
        opponent="Aurora Serverless v2",
        opponent_id="aurora_serverless_v2",
        run_id=run_id,
        outcome="declared",
        sealing_event="run_finished",
        has_measurements=True,
        metric="bout_elapsed_ms",
        lakebase=LaneReceipt(ms=2400.0, state="verified"),
        opponent_lane=LaneReceipt(ms=14570.0, state="verified"),
        margin_ms=12170.0,
        start_skew_ms=0.25,
        sealed_at=sealed_at,
    )


# ------------------------------------------------------------------ the predicate


def test_which_receipts_this_installation_may_show() -> None:
    """The whole predicate, one row per receipt shape it has to judge.

    Read the rows against each other rather than one at a time: the rule is
    "an explicit id decides, and only an unstamped receipt falls back to the
    timestamp", and it takes the pair of stamped-stranger rows to pin that a
    timestamp inside our window does not rescue a foreign id.

    `is` rather than truthiness throughout: this predicate gates what an
    audience is shown, so a stray truthy value must not read as a decision.
    """

    cases: tuple[tuple[str, str | None, datetime, bool], ...] = (
        (
            # The case that matters: stamped, in the same directory, not ours,
            # and sealed *after* we were installed, so only the id can rule it
            # out. A timestamp rule alone would let this one through.
            "a stranger sealed after we were installed",
            PRIOR_RUN,
            INSTALLED_AT + timedelta(hours=9),
            False,
        ),
        (
            # A stranger sealed well inside our window is still a stranger.
            "a stranger sealed a day into our window",
            PRIOR_RUN,
            INSTALLED_AT + timedelta(days=1),
            False,
        ),
        (
            "stamped with this run",
            THIS_RUN,
            INSTALLED_AT + timedelta(hours=9),
            True,
        ),
        (
            # No id to check, and it cannot be ours: we did not exist yet.
            "unstamped and sealed before we existed",
            None,
            INSTALLED_AT - timedelta(minutes=1),
            False,
        ),
        (
            # Every receipt written before run_id existed lands here. Excluding
            # these would be the cautious-sounding rule and would empty the
            # board of the rounds the operator actually ran.
            "unstamped and sealed inside our window",
            None,
            INSTALLED_AT + timedelta(hours=9),
            True,
        ),
        (
            # Every writer writes UTC; a naive value must not shift the boundary.
            "unstamped with a naive timestamp exactly on the boundary",
            None,
            INSTALLED_AT.replace(tzinfo=None),
            True,
        ),
    )

    for name, run_id, sealed_at, expected in cases:
        candidate = receipt(run_id=run_id, sealed_at=sealed_at)
        assert belongs_to_installation(candidate, INSTALLATION) is expected, name


# ------------------------------------------------------------------- the loader


async def test_the_loader_filters_only_when_it_has_an_identity_to_filter_against(
    tmp_path: Path,
) -> None:
    """One directory holding both installations' receipts, read three ways.

    The unfiltered readings are as load-bearing as the filtered one: a
    manifest-less dev server or unit test has nothing to compare against, and
    answering nothing there would be a worse failure than answering everything.
    Both spellings of "no identity" -- the argument omitted and the argument
    passed as None -- have to mean that.
    """

    root = tmp_path / "receipts"
    snapshot = await verified_round_one_snapshot()

    mine = derive_receipt(snapshot, "run_finished", run_id=THIS_RUN)
    write_receipt(mine, snapshot, root)

    theirs = derive_receipt(snapshot, "run_finished", run_id=PRIOR_RUN)
    theirs = theirs.model_copy(update={"receipt": "DEAD0000"})
    write_receipt(theirs, snapshot, root)

    assert len(load_receipts(root=root)) == 2
    assert len(load_receipts(root=root, installation=None)) == 2
    kept = load_receipts(root=root, installation=INSTALLATION)
    assert [item.run_id for item in kept] == [THIS_RUN]


# ------------------------------------------------------------------ the stamping


async def test_a_new_receipt_carries_the_run_id_it_was_given() -> None:
    snapshot = await verified_round_one_snapshot()

    assert derive_receipt(snapshot, "run_finished", run_id=THIS_RUN).run_id == THIS_RUN
    # Absent rather than invented when there is no installation to name.
    assert derive_receipt(snapshot, "run_finished").run_id is None


async def test_a_stamped_receipt_survives_the_round_trip_through_disk(
    tmp_path: Path,
) -> None:
    root = tmp_path / "receipts"
    snapshot = await verified_round_one_snapshot()
    write_receipt(derive_receipt(snapshot, "run_finished", run_id=THIS_RUN), snapshot, root)

    assert [item.run_id for item in load_receipts(root=root)] == [THIS_RUN]


# --------------------------------------------------------------- restart survival


async def test_a_bout_completed_before_a_restart_is_still_there_afterwards(
    tmp_path: Path, monkeypatch
) -> None:
    """The whole point of the summary.

    Sessions live in a dict that is created empty on every boot. A round run before
    a restart leaves nothing in it. The receipt is what carries the round across,
    so this asserts both halves: the store is empty, and the round is still there.
    """
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path / "artifacts"))

    # --- before the restart: one round runs and seals.
    snapshot = await verified_round_one_snapshot()
    write_receipt(derive_receipt(snapshot, "run_finished", run_id=THIS_RUN), snapshot)

    # A receipt the previous installation left behind, in the same directory.
    stranger = derive_receipt(snapshot, "run_finished", run_id=PRIOR_RUN)
    write_receipt(stranger.model_copy(update={"receipt": "DEAD0000"}), snapshot)

    # --- the restart: a brand new process, so a brand new session store.
    from server.manager import RunManager
    from server.verifier import NeutralVerifier, RetryPolicy
    from tests.test_manager import FakeResolver

    rebooted = RunManager(
        resolver=FakeResolver(),
        verifier=NeutralVerifier(RetryPolicy(overall_timeout_seconds=1)),
    )
    assert rebooted._records == {}, "a fresh process must start with no sessions"
    await rebooted.close()

    # --- after the restart: the round is still on the board, the stranger is not.
    survivors = load_receipts(installation=INSTALLATION)
    assert [item.session_id for item in survivors] == [snapshot.id]
    assert survivors[0].round_id == RoundId.WAKE_IDLE_APP
    assert survivors[0].lakebase.ms is not None


async def test_the_receipts_route_withholds_a_previous_installation(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    snapshot = await verified_round_one_snapshot()
    write_receipt(derive_receipt(snapshot, "run_finished", run_id=THIS_RUN), snapshot)
    stranger = derive_receipt(snapshot, "run_finished", run_id=PRIOR_RUN)
    write_receipt(stranger.model_copy(update={"receipt": "DEAD0000"}), snapshot)

    monkeypatch.setattr(
        "server.api.current_installation",
        lambda: INSTALLATION,
    )

    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        body = client.get("/api/receipts").json()["receipts"]

    assert [item["run_id"] for item in body] == [THIS_RUN]
