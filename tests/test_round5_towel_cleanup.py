"""A towel during Round 5 setup must not leave an RDS Proxy running and unsaid.

Both halves of one live defect are covered here, because they failed together.
An operator threw a towel during Round 5 setup; the cancelled SSM commands would
not confirm settlement inside a ten-second window; the timeout propagated out of
``_settle_commands`` and took the reverse cleanup with it, so ``DeleteDBProxy``
was never called at all; the automatic retry re-entered at the identical line
roughly every forty seconds and failed identically every time. Twenty minutes
later the Proxy was still ``available`` and ``/readyz`` said
``status: ready, degraded: false``.

So the tests come in two groups. The first group is about reaching the deletion:
settlement is tidy-up, deletion is money, and the tidy-up may not hold the money
hostage. The second is about saying so: a cleanup that genuinely cannot finish
has to name the surviving resource somewhere an operator reads without being
told to look.

Every assertion here was watched fail against the unfixed code before it was
kept. This project has found six guards that passed vacuously, and the standing
rule is that a guard which cannot be shown to fire is worse than no guard,
because it looks like coverage.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import botocore.session
import pytest

import app as app_module
from server.connection_spike_live import (
    SETTLEMENT_TIMEOUT_SECONDS,
    SSM_TIMEOUT_SECONDS,
    LiveConnectionSpikeSetupOrchestrator,
)
from server.manager import RunManager
from server.round5_cleanup_owed import (
    GRACE_SECONDS,
    clear_round5_cleanup_owed,
    leaked_proxy_sentence,
    record_round5_cleanup_owed,
    reset_round5_cleanup_owed,
    round5_cleanup_owed_notice,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
INSTANCE_ID = "i-0123456789abcdef0"
START = datetime(2026, 8, 25, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _no_inherited_leak_notices() -> Any:
    """One test's leaked Proxy must not become the next test's verdict."""

    reset_round5_cleanup_owed()
    yield
    reset_round5_cleanup_owed()


def _command(bout_id: str, lane_id: str, action: str, command_id: str) -> Any:
    return SimpleNamespace(
        bout_id=bout_id,
        lane_id=lane_id,
        action=action,
        command_id=command_id,
        ssm=SimpleNamespace(cancel_command=object(), get_command_invocation=object()),
    )


def _orchestrator(*, settlement_seconds: float = SETTLEMENT_TIMEOUT_SECONDS) -> Any:
    """A setup orchestrator with only the cleanup machinery wired up.

    ``object.__new__`` rather than the constructor, following
    ``test_connection_spike_setup_cancellation``: the real one wants a sealed
    config, a journal, a fence and a live host resolver, and none of the four
    takes part in what is under test here.
    """

    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        settlement_timeout_seconds=settlement_seconds,
        command_timeout_seconds=SSM_TIMEOUT_SECONDS,
        runner_instance_id=INSTANCE_ID,
        poll_interval_seconds=0.01,
        deterministic_name_prefix="anti-demo-r5",
        secret_name_prefix="",
    )
    orchestrator._active_commands = {}
    orchestrator._pending_sends = {}
    orchestrator._cleanup_start_lock = asyncio.Lock()
    orchestrator._cleanup_tasks = {}
    orchestrator._proxy_delete_accepted = {}
    orchestrator._coordinators = {}
    orchestrator._scopes = {}
    orchestrator._receipts = {}
    orchestrator._results = {}
    return orchestrator


def _wedge_ssm(orchestrator: Any) -> None:
    """Every SSM read answers ``Pending``, so settlement can only ever expire.

    This is the live failure reproduced at its source rather than simulated one
    layer up: the real ``_cancel_setup_command`` runs, its real
    ``asyncio.timeout`` is what fires, and the only thing stubbed is the wire.
    """

    async def call(_operation: Any, **_kwargs: Any) -> dict[str, object]:
        return {"Status": "Pending"}

    orchestrator._call = call
    orchestrator._sleep = lambda _delay: asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Half one: cleanup has to reach the deletion.
# ---------------------------------------------------------------------------


async def test_a_settlement_that_never_confirms_is_reported_and_never_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The propagation that took the Proxy deletion with it, closed at its source.

    Against the shipped code this call raised ``TimeoutError``. Everything that
    calls it runs immediately before the reverse cleanup, so raising here is
    what turned "an SSM command did not answer" into "the RDS Proxy was never
    deleted".
    """

    orchestrator = _orchestrator(settlement_seconds=0.05)
    orchestrator._active_commands = {
        "lakebase:verify": _command("bout-leak", "lakebase", "verify", "cmd-lakebase"),
        "competitor:verify": _command("bout-leak", "competitor", "verify", "cmd-comp"),
        # A neighbour's command. Settling one bout must not report another's.
        "other:verify": _command("bout-other", "other", "verify", "cmd-other"),
    }
    _wedge_ssm(orchestrator)
    caplog.set_level(logging.ERROR, logger="server.connection_spike_live")

    unsettled = await orchestrator._settle_commands("bout-leak")

    assert set(unsettled) == {
        "lakebase:verify=cmd-lakebase",
        "competitor:verify=cmd-comp",
    }
    reported = [
        record.getMessage()
        for record in caplog.records
        if "DID NOT CONFIRM SETTLEMENT" in record.getMessage()
    ]
    assert reported, "an unsettled SSM command was stepped over and never named"
    # Findable by hand, which is the whole point of stepping over it, and scoped
    # to the bout that was actually being settled.
    assert "cmd-lakebase" in reported[0]
    assert "cmd-comp" in reported[0]
    assert "cmd-other" not in reported[0]


async def test_a_wedged_ssm_command_cannot_stop_the_rds_proxy_from_being_deleted() -> None:
    """The money test. Settlement expires; the deletion still happens.

    This is the live bout in miniature: cancelled commands that will not confirm,
    and a cleanup that has to get past them to the one resource that bills. On
    the shipped code ``begin_cleanup`` raised out of ``_settle_commands`` and
    ``_cleanup_exactly`` was never created, so nothing ever called
    ``DeleteDBProxy``.
    """

    orchestrator = _orchestrator(settlement_seconds=0.05)
    orchestrator._active_commands = {
        "lakebase:verify": _command("bout-leak", "lakebase", "verify", "cmd-lakebase"),
    }
    _wedge_ssm(orchestrator)

    torn_down: list[str] = []

    async def cleanup(_scope: object, _receipt: object) -> Any:
        torn_down.append("reverse-cleanup")
        return SimpleNamespace(complete=True)

    orchestrator._coordinators = {"bout-leak": SimpleNamespace(cleanup=cleanup)}
    orchestrator._scopes = {"bout-leak": object()}
    orchestrator._receipts = {"bout-leak": object()}

    await orchestrator.begin_cleanup("bout-leak")
    await orchestrator.wait_for_cleanup_complete("bout-leak")

    assert torn_down == ["reverse-cleanup"], (
        "an SSM command that would not settle held the RDS Proxy hostage"
    )
    assert orchestrator.proxy_delete_accepted("bout-leak") is True


async def test_the_retry_path_also_gets_past_a_command_that_will_not_settle() -> None:
    """``reconcile_failed_cleanup`` is where the same timeout recurred every 40s.

    Fixing only ``begin_cleanup`` would have left the automatic retry looping on
    an identical failure, which is what made the live incident *durable* rather
    than merely a bad first attempt. The settlement here is the real one and it
    really expires; what follows it must still run.
    """

    orchestrator = _orchestrator(settlement_seconds=0.05)
    orchestrator._active_commands = {
        "lakebase:verify": _command("bout-leak", "lakebase", "verify", "cmd-lakebase"),
    }
    _wedge_ssm(orchestrator)
    reached: list[str] = []

    async def assumed_clients(_bout_id: str) -> Any:
        reached.append("past-settlement")
        raise RuntimeError("stop here; the reconcile itself is not under test")

    orchestrator._lock = asyncio.Lock()
    orchestrator._fence = SimpleNamespace(assert_current=lambda _authority: asyncio.sleep(0))
    orchestrator._assumed_clients = assumed_clients
    orchestrator.config.baseline_sha256 = "b" * 64

    with pytest.raises(RuntimeError, match="stop here"):
        await orchestrator.reconcile_failed_cleanup("bout-leak", 11)

    assert reached == ["past-settlement"], "the retry still cannot get past settling SSM commands"


def test_the_settlement_window_clears_the_floor_aws_itself_publishes() -> None:
    """Ten seconds could not have worked, and AWS says so in its own model.

    Read out of botocore rather than written down here, so that this states a
    fact about the platform instead of a number somebody once believed. If AWS
    ever moves the floor, this fails and the constant gets re-derived rather
    than silently falling under it.
    """

    model = botocore.session.get_session().get_service_model("ssm")
    floor = model.operation_model("SendCommand").input_shape.members["TimeoutSeconds"]
    published_minimum = float(floor.metadata["min"])

    assert published_minimum == 30.0
    assert SETTLEMENT_TIMEOUT_SECONDS >= published_minimum, (
        "a settlement window under the floor AWS publishes for command pickup "
        "expires inside the one step it has no influence over"
    )
    # And it stays inside the command's own boundary. Past that SSM ends the
    # command regardless, so there is nothing left for polling to learn.
    assert SETTLEMENT_TIMEOUT_SECONDS < SSM_TIMEOUT_SECONDS


def test_the_leak_report_names_the_proxy_the_creator_actually_builds() -> None:
    """A wrong name is worse than none: it sends the operator to an empty list."""

    orchestrator = _orchestrator()
    expected = LiveConnectionSpikeSetupOrchestrator.names_for_bout(
        "anti-demo-r5", "bout-named"
    ).proxy_name

    assert orchestrator.proxy_name_for_bout("bout-named") == expected


# ---------------------------------------------------------------------------
# Half two: a cleanup that cannot finish has to say what it left behind.
# ---------------------------------------------------------------------------


def test_the_notice_is_silent_until_an_ordinary_cleanup_would_have_finished() -> None:
    """Silence inside the window, on the precedent ``owed_stop_notice`` set.

    A warning that fires after every towel is learned away within a week, and
    then it is not a warning.
    """

    record_round5_cleanup_owed("session-a", resource="proxy-a", now=lambda: START)

    # Not at the instant it was recorded, which is the first moments after a
    # towel: a Proxy exists and its delete has not been issued yet, because
    # issuing it is what is being started.
    assert round5_cleanup_owed_notice(now=lambda: START) is None
    # And the window is worth having rather than nominal. Half a minute is the
    # floor claim; the constant is four times it.
    half_a_minute = START + timedelta(seconds=30)
    assert round5_cleanup_owed_notice(now=lambda: half_a_minute) is None

    outside = START + timedelta(seconds=GRACE_SECONDS + 1)
    assert round5_cleanup_owed_notice(now=lambda: outside) is not None


def test_re_recording_every_failed_attempt_does_not_restart_the_clock() -> None:
    """The live loop called this thirty-odd times. A clock it reset never fires.

    A counter that restarts faster than it counts is how a bound becomes no
    bound at all, and the automatic retry re-enters roughly every forty seconds
    -- well inside the grace window -- for as long as it runs.
    """

    record_round5_cleanup_owed("session-a", resource="proxy-a", now=lambda: START)
    for attempt in range(1, 31):
        moment = START + timedelta(seconds=40 * attempt)
        record_round5_cleanup_owed(
            "session-a",
            attempts=attempt,
            now=lambda moment=moment: moment,
        )

    just_past_grace = START + timedelta(seconds=GRACE_SECONDS + 1)
    notice = round5_cleanup_owed_notice(now=lambda: just_past_grace)

    assert notice is not None, "the notice never became due because it kept resetting"
    assert notice.since == START.isoformat()
    # And the name learned on the first call is not erased by later ones that
    # could not supply it.
    assert notice.resource == "proxy-a"


def test_abandoned_cleanup_is_due_immediately_and_says_it_stopped_trying() -> None:
    """Grace is for a cleanup that might still finish. This one will not."""

    owed = record_round5_cleanup_owed(
        "session-b",
        resource="proxy-b",
        attempts=120,
        still_retrying=False,
        now=lambda: START,
    )

    assert owed.due_at == START
    notice = round5_cleanup_owed_notice(now=lambda: START)
    assert notice is not None
    assert "after 120 automatic attempts" in notice.detail
    assert "automatic cleanup has stopped retrying" in notice.detail
    # The phrase the sealed receipt and the towel diagnostic are read for.
    assert "did not converge" in notice.detail


def test_a_confirmed_deletion_takes_the_notice_back_down() -> None:
    """A warning that outlives its cause teaches the reader to ignore warnings."""

    record_round5_cleanup_owed("session-c", resource="proxy-c", now=lambda: START)
    past = START + timedelta(seconds=GRACE_SECONDS + 1)
    assert round5_cleanup_owed_notice(now=lambda: past) is not None

    clear_round5_cleanup_owed("session-c")

    assert round5_cleanup_owed_notice(now=lambda: past) is None


def _manager() -> Any:
    """A ``RunManager`` with nothing constructed but real method dispatch.

    The two methods under test read the record and the notice module and touch
    no other manager state, so building one for real would mean a store, a
    lease, a ring and an event loop's worth of scaffolding to exercise thirty
    lines. Real dispatch is kept rather than passing ``None`` as ``self``, so
    that a later edit reaching for manager state fails here as an
    ``AttributeError`` instead of quietly not being covered.
    """

    return object.__new__(RunManager)


def _record(engine: Any, session_id: str = "session-d") -> Any:
    return SimpleNamespace(
        connection_spike_engine=engine,
        snapshot=SimpleNamespace(id=session_id),
    )


def test_a_slow_but_accepted_proxy_deletion_is_not_reported_as_a_leak() -> None:
    """The false alarm this gate exists to prevent.

    A healthy Round 5 cleanup crosses its delete handoff in seconds and then
    waits -- once, measured, for 31.5 minutes -- for AWS to make the Proxy
    disappear. Every one of those minutes marks cleanup pending. Reporting them
    would put a leak warning on ordinary bouts.
    """

    engine = SimpleNamespace(
        proxy_delete_accepted=lambda: True,
        proxy_name_for_bout=lambda bout: f"{bout}-proxy",
    )

    owed = _manager()._note_round5_proxy_at_risk(_record(engine))

    assert owed is None
    past = datetime.now(UTC) + timedelta(seconds=GRACE_SECONDS + 1)
    assert round5_cleanup_owed_notice(now=lambda: past) is None


def test_a_delete_that_never_landed_is_recorded_and_names_the_proxy() -> None:
    """The live case: nothing asked AWS to remove anything, so nothing will."""

    engine = SimpleNamespace(
        proxy_delete_accepted=lambda: False,
        proxy_name_for_bout=lambda bout: f"{bout}-proxy",
    )

    owed = _manager()._note_round5_proxy_at_risk(_record(engine))

    assert owed is not None
    assert owed.resource == "session-d-proxy"
    assert "session-d-proxy" in owed.detail


def test_abandonment_reports_even_when_the_delete_had_been_accepted() -> None:
    """Accepted is reassuring while retries continue and meaningless once they stop.

    After the whole budget is spent, "AWS took the request" and "the resource is
    gone" are not the same claim, and only the second one is safe to be quiet
    about.
    """

    engine = SimpleNamespace(
        proxy_delete_accepted=lambda: True,
        proxy_name_for_bout=lambda bout: f"{bout}-proxy",
    )

    owed = _manager()._note_round5_proxy_at_risk(
        _record(engine, "session-e"),
        attempts=120,
        still_retrying=False,
    )

    assert owed is not None
    assert owed.detail == leaked_proxy_sentence(
        "session-e-proxy",
        owed.since,
        attempts=120,
        still_retrying=False,
    )


def test_an_engine_that_cannot_name_its_proxy_still_reports_the_leak() -> None:
    """Not knowing the name is a reason to say less, never a reason to say nothing."""

    engine = SimpleNamespace(
        proxy_delete_accepted=lambda: False,
        proxy_name_for_bout=lambda _bout: (_ for _ in ()).throw(RuntimeError("no seal")),
    )

    owed = _manager()._note_round5_proxy_at_risk(_record(engine, "session-f"))

    assert owed is not None
    assert owed.resource == ""
    assert "MAY STILL BE RUNNING AND BILLING" in owed.detail


async def test_giving_up_writes_the_proxy_name_onto_the_towel_and_the_setup() -> None:
    """The diagnostic an operator actually reads has to carry the resource.

    ``cleanup_failure`` was already populated on abandonment and already sealed
    into the receipt -- both predate this work and neither was the defect. What
    it said was "cleanup did not converge", which is true and names nothing, so
    the receipt for the live bout would have recorded a tidy-up failure and not
    the RDS Proxy that failure left billing. This is the join between the two
    halves: one sentence, from the same writer ``/readyz`` reads, on both the
    towel and the Round 5 setup.
    """

    published: list[tuple[str, dict[str, Any]]] = []

    async def publish(event: str, payload: dict[str, Any]) -> None:
        published.append((event, payload))

    towel = SimpleNamespace(state=None, cleanup_failure=None)
    setup = SimpleNamespace(cleanup_retryable=False, cleanup_failure=None, state=None, failure=None)
    snapshot = SimpleNamespace(
        id="session-i",
        state=None,
        towel=towel,
        round5_setup=setup,
        updated_at=None,
        model_copy=lambda deep=False: SimpleNamespace(towel=towel, model_dump=lambda mode: {}),
    )
    record = SimpleNamespace(
        snapshot=snapshot,
        lock=asyncio.Lock(),
        event_log=SimpleNamespace(publish=publish),
        connection_spike_engine=SimpleNamespace(
            proxy_delete_accepted=lambda: False,
            proxy_name_for_bout=lambda bout: f"anti-demo-r5-{bout}-proxy",
        ),
    )

    await _manager()._abandon_connection_spike_cleanup_retry(record, 120)

    expected = "anti-demo-r5-session-i-proxy"
    assert expected in towel.cleanup_failure
    assert "MAY STILL BE RUNNING AND BILLING" in towel.cleanup_failure
    # One sentence, not two that can drift: the towel, the setup snapshot the
    # receipt is derived from, and `/readyz` all quote the same writer.
    assert setup.cleanup_failure == towel.cleanup_failure
    notice = round5_cleanup_owed_notice()
    assert notice is not None
    assert notice.detail == towel.cleanup_failure
    assert [event for event, _ in published] == ["towel_update"]


def test_only_one_place_in_the_tree_writes_the_leaked_proxy_sentence() -> None:
    """Two copies of a warning about money is how this project got three rates.

    ``owed_stop_sentence`` exists because the same sentence had been written
    twice within hours and the copies already disagreed. This one has three
    readers -- the towel, the Round 5 setup snapshot and ``/readyz`` -- reached
    by three different routes, which is the same shape.
    """

    phrase = "MAY STILL BE RUNNING AND BILLING"
    sources = [REPO_ROOT / "app.py", *sorted((REPO_ROOT / "server").glob("*.py"))]
    writers = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in sources
        if phrase in path.read_text(encoding="utf-8")
    ]

    assert writers == ["server/round5_cleanup_owed.py"]


def _readyz(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Render ``/readyz`` against a ring that is genuinely healthy."""

    state = app_module.app.state
    monkeypatch.setattr(
        state,
        "readiness_gate",
        SimpleNamespace(
            status=SimpleNamespace(
                ring_ready=True,
                maintenance_state="ready",
                maintenance_detail=None,
            )
        ),
        raising=False,
    )
    monkeypatch.setattr(state, "coordination_mode", "lakebase", raising=False)
    monkeypatch.setattr(state, "readiness_verified", True, raising=False)
    return cast(dict[str, Any], json.loads(app_module._readiness_response().body))


def test_readyz_names_a_proxy_cleanup_could_not_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The surface the live incident had nothing on.

    ``/readyz`` reported ``status: ready, degraded: false`` for the whole twenty
    minutes an RDS Proxy sat ``available``. It was not lying -- the ring really
    could serve -- it was answering a narrower question than the one being
    asked, and a surface that reports health must name what it checked.
    """

    clean = _readyz(monkeypatch)
    assert clean["status"] == "ready"
    assert clean["round5_cleanup_owed"] is False
    assert clean["round5_cleanup_owed_since"] is None
    assert clean["round5_cleanup_owed_detail"] is None

    record_round5_cleanup_owed(
        "session-g",
        resource="anti-demo-r5-0123456789abcdef-proxy",
        attempts=120,
        still_retrying=False,
    )
    leaking = _readyz(monkeypatch)

    assert leaking["round5_cleanup_owed"] is True
    assert leaking["round5_cleanup_owed_since"]
    detail = leaking["round5_cleanup_owed_detail"]
    assert "anti-demo-r5-0123456789abcdef-proxy" in detail
    assert "MAY STILL BE RUNNING AND BILLING" in detail


def test_a_leaked_proxy_does_not_lower_the_field_checked_before_a_demo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spend is not availability, held to by comparing whole payloads.

    ``_apply_startup_reap`` and ``_apply_owed_pipeline_stop`` both record this
    rule: a money problem that stops no round must not lower ``status``, because
    the field an operator checks before a demo is only worth checking if it
    means one thing. Asserting the three new keys are the *only* difference is
    what stops a later edit reaching for ``degraded_detail`` because the sentence
    felt important enough.
    """

    clean = _readyz(monkeypatch)
    record_round5_cleanup_owed(
        "session-h",
        resource="anti-demo-r5-cafe-proxy",
        attempts=120,
        still_retrying=False,
    )
    leaking = _readyz(monkeypatch)

    assert leaking["status"] == clean["status"] == "ready"
    assert leaking["degraded"] is False
    assert leaking["degraded_detail"] is None
    assert leaking["degraded_capabilities"] == []

    changed = {key for key in leaking if leaking[key] != clean.get(key)}
    assert changed == {
        "round5_cleanup_owed",
        "round5_cleanup_owed_since",
        "round5_cleanup_owed_detail",
    }
