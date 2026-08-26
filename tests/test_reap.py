"""Startup reap tests, weighted deliberately towards the refusals.

Deleting the right orphan saves a few dollars a day. Deleting the wrong thing
destroys live infrastructure belonging to somebody else's in-flight bout, and
this installation's isolation model explicitly promises that cannot happen:
rounds run independently, the same round is locked, several app installs share a
workspace without colliding, and several users share one install. So each of the
six ways the sweep could break that promise -- a held lease, a foreign run ID, a
resident, a live bout on the same round, a second process on the same install,
and a reconciliation that could not answer -- gets its own test asserting that
*nothing at all* was deleted.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import botocore.session
import pytest
from botocore.stub import Stubber

from server.aws_auth import AwsAuthConfigurationError
from server.coordination import RING_KEY, round_ring_key
from server.manifest import DemoManifest
from server.models import RoundId
from server.reap import (
    ACTION_DELETED,
    ACTION_REFUSED,
    ACTION_WOULD_DELETE,
    DEFAULT_MIN_AGE_SECONDS,
    MODE_DELETE,
    MODE_ENV,
    MODE_OFF,
    MODE_REPORT,
    REAP_BROKEN,
    REAP_SWEPT,
    REAP_UNAVAILABLE,
    AwsOrphanDeleter,
    ReapReport,
    plan_reap,
    predecessor_verdict,
    reap_health,
    reap_mode,
    reap_startup_orphans,
)
from server.reconcile import (
    AURORA_CLUSTER,
    AURORA_WRITER,
    EC2_RUNNER,
    RDS_INSTANCE,
    ObservedResource,
    reconcile,
)

RUN_ID = "ad-20260820-1446-abcd"
OTHER_RUN = "ad-20260819-0009-dcba"
INSTALLATION_ID = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"
NOW = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
OLD = NOW - timedelta(hours=1)
YOUNG = NOW - timedelta(minutes=3)

ROUND2_CLUSTER = f"adsc-{RUN_ID}-aurora"
ROUND2_WRITER = f"adsc-{RUN_ID}-aurora-writer"
ROUND3_INSTANCE = f"adrc-{RUN_ID}-rds"


def _aws_environment(number: int) -> SimpleNamespace:
    return SimpleNamespace(
        lakebase=SimpleNamespace(project_id=f"install-r{number}"),
        aurora=SimpleNamespace(
            cluster_id=f"seal-r{number}-aurora",
            writer_instance_id=f"seal-r{number}-aurora-writer",
        ),
        rds=SimpleNamespace(instance_id=f"seal-r{number}-rds"),
    )


def _manifest() -> DemoManifest:
    return DemoManifest.model_construct(
        manifest_version=7,
        installation_id=INSTALLATION_ID,
        run_id=RUN_ID,
        round_environments={
            RoundId.WAKE_IDLE_APP: _aws_environment(1),
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY: _aws_environment(2),
            RoundId.RECOVER_DELETED_ORDER: _aws_environment(3),
            RoundId.PUT_MODEL_SCORE_IN_APP: SimpleNamespace(
                lakebase=SimpleNamespace(project_id="install-r4")
            ),
            RoundId.SURVIVE_CONNECTION_SPIKE: _aws_environment(5),
            RoundId.ANALYZE_LIVE_ORDERS: SimpleNamespace(
                lakebase=SimpleNamespace(project_id="install-r6")
            ),
        },
        round5=SimpleNamespace(runner_instance_id="i-0123456789abcdef0"),
    )


def _resident() -> list[ObservedResource]:
    """The four idle opponents and the runner: standing infrastructure."""

    observed: list[ObservedResource] = []
    for number in (1, 2, 3, 5):
        observed.append(
            ObservedResource(
                AURORA_CLUSTER,
                f"seal-r{number}-aurora",
                "available",
                run_id=RUN_ID,
                created_at=OLD,
            )
        )
        observed.append(
            ObservedResource(
                AURORA_WRITER,
                f"seal-r{number}-aurora-writer",
                "available",
                run_id=RUN_ID,
                public_ipv4=True,
                created_at=OLD,
            )
        )
        observed.append(
            ObservedResource(
                RDS_INSTANCE,
                f"seal-r{number}-rds",
                "available",
                run_id=RUN_ID,
                public_ipv4=True,
                created_at=OLD,
            )
        )
    observed.append(
        ObservedResource(
            EC2_RUNNER,
            "i-0123456789abcdef0",
            "running",
            run_id=RUN_ID,
            public_ipv4=True,
            created_at=OLD,
        )
    )
    return observed


def _leaked_writer(created_at: datetime = OLD) -> ObservedResource:
    """The exact incident: a Round 2 clone writer that outlived its bout."""

    return ObservedResource(
        AURORA_WRITER,
        ROUND2_WRITER,
        "available",
        run_id=RUN_ID,
        public_ipv4=True,
        created_at=created_at,
    )


def _report(*extra: ObservedResource):
    return reconcile(_manifest(), [*_resident(), *extra])


class FakeScopedStore:
    def __init__(self, ring_key: str, held: dict[str, object], explode: bool) -> None:
        self.ring_key = ring_key
        self._held = held
        self._explode = explode

    async def current(self):
        if self._explode:
            raise RuntimeError("coordination endpoint is unreachable")
        return self._held.get(self.ring_key)


class FakeLeaseStore(FakeScopedStore):
    """A lease store whose rings can be individually held or made unreadable."""

    def __init__(
        self,
        *,
        held: dict[str, object] | None = None,
        explode: bool = False,
    ) -> None:
        super().__init__(RING_KEY, held or {}, explode)

    def for_ring_key(self, ring_key: str) -> FakeScopedStore:
        return FakeScopedStore(ring_key, self._held, self._explode)


def _lease(phase: str = "running", owner: str = "Ada") -> SimpleNamespace:
    return SimpleNamespace(
        phase=phase,
        fencing_token=7,
        owner_subject=owner.casefold(),
        operator=SimpleNamespace(display_name=owner),
    )


class RecordingDeleter:
    def __init__(self, explode: bool = False) -> None:
        self.calls: list[str] = []
        self._explode = explode

    async def delete(self, resource: ObservedResource) -> str:
        if self._explode:
            raise RuntimeError("AccessDenied: rds:DeleteDBInstance")
        self.calls.append(resource.identifier)
        return "RDS.DeleteDBInstance"


#: The two flags that make a per-bout clone actually disposable, on both RDS
#: delete APIs. A retained final snapshot or a retained automated backup is a
#: second billable thing to leak, which is the failure this whole module exists
#: to stop, so they are named once and asserted identically for cluster and
#: instance.
DISPOSABLE = {"SkipFinalSnapshot": True, "DeleteAutomatedBackups": True}


def stubbed_aws_deleter() -> tuple[AwsOrphanDeleter, Stubber]:
    """Production's ``AwsOrphanDeleter``, on a real RDS client with no wire.

    ``RecordingDeleter`` above keeps identifiers and validates nothing, so every
    delete this module issues has been unexamined: a flag flipped to ``False``,
    or a misspelled parameter name, reads as a passing test. Twice already that
    gap has cost real money -- an SSM ``TimeoutSeconds=15`` that AWS rejects
    client-side below 30, and a ``RestoreToTime``/``RestoreTime`` mix-up in
    Round 3 -- and both were found by a live run rather than by this suite.

    ``Stubber`` closes it by validating the call against the real RDS service
    model, which is the same check a genuine call performs client-side. It
    reaches no network: it answers on ``before-call``, above both the signing
    layer and the session guard in ``conftest.py``. The profile is pinned off
    for the reason ``test_recovery_live.py`` gives -- nothing here needs
    credentials, and resolving an ambient profile would fail on a machine that
    does not have it.
    """

    client = botocore.session.Session(
        session_vars={"profile": (None, None, None, None)}
    ).create_client(
        "rds",
        region_name="us-west-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    session = SimpleNamespace(client=lambda _service: client)
    return AwsOrphanDeleter(session), Stubber(client)


async def _sweep(
    report=None,
    *,
    mode: str = MODE_DELETE,
    lease_store=None,
    deleter=None,
    manifest=None,
    predecessor=(True, "no launch record for port 8000"),
    now: datetime = NOW,
    audit: bool = False,
) -> ReapReport:
    return await reap_startup_orphans(
        _manifest() if manifest is None else manifest,
        reconcile=lambda: _report(_leaked_writer()) if report is None else report,
        lease_store=FakeLeaseStore() if lease_store is None else lease_store,
        deleter=RecordingDeleter() if deleter is None else deleter,
        environ={MODE_ENV: mode},
        now=now,
        predecessor=predecessor,
        audit=audit,
    )


# --------------------------------------------------------------------------
# Mode selection
# --------------------------------------------------------------------------


def test_the_default_is_report_only_and_a_typo_disables_rather_than_arms() -> None:
    """A misspelled destructive setting must quieten the sweep, never arm it."""

    assert reap_mode({}) == MODE_REPORT
    assert reap_mode({MODE_ENV: ""}) == MODE_REPORT
    assert reap_mode({MODE_ENV: "delete"}) == MODE_DELETE
    assert reap_mode({MODE_ENV: "DELETE"}) == MODE_DELETE
    assert reap_mode({MODE_ENV: "report"}) == MODE_REPORT
    assert reap_mode({MODE_ENV: "off"}) == MODE_OFF
    assert reap_mode({MODE_ENV: "delte"}) == MODE_OFF
    assert reap_mode({MODE_ENV: "please"}) == MODE_OFF


async def test_disabled_mode_never_even_reconciles() -> None:
    """Off means off: no account read, no lease read, no deletion."""

    def explode():  # pragma: no cover - proving it is never called
        raise AssertionError("reconciliation must not run when the sweep is disabled")

    deleter = RecordingDeleter()
    outcome = await reap_startup_orphans(
        _manifest(),
        reconcile=explode,
        lease_store=FakeLeaseStore(),
        deleter=deleter,
        environ={MODE_ENV: "off"},
        audit=False,
    )

    assert outcome.mode == MODE_OFF
    assert not outcome.ran
    assert deleter.calls == []
    assert "disabled" in outcome.summary()


# --------------------------------------------------------------------------
# The one thing it is allowed to delete
# --------------------------------------------------------------------------


async def test_report_only_names_the_orphan_and_deletes_nothing() -> None:
    deleter = RecordingDeleter()

    outcome = await _sweep(mode=MODE_REPORT, deleter=deleter)

    assert deleter.calls == []
    decision = outcome.decisions[0]
    assert decision.action == ACTION_WOULD_DELETE
    assert decision.identifier == ROUND2_WRITER
    assert decision.round_key == "make_schema_change_safely"
    assert MODE_ENV in decision.reason
    assert outcome.reclaimed_usd_per_day == 0


async def test_delete_mode_reaps_only_the_leaked_clone_and_logs_it_loudly() -> None:
    """Identity, age and carrying cost all have to appear in the line."""

    deleter = RecordingDeleter()

    outcome = await _sweep(mode=MODE_DELETE, deleter=deleter)

    assert deleter.calls == [ROUND2_WRITER]
    decision = outcome.decisions[0]
    assert decision.action == ACTION_DELETED
    assert decision.age_seconds == pytest.approx(3600)
    # 2 ACU * $0.12 * 24h plus one chargeable address at $0.005 * 24h.
    assert decision.usd_per_day == Decimal("5.88")
    line = decision.line()
    assert ROUND2_WRITER in line
    assert "60.0 min old" in line
    assert "$5.8800/day" in line
    assert outcome.reclaimed_usd_per_day == decision.usd_per_day


async def test_a_leaked_writer_is_deleted_before_its_cluster() -> None:
    """A cluster cannot go while its writer holds it open.

    Driven through the real ``AwsOrphanDeleter``, so the one test that pins the
    order also pins the shape of all three calls. ``Stubber`` consumes its
    queued responses in sequence and asserts the exact parameters of each, which
    is what makes those two claims a single test: a reordered sweep and a
    ``SkipFinalSnapshot`` flipped to ``False`` both fail here.
    """

    deleter, stubber = stubbed_aws_deleter()
    report = _report(
        ObservedResource(
            AURORA_CLUSTER, ROUND2_CLUSTER, "available", run_id=RUN_ID, created_at=OLD
        ),
        _leaked_writer(),
        ObservedResource(
            RDS_INSTANCE,
            ROUND3_INSTANCE,
            "available",
            run_id=RUN_ID,
            public_ipv4=True,
            created_at=OLD,
        ),
    )

    with stubber:
        stubber.add_response(
            "delete_db_instance", {}, {"DBInstanceIdentifier": ROUND2_WRITER, **DISPOSABLE}
        )
        stubber.add_response(
            "delete_db_instance", {}, {"DBInstanceIdentifier": ROUND3_INSTANCE, **DISPOSABLE}
        )
        stubber.add_response(
            "delete_db_cluster", {}, {"DBClusterIdentifier": ROUND2_CLUSTER, **DISPOSABLE}
        )
        outcome = await _sweep(report, deleter=deleter)

    # The sweep turns a failed delete into a refusal carrying the reason, so
    # the refusals are read first: that puts botocore's parameter diff in the
    # failure message rather than leaving an unexplained short list below.
    assert [decision.reason for decision in outcome.refused] == []
    assert [decision.identifier for decision in outcome.deleted] == [
        ROUND2_WRITER,
        ROUND3_INSTANCE,
        ROUND2_CLUSTER,
    ]
    stubber.assert_no_pending_responses()


async def test_a_healthy_installation_produces_no_decisions_at_all() -> None:
    deleter = RecordingDeleter()

    outcome = await _sweep(_report(), deleter=deleter)

    assert outcome.ran
    assert outcome.decisions == ()
    assert deleter.calls == []
    assert "no ephemeral orphans" in outcome.summary()


# --------------------------------------------------------------------------
# Refusals. Every one of these must delete nothing.
# --------------------------------------------------------------------------


async def test_a_held_lease_on_the_round_stops_the_reap() -> None:
    """A live bout on the same round: its writer must survive underneath it."""

    deleter = RecordingDeleter()
    ring = round_ring_key(INSTALLATION_ID, "make_schema_change_safely")
    store = FakeLeaseStore(held={ring: _lease(phase="running", owner="Grace")})

    outcome = await _sweep(lease_store=store, deleter=deleter)

    assert deleter.calls == []
    decision = outcome.decisions[0]
    assert decision.action == ACTION_REFUSED
    assert "a bout may still be running" in decision.reason
    assert "GRACE" in decision.reason.upper()
    assert ring in decision.reason


async def test_a_held_main_ring_also_stops_the_reap() -> None:
    """Startup cleanup claims the main ring; that is a bout in progress too."""

    deleter = RecordingDeleter()
    store = FakeLeaseStore(held={RING_KEY: _lease(phase="startup_cleanup")})

    outcome = await _sweep(lease_store=store, deleter=deleter)

    assert deleter.calls == []
    assert outcome.decisions[0].action == ACTION_REFUSED
    assert "startup_cleanup" in outcome.decisions[0].reason


async def test_an_unreadable_lease_store_refuses_rather_than_assuming_free() -> None:
    """Not knowing whether a bout holds the ring is not the same as it being free."""

    deleter = RecordingDeleter()

    outcome = await _sweep(lease_store=FakeLeaseStore(explode=True), deleter=deleter)

    assert deleter.calls == []
    decision = outcome.decisions[0]
    assert decision.action == ACTION_REFUSED
    assert "ring lease could not be read" in decision.reason


async def test_a_missing_lease_store_refuses_before_reading_the_account() -> None:
    deleter = RecordingDeleter()

    outcome = await reap_startup_orphans(
        _manifest(),
        reconcile=lambda: _report(_leaked_writer()),
        lease_store=None,
        deleter=deleter,
        environ={MODE_ENV: MODE_DELETE},
        now=NOW,
        predecessor=(True, "alone"),
        audit=False,
    )

    assert deleter.calls == []
    assert not outcome.ran
    assert "held bout could not be ruled out" in outcome.unavailable


async def test_foreign_run_residue_is_reported_and_never_touched() -> None:
    """Another installation's resources cost money but are not ours to delete."""

    deleter = RecordingDeleter()
    report = _report(
        ObservedResource(
            RDS_INSTANCE,
            f"lakebase-anti-demo-{OTHER_RUN}-rds",
            "available",
            run_id=OTHER_RUN,
            public_ipv4=True,
            instance_class="db.t4g.micro",
            created_at=OLD,
        )
    )

    outcome = await _sweep(report, deleter=deleter)

    assert deleter.calls == []
    decision = outcome.decisions[0]
    assert decision.action == ACTION_REFUSED
    assert "ORPHAN_FOREIGN_RUN is report-only" in decision.reason


async def test_an_unexpected_tagged_resource_is_reported_and_never_touched() -> None:
    deleter = RecordingDeleter()
    report = _report(
        ObservedResource(
            RDS_INSTANCE, "hand-made-experiment", "available", run_id=RUN_ID, created_at=OLD
        )
    )

    outcome = await _sweep(report, deleter=deleter)

    assert deleter.calls == []
    assert "ORPHAN_UNEXPECTED is report-only" in outcome.decisions[0].reason


async def test_a_resident_is_never_reaped_even_if_it_is_named_like_a_clone() -> None:
    """Deleting a standing opponent breaks the next demo and costs ten minutes."""

    manifest = DemoManifest.model_construct(
        manifest_version=7,
        installation_id=INSTALLATION_ID,
        run_id=RUN_ID,
        # A seal that (pathologically) claims the Round 2 clone name as resident.
        round_environments={
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY: SimpleNamespace(
                lakebase=SimpleNamespace(project_id="install-r2"),
                aurora=SimpleNamespace(
                    cluster_id=ROUND2_CLUSTER, writer_instance_id=ROUND2_WRITER
                ),
                rds=None,
            )
        },
        round5=None,
    )
    report = reconcile(
        manifest,
        [
            ObservedResource(
                AURORA_WRITER,
                ROUND2_WRITER,
                "available",
                run_id=RUN_ID,
                created_at=OLD,
            )
        ],
    )
    candidates, refusals = plan_reap(
        manifest, report, now=NOW, minimum_age=DEFAULT_MIN_AGE_SECONDS
    )

    assert candidates == []
    assert all(decision.action == ACTION_REFUSED for decision in refusals)


async def test_the_standing_runner_is_excluded_by_kind_not_only_by_the_seal() -> None:
    """Even an EC2 machine tagged like a clone is out of reach here."""

    manifest = DemoManifest.model_construct(
        manifest_version=7,
        installation_id=INSTALLATION_ID,
        run_id=RUN_ID,
        round_environments={},
        round5=None,
    )
    report = reconcile(
        manifest,
        [
            ObservedResource(
                EC2_RUNNER,
                f"adsc-{RUN_ID}-aurora",
                "running",
                run_id=RUN_ID,
                created_at=OLD,
            )
        ],
    )

    candidates, refusals = plan_reap(
        manifest, report, now=NOW, minimum_age=DEFAULT_MIN_AGE_SECONDS
    )

    assert candidates == []
    assert "standing infrastructure" in refusals[0].reason


async def test_a_second_process_on_this_install_stops_the_reap() -> None:
    """A --reload restart or a second replica must not reap a live writer."""

    deleter = RecordingDeleter()

    outcome = await _sweep(
        deleter=deleter,
        predecessor=(False, "another process may be serving port 8000: PID 4242 IS SERVING"),
    )

    assert deleter.calls == []
    decision = outcome.decisions[0]
    assert decision.action == ACTION_REFUSED
    assert "another process may be serving" in decision.reason


async def test_a_reconciliation_error_deletes_nothing_and_says_why() -> None:
    """Half an account read looks exactly like an account with fewer resources."""

    deleter = RecordingDeleter()

    def unreachable(_manifest):
        raise RuntimeError("ExpiredToken: the security token has expired")

    from server.reconcile import reconcile_live

    outcome = await reap_startup_orphans(
        _manifest(),
        reconcile=lambda: reconcile_live(_manifest(), unreachable),
        lease_store=FakeLeaseStore(),
        deleter=deleter,
        environ={MODE_ENV: MODE_DELETE},
        now=NOW,
        predecessor=(True, "alone"),
        audit=False,
    )

    assert deleter.calls == []
    assert not outcome.ran
    assert "ExpiredToken" in outcome.unavailable
    assert "reconciliation unavailable" in outcome.summary()


async def test_a_clone_younger_than_a_bout_is_left_alone() -> None:
    """Existing is not proof the owning bout is over; age plus a free ring is.

    Both sides of the floor are asserted, because the floor *is* the rule and
    which side of it is inclusive is the whole of its content. Under the floor
    a live bout may still own the clone, so refusing is the only safe answer.
    Exactly on it the bout is provably over -- fifteen minutes against a
    six-minute bout is already all the margin there is -- so refusing there
    would leave a leaked writer billing on and call it caution.
    """

    deleter = RecordingDeleter()

    outcome = await _sweep(_report(_leaked_writer(YOUNG)), deleter=deleter)

    assert deleter.calls == []
    decision = outcome.decisions[0]
    assert decision.action == ACTION_REFUSED
    assert "3.0 min old against a 15.0 min floor" in decision.reason

    on_the_floor = RecordingDeleter()
    exactly_old_enough = NOW - timedelta(seconds=DEFAULT_MIN_AGE_SECONDS)

    outcome = await _sweep(_report(_leaked_writer(exactly_old_enough)), deleter=on_the_floor)

    assert on_the_floor.calls == [ROUND2_WRITER]
    decision = outcome.decisions[0]
    assert decision.action == ACTION_DELETED
    assert decision.age_seconds == pytest.approx(DEFAULT_MIN_AGE_SECONDS)


async def test_a_clone_whose_age_cannot_be_read_is_left_alone() -> None:
    deleter = RecordingDeleter()
    ageless = ObservedResource(
        AURORA_WRITER, ROUND2_WRITER, "available", run_id=RUN_ID, created_at=None
    )

    outcome = await _sweep(_report(ageless), deleter=deleter)

    assert deleter.calls == []
    assert "creation time is unreadable" in outcome.decisions[0].reason


async def test_a_clone_already_deleting_is_not_deleted_again() -> None:
    deleter = RecordingDeleter()
    retiring = ObservedResource(
        AURORA_WRITER, ROUND2_WRITER, "deleting", run_id=RUN_ID, created_at=OLD
    )

    outcome = await _sweep(_report(retiring), deleter=deleter)

    assert deleter.calls == []
    assert outcome.decisions == ()


async def test_a_failed_delete_is_recorded_and_does_not_abort_the_rest() -> None:
    deleter = RecordingDeleter(explode=True)

    outcome = await _sweep(deleter=deleter)

    assert deleter.calls == []
    decision = outcome.decisions[0]
    assert decision.action == ACTION_REFUSED
    assert "AccessDenied" in decision.reason
    assert outcome.reclaimed_usd_per_day == 0


# --------------------------------------------------------------------------
# Fail safe
# --------------------------------------------------------------------------


async def test_the_sweep_never_raises_however_broken_its_inputs_are() -> None:
    """Cleanup must never become the reason a server fails to start."""

    def detonate():
        raise MemoryError("everything is on fire")

    outcome = await reap_startup_orphans(
        None,
        reconcile=detonate,
        lease_store=None,
        environ={MODE_ENV: MODE_DELETE},
        audit=False,
    )

    assert not outcome.ran
    assert "no owned manifest" in outcome.unavailable

    outcome = await reap_startup_orphans(
        _manifest(),
        reconcile=detonate,
        lease_store=FakeLeaseStore(),
        environ={MODE_ENV: MODE_DELETE},
        audit=False,
    )

    assert not outcome.ran
    assert "MemoryError" in outcome.unavailable


@pytest.mark.parametrize(
    ("failure", "state"),
    [
        (
            AwsAuthConfigurationError("AWS_PROFILE is required in profile mode"),
            REAP_UNAVAILABLE,
        ),
        (AttributeError("'SimpleNamespace' object has no attribute 'aws'"), REAP_BROKEN),
    ],
    ids=["could_not_look", "the_safety_net_is_broken"],
)
async def test_a_failing_reap_still_lets_the_server_start(
    monkeypatch,
    caplog,
    tmp_path,
    failure: BaseException,
    state: str,
) -> None:
    """The whole point of running at startup is that startup still happens.

    Fail-soft is the easy half and was never broken. The hard half is that a
    sweep which could not run says so *somewhere a person looks*, and says which
    kind of failure it was: an expired session means the guard could not look and
    is expected on this install, whereas anything else means the guard against
    leaked billable resources is broken. One `except BaseException` logging
    "Startup orphan reap could not run" made those indistinguishable, and the
    report it built was thrown away because nothing read `app.state.startup_reap`.

    The two arms differ in exactly two places -- the log level and the reported
    state -- and asserting them together is what stops the distinction being
    quietly collapsed back into one sentence. Neither degrades the box: that is
    asserted too, because a spend fault borrowing the field an operator checks
    before a demo would be the opposite kind of dishonesty.
    """

    import app as app_module
    from server import lifecycle

    broken = state == REAP_BROKEN

    def detonate(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr(lifecycle, "_aws_session", detonate)
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / "manifest.json"))
    caplog.set_level(logging.DEBUG, logger="app")

    report = await app_module._reap_startup_orphans(_manifest(), FakeLeaseStore())

    # Fail-soft: a returned report, not an exception, so startup continues.
    assert isinstance(report, ReapReport)
    assert not report.ran
    assert report.failed
    assert report.broken is broken
    # Named, so the two causes are not one indistinguishable sentence.
    assert type(failure).__name__ in report.unavailable
    assert report.run_id == RUN_ID
    levels = {record.levelno for record in caplog.records if "REAP" in record.getMessage()}
    assert levels, "a sweep that did not run must say so at all"
    assert (logging.ERROR in levels) is broken

    # Visible outside the log, and the count is null rather than a zero from a
    # sweep that never looked. Read from an otherwise-healthy box, because that
    # is the only state in which "does this degrade?" has a visible answer.
    payload = _readyz_on_a_healthy_box(monkeypatch, app_module, report)

    assert payload["startup_reap_state"] == state
    assert payload["startup_reap_observed_orphans"] is None
    assert type(failure).__name__ in payload["startup_reap_detail"]
    # Reported, never conflated with a functional fault: the sweep failing stops
    # no round, and lowering the field an operator checks before a demo for a
    # spend problem would make that field less trustworthy, not more.
    assert payload["status"] == "ready"
    assert payload["degraded_detail"] is None

    # And durable, because `/readyz` cannot answer for a process that has died.
    # These two failures happen before the sweep's own audit is reached, so they
    # were the only outcomes with no line at all.
    journal = json.loads((tmp_path / "startup-reap.jsonl").read_text().splitlines()[-1])
    assert journal["failed"] is True
    assert journal["broken"] is broken
    assert journal["ran"] is False
    assert journal["observed_orphans"] is None
    assert type(failure).__name__ in journal["unavailable"]


def _readyz_on_a_healthy_box(monkeypatch, app_module, report: ReapReport | None) -> dict:
    """Render `/readyz` for a durable, verified, settled process.

    A default test app is already degraded on its coordination mode, which would
    hide whether the sweep changed anything. This is the state a demo actually
    runs in, and therefore the one in which "does this degrade?" has a visible
    answer.
    """

    state = app_module.app.state
    monkeypatch.setattr(state, "coordination_mode", "lakebase", raising=False)
    monkeypatch.setattr(state, "readiness_verified", True, raising=False)
    monkeypatch.setattr(state, "credential_sentry", None, raising=False)
    monkeypatch.setattr(state, "restart_history", None, raising=False)
    monkeypatch.setattr(state, "startup_reap", report, raising=False)
    monkeypatch.setattr(
        state,
        "readiness_gate",
        SimpleNamespace(
            status=SimpleNamespace(
                ring_ready=True, maintenance_state="ready", maintenance_detail=None
            ),
            recovery=None,
            round5_recovery=None,
        ),
        raising=False,
    )
    return json.loads(app_module._readiness_response().body)


async def test_a_sweep_that_looked_and_found_nothing_is_not_reported_as_a_failure(
    monkeypatch,
) -> None:
    """The distinction the whole signal exists for, asserted in both directions.

    `ran: true, observed_orphans: 0` is the only combination that may report a
    clean account. The audit record and `/readyz` must both refuse to write a
    zero for a sweep that could not reach the account, because a consumer
    reading the count without the flag would take one for the other -- which is
    exactly what the 03:39Z record in `startup-reap.jsonl` invited.
    """

    import app as app_module

    swept = await reap_startup_orphans(
        _manifest(),
        reconcile=lambda: _report(),
        lease_store=FakeLeaseStore(),
        predecessor=(True, "no launch record for port 8000"),
        now=NOW,
        audit=False,
    )

    assert swept.ran
    assert swept.as_dict()["observed_orphans"] == 0
    assert reap_health(swept).state == REAP_SWEPT

    blind = ReapReport(mode=MODE_REPORT, unavailable="ExpiredToken", failed=True)

    assert blind.as_dict()["observed_orphans"] is None
    assert reap_health(blind).observed_orphans is None
    assert reap_health(blind).state == REAP_UNAVAILABLE

    payload = _readyz_on_a_healthy_box(monkeypatch, app_module, swept)

    assert payload["startup_reap_state"] == REAP_SWEPT
    assert payload["startup_reap_observed_orphans"] == 0
    assert payload["status"] == "ready"
    assert payload["degraded_detail"] is None


# --------------------------------------------------------------------------
# Observability
# --------------------------------------------------------------------------


async def test_the_sweep_leaves_a_durable_audit_line(tmp_path, monkeypatch) -> None:
    """An operator has to be able to see what happened after the fact."""

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text("{}")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(manifest_file))

    outcome = await reap_startup_orphans(
        _manifest(),
        reconcile=lambda: _report(_leaked_writer()),
        lease_store=FakeLeaseStore(),
        deleter=RecordingDeleter(),
        environ={MODE_ENV: MODE_DELETE},
        now=NOW,
        predecessor=(True, "no launch record for port 8000"),
        state_dir=tmp_path,
        audit=True,
    )

    written = json.loads((tmp_path / "startup-reap.jsonl").read_text().strip())
    assert written["mode"] == MODE_DELETE
    assert written["run_id"] == RUN_ID
    assert written["ran"] is True
    assert written["decisions"][0]["identifier"] == ROUND2_WRITER
    assert written["decisions"][0]["action"] == ACTION_DELETED
    assert written["reclaimed_usd_per_day"] == "5.880000"
    assert outcome.deleted


async def test_a_process_with_no_owned_manifest_journals_nothing(tmp_path) -> None:
    """The ordinary non-event must not bury the entries that matter.

    Every startup of a server that owns no manifest reaches this path. Writing a
    line each time filled the live state directory with identical "nothing to do"
    records, which is how a log stops being read.
    """

    outcome = await reap_startup_orphans(
        None,
        reconcile=lambda: _report(),
        lease_store=FakeLeaseStore(),
        environ={MODE_ENV: MODE_DELETE},
        state_dir=tmp_path,
        audit=True,
    )

    assert not outcome.notable
    assert not any(tmp_path.iterdir())

    # A sweep that genuinely failed is the opposite: always recorded.
    def unreachable(_manifest):
        raise RuntimeError("ExpiredToken")

    from server.reconcile import reconcile_live

    failure = await reap_startup_orphans(
        _manifest(),
        reconcile=lambda: reconcile_live(_manifest(), unreachable),
        lease_store=FakeLeaseStore(),
        environ={MODE_ENV: MODE_DELETE},
        state_dir=tmp_path,
        audit=True,
    )

    assert failure.notable
    assert json.loads((tmp_path / "startup-reap.jsonl").read_text().strip())["failed"] is True


def test_only_an_absent_or_exited_record_proves_this_process_is_alone(tmp_path) -> None:
    alone, detail = predecessor_verdict(
        state_dir=tmp_path, argv=["uvicorn", "app:app"], environ={}
    )
    assert alone
    assert "no launch record" in detail

    (tmp_path / "server-8000.pid").write_text("4242\n")
    (tmp_path / "server-8000.launch.json").write_text(
        json.dumps({"pid": 4242, "port": 8000, "identity_tokens": ["app:app"]})
    )

    alone, detail = predecessor_verdict(
        state_dir=tmp_path,
        argv=["uvicorn", "app:app"],
        environ={},
        pid_is_alive=lambda _pid: False,
    )
    assert alone
    assert "has exited" in detail

    alone, detail = predecessor_verdict(
        state_dir=tmp_path,
        argv=["uvicorn", "app:app"],
        environ={},
        pid_is_alive=lambda _pid: True,
        command_line=lambda _pid: "uvicorn app:app --port 8000",
    )
    assert not alone
    assert "another process may be serving" in detail

    # Alive but unidentifiable is not proof of absence either.
    alone, detail = predecessor_verdict(
        state_dir=tmp_path,
        argv=["uvicorn", "app:app"],
        environ={},
        pid_is_alive=lambda _pid: True,
        command_line=lambda _pid: None,
    )
    assert not alone


def test_no_state_directory_means_the_sweep_cannot_prove_it_is_alone() -> None:
    alone, detail = predecessor_verdict(environ={})

    assert not alone
    assert "no state directory" in detail
