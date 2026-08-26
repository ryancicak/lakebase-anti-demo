"""Recovering from the sandbox reaper, and noticing that it ran.

The AWS sandbox this was developed in is swept by its own account automation every
fourteen days, which deletes the Aurora clusters, the RDS instances and the IAM
users with no final snapshot. Most accounts do not do this; this one does.
So the recovery path is exercised on a schedule, and two things about it were
broken in ways that only show up on the day it matters.

**The deadlock.** `antidemo setup` on a swept install takes the `ready` branch, which
runs `reconcile_infrastructure` -> `_refresh_operator_cidr`. That refused to
rebind the operator allowance unless `_aws_ownership` passed -- and ownership is
established by describing the very databases the reaper deleted. Over a fortnight
a laptop address changing is likely rather than exotic, so "my address moved and
my resources are gone" is the *common* recovery case, and it was a hard refusal:
the repair was gated on the thing that was broken.

**The blind spot.** `MISSING_RESIDENT` was computed and thrown away -- the only
call site routed it through `plan_reap`, which records nothing that is not an
orphan. Nothing an operator could run said the infrastructure had vanished.

The inversion that makes the second half subtle: a real sweep takes the IAM users
too, so the account cannot be read at all and the honest answer is "could not
look", not "gone". Detection keyed on `missing` alone would stay silent through
the only event it exists for. Both answers are surfaced here, and they are
required to read differently, because inviting a ~$35-40/day re-provision on the
strength of a failed credential is the expensive direction to be wrong in.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient
from test_api import _deployed_app_runtime, deployed_manifest
from test_lifecycle import make_manifest

import app as app_module
from app import app
from server import cli as cli_module
from server import lifecycle
from server.lifecycle import Check
from server.reconcile import (
    AURORA_CLUSTER,
    MISSING_RESIDENT,
    PRESENCE_MISSING,
    PRESENCE_NEVER_CHECKED,
    PRESENCE_PRESENT,
    PRESENCE_UNVERIFIED,
    RDS_INSTANCE,
    ExpectedResource,
    Finding,
    ReconciliationReport,
    presence_from_report,
)

SEALED = "203.0.113.10/32"
MOVED = "198.51.100.7/32"


class DBClusterNotFoundFault(Exception):
    """Named as botocore names it, which is how the classifier recognises it."""


class DBInstanceNotFoundFault(Exception):
    pass


def _client_error(code: str) -> Exception:
    """A botocore-shaped error, to prove the response payload is read too."""

    error = Exception(code)
    error.response = {"Error": {"Code": code}}
    return error


class _Rds:
    """A stand-in RDS client scripted per identifier.

    `answers` maps an identifier to either an exception to raise or a payload to
    return, so one fake covers gone, present, and unreadable.
    """

    def __init__(self, answers: dict[str, object], default: object) -> None:
        self.answers = answers
        self.default = default
        self.looked_up: list[str] = []

    def _answer(self, identifier: str) -> object:
        self.looked_up.append(identifier)
        answer = self.answers.get(identifier, self.default)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def describe_db_clusters(self, *, DBClusterIdentifier: str) -> object:
        return self._answer(DBClusterIdentifier)

    def describe_db_instances(self, *, DBInstanceIdentifier: str) -> object:
        return self._answer(DBInstanceIdentifier)


def _arm_rds(monkeypatch, answers: dict[str, object], default: object) -> _Rds:
    rds = _Rds(answers, default)
    monkeypatch.setattr(
        lifecycle,
        "_aws_session",
        lambda _manifest: SimpleNamespace(client=lambda _name: rds),
    )
    return rds


ALL_GONE = {
    "anti-demo-aurora": DBClusterNotFoundFault("gone"),
    "anti-demo-aurora-writer": DBInstanceNotFoundFault("gone"),
    "anti-demo-rds": DBInstanceNotFoundFault("gone"),
}


# ---------------------------------------------------------------------------
# Defect 1 -- the deadlock
# ---------------------------------------------------------------------------


def test_a_moved_address_no_longer_refuses_when_the_databases_are_gone(
    monkeypatch,
) -> None:
    """The deadlock itself: the reap case must rebind rather than refuse.

    The refusal protects an ingress rule on a resource that might not be ours.
    Once AWS says the sealed databases do not exist there is no rule left to
    protect, so the property it defends has already evaporated and holding the
    refusal only strands the install.
    """

    manifest = make_manifest()
    saved: list[str] = []
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda: MOVED)
    monkeypatch.setattr(
        lifecycle,
        "_aws_ownership",
        lambda _candidate: Check("aws_ownership", False, "could not validate"),
    )
    _arm_rds(monkeypatch, ALL_GONE, default=DBInstanceNotFoundFault("gone"))
    monkeypatch.setattr(
        lifecycle, "save_manifest", lambda candidate: saved.append(candidate.aws.operator_cidr)
    )

    lifecycle._refresh_operator_cidr(manifest)

    assert manifest.aws.operator_cidr == MOVED
    assert saved == [MOVED]


def test_a_moved_address_still_refuses_when_the_resources_belong_to_someone_else(
    monkeypatch,
) -> None:
    """The guard that must survive, unchanged.

    The databases are there and their tags disagree with the seal, so they may be
    somebody else's. Rebinding would point this installation's allowance at a
    resource it does not own and let the following Terraform apply rewrite a
    stranger's security group.
    """

    manifest = make_manifest()
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda: MOVED)
    monkeypatch.setattr(
        lifecycle,
        "_aws_ownership",
        lambda _candidate: Check("aws_ownership", False, "tag mismatch on anti-demo-aurora"),
    )
    _arm_rds(monkeypatch, {}, default={"DBInstances": [{}]})
    monkeypatch.setattr(
        lifecycle, "save_manifest", lambda _candidate: pytest.fail("must not rebind")
    )

    with pytest.raises(RuntimeError, match="Refusing to change database ingress"):
        lifecycle._refresh_operator_cidr(manifest)

    assert manifest.aws.operator_cidr == SEALED


@pytest.mark.parametrize(
    "failure",
    [
        _client_error("ExpiredToken"),
        _client_error("AccessDenied"),
        _client_error("Throttling"),
        OSError("no route to host"),
    ],
    ids=["expired", "denied", "throttled", "offline"],
)
def test_a_moved_address_still_refuses_when_the_account_cannot_be_read(
    monkeypatch, failure
) -> None:
    """A failure to look must never read as a confirmed absence.

    This is the inversion that makes the whole feature subtle: the reaper deletes
    the IAM users too, so the commonest way to reach this code is with
    credentials that no longer work. Treating that as "gone" would rebind the
    allowance on no evidence at all.
    """

    manifest = make_manifest()
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda: MOVED)
    monkeypatch.setattr(
        lifecycle,
        "_aws_ownership",
        lambda _candidate: Check("aws_ownership", False, "could not validate"),
    )
    _arm_rds(monkeypatch, {}, default=failure)
    monkeypatch.setattr(
        lifecycle, "save_manifest", lambda _candidate: pytest.fail("must not rebind")
    )

    with pytest.raises(RuntimeError, match="Refusing to change database ingress"):
        lifecycle._refresh_operator_cidr(manifest)

    assert manifest.aws.operator_cidr == SEALED


def test_a_verified_owner_still_rebinds_without_an_absence_probe(monkeypatch) -> None:
    """The healthy repair path is untouched, and costs no extra describes."""

    manifest = make_manifest()
    saved: list[str] = []
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda: MOVED)
    monkeypatch.setattr(
        lifecycle, "_aws_ownership", lambda _candidate: Check("aws_ownership", True, "owned")
    )
    monkeypatch.setattr(
        lifecycle,
        "_sealed_databases_absent",
        lambda _candidate: pytest.fail("a verified owner must not be probed for absence"),
    )
    monkeypatch.setattr(
        lifecycle, "save_manifest", lambda candidate: saved.append(candidate.aws.operator_cidr)
    )

    lifecycle._refresh_operator_cidr(manifest)

    assert saved == [MOVED]


def test_one_surviving_database_is_not_an_absence(monkeypatch) -> None:
    """Every sealed identifier must be gone, not merely one.

    A partial deletion is genuinely ambiguous and deserves an operator's eyes; a
    reap is unambiguous and is the case this unblocks.
    """

    manifest = make_manifest()
    _arm_rds(
        monkeypatch,
        {**ALL_GONE, "anti-demo-rds": {"DBInstances": [{}]}},
        default=DBInstanceNotFoundFault("gone"),
    )

    assert lifecycle._sealed_databases_absent(manifest) is False


def test_absence_is_recognised_from_the_botocore_error_payload(monkeypatch) -> None:
    """Real botocore raises a generated class *and* carries the code; read both."""

    manifest = make_manifest()
    _arm_rds(monkeypatch, {}, default=_client_error("DBInstanceNotFoundFault"))

    assert lifecycle._sealed_databases_absent(manifest) is True


def test_an_unbuildable_session_is_not_an_absence(monkeypatch) -> None:
    """No session, no evidence. Fail closed."""

    manifest = make_manifest()
    monkeypatch.setattr(
        lifecycle,
        "_aws_session",
        lambda _manifest: (_ for _ in ()).throw(RuntimeError("no credentials")),
    )

    assert lifecycle._sealed_databases_absent(manifest) is False


class _StopHere(Exception):
    """Reaching this is the assertion: the refusal no longer came first."""


def test_demo_setups_repair_path_now_gets_past_the_ingress_refusal(monkeypatch) -> None:
    """The deadlock proved where it actually bit: inside `reconcile_infrastructure`.

    `_refresh_operator_cidr` is called from there, *before* `reset()` is reached,
    so this refusal -- not `reset`'s own address check -- is what an operator
    recovering from a sweep hit first. Everything past the refresh is stubbed to
    raise, so arriving at the sentinel means the repair was allowed to proceed.
    """

    manifest = make_manifest()
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda: MOVED)
    monkeypatch.setattr(
        lifecycle, "_verify_databricks_identity", lambda _profile: manifest.databricks.user
    )
    monkeypatch.setattr(lifecycle, "_verify_aws_identity", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "_aws_ownership",
        lambda _candidate: Check("aws_ownership", False, "could not validate"),
    )
    _arm_rds(monkeypatch, ALL_GONE, default=DBInstanceNotFoundFault("gone"))
    monkeypatch.setattr(lifecycle, "save_manifest", lambda _candidate: None)
    monkeypatch.setattr(
        lifecycle,
        "_terraform_init",
        lambda _candidate: (_ for _ in ()).throw(_StopHere()),
    )

    with pytest.raises(_StopHere):
        lifecycle.reconcile_infrastructure(manifest)

    # And the rebind that unblocked it is what `reset()` reads, so the second
    # address guard on the way through `antidemo setup` is satisfied too.
    assert manifest.aws.operator_cidr == MOVED


# ---------------------------------------------------------------------------
# Defect 2 -- detection
# ---------------------------------------------------------------------------


def _report(*, missing: int = 0, unavailable: str = "", sealed: int = 3) -> ReconciliationReport:
    expected = tuple(
        ExpectedResource(AURORA_CLUSTER if index == 0 else RDS_INSTANCE, f"res-{index}", "r1")
        for index in range(sealed)
    )
    findings = tuple(
        Finding(MISSING_RESIDENT, RDS_INSTANCE, f"res-{index}", "sealed but absent")
        for index in range(missing)
    )
    return ReconciliationReport(
        run_id="ad-test-001",
        expected=expected,
        findings=findings,
        unavailable=unavailable,
    )


def test_the_three_answers_are_three_different_answers() -> None:
    """Gone, could-not-look, and present must never collapse into each other."""

    gone = presence_from_report(_report(missing=3))
    blind = presence_from_report(_report(unavailable="ExpiredToken: the session lapsed"))
    present = presence_from_report(_report())
    never = presence_from_report(None)

    assert (gone.state, blind.state, present.state, never.state) == (
        PRESENCE_MISSING,
        PRESENCE_UNVERIFIED,
        PRESENCE_PRESENT,
        PRESENCE_NEVER_CHECKED,
    )
    assert "IS GONE" in gone.detail
    assert "COULD NOT BE CHECKED" in blind.detail
    assert "HAS NOT BEEN CHECKED" in never.detail
    assert "are present in the account" in present.detail
    # The load-bearing property: not-looked must not read as a loss.
    for verdict in (blind, never, present):
        assert "IS GONE" not in verdict.detail
        assert verdict.capabilities == ()
    assert gone.capabilities
    # And the reason the read failed has to reach the operator, not be swallowed.
    assert "ExpiredToken" in blind.detail
    assert (gone.checked, blind.checked, never.checked) == (True, False, False)


def test_a_missing_resident_finding_is_no_longer_discarded() -> None:
    """`MISSING_RESIDENT` is deliberately not an orphan, which is why the server's
    only call site dropped it. It has to survive as a signal of its own."""

    report = _report(missing=2, sealed=3)

    assert report.orphans == ()
    presence = presence_from_report(report)

    assert presence.verified_missing is True
    assert (presence.absent, presence.sealed) == (2, 3)
    assert "2 of 3" in presence.detail


def _arm_presence(monkeypatch, report: ReconciliationReport | None) -> None:
    """Point the real presence probe at a scripted sweep, with no AWS behind it."""

    lifecycle.reset_installation_presence_cache()
    monkeypatch.setattr(
        lifecycle, "load_manifest", lambda: SimpleNamespace(run_id="ad-test-001")
    )
    if report is None:
        monkeypatch.setattr(
            lifecycle,
            "reconcile_live",
            lambda *_args, **_kwargs: pytest.fail("must not sweep"),
        )
        return
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda *_args, **_kwargs: report)


async def _readyz(monkeypatch, manifest) -> dict:
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            return (await client.get("/readyz")).json()


async def test_readyz_says_the_infrastructure_is_gone(monkeypatch) -> None:
    """A running server can finally report the sweep.

    Still a 200: Rounds 4 and 6 are Databricks-only and need no AWS, so a 503
    would take the app out of rotation and turn a diagnosable fault into an
    outage. The point is that a monitor comparing against "ready" stops being
    reassured.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _arm_presence(monkeypatch, _report(missing=3, sealed=3))

    payload = await _readyz(monkeypatch, manifest)

    assert payload["installation_state"] == PRESENCE_MISSING
    assert payload["installation_absent_resources"] == 3
    assert payload["installation_checked"] is True
    assert payload["status"] == "degraded"
    assert payload["degraded"] is True
    assert "THE SEALED AWS INFRASTRUCTURE IS GONE" in payload["degraded_detail"]
    assert "./antidemo setup" in payload["degraded_detail"]
    assert any("Aurora or RDS" in loss for loss in payload["degraded_capabilities"])


async def test_readyz_separates_could_not_look_from_gone(monkeypatch) -> None:
    """The case a real reap actually produces, and it must not claim a loss.

    The sweep that deletes the databases deletes the IAM users, so the account
    cannot be read and the only honest answer is that nothing was established.
    Reporting that as "gone" would invite a re-provision on no evidence.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _arm_presence(monkeypatch, _report(unavailable="ExpiredToken: the session lapsed"))

    payload = await _readyz(monkeypatch, manifest)

    assert payload["installation_state"] == PRESENCE_UNVERIFIED
    assert payload["installation_checked"] is False
    assert payload["installation_absent_resources"] == 0
    assert payload["status"] == "degraded"
    assert "COULD NOT BE CHECKED" in payload["degraded_detail"]
    assert "IS GONE" not in payload["degraded_detail"]
    # Nothing is known to be lost, so nothing is claimed as lost.
    assert payload["degraded_capabilities"] == []


async def test_readyz_stays_ready_when_the_infrastructure_is_there(monkeypatch) -> None:
    """A verified-present sweep says so and degrades nothing."""

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _arm_presence(monkeypatch, _report(sealed=3))

    payload = await _readyz(monkeypatch, manifest)

    assert payload["installation_state"] == PRESENCE_PRESENT
    assert payload["installation_sealed_resources"] == 3
    assert payload["installation_checked"] is True
    assert payload["status"] == "ready"
    assert payload["degraded"] is False
    assert payload["degraded_detail"] is None


async def test_a_total_credential_fault_keeps_the_sentence_from_the_presence_signal(
    monkeypatch,
) -> None:
    """Precedence, in the case that decides whether this feature is honest.

    On a real reap both signals fire at once: credentials are rejected and the
    sweep cannot look. The credential fault is the wider outage *and* the true
    root cause, so it keeps the one detail slot -- and the presence field is
    still readable beside it rather than overwritten.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _arm_presence(monkeypatch, _report(unavailable="InvalidClientTokenId: no such user"))
    monkeypatch.setattr(
        app_module,
        "_apply_credential_verdict",
        lambda payload: payload.update(
            {
                "credentials_state": "rejected",
                "degraded": True,
                "degraded_detail": "AWS HAS REJECTED THESE CREDENTIALS",
                "status": "degraded",
            }
        ),
    )

    payload = await _readyz(monkeypatch, manifest)

    assert payload["degraded_detail"] == "AWS HAS REJECTED THESE CREDENTIALS"
    assert payload["installation_state"] == PRESENCE_UNVERIFIED
    assert payload["installation_detail"]


async def test_an_unswept_process_is_not_reported_as_healthy_or_degraded(
    monkeypatch,
) -> None:
    """Never-checked is its own answer, and it must not flap a fresh start.

    Same reasoning as the credential verdict's "unknown": the first seconds of a
    process's life are not a fault. It still must not read as `verified_present`.
    """

    manifest = deployed_manifest()
    _deployed_app_runtime(monkeypatch, manifest)
    _arm_presence(monkeypatch, _report(sealed=3))
    monkeypatch.setattr(
        app_module, "installation_presence_async", _never_checked
    )

    payload = await _readyz(monkeypatch, manifest)

    assert payload["installation_state"] == PRESENCE_NEVER_CHECKED
    assert payload["installation_checked"] is False
    assert payload["status"] == "ready"
    assert payload["degraded_detail"] is None


async def _never_checked(**_kwargs):
    return presence_from_report(None)


def test_the_drift_hook_reads_the_cache_and_never_sweeps(monkeypatch) -> None:
    """The disclosure hook is rendered on every poll of a session, so it may not
    reach AWS. Before the first sweep it answers None, which the panel already
    renders as not-read rather than as agreement."""

    lifecycle.reset_installation_presence_cache()
    monkeypatch.setattr(
        lifecycle,
        "reconcile_live",
        lambda *_args, **_kwargs: pytest.fail("the hook must not sweep"),
    )

    assert lifecycle.cached_installation_report() is None

    report = _report(missing=1)
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: SimpleNamespace(run_id="x"))
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda *_args, **_kwargs: report)
    lifecycle.installation_presence()
    monkeypatch.setattr(
        lifecycle,
        "reconcile_live",
        lambda *_args, **_kwargs: pytest.fail("the hook must not sweep"),
    )

    assert lifecycle.cached_installation_report() is report


def test_the_manager_is_given_the_cached_hook_rather_than_a_live_sweep() -> None:
    """The hook was accepted and never passed, so the drift panel read
    `DRIFT NOT READ` forever. It is wired now -- to the cache, deliberately."""

    import inspect

    source = inspect.getsource(app_module._open_runtime)

    assert "drift_report=cached_installation_report" in source


def test_a_failed_sweep_is_cached_briefly_so_the_signal_recovers_itself(
    monkeypatch,
) -> None:
    """An unreadable account must not be retried per request, nor stick for a
    full TTL once credentials come back."""

    monkeypatch.setattr(lifecycle, "load_manifest", lambda: SimpleNamespace(run_id="x"))
    monkeypatch.setattr(
        lifecycle, "reconcile_live", lambda *_args, **_kwargs: _report(unavailable="ExpiredToken")
    )

    _presence, _report_out, ttl = lifecycle._observe_installation_presence(None)

    assert ttl == lifecycle.INSTALLATION_PRESENCE_FAILURE_TTL_SECONDS
    assert ttl < lifecycle.INSTALLATION_PRESENCE_TTL_SECONDS


def test_an_unreadable_manifest_is_could_not_look_not_gone(monkeypatch) -> None:
    """The probe feeds /readyz, so it may not raise -- and may not guess."""

    monkeypatch.setattr(
        lifecycle,
        "load_manifest",
        lambda: (_ for _ in ()).throw(RuntimeError("ANTI_DEMO_MANIFEST is unset")),
    )

    presence, report, _ttl = lifecycle._observe_installation_presence(None)

    assert presence.state == PRESENCE_UNVERIFIED
    assert report is None
    assert "IS GONE" not in presence.detail


# ---------------------------------------------------------------------------
# Defect 2 -- the two CLI surfaces
# ---------------------------------------------------------------------------


def test_demo_status_fails_on_a_confirmed_absence_and_only_advises_on_a_blind_sweep(
    monkeypatch,
) -> None:
    """`antidemo status` had no way to say the install was swept.

    A confirmed absence fails the command: it will not fix itself and no round
    can run. A sweep that could not look is advisory, because `antidemo status` is
    run precisely when credentials have lapsed and failing it there would make
    the command useless in the situation it exists for.
    """

    monkeypatch.setattr(lifecycle, "load_manifest", lambda: SimpleNamespace(run_id="x"))

    lifecycle.reset_installation_presence_cache()
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda *_a, **_k: _report(missing=3))
    gone = lifecycle.installation_presence_check()

    lifecycle.reset_installation_presence_cache()
    monkeypatch.setattr(
        lifecycle, "reconcile_live", lambda *_a, **_k: _report(unavailable="ExpiredToken")
    )
    blind = lifecycle.installation_presence_check()

    lifecycle.reset_installation_presence_cache()
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda *_a, **_k: _report())
    present = lifecycle.installation_presence_check()

    assert (gone.ok, gone.advisory) == (False, False)
    assert "IS GONE" in gone.detail
    assert (blind.ok, blind.advisory) == (True, True)
    assert "COULD NOT BE CHECKED" in blind.detail
    assert (present.ok, present.advisory) == (True, False)
    assert gone.name == blind.name == present.name == "installation_presence"


def test_demo_status_reports_presence_beside_the_other_generation_checks(
    monkeypatch, tmp_path
) -> None:
    """Wiring, not classification: the line has to actually appear."""

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text("{}\n", encoding="utf-8")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(manifest_file))
    monkeypatch.setattr(cli_module, "load_manifest", lambda _path: make_manifest())
    monkeypatch.setattr(cli_module, "lock_is_held", lambda _path: False)
    monkeypatch.setattr(
        cli_module,
        "installation_presence_check",
        lambda manifest=None: Check(
            "installation_presence", False, "THE SEALED AWS INFRASTRUCTURE IS GONE"
        ),
    )

    names = [check.name for check in cli_module._generation_checks()]

    assert "installation_presence" in names


def test_demo_doctor_separates_a_vanished_install_from_a_leaked_clone(monkeypatch) -> None:
    """Doctor's `resource_reconciliation` is False for *any* drift, including an
    orphan clone, so a single verdict over both reads as neither. Two lines."""

    manifest = make_manifest()
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda *_a, **_k: _report(missing=3))

    checks = {check.name: check for check in lifecycle._installation_presence_checks(manifest)}

    assert set(checks) == {"installation_presence", "resource_reconciliation"}
    assert checks["installation_presence"].ok is False
    assert "IS GONE" in checks["installation_presence"].detail

    monkeypatch.setattr(
        lifecycle, "reconcile_live", lambda *_a, **_k: _report(unavailable="ExpiredToken")
    )
    blind = {check.name: check for check in lifecycle._installation_presence_checks(manifest)}

    # Advisory, so an unreadable account does not fail `antidemo setup`'s final gate
    # on top of whatever already made the account unreadable.
    assert blind["installation_presence"].advisory is True
    assert "COULD NOT BE CHECKED" in blind["installation_presence"].detail

    monkeypatch.setattr(lifecycle, "reconcile_live", lambda *_a, **_k: _report())
    healthy = {check.name: check for check in lifecycle._installation_presence_checks(manifest)}

    assert healthy["installation_presence"].ok is True
    assert healthy["resource_reconciliation"].ok is True


def test_doctor_sweeps_the_account_once_for_both_lines(monkeypatch) -> None:
    """Two checks, one sweep. Doctor already makes plenty of AWS calls."""

    manifest = make_manifest()
    sweeps: list[int] = []

    def _sweep(*_args, **_kwargs):
        sweeps.append(1)
        return _report()

    monkeypatch.setattr(lifecycle, "reconcile_live", _sweep)
    lifecycle._installation_presence_checks(manifest)

    assert len(sweeps) == 1
