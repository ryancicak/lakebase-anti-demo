"""The AWS credential probe: what it proves, what it refuses to claim.

Every test here is about one of three properties. It must detect the fault it
exists for; it must never report a fault it has not established; and it must
never be able to break the thing it is watching.
"""

from __future__ import annotations

import asyncio
from contextlib import nullcontext

import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
    ProfileNotFound,
)

from server.aws_credential_probe import (
    DEFAULT_PROBE_INTERVAL_SECONDS,
    ESCALATE_AFTER_SECONDS,
    PROBE_INTERVAL_ENV,
    STALE_AFTER_INTERVALS,
    CredentialSentry,
    ProbeExpectations,
    expectations_from_manifest,
    principal_matches,
    probe_interval_seconds,
    probe_once,
)

ACCOUNT = "123456789012"
REGION = "us-west-2"
TRUSTED = f"arn:aws:iam::{ACCOUNT}:role/anti-demo-app"
CALLER = f"arn:aws:sts::{ACCOUNT}:assumed-role/anti-demo-app/session-1"
KEYS = {
    "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEEXAMPLE",
    "AWS_SECRET_ACCESS_KEY": "example-secret-access-key",
}


def client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeClient:
    def __init__(self, name: str, calls: list[str], answers: dict[str, object]) -> None:
        self._name = name
        self._calls = calls
        self._answers = answers

    def __getattr__(self, operation: str):
        def call(**kwargs):
            self._calls.append(f"{self._name}.{operation}")
            answer = self._answers.get(f"{self._name}.{operation}")
            if answer is None:
                raise AssertionError(
                    f"the probe called {self._name}.{operation}, which this test "
                    "did not authorise. A probe may only ever read."
                )
            if isinstance(answer, BaseException):
                raise answer
            return answer

        return call


class FakeSession:
    """Records every call, and refuses any operation a test did not allow.

    That refusal is the point: it is how "the probe never mutates anything"
    stays true as a property rather than as a comment.
    """

    def __init__(self, answers: dict[str, object], calls: list[str], **kwargs) -> None:
        self.kwargs = kwargs
        self._answers = answers
        self._calls = calls

    def client(self, name: str, **kwargs) -> FakeClient:
        return FakeClient(name, self._calls, self._answers)


def factory(answers: dict[str, object], calls: list[str]):
    def build(**kwargs):
        return FakeSession(answers, calls, **kwargs)

    return build


def _recording_factory(captured: list[dict[str, object]]):
    """A session factory that records the kwargs boto3 would have been given."""

    def build(**kwargs):
        captured.append(kwargs)
        return FakeSession(healthy_answers(), [], **kwargs)

    return build


def healthy_answers() -> dict[str, object]:
    return {
        "sts.get_caller_identity": {"Account": ACCOUNT, "Arn": CALLER},
        "rds.describe_db_instances": {"DBInstances": []},
    }


EXPECTED = ProbeExpectations(
    region=REGION,
    account_id=ACCOUNT,
    round5_trusted_principal_arn=TRUSTED,
)


def test_a_working_key_pair_is_proven_with_two_read_only_calls() -> None:
    """And with exactly those two, in that order.

    `GetCallerIdentity` alone would be a false green -- it needs no permission,
    so it succeeds for a principal that cannot do anything. The describe is what
    proves the grant every lane arms through.
    """

    calls: list[str] = []
    verdict = probe_once(
        EXPECTED,
        session_factory=factory(healthy_answers(), calls),
        environ=dict(KEYS),
    )
    assert verdict.state == "ok"
    assert verdict.healthy is True
    assert verdict.detail is None
    assert verdict.account == ACCOUNT
    assert verdict.arn == CALLER
    assert verdict.capabilities_lost == ()
    assert calls == ["sts.get_caller_identity", "rds.describe_db_instances"]


def test_the_session_the_probe_builds_for_each_auth_mode() -> None:
    """Exactly which kwargs reach boto3, in both modes, asserted as a whole.

    Two things ride on this and both were a test of their own. `rds:Describe*`
    is granted under `aws:RequestedRegion`, so a probe that issued its call in
    another region would be denied for a reason that has nothing to do with the
    credentials and would report a perfectly good key pair as unpermitted. And
    the runtime this probe reports on authenticates with long-lived ambient
    keys, so a `profile_name` in that mode would make the probe fail on exactly
    the installation it is meant to watch.

    Equality against the whole kwargs dict rather than membership tests, so an
    extra kwarg nobody intended is a failure rather than a silent pass.
    """

    cases: tuple[tuple[str, dict, dict], ...] = (
        ("ambient keys", dict(KEYS), {"region_name": REGION}),
        (
            "a sealed profile",
            {"AWS_PROFILE": "sandbox", "AWS_AUTH_MODE": "profile"},
            {"region_name": REGION, "profile_name": "sandbox"},
        ),
    )

    for name, environ, expected_kwargs in cases:
        captured: list[dict[str, object]] = []
        verdict = probe_once(
            EXPECTED,
            session_factory=_recording_factory(captured),
            environ=environ,
        )
        assert verdict.state == "ok", name
        assert captured == [expected_kwargs], name


def test_no_credentials_at_all_is_named_absent_and_costs_no_api_call() -> None:
    calls: list[str] = []
    verdict = probe_once(
        EXPECTED,
        session_factory=factory(healthy_answers(), calls),
        environ={},
    )
    assert verdict.state == "absent"
    assert "NO AWS CREDENTIALS ARE CONFIGURED" in verdict.detail
    # Nothing to ask AWS about: there is no credential to present.
    assert calls == []


def test_keys_exported_into_a_profile_sealed_installation_are_named_misconfigured() -> None:
    """The gap an operator hits the day they switch an existing box to keys.

    A manifest sealed in profile mode makes `antidemo serve` export `AWS_PROFILE`,
    and `server/aws_auth.py` refuses a process holding both that and ambient
    keys. Every AWS path raises the same refusal -- but it raises it when a
    round arms, so the server starts, `/readyz` says ready, and the first bout
    is where the operator finds out. Named separately from `absent` because the
    fix is different: this one needs a reseal, not an export.
    """

    calls: list[str] = []
    verdict = probe_once(
        EXPECTED,
        session_factory=factory(healthy_answers(), calls),
        environ={**KEYS, "AWS_PROFILE": "some-sso-profile", "AWS_AUTH_MODE": "profile"},
    )
    assert verdict.state == "misconfigured"
    assert "AMBIGUOUS" in verdict.detail
    assert "cannot be mixed" in verdict.detail
    assert calls == []
    # Terminal: the sealed mode is not going to change while this process runs.
    assert "every AWS lane" in " ".join(verdict.capabilities_lost)


def test_an_sso_role_with_a_path_matches_the_trust_it_was_sealed_from() -> None:
    """The shape a real SSO installation actually has.

    AWS Identity Center roles live under `role/aws-reserved/sso.amazonaws.com/...`,
    and STS hands back an assumed-role ARN with the path stripped. Whole-string
    comparison would report every SSO installation ever provisioned as a Round 5
    principal mismatch.
    """

    trusted = (
        "arn:aws:iam::111122223333:role/aws-reserved/sso.amazonaws.com/us-west-2/"
        "AWSReservedSSO_ExampleAdmin_aaaabbbbccccdddd"
    )
    caller = (
        "arn:aws:sts::111122223333:assumed-role/"
        "AWSReservedSSO_ExampleAdmin_aaaabbbbccccdddd/operator@example.com"
    )
    assert principal_matches(caller, trusted) is True
    # And the switch this task is about: keys are a different principal, so the
    # role sealed under SSO will not let them assume it. Same account on both
    # sides, so the mismatch this asserts is the principal kind and nothing else.
    assert principal_matches("arn:aws:iam::111122223333:user/anti-demo", trusted) is False


@pytest.mark.parametrize(
    "code",
    ["InvalidClientTokenId", "SignatureDoesNotMatch", "ExpiredToken", "AuthFailure"],
)
def test_a_revoked_or_rotated_key_is_named_rejected(code: str) -> None:
    calls: list[str] = []
    verdict = probe_once(
        EXPECTED,
        session_factory=factory(
            {"sts.get_caller_identity": client_error(code, "GetCallerIdentity")},
            calls,
        ),
        environ=dict(KEYS),
    )
    assert verdict.state == "rejected"
    assert code in verdict.detail
    assert "every AWS lane" in " ".join(verdict.capabilities_lost)
    # Fails at the first call: there is no point asking about a permission for a
    # credential AWS will not accept.
    assert calls == ["sts.get_caller_identity"]


def test_credentials_for_the_wrong_account_are_caught_before_any_permission_check() -> None:
    calls: list[str] = []
    answers = healthy_answers()
    answers["sts.get_caller_identity"] = {
        "Account": "999999999999",
        "Arn": "arn:aws:iam::999999999999:user/somebody",
    }
    verdict = probe_once(
        EXPECTED, session_factory=factory(answers, calls), environ=dict(KEYS)
    )
    assert verdict.state == "wrong_account"
    assert "999999999999" in verdict.detail
    assert ACCOUNT in verdict.detail
    assert calls == ["sts.get_caller_identity"]


def test_a_principal_round5_does_not_trust_narrows_round5_and_nothing_else() -> None:
    """The gap this probe was written for.

    An installation provisioned under one principal and served under another
    passes every existing check -- the account matches, the keys work, five
    rounds run -- and Round 5 is denied when it tries to assume a control role
    whose trust policy names the old principal. So the verdict has to be narrow:
    claiming every lane was lost here would be a louder answer and a false one.
    """

    calls: list[str] = []
    answers = healthy_answers()
    answers["sts.get_caller_identity"] = {
        "Account": ACCOUNT,
        "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/AWSReservedSSO_Admin_abc/operator",
    }
    verdict = probe_once(
        EXPECTED, session_factory=factory(answers, calls), environ=dict(KEYS)
    )
    assert verdict.state == "principal_mismatch"
    assert "AWSReservedSSO_Admin_abc" in verdict.detail
    assert TRUSTED in verdict.detail
    assert verdict.capabilities_lost == (
        "Round 5 (survive_connection_spike) -- its control role does not trust the "
        "principal this process is authenticating as, so assuming it will be denied "
        "and the round cannot arm",
    )


def test_the_faults_the_probe_declines_to_call_a_credential_fault() -> None:
    """Every answer that must not be read as "your keys are bad", one row each.

    This is the false-positive half of the module's contract and it was a test
    per fault. A probe that cries credential failure over a throttle, a dropped
    connection, an error code it has never seen, or a principal shape it cannot
    parse teaches the operator to ignore it -- and then the real one is ignored
    too. So each of these resolves to `unreachable`, `absent` or plain `ok`, and
    never to `rejected`, `unpermitted` or `principal_mismatch`.

    The row names itself, and `rejected` is asserted against explicitly so that
    a row which silently starts condemning the credential fails here rather than
    passing on a state nobody looked at.
    """

    cases: tuple[tuple[str, ProbeExpectations, dict, str, tuple[str, ...]], ...] = (
        (
            "an error code the probe has never seen",
            EXPECTED,
            {"sts.get_caller_identity": client_error("SomethingNew", "GetCallerIdentity")},
            "unreachable",
            (),
        ),
        (
            "sts is unreachable over the network",
            EXPECTED,
            {"sts.get_caller_identity": EndpointConnectionError(endpoint_url="https://sts")},
            "unreachable",
            (),
        ),
        # A busy account must not read as a revoked key.
        (
            "the account is throttled",
            EXPECTED,
            {"rds.describe_db_instances": client_error("Throttling", "DescribeDBInstances")},
            "unreachable",
            ("unverified",),
        ),
        (
            "the account is at its request limit",
            EXPECTED,
            {
                "rds.describe_db_instances": client_error(
                    "RequestLimitExceeded", "DescribeDBInstances"
                )
            },
            "unreachable",
            ("unverified",),
        ),
        (
            "the service is unavailable",
            EXPECTED,
            {
                "rds.describe_db_instances": client_error(
                    "ServiceUnavailable", "DescribeDBInstances"
                )
            },
            "unreachable",
            ("unverified",),
        ),
        # botocore's own way of saying there is nothing to present.
        (
            "botocore found no credential",
            EXPECTED,
            {"sts.get_caller_identity": NoCredentialsError()},
            "absent",
            (),
        ),
        (
            "botocore could not find the named profile",
            EXPECTED,
            {"sts.get_caller_identity": ProfileNotFound(profile="gone")},
            "absent",
            (),
        ),
        (
            # `None` from the comparison means "cannot tell", not "wrong".
            "a principal shape that cannot be compared",
            EXPECTED,
            {
                "sts.get_caller_identity": {
                    "Account": ACCOUNT,
                    "Arn": f"arn:aws:sts::{ACCOUNT}:federated-user/somebody",
                }
            },
            "ok",
            (),
        ),
        (
            "an installation with no Round 5 to compare against",
            ProbeExpectations(region=REGION, account_id=ACCOUNT),
            {},
            "ok",
            (),
        ),
    )

    for name, expectations, overrides, expected_state, fragments in cases:
        answers = healthy_answers()
        answers.update(overrides)
        verdict = probe_once(
            expectations, session_factory=factory(answers, []), environ=dict(KEYS)
        )
        assert verdict.state == expected_state, name
        assert verdict.state not in ("rejected", "unpermitted", "principal_mismatch"), name
        for fragment in fragments:
            assert fragment in verdict.detail, f"{name}: missing {fragment!r}"


def test_valid_keys_with_the_policy_detached_are_named_unpermitted() -> None:
    calls: list[str] = []
    answers = healthy_answers()
    answers["rds.describe_db_instances"] = client_error("AccessDenied", "DescribeDBInstances")
    verdict = probe_once(
        EXPECTED, session_factory=factory(answers, calls), environ=dict(KEYS)
    )
    assert verdict.state == "unpermitted"
    assert "VALID BUT NOT PERMITTED" in verdict.detail
    assert REGION in verdict.detail
    assert calls == ["sts.get_caller_identity", "rds.describe_db_instances"]


def test_the_probe_never_raises_at_its_caller() -> None:
    """Including when the probe itself is what is broken.

    A monitoring feature that can take the server down is worse than no
    monitoring, so the failure of the check is reported as a failure of the
    check -- not as a verdict about the credentials.
    """

    def explode(**kwargs):
        raise RuntimeError("boom")

    sentry = CredentialSentry(
        EXPECTED, session_factory=explode, interval_seconds=1.0, environ=dict(KEYS)
    )
    verdict = sentry.check_once()
    assert verdict.state == "unreachable"
    assert "FAILED TO RUN" in verdict.detail
    assert "RuntimeError" in verdict.detail


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_a_hard_verdict_escalates_on_its_second_showing_not_on_a_timer() -> None:
    """IAM is eventually consistent, so one denial may be a policy still landing.

    Two in a row is not, and an operator should not wait fifteen minutes to be
    told that the key their monitoring says is fine has actually been revoked.
    """

    clock = Clock()
    answers = healthy_answers()
    answers["rds.describe_db_instances"] = client_error("AccessDenied", "DescribeDBInstances")
    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(answers, []),
        interval_seconds=300.0,
        environ=dict(KEYS),
        monotonic=clock,
    )
    first = sentry.check_once()
    assert (first.state, first.recovery.state, first.recovery.attempts) == (
        "unpermitted",
        "retrying",
        1,
    )
    clock.advance(300.0)
    second = sentry.check_once()
    assert (second.state, second.recovery.state, second.recovery.attempts) == (
        "unpermitted",
        "escalated",
        2,
    )
    assert second.recovery.next_attempt_seconds == 300.0
    assert second.recovery.error == "unpermitted"


def test_a_transient_fault_gets_the_time_budget_instead() -> None:
    clock = Clock()
    answers = healthy_answers()
    answers["rds.describe_db_instances"] = client_error("Throttling", "DescribeDBInstances")
    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(answers, []),
        interval_seconds=60.0,
        environ=dict(KEYS),
        monotonic=clock,
    )
    assert sentry.check_once().recovery.state == "retrying"
    clock.advance(60.0)
    assert sentry.check_once().recovery.state == "retrying"
    clock.advance(ESCALATE_AFTER_SECONDS)
    assert sentry.check_once().recovery.state == "escalated"


def test_recovering_clears_the_schedule_without_a_restart() -> None:
    """An operator who reattaches the policy should see this go green by itself."""

    clock = Clock()
    answers = healthy_answers()
    answers["rds.describe_db_instances"] = client_error("AccessDenied", "DescribeDBInstances")
    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(answers, []),
        interval_seconds=10.0,
        environ=dict(KEYS),
        monotonic=clock,
    )
    sentry.check_once()
    sentry.check_once()
    assert sentry.verdict().recovery.state == "escalated"

    answers["rds.describe_db_instances"] = {"DBInstances": []}
    clock.advance(10.0)
    healed = sentry.check_once()
    assert healed.state == "ok"
    assert healed.recovery.state == "settled"
    assert healed.recovery.attempts == 0
    assert healed.capabilities_lost == ()


def test_recovery_is_announced_once_per_transition_and_never_on_a_healthy_start() -> None:
    """The signal the readiness gate has no other way to get.

    The gate backs off from AWS faults it cannot see the end of. This is the only
    thing in the process that finds out when they clear, so it has to say so --
    exactly once per bad-to-good transition. A process that was healthy all along
    has recovered from nothing, and announcing that would wake the gate for no
    reason on every start.
    """

    answers = healthy_answers()
    woken: list[int] = []
    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(answers, []),
        interval_seconds=1.0,
        environ=dict(KEYS),
        on_recovered=lambda: woken.append(1),
    )

    assert sentry.check_once().state == "ok"
    assert sentry.check_once().state == "ok"
    assert woken == []

    answers["sts.get_caller_identity"] = client_error("ExpiredToken", "GetCallerIdentity")
    assert sentry.check_once().state == "rejected"
    assert sentry.check_once().state == "rejected"
    assert woken == []

    answers["sts.get_caller_identity"] = {"Account": ACCOUNT, "Arn": CALLER}
    assert sentry.check_once().state == "ok"
    assert woken == [1]
    # Steady green afterwards is not a second recovery.
    assert sentry.check_once().state == "ok"
    assert woken == [1]


def test_a_recovery_listener_that_raises_cannot_break_the_probe() -> None:
    """The probe may never be broken by something it merely notifies."""

    answers = healthy_answers()
    answers["sts.get_caller_identity"] = client_error("ExpiredToken", "GetCallerIdentity")

    def explode() -> None:
        raise RuntimeError("the readiness gate is gone")

    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(answers, []),
        interval_seconds=1.0,
        environ=dict(KEYS),
        on_recovered=explode,
    )
    assert sentry.check_once().state == "rejected"

    answers["sts.get_caller_identity"] = {"Account": ACCOUNT, "Arn": CALLER}
    verdict = sentry.check_once()

    assert verdict.state == "ok"
    assert verdict.recovery.state == "settled"


def test_absent_credentials_stop_the_loop_because_nothing_here_can_change_them() -> None:
    """`given_up` means exactly that, and the loop has to match the word.

    The keys live in this process's environment. No amount of re-asking will put
    them there, so a monitor must not be told to expect self-recovery -- and the
    probe must not spend the life of the process re-learning one constant.
    """

    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(healthy_answers(), []),
        interval_seconds=0.01,
        environ={},
    )
    asyncio.get_event_loop  # noqa: B018 - documents that run() is the loop under test
    asyncio.run(asyncio.wait_for(sentry.run(), timeout=5))
    verdict = sentry.verdict()
    assert verdict.state == "absent"
    assert verdict.recovery.state == "given_up"
    assert verdict.recovery.next_attempt_seconds is None


def test_a_probe_that_stops_reporting_goes_stale_rather_than_staying_green() -> None:
    """The same silent failure this module exists for, one level up.

    If a dead probe left its last good verdict in place, `/readyz` would keep
    reporting healthy credentials on behalf of a check that had stopped running.
    """

    clock = Clock()
    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(healthy_answers(), []),
        interval_seconds=100.0,
        environ=dict(KEYS),
        monotonic=clock,
    )
    assert sentry.check_once().state == "ok"
    clock.advance(100.0 * STALE_AFTER_INTERVALS - 1)
    assert sentry.verdict().state == "ok"
    clock.advance(2)
    stale = sentry.verdict()
    assert stale.state == "stale"
    assert stale.recovery.state == "given_up"
    assert "HAS STOPPED REPORTING" in stale.detail


def test_a_terminal_verdict_does_not_age_into_a_vaguer_one() -> None:
    clock = Clock()
    sentry = CredentialSentry(
        EXPECTED,
        session_factory=factory(healthy_answers(), []),
        interval_seconds=1.0,
        environ={},
        monotonic=clock,
    )
    assert sentry.check_once().state == "absent"
    clock.advance(10_000)
    assert sentry.verdict().state == "absent"


def test_the_interval_is_configurable_and_refuses_nonsense() -> None:
    assert probe_interval_seconds({}) == DEFAULT_PROBE_INTERVAL_SECONDS
    assert probe_interval_seconds({PROBE_INTERVAL_ENV: "45"}) == 45.0
    with pytest.raises(ValueError, match="must be a number"):
        probe_interval_seconds({PROBE_INTERVAL_ENV: "soon"})
    with pytest.raises(ValueError, match="at least 1 second"):
        probe_interval_seconds({PROBE_INTERVAL_ENV: "0"})


def test_expectations_come_from_the_seal_not_the_environment() -> None:
    class Aws:
        region = REGION
        account_id = ACCOUNT

    class Round5:
        control_role_trusted_principal_arn = TRUSTED

    class Manifest:
        aws = Aws()
        round5 = Round5()

    assert expectations_from_manifest(Manifest()) == EXPECTED

    class NoRound5(Manifest):
        round5 = None

    assert expectations_from_manifest(NoRound5()).round5_trusted_principal_arn is None


def test_a_legacy_round5_seal_without_a_trusted_principal_is_tolerated() -> None:
    class Aws:
        region = REGION
        account_id = ACCOUNT

    class Manifest:
        aws = Aws()
        round5 = object()

    assert expectations_from_manifest(Manifest()).round5_trusted_principal_arn is None


@pytest.mark.parametrize(
    ("caller", "trusted", "expected"),
    [
        (f"arn:aws:iam::{ACCOUNT}:user/bob", f"arn:aws:iam::{ACCOUNT}:user/bob", True),
        (CALLER, TRUSTED, True),
        # A role with a path: STS drops the path from the assumed-role ARN, so
        # whole-string equality would call this a mismatch.
        (CALLER, f"arn:aws:iam::{ACCOUNT}:role/team/anti-demo-app", True),
        (CALLER, f"arn:aws:iam::{ACCOUNT}:user/anti-demo-app", False),
        (f"arn:aws:iam::{ACCOUNT}:user/bob", f"arn:aws:iam::{ACCOUNT}:user/alice", False),
        (CALLER, "arn:aws:iam::999999999999:role/anti-demo-app", False),
        (f"arn:aws:sts::{ACCOUNT}:federated-user/x", TRUSTED, None),
        ("not-an-arn", TRUSTED, None),
        (CALLER, "not-an-arn", None),
    ],
)
def test_principal_comparison(caller: str, trusted: str, expected: bool | None) -> None:
    assert principal_matches(caller, trusted) is expected


# --------------------------------------------------------------------------- #
# The serve-time refusal built on top of the same comparison
# --------------------------------------------------------------------------- #


class _Manifest:
    """Enough sealed manifest for `expectations_from_manifest` to read."""

    def __init__(self, trusted: str | None = TRUSTED) -> None:
        self.aws = type("Aws", (), {"region": REGION, "account_id": ACCOUNT})()
        self.round5 = type(
            "Round5", (), {"control_role_trusted_principal_arn": trusted}
        )()


def test_serve_serves_the_other_five_rounds_instead_of_refusing_to_start(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Five rounds, loudly missing a sixth, beats no server at all.

    This began as a refusal to start, on the reasoning that five rounds working
    and the sixth failing to assume its control role is worse than being told
    about it up front. The reasoning held; the conclusion did not survive the
    thing it was protecting. A sealed trust policy names one principal, and on
    this installation that principal is a federated SSO role whose session
    expires overnight -- so the only credential that can serve unattended for
    days is a long-lived key that the seal does not name, and refusing to start
    under it turns a missing Round 5 into a missing demo every morning.

    What made the refusal unnecessary rather than merely expensive is that the
    audience-facing half of it is already built somewhere better.
    `server.round_availability.refusal` turns this same `principal_mismatch`
    verdict into Round 5 reporting `unavailable` on `/api/catalog`, so the round
    is taken off the fight card rather than offered and failed at the bell --
    which is the outcome the refusal existed to guarantee. What only this notice
    can do is tell the operator, at launch, before anyone is watching.

    So both halves are asserted here: the operator still gets every fact the
    refusal carried, and the server comes up.
    """
    from server import aws_credential_probe, cli

    answers = healthy_answers()
    answers["sts.get_caller_identity"] = {
        "Account": ACCOUNT,
        "Arn": f"arn:aws:iam::{ACCOUNT}:user/somebody-else",
    }
    mismatched = lambda expectations, **_kwargs: probe_once(  # noqa: E731
        expectations, session_factory=factory(answers, []), environ=dict(KEYS)
    )

    notice = cli.round5_principal_notice(_Manifest(), probe=mismatched)

    assert notice is not None
    assert "not the principal Round 5 trusts" in notice
    # Names both sides, because "wrong principal" without the two ARNs sends the
    # operator hunting.
    assert "user/somebody-else" in notice
    assert TRUSTED in notice
    # And says what is being given up, so "the server started" is never mistaken
    # for "all six rounds are on the card".
    assert "Round 5" in notice

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    handed: list[tuple[str, list[str]]] = []
    monkeypatch.setenv("ANTI_DEMO_SERVER_HOST", "")
    monkeypatch.setenv("ANTI_DEMO_SERVER_PORT", "")
    monkeypatch.setattr(aws_credential_probe, "probe_once", mismatched)
    monkeypatch.setattr(cli, "require_serving_environment", lambda: tmp_path / ".venv")
    monkeypatch.setattr(cli, "manifest_path", lambda: manifest)
    monkeypatch.setattr(cli, "load_manifest", lambda: _Manifest())
    monkeypatch.setattr(cli, "_mutating", lambda _operation: nullcontext())
    monkeypatch.setattr(cli, "ensure_coordination", lambda _manifest: None)
    monkeypatch.setattr(cli, "apply_manifest_environment", lambda _manifest: None)
    monkeypatch.setattr(
        cli.os, "execvp", lambda file, arguments: handed.append((file, arguments))
    )

    assert cli._serve("127.0.0.1", 8123) == 0

    # The launch happened. This is the regression: a refusal here is a demo that
    # is simply not running the next morning.
    assert [file for file, _arguments in handed] == ["uv"]
    # And the operator was told, on the stream an operator reads.
    assert "not the principal Round 5 trusts" in capsys.readouterr().err


def test_serve_says_nothing_when_the_sealed_principal_matches() -> None:
    from server.cli import round5_principal_notice

    probe = lambda expectations, **_kwargs: probe_once(  # noqa: E731
        expectations, session_factory=factory(healthy_answers(), []), environ=dict(KEYS)
    )
    assert round5_principal_notice(_Manifest(), probe=probe) is None


@pytest.mark.parametrize(
    "answers",
    [
        # Offline.
        {"sts.get_caller_identity": EndpointConnectionError(endpoint_url="https://sts")},
        # Throttled.
        {"sts.get_caller_identity": client_error("Throttling", "GetCallerIdentity")},
        # No credentials in this process at all.
        {"sts.get_caller_identity": NoCredentialsError()},
        # Permitted to authenticate, not permitted to describe: a real fault, and
        # not this one.
        {
            "sts.get_caller_identity": {"Account": ACCOUNT, "Arn": CALLER},
            "rds.describe_db_instances": client_error(
                "AccessDenied", "DescribeDBInstances"
            ),
        },
        # A federated principal cannot be compared to a role at all, and `None` is
        # not `False`.
        {
            "sts.get_caller_identity": {
                "Account": ACCOUNT,
                "Arn": f"arn:aws:sts::{ACCOUNT}:federated-user/x",
            },
            "rds.describe_db_instances": {"DBInstances": []},
        },
    ],
    ids=["offline", "throttled", "no-credentials", "unpermitted", "incomparable"],
)
def test_a_probe_that_cannot_make_the_comparison_says_nothing(answers) -> None:
    """A probe failure must not print a scare at launch.

    Nothing here stops a serve any more, so the cost of a false positive is no
    longer an unstartable demo -- it is an operator who learns to ignore the one
    line that means something. Only `principal_mismatch`, which needs both ARNs to
    have parsed and named different principals, is worth saying out loud.
    """
    from server.cli import round5_principal_notice

    probe = lambda expectations, **_kwargs: probe_once(  # noqa: E731
        expectations, session_factory=factory(dict(answers), []), environ=dict(KEYS)
    )
    assert round5_principal_notice(_Manifest(), probe=probe) is None


def test_an_installation_without_round5_is_never_asked() -> None:
    """No sealed trust policy, nothing to compare, and no probe worth issuing."""
    from server.cli import round5_principal_notice

    def refuse(*_args, **_kwargs):
        raise AssertionError("the probe must not run when there is nothing to compare")

    assert round5_principal_notice(_Manifest(trusted=None), probe=refuse) is None
    assert round5_principal_notice(None, probe=refuse) is None


def test_a_probe_that_raises_outright_says_nothing() -> None:
    from server.cli import round5_principal_notice

    def explode(*_args, **_kwargs):
        raise RuntimeError("boto3 could not even be built")

    assert round5_principal_notice(_Manifest(), probe=explode) is None
