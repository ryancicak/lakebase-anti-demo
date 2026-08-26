"""Does this serving process still hold usable AWS credentials?

The failure this exists to make visible: the server comes up, `/readyz` says
``ready``, the catalog shows six green rounds, and every lane fails the moment
somebody starts a bout -- because the credentials the process is holding were
revoked, rotated, scoped wrong, or belong to a different principal than the one
this installation was provisioned under. Nothing in the process notices, because
nothing asks until a bout asks.

Long-lived IAM keys remove session expiry as a cause and remove none of the
others. A key can be deactivated, deleted, have its policy detached, or be
replaced by one for a different user; the process environment does not change
when any of that happens, so the only way to know is to ask AWS.

**What the probe asks, and why those two calls.**

``sts:GetCallerIdentity`` needs no IAM permission at all, which is exactly why it
cannot be the only call: it succeeds for a principal with an empty policy, so a
probe built on it alone reports green for credentials that cannot do a single
thing the demo needs. That is the false green this module exists to prevent, so
it is used for what it *can* prove -- that the credentials resolve, are not
revoked, and name the account and principal the manifest sealed -- and then a
second call proves the permission.

``rds:DescribeDBInstances`` is that second call. It is the operation the lanes
themselves issue: :func:`server.targets.RdsCredentialProvider._instance_sync`
and the Aurora equivalent both start there, and neither can arm without it. The
IAM policy grants it under an ``aws:RequestedRegion`` condition, so issuing it in
the sealed region proves the condition matches too -- a probe in the wrong region
would fail for a reason that has nothing to do with the credentials. It is issued
unfiltered rather than against a specific identifier because a round whose lane
is disclosed rather than raced legitimately has no instance, and a probe must not
report a credential fault for a resource that was never meant to exist.

Both calls are free. Neither is billed, neither mutates anything, and at one
round trip each per interval they are nowhere near any rate limit.

**What it deliberately does not prove.** ``secretsmanager:GetSecretValue`` is on
the lane hot path and is not probed: proving it requires actually reading a
database master password, which puts a live credential in this process's memory
and an entry in CloudTrail every interval, to learn something that is nearly
always granted by the same policy as the rest. ``DescribeSecret`` would be cheap
but proves a different permission than the one the lane uses, which is a false
green wearing a disguise. Round 5's runner authenticates as its own EC2 instance
profile, an entirely different principal this process cannot see. Those gaps are
named in the report the probe produces rather than papered over.

**Boundaries.** Read-only, always. This module never writes the manifest, never
takes the generation lock, never repairs anything and never touches a resource.
A probe failure is reported and nothing else: it must never be able to stop a
round that would otherwise have run, because a monitoring feature that can break
the thing it monitors is worse than no monitoring.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
    ProfileNotFound,
)

from .aws_auth import AwsAuthConfigurationError, runtime_auth_from_environment, session_arguments
from .readiness import RecoveryState

LOGGER = logging.getLogger(__name__)

PROBE_INTERVAL_ENV = "ANTI_DEMO_CREDENTIAL_PROBE_SECONDS"

#: Five minutes. Both calls are free, so the interval is not chosen to save
#: money -- it is chosen so that a revoked key is noticed long before somebody
#: walks up to the machine, without the log filling with probe lines. The
#: interval is also the detection latency, which is the number that matters.
DEFAULT_PROBE_INTERVAL_SECONDS = 300.0

#: A verdict older than this many intervals is not believed. Without it, a probe
#: task that died would leave the last good verdict in place forever and report
#: green on behalf of a check that had stopped running -- the same shape of
#: silent failure this module exists to remove, one level up.
STALE_AFTER_INTERVALS = 3.0

#: How long a *transient-looking* fault may keep failing before it stops being
#: reported as ordinary retrying. Fifteen minutes is three missed probes at the
#: default interval.
ESCALATE_AFTER_SECONDS = 900.0

#: A hard verdict -- rejected, denied, wrong account -- is not a blip, so it does
#: not get the time-based budget. It gets one repeat, because IAM is eventually
#: consistent and a policy attached seconds ago can deny once before it applies.
ESCALATE_AFTER_HARD_ATTEMPTS = 2

CredentialState = Literal[
    "unknown",
    "ok",
    "absent",
    "misconfigured",
    "rejected",
    "wrong_account",
    "principal_mismatch",
    "unpermitted",
    "unreachable",
    "stale",
]

#: States that will not change because of anything happening inside this
#: process. Everything else is worth asking about again.
#:
#: ``misconfigured`` is separated from ``absent`` because the two need different
#: fixes and the difference is invisible in a one-word status otherwise. Absent
#: means nothing is exported. Misconfigured means two credential sources are
#: exported at once and `server/aws_auth.py` refuses to guess between them --
#: which is what an operator gets when they export keys into an installation
#: whose manifest sealed profile mode. Every AWS path in the process raises the
#: same refusal, but it raises it when a round arms, so without this the server
#: looks healthy right up to the first bout.
_TERMINAL_STATES = frozenset({"absent", "misconfigured"})

_HARD_STATES = frozenset({"rejected", "wrong_account", "principal_mismatch", "unpermitted"})

_REJECTED_CODES = frozenset(
    {
        "AuthFailure",
        "ExpiredToken",
        "ExpiredTokenException",
        "InvalidAccessKeyId",
        "InvalidClientTokenId",
        "InvalidSignatureException",
        "RequestExpired",
        "SignatureDoesNotMatch",
        "TokenRefreshRequired",
        "UnrecognizedClientException",
    }
)
_DENIED_CODES = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "UnauthorizedOperation",
    }
)
#: Throttling is emphatically not a credential fault. Reading it as one would
#: turn a busy account into a fake credential outage.
_TRANSIENT_CODES = frozenset(
    {
        "InternalError",
        "InternalFailure",
        "RequestLimitExceeded",
        "RequestThrottled",
        "RequestThrottledException",
        "ServiceUnavailable",
        "Throttling",
        "ThrottlingException",
    }
)

#: What an operator loses, in the idiom `INMEMORY_COORDINATION_LOSSES` uses:
#: whole sentences naming the capability, not error codes.
_EVERY_LANE_LOST = (
    "every AWS lane -- the Aurora and RDS competitors cannot be described, so no "
    "round can arm and every bout fails at the starting line",
    "the capacity and cost disclosures -- they read the live control plane and "
    "will render as unavailable",
    "orphan reconciliation -- leaked per-bout resources cannot be seen, so they "
    "are neither reported nor cleaned up",
)
_ROUND5_LOST = (
    "Round 5 (survive_connection_spike) -- its control role does not trust the "
    "principal this process is authenticating as, so assuming it will be denied "
    "and the round cannot arm",
)

_ASSUMED_ROLE = re.compile(
    r"^arn:(?P<partition>[^:]+):sts::(?P<account>\d{12}):assumed-role/(?P<name>[^/]+)/"
)
_IAM_PRINCIPAL = re.compile(
    r"^arn:(?P<partition>[^:]+):iam::(?P<account>\d{12}):(?P<kind>role|user)/(?P<path>.*)$"
)


@dataclass(frozen=True)
class CredentialVerdict:
    """The last thing AWS said about this process's credentials.

    ``state`` is the whole answer for a monitor; ``detail`` is the sentence for a
    human. ``recovery`` reuses :class:`server.readiness.RecoveryState` verbatim so
    a credential fault reads through exactly the vocabulary every other fault in
    this process already reads through.
    """

    state: CredentialState = "unknown"
    detail: str | None = None
    recovery: RecoveryState = RecoveryState("settled")
    account: str | None = None
    arn: str | None = None
    attempts: int = 0
    checked_at_monotonic: float | None = None

    @property
    def healthy(self) -> bool:
        return self.state == "ok"

    @property
    def capabilities_lost(self) -> tuple[str, ...]:
        if self.state in {"ok", "unknown"}:
            return ()
        if self.state == "principal_mismatch":
            # Scoped deliberately. Naming every lane here would be a louder
            # answer and a false one: the five other rounds authenticate as this
            # principal directly and are unaffected by what a role trusts.
            return _ROUND5_LOST
        if self.state == "stale":
            return ()
        return _EVERY_LANE_LOST


_UNKNOWN = CredentialVerdict()

_STALE_DETAIL = (
    "THE AWS CREDENTIAL PROBE HAS STOPPED REPORTING: its last verdict is older "
    "than it should be, so the credentials backing every lane are unverified. "
    "This says nothing about whether they work -- only that nobody is checking."
)


def probe_interval_seconds(environ: Mapping[str, str] | None = None) -> float:
    """How often to ask. Refuses a nonsense value rather than silently defaulting."""
    environ = os.environ if environ is None else environ
    raw = (environ.get(PROBE_INTERVAL_ENV) or "").strip()
    if not raw:
        return DEFAULT_PROBE_INTERVAL_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"{PROBE_INTERVAL_ENV} must be a number of seconds, not {raw!r}"
        ) from exc
    if value < 1.0:
        raise ValueError(f"{PROBE_INTERVAL_ENV} must be at least 1 second, not {value}")
    return value


def principal_matches(caller_arn: str, trusted_arn: str) -> bool | None:
    """Is ``caller_arn`` the principal that ``trusted_arn`` names?

    Returns ``None`` when the question cannot be answered from the two strings,
    which is not the same as ``False``. A federated or service principal has no
    comparable form, and reporting a mismatch for one would raise a false alarm
    about a Round 5 that is fine.

    The comparison is by name rather than by whole ARN on purpose. A role's IAM
    ARN carries its path (``:role/team/name``) and the assumed-role ARN STS hands
    back does not (``:assumed-role/name/session``), so string equality would
    report every path-carrying role as a mismatch.
    """

    trusted = _IAM_PRINCIPAL.match(trusted_arn.strip())
    if trusted is None:
        return None
    trusted_account = trusted.group("account")
    trusted_name = trusted.group("path").rsplit("/", 1)[-1]

    assumed = _ASSUMED_ROLE.match(caller_arn.strip())
    if assumed is not None:
        if trusted.group("kind") != "role":
            return False
        return assumed.group("account") == trusted_account and (
            assumed.group("name") == trusted_name
        )

    caller = _IAM_PRINCIPAL.match(caller_arn.strip())
    if caller is None:
        return None
    return (
        caller.group("account") == trusted_account
        and caller.group("kind") == trusted.group("kind")
        and caller.group("path").rsplit("/", 1)[-1] == trusted_name
    )


def has_any_credential_source(environ: Mapping[str, str]) -> bool:
    """Whether anything at all is exported for AWS to reject.

    Public because the deployed startup path asks the same question. It is what
    separates ``absent`` from every other verdict, and two places deciding that
    separately is how the health surface and the startup surface would come to
    disagree about a process neither of them can fix.
    """

    return any(
        (environ.get(name) or "").strip()
        for name in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
        )
    )


def _error_code(error: ClientError) -> str:
    return str((error.response or {}).get("Error", {}).get("Code") or "")


def _classify(error: BotoCoreError | ClientError) -> tuple[CredentialState, str]:
    """Turn a botocore failure into one of the states, never into a guess."""
    if isinstance(error, NoCredentialsError | PartialCredentialsError | ProfileNotFound):
        return "absent", str(error)
    if isinstance(error, ClientError):
        code = _error_code(error)
        if code in _REJECTED_CODES:
            return "rejected", code
        if code in _DENIED_CODES:
            return "unpermitted", code
        if code in _TRANSIENT_CODES:
            return "unreachable", code
        # An unrecognised code is reported as unreachable rather than as a
        # credential fault: a wrong "your keys are revoked" is worse than an
        # honest "the probe could not tell".
        return "unreachable", code or type(error).__name__
    return "unreachable", type(error).__name__


@dataclass(frozen=True)
class ProbeExpectations:
    """What the manifest sealed, which is what the probe holds AWS against."""

    region: str
    account_id: str
    #: ``manifest.round5.control_role_trusted_principal_arn``, when Round 5 has
    #: been provisioned. Absent installations simply skip that comparison.
    round5_trusted_principal_arn: str | None = None
    #: ``manifest.aws.runtime_role_trusted_principal_arns``, when this
    #: installation seals a shared runtime role. Empty on every installation
    #: sealed before that existed, which is what keeps the direct comparison
    #: below the only one those ever make.
    #:
    #: Present, it *replaces* the direct comparison rather than joining it,
    #: because it answers a different question. With a runtime role sealed the
    #: control role trusts the runtime role and nothing else, so no process ever
    #: authenticates as the principal the control role names -- it authenticates
    #: as something that may assume it. Comparing the caller against the control
    #: role's principal would then report `principal_mismatch` for the two
    #: callers the design exists to admit.
    runtime_role_trusted_principal_arns: tuple[str, ...] = ()


def probe_once(
    expectations: ProbeExpectations,
    *,
    session_factory: Callable[..., Any],
    environ: Mapping[str, str] | None = None,
) -> CredentialVerdict:
    """Ask AWS once and return a verdict.

    Every way AWS can say no is a verdict, never an exception. Anything that is
    *not* AWS saying no -- a bug in here, a broken session factory -- is left to
    propagate, because reporting a programming error as "could not reach STS"
    would put a fault of the probe's own into a field an operator reads as a
    fact about their credentials. :meth:`CredentialSentry.check_once` is the
    fail-soft boundary that catches those and labels them honestly.
    """

    environ = os.environ if environ is None else environ
    try:
        auth = runtime_auth_from_environment(environ)
    except AwsAuthConfigurationError as exc:
        if has_any_credential_source(environ):
            return CredentialVerdict(
                state="misconfigured",
                detail=(
                    "THE AWS CREDENTIALS IN THIS PROCESS ARE AMBIGUOUS AND WILL "
                    f"BE REFUSED BY EVERY LANE: {exc}. This is what exporting "
                    "access keys into an installation whose manifest sealed a "
                    "named profile looks like -- the seal outranks the "
                    "environment, so the two cannot be mixed. Reprovisioning is "
                    "what changes the sealed mode; this process will not."
                ),
            )
        return CredentialVerdict(
            state="absent",
            detail=(
                "NO AWS CREDENTIALS ARE CONFIGURED IN THIS PROCESS: "
                f"{exc}. Nothing in this process can fix that -- the server has "
                "to be started from an environment that carries them."
            ),
        )

    try:
        session = session_factory(
            **session_arguments(auth.mode, auth.profile, expectations.region)
        )
        identity = session.client("sts", region_name=expectations.region).get_caller_identity()
    except (BotoCoreError, ClientError) as exc:
        state, code = _classify(exc)
        if state == "unpermitted":
            # GetCallerIdentity cannot be denied by a policy, so a denial here is
            # something else entirely -- an SCP or a permissions boundary.
            state = "rejected"
        return CredentialVerdict(
            state=state,
            detail=_identity_failure_detail(state, code),
        )

    account = str(identity.get("Account") or "")
    arn = str(identity.get("Arn") or "")
    if account != expectations.account_id:
        return CredentialVerdict(
            state="wrong_account",
            detail=(
                "THE AWS CREDENTIALS IN THIS PROCESS BELONG TO ANOTHER ACCOUNT: "
                f"they resolve to {account or 'an unreported account'}, and this "
                f"installation is sealed to {expectations.account_id}. Every lane "
                "will refuse rather than touch the wrong account's resources."
            ),
            account=account,
            arn=arn,
        )

    runtime_trusted = expectations.runtime_role_trusted_principal_arns
    trusted = expectations.round5_trusted_principal_arn
    if runtime_trusted:
        # Round 5's control role trusts the runtime role; the runtime role
        # trusts these. So the question this process has to answer is "may I
        # assume the runtime role", not "am I the control role's principal".
        # `None` from `principal_matches` is not a mismatch -- a federated or
        # service caller has no comparable form -- so only an all-False answer
        # refuses, exactly as the single-principal comparison below does.
        verdicts = [principal_matches(arn, candidate) for candidate in runtime_trusted]
        if all(verdict is False for verdict in verdicts):
            return CredentialVerdict(
                state="principal_mismatch",
                detail=(
                    "THIS PROCESS CANNOT REACH THE PRINCIPAL ROUND 5 TRUSTS: it "
                    f"is authenticating as {arn}, the Round 5 control role was "
                    f"sealed to trust {trusted}, and that role is only reachable "
                    "by assuming the sealed runtime role, whose trust policy "
                    f"names {', '.join(runtime_trusted)}. The other five rounds "
                    "are unaffected; Round 5 will be denied when it tries to "
                    "assume its control role. Reprovisioning is what re-seals "
                    "the trust -- this process will not change it."
                ),
                account=account,
                arn=arn,
            )
    elif trusted:
        verdict = principal_matches(arn, trusted)
        if verdict is False:
            return CredentialVerdict(
                state="principal_mismatch",
                detail=(
                    "THIS PROCESS IS NOT THE PRINCIPAL ROUND 5 TRUSTS: it is "
                    f"authenticating as {arn}, and the Round 5 control role was "
                    f"sealed to trust {trusted}. The other five rounds are "
                    "unaffected; Round 5 will be denied when it tries to assume "
                    "that role. Reprovisioning is what re-seals the trust -- "
                    "this process will not change it."
                ),
                account=account,
                arn=arn,
            )

    try:
        session.client("rds", region_name=expectations.region).describe_db_instances(
            MaxRecords=20
        )
    except (BotoCoreError, ClientError) as exc:
        state, code = _classify(exc)
        return CredentialVerdict(
            state=state,
            detail=_permission_failure_detail(state, code, expectations.region),
            account=account,
            arn=arn,
        )

    return CredentialVerdict(
        state="ok",
        detail=None,
        account=account,
        arn=arn,
    )


def _identity_failure_detail(state: CredentialState, code: str) -> str:
    if state == "rejected":
        return (
            "AWS REJECTED THE CREDENTIALS IN THIS PROCESS "
            f"({code or 'no code reported'}): they were deactivated, deleted or "
            "rotated after this server started. Every lane will fail until a "
            "working key pair is in the environment, which needs a restart."
        )
    return (
        "THE AWS CREDENTIAL PROBE COULD NOT REACH STS "
        f"({code or 'no code reported'}). This is not evidence that the "
        "credentials are bad -- only that they are unverified right now."
    )


def _permission_failure_detail(state: CredentialState, code: str, region: str) -> str:
    if state == "unpermitted":
        return (
            "THE AWS CREDENTIALS IN THIS PROCESS ARE VALID BUT NOT PERMITTED: "
            f"rds:DescribeDBInstances in {region} was denied ({code}). The "
            "principal is real and the account is right, so this is a missing or "
            "detached policy, or a region condition that does not match. Every "
            "lane arms through that call, so no round can start."
        )
    return (
        "THE AWS CREDENTIAL PROBE COULD NOT COMPLETE ITS PERMISSION CHECK "
        f"({code or 'no code reported'}). The credentials answered STS, so they "
        "exist and are accepted; whether they can drive a round is unverified."
    )


class CredentialSentry:
    """Holds the latest verdict and re-asks on an interval. Never repairs.

    Deliberately not consulted per request. ``/readyz`` reads whatever this last
    learned, so a burst of health checks cannot turn into a burst of AWS calls --
    and a probe that is slow or hanging cannot make the endpoint slow.
    """

    def __init__(
        self,
        expectations: ProbeExpectations,
        *,
        session_factory: Callable[..., Any] | None = None,
        interval_seconds: float | None = None,
        environ: Mapping[str, str] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        on_recovered: Callable[[], None] | None = None,
    ) -> None:
        if session_factory is None:
            import boto3

            session_factory = boto3.Session
        self._expectations = expectations
        self._session_factory = session_factory
        self._environ = environ
        self._monotonic = monotonic
        # Called once per bad-to-good transition. This module still repairs
        # nothing; it only tells whoever asked that the answer changed. The
        # readiness gate is the caller that needs it: it backs off from AWS
        # faults it cannot see the end of, and this is the only thing in the
        # process that finds out when they clear.
        self._on_recovered = on_recovered
        self._interval = (
            probe_interval_seconds(environ) if interval_seconds is None else interval_seconds
        )
        self._verdict = _UNKNOWN
        self._first_failure_at: float | None = None

    @property
    def interval_seconds(self) -> float:
        return self._interval

    def verdict(self) -> CredentialVerdict:
        """The current verdict, aged. A stale one is reported as stale, not as ok.

        Staleness is computed on read rather than written by the loop, because
        the case worth catching is the loop no longer running at all.
        """
        current = self._verdict
        checked = current.checked_at_monotonic
        if checked is None:
            return current
        if current.state in _TERMINAL_STATES:
            # Nothing is going to re-check this and nothing needs to: the answer
            # cannot change while the process lives. Ageing it into `stale` would
            # replace a precise verdict with a vaguer one.
            return current
        if self._monotonic() - checked > self._interval * STALE_AFTER_INTERVALS:
            return replace(
                current,
                state="stale",
                detail=_STALE_DETAIL,
                recovery=RecoveryState(
                    "given_up",
                    attempts=current.attempts,
                    detail=_STALE_DETAIL,
                    error="ProbeNotReporting",
                ),
            )
        return current

    def check_once(self) -> CredentialVerdict:
        """Run one probe synchronously and fold it into the recovery schedule."""
        try:
            fresh = probe_once(
                self._expectations,
                session_factory=self._session_factory,
                environ=self._environ,
            )
        except Exception as exc:  # noqa: BLE001 - the probe may never propagate
            # Not a credential verdict: the probe itself broke. Saying "your
            # credentials are bad" here would be a fabrication.
            LOGGER.warning("The AWS credential probe raised", exc_info=True)
            fresh = CredentialVerdict(
                state="unreachable",
                detail=(
                    "THE AWS CREDENTIAL PROBE FAILED TO RUN "
                    f"({type(exc).__name__}). The credentials are unverified; "
                    "this is a fault in the check, not a verdict about them."
                ),
            )
        now = self._monotonic()
        previous = self._verdict
        attempts = 0 if fresh.healthy else previous.attempts + 1
        self._verdict = replace(
            fresh,
            attempts=attempts,
            checked_at_monotonic=now,
            recovery=self._recovery_for(fresh, attempts, now),
        )
        # A transition, not a state: the first probe of a healthy process has
        # nothing to have recovered from, so `checked_at_monotonic` is what
        # separates "was broken, now works" from "has always worked".
        if (
            fresh.healthy
            and previous.checked_at_monotonic is not None
            and not previous.healthy
            and self._on_recovered is not None
        ):
            try:
                self._on_recovered()
            except Exception:  # noqa: BLE001 - an observer may never break the probe
                LOGGER.warning(
                    "A credential-recovery listener raised; the verdict stands",
                    exc_info=True,
                )
        return self._verdict

    def _recovery_for(
        self,
        fresh: CredentialVerdict,
        attempts: int,
        now: float,
    ) -> RecoveryState:
        if fresh.healthy:
            self._first_failure_at = None
            return RecoveryState("settled")
        if self._first_failure_at is None:
            self._first_failure_at = now
        if fresh.state in _TERMINAL_STATES:
            return RecoveryState(
                "given_up",
                attempts=attempts,
                detail=fresh.detail,
                error=fresh.state,
            )
        hard = fresh.state in _HARD_STATES
        outlasted = (
            attempts >= ESCALATE_AFTER_HARD_ATTEMPTS
            if hard
            else now - self._first_failure_at >= ESCALATE_AFTER_SECONDS
        )
        return RecoveryState(
            "escalated" if outlasted else "retrying",
            attempts=attempts,
            detail=fresh.detail,
            next_attempt_seconds=self._interval,
            error=fresh.state,
        )

    async def run(self) -> None:
        """Probe now, then on the interval, until cancelled.

        Stops only for a verdict nothing in this process can change. Everything
        else keeps being re-asked, because an operator who reattaches a policy
        should see the server go green without restarting it.
        """
        while True:
            verdict = await asyncio.to_thread(self.check_once)
            if not verdict.healthy:
                LOGGER.warning("AWS credential probe: %s · %s", verdict.state, verdict.detail)
            if verdict.state in _TERMINAL_STATES:
                LOGGER.warning(
                    "The AWS credential probe has stopped: %s cannot change "
                    "while this process is running.",
                    verdict.state,
                )
                return
            await asyncio.sleep(self._interval)


def expectations_from_manifest(manifest: Any) -> ProbeExpectations:
    """Read what to hold AWS against out of the sealed manifest.

    Tolerant by design about Round 5: an installation without it, or with the
    legacy shape, simply has one fewer thing to compare, and that must not stop
    the two checks that do apply.
    """
    round5 = getattr(manifest, "round5", None)
    trusted = getattr(round5, "control_role_trusted_principal_arn", None)
    runtime_trusted = getattr(manifest.aws, "runtime_role_trusted_principal_arns", None)
    return ProbeExpectations(
        region=manifest.aws.region,
        account_id=manifest.aws.account_id,
        round5_trusted_principal_arn=str(trusted) if trusted else None,
        runtime_role_trusted_principal_arns=tuple(str(arn) for arn in (runtime_trusted or ())),
    )


def effective_credential_verdict(state: Any) -> CredentialVerdict | None:
    """The credential answer every surface in this process must reason from.

    ``state`` is a FastAPI ``app.state``: this reads the running probe off it and
    the deployed startup check's refusal, and applies the one rule that decides
    between them. Two sources, and the probe outranks the startup check whenever
    it has something to say.

    The rule lives here rather than at each call site because there are two call
    sites, they are the two surfaces this repository has already had to reconcile
    once, and a copy each is how they would drift back apart. ``/readyz`` folds
    this into ``credentials_state``; the catalog folds it into whether an
    AWS-backed round is offered at all. A process where one of them could see a
    startup refusal and the other could not is a round shown green on the
    round-select screen while the health surface next door reports that every AWS
    lane is down -- and that was not hypothetical: the catalog read only the
    probe, and was covered only by a deployed network refusal that stopped being
    unconditional the moment an installation could seal the published egress
    prefixes.

    The direction of the rule matters as much as its existence. The probe is the
    only source that ever re-asks AWS, so it is the only one that can report the
    fault clearing; pinning a boot-time refusal over it would turn a degraded
    start into a permanent one that no published key could fix. ``unknown``
    counts as "nothing to say" rather than as an answer, because it is what a
    probe reports before its first check returns -- reading it as news would let
    the unremarkable state of a two-second-old process outrank a refusal AWS has
    already given, which is the window this defect lived in.

    Never raises. Both callers have to answer, and a probe accessor that faults
    is not allowed to take a health check or the round-select screen with it.
    """

    sentry = getattr(state, "credential_sentry", None)
    verdict: CredentialVerdict | None = None
    if sentry is not None:
        try:
            verdict = sentry.verdict()
        except Exception:  # noqa: BLE001 - a surface reading this may never fail
            LOGGER.warning("Could not read the AWS credential verdict", exc_info=True)
            verdict = None
    if verdict is not None and verdict.state != "unknown":
        return verdict
    startup = getattr(state, "startup_credential_verdict", None)
    if startup is not None:
        return startup
    # Either `unknown` from a probe that has not finished, or None from a process
    # that runs none. Both are passed through as themselves: a caller that has to
    # distinguish "nobody has looked" from "the probe is new" still can.
    return verdict
