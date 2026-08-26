"""Reap this installation's own leaked per-bout AWS clones, at startup.

The incident. A Round 2 bout creates an Aurora clone, uses it for about six
minutes, and deletes it. ``CreateDBInstance`` is wrapped in ``asyncio.shield``,
so a uvicorn process that dies mid-bout still gets its writer built; the
``_wait_for`` polling that leads to teardown is not shielded and dies with the
process. One such writer lived fifty-seven minutes instead of six and cost more
than every other bout that day combined. Nothing noticed until an unrelated
``arm()`` happened to sweep it.

Why startup and not shutdown. A dying process is the worst possible place to run
destructive AWS calls: no time, an event loop being torn down, and cancellation
racing every await. A freshly started one has all three. So a new process cleans
up after the previous one's death, and the shield asymmetry is left exactly as
it is.

That argument has since been narrowed rather than overturned, and the two halves
are complementary -- neither one makes the other redundant. Rounds 2, 3 and 5 now
do issue teardown on the cancellation path, each inside a short bounded shield
that logs an ``ORPHAN RISK`` line naming the resource if the bound expires. That
covers an orderly Ctrl-C, which is the common case, and it covers it sooner than
the next startup can. It cannot cover ``SIGKILL``, a segfault, an OOM kill, a
container eviction, or a host that simply goes away: there is no code path left
in the process to run, no log line to emit, and nothing to bound. Those are the
cases this module exists for, and they are precisely the ones the fifty-seven
minute writer arrived through. Deleting this because cancellation is handled now
would remove the only cover for the failures that handling cannot reach.

Why this is the dangerous half of the fix. "Delete every resource carrying our
tag" would destroy live infrastructure belonging to a bout that is still running
somewhere else, and this project has a verified isolation model that says that
must not happen: rounds run independently, the same round is locked, multiple
app installs in one workspace do not step on each other, and multiple users on
one install do not collide. A tag-based sweep breaks the last two immediately.

So every deletion has to clear all of these, and any one of them failing means
report and move on:

* the finding is ``ORPHAN_EPHEMERAL`` for *this* manifest's run ID -- never
  foreign-run residue, never an unexpected tagged resource, never a missing
  resident, never address drift;
* the identifier is independently re-derived as a per-bout artifact of this run,
  rather than trusted from the finding alone;
* the identifier is not a resident the seal expects to exist;
* the kind is an RDS instance or cluster, so the standing m6i.large runner is
  unreachable from here by construction;
* no ring lease covers the round that owns it, on either the round's own key or
  the main key, because a held lease means a bout may genuinely still be running;
* no other server process for this installation is alive, so a ``--reload``
  restart or a second replica cannot delete a writer out from under a live bout;
* the resource is provably older than a full bout, which is what turns "this
  exists" into "the bout that owned it is over".

Fail safe throughout. Reconciliation that could not reach the account, a lease
store that raised, a resource whose age cannot be read -- each of those is a
refusal, not a deletion, and none of them may stop the server from starting. A
cost problem must never be converted into an outage.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from .coordination import RING_KEY, round_ring_key
from .manifest import DemoManifest
from .process_registry import inspect_record, serving_endpoint, state_dir_from_environ
from .reconcile import (
    AURORA_CLUSTER,
    AURORA_WRITER,
    ORPHAN_EPHEMERAL,
    RDS_INSTANCE,
    Finding,
    ObservedResource,
    ReconciliationReport,
    ephemeral_artifact_ids,
    expected_resources,
)

LOGGER = logging.getLogger(__name__)

MODE_ENV = "ANTI_DEMO_STARTUP_REAP"
MIN_AGE_ENV = "ANTI_DEMO_STARTUP_REAP_MIN_AGE_SECONDS"
TIMEOUT_ENV = "ANTI_DEMO_STARTUP_REAP_TIMEOUT_SECONDS"

MODE_OFF = "off"
MODE_REPORT = "report"
MODE_DELETE = "delete"

#: Report-only unless an operator opts in. A sweep that deletes by default would
#: fire on every ``uvicorn --reload`` in a shared sandbox account, which is the
#: same class of unattended destruction this module exists to prevent. The leak
#: that motivated it was expensive because it was *invisible*, and report-only
#: already ends the invisibility.
DEFAULT_MODE = MODE_REPORT

#: A bout runs about six minutes. Fifteen leaves a wide margin for a slow clone
#: plus a slow teardown, so a resource older than this cannot belong to a bout
#: that started after the most recent lease observation.
DEFAULT_MIN_AGE_SECONDS = 900.0

#: The whole sweep is a handful of describe calls plus fire-and-forget deletes.
#: Bounded so a hung control plane delays startup by seconds, not forever.
DEFAULT_TIMEOUT_SECONDS = 60.0

#: Only these can ever be deleted here. The EC2 runner is standing infrastructure
#: and is excluded by kind, not merely by the resident check.
_DELETABLE_KINDS = frozenset({AURORA_WRITER, AURORA_CLUSTER, RDS_INSTANCE})

#: Writers hold the cluster open, so they go first. Anything else keeps its
#: relative order behind them.
_DELETION_ORDER = {AURORA_WRITER: 0, RDS_INSTANCE: 1, AURORA_CLUSTER: 2}

ACTION_DELETED = "deleted"
ACTION_WOULD_DELETE = "would_delete"
ACTION_REFUSED = "refused"


def reap_mode(environ: dict[str, str] | None = None) -> str:
    """Resolve the configured sweep mode, defaulting to report-only.

    An unrecognised value is treated as ``off`` rather than as the default: a
    typo in a destructive setting should quieten the sweep, never arm it.
    """

    environ = os.environ if environ is None else environ
    raw = (environ.get(MODE_ENV) or "").strip().casefold()
    if not raw:
        return DEFAULT_MODE
    if raw in {MODE_REPORT, "report-only", "dry-run", "inventory"}:
        return MODE_REPORT
    if raw in {MODE_DELETE, "reap", "yes", "true", "1"}:
        return MODE_DELETE
    return MODE_OFF


def _positive_float(environ: dict[str, str], name: str, fallback: float) -> float:
    raw = (environ.get(name) or "").strip()
    try:
        value = float(raw) if raw else fallback
    except ValueError:
        return fallback
    return value if value > 0 else fallback


def minimum_age_seconds(environ: dict[str, str] | None = None) -> float:
    environ = os.environ if environ is None else environ
    return _positive_float(environ, MIN_AGE_ENV, DEFAULT_MIN_AGE_SECONDS)


def sweep_timeout_seconds(environ: dict[str, str] | None = None) -> float:
    environ = os.environ if environ is None else environ
    return _positive_float(environ, TIMEOUT_ENV, DEFAULT_TIMEOUT_SECONDS)


@dataclass(frozen=True, slots=True)
class ReapDecision:
    """What the sweep concluded about exactly one resource, and why."""

    identifier: str
    kind: str
    round_key: str
    action: str
    reason: str
    usd_per_day: Decimal = Decimal(0)
    age_seconds: float | None = None

    @property
    def deleted(self) -> bool:
        return self.action == ACTION_DELETED

    def line(self) -> str:
        age = (
            f"{self.age_seconds / 60:.1f} min old"
            if self.age_seconds is not None
            else "age unknown"
        )
        return (
            f"REAP {self.action.upper()} {self.kind}={self.identifier} "
            f"round={self.round_key} · {age} · "
            f"${self.usd_per_day:.4f}/day carried · {self.reason}"
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "identifier": self.identifier,
            "kind": self.kind,
            "round_key": self.round_key,
            "action": self.action,
            "reason": self.reason,
            "usd_per_day": f"{self.usd_per_day:.6f}",
            "age_seconds": self.age_seconds,
        }


@dataclass(frozen=True, slots=True)
class ReapReport:
    """One sweep, in full: what it looked at, what it did, and what stopped it."""

    mode: str
    run_id: str = ""
    ran: bool = False
    unavailable: str = ""
    #: True when the sweep started and something went wrong, as opposed to never
    #: having had anything to do. Only the former belongs in the audit log.
    failed: bool = False
    #: True when the reason for ``failed`` is the sweep itself rather than the
    #: environment it ran in. The distinction is not cosmetic: "no usable AWS
    #: session" is the ordinary state of this install between logins and is
    #: already reported by the credential verdict, whereas "the safety net
    #: raised" is reported by nothing else and is the only one worth waking
    #: somebody for. Collapsing them would make the loud signal fire daily and
    #: therefore stop being read, which is how the leak went unnoticed before.
    broken: bool = False
    decisions: tuple[ReapDecision, ...] = ()
    observed_orphans: int = 0
    started_at: str = ""
    finished_at: str = ""
    carried: dict[str, object] = field(default_factory=dict)

    @property
    def deleted(self) -> tuple[ReapDecision, ...]:
        return tuple(decision for decision in self.decisions if decision.deleted)

    @property
    def refused(self) -> tuple[ReapDecision, ...]:
        return tuple(
            decision for decision in self.decisions if decision.action == ACTION_REFUSED
        )

    @property
    def reclaimed_usd_per_day(self) -> Decimal:
        return sum((decision.usd_per_day for decision in self.deleted), Decimal(0))

    @property
    def notable(self) -> bool:
        """Is this worth a line in the durable audit log?

        A sweep that examined the account is always worth recording, as is one
        that tried and could not. A sweep that never started because there is no
        owned manifest is not: that is the ordinary state of a process serving an
        installation it does not own, it happens on every such startup, and
        journaling it would bury the entries that matter.
        """

        return bool(self.decisions) or self.ran or self.failed

    def summary(self) -> str:
        if self.mode == MODE_OFF:
            return f"startup reap disabled via {MODE_ENV}"
        if not self.ran:
            return f"startup reap did not run: {self.unavailable or 'no reason recorded'}"
        if not self.decisions:
            return "startup reap found no ephemeral orphans for this run"
        return (
            f"startup reap in {self.mode} mode: {len(self.deleted)} deleted, "
            f"{len(self.refused)} refused, "
            f"{len(self.decisions) - len(self.deleted) - len(self.refused)} eligible, "
            f"${self.reclaimed_usd_per_day:.4f}/day reclaimed"
        )

    def report_lines(self) -> tuple[str, ...]:
        return tuple(decision.line() for decision in self.decisions)

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "run_id": self.run_id,
            "ran": self.ran,
            "failed": self.failed,
            "broken": self.broken,
            "unavailable": self.unavailable,
            # Null, never zero, when the sweep never looked. A count of 0 beside
            # `"ran": false` reads as "I looked and found nothing" to every
            # consumer that does not also check the flag, and the 03:39Z record
            # in `startup-reap.jsonl` is exactly that sentence written down.
            "observed_orphans": self.observed_orphans if self.ran else None,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "summary": self.summary(),
            "reclaimed_usd_per_day": f"{self.reclaimed_usd_per_day:.6f}",
            "decisions": [decision.as_dict() for decision in self.decisions],
            **self.carried,
        }


#: The six answers a status surface can honestly give about the sweep. Five of
#: them are not "it is fine", and only one of them is a green light.
REAP_DISABLED = "disabled"
REAP_NOT_STARTED = "not_started"
REAP_SKIPPED = "skipped"
#: Tried and could not see the account. Expected on this install between SSO
#: logins, and owned by the credential verdict rather than by this signal.
REAP_UNAVAILABLE = "unavailable"
#: Tried and the sweep itself failed. Nothing else reports this.
REAP_BROKEN = "broken"
REAP_SWEPT = "swept"


@dataclass(frozen=True, slots=True)
class ReapHealth:
    """What a status surface may say about the sweep, and nothing more.

    The sweep runs exactly once per process and its report was already being
    kept on ``app.state``; nothing read it, so a reaper that failed was visible
    only in a log line. This is the projection a health endpoint can render.

    ``observed_orphans`` is ``None`` rather than ``0`` whenever the sweep did
    not run, for the same reason :meth:`ReapReport.as_dict` nulls it: a zero
    from a sweep that never looked is indistinguishable from a clean account.
    """

    state: str
    detail: str
    observed_orphans: int | None = None

    @property
    def failed(self) -> bool:
        """Did the sweep try and not succeed, for any reason?"""

        return self.state in {REAP_UNAVAILABLE, REAP_BROKEN}

    @property
    def broken(self) -> bool:
        """Is the sweep itself at fault, rather than the account being unreadable?

        Only this warrants degrading a health surface. An unreadable account is
        already the credential verdict's answer, and degrading on it too would
        leave this box permanently degraded on an install whose SSO session
        expires every few hours -- at which point nobody reads the field and the
        signal has cost more than it bought.
        """

        return self.state == REAP_BROKEN


def reap_health(report: ReapReport | None) -> ReapHealth:
    """Classify one process's startup sweep for a status surface.

    ``None`` means the sweep has not been reached yet in this process, which is
    a distinct answer from every other one here and deliberately not a failure:
    it is the ordinary state of the seconds before startup finishes.
    """

    if report is None:
        return ReapHealth(
            REAP_NOT_STARTED,
            "the startup orphan sweep has not run in this process yet",
        )
    if report.mode == MODE_OFF:
        return ReapHealth(REAP_DISABLED, report.summary())
    if report.broken:
        return ReapHealth(REAP_BROKEN, report.summary())
    if report.failed:
        return ReapHealth(REAP_UNAVAILABLE, report.summary())
    if not report.ran:
        return ReapHealth(REAP_SKIPPED, report.summary())
    return ReapHealth(REAP_SWEPT, report.summary(), report.observed_orphans)


class OrphanDeleter(Protocol):
    async def delete(self, resource: ObservedResource) -> str: ...


class AwsOrphanDeleter:
    """Issue the delete and return; the sweep does not wait out the teardown.

    Both calls mirror what a round's own cleanup issues, including
    ``SkipFinalSnapshot`` -- a per-bout clone's whole point is that it is
    disposable, and a retained snapshot would be a second thing to leak.
    """

    def __init__(self, session: Any) -> None:
        self._session = session

    async def delete(self, resource: ObservedResource) -> str:
        client = self._session.client("rds")
        if resource.kind == AURORA_CLUSTER:
            await asyncio.to_thread(
                client.delete_db_cluster,
                DBClusterIdentifier=resource.identifier,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
            return "RDS.DeleteDBCluster"
        if resource.kind in {AURORA_WRITER, RDS_INSTANCE}:
            await asyncio.to_thread(
                client.delete_db_instance,
                DBInstanceIdentifier=resource.identifier,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
            return "RDS.DeleteDBInstance"
        raise RuntimeError(f"refusing to delete unsupported kind {resource.kind}")


def _ring_keys(manifest: DemoManifest, round_key: str) -> tuple[str, ...]:
    """Every ring a bout on this round could be holding.

    A v7 install fences each round separately, but the main ring still exists
    and startup cleanup claims it. Checking both means the sweep refuses under
    either coordination shape rather than only the one it expected.
    """

    keys = [RING_KEY]
    installation_id = getattr(manifest, "installation_id", None)
    if getattr(manifest, "manifest_version", 0) == 7 and installation_id:
        try:
            keys.append(round_ring_key(installation_id, round_key))
        except ValueError:
            # An unroutable round key is not proof that nothing holds the ring.
            raise
    return tuple(dict.fromkeys(keys))


async def _held_ring(lease_store: Any, ring_keys: Sequence[str]) -> str:
    """Name the first ring found held, or empty when every ring is free.

    Raises rather than returning "free" when a ring cannot be read: an
    unanswerable lease question must veto, and only the caller knows that.
    """

    for ring_key in ring_keys:
        scoped = (
            lease_store
            if getattr(lease_store, "ring_key", None) == ring_key
            else lease_store.for_ring_key(ring_key)
        )
        active = await scoped.current()
        if active is not None:
            owner = (active.operator.display_name or active.owner_subject or "unknown").strip()
            return (
                f"{ring_key} held by {owner} in phase {active.phase} "
                f"(fence {active.fencing_token})"
            )
    return ""


def predecessor_verdict(
    *,
    state_dir: Path | None = None,
    argv: Sequence[str] | None = None,
    environ: dict[str, str] | None = None,
    **probes: Any,
) -> tuple[bool, str]:
    """Decide whether this process is alone on its install's port.

    Only two record states prove there is no live predecessor: no record at all,
    and a record whose pid has exited. Everything else -- a live pid, a pid that
    could not be identified, a pidfile that disagrees with the launch record --
    leaves open the possibility that another process on this installation is
    mid-bout, so the sweep declines.
    """

    environ = os.environ if environ is None else environ
    resolved = state_dir_from_environ(environ) if state_dir is None else state_dir
    if resolved is None or not resolved.is_dir():
        return False, "no state directory; cannot prove this process is alone"
    _, port = serving_endpoint(list(argv) if argv is not None else None, environ)
    status = inspect_record(resolved, port, **probes)
    if status.state == "absent":
        return True, f"no launch record for port {port}"
    if status.state == "exited":
        return True, f"predecessor on port {port} has exited: {status.detail}"
    return False, f"another process may be serving port {port}: {status.detail}"


def plan_reap(
    manifest: DemoManifest,
    report: ReconciliationReport,
    *,
    now: datetime,
    minimum_age: float,
) -> tuple[list[tuple[Finding, ObservedResource, str]], list[ReapDecision]]:
    """Split reconciled findings into deletion candidates and recorded refusals.

    Pure, so the whole eligibility policy can be exercised without a lease store,
    an account, or a clock. Everything that is not an ephemeral orphan of this
    run, old enough, and of a deletable kind comes back as a refusal carrying the
    reason, because a silent omission is indistinguishable from a bug.
    """

    ephemeral = ephemeral_artifact_ids(manifest.run_id)
    residents = {resource.identifier for resource in expected_resources(manifest)}
    observed = {resource.identifier: resource for resource in report.observed}

    candidates: list[tuple[Finding, ObservedResource, str]] = []
    refusals: list[ReapDecision] = []

    for finding in report.findings:
        if finding.code != ORPHAN_EPHEMERAL:
            if finding.is_orphan:
                refusals.append(
                    ReapDecision(
                        finding.identifier,
                        finding.kind,
                        "",
                        ACTION_REFUSED,
                        f"{finding.code} is report-only; only this run's ephemeral "
                        "clones are ever reaped",
                        finding.usd_per_day,
                    )
                )
            continue

        resource = observed.get(finding.identifier)
        round_key = ephemeral.get(finding.identifier, "")
        if resource is None:
            refusals.append(
                ReapDecision(
                    finding.identifier,
                    finding.kind,
                    round_key,
                    ACTION_REFUSED,
                    "no observed resource backs this finding",
                    finding.usd_per_day,
                )
            )
            continue

        age = resource.age_seconds(now)
        veto = _static_veto(
            resource,
            round_key=round_key,
            run_id=manifest.run_id,
            residents=residents,
            age=age,
            minimum_age=minimum_age,
        )
        if veto:
            refusals.append(
                ReapDecision(
                    resource.identifier,
                    resource.kind,
                    round_key,
                    ACTION_REFUSED,
                    veto,
                    finding.usd_per_day,
                    age,
                )
            )
            continue
        candidates.append((finding, resource, round_key))

    candidates.sort(key=lambda item: (_DELETION_ORDER.get(item[1].kind, 9), item[1].identifier))
    return candidates, refusals


def _static_veto(
    resource: ObservedResource,
    *,
    round_key: str,
    run_id: str,
    residents: frozenset[str] | set[str],
    age: float | None,
    minimum_age: float,
) -> str:
    """Every reason to refuse that needs no network call. Empty means eligible."""

    if not round_key:
        return "identifier is not a per-bout artifact of this run"
    if resource.run_id != run_id:
        return f"tagged for run {resource.run_id or '(untagged)'}, not {run_id}"
    if resource.identifier in residents:
        return "the seal expects this resource to exist; residents are never reaped"
    if resource.kind not in _DELETABLE_KINDS:
        return f"{resource.kind} is standing infrastructure and is never reaped here"
    if resource.retiring:
        return f"already {resource.status}; teardown is in progress"
    if age is None:
        return "creation time is unreadable, so the owning bout cannot be proved over"
    if age < minimum_age:
        return (
            f"only {age / 60:.1f} min old against a {minimum_age / 60:.1f} min floor; "
            "a bout could still own it"
        )
    return ""


def write_audit(report: ReapReport, *, state_dir: Path | None = None) -> Path | None:
    """Append the sweep to a durable log so it is inspectable after the fact.

    Best effort by design. A sweep that could not write its own audit trail has
    still already logged every line, and failing startup over a log file would
    be the outage this module refuses to cause.
    """

    resolved = state_dir_from_environ() if state_dir is None else state_dir
    if resolved is None or not resolved.is_dir():
        return None
    path = resolved / "startup-reap.jsonl"
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(report.as_dict(), sort_keys=True) + "\n")
        path.chmod(0o600)
    except OSError:
        LOGGER.warning("Could not write the startup reap audit log", exc_info=True)
        return None
    return path


async def reap_startup_orphans(
    manifest: DemoManifest | None,
    *,
    reconcile: Any,
    lease_store: Any,
    deleter: OrphanDeleter | None = None,
    environ: dict[str, str] | None = None,
    now: datetime | None = None,
    state_dir: Path | None = None,
    argv: Sequence[str] | None = None,
    predecessor: tuple[bool, str] | None = None,
    audit: bool = True,
) -> ReapReport:
    """Sweep this run's leaked per-bout clones. Never raises.

    ``reconcile`` is a zero-argument callable returning a
    :class:`~server.reconcile.ReconciliationReport`; injecting it keeps the AWS
    session out of this module's contract and lets a test describe an account it
    does not have.
    """

    environ = os.environ if environ is None else environ
    mode = reap_mode(environ)
    started = datetime.now(UTC).isoformat()
    if mode == MODE_OFF:
        outcome = ReapReport(mode=mode, started_at=started, finished_at=started)
        LOGGER.info("REAP %s", outcome.summary())
        return outcome

    try:
        async with asyncio.timeout(sweep_timeout_seconds(environ)):
            outcome = await _sweep(
                manifest,
                mode=mode,
                reconcile=reconcile,
                lease_store=lease_store,
                deleter=deleter,
                environ=environ,
                now=now or datetime.now(UTC),
                state_dir=state_dir,
                argv=argv,
                predecessor=predecessor,
                started_at=started,
            )
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001 - startup must survive anything here
        outcome = ReapReport(
            mode=mode,
            run_id=getattr(manifest, "run_id", "") or "",
            unavailable=f"{type(exc).__name__}: {exc}",
            failed=True,
            # An unreachable account is a *return* from `_sweep`, not an
            # exception, so an exception arriving here is the sweep failing
            # rather than the environment refusing to answer.
            broken=True,
            started_at=started,
            finished_at=datetime.now(UTC).isoformat(),
        )
        LOGGER.warning("REAP %s", outcome.summary(), exc_info=True)

    for line in outcome.report_lines():
        LOGGER.warning("%s", line)
    if outcome.ran:
        LOGGER.info("REAP %s", outcome.summary())
    if audit and outcome.notable:
        write_audit(outcome, state_dir=state_dir)
    return outcome


async def _sweep(
    manifest: DemoManifest | None,
    *,
    mode: str,
    reconcile: Any,
    lease_store: Any,
    deleter: OrphanDeleter | None,
    environ: dict[str, str],
    now: datetime,
    state_dir: Path | None,
    argv: Sequence[str] | None,
    predecessor: tuple[bool, str] | None,
    started_at: str,
) -> ReapReport:
    def finish(**values: Any) -> ReapReport:
        return ReapReport(
            mode=mode,
            run_id=getattr(manifest, "run_id", "") or "",
            started_at=started_at,
            finished_at=datetime.now(UTC).isoformat(),
            **values,
        )

    if manifest is None:
        return finish(unavailable="no owned manifest is loaded")
    if lease_store is None:
        return finish(unavailable="no lease store; a held bout could not be ruled out")

    report = await _call(reconcile)
    if report.unavailable:
        # A partial inventory cannot prove anything is an orphan. Half an
        # account read looks exactly like an account with fewer resources.
        return finish(
            unavailable=f"reconciliation unavailable: {report.unavailable}", failed=True
        )

    alone, predecessor_detail = (
        predecessor
        if predecessor is not None
        else predecessor_verdict(state_dir=state_dir, argv=argv, environ=environ)
    )

    candidates, refusals = plan_reap(
        manifest,
        report,
        now=now,
        minimum_age=minimum_age_seconds(environ),
    )
    decisions: list[ReapDecision] = list(refusals)
    orphan_count = len(report.orphans)

    for finding, resource, round_key in candidates:
        age = resource.age_seconds(now)
        common = (resource.identifier, resource.kind, round_key)

        if not alone:
            decisions.append(
                ReapDecision(
                    *common, ACTION_REFUSED, predecessor_detail, finding.usd_per_day, age
                )
            )
            continue

        try:
            held = await _held_ring(lease_store, _ring_keys(manifest, round_key))
        except Exception as exc:  # noqa: BLE001 - an unreadable lease must veto
            decisions.append(
                ReapDecision(
                    *common,
                    ACTION_REFUSED,
                    f"ring lease could not be read ({type(exc).__name__}: {exc})",
                    finding.usd_per_day,
                    age,
                )
            )
            continue
        if held:
            decisions.append(
                ReapDecision(
                    *common,
                    ACTION_REFUSED,
                    f"a bout may still be running: {held}",
                    finding.usd_per_day,
                    age,
                )
            )
            continue

        if mode != MODE_DELETE:
            decisions.append(
                ReapDecision(
                    *common,
                    ACTION_WOULD_DELETE,
                    f"eligible; set {MODE_ENV}={MODE_DELETE} to reap it",
                    finding.usd_per_day,
                    age,
                )
            )
            continue
        if deleter is None:
            decisions.append(
                ReapDecision(
                    *common,
                    ACTION_REFUSED,
                    "no AWS deleter is configured for this runtime",
                    finding.usd_per_day,
                    age,
                )
            )
            continue

        try:
            call = await deleter.delete(resource)
        except Exception as exc:  # noqa: BLE001 - one failure must not stop the rest
            decisions.append(
                ReapDecision(
                    *common,
                    ACTION_REFUSED,
                    f"delete failed ({type(exc).__name__}: {exc})",
                    finding.usd_per_day,
                    age,
                )
            )
            continue
        decisions.append(
            ReapDecision(
                *common,
                ACTION_DELETED,
                f"{call} issued · {finding.detail} · {finding.basis}",
                finding.usd_per_day,
                age,
            )
        )

    return finish(
        ran=True,
        decisions=tuple(decisions),
        observed_orphans=orphan_count,
        carried={"predecessor": predecessor_detail, "alone": alone},
    )


async def _call(operation: Any) -> Any:
    result = operation()
    if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
        return await result
    return result


def format_report(report: ReapReport) -> Iterable[str]:
    """Operator-facing rendering, used by the CLI and by tests."""

    yield f"REAP {report.summary()}"
    yield from report.report_lines()


__all__ = [
    "ACTION_DELETED",
    "ACTION_REFUSED",
    "ACTION_WOULD_DELETE",
    "DEFAULT_MIN_AGE_SECONDS",
    "MODE_DELETE",
    "MODE_ENV",
    "MODE_OFF",
    "MODE_REPORT",
    "REAP_BROKEN",
    "REAP_DISABLED",
    "REAP_NOT_STARTED",
    "REAP_SKIPPED",
    "REAP_SWEPT",
    "REAP_UNAVAILABLE",
    "AwsOrphanDeleter",
    "ReapDecision",
    "ReapHealth",
    "ReapReport",
    "format_report",
    "minimum_age_seconds",
    "plan_reap",
    "predecessor_verdict",
    "reap_health",
    "reap_mode",
    "reap_startup_orphans",
    "write_audit",
]
