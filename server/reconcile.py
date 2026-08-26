"""Compare the AWS resources this installation is really running against its seal.

Rounds 2 and 3 restore a point-in-time clone, use it for about six minutes, and
delete it. That last step is the only thing between a six-minute artifact and an
open-ended one, and nothing in the runtime notices when it is skipped. A bout
whose process dies after the create call but before the delete leaves a writer
running with no owner and no clock; the first place it becomes visible is a bill,
weeks later.

This module gives that failure a name at the moment it happens. Every AWS
resource this demo creates carries an ``anti-demo-run-id`` tag, whether Terraform
made it or a round did, so ownership is decided by tag rather than by name. That
matters twice over: the sandbox account is shared, and sweeping a neighbour's
database into a list titled "delete these" would be far worse than missing an
orphan; and a resource left behind by an *earlier* installation still costs money
under a run ID this manifest has never heard of.

Findings are returned, never raised. A drifted installation is exactly the one an
operator needs to inspect, and an inventory that aborts on the first surprise
takes that inspection away.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from .cost_model import AURORA_MAX_ACU, RateCard
from .manifest import DemoManifest
from .pricing import rds_instance_hour_usd

HOURS_PER_DAY = Decimal(24)

# Which question is being asked of a price. A leak is a resource someone is
# deciding whether to delete; a resident is one the seal says should be there.
# The same writer is worth a different number to each, and the default is the
# leak because that is the direction it is safe to be wrong in.
LEAK = "leak"
RESIDENT = "resident"

# "2" rather than "2.0": the label follows the sealed constant so it cannot go on
# saying two after the capacity range moves, and is rendered once so the two
# basis strings below cannot disagree with each other.
_AURORA_MAX_ACU_LABEL = f"{AURORA_MAX_ACU.normalize():f}"

TAG_RUN_ID = "anti-demo-run-id"

AURORA_CLUSTER = "aurora_cluster"
AURORA_WRITER = "aurora_writer"
RDS_INSTANCE = "rds_instance"
EC2_RUNNER = "ec2_runner"

ORPHAN_EPHEMERAL = "ORPHAN_EPHEMERAL"
ORPHAN_FOREIGN_RUN = "ORPHAN_FOREIGN_RUN"
ORPHAN_UNEXPECTED = "ORPHAN_UNEXPECTED"
MISSING_RESIDENT = "MISSING_RESIDENT"
IPV4_DRIFT = "IPV4_DRIFT"

_ORPHAN_CODES = frozenset({ORPHAN_EPHEMERAL, ORPHAN_FOREIGN_RUN, ORPHAN_UNEXPECTED})

# A resource already on its way out is not residue an operator needs to act on.
# Anything else that exists is billable right now.
_RETIRING = frozenset({"deleting", "deleted", "terminated", "shutting-down"})

# Round 2 and Round 3 name their per-bout clones deterministically from the run
# ID. Mirrored here rather than imported so that a reconciliation can still run
# when the live adapters cannot be constructed, which is the situation an
# operator is usually in when they reach for it.
_ROUND2_PREFIX = "adsc"
_ROUND3_PREFIX = "adrc"
_AWS_PROVIDERS = ("aurora", "rds")


def _aws_child_id(artifact_id: str, suffix: str) -> str:
    """Mirror ``server.safe_change_live._aws_child_id``.

    An Aurora clone's writer is named after its cluster and digest-truncated at
    the 63-character RDS limit.
    """

    candidate = f"{artifact_id}-{suffix}"
    if len(candidate) <= 63:
        return candidate
    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()[:10]
    return f"{artifact_id[: 63 - len(digest) - 1]}-{digest}".rstrip("-")


def ephemeral_artifact_ids(run_id: str) -> dict[str, str]:
    """Every per-bout AWS identifier a run is allowed to create, and its round.

    Presence of any of these between bouts is residue by definition: these
    resources are created after the bell and are supposed to be gone before the
    next one.
    """

    artifacts: dict[str, str] = {}
    for prefix, round_key in (
        (_ROUND2_PREFIX, "make_schema_change_safely"),
        (_ROUND3_PREFIX, "recover_deleted_order"),
    ):
        for provider in _AWS_PROVIDERS:
            artifact = f"{prefix}-{run_id}-{provider}"
            artifacts[artifact] = round_key
            if provider == "aurora":
                artifacts[_aws_child_id(artifact, "writer")] = round_key
    return artifacts


@dataclass(frozen=True, slots=True)
class ExpectedResource:
    """One resource the seal says should exist between bouts."""

    kind: str
    identifier: str
    round_key: str
    # Every sealed database is reachable from the operator CIDR and the runner
    # holds an auto-assigned address, so each resident resource accounts for
    # exactly one chargeable address. A resource that stops being reachable is
    # drift from the sealed shape and a change in cost, so reporting it is right.
    public_ipv4: bool = True


@dataclass(frozen=True, slots=True)
class ObservedResource:
    """One demo-tagged resource the account is really running."""

    kind: str
    identifier: str
    status: str
    run_id: str = ""
    public_ipv4: bool = False
    instance_class: str = ""
    # When the account says this resource was created. ``None`` means the
    # description carried no usable timestamp, which is a different answer from
    # "new": a reaper that cannot age a resource must not assume it is old.
    created_at: datetime | None = None
    # The average ACU the account reported for this writer over the sample
    # window, as a ``Decimal``. ``None`` means nothing measured it on this run,
    # which is a different answer from zero -- an unmeasured writer must not
    # price as free.
    observed_acu: Decimal | None = None

    @property
    def retiring(self) -> bool:
        return self.status.strip().lower() in _RETIRING

    def age_seconds(self, now: datetime) -> float | None:
        """How long this resource has existed, or None when that is unknown."""

        if self.created_at is None:
            return None
        created = self.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        return (now.astimezone(UTC) - created.astimezone(UTC)).total_seconds()


@dataclass(frozen=True, slots=True)
class Finding:
    """One difference between the seal and the account, priced where possible."""

    code: str
    kind: str
    identifier: str
    detail: str
    usd_per_day: Decimal = Decimal(0)
    basis: str = ""

    @property
    def is_orphan(self) -> bool:
        return self.code in _ORPHAN_CODES

    def line(self) -> str:
        price = f" · ${self.usd_per_day:.4f}/day ({self.basis})" if self.usd_per_day else ""
        return f"{self.code} {self.kind}={self.identifier} · {self.detail}{price}"


def expected_resources(manifest: DemoManifest) -> tuple[ExpectedResource, ...]:
    """The resident AWS resources the manifest seals.

    Rounds 4 and 6 seal no Aurora or RDS block, so they contribute nothing. That
    is the manifest recording that those rounds have no AWS stack, not an
    omission on its part.
    """

    expected: list[ExpectedResource] = []
    for round_id, environment in (getattr(manifest, "round_environments", None) or {}).items():
        round_key = str(round_id)
        aurora = getattr(environment, "aurora", None)
        if aurora is not None:
            # A cluster is an addressless container; its writer carries the address.
            expected.append(
                ExpectedResource(AURORA_CLUSTER, aurora.cluster_id, round_key, public_ipv4=False)
            )
            expected.append(
                ExpectedResource(AURORA_WRITER, aurora.writer_instance_id, round_key)
            )
        rds = getattr(environment, "rds", None)
        if rds is not None:
            expected.append(ExpectedResource(RDS_INSTANCE, rds.instance_id, round_key))
    runner = getattr(getattr(manifest, "round5", None), "runner_instance_id", "") or ""
    if runner:
        expected.append(
            ExpectedResource(EC2_RUNNER, runner, "survive_connection_spike")
        )
    return tuple(expected)


def _rds_hour(resource: ObservedResource, rates: RateCard) -> tuple[Decimal, str]:
    """Price a leaked RDS instance by the class it actually reports.

    An orphan is not necessarily the configured class — during a resize the two
    differ, and that is exactly when someone is reading this. Falling back to the
    card is better than refusing, but the class that produced the number is named
    either way so the assumption is visible.
    """

    instance_class = resource.instance_class or rates.rds_instance_class
    try:
        return rds_instance_hour_usd(instance_class) * HOURS_PER_DAY, f"{instance_class} compute"
    except Exception:
        return Decimal(0), f"{instance_class} has no published rate; compute not priced"


def _aurora_ceiling(rates: RateCard) -> Decimal:
    """A full day of Aurora compute at the top of the sealed capacity range."""

    return rates.aurora_acu_hour.usd * AURORA_MAX_ACU * HOURS_PER_DAY


def _aurora_writer_cost(
    resource: ObservedResource, rates: RateCard, context: str
) -> tuple[Decimal, str]:
    """Price one Aurora writer against the question being asked of it.

    Aurora Serverless v2 bills per ACU-hour and a clone inherits the source's
    0-2 ACU range. The ceiling is not a mistake being corrected here. For a leak
    it is the right number and stays: overstating a resource you are deciding
    whether to delete errs toward action, and nothing can predict what a leaked
    writer does next. For a resident that a describe-* call just returned, the
    same figure is a multiple of what an observably idle fleet bills.
    """

    if context == RESIDENT and resource.observed_acu is not None:
        usd = rates.aurora_acu_hour.usd * resource.observed_acu * HOURS_PER_DAY
        return usd, f"measured: {resource.observed_acu} ACU average over the sample window"
    if context == RESIDENT:
        return (
            _aurora_ceiling(rates),
            f"ceiling: {_AURORA_MAX_ACU_LABEL} ACU for a full day; no ACU measurement "
            "was supplied for this resident, so this is an upper bound and not what "
            "the fleet is billing",
        )
    return _aurora_ceiling(rates), f"ceiling: {_AURORA_MAX_ACU_LABEL} ACU for a full day"


def _carrying_cost(
    resource: ObservedResource,
    rates: RateCard,
    *,
    context: str = LEAK,
) -> tuple[Decimal, str]:
    """Price one resource per day, and say what the price rests on.

    ``context`` defaults to :data:`LEAK`, so a caller that says nothing gets the
    upper bound. That is deliberate: the reaper and the orphan report are the
    callers that exist, and a resource nobody owns is the one case where being
    wrong high is better than being wrong low.

    **Nothing passes :data:`RESIDENT` in production, and the resident path cannot
    currently improve any number.** It is speculative machinery, kept because it
    is correct and tested, and labelled here because it reads like a fix for the
    resident overstatement and is not one. Three things would have to be true for
    it to matter, and today none of them is: a production caller would have to
    pass ``context=RESIDENT``; ``ObservedResource.observed_acu`` would have to be
    populated, which needs a CloudWatch read because ``describe-db-*`` does not
    carry current ACU; and until it is populated the ``observed_acu is None``
    branch of :func:`_aurora_writer_cost` returns the same ceiling anyway. So
    wiring a caller *without* building the ACU measurement would change nothing
    while appearing to change something.

    Note also that the one scenario in which a resident fleet does get priced --
    a re-seal that disowns surviving infrastructure -- is a scenario in which
    these resources are classified as *leaks*, so :data:`RESIDENT` is
    definitionally unavailable there. The lever in that case is classification,
    not this context.
    """

    kind = resource.kind
    if kind == AURORA_WRITER:
        usd, basis = _aurora_writer_cost(resource, rates, context)
    elif kind == RDS_INSTANCE:
        usd, basis = _rds_hour(resource, rates)
    elif kind == EC2_RUNNER:
        usd = rates.ec2_m6i_large_hour.usd * HOURS_PER_DAY
        basis = "m6i.large compute"
    elif kind == AURORA_CLUSTER:
        # A cluster with no writer parks at 0 ACU and bills only for storage,
        # which is cents. Reported so it is not lost, priced at zero so it is
        # not overstated.
        usd = Decimal(0)
        basis = "storage only; compute parks at 0 ACU"
    else:
        usd = Decimal(0)
        basis = "unpriced"
    if resource.public_ipv4:
        usd += rates.public_ipv4_hour.usd * HOURS_PER_DAY
        basis = f"{basis} + 1 public IPv4"
    return usd, basis


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    """What the seal expects, what the account runs, and the difference."""

    run_id: str
    expected: tuple[ExpectedResource, ...] = ()
    observed: tuple[ObservedResource, ...] = ()
    findings: tuple[Finding, ...] = ()
    expected_public_ipv4: int = 0
    observed_public_ipv4: int = 0
    unavailable: str = ""

    @property
    def orphans(self) -> tuple[Finding, ...]:
        return tuple(finding for finding in self.findings if finding.is_orphan)

    @property
    def missing(self) -> tuple[Finding, ...]:
        return tuple(f for f in self.findings if f.code == MISSING_RESIDENT)

    @property
    def orphan_usd_per_day(self) -> Decimal:
        return sum((finding.usd_per_day for finding in self.orphans), Decimal(0))

    @property
    def ok(self) -> bool:
        return not self.unavailable and not self.findings

    def summary(self) -> str:
        """One line, always. Silence would look the same as never having run."""

        if self.unavailable:
            return f"not reconciled: {self.unavailable}"
        if self.ok:
            return (
                f"{len(self.expected)} sealed AWS resources present, "
                f"{self.observed_public_ipv4} public IPv4, no orphans"
            )
        return (
            f"{len(self.orphans)} orphan(s), {len(self.missing)} missing, "
            f"public IPv4 {self.observed_public_ipv4} vs {self.expected_public_ipv4} expected, "
            f"orphan carrying cost ${self.orphan_usd_per_day:.4f}/day"
        )

    def report_lines(self) -> tuple[str, ...]:
        """Operator-facing detail, orphans first because they cost money now."""

        if self.unavailable or self.ok:
            return ()
        lines = [finding.line() for finding in self.findings]
        if self.orphans:
            lines.append(
                "Inventory only: nothing was changed. Deleting an orphan needs approval."
            )
        return tuple(lines)


#: The command that re-creates a swept installation.
INSTALLATION_REPAIR_COMMAND = "./antidemo setup"

PRESENCE_PRESENT = "verified_present"
PRESENCE_MISSING = "verified_missing"
PRESENCE_UNVERIFIED = "unverified"
PRESENCE_NEVER_CHECKED = "never_checked"


@dataclass(frozen=True, slots=True)
class InstallationPresence:
    """Whether the sealed residents are really in the account, and how sure of it.

    Three answers, and they must never collapse into two. "The account was read
    and they are gone" is a fact an operator has to act on; "the account could
    not be read" is an absence of knowledge that looks identical from a distance
    and means something completely different. Reporting the second as the first
    would be this project's worst recurring defect -- a surface stating health it
    never checked -- pointed in the more expensive direction, because the action
    it invites is a re-provision. For what that costs, read
    ``server.selfheal.daily_cost_usd``, which prices it from the same cost model
    the ringside disclosure quotes. It is deliberately not restated here: the
    figure that used to sit in this sentence was the folklore
    ``server.selfheal.COST_BASIS`` was written to retire.

    The distinction is not academic here. The sandbox reaper deletes the IAM
    users along with the databases, so a real sweep fails at the credential
    boundary and lands on ``unverified``, not ``missing``. Anything keyed only on
    ``missing`` would stay silent through the exact event it was built for.
    """

    state: str
    sealed: int = 0
    absent: int = 0
    #: Why the account could not be read. Empty unless `state` is `unverified`.
    reason: str = ""

    @property
    def verified_missing(self) -> bool:
        return self.state == PRESENCE_MISSING

    @property
    def checked(self) -> bool:
        return self.state in {PRESENCE_PRESENT, PRESENCE_MISSING}

    @property
    def detail(self) -> str:
        """One sentence, in the vocabulary /readyz and the CLI already use."""

        if self.state == PRESENCE_MISSING:
            return (
                f"THE SEALED AWS INFRASTRUCTURE IS GONE: the account was read and "
                f"{self.absent} of {self.sealed} sealed resources are absent from it. "
                f"Every round that connects to Aurora or RDS will fail until they "
                f"are re-created. Run '{INSTALLATION_REPAIR_COMMAND}', which "
                f"re-applies Terraform and reseeds. Nothing re-creates them from "
                f"inside the server, by design."
            )
        if self.state == PRESENCE_UNVERIFIED:
            return (
                f"THE SEALED AWS INFRASTRUCTURE COULD NOT BE CHECKED: "
                f"{self.reason}. This is not a report that anything is missing -- "
                f"the account was never read, so the {self.sealed} sealed resources "
                f"are neither confirmed present nor confirmed gone. Whatever stopped "
                f"the read is the thing to fix; this answers itself once it clears, "
                f"with no restart."
            )
        if self.state == PRESENCE_NEVER_CHECKED:
            return (
                f"THE SEALED AWS INFRASTRUCTURE HAS NOT BEEN CHECKED yet in this "
                f"process, so its {self.sealed} sealed resources are neither "
                f"confirmed present nor confirmed gone."
            )
        return f"all {self.sealed} sealed AWS resources are present in the account"

    @property
    def capabilities(self) -> tuple[str, ...]:
        """What is lost while this stands. Only ever claimed for a verified loss."""

        if self.state != PRESENCE_MISSING:
            return ()
        return (
            f"every round backed by Aurora or RDS -- {self.absent} of {self.sealed} "
            f"sealed AWS resources are absent from the account, so there is nothing "
            f"for those rounds to connect to",
        )


def presence_from_report(report: ReconciliationReport | None) -> InstallationPresence:
    """Classify a reconciliation into the three answers, plus "not yet asked".

    Pure and total, so every surface can render the same verdict without
    repeating the reasoning -- and, more to the point, without one surface
    quietly disagreeing with another about what "gone" means.

    ``None`` is the honest answer before any sweep has run, and it is deliberately
    not ``verified_present``.
    """

    if report is None:
        return InstallationPresence(PRESENCE_NEVER_CHECKED)
    sealed = len(report.expected)
    if report.unavailable:
        return InstallationPresence(
            PRESENCE_UNVERIFIED, sealed=sealed, reason=report.unavailable
        )
    absent = len(report.missing)
    if absent:
        return InstallationPresence(PRESENCE_MISSING, sealed=sealed, absent=absent)
    return InstallationPresence(PRESENCE_PRESENT, sealed=sealed)


def reconcile(
    manifest: DemoManifest,
    observed: Iterable[ObservedResource],
    *,
    rates: RateCard | None = None,
) -> ReconciliationReport:
    """Diff a live inventory against the sealed manifest. Pure, and total.

    Never raises on drift; drift is the output, not an error.
    """

    rates = rates or RateCard()
    expected = expected_resources(manifest)
    by_id = {resource.identifier: resource for resource in expected}
    ephemeral = ephemeral_artifact_ids(manifest.run_id)
    live = tuple(item for item in observed if not item.retiring)
    findings: list[Finding] = []
    seen: set[str] = set()

    for resource in live:
        seen.add(resource.identifier)
        if resource.identifier in by_id and resource.run_id == manifest.run_id:
            continue
        usd, basis = _carrying_cost(resource, rates)
        if resource.run_id and resource.run_id != manifest.run_id:
            findings.append(
                Finding(
                    ORPHAN_FOREIGN_RUN,
                    resource.kind,
                    resource.identifier,
                    f"tagged for run {resource.run_id}, which this manifest does not own "
                    f"(status {resource.status})",
                    usd,
                    basis,
                )
            )
        elif resource.identifier in ephemeral:
            findings.append(
                Finding(
                    ORPHAN_EPHEMERAL,
                    resource.kind,
                    resource.identifier,
                    f"per-bout {ephemeral[resource.identifier]} clone outlived its bout "
                    f"(status {resource.status})",
                    usd,
                    basis,
                )
            )
        else:
            findings.append(
                Finding(
                    ORPHAN_UNEXPECTED,
                    resource.kind,
                    resource.identifier,
                    f"carries this run's tag but is not in the seal (status {resource.status})",
                    usd,
                    basis,
                )
            )

    for resource in expected:
        if resource.identifier not in seen:
            findings.append(
                Finding(
                    MISSING_RESIDENT,
                    resource.kind,
                    resource.identifier,
                    f"sealed for {resource.round_key} but absent from the account",
                )
            )

    expected_ipv4 = sum(1 for resource in expected if resource.public_ipv4)
    observed_ipv4 = sum(1 for resource in live if resource.public_ipv4)
    if expected_ipv4 != observed_ipv4:
        findings.append(
            Finding(
                IPV4_DRIFT,
                "public_ipv4",
                str(observed_ipv4),
                f"{observed_ipv4} chargeable demo-tagged addresses, seal expects {expected_ipv4}",
                abs(observed_ipv4 - expected_ipv4)
                * rates.public_ipv4_hour.usd
                * HOURS_PER_DAY,
                "per address",
            )
        )

    findings.sort(key=lambda f: (not f.is_orphan, f.code, f.identifier))
    return ReconciliationReport(
        run_id=manifest.run_id,
        expected=expected,
        observed=live,
        findings=tuple(findings),
        expected_public_ipv4=expected_ipv4,
        observed_public_ipv4=observed_ipv4,
    )


def _created_at(value: object) -> datetime | None:
    """Read a creation timestamp out of a description, or admit it is unreadable.

    boto3 hands back aware datetimes, but a hand-built fixture or a future API
    revision may not. Returning None rather than guessing keeps "unknown age"
    distinguishable from "created just now", which is the distinction a reaper
    has to get right.
    """

    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return None


def _tags(items: object) -> dict[str, str]:
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes)):
        return {}
    tags: dict[str, str] = {}
    for item in items:
        if isinstance(item, Mapping):
            key = str(item.get("Key") or "")
            if key:
                tags[key] = str(item.get("Value") or "")
    return tags


def observed_from_descriptions(
    *,
    db_instances: Iterable[Mapping[str, object]] = (),
    db_clusters: Iterable[Mapping[str, object]] = (),
    ec2_instances: Iterable[Mapping[str, object]] = (),
) -> tuple[ObservedResource, ...]:
    """Keep only the demo-tagged resources out of raw ``describe-*`` payloads.

    Ownership is decided by the ``anti-demo-run-id`` tag alone. Untagged
    resources belong to somebody else in this shared account and are not this
    demo's business, however similar their names look.
    """

    observed: list[ObservedResource] = []

    for cluster in db_clusters:
        tags = _tags(cluster.get("TagList"))
        if TAG_RUN_ID not in tags:
            continue
        observed.append(
            ObservedResource(
                AURORA_CLUSTER,
                str(cluster.get("DBClusterIdentifier") or ""),
                str(cluster.get("Status") or "unknown"),
                run_id=tags[TAG_RUN_ID],
                created_at=_created_at(cluster.get("ClusterCreateTime")),
            )
        )

    for instance in db_instances:
        tags = _tags(instance.get("TagList"))
        if TAG_RUN_ID not in tags:
            continue
        engine = str(instance.get("Engine") or "").lower()
        observed.append(
            ObservedResource(
                AURORA_WRITER if engine.startswith("aurora") else RDS_INSTANCE,
                str(instance.get("DBInstanceIdentifier") or ""),
                str(instance.get("DBInstanceStatus") or "unknown"),
                run_id=tags[TAG_RUN_ID],
                public_ipv4=bool(instance.get("PubliclyAccessible")),
                instance_class=str(instance.get("DBInstanceClass") or ""),
                created_at=_created_at(instance.get("InstanceCreateTime")),
            )
        )

    for instance in ec2_instances:
        tags = _tags(instance.get("Tags"))
        if TAG_RUN_ID not in tags:
            continue
        state = instance.get("State")
        status = str(state.get("Name") or "unknown") if isinstance(state, Mapping) else "unknown"
        observed.append(
            ObservedResource(
                EC2_RUNNER,
                str(instance.get("InstanceId") or ""),
                status,
                run_id=tags[TAG_RUN_ID],
                public_ipv4=bool(instance.get("PublicIpAddress")),
                created_at=_created_at(instance.get("LaunchTime")),
            )
        )

    return tuple(observed)


def collect_observed(session: object) -> tuple[ObservedResource, ...]:
    """Inventory demo-tagged AWS resources through a boto3 session."""

    rds = session.client("rds")  # type: ignore[attr-defined]
    ec2 = session.client("ec2")  # type: ignore[attr-defined]
    instances: list[Mapping[str, object]] = []
    clusters: list[Mapping[str, object]] = []
    machines: list[Mapping[str, object]] = []
    for page in rds.get_paginator("describe_db_instances").paginate():
        instances.extend(page.get("DBInstances") or ())
    for page in rds.get_paginator("describe_db_clusters").paginate():
        clusters.extend(page.get("DBClusters") or ())
    for page in ec2.get_paginator("describe_instances").paginate():
        for reservation in page.get("Reservations") or ():
            machines.extend(reservation.get("Instances") or ())
    return observed_from_descriptions(
        db_instances=instances,
        db_clusters=clusters,
        ec2_instances=machines,
    )


def reconcile_live(
    manifest: DemoManifest,
    session_factory,
    *,
    rates: RateCard | None = None,
) -> ReconciliationReport:
    """Reconcile against the live account, reporting rather than raising.

    An operator reaching for this is often already in a broken state — expired
    credentials, a half-destroyed installation — and the reconciliation refusing
    to answer would be the least useful response available.
    """

    try:
        observed = collect_observed(session_factory(manifest))
    except Exception as exc:
        return ReconciliationReport(
            run_id=manifest.run_id,
            expected=expected_resources(manifest),
            unavailable=f"{type(exc).__name__}: {exc}",
        )
    return reconcile(manifest, observed, rates=rates)
