"""Predict what one bout costs before either provider posts a bill.

:mod:`server.pricing` publishes the rate card and deliberately refuses to fill in
a quantity it cannot prove -- a receipt shown to a customer must never present a
derived number as an observed one.  That discipline is right for the receipt and
useless for planning, because it leaves every quantity null until Databricks
system billing and AWS Cost Explorer catch up hours later.

This module is the other half of the same problem.  It takes the telemetry a bout
already records -- lane elapsed times, resource lifetimes, capacity integrals --
and turns it into a priced estimate in which every line carries the provenance of
its quantity.  A quantity read back from a provider API is ``MEASURED``; one
derived from a lane clock is ``MODELED`` and carries a low/high band; one taken
from sealed Terraform configuration is ``ASSUMED``.

Two properties are load-bearing and easy to lose in a refactor:

1. Nothing here is ever written into a :class:`~server.models.CostReceiptSnapshot`
   quantity field.  The receipt's evidence bar is deliberately higher than this
   module's, and merging the two would quietly lower it.
2. Marginal cost and carrying cost are separate totals and are never summed.
   "What this bout cost" and "what keeping this demo alive costs" are different
   questions, and a customer who is shown their sum has been misled.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from .capacity import AURORA_AUTO_PAUSE_SECONDS, LAKEBASE_SUSPEND_SECONDS
from .capacity import AURORA_MAX_ACU as _AURORA_MAX_ACU
from .models import CompetitorId, RoundId, SessionSnapshot
from .pricing import (
    CONFIGURED_RDS_INSTANCE_CLASS,
    rds_instance_compute_source,
    rds_instance_hour_usd,
)

# The sealed ceiling from `infra/aws/aurora.tf`, as a Decimal so it can be
# multiplied by an hour count without leaking float error into a price.
AURORA_MAX_ACU = Decimal(str(_AURORA_MAX_ACU))

SECONDS_PER_HOUR = Decimal(3600)
# AWS prices storage per GB-month against a 730-hour month, which is the
# convention its own Price List API documents.  Using 30 days instead would
# understate prorated storage by 1.4%.
SECONDS_PER_BILLING_MONTH = Decimal(730) * SECONDS_PER_HOUR

_NUMBER_WORDS: tuple[str, ...] = (
    "no",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
)


def number_word(count: int) -> str:
    """``"three"`` from ``3``, for prose that has to move with a count.

    Lives here because both cost panels spell a fleet size into a sentence, and
    both got it wrong the same way: the word "four" outlived the fourth RDS
    instance in :mod:`server.standing_cost`'s lane caveat and in
    :mod:`server.bout_cost`'s structural-zero rows.  A count read off
    :class:`InstallationShape` cannot do that.

    Falls back to digits past eight rather than raising: an ugly sentence beats a
    500 on the arena screen.
    """

    return _NUMBER_WORDS[count] if 0 <= count < len(_NUMBER_WORDS) else str(count)

# The Round 5 proxy secrets, which Terraform manages directly rather than RDS
# creating them alongside a database.  Named because they are the only part of
# `InstallationShape.managed_secrets` that is *not* one-per-database, which is
# what makes that field checkable against the two fleet counts beside it.
TERRAFORM_PROXY_SECRETS = 2

# An RDS instance is billed in one-second increments with a ten-minute minimum
# charge after a billable status change.  A point-in-time restore that lives for
# ninety seconds is still billed for six hundred, which is why Round 3's short
# failed bouts do not cost proportionally less than its long ones.
RDS_MINIMUM_BILLED_SECONDS = Decimal(600)

# A Lakebase compute unit meters at this rate on AWS at the ENTERPRISE tier.
#
# The load-bearing anchor is external and unit-explicit: $0.111/CU-hour is a
# *list* rate, so $0.111 / $0.52 = 0.2135.  That the published figure is
# pre-promotion follows from the discount structure -- Databricks bills Always-On
# baseline capacity 25% below the standard autoscaling rate, and the 50% Lakebase
# promotion stacks on top of that rather than being folded into it -- and the
# arithmetic closes: third-party coverage records the Always-On baseline at
# $0.083, and 0.75 x $0.111 = $0.08325.  This anchor is a *price*, not a
# quantity, so unlike everything else in the analysis it does not descend from
# system.billing.usage.
#
# The databricks-sizing skill's Lakebase reference agrees, stating 0.213 per
# CU-hour and fixing the unit in a worked example (1 CU x 0.213 x 730 h = 155.49
# DBU/month).  It was once the sole source; it is corroboration now.
LAKEBASE_DBU_PER_CU_HOUR = Decimal("0.213")

# One Lakebase compute node at the v7 ceiling of 2 CU therefore meters at this
# rate, and v7's posted usage agreed exactly: on 2026-08-20 Round 6's change-feed
# endpoint held a flat 0.426 DBU per hour around the clock, and every shorter
# Lakebase interval posted that day is an exact fraction of it.  It is the
# constant that makes a Lakebase line predictable from a clock.
#
# THAT PLATEAU IS A RETIRED CONFIGURATION, NOT A LAKEBASE PROPERTY, AND MUST NOT
# BE QUOTED AS A COST.  It exists because Round 6's endpoint was then the only
# one configured `no_suspension`, on the theory that a change feed needed a
# permanently live connection.  A live experiment disproved the theory -- a
# CDF-replicating endpoint reporting CDF_STATE_STREAMING held IDLE for 24x its
# suspend window -- so `capacity.NO_SUSPENSION_ROUNDS` is now empty, Round 6
# provisions its endpoint with a 60s suspend window like every other round, and
# `round6_lifecycle._endpoint_contract_findings` refuses to arm the round if the
# old setting ever comes back.  A run today cannot reproduce the plateau, so any
# archived per-day figure derived from it (~$2.66/day at the promotional rate)
# describes a configuration this project no longer ships.  It is retained here
# only as one of the two readings that fix the CU conversion below.
#
# The 2 CU reading was once contested -- a parity audit read the same two
# plateaus (0.213 and 0.426 DBU/hour, an exact 2:1 pair) as 0.5 CU and 1 CU,
# which halves every per-CU figure.  It is settled at high confidence in favour
# of 1 CU and 2 CU, and both documents that disagreed have been corrected.  Two
# empirical confirmations close it independently of the price anchor above,
# reading the *shape* of the account's data rather than its dollars:
#
# 1. Every sustained plateau in the account -- 0.213, 0.426, 0.639, 0.852, 1.278,
#    1.704, 2.556 and 30.672 DBU/hour -- maps to a valid CU size under 0.213.
#    Under the halved reading, 0.639 would require a 1.5 CU plateau, which is not
#    among the 31 published CU sizes.  Nor can it be three half-CU nodes: the
#    endpoint holding it reports group.min = group.max = 1 with readable
#    secondaries disabled.
# 2. The coordination endpoint, the only one capped at max_cu = 1, peaks at
#    0.204480 DBU/hour -- approaching 0.213 from below without ever crossing it.
#
# The honest residue, recorded rather than smoothed over: under 0.213, Round 6's
# endpoint sat at its 2 CU ceiling for nine hours while running no bout.  That is
# *explained* -- its change-data-feed config was created at 15:45:23Z, the flat
# plateau begins at 15:50Z, and the then-current `no_suspension` setting removes
# the scale-down path -- but it is explained, not measured.  Closing it would need
# the Lakebase metrics API, which was not consulted.  Do not restate it as
# verified, and do not restate it as what Lakebase costs at idle: the setting that
# caused it is gone, and a sealed endpoint's idle floor is now the 60s suspend
# window that `descent_cost.py` prices.
#
# What the conversion does and does not affect.  No dollar figure depends on it:
# costs are posted DBU multiplied by a posted price and never pass through CU,
# which `test_dollars_do_not_pass_through_capacity_units` pins.  What does depend
# on it is the Lakebase-versus-Aurora hourly rate comparison, which under 0.213
# is cheaper on both rates -- 7.7% at list, 53.9% promotional.
LAKEBASE_CEILING_CU = Decimal("2")
LAKEBASE_NODE_DBU_PER_HOUR = LAKEBASE_DBU_PER_CU_HOUR * LAKEBASE_CEILING_CU


# What the already-measured figures were metered on.  Every bout, receipt and
# posted figure this repository publishes was recorded before the db.t4g.medium
# resize reached the account, so restating any of them at the medium rate would
# invent history -- which is why this is a record of the past and not a reading
# of the present.  It must not be made to follow `RDS_INSTANCE_CLASS`, or
# changing the configured class would silently reprice what already ran.
#
# THIS IS NOT WHAT IS RUNNING NOW, AND MUST NEVER BE USED TO SAY SO.  It was,
# once: the standing-cost RDS lane compared this constant against the configured
# class and asserted from the difference that "all four instances are running
# db.t4g.micro".  Both sides were hardcoded, nothing consulted AWS, and the
# sentence went false the moment four `ModifyDBInstance` calls landed at
# 2026-08-21T14:48:36Z -- verified since by `describe-db-instances` returning
# db.t4g.medium on all four with `PendingModifiedValues` empty and
# `InstanceCreateTime` unchanged, so the boxes were modified in place rather
# than replaced.  A claim about live state needs a live reading, and
# `server/standing_cost.py:_rds_class_basis` now takes one as an argument.
AS_RUN_RDS_INSTANCE_CLASS = "db.t4g.micro"


class PricingBasis(StrEnum):
    """Whether a figure describes what will be spent or what already was.

    These are different questions with different answers whenever the configured
    class and the running class disagree, which is exactly the state the
    installation is in today.  ``CONFIGURED`` prices the box the next
    ``terraform apply`` will create and is what an estimate should use.
    ``AS_RUN`` prices the box that actually ran, and is the only honest basis for
    restating a figure that has already been measured or published.
    """

    CONFIGURED = "configured"
    AS_RUN = "as_run"


_INSTANCE_CLASS_BY_BASIS: dict[PricingBasis, str] = {
    PricingBasis.CONFIGURED: CONFIGURED_RDS_INSTANCE_CLASS,
    PricingBasis.AS_RUN: AS_RUN_RDS_INSTANCE_CLASS,
}


class Cloud(StrEnum):
    AWS = "aws"
    DATABRICKS = "databricks"


class CostKind(StrEnum):
    COMPUTE = "compute"
    STORAGE = "storage"
    NETWORK = "network"
    OTHER = "other"


class EstimateScope(StrEnum):
    """Deliberately the same three words :mod:`server.pricing` already uses."""

    BOUT = "bout_estimate"
    CARRYING = "required_monthly_carrying_cost"
    OVERHEAD = "installation_overhead"


class Provenance(StrEnum):
    MEASURED = "measured"
    MODELED = "modeled"
    ASSUMED = "assumed"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class Quantity:
    """A quantity with a band.  ``low <= point <= high`` is enforced."""

    point: Decimal | None
    low: Decimal | None
    high: Decimal | None
    provenance: Provenance
    basis: str

    def __post_init__(self) -> None:
        if not self.basis.strip():
            raise ValueError("a quantity must record the basis it was derived from")
        if self.point is None:
            if self.low is not None or self.high is not None:
                raise ValueError("an unavailable quantity cannot carry a band")
            if self.provenance is not Provenance.UNAVAILABLE:
                raise ValueError("a missing quantity must be marked unavailable")
            return
        if self.provenance is Provenance.UNAVAILABLE:
            raise ValueError("an unavailable quantity cannot carry a point value")
        if self.low is None or self.high is None:
            raise ValueError("a known quantity requires both bounds")
        if not (self.low <= self.point <= self.high):
            raise ValueError("a quantity band must contain its point estimate")
        if self.low < 0:
            raise ValueError("a quantity band must be non-negative")

    @classmethod
    def exact(cls, value: Decimal, *, provenance: Provenance, basis: str) -> Quantity:
        return cls(point=value, low=value, high=value, provenance=provenance, basis=basis)

    @classmethod
    def banded(
        cls,
        point: Decimal,
        *,
        low: Decimal,
        high: Decimal,
        basis: str,
        provenance: Provenance = Provenance.MODELED,
    ) -> Quantity:
        return cls(point=point, low=low, high=high, provenance=provenance, basis=basis)

    @classmethod
    def unavailable(cls, basis: str) -> Quantity:
        return cls(
            point=None,
            low=None,
            high=None,
            provenance=Provenance.UNAVAILABLE,
            basis=basis,
        )


@dataclass(frozen=True, slots=True)
class BurnRate:
    """DBU per second of bout wall-clock, and the rounds it was fitted on.

    The band is not a confidence interval in any statistical sense -- there are
    too few samples for that -- it is the literal range the reconciled samples
    spanned, which is the strongest honest claim available.

    ``rounds`` is the rate's *support*, and carrying it is what stops the rate
    being applied where it means nothing.  A rate fitted on the restore rounds
    over-predicted Round 5 by 13.8x, because a fixed DBU-per-bout-second cannot
    represent a bout that is long on account of the opposing lane rather than
    this one.  Applying a rate outside its support now yields an unavailable
    line instead of a confident wrong number.
    """

    point: Decimal
    low: Decimal
    high: Decimal
    sample_count: int
    rounds: frozenset[RoundId]
    basis: str = "reconciled per-bout samples"

    def __post_init__(self) -> None:
        if self.sample_count < 1:
            raise ValueError("a burn rate must rest on at least one sample")
        if not self.rounds:
            raise ValueError("a burn rate must name the rounds it was calibrated on")
        if not (self.low <= self.point <= self.high):
            raise ValueError("a burn rate band must contain its point estimate")
        if self.low < 0:
            raise ValueError("a burn rate must be non-negative")

    def covers(self, round_id: RoundId) -> bool:
        return round_id in self.rounds


@dataclass(frozen=True, slots=True)
class LakebaseBurnModel:
    """Several burn rates, each valid over the rounds it was fitted on.

    One rate for the whole demo was the defect: Rounds 2 and 3 keep the Lakebase
    endpoint busy for most of the bout, Round 5 does not, and no single
    DBU-per-bout-second constant describes both.  Supports may not overlap, so
    there is never a question of which rate a round gets, and a round no rate
    covers gets an unavailable line rather than the nearest available rate.
    """

    rates: tuple[BurnRate, ...]

    def __post_init__(self) -> None:
        if not self.rates:
            raise ValueError("a burn model needs at least one calibrated rate")
        claimed: set[RoundId] = set()
        for rate in self.rates:
            overlap = claimed & rate.rounds
            if overlap:
                names = ", ".join(sorted(round_id.value for round_id in overlap))
                raise ValueError(f"two burn rates both claim to cover {names}")
            claimed |= rate.rounds

    def for_round(self, round_id: RoundId) -> BurnRate | None:
        for rate in self.rates:
            if rate.covers(round_id):
                return rate
        return None


@dataclass(frozen=True, slots=True)
class LakebaseSample:
    """One bout whose posted Lakebase DBU could be isolated from billing.

    A sample is admissible only when the 10-minute records covering the bout
    carry no other bout on the same ``project_id``; anything looser is measuring
    the neighbours too.
    """

    label: str
    round_id: RoundId
    posted_dbu: Decimal
    bout_seconds: Decimal

    def __post_init__(self) -> None:
        if self.bout_seconds <= 0:
            raise ValueError("a calibration sample requires a positive bout interval")
        if self.posted_dbu < 0:
            raise ValueError("posted DBU must be non-negative")

    @property
    def dbu_per_second(self) -> Decimal:
        return self.posted_dbu / self.bout_seconds


@dataclass(frozen=True, slots=True)
class HeldOutPrediction:
    """One leave-one-out row: fitted on the others, scored on this one."""

    label: str
    round_id: RoundId
    trained_on: int
    predicted_dbu: Decimal
    posted_dbu: Decimal

    @property
    def error_dbu(self) -> Decimal:
        return self.predicted_dbu - self.posted_dbu

    @property
    def error_fraction(self) -> Decimal | None:
        if self.posted_dbu == 0:
            return None
        return self.error_dbu / self.posted_dbu


@dataclass(frozen=True, slots=True)
class Rate:
    """One unit price, with the source that established it."""

    usd: Decimal
    unit: str
    source: str

    def __post_init__(self) -> None:
        if self.usd < 0:
            raise ValueError("a unit rate must be non-negative")
        if not self.unit.strip() or not self.source.strip():
            raise ValueError("a rate must name its unit and its source")


@dataclass(frozen=True, slots=True)
class CostLine:
    component: str
    cloud: Cloud
    kind: CostKind
    scope: EstimateScope
    lane_id: str
    quantity: Quantity
    rate: Rate
    # Whether the resource this line prices exists at all.  ``MODELED`` on the
    # quantity is a weaker claim and not a substitute: a band fitted to a real
    # Aurora measurement is modeled and the cluster is still there.  ``imputed``
    # means *nothing is provisioned* and the line answers a counterfactual --
    # what a customer would pay -- so it may never enter a total that claims to
    # say what this installation is billed.  A bool the totals route on cannot be
    # dropped the way differently-worded prose can.
    imputed: bool = False

    def __post_init__(self) -> None:
        if not self.imputed:
            return
        if self.quantity.provenance is not Provenance.MODELED:
            raise ValueError(
                "an imputed line prices a resource nobody provisioned, so its quantity "
                f"cannot be {self.quantity.provenance.value}; only MODELED is honest here"
            )
        # Quantity already refuses an empty basis, which is what makes the rule
        # "no imputed figure without its derivation" structural rather than a
        # convention a renderer could forget.  Restated as an assertion because
        # it is the property the counterfactual rests on.
        if not self.quantity.basis.strip():
            raise ValueError("an imputed line must carry the derivation it was modelled from")

    @property
    def usd(self) -> Decimal | None:
        return None if self.quantity.point is None else self.quantity.point * self.rate.usd

    @property
    def usd_low(self) -> Decimal | None:
        return None if self.quantity.low is None else self.quantity.low * self.rate.usd

    @property
    def usd_high(self) -> Decimal | None:
        return None if self.quantity.high is None else self.quantity.high * self.rate.usd


def _total(lines: Iterable[CostLine], attribute: str) -> Decimal:
    return sum((getattr(line, attribute) or Decimal(0) for line in lines), Decimal(0))


@dataclass(frozen=True, slots=True)
class BoutCostEstimate:
    round_id: RoundId
    competitor_id: CompetitorId
    lines: tuple[CostLine, ...]

    def scoped(self, scope: EstimateScope) -> tuple[CostLine, ...]:
        return tuple(line for line in self.lines if line.scope is scope)

    def total_usd(self, scope: EstimateScope = EstimateScope.BOUT) -> Decimal:
        return _total(self.scoped(scope), "usd")

    def band_usd(self, scope: EstimateScope = EstimateScope.BOUT) -> tuple[Decimal, Decimal]:
        scoped = self.scoped(scope)
        return _total(scoped, "usd_low"), _total(scoped, "usd_high")

    def by_cloud(self, scope: EstimateScope = EstimateScope.BOUT) -> dict[Cloud, Decimal]:
        return {
            cloud: _total(
                (line for line in self.scoped(scope) if line.cloud is cloud),
                "usd",
            )
            for cloud in Cloud
        }

    def by_kind(self, scope: EstimateScope = EstimateScope.BOUT) -> dict[CostKind, Decimal]:
        return {
            kind: _total(
                (line for line in self.scoped(scope) if line.kind is kind),
                "usd",
            )
            for kind in CostKind
        }

    @property
    def unavailable(self) -> tuple[CostLine, ...]:
        """Lines whose quantity could not be established, so cost is unknown, not zero."""

        return tuple(line for line in self.lines if line.quantity.point is None)


@dataclass(frozen=True, slots=True)
class RateCard:
    """Public on-demand rates.  No contract, RI, Savings Plan, or EDP discount.

    Defaults mirror the rates :mod:`server.pricing` already publishes, and
    ``tests/test_cost_model.py`` asserts the two agree so a price cannot be
    updated in one place and silently drift in the other.

    The AWS rates are rate-card derived, not invoice-verified.
    ``ce:GetCostAndUsage`` is denied to this installation and is not being
    pursued, so no figure on the AWS side has been reconciled against a bill.
    """

    # The one input that decides the RDS instance-hour rate *and* the class named
    # in every RDS compute line item.  Defaults to the configured class, so an
    # estimate is forward-looking unless a caller deliberately asks otherwise via
    # :meth:`for_basis`.
    rds_instance_class: str = CONFIGURED_RDS_INSTANCE_CLASS
    lakebase_dbu: Rate = Rate(
        Decimal("0.26"),
        "DBU",
        "system.billing.list_prices pricing.effective_list.default (promotional)",
    )
    lakebase_dsu: Rate = Rate(
        Decimal("0.023"),
        "DSU",
        "system.billing.list_prices pricing.effective_list.default",
    )
    rds_gp3_gb_month: Rate = Rate(
        Decimal("0.115"),
        "GB-month",
        "AWS Price List API · AmazonRDS · OnDemand · us-west-2",
    )
    rds_backup_gb_month: Rate = Rate(
        Decimal("0.095"),
        "GB-month",
        "AWS Price List API · AmazonRDS · OnDemand · us-west-2",
    )
    aurora_acu_hour: Rate = Rate(
        Decimal("0.12"),
        "ACU-hour",
        "AWS Price List API · AmazonRDS · OnDemand · us-west-2",
    )
    aurora_storage_gb_month: Rate = Rate(
        Decimal("0.10"),
        "GB-month",
        "AWS Price List API · AmazonRDS · OnDemand · us-west-2",
    )
    rds_proxy_capacity_hour: Rate = Rate(
        Decimal("0.015"),
        "capacity-hour",
        "Amazon RDS Proxy pricing · us-west-2 · 10-minute minimum",
    )
    ec2_m6i_large_hour: Rate = Rate(
        Decimal("0.096"),
        "instance-hour",
        "AWS Price List API · AmazonEC2 · OnDemand · us-west-2",
    )
    ebs_gp3_gb_month: Rate = Rate(
        Decimal("0.08"),
        "GB-month",
        "AWS Price List API · AmazonEC2 · OnDemand · us-west-2",
    )
    public_ipv4_hour: Rate = Rate(
        Decimal("0.005"),
        "address-hour",
        "AWS Price List API · AmazonVPC · OnDemand · us-west-2",
    )
    secret_month: Rate = Rate(
        Decimal("0.40"),
        "secret-month",
        "AWS Price List API · AWSSecretsManager · OnDemand · us-west-2",
    )

    @property
    def rds_instance_hour(self) -> Rate:
        """Derived, never stored, so a resize cannot leave the rate behind.

        Raises :class:`~server.pricing.UnknownRdsInstanceClassError` for a class
        with no published rate.  Failing to price is the intended outcome there:
        a defaulted rate would be wrong but plausible, which is worse.
        """

        return Rate(
            rds_instance_hour_usd(self.rds_instance_class),
            "instance-hour",
            rds_instance_compute_source(self.rds_instance_class),
        )

    @property
    def rds_compute_label(self) -> str:
        """The class as it is named in a line item, from the same one source."""

        return f"RDS PostgreSQL {self.rds_instance_class}"

    @classmethod
    def for_basis(cls, basis: PricingBasis) -> RateCard:
        """A card pinned to the configured class or to the as-run one."""

        return cls(rds_instance_class=_INSTANCE_CLASS_BY_BASIS[basis])


@dataclass(frozen=True, slots=True)
class InstallationShape:
    """Sealed configuration of one provisioned generation.

    Every default here is read off `infra/aws/*.tf` and the v7 manifest rather
    than guessed, so a change to the Terraform must change this too.
    """

    # Three, not four. `infra/aws/locals.tf` stands an RDS instance up for
    # `v7_rds_round_keys = ["r2","r3","r5"]` only, and the v7 manifest agrees --
    # Round 1's `round_environments` entry seals `rds: null`. Round 1's instance
    # was deleted because its lane refuses to enter on engine semantics and was
    # never timed, so it billed to measure nothing. Its cost did not go away for
    # a *customer*, which is what `imputed_round_carrying_lines` prices; it went
    # away for this installation, which is what this shape prices.
    rds_instances: int = 3
    rds_allocated_gb: Decimal = Decimal(20)
    aurora_clusters: int = 4
    # `serverlessv2_scaling_configuration.min_capacity = 0` in the v7 state, which
    # is why an idle Aurora cluster here parks for free and Round 1 has something
    # to wake up. Raise this and the carrying total moves immediately.
    aurora_min_acu: Decimal = Decimal(0)
    aurora_storage_gb: Decimal = Decimal(1)
    # Every database is `publicly_accessible = true`: three RDS instances and
    # four Aurora writers, each holding one chargeable address.
    public_ipv4_addresses: int = 7
    # Seven RDS-managed master credentials -- three RDS instances plus four Aurora
    # clusters -- plus the two Terraform-managed Round 5 proxy secrets. The
    # `rds!`-prefixed managed secrets are chargeable: the RDS guide states plainly
    # that "you are charged for that secret".
    managed_secrets: int = 9
    runner_instances: int = 1
    runner_root_gb: Decimal = Decimal(20)
    lakebase_projects: int = 7

    def with_r1_rds_instance(self) -> InstallationShape:
        """The fleet as it stood before Round 1's RDS instance was deleted.

        Kept so the deletion identity stays checkable: what this installation
        stopped paying has to equal, to the last place, what
        :func:`imputed_round_carrying_lines` says a customer keeps paying.  That
        check needs both generations, and reconstructing the older one by
        addition means the two cannot drift apart.

        The instance took exactly three chargeable things with it: itself, the
        public IPv4 address attached to it, and the RDS-managed master credential
        that came with it.  Aurora is untouched -- Round 1 keeps the only lane
        that can compete in it.
        """

        return InstallationShape(
            rds_instances=self.rds_instances + 1,
            rds_allocated_gb=self.rds_allocated_gb,
            aurora_clusters=self.aurora_clusters,
            aurora_min_acu=self.aurora_min_acu,
            aurora_storage_gb=self.aurora_storage_gb,
            public_ipv4_addresses=self.public_ipv4_addresses + 1,
            managed_secrets=self.managed_secrets + 1,
            runner_instances=self.runner_instances,
            runner_root_gb=self.runner_root_gb,
            lakebase_projects=self.lakebase_projects,
        )


@dataclass(frozen=True, slots=True)
class BoutTelemetry:
    """Everything the estimator is allowed to read.

    The three ``*_seconds`` lifetime fields accept an exact provider-observed
    value when one is available.  When they are ``None`` the estimator falls back
    to the competitor lane clock, which is a *lower* bound on the resource's real
    lifetime -- the resource is created before the lane starts timing and deleted
    after it stops -- so the modeled band runs upward from it, never downward.

    ``observed_acu_seconds_low`` and ``observed_acu_seconds_high`` let a caller
    hand in an observation that is legitimately a *range* rather than a point.
    Two of this installation's four Aurora measurements are: Round 5's is the
    spread between two real bouts of the same round, and Rounds 2 and 3 carry an
    unresolved question about whether a deleting instance's reported capacity is
    billed.  Both ends of each are observed, so the resulting quantity is graded
    ``MEASURED`` with a band rather than being collapsed to a single number the
    evidence does not support.  ``acu_observation_basis`` travels with them so
    the line can say on screen where its quantity came from.
    """

    round_id: RoundId
    competitor_id: CompetitorId
    bout_seconds: Decimal
    lakebase_lane_seconds: Decimal | None = None
    competitor_lane_seconds: Decimal | None = None

    observed_restore_lifetime_seconds: Decimal | None = None
    observed_proxy_lifetime_seconds: Decimal | None = None
    observed_acu_seconds_above_floor: Decimal | None = None
    observed_acu_seconds_low: Decimal | None = None
    observed_acu_seconds_high: Decimal | None = None
    acu_observation_basis: str | None = None
    observed_lakebase_dbu: Decimal | None = None

    # How much longer than the measured lane a torn-down resource is assumed to
    # live: the harness waits through `backing-up` and
    # `configuring-enhanced-monitoring` before it can issue the delete, and the
    # delete itself is not instantaneous.  Only widens the upper bound.
    teardown_allowance_seconds: Decimal = Decimal(180)

    def __post_init__(self) -> None:
        if self.bout_seconds <= 0:
            raise ValueError("a bout must have a positive duration")
        for name in (
            "lakebase_lane_seconds",
            "competitor_lane_seconds",
            "observed_restore_lifetime_seconds",
            "observed_proxy_lifetime_seconds",
            "observed_acu_seconds_above_floor",
            "observed_acu_seconds_low",
            "observed_acu_seconds_high",
            "observed_lakebase_dbu",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.teardown_allowance_seconds < 0:
            raise ValueError("teardown_allowance_seconds must be non-negative")
        self._check_acu_band()

    def _check_acu_band(self) -> None:
        """A band without an observation is a band around nothing."""

        point = self.observed_acu_seconds_above_floor
        low = self.observed_acu_seconds_low
        high = self.observed_acu_seconds_high
        if point is None:
            if low is not None or high is not None:
                raise ValueError(
                    "an ACU band requires observed_acu_seconds_above_floor to sit inside it"
                )
            return
        if low is not None and low > point:
            raise ValueError("observed_acu_seconds_low must not exceed the observed point")
        if high is not None and high < point:
            raise ValueError("observed_acu_seconds_high must not fall below the observed point")


# Aurora Serverless v2 does not drop to zero when a bout stops using it: it
# descends over AWS's auto-pause interval, and every second of that descent is
# billed.  `AURORA_AUTO_PAUSE_SECONDS` is a product floor AWS will not accept a
# lower value for, so this is the *minimum* commitment a wake makes, not an
# estimate of the tail.  It is used to bound cost, never to size a window.
ACU_DESCENT_FLOOR_SECONDS = Decimal(AURORA_AUTO_PAUSE_SECONDS)

# The measured descents are longer than the floor and are not a constant: on
# 2026-08-21 two runs of Round 5 held 0.5 ACU for 5 and 15 minutes respectively
# (`.anti-demo-v7/aurora-acu-2026-08-21.md` §3, §10 item 3), and the cause was
# not established.  A sampling window sized to the 300-second floor would have
# truncated the second one, so the window reaches far past it and relies on
# trailing zero buckets to prove the integral closed.  The same 35 minutes is
# what the measurement pass used to demonstrate every descent reached 0 ACU.
ACU_SAMPLE_TAIL_SECONDS = Decimal(2100)

# Lead-in, matching the measurement pass.  Long enough to show the cluster was
# parked before the bout, which is what makes a non-zero integral attributable.
ACU_SAMPLE_LEAD_SECONDS = Decimal(300)

# CloudWatch's finest published period for this metric.
ACU_SAMPLE_PERIOD_SECONDS = Decimal(60)

# The smallest capacity a *running* Serverless v2 instance reports.  Below this
# there is only the paused state at 0 ACU; AWS scales in 0.5 ACU steps and the
# measured descents sat at exactly 0.500 for their whole length.  This is what
# makes a woken cluster's cost floor positive rather than zero.
AURORA_MIN_RUNNING_ACU = Decimal("0.5")


def integrate_acu_seconds(
    datapoints: Iterable[object],
    *,
    period_seconds: Decimal = ACU_SAMPLE_PERIOD_SECONDS,
    floor_acu: Decimal = Decimal(0),
    statistic: str = "Average",
) -> Decimal | None:
    """Turn CloudWatch ``ServerlessDatabaseCapacity`` datapoints into ACU-seconds.

    Aurora publishes this metric once per second, so a bucket's ``SampleCount``
    is the number of seconds it actually observed.  Using it rather than assuming
    a full ``period_seconds`` matters at both ends of a window, where the first
    and last buckets are partial -- counts as low as 9 were seen in the
    measurement pass, and counts slightly above 60 appear too and are clamped.
    A bucket that reports no count falls back to the full period, which reads
    high, so the fallback is the direction that cannot hide spend.

    ``floor_acu`` defaults to zero because ``infra/aws/aurora.tf`` pins
    ``min_capacity = 0`` and Round 1 refuses to arm unless the live cluster
    agrees, so on this installation everything observed is above the floor.

    This is what replaces a bound with a measurement, and the measurement went
    the opposite way from the assumption it replaced: the lane-clock convention
    it supersedes came out **1.73x low** across Rounds 1-3, and 16.1x low on
    Round 1, because Aurora bills outside every clock the harness keeps.

    Two properties matter more than the arithmetic:

    * **It never raises.** A malformed or missing datapoint is skipped, and no
      usable datapoint at all yields ``None`` rather than a zero. Sampling is an
      opportunistic improvement to an estimate and must never be able to fail a
      bout, so every failure mode here degrades to "unmeasured".
    * **Gaps read low, and that is disclosed rather than interpolated.** A period
      CloudWatch did not report contributes nothing, so a window with holes in it
      understates. The alternative -- interpolating across a gap -- would invent
      capacity that was never observed.
    """

    total = Decimal(0)
    counted = 0
    for datapoint in datapoints:
        if not isinstance(datapoint, dict):
            continue
        raw = datapoint.get(statistic)
        if raw is None or isinstance(raw, bool):
            continue
        try:
            capacity = Decimal(str(raw))
        except (ArithmeticError, ValueError):
            continue
        if not capacity.is_finite():
            continue
        counted += 1
        above_floor = capacity - floor_acu
        if above_floor > 0:
            total += above_floor * _observed_seconds(datapoint, period_seconds)
    return total if counted else None


def _observed_seconds(datapoint: dict[object, object], period_seconds: Decimal) -> Decimal:
    """How many seconds of the period a bucket actually observed."""

    raw = datapoint.get("SampleCount")
    if raw is None or isinstance(raw, bool):
        return period_seconds
    try:
        observed = Decimal(str(raw))
    except (ArithmeticError, ValueError):
        return period_seconds
    if not observed.is_finite() or observed <= 0:
        return period_seconds
    return min(observed, period_seconds)


def acu_sampling_window(
    started_at: datetime,
    ended_at: datetime,
    *,
    lead_seconds: Decimal = ACU_SAMPLE_LEAD_SECONDS,
    tail_seconds: Decimal = ACU_SAMPLE_TAIL_SECONDS,
) -> tuple[datetime, datetime]:
    """Widen a **bout** window to the interval that actually carries billed capacity.

    The lead shows the cluster parked before the bout, which is what makes a
    non-zero integral attributable to the bout.  The tail covers the auto-pause
    descent, which is billed and outlives every clock the harness keeps: Round
    1's 15.31-second bout provisioned 420 seconds of billed capacity, 97.2% of
    it after the bell.

    Both arguments must bracket the bout.  Passing the arming gate's pre-bout
    probe window instead produces a guaranteed zero, because the gate refuses to
    arm unless every sample in it reads zero -- and a zero from that window would
    be recorded as ``MEASURED``.
    """

    if ended_at < started_at:
        raise ValueError("an ACU sampling window must not end before it starts")
    if lead_seconds < 0 or tail_seconds < 0:
        raise ValueError("ACU sampling lead and tail must be non-negative")
    return (
        started_at - timedelta(seconds=float(lead_seconds)),
        ended_at + timedelta(seconds=float(tail_seconds)),
    )


def aurora_wake_commitment_acu_hours() -> Decimal:
    """The least capacity a bout that wakes Aurora at all can be billed for.

    A wake commits to the auto-pause interval, which AWS will not let this
    installation shorten, and a running instance cannot report less than
    :data:`AURORA_MIN_RUNNING_ACU`.  The product of the two is a floor that no
    bout can get under, however briefly it touches the database -- which is why
    a ``low`` bound of zero on an Aurora line is not conservative, it is
    unreachable.
    """

    return _seconds_to_hours(ACU_DESCENT_FLOOR_SECONDS * AURORA_MIN_RUNNING_ACU)


def aurora_ceiling_acu_hours(lane_seconds: Decimal, teardown_seconds: Decimal) -> Decimal:
    """The most capacity a bout can be billed for, over the corrected window.

    The window is the lane plus the teardown allowance plus the auto-pause
    descent, because the descent is billed and no lane clock covers it.  The
    rate is the 2 ACU ceiling sealed in ``infra/aws/aurora.tf``.  Both measured
    live-cluster rounds fall inside this bound; the lane-only version of it did
    not.
    """

    window = lane_seconds + teardown_seconds + ACU_DESCENT_FLOOR_SECONDS
    return _seconds_to_hours(window) * AURORA_MAX_ACU


@dataclass(frozen=True, slots=True)
class AuroraAcuMeasurement:
    """ACU-seconds a round was observed to consume, with the spread it showed.

    Every one of ``low``, ``point`` and ``high`` comes from a CloudWatch series,
    so the band is observational -- either the spread across repeat runs of the
    same round, or a documented ambiguity about what AWS bills -- rather than
    modelling slack.  That is why :meth:`as_quantity` grades it ``MEASURED``.

    It is deliberately *not* consulted by :func:`estimate_bout_cost`.  A past
    bout's integral is evidence about that bout, not a quantity belonging to the
    next one, and wiring it in automatically would manufacture the coverage this
    module just finished refusing to manufacture.  A caller who wants to price a
    round from these figures passes them in as
    ``BoutTelemetry.observed_acu_seconds_above_floor`` and owns that choice.

    ``server/bout_cost.py`` is that caller, and it is the only one.  It builds
    the on-screen per-round Aurora figures by handing each measurement's point
    and band in explicitly, together with :attr:`basis` as
    ``BoutTelemetry.acu_observation_basis``, so every rendered dollar says on the
    same element which bouts it came from and what was unresolved about them.
    """

    round_id: RoundId
    point: Decimal
    low: Decimal
    high: Decimal
    bouts: tuple[str, ...]
    basis: str

    def __post_init__(self) -> None:
        if not (self.low <= self.point <= self.high):
            raise ValueError("a measured ACU band must contain its point")
        if self.low < 0:
            raise ValueError("ACU-seconds cannot be negative")
        if not self.bouts:
            raise ValueError("a measurement must name the bouts it came from")

    @property
    def is_ambiguous(self) -> bool:
        """Whether the band spans an unresolved question rather than a spread."""

        return self.low != self.high

    def as_quantity(self) -> Quantity:
        return Quantity.banded(
            self.point,
            low=self.low,
            high=self.high,
            basis=self.basis,
            provenance=Provenance.MEASURED,
        )


# CloudWatch `ServerlessDatabaseCapacity` integrals from
# `.anti-demo-v7/aurora-acu-2026-08-21.md`, us-west-2, run `ad-20260820-1446-abcd`.
# These are the only real Aurora quantities this installation has.
#
# Rounds 4 and 6 are absent on purpose: `infra/aws/locals.tf` provisions no
# Aurora for them, so their zero is exact and needs no measurement.
V7_MEASURED_AURORA_ACU_SECONDS: dict[RoundId, AuroraAcuMeasurement] = {
    RoundId.WAKE_IDLE_APP: AuroraAcuMeasurement(
        round_id=RoundId.WAKE_IDLE_APP,
        point=Decimal("468.85"),
        low=Decimal("468.85"),
        high=Decimal("468.85"),
        bouts=("7ECE1CB0",),
        basis=(
            "CloudWatch integral over one bout; the writer is live throughout and "
            "the descent reached 0 ACU inside the window, so there is nothing to "
            "bound -- 13.00 ACU-s fell inside the 15.31s bout and 455.85 after it"
        ),
    ),
    RoundId.MAKE_SCHEMA_CHANGE_SAFELY: AuroraAcuMeasurement(
        round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        point=Decimal("1298"),
        low=Decimal("272"),
        high=Decimal("1298"),
        bouts=("063A5187",),
        # The band is a real open question, not a spread, and collapsing it would
        # be picking an answer AWS has not given.  Both ends are observed; what is
        # unobserved is which one is billed.
        basis=(
            "CloudWatch integral over one restore. The band is an unresolved "
            "billing question: the writer reported a dead-flat 2.0 ACU for 513s "
            "*after* DeleteDBInstance. Whether AWS bills capacity during deletion "
            "is undocumented and ce:GetCostAndUsage is denied to this principal, "
            "so it cannot be settled. The point takes the drain as billed, "
            "because a reported 2.0 ACU is the only observation available and "
            "assuming AWS forgoes instrumented revenue is the weaker claim"
        ),
    ),
    RoundId.RECOVER_DELETED_ORDER: AuroraAcuMeasurement(
        round_id=RoundId.RECOVER_DELETED_ORDER,
        point=Decimal("818.49"),
        low=Decimal("308"),
        high=Decimal("818.49"),
        bouts=("A672140E",),
        basis=(
            "CloudWatch integral over one restore, carrying the same unresolved "
            "deletion-drain question as Round 2 -- 276s of reported 2.0 ACU after "
            "DeleteDBInstance, billed or not is undocumented and unverifiable"
        ),
    ),
    RoundId.SURVIVE_CONNECTION_SPIKE: AuroraAcuMeasurement(
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        point=Decimal("866.195"),
        low=Decimal("714.91"),
        high=Decimal("1017.48"),
        bouts=("abcdef0123456789", "0123456789abcdef"),
        # Two runs of one round, 42% apart, with descents of 5 and 15 minutes for
        # reasons that were not established.  This is the clearest evidence that
        # Aurora's footprint is not a function of the lane clock.
        basis=(
            "CloudWatch integrals over the two bouts CloudTrail confirms ran the "
            "Aurora lane. The band is the observed spread between them, not "
            "modelling slack; the point is their mean. Neither bout has a receipt, "
            "so it is not established that the 128-client burst fully landed, "
            "making these a lower bound on a contract-satisfying Round 5"
        ),
    ),
}


def aurora_acu_seconds_for(round_id: RoundId) -> AuroraAcuMeasurement | None:
    """The measured ACU-seconds for a round, or ``None`` if it was never sampled."""

    return V7_MEASURED_AURORA_ACU_SECONDS.get(round_id)


def _seconds_to_hours(seconds: Decimal) -> Decimal:
    return seconds / SECONDS_PER_HOUR


def _lifetime(
    telemetry: BoutTelemetry,
    observed: Decimal | None,
    *,
    what: str,
) -> Quantity:
    """Resolve an ephemeral resource's lifetime in seconds.

    An exact provider observation wins outright.  Otherwise the competitor lane
    clock supplies the point estimate and the lower bound, and the teardown
    allowance supplies the upper bound.
    """

    if observed is not None:
        return Quantity.exact(
            observed,
            provenance=Provenance.MEASURED,
            basis=f"provider-observed {what} lifetime",
        )
    lane = telemetry.competitor_lane_seconds
    if lane is None:
        return Quantity.unavailable(f"no lane clock and no provider observation for {what}")
    return Quantity.banded(
        lane,
        low=lane,
        high=lane + telemetry.teardown_allowance_seconds,
        basis=f"competitor lane elapsed as a lower bound on {what} lifetime",
    )


def _billed_seconds(lifetime: Quantity, minimum: Decimal) -> Quantity:
    """Apply a provider minimum charge to a lifetime band."""

    if lifetime.point is None:
        return lifetime
    assert lifetime.low is not None and lifetime.high is not None
    return Quantity(
        point=max(minimum, lifetime.point),
        low=max(minimum, lifetime.low),
        high=max(minimum, lifetime.high),
        provenance=lifetime.provenance,
        basis=f"{lifetime.basis}; {minimum} s provider minimum applied",
    )


def _scale(quantity: Quantity, factor: Decimal, *, basis: str) -> Quantity:
    if quantity.point is None:
        return Quantity.unavailable(basis)
    assert quantity.low is not None and quantity.high is not None
    return Quantity(
        point=quantity.point * factor,
        low=quantity.low * factor,
        high=quantity.high * factor,
        provenance=quantity.provenance,
        basis=basis,
    )


def _line(
    component: str,
    *,
    cloud: Cloud,
    kind: CostKind,
    scope: EstimateScope,
    lane_id: str,
    quantity: Quantity,
    rate: Rate,
    imputed: bool = False,
) -> CostLine:
    return CostLine(
        component=component,
        cloud=cloud,
        kind=kind,
        scope=scope,
        lane_id=lane_id,
        quantity=quantity,
        rate=rate,
        imputed=imputed,
    )


def _burn_rate_for(
    round_id: RoundId,
    calibration: BurnRate | LakebaseBurnModel | None,
) -> BurnRate | None:
    if calibration is None:
        return None
    if isinstance(calibration, BurnRate):
        return calibration if calibration.covers(round_id) else None
    return calibration.for_round(round_id)


def _lakebase_dbu_quantity(
    telemetry: BoutTelemetry,
    calibration: BurnRate | LakebaseBurnModel | None,
) -> Quantity:
    """Lakebase DBU burned by the bout.

    Prefers a posted quantity.  Failing that it applies a calibrated burn rate to
    the bout's wall clock -- which is the whole point of the exercise, since it
    produces a number without waiting hours for billing to post.

    **The clock is bout wall-clock and the rate is per round shape, and the second
    half of that sentence is load-bearing.**  Wall-clock beats the Lakebase lane
    clock *within* a round shape and is kept for that reason: across the
    reconciled restore bouts the lane clock mispredicts by a factor of two while
    wall-clock holds to within a sixth, because the endpoint stays warm for as
    long as *either* lane is working.  What wall-clock cannot do is travel between
    shapes.  Round 5's clock is ``setup_elapsed_ms``, and 792.6 of one 813-second
    bout was AWS building an RDS Proxy while the Lakebase endpoint -- 3.96 seconds
    of setup behind it, 60-second idle timeout -- had nothing to do.  A single
    DBU-per-bout-second constant fitted on the restore rounds predicted 13.8x the
    measured cost there, and recalibrating it on five samples instead of two still
    leaves 12.2x, because the error is structural and not a bad fit: the rate is
    being asked to represent a bout that is long on account of the *opposing*
    lane.

    So each rate declares the rounds it was fitted on, and a round no rate covers
    yields an unavailable line.  What the predictor can represent is how much
    Lakebase burns while it is the thing doing the work; what it cannot represent
    is how long the endpoint sat idle waiting for someone else, which is why
    Round 5 needs its own rate rather than a wider band on the shared one.
    """

    if telemetry.observed_lakebase_dbu is not None:
        return Quantity.exact(
            telemetry.observed_lakebase_dbu,
            provenance=Provenance.MEASURED,
            basis="system.billing.usage posted DBU for the bout interval",
        )
    rate = _burn_rate_for(telemetry.round_id, calibration)
    if rate is None:
        if calibration is None:
            return Quantity.unavailable(
                "no posted DBU and no calibrated Lakebase burn rate is available yet"
            )
        return Quantity.unavailable(
            "no posted DBU, and no burn rate is calibrated on "
            f"{telemetry.round_id.value}; applying one fitted on other rounds "
            "would predict a number outside its support"
        )
    seconds = telemetry.bout_seconds
    return Quantity.banded(
        seconds * rate.point,
        low=seconds * rate.low,
        high=seconds * rate.high,
        basis=(
            f"bout wall-clock at a burn rate calibrated from {rate.sample_count} "
            f"reconciled {rate.basis}; band spans the observed sample spread"
        ),
    )


def _aurora_acu_quantity(telemetry: BoutTelemetry) -> Quantity:
    """Aurora capacity in ACU-hours -- measured, or explicitly unavailable.

    **There is no lane-clock estimate here any more, and that is the fix.**  The
    old one multiplied the competitor lane by the 2 ACU ceiling and called that
    "the conservative (higher) reading".  CloudWatch says otherwise
    (`.anti-demo-v7/aurora-acu-2026-08-21.md` §1): every round priced that way
    came out *above* its published figure, by 1.73x across Rounds 1-3 and 16.1x
    on Round 1 alone.  Two errors pointed in opposite directions and only the
    first had ever been reasoned about --

    * the **rate** was too high (the cluster peaks at a 1.525 ACU one-minute
      average and never reaches 2), which overstates, mildly; and
    * the **window** was far too short, which understates, massively.  Aurora
      bills the auto-pause descent, and no lane clock covers it: Round 1's
      15.31-second bout provisioned 420 seconds of billed capacity, of which
      97.2% fell after the bell.  For restores, a deleting instance kept
      reporting capacity for a further 276-513 seconds.

    A coefficient could not have fixed that, and neither can a wider band: two
    runs of the *same* round, with lanes within 6% of each other, measured 714.91
    and 1017.48 ACU-seconds, and the descents behind them ran 5 and 15 minutes
    for reasons that were not established.  The lane clock does not predict this
    quantity, so this function no longer pretends it does.

    What remains is the model's own §8 rule: a quantity that cannot be
    established yields an ``unavailable`` line, never a zero and never a guess.
    The bounds are still computed and named in the basis, because "somewhere
    between $0.005 and $0.06" is honest information and a fabricated point
    estimate is not.  :func:`aurora_acu_seconds_for` supplies real ACU-seconds
    for the rounds that have been sampled; ``AuroraCredentialProvider
    .sample_acu_seconds`` supplies them for a bout being run now.

    A supplied observation may be a band, and when it is, the band survives.
    ``server/bout_cost.py`` is the caller that uses this: Round 5's observation
    is the spread between two real bouts and Rounds 2 and 3 carry an unresolved
    deletion-drain question, and both are ``MEASURED`` at both ends.
    """

    point = telemetry.observed_acu_seconds_above_floor
    if point is not None:
        basis = telemetry.acu_observation_basis or (
            "CloudWatch ServerlessDatabaseCapacity integrated above the floor"
        )
        low = telemetry.observed_acu_seconds_low
        high = telemetry.observed_acu_seconds_high
        return _scale(
            Quantity(
                point=point,
                low=point if low is None else low,
                high=point if high is None else high,
                provenance=Provenance.MEASURED,
                basis=basis,
            ),
            Decimal(1) / SECONDS_PER_HOUR,
            basis=basis,
        )

    floor = aurora_wake_commitment_acu_hours()
    lane = telemetry.competitor_lane_seconds
    if lane is None:
        return Quantity.unavailable(
            "no ACU samples and no competitor lane clock; a wake commits to at "
            f"least {floor:.6f} ACU-hours it cannot get under"
        )
    ceiling = aurora_ceiling_acu_hours(lane, telemetry.teardown_allowance_seconds)
    return Quantity.unavailable(
        "no ACU samples; a lane clock does not predict Aurora's billed capacity "
        "-- it excludes the auto-pause descent, which was 97.2% of Round 1's "
        f"measured cost. Bounded to [{floor:.6f}, {ceiling:.6f}] ACU-hours: the "
        "lower bound is the auto-pause floor at the minimum running capacity, "
        "the upper is lane + teardown + descent at the sealed 2 ACU ceiling"
    )


def _restore_lines(
    telemetry: BoutTelemetry,
    rates: RateCard,
    shape: InstallationShape,
) -> list[CostLine]:
    """The point-in-time restore Rounds 2 and 3 provision and then destroy."""

    lifetime = _lifetime(
        telemetry,
        telemetry.observed_restore_lifetime_seconds,
        what="PITR restore",
    )
    lines: list[CostLine] = []

    if telemetry.competitor_id is CompetitorId.AURORA_SERVERLESS_V2:
        lines.append(
            _line(
                "Aurora temporary PITR restore compute",
                cloud=Cloud.AWS,
                kind=CostKind.COMPUTE,
                scope=EstimateScope.BOUT,
                lane_id="competitor",
                quantity=_aurora_acu_quantity(telemetry),
                rate=rates.aurora_acu_hour,
            )
        )
        storage_rate = rates.aurora_storage_gb_month
        storage_gb = shape.aurora_storage_gb
        storage_component = "Aurora temporary PITR restore storage"
    else:
        billed = _billed_seconds(lifetime, RDS_MINIMUM_BILLED_SECONDS)
        # `restore_db_instance_to_point_in_time` is called with the source
        # instance's own `DBInstanceClass`, so the restore bills at whatever the
        # baseline box is, and one class drives both lines.
        lines.append(
            _line(
                f"{rates.rds_compute_label} temporary PITR restore compute",
                cloud=Cloud.AWS,
                kind=CostKind.COMPUTE,
                scope=EstimateScope.BOUT,
                lane_id="competitor",
                quantity=_scale(
                    billed,
                    Decimal(1) / SECONDS_PER_HOUR,
                    basis=billed.basis,
                ),
                rate=rates.rds_instance_hour,
            )
        )
        storage_rate = rates.rds_gp3_gb_month
        storage_gb = shape.rds_allocated_gb
        storage_component = "RDS PostgreSQL temporary PITR restore gp3 storage"

    lines.append(
        _line(
            storage_component,
            cloud=Cloud.AWS,
            kind=CostKind.STORAGE,
            scope=EstimateScope.BOUT,
            lane_id="competitor",
            quantity=_scale(
                lifetime,
                storage_gb / SECONDS_PER_BILLING_MONTH,
                basis=f"{lifetime.basis}; {storage_gb} GB prorated across a 730-hour month",
            ),
            rate=storage_rate,
        )
    )
    lines.append(
        _line(
            "Temporary PITR restore public IPv4",
            cloud=Cloud.AWS,
            kind=CostKind.NETWORK,
            scope=EstimateScope.BOUT,
            lane_id="competitor",
            quantity=_scale(
                lifetime,
                Decimal(1) / SECONDS_PER_HOUR,
                basis=lifetime.basis,
            ),
            rate=rates.public_ipv4_hour,
        )
    )
    return lines


def _proxy_lines(telemetry: BoutTelemetry, rates: RateCard) -> list[CostLine]:
    """Round 5's per-bout RDS Proxy.

    Proxy capacity follows the target: 8 ACU for Aurora Serverless v2, 2 vCPU for
    the provisioned RDS instance.  This mirrors
    :func:`server.pricing.calculate_rds_proxy_cost`, and the tests assert the two
    produce the same number.
    """

    capacity = (
        Decimal(8) if telemetry.competitor_id is CompetitorId.AURORA_SERVERLESS_V2 else Decimal(2)
    )
    lifetime = _lifetime(telemetry, telemetry.observed_proxy_lifetime_seconds, what="RDS Proxy")
    billed = _billed_seconds(lifetime, RDS_MINIMUM_BILLED_SECONDS)
    return [
        _line(
            f"RDS Proxy capacity · {capacity} units",
            cloud=Cloud.AWS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.BOUT,
            lane_id="competitor",
            quantity=_scale(
                billed,
                capacity / SECONDS_PER_HOUR,
                basis=billed.basis,
            ),
            rate=rates.rds_proxy_capacity_hour,
        )
    ]


def _connection_spike_database_lines(
    telemetry: BoutTelemetry,
    rates: RateCard,
) -> list[CostLine]:
    """The database sitting behind Round 5's proxy.

    Round 5 provisions a competitor stack on *both* lanes.  ``infra/aws/locals.tf``
    binds a dedicated Aurora cluster and a dedicated RDS instance to the Round 5
    stack, ``server/connection_spike_live.py`` accepts either competitor, and its
    proxy registration hands the proxy ``DBClusterIdentifiers`` when Aurora is
    armed -- so the 128-client burst always lands on a real database.  Pricing
    only the proxy priced the pipe and not the thing on the end of it, and
    CloudTrail confirms two Round 5 bouts did run the Aurora lane: their measured
    compute is $0.023830 and $0.033916, missing from the estimate entirely.

    The two lanes are asymmetric and the asymmetry is the finding rather than an
    oversight:

    * A provisioned RDS instance is already running and already billed around the
      clock, so the burst adds no incremental instance-hours.  Its zero is a
      property of the lane, exactly the argument :func:`_baseline_wake_lines`
      makes for Round 1, and it is a real result rather than a missing number.
    * Aurora's minimum capacity is 0 ACU, so every unit of capacity the burst
      forces it to allocate is marginal and chargeable -- and it keeps being
      chargeable through the auto-pause descent after the burst has gone.
    """

    if telemetry.competitor_id is not CompetitorId.AURORA_SERVERLESS_V2:
        return [
            _line(
                f"{rates.rds_compute_label} connection spike · already-running instance",
                cloud=Cloud.AWS,
                kind=CostKind.COMPUTE,
                scope=EstimateScope.BOUT,
                lane_id="competitor",
                quantity=Quantity.exact(
                    Decimal(0),
                    provenance=Provenance.ASSUMED,
                    basis=(
                        "a provisioned RDS instance bills continuously, so absorbing a "
                        "burst adds no incremental instance-hours; only the proxy is "
                        "marginal"
                    ),
                ),
                rate=rates.rds_instance_hour,
            )
        ]
    return [
        _line(
            "Aurora Serverless v2 connection spike compute",
            cloud=Cloud.AWS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.BOUT,
            lane_id="competitor",
            quantity=_aurora_acu_quantity(telemetry),
            rate=rates.aurora_acu_hour,
        )
    ]


def _baseline_wake_lines(telemetry: BoutTelemetry, rates: RateCard) -> list[CostLine]:
    """Round 1 wakes an already-provisioned database; it creates nothing.

    Its only marginal AWS cost is the capacity the wake itself burns.  A
    provisioned RDS instance is already running and already billed, so waking it
    is marginally free -- which is a real result, not a gap.

    Waking Aurora is the opposite, and by a wider margin than the lane clock ever
    suggested: the one measured Round 1 bout ran 15.31 seconds and provisioned
    468.85 ACU-seconds, of which 13.00 fell inside the bout and 455.85 -- 97.2% --
    fell after the bell, on the auto-pause descent.  A round can be over long
    before the capacity it woke stops billing.
    """

    if telemetry.competitor_id is not CompetitorId.AURORA_SERVERLESS_V2:
        return [
            _line(
                "RDS PostgreSQL wake · already-running instance",
                cloud=Cloud.AWS,
                kind=CostKind.COMPUTE,
                scope=EstimateScope.BOUT,
                lane_id="competitor",
                quantity=Quantity.exact(
                    Decimal(0),
                    provenance=Provenance.ASSUMED,
                    basis=(
                        "a provisioned RDS instance bills continuously, so a wake adds "
                        "no incremental instance-hours"
                    ),
                ),
                rate=rates.rds_instance_hour,
            )
        ]
    return [
        _line(
            "Aurora Serverless v2 wake from zero ACU",
            cloud=Cloud.AWS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.BOUT,
            lane_id="competitor",
            quantity=_aurora_acu_quantity(telemetry),
            rate=rates.aurora_acu_hour,
        )
    ]


def _lakebase_lines(
    telemetry: BoutTelemetry,
    rates: RateCard,
    calibration: BurnRate | LakebaseBurnModel | None,
) -> list[CostLine]:
    return [
        _line(
            "Lakebase compute",
            cloud=Cloud.DATABRICKS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.BOUT,
            lane_id="lakebase",
            quantity=_lakebase_dbu_quantity(telemetry, calibration),
            rate=rates.lakebase_dbu,
        )
    ]


_ROUNDS_WITH_RESTORE = frozenset({RoundId.MAKE_SCHEMA_CHANGE_SAFELY, RoundId.RECOVER_DELETED_ORDER})
_ROUNDS_WITHOUT_AWS = frozenset({RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS})

# Which rounds Terraform actually stands a competitor database up for, keyed by
# the round key `infra/aws/locals.tf` uses.  `v7_round_keys = ["r1","r2","r3","r5"]`
# provisions an Aurora cluster per key; the narrower
# `v7_rds_round_keys = ["r2","r3","r5"]` provisions the RDS instances, which is
# why Round 1 appears here with a cluster and no box.  Every one of these lanes
# is armable, so every one of them owes a line.
# `tests/test_cost_model.py` reads the Terraform back and asserts this map agrees
# with it, because the two drifting apart is precisely how Round 5's Aurora lane
# went unpriced.
_AWS_ROUND_KEYS: dict[RoundId, str] = {
    RoundId.WAKE_IDLE_APP: "r1",
    RoundId.MAKE_SCHEMA_CHANGE_SAFELY: "r2",
    RoundId.RECOVER_DELETED_ORDER: "r3",
    RoundId.SURVIVE_CONNECTION_SPIKE: "r5",
}


def _competitor_database_rate(telemetry: BoutTelemetry, rates: RateCard) -> Rate:
    if telemetry.competitor_id is CompetitorId.AURORA_SERVERLESS_V2:
        return rates.aurora_acu_hour
    return rates.rds_instance_hour


def _lane_coverage_lines(
    lines: Sequence[CostLine],
    telemetry: BoutTelemetry,
    rates: RateCard,
) -> list[CostLine]:
    """Make a provisioned lane that produced no line impossible to miss.

    §8's rule is that a missing quantity yields an unavailable line and never a
    zero.  A missing *line* slipped underneath that rule: ``estimate.unavailable``
    counts lines whose quantity is absent and cannot count a line that was never
    emitted at all, which is how Round 5's Aurora compute read as $0.00 rather
    than as a gap for as long as it did.

    So the rule is enforced structurally rather than per route.  If a round
    Terraform provisions a competitor database for produces no competitor compute
    line, one is appended with an unavailable quantity.  Reaching this is a bug in
    the routing above, and the point is that the estimate says so out loud
    instead of quietly totalling less.
    """

    round_key = _AWS_ROUND_KEYS.get(telemetry.round_id)
    if round_key is None:
        return []
    if any(
        line.cloud is Cloud.AWS and line.kind is CostKind.COMPUTE and line.lane_id == "competitor"
        for line in lines
    ):
        return []
    return [
        _line(
            f"{telemetry.competitor_id.value} database compute · unrouted lane",
            cloud=Cloud.AWS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.BOUT,
            lane_id="competitor",
            quantity=Quantity.unavailable(
                f"infra/aws/locals.tf provisions a competitor database for {round_key}, "
                f"but the estimator emitted no compute line for {telemetry.round_id.value}"
            ),
            rate=_competitor_database_rate(telemetry, rates),
        )
    ]


def estimate_bout_cost(
    telemetry: BoutTelemetry,
    *,
    rates: RateCard | None = None,
    shape: InstallationShape | None = None,
    calibration: BurnRate | LakebaseBurnModel | None = None,
) -> BoutCostEstimate:
    """Price one bout's marginal cost from its telemetry.

    The result contains only ``bout_estimate`` lines.  Standing cost is a
    property of the installation, not of a bout, and is produced separately by
    :func:`estimate_carrying_cost`.
    """

    rates = rates or RateCard()
    shape = shape or InstallationShape()
    lines = _lakebase_lines(telemetry, rates, calibration)

    if telemetry.round_id in _ROUNDS_WITH_RESTORE:
        lines.extend(_restore_lines(telemetry, rates, shape))
    elif telemetry.round_id is RoundId.WAKE_IDLE_APP:
        lines.extend(_baseline_wake_lines(telemetry, rates))
    elif telemetry.round_id is RoundId.SURVIVE_CONNECTION_SPIKE:
        lines.extend(_connection_spike_database_lines(telemetry, rates))
        lines.extend(_proxy_lines(telemetry, rates))
    elif telemetry.round_id in _ROUNDS_WITHOUT_AWS:
        # Rounds 4 and 6 build no competing AWS stack, which is exactly what the
        # acceptance contract says they measure.  Emitting a zero here would
        # claim the AWS alternative is free; emitting nothing states the truth,
        # that this round priced no AWS side at all.
        pass

    lines.extend(_lane_coverage_lines(lines, telemetry, rates))

    return BoutCostEstimate(
        round_id=telemetry.round_id,
        competitor_id=telemetry.competitor_id,
        lines=tuple(lines),
    )


@dataclass(frozen=True, slots=True)
class CarryingWindow:
    """How long the standing cost is being measured over."""

    seconds: Decimal

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError("a carrying window must have a positive duration")

    @property
    def hours(self) -> Decimal:
        return self.seconds / SECONDS_PER_HOUR

    @property
    def months(self) -> Decimal:
        return self.seconds / SECONDS_PER_BILLING_MONTH


def estimate_carrying_cost(
    window: CarryingWindow,
    *,
    rates: RateCard | None = None,
    shape: InstallationShape | None = None,
    lakebase_always_on_dbu: Decimal | None = None,
    lakebase_storage_dsu: Decimal | None = None,
) -> BoutCostEstimate:
    """Price what the installation costs to keep alive over ``window``.

    This accrues whether or not anyone rings the bell.  It is returned in the
    same shape as a bout estimate so both can be rendered by one caller, but its
    lines are scoped ``CARRYING`` and ``OVERHEAD`` and must never be added to a
    bout total.
    """

    rates = rates or RateCard()
    shape = shape or InstallationShape()
    hours = window.hours
    months = window.months

    def fixed(value: Decimal, basis: str) -> Quantity:
        return Quantity.exact(value, provenance=Provenance.ASSUMED, basis=basis)

    lines = [
        _line(
            f"{rates.rds_compute_label} baseline instances",
            cloud=Cloud.AWS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.CARRYING,
            lane_id="competitor",
            quantity=fixed(
                hours * shape.rds_instances,
                "instance count sealed in the v7 manifest; class from "
                "server/capacity.py:RDS_INSTANCE_CLASS",
            ),
            rate=rates.rds_instance_hour,
        ),
        _line(
            "RDS PostgreSQL gp3 baseline storage",
            cloud=Cloud.AWS,
            kind=CostKind.STORAGE,
            scope=EstimateScope.CARRYING,
            lane_id="competitor",
            quantity=fixed(
                months * shape.rds_instances * shape.rds_allocated_gb,
                "allocated_storage = 20 GB gp3 in infra/aws/rds.tf",
            ),
            rate=rates.rds_gp3_gb_month,
        ),
        _line(
            "Aurora Serverless v2 baseline compute at the configured floor",
            cloud=Cloud.AWS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.CARRYING,
            lane_id="competitor",
            quantity=fixed(
                hours * shape.aurora_clusters * shape.aurora_min_acu,
                "min_capacity in infra/aws/aurora.tf; zero means the cluster parks free",
            ),
            rate=rates.aurora_acu_hour,
        ),
        _line(
            "Aurora baseline storage",
            cloud=Cloud.AWS,
            kind=CostKind.STORAGE,
            scope=EstimateScope.CARRYING,
            lane_id="competitor",
            quantity=fixed(
                months * shape.aurora_clusters * shape.aurora_storage_gb,
                "Aurora bills only the storage actually consumed by the cluster",
            ),
            rate=rates.aurora_storage_gb_month,
        ),
        _line(
            "Database public IPv4 addresses",
            cloud=Cloud.AWS,
            kind=CostKind.NETWORK,
            scope=EstimateScope.CARRYING,
            lane_id="competitor",
            quantity=fixed(
                hours * shape.public_ipv4_addresses,
                "one address per publicly reachable database endpoint",
            ),
            rate=rates.public_ipv4_hour,
        ),
        _line(
            "AWS-managed database credentials",
            cloud=Cloud.AWS,
            kind=CostKind.OTHER,
            scope=EstimateScope.CARRYING,
            lane_id="competitor",
            quantity=fixed(
                months * shape.managed_secrets,
                "one managed secret per database, from the v7 manifest",
            ),
            rate=rates.secret_month,
        ),
        _line(
            "Neutral m6i.large burst runner",
            cloud=Cloud.AWS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.OVERHEAD,
            lane_id="shared",
            quantity=fixed(
                hours * shape.runner_instances,
                "instance_type in infra/aws/round5_runner.tf",
            ),
            rate=rates.ec2_m6i_large_hour,
        ),
        _line(
            "Neutral runner gp3 root volume",
            cloud=Cloud.AWS,
            kind=CostKind.STORAGE,
            scope=EstimateScope.OVERHEAD,
            lane_id="shared",
            quantity=fixed(
                months * shape.runner_instances * shape.runner_root_gb,
                "root_block_device volume_size = 20 GB gp3 in the v7 Terraform state",
            ),
            rate=rates.ebs_gp3_gb_month,
        ),
        _line(
            "Neutral runner public IPv4",
            cloud=Cloud.AWS,
            kind=CostKind.NETWORK,
            scope=EstimateScope.OVERHEAD,
            lane_id="shared",
            quantity=fixed(
                hours * shape.runner_instances,
                "associate_public_ip_address = true for the runner",
            ),
            rate=rates.public_ipv4_hour,
        ),
    ]

    lines.append(
        _line(
            "Lakebase always-on minimum compute",
            cloud=Cloud.DATABRICKS,
            kind=CostKind.COMPUTE,
            scope=EstimateScope.CARRYING,
            lane_id="lakebase",
            quantity=(
                Quantity.exact(
                    lakebase_always_on_dbu,
                    provenance=Provenance.MEASURED,
                    basis=(
                        "system.billing.usage rows where "
                        "product_features.lakebase.compute_type = COMPUTE_NODE_ALWAYS_ON_MIN"
                    ),
                )
                if lakebase_always_on_dbu is not None
                else Quantity.unavailable("no posted always-on-minimum DBU for this window")
            ),
            rate=rates.lakebase_dbu,
        )
    )
    lines.append(
        _line(
            "Lakebase database, PITR, and snapshot storage",
            cloud=Cloud.DATABRICKS,
            kind=CostKind.STORAGE,
            scope=EstimateScope.CARRYING,
            lane_id="lakebase",
            quantity=(
                Quantity.exact(
                    lakebase_storage_dsu,
                    provenance=Provenance.MEASURED,
                    basis="system.billing.usage STORAGE_SPACE rows for the sealed projects",
                )
                if lakebase_storage_dsu is not None
                else Quantity.unavailable("no posted storage DSU for this window")
            ),
            rate=rates.lakebase_dsu,
        )
    )

    return BoutCostEstimate(
        round_id=RoundId.WAKE_IDLE_APP,
        competitor_id=CompetitorId.RDS_POSTGRES,
        lines=tuple(lines),
    )


# --------------------------------------------------------------------------- #
# The counterfactual: what a customer pays for rounds we provision nothing for
# --------------------------------------------------------------------------- #

# Rounds that stand no RDS instance up.  Round 1 has an Aurora cluster and no RDS
# box, because its RDS lane refuses to enter on engine semantics; Rounds 4 and 6
# build no competing AWS stack at all.  In all three a customer solving the same
# problem on AWS still pays for a Postgres, which is the whole point: RDS cannot
# scale to zero, so the bill does not follow the workload.
IMPUTED_RDS_ROUNDS: frozenset[RoundId] = frozenset(
    {
        RoundId.WAKE_IDLE_APP,
        RoundId.PUT_MODEL_SCORE_IN_APP,
        RoundId.ANALYZE_LIVE_ORDERS,
    }
)

# Rounds that stand no Aurora cluster up.  Round 1 is deliberately absent: its
# cluster is real, and giving it an imputed one on top would double-count the
# only lane that competes in it.
IMPUTED_AURORA_ROUNDS: frozenset[RoundId] = frozenset(
    {RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS}
)

# What a customer doing Rounds 4 and 6 on AWS would additionally need, and what
# is therefore missing from the figure.  Aurora has no native equivalent for
# either round's work, so a standing cluster is the floor of the alternative
# rather than the whole of it.  Named instead of guessed: an honest gap beats a
# confident number, and every one of these is priced per-provisioned-capacity or
# per-request in ways this installation has no measurement for.
UNPRICED_PIPELINE_SERVICES: tuple[str, ...] = ("DMS", "Glue", "Kinesis", "Firehose", "Lambda")

# The condition the customer-equivalent total may not be quoted without.  Lives
# here rather than in the renderer so the claim travels with the arithmetic that
# makes it true.
CUSTOMER_EQUIVALENT_FLOOR_REASON = (
    "What a customer would pay for the same workload, including rounds this "
    "installation provisions nothing for. A floor rather than an estimate: Rounds 4 "
    "and 6 additionally require pipeline services that are deliberately not priced "
    "here."
)

_IMPUTED_RDS_BASIS = (
    "No RDS instance is provisioned for this round. Priced as one {instance_class} "
    "running continuously, because that is what a customer pays: a provisioned RDS "
    "instance has no zero state. The smallest billable unit is the instance itself, "
    "and stopping it is an outage rather than a scale-down. That is a product "
    "boundary, not a setting we declined to configure. Modelled, not measured — no "
    "instance was described to produce this figure."
)

_IMPUTED_AURORA_COMPUTE_BASIS = (
    "serverlessv2_scaling_configuration.min_capacity = {min_acu}, so a parked "
    "cluster's compute is exactly zero rather than unmeasured. Modelled: no cluster "
    "is provisioned for this round."
)

# Stated only when the sealed floor is no longer zero, so the sentence above can
# never survive a configuration that has made it false.
_IMPUTED_AURORA_FLOOR_BASIS = (
    "serverlessv2_scaling_configuration.min_capacity = {min_acu}, so a parked "
    "cluster holds that floor rather than descending to zero, and its compute is a "
    "measured-rate charge against a modelled quantity. Modelled: no cluster is "
    "provisioned for this round."
)

_IMPUTED_AURORA_STANDING_BASIS = (
    "No Aurora cluster is provisioned for this round. Priced as one Serverless v2 "
    "cluster standing by at the sealed minimum capacity, so this line is storage, one "
    "public IPv4 address and one managed secret; the compute is a structural zero on "
    "its own line. Modelled, not measured. This is what it would cost to have Aurora "
    "standing by — it is not a lane result, and Aurora did not compete in this round. "
    "It covers a standing cluster only: Aurora has no native equivalent for this "
    "round's work, so a customer doing it on AWS would additionally need pipeline "
    "services (DMS, Glue, Kinesis, Firehose or Lambda), and those are deliberately "
    "not priced here. This figure is a floor, not an estimate."
)


def _imputed(value: Decimal, basis: str) -> Quantity:
    return Quantity.exact(value, provenance=Provenance.MODELED, basis=basis)


def imputed_round_carrying_lines(
    round_id: RoundId,
    window: CarryingWindow,
    *,
    rates: RateCard | None = None,
    shape: InstallationShape | None = None,
) -> tuple[CostLine, ...]:
    """What a customer would pay to stand this round's AWS lanes up, where we do not.

    Never added to :func:`estimate_carrying_cost`.  That function answers what this
    installation is billed, and these resources do not exist.

    Returns an empty tuple for a round that provisions its own AWS lanes, so a
    caller can ask about every round without knowing which ones owe a
    counterfactual.
    """

    rates = rates or RateCard()
    shape = shape or InstallationShape()
    hours = window.hours
    months = window.months
    lines: list[CostLine] = []

    if round_id in IMPUTED_RDS_ROUNDS:
        basis = _IMPUTED_RDS_BASIS.format(instance_class=rates.rds_instance_class)
        lines.extend(
            (
                _line(
                    f"{rates.rds_compute_label} · modelled continuous instance",
                    cloud=Cloud.AWS,
                    kind=CostKind.COMPUTE,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(hours, basis),
                    rate=rates.rds_instance_hour,
                    imputed=True,
                ),
                _line(
                    "RDS PostgreSQL gp3 storage · modelled",
                    cloud=Cloud.AWS,
                    kind=CostKind.STORAGE,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(months * shape.rds_allocated_gb, basis),
                    rate=rates.rds_gp3_gb_month,
                    imputed=True,
                ),
                _line(
                    "RDS PostgreSQL public IPv4 · modelled",
                    cloud=Cloud.AWS,
                    kind=CostKind.NETWORK,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(hours, basis),
                    rate=rates.public_ipv4_hour,
                    imputed=True,
                ),
                _line(
                    "RDS PostgreSQL managed credential · modelled",
                    cloud=Cloud.AWS,
                    kind=CostKind.OTHER,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(months, basis),
                    rate=rates.secret_month,
                    imputed=True,
                ),
            )
        )

    if round_id in IMPUTED_AURORA_ROUNDS:
        compute_units = hours * shape.aurora_min_acu
        compute_template = (
            _IMPUTED_AURORA_COMPUTE_BASIS if compute_units == 0 else _IMPUTED_AURORA_FLOOR_BASIS
        )
        lines.extend(
            (
                _line(
                    "Aurora Serverless v2 compute at the configured floor · modelled",
                    cloud=Cloud.AWS,
                    kind=CostKind.COMPUTE,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(
                        compute_units,
                        compute_template.format(min_acu=_plain(shape.aurora_min_acu)),
                    ),
                    rate=rates.aurora_acu_hour,
                    imputed=True,
                ),
                _line(
                    "Aurora baseline storage · modelled",
                    cloud=Cloud.AWS,
                    kind=CostKind.STORAGE,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(
                        months * shape.aurora_storage_gb, _IMPUTED_AURORA_STANDING_BASIS
                    ),
                    rate=rates.aurora_storage_gb_month,
                    imputed=True,
                ),
                _line(
                    "Aurora writer public IPv4 · modelled",
                    cloud=Cloud.AWS,
                    kind=CostKind.NETWORK,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(hours, _IMPUTED_AURORA_STANDING_BASIS),
                    rate=rates.public_ipv4_hour,
                    imputed=True,
                ),
                _line(
                    "Aurora managed credential · modelled",
                    cloud=Cloud.AWS,
                    kind=CostKind.OTHER,
                    scope=EstimateScope.CARRYING,
                    lane_id="competitor",
                    quantity=_imputed(months, _IMPUTED_AURORA_STANDING_BASIS),
                    rate=rates.secret_month,
                    imputed=True,
                ),
            )
        )

    return tuple(lines)


def imputed_total_usd(lines: Iterable[CostLine]) -> Decimal:
    """Sum *only* the imputed lines out of a mixed sequence.

    The filter is the point.  A caller that sums a sequence itself can include a
    real line by accident and produce a total that is neither of the two the model
    is allowed to state.
    """

    return _total((line for line in lines if line.imputed), "usd")


@dataclass(frozen=True, slots=True)
class CustomerEquivalent:
    """The counterfactual half of the standing cost, with its condition attached.

    Two questions, and their sum answers neither: *what this installation pays*
    and *what a customer would pay for the same workload*.  The first excludes
    every line in here by construction, because these resources do not exist.

    ``floor`` is not a caveat someone remembered to write down.  For Rounds 4 and
    6 the figure is a lower bound rather than an estimate, because Aurora has no
    native equivalent for either round's work and the pipeline services a customer
    would additionally need are deliberately unpriced.  Carrying it as a field is
    what stops the total being quoted as an estimate once the prose is out of
    sight.
    """

    # Kept per round rather than flattened, so the total can be taken apart again
    # without re-deriving which round owed what.  The round travels beside the
    # lines instead of being encoded into ``lane_id``: these are competitor-lane
    # lines and must stay recognisable as such to a renderer that groups by lane.
    by_round: tuple[tuple[RoundId, tuple[CostLine, ...]], ...]
    floor: bool
    floor_reason: str
    unpriced_services: tuple[str, ...]

    def __post_init__(self) -> None:
        if any(not line.imputed for line in self.lines):
            raise ValueError("a customer-equivalent total may contain only imputed lines")
        if self.floor and not self.floor_reason.strip():
            raise ValueError("a floor must say why it is a floor rather than an estimate")
        if not self.floor and self.unpriced_services:
            raise ValueError("naming an unpriced service makes the figure a floor")

    @property
    def rounds(self) -> tuple[RoundId, ...]:
        return tuple(round_id for round_id, _ in self.by_round)

    @property
    def lines(self) -> tuple[CostLine, ...]:
        return tuple(line for _, lines in self.by_round for line in lines)

    @property
    def usd(self) -> Decimal:
        return imputed_total_usd(self.lines)

    def for_round(self, round_id: RoundId) -> tuple[CostLine, ...]:
        for candidate, lines in self.by_round:
            if candidate is round_id:
                return lines
        return ()


def customer_equivalent_carrying_cost(
    window: CarryingWindow,
    *,
    rates: RateCard | None = None,
    shape: InstallationShape | None = None,
    rounds: Iterable[RoundId] | None = None,
) -> CustomerEquivalent:
    """Every imputed line the installation owes, as one object with its condition.

    ``rounds`` defaults to every round, of which only those provisioning no AWS
    lane of their own produce anything.
    """

    requested = tuple(rounds) if rounds is not None else tuple(RoundId)
    by_round: list[tuple[RoundId, tuple[CostLine, ...]]] = []
    floor_rounds: list[RoundId] = []
    for round_id in requested:
        produced = imputed_round_carrying_lines(round_id, window, rates=rates, shape=shape)
        if not produced:
            continue
        by_round.append((round_id, produced))
        if round_id in IMPUTED_AURORA_ROUNDS:
            floor_rounds.append(round_id)
    floor = bool(floor_rounds)
    return CustomerEquivalent(
        by_round=tuple(by_round),
        floor=floor,
        floor_reason=CUSTOMER_EQUIVALENT_FLOOR_REASON if floor else "",
        unpriced_services=UNPRICED_PIPELINE_SERVICES if floor else (),
    )


# --------------------------------------------------------------------------- #
# The idle contrast: three engines, one idle minute, expressed as one object
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class IdleContrastLane:
    """What one engine bills for an idle day, and how far down it can descend.

    ``descent_seconds`` is ``None`` for an engine that has no descent at all,
    which is a different statement from descending after a long interval and is
    the reason the three figures differ.
    """

    label: str
    descent_seconds: int | None
    usd_per_day: Decimal | None
    compute_usd_per_day: Decimal | None
    provenance: Provenance
    imputed: bool
    basis: str

    def __post_init__(self) -> None:
        if not self.label.strip():
            raise ValueError("an idle lane must name the engine it describes")
        if not self.basis.strip():
            raise ValueError("an idle lane must carry the basis of its figure")
        if self.usd_per_day is None and self.provenance is not Provenance.UNAVAILABLE:
            raise ValueError("a lane with no figure must be marked unavailable")
        if self.usd_per_day is not None and self.provenance is Provenance.UNAVAILABLE:
            raise ValueError("an unavailable lane cannot carry a figure")
        if self.imputed and self.provenance is not Provenance.MODELED:
            raise ValueError("an imputed lane's figure can only be modelled")


@dataclass(frozen=True, slots=True)
class IdleContrast:
    """What three engines bill for the same idle minute, and why they differ.

    One object rather than three numbers a renderer has to relate.  Leaving them
    loose is how the comparison once reached a surface labelled "Lakebase costs
    more than everything": the relationship between the figures was reconstructed
    at render time, and it was reconstructed wrongly.  Here the ordering, the
    multiple and the sentence that states it are all derived once, from the same
    inputs as the figures.
    """

    lanes: tuple[IdleContrastLane, ...]
    summary: str
    basis: str
    dearest: IdleContrastLane
    cheapest: IdleContrastLane
    multiple: Decimal | None

    def __post_init__(self) -> None:
        if not self.lanes:
            raise ValueError("a contrast needs lanes to contrast")
        if not self.summary.strip() or not self.basis.strip():
            raise ValueError("a contrast must state what it compares and on what basis")
        priced = [lane for lane in self.lanes if lane.usd_per_day is not None]
        if self.dearest not in priced or self.cheapest not in priced:
            raise ValueError("a contrast may only rank lanes whose figure is known")
        if any(
            lane.usd_per_day > self.dearest.usd_per_day  # type: ignore[operator]
            for lane in priced
        ):
            raise ValueError("the dearest lane is not the dearest lane")

    def lane(self, label: str) -> IdleContrastLane:
        for candidate in self.lanes:
            if candidate.label == label:
                return candidate
        raise KeyError(label)


_A_DAY_SECONDS = Decimal(24) * SECONDS_PER_HOUR

_IDLE_CONTRAST_BASIS = (
    "One idle day priced from the same rate card for all three engines, at each "
    "vendor's own configured floor. Nothing here is a lane result: no round is run "
    "to produce it, which is why it holds without a bout."
)


def _plain(value: Decimal) -> str:
    """A Decimal without trailing zeros, so a quantity reads as configured."""

    normalized = value.normalize()
    text = format(normalized, "f")
    return text


def _usd(value: Decimal, places: int = 2) -> str:
    return f"${value.quantize(Decimal(1).scaleb(-places)):,}"


def idle_contrast(
    *,
    rates: RateCard | None = None,
    shape: InstallationShape | None = None,
    lakebase_idle_usd_per_day: Decimal | None = None,
    lakebase_idle_basis: str = "",
) -> IdleContrast:
    """The three-way idle comparison, derived rather than assembled by a renderer.

    Lakebase's figure is the caller's to supply, because an idle Lakebase endpoint
    bills posted storage and this module is not allowed to invent a posted
    quantity.  Omitting it yields an unavailable lane that still carries its
    descent interval, which is the part of the comparison that does not need a
    price: the ranking below is stated over the lanes that priced and never over
    the one that did not.
    """

    rates = rates or RateCard()
    shape = shape or InstallationShape()
    window = CarryingWindow(seconds=_A_DAY_SECONDS)

    rds_lines = imputed_round_carrying_lines(
        RoundId.WAKE_IDLE_APP, window, rates=rates, shape=shape
    )
    aurora_lines = imputed_round_carrying_lines(
        RoundId.ANALYZE_LIVE_ORDERS, window, rates=rates, shape=shape
    )
    aurora_only = tuple(line for line in aurora_lines if "aurora" in line.component.lower())

    def compute_of(lines: Sequence[CostLine]) -> Decimal:
        return _total(
            (line for line in lines if line.kind is CostKind.COMPUTE),
            "usd",
        )

    rds = IdleContrastLane(
        label="RDS PostgreSQL",
        descent_seconds=None,
        usd_per_day=imputed_total_usd(rds_lines),
        compute_usd_per_day=compute_of(rds_lines),
        provenance=Provenance.MODELED,
        imputed=True,
        basis=rds_lines[0].quantity.basis,
    )
    aurora = IdleContrastLane(
        label="Aurora Serverless v2",
        descent_seconds=AURORA_AUTO_PAUSE_SECONDS,
        usd_per_day=imputed_total_usd(aurora_only),
        compute_usd_per_day=compute_of(aurora_only),
        provenance=Provenance.MODELED,
        imputed=True,
        basis=_IMPUTED_AURORA_STANDING_BASIS,
    )
    lakebase = IdleContrastLane(
        label="Lakebase",
        descent_seconds=LAKEBASE_SUSPEND_SECONDS,
        usd_per_day=lakebase_idle_usd_per_day,
        compute_usd_per_day=None,
        provenance=(
            Provenance.UNAVAILABLE if lakebase_idle_usd_per_day is None else Provenance.MEASURED
        ),
        imputed=False,
        basis=(
            lakebase_idle_basis.strip()
            or (
                "An idle Lakebase endpoint suspends and bills posted storage. No posted "
                "storage figure was supplied for this window, so the lane is unpriced "
                "rather than zero."
            )
        ),
    )

    lanes = (lakebase, aurora, rds)
    priced = [lane for lane in lanes if lane.usd_per_day is not None]
    dearest = max(priced, key=lambda lane: lane.usd_per_day or Decimal(0))
    cheapest = min(priced, key=lambda lane: lane.usd_per_day or Decimal(0))
    cheapest_usd = cheapest.usd_per_day or Decimal(0)
    multiple = None if cheapest_usd == 0 else (dearest.usd_per_day or Decimal(0)) / cheapest_usd

    summary = (
        f"At rest, {lakebase.label} descends after {LAKEBASE_SUSPEND_SECONDS}s and "
        f"{aurora.label} after {AURORA_AUTO_PAUSE_SECONDS}s; a provisioned "
        f"{rds.label} instance never descends. So {lakebase.label} and {aurora.label} "
        f"bill storage while {rds.label} bills a whole instance"
    )
    if multiple is not None:
        summary += f" — about {multiple.quantize(Decimal(1))}x more per idle day"
        summary += (
            f", and on compute alone {_usd(rds.compute_usd_per_day or Decimal(0))}/day "
            f"against exactly {_usd(aurora.compute_usd_per_day or Decimal(0))}/day."
        )
    else:
        summary += "."

    return IdleContrast(
        lanes=lanes,
        summary=summary,
        basis=_IDLE_CONTRAST_BASIS,
        dearest=dearest,
        cheapest=cheapest,
        multiple=multiple,
    )


@dataclass(frozen=True, slots=True)
class Reconciliation:
    """One round's estimate measured against what the provider actually posted."""

    label: str
    cloud: Cloud
    estimated_usd: Decimal
    posted_usd: Decimal | None
    estimate_low_usd: Decimal | None = None
    estimate_high_usd: Decimal | None = None

    @property
    def error_usd(self) -> Decimal | None:
        return None if self.posted_usd is None else self.estimated_usd - self.posted_usd

    @property
    def error_fraction(self) -> Decimal | None:
        if self.posted_usd is None or self.posted_usd == 0:
            return None
        error = self.error_usd
        return None if error is None else error / self.posted_usd

    @property
    def posted_within_band(self) -> bool | None:
        if self.posted_usd is None:
            return None
        low = self.estimate_low_usd
        high = self.estimate_high_usd
        if low is None or high is None:
            return None
        return low <= self.posted_usd <= high


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    rows: tuple[Reconciliation, ...] = field(default_factory=tuple)

    @property
    def total_estimated_usd(self) -> Decimal:
        return sum((row.estimated_usd for row in self.rows), Decimal(0))

    @property
    def total_posted_usd(self) -> Decimal | None:
        posted = [row.posted_usd for row in self.rows if row.posted_usd is not None]
        return sum(posted, Decimal(0)) if posted else None

    @property
    def total_error_usd(self) -> Decimal | None:
        total_posted = self.total_posted_usd
        if total_posted is None:
            return None
        covered = sum(
            (row.estimated_usd for row in self.rows if row.posted_usd is not None),
            Decimal(0),
        )
        return covered - total_posted

    @property
    def coverage(self) -> tuple[int, int]:
        """How many rows have a posted actual to compare against, and the total."""

        return sum(1 for row in self.rows if row.posted_usd is not None), len(self.rows)


def _lane_seconds(snapshot: SessionSnapshot, lane_id: str) -> Decimal | None:
    """The lane clock a receipt recorded, in seconds.

    Round 5 is judged on setup time rather than bout elapsed, and its setup
    snapshot is where the proxy's own lifetime is visible, so it takes priority
    for exactly the same reason :func:`server.receipts.derive_receipt` prefers it.
    """

    setup = snapshot.round5_setup
    if setup is not None:
        lane = setup.lanes.get(lane_id)
        elapsed = None if lane is None else lane.setup_elapsed_ms
    else:
        lane = snapshot.lanes.get(lane_id)
        elapsed = None if lane is None else lane.elapsed_ms
    return None if elapsed is None else Decimal(str(elapsed)) / Decimal(1000)


def telemetry_from_snapshot(
    snapshot: SessionSnapshot,
    **observations: Decimal | None,
) -> BoutTelemetry | None:
    """Turn a stored bout receipt into estimator input.

    Returns ``None`` for a bout that never started, because a session that was
    cancelled or failed before the bell has no window to price.  That is not the
    same as a bout that cost nothing, and the caller must not treat it as zero.

    Any ``observed_*`` keyword overrides the corresponding telemetry field, which
    is how a provider-confirmed lifetime is promoted over a lane clock.
    """

    if snapshot.run_started_at is None:
        return None
    span = (snapshot.updated_at - snapshot.run_started_at).total_seconds()
    if span <= 0:
        return None
    accepted = {name: value for name, value in observations.items() if value is not None}
    return BoutTelemetry(
        round_id=snapshot.round.id,
        competitor_id=snapshot.competitor.id,
        bout_seconds=Decimal(str(span)),
        lakebase_lane_seconds=_lane_seconds(snapshot, "lakebase"),
        competitor_lane_seconds=_lane_seconds(snapshot, "competitor"),
        **accepted,  # type: ignore[arg-type]
    )


def calibrate_lakebase_burn(
    samples: Sequence[tuple[Decimal, Decimal]],
    *,
    rounds: Iterable[RoundId],
    basis: str = "per-bout samples",
) -> BurnRate:
    """Derive the DBU-per-bout-second rate the estimator uses without billing.

    This is the bridge that makes prediction possible: reconciled windows produce
    a burn rate, and every later bout is then priced from its own clock alone.

    Each sample is ``(posted_dbu, bout_seconds)`` for a window in which exactly
    one bout ran, so nothing else can be sharing the meter.  The band is the
    observed spread of the samples rather than an invented tolerance.

    ``rounds`` is not optional, and that is deliberate.  A rate that does not say
    what it was fitted on is a rate somebody will apply to anything, which is the
    defect this signature exists to prevent.
    """

    if not samples:
        raise ValueError("calibration requires at least one reconciled sample")
    support = frozenset(rounds)
    if not support:
        raise ValueError("calibration must name the rounds the samples came from")
    rates: list[Decimal] = []
    for posted_dbu, bout_seconds in samples:
        if bout_seconds <= 0:
            raise ValueError("calibration requires a positive bout interval")
        if posted_dbu < 0:
            raise ValueError("posted DBU must be non-negative")
        rates.append(posted_dbu / bout_seconds)
    return BurnRate(
        point=sum(rates, Decimal(0)) / Decimal(len(rates)),
        low=min(rates),
        high=max(rates),
        sample_count=len(rates),
        rounds=support,
        basis=basis,
    )


def calibrate_from_samples(
    samples: Sequence[LakebaseSample],
    *,
    rounds: Iterable[RoundId] | None = None,
    basis: str = "per-bout samples",
) -> BurnRate:
    """Calibrate from labelled samples, defaulting the support to their rounds.

    Defaulting the support to the rounds the samples actually came from is the
    conservative choice: it can only ever be widened deliberately, never by
    forgetting to narrow it.
    """

    if not samples:
        raise ValueError("calibration requires at least one reconciled sample")
    support = (
        frozenset(rounds)
        if rounds is not None
        else frozenset(sample.round_id for sample in samples)
    )
    return calibrate_lakebase_burn(
        [(sample.posted_dbu, sample.bout_seconds) for sample in samples],
        rounds=support,
        basis=basis,
    )


def leave_one_out(samples: Sequence[LakebaseSample]) -> tuple[HeldOutPrediction, ...]:
    """Score the predictor out of sample: fit on n-1, predict the held-out one.

    Calibrating on every sample and then reporting the fit against those same
    samples is circular and flatters the model.  With this few samples
    leave-one-out is the only honest accuracy statement available, and it is the
    one the cost analysis already uses, so the two remain comparable.

    Raises for a single sample rather than inventing an error figure for it.  A
    one-sample rate cannot be validated out of sample at all, and saying so is
    the finding -- Round 5 has exactly one isolable bout.
    """

    if len(samples) < 2:
        raise ValueError("leave-one-out cross-validation needs at least two samples")
    rows: list[HeldOutPrediction] = []
    for index, held_out in enumerate(samples):
        trained = [sample for position, sample in enumerate(samples) if position != index]
        fitted = calibrate_from_samples(trained, rounds=frozenset({held_out.round_id}))
        rows.append(
            HeldOutPrediction(
                label=held_out.label,
                round_id=held_out.round_id,
                trained_on=len(trained),
                predicted_dbu=held_out.bout_seconds * fitted.point,
                posted_dbu=held_out.posted_dbu,
            )
        )
    return tuple(rows)


# Every bout this installation has posted whose Lakebase DBU could be isolated at
# 10-minute grain, from the restore-shaped rounds.  Five samples, up from the two
# the first calibration had: the three new ones all land below the old floor, so
# the point rate falls 11% from 8.234087e-05 to 7.291618e-05 and the band widens
# downward.  Windows and the isolating SQL are in
# `.anti-demo-v7/cost-analysis-2026-08-20.md` §2d and §3.
V7_RESTORE_SAMPLES: tuple[LakebaseSample, ...] = (
    LakebaseSample(
        "E27A9405", RoundId.MAKE_SCHEMA_CHANGE_SAFELY, Decimal("0.034080"), Decimal("479.219")
    ),
    LakebaseSample(
        "A672140E", RoundId.RECOVER_DELETED_ORDER, Decimal("0.011597"), Decimal("123.941")
    ),
    LakebaseSample(
        "063A5187", RoundId.MAKE_SCHEMA_CHANGE_SAFELY, Decimal("0.033961667"), Decimal("508.09")
    ),
    LakebaseSample(
        "6CF4C290", RoundId.RECOVER_DELETED_ORDER, Decimal("0.051356667"), Decimal("812.03")
    ),
    LakebaseSample(
        "92479CA8", RoundId.RECOVER_DELETED_ORDER, Decimal("0.044020000"), Decimal("630.57")
    ),
)

# Round 5's only isolable bout.  `F9D4023E` was complete and contract-satisfying
# -- 128 attempts, 128 successes, zero errors on both lanes -- and burned 82
# CU-seconds, making Round 5 the *cheapest* round on Databricks rather than the
# dearest.  One sample cannot be cross-validated, which is why this rate is
# published separately and its sample count travels with it.
V7_CONNECTION_SPIKE_SAMPLES: tuple[LakebaseSample, ...] = (
    LakebaseSample(
        "F9D4023E", RoundId.SURVIVE_CONNECTION_SPIKE, Decimal("0.004851667"), Decimal("813.37")
    ),
)


def v7_lakebase_burn_model() -> LakebaseBurnModel:
    """The calibration this installation's posted billing actually supports.

    Rounds 1, 4 and 6 are deliberately absent and get an unavailable line rather
    than a borrowed rate.  Round 1's only record covers two bouts and is an upper
    bound for the pair; Round 4's 18.6-second bout cannot be separated from the
    background activity sharing its 10-minute records; Round 6 has never run a
    bout at all.  None of those is a number, and none of them should be made to
    look like one.
    """

    return LakebaseBurnModel(
        rates=(
            calibrate_from_samples(
                V7_RESTORE_SAMPLES,
                basis="restore-round bouts, whose Lakebase endpoint works throughout",
            ),
            calibrate_from_samples(
                V7_CONNECTION_SPIKE_SAMPLES,
                basis=(
                    "connection-spike bout, whose clock measures the competitor's "
                    "proxy build while Lakebase idles"
                ),
            ),
        )
    )
