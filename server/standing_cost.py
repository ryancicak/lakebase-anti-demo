"""What this installation is billed for while nobody is ringing the bell.

The marginal question -- "what did that bout cost?" -- is answered elsewhere and
is usually the smaller number. This module answers the other one. Four RDS
instances, four Aurora clusters, a runner, a synced-table pipeline and an
always-on app bill every second of every day whether or not anyone opens the
browser, and none of that appears on a bout receipt.

**This path must never read CDF or change-feed status through the Lakebase
control plane. Reproduced twice, it wakes the endpoint and bills the thing being
measured. ``get-endpoint`` is safe across 137 samples and is the only
control-plane call this path may make.**

So there are no provider calls here at all. The AWS side is the sealed shape
priced by :func:`server.cost_model.estimate_carrying_cost`, whose arithmetic
already reproduces the account's day-rate; the Databricks side arrives as posted
usage the caller read from ``system.billing.usage``, which is a Delta query and
wakes nothing. A monitor that bills its subject is worse than no monitor.

Five properties are load-bearing and easy to lose in a refactor:

1. **There are no rate literals in this file.** Every dollar comes from
   :class:`~server.cost_model.RateCard`, :class:`~server.cost_model.InstallationShape`
   or an injected posted observation, evaluated at call time. The design note this
   was built from carried a rate table; transcribing it would have baked figures
   that had already moved into the very thing built to eliminate hardcoded
   figures.
2. **The origin is ``manifest.created_at``.** Not ``expires_at``, which is a
   reaper deadline, and not ``last_reset_at``, which re-seeds data and creates
   nothing. Neither is read here. An expired seal does not stop billing, so this
   disclosure never consults the TTL and there is no code path for expiry to
   take.
3. **Six lanes, not four.** The neutral runner belongs to neither corner and the
   Databricks platform lane is neither corner's engine; omitting either leaves a
   total that does not reconcile. No share of the bill is quoted for either here:
   this docstring used to call the platform lane "about two thirds", a fraction
   computed before the lane stopped counting compute that predates the
   installation, after which it described neither total. Equally, the platform
   lane is not Lakebase -- folding a pipeline and an app's compute into Lakebase
   would overstate Lakebase roughly ninefold, and an error that points against us
   is still an error.
4. **Two totals, each inseparable from its condition**, and posted actuals that
   never blend into a projection. Variance is measured only against the
   projection restricted to the posted window. The lane figures are the parts of
   the *first* total -- the one the panel headlines -- so a reader can add the
   figures in front of them and arrive at it; compute that predates the
   installation is disclosed on its own rather than folded into the lane it
   arrived in.
5. **A bare ``$0.00`` may never appear as a lane figure.** Missing data yields
   ``unavailable``, following ``Quantity.unavailable``. A zero is allowed only
   when its derivation travels on the same field, which is why Aurora compute at
   ``min_capacity = 0`` can be a zero and nothing else can.

Every internal sum is kept in ``Decimal`` and converted to ``float`` once, at the
edge, when the payload is built. Summing the rendered floats instead would make
the lane subtotals disagree with the total by a few units in the last place, and
a disclosure whose parts do not add up is the thing this module exists to avoid.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from .capacity import AURORA_AUTO_PAUSE_SECONDS, LAKEBASE_SUSPEND_SECONDS
from .cost_model import (
    AS_RUN_RDS_INSTANCE_CLASS,
    IMPUTED_RDS_ROUNDS,
    CarryingWindow,
    Cloud,
    CostKind,
    CostLine,
    InstallationShape,
    Provenance,
    RateCard,
    estimate_carrying_cost,
    number_word,
)
from .models import (
    RoundId,
    StandingCostComponent,
    StandingCostContinuous,
    StandingCostCredits,
    StandingCostDisclosure,
    StandingCostDrift,
    StandingCostDriftFinding,
    StandingCostFairness,
    StandingCostFigure,
    StandingCostLane,
    StandingCostLaneId,
    StandingCostPosted,
    StandingCostPredating,
    StandingCostTotal,
    StandingCostTotals,
)
from .pricing import UnknownRdsInstanceClassError, rds_instance_hour_usd
from .reconcile import IPV4_DRIFT, MISSING_RESIDENT, Finding, ObservedResource
from .reconcile import ReconciliationReport as DriftReport

HOURS_PER_DAY = Decimal(24)
SECONDS_PER_HOUR = Decimal(3600)

CloudName = Literal["aws", "databricks"]
LaneEvidence = Literal[
    "rate_card_derived",
    "posted_actual",
    "posted_projection",
    "sealed_shape_only",
    "unpriced",
]

_KIND_NAMES: dict[CostKind, str] = {
    CostKind.COMPUTE: "compute",
    CostKind.STORAGE: "storage",
    CostKind.NETWORK: "network",
    CostKind.OTHER: "other",
}

_PROVENANCE_NAMES: dict[Provenance, str] = {
    Provenance.MEASURED: "measured",
    Provenance.MODELED: "modeled",
    Provenance.ASSUMED: "assumed",
    Provenance.UNAVAILABLE: "unavailable",
}

# Which rounds an RDS instance actually stands for, derived rather than written
# down. The RDS caveat below used to name Rounds 1, 2, 3 and 5 and explain that
# Round 1's box billed without ever being timed. Round 1's instance has since
# been deleted -- `infra/aws/locals.tf` keys the fleet on `["r2","r3","r5"]` and
# the v7 manifest seals `rds: null` for Round 1 -- so that sentence described a
# box that no longer exists. Reading the list off `IMPUTED_RDS_ROUNDS` means the
# next deletion cannot leave the prose behind again.
_ROUND_NUMBERS: dict[RoundId, int] = {
    round_id: number for number, round_id in enumerate(RoundId, start=1)
}
_STANDING_RDS_ROUND_NUMBERS: tuple[int, ...] = tuple(
    _ROUND_NUMBERS[round_id] for round_id in RoundId if round_id not in IMPUTED_RDS_ROUNDS
)
_IMPUTED_RDS_ROUND_NUMBERS: tuple[int, ...] = tuple(
    _ROUND_NUMBERS[round_id] for round_id in RoundId if round_id in IMPUTED_RDS_ROUNDS
)


def _round_list(numbers: tuple[int, ...]) -> str:
    """"Rounds 2, 3 and 5" from (2, 3, 5), so the prose tracks the fleet."""

    if not numbers:
        return "no round"
    if len(numbers) == 1:
        return f"Round {numbers[0]}"
    head = ", ".join(str(number) for number in numbers[:-1])
    return f"Rounds {head} and {numbers[-1]}"

# The paragraph already on the proof surface, reused rather than rewritten. Only
# the figures are slots, so a rate change moves them and the argument stays in one
# voice. The claim it makes -- that our half is the larger one -- is checked against
# the derivation before the paragraph is stated, and withheld rather than reworded
# when it stops holding.
#
# It used to carry a second sentence comparing the margin to what it was "before
# their four boxes were resized up". When that was written the resize had not
# happened, so the sentence narrated an event that had not occurred and blamed
# the ratio on it. The resize has since landed -- 2026-08-21T14:48:36Z -- and the
# sentence stays deleted anyway, because the panel holds no earlier margin to
# compare against and would be sourcing the "before" from memory. The current
# ratio is the only comparison there is evidence for.
_FAIRNESS_PARAGRAPH = (
    "Both sides carry standing cost here, and ours is the larger half — {databricks}/day "
    "Databricks against {aws}/day AWS, a {now}x margin. The difference is capability, "
    "not this bill: Lakebase can scale to zero and does at {suspend}s. No provisioned "
    "RDS instance can scale to zero at any price."
)

# The one platform meter this module says anything more about than "it bills".
# Held here and imported by the caller that labels the posted rows, so the two
# cannot drift into naming the same pipeline two ways -- which would silently
# withhold the paragraph below rather than fail.
ROUND4_PIPELINE_LABEL = "Round 4 synced-table pipeline"

# The platform meters that are deliberately intermittent: brought up when a round
# arms and released once its bout has settled. Declared per component for the
# reason `bout_cost.ROUNDS_WITHOUT_AURORA` is -- being up around the clock is a
# property of the resource, not of the method that prices it -- because the
# divisor that is exact for one is wrong for the other.
#
# For a meter that never stops, the span from its first posted interval to its
# last *is* its uptime, so dividing the amount by that span gives a rate. That
# holds for the AWS side and for the App's own compute, and their denominators are
# deliberately not touched. For a meter that starts and stops, the span contains
# every hour it was down, and the quotient is a duty-cycle-blended average of one
# installation's habits wearing the label of a rate. Sampled at 62.5% uptime that
# blend read $11.07/day for a line that bills $14.57/day while it is actually up
# -- the lower of the two, so the error ran in the direction that under-warns.
INTERMITTENT_PLATFORM_COMPONENTS = frozenset({ROUND4_PIPELINE_LABEL})

# The pipeline's schedule is CONTINUOUS by requirement, not by accident, and the
# panel says so itself rather than leaving the reader to find it. An audience
# member who works out that the Databricks side carries a full day of pipeline
# for a round that runs for minutes reaches the opposite conclusion to the one
# this demo argues, and reaches it correctly from what the panel shows them.
# Naming the trade is a stronger position than hoping nobody does the arithmetic.
#
# Every slot is filled from the same components the totals are summed over --
# there is no rate literal here any more than anywhere else in this file. The
# per-bout figure a reader would want next is deliberately *not* offered: nothing
# in this payload measures how long a bout runs, and an assumed length multiplied
# by a real rate produces a figure that looks measured and is not.
#
# TWO CLAIMS WERE REMOVED FROM THIS PARAGRAPH BECAUSE A MEASUREMENT CONTRADICTED
# THEM. It used to justify running around the clock on the grounds that "starting
# the pipeline at the bell would move its startup inside the bout clock and
# change what the round measures". Neither half survived contact with the
# account. The pipeline is started at *arm*, not at the bell, and `armed_at` is
# captured after `arm()` returns, so no part of a start is inside the bout clock.
# And what the round measures is `sync_end - committed_at` on the commit the bell
# itself makes, with `_prove_update` refusing unless the status commit is that
# exact commit -- nothing reads a backfill watermark or any window before the
# bell -- so a pipeline started ninety seconds earlier measures the same quantity
# a resident one measures. Measured 2026-08-24: a stopped pipeline resumed to a
# fully healthy continuous sync in 19.2 s, at the same Delta version it stopped
# on rather than re-seeding.
#
# The accrued figure is here for a reason worth stating. `pipeline_power.
# session_notice` was the accepted mitigation for this cost and it reasons from
# "every session passes through `antidemo serve`" -- true of a checkout, false of
# the deployed app, which never executes `cli.py`. So the one bound ever placed
# on the largest standing line in this installation has never printed on the
# surface where that line is actually billing. This panel is rendered by the
# deployed app, and `accrued` is the pipeline's own `window_usd` -- the same
# amount the platform lane subtotal and both totals are summed from -- so it
# states the bill rather than deriving a second one.
_CONTINUOUS_PARAGRAPH = (
    "{component} bills {per_day}/day while it is running, {share} of the "
    "Databricks side of the total above{largest}, and {accrued} has accrued "
    "against it over the {elapsed} this installation has existed. Continuous "
    "governed sync is what the round demonstrates, so the pipeline is continuous "
    "whenever it is up. It does not have to be up between bouts to be continuous "
    "during them: it is started when a round arms and stopped once that bout has "
    "settled, which takes the {per_day} down to roughly {per_hour} for each hour "
    "a bout is actually held. What the round measures is unaffected, because the "
    "figure it reports is taken from the commit the bell itself makes and not "
    "from any window before it. No per-bout figure is claimed here: nothing in "
    "this disclosure measures how long a bout runs, and an assumed length "
    "multiplied by that rate would read as a measurement without being one."
)

_CONTINUOUS_LARGEST = ", and the largest single line on that side"

# The compute that would bill without this run, stated as its own line rather
# than folded into the lane it arrived in. Every slot comes off the same
# components both totals are summed over, so there is no second derivation here.
#
# It exists because the lane subtotal used to include it while the headline
# total did not, which left the six rendered lane figures adding to the larger
# total beside the smaller one the panel leads with -- inflating the Databricks
# side by an entire pre-existing workspace app, in the one form of error that
# needs no code reading to find.
_PREDATING_PARAGRAPH = (
    "{components} bills whether or not this run exists, at {per_day}. It predates "
    "this installation, so it is stated here on its own rather than folded into a "
    "lane above: each lane's figure is that lane's share of the first total, and "
    "adding them gives that total and not the larger one beside it{difference}."
)

_PREDATING_IS_THE_DIFFERENCE = ". This amount is the whole of the gap between the two"

# Also reused rather than restated: the two-question framing this panel opens with.
_SUMMARY = (
    "Two questions, both answered. Marginal asks what this bout added. Standing "
    "asks what is being spent anyway."
)

_AWS_HAS_NO_POSTED_COUNTERPART = (
    "AWS has no posted counterpart at all: ce:GetCostAndUsage is denied to this "
    "installation and is not being pursued. Any figure presented as a posted all-in "
    "would be a Databricks-only actual wearing an all-in label, so the posted "
    "comparison here is Databricks-only and says so."
)

_DRIFT_IS_NEVER_SUMMED = (
    "Unexpected accrual is a separate figure and is never added to the totals above. "
    "Sealed accrual answers what this installation is meant to cost; unexpected "
    "accrual answers what it is costing that nobody asked for. Their sum answers "
    "neither."
)

_UNPOSTED_BASIS = (
    "The disclosure window runs from created_at to as_of. Any part of it the posted "
    "read does not cover is carried here rather than assumed to match."
)


@dataclass(frozen=True, slots=True)
class PlatformComponent:
    """One Databricks platform meter, observed from posted usage.

    The price is the caller's, read from ``system.billing.list_prices`` alongside
    the usage rows, because the app's compute meters at a different rate from a
    Lakebase DBU and this module is not allowed to know either number.

    ``predates_installation`` is the field that decides which total a component
    lands in. The app was live before this ``run_id`` existed and would bill
    without it, which is why the second total may not be quoted without its
    condition.

    ``dbu_per_hour`` is always a rate *while the meter is up*, which for an
    intermittent component is not the same as its amount over the disclosure
    window. ``uptime_hours`` is what reconciles the two.
    """

    label: str
    dbu_per_hour: Decimal
    usd_per_dbu: Decimal
    attribution: str
    grade: Literal["measured", "projected"] = "projected"
    predates_installation: bool = False
    #: How many hours this meter was actually up inside the span it was sampled
    #: over, when the two differ. ``None`` says they do not -- a component that
    #: runs continuously -- and the disclosure window is then the right
    #: denominator for both its rate and its amount, which is what it has always
    #: been. A value here is what stops the rate being divided by hours the
    #: resource was stopped for, and it is also the multiplier that turns the
    #: rate back into the amount actually posted.
    uptime_hours: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.label.strip():
            raise ValueError("a platform component must name the meter it describes")
        if not self.attribution.strip():
            raise ValueError("a platform component must say how it was attributed")
        if self.dbu_per_hour < 0 or self.usd_per_dbu < 0:
            raise ValueError("a platform component cannot meter negatively")
        if self.uptime_hours is not None and self.uptime_hours <= 0:
            raise ValueError("a platform component cannot be up for no time at all")

    @property
    def usd_per_hour(self) -> Decimal:
        return self.dbu_per_hour * self.usd_per_dbu


@dataclass(frozen=True, slots=True)
class PostedDatabricksUsage:
    """Everything this disclosure knows about Databricks, from one Delta query.

    ``unavailable`` suppresses the posted figure and the variance and nothing
    else. A projection built from rates read earlier is still a projection, and
    losing the ability to query billing does not make the account stop spending.
    """

    window_start: datetime | None = None
    window_end: datetime | None = None
    posted_usd: Decimal | None = None
    lakebase_dbu_per_hour: Decimal | None = None
    lakebase_dbu_basis: str = ""
    lakebase_dsu_per_hour: Decimal | None = None
    lakebase_dsu_basis: str = ""
    platform: tuple[PlatformComponent, ...] = ()
    unavailable: str = ""
    source: str = "system.billing.usage"


@dataclass(frozen=True, slots=True)
class _Allocation:
    """How much of one priced line belongs to one lane."""

    lane: StandingCostLaneId
    units: int
    total_units: int
    share_basis: str = ""


@dataclass(frozen=True, slots=True)
class _Priced:
    """One rendered component and the exact amount behind it."""

    lane: StandingCostLaneId
    component: StandingCostComponent
    window_usd: Decimal | None
    cloud: CloudName
    predates: bool
    #: The hours ``window_usd`` is divided by to state this component's rate.
    #: ``None`` means the disclosure window, which is every component that is up
    #: for the whole of it. Carried so a reader of the rate downstream cannot
    #: re-divide by the wrong denominator and produce a second, quieter figure.
    rate_hours: Decimal | None = None


@dataclass(frozen=True, slots=True)
class _LaneCopy:
    product: str
    side: Literal["lakebase", "competitor", "shared", "platform"]
    idle_label: str
    caveat: str


def _money(value: Decimal, places: int) -> str:
    """Render an amount at enough precision that it cannot print as zero.

    Lakebase storage is a third of a cent a day. At two decimal places it renders
    ``$0.00``, which is the one thing a lane figure may never say, so the
    precision follows the number rather than the column.
    """

    for width in (places, places + 3, places + 6):
        rendered = f"${value:.{width}f}"
        if any(digit in rendered for digit in "123456789"):
            return rendered
    return f"${value:.{places}f}"


def _per_day(value: Decimal) -> str:
    return f"{_money(value, 4)}/day"


def _per_hour(value: Decimal) -> str:
    return f"{_money(value, 6)}/hour"


def _share(fraction: Decimal) -> str:
    """A share rendered so it cannot claim the whole unless it is the whole.

    Same rule as :func:`_money` and for the same reason: the precision follows
    the number rather than the column. No share is written down here -- this file
    has already had a docstring quote the platform lane as "about two thirds" and
    then describe neither total -- but the shape that makes this necessary is
    worth stating. Every Lakebase endpoint scales to zero and posts a
    structural-zero always-on minimum, which can leave Lakebase storage, a third
    of a cent a day, as the only other line on the Databricks side of
    ``totals.installation``. The continuous pipeline is then a whisker under the
    whole of that half, and at no decimal places that printed ``100%``: a claim
    that the pipeline is the entirety of a half whose Lakebase lane the panel
    renders as non-zero, and whose caveat says in as many words that nothing here
    claims Lakebase idles free. Two visible statements contradicting each other is
    the defect; the rounding is only how it got there.

    Widening rather than flooring, because flooring biases the figure downwards
    by up to a whole point, and understating our own standing cost is the one
    direction an error here would flatter us in.

    The fallback is reachable only for a share within 5e-7 of the whole, where the
    whole is what the arithmetic says at every precision the panel renders.
    """

    for places in range(7):
        rendered = f"{fraction * 100:.{places}f}"
        if fraction >= 1 or Decimal(rendered) < 100:
            return f"{rendered}%"
    return f"{fraction * 100:.6f}%"


def _quantity(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.000001')).normalize():f}"


def _aware(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _elapsed_display(hours: Decimal) -> str:
    if hours < HOURS_PER_DAY * 2:
        return f"{hours:.2f} h"
    return f"{hours / HOURS_PER_DAY:.2f} days"


def _sum(values: Iterable[Decimal]) -> Decimal:
    return sum(values, Decimal(0))


def _unavailable_figure(derivation: str) -> StandingCostFigure:
    return StandingCostFigure(state="unavailable", display="Unavailable", derivation=derivation)


def _figure(
    window_usd: Decimal | None,
    hours: Decimal,
    *,
    derivation: str,
    rate_source: str = "",
    zero_basis: str = "",
) -> StandingCostFigure:
    """One figure from one window amount, with the zero rule enforced here."""

    if window_usd is None:
        return StandingCostFigure(
            state="unavailable",
            display="Unavailable",
            derivation=derivation,
            rate_source=rate_source,
        )
    per_hour = window_usd / hours
    per_day = per_hour * HOURS_PER_DAY
    if window_usd == 0:
        basis = zero_basis.strip() or derivation
        return StandingCostFigure(
            state="structural_zero",
            usd_per_hour=0.0,
            usd_per_day=0.0,
            display=f"$0.00/day · {basis}",
            derivation=derivation,
            zero_basis=basis,
            rate_source=rate_source,
        )
    return StandingCostFigure(
        state="priced",
        usd_per_hour=float(per_hour),
        usd_per_day=float(per_day),
        display=f"{_per_day(per_day)} · {_per_hour(per_hour)}",
        derivation=derivation,
        rate_source=rate_source,
    )


def _rds_class_basis(configured: str, observed: str | None) -> str:
    """Whether the priced class is the one AWS is actually running.

    WHY THIS IS NOT COMPARED AGAINST A CONSTANT. It used to be, against
    ``AS_RUN_RDS_INSTANCE_CLASS``, and both sides of that comparison were
    hardcoded -- so the lane asserted "all four instances are running
    db.t4g.micro" as a fact about AWS that nothing in the process had looked up.
    The resize landed in the account on 2026-08-21 and the sentence went false
    the moment it did, with no mechanism by which it could ever have noticed.
    A claim about live state has to come from a live reading or not be made.

    So the only input that can retire the clause is an observed class, and the
    absent case is labelled rather than assumed. ``None`` means no control plane
    reported one on this run -- true at session-create, and true for every round
    that does not race the RDS lane -- and it yields the configured-only label,
    not silence and not a match. Silence would let the reader take the priced
    class for the billed one, which is the same error in a quieter voice.
    """

    if observed is None:
        return (
            f" This lane prices {configured}, the configured class. No instance class "
            "was read back from AWS on this run, so this is the configured target and "
            "no claim is made about what the running fleet is."
        )
    if observed == configured:
        return ""
    return (
        f" This lane prices {configured}, the configured class, but the live control "
        f"plane reports {observed} running, so the figure is the configured target and "
        "not what AWS is billing today."
    )


def _lane_copy(
    rates: RateCard,
    *,
    observed_rds_instance_class: str | None = None,
) -> dict[StandingCostLaneId, _LaneCopy]:
    """Lane labels, with every number in them read off the same inputs."""

    rds_basis = _rds_class_basis(rates.rds_instance_class, observed_rds_instance_class)
    return {
        StandingCostLaneId.RDS: _LaneCopy(
            product=f"RDS PostgreSQL {rates.rds_instance_class}",
            side="competitor",
            idle_label="Never sleeps · no idle floor to descend into",
            caveat=(
                # Every instance that stands is now also raced, which was not true
                # while Round 1 had one. The rounds that no longer carry a box are
                # not free for a customer -- RDS cannot scale to zero, so
                # `imputed_round_carrying_lines` prices them as if running -- and
                # this figure covers only what *this* installation is billed, so
                # the caveat has to say which is which or the total reads as the
                # whole of the alternative.
                f"An instance stands for {_round_list(_STANDING_RDS_ROUND_NUMBERS)}, and "
                "each of those rounds races it. This figure covers those instances only. "
                f"{_round_list(_IMPUTED_RDS_ROUND_NUMBERS)} have no instance here -- "
                "Round 1's was deleted because RDS has no idle state to wake from, so it "
                "billed without ever being timed -- so this is what we pay, not what the "
                "workload costs: a customer who needs those rounds pays for "
                f"{number_word(len(_IMPUTED_RDS_ROUND_NUMBERS))} more boxes, because RDS "
                "cannot scale to zero. Nothing in this app stops or starts the instances "
                f"that do stand.{rds_basis}"
            ),
        ),
        StandingCostLaneId.AURORA: _LaneCopy(
            product="Aurora Serverless v2",
            side="competitor",
            idle_label=(
                f"Sleeps · {AURORA_AUTO_PAUSE_SECONDS}s auto-pause, and parks at the "
                "sealed minimum capacity"
            ),
            caveat=(
                "Compute parks at the sealed minimum, and that zero is a configuration "
                "rather than a missing measurement. Storage, the writer's address and "
                "its managed secret bill regardless of whether the cluster is awake."
            ),
        ),
        StandingCostLaneId.LAKEBASE: _LaneCopy(
            product="Lakebase",
            side="lakebase",
            idle_label=f"Sleeps · {LAKEBASE_SUSPEND_SECONDS}s suspend floor on every endpoint",
            caveat=(
                "Lakebase idles at a floor and is billed for it; nothing here claims it "
                "idles free. No endpoint in this installation is configured "
                "no_suspension, so all six have a scale-down path and the standing "
                "compute is whatever the posted always-on minimum comes to."
            ),
        ),
        StandingCostLaneId.RDS_PROXY: _LaneCopy(
            product="RDS Proxy credentials",
            side="competitor",
            idle_label="Nothing to sleep · a secret bills whether or not a proxy stands",
            caveat=(
                "describe-db-proxies returns nothing, and this lane is still not zero: "
                "the Terraform-managed proxy secrets stand whether or not a proxy does. "
                "That is why the lane needs no special case to avoid reading as free."
            ),
        ),
        StandingCostLaneId.NEUTRAL_RUNNER: _LaneCopy(
            product="Neutral m6i.large runner",
            side="shared",
            idle_label="Never sleeps · the box bills whether or not a bout runs",
            caveat=(
                "The runner drives both lanes and belongs to neither corner, so it is "
                "neither side's cost and is still the installation's. Leaving it out "
                "would leave a total that does not reconcile."
            ),
        ),
        StandingCostLaneId.DATABRICKS_PLATFORM: _LaneCopy(
            product="Databricks platform",
            side="platform",
            idle_label="Never sleeps · the pipeline holds state and the app is always on",
            caveat=(
                "This is not the Lakebase lane. The synced-table pipeline and the app's "
                "own compute are the largest standing lines in the installation, and "
                "folding them into Lakebase would overstate Lakebase roughly ninefold. "
                "An error that points against us is still an error."
            ),
        ),
    }


def _allocations(line: CostLine, shape: InstallationShape) -> tuple[_Allocation, ...]:
    """Which lane or lanes one carrying line belongs to.

    Two of the lines are genuinely shared. One address per publicly reachable
    database endpoint covers both engines, and the managed-credential line covers
    both engines plus the proxy secrets. They are split by the counts that
    produced them rather than assigned wholesale, because the proxy lane's whole
    honest content is the secrets that outlive every proxy.

    An unrecognised line returns no allocation, which makes the totals partial and
    names the line rather than quietly dropping it -- a line that was never
    emitted is how Round 5's Aurora compute read as free for as long as it did.
    """

    lowered = line.component.lower()
    if line.cloud is Cloud.DATABRICKS:
        if "lakebase" in lowered:
            return (_Allocation(StandingCostLaneId.LAKEBASE, 1, 1),)
        return ()
    if line.lane_id == "shared":
        return (_Allocation(StandingCostLaneId.NEUTRAL_RUNNER, 1, 1),)
    if "public ipv4 addresses" in lowered:
        return _address_allocations(shape)
    if "credentials" in lowered:
        return _secret_allocations(shape)
    if "aurora" in lowered:
        return (_Allocation(StandingCostLaneId.AURORA, 1, 1),)
    if "rds" in lowered:
        return (_Allocation(StandingCostLaneId.RDS, 1, 1),)
    return ()


def _address_allocations(shape: InstallationShape) -> tuple[_Allocation, ...]:
    total = shape.public_ipv4_addresses
    rds = min(shape.rds_instances, total)
    parts = (
        (StandingCostLaneId.RDS, rds, "one address per publicly reachable RDS instance"),
        (
            StandingCostLaneId.AURORA,
            total - rds,
            "one address per publicly reachable Aurora writer",
        ),
    )
    return tuple(
        _Allocation(lane, units, total, f"{units} of {total} chargeable addresses · {basis}")
        for lane, units, basis in parts
        if units > 0
    )


def _secret_allocations(shape: InstallationShape) -> tuple[_Allocation, ...]:
    total = shape.managed_secrets
    rds = min(shape.rds_instances, total)
    aurora = min(shape.aurora_clusters, total - rds)
    parts = (
        (StandingCostLaneId.RDS, rds, "one RDS-managed master credential per instance"),
        (StandingCostLaneId.AURORA, aurora, "one RDS-managed master credential per cluster"),
        (
            StandingCostLaneId.RDS_PROXY,
            total - rds - aurora,
            "Terraform-managed proxy secrets, which stand whether or not a proxy does",
        ),
    )
    return tuple(
        _Allocation(lane, units, total, f"{units} of {total} managed secrets · {basis}")
        for lane, units, basis in parts
        if units > 0
    )


def _split_amounts(total: Decimal, allocations: Sequence[_Allocation]) -> list[Decimal]:
    """Divide one amount across allocations so the parts sum to the whole exactly.

    The last share takes the remainder rather than its own quotient, so a split
    line can never add up to a fraction more or less than the line it came from.
    """

    if not allocations:
        return []
    amounts = [
        total * Decimal(allocation.units) / Decimal(allocation.total_units)
        for allocation in allocations[:-1]
    ]
    amounts.append(total - _sum(amounts))
    return amounts


def _priced_line(
    line: CostLine,
    allocation: _Allocation,
    amount: Decimal | None,
    hours: Decimal,
    *,
    observation_basis: str = "",
) -> _Priced:
    """Render one allocated share of one carrying line.

    ``observation_basis`` is the caller's account of where an injected quantity
    came from. The estimator names the meter generically; only the caller knows
    which posted rows it read, and that belongs on the component rather than in a
    log line nobody will have.
    """

    quantity = line.quantity
    shared = allocation.total_units > 1
    label = f"{line.component} · {allocation.share_basis}" if shared else line.component
    basis = f"{quantity.basis} · {observation_basis}" if observation_basis else quantity.basis
    if quantity.point is None:
        derivation = f"quantity unavailable · {basis}"
    else:
        units = quantity.point * Decimal(allocation.units) / Decimal(allocation.total_units)
        derivation = (
            f"{_quantity(units)} {line.rate.unit} × {_money(line.rate.usd, 4)}/"
            f"{line.rate.unit} over {hours:.4f} h · {basis}"
        )
    cloud: CloudName = "aws" if line.cloud is Cloud.AWS else "databricks"
    return _Priced(
        lane=allocation.lane,
        component=StandingCostComponent(
            component=label,
            cloud=cloud,
            kind=_KIND_NAMES[line.kind],
            provenance=_PROVENANCE_NAMES[quantity.provenance],
            quantity_basis=basis,
            figure=_figure(
                amount,
                hours,
                derivation=derivation,
                rate_source=line.rate.source,
                zero_basis=quantity.basis,
            ),
        ),
        window_usd=amount,
        cloud=cloud,
        predates=False,
    )


def _observation_basis(line: CostLine, posted: PostedDatabricksUsage | None) -> str:
    """Which posted rows the caller says produced an injected Lakebase quantity.

    Only the two Lakebase lines take an injected quantity; every AWS line is
    priced from the sealed shape and has nothing a caller could have observed.
    """

    if posted is None or line.cloud is not Cloud.DATABRICKS:
        return ""
    if line.kind is CostKind.COMPUTE:
        return posted.lakebase_dbu_basis.strip()
    if line.kind is CostKind.STORAGE:
        return posted.lakebase_dsu_basis.strip()
    return ""


def _priced_platform(observed: PlatformComponent, hours: Decimal) -> _Priced:
    """Price one platform meter over the hours it was actually up.

    For everything that runs continuously those are the disclosure window's
    hours, and this is the arithmetic it has always been. For an intermittent
    component they are its own uptime, and using it here is what keeps two
    different quantities from collapsing into one wrong one: the rate is the
    amount over the hours it was *up*, and the amount is that rate over those
    same hours -- which is the amount that was actually posted, rather than the
    while-running rate extrapolated across a window the resource was stopped for
    most of.
    """

    billable_hours = observed.uptime_hours if observed.uptime_hours is not None else hours
    window_usd = observed.usd_per_hour * billable_hours
    derivation = (
        f"{_quantity(observed.dbu_per_hour)} DBU/hour × "
        f"{_money(observed.usd_per_dbu, 4)}/DBU over {billable_hours:.4f} h · "
        f"{observed.attribution}"
    )
    return _Priced(
        lane=StandingCostLaneId.DATABRICKS_PLATFORM,
        component=StandingCostComponent(
            component=observed.label,
            cloud="databricks",
            kind="compute",
            provenance="measured" if observed.grade == "measured" else "modeled",
            quantity_basis=observed.attribution,
            predates_installation=observed.predates_installation,
            figure=_figure(
                window_usd,
                billable_hours,
                derivation=derivation,
                rate_source="system.billing.usage × system.billing.list_prices",
                zero_basis=observed.attribution,
            ),
        ),
        window_usd=window_usd,
        cloud="databricks",
        predates=observed.predates_installation,
        rate_hours=billable_hours,
    )


def _unpriced(
    lane: StandingCostLaneId,
    *,
    component: str,
    cloud: CloudName,
    detail: str,
) -> _Priced:
    return _Priced(
        lane=lane,
        component=StandingCostComponent(
            component=component,
            cloud=cloud,
            kind="other",
            provenance="unavailable",
            quantity_basis=detail,
            figure=_unavailable_figure(detail),
        ),
        window_usd=None,
        cloud=cloud,
        predates=False,
    )


def _lane_subtotal(items: Sequence[_Priced]) -> tuple[list[_Priced], list[_Priced]]:
    """Which of a lane's components its rendered subtotal is summed from.

    The subtotal is the lane's share of ``totals.installation`` -- the total the
    panel headlines -- so a component that predates the installation is held out
    of it and disclosed separately by :func:`_predating`. Summing both put a
    pre-existing workspace app inside a figure rendered beside a headline that
    excludes it: the six lane figures added to ``totals.with_platform`` while the
    panel led with the smaller number, and the lane's
    ``counted_in_installation_total`` read ``True`` next to the inflated figure.

    A lane whose every component predates the installation has no share of that
    total to render, so it keeps its full figure and says so through
    ``counted_in_installation_total``. That is the one lane whose figure is not
    addable to the headline, and the flag beside it no longer claims otherwise.
    """

    counted = [item for item in items if not item.predates]
    if not counted:
        return list(items), []
    return counted, [item for item in items if item.predates]


def _lane_figure(items: Sequence[_Priced], hours: Decimal) -> StandingCostFigure:
    """A lane subtotal, or an admission that one of its components is missing.

    A lane with a missing component has no honest subtotal: adding the ones that
    priced would report less than the lane costs while looking like a total. The
    check is over every component the lane holds, predating ones included -- a
    lane that cannot price all of what it carries leaves both totals, and it is
    ``totals.with_platform`` that the predating component belongs to.
    """

    if not items:
        # Unreachable while every lane is guaranteed a component, and stated anyway:
        # summing an empty lane would produce exactly the bare zero this model
        # exists to prevent.
        return _unavailable_figure("no component was grouped into this lane")
    missing = [item.component.component for item in items if item.window_usd is None]
    if missing:
        return _unavailable_figure(
            "not priced: "
            + "; ".join(missing)
            + " could not be established, and a subtotal without it would read as a "
            "total"
        )
    counted, excluded = _lane_subtotal(items)
    zero_basis = "; ".join(
        item.component.figure.zero_basis for item in counted if item.component.figure.zero_basis
    )
    parts = ", ".join(item.component.component for item in counted)
    derivation = f"sum of {len(counted)} sealed component(s): {parts}"
    if excluded:
        held_out = ", ".join(item.component.component for item in excluded)
        derivation = (
            f"{derivation} · excludes {held_out}, which predates this installation: "
            "it is disclosed on its own rather than added here, so this figure and "
            "the other lanes' add to the total above them"
        )
    return _figure(
        _sum(item.window_usd or Decimal(0) for item in counted),
        hours,
        derivation=derivation,
        zero_basis=zero_basis,
    )


_DATABRICKS_LANES = frozenset(
    {StandingCostLaneId.LAKEBASE, StandingCostLaneId.DATABRICKS_PLATFORM}
)
_AWS_LANES = frozenset(
    {
        StandingCostLaneId.RDS,
        StandingCostLaneId.AURORA,
        StandingCostLaneId.RDS_PROXY,
        StandingCostLaneId.NEUTRAL_RUNNER,
    }
)


def _lane_evidence(
    lane_id: StandingCostLaneId,
    figure: StandingCostFigure,
    *,
    platform: Sequence[PlatformComponent],
    observed: bool,
) -> LaneEvidence:
    """What kind of number this lane is, which is not the same as how big it is."""

    if figure.state == "unavailable":
        return "unpriced"
    if lane_id is StandingCostLaneId.DATABRICKS_PLATFORM:
        if platform and all(item.grade == "measured" for item in platform):
            return "posted_actual"
        return "posted_projection"
    if lane_id is StandingCostLaneId.LAKEBASE:
        return "posted_projection"
    return "rate_card_derived" if observed else "sealed_shape_only"


def _lane_rate_source(items: Sequence[_Priced]) -> str:
    sources: list[str] = []
    for item in items:
        source = item.component.figure.rate_source
        if source and source not in sources:
            sources.append(source)
    return " · ".join(sources)


def _lanes(
    items: Sequence[_Priced],
    hours: Decimal,
    *,
    rates: RateCard,
    platform: Sequence[PlatformComponent],
    observed: bool,
    observed_rds_instance_class: str | None = None,
) -> list[StandingCostLane]:
    copy = _lane_copy(rates, observed_rds_instance_class=observed_rds_instance_class)
    lanes: list[StandingCostLane] = []
    for lane_id in StandingCostLaneId:
        owned = [item for item in items if item.lane is lane_id]
        figure = _lane_figure(owned, hours)
        lanes.append(
            StandingCostLane(
                lane_id=lane_id,
                product=copy[lane_id].product,
                side=copy[lane_id].side,
                idle_label=copy[lane_id].idle_label,
                figure=figure,
                components=[item.component for item in owned],
                evidence=_lane_evidence(
                    lane_id, figure, platform=platform, observed=observed
                ),
                rate_source=_lane_rate_source(owned),
                caveat=copy[lane_id].caveat,
                # True exactly when ``figure`` is the lane's share of
                # ``totals.installation``, which is what ``_lane_subtotal`` makes it
                # whenever the lane has anything this run created. The flag used to
                # read True beside a figure that had a pre-existing app inside it,
                # which is the contradiction that shape removes rather than annotates.
                counted_in_installation_total=(
                    figure.state != "unavailable" and any(not item.predates for item in owned)
                ),
                counted_in_platform_total=figure.state != "unavailable",
            )
        )
    return lanes


def _counted(items: Sequence[_Priced], lanes: Sequence[StandingCostLane]) -> list[_Priced]:
    """Components in lanes that priced. An unpriced lane leaves both totals."""

    priced = {
        lane.lane_id for lane in lanes if lane.figure.state != "unavailable"
    }
    return [item for item in items if item.lane in priced and item.window_usd is not None]


def _totals(
    items: Sequence[_Priced],
    lanes: Sequence[StandingCostLane],
    hours: Decimal,
    *,
    partial_reasons: Sequence[str],
) -> StandingCostTotals:
    """The two totals, each carrying the condition that makes it meaningful.

    They are summed over components rather than lanes because the platform lane
    holds one of each: a pipeline this run created, and an app that predates it.
    """

    counted = _counted(items, lanes)
    with_platform = _sum(item.window_usd or Decimal(0) for item in counted)
    installation = _sum(
        item.window_usd or Decimal(0) for item in counted if not item.predates
    )
    predating = [item.component.component for item in counted if item.predates]
    reasons = list(partial_reasons)
    unpriced = [lane.product for lane in lanes if lane.figure.state == "unavailable"]
    if unpriced:
        reasons.append("excluded from the total: " + ", ".join(unpriced) + " could not be priced")
    partial = bool(reasons)
    reason = " · ".join(reasons)
    lane_ids = [
        lane.lane_id for lane in lanes if lane.figure.state != "unavailable"
    ] or [lane.lane_id for lane in lanes]
    prefix = "Partial standing cost" if partial else "Standing cost"
    if predating:
        names = ", ".join(predating)
        installation_condition = (
            f"Covers what this run_id created. Excludes {names}, which bills whether or "
            "not this run exists."
        )
        with_platform_condition = (
            f"Adds {names}, which would bill anyway: it predates this installation. Do "
            "not quote this figure without that condition."
        )
    else:
        installation_condition = (
            "Covers what this run_id created, which on this reading is everything "
            "priced here: no component in the payload predates the installation."
        )
        with_platform_condition = (
            "Equal to the figure above on this reading, because no priced component "
            "predates this installation. Still shown separately, because the app's "
            "compute normally does and a single total that quietly changes meaning is "
            "worse than two that do not."
        )

    def total(amount: Decimal, *, label: str, condition: str) -> StandingCostTotal:
        per_hour = amount / hours
        per_day = per_hour * HOURS_PER_DAY
        return StandingCostTotal(
            label=label,
            usd_per_hour=float(per_hour),
            usd_per_day=float(per_day),
            display=f"{_per_day(per_day)} · {_per_hour(per_hour)}",
            condition=condition,
            lane_ids=lane_ids,
            partial=partial,
            partial_reason=reason,
        )

    return StandingCostTotals(
        installation=total(
            installation,
            label=f"{prefix} · created by this run",
            condition=installation_condition,
        ),
        with_platform=total(
            with_platform,
            label=f"{prefix} · with platform compute",
            condition=with_platform_condition,
        ),
    )


def _credits(
    totals: StandingCostTotals,
    *,
    origin: datetime,
    now: datetime,
    hours: Decimal,
    installation_accrued: Decimal,
    with_platform_accrued: Decimal,
) -> StandingCostCredits:
    """One snapshot of what has accrued, computed here and never advanced there."""

    return StandingCostCredits(
        as_of=now,
        origin=origin,
        elapsed_hours=float(hours),
        elapsed_display=_elapsed_display(hours),
        installation_accrued_usd=float(installation_accrued),
        with_platform_accrued_usd=float(with_platform_accrued),
        display=(
            f"{_money(installation_accrued, 2)} accrued since created_at, "
            f"{_money(with_platform_accrued, 2)} including compute that predates it, "
            f"over {_elapsed_display(hours)}"
        ),
        basis=(
            "One snapshot at as_of, from the sealed shape and the posted rates. Not a "
            "counter: nothing here advances between requests, and the figure is only "
            f"ever as recent as the as_of beside it. {totals.installation.condition}"
        ),
    )


def _posted_unavailable(
    *,
    reason: str,
    source: str,
    window_start: datetime | None,
    window_end: datetime | None,
    projection: Decimal | None,
    unposted_hours: Decimal | None,
    comparison_basis: str,
    explanation: str,
) -> StandingCostPosted:
    return StandingCostPosted(
        state="unavailable",
        source=source,
        window_start=window_start,
        window_end=window_end,
        projection_usd=float(projection) if projection is not None else None,
        unposted_hours=float(unposted_hours) if unposted_hours is not None else None,
        unposted_basis=_UNPOSTED_BASIS,
        comparison_basis=comparison_basis,
        explanation=explanation,
        aws_posted_basis=_AWS_HAS_NO_POSTED_COUNTERPART,
        unavailable_reason=reason,
        display=f"Posted actuals unavailable · {reason}",
    )


def _posted(
    posted: PostedDatabricksUsage | None,
    *,
    origin: datetime,
    now: datetime,
    hours: Decimal,
    databricks_usd_per_hour: Decimal | None,
) -> StandingCostPosted:
    """The posted Databricks actual, its window, and nothing blended together.

    The variance is measured against the projection restricted to the posted
    window and against nothing else. Comparing a whole-window projection with a
    partial posted day and calling the difference an error is arithmetic across
    two windows, which this project has already had to correct once.
    """

    source = posted.source if posted is not None else "system.billing.usage"
    window_start = _aware(posted.window_start) if posted and posted.window_start else None
    window_end = _aware(posted.window_end) if posted and posted.window_end else None
    projection = databricks_usd_per_hour * hours if databricks_usd_per_hour is not None else None

    if posted is None:
        reason = "no posted usage was supplied with this disclosure"
    elif posted.unavailable.strip():
        reason = posted.unavailable.strip()
    elif posted.posted_usd is None:
        reason = "the posted read returned no dollar figure for this window"
    elif window_start is None or window_end is None:
        reason = "the posted read carried no window, so it cannot be compared to one"
    elif databricks_usd_per_hour is None:
        reason = "no Databricks projection could be priced, so there is nothing to compare"
    else:
        reason = ""

    if reason:
        return _posted_unavailable(
            reason=reason,
            source=source,
            window_start=window_start,
            window_end=window_end,
            projection=projection,
            unposted_hours=hours,
            comparison_basis=(
                "No variance is computed. A projection with no posted counterpart is "
                "still a projection and is left standing rather than adjusted."
            ),
            explanation=(
                "Posted actuals are unavailable, which suppresses the variance and "
                "nothing else. The projection is unaffected: losing the ability to read "
                "billing does not make the account stop spending."
            ),
        )

    # Narrowed by the guard above: a posted read, a window, and a priced projection.
    posted_usd = posted.posted_usd if posted is not None else None
    if posted_usd is None or window_start is None or window_end is None:
        raise AssertionError("posted comparison lost its inputs")
    rate = databricks_usd_per_hour or Decimal(0)

    overlap_start = max(window_start, origin)
    overlap_end = min(window_end, now)
    overlap = Decimal(str((overlap_end - overlap_start).total_seconds()))
    posted_hours = max(Decimal(0), overlap) / SECONDS_PER_HOUR
    if posted_hours <= 0:
        return _posted_unavailable(
            reason="the posted window does not overlap the disclosure window",
            source=source,
            window_start=window_start,
            window_end=window_end,
            projection=projection,
            unposted_hours=hours,
            comparison_basis=(
                "No variance is computed: the posted window and the disclosure window "
                "do not overlap, so there is no shared window to compare over."
            ),
            explanation=(
                "The posted read covers a window this installation did not exist in. "
                "Differencing the two would compare a bill against a projection of "
                "nothing."
            ),
        )

    restricted = rate * posted_hours
    variance = posted_usd - restricted
    fraction = variance / restricted if restricted != 0 else None
    unposted = hours - posted_hours
    percentage = f" ({fraction * 100:+.1f}%)" if fraction is not None else ""
    return StandingCostPosted(
        state="posted_through_window",
        source=source,
        window_start=window_start,
        window_end=window_end,
        posted_usd=float(posted_usd),
        projection_usd=float(projection) if projection is not None else None,
        projection_in_posted_window_usd=float(restricted),
        variance_usd=float(variance),
        variance_fraction=float(fraction) if fraction is not None else None,
        posted_hours=float(posted_hours),
        unposted_hours=float(unposted),
        unposted_basis=_UNPOSTED_BASIS,
        comparison_basis=(
            f"Variance is the posted figure minus the projection over the "
            f"{posted_hours:.4f} h the posted window and this disclosure window share, "
            "and is computed against nothing else. The whole-window projection of "
            f"{_money(projection or Decimal(0), 6)} is carried separately and is not "
            "what the posted figure was differenced against."
        ),
        explanation=(
            "The gap is not an error term. Compute that predates this installation "
            "bills for the whole posted day rather than only the hours this run has "
            "existed, while the restricted projection covers only those hours. Both "
            "figures are shown and neither is adjusted to meet the other."
        ),
        aws_posted_basis=_AWS_HAS_NO_POSTED_COUNTERPART,
        display=(
            f"Posted {_money(posted_usd, 6)} for {posted_hours:.2f} h of "
            f"{window_start:%Y-%m-%d} · projection over the same window "
            f"{_money(restricted, 6)} · variance {_money(variance, 6)}{percentage} · "
            f"{unposted:.2f} h of this disclosure window not yet posted"
        ),
    )


def _drift_finding(
    finding: Finding,
    observed: Mapping[str, ObservedResource],
    now: datetime,
    *,
    charging_for_absent: bool,
) -> StandingCostDriftFinding:
    """One finding, aged from its own clock or explicitly not aged at all."""

    rate = finding.usd_per_day or Decimal(0)
    resource = observed.get(finding.identifier)
    age_seconds = resource.age_seconds(now) if resource is not None else None
    shared = {
        "code": finding.code,
        "kind": finding.kind,
        "identifier": finding.identifier,
        "detail": finding.detail,
    }

    if finding.code == MISSING_RESIDENT:
        return StandingCostDriftFinding(
            **shared,
            usd_per_day=None,
            accrued_usd=None,
            accrual_basis=(
                "Not accrued. An absence does not age into a dollar figure, and this "
                "one runs the other way: the sealed lanes above are pricing a resource "
                "the account does not have."
            ),
            rate_basis=(
                "No rate. The account is not billing for something that is not there; "
                "the sealed shape is."
            ),
            charging_for_absent=True,
        )
    if rate <= 0:
        return StandingCostDriftFinding(
            **shared,
            usd_per_day=None,
            accrued_usd=None,
            accrual_basis="No rate to accrue from, so no accrued figure is claimed.",
            rate_basis=finding.basis or "no rate basis was recorded with this finding",
            charging_for_absent=charging_for_absent,
        )
    if age_seconds is None:
        return StandingCostDriftFinding(
            **shared,
            usd_per_day=float(rate),
            accrued_usd=None,
            accrual_basis=(
                "Rate only. No readable creation time came back for this resource, so "
                "its age is unknown -- and a reaper that cannot age a resource must not "
                "assume it is new."
            ),
            rate_basis=finding.basis,
            charging_for_absent=charging_for_absent,
        )

    age_hours = Decimal(str(age_seconds)) / SECONDS_PER_HOUR
    created = resource.created_at if resource is not None else None
    stamp = f"{_aware(created):%Y-%m-%dT%H:%M:%SZ}" if created is not None else "its own clock"
    return StandingCostDriftFinding(
        **shared,
        usd_per_day=float(rate),
        accrued_usd=float(rate / HOURS_PER_DAY * age_hours),
        accrual_basis=(
            f"Aged {age_hours:.4f} h from its own created_at ({stamp}), not from the "
            "installation origin. An orphan's clock starts when the orphan was made."
        ),
        rate_basis=finding.basis,
        charging_for_absent=charging_for_absent,
    )


def _drift(report: DriftReport | None, now: datetime) -> StandingCostDrift:
    """Unexpected accrual, priced separately and never summed into the headline."""

    if report is None:
        return StandingCostDrift(
            state="unavailable",
            badge="DRIFT NOT READ",
            summary=(
                "No reconciliation accompanied this disclosure, so the account was not "
                "compared against the seal. The standing figures above are unaffected."
            ),
            separation_note=_DRIFT_IS_NEVER_SUMMED,
            unavailable_reason="no reconciliation was supplied with this disclosure",
        )
    if report.unavailable:
        return StandingCostDrift(
            state="unavailable",
            badge="DRIFT UNKNOWN",
            summary=(
                "The account could not be inventoried, so this is the sealed shape only "
                "-- and the sealed shape is still counting."
            ),
            separation_note=_DRIFT_IS_NEVER_SUMMED,
            unavailable_reason=report.unavailable,
        )

    observed = {resource.identifier: resource for resource in report.observed}
    short_of_seal = report.observed_public_ipv4 < report.expected_public_ipv4
    findings = [
        _drift_finding(
            finding,
            observed,
            now,
            charging_for_absent=finding.code == IPV4_DRIFT and short_of_seal,
        )
        for finding in report.findings
    ]
    if not findings:
        return StandingCostDrift(
            state="sealed_shape_holds",
            badge="SEALED SHAPE HOLDS",
            summary=report.summary(),
            separation_note=_DRIFT_IS_NEVER_SUMMED,
        )

    unexpected_rate = _sum(
        Decimal(str(item.usd_per_day))
        for item in findings
        if item.usd_per_day is not None and not item.charging_for_absent
    )
    accrued = [
        Decimal(str(item.accrued_usd)) for item in findings if item.accrued_usd is not None
    ]
    label = f"{_per_day(unexpected_rate)} unexpected" if unexpected_rate > 0 else "RATE UNAVAILABLE"
    return StandingCostDrift(
        state="unexpected_accrual",
        badge=f"{len(findings)} DRIFT · {label}",
        summary=report.summary(),
        unexpected_usd_per_day=float(unexpected_rate) if unexpected_rate > 0 else None,
        unexpected_accrued_usd=float(_sum(accrued)) if accrued else None,
        findings=findings,
        separation_note=_DRIFT_IS_NEVER_SUMMED,
    )


def _fairness(
    *,
    databricks_per_day: Decimal | None,
    aws_per_day: Decimal | None,
) -> StandingCostFairness:
    """Fill the existing paragraph's figures in, or withhold it entirely."""

    if databricks_per_day is None or aws_per_day is None:
        return StandingCostFairness(
            state="withheld",
            withheld_reason=(
                "The paragraph states that our half is the larger one. One of the two "
                "halves is unpriced here, so the claim has nothing behind it and the "
                "paragraph is withheld rather than reworded."
            ),
        )
    if aws_per_day <= 0 or databricks_per_day <= aws_per_day:
        return StandingCostFairness(
            state="withheld",
            withheld_reason=(
                "The paragraph concedes that our half is the larger one. The derived "
                "figures no longer show that, so it is withheld rather than rewritten "
                "into a claim it was not making."
            ),
        )
    now_ratio = databricks_per_day / aws_per_day
    return StandingCostFairness(
        state="stated",
        paragraph=_FAIRNESS_PARAGRAPH.format(
            databricks=f"${databricks_per_day:.2f}",
            aws=f"${aws_per_day:.2f}",
            now=f"{now_ratio:.1f}",
            suspend=LAKEBASE_SUSPEND_SECONDS,
        ),
    )


def _continuous(
    counted: Sequence[_Priced],
    *,
    hours: Decimal,
    databricks_per_day: Decimal | None,
) -> StandingCostContinuous:
    """Price the continuously-scheduled pipeline against the half it sits in.

    Both figures are the pipeline's own ``window_usd`` -- the same amount the
    platform lane subtotal and both totals are summed from -- divided by the same
    window. There is no second derivation here to drift away from the first, and
    the share's denominator is the Databricks half of ``totals.installation``,
    which is the figure the fairness paragraph beside it quotes. Anything else
    would compare the line against a total the panel never stated: taken against
    the half that adds compute predating the installation, this same line reads
    roughly half of what it is, because the denominator grows by an app the
    headline total excludes.

    Withheld rather than reworded, on the same rule as the fairness paragraph.
    The claim is a share of a half, so a half that did not price leaves it with
    no denominator, and a payload with no pipeline in it leaves nothing to
    disclose -- neither is an invitation to state part of it.
    """

    pipeline = next(
        (
            item
            for item in counted
            if item.component.component == ROUND4_PIPELINE_LABEL
            and item.window_usd is not None
        ),
        None,
    )
    if pipeline is None or pipeline.window_usd is None:
        return StandingCostContinuous(
            state="withheld",
            withheld_reason=(
                "No continuously-scheduled pipeline was priced in this payload, so "
                "there is nothing to disclose. Nothing is claimed about what a "
                "pipeline this disclosure could not read might be costing."
            ),
        )
    if databricks_per_day is None or databricks_per_day <= 0:
        return StandingCostContinuous(
            state="withheld",
            withheld_reason=(
                "The paragraph states this line as a share of the Databricks side of "
                "the total. That half is unpriced here, so the share has no "
                "denominator and the paragraph is withheld rather than restated "
                "without it."
            ),
        )

    # Divided by the hours the pipeline was up, not by the disclosure window.
    # Those are the same number for every other component in the payload and are
    # not the same number for this one -- it is started at arm and released once
    # the bout settles -- so dividing by the window here blended the rate with
    # the hours it was stopped and rendered a figure the published rate could not
    # be reconciled to. `rate_hours` is the denominator its own figure above
    # already used, so the paragraph and the lane cannot state two rates.
    per_hour = pipeline.window_usd / (pipeline.rate_hours or hours)
    per_day = per_hour * HOURS_PER_DAY
    # "The largest single line" is a claim about the other components, so it is
    # read off them. The pipeline being the biggest is true today and is not a
    # property of the design; a rate move that made the app's compute larger
    # would retire the clause rather than falsify the sentence.
    others = [
        item.window_usd or Decimal(0)
        for item in counted
        if item.cloud == "databricks" and not item.predates and item is not pipeline
    ]
    largest = (
        _CONTINUOUS_LARGEST if all(pipeline.window_usd > other for other in others) else ""
    )
    share = per_day / databricks_per_day
    return StandingCostContinuous(
        state="stated",
        component=pipeline.component.component,
        usd_per_hour=float(per_hour),
        usd_per_day=float(per_day),
        share_of_databricks=float(share),
        paragraph=_CONTINUOUS_PARAGRAPH.format(
            component=pipeline.component.component,
            per_day=_money(per_day, 2),
            per_hour=_money(per_hour, 2),
            share=_share(share),
            largest=largest,
            # The pipeline's own window figure, not a fresh multiplication. It is
            # the exact amount the totals above are summed from, which is what
            # keeps the accrued sentence from disagreeing with the panel it sits
            # in.
            accrued=_money(pipeline.window_usd, 2),
            elapsed=f"{hours:.1f} h",
        ),
        derivation=(
            f"{pipeline.component.figure.derivation} · share taken against the "
            "Databricks side of totals.installation, the same half the fairness "
            "paragraph quotes"
        ),
    )


def _predating(counted: Sequence[_Priced], *, hours: Decimal) -> StandingCostPredating:
    """Disclose the compute the lane figures hold out, or say there is none.

    The amount is the same ``window_usd`` ``totals.with_platform`` adds and
    ``totals.installation`` does not, over the same window -- there is no second
    derivation here to drift from the first.

    Only the components actually held out of a lane figure are disclosed. A lane
    whose every component predates the installation renders its full figure and
    is marked as not counted instead, so listing its component here would claim
    an exclusion that did not happen; the difference clause is then withheld
    rather than overstated, because the gap between the two totals is no longer
    this figure alone.
    """

    predating = [item for item in counted if item.predates and item.window_usd is not None]
    lanes_with_counted = {item.lane for item in counted if not item.predates}
    excluded = [item for item in predating if item.lane in lanes_with_counted]
    window_usd = _sum(item.window_usd or Decimal(0) for item in excluded)
    if not excluded or window_usd <= 0:
        return StandingCostPredating(
            state="withheld",
            withheld_reason=(
                "No priced component in this payload predates the installation and is "
                "held out of the lane it arrived in, so there is nothing to disclose. "
                "The lane figures are the whole of the total above them."
            ),
        )
    per_hour = window_usd / hours
    per_day = per_hour * HOURS_PER_DAY
    difference = _PREDATING_IS_THE_DIFFERENCE if len(excluded) == len(predating) else ""
    return StandingCostPredating(
        state="stated",
        components=[item.component.component for item in excluded],
        usd_per_hour=float(per_hour),
        usd_per_day=float(per_day),
        paragraph=_PREDATING_PARAGRAPH.format(
            components=", ".join(item.component.component for item in excluded),
            per_day=_per_day(per_day),
            difference=difference,
        ),
        derivation=(
            " · ".join(item.component.figure.derivation for item in excluded)
            + " · held out of every lane figure and of totals.installation, and added "
            "by totals.with_platform alone"
        ),
    )


def _unreadable(
    *,
    run_id: str,
    now: datetime,
    detail: str,
    report: DriftReport | None,
) -> StandingCostDisclosure:
    """A disclosure with no dollar figure anywhere, because the seal is unreadable."""

    return StandingCostDisclosure(
        run_id=run_id,
        origin_basis=(
            "The window is measured from manifest.created_at, the moment the resources "
            "came into being. No origin could be read here, so nothing is priced."
        ),
        as_of=now,
        seal_state="unreadable",
        seal_detail=detail,
        shape_basis="sealed_shape_only",
        shape_detail=(
            "No sealed shape could be read, so no lane is priced and no total is "
            "claimed. The account is presumably still billing; this disclosure simply "
            "cannot say for what."
        ),
        posted=_posted_unavailable(
            reason="the seal could not be read, so there is no window to compare against",
            source="system.billing.usage",
            window_start=None,
            window_end=None,
            projection=None,
            unposted_hours=None,
            comparison_basis=(
                "No variance is computed, because no projection could be built to "
                "compare one against."
            ),
            explanation=(
                "An unreadable seal leaves nothing to compare. The account is still "
                "billing; this disclosure cannot say how much."
            ),
        ),
        drift=_drift(report, now),
        fairness=StandingCostFairness(
            state="withheld",
            withheld_reason=(
                "No figure could be derived, and the paragraph exists to concede a "
                "comparison between two figures."
            ),
        ),
        continuous=StandingCostContinuous(
            state="withheld",
            withheld_reason=(
                "No lane priced, so the pipeline's standing figure and the half it "
                "would be a share of are both absent."
            ),
        ),
        predating=StandingCostPredating(
            state="withheld",
            withheld_reason=(
                "No lane priced, so there is no lane figure for compute that predates "
                "the installation to have been held out of."
            ),
        ),
        summary=_SUMMARY,
        note=(
            "The seal could not be read, so this disclosure carries no dollar figure at "
            "all rather than a zero. An unreadable manifest is not a free installation."
        ),
    )


def build_standing_cost_disclosure(
    manifest: object | None,
    *,
    now: datetime,
    rates: RateCard | None = None,
    shape: InstallationShape | None = None,
    report: DriftReport | None = None,
    posted: PostedDatabricksUsage | None = None,
    observed_rds_instance_class: str | None = None,
) -> StandingCostDisclosure:
    """Assemble the standing-cost disclosure for one installation at one instant.

    ``now`` is injected and is the only clock this function has. Given the same
    inputs it returns the same payload, which is what makes it a snapshot rather
    than a ticker: there is no counter here for a renderer to advance.

    ``observed_rds_instance_class`` is the class a live control plane reported,
    or ``None`` when nothing read one back on this run. It is passed in rather
    than fetched because this builder deliberately does no I/O -- it is rebuilt
    on every session read -- so the caller supplies the observation it already
    has from the arming gate. ``None`` is a first-class answer here: the RDS
    lane then labels its figure as the configured target and claims nothing
    about the running fleet.

    The window runs from ``manifest.created_at``. ``expires_at`` and
    ``last_reset_at`` are never read -- one is a reaper deadline and the other
    re-seeds data without creating anything, and an expired seal does not stop
    billing.
    """

    now = _aware(now)
    run_id = str(getattr(manifest, "run_id", "") or "")
    origin_value = getattr(manifest, "created_at", None) if manifest is not None else None
    if not isinstance(origin_value, datetime):
        return _unreadable(
            run_id=run_id,
            now=now,
            detail=(
                "manifest.created_at is missing or unreadable, so the window this cost "
                "accrued over cannot be established."
            ),
            report=report,
        )
    origin = _aware(origin_value)
    seconds = Decimal(str((now - origin).total_seconds()))
    if seconds <= 0:
        return _unreadable(
            run_id=run_id,
            now=now,
            detail=(
                f"manifest.created_at ({origin.isoformat()}) is not before as_of "
                f"({now.isoformat()}), so there is no window to price."
            ),
            report=report,
        )

    rates = rates or RateCard()
    shape = shape or InstallationShape()
    window = CarryingWindow(seconds=seconds)
    hours = window.hours

    # Asked before the estimate rather than after, because estimate_carrying_cost
    # reaches rates.rds_instance_hour and an unknown class would take every other
    # lane down with the one it cannot price.
    unpriced_rds = ""
    build_rates = rates
    try:
        rds_instance_hour_usd(rates.rds_instance_class)
    except UnknownRdsInstanceClassError as error:
        unpriced_rds = str(error)
        # Every other line still prices. A priceable class is substituted only so
        # they can be built; the one line it could reach is discarded, and the RDS
        # lane is reported unpriced and excluded from both totals.
        build_rates = replace(rates, rds_instance_class=AS_RUN_RDS_INSTANCE_CLASS)

    lakebase_dbu = (
        posted.lakebase_dbu_per_hour * hours
        if posted is not None and posted.lakebase_dbu_per_hour is not None
        else None
    )
    lakebase_dsu = (
        posted.lakebase_dsu_per_hour * hours
        if posted is not None and posted.lakebase_dsu_per_hour is not None
        else None
    )
    estimate = estimate_carrying_cost(
        window,
        rates=build_rates,
        shape=shape,
        lakebase_always_on_dbu=lakebase_dbu,
        lakebase_storage_dsu=lakebase_dsu,
    )

    items: list[_Priced] = []
    unclassified: list[str] = []
    for line in estimate.lines:
        allocations = _allocations(line, shape)
        if not allocations:
            unclassified.append(line.component)
            continue
        amounts: Sequence[Decimal | None] = (
            [None] * len(allocations)
            if line.usd is None
            else _split_amounts(line.usd, allocations)
        )
        for allocation, amount in zip(allocations, amounts, strict=True):
            if allocation.lane is StandingCostLaneId.RDS and unpriced_rds:
                continue
            items.append(
                _priced_line(
                    line,
                    allocation,
                    amount,
                    hours,
                    observation_basis=_observation_basis(line, posted),
                )
            )

    if unpriced_rds:
        items.append(
            _unpriced(
                StandingCostLaneId.RDS,
                component=f"RDS PostgreSQL {rates.rds_instance_class} · unpriced class",
                cloud="aws",
                detail=(
                    f"{unpriced_rds} Failing to price is the intended outcome: a "
                    "defaulted rate would be wrong but plausible, which is worse. The "
                    "lane's storage, address and secret shares are excluded with it, so "
                    "the totals are labelled partial rather than quietly totalling less."
                ),
            )
        )

    platform = tuple(posted.platform) if posted is not None else ()
    items.extend(_priced_platform(observed, hours) for observed in platform)

    for lane_id, detail in _missing_lane_details(items, posted).items():
        items.append(
            _unpriced(
                lane_id,
                component=f"{lane_id.value} · nothing to price from",
                cloud=(
                    "databricks"
                    if lane_id
                    in {StandingCostLaneId.LAKEBASE, StandingCostLaneId.DATABRICKS_PLATFORM}
                    else "aws"
                ),
                detail=detail,
            )
        )

    observed = report is not None and not report.unavailable
    lanes = _lanes(
        items,
        hours,
        rates=rates,
        platform=platform,
        observed=observed,
        observed_rds_instance_class=observed_rds_instance_class,
    )
    partial_reasons = (
        [
            "unclassified carrying line(s) not grouped into any lane: "
            + ", ".join(unclassified)
        ]
        if unclassified
        else []
    )
    totals = _totals(items, lanes, hours, partial_reasons=partial_reasons)
    counted = _counted(items, lanes)

    databricks_per_hour = _half_per_hour(
        counted, lanes, "databricks", hours, include_predating=False
    )
    aws_per_hour = _half_per_hour(counted, lanes, "aws", hours, include_predating=False)
    posted_databricks_per_hour = _half_per_hour(
        counted, lanes, "databricks", hours, include_predating=True
    )

    shape_basis: Literal["sealed_and_observed", "sealed_shape_only"] = (
        "sealed_and_observed" if observed else "sealed_shape_only"
    )
    shape_detail = (
        "The sealed shape was compared against the account, so the lanes are priced "
        "from the seal and the drift figure is priced from the difference."
        if observed
        else "The account was not inventoried, so every lane is the sealed shape priced "
        "at the rate card. The seal is what is billing; it is simply not confirmed "
        "against a describe-* call here."
    )

    return StandingCostDisclosure(
        run_id=run_id,
        origin=origin,
        origin_basis=(
            "Measured from manifest.created_at, when the resources came into being. "
            "expires_at is a reaper deadline and last_reset_at re-seeds data without "
            "creating anything, so neither one bounds a meter and neither is read."
        ),
        as_of=now,
        elapsed_hours=float(hours),
        seal_state="sealed",
        seal_detail=(
            "The seal is readable, and this disclosure never consults its TTL. An "
            "expired seal does not stop billing, so there is no expiry path for a "
            "figure here to take."
        ),
        shape_basis=shape_basis,
        shape_detail=shape_detail,
        lanes=lanes,
        totals=totals,
        credits=_credits(
            totals,
            origin=origin,
            now=now,
            hours=hours,
            installation_accrued=_sum(
                item.window_usd or Decimal(0) for item in counted if not item.predates
            ),
            with_platform_accrued=_sum(item.window_usd or Decimal(0) for item in counted),
        ),
        posted=_posted(
            posted,
            origin=origin,
            now=now,
            hours=hours,
            databricks_usd_per_hour=posted_databricks_per_hour,
        ),
        drift=_drift(report, now),
        fairness=_fairness(
            databricks_per_day=(
                databricks_per_hour * HOURS_PER_DAY if databricks_per_hour is not None else None
            ),
            aws_per_day=aws_per_hour * HOURS_PER_DAY if aws_per_hour is not None else None,
        ),
        continuous=_continuous(
            counted,
            hours=hours,
            databricks_per_day=(
                databricks_per_hour * HOURS_PER_DAY if databricks_per_hour is not None else None
            ),
        ),
        predating=_predating(counted, hours=hours),
        summary=_SUMMARY,
        note=_note(lanes),
    )


def _note(lanes: Sequence[StandingCostLane]) -> str:
    """The note, with its lane claim read off the lanes rather than asserted.

    The opening sentence said "every one of them billing" beside a payload in
    which two of the six lanes read unavailable. A lane that could not be priced
    is one this disclosure cannot say is billing -- that is what the third state
    is for -- so the claim was contradicted by the object carrying it. Both
    counts now come from the lanes, which is the only way the sentence cannot
    outlive the reading it describes.
    """

    unpriced = sum(1 for lane in lanes if lane.figure.state == "unavailable")
    if unpriced:
        opening = (
            f"{len(lanes)} lanes standing with no bout running, of which "
            f"{len(lanes) - unpriced} are priced here and {unpriced} could not be. An "
            "unpriced lane is one this disclosure cannot say is billing, not one that "
            "is free."
        )
    else:
        opening = f"{len(lanes)} lanes, every one of them billing with no bout running."
    return (
        f"{opening} AWS figures are rate-card derived and have never been checked "
        "against an invoice: ce:GetCostAndUsage is denied to this installation. "
        "Databricks figures are posted usage multiplied by a posted price. The two are "
        "not the same kind of number and the totals are a blend of them, which is why "
        "each half is also shown on its own. No figure here is presented as settled "
        "against a bill."
    )


def _missing_lane_details(
    items: Sequence[_Priced],
    posted: PostedDatabricksUsage | None,
) -> dict[StandingCostLaneId, str]:
    """Lanes the inputs said nothing about, stated as unknown rather than free."""

    present = {item.lane for item in items}
    details: dict[StandingCostLaneId, str] = {}
    for lane_id in StandingCostLaneId:
        if lane_id in present:
            continue
        if lane_id is StandingCostLaneId.DATABRICKS_PLATFORM:
            detail = (
                "No posted platform usage was supplied. The synced-table pipeline and "
                "the app's compute are the largest standing lines in this installation, "
                "so their absence is a gap in this disclosure rather than an absence of "
                "cost."
            )
            if posted is not None and posted.unavailable.strip():
                detail = f"{detail} Posted read unavailable: {posted.unavailable.strip()}"
        elif lane_id is StandingCostLaneId.RDS_PROXY:
            detail = (
                "The sealed secret count leaves nothing for this lane, which contradicts "
                "the Terraform-managed proxy secrets the installation stands. Reported "
                "as unknown rather than as nothing."
            )
        else:
            detail = (
                "No sealed carrying line was grouped into this lane, so its cost is "
                "unknown rather than zero."
            )
        details[lane_id] = detail
    return details


def _half_per_hour(
    counted: Sequence[_Priced],
    lanes: Sequence[StandingCostLane],
    cloud: CloudName,
    hours: Decimal,
    *,
    include_predating: bool,
) -> Decimal | None:
    """One cloud's hourly standing rate, or None when any of its lanes is unpriced.

    A half with a missing lane is not a half. Returning a number for it anyway
    would put an understated figure into the fairness comparison, and on the
    Databricks side that is the one direction that would flatter us.

    ``include_predating`` is the condition the two totals are separated by, and
    it has no default: a caller has to say which of the two it is asking for.
    ``False`` sums exactly what ``totals.installation`` sums, which is what the
    fairness comparison needs -- the halves are the two sides of that total, and
    a half summed under the other condition compares a figure the panel never
    stated. ``True`` sums what ``totals.with_platform`` sums, which is what the
    posted read has to be compared against: ``system.billing.usage`` bills the
    app whether or not this run exists, so a projection that dropped it would
    manufacture a variance rather than measure one.
    """

    owned = _DATABRICKS_LANES if cloud == "databricks" else _AWS_LANES
    if any(lane.lane_id in owned and lane.figure.state == "unavailable" for lane in lanes):
        return None
    amounts = [
        item.window_usd or Decimal(0)
        for item in counted
        if item.cloud == cloud and (include_predating or not item.predates)
    ]
    if not amounts:
        return None
    return _sum(amounts) / hours


def observed_platform_components(
    rows: Iterable[Mapping[str, object]],
) -> tuple[PlatformComponent, ...]:
    """Turn posted-usage rows into platform observations, dropping nothing silently.

    A row missing its rate, its meter or its attribution is skipped, and the
    caller sees a shorter tuple -- which surfaces as an unpriced platform lane
    rather than as a smaller total.
    """

    components: list[PlatformComponent] = []
    for row in rows:
        label = str(row.get("label") or "").strip()
        attribution = str(row.get("attribution") or "").strip()
        dbu = row.get("dbu_per_hour")
        price = row.get("usd_per_dbu")
        if not label or not attribution or dbu is None or price is None:
            continue
        # Absent for every continuous meter, and absent is the answer that means
        # "the window is the right denominator" rather than a missing value.
        uptime = row.get("uptime_hours")
        components.append(
            PlatformComponent(
                label=label,
                dbu_per_hour=Decimal(str(dbu)),
                usd_per_dbu=Decimal(str(price)),
                attribution=attribution,
                grade="measured" if str(row.get("grade") or "") == "measured" else "projected",
                predates_installation=bool(row.get("predates_installation")),
                uptime_hours=Decimal(str(uptime)) if uptime is not None else None,
            )
        )
    return tuple(components)
