"""What each round's Aurora lane actually cost, from CloudWatch rather than a clock.

:mod:`server.cost_model` will not invent an Aurora quantity any more, and it is
right not to: a lane clock excludes the auto-pause descent, which was 97.2% of
Round 1's measured cost.  The consequence is that ``_aurora_acu_quantity``
returns ``unavailable`` unless a caller hands it samples, and
``V7_MEASURED_AURORA_ACU_SECONDS`` is deliberately not read by the estimator --
a past bout's integral is evidence about *that* bout, so auto-applying it would
manufacture the coverage the model just finished refusing to manufacture.

**This module is the caller that owns that choice, and it is the only one.** It
takes the recorded CloudWatch integrals from
`.anti-demo-v7/aurora-acu-2026-08-21.md`, hands each one to
:func:`~server.cost_model.estimate_bout_cost` as an explicit observation, and
renders the result with the measurement's own basis string attached.  So the
model stays honest by default and the screen still shows a measured figure with
its provenance beside it rather than a gap.

Three distinctions are load-bearing and a flatter rendering would lose all
three:

1. **Rounds 4 and 6 are an exact zero, not an unavailable.**
   ``infra/aws/locals.tf`` provisions no Aurora cluster for them, so there is
   nothing to measure and nothing missing.  Their derivation sits on the same
   element as their ``$0.00``, which is the only condition under which a zero
   may be printed here at all.
2. **Round 5's band is a spread; Rounds 2 and 3's band is a question.** R5's two
   bouts measured 714.91 and 1017.48 ACU-seconds, 42% apart, for reasons that
   were not established -- that is an observed range.  R2 and R3 each reported a
   dead-flat 2.0 ACU for several minutes *after* ``DeleteDBInstance``, and
   whether AWS bills that is undocumented while ``ce:GetCostAndUsage`` is denied
   to this principal -- that is an unresolved question with both ends observed.
   They render as different ``band_kind`` values and neither collapses.
3. **The dearest round is a lane-specific claim.** Round 5 is the dearest single
   round on the Aurora lane and simultaneously the cheapest on the Lakebase lane.
   Both are measured and both are true, so every superlative here names its lane.

What is in the figure, and what is not.  Each row is the round's AWS *compute*
on the Aurora lane: the ACU integral, plus Round 5's RDS Proxy because that
proxy exists only for the duration of that bout.  Transient restore storage and
public IPv4 are real and are excluded, because they are not what CloudWatch
measured and the published figure this supersedes excluded them too -- so the
before and after compare like with like.  Every bout priced here has already
happened, so the rate card is pinned to
:attr:`~server.cost_model.PricingBasis.AS_RUN`.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .cost_model import (
    AuroraAcuMeasurement,
    BoutCostEstimate,
    BoutTelemetry,
    Cloud,
    CostKind,
    CostLine,
    InstallationShape,
    PricingBasis,
    RateCard,
    aurora_acu_seconds_for,
    estimate_bout_cost,
    number_word,
)
from .models import BoutCostDisclosure, BoutCostRound, CompetitorId, RoundId

# Every round in running order, with the label the panel prints.  The numbers are
# asserted against `cost_model._AWS_ROUND_KEYS` by test, so `r5` cannot end up
# labelled Round 4.
ROUND_ORDER: tuple[tuple[RoundId, int, str], ...] = (
    (RoundId.WAKE_IDLE_APP, 1, "Wake the idle app"),
    (RoundId.MAKE_SCHEMA_CHANGE_SAFELY, 2, "Schema change, safely"),
    (RoundId.RECOVER_DELETED_ORDER, 3, "Recover a deleted order"),
    (RoundId.PUT_MODEL_SCORE_IN_APP, 4, "Lakehouse data into the app"),
    (RoundId.SURVIVE_CONNECTION_SPIKE, 5, "Survive the connection spike"),
    (RoundId.ANALYZE_LIVE_ORDERS, 6, "Live app data into the lakehouse"),
)

# The rounds `infra/aws/locals.tf` stands no Aurora cluster up for.  Asserted
# against the estimator's own `_ROUNDS_WITHOUT_AWS` by test rather than trusted.
ROUNDS_WITHOUT_AURORA = frozenset({RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS})

# How many RDS instances actually stand, spelled into the Rounds 4 and 6 rows.
# Read off the sealed shape rather than written as a word: this sentence said
# "four" for as long as Round 1 had an instance and kept saying it after the
# instance was deleted, which is exactly the failure the derivation prevents.
_STANDING_RDS_INSTANCES = number_word(InstallationShape().rds_instances)

# The published six-round Aurora marginal this module's total supersedes.  It is
# carried on screen rather than deleted: a figure that moved by a factor of 2.6
# to 2.8 is one an audience may already have written down.
SUPERSEDED_SIX_ROUND_LOW_USD = Decimal("0.049800")
SUPERSEDED_SIX_ROUND_HIGH_USD = Decimal("0.050340")

# The two rounds Round 5 is compared against. The comparison is the panel's
# strongest claim, so the figure is summed from the priced rows rather than
# restated as a literal -- a superlative that is not recomputed from the same
# numbers on screen beside it is a superlative that can go stale silently.
ROUNDS_TWO_AND_THREE = (
    RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
    RoundId.RECOVER_DELETED_ORDER,
)

# Round 5's two Aurora bouts: the ACU integral CloudWatch reported and the RDS
# Proxy lifetime CloudTrail reported, paired per bout because they belong to the
# same bout and averaging across them would blur the spread that is the finding.
# `.anti-demo-v7/aurora-acu-2026-08-21.md` §2, §3.
ROUND_FIVE_BOUTS: tuple[tuple[str, Decimal, Decimal], ...] = (
    ("0123456789abcdef", Decimal("714.91"), Decimal("611")),
    ("abcdef0123456789", Decimal("1017.48"), Decimal("649")),
)

# `BoutTelemetry` requires a positive bout duration and this module never reads a
# line derived from one.  Every quantity it consumes comes from the observations
# it passes in explicitly; the Lakebase line is left unavailable on purpose,
# because this panel prices one lane and says so.  Passing a real wall clock here
# would imply otherwise.
_UNREAD_BOUT_SECONDS = Decimal(1)


@dataclass(frozen=True, slots=True)
class _PricedRound:
    """A row's dollars, kept in :class:`~decimal.Decimal` until the last moment.

    The payload carries floats because it is JSON, and float is lossy.  Totalling
    the floats would put rounding noise into a headline figure that is quoted to
    six places, so the totals are summed here and the conversion happens once.
    """

    row: BoutCostRound
    low: Decimal
    high: Decimal

    @property
    def total_low(self) -> Decimal:
        """This row's contribution to the low end of the six-round total.

        Rounds 2 and 3 contribute their *high* end, because the total is composed
        at the drain-billed reading -- the same composition the superseded figure
        used.  Only Round 5's genuine spread moves the band.
        """

        return self.low if self.row.band_kind == "observed_spread" else self.high

    @property
    def drain_delta(self) -> Decimal:
        """How much of this row rests on the unresolved deletion-drain question."""

        if self.row.band_kind != "unresolved_billing_question":
            return Decimal(0)
        return self.high - self.low


def _usd(value: Decimal) -> str:
    """Six places, because every figure here is a fraction of a cent.

    Rounding to two would print ``$0.02`` for three different rounds and
    ``$0.00`` for one that is not zero, which is how a small honest number turns
    into a dishonest one.
    """

    return f"${value:.6f}"


def _usd_band(low: Decimal, high: Decimal) -> str:
    return f"{_usd(low)} – {_usd(high)}"


def _aws_compute_lines(estimate: BoutCostEstimate) -> tuple[CostLine, ...]:
    return tuple(
        line
        for line in estimate.lines
        if line.cloud is Cloud.AWS
        and line.kind is CostKind.COMPUTE
        and line.lane_id == "competitor"
    )


def _priced_aurora_compute(
    round_id: RoundId,
    *,
    acu_seconds: Decimal,
    basis: str,
    acu_low: Decimal | None = None,
    acu_high: Decimal | None = None,
    proxy_seconds: Decimal | None = None,
) -> tuple[Decimal, Decimal]:
    """Price one round's Aurora-lane AWS compute through the estimator.

    Returns ``(low, high)`` in dollars.  The estimator does the pricing; this
    function only supplies the evidence, which is why no
    ``competitor_lane_seconds`` is passed -- a lane clock is exactly what stopped
    being admissible here.

    Raises if any contributing line came back without a quantity.  A line whose
    quantity is absent contributes nothing to a sum, so a silent failure would
    read on screen as a smaller bill rather than as a gap.
    """

    telemetry = BoutTelemetry(
        round_id=round_id,
        competitor_id=CompetitorId.AURORA_SERVERLESS_V2,
        bout_seconds=_UNREAD_BOUT_SECONDS,
        observed_acu_seconds_above_floor=acu_seconds,
        observed_acu_seconds_low=acu_low,
        observed_acu_seconds_high=acu_high,
        acu_observation_basis=basis,
        observed_proxy_lifetime_seconds=proxy_seconds,
    )
    estimate = estimate_bout_cost(telemetry, rates=RateCard.for_basis(PricingBasis.AS_RUN))
    lines = _aws_compute_lines(estimate)
    if not lines:
        raise ValueError(f"{round_id.value} produced no Aurora-lane compute line to price")
    unpriced = [line.component for line in lines if line.quantity.point is None]
    if unpriced:
        raise ValueError(
            f"{round_id.value} left {', '.join(unpriced)} without a quantity; a missing "
            "line must surface as unavailable rather than be summed as zero"
        )
    low = sum((line.usd_low or Decimal(0) for line in lines), Decimal(0))
    high = sum((line.usd_high or Decimal(0) for line in lines), Decimal(0))
    return low, high


def _measured_round(
    round_id: RoundId,
    number: int,
    label: str,
    measurement: AuroraAcuMeasurement,
) -> _PricedRound:
    if round_id is RoundId.SURVIVE_CONNECTION_SPIKE:
        return _connection_spike_round(round_id, number, label, measurement)

    low, high = _priced_aurora_compute(
        round_id,
        acu_seconds=measurement.point,
        acu_low=measurement.low,
        acu_high=measurement.high,
        basis=measurement.basis,
    )
    ambiguous = measurement.is_ambiguous
    return _PricedRound(
        row=BoutCostRound(
            round_id=round_id,
            round_number=number,
            label=label,
            provenance="measured",
            band_kind="unresolved_billing_question" if ambiguous else "single_bout",
            usd_display=_usd_band(low, high) if ambiguous else _usd(high),
            usd_low=float(low),
            usd_high=float(high),
            derivation=(
                f"{measurement.low:f}–{measurement.high:f} ACU-s ÷ 3600 × $0.12/ACU-hour"
                if ambiguous
                else f"{measurement.point:f} ACU-s ÷ 3600 × $0.12/ACU-hour"
            ),
            band_reason=measurement.basis,
            bouts=list(measurement.bouts),
        ),
        low=low,
        high=high,
    )


def _connection_spike_round(
    round_id: RoundId,
    number: int,
    label: str,
    measurement: AuroraAcuMeasurement,
) -> _PricedRound:
    """Round 5, priced per bout because its band is a spread between two of them.

    The proxy lifetime belongs to the same bout as the ACU integral, so the two
    are priced together and the band is the range the two totals spanned.
    Averaging the ACU figures and the proxy figures separately would produce a
    midpoint no bout ever cost.
    """

    totals = [
        _priced_aurora_compute(
            round_id,
            acu_seconds=acu_seconds,
            basis=f"{measurement.basis} — bout {bout}",
            proxy_seconds=proxy_seconds,
        )[0]
        for bout, acu_seconds, proxy_seconds in ROUND_FIVE_BOUTS
    ]
    low, high = min(totals), max(totals)
    return _PricedRound(
        row=BoutCostRound(
            round_id=round_id,
            round_number=number,
            label=label,
            provenance="measured",
            band_kind="observed_spread",
            usd_display=_usd_band(low, high),
            usd_low=float(low),
            usd_high=float(high),
            derivation=(
                "714.91–1017.48 ACU-s ÷ 3600 × $0.12/ACU-hour, plus RDS Proxy "
                "611–649 s × 8 units × $0.015/unit-hour"
            ),
            band_reason=(
                "The band is the spread between two real bouts of this round, 42% apart, "
                "not modelling slack. Their descents held 0.5 ACU for 5 and 15 minutes "
                "for reasons that were not established. Neither bout has a receipt, so "
                "it is not established that the 128-client burst fully landed, which "
                "makes these a lower bound on a contract-satisfying Round 5 rather than "
                "a certain equal."
            ),
            bouts=list(measurement.bouts),
        ),
        low=low,
        high=high,
    )


def _structural_zero_round(round_id: RoundId, number: int, label: str) -> _PricedRound:
    """A zero whose derivation sits on the same element as the zero.

    Rounds 4 and 6 stand no competitor database up at all, which is exactly what
    their acceptance contract measures.  The estimator emits no AWS line for them
    on purpose -- a zero line there would claim the AWS *alternative* is free --
    but the panel does have to say the Aurora lane cost nothing, because leaving
    the row blank would read as an unmeasured gap instead of an exact result.
    """

    return _PricedRound(
        row=BoutCostRound(
            round_id=round_id,
            round_number=number,
            label=label,
            provenance="structural_zero",
            band_kind="exact_zero",
            usd_display="$0.00",
            usd_low=0.0,
            usd_high=0.0,
            derivation="infra/aws/locals.tf stands up no Aurora cluster for this round",
            band_reason=(
                "Exact, not unavailable and not rounded down. There is no Aurora cluster "
                "to wake, so there is no capacity to bill. Marginal Aurora cost only — "
                f"the {_STANDING_RDS_INSTANCES} standing RDS instances bill straight "
                "through this round."
            ),
            bouts=[],
        ),
        low=Decimal(0),
        high=Decimal(0),
    )


def _unavailable_round(round_id: RoundId, number: int, label: str) -> _PricedRound:
    """Reached only if a provisioned round loses its measurement.

    It cannot happen today and the row exists so that it fails loudly if it ever
    does: a provisioned lane with no integral has an unknown cost, and unknown is
    not zero.
    """

    return _PricedRound(
        row=BoutCostRound(
            round_id=round_id,
            round_number=number,
            label=label,
            provenance="unavailable",
            band_kind="single_bout",
            usd_display="Unavailable",
            usd_low=None,
            usd_high=None,
            derivation="No CloudWatch integral has been recorded for this round",
            band_reason=(
                "Terraform stands an Aurora cluster up for this round, so it has a cost "
                "and the cost is unknown. Not zero — unknown."
            ),
            bouts=[],
        ),
        low=Decimal(0),
        high=Decimal(0),
    )


def build_bout_cost_disclosure() -> BoutCostDisclosure:
    """The whole six-round Aurora table, measured.

    It takes no round argument on purpose.  The claim the panel exists to carry
    is a comparison *between* rounds -- Round 5 is the dearest on this lane and
    still less than Rounds 2 and 3 combined -- and a per-round version would
    either repeat the table six times or state a superlative with nothing on
    screen to check it against.
    """

    priced: list[_PricedRound] = []
    for round_id, number, label in ROUND_ORDER:
        measurement = aurora_acu_seconds_for(round_id)
        if measurement is not None:
            priced.append(_measured_round(round_id, number, label, measurement))
        elif round_id in ROUNDS_WITHOUT_AURORA:
            priced.append(_structural_zero_round(round_id, number, label))
        else:
            priced.append(_unavailable_round(round_id, number, label))

    total_low = sum((item.total_low for item in priced), Decimal(0))
    total_high = sum((item.high for item in priced), Decimal(0))
    drain = sum((item.drain_delta for item in priced), Decimal(0))
    pair_total = sum(
        (item.high for item in priced if item.row.round_id in ROUNDS_TWO_AND_THREE),
        Decimal(0),
    )
    dearest = max(priced, key=lambda item: item.high)
    if dearest.high >= pair_total:
        raise ValueError(
            f"Round {dearest.row.round_number} at {_usd(dearest.high)} is no longer under "
            f"Rounds 2 and 3 combined at {_usd(pair_total)}; the panel's comparison claim "
            "would be false and must be rewritten, not re-rendered"
        )

    return BoutCostDisclosure(
        rounds=[item.row for item in priced],
        total_display=_usd_band(total_low, total_high),
        superseded_display=_usd_band(
            SUPERSEDED_SIX_ROUND_LOW_USD, SUPERSEDED_SIX_ROUND_HIGH_USD
        ),
        dearest_claim=(
            f"On the Aurora lane, Round {dearest.row.round_number} is the dearest single "
            f"round at {_usd(dearest.high)} — and still less than Rounds 2 and 3 "
            f"combined, {_usd(pair_total)}. That comparison is the stronger claim and it "
            "is the one to make: the dearest round on this lane is still cheaper than two "
            "ordinary ones together."
        ),
        lakebase_lane_claim=(
            "On the Lakebase lane, Round 5 is the cheapest round — 82 CU-seconds on its "
            "one isolable bout. Dearest against Aurora and cheapest on Lakebase are both "
            "measured and both true; they are different lanes, and neither figure is the "
            "other's answer."
        ),
        summary=(
            "Aurora's billed capacity, measured out of band rather than modelled from a "
            "lane clock. Every clock this harness keeps stops before Aurora does."
        ),
        scope_note=(
            "Aurora Serverless v2 compute, plus Round 5's RDS Proxy because that proxy "
            "exists only for the bout. The transient restores' storage and public IPv4 "
            "are real and are excluded, because they are not what CloudWatch measured "
            "and the superseded figure excluded them too — so the two compare like with "
            "like. The total takes Rounds 2 and 3 at the drain-billed reading; if AWS "
            "does not bill a deleting instance it falls to "
            f"{_usd_band(total_low - drain, total_high - drain)}."
        ),
        note=(
            "The quantity is measured; the rate is not. ce:GetCostAndUsage and "
            "pricing:GetProducts are both denied to this principal, so every dollar here "
            "is rate-card derived, not invoice-verified. What the measurement changed is "
            "the ACU-second count, never the price it is multiplied by."
        ),
        rate_source=(
            "CloudWatch ServerlessDatabaseCapacity · us-west-2 · AWS Price List API · "
            "rate-card derived, not invoice-verified"
        ),
    )


def aurora_lane_total_usd() -> tuple[Decimal, Decimal]:
    """The six-round Aurora marginal as a pair of numbers, for tests and callers.

    Derived by re-pricing the table rather than restated, so a figure quoted in
    copy cannot drift from the one the estimator produces.
    """

    priced: list[_PricedRound] = []
    for round_id, number, label in ROUND_ORDER:
        measurement = aurora_acu_seconds_for(round_id)
        if measurement is None:
            continue
        priced.append(_measured_round(round_id, number, label, measurement))
    return (
        sum((item.total_low for item in priced), Decimal(0)),
        sum((item.high for item in priced), Decimal(0)),
    )
