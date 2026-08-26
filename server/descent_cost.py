"""What one return to idle costs, on each engine, at each vendor's own floor.

Round 1 measures a return to idle. `server/capacity.py` already establishes that
most of the margin on that screen is a *policy floor* rather than a settling
speed: Lakebase is set to its shortest supported suspend timeout of 60s, Aurora
to AWS's documented minimum auto-pause of 300s. This module answers the next
question, which is the one a customer actually asks -- what does that floor cost?

The answer has to survive one specific challenge, and the copy is written for it:

    "If Aurora bills its 300 seconds, doesn't Lakebase bill its 60 seconds too?"

Yes. It does. **Both floors are billed compute.** Nothing here claims Lakebase
idles free, because it does not. The difference on this axis is a ratio and only
a ratio: Aurora's floor is 5x longer than Lakebase's. That is the whole claim,
and it is enough.

Three properties are what make it worth putting on screen at all:

1. **It is per descent, not per day.** The floor is a tax on the act of scaling
   down, so it is paid once for every descent. At one descent a day the 5x is
   irrelevant; a workload that parks twenty times a day pays it twenty times.
   Cost tracks *how often you descend*, not how long you sat idle -- which is
   why this is the cost face of Round 5's spike-readiness story rather than an
   isolated aside.

2. **The dollars are small and are reported as small.** Per descent these are
   fractions of a cent. Inflating them would be trivially demolished, so the
   figures are printed at full precision and the frequency multiplier is what
   carries the point.

3. **RDS carries the unconditional version.** Provisioned RDS never descends at
   all, so it has no floor to pay and is billed 100% of the time instead. That
   claim needs no hedging, unlike the Aurora one.

Aurora's band is deliberately wide, and the reason is now a measurement rather
than a gap. `.anti-demo-v7/aurora-acu-2026-08-21.md` section 3 sampled
CloudWatch `ServerlessDatabaseCapacity` across two real descents, and what it
found is not a decay: the writer drops straight to **0.500 ACU and holds there,
dead flat, for the whole descent** before the pause completes. So `2 ACU x 300s`
remains an upper bound and not an estimate -- the descent never came near the
ceiling -- but the reason it overstates is a measured plateau, not an unobserved
curve. The same two descents ran 5 and 15 minutes against a 300s product floor
for reasons that were not established, so the 300s is a *minimum* commitment and
the measured cost of one descent came out $0.005363 and $0.009508.

The low bound stays $0 and the derivation stays on the same element: Aurora's
minimum capacity is 0 ACU, and Round 1 refuses to arm unless it is
(`server/targets.py:_assert_armed_sync`), so a fully paused cluster genuinely
bills nothing -- one published claim the measurement strengthened rather than
corrected. What the band's low end does *not* describe is a descent, because a
running Serverless v2 instance cannot report below 0.5 ACU
(`server/cost_model.py:AURORA_MIN_RUNNING_ACU`). The floor of an actual descent
is 300s at 0.5 ACU, which is $0.005 and not zero, and the band reason says so
next to the band.

Lakebase's floor is 0.5 CU and cannot reach zero either, so on the descent
itself neither engine is free. That is the claim this panel makes and the whole
claim: the difference is the 5x ratio between the two intervals, not the presence
of a charge.

Every rate is imported rather than restated. `server/cost_model.py` owns the rate
card and is asserted equal to `server.pricing` by test; `server/capacity.py` owns
the configured capacities and idle policies, and `capacity_parity` proves them
against live control planes. A resize therefore moves the box, the disclosure and
this arithmetic together.
"""

from __future__ import annotations

from decimal import Decimal

from .capacity import (
    AURORA_AUTO_PAUSE_SECONDS,
    AURORA_MAX_ACU,
    AURORA_MIN_ACU,
    LAKEBASE_MAX_CU,
    LAKEBASE_MIN_CU,
    LAKEBASE_SUSPEND_SECONDS,
    competitor_is_aurora,
)
from .cost_model import AURORA_MIN_RUNNING_ACU, LAKEBASE_DBU_PER_CU_HOUR, RateCard
from .models import (
    CompetitorId,
    DescentCostDisclosure,
    DescentCostLane,
    RoundId,
)
from .pricing import rds_instance_hour_usd

SECONDS_PER_HOUR = Decimal(3600)
HOURS_PER_DAY = Decimal(24)

# The round whose measured quantity *is* a descent. Every other round descends
# too, but only here is the descent the thing on the clock, so only here does its
# price belong on screen. Repeating it elsewhere would be noise.
DESCENT_ROUND = RoundId.WAKE_IDLE_APP

# A frequency to multiply by, chosen to make the per-descent point legible rather
# than to model any real workload. It is labelled as an illustration everywhere it
# is rendered, and no measurement in this demo produces it.
ILLUSTRATIVE_DESCENTS_PER_DAY = 20

# The two descents `.anti-demo-v7/aurora-acu-2026-08-21.md` section 3 integrated,
# in ACU-seconds. Both held a dead-flat 0.500 ACU for their whole length; they
# differ because one ran 5 minutes and the other 15 against the same 300s product
# floor, for reasons that were not established. This is the only sampling of an
# Aurora descent this installation has, and it is what retired the old hedge that
# the decay "was never sampled here".
MEASURED_DESCENT_ACU_SECONDS: tuple[Decimal, Decimal] = (
    Decimal("160.90"),
    Decimal("285.25"),
)


def lakebase_descent_usd(cu: float, *, rates: RateCard | None = None) -> Decimal:
    """Cost of holding `cu` Lakebase compute units for one 60s suspend timeout.

    Lakebase meters compute in DBU and one CU meters 0.213 DBU/hour
    (`server/cost_model.py:LAKEBASE_DBU_PER_CU_HOUR`), so this is
    `CU x 0.213 x seconds/3600 x $/DBU`.
    """

    card = rates or RateCard()
    dbu = (
        LAKEBASE_DBU_PER_CU_HOUR
        * Decimal(str(cu))
        * Decimal(LAKEBASE_SUSPEND_SECONDS)
        / SECONDS_PER_HOUR
    )
    return dbu * card.lakebase_dbu.usd


def aurora_descent_usd(acu: float, *, rates: RateCard | None = None) -> Decimal:
    """Cost of holding `acu` Aurora capacity units for one 300s auto-pause floor.

    Aurora bills ACU-hours directly, so this is `ACU x $/ACU-hour x seconds/3600`.
    Passing the 2 ACU ceiling yields an upper bound, not an estimate, and the
    measurement says how far over: CloudWatch sampled two real descents and both
    sat dead flat at 0.500 ACU for their whole length, a quarter of the ceiling
    (`.anti-demo-v7/aurora-acu-2026-08-21.md` section 3). Passing
    `AURORA_MIN_RUNNING_ACU` therefore gives the floor a descent can actually
    reach -- $0.005 for the 300s -- where passing 0 gives the paused state.
    """

    card = rates or RateCard()
    return (
        Decimal(str(acu))
        * card.aurora_acu_hour.usd
        * Decimal(AURORA_AUTO_PAUSE_SECONDS)
        / SECONDS_PER_HOUR
    )


def measured_descent_band_usd(*, rates: RateCard | None = None) -> tuple[Decimal, Decimal]:
    """What the two sampled descents actually billed, cheapest first.

    These are integrals, not the `ACU x floor` arithmetic above: the longer of
    the two ran three times the 300s floor, so it costs more than the floor
    permits and no amount of tightening the floor calculation would find it.
    """

    card = rates or RateCard()
    priced = sorted(
        acu_seconds / SECONDS_PER_HOUR * card.aurora_acu_hour.usd
        for acu_seconds in MEASURED_DESCENT_ACU_SECONDS
    )
    return priced[0], priced[-1]


def rds_daily_usd(instance_class: str) -> Decimal:
    """What one provisioned RDS instance bills per day while doing nothing.

    There is no descent term because provisioned RDS has no automatic idle pause
    to descend into. The instance-hour rate simply runs for all 24 hours.
    """

    return rds_instance_hour_usd(instance_class) * HOURS_PER_DAY


def _usd(value: Decimal, places: int) -> str:
    return f"${value:.{places}f}"


def _usd_band(low: Decimal, high: Decimal, places: int) -> str:
    return f"{_usd(low, places)} – {_usd(high, places)}"


def _headline_usd(value: Decimal) -> str:
    """A short rounded figure for the on-screen line, not for the arithmetic.

    Sub-cent values round to one significant figure so the sentence reads as
    "$0.002" rather than "$0.00185"; the exact band stays behind the click.
    """

    if value >= Decimal("0.01"):
        return _usd(value, 2)
    exponent = value.adjusted()
    quantum = Decimal(1).scaleb(exponent)
    return f"${value.quantize(quantum).normalize():f}"


def _floor_ratio(longer: int, shorter: int) -> Decimal:
    return Decimal(longer) / Decimal(shorter)


def _ratio_label(value: Decimal) -> str:
    return f"{value.normalize():f}x"


def _lakebase_lane(rates: RateCard) -> DescentCostLane:
    low = lakebase_descent_usd(LAKEBASE_MIN_CU, rates=rates)
    high = lakebase_descent_usd(LAKEBASE_MAX_CU, rates=rates)
    day_low = low * ILLUSTRATIVE_DESCENTS_PER_DAY
    day_high = high * ILLUSTRATIVE_DESCENTS_PER_DAY
    return DescentCostLane(
        lane_id="lakebase",
        product="Lakebase",
        descends=True,
        floor_label=f"{LAKEBASE_SUSPEND_SECONDS}s suspend timeout (vendor minimum)",
        per_descent_low_usd=float(low),
        per_descent_high_usd=float(high),
        per_descent_display=_usd_band(low, high, 5),
        per_descent_headline=_headline_usd(high),
        per_day_display=_usd_band(day_low, day_high, 2),
        derivation=(
            f"{LAKEBASE_MIN_CU:g}–{LAKEBASE_MAX_CU:g} CU × "
            f"{LAKEBASE_DBU_PER_CU_HOUR} DBU per CU-hour × "
            f"{LAKEBASE_SUSPEND_SECONDS}s ÷ 3600 × "
            f"{_usd(rates.lakebase_dbu.usd, 2)}/DBU"
        ),
        rate_source=(
            "Databricks posted list price (promotional) · "
            "system.billing.list_prices · CU-hour metering per cost analysis §7"
        ),
        band_reason=(
            f"Billed at its {LAKEBASE_MIN_CU:g} CU floor at least, and at most its "
            f"{LAKEBASE_MAX_CU:g} CU ceiling. Unlike Aurora this floor is not zero, "
            "so a descending Lakebase endpoint always bills something."
        ),
    )


def _aurora_lane(rates: RateCard) -> DescentCostLane:
    low = aurora_descent_usd(AURORA_MIN_ACU, rates=rates)
    high = aurora_descent_usd(AURORA_MAX_ACU, rates=rates)
    running_floor = aurora_descent_usd(float(AURORA_MIN_RUNNING_ACU), rates=rates)
    measured_low, measured_high = measured_descent_band_usd(rates=rates)
    day_low = low * ILLUSTRATIVE_DESCENTS_PER_DAY
    day_high = high * ILLUSTRATIVE_DESCENTS_PER_DAY
    return DescentCostLane(
        lane_id="competitor",
        product="Aurora Serverless v2",
        descends=True,
        floor_label=f"{AURORA_AUTO_PAUSE_SECONDS}s auto-pause (AWS documented minimum)",
        per_descent_low_usd=float(low),
        per_descent_high_usd=float(high),
        per_descent_display=_usd_band(low, high, 5),
        per_descent_headline=_headline_usd(high),
        per_day_display=_usd_band(day_low, day_high, 2),
        derivation=(
            f"{AURORA_MIN_ACU:g}–{AURORA_MAX_ACU:g} ACU × "
            f"{_usd(rates.aurora_acu_hour.usd, 2)}/ACU-hour × "
            f"{AURORA_AUTO_PAUSE_SECONDS}s ÷ 3600"
        ),
        rate_source=("AWS Price List API · us-west-2 · rate-card derived, not invoice-verified"),
        band_reason=(
            "Upper bound, not an estimate, and now a sampled one. CloudWatch caught "
            f"two real descents and both held a dead-flat {AURORA_MIN_RUNNING_ACU:g} "
            f"ACU — a quarter of the {AURORA_MAX_ACU:g} ACU ceiling — for the whole "
            f"way down, costing {_usd(measured_low, 6)} and {_usd(measured_high, 6)}. "
            "The floor is $0 because Aurora's minimum capacity is 0 ACU, but that is "
            f"the paused state: a descent cannot bill less than "
            f"{AURORA_AUTO_PAUSE_SECONDS}s at {AURORA_MIN_RUNNING_ACU:g} ACU, which "
            f"is {_usd(running_floor, 3)}."
        ),
    )


def _rds_lane(instance_class: str) -> DescentCostLane:
    daily = rds_daily_usd(instance_class)
    hourly = rds_instance_hour_usd(instance_class)
    return DescentCostLane(
        lane_id="competitor",
        product="RDS PostgreSQL",
        descends=False,
        floor_label="No automatic idle pause exists for provisioned RDS",
        per_descent_low_usd=None,
        per_descent_high_usd=None,
        per_descent_display="Never descends · no floor to pay",
        per_descent_headline=_usd(daily, 2),
        per_day_display=f"{_usd(daily, 2)} every day, idle or not",
        derivation=(
            f"{instance_class} at {_usd(hourly, 3)}/instance-hour × 24 h. "
            "No idle term: there is nothing to descend into."
        ),
        rate_source=("AWS Price List API · us-west-2 · rate-card derived, not invoice-verified"),
        band_reason=(
            "No band. This is not a floor paid per descent, it is the whole day "
            "billed whether or not anything ever connects."
        ),
    )


def build_descent_cost_disclosure(
    round_id: RoundId,
    competitor_id: CompetitorId,
    *,
    rates: RateCard | None = None,
) -> DescentCostDisclosure | None:
    """Assemble the per-descent cost disclosure, or None where it does not apply.

    Only Round 1 gets one. It is the round whose measured quantity is a descent,
    and putting the same arithmetic on rounds that merely happen to descend would
    repeat the point without adding evidence.
    """

    if round_id is not DESCENT_ROUND:
        return None

    card = rates or RateCard()
    lakebase = _lakebase_lane(card)

    if competitor_is_aurora(competitor_id):
        competitor = _aurora_lane(card)
        ratio = _floor_ratio(AURORA_AUTO_PAUSE_SECONDS, LAKEBASE_SUSPEND_SECONDS)
        ratio_label = (
            f"Aurora's floor is {_ratio_label(ratio)} Lakebase's · "
            f"{AURORA_AUTO_PAUSE_SECONDS}s vs {LAKEBASE_SUSPEND_SECONDS}s"
        )
        summary = (
            "Both engines bill their idle floor. Aurora's is "
            f"{_ratio_label(ratio)} longer, and it is charged on every descent."
        )
        note = (
            "Neither floor is free, and neither figure is an invoice. Lakebase bills "
            f"its {LAKEBASE_SUSPEND_SECONDS}s exactly as Aurora bills its "
            f"{AURORA_AUTO_PAUSE_SECONDS}s; the difference is the ratio, not the "
            "presence of a charge. Aurora's upper bound assumes its ceiling for the "
            "whole descent, and the sampled descents show how far that overstates: "
            f"both sat at {AURORA_MIN_RUNNING_ACU:g} ACU the whole way down, so a "
            "measured descent came out nearer the low end of the band than the high. "
            "Both figures are compute "
            "only — storage bills on every engine regardless of idle state and is a "
            "separate line. AWS figures are rate-card derived; Databricks figures "
            "are posted list prices."
        )
    else:
        competitor = _rds_lane(card.rds_instance_class)
        ratio_label = "Provisioned RDS never descends · billed 100% of the time"
        summary = (
            "Lakebase bills a 60s floor each time it descends. Provisioned RDS "
            "never descends, so it bills around the clock instead."
        )
        note = (
            "Lakebase's floor is billed, not free — it is simply paid per descent "
            "and then stops. Provisioned RDS has no automatic idle pause at any "
            "price, so there is no floor to compare: the instance bills every hour "
            "of every day whether or not a connection ever arrives. Compute only; "
            "storage is a separate line on both engines. AWS figures are rate-card "
            "derived, not invoice-verified."
        )

    return DescentCostDisclosure(
        lanes=[lakebase, competitor],
        floor_ratio_label=ratio_label,
        illustrative_descents_per_day=ILLUSTRATIVE_DESCENTS_PER_DAY,
        summary=summary,
        note=note,
    )
