"""The price of an idle floor, and the claims that price is allowed to support.

Two kinds of test live here. The arithmetic tests pin the dollars and the
derivation that produced them. The claim tests pin what the copy may and may not
say -- specifically that it never implies Lakebase's floor is unbilled, which is
the one overclaim on this axis that a customer would immediately take apart.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from server.capacity import (
    AURORA_AUTO_PAUSE_SECONDS,
    AURORA_MAX_ACU,
    LAKEBASE_MAX_CU,
    LAKEBASE_MIN_CU,
    LAKEBASE_SUSPEND_SECONDS,
)
from server.cost_model import AURORA_MIN_RUNNING_ACU, LAKEBASE_DBU_PER_CU_HOUR, RateCard
from server.descent_cost import (
    ILLUSTRATIVE_DESCENTS_PER_DAY,
    aurora_descent_usd,
    build_descent_cost_disclosure,
    lakebase_descent_usd,
    measured_descent_band_usd,
    rds_daily_usd,
)
from server.models import CompetitorId, RoundId


def _aurora():
    disclosure = build_descent_cost_disclosure(
        RoundId.WAKE_IDLE_APP, CompetitorId.AURORA_SERVERLESS_V2
    )
    assert disclosure is not None
    return disclosure


def _rds():
    disclosure = build_descent_cost_disclosure(RoundId.WAKE_IDLE_APP, CompetitorId.RDS_POSTGRES)
    assert disclosure is not None
    return disclosure


def _lane(disclosure, lane_id: str):
    (lane,) = [item for item in disclosure.lanes if item.lane_id == lane_id]
    return lane


class TestTheArithmetic:
    """Each figure recomputed from first principles, not from a stored constant."""

    def test_lakebase_bills_its_floor_at_its_configured_floor_capacity(self):
        # 0.5 CU x 0.213 DBU/CU-hour x 60s/3600 x $0.26/DBU
        expected = (
            Decimal("0.5")
            * LAKEBASE_DBU_PER_CU_HOUR
            * Decimal(LAKEBASE_SUSPEND_SECONDS)
            / Decimal(3600)
            * Decimal("0.26")
        )
        assert lakebase_descent_usd(LAKEBASE_MIN_CU) == expected
        assert expected == pytest.approx(Decimal("0.0004615"))

    def test_lakebase_ceiling_descent_is_the_upper_bound(self):
        expected = (
            Decimal("2")
            * LAKEBASE_DBU_PER_CU_HOUR
            * Decimal(LAKEBASE_SUSPEND_SECONDS)
            / Decimal(3600)
            * Decimal("0.26")
        )
        assert lakebase_descent_usd(LAKEBASE_MAX_CU) == expected
        assert expected == pytest.approx(Decimal("0.001846"))

    def test_aurora_ceiling_descent_is_two_cents(self):
        # 2 ACU x $0.12/ACU-hour x 300s/3600 = $0.02
        assert aurora_descent_usd(AURORA_MAX_ACU) == pytest.approx(Decimal("0.02"))

    def test_aurora_floor_descent_is_zero_because_its_minimum_is_zero(self):
        assert aurora_descent_usd(0.0) == Decimal(0)

    def test_lakebase_floor_is_not_zero_and_that_asymmetry_is_kept(self):
        """Lakebase's 0.5 CU floor cannot reach $0; Aurora's 0 ACU floor can.

        This cuts against Lakebase and is deliberately not smoothed away.
        """

        assert lakebase_descent_usd(LAKEBASE_MIN_CU) > 0
        assert aurora_descent_usd(0.0) == 0

    def test_the_floor_duration_ratio_is_exactly_five(self):
        assert AURORA_AUTO_PAUSE_SECONDS / LAKEBASE_SUSPEND_SECONDS == 5

    def test_rds_daily_cost_reproduces_the_cost_analysis_measured_fleet(self):
        """Four idle db.t4g.micro at $1.54/day is the figure of record.

        `.anti-demo-v7/cost-analysis-2026-08-20.md` section 4 measured the live
        fleet at $1.5360/day. Reproducing it from the formula is what licenses
        using the same formula for whatever class is configured now.
        """

        assert rds_daily_usd("db.t4g.micro") * 4 == pytest.approx(Decimal("1.536"))

    def test_rds_daily_cost_follows_the_configured_class(self):
        card = RateCard()
        assert rds_daily_usd(card.rds_instance_class) == pytest.approx(Decimal("0.065") * 24)

    def test_dollars_scale_linearly_with_descent_count(self):
        """The whole point: a floor is a per-descent tax, so frequency multiplies it."""

        single = aurora_descent_usd(AURORA_MAX_ACU)
        assert single * ILLUSTRATIVE_DESCENTS_PER_DAY == pytest.approx(Decimal("0.40"))


class TestScope:
    def test_only_the_descent_round_gets_a_descent_cost(self):
        for round_id in RoundId:
            disclosure = build_descent_cost_disclosure(round_id, CompetitorId.AURORA_SERVERLESS_V2)
            if round_id is RoundId.WAKE_IDLE_APP:
                assert disclosure is not None
            else:
                assert disclosure is None, f"{round_id} should not repeat the point"


class TestTheClaimsItMakes:
    """What the copy is allowed to say, pinned so it cannot drift into an overclaim."""

    def test_it_never_implies_lakebase_idles_free(self):
        for disclosure in (_aurora(), _rds()):
            blob = " ".join(
                [disclosure.summary, disclosure.note, disclosure.floor_ratio_label]
                + [lane.band_reason for lane in disclosure.lanes]
            ).lower()
            assert "lakebase idles free" not in blob
            assert "free on lakebase" not in blob
            assert "no charge" not in blob

    def test_it_states_outright_that_both_floors_are_billed(self):
        aurora = _aurora()
        assert "both engines bill their idle floor" in aurora.summary.lower()
        assert "neither floor is free" in aurora.note.lower()

    def test_the_rds_variant_still_says_lakebase_pays_its_own_floor(self):
        """RDS carries the unconditional claim, but not by implying Lakebase is free."""

        rds = _rds()
        assert "billed, not free" in rds.note.lower()

    def test_it_never_claims_aurora_burns_compute_indefinitely_while_idle(self):
        aurora = _aurora()
        blob = " ".join(
            [aurora.summary, aurora.note] + [lane.band_reason for lane in aurora.lanes]
        ).lower()
        for forbidden in ("forever", "indefinitely", "never stops", "always billing"):
            assert forbidden not in blob
        # And it says why Aurora's floor is $0: its minimum capacity is zero.
        competitor = _lane(aurora, "competitor")
        assert "minimum capacity is 0 acu" in competitor.band_reason.lower()

    def test_the_aurora_band_is_labelled_a_bound_not_an_estimate(self):
        competitor = _lane(_aurora(), "competitor")
        assert "upper bound, not an estimate" in competitor.band_reason.lower()
        assert "dead-flat 0.5 acu" in competitor.band_reason.lower()

    def test_the_band_reason_no_longer_claims_the_descent_went_unsampled(self):
        """It was sampled, twice, and it does not decay -- it holds 0.5 ACU flat.

        The old copy hedged that Aurora "decays ACU on the way down, and that decay
        was never sampled here". Both halves are now contradicted by
        `.anti-demo-v7/aurora-acu-2026-08-21.md` section 3, and a hedge that
        survives its own disproof is worse than no hedge.
        """

        competitor = _lane(_aurora(), "competitor")
        blob = competitor.band_reason.lower()
        for retired in ("never sampled", "not sampled", "decays acu", "was never observed"):
            assert retired not in blob
        assert "sampled" in blob

    def test_the_measured_descents_are_quoted_and_sit_inside_the_band(self):
        """The point of the sample is that the ceiling overstates -- show by how much."""

        competitor = _lane(_aurora(), "competitor")
        low, high = measured_descent_band_usd()
        assert f"${low:.6f}" in competitor.band_reason
        assert f"${high:.6f}" in competitor.band_reason
        assert competitor.per_descent_low_usd is not None
        assert competitor.per_descent_high_usd is not None
        assert float(low) >= competitor.per_descent_low_usd
        assert float(high) <= competitor.per_descent_high_usd

    def test_the_zero_floor_is_qualified_as_the_paused_state_not_a_descent(self):
        """$0 is honest about a paused cluster and misleading about a descent.

        A descent that is happening is running, and a running Serverless v2 writer
        reports no less than 0.5 ACU, so the cheapest descent AWS will sell is
        300s at 0.5 ACU. The band keeps its $0 floor -- the parked state really is
        free of compute -- but the copy may not leave $0 looking reachable by a
        descent.
        """

        competitor = _lane(_aurora(), "competitor")
        blob = competitor.band_reason.lower()
        assert "paused state" in blob
        floor = aurora_descent_usd(float(AURORA_MIN_RUNNING_ACU))
        assert f"${floor:.3f}" in competitor.band_reason
        assert competitor.per_descent_low_usd == 0.0

    def test_aws_figures_are_never_called_invoice_verified(self):
        for disclosure in (_aurora(), _rds()):
            for lane in disclosure.lanes:
                if lane.product == "Lakebase":
                    continue
                assert "not invoice-verified" in lane.rate_source

    def test_databricks_and_aws_provenance_stay_distinct(self):
        """Databricks figures are posted; AWS figures are rate-card derived."""

        aurora = _aurora()
        assert "posted list price" in _lane(aurora, "lakebase").rate_source
        assert "rate-card derived" in _lane(aurora, "competitor").rate_source

    def test_storage_is_named_as_a_separate_line_not_folded_in(self):
        for disclosure in (_aurora(), _rds()):
            assert "storage" in disclosure.note.lower()
            assert "separate line" in disclosure.note.lower()

    def test_rds_is_the_unconditional_claim_and_carries_no_descent_figure(self):
        competitor = _lane(_rds(), "competitor")
        assert competitor.descends is False
        assert competitor.per_descent_low_usd is None
        assert competitor.per_descent_high_usd is None
        assert "never descends" in competitor.per_descent_display.lower()
        assert "idle or not" in competitor.per_day_display.lower()

    def test_no_verified_verdict_leaks_into_cost_copy(self):
        """`VERIFIED` is a technical state, never a slogan (ROUNDS.md copy contract)."""

        for disclosure in (_aurora(), _rds()):
            blob = " ".join([disclosure.summary, disclosure.note])
            assert "VERIFIED" not in blob


class TestTheDemoDocMatchesTheScreen:
    """ROUNDS.md carries this claim too, and it is the copy that drifted alone.

    `server/descent_cost.py` and its band reason were corrected when the two
    descents were sampled. ROUNDS.md's own bullet was not, so the doc went on
    telling a reader that Aurora "decays ACU during the descent" and that "that
    decay was never sampled" for as long as the screen said the opposite. A doc
    that contradicts the product is worse than a doc that says less, so the
    figures below are cross-referenced against the payload rather than restated.
    """

    @staticmethod
    def _bullet() -> str:
        doc = Path(__file__).resolve().parent.parent / "ROUNDS.md"
        bullets = [
            line
            for line in doc.read_text(encoding="utf-8").splitlines()
            if "Aurora bills at most" in line
        ]
        assert len(bullets) == 1, "the descent-cost bullet moved or was duplicated"
        return bullets[0]

    def test_the_doc_no_longer_says_the_descent_decays_or_went_unsampled(self):
        bullet = self._bullet().lower()
        for retired in ("decays acu during the descent", "that decay was never sampled"):
            assert retired not in bullet
        # And it states the shape that was actually measured: flat, then a step.
        assert "dead-flat" in bullet

    def test_the_doc_quotes_the_same_measured_descents_as_the_band_reason(self):
        low, high = measured_descent_band_usd()
        bullet = self._bullet()
        assert f"${low:.6f}" in bullet
        assert f"${high:.6f}" in bullet
        assert f"{AURORA_MIN_RUNNING_ACU:.3f} ACU" in bullet

    def test_the_doc_qualifies_its_zero_floor_the_way_the_band_reason_does(self):
        bullet = self._bullet()
        floor = aurora_descent_usd(float(AURORA_MIN_RUNNING_ACU))
        assert f"${floor:.3f}" in bullet
        assert "minimum capacity is 0 ACU" in bullet

    def test_the_doc_says_300s_is_a_product_floor_and_not_a_measured_duration(self):
        # Both sampled descents overran it -- 5 minutes and 15 -- so a reader who
        # takes 300s for an observation takes the bound for a measurement.
        bullet = self._bullet()
        assert f"{AURORA_AUTO_PAUSE_SECONDS}s" in bullet
        assert "documented minimum rather than an observed duration" in bullet


class TestRenderedFigures:
    """The exact strings the screen shows, pinned against silent reformatting."""

    def test_aurora_headline_figures(self):
        aurora = _aurora()
        assert _lane(aurora, "lakebase").per_descent_headline == "$0.002"
        assert _lane(aurora, "competitor").per_descent_headline == "$0.02"

    def test_aurora_bands_and_day_columns(self):
        competitor = _lane(_aurora(), "competitor")
        assert competitor.per_descent_display == "$0.00000 – $0.02000"
        assert competitor.per_day_display == "$0.00 – $0.40"
        lakebase = _lane(_aurora(), "lakebase")
        assert lakebase.per_descent_display == "$0.00046 – $0.00185"
        assert lakebase.per_day_display == "$0.01 – $0.04"

    def test_the_ratio_label_names_both_intervals(self):
        assert _aurora().floor_ratio_label == ("Aurora's floor is 5x Lakebase's · 300s vs 60s")

    def test_the_rds_ratio_label_is_the_unconditional_one(self):
        assert _rds().floor_ratio_label == (
            "Provisioned RDS never descends · billed 100% of the time"
        )

    def test_derivations_show_their_working(self):
        aurora = _aurora()
        assert _lane(aurora, "competitor").derivation == ("0–2 ACU × $0.12/ACU-hour × 300s ÷ 3600")
        assert _lane(aurora, "lakebase").derivation == (
            "0.5–2 CU × 0.213 DBU per CU-hour × 60s ÷ 3600 × $0.26/DBU"
        )

    def test_rds_derivation_names_the_configured_class_and_has_no_idle_term(self):
        derivation = _lane(_rds(), "competitor").derivation
        assert RateCard().rds_instance_class in derivation
        assert "× 24 h" in derivation
        assert "nothing to descend into" in derivation
