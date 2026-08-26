"""The Aurora lane's per-round cost, and the boundary that keeps it honest.

Two things are being defended here at once, and they pull in opposite
directions. The cost model must not read ``V7_MEASURED_AURORA_ACU_SECONDS`` on
its own, because one bout's integral is evidence about that bout only. But the
screen must not show ``unavailable`` for six rounds that were, in fact,
measured. The resolution is that this module is the single caller that supplies
the evidence explicitly, so the tests below pin both halves: the estimator still
refuses to guess, and the panel still shows a number.

The figures come from `.anti-demo-v7/aurora-acu-2026-08-21.md`. Where a test
hard-codes a dollar value it is pinning a *published* figure -- one an audience
may have written down -- rather than restating an implementation detail.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from server.bout_cost import (
    ROUND_FIVE_BOUTS,
    ROUND_ORDER,
    ROUNDS_TWO_AND_THREE,
    ROUNDS_WITHOUT_AURORA,
    SUPERSEDED_SIX_ROUND_HIGH_USD,
    SUPERSEDED_SIX_ROUND_LOW_USD,
    aurora_lane_total_usd,
    build_bout_cost_disclosure,
)
from server.cost_model import (
    V7_MEASURED_AURORA_ACU_SECONDS,
    BoutTelemetry,
    Cloud,
    CompetitorId,
    CostKind,
    InstallationShape,
    PricingBasis,
    Provenance,
    RateCard,
    estimate_bout_cost,
    number_word,
)
from server.models import BoutCostDisclosure, BoutCostRound, RoundId, SessionSnapshot

# The published six-round Aurora marginal, superseded by measurement. Every
# figure below that claims to be "the new total" is asserted to differ from it,
# because a 2.7x correction with nothing pinning it is a correction that can be
# silently undone.
SUPERSEDED_TOTAL_DISPLAY = "$0.049800 – $0.050340"


def _disclosure():
    return build_bout_cost_disclosure()


def _row(number: int) -> BoutCostRound:
    match = [row for row in _disclosure().rounds if row.round_number == number]
    assert len(match) == 1, f"expected exactly one Round {number} row, got {len(match)}"
    return match[0]


class TestTheRegressionThisFixes:
    """No Aurora row may render `unavailable`, and none may render a bare `$0.00`."""

    def test_every_round_renders_a_figure(self):
        for row in _disclosure().rounds:
            assert row.usd_display
            assert row.usd_display.lower() != "unavailable", (
                f"Round {row.round_number} lost its measurement; the samples exist in "
                "V7_MEASURED_AURORA_ACU_SECONDS and this panel is what passes them in"
            )

    def test_no_round_is_unavailable_provenance(self):
        for row in _disclosure().rounds:
            assert row.provenance in {"measured", "structural_zero"}

    def test_the_four_provisioned_rounds_are_measured(self):
        """R1, R2, R3, R5 have CloudWatch integrals, so they are measured, not modelled."""

        rounds = _disclosure().rounds
        measured = {row.round_number for row in rounds if row.provenance == "measured"}
        assert measured == {1, 2, 3, 5}

    def test_every_measured_row_carries_its_derivation_and_basis(self):
        """Provenance is only worth anything if the reader can see where it came from."""

        for row in _disclosure().rounds:
            if row.provenance != "measured":
                continue
            assert "ACU-s" in row.derivation
            assert "$0.12/ACU-hour" in row.derivation
            assert row.band_reason
            assert row.bouts, f"Round {row.round_number} cites no bout id"


class TestTheStructuralZeros:
    """Rounds 4 and 6 are an exact $0.00. That is a different fact from `unavailable`."""

    def test_rounds_four_and_six_are_exactly_zero(self):
        for number in (4, 6):
            row = _row(number)
            assert row.usd_display == "$0.00"
            assert row.usd_low == 0.0
            assert row.usd_high == 0.0
            assert row.provenance == "structural_zero"
            assert row.band_kind == "exact_zero"

    def test_the_zero_carries_its_reason_on_the_same_row(self):
        """A bare $0.00 is indistinguishable from a failed lookup, so it never ships bare."""

        for number in (4, 6):
            row = _row(number)
            assert "no Aurora cluster" in row.derivation
            assert "not unavailable" in row.band_reason.lower()

    def test_the_zero_rounds_are_the_ones_terraform_stands_nothing_up_for(self):
        """Pinned against the estimator's own view rather than trusted as a literal."""

        zero_ids = {row.round_id for row in _disclosure().rounds if row.band_kind == "exact_zero"}
        assert zero_ids == set(ROUNDS_WITHOUT_AURORA)

    def test_a_structural_zero_never_borrows_the_unavailable_rendering(self):
        for number in (4, 6):
            assert "unavailable" not in _row(number).usd_display.lower()


class TestBandsThatMustNotCollapse:
    """R5's band is a spread; R2 and R3's band is an unanswered question."""

    def test_round_five_is_an_observed_spread_across_two_bouts(self):
        row = _row(5)
        assert row.band_kind == "observed_spread"
        assert row.usd_low is not None and row.usd_high is not None
        assert row.usd_low < row.usd_high
        assert len(ROUND_FIVE_BOUTS) == 2
        assert "–" in row.usd_display

    def test_round_five_matches_the_published_spread(self):
        row = _row(5)
        assert row.usd_display == "$0.044197 – $0.055549"

    def test_rounds_two_and_three_are_an_unresolved_billing_question(self):
        for number in (2, 3):
            row = _row(number)
            assert row.band_kind == "unresolved_billing_question"
            assert row.usd_low is not None and row.usd_high is not None
            assert row.usd_low < row.usd_high

    def test_the_spread_and_the_question_are_told_apart(self):
        """Identical-looking bands, different epistemics. One rendering for both would lie."""

        assert _row(5).band_kind != _row(2).band_kind

    def test_the_question_rounds_say_what_is_unresolved(self):
        for number in (2, 3):
            blob = f"{_row(number).band_reason} {_row(number).derivation}".lower()
            assert "deleting" in blob or "drain" in blob or "delete" in blob

    def test_no_banded_round_is_collapsed_to_a_point(self):
        for row in _disclosure().rounds:
            if row.band_kind in {"observed_spread", "unresolved_billing_question"}:
                assert "–" in row.usd_display, (
                    f"Round {row.round_number} printed one number for a range; that picks "
                    "an answer nobody gave"
                )

    def test_round_one_is_a_single_bout_and_needs_no_band(self):
        row = _row(1)
        assert row.band_kind == "single_bout"
        assert row.usd_display == "$0.015628"
        assert "–" not in row.usd_display


class TestTheSixRoundTotal:
    """The headline figure, and the superseded one it replaces."""

    def test_the_total_is_the_measured_figure(self):
        assert _disclosure().total_display == "$0.130375 – $0.141727"

    def test_the_total_is_not_the_superseded_figure(self):
        """The correction is 2.6-2.8x. This is the assertion that fails if it reverts."""

        disclosure = _disclosure()
        assert disclosure.total_display != SUPERSEDED_TOTAL_DISPLAY
        assert "$0.04980" not in disclosure.total_display
        low, high = aurora_lane_total_usd()
        assert low > SUPERSEDED_SIX_ROUND_HIGH_USD * 2
        assert high < SUPERSEDED_SIX_ROUND_LOW_USD * 3

    def test_the_superseded_figure_is_shown_rather_than_deleted(self):
        """An audience may have written the old number down; it is retired in public."""

        assert _disclosure().superseded_display == SUPERSEDED_TOTAL_DISPLAY

    def test_the_total_is_the_sum_of_the_rows(self):
        """Composed, not restated. A total that is typed in can disagree with its rows."""

        rows = _disclosure().rounds
        expected_high = sum(Decimal(str(row.usd_high or 0)) for row in rows)
        low, high = aurora_lane_total_usd()
        assert high == pytest.approx(expected_high, abs=Decimal("0.000001"))

    def test_the_drain_unbilled_alternative_is_stated_and_is_lower(self):
        """The R2/R3 question has a cheaper reading, and hiding it would be selective."""

        note = _disclosure().scope_note
        assert "$0.079159 – $0.090511" in note
        assert "does not bill a deleting instance" in note

    def test_the_scope_names_what_is_excluded(self):
        note = _disclosure().scope_note.lower()
        assert "storage" in note
        assert "ipv4" in note
        assert "rds proxy" in note


class TestSuperlativesNameTheirLane:
    """`Dearest` and `cheapest` are both true of Round 5, on different lanes."""

    def test_the_dearest_claim_names_the_aurora_lane(self):
        claim = _disclosure().dearest_claim
        assert "aurora lane" in claim.lower()
        assert "dearest" in claim.lower()

    def test_the_cheapest_claim_names_the_lakebase_lane(self):
        claim = _disclosure().lakebase_lane_claim
        assert "lakebase lane" in claim.lower()
        assert "cheapest" in claim.lower()

    def test_the_two_superlatives_are_reconciled_rather_than_left_to_collide(self):
        """Said in two panels without naming the lane, this is a live contradiction."""

        claim = _disclosure().lakebase_lane_claim.lower()
        assert "different lanes" in claim
        assert "both true" in claim

    def test_no_bare_superlative_anywhere_in_the_payload(self):
        """Every `dearest`/`cheapest` in this payload sits within reach of its lane name."""

        disclosure = _disclosure()
        blobs = [
            disclosure.summary,
            disclosure.dearest_claim,
            disclosure.lakebase_lane_claim,
            disclosure.scope_note,
            disclosure.note,
        ] + [f"{row.band_reason} {row.derivation}" for row in disclosure.rounds]
        for blob in blobs:
            lowered = blob.lower()
            for word in ("dearest", "cheapest", "most expensive"):
                if word in lowered:
                    assert "aurora" in lowered or "lakebase" in lowered, (
                        f"unqualified '{word}' in: {blob}"
                    )

    def test_the_stronger_comparison_is_the_one_encoded(self):
        """`Dearest single round` is weak; `still under two rounds combined` is not."""

        claim = _disclosure().dearest_claim
        assert "$0.070550" in claim
        assert "rounds 2 and 3 combined" in claim.lower()

    def test_the_pair_total_is_summed_from_the_rows_it_names(self):
        rows = {row.round_id: row for row in _disclosure().rounds}
        pair = sum(Decimal(str(rows[rid].usd_high or 0)) for rid in ROUNDS_TWO_AND_THREE)
        assert f"${pair:.6f}" in _disclosure().dearest_claim

    def test_round_five_really_is_under_the_pair(self):
        """The claim is checked against the numbers, not asserted alongside them."""

        rows = {row.round_id: row for row in _disclosure().rounds}
        pair = sum(Decimal(str(rows[rid].usd_high or 0)) for rid in ROUNDS_TWO_AND_THREE)
        assert Decimal(str(_row(5).usd_high)) < pair

    def test_round_five_is_actually_the_dearest_on_this_lane(self):
        highs = {row.round_number: row.usd_high or 0.0 for row in _disclosure().rounds}
        assert max(highs, key=lambda number: highs[number]) == 5


class TestTheBoundaryWithTheCostModel:
    """The model still refuses to guess. This module is why the screen is not blank."""

    def test_the_estimator_still_returns_unavailable_without_samples(self):
        """The separation the last correction drew survives this wiring."""

        telemetry = BoutTelemetry(
            round_id=RoundId.WAKE_IDLE_APP,
            competitor_id=CompetitorId.AURORA_SERVERLESS_V2,
            bout_seconds=Decimal(120),
            competitor_lane_seconds=Decimal(120),
        )
        estimate = estimate_bout_cost(telemetry, rates=RateCard.for_basis(PricingBasis.AS_RUN))
        acu = [
            line
            for line in estimate.lines
            if line.cloud is Cloud.AWS and line.kind is CostKind.COMPUTE
        ]
        assert acu, "expected an Aurora compute line for a provisioned round"
        assert all(line.quantity.provenance is Provenance.UNAVAILABLE for line in acu)
        assert all(line.quantity.point is None for line in acu)

    def test_the_estimator_does_not_reach_for_the_measured_constant(self):
        """`V7_MEASURED_AURORA_ACU_SECONDS` is evidence about past bouts, not a default.

        Round 1 has a recorded integral. An estimator that quietly applied it
        would price this sample-free telemetry at the measured figure, and the
        error that fix was written to prevent would be back.
        """

        recorded = V7_MEASURED_AURORA_ACU_SECONDS[RoundId.WAKE_IDLE_APP]
        telemetry = BoutTelemetry(
            round_id=RoundId.WAKE_IDLE_APP,
            competitor_id=CompetitorId.AURORA_SERVERLESS_V2,
            bout_seconds=Decimal(120),
            competitor_lane_seconds=Decimal(120),
        )
        estimate = estimate_bout_cost(telemetry, rates=RateCard.for_basis(PricingBasis.AS_RUN))
        for line in estimate.lines:
            assert line.quantity.point != recorded.point

    def test_this_module_supplies_the_evidence_and_gets_a_measured_quantity(self):
        """The other half: given samples, the same estimator returns MEASURED."""

        row = _row(1)
        assert row.provenance == "measured"
        assert row.usd_high is not None and row.usd_high > 0

    def test_the_measured_rows_are_priced_at_the_as_run_basis(self):
        """Every bout on this panel already happened, so it prices at what it ran on."""

        as_run = RateCard.for_basis(PricingBasis.AS_RUN)
        assert f"${as_run.aurora_acu_hour.usd:.2f}/ACU-hour" in _row(1).derivation

    def test_the_rows_cover_every_round_exactly_once(self):
        rows = _disclosure().rounds
        assert [row.round_number for row in rows] == [1, 2, 3, 4, 5, 6]
        assert [row.round_id for row in rows] == [entry[0] for entry in ROUND_ORDER]


class TestTheWiring:
    """The panel has to reach a session, or none of the above is on screen."""

    def test_the_session_snapshot_carries_the_disclosure(self):
        """Wired at the same place as its sibling disclosures, not bolted on later."""

        # Asserted as "inside the same SessionSnapshot(...) call" rather than
        # "within 600 characters of its sibling", which is what this used to
        # say. Distance in characters is not the property worth guarding: a
        # comment added next to one of the arguments moved the other out of
        # range and failed a wiring test about wiring that had not changed.
        source = (Path(__file__).resolve().parent.parent / "server" / "manager.py").read_text()
        construction = source[source.index("snapshot = SessionSnapshot(") :]
        arguments = construction[: construction.index("\n        )")]
        assert "descent_cost=build_descent_cost_disclosure" in arguments
        assert "bout_cost=build_bout_cost_disclosure()" in arguments
        assert "from .bout_cost import build_bout_cost_disclosure" in source

    def test_the_field_is_optional_so_a_missing_panel_is_absent_not_empty(self):
        field = SessionSnapshot.model_fields["bout_cost"]
        assert field.default is None
        assert not field.is_required()

    def test_the_disclosure_round_trips_through_json(self):
        """It travels as a payload, so float conversion must not lose a figure."""

        payload = _disclosure().model_dump(mode="json")
        restored = BoutCostDisclosure.model_validate(payload)
        assert restored.total_display == "$0.130375 – $0.141727"
        assert [row.usd_display for row in restored.rounds] == [
            row.usd_display for row in _disclosure().rounds
        ]

    def test_it_takes_no_round_argument(self):
        """Its claim is a comparison between rounds, so every round shows the table."""

        assert build_bout_cost_disclosure() is not None


class TestCopyContract:
    """The house rules, applied to a new panel."""

    def test_no_verified_verdict_leaks_into_the_copy(self):
        disclosure = _disclosure()
        blob = " ".join(
            [disclosure.summary, disclosure.scope_note, disclosure.note, disclosure.rate_source]
            + [row.band_reason for row in disclosure.rounds]
        )
        assert "VERIFIED" not in blob

    def test_the_rate_is_never_called_invoice_verified(self):
        disclosure = _disclosure()
        assert "not invoice-verified" in disclosure.rate_source
        assert "not invoice-verified" in disclosure.note

    def test_it_separates_a_measured_quantity_from_an_unverified_rate(self):
        """The honest shape of this figure: the count is real, the price is a rate card."""

        note = _disclosure().note.lower()
        assert "quantity is measured" in note
        assert "rate is not" in note

    def test_the_structural_zero_rows_count_the_instances_that_actually_stand(self):
        """The sentence said "four" for a fleet that had been down to three.

        Rounds 4 and 6 print `$0.00` and explain that the figure is *marginal*
        Aurora only, by naming what keeps billing regardless. It named four RDS
        instances. Round 1's was deleted -- `infra/aws/locals.tf` keys the fleet
        on `["r2","r3","r5"]` and the v7 manifest seals `rds: null` for Round 1 --
        so the sentence overstated the standing fleet by one box on the two rows
        whose whole job is to say what the zero does not cover.

        The count is now read off `InstallationShape`, which is the same object
        the standing-cost panel prices, so the two cannot disagree.
        """

        expected = number_word(InstallationShape().rds_instances)
        assert expected == "three"
        for number in (4, 6):
            reason = _row(number).band_reason
            assert f"the {expected} standing RDS instances" in reason
            assert "four standing RDS instances" not in reason

    def test_the_row_count_and_the_fleet_count_are_the_same_number(self):
        """Belt and braces: the word tracks the shape, not a literal.

        If someone re-types the numeral, this fails while the assertion above
        still passes -- it is the derivation that is being pinned, not the string.
        """

        for size in (0, 1, 2, 3, 4):
            assert number_word(size) == ("no", "one", "two", "three", "four")[size]
        assert number_word(InstallationShape().rds_instances) in _row(4).band_reason
