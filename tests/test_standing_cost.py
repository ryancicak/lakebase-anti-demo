"""What the installation costs while nobody is ringing the bell.

Three kinds of test live here, and the second kind is the point.

The arithmetic tests recompute every figure inside the test from
:class:`RateCard` and :class:`InstallationShape`, so a rate change moves the
builder and the expectation together or fails loudly in both.

The invariant tests pin the properties that make this disclosure trustworthy
rather than merely large: that a bare ``$0.00`` cannot appear on a lane, that
neither total can be read without the condition it holds under, that a posted
actual is never differenced against a projection over a different window, and
that drift is never summed into the headline.

The copy tests pin what the payload may and may not say. The fairness paragraph
is the one already on the proof surface and is reused rather than rewritten, so
the test that matters is that its figures are derived and that it is withheld --
not reworded -- when the claim it makes stops holding.
"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from server.capacity import (
    LAKEBASE_SUSPEND_SECONDS,
    RDS_SCORED_ROUNDS,
    rds_lane_is_scored,
)
from server.cost_model import (
    AS_RUN_RDS_INSTANCE_CLASS,
    IMPUTED_RDS_ROUNDS,
    CarryingWindow,
    Cloud,
    InstallationShape,
    RateCard,
    estimate_carrying_cost,
    number_word,
)
from server.manifest import DemoManifest
from server.models import RoundId, StandingCostLaneId
from server.pricing import rds_instance_hour_usd
from server.reconcile import (
    AURORA_WRITER,
    EC2_RUNNER,
    IPV4_DRIFT,
    MISSING_RESIDENT,
    ORPHAN_EPHEMERAL,
    RDS_INSTANCE,
    Finding,
    ObservedResource,
    ReconciliationReport,
)
from server.standing_cost import (
    HOURS_PER_DAY,
    PlatformComponent,
    PostedDatabricksUsage,
    build_standing_cost_disclosure,
    observed_platform_components,
)

RUN_ID = "v7-standing-cost-test"
INSTALLATION_ID = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"

# The real installation's window: sealed 2026-08-20T14:48Z, read at midnight.
# 9.2 hours is the figure the posted Databricks day has to be compared against,
# and getting that comparison wrong once is why this module carries three figures
# instead of two.
ORIGIN = datetime(2026, 8, 20, 14, 48, tzinfo=UTC)
NOW = datetime(2026, 8, 21, 0, 0, tzinfo=UTC)
ELAPSED_HOURS = Decimal("9.2")

# Posted prices, supplied by the caller rather than known to the module. These are
# inputs to the test, not rates the builder is allowed to hold.
POSTED_DBU_USD = Decimal("0.30")
POSTED_PIPELINE_DBU_PER_HOUR = Decimal("2.4")
POSTED_APP_DBU_PER_HOUR = Decimal("1.2")
POSTED_LAKEBASE_DBU_PER_HOUR = Decimal("0.5273")
POSTED_LAKEBASE_DSU_PER_HOUR = Decimal("0.00615")


class FakeManifest:
    """A seal that answers for ``created_at`` and objects to anything else.

    ``expires_at`` is a reaper deadline and ``last_reset_at`` re-seeds data
    without creating anything, so neither one bounds a meter. Raising on both is
    how the test proves the builder does not reach for them rather than merely
    that it currently gets the same answer without them.
    """

    def __init__(self, created_at: datetime | None = ORIGIN, run_id: str = RUN_ID) -> None:
        self.run_id = run_id
        self.created_at = created_at

    @property
    def expires_at(self) -> datetime:
        raise AssertionError("the disclosure must not read expires_at: a TTL is not a meter")

    @property
    def last_reset_at(self) -> datetime:
        raise AssertionError("the disclosure must not read last_reset_at: a reset creates nothing")


def platform(
    *,
    pipeline: Decimal = POSTED_PIPELINE_DBU_PER_HOUR,
    app: Decimal = POSTED_APP_DBU_PER_HOUR,
) -> tuple[PlatformComponent, ...]:
    return (
        PlatformComponent(
            label="Round 4 synced-table pipeline",
            dbu_per_hour=pipeline,
            usd_per_dbu=POSTED_DBU_USD,
            attribution="system.billing.usage DLT rows tagged to this run",
            grade="measured",
        ),
        PlatformComponent(
            label="Databricks App compute",
            dbu_per_hour=app,
            usd_per_dbu=POSTED_DBU_USD,
            attribution="app compute rows; the app was serving before this run_id existed",
            grade="measured",
            predates_installation=True,
        ),
    )


def posted_usage(**overrides: object) -> PostedDatabricksUsage:
    defaults: dict[str, object] = {
        "window_start": datetime(2026, 8, 20, 0, 0, tzinfo=UTC),
        "window_end": NOW,
        "posted_usd": Decimal("18.269164"),
        "lakebase_dbu_per_hour": POSTED_LAKEBASE_DBU_PER_HOUR,
        "lakebase_dbu_basis": "COMPUTE_NODE_ALWAYS_ON_MIN rows",
        "lakebase_dsu_per_hour": POSTED_LAKEBASE_DSU_PER_HOUR,
        "lakebase_dsu_basis": "STORAGE_SPACE rows",
        "platform": platform(),
    }
    defaults.update(overrides)
    return PostedDatabricksUsage(**defaults)  # type: ignore[arg-type]


def build(**kwargs: object):
    kwargs.setdefault("manifest", FakeManifest())
    kwargs.setdefault("now", NOW)
    manifest = kwargs.pop("manifest")
    return build_standing_cost_disclosure(manifest, **kwargs)  # type: ignore[arg-type]


def full():
    """The disclosure with everything available: shape, posted usage, a clean seal."""

    return build(posted=posted_usage(), report=ReconciliationReport(run_id=RUN_ID))


def lane(disclosure, lane_id: StandingCostLaneId):
    (found,) = [item for item in disclosure.lanes if item.lane_id is lane_id]
    return found


def figures(disclosure):
    """Every figure in the payload, lanes and components alike."""

    for item in disclosure.lanes:
        yield item.figure
        for component in item.components:
            yield component.figure


def installation_half_usd_per_day(disclosure, cloud: str) -> Decimal:
    """One cloud's half of ``totals.installation``, read off the payload.

    Summed from the components rather than from the lane subtotals, and under the
    same condition ``totals.installation`` is summed under: a component that
    would bill whether or not this run exists is not something this installation
    created. An unpriced lane leaves both, so its components are skipped here for
    the same reason they are skipped there.
    """

    return sum(
        (
            Decimal(str(component.figure.usd_per_day or 0.0))
            for item in disclosure.lanes
            if item.figure.state != "unavailable"
            for component in item.components
            if component.cloud == cloud and not component.predates_installation
        ),
        Decimal(0),
    )


def aws_usd_per_day(rates: RateCard, shape: InstallationShape) -> Decimal:
    """The AWS half, recomputed from the same two inputs the builder was given.

    Deliberately not a stored constant. A rate change has to move this and the
    builder together, and if it moves only one the assertion says so.
    """

    window = CarryingWindow(seconds=ELAPSED_HOURS * Decimal(3600))
    estimate = estimate_carrying_cost(window, rates=rates, shape=shape)
    total = sum(
        (line.usd or Decimal(0) for line in estimate.lines if line.cloud is Cloud.AWS),
        Decimal(0),
    )
    return total / window.hours * HOURS_PER_DAY


class TestTheOrigin:
    """Where the window starts, and which fields are not allowed to decide that."""

    def test_the_window_runs_from_created_at(self):
        disclosure = full()
        assert disclosure.origin == ORIGIN
        assert disclosure.origin_field == "created_at"
        assert disclosure.as_of == NOW
        assert disclosure.elapsed_hours == pytest.approx(float(ELAPSED_HOURS))

    def test_neither_expires_at_nor_last_reset_at_is_read(self):
        # FakeManifest raises on both. Reaching for either fails the build rather
        # than quietly returning a plausible window.
        manifest = FakeManifest()
        with pytest.raises(AssertionError):
            _ = manifest.expires_at
        with pytest.raises(AssertionError):
            _ = manifest.last_reset_at
        assert build(manifest=manifest, posted=posted_usage()).origin == ORIGIN

    def test_the_real_seal_type_works_and_an_expired_one_still_renders(self):
        # The builder is duck-typed against created_at, so this is the test that it
        # is duck-typed against the right field of the real thing. The seal here is
        # expired and has been reset since, which is the live condition today: the
        # reaper deadline passed and the account went on charging.
        manifest = DemoManifest.model_construct(
            manifest_version=7,
            run_id=RUN_ID,
            created_at=ORIGIN,
            expires_at=ORIGIN + timedelta(hours=4),
            last_reset_at=ORIGIN + timedelta(hours=6),
        )
        disclosure = build(manifest=manifest, posted=posted_usage())
        assert disclosure.run_id == RUN_ID
        assert disclosure.seal_state == "sealed"
        assert disclosure.origin == ORIGIN
        assert disclosure.elapsed_hours == pytest.approx(float(ELAPSED_HOURS))
        assert disclosure.totals is not None
        assert disclosure.totals.installation.usd_per_day > 0
        # Neither the deadline nor the reset moved the window.
        assert disclosure.elapsed_hours != pytest.approx(4.0)
        assert disclosure.elapsed_hours != pytest.approx(3.2)
        assert "does not stop billing" in disclosure.seal_detail

    def test_an_expired_seal_still_renders_because_it_still_bills(self):
        # The live condition today. An expired TTL is a reaper deadline that has
        # passed; the account did not stop charging when it did.
        expired = build(manifest=FakeManifest(), now=ORIGIN + timedelta(days=30))
        assert expired.seal_state == "sealed"
        assert expired.totals is not None
        assert expired.totals.installation.usd_per_day > 0

    def test_an_unreadable_seal_produces_no_dollar_figure_anywhere(self):
        disclosure = build(manifest=FakeManifest(created_at=None), posted=posted_usage())
        assert disclosure.seal_state == "unreadable"
        assert disclosure.lanes == []
        assert disclosure.totals is None
        assert disclosure.credits is None
        assert disclosure.origin is None
        assert "$" not in disclosure.note
        assert disclosure.posted.state == "unavailable"
        assert disclosure.fairness.state == "withheld"

    def test_a_window_that_has_not_opened_yet_prices_nothing(self):
        disclosure = build(now=ORIGIN)
        assert disclosure.seal_state == "unreadable"
        assert "no window to price" in disclosure.seal_detail

    def test_the_builder_reads_no_clock_but_the_one_it_is_given(self):
        first = build(posted=posted_usage())
        second = build(posted=posted_usage())
        assert first.model_dump() == second.model_dump()
        # A different injected instant, and only the instant, moves the payload.
        later = build(posted=posted_usage(), now=NOW + timedelta(hours=1))
        assert later.elapsed_hours == pytest.approx(float(ELAPSED_HOURS) + 1)
        assert later.model_dump() != first.model_dump()


class TestTheSixLanes:
    """Six, because four does not reconcile and five understates one side."""

    def test_all_six_lanes_are_present(self):
        assert [item.lane_id for item in full().lanes] == list(StandingCostLaneId)

    def test_the_platform_lane_is_not_the_lakebase_lane(self):
        disclosure = full()
        lakebase = lane(disclosure, StandingCostLaneId.LAKEBASE)
        platform_lane = lane(disclosure, StandingCostLaneId.DATABRICKS_PLATFORM)
        assert lakebase.side == "lakebase"
        assert platform_lane.side == "platform"
        # Folding the pipeline and the app into Lakebase would overstate Lakebase
        # several times over. That the error would point against us does not make
        # it safe.
        assert platform_lane.figure.usd_per_day > lakebase.figure.usd_per_day * 3
        assert "not the Lakebase lane" in platform_lane.caveat

    def test_the_runner_belongs_to_neither_corner(self):
        runner = lane(full(), StandingCostLaneId.NEUTRAL_RUNNER)
        assert runner.side == "shared"
        assert runner.figure.state == "priced"

    def test_the_proxy_lane_is_not_zero_because_its_secrets_stand(self):
        # describe-db-proxies returns nothing, and the lane is still not free: the
        # two Terraform-managed proxy secrets outlive every proxy. That is why this
        # lane needs no special case to avoid reading as $0.00.
        shape = InstallationShape()
        proxy = lane(full(), StandingCostLaneId.RDS_PROXY)
        secrets = shape.managed_secrets - shape.rds_instances - shape.aurora_clusters
        assert secrets == 2
        expected = (
            RateCard().secret_month.usd
            * Decimal(secrets)
            * CarryingWindow(seconds=ELAPSED_HOURS * Decimal(3600)).months
            / ELAPSED_HOURS
            * HOURS_PER_DAY
        )
        assert proxy.figure.state == "priced"
        assert proxy.figure.usd_per_day == pytest.approx(float(expected))

    def test_an_injected_quantity_carries_the_callers_account_of_where_it_came_from(self):
        # The estimator names the meter generically. Which posted rows produced the
        # number is something only the caller knows, so it travels on the component.
        lakebase = lane(full(), StandingCostLaneId.LAKEBASE)
        bases = [component.quantity_basis for component in lakebase.components]
        assert any("COMPUTE_NODE_ALWAYS_ON_MIN rows" in basis for basis in bases)
        assert any("STORAGE_SPACE rows" in basis for basis in bases)

    def test_every_lane_names_the_components_it_is_made_of(self):
        for item in full().lanes:
            assert item.components
            assert item.rate_source or item.figure.state == "unavailable"


class TestTheZeroInvariant:
    """A bare ``$0.00`` on a lane is the defect this whole model exists to block."""

    def test_no_figure_renders_as_a_bare_zero(self):
        for figure in figures(full()):
            if figure.state == "priced":
                assert any(digit in figure.display for digit in "123456789")
            elif figure.state == "structural_zero":
                assert figure.zero_basis
                assert figure.zero_basis in figure.display
            else:
                assert figure.usd_per_day is None
                assert figure.derivation

    def test_aurora_compute_is_the_one_zero_allowed_and_it_carries_its_reason(self):
        aurora = lane(full(), StandingCostLaneId.AURORA)
        (compute,) = [
            component
            for component in aurora.components
            if component.figure.state == "structural_zero"
        ]
        assert InstallationShape().aurora_min_acu == 0
        assert "min_capacity" in compute.figure.zero_basis
        assert compute.figure.zero_basis in compute.figure.display
        # The lane it sits in is not zero, so the zero cannot be mistaken for the
        # cluster costing nothing.
        assert aurora.figure.usd_per_day > 0

    def test_no_zero_is_rendered_anywhere_without_its_reason_beside_it(self):
        # Structural rather than per-field: whatever else the payload grows, an
        # amount that renders as zero has to arrive with its derivation attached.
        # The negative lookahead matters -- $0.0034/day is a real figure that only
        # begins like a zero, and the formatter widens precision precisely so that
        # a small number cannot print as $0.00 in the first place.
        blob = full().model_dump_json()
        zeros = list(re.finditer(r"\$0\.00(?!\d)", blob))
        assert zeros, "Aurora compute parks at zero and should be rendering as one"
        for match in zeros:
            assert blob[match.end() :].startswith("/day · ")

    def test_a_lakebase_lane_with_no_posted_usage_is_unknown_not_free(self):
        lakebase = lane(build(), StandingCostLaneId.LAKEBASE)
        assert lakebase.figure.state == "unavailable"
        assert lakebase.figure.usd_per_day is None
        assert lakebase.evidence == "unpriced"
        assert not lakebase.counted_in_installation_total

    def test_a_missing_component_takes_its_lane_out_rather_than_understating_it(self):
        # Lakebase storage posted, compute not. The lane has a priced component and
        # still has no honest subtotal, because a partial sum would read as a total.
        disclosure = build(posted=posted_usage(lakebase_dbu_per_hour=None))
        lakebase = lane(disclosure, StandingCostLaneId.LAKEBASE)
        assert lakebase.figure.state == "unavailable"
        assert "would read as a" in lakebase.figure.derivation
        assert any(
            component.figure.state == "priced" for component in lakebase.components
        )


class TestTheTwoTotals:
    """Two figures, neither of which means anything without its condition."""

    def test_both_totals_render_together_with_their_conditions(self):
        totals = full().totals
        assert totals is not None
        assert totals.installation.condition.strip()
        assert totals.with_platform.condition.strip()
        assert "run_id" in totals.installation.condition
        assert "predates this installation" in totals.with_platform.condition
        # Both or neither: the model has no shape in which one is present alone.
        assert set(totals.model_dump()) == {"installation", "with_platform"}

    def test_the_second_total_adds_exactly_the_compute_that_predates_the_run(self):
        disclosure = full()
        totals = disclosure.totals
        assert totals is not None
        app = POSTED_APP_DBU_PER_HOUR * POSTED_DBU_USD * HOURS_PER_DAY
        difference = Decimal(str(totals.with_platform.usd_per_day)) - Decimal(
            str(totals.installation.usd_per_day)
        )
        assert float(difference) == pytest.approx(float(app))
        platform_lane = lane(disclosure, StandingCostLaneId.DATABRICKS_PLATFORM)
        (predating,) = [
            component
            for component in platform_lane.components
            if component.predates_installation
        ]
        assert predating.figure.usd_per_day == pytest.approx(float(app))
        # And it is not inside the lane's own subtotal, which is what the reader
        # in front of the panel adds up. The lane figure plus the component held
        # out of it is the lane's contribution to the second total.
        assert platform_lane.figure.usd_per_day == pytest.approx(
            float(
                sum(
                    (
                        Decimal(str(component.figure.usd_per_day))
                        for component in platform_lane.components
                        if not component.predates_installation
                    ),
                    Decimal(0),
                )
            )
        )
        assert "predates this installation" in platform_lane.figure.derivation
        assert platform_lane.counted_in_installation_total

    def test_the_lane_figures_on_screen_add_up_to_the_total_the_panel_headlines(self):
        # The defect this pins, and it needed no code reading to find: the six
        # rendered lane figures summed to ``totals.with_platform`` while the panel
        # headlined ``totals.installation`` -- because the platform lane's subtotal
        # folded in a pre-existing workspace app this run never created, and said
        # ``counted_in_installation_total`` beside it. A reader who added the
        # numbers in front of them got the Databricks side high by a whole app.
        #
        # Stated as an identity over whatever the payload carries rather than as
        # recomputed constants: it holds at any rate card, and no coincidence of
        # figures can satisfy it. The flag is the arbiter of which figures are
        # addends, so an unpriced lane and a lane that is entirely predating are
        # both excluded here for the same reason the total excludes them.
        payloads = (
            full(),
            # A rate card that cannot price one lane: the identity has to hold over
            # a partial total too, which is the case a lane-count would have missed.
            build(rates=RateCard(rds_instance_class="db.t4g.enormous"), posted=posted_usage()),
            # And with the predating app the larger of the two platform lines, so
            # the identity is not resting on which line happens to dominate.
            build(
                posted=posted_usage(
                    platform=platform(pipeline=Decimal("0.4"), app=Decimal("6.0"))
                )
            ),
        )
        for disclosure in payloads:
            totals = disclosure.totals
            assert totals is not None
            addends = [
                Decimal(str(item.figure.usd_per_day))
                for item in disclosure.lanes
                if item.counted_in_installation_total
            ]
            assert len(addends) > 3
            assert float(sum(addends, Decimal(0))) == pytest.approx(
                totals.installation.usd_per_day, abs=1e-9
            )
            # Not vacuous: this really is the smaller of the two totals, so an
            # unfiltered lane subtotal would have failed the line above.
            assert totals.installation.usd_per_day < totals.with_platform.usd_per_day
            # And the amount the lanes hold out is disclosed rather than dropped,
            # as its own figure and as the gap between the two totals.
            predating = disclosure.predating
            assert predating is not None and predating.state == "stated"
            assert predating.usd_per_day == pytest.approx(
                totals.with_platform.usd_per_day - totals.installation.usd_per_day
            )
            assert predating.components
            for component in predating.components:
                assert component in predating.paragraph
            # And the continuous line the panel now discloses one click in is an
            # addend rather than a figure beside them. It is the largest single
            # component in the installation -- 63% of this total live -- and the
            # only surface that prices it is behind the remainder door, so an edit
            # that dropped it from the counted set would take most of the headline
            # figure with it while every assertion above still passed: the
            # identity is stated over whatever the payload carries, and a payload
            # with no pipeline in it satisfies it too. Asserted as the total
            # depending on the collapsed amount, which is the property that makes
            # collapsing it honest.
            continuous = disclosure.continuous
            assert continuous is not None and continuous.state == "stated"
            assert continuous.usd_per_day is not None and continuous.usd_per_day > 0
            (owner,) = [
                item
                for item in disclosure.lanes
                if any(part.component == continuous.component for part in item.components)
            ]
            assert owner.counted_in_installation_total
            rest = sum(
                (
                    Decimal(str(part.figure.usd_per_day))
                    for part in owner.components
                    if not part.predates_installation
                    and part.component != continuous.component
                ),
                Decimal(0),
            )
            assert float(rest + Decimal(str(continuous.usd_per_day))) == pytest.approx(
                owner.figure.usd_per_day, abs=1e-9
            )

    def test_nothing_predating_leaves_the_disclosure_withheld_rather_than_a_zero(self):
        # No posted platform usage, so nothing is held out of any lane figure. A
        # paragraph stating that a zero was excluded is the bare zero this module
        # exists to prevent, one field along.
        predating = build(report=ReconciliationReport(run_id=RUN_ID)).predating
        assert predating is not None and predating.state == "withheld"
        assert predating.paragraph == ""
        assert predating.usd_per_day is None
        assert "nothing to disclose" in predating.withheld_reason

    def test_both_totals_state_their_condition_even_when_nothing_predates_the_run(self):
        # With no posted platform usage there is nothing in the payload that
        # predates the run, so the two totals coincide. They are still both
        # rendered: one total that quietly changes meaning is worse than two that
        # do not.
        totals = build(report=ReconciliationReport(run_id=RUN_ID)).totals
        assert totals is not None
        assert totals.with_platform.usd_per_day == totals.installation.usd_per_day
        assert "no component in the payload predates" in totals.installation.condition
        assert "quietly changes meaning" in totals.with_platform.condition

    def test_the_aws_subtotal_equals_the_rate_card_recomputed_in_this_test(self):
        rates, shape = RateCard(), InstallationShape()
        disclosure = build(rates=rates, shape=shape, posted=posted_usage())
        aws_lanes = (
            StandingCostLaneId.RDS,
            StandingCostLaneId.AURORA,
            StandingCostLaneId.RDS_PROXY,
            StandingCostLaneId.NEUTRAL_RUNNER,
        )
        subtotal = sum(
            Decimal(str(lane(disclosure, lane_id).figure.usd_per_day)) for lane_id in aws_lanes
        )
        assert float(subtotal) == pytest.approx(float(aws_usd_per_day(rates, shape)), abs=1e-9)

    def test_resizing_the_rds_class_moves_the_total_by_exactly_the_rate_delta(self):
        # The resize guard. The instances that actually stand, the published hourly
        # difference between the two classes, twenty-four hours -- and nothing else
        # in the payload may move, because nothing else in the installation changed.
        shape = InstallationShape()
        before = build(posted=posted_usage())
        after = build(rates=RateCard(rds_instance_class="db.t4g.large"), posted=posted_usage())
        delta = (
            Decimal(shape.rds_instances)
            * (rds_instance_hour_usd("db.t4g.large") - rds_instance_hour_usd("db.t4g.medium"))
            * HOURS_PER_DAY
        )
        assert RateCard().rds_instance_class == "db.t4g.medium"
        assert shape.rds_instances == 3
        assert delta == Decimal(3) * (Decimal("0.129") - Decimal("0.065")) * Decimal(24)
        assert before.totals is not None and after.totals is not None
        moved = Decimal(str(after.totals.installation.usd_per_day)) - Decimal(
            str(before.totals.installation.usd_per_day)
        )
        assert float(moved) == pytest.approx(float(delta), abs=1e-9)
        # The lane that moved is the only lane that moved.
        for lane_id in StandingCostLaneId:
            if lane_id is StandingCostLaneId.RDS:
                continue
            assert lane(after, lane_id).figure.usd_per_day == lane(
                before, lane_id
            ).figure.usd_per_day

    def test_an_unknown_rds_class_leaves_that_lane_out_and_says_the_total_is_partial(self):
        disclosure = build(
            rates=RateCard(rds_instance_class="db.t4g.enormous"), posted=posted_usage()
        )
        rds = lane(disclosure, StandingCostLaneId.RDS)
        assert rds.figure.state == "unavailable"
        assert not rds.counted_in_installation_total
        assert "db.t4g.enormous" in rds.components[0].quantity_basis
        totals = disclosure.totals
        assert totals is not None
        assert totals.installation.partial
        assert "partial" in totals.installation.label.lower()
        assert "excluded from the total" in totals.installation.partial_reason
        assert StandingCostLaneId.RDS not in totals.installation.lane_ids
        # The other lanes still price. A class with no published rate does not make
        # the runner stop billing.
        assert lane(disclosure, StandingCostLaneId.NEUTRAL_RUNNER).figure.state == "priced"

    def test_a_complete_total_is_not_labelled_partial(self):
        totals = full().totals
        assert totals is not None
        assert not totals.installation.partial
        assert totals.installation.partial_reason == ""

    def test_the_accrued_snapshot_does_not_tick(self):
        credits = full().credits
        assert credits is not None
        assert credits.ticks is False
        assert credits.as_of == NOW
        assert credits.origin == ORIGIN
        totals = full().totals
        assert totals is not None
        expected = Decimal(str(totals.installation.usd_per_hour)) * ELAPSED_HOURS
        assert credits.installation_accrued_usd == pytest.approx(float(expected))
        assert "9.20 h" in credits.elapsed_display


class TestPostedActuals:
    """Three figures, one window, and no blending."""

    def test_variance_is_measured_only_against_the_window_restricted_projection(self):
        disclosure = full()
        posted = disclosure.posted
        assert posted.state == "posted_through_window"
        # The posted read covers all of 2026-08-20; this installation existed for
        # 9.2 of those hours. The overlap is the only window a variance may be
        # taken over.
        assert posted.posted_hours == pytest.approx(float(ELAPSED_HOURS))
        assert posted.unposted_hours == pytest.approx(0.0)
        # Reconstructed from the components and not from the lane subtotals. A
        # lane figure is that lane's share of ``totals.installation`` and holds
        # the app's compute out of itself, while system.billing.usage bills the
        # app whether or not this run exists -- so the projection the posted
        # figure is differenced against is the one half in this payload that must
        # count predating compute. Summing the lane figures here would drop it and
        # manufacture a variance rather than measure one.
        databricks_per_hour = sum(
            (
                Decimal(str(component.figure.usd_per_hour))
                for lane_id in (
                    StandingCostLaneId.LAKEBASE,
                    StandingCostLaneId.DATABRICKS_PLATFORM,
                )
                for component in lane(disclosure, lane_id).components
            ),
            Decimal(0),
        )
        restricted = databricks_per_hour * ELAPSED_HOURS
        # Not vacuous: the projection really is larger than the lane figures add
        # to, and the difference is the compute that predates the installation.
        assert databricks_per_hour > sum(
            (
                Decimal(str(lane(disclosure, lane_id).figure.usd_per_hour))
                for lane_id in (
                    StandingCostLaneId.LAKEBASE,
                    StandingCostLaneId.DATABRICKS_PLATFORM,
                )
            ),
            Decimal(0),
        )
        assert posted.projection_in_posted_window_usd == pytest.approx(float(restricted))
        assert posted.projection_usd == pytest.approx(float(restricted))
        assert posted.variance_usd == pytest.approx(float(Decimal("18.269164") - restricted))
        assert posted.posted_usd == pytest.approx(18.269164)

    def test_the_unposted_remainder_is_explicit_rather_than_assumed_to_match(self):
        # A posted read that stops early. The hours it does not cover are carried
        # as unposted, not projected onto and called reconciled.
        disclosure = build(
            posted=posted_usage(window_end=datetime(2026, 8, 20, 20, 48, tzinfo=UTC)),
            report=ReconciliationReport(run_id=RUN_ID),
        )
        posted = disclosure.posted
        assert posted.posted_hours == pytest.approx(6.0)
        assert posted.unposted_hours == pytest.approx(float(ELAPSED_HOURS) - 6.0)
        assert posted.projection_usd is not None
        assert posted.projection_in_posted_window_usd is not None
        assert posted.projection_in_posted_window_usd < posted.projection_usd
        assert posted.unposted_basis.strip()

    def test_nothing_is_ever_labelled_reconciled(self):
        disclosure = full()
        blob = disclosure.model_dump_json().lower()
        assert "reconciled" not in blob

    def test_an_unreachable_billing_table_suppresses_variance_and_nothing_else(self):
        disclosure = build(
            posted=posted_usage(unavailable="system.billing.usage query failed: timeout"),
            report=ReconciliationReport(run_id=RUN_ID),
        )
        posted = disclosure.posted
        assert posted.state == "unavailable"
        assert posted.posted_usd is None
        assert posted.variance_usd is None
        assert "timeout" in posted.unavailable_reason
        # The projection is unaffected: the account did not stop spending because
        # a query failed.
        totals = disclosure.totals
        assert totals is not None
        assert totals.installation.usd_per_day > 0
        assert lane(disclosure, StandingCostLaneId.DATABRICKS_PLATFORM).figure.state == "priced"

    def test_a_posted_window_that_does_not_overlap_is_not_differenced(self):
        disclosure = build(
            posted=posted_usage(
                window_start=datetime(2026, 8, 18, 0, 0, tzinfo=UTC),
                window_end=datetime(2026, 8, 19, 0, 0, tzinfo=UTC),
            )
        )
        assert disclosure.posted.state == "unavailable"
        assert "does not overlap" in disclosure.posted.unavailable_reason

    def test_the_absence_of_an_aws_posted_figure_is_stated_not_inferred(self):
        posted = full().posted
        assert posted.cloud == "databricks"
        assert posted.aws_posted == "no_posted_counterpart"
        assert "ce:GetCostAndUsage is denied" in posted.aws_posted_basis
        assert "all-in label" in posted.aws_posted_basis


class TestDrift:
    """A second number, kept second."""

    def _report(self, **kwargs: object) -> ReconciliationReport:
        defaults: dict[str, object] = {"run_id": RUN_ID}
        defaults.update(kwargs)
        return ReconciliationReport(**defaults)  # type: ignore[arg-type]

    def test_an_orphan_is_aged_from_its_own_created_at(self):
        # Created four hours ago, in an installation that has stood for 9.2. Aging
        # it from the origin would more than double the figure.
        created = NOW - timedelta(hours=4)
        report = self._report(
            findings=(
                Finding(
                    ORPHAN_EPHEMERAL,
                    RDS_INSTANCE,
                    "adsc-v7-rds",
                    "round 2 clone still standing",
                    usd_per_day=Decimal("1.56"),
                    basis="db.t4g.medium compute + 1 public IPv4",
                ),
            ),
            observed=(
                ObservedResource(
                    RDS_INSTANCE, "adsc-v7-rds", "available", created_at=created
                ),
            ),
        )
        (finding,) = build(posted=posted_usage(), report=report).drift.findings
        assert finding.accrued_usd == pytest.approx(float(Decimal("1.56") / 24 * 4))
        assert "its own created_at" in finding.accrual_basis
        assert finding.usd_per_day == pytest.approx(1.56)

    def test_a_resource_that_cannot_be_aged_gets_a_rate_and_no_accrued_figure(self):
        report = self._report(
            findings=(
                Finding(
                    ORPHAN_EPHEMERAL,
                    AURORA_WRITER,
                    "adrc-v7-aurora-writer",
                    "round 3 clone still standing",
                    usd_per_day=Decimal("5.88"),
                    basis="2 ACU ceiling + 1 public IPv4",
                ),
            ),
            observed=(
                ObservedResource(AURORA_WRITER, "adrc-v7-aurora-writer", "available"),
            ),
        )
        (finding,) = build(posted=posted_usage(), report=report).drift.findings
        assert finding.usd_per_day == pytest.approx(5.88)
        assert finding.accrued_usd is None
        assert "must not assume it is new" in finding.accrual_basis

    def test_a_missing_resident_is_not_accrued_and_says_which_way_it_points(self):
        report = self._report(
            findings=(
                Finding(
                    MISSING_RESIDENT,
                    EC2_RUNNER,
                    "anti-demo-runner",
                    "sealed but not present in the account",
                ),
            )
        )
        (finding,) = build(posted=posted_usage(), report=report).drift.findings
        assert finding.usd_per_day is None
        assert finding.accrued_usd is None
        assert finding.charging_for_absent
        assert "the account does not have" in finding.accrual_basis

    def test_address_drift_below_the_seal_points_the_other_way_too(self):
        report = self._report(
            findings=(
                Finding(
                    IPV4_DRIFT,
                    "public_ipv4",
                    "account",
                    "8 chargeable addresses observed, 9 sealed",
                    usd_per_day=Decimal("0.12"),
                    basis="1 address at the published hourly rate",
                ),
            ),
            expected_public_ipv4=9,
            observed_public_ipv4=8,
        )
        drift = build(posted=posted_usage(), report=report).drift
        (finding,) = drift.findings
        assert finding.charging_for_absent
        # Charged for by the seal, not by the account, so it does not join the
        # unexpected-accrual figure.
        assert drift.unexpected_usd_per_day is None

    def test_drift_is_never_summed_into_either_total(self):
        report = self._report(
            findings=(
                Finding(
                    ORPHAN_EPHEMERAL,
                    RDS_INSTANCE,
                    "adsc-v7-rds",
                    "round 2 clone still standing",
                    usd_per_day=Decimal("1.56"),
                    basis="db.t4g.medium compute + 1 public IPv4",
                ),
            ),
            observed=(
                ObservedResource(
                    RDS_INSTANCE, "adsc-v7-rds", "available", created_at=NOW - timedelta(hours=4)
                ),
            ),
        )
        clean = full()
        drifting = build(posted=posted_usage(), report=report)
        assert drifting.drift.state == "unexpected_accrual"
        assert drifting.drift.unexpected_usd_per_day == pytest.approx(1.56)
        assert clean.totals is not None and drifting.totals is not None
        assert drifting.totals.installation.usd_per_day == clean.totals.installation.usd_per_day
        assert "never added to the totals" in drifting.drift.separation_note

    def test_a_clean_account_says_so_rather_than_saying_nothing(self):
        drift = full().drift
        assert drift.state == "sealed_shape_holds"
        assert drift.badge == "SEALED SHAPE HOLDS"
        assert drift.unexpected_usd_per_day is None

    def test_a_denied_describe_leaves_the_sealed_shape_still_counting(self):
        disclosure = build(
            posted=posted_usage(),
            report=self._report(unavailable="AccessDenied: rds:DescribeDBInstances"),
        )
        assert disclosure.drift.state == "unavailable"
        assert "AccessDenied" in disclosure.drift.unavailable_reason
        assert disclosure.shape_basis == "sealed_shape_only"
        assert lane(disclosure, StandingCostLaneId.RDS).evidence == "sealed_shape_only"
        totals = disclosure.totals
        assert totals is not None
        assert totals.installation.usd_per_day > 0

    def test_a_reconciled_account_is_labelled_as_observed(self):
        disclosure = full()
        assert disclosure.shape_basis == "sealed_and_observed"
        assert lane(disclosure, StandingCostLaneId.RDS).evidence == "rate_card_derived"


class TestTheCopy:
    """What the payload is allowed to say, and in whose voice."""

    def test_the_fairness_paragraph_is_the_one_already_on_the_proof_surface(self):
        # Reused verbatim, with only the figures filled in. The house rule against
        # a third voice applies to reasoning as much as to tone.
        fairness = full().fairness
        assert fairness.state == "stated"
        for sentence in (
            "Both sides carry standing cost here, and ours is the larger half",
            "The difference is capability, not this bill",
            f"scale to zero and does at {LAKEBASE_SUSPEND_SECONDS}s.",
            "No provisioned RDS instance can scale to zero at any price.",
        ):
            assert sentence in fairness.paragraph
        # And what it may no longer concede. Every sealed endpoint scales to
        # zero: the posted read finds no COMPUTE_NODE_ALWAYS_ON_MIN row behind
        # any of them. The clause conceding that somewhere it does not was true
        # when Round 6 was configured no_suspension and is vacuous now, and the
        # sentence is stronger without it.
        assert "setting we picked" not in fairness.paragraph

    def test_the_lakebase_lane_no_longer_claims_an_endpoint_cannot_scale_down(self):
        copy = lane(full(), StandingCostLaneId.LAKEBASE)
        assert "no_suspension, so all six have a scale-down path" in copy.caveat
        assert "has no scale-down path at all" not in copy.caveat
        assert "except where" not in copy.idle_label

    def test_what_the_rds_lane_may_say_about_the_class_it_prices(self):
        """Three readings, and the sentence has to follow the reading each time.

        The assertion this replaced pinned the opposite, and the reason it had
        to go is the point of the test. The old lane said "all four instances
        are running db.t4g.micro", decided by comparing two hardcoded constants,
        and the account was resized to db.t4g.medium on 2026-08-21 at 14:48:36Z.
        Nothing in the process could notice, so the sentence would have kept
        asserting a false fact about AWS forever.

        So: with no observation the lane may say what it prices and no more;
        with an observation that agrees there is nothing to disclaim; and with
        one that disagrees the figure is a target again and says so.

        The regression guard proper is the ``AS_RUN_RDS_INSTANCE_CLASS`` entry
        in the first two rows' absent lists: whatever that constant is set to,
        it must not be able to put words about the live fleet on screen unless
        the control plane was actually read saying so. The precondition below is
        what stops the matching row from passing that guard vacuously -- if the
        constant ever equals the configured class, the guard proves nothing and
        this test says so rather than going quiet.
        """

        configured = RateCard().rds_instance_class
        assert configured != AS_RUN_RDS_INSTANCE_CLASS

        cases: tuple[tuple[str, str | None, tuple[str, ...], tuple[str, ...]], ...] = (
            (
                "nobody read the class",
                None,
                (
                    f"prices {configured}, the configured class",
                    "no claim is made about what the running fleet is",
                ),
                (
                    "has not been applied",
                    "not what AWS is billing today",
                    AS_RUN_RDS_INSTANCE_CLASS,
                ),
            ),
            (
                "the observed class matches what is priced",
                configured,
                (),
                (
                    "configured class",
                    "configured target",
                    "billing today",
                    AS_RUN_RDS_INSTANCE_CLASS,
                ),
            ),
            (
                "a real mismatch is observed",
                "db.t4g.micro",
                (
                    f"prices {configured}, the configured class",
                    "live control plane reports db.t4g.micro running",
                    "not what AWS is billing today",
                ),
                (),
            ),
        )

        for name, observed, present, absent in cases:
            payload = (
                full()
                if observed is None
                else build(posted=posted_usage(), observed_rds_instance_class=observed)
            )
            copy = lane(payload, StandingCostLaneId.RDS)
            for fragment in present:
                assert fragment in copy.caveat, f"{name}: missing {fragment!r}"
            for fragment in absent:
                assert fragment not in copy.caveat, f"{name}: said {fragment!r}"

    def test_the_lane_names_the_instances_that_stand_and_no_others(self):
        # This caveat named Rounds 1, 2, 3 and 5 for as long as r1 had an
        # instance, and went on naming them after the instance was deleted --
        # describing a box that no longer exists. The list is now read off
        # IMPUTED_RDS_ROUNDS, and the round that lost its box has to be named as
        # imputed rather than billed.
        copy = lane(full(), StandingCostLaneId.RDS)
        assert "An instance stands for Rounds 2, 3 and 5" in copy.caveat
        assert "each of those rounds races it" in copy.caveat
        assert "Rounds 1, 4 and 6 have no instance here" in copy.caveat
        assert "Round 1's was deleted" in copy.caveat
        # And it says which question the figure answers, because the two answers
        # differ by three instance-days and only one of them is on screen.
        assert "what we pay, not what the workload costs" in copy.caveat
        assert "three more boxes" in copy.caveat
        # The old wording, in the exact shapes it took, may not come back.
        assert "Rounds 1, 2, 3 and 5" not in copy.caveat
        assert "bills without ever being timed" not in copy.caveat

    def test_the_rounds_the_caveat_names_are_the_fleet_it_prices(self):
        # The prose is prose, so this pins it to the two things it describes: the
        # count of instances the figure prices, and the policy set that decides
        # which rounds have one. Retyping either list breaks this.
        shape = InstallationShape()
        standing = tuple(
            number
            for number, round_id in enumerate(RoundId, start=1)
            if round_id not in IMPUTED_RDS_ROUNDS
        )
        imputed = tuple(
            number
            for number, round_id in enumerate(RoundId, start=1)
            if round_id in IMPUTED_RDS_ROUNDS
        )
        assert len(standing) == shape.rds_instances
        assert set(standing) | set(imputed) == set(range(1, 7))
        caveat = lane(full(), StandingCostLaneId.RDS).caveat
        assert f"An instance stands for Rounds {standing[0]}, {standing[1]} and {standing[2]}" in (
            caveat
        )
        assert f"Rounds {imputed[0]}, {imputed[1]} and {imputed[2]} have no instance" in caveat
        # The count of extra boxes a customer pays for is the count of rounds
        # without one here, spelled from the same list.
        assert f"{number_word(len(imputed))} more boxes" in caveat

    def test_the_scored_rounds_named_here_are_the_ones_the_parity_check_uses(self):
        # The prose above is prose, so this pins it to the policy it describes
        # rather than letting the two drift.
        assert {round_id.value for round_id in RDS_SCORED_ROUNDS} == {
            RoundId.SURVIVE_CONNECTION_SPIKE.value,
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY.value,
            RoundId.RECOVER_DELETED_ORDER.value,
        }
        assert not rds_lane_is_scored(RoundId.WAKE_IDLE_APP)

    def test_the_paragraphs_figures_are_derived_not_typed(self):
        # Summed over components rather than over lane subtotals, because the
        # platform lane holds one of each: a pipeline this run created and an app
        # that predates it. The paragraph's claim is about what this installation
        # costs, so it may only count the first.
        disclosure = full()
        databricks = installation_half_usd_per_day(disclosure, "databricks")
        aws = aws_usd_per_day(RateCard(), InstallationShape())
        assert f"${databricks:.2f}/day" in disclosure.fairness.paragraph
        assert f"${aws:.2f}/day" in disclosure.fairness.paragraph
        assert f"{databricks / aws:.1f}x" in disclosure.fairness.paragraph
        # The AWS half is recomputed from the rate card above and has no
        # predating component, so the two derivations must agree on it.
        assert float(installation_half_usd_per_day(disclosure, "aws")) == pytest.approx(
            float(aws), abs=5e-3
        )

    def test_the_paragraph_and_the_installation_total_sum_the_same_components(self):
        # The defect this pins. ``totals.installation`` excluded compute that
        # predates the installation and the fairness paragraph did not, so one
        # panel stated a total under one condition and compared it under another
        # -- $24.93/day standing against a "$26.20/day Databricks" half quoting a
        # pre-existing workspace app the demo never created. Pinning the new
        # figures alone would re-arm that: what has to hold is that the two are
        # derived from the same filtered set, so no future edit can move one
        # without the other.
        #
        # Stated as the halves summing to the total rather than as two recomputed
        # numbers, because that is the property, and it holds whatever the rates
        # are.
        for disclosure in (full(), build(posted=posted_usage())):
            halves = installation_half_usd_per_day(
                disclosure, "databricks"
            ) + installation_half_usd_per_day(disclosure, "aws")
            assert disclosure.totals is not None
            assert float(halves) == pytest.approx(disclosure.totals.installation.usd_per_day)
            # And the paragraph renders those halves, not some third derivation.
            paragraph = disclosure.fairness.paragraph
            assert paragraph
            databricks = installation_half_usd_per_day(disclosure, "databricks")
            aws = installation_half_usd_per_day(disclosure, "aws")
            assert (
                f"${databricks:.2f}/day Databricks against ${aws:.2f}/day AWS, "
                f"a {databricks / aws:.1f}x margin"
            ) in paragraph
            # And the test is not vacuous: this payload really does carry a
            # priced Databricks component that predates the installation, so an
            # unfiltered half would be a different number.
            predating = sum(
                (
                    Decimal(str(component.figure.usd_per_day or 0.0))
                    for item in disclosure.lanes
                    for component in item.components
                    if component.predates_installation and component.figure.state == "priced"
                ),
                Decimal(0),
            )
            assert predating > 0
            assert f"${databricks + predating:.2f}/day Databricks" not in paragraph

    def test_the_continuous_pipeline_is_disclosed_from_the_line_the_totals_count(self):
        # Round 4's pipeline is hard-required CONTINUOUS: it bills around the
        # clock for a round that runs for minutes, and it is the largest single
        # line on the Databricks side. That is a design choice and is disclosed
        # rather than restructured -- but a disclosure carrying its own arithmetic
        # would go stale exactly the way five figures in the frontend panel did.
        #
        # So this asserts an identity and never a number. The disclosed amount IS
        # the pipeline component the platform lane and both totals are summed
        # from, and the share's denominator IS the Databricks half of
        # totals.installation that the fairness paragraph quotes. A rate change
        # moves all of them together or fails here.
        disclosure = full()
        continuous = disclosure.continuous
        assert continuous is not None and continuous.state == "stated"
        (pipeline,) = [
            component
            for item in disclosure.lanes
            for component in item.components
            if component.component == continuous.component
        ]
        assert continuous.usd_per_day == pipeline.figure.usd_per_day
        assert continuous.usd_per_hour == pipeline.figure.usd_per_hour
        # The share is a share of the half the panel beside it states, so the
        # denominator is reconstructed from the payload rather than asserted.
        databricks = installation_half_usd_per_day(disclosure, "databricks")
        assert float(databricks) == pytest.approx(
            continuous.usd_per_day / continuous.share_of_databricks
        )
        assert f"${databricks:.2f}/day Databricks" in disclosure.fairness.paragraph
        # And it is that half and not the other one. Taken against the half that
        # adds compute predating the installation, this same line reads roughly
        # half of what it is -- the recorded "56% of the Databricks side" was
        # exactly that substitution -- so the two bases are asserted apart rather
        # than only the right one asserted right.
        with_predating = databricks + sum(
            (
                Decimal(str(component.figure.usd_per_day or 0.0))
                for item in disclosure.lanes
                if item.figure.state != "unavailable"
                for component in item.components
                if component.cloud == "databricks" and component.predates_installation
            ),
            Decimal(0),
        )
        assert with_predating > databricks
        assert continuous.share_of_databricks != pytest.approx(
            continuous.usd_per_day / float(with_predating)
        )
        # And the prose renders those figures rather than a third derivation.
        assert f"${continuous.usd_per_day:.2f}/day while it is running" in continuous.paragraph
        assert f"{continuous.share_of_databricks * 100:.0f}%" in continuous.paragraph
        assert f"${continuous.usd_per_hour:.2f}" in continuous.paragraph
        # The accrued figure, which is the whole reason this paragraph is worth
        # rendering in the deployed app. `pipeline_power.session_notice` was the
        # accepted mitigation for this cost and reasons from "every session
        # passes through `antidemo serve`" -- true of a checkout, false of the
        # app, whose one code path never executes `cli.py`. So the only bound
        # ever placed on the largest standing line in this installation printed
        # nowhere near where it was billing. Reconstructed from the payload,
        # because a paragraph quoting an accrued total the panel's own lanes do
        # not add up to is the defect this whole module exists to prevent.
        accrued = Decimal(str(continuous.usd_per_hour)) * Decimal(str(disclosure.elapsed_hours))
        assert f"${accrued:.2f} has accrued" in continuous.paragraph
        # A justification is a claim, and this one was measured false. The panel
        # used to tell an audience the pipeline had to run around the clock
        # because "starting the pipeline at the bell would move its startup
        # inside the bout clock and change what the round measures". It is
        # started at arm rather than at the bell, `armed_at` is captured after
        # `arm()` returns, and the round's figure is taken from the commit the
        # bell itself makes -- so no part of a start is inside the bout clock and
        # none of it reaches the measurement. A wrong claim on the panel an
        # audience reads is worse than no claim.
        assert "inside the bout clock" not in continuous.paragraph
        assert "around the clock" not in continuous.paragraph
        # A share that is not the whole may not print as the whole. Live, every
        # Lakebase endpoint scales to zero and posts a structural-zero always-on
        # minimum, which leaves storage at a third of a cent a day as the only
        # other line on this half: the share is 99.97%, and at no decimal places
        # that printed "100%" beside a Lakebase lane the panel renders non-zero.
        near = build(
            posted=posted_usage(
                lakebase_dbu_per_hour=Decimal(0),
                lakebase_dsu_per_hour=Decimal("0.0094"),
            )
        ).continuous
        assert near is not None and near.state == "stated"
        assert near.share_of_databricks is not None
        assert 0.999 < near.share_of_databricks < 1
        assert "100%" not in near.paragraph and "100.0%" not in near.paragraph
        assert f"{near.share_of_databricks * 100:.2f}%" in near.paragraph
        # And the widening is not a floor: the shipped share is 83.99% of a half
        # and renders as 84%, not 83%. Understating our own standing cost is the
        # one direction an error here would flatter us in.
        assert f"{continuous.share_of_databricks * 100:.0f}%" in continuous.paragraph
        assert continuous.share_of_databricks * 100 < 84
        # Not vacuous: the line really is inside the total it is a share of, and
        # really is the largest of them.
        assert not pipeline.predates_installation
        assert continuous.usd_per_day < disclosure.totals.installation.usd_per_day
        assert "largest single line" in continuous.paragraph
        # No per-bout figure is offered, because nothing here measures a bout.
        assert "per-bout figure is claimed" in continuous.paragraph
        # Withheld rather than reworded when the half it is a share of is unpriced.
        withheld = build().continuous
        assert withheld is not None and withheld.state == "withheld"
        assert withheld.paragraph == ""
        assert withheld.usd_per_day is None

    def test_the_paragraph_claims_no_resize_that_never_happened(self):
        # The db.t4g.medium resize is an unapplied Terraform diff. A sentence
        # comparing the margin to what it was "before their four boxes were
        # resized up" narrated an event that never occurred, and blamed the
        # change in ratio on it. Both are gone; the current ratio stands alone.
        paragraph = full().fairness.paragraph
        for invented in ("resized", "That was", "narrowed", "grew", "shrank", "before"):
            assert invented not in paragraph

    def test_the_paragraph_reproduces_the_shipped_figures_from_the_observed_day(self):
        # Feeding the observed Databricks day-rate in reproduces all three
        # figures from derivation alone, which is what makes the shipped copy
        # checkable rather than merely consistent.
        #
        # What the ratio measures, since two earlier edits moved it without
        # saying: it is this installation's Databricks standing cost over its AWS
        # standing cost, so a *larger* ratio is a worse result for us, not a
        # better one. Both of those edits were nevertheless corrections. It read
        # 2.9x against a four-instance AWS fleet; Round 1's instance was deleted,
        # the AWS half fell to $8.35/day, and the ratio rose to 3.6x -- the
        # denominator got smaller because the fleet did. Then the Databricks
        # numerator stopped counting a pre-existing workspace app this
        # installation never created, which ``totals.installation`` had always
        # excluded, and the ratio fell again. Neither move is evidence about
        # Lakebase either way; both are the panel counting the right things.
        #
        # So this test pins the derivation and not the ratio. The figure it
        # asserts is computed from the payload, and
        # test_the_paragraph_and_the_installation_total_sum_the_same_components
        # is what stops the two halves drifting apart again. If a future change
        # moves this number, read that test's failure first.
        target = Decimal("29.8568")
        lakebase_per_day = (
            POSTED_LAKEBASE_DBU_PER_HOUR * RateCard().lakebase_dbu.usd
            + POSTED_LAKEBASE_DSU_PER_HOUR * RateCard().lakebase_dsu.usd
        ) * HOURS_PER_DAY
        pipeline_share = Decimal("2") / Decimal("3")
        remaining = (target - lakebase_per_day) / POSTED_DBU_USD / HOURS_PER_DAY
        disclosure = build(
            posted=posted_usage(
                platform=platform(
                    pipeline=remaining * pipeline_share,
                    app=remaining * (1 - pipeline_share),
                )
            )
        )
        paragraph = disclosure.fairness.paragraph
        # The observed day was fed in as $29.8568 across both Databricks lanes,
        # of which the app's third predates this installation. The paragraph
        # quotes the other two thirds, against an AWS half that is still the
        # deleted-instance fleet's $8.35/day.
        databricks = installation_half_usd_per_day(disclosure, "databricks")
        aws = aws_usd_per_day(RateCard(), InstallationShape())
        assert f"${aws:.2f}/day AWS" in paragraph
        assert f"{aws:.2f}" == "8.35"
        assert (
            f"${databricks:.2f}/day Databricks against ${aws:.2f}/day AWS, "
            f"a {databricks / aws:.1f}x margin"
        ) in paragraph
        # The figure the unfiltered comprehension used to produce may not come
        # back beside it, nor may the superseded four-instance AWS half.
        assert f"${target:.2f}/day Databricks" not in paragraph
        assert "$10.12/day" not in paragraph

    def test_the_paragraph_is_withheld_rather_than_reworded_when_a_half_is_unpriced(self):
        fairness = build().fairness
        assert fairness.state == "withheld"
        assert fairness.paragraph == ""
        assert "unpriced" in fairness.withheld_reason

    def test_the_paragraph_is_withheld_when_our_half_is_no_longer_the_larger_one(self):
        # A tiny posted platform day makes the concession false. The paragraph goes
        # away; it is not quietly turned into a boast.
        disclosure = build(
            posted=posted_usage(platform=platform(pipeline=Decimal("0.01"), app=Decimal("0.01")))
        )
        assert disclosure.fairness.state == "withheld"
        assert "larger one" in disclosure.fairness.withheld_reason

    def test_the_word_verified_appears_nowhere(self):
        # No figure here has met an invoice, so no copy field may imply one has.
        assert "verified" not in full().model_dump_json().lower()

    def test_the_two_questions_framing_is_reused_rather_than_restated(self):
        assert "Marginal asks what this bout added" in full().summary

    def test_the_note_says_the_aws_side_has_never_met_an_invoice(self):
        note = full().note
        assert "ce:GetCostAndUsage is denied" in note
        assert "not the same kind of number" in note

    def test_the_notes_lane_claim_follows_the_lanes_rather_than_a_count(self):
        # "6 lanes, every one of them billing" was a fixed sentence beside a
        # payload in which two of the six read unavailable. A lane that could not
        # be priced is one this disclosure cannot say is billing.
        priced = full()
        assert all(item.figure.state != "unavailable" for item in priced.lanes)
        assert f"{len(priced.lanes)} lanes, every one of them billing" in priced.note

        partial = build()
        unpriced = [item for item in partial.lanes if item.figure.state == "unavailable"]
        assert unpriced, "this builder is meant to leave lanes unpriced"
        assert "every one of them billing" not in partial.note
        assert (
            f"{len(partial.lanes)} lanes standing with no bout running, of which "
            f"{len(partial.lanes) - len(unpriced)} are priced here and "
            f"{len(unpriced)} could not be"
        ) in partial.note
        assert "cannot say is billing, not one that is free" in partial.note


class TestNoRateLiterals:
    """The module may not carry a figure of its own. It had one job."""

    def test_the_stale_figures_from_the_design_note_were_not_transcribed(self):
        source = (
            __import__("pathlib").Path(__file__).resolve().parents[1]
            / "server"
            / "standing_cost.py"
        ).read_text(encoding="utf-8")
        # These were computed before the Aurora correction landed. Their presence
        # in this file would mean a figure was copied rather than derived.
        for stale in ("10.1237", "39.98", "29.856", "0.04980", "26.57", "0.213", "0.26"):
            assert stale not in source

    def test_every_databricks_figure_moves_with_the_posted_price(self):
        doubled = replace(
            RateCard(),
            lakebase_dbu=replace(RateCard().lakebase_dbu, usd=RateCard().lakebase_dbu.usd * 2),
        )
        base = lane(build(posted=posted_usage()), StandingCostLaneId.LAKEBASE)
        moved = lane(
            build(rates=doubled, posted=posted_usage()), StandingCostLaneId.LAKEBASE
        )
        compute = POSTED_LAKEBASE_DBU_PER_HOUR * RateCard().lakebase_dbu.usd * HOURS_PER_DAY
        assert moved.figure.usd_per_day == pytest.approx(
            base.figure.usd_per_day + float(compute)
        )

    def test_the_shape_decides_the_quantities_not_the_module(self):
        one_of_each = InstallationShape(
            rds_instances=1,
            aurora_clusters=1,
            public_ipv4_addresses=2,
            managed_secrets=3,
        )
        disclosure = build(shape=one_of_each, posted=posted_usage())
        rds = lane(disclosure, StandingCostLaneId.RDS)
        assert rds.figure.usd_per_day == pytest.approx(
            float(
                (
                    rds_instance_hour_usd("db.t4g.medium")
                    + RateCard().public_ipv4_hour.usd
                    + RateCard().rds_gp3_gb_month.usd
                    * one_of_each.rds_allocated_gb
                    / Decimal(730)
                    + RateCard().secret_month.usd / Decimal(730)
                )
                * HOURS_PER_DAY
            ),
            rel=1e-3,
        )
        assert lane(disclosure, StandingCostLaneId.RDS_PROXY).figure.state == "priced"


class TestPostedUsageParsing:
    """Rows in, observations out, and nothing dropped quietly."""

    def test_a_row_missing_its_price_is_skipped_rather_than_defaulted(self):
        rows = [
            {
                "label": "Round 4 synced-table pipeline",
                "dbu_per_hour": "2.4",
                "usd_per_dbu": "0.30",
                "attribution": "DLT rows tagged to this run",
                "grade": "measured",
            },
            {"label": "Databricks App compute", "dbu_per_hour": "1.2"},
        ]
        components = observed_platform_components(rows)
        assert len(components) == 1
        assert components[0].grade == "measured"
        # And a lane built from the shortfall reads as unpriced, not as smaller.
        assert components[0].usd_per_hour == Decimal("2.4") * Decimal("0.30")

    def test_a_component_must_say_where_its_attribution_came_from(self):
        with pytest.raises(ValueError, match="how it was attributed"):
            PlatformComponent(
                label="Databricks App compute",
                dbu_per_hour=Decimal("1.2"),
                usd_per_dbu=POSTED_DBU_USD,
                attribution="   ",
            )


class TestTheManagerRebuildsItOnEveryRead:
    """Why the call site computes this per read rather than once at creation.

    ``as_of`` and the accrued figure beside it are the whole subject of this
    panel. Building the disclosure when the session is created freezes both, so
    a fight card left open for an hour would show an hour-old ``as_of`` and an
    accrued total that stopped accruing -- on a panel whose entire claim is
    elapsed spend. The builder does no I/O, which is what makes rebuilding it
    cheap enough to do that way.
    """

    @staticmethod
    async def _session(**kwargs):
        from server.cost_ledger import InMemoryCostLedgerStore
        from server.manager import RunManager
        from server.models import CompetitorId, Corner, RoundId, SessionCreate

        # The seal the builder actually prices, carried by a manager configured
        # the way app.py configures one: a v7 manifest and the durable ledger
        # that v7 requires alongside it.
        seal = DemoManifest.model_construct(
            manifest_version=7,
            installation_id=INSTALLATION_ID,
            run_id=RUN_ID,
            created_at=ORIGIN,
            aws=SimpleNamespace(account_id="123456789012", region="us-west-2"),
            round_environments={},
        )
        manager = RunManager(
            round_isolation=True,
            installation_id=INSTALLATION_ID,
            cost_ledger_store=InMemoryCostLedgerStore(),
            cost_manifest=seal,
            **kwargs,
        )
        created = await manager.create(
            SessionCreate(
                competitor=CompetitorId.AURORA_SERVERLESS_V2,
                primary_persona="sre",
                corners=[Corner.PERFORMANCE],
                round_id=RoundId.WAKE_IDLE_APP,
            )
        )
        return manager, created

    async def test_the_snapshot_carries_a_disclosure_from_the_moment_it_is_created(self):
        manager, created = await self._session()
        assert created.standing_cost is not None
        assert created.standing_cost.seal_state == "sealed"
        assert len(created.standing_cost.lanes) == 6
        await manager.close()

    async def test_as_of_moves_between_two_reads_of_the_same_session(self):
        manager, created = await self._session()
        first = await manager.get(created.id)
        second = await manager.get(created.id)
        # Pinned at creation, these would be equal and would stay equal for as
        # long as the card stayed open.
        assert second.standing_cost.as_of > first.standing_cost.as_of
        assert second.standing_cost.credits.elapsed_hours >= (
            first.standing_cost.credits.elapsed_hours
        )
        await manager.close()

    async def test_a_read_takes_the_cached_posted_usage_and_never_a_provider(self):
        reads: list[int] = []

        def posted():
            reads.append(1)
            return posted_usage()

        manager, created = await self._session(posted_usage=posted)
        # One at create, one per read: each is a cache lookup, and the callable
        # the manager is given is the cache's accessor rather than the reader.
        before = len(reads)
        snapshot = await manager.get(created.id)
        assert len(reads) == before + 1
        assert snapshot.standing_cost.posted.state == "posted_through_window"
        await manager.close()

    async def test_no_posted_usage_at_all_still_yields_a_renderable_panel(self):
        manager, created = await self._session()
        snapshot = await manager.get(created.id)
        standing = snapshot.standing_cost
        # The state a session opened before the first refresh renders in.
        assert standing.posted.state == "unavailable"
        assert standing.fairness.state == "withheld"
        assert standing.totals is not None
        assert standing.totals.installation.partial is True
        await manager.close()

    async def test_no_drift_report_gives_the_badge_rather_than_a_provider_call(self):
        manager, created = await self._session()
        snapshot = await manager.get(created.id)
        # A disclosure render must not reconcile inline: that would put a
        # describe-* sweep behind every poll of a session.
        assert snapshot.standing_cost.drift.state == "unavailable"
        assert snapshot.standing_cost.drift.badge == "DRIFT NOT READ"
        await manager.close()


# --------------------------------------------------------------------------
# The published cost box, across every document that carries it.
# --------------------------------------------------------------------------
#
# WHY THIS EXISTS. On 2026-08-24 the headline figure moved twice in one night
# and the correction reached one document at a time. A sweep settled README,
# `docs/BOOTSTRAP.md` and `CONTRIBUTING.md` on `$28.93/day`; the next seal
# superseded it; README was corrected and the other two were not. Each file was
# internally coherent and they disagreed with each other, which is the one
# defect no single-file review catches. `CONTRIBUTING.md` was still quoting a
# weekend at the *retired* pipeline rate -- right sentence, dead number.
#
# WHAT IS INVARIANT, and it is deliberately not a dollar amount. These figures
# are a live meter read through a method that has itself been corrected: the
# pipeline lane alone read $15.08/day seven hours before it read $11.07/day,
# and dividing that lane by uptime rather than by the span between its first
# and last posted interval moved the published figure again, to $14.57/day. A
# test pinning cents against a receipt would be red by morning for the wrong
# reason. What does not move is that the documents agree
# *with each other*, and that the totals they state are the arithmetic of the
# components they state.
#
# The plan below is declarative for the reason `_coordination_runtime_grants`
# is: a hand-written list of "these files say $X" is another copy to go stale.
# Nothing here carries a dollar amount. Every figure is read out of the
# documents, and a document that stops stating a quantity it is declared to
# carry fails loudly rather than passing quietly.

#: The tracked documents that publish the standing-cost box. A stranger reads
#: one of these before spending money, so all three must tell the same story.
_COST_DOCUMENTS = ("README.md", "docs/BOOTSTRAP.md", "CONTRIBUTING.md")

#: One entry per quantity, with the phrasings that state it. Alternatives exist
#: because the three documents are prose written for different readers, not
#: three renders of one template -- but each alternative captures the figure
#: rather than asserting it.
_COST_CLAIMS: dict[str, tuple[str, ...]] = {
    # The subtotal and the pipeline-stopped all-in, from the sentence whose
    # whole job is to hold them apart. Opposite meanings, and the gap between
    # them is itself a figure: it read fourteen cents while the pipeline lane
    # was divided by span, and $3.64 once that lane was divided by uptime. The
    # anchor stops before the gap for the reason the docstring above gives --
    # nothing in this plan may carry a dollar amount, and "fourteen cents" was
    # one spelled in words.
    "installation": (
        r"\$([\d.]+) and \$[\d.]+ are two different quantities",
    ),
    "with_platform_stopped": (
        r"\$[\d.]+ and \$([\d.]+) are two different quantities",
        r"or about \$([\d.]+)/day with the pipeline stopped",
    ),
    "with_platform": (
        r"all-in figure is about \$([\d.]+)/day",
        r"expect about \$([\d.]+)/day",
    ),
    "app_compute": (
        r"compute[ ,]+(?:-- )?about \$([\d.]+)/day",
        r"the App's own compute \| ~\$([\d.]+)",
    ),
    "pipeline": (
        r"\$([\d.]+) is the Round 4",
        r"Round 4 reverse-ETL pipeline \| ~\$([\d.]+)",
    ),
    # "that pipeline" is the Round 4 one just named, so this is the subtotal;
    # "the pipeline" above belongs to the all-in. The two read almost alike and
    # mean opposite things, which is the whole reason both are pinned.
    "installation_stopped": (
        r"\$([\d.]+)/day with that pipeline stopped",
        r"takes those two figures to about \$([\d.]+)/day",
    ),
}

#: Totals are the sum of the parts, in the documents as in the disclosure. Read
#: as ``left`` minus/plus ``right`` equals ``result``.
_COST_IDENTITIES = (
    ("installation", "-", "pipeline", "installation_stopped"),
    ("installation", "+", "app_compute", "with_platform"),
    ("with_platform", "-", "pipeline", "with_platform_stopped"),
)

#: The AWS half is rate-card arithmetic with no invoice behind it, and the
#: receipt agrees: every AWS component is ``provenance: assumed`` while every
#: Databricks component is ``measured``. A revision that makes the number more
#: accurate and drops this is a regression, so each document must keep saying it.
_AWS_UNEVIDENCED_PHRASE = "no invoice has ever confirmed"

#: The other two documents that describe where the cost evidence comes from.
#: They are deliberately *not* in `_COST_DOCUMENTS`: neither states the headline
#: totals, and `docs/PRICING.md` says in so many words that it quotes none, so
#: adding it there would fail `test_every_document_states_the_whole_headline` by
#: design. That left both of them outside every check in this section -- the two
#: files that go into the most detail about provenance were the two nothing
#: guarded. This tuple is the caveat check's reach, which is the part that
#: applies to a document whether or not it states a total.
_EVIDENCE_DOCUMENTS = (*_COST_DOCUMENTS, "docs/PRICING.md", "PRICING_DISCOVERY.md")

#: Recognised ways of saying the AWS half has never been checked against a bill.
#: Alternatives for the same reason `_COST_CLAIMS` has them: five documents
#: written for five different readers, one claim. A document satisfying any one
#: of these has not quietly promoted the estimate to evidence.
_AWS_UNEVIDENCED_PHRASES = (
    _AWS_UNEVIDENCED_PHRASE,
    "has never met an invoice",
    "reconciled against an invoice",
    "reconciled against a bill",
)

#: Which grade each cloud's components carry, in the receipts and therefore in
#: any document that reproduces them.
#:
#: Recorded in this direction because the *inversion* is a live hazard rather
#: than a hypothetical one: it has been written down backwards more than once in
#: this project's own working notes -- AWS as the invoice-evidenced half and
#: Databricks as the unevidenced one -- and a brief carrying that error would
#: have an editor "correct" the documents into claiming evidence that does not
#: exist. `ce:GetCostAndUsage` is denied to this installation, so AWS has no
#: posted counterpart at all; Databricks is posted provider records. Nothing in
#: `_COST_CLAIMS` or the caveat check can see a table that swaps the two, because
#: every figure and every phrase would still be present and correct.
_PROVENANCE_BY_CLOUD = {"aws": "assumed", "databricks": "measured"}

#: The document that tabulates provenance per component, and so the one where a
#: swap would be both invisible to every other check here and read as fact.
_PROVENANCE_DOCUMENT = "docs/PRICING.md"

#: Words that make a cost figure conditional rather than standing. Deliberately
#: broad: the check below is not grading the prose, it is asking whether *any*
#: condition travels with the number in the place a reader meets it.
_CONDITION_PHRASES = (
    "while",
    "if left",
    "left up",
    "left running",
    "until",
    "only",
    "per hour",
    "/hour",
)


def _unconditional_pipeline_rows(text: str) -> list[str]:
    """Table rows stating the Round 4 pipeline's cost with no condition attached.

    WHY THIS IS ROW-SCOPED AND NOT DOCUMENT-SCOPED. Everything else in this
    section reads a document as one flattened string, which is right for
    checking that figures agree and wrong for checking how one is framed: the
    README that shipped this defect *did* say "the receipt captured the pipeline
    while it was running", three table rows below the row that presented the
    pipeline's per-day figure in a column headed "Approximate cost per day".
    Flattened, the
    caveat and the number are neighbours. To a reader they are not. A table row
    is the whole of the context a number in a table gets.
    """

    rows: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        flat = stripped.replace("`", "").replace("*", "")
        if not re.search(r"Round 4[^|]*pipeline", flat):
            continue
        if not re.search(r"\$\d", flat):
            continue
        if any(phrase in flat.lower() for phrase in _CONDITION_PHRASES):
            continue
        rows.append(stripped)
    return rows


def _provenance_pairings(text: str) -> list[tuple[str, str]]:
    """Every ``(cloud, grade)`` a markdown table in `text` states on one row.

    Pure, so the guard can be shown to fire on a swapped table rather than only
    to pass on this tree. Cells are matched whole: `Databricks platform` is a
    lane name and not a cloud, and only a row naming exactly one of each is a
    pairing to check -- which skips headers, rules, and the rate-source table.
    """

    grades = set(_PROVENANCE_BY_CLOUD.values())
    found: list[tuple[str, str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [
            cell.replace("`", "").replace("*", "").strip().lower()
            for cell in stripped.strip("|").split("|")
        ]
        clouds = [cell for cell in cells if cell in _PROVENANCE_BY_CLOUD]
        stated = [cell for cell in cells if cell in grades]
        if len(clouds) == 1 and len(stated) == 1:
            found.append((clouds[0], stated[0]))
    return found


def _normalised_cost_prose(name: str) -> str:
    """A document with its markup flattened, so one regex reads all three.

    Blockquote markers, bold runs, inline-code ticks and hard line wraps are
    typography rather than content, and a claim that spans a wrapped line is
    exactly the shape the last sweep's line-at-a-time scan missed.
    """

    from server.manifest import PROJECT_ROOT

    text = (PROJECT_ROOT / name).read_text(encoding="utf-8")
    text = re.sub(r"^\s*>\s?", " ", text, flags=re.MULTILINE)
    text = text.replace("**", "").replace("`", "").replace("\u2014", "--")
    return re.sub(r"\s+", " ", text)


def _stated_cost_figures(name: str) -> dict[str, Decimal]:
    """Every headline figure a document states, keyed by what it means.

    Raises rather than skipping when one phrasing of a quantity disagrees with
    another *inside the same file*: a half-applied correction is the failure
    this exists to catch, and it looks exactly like that.
    """

    prose = _normalised_cost_prose(name)
    found: dict[str, Decimal] = {}
    for claim, patterns in _COST_CLAIMS.items():
        hits = {
            Decimal(match)
            for pattern in patterns
            for match in re.findall(pattern, prose)
        }
        if not hits:
            continue
        assert len(hits) == 1, (
            f"{name} states {claim} as {sorted(hits)} -- one of these is a "
            "correction that was not carried through the whole file"
        )
        found[claim] = hits.pop()
    return found


class TestThePublishedCostBoxAgrees:
    """Three documents, one bill. They drifted apart once; not silently again."""

    def test_every_document_states_the_whole_headline(self):
        # Not a completeness nicety. `CONTRIBUTING.md` and `docs/BOOTSTRAP.md`
        # both quoted the subtotal and the all-in while saying nothing about
        # the all-in with the pipeline stopped -- so a reader who stopped the
        # pipeline had the subtotal and the all-in on the page and no way to
        # reach the figure that was actually theirs.
        for name in _COST_DOCUMENTS:
            stated = _stated_cost_figures(name)
            missing = sorted(set(_COST_CLAIMS) - set(stated))
            assert not missing, (
                f"{name} no longer states {missing}. Either the figure was "
                "dropped or the sentence was reworded past _COST_CLAIMS; both "
                "need a human, because this file is published."
            )

    def test_no_two_documents_quote_different_figures(self):
        per_claim: dict[str, dict[str, Decimal]] = {}
        for name in _COST_DOCUMENTS:
            for claim, value in _stated_cost_figures(name).items():
                per_claim.setdefault(claim, {})[name] = value
        for claim, by_document in sorted(per_claim.items()):
            assert len(set(by_document.values())) == 1, (
                f"{claim} disagrees across the published documents: "
                f"{by_document}. One of them was corrected and the rest were "
                "not, which is how $28.93/day outlived the seal it came from."
            )

    def test_the_totals_are_the_arithmetic_of_the_parts(self):
        # The check that survives the meter moving. Whatever tonight's pipeline
        # rate is, the stopped figures are the running ones less that rate and
        # the all-in is the subtotal plus the App lane -- so a figure edited on
        # its own, without its dependents, lands here instead of on a stranger.
        for name in _COST_DOCUMENTS:
            stated = _stated_cost_figures(name)
            for left, operator, right, result in _COST_IDENTITIES:
                if not {left, right, result} <= set(stated):
                    continue
                expected = (
                    stated[left] - stated[right]
                    if operator == "-"
                    else stated[left] + stated[right]
                )
                assert abs(expected - stated[result]) <= Decimal("0.01"), (
                    f"{name}: {left} {operator} {right} is {expected}, but the "
                    f"document states {result} as {stated[result]}"
                )

    def test_all_three_cite_one_receipt_and_it_is_not_a_live_identifier(self):
        # Every row is claimed to trace to a sealed receipt, so the citation is
        # load-bearing: three documents citing three seals cannot all be the
        # provenance of one table. A short code is publishable; the 32-hex
        # session ids and UUIDs beside it in the same payload are not, and this
        # shape check is what keeps a paste of the wrong one out of the tree.
        # Scoped to the provenance sentence: these documents cite other seals
        # elsewhere for other claims, and only this one backs the cost box.
        cited: dict[str, set[str]] = {}
        for name in _COST_DOCUMENTS:
            cited[name] = set(
                re.findall(
                    r"traces to one sealed receipt[^.]*?receipt ([0-9A-F]{8})\b",
                    _normalised_cost_prose(name),
                )
            )
            assert len(cited[name]) == 1, f"{name} cites {sorted(cited[name])}"
        codes = set().union(*cited.values())
        assert len(codes) == 1, f"the published documents cite {sorted(codes)}"

    def test_the_aws_half_is_never_quietly_promoted_to_evidence(self):
        for name in _COST_DOCUMENTS:
            assert _AWS_UNEVIDENCED_PHRASE in _normalised_cost_prose(name), (
                f"{name} dropped the caveat that the AWS half has no invoice "
                "behind it. The error bar runs upward and the reader is about "
                "to spend money on the strength of this box."
            )


class TestWhichHalfHasMetABill:
    """Two documents this section could not see, and the one swap it cannot see.

    Everything above checks the *figures*: that five documents agree on them and
    that the totals are the arithmetic of their parts. None of it can see a
    document that keeps every figure and describes the wrong half as evidenced,
    and two of the five documents were outside the caveat check entirely.
    """

    def test_every_document_describing_the_evidence_keeps_the_caveat(self):
        # `docs/PRICING.md` and `PRICING_DISCOVERY.md` say the most about
        # provenance and were checked by nothing, because neither quotes a total
        # and the whole section keyed off the documents that do.
        for name in _EVIDENCE_DOCUMENTS:
            prose = _normalised_cost_prose(name)
            assert any(phrase in prose for phrase in _AWS_UNEVIDENCED_PHRASES), (
                f"{name} no longer states, in any recognised phrasing, that the "
                "AWS half has never been checked against a bill. Either the "
                "caveat was dropped or it was reworded past "
                "_AWS_UNEVIDENCED_PHRASES; both need a human."
            )

    def test_the_provenance_table_never_swaps_the_two_halves(self):
        from server.manifest import PROJECT_ROOT

        pairings = _provenance_pairings(
            (PROJECT_ROOT / _PROVENANCE_DOCUMENT).read_text(encoding="utf-8")
        )
        # Sixteen component rows plus the two-halves summary at the top of the
        # page. Asserted as a floor rather than an equality so that adding a
        # component does not fail this, but deleting the tables cannot make it
        # pass vacuously.
        assert len(pairings) >= 14, (
            f"{_PROVENANCE_DOCUMENT} states {len(pairings)} cloud/provenance "
            "pairings; the provenance tables appear to have been removed or "
            "restructured, which this guard cannot then check"
        )
        for cloud, grade in pairings:
            assert grade == _PROVENANCE_BY_CLOUD[cloud], (
                f"{_PROVENANCE_DOCUMENT} describes a {cloud} component as "
                f"{grade!r}. The receipt says {_PROVENANCE_BY_CLOUD[cloud]!r}: "
                "ce:GetCostAndUsage is denied, so AWS has no posted counterpart "
                "and Databricks is posted provider records. Calling the AWS half "
                "measured claims evidence this repository has never had."
            )
        # Both halves are really present, so the loop above is not passing on a
        # table that happens to name only one cloud.
        assert {cloud for cloud, _ in pairings} == set(_PROVENANCE_BY_CLOUD)
        assert {grade for _, grade in pairings} == set(_PROVENANCE_BY_CLOUD.values())

    def test_the_swap_this_guard_exists_for_is_actually_detected(self):
        # The inversion, planted. Without this the guard above is indistinguish-
        # able from one that cannot fail, which is the defect this file's own
        # docstring warns about.
        swapped = (
            "| Half | Source | Provenance | Has it met a bill? |\n"
            "| --- | --- | --- | --- |\n"
            "| Databricks | system.billing.usage | `assumed` | No. Never. |\n"
            "| AWS | Price List API | `measured` | Yes |\n"
        )
        assert _provenance_pairings(swapped) == [
            ("databricks", "assumed"),
            ("aws", "measured"),
        ]
        for cloud, grade in _provenance_pairings(swapped):
            assert grade != _PROVENANCE_BY_CLOUD[cloud]

        # And it stays quiet on the shapes that are correct or irrelevant: the
        # right pairing, a lane name that merely starts with a cloud's name, and
        # a row naming neither.
        assert _provenance_pairings(
            "| Databricks platform | App compute | Databricks | measured | 1 | 2 |\n"
            "| RDS | instances | AWS | assumed | 3 | 4 |\n"
            "| Amazon VPC | 2026-07-24T15:42:25Z | [offer file](https://example.com) |\n"
        ) == [("databricks", "measured"), ("aws", "assumed")]


class TestTheConditionTravelsWithTheFigure:
    """The one defect every check above agrees on and none of them can see.

    Those checks ask whether the documents state the same figures and whether
    the totals are the arithmetic of their parts. Both were true of the table
    that shipped on 2026-08-25, which listed a pipeline that runs for the
    minutes of a bout in the same "Approximate cost per day" column as three
    Aurora clusters that bill around the clock. Consistent, arithmetically
    sound, and wrong about the only thing a stranger reads it for -- and a
    guard built on document-to-document agreement cannot notice that all the
    documents are misframed the same way.

    The Round 4 line is the one worth pinning because it is the only figure in
    the box whose default value is zero: the installation's AWS half really is
    standing, the App's compute really does bill until it is stopped, and the
    pipeline really is released twenty minutes after a bout settles.
    """

    def test_no_table_presents_the_pipeline_as_an_unconditional_daily_cost(self):
        from server.manifest import PROJECT_ROOT

        for name in _EVIDENCE_DOCUMENTS:
            offenders = _unconditional_pipeline_rows(
                (PROJECT_ROOT / name).read_text(encoding="utf-8")
            )
            assert not offenders, (
                f"{name} states the Round 4 pipeline's cost in a table row that "
                f"carries no condition: {offenders}. That line bills only while "
                "the pipeline is up, and a bout releases it after 20 minutes, so "
                "a bare figure in a per-day column overstates the default "
                "install by the largest single line in it."
            )

    def test_the_row_this_guard_exists_for_is_actually_detected(self):
        # The exact row that was published, planted. Without this the check
        # above is indistinguishable from one that cannot fail.
        assert _unconditional_pipeline_rows(
            "| Cost item | Approximate cost per day |\n"
            "| --- | ---: |\n"
            "| AWS databases, runner, storage, addresses, and secrets | **~$8.36** |\n"
            "| Round 4 reverse-ETL pipeline | **~$14.57** |\n"
            "| Installation subtotal | **~$22.93** |\n"
        ) == ["| Round 4 reverse-ETL pipeline | **~$14.57** |"]

        # And it stays quiet on the shapes that are honest or irrelevant: the
        # row that replaced it, a rate stated per hour, the leak stated as a
        # leak, and a pipeline row carrying no money at all.
        assert not _unconditional_pipeline_rows(
            "| Round 4 reverse-ETL pipeline | Only while it is actually up | **~$0.61/hour** |\n"
            "| Round 4 reverse-ETL pipeline | ~$14.57/day while running |\n"
            "| Round 4 reverse-ETL pipeline | ~$14.57 if left up for a full day |\n"
            "| Round 4 | 2 verified Lakebase bouts; no AWS lane was timed |\n"
        )
