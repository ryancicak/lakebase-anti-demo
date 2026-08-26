import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import server.cost_model as cost_model_module
import server.pricing as pricing_module
from server.capacity import (
    AURORA_AUTO_PAUSE_SECONDS,
    LAKEBASE_SUSPEND_SECONDS,
    RDS_CLASS_MEMORY_GIB,
    RDS_INSTANCE_CLASS,
)
from server.cost_model import (
    ACU_DESCENT_FLOOR_SECONDS,
    ACU_SAMPLE_LEAD_SECONDS,
    ACU_SAMPLE_TAIL_SECONDS,
    AS_RUN_RDS_INSTANCE_CLASS,
    AURORA_MIN_RUNNING_ACU,
    CUSTOMER_EQUIVALENT_FLOOR_REASON,
    IMPUTED_AURORA_ROUNDS,
    IMPUTED_RDS_ROUNDS,
    LAKEBASE_CEILING_CU,
    LAKEBASE_DBU_PER_CU_HOUR,
    LAKEBASE_NODE_DBU_PER_HOUR,
    RDS_MINIMUM_BILLED_SECONDS,
    SECONDS_PER_BILLING_MONTH,
    SECONDS_PER_HOUR,
    TERRAFORM_PROXY_SECRETS,
    UNPRICED_PIPELINE_SERVICES,
    V7_CONNECTION_SPIKE_SAMPLES,
    V7_MEASURED_AURORA_ACU_SECONDS,
    V7_RESTORE_SAMPLES,
    AuroraAcuMeasurement,
    BoutTelemetry,
    BurnRate,
    CarryingWindow,
    Cloud,
    CostKind,
    CostLine,
    CustomerEquivalent,
    EstimateScope,
    HeldOutPrediction,
    IdleContrastLane,
    InstallationShape,
    LakebaseBurnModel,
    PricingBasis,
    Provenance,
    Quantity,
    Rate,
    RateCard,
    Reconciliation,
    ReconciliationReport,
    acu_sampling_window,
    aurora_acu_seconds_for,
    aurora_ceiling_acu_hours,
    aurora_wake_commitment_acu_hours,
    calibrate_from_samples,
    calibrate_lakebase_burn,
    customer_equivalent_carrying_cost,
    estimate_bout_cost,
    estimate_carrying_cost,
    idle_contrast,
    imputed_round_carrying_lines,
    imputed_total_usd,
    integrate_acu_seconds,
    leave_one_out,
    telemetry_from_snapshot,
    v7_lakebase_burn_model,
)
from server.models import CompetitorId, RoundId
from server.pricing import (
    CONFIGURED_RDS_INSTANCE_CLASS,
    RDS_INSTANCE_HOUR_PRICES,
    RDS_PROXY_UNIT_RATE_USD,
    UnknownRdsInstanceClassError,
    build_cost_receipt,
    calculate_rds_proxy_cost,
    rds_instance_hour_usd,
)


def _telemetry(
    round_id: RoundId,
    competitor_id: CompetitorId = CompetitorId.RDS_POSTGRES,
    **overrides: object,
) -> BoutTelemetry:
    defaults: dict[str, object] = {
        "bout_seconds": Decimal("812"),
        "lakebase_lane_seconds": Decimal("17.43"),
        "competitor_lane_seconds": Decimal("811.31"),
    }
    defaults.update(overrides)
    return BoutTelemetry(round_id=round_id, competitor_id=competitor_id, **defaults)  # type: ignore[arg-type]


_RESTORE_ROUNDS = (RoundId.MAKE_SCHEMA_CHANGE_SAFELY, RoundId.RECOVER_DELETED_ORDER)


def _restore_burn(
    samples: list[tuple[Decimal, Decimal]] | None = None,
) -> BurnRate:
    """A burn rate whose support is the restore rounds, as every real one is."""

    return calibrate_lakebase_burn(
        samples or [(Decimal("0.5"), Decimal("1000"))],
        rounds=_RESTORE_ROUNDS,
    )


class TestQuantity:
    def test_what_the_constructor_refuses(self) -> None:
        """Every rejection this type makes, one row each.

        These were a test apiece, all of the same shape: build one bad Quantity
        and name the message. The `match` is what distinguishes them, so it is
        the row identifier as well as the assertion.
        """

        cases: tuple[tuple[str, dict], ...] = (
            (
                "must contain its point",
                dict(
                    point=Decimal(5),
                    low=Decimal(6),
                    high=Decimal(7),
                    provenance=Provenance.MODELED,
                    basis="test",
                ),
            ),
            (
                "cannot carry a band",
                dict(
                    point=None,
                    low=Decimal(1),
                    high=Decimal(2),
                    provenance=Provenance.UNAVAILABLE,
                    basis="test",
                ),
            ),
            (
                "cannot carry a point value",
                dict(
                    point=Decimal(1),
                    low=Decimal(1),
                    high=Decimal(1),
                    provenance=Provenance.UNAVAILABLE,
                    basis="test",
                ),
            ),
        )

        for match, kwargs in cases:
            with pytest.raises(ValueError, match=match):
                Quantity(**kwargs)

        # `exact` takes a different signature, so it cannot ride the table, but
        # it enforces the same rule: a figure without its derivation is not a
        # figure this model will carry.
        with pytest.raises(ValueError, match="basis"):
            Quantity.exact(Decimal(1), provenance=Provenance.MEASURED, basis="  ")

    def test_unavailable_is_not_zero(self) -> None:
        quantity = Quantity.unavailable("no evidence")
        assert quantity.point is None
        assert quantity.provenance is Provenance.UNAVAILABLE


class TestRate:
    def test_what_the_constructor_refuses(self) -> None:
        with pytest.raises(ValueError, match="unit and its source"):
            Rate(Decimal("0.1"), "DBU", "")
        with pytest.raises(ValueError, match="non-negative"):
            Rate(Decimal("-1"), "DBU", "source")


class TestRateCardAgreesWithPricingModule:
    """The rate card must not drift from the receipt the customer is shown."""

    def test_rds_proxy_rate_matches_pricing(self) -> None:
        assert RateCard().rds_proxy_capacity_hour.usd == Decimal(str(RDS_PROXY_UNIT_RATE_USD))

    @pytest.mark.parametrize(
        ("attribute", "expected"),
        [
            ("lakebase_dbu", "0.26"),
            ("lakebase_dsu", "0.023"),
            ("rds_gp3_gb_month", "0.115"),
            ("rds_backup_gb_month", "0.095"),
            ("aurora_acu_hour", "0.12"),
            ("aurora_storage_gb_month", "0.10"),
            ("ec2_m6i_large_hour", "0.096"),
            ("ebs_gp3_gb_month", "0.08"),
            ("public_ipv4_hour", "0.005"),
            ("secret_month", "0.40"),
        ],
    )
    def test_published_rate_is_carried_verbatim(self, attribute: str, expected: str) -> None:
        assert getattr(RateCard(), attribute).usd == Decimal(expected)


def _baseline_line(estimate):
    return next(line for line in estimate.lines if "baseline instances" in line.component)


def _restore_line(estimate):
    return next(line for line in estimate.lines if "PITR restore compute" in line.component)


_A_DAY = CarryingWindow(seconds=Decimal(86400))


class TestTheRdsRateIsDerivedFromTheConfiguredClass:
    """The rate, the label and capacity parity must move on one edit.

    Round 5's RDS lane was resized from db.t4g.micro to db.t4g.medium and the
    cost model kept charging the micro rate, because the class name and its price
    had been written out by hand in three separate places.  Everything below
    exists so that the class can only be stated once.
    """

    def test_the_card_reads_the_class_from_the_capacity_module(self) -> None:
        assert CONFIGURED_RDS_INSTANCE_CLASS == RDS_INSTANCE_CLASS
        assert RateCard().rds_instance_class == RDS_INSTANCE_CLASS

    def test_the_two_named_classes_carry_their_published_sku(self) -> None:
        micro = RDS_INSTANCE_HOUR_PRICES["db.t4g.micro"]
        medium = RDS_INSTANCE_HOUR_PRICES["db.t4g.medium"]
        assert (micro.usd_per_hour, micro.sku) == (Decimal("0.016"), "CT79XNCJJGH56FA8")
        assert (medium.usd_per_hour, medium.sku) == (Decimal("0.065"), "N2BHMKBGM78G338C")

    def test_every_class_capacity_approves_can_also_be_priced(self) -> None:
        # A class parity would accept but cost could not price would fail loudly
        # at the worst possible moment, so the two tables cover the same set.
        assert set(RDS_INSTANCE_HOUR_PRICES) == set(RDS_CLASS_MEMORY_GIB)

    @pytest.mark.parametrize("instance_class", sorted(RDS_INSTANCE_HOUR_PRICES))
    def test_changing_the_class_moves_the_rate_and_every_label_together(
        self,
        instance_class: str,
    ) -> None:
        rates = RateCard(rds_instance_class=instance_class)
        published = RDS_INSTANCE_HOUR_PRICES[instance_class]
        assert rates.rds_instance_hour.usd == published.usd_per_hour

        carrying = _baseline_line(estimate_carrying_cost(_A_DAY, rates=rates))
        restore = _restore_line(
            estimate_bout_cost(_telemetry(RoundId.RECOVER_DELETED_ORDER), rates=rates)
        )
        wake = next(
            line
            for line in estimate_bout_cost(_telemetry(RoundId.WAKE_IDLE_APP), rates=rates).lines
            if "wake" in line.component
        )

        others = set(RDS_INSTANCE_HOUR_PRICES) - {instance_class}
        for line in (carrying, restore):
            assert instance_class in line.component
            assert not any(other in line.component for other in others)
        for line in (carrying, restore, wake):
            assert line.rate.usd == published.usd_per_hour
            assert published.sku in line.rate.source

    def test_the_receipt_shown_to_a_customer_derives_from_the_same_class(self) -> None:
        receipt = build_cost_receipt(RoundId.WAKE_IDLE_APP, CompetitorId.RDS_POSTGRES)
        compute = next(
            line
            for line in receipt.lines
            if line.component.startswith("RDS PostgreSQL") and "compute" in line.component
        )
        assert CONFIGURED_RDS_INSTANCE_CLASS in compute.component
        assert compute.unit_rate_usd == float(rds_instance_hour_usd(CONFIGURED_RDS_INSTANCE_CLASS))

    def test_an_unrecognised_class_fails_loudly_rather_than_defaulting(self) -> None:
        card = RateCard(rds_instance_class="db.t4g.nano")
        with pytest.raises(UnknownRdsInstanceClassError) as excinfo:
            _ = card.rds_instance_hour
        message = str(excinfo.value)
        assert "db.t4g.nano" in message
        assert "RDS_INSTANCE_HOUR_PRICES" in message

    def test_an_unpriceable_class_stops_the_estimate_instead_of_zeroing_it(self) -> None:
        # A wrong-but-plausible dollar figure is the worst outcome available; an
        # estimate that refuses to be produced is recoverable.
        with pytest.raises(UnknownRdsInstanceClassError):
            estimate_carrying_cost(_A_DAY, rates=RateCard(rds_instance_class="db.r6g.xlarge"))


class TestTheClassIsNamedOnlyOnce:
    """Guards the shape of the fix, not just its current output."""

    @staticmethod
    def _class_literals(module) -> list[str]:
        lines = Path(module.__file__).read_text().splitlines()
        return [
            stripped
            for stripped in (line.strip() for line in lines)
            if not stripped.startswith("#")
            if "db.t4g." in stripped or "db.m6g." in stripped
        ]

    def test_the_cost_model_spells_a_class_out_only_to_record_history(self) -> None:
        assert self._class_literals(cost_model_module) == [
            'AS_RUN_RDS_INSTANCE_CLASS = "db.t4g.micro"'
        ]

    def test_pricing_spells_a_class_out_only_inside_the_rate_table(self) -> None:
        literals = self._class_literals(pricing_module)
        assert len(literals) == len(RDS_INSTANCE_HOUR_PRICES)
        assert all(line.startswith('"db.') and "RdsInstancePrice(" in line for line in literals)


class TestHistoryIsNotRepricedByTheResize:
    """The resize has landed, and it does not reach backwards.

    Every figure this repository has already measured, posted or published was
    metered before four ``ModifyDBInstance`` calls took the fleet to
    db.t4g.medium on 2026-08-21 at 14:48:36Z, so all of it was recorded on
    db.t4g.micro.  Restating any of it at the medium rate would invent history,
    which is why the as-run basis stays pinned to micro even though micro is no
    longer what stands.  That constant is a record of what ran, and these tests
    keep it from being read as a claim about what is running -- the mistake the
    standing-cost lane made until the observed class replaced it there.
    """

    def test_the_as_run_class_records_what_the_measured_figures_ran_on(self) -> None:
        assert AS_RUN_RDS_INSTANCE_CLASS == "db.t4g.micro"
        assert RateCard.for_basis(PricingBasis.AS_RUN).rds_instance_hour.usd == Decimal("0.016")

    def test_an_estimate_is_forward_looking_unless_asked_otherwise(self) -> None:
        assert RateCard().rds_instance_class == CONFIGURED_RDS_INSTANCE_CLASS
        assert (
            RateCard.for_basis(PricingBasis.CONFIGURED).rds_instance_class
            == CONFIGURED_RDS_INSTANCE_CLASS
        )

    def test_a_window_that_ran_on_micro_still_prices_at_the_micro_rate(self) -> None:
        as_run = _baseline_line(
            estimate_carrying_cost(_A_DAY, rates=RateCard.for_basis(PricingBasis.AS_RUN))
        )
        assert as_run.rate.usd == Decimal("0.016")
        assert (
            as_run.usd
            == Decimal(24) * Decimal(InstallationShape().rds_instances) * Decimal("0.016")
        )
        assert "db.t4g.micro" in as_run.component

    def test_the_two_bases_do_not_collapse_into_one_number(self) -> None:
        forward = _baseline_line(estimate_carrying_cost(_A_DAY))
        as_run = _baseline_line(
            estimate_carrying_cost(_A_DAY, rates=RateCard.for_basis(PricingBasis.AS_RUN))
        )
        if CONFIGURED_RDS_INSTANCE_CLASS != AS_RUN_RDS_INSTANCE_CLASS:
            assert forward.usd != as_run.usd
            assert forward.component != as_run.component

    def test_the_published_standing_cost_delta_of_the_resize(self) -> None:
        """+$1.176 per instance-day, against a fleet that has since shrunk.

        Past tense as of 2026-08-21T14:48:36Z: this is what the AWS standing
        cost rose by when the fleet was modified in place, not what it would
        rise by if a pending diff were applied.  Four instances stood at that
        moment, so the resize cost +$4.704/day when it landed; Round 1's
        instance was deleted afterwards, so the fleet carrying the higher rate
        today is three boxes and +$3.528/day.  Both are true of different
        moments and neither may be quoted as the other.
        """

        per_hour = rds_instance_hour_usd("db.t4g.medium") - rds_instance_hour_usd("db.t4g.micro")
        assert per_hour == Decimal("0.049")
        assert per_hour * 24 == Decimal("1.176")
        # Four per-round RDS instances at the moment of the resize (r1, r2, r3,
        # r5), confirmed against `aws rds describe-db-instances` at the time.
        at_resize = InstallationShape().with_r1_rds_instance()
        assert at_resize.rds_instances == 4
        assert per_hour * 24 * at_resize.rds_instances == Decimal("4.704")
        # And what the fleet carries now that r1's box is gone.
        assert InstallationShape().rds_instances == 3
        assert per_hour * 24 * InstallationShape().rds_instances == Decimal("3.528")


class TestTelemetryValidation:
    def test_what_the_constructor_refuses(self) -> None:
        cases: tuple[tuple[str, dict], ...] = (
            ("positive duration", dict(bout_seconds=Decimal(0))),
            (
                "non-negative",
                dict(bout_seconds=Decimal(10), competitor_lane_seconds=Decimal(-1)),
            ),
        )
        for match, overrides in cases:
            with pytest.raises(ValueError, match=match):
                BoutTelemetry(
                    round_id=RoundId.WAKE_IDLE_APP,
                    competitor_id=CompetitorId.RDS_POSTGRES,
                    **overrides,
                )


class TestRestoreRounds:
    def test_rds_restore_is_priced_from_the_lane_clock(self) -> None:
        telemetry = _telemetry(RoundId.RECOVER_DELETED_ORDER)
        estimate = estimate_bout_cost(telemetry)
        compute = next(line for line in estimate.lines if "PITR restore compute" in line.component)
        expected_hours = Decimal("811.31") / SECONDS_PER_HOUR
        assert compute.quantity.point == expected_hours
        assert compute.usd == expected_hours * RateCard().rds_instance_hour.usd
        assert compute.quantity.provenance is Provenance.MODELED

    def test_a_provider_observed_lifetime_wins_and_removes_the_band(self) -> None:
        telemetry = _telemetry(
            RoundId.RECOVER_DELETED_ORDER,
            observed_restore_lifetime_seconds=Decimal("905"),
        )
        estimate = estimate_bout_cost(telemetry)
        compute = next(line for line in estimate.lines if "PITR restore compute" in line.component)
        assert compute.quantity.provenance is Provenance.MEASURED
        assert compute.quantity.low == compute.quantity.high == compute.quantity.point
        assert compute.quantity.point == Decimal("905") / SECONDS_PER_HOUR

    def test_the_ten_minute_minimum_floors_a_short_restore(self) -> None:
        telemetry = _telemetry(
            RoundId.RECOVER_DELETED_ORDER,
            bout_seconds=Decimal("124"),
            competitor_lane_seconds=Decimal("123.1"),
        )
        estimate = estimate_bout_cost(telemetry)
        compute = next(line for line in estimate.lines if "PITR restore compute" in line.component)
        assert compute.quantity.point == RDS_MINIMUM_BILLED_SECONDS / SECONDS_PER_HOUR

    def test_a_short_restore_costs_the_same_as_a_ten_minute_one(self) -> None:
        short = estimate_bout_cost(
            _telemetry(
                RoundId.RECOVER_DELETED_ORDER,
                bout_seconds=Decimal("124"),
                competitor_lane_seconds=Decimal("123.1"),
            )
        )
        floor = estimate_bout_cost(
            _telemetry(
                RoundId.RECOVER_DELETED_ORDER,
                bout_seconds=Decimal("601"),
                competitor_lane_seconds=Decimal("600"),
            )
        )
        short_compute = next(
            line for line in short.lines if "PITR restore compute" in line.component
        )
        floor_compute = next(
            line for line in floor.lines if "PITR restore compute" in line.component
        )
        assert short_compute.usd == floor_compute.usd

    def test_restore_storage_is_prorated_across_a_730_hour_month(self) -> None:
        telemetry = _telemetry(RoundId.RECOVER_DELETED_ORDER)
        estimate = estimate_bout_cost(telemetry)
        storage = next(line for line in estimate.lines if line.kind is CostKind.STORAGE)
        expected = Decimal("811.31") * Decimal(20) / SECONDS_PER_BILLING_MONTH
        assert storage.quantity.point == expected

    def test_an_aurora_restore_without_samples_is_unavailable_not_bounded(self) -> None:
        # This used to assert `lane x 2 ACU` with a floor of zero. CloudWatch
        # settled that convention on 2026-08-21: it came out 1.73x *low* across
        # Rounds 1-3, because Aurora bills a descent no lane clock covers.
        telemetry = _telemetry(
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=Decimal("508.9"),
        )
        estimate = estimate_bout_cost(telemetry)
        compute = next(line for line in estimate.lines if line.cloud is Cloud.AWS)
        assert compute.quantity.provenance is Provenance.UNAVAILABLE
        assert compute.usd is None

    def test_measured_acu_seconds_are_converted_to_acu_hours(self) -> None:
        telemetry = _telemetry(
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            CompetitorId.AURORA_SERVERLESS_V2,
            observed_acu_seconds_above_floor=Decimal("720"),
        )
        estimate = estimate_bout_cost(telemetry)
        compute = next(line for line in estimate.lines if line.cloud is Cloud.AWS)
        assert compute.quantity.point == Decimal("0.2")
        assert compute.quantity.provenance is Provenance.MEASURED

    def test_the_upper_bound_allows_for_teardown(self) -> None:
        telemetry = _telemetry(RoundId.RECOVER_DELETED_ORDER)
        estimate = estimate_bout_cost(telemetry)
        low, high = estimate.band_usd()
        assert low < high
        assert low == estimate.total_usd()


class TestRoundFiveProxy:
    @pytest.mark.parametrize(
        ("competitor", "seconds"),
        [
            (CompetitorId.RDS_POSTGRES, Decimal("792.6")),
            (CompetitorId.AURORA_SERVERLESS_V2, Decimal("1296")),
            (CompetitorId.RDS_POSTGRES, Decimal("120")),
        ],
    )
    def test_proxy_cost_matches_the_published_receipt_formula(
        self,
        competitor: CompetitorId,
        seconds: Decimal,
    ) -> None:
        telemetry = _telemetry(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            competitor,
            observed_proxy_lifetime_seconds=seconds,
        )
        estimate = estimate_bout_cost(telemetry)
        proxy = next(line for line in estimate.lines if line.component.startswith("RDS Proxy"))
        _, expected_usd = calculate_rds_proxy_cost(competitor, float(seconds))
        assert proxy.usd is not None
        assert proxy.usd == pytest.approx(Decimal(str(expected_usd)), rel=Decimal("1e-9"))


class TestAcuSamplesReplaceTheCeilingConvention:
    """Aurora was priced at `lane elapsed x the full 2 ACU ceiling` with a floor
    of zero, which the model's own comment called "the conservative (higher)
    reading". CloudWatch measured it on 2026-08-21 and the sign was backwards:
    1.73x *low* across Rounds 1-3 and 16.1x low on Round 1, because the ceiling
    was applied to a window that excludes the billed auto-pause descent. Real
    ACU-seconds are now the only thing that prices this line.
    """

    @staticmethod
    def _points(*capacities: float, statistic: str = "Average"):
        return [{"Timestamp": object(), statistic: value} for value in capacities]

    def test_what_a_window_of_datapoints_integrates_to(self) -> None:
        """Every window that yields a number, and the number it yields.

        One row per reading. These were a test each; the arithmetic is the
        whole assertion in all of them, so the table is the test.
        """

        cases: tuple[tuple[str, list, dict, Decimal], ...] = (
            (
                "three 60-second periods at 2, 1 and 0.5 ACU",
                self._points(2.0, 1.0, 0.5),
                {},
                Decimal("210"),
            ),
            (
                # The configured floor comes off the top and the result never
                # goes negative.
                "a floor of 1 ACU subtracted from 2 and 0.5",
                self._points(2.0, 0.5),
                {"floor_acu": Decimal("1")},
                Decimal("60"),
            ),
            (
                # Distinct from an unusable window below: CloudWatch answered,
                # and the answer was that the cluster was parked. That is a
                # measurement of zero.
                "an all-zero window",
                self._points(0.0, 0.0),
                {},
                Decimal(0),
            ),
            (
                "one good datapoint beside one junk datapoint",
                [{"Average": 1.0}, {"Average": "junk"}],
                {},
                Decimal("60"),
            ),
            (
                # Aurora publishes this metric once per second, so a bucket's
                # SampleCount is the seconds it observed. The first and last
                # buckets of any window are partial -- counts as low as 9 were
                # seen -- and charging them a full minute would inflate every
                # integral at both ends.
                "partial buckets integrated by their sample count",
                [
                    {"Average": 2.0, "SampleCount": 9},
                    {"Average": 2.0, "SampleCount": 60},
                    {"Average": 2.0, "SampleCount": 51},
                ],
                {},
                Decimal("240"),
            ),
            (
                # CloudWatch occasionally reports 62, 65, 66 for a 60s period.
                "a sample count above the period is clamped",
                [{"Average": 1.0, "SampleCount": 66}],
                {},
                Decimal("60"),
            ),
            # The three fallbacks to a full period. The fallback reads high,
            # which is the direction that cannot hide spend.
            ("no sample count at all", [{"Average": 1.0}], {}, Decimal("60")),
            (
                "an unparseable sample count",
                [{"Average": 1.0, "SampleCount": "junk"}],
                {},
                Decimal("60"),
            ),
            (
                "a zero sample count",
                [{"Average": 1.0, "SampleCount": 0}],
                {},
                Decimal("60"),
            ),
        )

        for name, datapoints, kwargs, expected in cases:
            assert integrate_acu_seconds(datapoints, **kwargs) == expected, name

    def test_what_yields_no_measurement_at_all(self) -> None:
        """None is not zero here, and a malformed payload is skipped, never raised.

        The malformed rows matter as much as the empty ones: this reads a live
        CloudWatch response, and a payload that surprises it must cost the line
        its measurement rather than cost the caller its process.
        """

        cases: tuple[tuple[str, list], ...] = (
            ("no datapoints at all", []),
            ("a datapoint carrying only a timestamp", [{"Timestamp": object()}]),
            ("the statistic the caller did not ask for", self._points(1.0, statistic="Maximum")),
            ("a non-numeric average", [{"Average": "not a number"}]),
            ("a null average", [{"Average": None}]),
            ("a NaN average", [{"Average": float("nan")}]),
            ("a boolean average", [{"Average": True}]),
            ("a datapoint that is not a mapping", ["not a datapoint at all"]),
            ("a null datapoint", [None]),
        )

        for name, datapoints in cases:
            assert integrate_acu_seconds(datapoints) is None, name

    def test_the_integral_feeds_the_line_as_a_measurement(self) -> None:
        acu_seconds = integrate_acu_seconds(self._points(2.0, 2.0, 2.0))
        assert acu_seconds is not None
        telemetry = _telemetry(
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            CompetitorId.AURORA_SERVERLESS_V2,
            observed_acu_seconds_above_floor=acu_seconds,
        )
        compute = next(
            line for line in estimate_bout_cost(telemetry).lines if line.cloud is Cloud.AWS
        )
        assert compute.quantity.provenance is Provenance.MEASURED
        assert compute.quantity.point == Decimal("360") / SECONDS_PER_HOUR

    def test_a_lane_clock_no_longer_prices_this_line_at_all(self) -> None:
        # The lane clock is not a weaker predictor of Aurora capacity, it is not
        # a predictor: two runs of Round 5 with lanes 6% apart measured 714.91
        # and 1017.48 ACU-seconds. Only samples price the line now.
        lane = Decimal("508.9")
        measured = _telemetry(
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=lane,
            observed_acu_seconds_above_floor=lane * Decimal("0.5"),
        )
        unsampled = _telemetry(
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=lane,
        )

        def compute(telemetry):
            return next(
                line for line in estimate_bout_cost(telemetry).lines if line.cloud is Cloud.AWS
            )

        assert compute(measured).usd == pytest.approx(
            lane * Decimal("0.5") / SECONDS_PER_HOUR * Decimal("0.12"), rel=Decimal("1e-9")
        )
        assert compute(unsampled).usd is None

    def test_the_unpriced_line_still_states_the_bounds_it_is_between(self) -> None:
        # An unavailable line is a gap, but "somewhere between $0.005 and $0.06"
        # is real information and there is no reason to throw it away with the
        # point estimate that could not be justified.
        lane = Decimal("508.9")
        telemetry = _telemetry(
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=lane,
        )
        compute = next(
            line for line in estimate_bout_cost(telemetry).lines if line.cloud is Cloud.AWS
        )
        floor = aurora_wake_commitment_acu_hours()
        ceiling = aurora_ceiling_acu_hours(lane, telemetry.teardown_allowance_seconds)
        assert f"{floor:.6f}" in compute.quantity.basis
        assert f"{ceiling:.6f}" in compute.quantity.basis

    def test_the_lower_bound_is_the_auto_pause_floor_and_never_zero(self) -> None:
        # `low=0` said a bout could touch Aurora and be billed nothing. A wake
        # commits to the 300-second auto-pause interval at the smallest running
        # capacity, so the floor is positive and unavoidable.
        floor = aurora_wake_commitment_acu_hours()
        assert floor > 0
        assert floor == ACU_DESCENT_FLOOR_SECONDS * AURORA_MIN_RUNNING_ACU / SECONDS_PER_HOUR
        assert ACU_DESCENT_FLOOR_SECONDS == Decimal(AURORA_AUTO_PAUSE_SECONDS)
        # $0.005 of Aurora that a bout cannot get under, whatever it does.
        assert floor * Decimal("0.12") == pytest.approx(Decimal("0.005"), rel=Decimal("1e-9"))

    def test_the_upper_bound_covers_the_descent_the_old_one_missed(self) -> None:
        lane = Decimal("15.31")
        teardown = Decimal("180")
        ceiling = aurora_ceiling_acu_hours(lane, teardown)
        lane_only = (lane + teardown) / SECONDS_PER_HOUR * Decimal(2)
        assert ceiling > lane_only
        # Round 1's measured 468.85 ACU-seconds sat *outside* the old bound and
        # inside the corrected one. That is the whole correction in one line.
        measured_acu_hours = Decimal("468.85") / SECONDS_PER_HOUR
        assert measured_acu_hours > lane_only
        assert measured_acu_hours < ceiling

    def test_the_window_reaches_well_past_the_auto_pause_floor(self) -> None:
        # Sizing the tail to the 300-second floor would have truncated a real
        # descent: one measured Round 5 bout held 0.5 ACU for 15 minutes after
        # its proxy was deleted, for reasons that were not established. The tail
        # is therefore generous and leans on trailing zero buckets to prove the
        # integral closed, rather than assuming the floor is the whole descent.
        started = datetime(2026, 8, 21, 1, 31, 32, tzinfo=UTC)
        ended = datetime(2026, 8, 21, 1, 45, 6, tzinfo=UTC)
        window_start, window_end = acu_sampling_window(started, ended)
        assert window_start == started - timedelta(seconds=float(ACU_SAMPLE_LEAD_SECONDS))
        assert window_end == ended + timedelta(seconds=float(ACU_SAMPLE_TAIL_SECONDS))
        assert ACU_SAMPLE_TAIL_SECONDS > ACU_DESCENT_FLOOR_SECONDS

    def test_a_backwards_window_is_rejected(self) -> None:
        instant = datetime(2026, 8, 21, 1, 31, 32, tzinfo=UTC)
        with pytest.raises(ValueError, match="must not end before it starts"):
            acu_sampling_window(instant, instant - timedelta(seconds=1))


class TestRoundFiveHasAnAuroraLane:
    """Round 5's Aurora database was priced at nothing, not at unavailable.

    `infra/aws/locals.tf:98` binds a dedicated Aurora cluster to the Round 5
    stack and `server/connection_spike_live.py` points the proxy at it when the
    operator arms Aurora, so the 128-client burst lands on a real database. The
    estimator emitted the proxy line and stopped, which read as $0.00 rather than
    as a gap and could not even be counted in `estimate.unavailable`.
    """

    @staticmethod
    def _aurora_compute(estimate):
        return next(
            line
            for line in estimate.lines
            if line.cloud is Cloud.AWS
            and line.kind is CostKind.COMPUTE
            and not line.component.startswith("RDS Proxy")
        )

    @pytest.mark.parametrize(
        ("acu_seconds", "proxy_seconds", "expected_compute", "expected_total"),
        [
            # The two bouts CloudTrail confirms ran the Aurora lane, priced from
            # their own CloudWatch integrals. `.anti-demo-v7/aurora-acu-2026-08-21.md` §3.
            ("1017.48", "649", "0.033916", "0.055549"),
            ("714.91", "611", "0.023830", "0.044197"),
        ],
    )
    def test_the_aurora_lane_is_priced_rather_than_omitted(
        self,
        acu_seconds: str,
        proxy_seconds: str,
        expected_compute: str,
        expected_total: str,
    ) -> None:
        telemetry = _telemetry(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            CompetitorId.AURORA_SERVERLESS_V2,
            observed_acu_seconds_above_floor=Decimal(acu_seconds),
            observed_proxy_lifetime_seconds=Decimal(proxy_seconds),
        )
        estimate = estimate_bout_cost(telemetry)
        compute = self._aurora_compute(estimate)
        assert compute.rate.unit == "ACU-hour"
        assert compute.quantity.provenance is Provenance.MEASURED
        assert compute.usd == pytest.approx(Decimal(expected_compute), rel=Decimal("1e-4"))
        assert estimate.by_cloud()[Cloud.AWS] == pytest.approx(
            Decimal(expected_total), rel=Decimal("1e-4")
        )

    def test_a_lane_clock_alone_leaves_the_aurora_line_unavailable(self) -> None:
        # The line is emitted -- that is defect 1 -- but it is emitted as a gap,
        # because a lane clock cannot establish Aurora's billed capacity.
        telemetry = _telemetry(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=Decimal("792.607613709"),
        )
        estimate = estimate_bout_cost(telemetry)
        compute = self._aurora_compute(estimate)
        assert compute.usd is None
        assert compute in estimate.unavailable

    def test_a_missing_quantity_yields_unavailable_and_never_zero(self) -> None:
        telemetry = BoutTelemetry(
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
            competitor_id=CompetitorId.AURORA_SERVERLESS_V2,
            bout_seconds=Decimal("813"),
        )
        estimate = estimate_bout_cost(telemetry)
        compute = self._aurora_compute(estimate)
        assert compute.usd is None
        assert compute.quantity.provenance is Provenance.UNAVAILABLE
        assert compute in estimate.unavailable

    def test_the_rds_lane_zero_is_a_result_and_survives_the_fix(self) -> None:
        # A provisioned instance bills continuously, so absorbing a burst adds no
        # incremental instance-hours. Turning this correct zero into an
        # unavailable would be a regression, not a fix.
        telemetry = _telemetry(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            CompetitorId.RDS_POSTGRES,
            competitor_lane_seconds=Decimal("792.607613709"),
        )
        compute = self._aurora_compute(estimate_bout_cost(telemetry))
        assert compute.usd == Decimal(0)
        assert compute.quantity.provenance is Provenance.ASSUMED
        assert compute not in estimate_bout_cost(telemetry).unavailable

    def test_the_eight_acu_proxy_minimum_applies_only_to_an_aurora_target(self) -> None:
        # AWS prices RDS Proxy against an Aurora Serverless v2 target at a
        # documented 8-unit minimum regardless of the cluster's own ceiling, so
        # merely pointing the proxy at Aurora quadruples the line. The RDS-target
        # price must not move.
        def proxy(competitor: CompetitorId):
            estimate = estimate_bout_cost(
                _telemetry(
                    RoundId.SURVIVE_CONNECTION_SPIKE,
                    competitor,
                    observed_proxy_lifetime_seconds=Decimal("792.607613709"),
                )
            )
            return next(line for line in estimate.lines if line.component.startswith("RDS Proxy"))

        aurora = proxy(CompetitorId.AURORA_SERVERLESS_V2)
        rds = proxy(CompetitorId.RDS_POSTGRES)
        assert "8 units" in aurora.component
        assert "2 units" in rds.component
        assert aurora.usd == pytest.approx(rds.usd * 4, rel=Decimal("1e-9"))
        assert rds.usd == pytest.approx(Decimal("0.00660506345"), rel=Decimal("1e-9"))

    def test_round_five_is_the_dearest_single_round_but_not_more_than_two_and_three(
        self,
    ) -> None:
        """The ceiling convention said Round 5 beat Rounds 2 and 3 combined.

        Measured, it does not: R5 is $0.0442-$0.0555 against a combined R2+R3 of
        $0.070550. It is still the single dearest round against Aurora, and that
        much survives. The stronger claim was an artefact of pricing R5 at a
        ceiling while R2 and R3 were under-counted, and it is not encoded here.
        """

        def acu_usd(round_id: RoundId, acu_seconds: Decimal) -> Decimal:
            return acu_seconds / SECONDS_PER_HOUR * Decimal("0.12")

        measured = V7_MEASURED_AURORA_ACU_SECONDS
        spike_low = acu_usd(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            measured[RoundId.SURVIVE_CONNECTION_SPIKE].low,
        ) + Decimal("0.020367")
        spike_high = acu_usd(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            measured[RoundId.SURVIVE_CONNECTION_SPIKE].high,
        ) + Decimal("0.021633")
        schema = acu_usd(
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            measured[RoundId.MAKE_SCHEMA_CHANGE_SAFELY].point,
        )
        recover = acu_usd(
            RoundId.RECOVER_DELETED_ORDER,
            measured[RoundId.RECOVER_DELETED_ORDER].point,
        )

        assert spike_low > schema
        assert spike_low > recover
        assert spike_high < schema + recover


class TestEveryProvisionedLaneProducesALine:
    """The structural guarantee, not the one route that was broken.

    Round 5's Aurora gap was invisible because a line that is never emitted
    cannot appear in `estimate.unavailable`. These assertions are what stops the
    next instance: they read the round keys back out of the Terraform and require
    a competitor compute line for every provisioned round on every competitor the
    round accepts.
    """

    @staticmethod
    def _terraform_round_keys() -> set[str]:
        source = Path("infra/aws/locals.tf").read_text()
        match = re.search(r"v7_round_keys\s*=\s*toset\(\[([^\]]*)\]\)", source)
        assert match is not None, "v7_round_keys is no longer declared as a toset literal"
        return set(re.findall(r'"([^"]+)"', match.group(1)))

    def test_the_model_agrees_with_the_terraform_about_which_rounds_have_a_lane(
        self,
    ) -> None:
        assert set(cost_model_module._AWS_ROUND_KEYS.values()) == self._terraform_round_keys()

    def test_every_round_is_either_provisioned_or_explicitly_not(self) -> None:
        provisioned = set(cost_model_module._AWS_ROUND_KEYS)
        assert provisioned | cost_model_module._ROUNDS_WITHOUT_AWS == set(RoundId)
        assert not provisioned & cost_model_module._ROUNDS_WITHOUT_AWS

    @pytest.mark.parametrize("round_id", sorted(cost_model_module._AWS_ROUND_KEYS, key=str))
    @pytest.mark.parametrize("competitor", sorted(CompetitorId, key=str))
    def test_a_provisioned_round_prices_its_database_on_either_lane(
        self,
        round_id: RoundId,
        competitor: CompetitorId,
    ) -> None:
        estimate = estimate_bout_cost(_telemetry(round_id, competitor))
        assert any(
            line.cloud is Cloud.AWS
            and line.kind is CostKind.COMPUTE
            and line.lane_id == "competitor"
            for line in estimate.lines
        )

    def test_a_routing_miss_becomes_an_unavailable_line_rather_than_silence(self) -> None:
        # Reaching this is a bug in the routing, and the point is that the
        # estimate says so out loud instead of quietly totalling less.
        telemetry = _telemetry(RoundId.SURVIVE_CONNECTION_SPIKE, CompetitorId.AURORA_SERVERLESS_V2)
        gap = cost_model_module._lane_coverage_lines([], telemetry, RateCard())
        assert len(gap) == 1
        assert gap[0].quantity.provenance is Provenance.UNAVAILABLE
        assert gap[0].usd is None
        assert "r5" in gap[0].quantity.basis

    def test_a_round_with_no_aws_stack_is_not_given_a_placeholder(self) -> None:
        telemetry = _telemetry(RoundId.ANALYZE_LIVE_ORDERS, CompetitorId.AURORA_SERVERLESS_V2)
        assert cost_model_module._lane_coverage_lines([], telemetry, RateCard()) == []


class TestTheFleetShapeMatchesTheFleet:
    """`InstallationShape` is what the standing-cost panel prices. It drifted.

    Terraform stands an Aurora cluster up per `v7_round_keys` but an RDS instance
    up only per the narrower `v7_rds_round_keys`, because Round 1's instance was
    deleted -- its lane refuses to enter on engine semantics and was never timed,
    so it billed to measure nothing. The shape kept saying four, so the panel
    quoted `$10.12/day` against a fleet of three and the RDS lane's caveat named
    a box that no longer existed.

    That is the worst direction for this particular error to point: it
    *overstates the opponent's bill*, which is the number an audience is most
    likely to check and the one this demo has least right to inflate. These tests
    read both key sets back out of the Terraform so the shape cannot drift again
    in either direction.
    """

    @staticmethod
    def _keys(name: str) -> set[str]:
        source = Path("infra/aws/locals.tf").read_text()
        match = re.search(rf"{name}\s*=\s*toset\(\[([^\]]*)\]\)", source)
        assert match is not None, f"{name} is no longer declared as a toset literal"
        return set(re.findall(r'"([^"]+)"', match.group(1)))

    def test_the_instance_count_is_the_number_terraform_stands_up(self) -> None:
        assert InstallationShape().rds_instances == len(self._keys("v7_rds_round_keys"))

    def test_the_cluster_count_is_the_number_terraform_stands_up(self) -> None:
        assert InstallationShape().aurora_clusters == len(self._keys("v7_round_keys"))

    def test_the_rds_fleet_is_the_rounds_that_are_not_imputed(self) -> None:
        # The two definitions have to name the same rounds: a round with no box
        # here is exactly a round whose box gets imputed for a customer. If these
        # disagreed, some round would be either double-counted or dropped.
        keys = self._keys("v7_rds_round_keys")
        standing = {
            f"r{number}"
            for number, round_id in enumerate(RoundId, start=1)
            if round_id not in IMPUTED_RDS_ROUNDS
        }
        assert standing == keys

    def test_one_address_and_one_managed_secret_per_database(self) -> None:
        # Every database is `publicly_accessible = true` and every one arrives
        # with an RDS-managed master credential, so both counts are a function of
        # the two fleet sizes. The two static Round 5 proxy secrets are the only
        # thing that is not.
        shape = InstallationShape()
        databases = shape.rds_instances + shape.aurora_clusters
        assert shape.public_ipv4_addresses == databases
        assert shape.managed_secrets == databases + TERRAFORM_PROXY_SECRETS

    def test_the_deleted_instance_is_reconstructible_and_not_re_typed(self) -> None:
        # `with_r1_rds_instance` exists so the deletion identity stays checkable.
        # It must be the current shape plus one box's worth and nothing else --
        # written by addition rather than as a second set of literals, which is
        # how the two generations stayed in agreement while one of them moved.
        now = InstallationShape()
        before = now.with_r1_rds_instance()
        assert before.rds_instances == now.rds_instances + 1
        assert before.public_ipv4_addresses == now.public_ipv4_addresses + 1
        assert before.managed_secrets == now.managed_secrets + 1
        assert before.aurora_clusters == now.aurora_clusters
        assert before.with_r1_rds_instance().rds_instances == now.rds_instances + 2


class TestTheMeasuredAuroraQuantities:
    """The four CloudWatch integrals, and the one question they could not settle.

    `.anti-demo-v7/aurora-acu-2026-08-21.md`. These are the only real Aurora
    quantities this installation has, and the totals they produce supersede the
    published ceiling figures in both directions -- larger on every round, and
    smaller on Round 5 than the ceiling convention had projected.
    """

    def test_only_the_rounds_terraform_provisions_aurora_for_are_measured(self) -> None:
        assert set(V7_MEASURED_AURORA_ACU_SECONDS) == set(cost_model_module._AWS_ROUND_KEYS)

    @pytest.mark.parametrize(
        ("round_id", "expected_usd"),
        [
            (RoundId.WAKE_IDLE_APP, "0.015628"),
            (RoundId.MAKE_SCHEMA_CHANGE_SAFELY, "0.043267"),
            (RoundId.RECOVER_DELETED_ORDER, "0.027283"),
        ],
    )
    def test_the_measured_figures_reproduce_the_published_dollars(
        self,
        round_id: RoundId,
        expected_usd: str,
    ) -> None:
        measurement = V7_MEASURED_AURORA_ACU_SECONDS[round_id]
        usd = measurement.point / SECONDS_PER_HOUR * Decimal("0.12")
        assert usd == pytest.approx(Decimal(expected_usd), rel=Decimal("1e-4"))

    def test_the_ceiling_convention_understated_every_round_it_priced(self) -> None:
        # The sign the model asserted for as long as it had an Aurora line.
        published = Decimal("0.049800")
        measured = sum(
            (
                V7_MEASURED_AURORA_ACU_SECONDS[round_id].point
                for round_id in (
                    RoundId.WAKE_IDLE_APP,
                    RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
                    RoundId.RECOVER_DELETED_ORDER,
                )
            ),
            Decimal(0),
        ) / SECONDS_PER_HOUR * Decimal("0.12")
        assert measured > published
        assert measured / published == pytest.approx(Decimal("1.73"), rel=Decimal("1e-2"))

    @pytest.mark.parametrize(
        "round_id",
        [RoundId.MAKE_SCHEMA_CHANGE_SAFELY, RoundId.RECOVER_DELETED_ORDER],
    )
    def test_the_restore_drain_question_survives_as_a_range(self, round_id: RoundId) -> None:
        # CloudWatch reported 2.0 ACU for 276-513 seconds *after* DeleteDBInstance.
        # Whether AWS bills that is undocumented and ce:GetCostAndUsage is denied,
        # so it cannot be settled. Collapsing the band either way would be picking
        # an answer nobody has given.
        measurement = V7_MEASURED_AURORA_ACU_SECONDS[round_id]
        assert measurement.is_ambiguous
        assert measurement.low < measurement.high
        assert "undocumented" in measurement.basis or "unresolved" in measurement.basis
        assert measurement.point == measurement.high

    @pytest.mark.parametrize("round_id", [RoundId.WAKE_IDLE_APP])
    def test_a_live_cluster_round_carries_no_such_ambiguity(self, round_id: RoundId) -> None:
        # R1's writer exists throughout and its descent reached 0 ACU inside the
        # window, so there is nothing left to bound.
        assert not V7_MEASURED_AURORA_ACU_SECONDS[round_id].is_ambiguous

    def test_round_fives_band_is_the_spread_between_two_real_bouts(self) -> None:
        measurement = V7_MEASURED_AURORA_ACU_SECONDS[RoundId.SURVIVE_CONNECTION_SPIKE]
        assert len(measurement.bouts) == 2
        assert measurement.point == (measurement.low + measurement.high) / 2
        # 42% apart on two runs of the same round, which is why the lane clock
        # cannot be trusted to predict this quantity at all.
        assert measurement.high / measurement.low > Decimal("1.4")

    def test_the_table_grades_itself_as_measured(self) -> None:
        for measurement in V7_MEASURED_AURORA_ACU_SECONDS.values():
            assert measurement.as_quantity().provenance is Provenance.MEASURED

    def test_an_unsampled_round_has_no_entry_rather_than_a_zero(self) -> None:
        assert aurora_acu_seconds_for(RoundId.ANALYZE_LIVE_ORDERS) is None
        assert aurora_acu_seconds_for(RoundId.PUT_MODEL_SCORE_IN_APP) is None

    def test_a_measurement_must_name_the_bouts_it_came_from(self) -> None:
        with pytest.raises(ValueError, match="must name the bouts"):
            AuroraAcuMeasurement(
                round_id=RoundId.WAKE_IDLE_APP,
                point=Decimal(1),
                low=Decimal(1),
                high=Decimal(1),
                bouts=(),
                basis="test",
            )

    def test_a_measured_band_must_contain_its_point(self) -> None:
        with pytest.raises(ValueError, match="must contain its point"):
            AuroraAcuMeasurement(
                round_id=RoundId.WAKE_IDLE_APP,
                point=Decimal(5),
                low=Decimal(1),
                high=Decimal(2),
                bouts=("7ECE1CB0",),
                basis="test",
            )

    def test_the_table_is_not_wired_into_the_estimator_behind_the_callers_back(self) -> None:
        # A past bout's integral is evidence about that bout. Applying it to the
        # next one automatically would manufacture exactly the coverage this
        # module just finished refusing to manufacture.
        telemetry = _telemetry(
            RoundId.WAKE_IDLE_APP,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=Decimal("15.31"),
        )
        compute = next(
            line
            for line in estimate_bout_cost(telemetry).lines
            if line.cloud is Cloud.AWS and line.kind is CostKind.COMPUTE
        )
        assert compute.quantity.provenance is Provenance.UNAVAILABLE


class TestWakeRound:
    def test_waking_a_provisioned_instance_adds_no_instance_hours(self) -> None:
        telemetry = _telemetry(
            RoundId.WAKE_IDLE_APP,
            CompetitorId.RDS_POSTGRES,
            competitor_lane_seconds=Decimal("14.57"),
        )
        estimate = estimate_bout_cost(telemetry)
        wake = next(line for line in estimate.lines if "wake" in line.component)
        assert wake.usd == Decimal(0)
        assert wake.quantity.provenance is Provenance.ASSUMED

    def test_waking_aurora_from_zero_acu_is_not_free_but_is_not_a_clock_either(self) -> None:
        # The measured Round 1 bout ran 15.31 seconds and provisioned 420 seconds
        # of billed capacity, 97.2% of it after the bell -- 16.1x what the lane
        # clock predicted. A wake is expensive and the lane does not say how.
        telemetry = _telemetry(
            RoundId.WAKE_IDLE_APP,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=Decimal("14.57"),
        )
        estimate = estimate_bout_cost(telemetry)
        wake = next(line for line in estimate.lines if "wake" in line.component.lower())
        assert wake.usd is None
        assert wake in estimate.unavailable
        assert aurora_wake_commitment_acu_hours() > 0

    def test_the_measured_wake_is_priced_when_it_is_supplied(self) -> None:
        measurement = aurora_acu_seconds_for(RoundId.WAKE_IDLE_APP)
        assert measurement is not None
        telemetry = _telemetry(
            RoundId.WAKE_IDLE_APP,
            CompetitorId.AURORA_SERVERLESS_V2,
            competitor_lane_seconds=Decimal("15.31"),
            observed_acu_seconds_above_floor=measurement.point,
        )
        wake = next(
            line
            for line in estimate_bout_cost(telemetry).lines
            if "wake" in line.component.lower()
        )
        assert wake.usd == pytest.approx(Decimal("0.015628"), rel=Decimal("1e-4"))
        assert wake.quantity.provenance is Provenance.MEASURED


class TestRoundsWithoutAnAwsStack:
    @pytest.mark.parametrize(
        "round_id",
        [RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS],
    )
    def test_no_aws_line_is_invented_for_a_round_that_built_no_aws_stack(
        self,
        round_id: RoundId,
    ) -> None:
        estimate = estimate_bout_cost(_telemetry(round_id, CompetitorId.AURORA_SERVERLESS_V2))
        assert all(line.cloud is Cloud.DATABRICKS for line in estimate.lines)
        assert estimate.by_cloud()[Cloud.AWS] == Decimal(0)


class TestCapacityUnitConversion:
    """Pins the DBU-per-CU-hour reading that a parity audit contested.

    The audit read the two observed plateaus as 0.5 CU and 1 CU, which would halve
    every per-CU figure.  These assertions encode the 1 CU / 2 CU reading and the
    evidence for it, so that a future edit cannot silently flip a 2x factor.
    """

    def test_one_capacity_unit_meters_at_the_documented_rate(self) -> None:
        assert LAKEBASE_DBU_PER_CU_HOUR == Decimal("0.213")

    def test_the_sizing_reference_worked_example_reproduces(self) -> None:
        # 1 CU x 0.213 DBU/CU-hour x 1 node x 730 hours = 155.49 DBU/month.
        # This is what fixes the unit as *per CU-hour* rather than per node-hour.
        monthly = Decimal("1") * LAKEBASE_DBU_PER_CU_HOUR * Decimal("730")
        assert monthly == Decimal("155.490")

    def test_the_observed_plateaus_land_on_whole_capacity_units(self) -> None:
        # Both plateaus posted by v7 on 2026-08-20, in an exact 2:1 ratio.
        lower = Decimal("0.213")
        upper = Decimal("0.426")
        assert lower / LAKEBASE_DBU_PER_CU_HOUR == Decimal("1")
        assert upper / LAKEBASE_DBU_PER_CU_HOUR == Decimal("2")

    def test_the_node_rate_is_the_ceiling_not_a_free_standing_constant(self) -> None:
        assert LAKEBASE_NODE_DBU_PER_HOUR == LAKEBASE_DBU_PER_CU_HOUR * LAKEBASE_CEILING_CU
        assert LAKEBASE_NODE_DBU_PER_HOUR == Decimal("0.426")

    def test_the_corroborating_price_cannot_discriminate_between_the_readings(self) -> None:
        # Why the $0.111/CU-hour figure settles nothing: the Lakebase promotion is
        # exactly 50%, so the contested reading and this one agree to the cent.
        this_reading = LAKEBASE_DBU_PER_CU_HOUR * Decimal("0.52")
        contested_reading = Decimal("0.426") * Decimal("0.26")
        assert this_reading == contested_reading == Decimal("0.11076")

    def test_dollars_do_not_pass_through_capacity_units(self) -> None:
        # The reconciliation claim must not inherit the 2x bound.  A posted-DBU
        # bout is priced straight off the meter, so it is identical under either
        # reading of what a CU is.
        telemetry = _telemetry(
            RoundId.RECOVER_DELETED_ORDER,
            observed_lakebase_dbu=Decimal("0.034080"),
        )
        line = next(
            line for line in estimate_bout_cost(telemetry).lines if line.cloud is Cloud.DATABRICKS
        )
        assert line.usd == Decimal("0.034080") * Decimal("0.26")


class TestLakebaseQuantity:
    def test_posted_dbu_is_used_verbatim(self) -> None:
        telemetry = _telemetry(
            RoundId.RECOVER_DELETED_ORDER,
            observed_lakebase_dbu=Decimal("0.42"),
        )
        estimate = estimate_bout_cost(telemetry)
        line = next(line for line in estimate.lines if line.cloud is Cloud.DATABRICKS)
        assert line.quantity.provenance is Provenance.MEASURED
        assert line.usd == Decimal("0.42") * Decimal("0.26")

    def test_without_posted_dbu_or_calibration_the_line_is_unavailable_not_zero(self) -> None:
        estimate = estimate_bout_cost(_telemetry(RoundId.RECOVER_DELETED_ORDER))
        line = next(line for line in estimate.lines if line.cloud is Cloud.DATABRICKS)
        assert line.usd is None
        assert line in estimate.unavailable

    def test_calibration_produces_a_number_without_waiting_for_billing(self) -> None:
        burn = _restore_burn()
        estimate = estimate_bout_cost(
            _telemetry(RoundId.RECOVER_DELETED_ORDER),
            calibration=burn,
        )
        line = next(line for line in estimate.lines if line.cloud is Cloud.DATABRICKS)
        assert line.quantity.provenance is Provenance.MODELED
        assert line.quantity.point == Decimal("812") * burn.point

    def test_the_predictor_is_bout_wall_clock_not_the_lakebase_lane(self) -> None:
        burn = _restore_burn()
        estimate = estimate_bout_cost(
            _telemetry(RoundId.RECOVER_DELETED_ORDER),
            calibration=burn,
        )
        line = next(line for line in estimate.lines if line.cloud is Cloud.DATABRICKS)
        assert line.quantity.point != Decimal("17.43") * burn.point
        assert line.quantity.point == Decimal("812") * burn.point

    def test_the_band_spans_the_observed_sample_spread(self) -> None:
        burn = calibrate_lakebase_burn(
            [
                (Decimal("0.03408"), Decimal("479.2")),
                (Decimal("0.011596"), Decimal("124.0")),
            ],
            rounds=[RoundId.MAKE_SCHEMA_CHANGE_SAFELY, RoundId.RECOVER_DELETED_ORDER],
        )
        assert burn.sample_count == 2
        assert burn.low == Decimal("0.03408") / Decimal("479.2")
        assert burn.high == Decimal("0.011596") / Decimal("124.0")
        assert burn.low < burn.point < burn.high

    def test_what_calibration_refuses(self) -> None:
        """Three rejections that were a test each, all of one shape."""

        with pytest.raises(ValueError, match="positive bout interval"):
            calibrate_lakebase_burn(
                [(Decimal("1"), Decimal(0))],
                rounds=[RoundId.RECOVER_DELETED_ORDER],
            )
        with pytest.raises(ValueError, match="at least one reconciled sample"):
            calibrate_lakebase_burn([], rounds=[RoundId.RECOVER_DELETED_ORDER])
        with pytest.raises(ValueError, match="must contain its point"):
            BurnRate(
                point=Decimal("1"),
                low=Decimal("2"),
                high=Decimal("3"),
                sample_count=1,
                rounds=frozenset({RoundId.RECOVER_DELETED_ORDER}),
            )


class TestTheBurnRateIsNotAppliedOutsideItsSupport:
    """A rate fitted on the restore rounds over-predicted Round 5 by 13.8x.

    Recalibrating on five samples instead of two moves the point rate 11% and
    still leaves 12.2x on Round 5, because the error is structural rather than a
    bad fit: Round 5's clock is `setup_elapsed_ms`, and 792.6 of its 813 seconds
    were AWS building a proxy while the Lakebase endpoint slept. A rate now
    carries the rounds it was fitted on, and a round it does not cover gets an
    unavailable line instead of a confident wrong number.
    """

    def test_what_a_rate_and_a_model_refuse_to_be_built_without(self) -> None:
        """The three declarations that make a rate's support checkable at all."""

        with pytest.raises(ValueError, match="must name the rounds"):
            calibrate_lakebase_burn([(Decimal("0.5"), Decimal("1000"))], rounds=[])
        with pytest.raises(ValueError, match="both claim to cover"):
            LakebaseBurnModel(
                rates=(
                    calibrate_from_samples(V7_RESTORE_SAMPLES),
                    calibrate_lakebase_burn(
                        [(Decimal("1"), Decimal("10"))],
                        rounds=[RoundId.RECOVER_DELETED_ORDER],
                    ),
                )
            )
        with pytest.raises(ValueError, match="at least one calibrated rate"):
            LakebaseBurnModel(rates=())

    def test_the_restore_rate_refuses_to_price_round_five(self) -> None:
        estimate = estimate_bout_cost(
            _telemetry(RoundId.SURVIVE_CONNECTION_SPIKE, bout_seconds=Decimal("813.37")),
            calibration=_restore_burn(),
        )
        line = next(line for line in estimate.lines if line.cloud is Cloud.DATABRICKS)
        assert line.usd is None
        assert line in estimate.unavailable
        assert "outside its support" in line.quantity.basis

    def test_the_recalibrated_restore_rate_reproduces_the_published_point(self) -> None:
        burn = calibrate_from_samples(V7_RESTORE_SAMPLES)
        assert burn.sample_count == 5
        assert burn.point == pytest.approx(Decimal("7.291618e-05"), rel=Decimal("1e-5"))
        assert burn.low == pytest.approx(Decimal("6.324479e-05"), rel=Decimal("1e-5"))
        assert burn.high == pytest.approx(Decimal("9.356871e-05"), rel=Decimal("1e-5"))
        # Every new sample landed below the old two-sample floor, so the point
        # rate falls rather than merely tightening.
        assert burn.point < Decimal("8.234087e-05")
        assert burn.low < Decimal("7.111571e-05")

    def test_recalibration_alone_would_not_have_fixed_round_five(self) -> None:
        """The justification for segmenting rather than refitting."""

        burn = calibrate_from_samples(V7_RESTORE_SAMPLES)
        measured = Decimal("0.004851667")
        predicted = Decimal("813.37") * burn.point
        assert predicted / measured > Decimal(12)

    def test_the_model_prices_round_five_from_its_own_sample(self) -> None:
        model = v7_lakebase_burn_model()
        estimate = estimate_bout_cost(
            _telemetry(RoundId.SURVIVE_CONNECTION_SPIKE, bout_seconds=Decimal("813.37")),
            calibration=model,
        )
        line = next(line for line in estimate.lines if line.cloud is Cloud.DATABRICKS)
        assert line.usd is not None
        assert line.usd == pytest.approx(Decimal("0.001261"), rel=Decimal("1e-3"))

    @pytest.mark.parametrize(
        "round_id",
        [RoundId.WAKE_IDLE_APP, RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS],
    )
    def test_a_round_with_no_isolable_sample_is_left_unavailable(
        self,
        round_id: RoundId,
    ) -> None:
        # R1's only record covers two bouts, R4's cannot be separated from the
        # background sharing its records, and R6 has never run a bout. None is a
        # number and none should be made to look like one.
        estimate = estimate_bout_cost(
            _telemetry(round_id),
            calibration=v7_lakebase_burn_model(),
        )
        line = next(line for line in estimate.lines if line.cloud is Cloud.DATABRICKS)
        assert line.usd is None


class TestAccuracyIsReportedOutOfSample:
    """Calibrating on every sample then scoring against them is circular.

    Leave-one-out is what the cost analysis already uses, so keeping it keeps the
    two comparable: the two-sample basis reported +31.6% / -24.0% per row and
    +17.5% aggregate, and this is the five-sample restatement of the same thing.
    """

    def test_every_row_is_predicted_by_a_model_that_never_saw_it(self) -> None:
        rows = leave_one_out(V7_RESTORE_SAMPLES)
        assert len(rows) == len(V7_RESTORE_SAMPLES)
        assert {row.label for row in rows} == {s.label for s in V7_RESTORE_SAMPLES}
        assert all(row.trained_on == len(V7_RESTORE_SAMPLES) - 1 for row in rows)

    def test_the_out_of_sample_error_band_is_within_thirty_percent(self) -> None:
        rows = leave_one_out(V7_RESTORE_SAMPLES)
        errors = [row.error_fraction for row in rows]
        assert all(error is not None for error in errors)
        worst = max(abs(error) for error in errors if error is not None)
        assert worst < Decimal("0.30")
        # It runs high on long bouts and low on short ones, which is structural:
        # a fixed rate cannot capture a wake cost that a long bout amortises.
        shortest = next(row for row in rows if row.label == "A672140E")
        longest = next(row for row in rows if row.label == "6CF4C290")
        assert shortest.error_fraction is not None and shortest.error_fraction < 0
        assert longest.error_fraction is not None and longest.error_fraction > 0

    def test_the_aggregate_error_improved_against_the_two_sample_basis(self) -> None:
        rows = leave_one_out(V7_RESTORE_SAMPLES)
        predicted = sum((row.predicted_dbu for row in rows), Decimal(0))
        posted = sum((row.posted_dbu for row in rows), Decimal(0))
        aggregate = (predicted - posted) / posted
        assert Decimal(0) < aggregate < Decimal("0.175")

    def test_a_single_sample_cannot_be_validated_out_of_sample(self) -> None:
        # Round 5 has exactly one isolable bout, and saying so is the finding.
        with pytest.raises(ValueError, match="at least two samples"):
            leave_one_out(V7_CONNECTION_SPIKE_SAMPLES)

    def test_error_is_prediction_minus_posted(self) -> None:
        row = HeldOutPrediction(
            label="X",
            round_id=RoundId.RECOVER_DELETED_ORDER,
            trained_on=4,
            predicted_dbu=Decimal("0.012"),
            posted_dbu=Decimal("0.010"),
        )
        assert row.error_dbu == Decimal("0.002")
        assert row.error_fraction == Decimal("0.2")


class TestCarryingCost:
    def test_carrying_and_overhead_are_never_merged_into_a_bout_total(self) -> None:
        carrying = estimate_carrying_cost(CarryingWindow(seconds=Decimal(86400)))
        assert carrying.total_usd(EstimateScope.BOUT) == Decimal(0)
        assert carrying.total_usd(EstimateScope.CARRYING) > 0
        assert carrying.total_usd(EstimateScope.OVERHEAD) > 0

    def test_a_zero_floor_aurora_cluster_parks_free(self) -> None:
        carrying = estimate_carrying_cost(CarryingWindow(seconds=Decimal(86400)))
        aurora = next(
            line for line in carrying.lines if "Aurora Serverless v2 baseline" in line.component
        )
        assert aurora.usd == Decimal(0)

    def test_raising_the_aurora_floor_makes_it_cost_money(self) -> None:
        shape = InstallationShape(aurora_min_acu=Decimal("0.5"))
        carrying = estimate_carrying_cost(CarryingWindow(seconds=Decimal(86400)), shape=shape)
        aurora = next(
            line for line in carrying.lines if "Aurora Serverless v2 baseline" in line.component
        )
        assert aurora.usd == Decimal(24) * Decimal(4) * Decimal("0.5") * Decimal("0.12")

    def test_the_runner_is_overhead_and_not_charged_to_round_five(self) -> None:
        carrying = estimate_carrying_cost(CarryingWindow(seconds=Decimal(3600)))
        runner = next(line for line in carrying.lines if "burst runner" in line.component)
        assert runner.scope is EstimateScope.OVERHEAD
        assert runner.usd == Decimal("0.096")

    def test_posted_lakebase_carrying_usage_is_priced_when_supplied(self) -> None:
        carrying = estimate_carrying_cost(
            CarryingWindow(seconds=Decimal(86400)),
            lakebase_always_on_dbu=Decimal("12"),
            lakebase_storage_dsu=Decimal("3.5"),
        )
        compute = next(line for line in carrying.lines if "always-on minimum" in line.component)
        storage = next(
            line for line in carrying.lines if line.component.startswith("Lakebase database")
        )
        assert compute.usd == Decimal("12") * Decimal("0.26")
        assert storage.usd == Decimal("3.5") * Decimal("0.023")

    def test_a_carrying_window_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="positive duration"):
            CarryingWindow(seconds=Decimal(0))

    def test_a_day_of_standing_aws_cost_dominates_a_single_bout(self) -> None:
        """The headline finding: standing cost is orders of magnitude larger."""

        carrying = estimate_carrying_cost(CarryingWindow(seconds=Decimal(86400)))
        standing = carrying.total_usd(EstimateScope.CARRYING) + carrying.total_usd(
            EstimateScope.OVERHEAD
        )
        bout = estimate_bout_cost(_telemetry(RoundId.RECOVER_DELETED_ORDER)).total_usd()
        assert standing > bout * 100


def _aws_usd(estimate) -> Decimal:
    """Every AWS line in a carrying estimate, across both of its scopes.

    The standing-cost surface's "AWS" half is the RDS, Aurora, proxy-secret and
    neutral-runner lanes together, and the runner is ``OVERHEAD`` rather than
    ``CARRYING``, so filtering by one scope would drop it and understate the half.
    """

    return sum(
        ((line.usd or Decimal(0)) for line in estimate.lines if line.cloud is Cloud.AWS),
        Decimal(0),
    )


def _imputed_rds_day(rates: RateCard | None = None, shape: InstallationShape | None = None):
    return imputed_total_usd(
        imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY, rates=rates, shape=shape)
    )


class TestTheImputationIsStructuralNotTextual:
    """A flag the totals route on, not prose a renderer could drop."""

    def test_an_imputed_line_is_marked_on_the_line_itself(self) -> None:
        lines = imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY)
        assert lines
        assert all(line.imputed for line in lines)

    def test_every_imputed_quantity_is_modelled_and_never_measured(self) -> None:
        for round_id in IMPUTED_RDS_ROUNDS:
            for line in imputed_round_carrying_lines(round_id, _A_DAY):
                assert line.quantity.provenance is Provenance.MODELED

    def test_an_imputed_line_cannot_claim_a_measured_quantity(self) -> None:
        # The stronger half of the rule: MODELED is not merely what the builder
        # happens to pass, it is the only thing the type will accept.
        with pytest.raises(ValueError, match="only MODELED is honest"):
            CostLine(
                component="RDS PostgreSQL · modelled continuous instance",
                cloud=Cloud.AWS,
                kind=CostKind.COMPUTE,
                scope=EstimateScope.CARRYING,
                lane_id="competitor",
                quantity=Quantity.exact(
                    Decimal(24),
                    provenance=Provenance.MEASURED,
                    basis="describe-db-instances",
                ),
                rate=RateCard().rds_instance_hour,
                imputed=True,
            )

    def test_an_imputed_line_cannot_render_without_its_derivation(self) -> None:
        # Same rule the codebase already applies to a bare $0.00: the figure and
        # the reason it is that figure travel together or neither travels.
        with pytest.raises(ValueError, match="basis"):
            Quantity.exact(Decimal(24), provenance=Provenance.MODELED, basis="   ")

    def test_the_rds_basis_says_why_it_cannot_scale_to_zero(self) -> None:
        line = imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY)[0]
        basis = line.quantity.basis
        assert "has no zero state" in basis
        assert "smallest billable unit is the instance" in basis
        assert "outage rather than a scale-down" in basis
        assert "product boundary, not a setting we declined to configure" in basis
        assert "Modelled, not measured" in basis

    def test_the_rds_basis_names_the_class_from_the_rate_card(self) -> None:
        # Interpolated, never literal, so a resize cannot leave the sentence behind.
        large = RateCard(rds_instance_class="db.t4g.large")
        line = imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY, rates=large)[0]
        assert "db.t4g.large" in line.quantity.basis
        assert RateCard().rds_instance_class not in line.quantity.basis

    def test_the_aurora_compute_zero_carries_its_own_derivation(self) -> None:
        lines = imputed_round_carrying_lines(RoundId.ANALYZE_LIVE_ORDERS, _A_DAY)
        compute = next(
            line for line in lines if line.component.startswith("Aurora Serverless v2 compute")
        )
        assert compute.usd == Decimal(0)
        assert "min_capacity = 0" in compute.quantity.basis
        assert "exactly zero rather than unmeasured" in compute.quantity.basis

    def test_a_raised_aurora_floor_cannot_leave_the_zero_sentence_behind(self) -> None:
        # The sentence is true of the sealed configuration. Raise the floor and it
        # must stop claiming a zero it no longer has.
        raised = InstallationShape(aurora_min_acu=Decimal("0.5"))
        compute = next(
            line
            for line in imputed_round_carrying_lines(
                RoundId.ANALYZE_LIVE_ORDERS, _A_DAY, shape=raised
            )
            if line.component.startswith("Aurora Serverless v2 compute")
        )
        assert compute.usd > 0
        assert "exactly zero" not in compute.quantity.basis
        assert "min_capacity = 0.5" in compute.quantity.basis

    def test_the_aurora_standing_basis_names_the_gap_it_does_not_price(self) -> None:
        line = next(
            item
            for item in imputed_round_carrying_lines(RoundId.PUT_MODEL_SCORE_IN_APP, _A_DAY)
            if item.component == "Aurora baseline storage · modelled"
        )
        basis = line.quantity.basis
        assert "not a lane result" in basis
        assert "floor, not an estimate" in basis
        for service in ("DMS", "Glue", "Kinesis", "Firehose", "Lambda"):
            assert service in basis

    def test_no_pipeline_service_is_ever_given_a_price(self) -> None:
        # An honest gap, named, beats a confident guess. Naming them in a basis is
        # the disclosure; emitting a line for them would be the invention.
        for round_id in IMPUTED_RDS_ROUNDS:
            for line in imputed_round_carrying_lines(round_id, _A_DAY):
                for service in UNPRICED_PIPELINE_SERVICES:
                    assert service.lower() not in line.component.lower()


class TestWhichRoundsOweACounterfactual:
    @pytest.mark.parametrize(
        "round_id",
        [RoundId.WAKE_IDLE_APP, RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS],
    )
    def test_a_round_with_no_rds_instance_gets_a_modelled_one(self, round_id: RoundId) -> None:
        lines = imputed_round_carrying_lines(round_id, _A_DAY)
        assert [line.kind for line in lines if "RDS" in line.component] == [
            CostKind.COMPUTE,
            CostKind.STORAGE,
            CostKind.NETWORK,
            CostKind.OTHER,
        ]

    @pytest.mark.parametrize(
        "round_id",
        [
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            RoundId.RECOVER_DELETED_ORDER,
            RoundId.SURVIVE_CONNECTION_SPIKE,
        ],
    )
    def test_a_round_that_stands_its_own_lanes_up_gets_nothing(self, round_id: RoundId) -> None:
        assert imputed_round_carrying_lines(round_id, _A_DAY) == ()

    def test_round_one_gets_a_modelled_rds_instance_and_no_modelled_aurora(self) -> None:
        # Round 1's Aurora cluster is real and is the only lane that can compete in
        # it. An imputed cluster on top would double-count it.
        lines = imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY)
        assert any("RDS" in line.component for line in lines)
        assert not any("Aurora" in line.component for line in lines)
        assert RoundId.WAKE_IDLE_APP in IMPUTED_RDS_ROUNDS
        assert RoundId.WAKE_IDLE_APP not in IMPUTED_AURORA_ROUNDS

    @pytest.mark.parametrize(
        "round_id", [RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS]
    )
    def test_a_round_with_no_aws_lane_at_all_gets_both(self, round_id: RoundId) -> None:
        lines = imputed_round_carrying_lines(round_id, _A_DAY)
        assert sum(1 for line in lines if "RDS" in line.component) == 4
        assert sum(1 for line in lines if "Aurora" in line.component) == 4


class TestTheImputedFiguresDeriveFromTheRateCard:
    """Every figure recomputed from ``RateCard()`` inside the test.

    Typing the expected numbers in would let a rate change move one side only, and
    restating the fixture would let both drift together.  These derive.
    """

    def test_one_modelled_rds_instance_costs_a_days_worth_of_every_part_of_it(self) -> None:
        rates = RateCard()
        shape = InstallationShape()
        hours = _A_DAY.hours
        months = _A_DAY.months
        expected = (
            hours * rates.rds_instance_hour.usd
            + months * shape.rds_allocated_gb * rates.rds_gp3_gb_month.usd
            + hours * rates.public_ipv4_hour.usd
            + months * rates.secret_month.usd
        )
        assert _imputed_rds_day() == expected
        # And the four parts are separately visible, so a renderer can show the
        # instance charge is the overwhelming majority of it.
        lines = imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY)
        compute = next(line for line in lines if line.kind is CostKind.COMPUTE)
        assert compute.usd == hours * rates.rds_instance_hour.usd
        assert compute.usd > expected * Decimal("0.85")

    def test_one_modelled_idle_aurora_cluster_costs_storage_an_address_and_a_secret(
        self,
    ) -> None:
        rates = RateCard()
        shape = InstallationShape()
        hours = _A_DAY.hours
        months = _A_DAY.months
        aurora = tuple(
            line
            for line in imputed_round_carrying_lines(RoundId.ANALYZE_LIVE_ORDERS, _A_DAY)
            if "Aurora" in line.component
        )
        expected = (
            hours * shape.aurora_min_acu * rates.aurora_acu_hour.usd
            + months * shape.aurora_storage_gb * rates.aurora_storage_gb_month.usd
            + hours * rates.public_ipv4_hour.usd
            + months * rates.secret_month.usd
        )
        assert imputed_total_usd(aurora) == expected

    def test_a_resize_moves_the_modelled_instance_and_nothing_else(self) -> None:
        base = _imputed_rds_day()
        large = _imputed_rds_day(rates=RateCard(rds_instance_class="db.t4g.large"))
        delta = _A_DAY.hours * (
            rds_instance_hour_usd("db.t4g.large") - rds_instance_hour_usd("db.t4g.medium")
        )
        assert large - base == delta

    def test_the_modelled_instance_scales_with_the_window(self) -> None:
        # Prorating across a 730-hour month is a non-terminating division, so a
        # doubled window and a doubled day can disagree in the 28th place. The
        # linearity is the claim; the last place is Decimal's context.
        two_days = CarryingWindow(seconds=_A_DAY.seconds * 2)
        doubled = imputed_total_usd(
            imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, two_days)
        )
        assert abs(doubled - _imputed_rds_day() * 2) < Decimal("1e-24")


class TestTheTwoTotalsNeverMerge:
    def test_the_installation_estimate_never_emits_an_imputed_line(self) -> None:
        # What this installation pays is a different question, and these resources
        # do not exist. The separation is enforced at the producer.
        carrying = estimate_carrying_cost(_A_DAY)
        assert not any(line.imputed for line in carrying.lines)
        assert imputed_total_usd(carrying.lines) == Decimal(0)

    def test_no_bout_estimate_emits_an_imputed_line_either(self) -> None:
        for round_id in RoundId:
            estimate = estimate_bout_cost(
                _telemetry(round_id, CompetitorId.AURORA_SERVERLESS_V2)
            )
            assert not any(line.imputed for line in estimate.lines)

    def test_the_counterfactual_total_carries_only_imputed_lines(self) -> None:
        equivalent = customer_equivalent_carrying_cost(_A_DAY)
        assert equivalent.lines
        assert all(line.imputed for line in equivalent.lines)
        with pytest.raises(ValueError, match="only imputed lines"):
            CustomerEquivalent(
                by_round=((RoundId.WAKE_IDLE_APP, estimate_carrying_cost(_A_DAY).lines[:1]),),
                floor=False,
                floor_reason="",
                unpriced_services=(),
            )

    def test_the_counterfactual_covers_exactly_the_three_unprovisioned_rounds(self) -> None:
        equivalent = customer_equivalent_carrying_cost(_A_DAY)
        assert set(equivalent.rounds) == set(IMPUTED_RDS_ROUNDS)

    def test_the_counterfactual_is_a_floor_and_says_why_in_the_model(self) -> None:
        equivalent = customer_equivalent_carrying_cost(_A_DAY)
        assert equivalent.floor is True
        assert equivalent.floor_reason == CUSTOMER_EQUIVALENT_FLOOR_REASON
        assert "floor rather than an estimate" in equivalent.floor_reason
        assert "Rounds 4 and 6" in equivalent.floor_reason
        assert equivalent.unpriced_services == UNPRICED_PIPELINE_SERVICES

    def test_the_floor_claim_follows_the_rounds_that_make_it_true(self) -> None:
        # Round 1 alone is not a floor: its own Aurora cluster is real, and nothing
        # about it is missing a pipeline service. The claim is not a blanket caveat.
        round_one = customer_equivalent_carrying_cost(
            _A_DAY, rounds=(RoundId.WAKE_IDLE_APP,)
        )
        assert round_one.floor is False
        assert round_one.floor_reason == ""
        assert round_one.unpriced_services == ()
        for round_id in (RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS):
            assert customer_equivalent_carrying_cost(_A_DAY, rounds=(round_id,)).floor is True

    def test_a_floor_cannot_be_declared_without_saying_why(self) -> None:
        with pytest.raises(ValueError, match="why it is a floor"):
            CustomerEquivalent(
                by_round=(
                    (
                        RoundId.WAKE_IDLE_APP,
                        imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY),
                    ),
                ),
                floor=True,
                floor_reason="",
                unpriced_services=(),
            )

    def test_naming_an_unpriced_service_cannot_be_done_without_the_floor(self) -> None:
        with pytest.raises(ValueError, match="makes the figure a floor"):
            CustomerEquivalent(
                by_round=(
                    (
                        RoundId.WAKE_IDLE_APP,
                        imputed_round_carrying_lines(RoundId.WAKE_IDLE_APP, _A_DAY),
                    ),
                ),
                floor=False,
                floor_reason="",
                unpriced_services=("DMS",),
            )

    def test_the_counterfactual_can_be_taken_apart_by_round_again(self) -> None:
        equivalent = customer_equivalent_carrying_cost(_A_DAY)
        assert imputed_total_usd(
            equivalent.for_round(RoundId.WAKE_IDLE_APP)
        ) == _imputed_rds_day()
        assert equivalent.usd == sum(
            (imputed_total_usd(equivalent.for_round(round_id)) for round_id in equivalent.rounds),
            Decimal(0),
        )

    def test_the_customer_total_is_the_installation_total_plus_the_imputed_lines(self) -> None:
        # The sum is only ever formed with both halves named. Three modelled RDS
        # instances and two modelled idle clusters, on top of what is really billed.
        after = InstallationShape()
        installation = _aws_usd(estimate_carrying_cost(_A_DAY, shape=after))
        equivalent = customer_equivalent_carrying_cost(_A_DAY, shape=after)
        aurora_day = imputed_total_usd(
            tuple(
                line
                for line in imputed_round_carrying_lines(
                    RoundId.ANALYZE_LIVE_ORDERS, _A_DAY, shape=after
                )
                if "Aurora" in line.component
            )
        )
        parts = 3 * _imputed_rds_day(shape=after) + 2 * aurora_day
        assert abs(equivalent.usd - parts) < Decimal("1e-24")
        assert equivalent.usd > 0
        # The two never collapse into one number.
        assert installation != installation + equivalent.usd


class TestTheDeletionIdentities:
    """The two strongest checks available, because both are identities.

    If either fails, the derivation disagrees with what is on screen, and the
    figure on screen is not the thing to adjust.
    """

    def test_the_model_reproduces_the_shipped_aws_standing_figure(self) -> None:
        # `$10.12/day AWS` was the published figure while four RDS instances,
        # eight addresses and ten managed secrets stood. Round 1's instance has
        # since been deleted, so the sealed shape is three/seven/nine and the
        # installation now carries `$8.35/day`. Both are pinned: the older figure
        # because an audience may have written it down, the current one because it
        # is what the panel prints.
        before = InstallationShape().with_r1_rds_instance()
        assert before.rds_instances == 4
        assert _aws_usd(estimate_carrying_cost(_A_DAY, shape=before)).quantize(
            Decimal("0.01")
        ) == Decimal("10.12")
        now = InstallationShape()
        assert (now.rds_instances, now.public_ipv4_addresses, now.managed_secrets) == (3, 7, 9)
        assert _aws_usd(estimate_carrying_cost(_A_DAY, shape=now)).quantize(
            Decimal("0.01")
        ) == Decimal("8.35")

    def test_removing_r1s_instance_takes_exactly_one_imputed_line_off_the_bill(self) -> None:
        # The internal consistency check the whole imputation rests on: what this
        # installation stopped paying is, to the last place, what a customer keeps
        # paying. If these two disagreed, one of them would be wrong.
        after = InstallationShape()
        before = after.with_r1_rds_instance()
        fall = _aws_usd(estimate_carrying_cost(_A_DAY, shape=before)) - _aws_usd(
            estimate_carrying_cost(_A_DAY, shape=after)
        )
        assert fall == _imputed_rds_day(shape=after)
        assert fall == _imputed_rds_day(shape=before)

    def test_the_deletion_removes_an_instance_an_address_and_a_secret_and_nothing_else(
        self,
    ) -> None:
        after = InstallationShape()
        before = after.with_r1_rds_instance()
        assert (after.rds_instances, after.public_ipv4_addresses, after.managed_secrets) == (
            before.rds_instances - 1,
            before.public_ipv4_addresses - 1,
            before.managed_secrets - 1,
        )
        # Aurora is untouched: Round 1 keeps the only lane that can compete in it.
        assert after.aurora_clusters == before.aurora_clusters
        assert after.aurora_min_acu == before.aurora_min_acu
        assert after.rds_allocated_gb == before.rds_allocated_gb
        assert after.lakebase_projects == before.lakebase_projects

    def test_the_deletion_moves_our_bill_and_leaves_a_customers_untouched(self) -> None:
        # The two totals separate here, and this is the sharpest way to say it.
        # Before the deletion r1 has a real instance and owes no counterfactual;
        # after it, r1 owes one. So what *we* pay falls by exactly one instance-day
        # while what a *customer* pays does not move at all -- a customer's bill was
        # never a function of what we chose to provision. The gap between the two
        # widens by precisely the line we started imputing.
        after = InstallationShape()
        before = after.with_r1_rds_instance()
        installation_before = _aws_usd(estimate_carrying_cost(_A_DAY, shape=before))
        installation_after = _aws_usd(estimate_carrying_cost(_A_DAY, shape=after))
        customer_before = (
            installation_before
            + customer_equivalent_carrying_cost(
                _A_DAY,
                shape=before,
                rounds=(RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS),
            ).usd
        )
        customer_after = (
            installation_after + customer_equivalent_carrying_cost(_A_DAY, shape=after).usd
        )
        assert installation_after < installation_before
        assert customer_after == customer_before
        gap_before = customer_before - installation_before
        gap_after = customer_after - installation_after
        assert gap_after - gap_before == _imputed_rds_day()


class TestTheIdleContrastIsOneObject:
    """Three engines, one idle minute, related in the model rather than on screen."""

    def test_the_three_lanes_arrive_together_with_their_descent_intervals(self) -> None:
        contrast = idle_contrast()
        assert [lane.label for lane in contrast.lanes] == [
            "Lakebase",
            "Aurora Serverless v2",
            "RDS PostgreSQL",
        ]
        assert contrast.lane("Lakebase").descent_seconds == LAKEBASE_SUSPEND_SECONDS
        assert contrast.lane("Aurora Serverless v2").descent_seconds == AURORA_AUTO_PAUSE_SECONDS
        # None is a different claim from a long interval, and it is the reason the
        # figures differ at all.
        assert contrast.lane("RDS PostgreSQL").descent_seconds is None

    def test_the_multiple_is_derived_in_the_model_not_by_a_renderer(self) -> None:
        contrast = idle_contrast()
        rds = contrast.lane("RDS PostgreSQL")
        aurora = contrast.lane("Aurora Serverless v2")
        assert contrast.multiple == rds.usd_per_day / aurora.usd_per_day
        assert contrast.multiple.quantize(Decimal(1)) == Decimal(13)

    def test_the_ranking_is_stated_rather_than_left_to_be_reconstructed(self) -> None:
        # A previous surface mislabelled this comparison "Lakebase costs more than
        # everything" by relating the numbers at render time. The model names which
        # lane is dearest, so the label cannot be derived wrongly.
        contrast = idle_contrast()
        assert contrast.dearest.label == "RDS PostgreSQL"
        assert contrast.cheapest.label == "Aurora Serverless v2"

    def test_an_unpriced_lakebase_lane_is_never_ranked(self) -> None:
        contrast = idle_contrast()
        lakebase = contrast.lane("Lakebase")
        assert lakebase.usd_per_day is None
        assert lakebase.provenance is Provenance.UNAVAILABLE
        assert "unpriced rather than zero" in lakebase.basis
        assert contrast.dearest is not lakebase
        assert contrast.cheapest is not lakebase

    def test_a_supplied_lakebase_figure_is_measured_and_not_imputed(self) -> None:
        contrast = idle_contrast(
            lakebase_idle_usd_per_day=Decimal("0.05"),
            lakebase_idle_basis="system.billing.usage STORAGE_SPACE rows for the sealed projects",
        )
        lakebase = contrast.lane("Lakebase")
        assert lakebase.provenance is Provenance.MEASURED
        assert lakebase.imputed is False
        assert contrast.cheapest.label == "Lakebase"
        assert contrast.dearest.label == "RDS PostgreSQL"

    def test_the_competitor_lanes_are_marked_modelled_because_neither_exists(self) -> None:
        contrast = idle_contrast()
        for label in ("Aurora Serverless v2", "RDS PostgreSQL"):
            lane = contrast.lane(label)
            assert lane.imputed is True
            assert lane.provenance is Provenance.MODELED

    def test_the_compute_halves_are_the_sharpest_part_of_the_contrast(self) -> None:
        contrast = idle_contrast()
        rds = contrast.lane("RDS PostgreSQL")
        aurora = contrast.lane("Aurora Serverless v2")
        assert rds.compute_usd_per_day == _A_DAY.hours * RateCard().rds_instance_hour.usd
        assert aurora.compute_usd_per_day == Decimal(0)

    def test_the_summary_derives_every_figure_and_interval_it_states(self) -> None:
        contrast = idle_contrast()
        summary = contrast.summary
        assert f"descends after {LAKEBASE_SUSPEND_SECONDS}s" in summary
        assert f"after {AURORA_AUTO_PAUSE_SECONDS}s" in summary
        assert "never descends" in summary
        assert "about 13x more per idle day" in summary
        assert "$1.56/day against exactly $0.00/day" in summary

    def test_the_summary_moves_with_the_rate_card(self) -> None:
        # The trap this avoids: a sentence that restates its own fixture reads
        # correct forever. Resize the instance and the sentence must move.
        large = idle_contrast(rates=RateCard(rds_instance_class="db.t4g.large"))
        expected = _A_DAY.hours * rds_instance_hour_usd("db.t4g.large")
        assert f"${expected:.2f}/day against exactly" in large.summary
        assert "$1.56/day" not in large.summary
        assert large.multiple > idle_contrast().multiple

    def test_a_contrast_cannot_rank_a_lane_it_could_not_price(self) -> None:
        contrast = idle_contrast()
        lakebase = contrast.lane("Lakebase")
        with pytest.raises(ValueError, match="only rank lanes whose figure is known"):
            replace(contrast, dearest=lakebase)

    def test_a_lane_with_no_figure_must_say_so_rather_than_read_as_zero(self) -> None:
        with pytest.raises(ValueError, match="must be marked unavailable"):
            IdleContrastLane(
                label="Lakebase",
                descent_seconds=LAKEBASE_SUSPEND_SECONDS,
                usd_per_day=None,
                compute_usd_per_day=None,
                provenance=Provenance.MEASURED,
                imputed=False,
                basis="nothing was read",
            )


class TestAggregation:
    def test_totals_split_by_cloud_and_kind_agree_with_the_whole(self) -> None:
        # Prorating a lifetime across a 730-hour month produces a non-terminating
        # decimal, so regrouping the same lines can disagree in the 28th
        # significant digit. A picodollar is nine orders of magnitude below the
        # smallest number this analysis reports.
        picodollar = Decimal("1e-12")
        estimate = estimate_bout_cost(
            _telemetry(RoundId.RECOVER_DELETED_ORDER, observed_lakebase_dbu=Decimal("0.2"))
        )
        total = estimate.total_usd().quantize(picodollar)
        assert sum(estimate.by_cloud().values()).quantize(picodollar) == total
        assert sum(estimate.by_kind().values()).quantize(picodollar) == total

    def test_an_unavailable_line_is_excluded_from_the_total_and_listed(self) -> None:
        estimate = estimate_bout_cost(_telemetry(RoundId.RECOVER_DELETED_ORDER))
        assert len(estimate.unavailable) == 1
        assert estimate.total_usd() > 0


def _snapshot(
    *,
    round_id: RoundId = RoundId.RECOVER_DELETED_ORDER,
    competitor_id: CompetitorId = CompetitorId.RDS_POSTGRES,
    run_started_at: datetime | None = datetime(2026, 8, 21, 0, 2, 28, tzinfo=UTC),
    updated_at: datetime = datetime(2026, 8, 21, 0, 16, 0, tzinfo=UTC),
    lakebase_ms: float | None = 17432.582375,
    competitor_ms: float | None = 811308.759458,
    round5_setup: object | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        run_started_at=run_started_at,
        updated_at=updated_at,
        round=SimpleNamespace(id=round_id),
        competitor=SimpleNamespace(id=competitor_id),
        lanes={
            "lakebase": SimpleNamespace(elapsed_ms=lakebase_ms),
            "competitor": SimpleNamespace(elapsed_ms=competitor_ms),
        },
        round5_setup=round5_setup,
    )


class TestTelemetryFromSnapshot:
    def test_a_declared_bout_becomes_priceable_telemetry(self) -> None:
        telemetry = telemetry_from_snapshot(_snapshot())  # type: ignore[arg-type]
        assert telemetry is not None
        assert telemetry.bout_seconds == Decimal("812.0")
        assert telemetry.competitor_lane_seconds == Decimal("811.308759458")
        assert telemetry.lakebase_lane_seconds == Decimal("17.432582375")

    def test_a_snapshot_with_no_usable_window_yields_no_telemetry(self) -> None:
        instant = datetime(2026, 8, 21, 0, 2, 28, tzinfo=UTC)
        cases: tuple[tuple[str, SimpleNamespace], ...] = (
            ("the bout never started", _snapshot(run_started_at=None)),
            (
                "the window is not positive",
                _snapshot(run_started_at=instant, updated_at=instant),
            ),
        )
        for name, snapshot in cases:
            assert telemetry_from_snapshot(snapshot) is None, name  # type: ignore[arg-type]

    def test_round_five_prefers_its_setup_clock(self) -> None:
        setup = SimpleNamespace(
            lanes={
                "lakebase": SimpleNamespace(setup_elapsed_ms=3963.235042),
                "competitor": SimpleNamespace(setup_elapsed_ms=792607.613709),
            }
        )
        telemetry = telemetry_from_snapshot(
            _snapshot(  # type: ignore[arg-type]
                round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
                lakebase_ms=None,
                competitor_ms=None,
                round5_setup=setup,
            )
        )
        assert telemetry is not None
        assert telemetry.competitor_lane_seconds == Decimal("792.607613709")

    def test_a_missing_lane_becomes_none_rather_than_zero(self) -> None:
        telemetry = telemetry_from_snapshot(_snapshot(competitor_ms=None))  # type: ignore[arg-type]
        assert telemetry is not None
        assert telemetry.competitor_lane_seconds is None

    def test_a_provider_observation_overrides_the_lane_clock(self) -> None:
        telemetry = telemetry_from_snapshot(
            _snapshot(),  # type: ignore[arg-type]
            observed_restore_lifetime_seconds=Decimal("930"),
        )
        assert telemetry is not None
        assert telemetry.observed_restore_lifetime_seconds == Decimal("930")
        estimate = estimate_bout_cost(telemetry)
        compute = next(line for line in estimate.lines if "PITR restore compute" in line.component)
        assert compute.quantity.provenance is Provenance.MEASURED

    def test_a_none_observation_is_ignored_rather_than_overriding(self) -> None:
        telemetry = telemetry_from_snapshot(
            _snapshot(),  # type: ignore[arg-type]
            observed_restore_lifetime_seconds=None,
        )
        assert telemetry is not None
        assert telemetry.observed_restore_lifetime_seconds is None


class TestReconciliation:
    def test_what_a_row_derives_from_an_estimate_and_an_actual(self) -> None:
        """One row per case: the figures in, the derived attributes out.

        A missing actual has to produce None on every derived attribute rather
        than a zero, which is why that row names all three.
        """

        cases: tuple[tuple[str, dict, dict], ...] = (
            (
                "error is estimate minus posted",
                dict(
                    label="R3",
                    cloud=Cloud.AWS,
                    estimated_usd=Decimal("0.0054"),
                    posted_usd=Decimal("0.0050"),
                ),
                {
                    "error_usd": Decimal("0.0004"),
                    "error_fraction": Decimal("0.0004") / Decimal("0.0050"),
                },
            ),
            (
                "no posted actual",
                dict(
                    label="R6",
                    cloud=Cloud.DATABRICKS,
                    estimated_usd=Decimal("0.10"),
                    posted_usd=None,
                ),
                {
                    "error_usd": None,
                    "error_fraction": None,
                    "posted_within_band": None,
                },
            ),
            (
                "posted inside the band",
                dict(
                    label="R2",
                    cloud=Cloud.AWS,
                    estimated_usd=Decimal("0.010"),
                    posted_usd=Decimal("0.011"),
                    estimate_low_usd=Decimal("0.009"),
                    estimate_high_usd=Decimal("0.013"),
                ),
                {"posted_within_band": True},
            ),
            (
                "posted outside the band",
                dict(
                    label="R2",
                    cloud=Cloud.AWS,
                    estimated_usd=Decimal("0.010"),
                    posted_usd=Decimal("0.030"),
                    estimate_low_usd=Decimal("0.009"),
                    estimate_high_usd=Decimal("0.013"),
                ),
                {"posted_within_band": False},
            ),
        )

        for name, kwargs, expected in cases:
            row = Reconciliation(**kwargs)
            for attribute, value in expected.items():
                found = getattr(row, attribute)
                # `is` for the sentinels, so a truthy stand-in cannot pass for
                # True and a zero cannot pass for None, which is the whole
                # distinction the second row exists to make.
                if value is None or isinstance(value, bool):
                    assert found is value, f"{name}.{attribute}"
                else:
                    assert found == value, f"{name}.{attribute}"

    def test_aggregate_error_only_counts_rows_that_have_an_actual(self) -> None:
        report = ReconciliationReport(
            rows=(
                Reconciliation("R1", Cloud.AWS, Decimal("1.00"), Decimal("1.20")),
                Reconciliation("R2", Cloud.AWS, Decimal("2.00"), None),
            )
        )
        assert report.total_estimated_usd == Decimal("3.00")
        assert report.total_posted_usd == Decimal("1.20")
        assert report.total_error_usd == Decimal("-0.20")
        assert report.coverage == (1, 2)

    def test_an_empty_report_has_no_posted_total(self) -> None:
        report = ReconciliationReport()
        assert report.total_posted_usd is None
        assert report.total_error_usd is None
        assert report.coverage == (0, 0)
