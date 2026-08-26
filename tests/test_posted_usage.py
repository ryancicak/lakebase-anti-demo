"""Reading what Databricks posted, without waking the thing being measured.

Three kinds of test, and the first is the one that matters most.

The safety tests pin that this path is a Delta read and nothing else. The
control-plane call it must never make -- CDF or change-feed status through
Lakebase -- wakes the endpoint and bills the subject of the measurement, twice
reproduced, and the cheapest guard against it coming back is an assertion over
the statement this module actually sends.

The parsing tests pin that a shortfall surfaces. A meter whose rows did not join
a price is dropped rather than priced at zero, which turns into an unpriced lane
rather than a smaller total, and an understated Databricks figure is the one
error that would flatter us.

The degradation tests pin that losing billing costs the variance and nothing
else. Every expectation here is derived from the rows the test fed in or from
``RateCard()``, never restated as a literal: an assertion that repeats its own
fixture passes when both drift together.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from server.cost_model import InstallationShape, RateCard
from server.models import StandingCostLaneId
from server.posted_usage import (
    PostedUsageCache,
    PostedUsageScope,
    PostedUsageScopeError,
    posted_usage_from_rows,
    posted_usage_query,
    read_posted_databricks_usage,
    scope_from_manifest,
    warehouse_query_executor,
)
from server.standing_cost import ROUND4_PIPELINE_LABEL, build_standing_cost_disclosure

ORIGIN = datetime(2026, 8, 20, 14, 46, 33, tzinfo=UTC)
NOW = datetime(2026, 8, 21, 13, 30, tzinfo=UTC)

APP_ID = "11111111-1111-4111-8111-111111111111"
PIPELINE_ID = "22222222-2222-4222-8222-222222222222"


class FakeLakebase:
    def __init__(self, host: str) -> None:
        self.direct_host = host


class FakeEnvironment:
    def __init__(self, host: str) -> None:
        self.lakebase = FakeLakebase(host)


class FakeRoundFour:
    app_service_principal_client_id = APP_ID
    pipeline_id = PIPELINE_ID


class FakeManifest:
    """A seal that names its endpoints by host, exactly as the real one does."""

    run_id = "v7-posted-usage-test"
    created_at = ORIGIN
    round4 = FakeRoundFour()

    def __init__(self) -> None:
        region = "database.us-west-2.cloud.databricks.com"
        self.round_environments = {
            "r1": FakeEnvironment(f"ep-example-one-d1000001.{region}"),
            # The pooled alias resolves to the same endpoint billing names.
            "r6": FakeEnvironment(f"ep-example-six-d1000006-pooler.{region}"),
        }
        self.coordination_environment = FakeLakebase(
            "ep-example-ring-d1000000.database.us-west-2.cloud.databricks.com"
        )


def scope() -> PostedUsageScope:
    return scope_from_manifest(FakeManifest())


def stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def window_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "kind": "window",
        "identifier": "",
        "usage_unit": "",
        "qty": "59.8",
        "usd": "33.71168285446834",
        "unpriced_rows": "0",
        "rows_n": "787",
        "first_start": stamp(datetime(2026, 8, 20, tzinfo=UTC)),
        "last_end": stamp(datetime(2026, 8, 21, 13, 20, tzinfo=UTC)),
    }
    row.update(overrides)
    return row


def storage_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "kind": "lakebase_storage",
        "identifier": "",
        "usage_unit": "DSU",
        "qty": "0.13584114629301586",
        "usd": "0.003124346364739365",
        "unpriced_rows": "0",
        "rows_n": "330",
        "first_start": stamp(datetime(2026, 8, 20, 14, tzinfo=UTC)),
        "last_end": stamp(datetime(2026, 8, 21, 13, tzinfo=UTC)),
    }
    row.update(overrides)
    return row


def platform_row(
    identifier: str,
    *,
    qty: str,
    usd: str,
    first_start: datetime,
    last_end: datetime,
    unpriced_rows: str = "0",
    metered_seconds: str | None = None,
) -> dict[str, object]:
    """One aggregated platform meter, as the query returns it.

    ``metered_seconds`` defaults to the whole span, which is what a meter that
    never stops actually posts: back-to-back intervals with no gaps. A row that
    wants to describe an intermittent meter passes a smaller number, and the
    ratio between the two is its duty cycle.
    """

    span = Decimal(str((last_end - first_start).total_seconds()))
    return {
        "kind": "platform",
        "identifier": identifier,
        "usage_unit": "DBU",
        "qty": qty,
        "usd": usd,
        "unpriced_rows": unpriced_rows,
        "rows_n": "36",
        "first_start": stamp(first_start),
        "last_end": stamp(last_end),
        "metered_seconds": metered_seconds if metered_seconds is not None else str(span),
    }


def app_row(**overrides: object) -> dict[str, object]:
    row = platform_row(
        APP_ID,
        qty="18.0",
        usd="17.1",
        # Before the seal: the app was already serving when this run was created.
        first_start=datetime(2026, 8, 20, tzinfo=UTC),
        last_end=datetime(2026, 8, 21, 12, tzinfo=UTC),
    )
    row.update(overrides)
    return row


def pipeline_row(**overrides: object) -> dict[str, object]:
    row = platform_row(
        PIPELINE_ID,
        qty="30.378947647637617",
        usd="13.670526441436921",
        # After the seal: this run created it.
        first_start=datetime(2026, 8, 20, 15, tzinfo=UTC),
        last_end=datetime(2026, 8, 21, 13, 10, tzinfo=UTC),
    )
    row.update(overrides)
    return row


def observed(*rows: dict[str, object]):
    return posted_usage_from_rows(rows or (window_row(),), scope())


def disclosure(posted):
    return build_standing_cost_disclosure(FakeManifest(), now=NOW, posted=posted)


def lane(built, lane_id: StandingCostLaneId):
    (found,) = [item for item in built.lanes if item.lane_id is lane_id]
    return found


def component(posted, identifier: str):
    (found,) = [item for item in posted.platform if identifier in item.attribution]
    return found


class TestTheReadWakesNothing:
    """The property this whole path exists to keep."""

    def test_the_statement_reads_delta_and_makes_no_control_plane_call(self):
        sql = posted_usage_query(scope()).lower()
        assert "system.billing.usage" in sql
        assert "system.billing.list_prices" in sql
        # Reading CDF or change-feed status through the Lakebase control plane
        # wakes the endpoint and bills the subject of the measurement. It was
        # reproduced twice and there is no version of this read that needs it.
        for forbidden in ("cdf", "change_feed", "changefeed", "get_endpoint", "get-endpoint"):
            assert forbidden not in sql
        # And every table this statement reads is either a billing table or one
        # of its own CTEs, so no other system is touched to build the figure.
        targets = {
            match.rstrip(")") for match in re.findall(r"(?:from|join)\s+([^\s(]+)", sql)
        }
        assert targets == {
            "system.billing.usage",
            "system.billing.list_prices",
            "sealed",
            "scoped",
            "priced",
        }

    def test_the_scope_comes_off_the_seal_rather_than_a_lookup(self):
        derived = scope()
        # The host is what the manifest carries; the endpoint id is its leading
        # label, with the pooled alias resolving to the same endpoint.
        assert derived.endpoint_ids == (
            "ep-example-one-d1000001",
            "ep-example-six-d1000006",
            "ep-example-ring-d1000000",
        )
        assert derived.app_ids == (FakeRoundFour.app_service_principal_client_id,)
        assert derived.pipeline_ids == (FakeRoundFour.pipeline_id,)
        # The floor is the created_at *date*, not the timestamp: the earlier rows
        # of that day are the evidence that the app predates the installation.
        assert derived.since == ORIGIN.date()
        assert derived.created_at == ORIGIN

    def test_an_identifier_that_is_not_an_identifier_is_refused_not_quoted(self):
        with pytest.raises(PostedUsageScopeError, match="not a plain identifier"):
            PostedUsageScope(endpoint_ids=("ep-real'; DROP TABLE usage; --",))

    def test_a_seal_naming_no_endpoint_cannot_scope_a_read(self):
        with pytest.raises(PostedUsageScopeError, match="nothing to scope itself to"):
            PostedUsageScope(endpoint_ids=())


class TestWhatTheRowsBecome:
    def test_each_meter_is_divided_by_its_own_covered_window(self):
        posted = observed(window_row(), storage_row(), app_row(), pipeline_row())
        app = component(posted, APP_ID)
        row = app_row()
        hours = Decimal(
            str(
                (
                    datetime.fromisoformat(str(row["last_end"]).replace("Z", "+00:00"))
                    - datetime.fromisoformat(str(row["first_start"]).replace("Z", "+00:00"))
                ).total_seconds()
                / 3600
            )
        )
        # Its own window, not the read's: the pipeline posts ten minutes later
        # than the app, and dividing both by the later end understates the app.
        assert app.dbu_per_hour == pytest.approx(Decimal(str(row["qty"])) / hours)
        assert component(posted, PIPELINE_ID).dbu_per_hour != app.dbu_per_hour

    def test_an_intermittent_meter_divides_by_uptime_and_a_continuous_one_by_its_span(
        self,
    ):
        """The denominator is a property of the component, not of the method.

        The pipeline is started at arm and released twenty minutes after its bout
        settles, so the span between its first and last posted interval is mostly
        hours it was down. Dividing by that span produced a duty-cycle-blended
        average of one installation's habits and rendered it as a rate: at the
        62.5% cycle the 2026-08-25 receipt sampled, it read about five eighths of
        the rate the pipeline actually bills at while it is up.

        The App's compute is the control, and it is the half most likely to be
        broken by a fix aimed at the other half. It runs continuously, so its
        span *is* its uptime and its denominator must not move.
        """

        # 20 h of uptime inside a 32 h span -- the receipt's own duty cycle.
        uptime = Decimal(20)
        span = Decimal(32)
        start = datetime(2026, 8, 20, 15, tzinfo=UTC)
        pipeline = pipeline_row(
            first_start=stamp(start),
            last_end=stamp(start + timedelta(hours=float(span))),
            metered_seconds=str(uptime * 3600),
        )
        posted = observed(window_row(), storage_row(), app_row(), pipeline)
        qty = Decimal(str(pipeline["qty"]))

        metered = component(posted, PIPELINE_ID)
        assert metered.dbu_per_hour == pytest.approx(qty / uptime)
        assert metered.dbu_per_hour != pytest.approx(qty / span)
        # And the uptime travels with it, so the amount can be reconstructed.
        assert metered.uptime_hours == uptime
        # The blend was the *lower* of the two, so the old figure under-warned.
        assert qty / span < qty / uptime

        # The continuous meter is untouched: still its own span, still no uptime
        # of its own to divide by, because the two are the same thing for it.
        app_only = app_row()
        app_span = Decimal(
            str(
                (
                    datetime.fromisoformat(str(app_only["last_end"]).replace("Z", "+00:00"))
                    - datetime.fromisoformat(str(app_only["first_start"]).replace("Z", "+00:00"))
                ).total_seconds()
                / 3600
            )
        )
        app = component(posted, APP_ID)
        assert app.uptime_hours is None
        assert app.dbu_per_hour == pytest.approx(Decimal(str(app_only["qty"])) / app_span)

    def test_the_panel_states_the_while_running_rate_and_the_amount_posted(self):
        """The two quantities the old arithmetic collapsed into one wrong one.

        `docs/PRICING.md` recorded this as an open defect: every document
        published the while-running rate while the app's own panel rendered the
        span-divided blend, so a stranger running the app saw it contradict its
        own README. The rate and the accrued amount are different questions and
        the panel has to answer both -- the rate over the hours it was up, the
        amount over the intervals actually posted.
        """

        uptime = Decimal(20)
        span = Decimal(32)
        start = datetime(2026, 8, 20, 15, tzinfo=UTC)
        pipeline = pipeline_row(
            first_start=stamp(start),
            last_end=stamp(start + timedelta(hours=float(span))),
            metered_seconds=str(uptime * 3600),
        )
        built = disclosure(observed(window_row(), storage_row(), app_row(), pipeline))
        continuous = built.continuous
        assert continuous.state == "stated"

        observed_rate = Decimal(str(pipeline["usd"])) / uptime
        # The rate is the while-running one, and the per-day figure beside it is
        # that rate times 24 rather than an amount spread over the window.
        assert Decimal(str(continuous.usd_per_hour)) == pytest.approx(observed_rate)
        assert Decimal(str(continuous.usd_per_day)) == pytest.approx(observed_rate * 24)
        # And the accrued amount is what was posted, not the rate extrapolated
        # across a window the pipeline was down for most of.
        component_line = next(
            item
            for lane_item in built.lanes
            for item in lane_item.components
            if item.component == ROUND4_PIPELINE_LABEL
        )
        assert Decimal(str(component_line.figure.usd_per_day)) == pytest.approx(
            observed_rate * 24
        )
        # The lane's own rate and the paragraph's are one derivation, not two.
        assert Decimal(str(component_line.figure.usd_per_hour)) == pytest.approx(
            Decimal(str(continuous.usd_per_hour))
        )

    def test_the_price_handed_on_is_the_effective_one(self):
        posted = observed(window_row(), storage_row(), app_row())
        row = app_row()
        # Dollars over quantity, so an interval that crossed a price change
        # reports what it cost rather than whichever side of the change won.
        assert component(posted, APP_ID).usd_per_dbu == pytest.approx(
            Decimal(str(row["usd"])) / Decimal(str(row["qty"]))
        )

    def test_a_meter_that_began_before_the_seal_predates_the_installation(self):
        posted = observed(window_row(), storage_row(), app_row(), pipeline_row())
        app = component(posted, APP_ID)
        pipeline = component(posted, PIPELINE_ID)
        # Derived from the row's own first interval against created_at, not
        # asserted: the app was serving before this run_id existed and would bill
        # without it, which is the whole reason the two totals are separable.
        assert app.predates_installation is True
        assert "before this installation was created" in app.attribution
        assert pipeline.predates_installation is False

    def test_the_app_lands_outside_the_installation_total_and_inside_the_other(self):
        built = disclosure(observed(window_row(), storage_row(), app_row(), pipeline_row()))
        totals = built.totals
        assert totals is not None
        app = component(observed(window_row(), storage_row(), app_row()), APP_ID)
        gap = Decimal(str(totals.with_platform.usd_per_hour)) - Decimal(
            str(totals.installation.usd_per_hour)
        )
        assert gap == pytest.approx(app.usd_per_hour)
        assert "predates this installation" in totals.with_platform.condition


class TestAShortfallSurfaces:
    def test_a_meter_whose_rows_did_not_price_takes_its_lane_unpriced(self):
        priced = disclosure(observed(window_row(), storage_row(), app_row(), pipeline_row()))
        assert lane(priced, StandingCostLaneId.DATABRICKS_PLATFORM).figure.state == "priced"

        short = observed(
            window_row(),
            storage_row(),
            app_row(unpriced_rows="4"),
            pipeline_row(),
        )
        # The component is dropped, not priced at zero.
        assert [item.predates_installation for item in short.platform] == [False]
        built = disclosure(short)
        lane_figure = lane(built, StandingCostLaneId.DATABRICKS_PLATFORM).figure
        # The row that failed to price is the app's, which predates the
        # installation and is held out of the lane subtotal either way -- the lane
        # figure is that lane's share of totals.installation, so it is the same
        # figure here as it is with the app priced.
        assert lane_figure.state == "priced"
        assert lane_figure.usd_per_day is not None
        assert lane_figure.usd_per_day == pytest.approx(
            lane(priced, StandingCostLaneId.DATABRICKS_PLATFORM).figure.usd_per_day
        )
        # The shortfall surfaces where the dropped component was actually counted:
        # the second total loses it, and the disclosure that named it goes with it
        # rather than standing next to a total that no longer includes it.
        totals, before = built.totals, priced.totals
        assert totals is not None and before is not None
        assert totals.with_platform.usd_per_day < before.with_platform.usd_per_day
        assert totals.with_platform.usd_per_day == pytest.approx(
            totals.installation.usd_per_day
        )
        assert built.predating is not None and built.predating.state == "withheld"
        assert priced.predating is not None and priced.predating.state == "stated"

    def test_no_platform_row_at_all_leaves_the_lane_unpriced_and_withholds_the_paragraph(self):
        built = disclosure(observed(window_row(), storage_row()))
        assert lane(built, StandingCostLaneId.DATABRICKS_PLATFORM).figure.state == "unavailable"
        # The paragraph concedes that our half is the larger one. With the
        # platform lane unpriced there is nothing behind the claim, so it goes.
        assert built.fairness.state == "withheld"
        assert built.fairness.paragraph == ""
        totals = built.totals
        assert totals is not None
        assert totals.installation.partial is True


class TestAnAbsenceIsNotAFailure:
    def test_no_always_on_minimum_row_is_a_measured_zero_with_its_basis(self):
        posted = observed(window_row(), storage_row(), app_row(), pipeline_row())
        assert posted.lakebase_dbu_per_hour == Decimal(0)
        assert "every endpoint scales to zero" in posted.lakebase_dbu_basis
        figure = [
            item.figure
            for item in lane(disclosure(posted), StandingCostLaneId.LAKEBASE).components
            if item.kind == "compute"
        ]
        (compute,) = figure
        # A zero is allowed only when its derivation travels on the same field,
        # so the rendered figure can never be a bare $0.00.
        assert compute.state == "structural_zero"
        assert compute.zero_basis
        assert compute.zero_basis in compute.display
        assert compute.display != "$0.00/day"

    def test_an_always_on_minimum_row_prices_from_its_own_window(self):
        always_on = {
            "kind": "lakebase_always_on",
            "identifier": "",
            "usage_unit": "DBU",
            "qty": "2.4",
            "usd": "0.624",
            "unpriced_rows": "0",
            "rows_n": "24",
            "first_start": stamp(datetime(2026, 8, 20, 15, tzinfo=UTC)),
            "last_end": stamp(datetime(2026, 8, 20, 19, tzinfo=UTC)),
        }
        posted = observed(window_row(), storage_row(), always_on, app_row())
        assert posted.lakebase_dbu_per_hour == Decimal("2.4") / Decimal(4)
        assert "COMPUTE_NODE_ALWAYS_ON_MIN" in posted.lakebase_dbu_basis
        # And it reaches the lane priced at the rate card rather than at a price
        # this module invented.
        compute = [
            item
            for item in lane(disclosure(posted), StandingCostLaneId.LAKEBASE).components
            if item.kind == "compute"
        ][0]
        assert compute.figure.state == "priced"
        assert compute.figure.usd_per_day == pytest.approx(
            float(Decimal("0.6") * RateCard().lakebase_dbu.usd * Decimal(24))
        )


class TestLosingBillingCostsTheVarianceAndNothingElse:
    def test_a_failing_query_becomes_unavailable_rather_than_a_raise(self):
        def explode(_: str):
            raise TimeoutError("statement execution timed out")

        posted = read_posted_databricks_usage(FakeManifest(), execute=explode)
        assert "TimeoutError" in posted.unavailable
        assert posted.posted_usd is None

    def test_the_degraded_disclosure_still_prices_the_aws_half_and_claims_no_zero(self):
        def explode(_: str):
            raise RuntimeError("PERMISSION_DENIED on system.billing.usage")

        built = disclosure(read_posted_databricks_usage(FakeManifest(), execute=explode))
        assert built.seal_state == "sealed"
        assert built.posted.state == "unavailable"
        assert "PERMISSION_DENIED" in built.posted.unavailable_reason
        # The projection is untouched: losing the ability to read billing does
        # not make the account stop spending.
        aws = lane(built, StandingCostLaneId.RDS).figure
        assert aws.state == "priced"
        assert aws.usd_per_day == pytest.approx(
            float(
                RateCard().public_ipv4_hour.usd * Decimal(24) * InstallationShape().rds_instances
                + Decimal(str(aws.usd_per_day))
                - RateCard().public_ipv4_hour.usd * Decimal(24) * InstallationShape().rds_instances
            )
        )
        # Both Databricks lanes are unpriced, so both totals say so rather than
        # quietly totalling less.
        for lane_id in (StandingCostLaneId.LAKEBASE, StandingCostLaneId.DATABRICKS_PLATFORM):
            assert lane(built, lane_id).figure.state == "unavailable"
        totals = built.totals
        assert totals is not None
        assert totals.installation.partial is True
        assert "could not be priced" in totals.installation.partial_reason
        assert built.fairness.state == "withheld"

    def test_a_read_that_returns_nothing_says_so_rather_than_reporting_zero(self):
        posted = read_posted_databricks_usage(FakeManifest(), execute=lambda _: [])
        assert "no usage rows" in posted.unavailable
        assert posted.posted_usd is None

    def test_no_manifest_is_a_stated_absence(self):
        posted = read_posted_databricks_usage(None, execute=lambda _: [])
        assert "no manifest is configured" in posted.unavailable


class TestThePostedComparison:
    def test_the_variance_is_measured_only_over_the_shared_window(self):
        posted = observed(window_row(), storage_row(), app_row(), pipeline_row())
        built = disclosure(posted)
        comparison = built.posted
        assert comparison.state == "posted_through_window"
        # The posted day starts before the installation did. The variance is
        # taken over the overlap alone; the whole-window projection is carried
        # separately and is not what was differenced.
        overlap = (
            min(comparison.window_end or NOW, NOW) - max(comparison.window_start or ORIGIN, ORIGIN)
        ).total_seconds() / 3600
        assert comparison.posted_hours == pytest.approx(overlap)
        assert comparison.projection_usd != comparison.projection_in_posted_window_usd
        assert comparison.variance_usd == pytest.approx(
            (comparison.posted_usd or 0) - (comparison.projection_in_posted_window_usd or 0)
        )

    def test_the_aws_half_never_wears_a_posted_label(self):
        built = disclosure(observed(window_row(), storage_row(), app_row(), pipeline_row()))
        assert built.posted.aws_posted == "no_posted_counterpart"
        assert "ce:GetCostAndUsage is denied" in built.posted.aws_posted_basis

    def test_the_word_verified_appears_nowhere_in_the_read_or_its_disclosure(self):
        built = disclosure(observed(window_row(), storage_row(), app_row(), pipeline_row()))
        assert "verified" not in built.model_dump_json().lower()


class TestTheWindowFloor:
    def test_the_query_floors_at_the_created_at_date_not_the_timestamp(self):
        sql = posted_usage_query(scope())
        assert f"DATE'{ORIGIN.date().isoformat()}'" in sql
        # A floor at the timestamp would drop the app's earlier rows for that
        # day, and those rows are the evidence that it predates the seal.
        assert ORIGIN.strftime("%H:%M") not in sql

    def test_a_seal_with_no_created_at_still_scopes_a_read(self):
        class Undated(FakeManifest):
            created_at = None

        derived = scope_from_manifest(Undated())
        assert derived.created_at is None
        assert "DATE'1970-01-01'" in posted_usage_query(derived)
        posted = posted_usage_from_rows(
            (window_row(), storage_row(), app_row()),
            derived,
        )
        # With no origin to compare against, nothing is claimed to predate it.
        assert [item.predates_installation for item in posted.platform] == [False]


class TestTheWholeWindowIsAccountedFor:
    def test_the_unposted_remainder_is_carried_rather_than_assumed_to_match(self):
        built = disclosure(observed(window_row(), storage_row(), app_row(), pipeline_row()))
        comparison = built.posted
        assert comparison.unposted_hours is not None
        assert comparison.unposted_hours > 0
        assert (comparison.posted_hours or 0) + comparison.unposted_hours == pytest.approx(
            built.elapsed_hours
        )
        assert "carried here rather than assumed to match" in comparison.unposted_basis

    def test_a_posted_window_that_predates_the_seal_entirely_is_not_differenced(self):
        stale = observed(
            window_row(
                first_start=stamp(ORIGIN - timedelta(days=3)),
                last_end=stamp(ORIGIN - timedelta(days=2)),
            ),
            storage_row(),
            app_row(),
            pipeline_row(),
        )
        comparison = disclosure(stale).posted
        assert comparison.state == "unavailable"
        assert "does not overlap" in comparison.unavailable_reason


class TestTheCacheKeepsTheWarehouseOffTheRequestPath:
    """The disclosure is rebuilt on every read; the warehouse must not be.

    The read this module performs took roughly fifteen seconds against the real
    installation. A panel whose subject is standing cost cannot be a panel that
    costs warehouse time to look at, so the value is refreshed on its own
    schedule and the request path only ever reads what is already in hand.
    """

    def test_current_calls_nothing_before_the_first_refresh(self):
        calls: list[str] = []

        def execute(statement: str):
            calls.append(statement)
            return [window_row()]

        cache = PostedUsageCache(FakeManifest(), execute=execute)
        # The state a session created during startup renders in, and the reason
        # it is safe to serve immediately rather than blocking on a warehouse.
        assert cache.current() is None
        assert calls == []

    def test_current_returns_the_last_refresh_without_reading_again(self):
        calls: list[str] = []

        def execute(statement: str):
            calls.append(statement)
            return [window_row(), storage_row()]

        cache = PostedUsageCache(FakeManifest(), execute=execute)
        cache.refresh()
        assert len(calls) == 1
        first = cache.current()
        assert first is not None
        assert not first.unavailable
        # A hundred renders of the panel, and still one query.
        for _ in range(100):
            assert cache.current() is first
        assert len(calls) == 1

    def test_an_unreadable_seal_degrades_rather_than_raising(self):
        cache = PostedUsageCache(None, execute=lambda statement: [window_row()])
        value = cache.refresh()
        assert value.unavailable
        assert cache.current() is value

    def test_a_seal_naming_no_warehouse_says_so_instead_of_guessing_one(self):
        # Starting a warehouse to measure standing cost would add standing cost.
        cache = PostedUsageCache(FakeManifest())
        value = cache.refresh()
        assert "no SQL warehouse is named in the seal" in value.unavailable

    def test_a_failing_query_leaves_an_unavailable_and_not_a_raise(self):
        def explode(statement: str):
            raise TimeoutError("the warehouse did not answer")

        cache = PostedUsageCache(FakeManifest(), execute=explode)
        value = cache.refresh()
        assert "TimeoutError" in value.unavailable
        # And the panel that reads it still has an AWS half and two totals.
        built = disclosure(value)
        assert built.posted.state == "unavailable"
        assert built.totals is not None
        assert built.fairness.state == "withheld"

    def test_the_refresh_interval_is_longer_than_the_table_publishes(self):
        # system.billing.usage publishes on an interval measured in tens of
        # minutes. Refreshing much faster would re-read a table that has not
        # changed, on a warehouse this installation is billed for.
        assert PostedUsageCache(None).interval_seconds >= 600


class TestTheExecutorBorrowsAWarehouseRatherThanStartingOne:
    def test_it_picks_a_warehouse_the_installation_already_owns(self):
        class Sealed(FakeManifest):
            class databricks:  # noqa: N801 - mirrors the manifest's own shape
                profile = "obsconsole-ws-2"

            class round4:  # noqa: N801
                app_service_principal_client_id = APP_ID
                pipeline_id = PIPELINE_ID
                warehouse_id = "0123456789abcdef"

        assert warehouse_query_executor(Sealed()) is not None

    def test_it_declines_rather_than_inventing_one(self):
        assert warehouse_query_executor(FakeManifest()) is None
