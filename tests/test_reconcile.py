"""Reconciliation tests: the seal, the account, and the difference between them.

The leak this module exists to catch was an Aurora clone writer that outlived its
bout by fifty-one minutes because the process that created it was cancelled a few
seconds later. Nothing raised, nothing logged, and the account simply carried a
resource nobody was tracking. So the cases that matter here are the ones where a
resource is present and unowned, or absent and expected, and in every one of them
the reconciliation is required to finish and report rather than raise.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from server.cost_model import RateCard
from server.manifest import DemoManifest
from server.models import RoundId
from server.reconcile import (
    AURORA_CLUSTER,
    AURORA_WRITER,
    EC2_RUNNER,
    IPV4_DRIFT,
    MISSING_RESIDENT,
    ORPHAN_EPHEMERAL,
    ORPHAN_FOREIGN_RUN,
    ORPHAN_UNEXPECTED,
    RDS_INSTANCE,
    RESIDENT,
    ObservedResource,
    _carrying_cost,
    ephemeral_artifact_ids,
    expected_resources,
    observed_from_descriptions,
    reconcile,
    reconcile_live,
)

RUN_ID = "ad-20260820-1446-abcd"
OTHER_RUN = "ad-20260819-0009-dcba"


def _aws_environment(number: int, *, rds: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        lakebase=SimpleNamespace(project_id=f"install-r{number}"),
        aurora=SimpleNamespace(
            cluster_id=f"seal-r{number}-aurora",
            writer_instance_id=f"seal-r{number}-aurora-writer",
        ),
        rds=SimpleNamespace(instance_id=f"seal-r{number}-rds") if rds else None,
    )


def _manifest() -> DemoManifest:
    """A v7 seal shaped like the real one after Round 1's RDS instance is removed.

    Four Aurora rounds, three RDS instances, two rounds with no AWS stack at all.
    Round 1 keeps its Aurora cluster -- it is the only engine that can compete in
    a wake-from-idle round -- and seals ``rds = None`` because its RDS lane
    refuses to enter on engine semantics and was never timed.
    """

    return DemoManifest.model_construct(
        manifest_version=7,
        run_id=RUN_ID,
        round_environments={
            RoundId.WAKE_IDLE_APP: _aws_environment(1, rds=False),
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY: _aws_environment(2),
            RoundId.RECOVER_DELETED_ORDER: _aws_environment(3),
            RoundId.PUT_MODEL_SCORE_IN_APP: SimpleNamespace(
                lakebase=SimpleNamespace(project_id="install-r4")
            ),
            RoundId.SURVIVE_CONNECTION_SPIKE: _aws_environment(5),
            RoundId.ANALYZE_LIVE_ORDERS: SimpleNamespace(
                lakebase=SimpleNamespace(project_id="install-r6")
            ),
        },
        round5=SimpleNamespace(runner_instance_id="i-0123456789abcdef0"),
    )


def _resident() -> list[ObservedResource]:
    """Exactly what a healthy installation runs between bouts."""

    observed: list[ObservedResource] = []
    for number in (1, 2, 3, 5):
        observed.append(
            ObservedResource(
                AURORA_CLUSTER, f"seal-r{number}-aurora", "available", run_id=RUN_ID
            )
        )
        observed.append(
            ObservedResource(
                AURORA_WRITER,
                f"seal-r{number}-aurora-writer",
                "available",
                run_id=RUN_ID,
                public_ipv4=True,
            )
        )
        if number == 1:
            continue
        observed.append(
            ObservedResource(
                RDS_INSTANCE,
                f"seal-r{number}-rds",
                "available",
                run_id=RUN_ID,
                public_ipv4=True,
            )
        )
    observed.append(
        ObservedResource(
            EC2_RUNNER, "i-0123456789abcdef0", "running", run_id=RUN_ID, public_ipv4=True
        )
    )
    return observed


def test_healthy_installation_reports_no_orphans_and_eight_addresses() -> None:
    """The standing cost is by design, and the reconciliation must say so.

    This is also the guard on the only way the Aurora ceiling can reach an
    operator as a wrong figure. Priced through ``_carrying_cost`` -- four writers
    at the 2-ACU ceiling, three RDS instances, one runner, eight addresses -- this
    exact fleet comes to just over $30/day against roughly $8.39 the account
    really bills, because the ceiling is the honest number for a leak and a
    multiple of the truth for an observably idle resident. That total is only
    reachable by pricing residents, and ``reconcile`` prices only what failed the
    seal match, so the two conditions are mutually exclusive by construction.

    Which makes silence the whole defence: a seal that holds must quote no price
    at all -- no total, no line, not a currency symbol. A later change that
    summed the observed inventory, or handed the residents to ``_carrying_cost``
    for a "completeness" total, would put the overstatement on screen and this is
    what notices.
    """

    report = reconcile(_manifest(), _resident())

    assert report.ok
    assert report.findings == ()
    # Four Aurora writers, three RDS instances, one runner. One address fewer
    # than before Round 1's RDS instance was removed.
    assert report.expected_public_ipv4 == 8
    assert report.observed_public_ipv4 == 8
    assert report.orphan_usd_per_day == Decimal(0)
    assert "no orphans" in report.summary()
    assert "$" not in report.summary()
    assert report.report_lines() == ()
    # Asserted so the silence above is not vacuous: there really is a large
    # number here to leak, and it is the one the original complaint measured.
    priced_as_leaks = sum(
        (_carrying_cost(resource, RateCard())[0] for resource in report.observed),
        Decimal(0),
    )
    assert priced_as_leaks > Decimal(30)


def test_expected_resources_ignores_rounds_that_seal_no_aws_stack() -> None:
    """Rounds 4 and 6 have no Aurora or RDS block; that is a fact, not a gap."""

    expected = expected_resources(_manifest())
    rounds = {resource.round_key for resource in expected}

    assert str(RoundId.PUT_MODEL_SCORE_IN_APP) not in rounds
    assert str(RoundId.ANALYZE_LIVE_ORDERS) not in rounds
    assert len(expected) == 12  # 4 clusters + 4 writers + 3 instances + 1 runner


def test_round_one_keeps_its_aurora_and_expects_no_rds_instance() -> None:
    """Removing r1's RDS must not take r1's Aurora with it, or invent an absence.

    Two ways this goes wrong. Drop r1 from the shared Terraform key list and the
    Aurora cluster disappears too, which would delete the only lane that can
    compete in Round 1. Leave the expectation at four RDS instances and every
    reconciliation reports a phantom MISSING_RESIDENT for a box nobody is paying
    for -- an operator chasing a resource that was removed on purpose.
    """

    report = reconcile(_manifest(), _resident())
    kinds = [(resource.kind, resource.round_key) for resource in report.expected]
    round_one = str(RoundId.WAKE_IDLE_APP)

    assert (AURORA_CLUSTER, round_one) in kinds
    assert (AURORA_WRITER, round_one) in kinds
    assert (RDS_INSTANCE, round_one) not in kinds
    assert [resource.kind for resource in report.expected].count(RDS_INSTANCE) == 3
    assert report.missing == ()
    assert report.ok


def test_the_expectation_follows_the_seal_rather_than_a_hardcoded_count() -> None:
    """A seal that still carries four RDS instances must still expect four.

    The count is not a constant to be decremented. If a future round provisions
    an RDS instance again, or an older seal is reconciled, the expectation has to
    move with the seal on its own.
    """

    manifest = _manifest()
    manifest.round_environments[RoundId.WAKE_IDLE_APP] = _aws_environment(1)

    expected = expected_resources(manifest)

    assert [resource.kind for resource in expected].count(RDS_INSTANCE) == 4
    assert len(expected) == 13


def test_leaked_bout_writer_is_named_priced_and_attributed_to_its_round() -> None:
    """The exact shape of the incident: a Round 2 clone writer left running."""

    leaked = f"adsc-{RUN_ID}-aurora-writer"
    observed = [
        *_resident(),
        ObservedResource(
            AURORA_CLUSTER, f"adsc-{RUN_ID}-aurora", "available", run_id=RUN_ID
        ),
        ObservedResource(
            AURORA_WRITER, leaked, "available", run_id=RUN_ID, public_ipv4=True
        ),
    ]

    report = reconcile(_manifest(), observed)

    assert not report.ok
    orphan = next(finding for finding in report.orphans if finding.identifier == leaked)
    assert orphan.code == ORPHAN_EPHEMERAL
    assert "make_schema_change_safely" in orphan.detail
    # 2 ACU * $0.12 * 24h + one address at $0.005 * 24h.
    assert orphan.usd_per_day == Decimal("5.88")
    # The basis is asserted exactly, not only the amount. Pricing an orphan at the
    # ceiling is the deliberate direction -- overstating a resource someone is
    # deciding whether to delete errs toward action -- and a later change that
    # handed every caller a measured figure would still produce a plausible
    # number here while quietly reversing that.
    assert orphan.basis == "ceiling: 2 ACU for a full day + 1 public IPv4"
    assert report.orphan_usd_per_day > Decimal(0)
    assert any("approval" in line for line in report.report_lines())


def test_a_resident_writer_prices_from_measured_acu_and_a_leak_still_does_not() -> None:
    """The same writer is worth two numbers, and the default is still the ceiling.

    The resident fleet is observably idle -- 0.033 to 0.051 ACU measured across a
    day against a sealed minimum capacity of 0 -- so pricing four resident writers
    at the 2-ACU ceiling reports about $23.52/day of Aurora compute against roughly
    $0.44 of real one, and the reconciliation total inherits the overstatement.
    That figure is owner-facing, which is why it is guarded here.

    The leak direction must not move with it. ``reconcile()`` prices only resources
    that failed the seal match, which are the ones nobody owns, so ``_carrying_cost``
    is called through its default context and this asserts that the default stays
    the upper bound.
    """

    rates = RateCard()
    measured = ObservedResource(
        AURORA_WRITER,
        "seal-r2-aurora-writer",
        "available",
        run_id=RUN_ID,
        observed_acu=Decimal("0.042"),
    )
    unmeasured = ObservedResource(
        AURORA_WRITER, "seal-r3-aurora-writer", "available", run_id=RUN_ID
    )

    resident_usd, resident_basis = _carrying_cost(measured, rates, context=RESIDENT)
    leaked_usd, leaked_basis = _carrying_cost(measured, rates)
    absent_usd, absent_basis = _carrying_cost(unmeasured, rates, context=RESIDENT)

    assert resident_usd == rates.aurora_acu_hour.usd * Decimal("0.042") * Decimal(24)
    assert resident_usd == Decimal("0.12096")
    assert "0.042 ACU" in resident_basis
    # Holding a measurement is not the same as being asked for one: a caller that
    # names no context keeps the ceiling, measurement or no measurement.
    assert leaked_usd == Decimal("5.76")
    assert leaked_basis == "ceiling: 2 ACU for a full day"
    # An unmeasured resident is not a free one. Nothing measured it on this run,
    # which is a different answer from zero, so it falls back to the ceiling and
    # says on the figure that the figure is an upper bound.
    assert absent_usd == leaked_usd
    assert "upper bound" in absent_basis


def test_residue_from_an_earlier_installation_is_flagged_not_ignored() -> None:
    """A prior run's resources still cost money under a run ID the seal disowns."""

    observed = [
        *_resident(),
        ObservedResource(
            RDS_INSTANCE,
            f"lakebase-anti-demo-{OTHER_RUN}-rds",
            "available",
            run_id=OTHER_RUN,
            public_ipv4=True,
            instance_class="db.t4g.micro",
        ),
    ]

    report = reconcile(_manifest(), observed)

    orphan = report.orphans[0]
    assert orphan.code == ORPHAN_FOREIGN_RUN
    assert OTHER_RUN in orphan.detail
    assert "db.t4g.micro" in orphan.basis
    assert orphan.usd_per_day == Decimal("0.504")  # 0.016*24 + 0.005*24


def test_demo_tagged_resource_outside_the_seal_is_unexpected_not_ephemeral() -> None:
    observed = [
        *_resident(),
        ObservedResource(
            RDS_INSTANCE, "hand-made-experiment", "available", run_id=RUN_ID
        ),
    ]

    report = reconcile(_manifest(), observed)

    assert [finding.code for finding in report.orphans] == [ORPHAN_UNEXPECTED]


def test_a_deleting_resource_is_not_reported_as_residue() -> None:
    """Teardown in progress is teardown working, not a leak."""

    observed = [
        *_resident(),
        ObservedResource(
            AURORA_WRITER,
            f"adsc-{RUN_ID}-aurora-writer",
            "deleting",
            run_id=RUN_ID,
            public_ipv4=True,
        ),
    ]

    report = reconcile(_manifest(), observed)

    assert report.ok
    assert report.observed_public_ipv4 == 8


def test_a_missing_resident_is_reported_without_being_priced_as_an_orphan() -> None:
    observed = [
        resource for resource in _resident() if resource.identifier != "seal-r3-rds"
    ]

    report = reconcile(_manifest(), observed)

    codes = {finding.code for finding in report.findings}
    assert MISSING_RESIDENT in codes
    assert IPV4_DRIFT in codes  # one fewer chargeable address than the seal expects
    assert report.orphans == ()
    assert report.orphan_usd_per_day == Decimal(0)


def test_address_drift_is_reported_when_a_resident_stops_being_reachable() -> None:
    observed = [
        (
            resource
            if resource.identifier != "seal-r2-rds"
            else ObservedResource(
                RDS_INSTANCE, "seal-r2-rds", "available", run_id=RUN_ID, public_ipv4=False
            )
        )
        for resource in _resident()
    ]

    report = reconcile(_manifest(), observed)

    drift = next(finding for finding in report.findings if finding.code == IPV4_DRIFT)
    assert report.observed_public_ipv4 == 7
    assert drift.usd_per_day == Decimal("0.12")


def test_ephemeral_ids_cover_both_aws_lanes_of_rounds_two_and_three() -> None:
    """Round 3 leaks the same way Round 2 does and must be swept the same way."""

    artifacts = ephemeral_artifact_ids(RUN_ID)

    assert artifacts[f"adsc-{RUN_ID}-aurora"] == "make_schema_change_safely"
    assert artifacts[f"adsc-{RUN_ID}-aurora-writer"] == "make_schema_change_safely"
    assert artifacts[f"adsc-{RUN_ID}-rds"] == "make_schema_change_safely"
    assert artifacts[f"adrc-{RUN_ID}-aurora"] == "recover_deleted_order"
    assert artifacts[f"adrc-{RUN_ID}-aurora-writer"] == "recover_deleted_order"
    assert artifacts[f"adrc-{RUN_ID}-rds"] == "recover_deleted_order"


def test_untagged_neighbours_in_the_shared_account_are_never_inventoried() -> None:
    """The sandbox is shared. Ownership is the tag, never the name."""

    observed = observed_from_descriptions(
        db_instances=[
            {
                "DBInstanceIdentifier": "airbnb-cdc-poc-rdspg",
                "DBInstanceStatus": "available",
                "Engine": "postgres",
                "PubliclyAccessible": True,
                "TagList": [{"Key": "Owner", "Value": "someone.else@databricks.com"}],
            },
            {
                "DBInstanceIdentifier": f"adsc-{RUN_ID}-aurora-writer",
                "DBInstanceStatus": "available",
                "Engine": "aurora-postgresql",
                "PubliclyAccessible": True,
                "TagList": [{"Key": "anti-demo-run-id", "Value": RUN_ID}],
            },
        ],
        db_clusters=[
            {
                "DBClusterIdentifier": "airbnb-cdc-poc-mysql",
                "Status": "available",
                "TagList": [],
            }
        ],
        ec2_instances=[
            {
                "InstanceId": "i-0123456789abcdef0",
                "State": {"Name": "running"},
                "PublicIpAddress": "198.51.100.24",
                "Tags": [{"Key": "anti-demo-run-id", "Value": RUN_ID}],
            },
            {"InstanceId": "i-neighbour", "State": {"Name": "running"}, "Tags": []},
        ],
    )

    assert [resource.identifier for resource in observed] == [
        f"adsc-{RUN_ID}-aurora-writer",
        "i-0123456789abcdef0",
    ]
    assert observed[0].kind == AURORA_WRITER
    assert observed[1].kind == EC2_RUNNER


def test_an_unreachable_account_reports_why_instead_of_raising() -> None:
    """An operator reaching for this is often already in a broken state."""

    def explode(_manifest_argument: DemoManifest) -> object:
        raise RuntimeError("ExpiredToken: the security token has expired")

    report = reconcile_live(_manifest(), explode)

    assert not report.ok
    assert "ExpiredToken" in report.unavailable
    assert "not reconciled" in report.summary()
    assert report.report_lines() == ()
    # The seal is still readable even when the account is not.
    assert len(report.expected) == 12


def test_rates_are_taken_from_the_shared_card_not_reinvented() -> None:
    observed = [
        *_resident(),
        ObservedResource(
            EC2_RUNNER, "i-orphaned-runner", "running", run_id=RUN_ID, public_ipv4=True
        ),
    ]
    rates = RateCard()

    report = reconcile(_manifest(), observed, rates=rates)

    orphan = next(
        finding for finding in report.orphans if finding.identifier == "i-orphaned-runner"
    )
    expected = (rates.ec2_m6i_large_hour.usd + rates.public_ipv4_hour.usd) * Decimal(24)
    assert orphan.usd_per_day == expected
