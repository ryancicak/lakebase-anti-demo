"""Compute-parity tests: the matched band, the disclosure, and the drift gate."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server import capacity, lifecycle
from server.capacity import (
    AURORA_AUTO_PAUSE_SECONDS,
    AURORA_MAX_ACU,
    CU_MEMORY_GB,
    LAKEBASE_MAX_CU,
    LAKEBASE_MIN_CU,
    LAKEBASE_SUSPEND_SECONDS,
    NO_SUSPENSION_ROUNDS,
    RDS_CLASS_MEMORY_GIB,
    RDS_INSTANCE_CLASS,
    ROUND5_PEAK_CLIENTS_PER_LANE,
    ObservedCapacity,
    build_capacity_disclosure,
    capacity_parity,
    max_connections_for_memory_gib,
    observed_rds_instance_class,
    rds_memory_gib,
)
from server.manager import _observed_capacity
from server.manifest import (
    AwsManifest,
    AwsResources,
    DatabricksManifest,
    DemoManifest,
)
from server.models import CompetitorId, RoundId

AURORA = CompetitorId.AURORA_SERVERLESS_V2
RDS = CompetitorId.RDS_POSTGRES


def _manifest() -> DemoManifest:
    return DemoManifest(
        run_id="ad-test-capacity",
        owner="operator@databricks.com",
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=24),
        status="ready",
        aws=AwsManifest(
            profile="sandbox-admin",
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state="/tmp/anti-demo-capacity.tfstate",
            resources=AwsResources(
                aurora_cluster_id="anti-demo-aurora",
                aurora_writer_instance_id="anti-demo-aurora-writer",
                aurora_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:a",
                rds_instance_id="anti-demo-rds",
                rds_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:r",
                security_group_id="sg-aurora",
                rds_security_group_id="sg-rds",
                db_subnet_group_name="anti-demo-subnets",
            ),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-capacity",
            endpoint_name="projects/ad-test-capacity/branches/production/endpoints/primary",
            user="operator@databricks.com",
        ),
        schema_sha256="abc123",
    )


class _FakeRds:
    """Minimal RDS control plane returning one Aurora cluster and one instance."""

    def __init__(self, *, max_acu: float | None = 2.0, instance_class: str = "db.t4g.medium"):
        self.max_acu = max_acu
        self.instance_class = instance_class

    def describe_db_clusters(self, **_: object) -> dict:
        scaling: dict[str, object] = {"MinCapacity": 0.0, "SecondsUntilAutoPause": 300}
        if self.max_acu is not None:
            scaling["MaxCapacity"] = self.max_acu
        return {"DBClusters": [{"ServerlessV2ScalingConfiguration": scaling}]}

    def describe_db_instances(self, **_: object) -> dict:
        return {"DBInstances": [{"DBInstanceClass": self.instance_class}]}


def _arm_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    *,
    max_cu: float | None = 2.0,
    min_cu: float | None = LAKEBASE_MIN_CU,
    max_acu: float | None = 2.0,
    instance_class: str = "db.t4g.medium",
    capacity_by_endpoint: dict[str, tuple[float | None, float | None]] | None = None,
) -> list[str]:
    """Stand both control planes up and record which endpoints were interrogated.

    The returned list is the evidence for defect 3: a check that reads one round's
    endpoint and compares it against every round's AWS lane never appears in the
    result, only in which names it asked for.
    """

    asked: list[str] = []

    def endpoint_capacity(
        _profile: str, endpoint_name: str
    ) -> tuple[float | None, float | None]:
        asked.append(endpoint_name)
        if capacity_by_endpoint is not None and endpoint_name in capacity_by_endpoint:
            return capacity_by_endpoint[endpoint_name]
        return (max_cu, min_cu)

    monkeypatch.setattr(lifecycle, "_endpoint_capacity", endpoint_capacity)
    monkeypatch.setattr(
        lifecycle,
        "_aws_session",
        lambda _: SimpleNamespace(
            client=lambda service: _FakeRds(max_acu=max_acu, instance_class=instance_class)
        ),
    )
    return asked


def _round_endpoint(number: int) -> str:
    return f"projects/ad-test-capacity-r{number}/branches/production/endpoints/primary"


def _v7_manifest(monkeypatch: pytest.MonkeyPatch) -> DemoManifest:
    """A v7 manifest whose four AWS rounds each seal their own endpoint.

    Round 1 seals `rds=None`, which is the configuration after its instance is
    deleted: Terraform stands one up only for the rounds that race the lane.
    """

    manifest = _manifest()
    manifest.manifest_version = 7
    seals = {
        number: SimpleNamespace(
            lakebase=SimpleNamespace(endpoint_name=_round_endpoint(number)),
            aurora=SimpleNamespace(cluster_id=f"anti-demo-aurora-r{number}"),
            rds=(
                None
                if number == 1
                else SimpleNamespace(instance_id=f"anti-demo-rds-r{number}")
            ),
        )
        for number in (1, 2, 3, 4, 5, 6)
    }
    numbers = {round_id: number for number, round_id in enumerate(RoundId, start=1)}

    def resolve(_self: object, key: RoundId | int) -> SimpleNamespace:
        return seals[key if isinstance(key, int) else numbers[RoundId(key)]]

    monkeypatch.setattr(type(manifest), "round_environment", resolve, raising=False)
    return manifest


class TestConfiguredSizes:
    def test_rds_class_is_memory_matched_to_both_competitors(self) -> None:
        """4 GiB of RDS memory against a 2 CU and a 2 ACU ceiling."""

        assert RDS_INSTANCE_CLASS == "db.t4g.medium"
        assert rds_memory_gib(RDS_INSTANCE_CLASS) == 4.0
        assert LAKEBASE_MAX_CU * 2.0 == 4.0
        assert AURORA_MAX_ACU * 2.0 == 4.0

    def test_idle_policies_are_each_vendor_minimum(self) -> None:
        assert LAKEBASE_SUSPEND_SECONDS == 60
        assert AURORA_AUTO_PAUSE_SECONDS == 300

    def test_lakebase_floor_is_above_aurora_floor(self) -> None:
        """Aurora pauses to zero; Lakebase never goes below half a CU."""

        assert LAKEBASE_MIN_CU > 0


class TestMaxConnections:
    @pytest.mark.parametrize(
        ("memory_gib", "expected"),
        [(1.0, 112), (2.0, 225), (4.0, 450), (8.0, 901)],
    )
    def test_formula_matches_parameter_group(self, memory_gib: float, expected: int) -> None:
        """LEAST({DBInstanceClassMemory/9531392},5000) against nominal memory."""

        assert max_connections_for_memory_gib(memory_gib) == expected

    def test_configured_class_clears_the_round5_contract(self) -> None:
        headroom = max_connections_for_memory_gib(rds_memory_gib(RDS_INSTANCE_CLASS) or 0)
        assert headroom >= ROUND5_PEAK_CLIENTS_PER_LANE
        assert headroom / ROUND5_PEAK_CLIENTS_PER_LANE > 3

    def test_previous_class_did_not_clear_the_contract(self) -> None:
        """db.t4g.micro sat under the 128-client contract, not merely below Lakebase."""

        assert max_connections_for_memory_gib(1.0) < ROUND5_PEAK_CLIENTS_PER_LANE

    def test_ceiling_is_applied(self) -> None:
        assert max_connections_for_memory_gib(100000.0) == 5000

    def test_non_positive_memory_is_zero(self) -> None:
        assert max_connections_for_memory_gib(0) == 0
        assert max_connections_for_memory_gib(-1) == 0


class TestCapacityParity:
    def test_the_verdict_and_the_sentence_that_explains_it(self) -> None:
        """One configuration per row: the verdict, and what the detail must say.

        These were a test each, one per fix, all making the same call and each
        asserting on one attribute of the same result. They are one table now.
        The verdict and the message are checked together on purpose: a check
        that refuses for the right reason but names the wrong one is still
        wrong, and separating them let a message regress under a green verdict.

        `absent` is as load-bearing as `present`. Every row names itself so a
        failure says which configuration produced it.
        """

        # The receipt case, and the reason its wording is pinned this precisely.
        # Receipt `F9D4023E` ran the full 128-client Round 5 burst against RDS
        # Proxy on a live `db.t4g.micro` and logged 128 attempts / 128 successes
        # / 0 errors, because proxy multiplexing seated them. The 112 figure is
        # the parameter-group formula against nominal memory, so the refusal is
        # sound only as a statement about a nominal budget -- never as a claim
        # that the box was observed unable to serve the contract. The memory
        # ratio here agrees (0.5 CU against 1 GiB), so the connection budget is
        # the only thing that can fail, which is what "does not match" being
        # absent pins.
        micro_under_the_burst = dict(
            lakebase_max_cu=0.5,
            aurora_max_acu=None,
            rds_instance_class="db.t4g.micro",
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )

        cases: tuple[tuple[str, dict, bool, tuple[str, ...], tuple[str, ...]], ...] = (
            (
                "matched aurora lane",
                dict(lakebase_max_cu=2.0, aurora_max_acu=2.0, rds_instance_class=None),
                True,
                ("matched memory ceiling",),
                (),
            ),
            (
                "matched rds lane",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=None,
                    rds_instance_class="db.t4g.medium",
                ),
                True,
                (),
                (),
            ),
            (
                "both lanes together",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=2.0,
                    rds_instance_class="db.t4g.medium",
                ),
                True,
                (),
                (),
            ),
            (
                # The regression this check exists to catch.
                "shrunken rds lane",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=None,
                    rds_instance_class="db.t4g.micro",
                ),
                False,
                ("1 GiB",),
                (),
            ),
            (
                "shrunken rds lane in the round that bursts",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=None,
                    rds_instance_class="db.t4g.micro",
                    round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
                ),
                False,
                ("128-client",),
                (),
            ),
            (
                # The gate being round-scoped must not make it unreachable.
                "a matched class still clears round five",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=None,
                    rds_instance_class=RDS_INSTANCE_CLASS,
                    round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
                ),
                True,
                (),
                (),
            ),
            (
                "shrunken aurora lane",
                dict(lakebase_max_cu=2.0, aurora_max_acu=1.0, rds_instance_class=None),
                False,
                ("Aurora ceiling",),
                (),
            ),
            (
                # A quiet max_cu bump must not pass while AWS stays where it was.
                "lakebase grown on its own",
                dict(
                    lakebase_max_cu=8.0,
                    aurora_max_acu=2.0,
                    rds_instance_class="db.t4g.medium",
                ),
                False,
                (),
                (),
            ),
            (
                "instance class outside the approved table",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=None,
                    rds_instance_class="db.r7g.24xlarge",
                ),
                False,
                ("approved matched list",),
                (),
            ),
            (
                "no lakebase reading at all",
                dict(lakebase_max_cu=None, aurora_max_acu=2.0, rds_instance_class=None),
                False,
                (),
                (),
            ),
            (
                # Rounds 4 and 6 provision no AWS box, so there is nothing to
                # mismatch.
                "neither aws lane provisioned",
                dict(lakebase_max_cu=2.0, aurora_max_acu=None, rds_instance_class=None),
                True,
                ("Aurora not provisioned", "RDS not provisioned"),
                (),
            ),
            (
                "memory-matched but under the burst budget",
                micro_under_the_burst,
                False,
                (
                    "128-client",
                    "nominal connection budget",
                    "not an observed limit",
                    "RDS Proxy multiplexing may still seat the burst",
                ),
                ("does not match", "allows about"),
            ),
        )

        for name, kwargs, expected_ok, present, absent in cases:
            result = capacity_parity(**kwargs)
            assert result.ok is expected_ok, name
            for fragment in present:
                assert fragment in result.detail, f"{name}: missing {fragment!r}"
            for fragment in absent:
                assert fragment not in result.detail, f"{name}: unwanted {fragment!r}"

    def test_the_connection_budget_binds_only_the_round_that_bursts(self) -> None:
        """The 128-client contract is Round 5's, and only Round 5's.

        A db.t4g.micro seats about 112 connections, under Round 5's burst. In
        Round 2 or Round 3 nothing comes near that number, so refusing the class
        there rejects a lane on a requirement the round never has to meet. The
        memory mismatch is still a failure -- that one is about parity and holds
        everywhere -- but it must be the only one named.
        """

        for round_id in (
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            RoundId.RECOVER_DELETED_ORDER,
            None,
        ):
            result = capacity_parity(
                lakebase_max_cu=2.0,
                aurora_max_acu=None,
                rds_instance_class="db.t4g.micro",
                round_id=round_id,
            )
            assert not result.ok, round_id
            assert "1 GiB" in result.detail, round_id
            assert "128-client" not in result.detail, round_id

    def test_every_approved_class_matches_its_own_memory_ceiling(self) -> None:
        """Memory matching is checked per class, independent of the class chosen."""

        for instance_class, memory in RDS_CLASS_MEMORY_GIB.items():
            result = capacity_parity(
                lakebase_max_cu=memory / 2.0,
                aurora_max_acu=None,
                rds_instance_class=instance_class,
                round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
            )
            expected_ok = (
                max_connections_for_memory_gib(memory) >= ROUND5_PEAK_CLIENTS_PER_LANE
            )
            assert result.ok is expected_ok, f"{instance_class} at {memory} GiB"
            if result.ok:
                assert "does not match" not in result.detail


class TestFloors:
    """The minimum capacities: reported, and deliberately never compared.

    Ceiling-against-ceiling made the floors invisible, and the floor is the half
    of the configuration that decides what an idle lane costs -- which is the
    demo's whole subject. Aurora at 0 ACU against Lakebase at 0.5 CU is a
    published asymmetry, not a misconfiguration, so it belongs in the disclosure
    and not in the verdict.
    """

    def test_what_each_floor_configuration_puts_in_the_detail(self) -> None:
        """Every row passes: a floor is disclosed, never compared, never a failure."""

        cases: tuple[tuple[str, dict, tuple[str, ...]], ...] = (
            (
                "both floors supplied",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=2.0,
                    rds_instance_class=None,
                    lakebase_min_cu=LAKEBASE_MIN_CU,
                    aurora_min_acu=0.0,
                ),
                (
                    f"Lakebase {LAKEBASE_MIN_CU:g} CU",
                    "Aurora 0 ACU",
                    f"suspends after {LAKEBASE_SUSPEND_SECONDS}s",
                    f"auto-pauses after {AURORA_AUTO_PAUSE_SECONDS}s",
                ),
            ),
            (
                # 0 ACU against 0.5 CU is the finding. It must not read as a red
                # check, so the matched verdict and the disclosure appear together.
                "the floor gap is disclosed alongside a matched ceiling",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=2.0,
                    rds_instance_class=RDS_INSTANCE_CLASS,
                    lakebase_min_cu=LAKEBASE_MIN_CU,
                    aurora_min_acu=0.0,
                ),
                ("matched memory ceiling", "disclosed, not compared", "floor"),
            ),
            (
                # The engine has no idle state, so its floor and ceiling are one
                # number.
                "an rds floor is named as its ceiling",
                dict(
                    lakebase_max_cu=2.0,
                    aurora_max_acu=None,
                    rds_instance_class=RDS_INSTANCE_CLASS,
                    lakebase_min_cu=LAKEBASE_MIN_CU,
                ),
                (f"RDS {RDS_INSTANCE_CLASS} never idles",),
            ),
            (
                # The project's rule: a missing quantity is unavailable, never a
                # zero. A floor is the one place where a silently-defaulted zero
                # would be indistinguishable from Aurora's real zero, which is the
                # number the whole idle-cost argument turns on.
                "neither floor supplied",
                dict(lakebase_max_cu=2.0, aurora_max_acu=2.0, rds_instance_class=None),
                ("Lakebase floor not reported", "Aurora floor not reported"),
            ),
        )

        for name, kwargs, present in cases:
            result = capacity_parity(**kwargs)
            assert result.ok, name
            for fragment in present:
                assert fragment in result.detail, f"{name}: missing {fragment!r}"

    def test_an_absent_aurora_lane_claims_no_floor_at_all(self) -> None:
        """Rounds 4 and 6 have no cluster, so it has no floor to disclose."""

        result = capacity_parity(
            lakebase_max_cu=2.0,
            aurora_max_acu=None,
            rds_instance_class=None,
            lakebase_min_cu=LAKEBASE_MIN_CU,
        )

        assert result.ok
        assert "Aurora" not in result.detail.split("floors")[1]


class TestDisclosure:
    @pytest.mark.parametrize("competitor", [AURORA, RDS])
    def test_measured_rounds_disclose_both_lanes(self, competitor: CompetitorId) -> None:
        disclosure = build_capacity_disclosure(RoundId.WAKE_IDLE_APP, competitor)
        assert [lane.lane_id for lane in disclosure.lanes] == ["lakebase", "competitor"]
        assert disclosure.matched

    def test_each_lane_states_its_configured_size(self) -> None:
        """One row per lane of Round 1's disclosure: exact values, then fragments.

        Lakebase reports a major version only because that is all the endpoint
        publishes; the AWS lanes report the full engine version. The RDS lane
        must deny an idle pause outright -- Round 1 never times it, and the copy
        must not imply that it does.
        """

        cases: tuple[tuple[str, CompetitorId, int, dict, dict], ...] = (
            (
                "lakebase lane",
                AURORA,
                0,
                {
                    "configured": "0.5–2 CU",
                    "memory": "~1–4 GB",
                    "max_connections": 443,
                    "engine_version": "PostgreSQL 17 (major only)",
                },
                {},
            ),
            (
                "aurora lane",
                AURORA,
                1,
                {
                    "configured": "0–2 ACU",
                    "memory": "~0–4 GiB",
                    "engine_version": "PostgreSQL 17.10",
                },
                {"idle_policy": ("300s", "AWS documented minimum")},
            ),
            (
                "rds lane",
                RDS,
                1,
                {
                    "configured": "db.t4g.medium",
                    "memory": "4 GiB",
                    "max_connections": 450,
                },
                {"idle_policy": ("No automatic idle pause",)},
            ),
        )

        for name, competitor, index, equals, contains in cases:
            lane = build_capacity_disclosure(RoundId.WAKE_IDLE_APP, competitor).lanes[index]
            for attribute, expected in equals.items():
                assert getattr(lane, attribute) == expected, f"{name}.{attribute}"
            for attribute, fragments in contains.items():
                for fragment in fragments:
                    assert fragment in getattr(lane, attribute), (
                        f"{name}.{attribute}: missing {fragment!r}"
                    )

    @pytest.mark.parametrize(
        "round_id", [RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS]
    )
    @pytest.mark.parametrize("competitor", [AURORA, RDS])
    def test_lakebase_only_rounds_disclose_one_lane(
        self, round_id: RoundId, competitor: CompetitorId
    ) -> None:
        disclosure = build_capacity_disclosure(round_id, competitor)
        assert [lane.lane_id for lane in disclosure.lanes] == ["lakebase"]
        assert "no compute comparison is made" in disclosure.note

    def test_round_six_discloses_that_it_scales_to_zero_like_every_other_round(
        self,
    ) -> None:
        disclosure = build_capacity_disclosure(RoundId.ANALYZE_LIVE_ORDERS, AURORA)
        assert disclosure.lanes[0].idle_policy == (
            f"Scale to zero after {LAKEBASE_SUSPEND_SECONDS}s (vendor minimum)"
        )
        assert "Suspension disabled" not in disclosure.lanes[0].idle_policy

    def test_no_round_disables_suspension(self) -> None:
        assert NO_SUSPENSION_ROUNDS == frozenset()

    def test_the_idle_policy_copy_follows_the_set_rather_than_being_hardcoded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The disclosure must be generated, so an exception cannot go unstated.

        If a round is ever exempted again, the sentence on screen has to change
        with it. Pinning that here is what stops the copy and the configuration
        from drifting apart in either direction.
        """

        monkeypatch.setattr(
            capacity,
            "NO_SUSPENSION_ROUNDS",
            frozenset({RoundId.ANALYZE_LIVE_ORDERS}),
        )
        exempted = build_capacity_disclosure(RoundId.ANALYZE_LIVE_ORDERS, AURORA)
        assert exempted.lanes[0].idle_policy == (
            "Suspension disabled so the change feed stays live"
        )

        monkeypatch.setattr(capacity, "NO_SUSPENSION_ROUNDS", frozenset())
        restored = build_capacity_disclosure(RoundId.ANALYZE_LIVE_ORDERS, AURORA)
        assert restored.lanes[0].idle_policy == (
            f"Scale to zero after {LAKEBASE_SUSPEND_SECONDS}s (vendor minimum)"
        )

    def test_configured_values_are_labelled_as_configured(self) -> None:
        disclosure = build_capacity_disclosure(RoundId.WAKE_IDLE_APP, AURORA)
        assert all(lane.basis == "configured" for lane in disclosure.lanes)

    def test_observed_values_replace_the_configured_ones(self) -> None:
        disclosure = build_capacity_disclosure(
            RoundId.WAKE_IDLE_APP,
            AURORA,
            observed=ObservedCapacity(
                lakebase_min_cu=1.0,
                lakebase_max_cu=1.0,
                aurora_min_acu=0.0,
                aurora_max_acu=1.0,
                aurora_auto_pause_seconds=600,
                aurora_engine_version="17.9",
            ),
        )
        lakebase, aurora = disclosure.lanes
        assert lakebase.basis == "observed"
        assert lakebase.configured == "1–1 CU"
        assert aurora.basis == "observed"
        assert aurora.configured == "0–1 ACU"
        assert aurora.engine_version == "PostgreSQL 17.9"
        assert "600s" in aurora.idle_policy

    def test_observed_drift_is_reported_as_unmatched(self) -> None:
        """A live box smaller than configured must not render as matched."""

        disclosure = build_capacity_disclosure(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            RDS,
            observed=ObservedCapacity(rds_instance_class="db.t4g.micro"),
        )
        assert not disclosure.matched
        assert disclosure.lanes[1].configured == "db.t4g.micro"
        assert disclosure.lanes[1].basis == "observed"

    def test_partial_observation_falls_back_without_claiming_observation(self) -> None:
        """An empty reading must not be dressed up as an observed value."""

        disclosure = build_capacity_disclosure(
            RoundId.WAKE_IDLE_APP,
            AURORA,
            observed=ObservedCapacity(lakebase_max_cu=2.0),
        )
        lakebase, aurora = disclosure.lanes
        assert lakebase.basis == "configured"
        assert aurora.basis == "configured"

    def test_unknown_observed_class_is_reported_honestly(self) -> None:
        disclosure = build_capacity_disclosure(
            RoundId.WAKE_IDLE_APP,
            RDS,
            observed=ObservedCapacity(rds_instance_class="db.x9z.enormous"),
        )
        rds = disclosure.lanes[1]
        assert rds.memory == "memory not published here"
        assert rds.max_connections is None
        assert not disclosure.matched

    def test_summary_names_both_products_and_sizes(self) -> None:
        disclosure = build_capacity_disclosure(RoundId.SURVIVE_CONNECTION_SPIKE, AURORA)
        assert "Lakebase 0.5–2 CU" in disclosure.summary
        assert "Aurora Serverless v2 0–2 ACU" in disclosure.summary

    def test_note_denies_the_vendor_default_claim(self) -> None:
        """The demo hand-sets both sides and must not imply otherwise."""

        disclosure = build_capacity_disclosure(RoundId.WAKE_IDLE_APP, AURORA)
        assert "not vendor defaults" in disclosure.note


class TestTheNoteMayNotOutClaimTheVerdict:
    """The paragraph under the lanes asserted a match the verdict had refused.

    `matched` went false on a drifted lane and the note kept saying "Both sides
    are hand-set to the same memory ceiling" regardless, so the screen carried a
    summary reading "Ceilings do not match" over a paragraph asserting they did.
    The paragraph was the false one. These pin the absence of that claim, which
    is the half a passing matched-case test cannot cover.
    """

    def test_a_drifted_lane_denies_the_match_and_names_the_drift(self) -> None:
        """A denial nobody can act on is weaker than the measurement behind it.

        Both rows are unmatched, so neither may carry the affirmative sentence.
        The absence is anchored on "hand-set to" because the denial legitimately
        contains the phrase "same memory ceiling" as part of "not on the same
        memory ceiling".
        """

        cases: tuple[tuple[str, RoundId, str, tuple[str, ...]], ...] = (
            (
                "a smaller approved class",
                RoundId.SURVIVE_CONNECTION_SPIKE,
                "db.t4g.micro",
                (
                    "not on the same memory ceiling",
                    "no capacity parity is claimed",
                    "db.t4g.micro",
                    "1 GiB",
                ),
            ),
            (
                "a class outside the approved table",
                RoundId.WAKE_IDLE_APP,
                "db.x9z.enormous",
                ("approved",),
            ),
        )

        for name, round_id, instance_class, present in cases:
            disclosure = build_capacity_disclosure(
                round_id,
                RDS,
                observed=ObservedCapacity(rds_instance_class=instance_class),
            )
            assert not disclosure.matched, name
            assert "hand-set to the same memory ceiling" not in disclosure.note, name
            for fragment in present:
                assert fragment in disclosure.note, f"{name}: missing {fragment!r}"

    def test_the_matched_case_keeps_its_wording_exactly(self) -> None:
        disclosure = build_capacity_disclosure(RoundId.WAKE_IDLE_APP, AURORA)
        assert disclosure.matched
        assert disclosure.note == (
            "Both sides are hand-set to the same memory ceiling, single node, "
            "no HA on either side. These are not vendor defaults, and each idle "
            "policy is that vendor's shortest supported setting."
        )

    @pytest.mark.parametrize("competitor", [AURORA, RDS])
    def test_the_hand_set_provenance_survives_either_verdict(
        self, competitor: CompetitorId
    ) -> None:
        """It is a claim about configuration, not about agreement, so it holds."""

        matched = build_capacity_disclosure(RoundId.SURVIVE_CONNECTION_SPIKE, competitor)
        drifted = build_capacity_disclosure(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            competitor,
            observed=ObservedCapacity(
                lakebase_min_cu=0.5,
                lakebase_max_cu=1.0,
                aurora_min_acu=0.0,
                aurora_max_acu=2.0,
                rds_instance_class="db.t4g.micro",
            ),
        )
        assert matched.matched and not drifted.matched
        for disclosure in (matched, drifted):
            assert "not vendor defaults" in disclosure.note
            assert "shortest supported setting" in disclosure.note

    def test_the_failures_travel_on_the_result_rather_than_being_reparsed(
        self,
    ) -> None:
        parity = capacity_parity(
            lakebase_max_cu=2.0,
            aurora_max_acu=None,
            rds_instance_class="db.t4g.micro",
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
        assert not parity.ok
        assert parity.failures
        assert all(failure in parity.detail for failure in parity.failures)

    def test_a_passing_check_carries_no_failures(self) -> None:
        parity = capacity_parity(
            lakebase_max_cu=2.0,
            aurora_max_acu=2.0,
            rds_instance_class=RDS_INSTANCE_CLASS,
        )
        assert parity.ok
        assert parity.failures == ()

    def test_an_unreported_lakebase_ceiling_is_a_named_failure(self) -> None:
        # The one early return, which used to leave `failures` empty and would
        # therefore have rendered a note that named nothing at all.
        parity = capacity_parity(
            lakebase_max_cu=None,
            aurora_max_acu=2.0,
            rds_instance_class=RDS_INSTANCE_CLASS,
        )
        assert not parity.ok
        assert parity.failures == ("Lakebase maximum CU was not reported",)


class TestCapacityParityCheck:
    """The doctor gate, alongside the existing region_parity check."""

    def test_the_gate_reflects_what_the_control_planes_report(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One row per drift, each arming both control planes and reading the gate.

        The check's name is asserted on every row rather than only the passing
        one: a gate that renamed itself would stop being findable in `doctor`
        output whichever verdict it reached.
        """

        cases: tuple[tuple[str, dict, bool, tuple[str, ...]], ...] = (
            ("matched installation", {}, True, ("r1",)),
            (
                "shrunken rds lane",
                {"instance_class": "db.t4g.micro"},
                False,
                ("db.t4g.micro",),
            ),
            ("shrunken aurora lane", {"max_acu": 1.0}, False, ()),
            ("grown lakebase ceiling", {"max_cu": 8.0}, False, ()),
            ("unreadable lakebase ceiling", {"max_cu": None}, False, ()),
        )

        for name, arming, expected_ok, present in cases:
            _arm_lifecycle(monkeypatch, **arming)
            check = lifecycle._capacity_parity(_manifest())
            assert check.name == "capacity_parity", name
            assert check.ok is expected_ok, name
            for fragment in present:
                assert fragment in check.detail, f"{name}: missing {fragment!r}"

    def test_control_plane_failure_is_reported_not_raised(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _arm_lifecycle(monkeypatch)

        def explode(_profile: str, _endpoint: str) -> tuple[float | None, float | None]:
            raise RuntimeError("endpoint unavailable")

        monkeypatch.setattr(lifecycle, "_endpoint_capacity", explode)
        check = lifecycle._capacity_parity(_manifest())
        assert not check.ok
        assert "endpoint unavailable" in check.detail

    def test_doctor_registers_the_check_beside_region_parity(self) -> None:
        """The gate is worthless if nothing runs it."""

        source = inspect.getsource(lifecycle.doctor)
        assert "_capacity_parity(manifest)" in source
        assert source.index("_region_parity(manifest)") < source.index(
            "_capacity_parity(manifest)"
        )

    def test_every_provisioned_aws_lane_is_checked(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Drift in any round must fail, not only in the round being run."""

        _arm_lifecycle(monkeypatch)
        check = lifecycle._capacity_parity(_v7_manifest(monkeypatch))
        assert check.ok
        for round_key in ("r1", "r2", "r3", "r5"):
            assert round_key in check.detail


class TestTheRoundOneRdsLaneLeavesTheReport:
    """Defect 1: the check validated a lane that does not compete.

    Round 1's bout is a wake-from-idle race and RDS has no idle state to wake from,
    so its lane refuses to enter and is never timed. Terraform therefore stands no
    instance up for it. Validating that lane described a box that does not exist,
    and reporting it as "not compared" would still put a row on screen for a
    comparison nobody is entitled to expect.
    """

    def test_round_one_reports_aurora_and_says_nothing_about_rds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _arm_lifecycle(monkeypatch)
        detail = lifecycle._capacity_parity(_v7_manifest(monkeypatch)).detail
        r1 = detail.split("; ")[0]
        assert r1.startswith("r1 ")
        assert "Aurora 2 ACU" in r1
        assert "RDS not provisioned" in r1
        # No comparison entry and no floor claim: naming an instance class here
        # would stamp a size onto a box that does not exist.
        assert RDS_INSTANCE_CLASS not in r1
        assert "never idles" not in r1

    def test_the_scored_rounds_still_have_their_instance_compared(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The short circuit must not quietly disable the lanes that are raced.
        _arm_lifecycle(monkeypatch)
        detail = lifecycle._capacity_parity(_v7_manifest(monkeypatch)).detail
        for round_key in ("r2", "r3", "r5"):
            lane = next(part for part in detail.split("; ") if part.startswith(f"{round_key} "))
            assert RDS_INSTANCE_CLASS in lane

    def test_an_absent_r1_instance_is_not_read_from_the_control_plane(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # There is nothing to describe. Asking would be an error against the live
        # account rather than a comparison.
        described: list[str] = []

        class _Recording(_FakeRds):
            def describe_db_instances(self, **kwargs: object) -> dict:
                described.append(str(kwargs.get("DBInstanceIdentifier")))
                return super().describe_db_instances(**kwargs)

        monkeypatch.setattr(
            lifecycle,
            "_aws_session",
            lambda _: SimpleNamespace(client=lambda _service: _Recording()),
        )
        monkeypatch.setattr(
            lifecycle,
            "_endpoint_capacity",
            lambda _profile, _endpoint: (2.0, LAKEBASE_MIN_CU),
        )
        assert lifecycle._capacity_parity(_v7_manifest(monkeypatch)).ok
        assert described == [
            "anti-demo-rds-r2",
            "anti-demo-rds-r3",
            "anti-demo-rds-r5",
        ]

    def test_shrinking_a_scored_instance_still_fails_the_gate_under_v7(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _arm_lifecycle(monkeypatch, instance_class="db.t4g.micro")
        check = lifecycle._capacity_parity(_v7_manifest(monkeypatch))
        assert not check.ok
        assert check.detail.startswith("r2:")

    def test_the_pre_v7_single_instance_is_still_compared(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The pre-v7 layout mirrors one installation-wide instance into fields named
        # for Round 1. That instance served every round, so the per-round scored-lane
        # rule does not reach it and dropping it would blind the gate entirely.
        _arm_lifecycle(monkeypatch, instance_class="db.t4g.micro")
        check = lifecycle._capacity_parity(_manifest())
        assert not check.ok
        assert "db.t4g.micro" in check.detail

    def test_the_gate_binds_the_doctor_but_never_arming(self) -> None:
        # Preserved property: a parity failure is a `antidemo doctor` / `antidemo setup`
        # check and must not become able to refuse a bout. The arming path reaches
        # the same function through build_capacity_disclosure, which *renders*
        # parity into `matched` and neither raises nor withholds the disclosure.
        assert "_capacity_parity(manifest)" in inspect.getsource(lifecycle.doctor)
        mismatched = build_capacity_disclosure(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            RDS,
            observed=ObservedCapacity(
                lakebase_min_cu=LAKEBASE_MIN_CU,
                lakebase_max_cu=8.0,
                rds_instance_class=RDS_INSTANCE_CLASS,
            ),
        )
        assert mismatched.matched is False
        assert len(mismatched.lanes) == 2


class TestEachRoundIsComparedAgainstItsOwnEndpoint:
    """Defect 3: `_lakebase_max_cu` read Round 1's endpoint for every round.

    Seven endpoints exist. They all sit at 2 CU today, which is the only reason
    this was latent rather than wrong, and it goes live the moment any round's CU
    is changed -- at which point the check would pass a lane it never looked at.
    """

    def test_all_four_lanes_read_four_different_endpoints(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        asked = _arm_lifecycle(monkeypatch)
        assert lifecycle._capacity_parity(_v7_manifest(monkeypatch)).ok
        assert asked == [_round_endpoint(number) for number in (1, 2, 3, 5)]
        assert len(set(asked)) == 4

    def test_a_single_round_resized_on_its_own_fails_the_gate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The live failure the old read could not see: r3's endpoint alone is grown
        # to 8 CU while every other endpoint stays matched. Reading r1's ceiling
        # four times would have reported four matched lanes.
        _arm_lifecycle(
            monkeypatch,
            capacity_by_endpoint={_round_endpoint(3): (8.0, LAKEBASE_MIN_CU)},
        )
        check = lifecycle._capacity_parity(_v7_manifest(monkeypatch))
        assert not check.ok
        assert check.detail.startswith("r3:")
        assert "Lakebase 8 CU" in check.detail

    def test_the_floors_are_read_per_round_and_reported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Defect 4's disclosure needs a floor to disclose, and it has to come from
        # the same read as the ceiling or the pair can drift.
        _arm_lifecycle(monkeypatch)
        detail = lifecycle._capacity_parity(_v7_manifest(monkeypatch)).detail
        assert f"Lakebase {LAKEBASE_MIN_CU} CU" in detail
        assert f"suspends after {LAKEBASE_SUSPEND_SECONDS}s" in detail
        assert f"auto-pauses after {AURORA_AUTO_PAUSE_SECONDS}s" in detail
        assert "disclosed, not compared" in detail

    def test_an_unreported_floor_says_so_rather_than_reading_as_zero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Aurora's floor really is zero, so a defaulted zero would be
        # indistinguishable from a measurement.
        _arm_lifecycle(monkeypatch, min_cu=None)
        detail = lifecycle._capacity_parity(_v7_manifest(monkeypatch)).detail
        assert "Lakebase floor not reported" in detail

    def test_the_burst_budget_binds_round_five_alone(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Defect 2, already fixed in capacity.py and pinned from this end too: the
        # round context has to reach `capacity_parity` for the rule to be scoped at
        # all. A class that seats fewer than 128 clients fails on r5 and on r5 only.
        narrow = "db.t4g.micro"
        memory = RDS_CLASS_MEMORY_GIB[narrow]
        assert max_connections_for_memory_gib(memory) < ROUND5_PEAK_CLIENTS_PER_LANE
        # Memory-matched on both sides, so the only thing that can fail is the
        # client budget.
        for round_id, expected in (
            (RoundId.RECOVER_DELETED_ORDER, True),
            (RoundId.SURVIVE_CONNECTION_SPIKE, False),
        ):
            result = capacity_parity(
                lakebase_max_cu=memory / CU_MEMORY_GB,
                aurora_max_acu=None,
                rds_instance_class=narrow,
                round_id=round_id,
            )
            assert result.ok is expected


class TestObservedCapacityMapping:
    """Arming evidence -> ObservedCapacity, the plumbing behind the disclosure."""

    def test_lakebase_and_aurora_evidence_is_read(self) -> None:
        observed = _observed_capacity(
            {
                "lakebase": {
                    "autoscaling_limit_min_cu": 0.5,
                    "autoscaling_limit_max_cu": 2,
                },
                "competitor": {
                    "min_capacity_acu": 0.0,
                    "max_capacity_acu": 2.0,
                    "auto_pause_seconds": 300,
                    "engine_version": "17.10",
                },
            }
        )
        assert observed.lakebase_min_cu == 0.5
        assert observed.lakebase_max_cu == 2.0
        assert observed.aurora_max_acu == 2.0
        assert observed.aurora_auto_pause_seconds == 300
        assert observed.aurora_engine_version == "17.10"

    def test_rds_instance_class_is_read_from_the_qualify_payload(self) -> None:
        """server/targets.py captured this and used to discard it."""

        observed = _observed_capacity(
            {
                "lakebase": {},
                "competitor": {
                    "state": "NO_SCALE_TO_ZERO",
                    "eligible": False,
                    "instance_class": "db.t4g.medium",
                    "engine_version": "17.10",
                },
            }
        )
        assert observed.rds_instance_class == "db.t4g.medium"
        assert observed.rds_engine_version == "17.10"
        assert observed.aurora_max_acu is None

    def test_missing_evidence_stays_none(self) -> None:
        observed = _observed_capacity({})
        assert observed.lakebase_max_cu is None
        assert observed.aurora_max_acu is None
        assert observed.rds_instance_class is None

    def test_empty_strings_do_not_become_values(self) -> None:
        observed = _observed_capacity(
            {"competitor": {"instance_class": "", "engine_version": ""}}
        )
        assert observed.rds_instance_class is None
        assert observed.rds_engine_version is None

    def test_evidence_round_trips_into_a_disclosure(self) -> None:
        observed = _observed_capacity(
            {
                "lakebase": {
                    "autoscaling_limit_min_cu": 0.5,
                    "autoscaling_limit_max_cu": 2,
                },
                "competitor": {"instance_class": "db.t4g.medium", "engine_version": "17.10"},
            }
        )
        disclosure = build_capacity_disclosure(
            RoundId.WAKE_IDLE_APP, RDS, observed=observed
        )
        assert disclosure.matched
        assert all(lane.basis == "observed" for lane in disclosure.lanes)
        assert disclosure.lanes[1].configured == "db.t4g.medium"


class TestReadingTheObservedInstanceClassBackOut:
    """The standing-cost lane's only honest source for what AWS is running.

    It used to compare two hardcoded constants and assert the answer, which is
    how it came to say "all four instances are running db.t4g.micro" after the
    fleet had been resized to db.t4g.medium. Anything that claims live state has
    to come through here, and here returns None when nobody looked.
    """

    @staticmethod
    def _armed(instance_class: str):
        return build_capacity_disclosure(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            RDS,
            observed=_observed_capacity({"competitor": {"instance_class": instance_class}}),
        )

    def test_an_observed_class_is_returned(self) -> None:
        assert observed_rds_instance_class(self._armed("db.t4g.medium")) == "db.t4g.medium"

    def test_a_class_nobody_observed_is_not_invented(self) -> None:
        # The disclosure still shows a class here -- it falls back to the
        # configured constant so the lane can render. That fallback is not an
        # observation, and laundering it into one is the whole bug.
        disclosure = build_capacity_disclosure(RoundId.SURVIVE_CONNECTION_SPIKE, RDS)
        assert disclosure.lanes[1].configured == RDS_INSTANCE_CLASS
        assert disclosure.lanes[1].basis == "configured"
        assert observed_rds_instance_class(disclosure) is None

    def test_an_aurora_round_reports_no_rds_class(self) -> None:
        disclosure = build_capacity_disclosure(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            AURORA,
            observed=_observed_capacity(
                {"competitor": {"min_capacity_acu": 0, "max_capacity_acu": 2}}
            ),
        )
        assert observed_rds_instance_class(disclosure) is None

    def test_no_disclosure_at_all_reports_nothing(self) -> None:
        assert observed_rds_instance_class(None) is None

    def test_a_class_outside_the_approved_table_is_still_reported(self) -> None:
        # Reporting is not approving. If AWS says it is running something this
        # repo never sanctioned, that is exactly when the cost lane most needs
        # to hear about it, so the reader must not filter by the memory table.
        assert observed_rds_instance_class(self._armed("db.r7g.4xlarge")) == "db.r7g.4xlarge"
