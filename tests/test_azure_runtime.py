"""The serving App on an Azure installation: every AWS builder hands it to the Azure side.

"Nothing to do with AWS" is a test, not a promise (design 11.4): each path here runs with boto3's
session and client constructors made to raise, so a lane that reached for AWS would fail loudly.
Identifiers are placeholders; nothing here reaches Azure or Databricks.
"""

from __future__ import annotations

import asyncio
from typing import Any

import boto3
import pytest
from test_azure_manifest import manifest as azure_section
from test_azure_manifest import server as azure_server
from test_azure_round5_engine import seal as round5_seal
from test_manifest import _round5_round4_resources
from test_manifest_v8 import azure_manifest

from server import lifecycle
from server.azure_recovery import AzurePostgresRecoveryAdapter
from server.azure_runtime import (
    ADMIN_PASSWORD_ENV,
    AzureRuntimeError,
    admin_password,
    app_arm_client,
)
from server.azure_safe_change import AzurePostgresSafeChangeAdapter
from server.capacity import build_capacity_disclosure
from server.models import CompetitorId, RoundId
from server.pricing import build_cost_receipt
from server.reconcile import PRESENCE_MISSING, PRESENCE_PRESENT
from server.targets import TargetResolver

AWS_WORDS = ("AWS", "Aurora", "RDS", "Glue", "DMS")


@pytest.fixture(autouse=True)
def no_aws(monkeypatch):
    def refuse(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("an Azure installation's path made a boto3 client")

    monkeypatch.setattr(boto3, "Session", refuse)
    monkeypatch.setattr(boto3, "client", refuse)


def sealed(**changes: Any):
    """An Azure installation that has sealed Round 4's Lakebase half and Rounds 2-6' servers."""

    base = azure_manifest()
    round4_lakebase = base.round_lakebase(4)
    round4 = _round5_round4_resources().model_copy(update={
        "project_uid": round4_lakebase.project_uid, "branch_uid": round4_lakebase.branch_uid,
        "branch": round4_lakebase.branch_name, "endpoint_name": round4_lakebase.endpoint_name})
    servers = {key: azure_server(f"{key}-src") for key in ("r2", "r3", "r4", "r5", "r6")}
    return azure_manifest(azure=azure_section(servers=servers), round4=round4, **changes)


class FakeRunner:
    """A Databricks REST runner that is never asked anything at construction."""

    async def json(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no Databricks call at construction")


def test_round_two_builds_on_the_azure_adapter_and_no_aws_client() -> None:
    from server.safe_change_live import build_safe_change_engine

    engine = build_safe_change_engine(sealed(), databricks_runner=FakeRunner(), environment={})
    adapter = engine.competitors[CompetitorId.AZURE_POSTGRES]
    assert type(adapter) is AzurePostgresSafeChangeAdapter
    assert adapter.config.source_server == "r2-src" and adapter.config.region == "westus2"
    assert engine.scope.cloud == "azure" and engine.scope.aws_account_id == ""
    assert set(engine.competitors) == {CompetitorId.AZURE_POSTGRES}


def test_round_three_builds_on_the_azure_recovery_adapter() -> None:
    from server.recovery_live import build_recovery_engine

    engine = build_recovery_engine(sealed(), cleanup_only=True)
    adapter = engine.competitors[CompetitorId.AZURE_POSTGRES]
    assert type(adapter) is AzurePostgresRecoveryAdapter
    assert adapter.config.source_server == "r3-src"


def test_every_engine_the_app_builds_shares_one_record_of_restores_sent() -> None:
    # A towel's cleanup runs on an engine of its own, and must know the bout's restore is still
    # on its way (bar 3, 2026-10-21).
    from server.azure_safe_change import RESTORES_SENT
    from server.recovery_live import build_recovery_engine
    from server.safe_change_live import build_safe_change_engine

    bout = build_safe_change_engine(sealed(), databricks_runner=FakeRunner(), environment={})
    cleanup = build_recovery_engine(sealed(), cleanup_only=True)
    for engine in (bout, cleanup):
        assert engine.competitors[CompetitorId.AZURE_POSTGRES]._restores is RESTORES_SENT


def test_rounds_two_and_three_keep_the_aws_editions_bounds() -> None:
    # Built without them, the engines took their own defaults: an arm older than 60 s was refused
    # at the bell while the card read armed for 180 s, and a lane ran 1,800 s instead of the bout's
    # 900 s limit (a Round 3 bout ran 1,175 s, 2026-10-20).
    from server.bout_limit import BOUT_TIME_LIMIT_SECONDS
    from server.recovery_live import build_recovery_engine
    from server.safe_change_live import DEFAULT_RUN_TIMEOUT_SECONDS, build_safe_change_engine

    round2 = build_safe_change_engine(sealed(), databricks_runner=FakeRunner(), environment={})
    round3 = build_recovery_engine(sealed(), cleanup_only=True)
    for engine in (round2, round3):
        assert (engine.arm_ttl_seconds, engine.run_timeout_seconds,
                engine.reset_timeout_seconds) == (180.0, BOUT_TIME_LIMIT_SECONDS,
                                                  DEFAULT_RUN_TIMEOUT_SECONDS)
    assert round2.preflight_timeout_seconds == 180.0
    tuned = build_safe_change_engine(sealed(), databricks_runner=FakeRunner(),
                                     environment={"ANTI_DEMO_ARM_TTL_SECONDS": "240"})
    assert tuned.arm_ttl_seconds == 240.0


def with_lanes(manifest: Any) -> Any:
    """The same installation once both data flows have been proven and sealed."""

    from server.azure_manifest import AzureDataFlowSeal

    def lane(key: str, container: str, folder: str) -> AzureDataFlowSeal:
        return AzureDataFlowSeal(
            factory="adf-test", pipeline=f"{key}_pipeline", data_flow=f"{key}_flow",
            runtime="ir-test", server_endpoint=f"mpe-{key}-postgres",
            storage_endpoint="mpe-adls-dfs", container=container, folder=folder)

    azure = manifest.azure.model_copy(update={
        "round4": lane("r4", "r4source", "catalog/__unitystorage/tables/source"),
        "round6": lane("r6", "r6history", "live_orders")})
    return manifest.model_copy(update={"azure": azure})


def test_round_four_races_the_synced_table_against_the_sealed_data_flow(monkeypatch) -> None:
    # The Azure edition's first Round 4 bout was refused while this engine was built: Round 4's
    # Lakebase engine accepted manifests v2-v7, and an Azure installation's is v8 (2026-10-19).
    from types import SimpleNamespace

    from server import model_score_live
    from server.azure_runtime import build_azure_round4_race_engine

    monkeypatch.setattr(model_score_live, "WorkspaceClient", lambda *_a, **_k: SimpleNamespace())
    monkeypatch.setenv(ADMIN_PASSWORD_ENV, "for-construction-only")
    manifest = with_lanes(sealed())
    round4 = manifest.round4
    contract = model_score_live.ModelScoreContract(
        pipeline_id=round4.pipeline_id, source_table=round4.source_table_full_name,
        synced_table=f"{round4.physical_database}.{round4.physical_schema}.{round4.physical_table}")
    manifest = manifest.model_copy(
        update={"round4": round4.model_copy(update={"contract_sha256": contract.sha256})})
    engine = build_azure_round4_race_engine(manifest, CompetitorId.AZURE_POSTGRES)
    assert [type(lane).__name__ for lane in engine.lanes] == ["LakebaseSyncLane", "AdfWriterLane"]


def test_round_six_races_the_change_feed_against_the_sealed_extract(monkeypatch) -> None:
    from types import SimpleNamespace

    from test_manifest import _v7_round6

    from server import live_orders
    from server.azure_runtime import build_azure_round6_race_engine

    monkeypatch.setattr(live_orders, "WorkspaceClient", lambda *_a, **_k: SimpleNamespace())
    monkeypatch.setenv(ADMIN_PASSWORD_ENV, "for-construction-only")
    round6 = _v7_round6(azure_manifest().round_lakebase(6))
    engine = build_azure_round6_race_engine(with_lanes(sealed(round6=round6)),
                                            CompetitorId.AZURE_POSTGRES)
    assert [type(lane).__name__ for lane in engine.lanes] == ["LakebaseFeedLane", "AdfExtractLane"]


def test_round_one_times_lakebase_alone_without_asking_azure() -> None:
    _lakebase, azure = TargetResolver().resolve(CompetitorId.AZURE_POSTGRES)
    verdict = asyncio.run(azure.assert_armed())
    assert verdict["eligible"] is False and verdict["state"] == "NO_SCALE_TO_ZERO"
    assert verdict["azure_calls"] == 0


@pytest.mark.parametrize("round_id", list(RoundId), ids=lambda item: item.value)
def test_capacity_and_cost_name_azures_products_alone(round_id: RoundId) -> None:
    capacity = build_capacity_disclosure(round_id, CompetitorId.AZURE_POSTGRES)
    receipt = build_cost_receipt(round_id, CompetitorId.AZURE_POSTGRES)
    words = capacity.model_dump_json() + receipt.model_dump_json()
    assert not [word for word in AWS_WORDS if word in words]
    # Lakebase's Azure rate is the workspace's own: pending, never AWS's $0.26/DBU.
    assert all(line.unit_rate_usd is None for line in receipt.lines if line.lane_id == "lakebase")


def test_round_fives_receipt_lists_azures_runner_overhead_never_in_a_bout() -> None:
    receipt = build_cost_receipt(RoundId.SURVIVE_CONNECTION_SPIKE, CompetitorId.AZURE_POSTGRES)
    shared = {line.component: line for line in receipt.lines if line.lane_id == "shared"}
    assert set(shared) == {
        "Two isolated Standard_D8ls_v7 runners", "Two isolated runner Standard HDD S4 OS disks",
        "Runner NAT gateway", "Runner NAT public IPv4 address",
        "Service Bus Standard namespace (the runners' control queues)"}
    assert all(line.scope == "installation_overhead" for line in shared.values())
    # D8ls v7 Linux, westus2, pay as you go (Retail Prices API, read 2026-10-20).
    assert shared["Two isolated Standard_D8ls_v7 runners"].unit_rate_usd == 0.469


def test_the_deployed_posture_reads_the_azure_section() -> None:
    def posture_of(manifest: Any) -> lifecycle.DeployedAwsPosture:
        lifecycle.reset_deployed_aws_posture_cache()
        try:
            return lifecycle.deployed_aws_posture(manifest=manifest)
        finally:
            lifecycle.reset_deployed_aws_posture_cache()

    posture = posture_of(sealed())
    assert posture.egress_sealed and posture.egress_prefix_count == 2
    assert not posture.round4_aws_sealed and not posture.runtime_role_sealed
    # A sealed Round 5 is the deployed App's to run: there is no AWS control role to share.
    # The first deployed Round 5 warmed READY and sat behind AWS's refusal (2026-10-20).
    with_round5 = sealed()
    with_round5.round5_azure = round5_seal()
    assert posture_of(with_round5).runtime_role_sealed


class FakeArm:
    def __init__(self, absent: set[str]) -> None:
        self.absent = absent
        self.asked: list[str] = []

    def get_server(self, name: str) -> dict[str, Any] | None:
        self.asked.append(name)
        return None if name in self.absent else {"name": name}


@pytest.mark.parametrize(
    "absent, state", [(set(), PRESENCE_PRESENT), ({"r6-src"}, PRESENCE_MISSING)]
)
def test_presence_is_one_read_per_sealed_server(monkeypatch, absent, state) -> None:
    arm = FakeArm(absent)
    monkeypatch.setattr("server.azure_runtime.app_arm_client", lambda *_a, **_k: arm)
    presence, report, _ttl = lifecycle._observe_azure_presence(sealed())
    assert presence.state == state and report is None
    assert sorted(arm.asked) == ["r2-src", "r3-src", "r4-src", "r5-src", "r6-src"]
    assert "Azure" in presence.detail and "Aurora" not in presence.detail


def test_the_deployed_app_takes_its_token_from_its_service_credential() -> None:
    from server.azure_manifest import AzureUnityCatalogSeal

    uc = AzureUnityCatalogSeal(
        storage_credential="anti_demo_test_uc", external_locations={},
        catalog="anti_demo_test", app_service_credential="anti_demo_test_app")
    manifest = azure_manifest(azure=azure_section(unity_catalog=uc.model_dump()))

    class Credentials:
        def __init__(self) -> None:
            self.asked: list[tuple[str, Any]] = []

        def generate_temporary_service_credential(self, name: str, *, azure_options: Any) -> Any:
            self.asked.append((name, azure_options.resources))
            return type("Minted", (), {"azure_aad": type("Token", (), {"aad_token": "t0ken"})()})()

    workspace = type("Workspace", (), {"credentials": Credentials()})()
    arm = app_arm_client(manifest, environment={"ANTI_DEMO_ENV": "databricks-app"},
                         workspace=workspace)
    assert arm._bearer() == "t0ken"
    assert workspace.credentials.asked == [
        ("anti_demo_test_app", ["https://management.azure.com/.default"])]
    with pytest.raises(AzureRuntimeError, match="service credential is not sealed"):
        app_arm_client(azure_manifest(), environment={"ANTI_DEMO_ENV": "databricks-app"},
                       workspace=workspace)


def test_the_admin_password_is_the_apps_binding_and_never_a_default() -> None:
    assert admin_password({ADMIN_PASSWORD_ENV: "bound"})() == "bound"
    with pytest.raises(AzureRuntimeError, match="not bound"):
        admin_password({"ANTI_DEMO_ENV": "databricks-app"})()


def test_the_ingress_check_reads_the_azure_operator(monkeypatch) -> None:
    manifest = sealed()
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda **_k: "192.0.2.10/32")
    drift, _ttl = lifecycle._observe_operator_ingress(manifest)
    assert drift is None
    monkeypatch.setattr(lifecycle, "detect_operator_cidr", lambda **_k: "192.0.2.99/32")
    drift, _ttl = lifecycle._observe_operator_ingress(manifest)
    assert drift is not None and drift.sealed_cidr == "192.0.2.10/32"
