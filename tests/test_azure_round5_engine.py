"""server/azure_round5_engine.py: AWS's engine and warm provider, with Azure's one competitor.

The warm provider warms one engine and seals its receipts from the Azure seal; its failures are
classified under AWS's codes. The engine factory builds AWS's `LiveConnectionSpikeEngine` from the
Azure seal, with Azure's setup orchestrator and lane adapters.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from server.azure_round5_adapter import AzureConnectionSpikeAdapter
from server.azure_round5_engine import (
    AzureRound5WarmProvider,
    azure_lane_config,
    azure_round5_config_sha256,
    build_azure_connection_spike_engine,
)
from server.azure_round5_runner import azure_runner_sha256
from server.azure_round5_setup import AzureConnectionSpikeSetupOrchestrator
from server.connection_spike_live import (
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveSourceUnavailableError,
    runner_harness_sha256,
)
from server.manifest import Round5AzureResources
from server.round5_warm import BlockedWarmError, RetryableWarmError, Round5Variant

NOW = datetime(2026, 10, 20, 23, 45, tzinfo=UTC)
CONTRACT = "c" * 64
BASELINE = "b" * 64


def seal(**overrides: Any) -> Round5AzureResources:
    values: dict[str, Any] = dict(
        lakebase_direct_host="lakebase-direct.test", lakebase_pooled_host="lakebase-pooled.test",
        server_name="psql-test-r5",
        server_resource_id=("/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/"
                            "rg-test/providers/Microsoft.DBforPostgreSQL/flexibleServers/"
                            "psql-test-r5"),
        server_host="psql-test-r5.postgres.database.azure.com",
        lakebase_runner_vm="vm-test-r5-runner-0", competitor_runner_vm="vm-test-r5-runner-1",
        runner_vm_size="Standard_D8ls_v7",
        lakebase_runner_principal_id="11111111-1111-1111-1111-111111111111",
        competitor_runner_principal_id="22222222-2222-2222-2222-222222222222",
        runner_egress_ip="203.0.113.7", service_bus_namespace="sb-test-r5",
        lakebase_control_queue="sb://sb-test-r5.servicebus.windows.net/queue-r5-runner-0",
        competitor_control_queue="sb://sb-test-r5.servicebus.windows.net/queue-r5-runner-1",
        key_vault_uri="https://kv-test-r5.vault.azure.net/",
        lakebase_control_secret=(
            "arn:aws:secretsmanager:westus2:000000000000:secret:round5-lakebase-control"),
        competitor_control_secret=(
            "arn:aws:secretsmanager:westus2:000000000000:secret:round5-competitor-control"),
        competitor_master_secret=(
            "arn:aws:secretsmanager:westus2:000000000000:secret:round5-competitor-master"),
        network_sha256="d" * 64, runner_harness_sha256=runner_harness_sha256(),
        azure_runner_sha256=azure_runner_sha256(), trust_bundle_sha256="f" * 64,
        runner_public_key_sha256="1" * 64, competitor_runner_public_key_sha256="2" * 64,
        lakebase_credential_sha256="3" * 64, lakebase_observer_credential_sha256="4" * 64,
        competitor_credential_sha256="5" * 64, competitor_observer_credential_sha256="6" * 64,
        baseline_sha256=BASELINE, contract_sha256=CONTRACT,
        config_sha256=azure_round5_config_sha256(BASELINE, CONTRACT), sealed_at=NOW)
    return Round5AzureResources(**{**values, **overrides})


class Manifest:
    cloud = "azure"
    run_id = "ad-20261020-test"
    installation_id = "ad-20261020-test"

    def __init__(self, resources: Round5AzureResources) -> None:
        self.round5_azure = resources

    def require_azure_round5_resources(self) -> Round5AzureResources:
        return self.round5_azure


class Engine:
    """The parts of a warmed `LiveConnectionSpikeEngine` the provider reads."""

    def __init__(self, resources: Round5AzureResources, *, fails: BaseException | None = None,
                 token: str = "attempt-1") -> None:
        self.fails, self.token = fails, token
        self.warmed: list[int] = []
        self.refreshed: list[int] = []
        self._lane_adapters = {lane: SimpleNamespace(_resident_process_pid=pid)
                               for lane, pid in (("lakebase", 4101), ("competitor", 4102))}
        self._adapter = SimpleNamespace(config=SimpleNamespace(contract_sha256=CONTRACT))
        self.resources = resources

    def receipt(self) -> Any:
        preflights = {
            lane: SimpleNamespace(runner_harness_sha256=self.resources.runner_harness_sha256)
            for lane in ("lakebase", "competitor")}
        return SimpleNamespace(
            arm=SimpleNamespace(preflights=preflights),
            runner_boot_ids={"lakebase": "boot-0", "competitor": "boot-1"},
            runner_process_boot_ids={"lakebase": "process-0", "competitor": "process-1"},
            warm_attempt_token=self.token,
            setup_context=SimpleNamespace(
                observed_at=NOW, clients=SimpleNamespace(expires_at=NOW + timedelta(hours=1))),
            dispatch_expires_at={"lakebase": NOW + timedelta(hours=1),
                                 "competitor": NOW + timedelta(hours=1)})

    async def warm(self, generation: int, warm_attempt_token: str) -> Any:
        self.warmed.append(generation)
        if self.fails is not None:
            raise self.fails
        return self.receipt()

    async def refresh_warm(self, generation: int) -> Any:
        self.refreshed.append(generation)
        return self.receipt()

    async def validate_ready_provenance(self, shared: Any, token: str) -> bool:
        return token == self.token


def provider(engine: Engine, resources: Round5AzureResources) -> AzureRound5WarmProvider:
    asked: list[str] = []

    def factory(competitor: Any) -> Engine:
        asked.append(str(competitor))
        return engine

    built = AzureRound5WarmProvider(Manifest(resources), factory)  # type: ignore[arg-type]
    built.asked = asked  # type: ignore[attr-defined]
    return built


async def warm(built: AzureRound5WarmProvider) -> Any:
    return await built.prepare(generation=3, coordinator_fence=9, process_epoch="p",
                               broker_epoch="broker-1", warm_attempt_token="attempt-1",
                               requires_cleaned_bout=False)


async def test_prepare_warms_the_one_azure_engine_and_seals_receipts_from_the_azure_seal() -> None:
    resources = seal()
    engine = Engine(resources)
    built = provider(engine, resources)
    preparation = await warm(built)
    assert built.asked == ["azure_postgres"] and engine.warmed == [3]  # type: ignore[attr-defined]
    assert set(preparation.variants) == {Round5Variant.AZURE}
    shared = preparation.shared_receipt
    assert shared.lakebase_runner.instance_id == "vm-test-r5-runner-0"
    assert shared.competitor_runner.instance_id == "vm-test-r5-runner-1"
    assert shared.competitor_runner.process_pid == 4102
    assert shared.static_network_fixture_sha256 == resources.network_sha256
    assert preparation.capsule.variant_contexts == {Round5Variant.AZURE: engine}
    assert preparation.capsule.coordinator_fence == 9


async def test_a_warm_that_fails_is_classified_under_awss_codes() -> None:
    resources = seal()
    unavailable = provider(Engine(resources, fails=ConnectionSpikeLiveSourceUnavailableError(
        "server busy")), resources)
    with pytest.raises(RetryableWarmError) as raised:
        await warm(unavailable)
    assert raised.value.code == "warm_source_unavailable"
    invalid = provider(Engine(resources, fails=ConnectionSpikeLiveConfigurationError(
        "sealed server differs")), resources)
    with pytest.raises(BlockedWarmError) as blocked:
        await warm(invalid)
    assert blocked.value.code == "warm_baseline_invalid"


def test_azures_failures_in_passing_are_retried_and_its_refusals_are_not() -> None:
    from server.azure_arm import ArmError

    retryable = AzureRound5WarmProvider._retryable
    assert retryable(ArmError("ARM gave no answer to a GET (TimeoutError)"))
    assert retryable(ArmError("throttled", status=429))
    assert retryable(ArmError("gateway", status=504))
    assert retryable(ArmError("Run Command busy", status=409))
    assert retryable(RuntimeError("wrapped").with_traceback(None)) is False
    wrapped = ConnectionSpikeLiveConfigurationError("x")
    wrapped.__cause__ = ArmError("gateway", status=503)
    assert retryable(wrapped)
    assert not retryable(ArmError("forbidden", status=403))
    assert not retryable(ArmError("not found", status=404))


async def test_a_warm_from_another_attempt_or_another_harness_is_blocked() -> None:
    resources = seal()
    with pytest.raises(BlockedWarmError) as stale:
        await warm(provider(Engine(resources, token="attempt-0"), resources))
    assert stale.value.code == "warm_ready_without_fresh_preload"
    drifted = seal(runner_harness_sha256=runner_harness_sha256())
    engine = Engine(drifted)
    engine.resources = seal(runner_harness_sha256="9" * 64)
    with pytest.raises(BlockedWarmError) as changed:
        await warm(provider(engine, drifted))
    assert changed.value.code == "runner_harness_identity_changed"


async def test_refreshes_renew_the_one_engine_and_ready_reads_its_provenance() -> None:
    resources = seal()
    engine = Engine(resources)
    built = provider(engine, resources)
    preparation = await warm(built)
    slot = SimpleNamespace(generation=3, coordinator_fence=9, broker_epoch="broker-1",
                           warm_attempt_token="attempt-1",
                           shared_receipt=preparation.shared_receipt)
    renewed = await built.refresh_preparation(slot, preparation.capsule)
    capsule = await built.refresh_capsule(slot, preparation.capsule)
    assert engine.refreshed == [3, 3]
    assert set(renewed.variants) == {Round5Variant.AZURE}
    assert capsule.broker_epoch == "broker-1"
    assert await built.validate_ready(slot, capsule)


def test_the_engine_is_awss_with_azures_setup_and_adapters() -> None:
    manifest = Manifest(seal())
    engine = build_azure_connection_spike_engine(
        manifest, arm=object(), run_script=lambda *a, **k: None,  # type: ignore[arg-type]
        pgbouncer=object(), journal=object(), fence=object(),  # type: ignore[arg-type]
        fresh_lakebase_host=object(), resident_transport=object())
    assert isinstance(engine._setup_orchestrator, AzureConnectionSpikeSetupOrchestrator)
    assert all(isinstance(adapter, AzureConnectionSpikeAdapter)
               for adapter in engine._lane_adapters.values())
    competitor = azure_lane_config(manifest, "competitor")  # type: ignore[arg-type]
    assert competitor.runner_instance_id == "vm-test-r5-runner-1"
    assert competitor.resident_control_queue_url.endswith("/queue-r5-runner-1")
    (_, target) = competitor.targets
    assert (target.competitor_id, target.endpoint_host) == (
        "azure_postgres", "psql-test-r5.postgres.database.azure.com")


def test_awss_builders_still_refuse_the_azure_competitor() -> None:
    # Only the lane target, the one type both editions build, knows Azure's competitor: an AWS
    # manifest asked for it is refused before any of its resources are read, as before.
    from server.connection_spike_live import _COMPETITOR_IDS, _competitor_manifest_bindings

    assert _COMPETITOR_IDS == {"rds_postgres", "aurora_serverless_v2"}
    with pytest.raises(ConnectionSpikeLiveConfigurationError,
                       match="AWS competitor is unsupported"):
        _competitor_manifest_bindings(object(), "azure_postgres")  # type: ignore[arg-type]


def test_a_seal_whose_config_digest_does_not_match_is_refused() -> None:
    # Runners still on the Azure files of an older App wait for the setup run that refreshes them.
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="run the Azure setup again"):
        build_azure_connection_spike_engine(
            Manifest(seal(azure_runner_sha256="e" * 64)), arm=object(),  # type: ignore[arg-type]
            run_script=lambda *a, **k: None, pgbouncer=object(),  # type: ignore[arg-type]
            journal=object(), fence=object(), fresh_lakebase_host=object(),
            resident_transport=object())
    manifest = Manifest(seal(config_sha256="0" * 64))
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="sealed digest"):
        azure_lane_config(manifest, "lakebase")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="two distinct"):
        seal(competitor_runner_vm="vm-test-r5-runner-0")
