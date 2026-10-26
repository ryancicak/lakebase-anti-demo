"""Round 5's engine on Azure: AWS's engine and warm provider, with Azure's one competitor.

`build_azure_connection_spike_engine` is `build_connection_spike_live_engine` for the Azure seal
(`Round5AzureResources`): the same `LiveConnectionSpikeEngine`, with Azure's setup orchestrator
(PgBouncer timed at the bell) and lane adapters (Run Command where SSM was).

`AzureRound5WarmProvider` is `LiveRound5WarmProvider` with one variant. AWS warms two competitors
on the same physical runners (Aurora, then RDS from Aurora's runners) and seals receipts from its
AWS seal; Azure warms one, `Round5Variant.AZURE`, and seals them from its own. The rest is
inherited: reconcile, re-establishment, adopted engines, the claimed-dispatch rotation. Its warm
failures are classified as AWS's are, under the same codes, so the coordinator retries or blocks
the same way.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from .azure_arm import ArmClient, ArmError
from .azure_pgbouncer import AzurePgBouncer
from .azure_round5_adapter import AzureConnectionSpikeAdapter, AzureConnectionSpikeLiveConfig
from .azure_round5_runner import azure_runner_sha256
from .azure_round5_setup import AzureConnectionSpikeSetupOrchestrator, AzureRound5SetupConfig
from .connection_spike_live import (
    DISPATCH_CAPSULE_SAFETY_SECONDS,
    FANIN_RUNNER_INSTANCE_TYPE,
    SETUP_DEADLINE_SECONDS,
    ConnectionSpikeCleanupError,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveSourceUnavailableError,
    ConnectionSpikeLiveSourceUnresolvedError,
    ConnectionSpikeTarget,
    FanInContract,
    LiveConnectionSpikeEngine,
    LiveRound5WarmEngineReceipt,
    LiveRound5WarmProvider,
    Round5ResidentBindingChangedError,
    fanin_capacity_model_sha256,
    runner_harness_sha256,
)
from .manifest import DemoManifest, Round5AzureResources
from .models import CompetitorId

logger = logging.getLogger(__name__)

COMPETITOR = CompetitorId.AZURE_POSTGRES


def azure_round5_config_sha256(baseline_sha256: str, contract_sha256: str) -> str:
    """The seal's config digest, computed as AWS's is."""

    return hashlib.sha256(json.dumps(
        {"baseline_sha256": baseline_sha256, "contract_sha256": contract_sha256},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _targets(resources: Round5AzureResources) -> tuple[ConnectionSpikeTarget, ...]:
    return (
        ConnectionSpikeTarget(
            lane_id="lakebase", secret_arn="", endpoint_host=resources.lakebase_pooled_host,
            credential_host=resources.lakebase_direct_host,
            credential_sha256=resources.lakebase_credential_sha256,
            observer_credential_sha256=resources.lakebase_observer_credential_sha256),
        ConnectionSpikeTarget(
            # The endpoint is the server's own: Azure's pooled path is this host, on 6432.
            lane_id="competitor", secret_arn=resources.competitor_master_secret,
            endpoint_host=resources.server_host, credential_host=resources.server_host,
            competitor_id=COMPETITOR.value, competitor_target_id=resources.server_name,
            competitor_resource_id=resources.server_resource_id,
            credential_sha256=resources.competitor_credential_sha256,
            observer_credential_sha256=resources.competitor_observer_credential_sha256),
    )


def azure_lane_config(manifest: DemoManifest, lane: str) -> AzureConnectionSpikeLiveConfig:
    resources = manifest.require_azure_round5_resources()
    if azure_round5_config_sha256(resources.baseline_sha256,
                                  resources.contract_sha256) != resources.config_sha256:
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5 runtime configuration does not match its sealed digest")
    lakebase = lane == "lakebase"
    return AzureConnectionSpikeLiveConfig(
        targets=_targets(resources),
        runner_instance_id=(resources.lakebase_runner_vm if lakebase
                            else resources.competitor_runner_vm),
        resident_installation_id=(getattr(manifest, "installation_id", None)
                                  or manifest.run_id),
        runner_harness_sha256=resources.runner_harness_sha256,
        trust_bundle_sha256=resources.trust_bundle_sha256,
        vault_uri=resources.key_vault_uri,
        resident_control_queue_url=(resources.lakebase_control_queue if lakebase
                                    else resources.competitor_control_queue),
        resident_control_secret_arn=(resources.lakebase_control_secret if lakebase
                                     else resources.competitor_control_secret),
        runner_vm_size=resources.runner_vm_size,
        contract_sha256=resources.contract_sha256,
    )


def azure_setup_config(manifest: DemoManifest) -> AzureRound5SetupConfig:
    resources = manifest.require_azure_round5_resources()
    return AzureRound5SetupConfig(
        baseline_sha256=resources.baseline_sha256,
        deterministic_name_prefix=f"anti-demo-{manifest.run_id}"[:40],
        lakebase_pooled_host=resources.lakebase_pooled_host,
        lakebase_credential_sha256=resources.lakebase_credential_sha256,
        competitor_credential_sha256=resources.competitor_credential_sha256,
        proxy_secret_arn=resources.competitor_master_secret,
        competitor_target_id=resources.server_name,
        competitor_resource_id=resources.server_resource_id,
        competitor_host=resources.server_host,
        runner_vm_names={"lakebase": resources.lakebase_runner_vm,
                         "competitor": resources.competitor_runner_vm},
    )


def build_azure_connection_spike_engine(
    manifest: DemoManifest,
    *,
    arm: ArmClient,
    run_script: Callable[..., Any],
    pgbouncer: AzurePgBouncer,
    journal: Any,
    fence: Any,
    fresh_lakebase_host: Any,
    resident_transport: Any,
) -> LiveConnectionSpikeEngine:
    """AWS's Round 5 engine, wired to Azure's setup and adapters from the Azure seal."""

    resources = manifest.require_azure_round5_resources()
    if resources.runner_harness_sha256 != runner_harness_sha256():
        raise ConnectionSpikeLiveConfigurationError(
            "Installed Round 5 runner assets do not match the sealed harness digest")
    if resources.azure_runner_sha256 != azure_runner_sha256():
        # The runners still run the Azure files they were sealed with; the Azure setup run
        # refreshes them (azure_lifecycle._round5_stale), as `runner refresh` does on AWS.
        raise ConnectionSpikeLiveConfigurationError(
            "Round 5's runners run other Azure runner files than this App's: run the Azure "
            "setup again to refresh them")
    setup = AzureConnectionSpikeSetupOrchestrator(
        azure_setup_config(manifest), arm=arm, pgbouncer=pgbouncer, journal=journal,
        fence=fence, fresh_lakebase_host=fresh_lakebase_host)
    adapters = {
        lane: AzureConnectionSpikeAdapter(azure_lane_config(manifest, lane), arm=arm,
                                          run_script=run_script,
                                          resident_transport=resident_transport)
        for lane in ("lakebase", "competitor")
    }
    return LiveConnectionSpikeEngine(adapters["lakebase"], lane_adapters=adapters,
                                     setup_orchestrator=setup)


class AzureRound5WarmProvider(LiveRound5WarmProvider):
    """AWS's warm provider with Azure's one competitor variant."""

    def _variant_engine(self) -> LiveConnectionSpikeEngine | None:
        return self._engines.get(COMPETITOR.value)

    @staticmethod
    def _retryable(error: BaseException) -> bool:
        """AWS's taxonomy, with Azure's own failures in passing beside it.

        ARM that gave no answer, throttled (429), or answered from its gateway or the service
        behind it (5xx); a VM still busy with another Run Command (409); and the network under
        either. A refusal (403, 404, a bad request) still fails closed, as AWS's does.
        """

        if LiveRound5WarmProvider._retryable(error):
            return True
        current: BaseException | None = error
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, ArmError) and (
                current.status is None or current.status in (409, 429) or current.status >= 500
            ):
                return True
            if isinstance(current, OSError):  # urllib's URLError, socket timeouts
                return True
            current = current.__cause__
        return False

    @staticmethod
    def _classified(exc: BaseException, generation: int, requires_cleaned_bout: bool,
                    retryable: Callable[[BaseException], bool]) -> BaseException:
        """AWS's warm-failure taxonomy (`LiveRound5WarmProvider.prepare`), code for code."""

        from .manager import operator_diagnosis
        from .round5_warm import BlockedWarmError, RetryableWarmError

        diagnosis = operator_diagnosis(exc)
        if isinstance(exc, ConnectionSpikeLiveSourceUnresolvedError):
            logger.warning("round5_warm_source_unresolved generation=%s diagnosis=%s",
                           generation, diagnosis)
            return RetryableWarmError("warm_source_unresolved")
        if isinstance(exc, ConnectionSpikeLiveSourceUnavailableError):
            logger.warning("round5_warm_source_unavailable generation=%s diagnosis=%s",
                           generation, diagnosis)
            return RetryableWarmError("warm_source_unavailable")
        if isinstance(exc, ConnectionSpikeCleanupError):
            logger.warning("round5_warm_runner_settlement_unconfirmed generation=%s diagnosis=%s",
                           generation, diagnosis)
            return RetryableWarmError("warm_runner_settlement_unconfirmed")
        if retryable(exc):
            logger.warning("round5_warm_provider_retryable generation=%s diagnosis=%s",
                           generation, diagnosis)
            return RetryableWarmError("warm_provider_retryable")
        if isinstance(exc, ConnectionSpikeLiveConfigurationError):
            logger.warning("round5_warm_baseline_invalid generation=%s diagnosis=%s",
                           generation, diagnosis)
            return BlockedWarmError("warm_baseline_invalid")
        if isinstance(exc, Round5ResidentBindingChangedError) and requires_cleaned_bout:
            logger.warning("round5_warm_resident_binding_transition generation=%s diagnosis=%s",
                           generation, diagnosis)
            return RetryableWarmError("warm_resident_binding_transition")
        logger.warning("round5_warm_baseline_unexpected generation=%s diagnosis=%s",
                       generation, diagnosis)
        return BlockedWarmError("warm_baseline_unexpected")

    async def prepare(self, *, generation: int, coordinator_fence: int, process_epoch: str,
                      broker_epoch: str, warm_attempt_token: str,
                      requires_cleaned_bout: bool) -> object:
        from .round5_warm import BlockedWarmError, Round5Variant

        del process_epoch
        self._requires_cleaned_bout = requires_cleaned_bout
        if not requires_cleaned_bout:
            self._cleaned_bout_id = None
        await self._check_authority()
        try:
            engine = self._factory_engine(COMPETITOR)
            receipt = await engine.warm(generation, warm_attempt_token)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise self._classified(exc, generation, requires_cleaned_bout,
                                   self._retryable) from exc
        boots = receipt.runner_process_boot_ids
        if (receipt.warm_attempt_token != warm_attempt_token
                or set(boots) != {"lakebase", "competitor"}
                or any((not boot) or boot == "unattested" for boot in boots.values())):
            raise BlockedWarmError("warm_ready_without_fresh_preload")
        self._engines = {COMPETITOR.value: engine}
        self._receipts = {COMPETITOR.value: receipt}
        self._credential_generation += 1
        return self._assemble_preparation(
            generation=generation, coordinator_fence=coordinator_fence, broker_epoch=broker_epoch,
            warm_attempt_token=warm_attempt_token, engines={Round5Variant.AZURE: engine},
            receipts={Round5Variant.AZURE: receipt})

    async def validate_ready(self, slot: object, capsule: object) -> bool:
        del capsule
        engine = self._variant_engine()
        if slot.shared_receipt is None or engine is None:
            return False
        try:
            return await engine.validate_ready_provenance(slot.shared_receipt,
                                                          slot.warm_attempt_token or "")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            from .round5_warm import RetryableWarmError

            if isinstance(exc, RetryableWarmError):
                raise
            if self._retryable(exc):
                raise RetryableWarmError("runner_provenance_probe_retryable") from exc
            return False

    async def _refreshed(self, slot: object, *, failure: str) -> tuple[Any, Any]:
        from .round5_warm import BlockedWarmError, RetryableWarmError, Round5Variant

        engine = self._variant_engine()
        if engine is None:
            raise BlockedWarmError("launch_capsule_missing")
        try:
            receipt = await engine.refresh_warm(slot.generation)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._retryable(exc):
                raise RetryableWarmError("credential_refresh_retryable") from exc
            raise BlockedWarmError(failure) from exc
        return {Round5Variant.AZURE: engine}, {Round5Variant.AZURE: receipt}

    async def refresh_preparation(self, slot: object, capsule: object) -> object:
        del capsule
        engines, receipts = await self._refreshed(slot, failure="credential_refresh_failed")
        self._receipts = {COMPETITOR.value: next(iter(receipts.values()))}
        self._credential_generation += 1
        # The slot's broker_epoch is kept, as AWS keeps it (see its refresh_preparation).
        return self._assemble_preparation(
            generation=slot.generation, coordinator_fence=slot.coordinator_fence,
            broker_epoch=slot.broker_epoch, warm_attempt_token=slot.warm_attempt_token,
            engines=engines, receipts=receipts)

    async def refresh_capsule(self, slot: object, previous: object) -> object:
        del previous
        engines, receipts = await self._refreshed(slot, failure="credential_refresh_failed")
        self._credential_generation += 1
        return self._capsule(generation=slot.generation, coordinator_fence=slot.coordinator_fence,
                             broker_epoch=slot.broker_epoch, engines=engines, receipts=receipts)

    def _assemble_preparation(self, *, generation: int, coordinator_fence: int,
                              broker_epoch: str, warm_attempt_token: str,
                              engines: Mapping[object, LiveConnectionSpikeEngine | None],
                              receipts: Mapping[object, LiveRound5WarmEngineReceipt]) -> object:
        from .round5_warm import (
            BlockedWarmError,
            Round5RunnerReceipt,
            Round5SharedReceipt,
            Round5Variant,
            Round5WarmPreparation,
        )

        resources = self._manifest.require_azure_round5_resources()
        runner_expiration = datetime.now(UTC) + timedelta(minutes=45)
        reference = receipts[Round5Variant.AZURE]
        if reference.warm_attempt_token != warm_attempt_token:
            raise BlockedWarmError("runner_boot_identity_changed")
        harnesses = {lane: reference.arm.preflights[lane].runner_harness_sha256
                     for lane in ("lakebase", "competitor")}
        if (len(set(harnesses.values())) != 1
                or next(iter(harnesses.values())) != resources.runner_harness_sha256):
            raise BlockedWarmError("runner_harness_identity_changed")
        image_digest = next(iter(harnesses.values()))
        capacity_digest = fanin_capacity_model_sha256()
        engine = engines[Round5Variant.AZURE]
        assert engine is not None
        runners = {
            lane: Round5RunnerReceipt(
                lane_id=lane,
                instance_id=(resources.lakebase_runner_vm if lane == "lakebase"
                             else resources.competitor_runner_vm),
                boot_id=reference.runner_boot_ids[lane],
                process_boot_id=reference.runner_process_boot_ids[lane],
                process_pid=engine._lane_adapters[lane]._resident_process_pid,
                instance_type=FANIN_RUNNER_INSTANCE_TYPE,
                image_sha256=image_digest,
                loaded_harness_sha256=harnesses[lane],
                capacity_model_sha256=capacity_digest,
                expires_at=runner_expiration,
            )
            for lane in ("lakebase", "competitor")
        }
        shared = Round5SharedReceipt(
            source_sha256=resources.baseline_sha256,
            config_sha256=resources.config_sha256,
            runner_image_sha256=image_digest,
            fanin_contract_sha256=FanInContract().sha256,
            capacity_model_sha256=capacity_digest,
            lakebase_binding_sha256=hashlib.sha256(
                (resources.lakebase_direct_host + "\0" + resources.lakebase_pooled_host).encode()
            ).hexdigest(),
            static_network_fixture_sha256=resources.network_sha256,
            lakebase_runner=runners["lakebase"],
            competitor_runner=runners["competitor"],
        )
        return Round5WarmPreparation(
            shared_receipt=shared,
            variants={Round5Variant.AZURE: self._variant_receipt(
                Round5Variant.AZURE, reference, expires_at=runner_expiration)},
            capsule=self._capsule(generation=generation, coordinator_fence=coordinator_fence,
                                  broker_epoch=broker_epoch, engines=engines, receipts=receipts),
        )

    def _variant_receipt(self, variant: object, receipt: LiveRound5WarmEngineReceipt, *,
                         expires_at: datetime) -> object:
        from .round5_warm import Round5VariantReceipt

        resources = self._manifest.require_azure_round5_resources()
        engine = self._variant_engine()
        values = {
            "target": resources.server_resource_id,
            "source": resources.server_host,
            "secret": resources.competitor_master_secret,
            "role": resources.competitor_runner_principal_id,
            "auth": "SCRAM-SHA-256",
            "tls": "verify-full",
            "sg": resources.network_sha256,
            "subnets": resources.network_sha256,
            "vpc": resources.network_sha256,
            "request": engine._adapter.config.contract_sha256 if engine is not None else "",
        }

        def hashed(name: str) -> str:
            return hashlib.sha256(str(values[name]).encode()).hexdigest()

        # "proxy_absent": the per-bout pooler is off at warm (PgBouncer at the database-only
        # start), which the setup orchestrator's warm proved and stamped.
        return Round5VariantReceipt(
            variant=variant, target_sha256=hashed("target"), source_sha256=hashed("source"),
            secret_ref_sha256=hashed("secret"), role_sha256=hashed("role"),
            auth_sha256=hashed("auth"), tls_sha256=hashed("tls"),
            security_group_sha256=hashed("sg"), subnet_sha256=hashed("subnets"),
            vpc_sha256=hashed("vpc"), proxy_absent=True,
            proxy_absence_observed_at=receipt.setup_context.observed_at,
            request_template_sha256=hashed("request"), expires_at=expires_at)

    def _capsule(self, *, generation: int, coordinator_fence: int, broker_epoch: str,
                 engines: Mapping[object, LiveConnectionSpikeEngine | None],
                 receipts: Mapping[object, LiveRound5WarmEngineReceipt]) -> object:
        from .round5_warm import Round5LaunchCapsule, Round5Variant

        engine = engines.get(Round5Variant.AZURE)
        receipt = receipts[Round5Variant.AZURE]
        if engine is None:
            raise ValueError("Round 5 capsule requires its variant engine")
        control_expires = receipt.setup_context.clients.expires_at
        dispatch_expires = {lane: receipt.dispatch_expires_at[lane]
                            for lane in ("lakebase", "competitor")}
        return Round5LaunchCapsule(
            generation=generation,
            coordinator_fence=coordinator_fence,
            credential_generation=self._credential_generation,
            broker_epoch=broker_epoch,
            runner_contexts=dict(dispatch_expires),
            aws_control_contexts={Round5Variant.AZURE: receipt.setup_context},
            lakebase_context=receipt.setup_context,
            variant_contexts={Round5Variant.AZURE: engine},
            control_expires_at=control_expires,
            dispatch_expires_at=dispatch_expires,
            expires_at=min(control_expires, *dispatch_expires.values()),
            renew_by=min(
                control_expires - timedelta(seconds=SETUP_DEADLINE_SECONDS + 60),
                *(value - timedelta(seconds=DISPATCH_CAPSULE_SAFETY_SECONDS)
                  for value in dispatch_expires.values()),
            ),
            warm_attempt_token=receipt.warm_attempt_token,
        )
