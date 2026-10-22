"""Round 5's setup on Azure: the competitor builds its pooler at the bell, as AWS's builds a Proxy.

AWS's `LiveConnectionSpikeSetupOrchestrator` is two things. One is the sequencing every Round 5
setup needs: the bout's fence; the creation journal's intent, committed before the bell; the shared
gate and two-party launch barrier that keep the lanes' clocks fair; the cleanup handoffs; and
`reconcile_failed_cleanup` after a restart. That sequencing is what took Round 5 longest to
stabilize. The other is the AWS calls: an RDS Proxy, its target group, roles, secrets and security
groups. This class inherits the first unchanged and replaces only the second (Ryan, 2026-10-20:
AWS untouched).

- **The timed resource** is the r5 server's built-in PgBouncer (`AzurePgBouncer`). At the bell it
  is switched on, its client limit raised to 10,100, and 6432 must commit the exact transaction.
  It is the journal's one resource, under the kind the inherited sequencing looks up
  (``rds_proxy``: internal, never shown). Deleting it is the baseline reset: the limit back to
  5000, then off.
- **Warm** reads over ARM: the server, both runner VMs (running, VM agent ready: what runs Run
  Command), and the database-only start.
- **No per-bout network.** The server's firewall already admits the runners' NAT address.

**One difference AWS doesn't have.** The timed resource is a setting on a server every bout shares,
not a resource each bout owns, so it carries no bout tag. What keeps one bout's cleanup away from
another's PgBouncer is what AWS relies on as well. A new bout's setup is refused while any bout's
cleanup is unresolved (`assert_no_unresolved_bouts`), and every mutation asserts the current fence
first.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from .azure_arm import ArmClient
from .azure_pgbouncer import (
    POOLED_PORT,
    ROUND5_MAX_CLIENT_CONN,
    AzurePgBouncer,
    PgBouncerSetupError,
    PooledPathStart,
)
from .connection_spike import PublicSetupEvidence, SetupLaneObservation
from .connection_spike_journal import (
    CreationScope,
    FenceGuard,
    JournalEvent,
    ResourceObservation,
    ResourceSpec,
    Round5CreationCoordinator,
)
from .connection_spike_live import (
    SETUP_DEADLINE_SECONDS,
    ConnectionSpikeCleanupError,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeLiveSourceUnavailableError,
    ConnectionSpikeLiveSourceUnresolvedError,
    ConnectionSpikeLiveTransientError,
    ConnectionSpikeSetupLaneStop,
    LiveConnectionSpikeSetupOrchestrator,
    SetupLaneReadyCallback,
    SetupLaneStageCallback,
    SetupProgressCallback,
    _SetupResourceAdapter,
    _SetupResources,
)

logger = logging.getLogger(__name__)

#: The journal kind the inherited `setup`, `precommit_launch_intent` and cleanup look the timed
#: mutation up by. On Azure it names PgBouncer's switch; it never reaches a screen.
TIMED_KIND = "rds_proxy"
#: How long the Azure clients are good for. ARM tokens are minted as needed, so this is only the
#: window the inherited checks ask for (`prepare` wants a bout deadline plus a minute of it).
CLIENT_LIFETIME_SECONDS = 3600.0
COMPETITOR_ID = "azure_postgres"
GATE_ID = "azure_pgbouncer_exact_data_plane"
#: The server states in which the competitor still serves (azure_safe_change's own). A parameter
#: write leaves a server Updating, and every bout's own PgBouncer reset is one: AWS's "backing-up".
SERVING_STATES = frozenset({"ready", "updating"})
#: One PgBouncer reset at a time per server, in this process. After a bout the warm owner runs a
#: reconstructive pass over the bout's own cleanup (`reconcile_post_bell_claim`), and both reach
#: the delete. AWS's DeleteDBProxy is idempotent; two resets race their ARM writes. The second
#: waits for the first, then finds PgBouncer off and writes nothing (the first live bout reset it
#: twice at once, 2026-10-20).
_RESETS: dict[str, asyncio.Lock] = {}


@dataclass(frozen=True)
class AzureRound5SetupConfig:
    """Secret-free inputs for Azure's timed setup.

    The first group is what the inherited sequencing reads from AWS's config, under the same
    names; the rest is what the Azure calls need.
    """

    baseline_sha256: str
    deterministic_name_prefix: str
    lakebase_pooled_host: str
    lakebase_credential_sha256: str
    competitor_credential_sha256: str
    #: The competitor's sealed credential in Key Vault, by its ARN-shaped identifier.
    proxy_secret_arn: str
    #: The r5 Flexible Server: its name, ARM id and FQDN. Azure's pooled path is this host, on 6432.
    competitor_target_id: str
    competitor_resource_id: str
    competitor_host: str
    #: The runner VM serving each lane.
    runner_vm_names: Mapping[str, str]
    deadline_seconds: float = SETUP_DEADLINE_SECONDS
    secret_name_prefix: str = "anti-demo-round5"
    #: No role is made on Azure.
    proxy_service_role_arn: str = ""
    competitor_id: str = COMPETITOR_ID

    def __post_init__(self) -> None:
        if set(self.runner_vm_names) != {"lakebase", "competitor"}:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 on Azure needs exactly one runner VM per lane")
        if len(set(self.runner_vm_names.values())) != 2:
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 on Azure requires two distinct runner VMs")
        for name in ("baseline_sha256", "competitor_target_id", "competitor_resource_id",
                     "competitor_host", "proxy_secret_arn", "lakebase_pooled_host"):
            if not str(getattr(self, name)).strip():
                raise ConnectionSpikeLiveConfigurationError(
                    f"Round 5 Azure setup config is missing {name}")


@dataclass(frozen=True)
class AzureSetupClients:
    """The inherited checks' "clients": the ARM client, and the window they're good for."""

    arm: ArmClient
    expires_at: datetime


@dataclass(frozen=True)
class AzureWarmSetupContext:
    """What warm proved, in the shape the engine and warm provider read (`observed_at`,
    `clients.expires_at`). Azure has no security groups to carry."""

    clients: AzureSetupClients
    observed_at: datetime
    rds_security_group_id: str = ""
    proxy_security_group_id: str = ""


@dataclass
class _Switches:
    """Each bout's switch in flight, from its timed create to its gate."""

    by_bout: dict[str, PooledPathStart] = field(default_factory=dict)


class AzureConnectionSpikeSetupOrchestrator(LiveConnectionSpikeSetupOrchestrator):
    """AWS's setup sequencing, with Azure's PgBouncer as the timed resource."""

    def __init__(
        self,
        config: AzureRound5SetupConfig,
        *,
        arm: ArmClient,
        pgbouncer: AzurePgBouncer,
        journal: Any,
        fence: FenceGuard,
        fresh_lakebase_host: Any,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        **kwargs: Any,
    ) -> None:
        super().__init__(config, journal=journal, fence=fence,  # type: ignore[arg-type]
                         fresh_lakebase_host=fresh_lakebase_host, session_factory=None, **kwargs)
        self._arm = arm
        self._pgbouncer = pgbouncer
        self._clock = clock
        self._switches = _Switches()

    # Clients and warm -------------------------------------------------------------------------

    async def _assumed_clients(  # type: ignore[override]
        self, bout_id: str, *, minimum_lifetime_seconds: float = SETUP_DEADLINE_SECONDS + 60,
    ) -> AzureSetupClients:
        lifetime = max(CLIENT_LIFETIME_SECONDS, float(minimum_lifetime_seconds) + 60.0)
        return AzureSetupClients(arm=self._arm,
                                 expires_at=self._clock() + timedelta(seconds=lifetime))

    async def warm(  # type: ignore[override]
        self, generation: int, *, cleaned_bout_id: str | None = None,
    ) -> AzureWarmSetupContext:
        """Every slow prerequisite before a session can claim, read over ARM."""

        clients = await self._assumed_clients(f"warm-{generation}")
        server, *runners = await asyncio.gather(
            asyncio.to_thread(self._arm.get_server, self.config.competitor_target_id),
            *(asyncio.to_thread(self._arm.vm_ready, self.config.runner_vm_names[lane])
              for lane in ("lakebase", "competitor")),
        )
        self._require_sealed_server(server)
        for lane, ready in zip(("lakebase", "competitor"), runners, strict=True):
            if not ready:
                # AWS's "temporarily absent from SSM": retried, never a configuration fault.
                raise ConnectionSpikeLiveTransientError(
                    f"Round 5's {lane} runner VM is not running with its agent ready")
        await self._discover_orphaned_addons(clients, "", include_legacy=False,
                                             bout_id=cleaned_bout_id)
        context = AzureWarmSetupContext(clients=clients, observed_at=self._clock())
        self._warm_context = context  # type: ignore[assignment]
        return context

    def _require_sealed_server(self, server: Mapping[str, Any] | None) -> None:
        """AWS's source taxonomy: unresolved and busy are retried, a changed identity blocks."""

        if server is None:
            raise ConnectionSpikeLiveSourceUnresolvedError(
                "Round 5's Azure server did not resolve")
        properties = server.get("properties") or {}
        if (
            str(server.get("id") or "").lower() != self.config.competitor_resource_id.lower()
            or str(properties.get("fullyQualifiedDomainName") or "") != self.config.competitor_host
        ):
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5's Azure server differs from the sealed one")
        state = str(properties.get("state") or "")
        if state.lower() not in SERVING_STATES:
            raise ConnectionSpikeLiveSourceUnavailableError(
                f"Round 5's Azure server is {state or 'unknown'}, not serving")

    async def _discover_orphaned_addons(  # type: ignore[override]
        self, clients: Any, rds_security_group_id: str, *, include_legacy: bool = True,
        bout_id: str | None = None,
    ) -> None:
        """Azure's per-bout leftovers are one: PgBouncer left on. Prove the database-only start.

        A PgBouncer still on with no bout's cleanup outstanding is a leftover no journal holds
        (a reset that never landed, or a hand on the portal), so it is reset here, as each Azure
        round's own reset settles what it finds. With cleanup outstanding, that cleanup owns it.
        """

        try:
            await self._pgbouncer.prove_database_only_start()
            return
        except PgBouncerSetupError:
            if await self._journal.unresolved_bout_ids():
                # AWS's "bout-owned proxy is still deleting": the cleanup will settle it.
                raise ConnectionSpikeLiveTransientError(
                    "Round 5's Azure PgBouncer is on, and a bout's cleanup still owns it"
                ) from None
        logger.warning("Round 5's Azure PgBouncer was on with no bout's cleanup outstanding; "
                       "resetting it to the database-only start")
        await self._pgbouncer.reset_baseline()

    async def _baseline_rds_security_group(self, clients: Any) -> str:  # type: ignore[override]
        self._require_sealed_server(
            await asyncio.to_thread(self._arm.get_server, self.config.competitor_target_id))
        return ""

    def _require_rule_bindings(self, resources: _SetupResources) -> None:
        """No per-bout security-group rules on Azure: the firewall admits the runners' NAT."""

    async def _settle_commands(self, bout_id: str) -> tuple[str, ...]:
        """No setup commands run on the runners during an Azure bout."""

        return ()

    # The journaled timed resource ---------------------------------------------------------------

    def _coordinator(  # type: ignore[override]
        self, scope: CreationScope, clients: Any, resources: _SetupResources, *,
        fence: FenceGuard | None = None,
    ) -> tuple[Round5CreationCoordinator, tuple[ResourceSpec, ...]]:
        mutation_fence = fence or self._fence

        async def assert_mutation_authority() -> None:
            await mutation_fence.assert_current(scope)

        metadata = {
            "baseline_sha256": self.config.baseline_sha256,
            "competitor_id": self.config.competitor_id,
            "competitor_target_id": self.config.competitor_target_id,
            "competitor_resource_id": self.config.competitor_resource_id,
            "pooled_port": POOLED_PORT,
            "max_client_conn": ROUND5_MAX_CLIENT_CONN,
        }
        specs = (ResourceSpec(1, TIMED_KIND, resources.names.proxy_name, metadata=metadata),)
        adapters = {
            TIMED_KIND: _SetupResourceAdapter(
                lambda spec: self._switch_on(resources, spec, scope=scope),
                lambda spec, provider_id: self._inspect_pgbouncer(spec, provider_id),
                lambda observed: self._reset_pgbouncer(
                    observed, bout_id=scope.bout_id, assert_authority=assert_mutation_authority),
            ),
        }
        return (Round5CreationCoordinator(journal=self._journal, fence=mutation_fence,
                                          adapters=adapters), specs)

    def _observed(self, spec: ResourceSpec) -> ResourceObservation:
        # No tag to read ownership from: the server is the installation's own, sealed.
        return ResourceObservation(resource_kind=spec.resource_kind,
                                   provider_id=self.config.competitor_resource_id,
                                   deterministic_name=spec.deterministic_name,
                                   metadata=spec.metadata)

    async def _switch_on(self, resources: _SetupResources, spec: ResourceSpec, *,
                         scope: CreationScope) -> ResourceObservation:
        """The bell's first timed mutation: send the switch, and return at once.

        The bell capability is checked first, with no await, as AWS checks it before
        CreateDBProxy. The switch's ARM call stamps its own request time, in its worker.
        """

        self._require_bell_capability(scope.bout_id, scope.fencing_token)
        resources.proxy_create_submitted_ns = self._monotonic_ns()
        start = self._pgbouncer.begin_pooled_path(executor=self._ensure_createproxy_executor())
        self._switches.by_bout[scope.bout_id] = start
        resources.proxy_endpoint = self.config.competitor_host
        resources.proxy_arn = self.config.competitor_resource_id
        return self._observed(spec)

    async def _inspect_pgbouncer(self, spec: ResourceSpec,
                                 provider_id: str | None) -> ResourceObservation | None:
        if provider_id is not None and provider_id.lower() != (
                self.config.competitor_resource_id.lower()):
            return None
        enabled = await asyncio.to_thread(self._arm.get_configuration,
                                          self.config.competitor_target_id, "pgbouncer.enabled")
        return self._observed(spec) if str(enabled).lower() == "true" else None

    async def _reset_pgbouncer(self, observed: ResourceObservation, *, bout_id: str,
                               assert_authority: Callable[[], Any]) -> None:
        """The delete: the limit back to the default, then off, then 6432 refusing.

        The journal committed DELETE_INTENT before calling this, so the handoff the engine waits
        on is crossed once the reset starts, as AWS's is once DeleteDBProxy is accepted.
        """

        await assert_authority()
        self._switches.by_bout.pop(bout_id, None)
        self._proxy_delete_accepted.setdefault(bout_id, asyncio.Event()).set()
        async with _RESETS.setdefault(self.config.competitor_target_id, asyncio.Lock()):
            await self._pgbouncer.reset_baseline()

    # The timed setup ---------------------------------------------------------------------------

    async def _setup_competitor(  # type: ignore[override]
        self,
        bout_id: str,
        scope: CreationScope,
        clients: Any,
        coordinator: Round5CreationCoordinator,
        specs: tuple[ResourceSpec, ...],
        resources: _SetupResources,
        gate: asyncio.Event,
        t0_box: list[int],
        on_progress: SetupProgressCallback | None,
        on_lane_ready: SetupLaneReadyCallback | None = None,
        on_lane_stage: SetupLaneStageCallback | None = None,
        launch_barrier: asyncio.Barrier | None = None,
        proxy_intent: JournalEvent | None = None,
    ) -> ConnectionSpikeSetupLaneStop:
        await gate.wait()
        launched_ns = self._monotonic_ns()
        # Both lanes stamp their launch before either runs downstream work (see AWS's
        # `_setup_lakebase`). Nothing is awaited between here and the switch request.
        if launch_barrier is not None:
            await launch_barrier.wait()

        async def report(phase: str, status: str = "running") -> None:
            await self._report(on_progress, "competitor", phase, status, t0_ns=t0_box[0])

        (spec,) = specs
        if proxy_intent is None:
            raise ConnectionSpikeLiveOperationError(
                "Round 5's PgBouncer switch intent was not pre-staged before the bell")
        await coordinator.complete_prestaged(scope, spec, intent=proxy_intent)
        start = self._switches.by_bout.get(bout_id)
        if start is None:
            raise ConnectionSpikeLiveOperationError("Round 5's PgBouncer switch was not sent")
        await report("switching_on_pgbouncer")
        # The endpoint is the server's own, known before the switch lands: the resident prepares
        # its workers while PgBouncer comes up, its release held behind the gate. The two run side
        # by side. AWS's Proxy takes minutes and hides staging behind it; PgBouncer's ~9 s may not,
        # and the competitor's clock must not carry the runner's transport.
        staging = None if on_lane_stage is None else asyncio.create_task(on_lane_stage(
            ConnectionSpikeSetupLaneStop(
                lane_id="competitor", launched_ns=launched_ns, stopped_ns=self._monotonic_ns(),
                credential_sha256=self.config.competitor_credential_sha256,
                endpoint_host=resources.proxy_endpoint, secret_arn=resources.secret_arn)))
        await report("waiting_for_pooled_port")
        try:
            await self._pgbouncer.finish_pooled_path(start)
            resources.proxy_create_requested_ns = (
                start.requested_ns[0] if start.requested_ns else None)
            await self._verify_journaled_resources(scope, coordinator, specs)
        except BaseException as exc:
            if staging is not None:
                staging.cancel()
                await asyncio.gather(staging, return_exceptions=True)
            if isinstance(exc, PgBouncerSetupError):
                raise ConnectionSpikeLiveOperationError(str(exc)) from exc
            raise
        # The setup clock stops at the gate; the release still waits for the resident's PREPARED.
        stopped_ns = self._monotonic_ns()
        if staging is not None:
            await staging
        stop = ConnectionSpikeSetupLaneStop(
            lane_id="competitor", launched_ns=launched_ns, stopped_ns=stopped_ns,
            credential_sha256=self.config.competitor_credential_sha256,
            endpoint_host=resources.proxy_endpoint, secret_arn=resources.secret_arn,
            create_db_proxy_requested_ns=resources.proxy_create_requested_ns)
        # Ready the instant 6432 commits, so its 10,000 start then.
        if on_lane_ready is not None:
            await on_lane_ready(stop)
        await self._report(on_progress, "competitor", "setup_stop", "verified",
                           setup_elapsed_ms=(stopped_ns - t0_box[0]) / 1_000_000)
        return stop

    def _setup_observation(self, stop: ConnectionSpikeSetupLaneStop) -> SetupLaneObservation:
        observation = super()._setup_observation(stop)
        if stop.lane_id != "competitor":
            return observation
        facts = (
            PublicSetupEvidence("pgbouncer_enabled", True),
            PublicSetupEvidence("max_client_conn", int(ROUND5_MAX_CLIENT_CONN)),
            PublicSetupEvidence("pooled_port", POOLED_PORT),
            PublicSetupEvidence("pooled_transaction_committed", True),
        )
        # The stamp AWS takes at CreateDBProxy is the switch's here, and is required the same way.
        return replace(observation, stop_gate_evidence=replace(
            observation.stop_gate_evidence, gate_id=GATE_ID, expected=facts, observed=facts))

    def _cancelled_setup_identifier(self, bout_id: str, specs: Sequence[ResourceSpec],
                                    resources: _SetupResources) -> str:
        return (f"Round 5 bout {bout_id} setup [PgBouncer on Azure server "
                f"{self.config.competitor_target_id}, possibly switched on]")

    # Absence ----------------------------------------------------------------------------------

    def proxy_name_for_bout(self, bout_id: str) -> str:
        """What a leak would be: the server whose PgBouncer may still be on."""

        return f"{self.config.competitor_target_id} (PgBouncer)"

    async def prove_bout_absent(self, bout_id: str) -> None:
        """Prove a journal-free inherited claim left nothing on: no journal debt, PgBouncer off."""

        self._validate(bout_id)
        try:
            unresolved = await self._journal.unresolved_bout_ids()
        except Exception as exc:
            raise ConnectionSpikeCleanupError(
                "Round 5 absence proof could not read cleanup ownership",
                stage="journal_ownership", reason_code="journal_ownership_unavailable",
            ) from exc
        if bout_id in unresolved:
            raise ConnectionSpikeCleanupError(
                "Round 5 inherited claim still has journal debt",
                stage="journal_ownership", reason_code="current_bout_journal_debt")
        try:
            await self._pgbouncer.prove_database_only_start()
        except PgBouncerSetupError as exc:
            raise ConnectionSpikeCleanupError(
                "Round 5's Azure PgBouncer is still on", stage="scoped_orphan_discovery",
                reason_code="pgbouncer_left_on") from exc

    @staticmethod
    def _validate(bout_id: str) -> None:
        from .connection_spike_live import LiveConnectionSpikeAdapter

        LiveConnectionSpikeAdapter._validate_run_id(bout_id)
