from __future__ import annotations

import asyncio
import hashlib
import json
import pickle
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from server.api import router
from server.connection_fanin import FANIN_PROTOCOL, FANIN_SCHEMA_VERSION
from server.connection_spike import (
    PublicSetupEvidence,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupStopGateEvidence,
    arm_setup_phase,
)
from server.connection_spike_journal import (
    CreationScope,
    JournalEvent,
    LifecycleState,
    ResourceObservation,
    ResourceSpec,
    Round5CreationCoordinator,
    build_receipt,
)
from server.connection_spike_live import (
    PROXY_DELETE_ABSENCE_CONFIRMATIONS,
    RUNNER_ASSETS,
    ConnectionSpikeCleanupError,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeSetupLaneStop,
    ConnectionSpikeTarget,
    LiveConnectionSpikeAdapter,
    LiveConnectionSpikeEngine,
    LiveConnectionSpikeSetupOrchestrator,
    LiveRound5WarmProvider,
)
from server.manager import RunManager
from server.models import (
    CompetitorId,
    RoundId,
    SessionState,
)
from server.round5_control import (
    InMemoryRound5ControlStore,
    Round5ControlBinding,
    Round5ControlDispatcher,
    Round5ControlKind,
    Round5ResidentTransport,
    Round5RunnerEvent,
    Round5RunnerEventKind,
)
from server.round5_warm import (
    BlockedWarmError,
    InMemoryRound5WarmStore,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmPreparation,
    Round5WarmState,
)
from tests.test_connection_fanin import raw_lane as raw_fanin_lane
from tests.test_round5_mutation_authority import _config
from tests.test_round5_pre_deploy_acceptance import (
    _arm_via_http,
    _asgi,
    _EngineProvider,
    _make_manager,
    _session_body,
    _SuccessfulPlan,
    _warm_to_ready,
)
from tests.test_round5_warm import DIGEST, Clock, Provider, preparation


def _client_error(code: str) -> Exception:
    error = Exception(code)
    error.response = {"Error": {"Code": code}}  # type: ignore[attr-defined]
    return error


class _SerializedRound5WarmStore(InMemoryRound5WarmStore):
    """Test-only durable store whose replacement reloads serialized state."""

    mode = "serialized-test"

    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path

    async def initialize(self) -> None:
        if not self.path.exists():
            return
        slots, events, control_outbox = pickle.loads(self.path.read_bytes())
        self._slots = slots
        self._events = events
        self.control_outbox = control_outbox

    async def close(self) -> None:
        self._persist()

    def _persist(self) -> None:
        payload = pickle.dumps(
            (self._slots, self._events, self.control_outbox),
            protocol=pickle.HIGHEST_PROTOCOL,
        )
        temporary = self.path.with_suffix(f"{self.path.suffix}.tmp")
        temporary.write_bytes(payload)
        temporary.replace(self.path)

    def _put(self, *args, **kwargs):
        stored = super()._put(*args, **kwargs)
        self._persist()
        return stored


class _AbsenceJournal:
    def __init__(self) -> None:
        self.unresolved: set[str] = set()
        self._events: dict[CreationScope, list[JournalEvent]] = {}

    def seed(
        self,
        scope: CreationScope,
        events: tuple[JournalEvent, ...],
    ) -> None:
        self._events[scope] = list(events)
        self.unresolved.add(scope.bout_id)

    async def unresolved_bout_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.unresolved))

    async def commit(
        self,
        event: JournalEvent,
        *,
        authority_scope: CreationScope | None = None,
    ) -> None:
        del authority_scope
        scope = CreationScope(
            event.bout_id,
            event.fencing_token,
            event.runtime_seal_sha256,
        )
        self._events.setdefault(scope, []).append(event)
        bout_scopes = [
            values
            for ownership, values in self._events.items()
            if ownership.bout_id == event.bout_id
        ]
        complete = bool(bout_scopes)
        for values in bout_scopes:
            latest = {value.ordinal: value for value in values}
            complete = complete and bool(latest) and all(
                value.lifecycle_state is LifecycleState.DELETED
                for value in latest.values()
            )
        if complete:
            self.unresolved.discard(event.bout_id)
        else:
            self.unresolved.add(event.bout_id)

    async def events(self, scope: CreationScope) -> tuple[JournalEvent, ...]:
        return tuple(self._events.get(scope, ()))

    async def scopes(self, bout_id: str) -> tuple[CreationScope, ...]:
        return tuple(
            scope
            for scope in self._events
            if scope.bout_id == bout_id
        )


class _AbsenceRds:
    def __init__(self) -> None:
        self.proxies: dict[str, dict[str, object]] = {}
        self.target_groups: dict[str, list[dict[str, object]]] = {}
        self.named_proxy_not_found = False
        self.describe_instance_calls = 0
        self.describe_proxy_calls: list[str | None] = []
        self.describe_target_group_calls: list[str] = []
        self.delete_proxy_calls: list[str] = []

    def describe_db_instances(self, **kwargs):
        assert kwargs["DBInstanceIdentifier"] == "rds-source"
        self.describe_instance_calls += 1
        return {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "rds-source",
                    "DbiResourceId": "db-RESOURCE",
                    "Endpoint": {"Address": "rds-direct.test"},
                    "DBInstanceStatus": "available",
                    "DBSubnetGroup": {"VpcId": "vpc-sealed"},
                    "VpcSecurityGroups": [
                        {"VpcSecurityGroupId": "sg-rds"}
                    ],
                }
            ]
        }

    def describe_db_proxies(self, **kwargs):
        name = kwargs.get("DBProxyName")
        self.describe_proxy_calls.append(str(name) if name is not None else None)
        if name is None:
            return {"DBProxies": list(self.proxies.values())}
        if self.named_proxy_not_found:
            raise _client_error("DBProxyNotFoundFault")
        proxy = self.proxies.get(str(name))
        if proxy is None:
            raise _client_error("DBProxyNotFoundFault")
        return {"DBProxies": [proxy]}

    def describe_db_proxy_target_groups(self, **kwargs):
        name = str(kwargs["DBProxyName"])
        self.describe_target_group_calls.append(name)
        # AWS scopes this API to the parent Proxy. Once the Proxy is gone the
        # target-group describe returns DBProxyNotFoundFault; a stale test-only
        # dict entry must never manufacture a target-group-only identity that
        # production cannot observe.
        if name not in self.proxies:
            raise _client_error("DBProxyNotFoundFault")
        return {"TargetGroups": list(self.target_groups.get(name, ()))}

    def list_tags_for_resource(self, **kwargs):
        arn = str(kwargs["ResourceName"])
        for proxy in self.proxies.values():
            if proxy["DBProxyArn"] == arn:
                return {"TagList": proxy.get("Tags", [])}
        raise _client_error("DBProxyNotFoundFault")

    def delete_db_proxy(self, **kwargs):
        name = str(kwargs["DBProxyName"])
        self.delete_proxy_calls.append(name)
        self.proxies.pop(name, None)
        self.target_groups.pop(name, None)
        return {}


class _AbsenceSsm:
    def describe_instance_information(self, **kwargs):
        instance_id = str(kwargs["Filters"][0]["Values"][0])
        return {
            "InstanceInformationList": [
                {"InstanceId": instance_id, "PingStatus": "Online"}
            ]
        }


class _AbsenceEc2:
    def __init__(self) -> None:
        self.groups: list[dict[str, object]] = []
        self.rules: list[dict[str, object]] = []

    def describe_security_groups(self, **_kwargs):
        return {"SecurityGroups": list(self.groups)}

    def describe_security_group_rules(self, **_kwargs):
        return {"SecurityGroupRules": list(self.rules)}


def _absence_orchestrator(*, journal=None, rds=None, ec2=None):
    journal = journal if journal is not None else _AbsenceJournal()
    rds = rds if rds is not None else _AbsenceRds()
    ec2 = ec2 if ec2 is not None else _AbsenceEc2()
    config = _config()

    async def pooled_host() -> str:
        return config.lakebase_pooled_host

    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        config,
        journal=journal,
        fence=SimpleNamespace(assert_current=lambda _scope: asyncio.sleep(0)),
        fresh_lakebase_host=pooled_host,
        sleep=lambda _delay: asyncio.sleep(0),
    )
    clients = SimpleNamespace(rds=rds, ec2=ec2, ssm=_AbsenceSsm())

    async def assumed_clients(*_args, **_kwargs):
        return clients

    async def verified_static_fixture(_clients) -> None:
        return None

    orchestrator._assumed_clients = assumed_clients
    orchestrator._verify_proxy_service_role = verified_static_fixture
    orchestrator._verify_static_proxy_network = verified_static_fixture
    return orchestrator, journal, rds, ec2


def _seed_proxy_cleanup_debt(
    orchestrator: LiveConnectionSpikeSetupOrchestrator,
    journal: _AbsenceJournal,
    rds: _AbsenceRds,
    *,
    bout_id: str,
    fencing_token: int,
) -> str:
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    tags = dict(orchestrator.config.ownership_tags)
    tags.update(
        {
            "anti-demo-bout-id": bout_id,
            "anti-demo:bout-token": names.token,
            "anti-demo:bout-fence": str(fencing_token),
        }
    )
    metadata = {
        "tags": tags,
        "baseline_sha256": orchestrator.config.baseline_sha256,
        "competitor_id": orchestrator.config.competitor_id,
        "competitor_target_id": orchestrator.config.competitor_target_id,
        "competitor_resource_id": orchestrator.config.competitor_resource_id,
    }
    scope = CreationScope(
        bout_id,
        fencing_token,
        orchestrator.config.baseline_sha256,
    )
    spec = ResourceSpec(
        ordinal=1,
        resource_kind="rds_proxy",
        deterministic_name=names.proxy_name,
        metadata=metadata,
    )
    now = datetime(2026, 9, 23, tzinfo=UTC)
    intent = JournalEvent.creation_intent(scope, spec, now=now)
    proxy_arn = (
        "arn:aws:rds:us-west-2:123456789012:"
        f"db-proxy:prx-{names.token}"
    )
    created = replace(
        intent,
        provider_id=proxy_arn,
        lifecycle_state=LifecycleState.CREATED,
        completed_at=now,
    )
    journal.seed(scope, (intent, created))
    rds.proxies[names.proxy_name] = {
        "DBProxyName": names.proxy_name,
        "DBProxyArn": proxy_arn,
        "Endpoint": f"{names.proxy_name}.proxy.test",
        "Tags": [
            {"Key": key, "Value": value}
            for key, value in sorted(tags.items())
        ],
    }
    return names.proxy_name


class _ResidentRegistrySim:
    """Deterministic runner responses behind the production resident transport."""

    def __init__(self, store: InMemoryRound5ControlStore) -> None:
        self.store = store
        self.staged: dict[str, Round5ControlBinding] = {}
        self.requests: dict[str, dict[str, object]] = {}
        self.cancel_calls: list[str] = []
        self._cancelled: dict[str, Round5ControlBinding] = {}
        self._sequence: dict[str, int] = {}

    def _next_sequence(self, job_id: str) -> int:
        sequence = self._sequence.get(job_id, 0) + 1
        self._sequence[job_id] = sequence
        return sequence

    async def _append(
        self,
        binding: Round5ControlBinding,
        kind: Round5RunnerEventKind,
        payload: dict[str, object],
    ) -> None:
        if kind == Round5RunnerEventKind.SETTLED and any(
            event.kind == kind
            for event in await self.store.runner_events(
                binding.job_id,
                after_sequence=0,
            )
        ):
            return
        sequence = self._next_sequence(binding.job_id)
        event_id = hashlib.sha256(
            f"{binding.job_id}\0{sequence}\0{kind.value}".encode()
        ).hexdigest()
        await self.store.append_runner_event(
            Round5RunnerEvent(
                event_id=event_id,
                binding=binding,
                sequence=sequence,
                kind=kind,
                occurred_at=datetime.now(UTC),
                payload=payload,
            )
        )

    async def __call__(self, event) -> None:
        if event.kind == Round5ControlKind.PRELOAD:
            process_binding = replace(
                event.binding,
                runner_process_boot_id=f"process-{event.binding.lane_id}",
            )
            await self._append(
                process_binding,
                Round5RunnerEventKind.AGENT_READY,
                {
                    "worker_count": 4,
                    "worker_ready_indexes": [0, 1, 2, 3],
                    "warm_attempt_token": event.binding.warm_attempt_token,
                    "runner_boot_id": event.binding.runner_boot_id,
                    "runner_process_boot_id": (
                        process_binding.runner_process_boot_id
                    ),
                    "process_pid": 4242,
                    "runner_harness_sha256": (
                        event.binding.runner_harness_sha256
                    ),
                },
            )
        elif event.kind == Round5ControlKind.STAGE:
            self.staged[event.job_id] = event.binding
            request = event.payload.get("request")
            assert isinstance(request, dict)
            self.requests[event.job_id] = dict(request)
            await self._append(
                event.binding,
                Round5RunnerEventKind.PREPARED,
                {
                    "state": "prepared",
                    "worker_ready_count": 4,
                    "request_sha256": event.binding.request_sha256,
                },
            )
        elif event.kind == Round5ControlKind.RELEASE:
            request = self.requests[event.job_id]
            lane_id = event.binding.lane_id
            await self._append(
                event.binding,
                Round5RunnerEventKind.PROGRESS,
                {
                    "sequence": self._sequence.get(event.job_id, 0) + 1,
                    "protocol": FANIN_PROTOCOL,
                    "schema_version": FANIN_SCHEMA_VERSION,
                    "lane_id": lane_id,
                    "phase": "holding",
                    "initiated_clients": 10_000,
                    "authenticated_clients": 10_000,
                    "held_clients": 10_000,
                    "peak_held_clients": 10_000,
                    "terminal_failures": 0,
                    "sampled_queries_succeeded": 64,
                    "sampled_queries_failed": 0,
                    "time_to_target_ms": 12_500.0,
                    "elapsed_ms": 12_500.0,
                },
            )
            lane = raw_fanin_lane(
                lane_id,
                config_digest=str(request["config_sha256"]),
            )
            lane["generator_sha256"] = request["generator_sha256"]
            lane["capacity_model_sha256"] = request["capacity_model_sha256"]
            assets = {name: "a" * 64 for name in RUNNER_ASSETS}
            assets["round5_fanin.py"] = str(request["generator_sha256"])
            await self._append(
                event.binding,
                Round5RunnerEventKind.RESULT,
                {
                    "protocol": request["protocol"],
                    "schema_version": request["schema_version"],
                    "contract_sha256": request["contract_sha256"],
                    "config_sha256": request["config_sha256"],
                    "generator_sha256": request["generator_sha256"],
                    "capacity_model_sha256": request["capacity_model_sha256"],
                    "runner_harness_sha256": request["runner_harness_sha256"],
                    "runner_boot_id": event.binding.runner_boot_id,
                    "runner_asset_sha256s": assets,
                    "lanes": [lane],
                    "runtime_diagnostics": {"simulated_resident": True},
                },
            )
            await self._append(
                event.binding,
                Round5RunnerEventKind.SETTLED,
                {"state": "completed"},
            )
        elif event.kind == Round5ControlKind.CANCEL:
            self.cancel_calls.append(event.job_id)
            self._cancelled[event.job_id] = event.binding

    async def allow_settlement(self) -> None:
        for binding in tuple(self._cancelled.values()):
            await self._append(
                binding,
                Round5RunnerEventKind.SETTLED,
                {"state": "cancelled"},
            )


def _resident_registry() -> tuple[
    InMemoryRound5ControlStore,
    _ResidentRegistrySim,
    Round5ResidentTransport,
]:
    store = InMemoryRound5ControlStore()
    resident = _ResidentRegistrySim(store)
    dispatcher = Round5ControlDispatcher(
        store,
        resident,
        sleep=lambda _delay: asyncio.sleep(0),
    )
    transport = Round5ResidentTransport(
        store,
        dispatcher,
        sleep=lambda _delay: asyncio.sleep(0),
        deadline_seconds=0.01,
    )
    return store, resident, transport


def _bridge_in_memory_bell_release(
    coordinator: Round5WarmCoordinator,
    engine: LiveConnectionSpikeEngine,
    transport: Round5ResidentTransport,
) -> None:
    """Give the non-transactional warm-store fake the production release row."""

    accept_bell = coordinator.accept_bell

    async def accept_and_enqueue_release(claim_id: str):
        context = await accept_bell(claim_id)
        release = engine.lakebase_release_event(context.bell_at_utc)
        await transport.store.enqueue(release)
        return context

    coordinator.accept_bell = accept_and_enqueue_release  # type: ignore[method-assign]


def _registry_request() -> dict[str, object]:
    request: dict[str, object] = {
        "protocol": "round5-fanin-v2",
        "action": "run_lane_v3",
    }
    request["prepared_request_digest"] = hashlib.sha256(
        json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return request


async def _stage_claim_registry(
    transport: Round5ResidentTransport,
    claim: object,
    *,
    installation_id: str,
    lanes: tuple[str, ...],
) -> dict[str, Round5ControlBinding]:
    request = _registry_request()
    bindings = {
        lane_id: _resident_binding(
            lane_id,
            str(getattr(claim, f"{lane_id}_job_id")),
            claim=claim,
            installation_id=installation_id,
            request_sha256=str(request["prepared_request_digest"]),
        )
        for lane_id in lanes
    }
    for binding in bindings.values():
        await transport.stage(binding=binding, request=request)
    return bindings


class _RegistryBackedLaneAdapter(LiveConnectionSpikeAdapter):
    """Production adapter cancellation over the production resident registry."""

    def __init__(
        self,
        config: object,
        transport: Round5ResidentTransport,
        *,
        lane_id: str,
    ) -> None:
        super().__init__(config, resident_transport=transport)  # type: ignore[arg-type]
        self._prepared_boot_id = f"boot-{lane_id}"
        self._resident_process_boot_id = f"process-{lane_id}"

    async def preflight_capacity(self, _run_id: str, **_digests):
        self._prepared_clients = SimpleNamespace(
            expires_at=datetime.now(UTC).replace(
                year=datetime.now(UTC).year + 1
            )
        )
        return SimpleNamespace(
            sufficient=True,
            failures=(),
            boot_id=self._prepared_boot_id,
            runner_harness_sha256=self.config.runner_harness_sha256,
            model_sha256="c" * 64,
            receipt={"hard_safety_verified": True},
        )

    async def ensure_dispatch_capsule(
        self,
        _context_id: str,
        *,
        refresh_lead_seconds: float = 0,
    ) -> datetime:
        del refresh_lead_seconds
        expires_at = datetime.now(UTC).replace(year=datetime.now(UTC).year + 1)
        self._prepared_clients = SimpleNamespace(expires_at=expires_at)
        return expires_at

    async def refresh_launch_context(self, context_id: str) -> datetime:
        return await self.ensure_dispatch_capsule(context_id)


class _ArmInstalledEmptyAwsOrchestrator:
    """ARM seam plus production absence and warm-provider discovery."""

    def __init__(
        self,
        absence: LiveConnectionSpikeSetupOrchestrator,
    ) -> None:
        self._absence = absence
        self._cleanup_bouts: set[str] = set()
        self.warm_generations: list[int] = []

    async def warm(
        self,
        generation: int,
        *,
        cleaned_bout_id: str | None = None,
    ) -> object:
        self.warm_generations.append(generation)
        return await self._absence.warm(
            generation,
            cleaned_bout_id=cleaned_bout_id,
        )

    async def prepare(self, bout_id: str, fencing_token: int) -> None:
        assert bout_id and fencing_token > 0

    async def precommit_launch_intent(
        self,
        bout_id: str,
        fencing_token: int,
    ) -> None:
        assert bout_id and fencing_token > 0

    async def setup(
        self,
        bout_id: str,
        fencing_token: int,
        on_progress,
        on_lane_ready,
        on_lane_stage,
        *,
        t0_ns: int | None = None,
    ) -> object:
        del on_progress
        assert bout_id and fencing_token > 0
        origin = t0_ns or 1_000_000_000
        lakebase = ConnectionSpikeSetupLaneStop(
            lane_id="lakebase",
            launched_ns=origin + 1,
            stopped_ns=origin + 2,
            credential_sha256="a" * 64,
            endpoint_host="lakebase-pooled.test",
        )
        competitor = ConnectionSpikeSetupLaneStop(
            lane_id="competitor",
            launched_ns=origin + 1,
            stopped_ns=origin + 3,
            credential_sha256="d" * 64,
            endpoint_host="competitor-proxy.test",
            secret_arn=(
                "arn:aws:secretsmanager:us-west-2:123456789012:"
                "secret:round5-competitor"
            ),
            create_db_proxy_requested_ns=origin + 2,
        )
        await on_lane_ready(lakebase)
        await on_lane_stage(competitor)
        await on_lane_ready(competitor)
        arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=origin)
        fact = PublicSetupEvidence("resident_ready", True)
        return SimpleNamespace(
            bout_id=bout_id,
            arm=arm,
            observations=(
                SetupLaneObservation(
                    lane_id="lakebase",
                    workflow_launched_ns=lakebase.launched_ns,
                    status=SetupLaneStatus.SUCCEEDED,
                    stop_gate_evidence=SetupStopGateEvidence(
                        "lakebase-stop",
                        (fact,),
                        (fact,),
                        lakebase.stopped_ns,
                    ),
                ),
                SetupLaneObservation(
                    lane_id="competitor",
                    workflow_launched_ns=competitor.launched_ns,
                    status=SetupLaneStatus.SUCCEEDED,
                    stop_gate_evidence=SetupStopGateEvidence(
                        "competitor-stop",
                        (fact,),
                        (fact,),
                        competitor.stopped_ns,
                    ),
                    create_db_proxy_requested_ns=(
                        competitor.create_db_proxy_requested_ns
                    ),
                    requires_create_db_proxy_stamp=True,
                ),
            ),
            lakebase=lakebase,
            competitor=competitor,
        )

    async def begin_cleanup(self, bout_id: str) -> None:
        self._cleanup_bouts.add(bout_id)

    def proxy_delete_accepted(self, bout_id: str) -> bool:
        return bout_id in self._cleanup_bouts

    async def wait_for_proxy_delete_accepted(self, bout_id: str) -> None:
        assert self.proxy_delete_accepted(bout_id)

    async def wait_for_cleanup_complete(self, bout_id: str) -> None:
        assert self.proxy_delete_accepted(bout_id)
        await self._absence.prove_bout_absent(bout_id)

    async def prove_bout_absent(self, bout_id: str) -> None:
        await self._absence.prove_bout_absent(bout_id)

    async def unresolved_bout_ids(self) -> tuple[str, ...]:
        return await self._absence.unresolved_bout_ids()

    async def reconcile_failed_cleanup(
        self,
        bout_id: str,
        fencing_token: int,
        *,
        cleanup_authority=None,
    ) -> None:
        await self._absence.reconcile_failed_cleanup(
            bout_id,
            fencing_token,
            cleanup_authority=cleanup_authority,
        )

    def proxy_name_for_bout(self, bout_id: str) -> str:
        return self._absence.proxy_name_for_bout(bout_id)


def _arm_installed_live_engine(
    *,
    installation_id: str,
) -> tuple[
    LiveConnectionSpikeEngine,
    Round5ResidentTransport,
    _ResidentRegistrySim,
    _AbsenceJournal,
    _AbsenceRds,
    _AbsenceEc2,
]:
    absence, journal, rds, ec2 = _absence_orchestrator()
    _control_store, resident, transport = _resident_registry()
    config = _arm_installed_adapter_config(installation_id)
    adapters = {
        lane_id: _RegistryBackedLaneAdapter(
            config,
            transport,
            lane_id=lane_id,
        )
        for lane_id in ("lakebase", "competitor")
    }
    engine = LiveConnectionSpikeEngine(
        adapters["lakebase"],
        lane_adapters=adapters,
        setup_orchestrator=_ArmInstalledEmptyAwsOrchestrator(absence),  # type: ignore[arg-type]
    )
    return engine, transport, resident, journal, rds, ec2


def _arm_installed_adapter_config(installation_id: str) -> SimpleNamespace:
    targets = (
        ConnectionSpikeTarget(
            lane_id="lakebase",
            secret_arn="",
            endpoint_host="lakebase-pooled.test",
            credential_host="lakebase-direct.test",
            credential_sha256="a" * 64,
            observer_credential_sha256="b" * 64,
        ),
        ConnectionSpikeTarget(
            lane_id="competitor",
            secret_arn=(
                "arn:aws:secretsmanager:us-west-2:123456789012:"
                "secret:round5-competitor"
            ),
            endpoint_host="competitor-pooled.test",
            credential_host="competitor-direct.test",
            credential_sha256="d" * 64,
            observer_credential_sha256="e" * 64,
        ),
    )
    config = SimpleNamespace(
        targets=targets,
        runner_instance_type="c7i.2xlarge",
        trust_bundle_sha256="f" * 64,
        runner_harness_sha256="a" * 64,
        resident_installation_id=installation_id,
    )
    return config


@pytest.mark.parametrize(
    "warm_method",
    ("warm", "refresh_warm", "warm_with_physical_runners_from"),
)
async def test_real_engine_rewarm_names_cleaned_proxy_target_group(
    warm_method: str,
) -> None:
    orchestrator, _journal, rds, _ec2 = _absence_orchestrator()
    _control_store, _resident, transport = _resident_registry()
    config = _arm_installed_adapter_config("install-real-engine-rewarm")

    def engine_factory(_competitor: CompetitorId) -> LiveConnectionSpikeEngine:
        adapters = {
            lane_id: _RegistryBackedLaneAdapter(
                config,
                transport,
                lane_id=lane_id,
            )
            for lane_id in ("lakebase", "competitor")
        }
        engine = LiveConnectionSpikeEngine(
            adapters["lakebase"],
            lane_adapters=adapters,
            setup_orchestrator=orchestrator,
        )
        return engine

    cleaned_bout_id = "bout-cleaned-before-rewarm"
    cleaned_proxy_name = orchestrator.proxy_name_for_bout(cleaned_bout_id)
    provider = LiveRound5WarmProvider(SimpleNamespace(), engine_factory)
    assert (
        await provider.reconcile(
            SimpleNamespace(
                state=Round5WarmState.WARMING,
                claim=None,
                cleaned_bout_id=cleaned_bout_id,
            )
        )
        is False
    )
    source = provider._factory_engine(CompetitorId.AURORA_SERVERLESS_V2)

    try:
        if warm_method == "warm":
            await source.warm(2, "attempt-real-engine-rewarm")
        else:
            await source.warm(1, "attempt-real-engine-rewarm")
            rds.describe_target_group_calls.clear()
            if warm_method == "refresh_warm":
                await source.refresh_warm(2)
            else:
                # Deliberately factory-fresh: the RDS engine has no cleanup ID.
                # The shared-runner path must use the source engine's retained ID.
                destination = engine_factory(CompetitorId.RDS_POSTGRES)
                await destination.warm_with_physical_runners_from(source, 2)
    finally:
        await transport.dispatcher.close()

    assert rds.describe_target_group_calls == [cleaned_proxy_name]


@pytest.mark.parametrize(
    "warm_method",
    ("warm", "refresh_warm", "warm_with_physical_runners_from"),
)
async def test_real_engine_post_cleanup_rewarm_requires_cleaned_bout(
    warm_method: str,
) -> None:
    orchestrator, _journal, rds, _ec2 = _absence_orchestrator()
    _control_store, _resident, transport = _resident_registry()
    config = _arm_installed_adapter_config("install-missing-cleaned-bout")

    def engine_factory(_competitor: CompetitorId) -> LiveConnectionSpikeEngine:
        adapters = {
            lane_id: _RegistryBackedLaneAdapter(
                config,
                transport,
                lane_id=lane_id,
            )
            for lane_id in ("lakebase", "competitor")
        }
        return LiveConnectionSpikeEngine(
            adapters["lakebase"],
            lane_adapters=adapters,
            setup_orchestrator=orchestrator,
        )

    source = engine_factory(CompetitorId.AURORA_SERVERLESS_V2)
    try:
        with pytest.raises(
            ConnectionSpikeLiveConfigurationError,
            match="retained cleaned bout",
        ):
            if warm_method == "warm":
                source.require_cleaned_bout()
                await source.warm(2, "attempt-missing-cleaned-bout")
            else:
                await source.warm(1, "attempt-missing-cleaned-bout")
                source.require_cleaned_bout()
                rds.describe_target_group_calls.clear()
                if warm_method == "refresh_warm":
                    await source.refresh_warm(2)
                else:
                    destination = engine_factory(CompetitorId.RDS_POSTGRES)
                    await destination.warm_with_physical_runners_from(source, 2)
    finally:
        await transport.dispatcher.close()

    assert rds.describe_target_group_calls == []


async def test_live_provider_prepare_names_restored_cleaned_proxy_target_group() -> None:
    orchestrator, _journal, rds, _ec2 = _absence_orchestrator()
    _control_store, _resident, transport = _resident_registry()
    config = _arm_installed_adapter_config("install-provider-prepare-rewarm")

    def engine_factory(_competitor: CompetitorId) -> LiveConnectionSpikeEngine:
        adapters = {
            lane_id: _RegistryBackedLaneAdapter(
                config,
                transport,
                lane_id=lane_id,
            )
            for lane_id in ("lakebase", "competitor")
        }
        return LiveConnectionSpikeEngine(
            adapters["lakebase"],
            lane_adapters=adapters,
            setup_orchestrator=orchestrator,
        )

    cleaned_bout_id = "bout-restored-before-provider-prepare"
    cleaned_proxy_name = orchestrator.proxy_name_for_bout(cleaned_bout_id)
    provider = LiveRound5WarmProvider(SimpleNamespace(), engine_factory)
    provider._assemble_preparation = lambda **_kwargs: SimpleNamespace()  # type: ignore[method-assign]
    slot = SimpleNamespace(
        state=Round5WarmState.WARMING,
        claim=None,
        cleaned_bout_id=cleaned_bout_id,
        requires_cleaned_bout=True,
    )

    assert await provider.reconcile(slot) is False
    assert provider._cleaned_bout_id == cleaned_bout_id

    try:
        await provider.prepare(
            generation=2,
            coordinator_fence=1,
            process_epoch="process-provider-prepare-rewarm",
            broker_epoch="broker-provider-prepare-rewarm",
            warm_attempt_token="attempt-provider-prepare-rewarm",
            requires_cleaned_bout=True,
        )
    finally:
        await transport.dispatcher.close()

    assert cleaned_proxy_name in rds.describe_target_group_calls


async def test_live_provider_prepare_requires_post_cleanup_identity() -> None:
    orchestrator, _journal, rds, _ec2 = _absence_orchestrator()
    _control_store, _resident, transport = _resident_registry()
    config = _arm_installed_adapter_config("install-provider-missing-cleaned-bout")

    def engine_factory(_competitor: CompetitorId) -> LiveConnectionSpikeEngine:
        adapters = {
            lane_id: _RegistryBackedLaneAdapter(
                config,
                transport,
                lane_id=lane_id,
            )
            for lane_id in ("lakebase", "competitor")
        }
        return LiveConnectionSpikeEngine(
            adapters["lakebase"],
            lane_adapters=adapters,
            setup_orchestrator=orchestrator,
        )

    provider = LiveRound5WarmProvider(SimpleNamespace(), engine_factory)
    assert (
        await provider.reconcile(
            SimpleNamespace(
                state=Round5WarmState.WARMING,
                claim=None,
                cleaned_bout_id=None,
                requires_cleaned_bout=True,
            )
        )
        is False
    )
    try:
        with pytest.raises(BlockedWarmError) as blocked:
            await provider.prepare(
                generation=2,
                coordinator_fence=1,
                process_epoch="process-provider-missing-cleaned-bout",
                broker_epoch="broker-provider-missing-cleaned-bout",
                warm_attempt_token="attempt-provider-missing-cleaned-bout",
                requires_cleaned_bout=True,
            )
    finally:
        await transport.dispatcher.close()

    assert blocked.value.code == "warm_baseline_invalid"
    assert rds.describe_target_group_calls == []


class _ArmInstalledLiveProvider(Provider):
    """Uses concrete live engines while stubbing only receipt assembly."""

    def __init__(
        self,
        clock: Clock,
        live_engine: LiveConnectionSpikeEngine,
        *,
        reuse_live_engine_for_recovery: bool = True,
    ) -> None:
        super().__init__(clock)
        self.live_engine = live_engine
        self.reuse_live_engine_for_recovery = reuse_live_engine_for_recovery
        self.engines: list[LiveConnectionSpikeEngine] = []
        self._reconciler = LiveRound5WarmProvider(
            SimpleNamespace(),
            self._factory_engine,
        )
        self._reconciler._assemble_preparation = self._assemble_preparation  # type: ignore[method-assign]

    def _factory_engine(
        self,
        _competitor: CompetitorId,
    ) -> LiveConnectionSpikeEngine:
        if (
            self.reuse_live_engine_for_recovery
            and self._reconciler._cleaned_bout_id is None
        ):
            engine = self.live_engine
        else:
            engine = self._new_engine()
        if engine not in self.engines:
            self.engines.append(engine)
        return engine

    def _new_engine(self) -> LiveConnectionSpikeEngine:
        lane_adapters = {
            lane_id: _RegistryBackedLaneAdapter(
                adapter.config,
                adapter._resident_transport,
                lane_id=lane_id,
            )
            for lane_id, adapter in self.live_engine._lane_adapters.items()
        }
        return LiveConnectionSpikeEngine(
            lane_adapters["lakebase"],
            lane_adapters=lane_adapters,
            setup_orchestrator=self.live_engine._setup_orchestrator,
        )

    def _assemble_preparation(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        broker_epoch: str,
        warm_attempt_token: str,
        engines,
        receipts,
    ) -> Round5WarmPreparation:
        del receipts
        prepared = preparation(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            warm_attempt_token=warm_attempt_token,
            broker_epoch=broker_epoch,
        )
        return replace(
            prepared,
            capsule=replace(
                prepared.capsule,
                variant_contexts=dict(engines),
            ),
        )

    async def reconcile(self, slot) -> bool:
        self.reconcile_calls += 1
        return await self._reconciler.reconcile(slot)

    # The manager's adopted claimed engine must reach the SAME provider object that
    # actually reconciles cleanup (the inner ``_reconciler``), exactly as in
    # production where a single ``LiveRound5WarmProvider`` both adopts and
    # reconciles. Without forwarding, the adopted engine would be lost and the
    # provider would reconcile a fresh factory engine -- silently dropping that
    # engine's post-bell teardown (``_cleanup_bout_id``) and re-introducing the
    # dual-owner gap this integration removes.
    def adopt_claimed_engine(self, claim_id, engine) -> None:
        self._reconciler.adopt_claimed_engine(claim_id, engine)

    def transfer_adopted_engine_at_cleaning(self, claim_id) -> None:
        self._reconciler.transfer_adopted_engine_at_cleaning(claim_id)

    def release_adopted_engine(self, claim_id) -> None:
        self._reconciler.release_adopted_engine(claim_id)

    def release_all_adopted_engines(self) -> None:
        self._reconciler.release_all_adopted_engines()

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
        requires_cleaned_bout: bool,
    ):
        self.attempt_tokens.append(warm_attempt_token)
        self.prepare_calls += 1
        self.prepare_started.set()
        await self.release_prepare.wait()
        if self.prepare_error is not None:
            raise self.prepare_error
        return await self._reconciler.prepare(
            generation=generation,
            coordinator_fence=coordinator_fence,
            process_epoch=process_epoch,
            broker_epoch=broker_epoch,
            warm_attempt_token=warm_attempt_token,
            requires_cleaned_bout=requires_cleaned_bout,
        )


async def test_live_provider_contract_adoption_warms_never_bouted_generation_two() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    first_provider = Provider(clock)
    first_provider.release_prepare.set()
    first = Round5WarmCoordinator(
        installation_id="install-contract-adoption",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=first_provider,
        process_epoch="process-contract-original",
        broker_epoch="broker-contract-original",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await first.run_one_cycle()
    original = await store.read("install-contract-adoption")
    assert original is not None and original.state == Round5WarmState.READY
    assert original.generation == 1
    assert original.cleaned_bout_id is None
    assert original.requires_cleaned_bout is False

    orchestrator, _journal, _rds, _ec2 = _absence_orchestrator()
    _control_store, _resident, transport = _resident_registry()
    config = _arm_installed_adapter_config("install-contract-adoption")
    adapters = {
        lane_id: _RegistryBackedLaneAdapter(
            config,
            transport,
            lane_id=lane_id,
        )
        for lane_id in ("lakebase", "competitor")
    }
    engine = LiveConnectionSpikeEngine(
        adapters["lakebase"],
        lane_adapters=adapters,
        setup_orchestrator=orchestrator,
    )
    replacement_provider = _ArmInstalledLiveProvider(
        clock,
        engine,
        reuse_live_engine_for_recovery=False,
    )
    replacement_provider.release_prepare.set()
    clock.advance(91)
    replacement = Round5WarmCoordinator(
        installation_id="install-contract-adoption",
        warm_contract_sha256="f" * 64,
        store=store,
        provider=replacement_provider,
        process_epoch="process-contract-replacement",
        broker_epoch="broker-contract-replacement",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    try:
        await replacement.run_one_cycle()
    finally:
        await transport.dispatcher.close()

    adopted = await store.read("install-contract-adoption")
    assert adopted is not None and adopted.state == Round5WarmState.READY
    assert adopted.generation == 2
    assert adopted.cleaned_bout_id is None
    assert adopted.requires_cleaned_bout is False
    factory_engines = tuple(replacement_provider._reconciler._engines.values())
    assert len(factory_engines) == 2
    assert len({id(factory_engine) for factory_engine in factory_engines}) == 2
    assert all(factory_engine is not engine for factory_engine in factory_engines)


def _lifespan_asgi(
    manager: RunManager,
    coordinator: Round5WarmCoordinator,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app):
        coordinator._last_slot = await coordinator.store.read(
            coordinator.installation_id
        )
        try:
            yield
        finally:
            await manager.close()
            await coordinator.close()

    api = FastAPI(lifespan=lifespan)
    api.include_router(router)
    api.state.run_manager = manager
    api.state.readiness_gate = SimpleNamespace(
        status=SimpleNamespace(ring_ready=True, maintenance_detail=None),
        round5_status=SimpleNamespace(
            ring_ready=True,
            reason_code=None,
            maintenance_state="ready",
            maintenance_detail=None,
        ),
    )
    return api


@pytest.mark.parametrize(
    "leftover",
    ["proxy", "security_group", "security_group_rule"],
)
async def test_real_absence_proof_fails_closed_for_each_provider_identity(
    leftover: str,
) -> None:
    orchestrator, journal, rds, ec2 = _absence_orchestrator()
    bout_id = "bout-inherited"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    ownership = [
        {"Key": key, "Value": value}
        for key, value in orchestrator.config.ownership_tags
    ]
    ownership.append({"Key": "anti-demo-bout-id", "Value": bout_id})

    if leftover == "proxy":
        rds.proxies[names.proxy_name] = {
            "DBProxyName": names.proxy_name,
            "DBProxyArn": (
                "arn:aws:rds:us-west-2:123456789012:db-proxy:prx-inherited"
            ),
            "Tags": ownership,
        }
    elif leftover == "security_group":
        # The exact name production would have created: names are derived from a
        # hash of the bout id (names_for_bout), so a group named after the raw id
        # is not this bout's and would prove nothing.
        ec2.groups.append(
            {
                "GroupId": "sg-inherited",
                "GroupName": names.proxy_security_group_name,
            }
        )
    else:
        ec2.rules.append(
            {
                "SecurityGroupRuleId": "sgr-inherited",
                "Description": names.proxy_name.removesuffix("-proxy") + "-runner-to-proxy",
            }
        )

    with pytest.raises(
        (ConnectionSpikeCleanupError, ConnectionSpikeLiveConfigurationError)
    ):
        await orchestrator.prove_bout_absent(bout_id)

    # Clear the exact provider identity. The same production path now proves
    # absence; no journal debt was hidden to make the result pass.
    rds.proxies.clear()
    rds.target_groups.clear()
    ec2.groups.clear()
    ec2.rules.clear()
    assert await journal.unresolved_bout_ids() == ()
    await orchestrator.prove_bout_absent(bout_id)


async def test_absence_proof_treats_target_group_only_after_proxy_delete_as_absent() -> None:
    orchestrator, journal, rds, _ec2 = _absence_orchestrator()
    bout_id = "bout-target-group-after-delete"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    # This stale dict entry represents the old fake's impossible state. The AWS
    # API cannot enumerate it without its parent Proxy, so it is not production
    # cleanup evidence and must not hold the warm fence.
    rds.target_groups[names.proxy_name] = [
        {
            "TargetGroupName": "default",
            "TargetGroupArn": (
                "arn:aws:rds:us-west-2:123456789012:"
                "target-group:tg-unobservable-after-delete"
            ),
        }
    ]

    await orchestrator.prove_bout_absent(bout_id)

    assert await journal.unresolved_bout_ids() == ()
    assert rds.proxies == {}
    assert rds.describe_target_group_calls == [names.proxy_name] * (
        PROXY_DELETE_ABSENCE_CONFIRMATIONS + 1
    )
    assert rds.target_groups[names.proxy_name]


async def test_final_discovery_fences_target_group_after_three_empty_samples() -> None:
    class _LateTargetGroupRds(_AbsenceRds):
        def __init__(self) -> None:
            super().__init__()
            self.target_group_pages = [
                {"TargetGroups": []},
                {"TargetGroups": []},
                {"TargetGroups": []},
                {
                    "TargetGroups": [
                        {
                            "TargetGroupName": "default",
                            "TargetGroupArn": (
                                "arn:aws:rds:us-west-2:123456789012:"
                                "target-group:tg-late-final-discovery"
                            ),
                        }
                    ]
                },
            ]

        def describe_db_proxy_target_groups(self, **kwargs):
            name = str(kwargs["DBProxyName"])
            self.describe_target_group_calls.append(name)
            if not self.target_group_pages:
                raise AssertionError("unexpected target-group discovery")
            return self.target_group_pages.pop(0)

    rds = _LateTargetGroupRds()
    orchestrator, journal, _, _ec2 = _absence_orchestrator(rds=rds)
    bout_id = "bout-late-target-group"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )

    # Scoped to this bout, so a late target group is a current-bout add-on that
    # fails the absence proof closed; "prior-bout add-ons" is the unscoped scan.
    with pytest.raises(
        ConnectionSpikeCleanupError,
        match="current-bout add-ons",
    ):
        await orchestrator.prove_bout_absent(bout_id)

    assert rds.describe_target_group_calls == [names.proxy_name] * 4
    assert rds.target_group_pages == []
    assert await journal.unresolved_bout_ids() == ()


async def test_absence_proof_requires_delete_confirmation_count() -> None:
    class _ExactlyThreeAbsentProxySamplesRds(_AbsenceRds):
        def __init__(self) -> None:
            super().__init__()
            self.absent_proxy_samples = [
                "DBProxyNotFoundFault",
                "DBProxyNotFoundFault",
                "DBProxyNotFoundFault",
            ]

        def describe_db_proxies(self, **kwargs):
            name = kwargs.get("DBProxyName")
            self.describe_proxy_calls.append(
                str(name) if name is not None else None
            )
            if name is None:
                return {"DBProxies": []}
            if not self.absent_proxy_samples:
                raise AssertionError("absence proof exceeded three samples")
            raise _client_error(self.absent_proxy_samples.pop(0))

    rds = _ExactlyThreeAbsentProxySamplesRds()
    orchestrator, journal, _, _ec2 = _absence_orchestrator(rds=rds)
    bout_id = "bout-confirmed-absent"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )

    await orchestrator.prove_bout_absent(bout_id)

    assert [
        name for name in rds.describe_proxy_calls if name == names.proxy_name
    ] == [names.proxy_name] * 3
    assert rds.absent_proxy_samples == []
    assert rds.delete_proxy_calls == []
    assert await journal.unresolved_bout_ids() == ()


async def test_live_parent_on_third_sample_blocks_after_two_exact_sleeps() -> None:
    class _TwoNotFoundsThenLiveSnapshotRds(_AbsenceRds):
        def __init__(self) -> None:
            super().__init__()
            self.named_describes = 0
            self.live_snapshot: dict[str, object] | None = None

        def describe_db_proxies(self, **kwargs):
            name = kwargs.get("DBProxyName")
            self.describe_proxy_calls.append(
                str(name) if name is not None else None
            )
            if name is None:
                return {"DBProxies": []}
            self.named_describes += 1
            if self.named_describes <= 2:
                raise _client_error("DBProxyNotFoundFault")
            assert self.live_snapshot is not None
            return {"DBProxies": [self.live_snapshot]}

    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    rds = _TwoNotFoundsThenLiveSnapshotRds()
    orchestrator, journal, _, _ec2 = _absence_orchestrator(rds=rds)
    orchestrator._sleep = record_sleep
    bout_id = "bout-third-sample-live"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    rds.live_snapshot = {
        "DBProxyName": names.proxy_name,
        "DBProxyArn": (
            "arn:aws:rds:us-west-2:123456789012:"
            "db-proxy:prx-third-sample-live"
        ),
        "Tags": [],
    }

    with pytest.raises(ConnectionSpikeCleanupError, match="RDS Proxy$"):
        await orchestrator.prove_bout_absent(bout_id)

    assert rds.named_describes == 3
    assert sleeps == [
        orchestrator.config.poll_interval_seconds,
        orchestrator.config.poll_interval_seconds,
    ]
    assert rds.describe_target_group_calls == [names.proxy_name] * 2
    assert await journal.unresolved_bout_ids() == ()


async def test_single_proxy_not_found_does_not_prove_absence() -> None:
    class _OneNotFoundThenLiveProxyRds(_AbsenceRds):
        def __init__(self) -> None:
            super().__init__()
            self.named_describes = 0

        def describe_db_proxies(self, **kwargs):
            name = kwargs.get("DBProxyName")
            self.describe_proxy_calls.append(
                str(name) if name is not None else None
            )
            if name is None:
                return {"DBProxies": list(self.proxies.values())}
            self.named_describes += 1
            if self.named_describes == 1:
                raise _client_error("DBProxyNotFoundFault")
            return {"DBProxies": [self.proxies[str(name)]]}

    rds = _OneNotFoundThenLiveProxyRds()
    orchestrator, journal, _, _ec2 = _absence_orchestrator(rds=rds)
    bout_id = "bout-one-not-found"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    rds.proxies[names.proxy_name] = {
        # The broad discovery scan deliberately ignores this mutation. Only a
        # second exact-name sample can catch the live parent after one NotFound.
        "DBProxyName": "foreign-live-proxy",
        "DBProxyArn": (
            "arn:aws:rds:us-west-2:123456789012:db-proxy:prx-one-not-found"
        ),
        "Tags": [],
    }

    with pytest.raises(ConnectionSpikeCleanupError, match="RDS Proxy$"):
        await orchestrator.prove_bout_absent(bout_id)

    assert rds.named_describes == 2
    assert rds.delete_proxy_calls == []
    assert names.proxy_name in rds.proxies
    assert await journal.unresolved_bout_ids() == ()


async def test_absence_proof_fences_split_brain_target_group_without_proxy_classification() -> None:
    class _SplitBrainTargetGroupRds(_AbsenceRds):
        def describe_db_proxies(self, **kwargs):
            name = kwargs.get("DBProxyName")
            self.describe_proxy_calls.append(
                str(name) if name is not None else None
            )
            if name is not None:
                raise _client_error("DBProxyNotFoundFault")
            return {"DBProxies": list(self.proxies.values())}

    rds = _SplitBrainTargetGroupRds()
    orchestrator, journal, _, _ec2 = _absence_orchestrator(rds=rds)
    bout_id = "bout-split-brain-target-group"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    rds.proxies[names.proxy_name] = {
        # This live parent fails the broad prefix/tag leftover classification.
        # The named target-group result must therefore carry the fence itself.
        "DBProxyName": "foreign-live-proxy",
        "DBProxyArn": (
            "arn:aws:rds:us-west-2:123456789012:"
            "db-proxy:prx-split-brain-live-parent"
        ),
        "Tags": [],
    }
    rds.target_groups[names.proxy_name] = [
        {
            "TargetGroupName": "default",
            "TargetGroupArn": (
                "arn:aws:rds:us-west-2:123456789012:"
                "target-group:tg-split-brain-live-parent"
            ),
        }
    ]

    with pytest.raises(ConnectionSpikeCleanupError, match="target group"):
        await orchestrator.prove_bout_absent(bout_id)

    assert await journal.unresolved_bout_ids() == ()
    assert rds.describe_target_group_calls == [names.proxy_name]
    assert rds.delete_proxy_calls == []
    assert names.proxy_name in rds.proxies
    assert rds.target_groups[names.proxy_name]


async def test_real_absence_proof_refuses_journal_debt_before_provider_reads() -> None:
    orchestrator, journal, rds, ec2 = _absence_orchestrator()
    bout_id = "bout-journal-debt"
    journal.unresolved.add(bout_id)

    with pytest.raises(ConnectionSpikeCleanupError, match="journal debt"):
        await orchestrator.prove_bout_absent(bout_id)
    assert rds.proxies == {}
    assert rds.target_groups == {}
    assert ec2.groups == []
    assert ec2.rules == []

    journal.unresolved.clear()
    await orchestrator.prove_bout_absent(bout_id)


async def test_reconcile_proxy_delete_cascades_target_group_like_aws() -> None:
    orchestrator, journal, rds, _ec2 = _absence_orchestrator()
    bout_id = "bout-reconcile-target-group"
    proxy_name = _seed_proxy_cleanup_debt(
        orchestrator,
        journal,
        rds,
        bout_id=bout_id,
        fencing_token=19,
    )
    rds.target_groups[proxy_name] = [
        {
            "TargetGroupName": "default",
            "TargetGroupArn": (
                "arn:aws:rds:us-west-2:123456789012:"
                "target-group:tg-reconcile-leftover"
            ),
        }
    ]

    await orchestrator.reconcile_failed_cleanup(bout_id, 19)

    assert rds.describe_instance_calls == 1
    assert proxy_name in rds.describe_proxy_calls
    assert rds.delete_proxy_calls == [proxy_name]
    assert rds.proxies == {}
    assert await journal.unresolved_bout_ids() == ()
    assert rds.describe_target_group_calls[-1] == proxy_name
    assert rds.target_groups == {}


async def test_reconcile_unresolved_journal_fences_target_group_with_live_proxy() -> None:
    orchestrator, journal, rds, _ec2 = _absence_orchestrator()
    bout_id = "bout-reconcile-live-target-group"
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    ownership = [
        {"Key": key, "Value": value}
        for key, value in orchestrator.config.ownership_tags
    ]
    ownership.append({"Key": "anti-demo-bout-id", "Value": bout_id})
    rds.proxies[names.proxy_name] = {
        "DBProxyName": names.proxy_name,
        "DBProxyArn": (
            "arn:aws:rds:us-west-2:123456789012:"
            "db-proxy:prx-unresolved-live-parent"
        ),
        "Tags": ownership,
    }
    rds.target_groups[names.proxy_name] = [
        {
            "TargetGroupName": "default",
            "TargetGroupArn": (
                "arn:aws:rds:us-west-2:123456789012:"
                "target-group:tg-unresolved-live-parent"
            ),
        }
    ]
    # The unresolved index survived, but no recoverable scope did. Reconcile
    # must still run deterministic provider discovery and must not report
    # cleanup success while the parent and its target group remain enumerable.
    journal.unresolved.add(bout_id)

    # The live parent carries this bout's id but not its exact fence, so it cannot
    # be proven to be the Proxy this cleanup owns: reconcile refuses before any
    # delete rather than touching what may be a replacement.
    with pytest.raises(
        ConnectionSpikeLiveConfigurationError,
        match="ownership tags changed",
    ):
        await orchestrator.reconcile_failed_cleanup(bout_id, 23)

    assert await journal.unresolved_bout_ids() == (bout_id,)
    assert rds.delete_proxy_calls == []
    assert rds.proxies[names.proxy_name]
    assert rds.target_groups[names.proxy_name]
    # The refusal happens at the exact-tag check, before any discovery, so no
    # target group is enumerated here. The mutation lock on the bout-scoped
    # target-group gather in _discover_orphaned_addons lives in
    # test_final_discovery_fences_target_group_after_three_empty_samples.
    assert rds.describe_target_group_calls == []


@pytest.mark.parametrize(
    "boundary",
    [
        "inspect",
        "delete",
        "confirm_absent",
        "delete_intent_commit",
        "deleted_commit",
    ],
)
async def test_each_cleanup_boundary_fails_once_then_recovers(boundary: str) -> None:
    scope = CreationScope("bout-cleanup-boundary", 9, "c" * 64)
    spec = ResourceSpec(
        ordinal=1,
        resource_kind="rds_proxy",
        deterministic_name="anti-demo-r5-cleanup-boundary-proxy",
        metadata={"tags": {"anti-demo-bout-id": scope.bout_id}},
    )
    now = datetime(2026, 9, 23, tzinfo=UTC)
    intent = JournalEvent.creation_intent(scope, spec, now=now)
    created = replace(
        intent,
        provider_id="arn:aws:rds:us-west-2:123456789012:db-proxy:prx-cleanup",
        lifecycle_state=LifecycleState.CREATED,
        occurred_at=now,
        completed_at=now,
    )

    class _Journal:
        def __init__(self) -> None:
            self.values = [intent, created]
            self.failed = False

        async def commit(self, event, *, authority_scope=None) -> None:
            del authority_scope
            target = {
                "delete_intent_commit": LifecycleState.DELETE_INTENT,
                "deleted_commit": LifecycleState.DELETED,
            }.get(boundary)
            if target is event.lifecycle_state and not self.failed:
                self.failed = True
                raise RuntimeError("fail once at durable cleanup boundary")
            self.values.append(event)

        async def events(self, _scope):
            return tuple(self.values)

        async def scopes(self, _bout_id):
            return (scope,)

    class _Adapter:
        def __init__(self) -> None:
            self.present = True
            self.inspect_failed = False
            self.delete_failed = False
            self.confirm_lied = False

        def observation(self) -> ResourceObservation:
            return ResourceObservation(
                resource_kind=spec.resource_kind,
                provider_id=created.provider_id or "",
                deterministic_name=spec.deterministic_name,
                metadata=spec.metadata,
            )

        async def inspect(self, _spec, *, provider_id):
            assert provider_id == created.provider_id
            if boundary == "inspect" and not self.inspect_failed:
                self.inspect_failed = True
                raise RuntimeError("fail once inspecting")
            if (
                boundary == "confirm_absent"
                and not self.present
                and not self.confirm_lied
            ):
                self.confirm_lied = True
                return self.observation()
            return self.observation() if self.present else None

        async def delete(self, _observed):
            if boundary == "delete" and not self.delete_failed:
                self.delete_failed = True
                raise RuntimeError("fail once deleting")
            self.present = False

    journal = _Journal()
    adapter = _Adapter()
    coordinator = Round5CreationCoordinator(
        journal=journal,
        fence=SimpleNamespace(assert_current=lambda _scope: asyncio.sleep(0)),
        adapters={"rds_proxy": adapter},
        clock=lambda: now,
    )
    receipt = build_receipt(scope, journal.values, issued_at=now)

    if boundary == "deleted_commit":
        with pytest.raises(RuntimeError, match="fail once"):
            await coordinator.cleanup(scope, receipt)
    else:
        first = await coordinator.cleanup(scope, receipt)
        assert first.complete is False

    second = await coordinator.cleanup(scope, receipt)
    assert second.complete is True
    assert adapter.present is False
    assert journal.values[-1].lifecycle_state == LifecycleState.DELETED


@pytest.mark.parametrize(
    ("checkpoint", "expected_stage"),
    [
        ("armed_prebell", "claim-drain"),
        ("running", "rewarming"),
        ("cleaning", "cleaning"),
        ("warming", "rewarming"),
        ("ready_identity_refresh", "identity-refresh"),
    ],
)
async def test_full_process_replacement_fails_closed_then_converges(
    checkpoint: str,
    expected_stage: str,
    tmp_path: Path,
) -> None:
    clock = Clock()
    store_path = tmp_path / f"warm-{checkpoint}.pickle"
    store = _SerializedRound5WarmStore(store_path)
    first_provider = _EngineProvider(clock, _SuccessfulPlan)
    first = Round5WarmCoordinator(
        installation_id="install-process-replacement",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=first_provider,
        process_epoch="process-first",
        broker_epoch="broker-first",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await _warm_to_ready(first, first_provider)
    durable_claim = None
    if checkpoint != "ready_identity_refresh":
        claimed, _ = await first.claim(
            session_id=f"session-{checkpoint}",
            bout_id=f"bout-{checkpoint}",
            selected_variant=Round5Variant.AURORA,
            bout_fence=17,
        )
        assert claimed.claim is not None
        durable_claim = claimed.claim
        if checkpoint != "armed_prebell":
            await first.accept_bell(claimed.claim.claim_id)
        if checkpoint in {"cleaning", "warming"}:
            await first.begin_cleanup(claimed.claim.claim_id)
        if checkpoint == "warming":
            await first.finish_cleanup_and_rewarm(claimed.claim.claim_id)

    first_manager = _make_manager(first, clock)
    first_app = _lifespan_asgi(first_manager, first)
    async with first_app.router.lifespan_context(first_app):
        async with AsyncClient(
            transport=ASGITransport(app=first_app),
            base_url="http://anti-demo.test",
        ) as client:
            before = (await client.get("/api/bout/all")).json()["rounds"][
                RoundId.SURVIVE_CONNECTION_SPIKE.value
            ]
            assert before["can_start"] is (
                checkpoint == "ready_identity_refresh"
            )

    persisted_before_restart = await store.read("install-process-replacement")
    assert persisted_before_restart is not None
    assert store_path.exists()
    clock.advance(181 if checkpoint == "armed_prebell" else 91)
    replacement_store = _SerializedRound5WarmStore(store_path)
    await replacement_store.initialize()
    round_tripped = await replacement_store.read("install-process-replacement")
    assert round_tripped == persisted_before_restart
    assert round_tripped is not persisted_before_restart
    replacement_orchestrator, replacement_journal, replacement_rds, _ = (
        _absence_orchestrator()
    )
    recovery_engines: list[LiveConnectionSpikeEngine] = []
    recovery_checkpoints = {"armed_prebell", "running", "cleaning"}
    postbell_checkpoints = {"running", "cleaning"}
    post_cleanup_restart_checkpoints = {"running", "cleaning", "warming"}
    registry_store, resident_registry, registry_transport = _resident_registry()
    replacement_setup = _ArmInstalledEmptyAwsOrchestrator(
        replacement_orchestrator,
    )
    replacement_engine_config = _arm_installed_adapter_config(
        "install-process-replacement"
    )
    staged_registry_bindings: dict[str, Round5ControlBinding] = {}
    if checkpoint in recovery_checkpoints and durable_claim is not None:
        staged_registry_bindings = await _stage_claim_registry(
            registry_transport,
            durable_claim,
            installation_id="install-process-replacement",
            lanes=(
                ("lakebase", "competitor")
                if checkpoint in postbell_checkpoints
                else ("lakebase",)
            ),
        )
    leftover_target_groups: dict[str, list[dict[str, object]]] = {}
    if checkpoint in postbell_checkpoints and durable_claim is not None:
        names = replacement_orchestrator.names_for_bout(
            replacement_orchestrator.config.deterministic_name_prefix,
            durable_claim.bout_id,
            replacement_orchestrator.config.secret_name_prefix
            or "anti-demo-round5",
        )
        seeded_name = _seed_proxy_cleanup_debt(
            replacement_orchestrator,
            replacement_journal,
            replacement_rds,
            bout_id=durable_claim.bout_id,
            fencing_token=durable_claim.bout_fence,
        )
        assert seeded_name == names.proxy_name
        leftover_target_groups[names.proxy_name] = [
            {
                "TargetGroupName": "default",
                "TargetGroupArn": (
                    "arn:aws:rds:us-west-2:123456789012:"
                    "target-group:tg-process-replacement"
                ),
            }
        ]
        replacement_rds.target_groups.update(leftover_target_groups)

    def recovery_engine_factory(_competitor) -> LiveConnectionSpikeEngine:
        adapters = {
            lane_id: _RegistryBackedLaneAdapter(
                replacement_engine_config,
                registry_transport,
                lane_id=lane_id,
            )
            for lane_id in ("lakebase", "competitor")
        }
        engine = LiveConnectionSpikeEngine(
            adapters["lakebase"],
            lane_adapters=adapters,
            setup_orchestrator=replacement_setup,  # type: ignore[arg-type]
        )
        recovery_engines.append(engine)
        return engine

    replacement_provider = LiveRound5WarmProvider(
        SimpleNamespace(),
        recovery_engine_factory,
    )

    def assemble_live_preparation(
        *,
        generation,
        coordinator_fence,
        broker_epoch,
        warm_attempt_token,
        engines,
        receipts,
    ) -> Round5WarmPreparation:
        del receipts
        prepared = preparation(
            clock,
            generation=generation,
            fence=coordinator_fence,
            warm_attempt_token=warm_attempt_token,
            broker_epoch=broker_epoch,
        )
        return replace(
            prepared,
            capsule=replace(
                prepared.capsule,
                variant_contexts=dict(engines),
            ),
        )

    replacement_provider._assemble_preparation = assemble_live_preparation  # type: ignore[method-assign]
    replacement = Round5WarmCoordinator(
        installation_id="install-process-replacement",
        warm_contract_sha256=DIGEST,
        store=replacement_store,
        provider=replacement_provider,
        process_epoch="process-replacement",
        broker_epoch="broker-replacement",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    replacement_manager = _make_manager(replacement, clock)
    replacement_app = _lifespan_asgi(replacement_manager, replacement)
    async with replacement_app.router.lifespan_context(replacement_app):
        async with AsyncClient(
            transport=ASGITransport(app=replacement_app),
            base_url="http://anti-demo.test",
        ) as client:
            board = (await client.get("/api/bout/all")).json()
            round_five = board["rounds"][RoundId.SURVIVE_CONNECTION_SPIKE.value]
            assert round_five["can_start"] is False
            assert round_five["round5_start"]["stage"] == expected_stage

            if checkpoint in recovery_checkpoints:
                # A replacement process has no process-local binding to consult.
                # The first cycle must retain provider residue while the durable
                # jobs still lack SETTLED.
                if checkpoint in postbell_checkpoints:
                    assert durable_claim is not None
                    assert await replacement_journal.unresolved_bout_ids() == (
                        durable_claim.bout_id,
                    )
                    assert replacement_rds.proxies
                else:
                    assert replacement_journal.unresolved == set()
                    assert replacement_rds.proxies == {}
                assert replacement_rds.target_groups == leftover_target_groups
                retry_delay = await replacement.run_one_cycle()
                cleaning = await replacement_store.read(
                    "install-process-replacement"
                )
                assert cleaning is not None
                assert cleaning.state == Round5WarmState.CLEANING
                assert cleaning.generation == persisted_before_restart.generation
                assert cleaning.last_error_code == "cleanup_reconcile_blocked"
                assert cleaning.next_retry_at is not None
                assert replacement.ring_ready is False
                if checkpoint in postbell_checkpoints:
                    assert replacement_rds.proxies
                    assert replacement_rds.target_groups == leftover_target_groups

                await resident_registry.allow_settlement()
                clock.advance(retry_delay)
                for binding in staged_registry_bindings.values():
                    assert any(
                        event.kind == Round5RunnerEventKind.SETTLED
                        for event in await registry_store.runner_events(
                            binding.job_id,
                            after_sequence=0,
                        )
                    )

                # SETTLED permits AWS reconciliation. For post-bell claims this
                # cycle executes the real journal reconciliation: deleting the
                # parent Proxy also makes its target group unobservable, exactly
                # as AWS does. Pre-bell claims prove the provider scope empty.
                await replacement.run_one_cycle()
                recovering = await replacement_store.read(
                    "install-process-replacement"
                )
                assert recovering is not None
                assert recovering.state == Round5WarmState.WARMING
                assert replacement_rds.proxies == {}
                assert replacement_rds.target_groups == {}
                if checkpoint in postbell_checkpoints:
                    assert durable_claim is not None
                    assert replacement_rds.describe_instance_calls >= 1
                    assert replacement_rds.delete_proxy_calls
                    assert await replacement_journal.unresolved_bout_ids() == ()

            before_live_prepare = await replacement_store.read(
                "install-process-replacement"
            )
            assert before_live_prepare is not None
            reconciled_engine_ids = {id(engine) for engine in recovery_engines}
            target_group_reads_before_prepare = len(
                replacement_rds.describe_target_group_calls
            )
            for _ in range(4):
                if replacement.ring_ready:
                    break
                await replacement.run_one_cycle()
            assert replacement.ring_ready is True
            prepared_engines = tuple(replacement_provider._engines.values())
            assert len(prepared_engines) == 2
            assert len({id(engine) for engine in prepared_engines}) == 2
            assert first_provider.engines
            assert all(
                engine is not first_provider.engines[-1]
                for engine in prepared_engines
            )
            assert not reconciled_engine_ids & {
                id(engine) for engine in prepared_engines
            }
            cleaned_bout_id = before_live_prepare.cleaned_bout_id
            target_group_reads_during_prepare = (
                replacement_rds.describe_target_group_calls[
                    target_group_reads_before_prepare:
                ]
            )
            if checkpoint in post_cleanup_restart_checkpoints:
                assert before_live_prepare.requires_cleaned_bout is True
                assert durable_claim is not None
                assert cleaned_bout_id == durable_claim.bout_id
                cleaned_proxy_name = replacement_orchestrator.proxy_name_for_bout(
                    durable_claim.bout_id
                )
                assert target_group_reads_during_prepare == [
                    cleaned_proxy_name,
                    cleaned_proxy_name,
                ]
            elif before_live_prepare.requires_cleaned_bout:
                assert cleaned_bout_id is not None
                cleaned_proxy_name = replacement_orchestrator.proxy_name_for_bout(
                    cleaned_bout_id
                )
                assert target_group_reads_during_prepare == [
                    cleaned_proxy_name,
                    cleaned_proxy_name,
                ]
            else:
                assert cleaned_bout_id is None
                assert target_group_reads_during_prepare == []
            assert replacement_manager is not first_manager
            assert replacement_app is not first_app
            if checkpoint in {"armed_prebell", "running", "cleaning"}:
                assert durable_claim is not None
                assert recovery_engines
                assert set(resident_registry.cancel_calls) == {
                    binding.job_id for binding in staged_registry_bindings.values()
                }
                assert recovery_engines[-1] is not first_provider.engines[-1]
            ready_board = (await client.get("/api/bout/all")).json()
            assert (
                ready_board["rounds"][RoundId.SURVIVE_CONNECTION_SPIKE.value][
                    "can_start"
                ]
                is True
            )

            # Receipt assembly above is intentionally test-built. The capsule still
            # carries the factory-new live engines warmed by provider.prepare, so
            # run the next bout through those exact variant contexts.
            second = await client.post(
                "/api/sessions",
                json=_session_body("rds_postgres"),
            )
            second_id = second.json()["id"]
            armed = await _arm_via_http(client, second_id)
            assert armed["state"] == SessionState.ARMED.value
            second_engine = replacement_manager._records[
                second_id
            ].connection_spike_engine
            assert second_engine in prepared_engines
            _bridge_in_memory_bell_release(
                replacement,
                second_engine,
                registry_transport,
            )
            started = await client.post(f"/api/sessions/{second_id}/run")
            assert started.status_code == 200
            for _ in range(400):
                terminal = (
                    await client.get(f"/api/sessions/{second_id}")
                ).json()
                if terminal["state"] == SessionState.VERIFIED.value:
                    break
                await asyncio.sleep(0)
            assert terminal["state"] == SessionState.VERIFIED.value
    await registry_transport.dispatcher.close()


async def test_live_proxy_target_group_recovery_rewarms_new_engines_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Reloaded cleanup identity reaches only new factory engines.

    Recovery uses one production engine to settle the durable CLEANING slot.
    Both subsequent generations call ``LiveRound5WarmProvider.prepare`` with
    factory-new engines; neither may reuse the engine that retained cleanup. The
    helper intentionally stubs receipt assembly, so this test does not claim
    production ``_assemble_preparation`` coverage.
    """

    import server.connection_spike_live as live

    monkeypatch.setattr(live, "ROUND5_ABANDONED_ARM_SETTLEMENT_SECONDS", 0.01)
    clock = Clock()
    first_provider = _EngineProvider(clock, _SuccessfulPlan)
    store_path = tmp_path / "restart-warm.pickle"
    first_store = _SerializedRound5WarmStore(store_path)
    first = Round5WarmCoordinator(
        installation_id="install-absence-restart",
        warm_contract_sha256=DIGEST,
        store=first_store,
        provider=first_provider,
        process_epoch="process-before-restart",
        broker_epoch="broker-before-restart",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await _warm_to_ready(first, first_provider)
    old, _capsule = await first.claim(
        session_id="old-prebell-session",
        bout_id="old-prebell-bout",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    assert old.claim is not None

    old_orchestrator, old_journal, old_rds, old_ec2 = _absence_orchestrator()
    old_names = old_orchestrator.names_for_bout(
        old_orchestrator.config.deterministic_name_prefix,
        old.claim.bout_id,
        old_orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    provider_state_path = tmp_path / "provider-state.json"
    provider_state_path.write_text(
        json.dumps({"proxy_present": True}),
        encoding="utf-8",
    )

    class _OldLaneAdapter:
        async def cancel_resident(self, *, binding) -> None:
            del binding
            await asyncio.Event().wait()

        async def cancel_job(self, _job_id: str) -> None:
            raise AssertionError("the stopped process cannot reconcile")

    old_engine = object.__new__(LiveConnectionSpikeEngine)
    old_engine._resident_bindings = {
        "lakebase": _resident_binding("lakebase", old.claim.lakebase_job_id),
        "competitor": _resident_binding(
            "competitor",
            old.claim.competitor_job_id,
        ),
    }
    old_engine._active_run_ids = {
        "lakebase": old.claim.lakebase_job_id,
        "competitor": old.claim.competitor_job_id,
    }
    old_lane_adapters = {
        "lakebase": _OldLaneAdapter(),
        "competitor": _OldLaneAdapter(),
    }
    old_engine._lane_adapters = old_lane_adapters
    old_engine._setup_orchestrator = old_orchestrator
    old_engine._warm_attempt_token = old.claim.warm_attempt_token
    old_engine._bound_claim = old.claim
    deferred = await old_engine.settle_abandoned_arm()
    assert deferred == {
        old.claim.lakebase_job_id,
        old.claim.competitor_job_id,
    }
    await first.begin_cleanup(old.claim.claim_id)

    class _RestartRds(_AbsenceRds):
        def __init__(self, path: Path) -> None:
            super().__init__()
            self.path = path
            state = json.loads(path.read_text(encoding="utf-8"))
            if state.get("proxy_present"):
                self.proxies[old_names.proxy_name] = {
                    "DBProxyName": old_names.proxy_name,
                    "DBProxyArn": (
                        "arn:aws:rds:us-west-2:123456789012:"
                        "db-proxy:prx-restart-live-parent"
                    ),
                    "Tags": [],
                }
                self.target_groups[old_names.proxy_name] = [
                    {
                        "TargetGroupName": "default",
                        "TargetGroupArn": (
                            "arn:aws:rds:us-west-2:123456789012:"
                            "target-group:tg-restart-live-parent"
                        ),
                    }
                ]

        def remove_parent_proxy(self) -> None:
            self.proxies.pop(old_names.proxy_name, None)
            self.path.write_text(
                json.dumps({"proxy_present": False}),
                encoding="utf-8",
            )

    replacement_rds = _RestartRds(provider_state_path)
    replacement_ec2 = _AbsenceEc2()
    replacement_journal = _AbsenceJournal()
    replacement_orchestrator, _, _, _ = _absence_orchestrator(
        journal=replacement_journal,
        rds=replacement_rds,
        ec2=replacement_ec2,
    )
    registry_store, resident_registry, registry_transport = _resident_registry()
    staged_registry_bindings = await _stage_claim_registry(
        registry_transport,
        old.claim,
        installation_id="install-absence-restart",
        lanes=("lakebase", "competitor"),
    )
    replacement_adapters = {
        lane_id: _RegistryBackedLaneAdapter(
            _arm_installed_adapter_config("install-absence-restart"),
            registry_transport,
            lane_id=lane_id,
        )
        for lane_id in ("lakebase", "competitor")
    }
    replacement_setup = _ArmInstalledEmptyAwsOrchestrator(
        replacement_orchestrator,
    )
    replacement_engine = LiveConnectionSpikeEngine(
        replacement_adapters["lakebase"],
        lane_adapters=replacement_adapters,
        setup_orchestrator=replacement_setup,  # type: ignore[arg-type]
    )

    await first.close()
    replacement_store = _SerializedRound5WarmStore(store_path)
    await replacement_store.initialize()
    reloaded = await replacement_store.read("install-absence-restart")
    persisted = await first_store.read("install-absence-restart")
    assert reloaded == persisted
    assert reloaded is not persisted
    clock.advance(91)
    replacement_provider = _ArmInstalledLiveProvider(clock, replacement_engine)
    replacement_provider.release_prepare.set()
    replacement = Round5WarmCoordinator(
        installation_id="install-absence-restart",
        warm_contract_sha256=DIGEST,
        store=replacement_store,
        provider=replacement_provider,
        process_epoch="process-after-restart",
        broker_epoch="broker-after-restart",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    assert replacement_provider.live_engine is replacement_engine
    assert replacement_engine is not old_engine
    assert (
        replacement_engine._setup_orchestrator
        is not old_engine._setup_orchestrator
    )
    assert all(
        replacement_engine._lane_adapters[lane] is not old_lane_adapters[lane]
        for lane in ("lakebase", "competitor")
    )
    try:
        first_retry_delay = await replacement.run_one_cycle()
        fenced = await replacement_store.read("install-absence-restart")
        assert fenced is not None and fenced.state == Round5WarmState.CLEANING
        assert fenced.last_error_code == "cleanup_reconcile_blocked"
        assert fenced.next_retry_at is not None
        assert replacement.ring_ready is False

        await resident_registry.allow_settlement()
        clock.advance(first_retry_delay)
        for binding in staged_registry_bindings.values():
            assert any(
                event.kind == Round5RunnerEventKind.SETTLED
                for event in await registry_store.runner_events(
                    binding.job_id,
                    after_sequence=0,
                )
            )
        second_retry_delay = await replacement.run_one_cycle()
        still_fenced = await replacement_store.read("install-absence-restart")
        assert still_fenced is not None
        assert still_fenced.state == Round5WarmState.CLEANING
        assert still_fenced.last_error_code == "cleanup_reconcile_blocked"
        assert still_fenced.next_retry_at is not None
        assert replacement_rds.proxies[old_names.proxy_name]
        assert replacement_rds.target_groups[old_names.proxy_name]

        replacement_rds.named_proxy_not_found = True
        with pytest.raises(BlockedWarmError) as split_brain:
            await replacement_provider.reconcile(still_fenced)
        assert isinstance(
            split_brain.value.__cause__,
            ConnectionSpikeCleanupError,
        )
        assert "target group" in str(split_brain.value.__cause__)
        assert replacement_rds.describe_target_group_calls == [
            old_names.proxy_name
        ]

        replacement_rds.remove_parent_proxy()
        replacement_rds.named_proxy_not_found = False
        clock.advance(second_retry_delay)
        await replacement.run_one_cycle()
        warming = await replacement_store.read("install-absence-restart")
        assert warming is not None and warming.state == Round5WarmState.WARMING
        assert warming.generation == old.generation + 1
        assert warming.cleaned_bout_id == old.claim.bout_id
        target_group_reads_before_rewarm = len(
            replacement_rds.describe_target_group_calls
        )
        await replacement.run_one_cycle()
        assert replacement.ring_ready is True
        assert replacement_setup.warm_generations == [
            warming.generation,
            warming.generation,
        ]
        assert replacement_rds.describe_target_group_calls[
            target_group_reads_before_rewarm:
        ] == [old_names.proxy_name, old_names.proxy_name]
        first_prepared_engines = tuple(
            replacement_provider._reconciler._engines.values()
        )
        assert len(first_prepared_engines) == 2
        assert len({id(engine) for engine in first_prepared_engines}) == 2
        assert all(engine is not replacement_engine for engine in first_prepared_engines)
        assert replacement_engine._cleanup_bout_id == old.claim.bout_id
        assert all(
            engine._cleanup_bout_id == old.claim.bout_id
            for engine in first_prepared_engines
        )

        manager = _make_manager(replacement, clock)
        app = _asgi(manager)
        try:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://anti-demo.test",
            ) as client:
                created = await client.post(
                    "/api/sessions",
                    json=_session_body("rds_postgres"),
                )
                second_id = created.json()["id"]
                assert second_id != old.claim.bout_id
                await _arm_via_http(client, second_id)
                second_engine = manager._records[
                    second_id
                ].connection_spike_engine
                assert second_engine is not replacement_engine
                assert second_engine is replacement_provider.engines[-1]
                assert isinstance(second_engine, LiveConnectionSpikeEngine)
                # The restart-recovery phase (above) settled exactly the OLD claim's
                # two residents; snapshot that so the assertion after this bout can
                # prove post-bell cleanup ALSO settled this bout's staged residents
                # (lakebase at ARM + competitor at the bell) rather than leaking
                # them, without depending on their dynamically generated job ids.
                cancels_before_second_bout = set(resident_registry.cancel_calls)
                _bridge_in_memory_bell_release(
                    replacement,
                    second_engine,
                    registry_transport,
                )
                started = await client.post(f"/api/sessions/{second_id}/run")
                assert started.status_code == 200
                for _ in range(400):
                    terminal = (await client.get(f"/api/sessions/{second_id}")).json()
                    if terminal["state"] == SessionState.VERIFIED.value:
                        break
                    await asyncio.sleep(0)
                assert terminal["state"] == SessionState.VERIFIED.value

                for _ in range(400):
                    current = await replacement_store.read(
                        "install-absence-restart"
                    )
                    cleanup_lease = await manager._round5_cleanup_store().current()
                    if (
                        current is not None
                        and current.state == Round5WarmState.WARMING
                        and cleanup_lease is None
                    ):
                        break
                    await asyncio.sleep(0)
                assert current is not None and current.state == Round5WarmState.WARMING
                second_proxy_name = replacement_orchestrator.proxy_name_for_bout(
                    second_id
                )
                target_group_reads_before_second_rewarm = len(
                    replacement_rds.describe_target_group_calls
                )
                await replacement.run_one_cycle()
                assert replacement.ring_ready is True
                assert replacement_rds.describe_target_group_calls[
                    target_group_reads_before_second_rewarm:
                ] == [second_proxy_name, second_proxy_name]
                second_prepared_engines = tuple(
                    replacement_provider._reconciler._engines.values()
                )
                assert len(second_prepared_engines) == 2
                assert len({id(engine) for engine in second_prepared_engines}) == 2
                assert not {
                    id(engine) for engine in first_prepared_engines
                } & {
                    id(engine) for engine in second_prepared_engines
                }
                assert all(engine is not second_engine for engine in second_prepared_engines)
                assert second_engine._cleanup_bout_id == second_id
                assert all(
                    engine._cleanup_bout_id == second_id
                    for engine in second_prepared_engines
                )
        finally:
            await manager.close()

        # Restart recovery settled exactly the old claim's two residents.
        assert cancels_before_second_bout == {
            old.claim.lakebase_job_id,
            old.claim.competitor_job_id,
        }
        # Post-bell cleanup of the second bout SETTLED its two staged residents
        # (lakebase + competitor) rather than leaking them -- the hole this fix
        # closes -- and touched no others.
        second_bout_cancels = (
            set(resident_registry.cancel_calls) - cancels_before_second_bout
        )
        assert len(second_bout_cancels) == 2
        assert not (
            second_bout_cancels
            & {old.claim.lakebase_job_id, old.claim.competitor_job_id}
        )
        assert await old_journal.unresolved_bout_ids() == ()
        assert await replacement_journal.unresolved_bout_ids() == ()
        assert old_ec2.groups == []
        assert replacement_rds.proxies == {}
        assert replacement_rds.target_groups[old_names.proxy_name]
        assert replacement_ec2.groups == []
        assert replacement_ec2.rules == []
        assert json.loads(provider_state_path.read_text(encoding="utf-8")) == {
            "proxy_present": False
        }
    finally:
        await registry_transport.dispatcher.close()
        await replacement.close()


def _resident_binding(
    lane_id: str,
    job_id: str,
    *,
    claim: object | None = None,
    installation_id: str = "install-timeout-proof",
    request_sha256: str = "b" * 64,
) -> Round5ControlBinding:
    return Round5ControlBinding(
        installation_id=installation_id,
        lane_id=lane_id,
        generation=int(getattr(claim, "capsule_generation", 7)),
        warm_attempt_token=str(
            getattr(claim, "warm_attempt_token", "attempt-timeout-proof")
        ),
        claim_id=str(getattr(claim, "claim_id", "claim-timeout-proof")),
        bout_id=str(getattr(claim, "bout_id", "bout-timeout-proof")),
        bell_id=str(getattr(claim, "bell_id", "bell-timeout-proof")),
        fence=int(getattr(claim, "bout_fence", 11)),
        job_id=job_id,
        runner_boot_id="runner-boot",
        runner_process_boot_id="runner-process",
        runner_harness_sha256="a" * 64,
        request_sha256=request_sha256,
    )


async def test_refused_bell_deferred_settlement_retains_fence_and_absence_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import server.connection_spike_live as live

    monkeypatch.setattr(live, "ROUND5_ABANDONED_ARM_SETTLEMENT_SECONDS", 0.01)
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "10")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "10")
    clock = Clock()
    engine, transport, resident_registry, journal, rds, ec2 = (
        _arm_installed_live_engine(
            installation_id="install-refused-bell",
        )
    )
    provider = _ArmInstalledLiveProvider(clock, engine)
    coordinator = Round5WarmCoordinator(
        installation_id="install-refused-bell",
        warm_contract_sha256=DIGEST,
        store=InMemoryRound5WarmStore(),
        provider=provider,
        process_epoch="process-refused-bell",
        broker_epoch="broker-refused-bell",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock, round_isolation=True)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://anti-demo.test",
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)

            record = manager._records[session_id]
            slot = await coordinator.store.read("install-refused-bell")
            assert slot is not None and slot.claim is not None
            claim = slot.claim
            generation = slot.generation
            assert record.connection_spike_engine is engine
            assert isinstance(record.connection_spike_engine, LiveConnectionSpikeEngine)
            assert resident_registry.staged == {
                claim.lakebase_job_id: engine._resident_bindings["lakebase"]
            }
            assert (
                await transport.store.binding_for_job(claim.competitor_job_id)
                is None
            )
            assert rds.proxies == {}
            assert rds.target_groups == {}

            async def refused_bell(*_args, **_kwargs):
                raise RuntimeError("bell CAS lost the claim")

            accept_bell_with_leases = coordinator.accept_bell_with_leases
            accept_bell = coordinator.accept_bell
            monkeypatch.setattr(
                coordinator,
                "accept_bell_with_leases",
                refused_bell,
            )
            monkeypatch.setattr(coordinator, "accept_bell", refused_bell)

            response = await client.post(f"/api/sessions/{session_id}/run")
            assert response.status_code == 409
            assert set(engine._resident_bindings) == {"lakebase"}
            cleaning = await coordinator.store.read("install-refused-bell")
            assert cleaning is not None and cleaning.state == Round5WarmState.CLEANING
            assert claim.claim_id not in coordinator._active_claim_ids
            assert record.round5_lease is not None
            assert record.round5_lease.phase == "round5_cleanup"
            assert await manager._lease_store_for_record(record).current() is not None
            assert await manager._round5_cleanup_store().current() == record.round5_lease
            assert record.snapshot.round5_setup is not None
            assert record.snapshot.round5_setup.cleanup_retryable is True
            assert record.connection_spike_cleanup_retry_task is not None

            # The durable retry moves the refused claim under CLEANING, but exact
            # resident settlement is still outstanding.
            assert await manager._retry_connection_spike_cleanup(record, engine) is False
            still_cleaning = await coordinator.store.read("install-refused-bell")
            assert still_cleaning is not None
            assert still_cleaning.state == Round5WarmState.CLEANING
            assert still_cleaning.generation == generation
            assert provider.prepare_calls == 1
            assert coordinator.ring_ready is False
            assert rds.proxies == {}
            assert rds.target_groups == {}
            assert await manager._round5_cleanup_store().current() == record.round5_lease
            assert await journal.unresolved_bout_ids() == ()
            assert ec2.groups == []

            # Production registry semantics distinguish an unstaged job from a
            # staged job whose resident has not published SETTLED.
            await transport.settle_registry_job(claim.competitor_job_id)
            with pytest.raises(TimeoutError):
                await transport.settle_registry_job(claim.lakebase_job_id)

            # Exercise the production warm-provider recovery seam too. Empty AWS
            # is deliberately not enough: exact logical jobs must report SETTLED.
            reconcile_calls_before = provider.reconcile_calls
            with pytest.raises(BlockedWarmError):
                await provider.reconcile(still_cleaning)
            assert provider.reconcile_calls == reconcile_calls_before + 1

            # This no-op clear makes the adversarial boundary explicit. If
            # reconcile_abandoned_claim ever swallows either SETTLED error, empty
            # journal plus empty RDS would otherwise let this retry rewarm.
            rds.target_groups.clear()
            assert (
                await manager._retry_connection_spike_cleanup(record, engine)
                is False
            )
            empty_aws_cleaning = await coordinator.store.read("install-refused-bell")
            assert empty_aws_cleaning is not None
            assert empty_aws_cleaning.state == Round5WarmState.CLEANING

            blocked = await client.post("/api/sessions", json=_session_body())
            for _ in range(5):
                refused = await client.post(
                    f"/api/sessions/{blocked.json()['id']}/arm"
                )
                assert refused.status_code == 409
                assert "STAGE CLEANING" in refused.json()["detail"]

            await resident_registry.allow_settlement()
            assert await provider.reconcile(empty_aws_cleaning) is True
            # +3 not +2: the manager's _retry_connection_spike_cleanup no longer
            # reconciles the engine itself -- it delegates to the coordinator's
            # single-owner converge_cleanup, which drives one provider.reconcile.
            assert provider.reconcile_calls == reconcile_calls_before + 3
            assert (
                await manager._retry_connection_spike_cleanup(record, engine)
                is True
            )
            warming = await coordinator.store.read("install-refused-bell")
            assert warming is not None
            assert warming.state == Round5WarmState.WARMING
            assert warming.generation == generation + 1
            assert set(resident_registry.cancel_calls) == {
                claim.lakebase_job_id
            }

            await coordinator.run_one_cycle()
            assert coordinator.ring_ready is True

            monkeypatch.setattr(
                coordinator,
                "accept_bell_with_leases",
                accept_bell_with_leases,
            )
            monkeypatch.setattr(coordinator, "accept_bell", accept_bell)
            second = await client.post(
                "/api/sessions",
                json=_session_body("rds_postgres"),
            )
            second_id = second.json()["id"]
            await _arm_via_http(client, second_id)
            second_engine = manager._records[second_id].connection_spike_engine
            assert isinstance(second_engine, LiveConnectionSpikeEngine)
            assert second_engine is provider.engines[-1]
            assert second_engine is not engine
            assert all(
                adapter._resident_transport is transport
                for adapter in second_engine._lane_adapters.values()
            )
            _bridge_in_memory_bell_release(
                coordinator,
                second_engine,
                transport,
            )
            second_operator = manager._records[second_id].operator
            assert second_operator is not None
            started = await manager.start_run(second_id, second_operator)
            assert started.state == SessionState.RUNNING
            for _ in range(400):
                terminal = (await client.get(f"/api/sessions/{second_id}")).json()
                if terminal["state"] == SessionState.VERIFIED.value:
                    break
                await asyncio.sleep(0.001)
            assert second_engine._fatal_lane_error is None, repr(
                second_engine._fatal_lane_error
            )
            assert terminal["state"] == SessionState.VERIFIED.value
    finally:
        await manager.close()
        await coordinator.close()
        await transport.dispatcher.close()


async def test_timeout_success_cannot_rewarm_until_durable_jobs_settle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deferred bounded settle on the ARM engine is debt, never success."""

    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "10")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "10")

    clock = Clock()
    engine, transport, resident_registry, journal, rds, ec2 = (
        _arm_installed_live_engine(
            installation_id="install-timeout-proof",
        )
    )
    provider = _ArmInstalledLiveProvider(clock, engine)
    coordinator = Round5WarmCoordinator(
        installation_id="install-timeout-proof",
        warm_contract_sha256=DIGEST,
        store=InMemoryRound5WarmStore(),
        provider=provider,
        process_epoch="process-timeout-proof",
        broker_epoch="broker-timeout-proof",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock, round_isolation=True)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://anti-demo.test",
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            record = manager._records[session_id]
            claimed = await coordinator.store.read("install-timeout-proof")
            assert claimed is not None
            assert claimed.state == Round5WarmState.CLAIMED
            assert claimed.claim is not None
            claim = claimed.claim
            assert record.connection_spike_engine is engine
            assert isinstance(record.connection_spike_engine, LiveConnectionSpikeEngine)
            assert engine._bound_claim == claim
            assert set(engine._resident_bindings) == {"lakebase"}
            assert rds.proxies == {}
            assert rds.target_groups == {}
            assert await journal.unresolved_bout_ids() == ()
            assert ec2.groups == []

            manager._cancel_armed_expiry(record)
            armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            armed_ttl = manager._armed_ttl
            manager._armed_ttl = 0

            # Drive the real armed-expiry path over a real coordinator and store.
            # If a nonempty deferred set is ever treated as success, production
            # abandon_claim returns this exact generation to READY and the
            # CLEANING assertion below fails.
            await manager._expire_abandoned_arm(record, armed_at_monotonic)
            manager._armed_ttl = armed_ttl
            assert record.snapshot.state == SessionState.FAILED
            assert record.snapshot.round5_setup is not None
            assert record.snapshot.round5_setup.cleanup_retryable is True
            cleaning = await coordinator.store.read("install-timeout-proof")
            assert cleaning is not None
            assert cleaning.state == Round5WarmState.CLEANING
            assert cleaning.claim == claim
            assert coordinator.ring_ready is False
            # The resident cancel is now performed by the PROVIDER (single janitor)
            # during convergence -- not synchronously by the manager at arm-expiry --
            # so it appears once the provider reconcile below runs (see assertion after).
            retained = await manager._round5_cleanup_store().current()
            assert retained == record.round5_lease
            assert retained is not None and retained.phase == "round5_cleanup"

            assert (
                await transport.store.binding_for_job(claim.competitor_job_id)
                is None
            )
            await transport.settle_registry_job(claim.competitor_job_id)
            with pytest.raises(TimeoutError):
                await transport.settle_registry_job(claim.lakebase_job_id)

            # Empty AWS and an empty journal are not proof of SETTLED. This
            # invokes LiveConnectionSpikeEngine.reconcile_abandoned_claim on the
            # exact engine installed and driven by ARM. If its
            # gather(return_exceptions=True) results are ever ignored, this call
            # falsely returns True and the test fails at this boundary.
            reconcile_calls_before = provider.reconcile_calls
            with pytest.raises(BlockedWarmError):
                await provider.reconcile(cleaning)
            assert provider.reconcile_calls == reconcile_calls_before + 1
            # The provider (sole janitor) cancelled the ARM-staged resident on the
            # adopted claimed engine before proving durable settlement.
            assert claim.lakebase_job_id in resident_registry.cancel_calls
            assert (
                await manager._retry_connection_spike_cleanup(record, engine)
                is False
            )
            assert (
                await coordinator.store.read("install-timeout-proof")
            ).state == Round5WarmState.CLEANING

            await resident_registry.allow_settlement()
            assert await provider.reconcile(cleaning) is True
            # +3 not +2: the manager's _retry_connection_spike_cleanup no longer
            # reconciles the engine itself -- it delegates to the coordinator's
            # single-owner converge_cleanup, which drives one provider.reconcile.
            assert provider.reconcile_calls == reconcile_calls_before + 3
            assert (
                await manager._retry_connection_spike_cleanup(record, engine)
                is True
            )
            warming = await coordinator.store.read("install-timeout-proof")
            assert warming is not None
            assert warming.state == Round5WarmState.WARMING

            await coordinator.run_one_cycle()
            assert coordinator.ring_ready is True
            assert provider.engines[-1] is not engine

            second = await client.post(
                "/api/sessions",
                json=_session_body("rds_postgres"),
            )
            second_id = second.json()["id"]
            await _arm_via_http(client, second_id)
            second_engine = manager._records[second_id].connection_spike_engine
            assert isinstance(second_engine, LiveConnectionSpikeEngine)
            assert second_engine is provider.engines[-1]
            assert second_engine is not engine
            assert all(
                adapter._resident_transport is transport
                for adapter in second_engine._lane_adapters.values()
            )
            _bridge_in_memory_bell_release(
                coordinator,
                second_engine,
                transport,
            )
            second_operator = manager._records[second_id].operator
            assert second_operator is not None
            started = await manager.start_run(second_id, second_operator)
            assert started.state == SessionState.RUNNING
            for _ in range(400):
                terminal = (await client.get(f"/api/sessions/{second_id}")).json()
                if terminal["state"] == SessionState.VERIFIED.value:
                    break
                await asyncio.sleep(0.001)
            assert terminal["state"] == SessionState.VERIFIED.value
    finally:
        await manager.close()
        await coordinator.close()
        await transport.dispatcher.close()
