"""Round 5 swarm defects #2, #4, #5: CreateDBProxy mutation & cleanup authority.

* **#2 mutation authority** -- the single timed CreateDBProxy does no remote fence
  I/O after the bell, but must still refuse a stale owner: a process-local,
  non-expired bell capability keyed to the exact bout/fence, plus immutable
  operation-identity tags (the bout fence) verified exactly on inspect.
* **#4 ambiguous AlreadyExists** -- adopt ONLY the exact owned Proxy; retry only
  after proving absence; refuse a same-name Proxy that is not our operation.
* **#5 cleanup** -- verify ARN + operation before delete; a same-name/different-ARN
  Proxy is a replacement our stale cleanup must not touch; one NotFound is unknown,
  not clean (bounded repeated absence) before declaring the billable Proxy gone.

These drive the REAL orchestrator against a fake RDS control plane.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server.connection_spike_journal import (
    CreationScope,
    JournalEvent,
    LifecycleState,
    ResourceObservation,
    ResourceSpec,
    Round5CreationCoordinator,
)
from server.connection_spike_live import (
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeSetupConfig,
    ConnectionSpikeWarmSetupContext,
    LiveConnectionSpikeSetupOrchestrator,
    _SetupResources,
)

ACCOUNT = "123456789012"
REGION = "us-west-2"
BOUT = "bout-authority"
FENCE = 11
BASELINE = "d" * 64
_ARN = f"arn:aws:rds:{REGION}:{ACCOUNT}:db-proxy:"
ARN_OLD = f"{_ARN}prx-OLD"
ARN_NEW = f"{_ARN}prx-NEW"
ARN_REPLACEMENT = f"{_ARN}prx-REPLACEMENT"


def _config() -> ConnectionSpikeSetupConfig:
    return ConnectionSpikeSetupConfig(
        region=REGION,
        expected_account_id=ACCOUNT,
        baseline_control_role_arn=f"arn:aws:iam::{ACCOUNT}:role/baseline-control",
        runner_instance_id="i-0123456789abcdef0",
        competitor_runner_instance_id="i-0fedcba9876543210",
        vpc_id="vpc-sealed",
        proxy_subnet_ids=("subnet-a", "subnet-b"),
        lakebase_direct_host="lakebase-direct.test",
        lakebase_pooled_host="lakebase-pooled.test",
        competitor_id="rds_postgres",
        competitor_target_id="rds-source",
        competitor_resource_id="db-RESOURCE",
        competitor_direct_host="rds-direct.test",
        competitor_security_group_id="sg-rds",
        runner_security_group_id="sg-runner",
        proxy_security_group_id="sg-proxy-rds",
        proxy_service_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-service",
        proxy_service_policy_name="proxy-service-secrets",
        aurora_proxy_secret_arn=f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:aurora-proxy",
        rds_proxy_secret_arn=f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:rds-proxy",
        deterministic_name_prefix="anti-demo-r5",
        ownership_tags=(("Owner", "anti-demo"), ("owner", "anti-demo")),
        trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
        trust_bundle_sha256="b" * 64,
        runner_public_key_sha256="c" * 64,
        baseline_sha256=BASELINE,
        lakebase_credential_sha256="e" * 64,
        competitor_credential_sha256="f" * 64,
        poll_interval_seconds=0.5,
    )


def _client_error(code: str) -> Exception:
    exc = Exception(code)
    exc.response = {"Error": {"Code": code}}  # type: ignore[attr-defined]
    return exc


class _FakeRds:
    """A minimal RDS control plane keyed by Proxy name (one Proxy per name)."""

    def __init__(self) -> None:
        self.proxies: dict[str, dict] = {}
        # Last-known snapshot per name, retained across delete so a scripted
        # eventual-consistency "reappearance" can report the same identity.
        self._snapshots: dict[str, dict] = {}
        self.create_calls = 0
        self.delete_calls: list[str] = []
        # Optional scripted overrides for eventual-consistency scenarios.
        self.create_raises_already_exists_first = False
        self.describe_sequence: list[bool] | None = None  # True=present, False=absent
        self._describe_index = 0

    def _arn(self, name: str) -> str:
        return f"arn:aws:rds:{REGION}:{ACCOUNT}:db-proxy:prx-{name[-12:]}"

    def seed(self, name: str, *, tags: dict[str, str], arn: str | None = None) -> str:
        arn = arn or self._arn(name)
        record = {
            "arn": arn,
            "endpoint": f"{name}.proxy.test",
            "tags": dict(tags),
        }
        self.proxies[name] = record
        self._snapshots[name] = dict(record)
        return arn

    def create_db_proxy(self, **kwargs: object) -> dict:
        self.create_calls += 1
        name = str(kwargs["DBProxyName"])
        if self.create_raises_already_exists_first and self.create_calls == 1:
            raise _client_error("DBProxyAlreadyExistsFault")
        if name in self.proxies:
            raise _client_error("DBProxyAlreadyExistsFault")
        tags = {t["Key"]: t["Value"] for t in kwargs.get("Tags", [])}  # type: ignore[union-attr]
        arn = self.seed(name, tags=tags)
        return {"DBProxy": {"DBProxyName": name, "DBProxyArn": arn}}

    def describe_db_proxies(self, **kwargs: object) -> dict:
        name = str(kwargs["DBProxyName"])
        if self.describe_sequence is not None:
            present = self.describe_sequence[
                min(self._describe_index, len(self.describe_sequence) - 1)
            ]
            self._describe_index += 1
            if not present:
                raise _client_error("DBProxyNotFoundFault")
            proxy = self._snapshots.get(name)
            if proxy is None:
                raise _client_error("DBProxyNotFoundFault")
        else:
            proxy = self.proxies.get(name)
            if proxy is None:
                raise _client_error("DBProxyNotFoundFault")
        return {
            "DBProxies": [
                {
                    "DBProxyName": name,
                    "DBProxyArn": proxy["arn"],
                    "Endpoint": proxy["endpoint"],
                }
            ]
        }

    def list_tags_for_resource(self, **kwargs: object) -> dict:
        arn = str(kwargs["ResourceName"])
        for proxy in (*self.proxies.values(), *self._snapshots.values()):
            if proxy["arn"] == arn:
                return {"TagList": [{"Key": k, "Value": v} for k, v in proxy["tags"].items()]}
        raise _client_error("DBProxyNotFoundFault")

    def delete_db_proxy(self, **kwargs: object) -> dict:
        name = str(kwargs["DBProxyName"])
        self.delete_calls.append(name)
        self.proxies.pop(name, None)
        return {}


class _Clock:
    def __init__(self) -> None:
        self.ns = 1_000_000_000

    def __call__(self) -> int:
        self.ns += 1
        return self.ns

    def advance(self, seconds: float) -> None:
        self.ns += int(seconds * 1_000_000_000)


def _orchestrator(rds: _FakeRds, clock: _Clock) -> LiveConnectionSpikeSetupOrchestrator:
    async def _pooled() -> str:
        return "lakebase-pooled.test"

    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        _config(),
        journal=SimpleNamespace(
            commit=lambda *a, **k: _noop(),
            events=lambda *a, **k: _empty(),
            scopes=lambda *a, **k: _empty(),
        ),
        fence=SimpleNamespace(assert_current=lambda *a, **k: _noop()),
        fresh_lakebase_host=_pooled,
        sleep=lambda _delay: _noop(),
        monotonic_ns=clock,
    )
    clients = SimpleNamespace(rds=rds)
    orchestrator._warm_context = ConnectionSpikeWarmSetupContext(
        clients=clients,
        rds_security_group_id="sg-rds",
        proxy_security_group_id="sg-proxy-rds",
        observed_at=datetime.now(UTC),
    )
    return orchestrator


async def _noop() -> None:
    return None


async def _empty():
    return ()


def _scope(fence: int = FENCE) -> CreationScope:
    return CreationScope(BOUT, fence, BASELINE)


def _resources(orchestrator: LiveConnectionSpikeSetupOrchestrator) -> _SetupResources:
    names = orchestrator.names_for_bout("anti-demo-r5", BOUT, "anti-demo-round5")
    return _SetupResources(
        names,
        secret_arn=f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:rds-proxy",
        proxy_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-service",
        proxy_security_group_id="sg-proxy-rds",
        rds_security_group_id="sg-rds",
    )


def _proxy_spec_and_tags(
    orchestrator: LiveConnectionSpikeSetupOrchestrator, scope: CreationScope
) -> tuple[ResourceSpec, dict[str, str]]:
    resources = _resources(orchestrator)
    clients = orchestrator._warm_context.clients  # type: ignore[union-attr]
    _coordinator, specs = orchestrator._coordinator(scope, clients, resources)
    spec = next(s for s in specs if s.resource_kind == "rds_proxy")
    tags = {t["Key"]: t["Value"] for t in orchestrator._tags(spec)}
    return spec, tags


# --------------------------------------------------------------------------- #
# Defect #2: local bell capability + immutable operation-identity (fence) tag
# --------------------------------------------------------------------------- #


def test_fence_is_an_immutable_operation_identity_tag() -> None:
    clock = _Clock()
    orchestrator = _orchestrator(_FakeRds(), clock)
    _spec, tags = _proxy_spec_and_tags(orchestrator, _scope())
    assert tags["anti-demo:bout-fence"] == str(FENCE)
    # A different fence produces a different, exactly-verifiable tag set.
    _spec2, tags2 = _proxy_spec_and_tags(orchestrator, _scope(fence=FENCE + 1))
    assert tags2["anti-demo:bout-fence"] == str(FENCE + 1)
    assert tags != tags2


async def test_create_proxy_requires_a_bell_capability() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, _tags = _proxy_spec_and_tags(orchestrator, scope)

    # No capability minted -> refused before any AWS mutation.
    with pytest.raises(ConnectionSpikeLiveOperationError, match="no local bell capability"):
        await orchestrator._create_proxy(
            orchestrator._warm_context.clients, resources, spec, scope=scope
        )
    assert rds.create_calls == 0


async def test_create_proxy_succeeds_with_a_fresh_capability() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, _tags = _proxy_spec_and_tags(orchestrator, scope)
    orchestrator.arm_bell_capability(BOUT, FENCE)

    observed = await orchestrator._create_proxy(
        orchestrator._warm_context.clients, resources, spec, scope=scope
    )
    assert observed.provider_id.startswith(f"arn:aws:rds:{REGION}:{ACCOUNT}:db-proxy:")
    assert rds.create_calls == 1


async def test_create_proxy_refuses_an_expired_capability() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, _tags = _proxy_spec_and_tags(orchestrator, scope)
    orchestrator.arm_bell_capability(BOUT, FENCE, ttl_seconds=1.0)
    clock.advance(2.0)  # past the capability TTL

    with pytest.raises(ConnectionSpikeLiveOperationError, match="expired"):
        await orchestrator._create_proxy(
            orchestrator._warm_context.clients, resources, spec, scope=scope
        )
    assert rds.create_calls == 0


async def test_create_proxy_refuses_a_capability_for_a_different_fence() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, _tags = _proxy_spec_and_tags(orchestrator, scope)
    orchestrator.arm_bell_capability(BOUT, FENCE + 7)  # wrong fence

    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="fence"):
        await orchestrator._create_proxy(
            orchestrator._warm_context.clients, resources, spec, scope=scope
        )
    assert rds.create_calls == 0


# --------------------------------------------------------------------------- #
# Defect #4: ambiguous AlreadyExists -> adopt exact / retry after absence / refuse
# --------------------------------------------------------------------------- #


async def test_already_exists_adopts_the_exact_owned_proxy() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, tags = _proxy_spec_and_tags(orchestrator, scope)
    # Pre-existing Proxy with OUR exact tags (a retried/ambiguous prior create).
    rds.seed(resources.names.proxy_name, tags=tags)
    orchestrator.arm_bell_capability(BOUT, FENCE)

    observed = await orchestrator._create_proxy(
        orchestrator._warm_context.clients, resources, spec, scope=scope
    )
    # Adopted the existing one; create raised AlreadyExists exactly once, no new Proxy.
    assert rds.create_calls == 1
    assert observed.provider_id == rds.proxies[resources.names.proxy_name]["arn"]


async def test_already_exists_with_foreign_identity_is_refused() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, tags = _proxy_spec_and_tags(orchestrator, scope)
    # A Proxy with our NAME but a foreign fence tag (a different operation).
    foreign = dict(tags)
    foreign["anti-demo:bout-fence"] = str(FENCE + 99)
    rds.seed(resources.names.proxy_name, tags=foreign)
    orchestrator.arm_bell_capability(BOUT, FENCE)

    with pytest.raises(ConnectionSpikeLiveConfigurationError):
        await orchestrator._create_proxy(
            orchestrator._warm_context.clients, resources, spec, scope=scope
        )
    # Never adopted, never deleted the foreign Proxy.
    assert rds.delete_calls == []


async def test_already_exists_but_absent_retries_after_proving_absence() -> None:
    clock = _Clock()
    rds = _FakeRds()
    rds.create_raises_already_exists_first = True  # phantom AlreadyExists
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, _tags = _proxy_spec_and_tags(orchestrator, scope)
    orchestrator.arm_bell_capability(BOUT, FENCE)

    observed = await orchestrator._create_proxy(
        orchestrator._warm_context.clients, resources, spec, scope=scope
    )
    # First create -> phantom AlreadyExists; absence proven; second create succeeds.
    assert rds.create_calls == 2
    assert observed.provider_id == rds.proxies[resources.names.proxy_name]["arn"]


# --------------------------------------------------------------------------- #
# Defect #5: verify ARN + operation, replacement safety, bounded repeated absence
# --------------------------------------------------------------------------- #


async def test_inspect_treats_a_different_arn_as_our_operation_absent() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, tags = _proxy_spec_and_tags(orchestrator, scope)
    # A replacement Proxy holds the name under a DIFFERENT arn (generation N+1).
    replacement_arn = rds.seed(resources.names.proxy_name, tags=tags, arn=ARN_REPLACEMENT)

    # Our operation's arn is the OLD one -> inspect must report absence, not adopt.
    absent = await orchestrator._inspect_proxy(
        orchestrator._warm_context.clients, spec, ARN_OLD
    )
    assert absent is None
    # Inspecting for the replacement's own arn does find it.
    present = await orchestrator._inspect_proxy(
        orchestrator._warm_context.clients, spec, replacement_arn
    )
    assert present is not None and present.provider_id == replacement_arn


async def test_stale_cleanup_does_not_delete_a_replacement() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    scope = _scope()
    resources = _resources(orchestrator)
    spec, tags = _proxy_spec_and_tags(orchestrator, scope)

    # The stale generation's journal says it created a Proxy at OLD_ARN.
    old_arn = ARN_OLD
    # But the live control plane now holds a REPLACEMENT under the same name.
    replacement_arn = rds.seed(resources.names.proxy_name, tags=tags, arn=ARN_NEW)

    committed: list[JournalEvent] = []
    now = datetime(2026, 9, 23, tzinfo=UTC)
    intent = JournalEvent.creation_intent(scope, spec, now=now)
    created = JournalEvent(
        bout_id=BOUT,
        fencing_token=FENCE,
        ordinal=spec.ordinal,
        resource_kind="rds_proxy",
        deterministic_name=spec.deterministic_name,
        client_token=spec.client_token,
        provider_id=old_arn,
        lifecycle_state=LifecycleState.CREATED,
        metadata=spec.metadata,
        runtime_seal_sha256=BASELINE,
        intent_at=now,
        occurred_at=now + timedelta(milliseconds=1),
        completed_at=now + timedelta(milliseconds=1),
    )
    committed.extend([intent, created])

    class Journal:
        async def commit(self, event, *, authority_scope=None) -> None:
            committed.append(event)

        async def events(self, s):
            return list(committed)

        async def scopes(self, bout_id):
            return ()

    coordinator = Round5CreationCoordinator(
        journal=Journal(),
        fence=SimpleNamespace(assert_current=lambda *a, **k: _noop()),
        adapters={
            "rds_proxy": SimpleNamespace(
                create=lambda spec: _noop(),
                inspect=lambda s, *, provider_id: orchestrator._inspect_proxy(
                    orchestrator._warm_context.clients, s, provider_id
                ),
                delete=lambda observed: orchestrator._delete_proxy(
                    orchestrator._warm_context.clients, observed, bout_id=BOUT
                ),
            )
        },
        clock=lambda: datetime.now(UTC),
    )
    report = await coordinator.reconcile_incomplete(scope)

    # Our exact operation (OLD_ARN) is reported absent; the replacement is never
    # deleted and still holds the name.
    assert report.complete
    assert rds.delete_calls == []
    assert resources.names.proxy_name in rds.proxies
    assert rds.proxies[resources.names.proxy_name]["arn"] == replacement_arn


async def test_delete_requires_bounded_consecutive_absence() -> None:
    clock = _Clock()
    rds = _FakeRds()
    orchestrator = _orchestrator(rds, clock)
    resources = _resources(orchestrator)
    scope = _scope()
    spec, tags = _proxy_spec_and_tags(orchestrator, scope)
    arn = rds.seed(resources.names.proxy_name, tags=tags)
    observed = ResourceObservation(
        resource_kind="rds_proxy",
        provider_id=arn,
        deterministic_name=resources.names.proxy_name,
        metadata=spec.metadata,
    )
    # After delete acceptance, the control plane reports: gone, then briefly
    # PRESENT AGAIN (eventual consistency), then gone for good.  One NotFound must
    # not be accepted as clean; a reappearance resets the confirmation count.
    rds.describe_sequence = [False, True, False, False, False]

    await orchestrator._delete_proxy(
        orchestrator._warm_context.clients, observed, bout_id=BOUT
    )
    # It kept polling through the reappearance and only returned after 3 clean
    # confirmations in a row (indices 2,3,4 of the sequence).
    assert rds._describe_index >= 5
    assert rds.delete_calls == [resources.names.proxy_name]
    assert orchestrator.proxy_delete_accepted(BOUT) is True
