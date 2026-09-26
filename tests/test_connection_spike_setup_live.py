from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app import _Round5ArtifactCleanupFence
from runner import round5_fanin as runner_fanin
from server.connection_spike import arm_setup_phase, finalize_setup_phase
from server.connection_spike_journal import CreationScope, JournalEvent, ResourceSpec
from server.connection_spike_live import (
    PROXY_DELETE_ABSENCE_CONFIRMATIONS,
    ConnectionSpikeCleanupError,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeLiveTransientError,
    ConnectionSpikeSetupConfig,
    ConnectionSpikeSetupLaneStop,
    LakebaseCreationJournalStore,
    LiveConnectionSpikeAdapter,
    LiveConnectionSpikeEngine,
    LiveConnectionSpikeSetupOrchestrator,
    _require_warm_runner_online,
    connection_spike_config_sha256,
    connection_spike_live_config_from_manifest,
    connection_spike_setup_config_from_manifest,
)
from server.coordination import round_ring_key
from server.manager import InvalidStateError, RunManager, operator_diagnosis

ACCOUNT = "123456789012"


async def test_cleanup_fence_accepts_exact_active_bout_lease_for_setup() -> None:
    active = SimpleNamespace(
        session_id="bout",
        fencing_token=43,
        phase="checking",
    )

    class Store:
        async def current(self):
            return active

    fence = _Round5ArtifactCleanupFence(Store())

    await fence.assert_current(CreationScope("bout", 43, "b" * 64))


async def test_cleanup_fence_refuses_to_adopt_an_active_artifact_lease() -> None:
    active = SimpleNamespace(
        session_id="another-session",
        fencing_token=43,
        phase="run_committed",
    )
    reclaim_calls = 0

    class Store:
        async def current(self):
            return active

        async def reclaim_expired_cleanup(self, **_kwargs):
            nonlocal reclaim_calls
            reclaim_calls += 1

    fence = _Round5ArtifactCleanupFence(Store())
    scope = CreationScope("bout", 43, "b" * 64)

    with pytest.raises(InvalidStateError, match="still active"):
        await fence.reclaim_expired_cleanup(scope)

    assert reclaim_calls == 0


async def test_cleanup_fence_adopts_recovery_session_cleanup_lease() -> None:
    """A replacement warm process may continue the one recovery-owned lease."""

    active = SimpleNamespace(
        session_id="642cc8be414d47f5a8d51dc24dc29d74",
        fencing_token=44,
        phase="round5_cleanup",
        owner_subject="round5-cleanup-recovery",
    )
    releases: list[object] = []

    class Store:
        async def current(self):
            return active

        async def release(self, lease):
            releases.append(lease)
            return True

        async def renew(self, lease, *, ttl):
            del ttl
            return lease

    fence = _Round5ArtifactCleanupFence(Store())
    recovered = await fence.reclaim_expired_cleanup(
        CreationScope("bout-16258e04b47ab9df", 43, "b" * 64)
    )
    await fence.assert_current(recovered)
    await fence.release_cleanup(recovered)

    assert recovered.fencing_token == 44
    assert fence._cleanup_leases == {}
    assert releases == []


async def test_cleanup_fence_never_adopts_live_manager_cleanup_lease() -> None:
    active = SimpleNamespace(
        session_id="642cc8be414d47f5a8d51dc24dc29d74",
        fencing_token=44,
        phase="round5_cleanup",
        owner_subject="manager@example.com",
    )

    class Store:
        async def current(self):
            return active

    fence = _Round5ArtifactCleanupFence(Store())

    with pytest.raises(InvalidStateError, match="owner is still active"):
        await fence.reclaim_expired_cleanup(CreationScope("bout", 43, "b" * 64))


async def test_cleanup_fence_renews_and_releases_its_reclaimed_lease() -> None:
    reclaimed = SimpleNamespace(
        lease_id="lease-reclaimed",
        session_id="bout",
        fencing_token=44,
    )
    renewals: list[float] = []
    releases: list[object] = []

    class Store:
        async def current(self):
            return None

        async def reclaim_expired_cleanup(self, **kwargs):
            assert kwargs["session_id"] == "bout"
            assert kwargs["expected_previous_token"] == 43
            assert kwargs["ttl"] == timedelta(seconds=90)
            return reclaimed

        async def renew(self, lease, *, ttl):
            assert lease is reclaimed
            renewals.append(ttl.total_seconds())
            return lease

        async def release(self, lease):
            releases.append(lease)
            return True

    fence = _Round5ArtifactCleanupFence(Store())
    recovered = await fence.reclaim_expired_cleanup(
        CreationScope("bout", 43, "b" * 64)
    )
    await fence.assert_current(recovered)
    recovered_again = await fence.reclaim_expired_cleanup(recovered)
    await fence.release_cleanup(recovered_again)

    assert recovered.fencing_token == 44
    assert renewals == [90.0, 90.0]
    assert releases == [reclaimed]


async def test_missing_process_cleanup_graph_never_claims_delete_acceptance() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._coordinators = {}
    orchestrator._scopes = {}
    orchestrator._proxy_delete_accepted = {}

    with pytest.raises(
        ConnectionSpikeCleanupError,
        match="durable resource reconstruction",
    ):
        await orchestrator._cleanup_exactly("bout-missing-graph")

    assert orchestrator.proxy_delete_accepted("bout-missing-graph") is False


async def test_reconstructed_cleanup_accepts_delete_already_in_flight() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(poll_interval_seconds=0)
    orchestrator._proxy_delete_accepted = {}
    orchestrator._sleep = lambda _seconds: asyncio.sleep(0)

    class InvalidState(Exception):
        response = {"Error": {"Code": "InvalidDBProxyStateFault"}}

    class Rds:
        async def delete_db_proxy(self, **_kwargs):
            raise InvalidState()

    async def call(operation, **kwargs):
        return await operation(**kwargs)

    orchestrator._call = call
    orchestrator._inspect_proxy = lambda *_args, **_kwargs: asyncio.sleep(
        0, result=None
    )
    observed = SimpleNamespace(
        deterministic_name="owned-proxy",
        metadata={},
        provider_id="arn:aws:rds:us-west-2:123456789012:db-proxy:owned",
    )

    await orchestrator._delete_proxy(
        SimpleNamespace(rds=Rds()),
        observed,
        bout_id="bout-delete-in-flight",
    )

    assert orchestrator.proxy_delete_accepted("bout-delete-in-flight") is True


async def test_invalid_state_without_deleting_does_not_claim_delete_acceptance() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(poll_interval_seconds=0)
    orchestrator._proxy_delete_accepted = {}

    class InvalidState(Exception):
        response = {"Error": {"Code": "InvalidDBProxyStateFault"}}

    arn = "arn:aws:rds:us-west-2:123456789012:db-proxy:owned"

    class Rds:
        async def delete_db_proxy(self, **_kwargs):
            raise InvalidState()

        async def describe_db_proxies(self, **_kwargs):
            return {"DBProxies": [{"DBProxyArn": arn, "Status": "modifying"}]}

    async def call(operation, **kwargs):
        return await operation(**kwargs)

    observed = SimpleNamespace(
        deterministic_name="owned-proxy",
        metadata={},
        provider_id=arn,
    )
    orchestrator._call = call
    orchestrator._inspect_proxy = lambda *_args, **_kwargs: asyncio.sleep(
        0, result=observed
    )

    with pytest.raises(InvalidState):
        await orchestrator._delete_proxy(
            SimpleNamespace(rds=Rds()),
            observed,
            bout_id="bout-not-deleting",
        )

    assert orchestrator.proxy_delete_accepted("bout-not-deleting") is False


async def test_visible_deleting_proxy_accepts_duplicate_delete_handoff() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(poll_interval_seconds=0)
    orchestrator._proxy_delete_accepted = {}
    orchestrator._sleep = lambda _seconds: asyncio.sleep(0)

    class InvalidState(Exception):
        response = {"Error": {"Code": "InvalidDBProxyStateFault"}}

    arn = "arn:aws:rds:us-west-2:123456789012:db-proxy:owned"

    class Rds:
        async def delete_db_proxy(self, **_kwargs):
            raise InvalidState()

        async def describe_db_proxies(self, **_kwargs):
            return {"DBProxies": [{"DBProxyArn": arn, "Status": "deleting"}]}

    async def call(operation, **kwargs):
        return await operation(**kwargs)

    observed = SimpleNamespace(
        deterministic_name="owned-proxy",
        metadata={},
        provider_id=arn,
    )
    inspections = 0

    async def inspect(*_args, **_kwargs):
        nonlocal inspections
        inspections += 1
        return observed if inspections == 1 else None

    orchestrator._call = call
    orchestrator._inspect_proxy = inspect

    await orchestrator._delete_proxy(
        SimpleNamespace(rds=Rds()),
        observed,
        bout_id="bout-deleting",
    )

    assert orchestrator.proxy_delete_accepted("bout-deleting") is True


async def test_proxy_delete_poll_renews_cleanup_authority() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(poll_interval_seconds=0)
    orchestrator._proxy_delete_accepted = {}
    orchestrator._sleep = lambda _seconds: asyncio.sleep(0)
    arn = "arn:aws:rds:us-west-2:123456789012:db-proxy:owned"

    class Rds:
        async def delete_db_proxy(self, **_kwargs):
            return {}

    async def call(operation, **kwargs):
        return await operation(**kwargs)

    observed = SimpleNamespace(
        deterministic_name="owned-proxy",
        metadata={},
        provider_id=arn,
    )
    inspections = 0
    authority_calls = 0

    async def inspect(*_args, **_kwargs):
        nonlocal inspections
        inspections += 1
        return observed if inspections <= 2 else None

    async def authority() -> None:
        nonlocal authority_calls
        authority_calls += 1

    orchestrator._call = call
    orchestrator._inspect_proxy = inspect

    await orchestrator._delete_proxy(
        SimpleNamespace(rds=Rds()),
        observed,
        bout_id="bout-poll-renewal",
        assert_authority=authority,
    )

    assert authority_calls == 1 + PROXY_DELETE_ABSENCE_CONFIRMATIONS
    assert orchestrator.proxy_delete_accepted("bout-poll-renewal") is True


async def test_stale_cleanup_never_mutates_replacement_proxy_children() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    reset: list[str] = []
    resources = SimpleNamespace(
        proxy_arn="arn:aws:rds:us-west-2:123456789012:db-proxy:old",
        names=SimpleNamespace(proxy_name="deterministic-proxy"),
    )
    child = SimpleNamespace(metadata={"tags": {"anti-demo:bout-fence": "43"}})
    orchestrator._inspect_proxy = lambda *_args, **_kwargs: asyncio.sleep(
        0, result=None
    )
    orchestrator._reset_target_group = lambda *_args: asyncio.sleep(
        0, result=reset.append("reset")
    )
    orchestrator._deregister_proxy_target = lambda *_args: asyncio.sleep(
        0, result=reset.append("deregister")
    )
    async def assert_authority() -> None:
        return None

    await orchestrator._reset_target_group_if_parent_matches(
        object(), resources, child, assert_authority
    )
    await orchestrator._deregister_proxy_target_if_parent_matches(
        object(), resources, child, assert_authority
    )

    assert reset == []


async def test_matching_parent_cleanup_mutates_owned_children() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    mutations: list[str] = []
    resources = SimpleNamespace(
        proxy_arn="arn:aws:rds:us-west-2:123456789012:db-proxy:owned",
        names=SimpleNamespace(proxy_name="deterministic-proxy"),
    )
    child = SimpleNamespace(metadata={"tags": {"anti-demo:bout-fence": "43"}})
    orchestrator._inspect_proxy = lambda *_args, **_kwargs: asyncio.sleep(
        0, result=SimpleNamespace()
    )
    orchestrator._reset_target_group = lambda *_args: asyncio.sleep(
        0, result=mutations.append("reset")
    )
    orchestrator._deregister_proxy_target = lambda *_args: asyncio.sleep(
        0, result=mutations.append("deregister")
    )
    async def assert_authority() -> None:
        return None

    await orchestrator._reset_target_group_if_parent_matches(
        object(), resources, child, assert_authority
    )
    await orchestrator._deregister_proxy_target_if_parent_matches(
        object(), resources, child, assert_authority
    )

    assert mutations == ["reset", "deregister"]


async def test_empty_parent_arn_never_authorizes_child_mutation() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    resources = SimpleNamespace(
        proxy_arn="",
        names=SimpleNamespace(proxy_name="deterministic-proxy"),
    )
    child = SimpleNamespace(metadata={"tags": {"anti-demo:bout-fence": "43"}})
    inspected = False

    async def inspect(*_args, **_kwargs):
        nonlocal inspected
        inspected = True
        return SimpleNamespace()

    orchestrator._inspect_proxy = inspect

    assert (
        await orchestrator._cleanup_parent_matches(object(), resources, child)
        is False
    )
    assert inspected is False


async def test_cleanup_rechecks_parent_after_authority_before_name_mutation() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    mutations: list[str] = []
    parent_present = True
    resources = SimpleNamespace(
        proxy_arn="arn:aws:rds:us-west-2:123456789012:db-proxy:old",
        names=SimpleNamespace(proxy_name="deterministic-proxy"),
    )
    child = SimpleNamespace(metadata={"tags": {"anti-demo:bout-fence": "43"}})

    async def inspect(*_args, **_kwargs):
        return SimpleNamespace() if parent_present else None

    async def authority() -> None:
        nonlocal parent_present
        parent_present = False

    orchestrator._inspect_proxy = inspect
    orchestrator._reset_target_group = lambda *_args: asyncio.sleep(
        0, result=mutations.append("reset")
    )

    await orchestrator._reset_target_group_if_parent_matches(
        object(),
        resources,
        child,
        authority,
    )

    assert mutations == []


async def test_deregister_rechecks_parent_after_authority() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    mutations: list[str] = []
    parent_present = True
    resources = SimpleNamespace(
        proxy_arn="arn:aws:rds:us-west-2:123456789012:db-proxy:old",
        names=SimpleNamespace(proxy_name="deterministic-proxy"),
    )
    child = SimpleNamespace(metadata={"tags": {"anti-demo:bout-fence": "43"}})

    async def inspect(*_args, **_kwargs):
        return SimpleNamespace() if parent_present else None

    async def authority() -> None:
        nonlocal parent_present
        parent_present = False

    orchestrator._inspect_proxy = inspect
    orchestrator._deregister_proxy_target = lambda *_args: asyncio.sleep(
        0, result=mutations.append("deregister")
    )

    await orchestrator._deregister_proxy_target_if_parent_matches(
        object(),
        resources,
        child,
        authority,
    )

    assert mutations == []


async def test_delete_rechecks_parent_after_authority() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._proxy_delete_accepted = {}
    parent_present = True
    delete_calls = 0
    observed = SimpleNamespace(
        deterministic_name="deterministic-proxy",
        metadata={"tags": {"anti-demo:bout-fence": "43"}},
        provider_id="arn:aws:rds:us-west-2:123456789012:db-proxy:old",
    )

    async def inspect(*_args, **_kwargs):
        return observed if parent_present else None

    async def authority() -> None:
        nonlocal parent_present
        parent_present = False

    class Rds:
        async def delete_db_proxy(self, **_kwargs):
            nonlocal delete_calls
            delete_calls += 1

    orchestrator._inspect_proxy = inspect

    await orchestrator._delete_proxy(
        SimpleNamespace(rds=Rds()),
        observed,
        bout_id="bout-stale-delete",
        assert_authority=authority,
    )

    assert delete_calls == 0
    assert orchestrator.proxy_delete_accepted("bout-stale-delete") is False


@pytest.mark.parametrize("proxy_present", [True, False])
async def test_restart_with_empty_journal_reconstructs_and_deletes_owned_proxy(
    proxy_present: bool,
) -> None:
    """Journal absence after CreateDBProxy is unknown, not provider absence."""

    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._lock = asyncio.Lock()
    orchestrator.config = SimpleNamespace(
        baseline_sha256="b" * 64,
        deterministic_name_prefix="anti-demo-r5",
        secret_name_prefix="",
        proxy_secret_arn="arn:secret",
        proxy_service_role_arn="arn:role",
        competitor_security_group_id="sg-database",
    )
    orchestrator._settle_commands = lambda _bout: asyncio.sleep(0)
    orchestrator._assumed_clients = lambda _bout: asyncio.sleep(0, result=object())
    orchestrator._baseline_rds_security_group = lambda _clients: asyncio.sleep(
        0, result="sg-database"
    )
    orchestrator._journal = SimpleNamespace(
        scopes=lambda _bout: asyncio.sleep(0, result=())
    )
    orchestrator._discover_orphaned_addons = (
        lambda *_args, **_kwargs: asyncio.sleep(0)
    )

    deleted: list[str] = []
    inspected: list[str] = []
    present = {"rds_proxy": proxy_present}

    class Adapter:
        def __init__(self, kind: str) -> None:
            self.kind = kind

        async def inspect(self, spec, provider_id=None):
            del provider_id
            inspected.append(spec.resource_kind)
            return (
                SimpleNamespace(resource_kind=self.kind)
                if present.get(self.kind)
                else None
            )

        async def delete(self, observed):
            deleted.append(observed.resource_kind)
            present[observed.resource_kind] = False

    specs = (
        ResourceSpec(1, "rds_proxy", "owned-proxy"),
        ResourceSpec(2, "proxy_target_group", "owned-target-group"),
        ResourceSpec(3, "proxy_target", "owned-target"),
    )
    coordinator = SimpleNamespace(
        _adapters={spec.resource_kind: Adapter(spec.resource_kind) for spec in specs}
    )
    coordinator_tokens: list[int] = []

    def build_coordinator(scope, *_args, **_kwargs):
        coordinator_tokens.append(scope.fencing_token)
        return coordinator, specs

    orchestrator._coordinator = build_coordinator
    artifact_fence_calls = 0
    released_tokens: list[int] = []

    class ReclaimedBoutFence:
        async def reclaim_expired_cleanup(self, scope):
            raise AssertionError(
                f"coordinator-owned cleanup must not reclaim the ring lease: {scope}"
            )

        async def assert_current(self, scope):
            nonlocal artifact_fence_calls
            artifact_fence_calls += 1
            assert scope.fencing_token == 43

        async def release_cleanup(self, scope):
            released_tokens.append(scope.fencing_token)

    orchestrator._fence = ReclaimedBoutFence()
    cleanup_authority_calls = 0

    async def cleanup_authority() -> None:
        nonlocal cleanup_authority_calls
        cleanup_authority_calls += 1

    await orchestrator.reconcile_failed_cleanup(
        "bout-restart",
        43,
        cleanup_authority=cleanup_authority,
    )

    assert inspected == ["rds_proxy", "rds_proxy"]
    assert deleted == (["rds_proxy"] if proxy_present else [])
    assert cleanup_authority_calls >= (3 if proxy_present else 2)
    assert artifact_fence_calls == 1
    assert coordinator_tokens == [43]
    assert released_tokens == []


async def test_coordinator_cleanup_ignores_live_manager_ring_lease() -> None:
    """Manager-owned round5_cleanup must not block coordinator provider delete."""

    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._lock = asyncio.Lock()
    orchestrator.config = SimpleNamespace(
        baseline_sha256="b" * 64,
        deterministic_name_prefix="anti-demo-r5",
        secret_name_prefix="",
        proxy_secret_arn="arn:secret",
        proxy_service_role_arn="arn:role",
        competitor_security_group_id="sg-database",
    )
    orchestrator._settle_commands = lambda _bout: asyncio.sleep(0)
    orchestrator._assumed_clients = lambda _bout: asyncio.sleep(0, result=object())
    orchestrator._baseline_rds_security_group = lambda _clients: asyncio.sleep(
        0, result="sg-database"
    )
    orchestrator._journal = SimpleNamespace(
        scopes=lambda _bout: asyncio.sleep(0, result=())
    )
    orchestrator._discover_orphaned_addons = (
        lambda *_args, **_kwargs: asyncio.sleep(0)
    )
    deleted: list[str] = []

    class Adapter:
        def __init__(self, kind: str) -> None:
            self.kind = kind

        async def inspect(self, spec, provider_id=None):
            del spec, provider_id
            return (
                SimpleNamespace(resource_kind=self.kind)
                if self.kind == "rds_proxy" and not deleted
                else None
            )

        async def delete(self, observed):
            deleted.append(observed.resource_kind)

    specs = (
        ResourceSpec(1, "rds_proxy", "owned-proxy"),
        ResourceSpec(2, "proxy_target_group", "owned-target-group"),
        ResourceSpec(3, "proxy_target", "owned-target"),
    )
    coordinator = SimpleNamespace(
        _adapters={spec.resource_kind: Adapter(spec.resource_kind) for spec in specs}
    )
    orchestrator._coordinator = lambda *_args, **_kwargs: (coordinator, specs)

    class ManagerOwnedFence:
        async def reclaim_expired_cleanup(self, scope):
            del scope
            raise InvalidStateError("Round 5 prior cleanup owner is still active")

        async def assert_current(self, scope):
            del scope
            raise InvalidStateError("Round 5 ring fence is no longer current")

    orchestrator._fence = ManagerOwnedFence()

    await orchestrator.reconcile_failed_cleanup(
        "bout-manager-lease",
        43,
        cleanup_authority=lambda: asyncio.sleep(0),
    )

    assert deleted == ["rds_proxy"]


async def test_coordinator_restart_reclaims_expired_ring_to_seal_leftover_journal() -> None:
    """Restart-during-cleanup must reclaim the departed bout ring.

    Coordinator CAS is live, so the gen54 skip-reclaim path would otherwise
    skip the artifact fence. Journal DELETED commits JOIN that ring, and the
    write becomes a permanent cleanup_reconcile_blocked after AWS is gone.
    """

    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._lock = asyncio.Lock()
    orchestrator.config = SimpleNamespace(
        baseline_sha256="b" * 64,
        deterministic_name_prefix="anti-demo-r5",
        secret_name_prefix="",
        proxy_secret_arn="arn:secret",
        proxy_service_role_arn="arn:role",
        competitor_security_group_id="sg-database",
    )
    orchestrator._settle_commands = lambda _bout: asyncio.sleep(0)
    orchestrator._assumed_clients = lambda _bout: asyncio.sleep(0, result=object())
    orchestrator._baseline_rds_security_group = lambda _clients: asyncio.sleep(
        0, result="sg-database"
    )
    scope = CreationScope("bout-restart-journal", 43, "b" * 64)
    orchestrator._journal = SimpleNamespace(
        scopes=lambda _bout: asyncio.sleep(0, result=(scope,)),
        events=lambda _scope: asyncio.sleep(0, result=()),
    )
    orchestrator._restore_resource_bindings = lambda *_args, **_kwargs: asyncio.sleep(0)
    orchestrator._discover_orphaned_addons = (
        lambda *_args, **_kwargs: asyncio.sleep(0)
    )
    coordinator_tokens: list[int] = []
    released_tokens: list[int] = []
    reclaimed: list[int] = []
    deleted: list[str] = []

    class Adapter:
        def __init__(self, kind: str) -> None:
            self.kind = kind

        async def inspect(self, spec, provider_id=None):
            del spec, provider_id
            return None

        async def delete(self, observed):
            deleted.append(observed.resource_kind)

    specs = (
        ResourceSpec(1, "rds_proxy", "owned-proxy"),
        ResourceSpec(2, "proxy_target_group", "owned-target-group"),
        ResourceSpec(3, "proxy_target", "owned-target"),
    )
    coordinator = SimpleNamespace(
        _adapters={spec.resource_kind: Adapter(spec.resource_kind) for spec in specs},
    )

    async def reconcile_incomplete(*_args, **_kwargs):
        return SimpleNamespace(complete=True)

    coordinator.reconcile_incomplete = reconcile_incomplete

    def build_coordinator(auth_scope, *_args, **_kwargs):
        coordinator_tokens.append(auth_scope.fencing_token)
        return coordinator, specs

    orchestrator._coordinator = build_coordinator

    class ExpiredBoutFence:
        async def reclaim_expired_cleanup(self, scope):
            reclaimed.append(scope.fencing_token)
            return CreationScope(scope.bout_id, 44, scope.runtime_seal_sha256)

        async def assert_current(self, scope):
            if scope.fencing_token != 44:
                raise InvalidStateError("Round 5 ring fence is no longer current")

        async def release_cleanup(self, scope):
            released_tokens.append(scope.fencing_token)

    orchestrator._fence = ExpiredBoutFence()

    await orchestrator.reconcile_failed_cleanup(
        "bout-restart-journal",
        43,
        cleanup_authority=lambda: asyncio.sleep(0),
    )

    assert reclaimed == [43]
    assert coordinator_tokens == [44]
    assert released_tokens == [44]
    assert deleted == []


async def test_departed_process_without_coordinator_still_reclaims_ring_lease() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._lock = asyncio.Lock()
    orchestrator.config = SimpleNamespace(
        baseline_sha256="b" * 64,
        deterministic_name_prefix="anti-demo-r5",
        secret_name_prefix="",
        proxy_secret_arn="arn:secret",
        proxy_service_role_arn="arn:role",
        competitor_security_group_id="sg-database",
    )
    orchestrator._settle_commands = lambda _bout: asyncio.sleep(0)
    orchestrator._assumed_clients = lambda _bout: asyncio.sleep(0, result=object())
    orchestrator._baseline_rds_security_group = lambda _clients: asyncio.sleep(
        0, result="sg-database"
    )
    orchestrator._journal = SimpleNamespace(
        scopes=lambda _bout: asyncio.sleep(0, result=())
    )
    orchestrator._discover_orphaned_addons = (
        lambda *_args, **_kwargs: asyncio.sleep(0)
    )
    deleted: list[str] = []
    coordinator_tokens: list[int] = []
    released_tokens: list[int] = []

    class Adapter:
        def __init__(self, kind: str) -> None:
            self.kind = kind

        async def inspect(self, spec, provider_id=None):
            del spec, provider_id
            return (
                SimpleNamespace(resource_kind=self.kind)
                if self.kind == "rds_proxy" and not deleted
                else None
            )

        async def delete(self, observed):
            deleted.append(observed.resource_kind)

    specs = (
        ResourceSpec(1, "rds_proxy", "owned-proxy"),
        ResourceSpec(2, "proxy_target_group", "owned-target-group"),
        ResourceSpec(3, "proxy_target", "owned-target"),
    )
    coordinator = SimpleNamespace(
        _adapters={spec.resource_kind: Adapter(spec.resource_kind) for spec in specs}
    )

    def build_coordinator(scope, *_args, **_kwargs):
        coordinator_tokens.append(scope.fencing_token)
        return coordinator, specs

    orchestrator._coordinator = build_coordinator

    class ReclaimedBoutFence:
        async def reclaim_expired_cleanup(self, scope):
            return CreationScope(scope.bout_id, 44, scope.runtime_seal_sha256)

        async def assert_current(self, scope):
            assert scope.fencing_token == 44

        async def release_cleanup(self, scope):
            released_tokens.append(scope.fencing_token)

    orchestrator._fence = ReclaimedBoutFence()

    await orchestrator.reconcile_failed_cleanup("bout-departed", 43)

    assert deleted == ["rds_proxy"]
    assert coordinator_tokens == [44]
    assert released_tokens == [44]


async def test_incomplete_child_journal_still_deletes_parent_proxy() -> None:
    """Child journal incompleteness must not become cleanup_reconcile_blocked."""

    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator._lock = asyncio.Lock()
    orchestrator.config = SimpleNamespace(
        baseline_sha256="b" * 64,
        deterministic_name_prefix="anti-demo-r5",
        secret_name_prefix="",
        proxy_secret_arn="arn:secret",
        proxy_service_role_arn="arn:role",
        competitor_security_group_id="sg-database",
    )
    orchestrator._settle_commands = lambda _bout: asyncio.sleep(0)
    orchestrator._assumed_clients = lambda _bout: asyncio.sleep(0, result=object())
    orchestrator._baseline_rds_security_group = lambda _clients: asyncio.sleep(
        0, result="sg-database"
    )
    scope = CreationScope("bout-child", 43, "b" * 64)
    orchestrator._journal = SimpleNamespace(
        scopes=lambda _bout: asyncio.sleep(0, result=(scope,)),
        events=lambda _scope: asyncio.sleep(0, result=()),
    )
    orchestrator._restore_resource_bindings = lambda *_args, **_kwargs: asyncio.sleep(0)
    orchestrator._discover_orphaned_addons = (
        lambda *_args, **_kwargs: asyncio.sleep(0)
    )
    present = {"rds_proxy": True}
    deleted: list[str] = []

    class Adapter:
        def __init__(self, kind: str) -> None:
            self.kind = kind

        async def inspect(self, spec, provider_id=None):
            del spec, provider_id
            return (
                SimpleNamespace(resource_kind=self.kind)
                if present.get(self.kind)
                else None
            )

        async def delete(self, observed):
            deleted.append(observed.resource_kind)
            present[observed.resource_kind] = False

    specs = (
        ResourceSpec(1, "rds_proxy", "owned-proxy"),
        ResourceSpec(2, "proxy_target_group", "owned-target-group"),
        ResourceSpec(3, "proxy_target", "owned-target"),
    )
    coordinator = SimpleNamespace(
        _adapters={spec.resource_kind: Adapter(spec.resource_kind) for spec in specs},
        reconcile_incomplete=lambda *_args, **_kwargs: asyncio.sleep(
            0, result=SimpleNamespace(complete=False)
        ),
    )
    reconciles = {"n": 0}

    async def reconcile_incomplete(*_args, **_kwargs):
        reconciles["n"] += 1
        return SimpleNamespace(complete=reconciles["n"] > 1)

    coordinator.reconcile_incomplete = reconcile_incomplete
    orchestrator._coordinator = lambda *_args, **_kwargs: (coordinator, specs)
    orchestrator._fence = SimpleNamespace(
        assert_current=lambda _scope: asyncio.sleep(0),
    )

    await orchestrator.reconcile_failed_cleanup("bout-child", 43)

    assert deleted == ["rds_proxy"]
    assert present["rds_proxy"] is False
    assert reconciles["n"] >= 2


async def test_deleting_proxy_target_group_describe_is_transient() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        deterministic_name_prefix="anti-demo-r5",
        secret_name_prefix="",
        ownership_tags={"anti-demo": "true"},
        vpc_id="vpc-1",
        runner_security_group_id="sg-runner",
    )

    class Fault(Exception):
        response = {"Error": {"Code": "InvalidDBProxyStateFault"}}

    class Rds:
        def describe_db_proxies(self, **_kwargs):
            return {"DBProxies": []}

        def describe_db_proxy_target_groups(self, **_kwargs):
            raise Fault()

    class Ec2:
        def describe_security_groups(self, **_kwargs):
            return {"SecurityGroups": []}

        def describe_security_group_rules(self, **_kwargs):
            return {"SecurityGroupRules": []}

    async def call(operation, **kwargs):
        return operation(**kwargs)

    orchestrator._call = call
    orchestrator._error_code = lambda exc: str(
        (getattr(exc, "response", {}) or {}).get("Error", {}).get("Code") or ""
    )

    with pytest.raises(ConnectionSpikeLiveTransientError, match="still deleting"):
        await orchestrator._discover_orphaned_addons(
            SimpleNamespace(
                rds=Rds(),
                ec2=Ec2(),
                iam=SimpleNamespace(),
                secretsmanager=SimpleNamespace(),
            ),
            "sg-database",
            include_legacy=False,
            bout_id="bout-deleting-tg",
        )


def test_in_flight_proxy_delete_faults_are_retryable_not_blocked() -> None:
    from server.connection_spike_live import LiveRound5WarmProvider

    provider = object.__new__(LiveRound5WarmProvider)

    class Fault(Exception):
        response = {"Error": {"Code": "InvalidDBProxyStateFault"}}

    wrapped = ConnectionSpikeCleanupError(
        "Round 5 scoped orphan discovery failed",
        stage="scoped_orphan_discovery",
        reason_code="scoped_orphan_read_failed",
    )
    wrapped.__cause__ = ConnectionSpikeLiveTransientError(
        "Round 5 bout-owned proxy is still deleting"
    )
    assert provider._retryable(ConnectionSpikeLiveTransientError("still deleting"))
    assert provider._retryable(Fault())
    assert provider._retryable(wrapped)
    assert provider._retryable(
        InvalidStateError("Round 5 prior cleanup owner is still active")
    )
    assert provider._retryable(
        ConnectionSpikeLiveOperationError(
            "Round 5 journal write lost its active lease fence"
        )
    )
    assert not provider._retryable(
        ConnectionSpikeLiveConfigurationError("identity changed")
    )


def test_warm_runner_ssm_outage_is_retryable_without_hiding_identity_drift() -> None:
    with pytest.raises(ConnectionSpikeLiveTransientError, match="temporarily unavailable"):
        _require_warm_runner_online(
            [{"InstanceId": "i-0123456789abcdef0", "PingStatus": "ConnectionLost"}],
            expected_instance_id="i-0123456789abcdef0",
            lane_id="lakebase",
        )
    with pytest.raises(ConnectionSpikeLiveTransientError, match="temporarily absent"):
        _require_warm_runner_online(
            [],
            expected_instance_id="i-0123456789abcdef0",
            lane_id="lakebase",
        )
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="identity changed"):
        _require_warm_runner_online(
            [{"InstanceId": "i-0fedcba9876543210", "PingStatus": "ConnectionLost"}],
            expected_instance_id="i-0123456789abcdef0",
            lane_id="lakebase",
        )
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="invalid SSM ping status"):
        _require_warm_runner_online(
            [{"InstanceId": "i-0123456789abcdef0", "PingStatus": "Inactive"}],
            expected_instance_id="i-0123456789abcdef0",
            lane_id="lakebase",
        )


async def test_real_warm_and_preflight_paths_retry_connection_lost() -> None:
    runner_id = "i-0123456789abcdef0"
    competitor_runner_id = "i-0fedcba9876543210"

    class Ssm:
        def describe_instance_information(self, **kwargs):
            instance_id = kwargs["Filters"][0]["Values"][0]
            return {
                "InstanceInformationList": [
                    {
                        "InstanceId": instance_id,
                        "PingStatus": "ConnectionLost",
                        "PlatformType": "Linux",
                    }
                ]
            }

    setup = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    setup.config = SimpleNamespace(
        runner_instance_id=runner_id,
        competitor_runner_instance_id=competitor_runner_id,
        competitor_target_id="rds-source",
        competitor_resource_id="db-RESOURCE",
        competitor_direct_host="rds-direct.test",
        vpc_id="vpc-sealed",
        competitor_security_group_id="sg-rds",
    )
    clients = SimpleNamespace(ssm=Ssm())

    async def assumed_clients(_session_name):
        return clients

    async def read_source(_clients):
        return SimpleNamespace(
            identifier="rds-source",
            resource_id="db-RESOURCE",
            direct_host="rds-direct.test",
            status="available",
            vpc_id="vpc-sealed",
            security_group_ids=("sg-rds",),
        )

    setup._assumed_clients = assumed_clients
    setup._read_competitor_source = read_source
    with pytest.raises(ConnectionSpikeLiveTransientError, match="ConnectionLost"):
        await setup.warm(4)

    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter.config = SimpleNamespace(
        runner_instance_id=runner_id,
        runner_security_group_id="sg-runner",
        runner_lane="lakebase",
        runner_instance_type="c7i.2xlarge",
        runner_subnet_id="subnet-runner",
        runner_instance_profile_arn=(
            f"arn:aws:iam::{ACCOUNT}:instance-profile/runner"
        ),
    )
    instance_state = "running"
    preflight_clients = SimpleNamespace(
        ssm=Ssm(),
        ec2=SimpleNamespace(
            describe_instances=lambda **_kwargs: {
                "Reservations": [
                    {
                        "Instances": [
                            {
                                "InstanceId": runner_id,
                                    "State": {"Name": instance_state},
                                "InstanceType": "c7i.2xlarge",
                                "SubnetId": "subnet-runner",
                                "IamInstanceProfile": {
                                    "Arn": (
                                        f"arn:aws:iam::{ACCOUNT}:instance-profile/runner"
                                    )
                                },
                                "SecurityGroups": [{"GroupId": "sg-runner"}],
                                "PublicIpAddress": "203.0.113.10",
                                "MetadataOptions": {"HttpTokens": "required"},
                            }
                        ]
                    }
                ]
            },
            describe_security_groups=lambda **_kwargs: {
                "SecurityGroups": [
                    {
                        "GroupId": "sg-runner",
                        "IpPermissions": [],
                    }
                ]
            },
        ),
    )
    with pytest.raises(ConnectionSpikeLiveTransientError, match="ConnectionLost"):
        await adapter._preflight_runner(preflight_clients)
    instance_state = "stopped"
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="sealed contract"):
        await adapter._preflight_runner(preflight_clients)


async def test_creation_journal_store_uses_parameterized_append_only_boundaries() -> None:
    rows: list[tuple[object, ...]] = []
    calls: list[tuple[str, tuple[object, ...]]] = []

    class Cursor:
        statement = ""

        async def execute(self, statement, parameters):
            self.statement = statement
            calls.append((statement, parameters))
            if "INSERT INTO" in statement:
                rows.append(parameters)

        async def fetchone(self):
            return (1,)

        async def fetchall(self):
            if "GROUP BY" in self.statement:
                return [(row[0], row[1], row[9]) for row in rows]
            return [
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    json.loads(str(row[8])),
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                )
                for row in rows
            ]

    async def run(operation):
        return await operation(Cursor())

    store = LakebaseCreationJournalStore(run)
    scope = CreationScope("bout-parameterized", 7, "a" * 64)
    event = JournalEvent.creation_intent(
        scope,
        ResourceSpec(
            ordinal=1,
            resource_kind="proxy_secret",
            deterministic_name="r5-deadbeef-secret",
            metadata={"resource_id": "safe-id"},
        ),
        now=datetime.now(UTC),
    )

    authority = CreationScope("bout-parameterized", 8, "a" * 64)
    await store.commit(event, authority_scope=authority)
    measured = await store.events(scope)
    scopes = await store.scopes(scope.bout_id)

    assert measured == (event,)
    assert scopes == (scope,)
    insert, select, list_scopes = calls
    assert "bout-parameterized" not in insert[0]
    assert insert[1][0:4] == ("bout-parameterized", 7, 1, "proxy_secret")
    assert "expires_at > clock_timestamp()" in insert[0]
    assert insert[1][-3:] == ("main", "bout-parameterized", 8)
    assert "WHERE bout_id = %s AND fencing_token = %s" in select[0]
    assert select[1] == ("bout-parameterized", 7)
    assert list_scopes[1] == ("bout-parameterized",)


async def test_creation_journal_store_targets_configured_authority_ring() -> None:
    authority_keys: list[str] = []

    class Cursor:
        async def execute(self, statement, parameters):
            assert "expires_at > clock_timestamp()" in statement
            authority_keys.append(str(parameters[-3]))

        async def fetchone(self):
            return (1,)

    async def run(operation):
        return await operation(Cursor())

    scope = CreationScope("bout-authority-ring", 11, "b" * 64)
    event = JournalEvent.creation_intent(
        scope,
        ResourceSpec(
            ordinal=1,
            resource_kind="proxy_secret",
            deterministic_name="r5-authority-secret",
            metadata={"resource_id": "safe-id"},
        ),
        now=datetime.now(UTC),
    )

    await LakebaseCreationJournalStore(
        run,
        authority_ring_key="round5",
    ).commit(event)
    scoped_key = round_ring_key(
        "install-a",
        "survive_connection_spike",
        cleanup=True,
    )
    await LakebaseCreationJournalStore(
        run,
        authority_ring_key=scoped_key,
    ).commit(event)
    await LakebaseCreationJournalStore(run).commit(event)

    assert authority_keys == ["round5", scoped_key, "main"]
    with pytest.raises(
        ConnectionSpikeLiveConfigurationError,
        match="authority ring key is invalid",
    ):
        LakebaseCreationJournalStore(run, authority_ring_key="round5 cleanup")


async def test_creation_journal_store_discovers_only_unresolved_bouts() -> None:
    statements: list[str] = []

    class Cursor:
        async def execute(self, statement):
            statements.append(statement)

        async def fetchall(self):
            return [("bout-stale-a",), ("bout-stale-b",)]

    async def run(operation):
        return await operation(Cursor())

    store = LakebaseCreationJournalStore(run)

    assert await store.unresolved_bout_ids() == (
        "bout-stale-a",
        "bout-stale-b",
    )
    assert "row_number() OVER" in statements[0]
    assert "lifecycle_state <> 'deleted'" in statements[0]


def test_manifest_factories_select_static_proxy_secret_and_checksum_binding() -> None:
    contract_sha256 = "f0e9a6960fb22cc052486b62cf01e32dcaabacf70508a2bc087ddc25deafa81c"
    baseline_sha256 = "d" * 64
    config_sha256 = hashlib.sha256(
        json.dumps(
            {
                "baseline_sha256": baseline_sha256,
                "contract_sha256": contract_sha256,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    resources = SimpleNamespace(
        control_role_arn=f"arn:aws:iam::{ACCOUNT}:role/baseline-control",
        runner_instance_id="i-0123456789abcdef0",
        competitor_runner_instance_id="i-0fedcba9876543210",
        lakebase_control_queue_url=(f"https://sqs.us-west-2.amazonaws.com/{ACCOUNT}/lakebase.fifo"),
        competitor_control_queue_url=(
            f"https://sqs.us-west-2.amazonaws.com/{ACCOUNT}/competitor.fifo"
        ),
        runner_control_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:runner-control"
        ),
        competitor_runner_control_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:competitor-control"
        ),
        runner_instance_profile_arn=f"arn:aws:iam::{ACCOUNT}:instance-profile/runner",
        competitor_runner_instance_profile_arn=(
            f"arn:aws:iam::{ACCOUNT}:instance-profile/competitor-runner"
        ),
        runner_subnet_id="subnet-a",
        runner_security_group_id="sg-runner",
        competitor_runner_security_group_id="sg-competitor-runner",
        aurora_proxy_security_group_id="sg-proxy-aurora",
        rds_proxy_security_group_id="sg-proxy-rds",
        runner_role_arn=f"arn:aws:iam::{ACCOUNT}:role/runner",
        vpc_id="vpc-sealed",
        proxy_subnet_ids=("subnet-a", "subnet-b"),
        proxy_service_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-service",
        proxy_service_policy_name="proxy-service-secrets",
        lakebase_direct_host="lakebase-direct.test",
        lakebase_pooled_host="lakebase-pooled.test",
        aurora_cluster_id="aurora-source",
        aurora_cluster_resource_id="cluster-RESOURCE",
        aurora_direct_host="aurora-direct.test",
        aurora_credential_sha256="a" * 64,
        aurora_proxy_secret_arn=(f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora-proxy"),
        rds_resource_id="db-RESOURCE",
        rds_direct_host="rds-direct.test",
        rds_credential_sha256="f" * 64,
        # A complete seal names an observer credential per lane; the fan-in
        # request requires one and the runner refuses a request without it.
        lakebase_observer_credential_sha256="f" * 64,
        aurora_observer_credential_sha256="f" * 64,
        rds_observer_credential_sha256="f" * 64,
        rds_proxy_secret_arn=(f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds-proxy"),
        bout_name_prefix="anti-demo-r5",
        ownership_tags=SimpleNamespace(
            as_aws_tags=lambda: {"Owner": "anti-demo", "owner": "anti-demo"}
        ),
        trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
        trust_bundle_sha256="b" * 64,
        runner_public_key_sha256="c" * 64,
        baseline_sha256=baseline_sha256,
        lakebase_credential_sha256="e" * 64,
        runner_path="/opt/lakebase-anti-demo/round5/run_connection_spike.sh",
        runner_harness_sha256="1" * 64,
        ssm_document_name="AWS-RunShellScript",
        native_role="anti_demo_burst",
        frozen_constants=SimpleNamespace(
            runner_instance_type="c7i.2xlarge",
            rds_proxy_max_connections_percent=90,
            rds_proxy_borrow_timeout_seconds=120,
        ),
        contract_sha256=contract_sha256,
        config_sha256=config_sha256,
    )
    manifest = SimpleNamespace(
        aws=SimpleNamespace(
            region="us-west-2",
            account_id=ACCOUNT,
            resources=SimpleNamespace(
                rds_instance_id="rds-source",
                security_group_id="sg-aurora",
                rds_security_group_id="sg-rds",
            ),
        ),
        databricks=SimpleNamespace(database="anti_demo"),
        expiry_warning=lambda: None,
        require_round5_resources=lambda: resources,
    )

    rds_live = connection_spike_live_config_from_manifest(manifest, "rds_postgres")
    competitor_live = connection_spike_live_config_from_manifest(
        manifest,
        "rds_postgres",
        runner_lane="competitor",
    )
    aurora_live = connection_spike_live_config_from_manifest(manifest, "aurora_serverless_v2")
    rds_setup = connection_spike_setup_config_from_manifest(manifest, "rds_postgres")
    aurora_setup = connection_spike_setup_config_from_manifest(manifest, "aurora_serverless_v2")

    assert rds_live.targets[1].secret_arn == resources.rds_proxy_secret_arn
    assert rds_live.runner_security_group_id == resources.runner_security_group_id
    assert rds_live.resident_control_secret_arn == resources.runner_control_secret_arn
    assert competitor_live.runner_security_group_id == resources.competitor_runner_security_group_id
    assert (
        competitor_live.resident_control_secret_arn
        == resources.competitor_runner_control_secret_arn
    )
    assert aurora_live.targets[1].secret_arn == resources.aurora_proxy_secret_arn
    assert rds_setup.proxy_secret_arn == resources.rds_proxy_secret_arn
    assert rds_setup.runner_security_group_id == resources.competitor_runner_security_group_id
    assert aurora_setup.proxy_secret_arn == resources.aurora_proxy_secret_arn
    assert rds_setup.proxy_service_role_arn == resources.proxy_service_role_arn
    assert connection_spike_config_sha256(rds_live) != connection_spike_config_sha256(aurora_live)


def test_v7_round5_rds_setup_uses_dedicated_instance_and_security_group(tmp_path) -> None:
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    round5 = manifest.round_environment("survive_connection_spike")
    assert round5.rds is not None

    config = connection_spike_setup_config_from_manifest(manifest, "rds_postgres")

    assert config.competitor_target_id == round5.rds.instance_id
    assert config.competitor_security_group_id == round5.rds.security_group_id
    assert config.competitor_target_id != manifest.aws.resources.rds_instance_id
    assert config.competitor_security_group_id != manifest.aws.resources.rds_security_group_id


def test_no_round5_manifest_gate_consults_expiry_at_all(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Both Round 5 gates must report a passed TTL, never refuse on it.

    This used to install a detonating `assert_not_expired` to prove the call was
    gone rather than merely no longer fatal. That method has since been deleted
    outright -- `tests/test_expiry_renew.py` asserts it cannot come back -- so
    there is nothing left to detonate, and what remains to check here is the
    behaviour: both configs build from a manifest that is hours past its TTL, and
    the only trace of the expiry is the advisory line.
    """
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    assert manifest.expires_at < datetime.now(UTC), "fixture must be past its TTL"
    assert not hasattr(type(manifest), "assert_not_expired"), (
        "a refusing expiry gate is back on the manifest; these builders swallow "
        "RuntimeError, so it would silently stop Round 5 arming again"
    )

    for competitor_id in ("rds_postgres", "aurora_serverless_v2"):
        assert connection_spike_live_config_from_manifest(manifest, competitor_id)
        assert connection_spike_setup_config_from_manifest(manifest, competitor_id)

    # The same line the Round 2/3 builder prints, so an operator reading an
    # expired installation's log cannot tell the rounds apart.
    assert f"WARN  {manifest.expiry_warning()}" in capsys.readouterr().out


def test_expired_manifest_no_longer_deletes_round5_from_a_running_installation(
    tmp_path,
) -> None:
    """The live symptom, not just the call site that caused it.

    `connection_spike_factory_from_manifest` builds both Round 5 configs inside
    `except (RuntimeError, ValueError): return None`, and `assert_not_expired`
    raises `RuntimeError`.  So a passed TTL did not surface as a refusal an
    operator could read: it silently returned no factory, and Round 5 alone
    stopped being able to arm while Rounds 1-4 and 6 carried on.
    """
    import app as app_module

    manifest = _v7_manifest_past_ttl(tmp_path)

    class LakebaseLeaseStore:
        mode = "lakebase"
        ring_key = ""

        def _run(self, *_args, **_kwargs) -> None: ...

        async def current(self) -> None:
            return None

    lease_store = LakebaseLeaseStore()
    lease_store.ring_key = app_module._round5_lease_ring_key(manifest)

    factory = app_module.connection_spike_factory_from_manifest(manifest, lease_store=lease_store)

    assert factory is not None, "Round 5 must still be offered past the TTL"


def _v7_manifest_past_ttl(tmp_path):
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    assert manifest.expires_at < datetime.now(UTC), "fixture must be past its TTL"
    assert manifest.round5_ready
    return manifest


def test_production_setup_stop_facts_survive_public_projection_end_to_end() -> None:
    """The exact shipped Lakebase + Aurora stop-gate facts must survive the
    public projection and drive ``setup_validated=true`` through the manager.

    Derived from the production ``_setup_observation`` so a fact-key regression
    (the ``endpoint`` denylist collision that once nulled the Lakebase gate)
    fails here. This is the collected replacement for the v4-stale
    ``legacy_two_phase_setup_*`` flow, which now trips the warm-context guard for
    reasons unrelated to the setup-stop evidence contract.
    """

    from test_connection_spike import verified_fanin_lane

    t0_ns = 1_000_000_000

    def observation(lane_id: str, *, launch_delay_ns: int, elapsed_ns: int):
        stop = ConnectionSpikeSetupLaneStop(
            lane_id=lane_id,
            launched_ns=t0_ns + launch_delay_ns,
            stopped_ns=t0_ns + elapsed_ns,
            credential_sha256="a" * 64,
            endpoint_host="pooled.internal" if lane_id == "lakebase" else "proxy.internal",
            secret_arn="" if lane_id == "lakebase" else f"arn:aws:secretsmanager:x:{ACCOUNT}:s:z",
            # The AWS competitor now fails closed without its CreateDBProxy request
            # stamp; supply a within-budget one (~12 ms after T0) so this test
            # exercises the public-projection contract, not the missing-stamp gate.
            create_db_proxy_requested_ns=(
                t0_ns + 12_000_000 if lane_id == "competitor" else None
            ),
        )
        return LiveConnectionSpikeSetupOrchestrator._setup_observation(None, stop)

    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observations = (
        observation("lakebase", launch_delay_ns=2_000_000, elapsed_ns=3_600_000_000),
        observation("competitor", launch_delay_ns=6_000_000, elapsed_ns=650_000_000_000),
    )
    fanin = {lane_id: verified_fanin_lane(lane_id) for lane_id in ("lakebase", "competitor")}
    result = finalize_setup_phase(arm, observations, fanin)
    assert result.setup_validated

    fake_snapshot = SimpleNamespace(
        id="sess-live-projection",
        lanes={
            "lakebase": SimpleNamespace(name="Lakebase"),
            "competitor": SimpleNamespace(name="Aurora"),
        },
    )
    projected = RunManager._round_five_setup_snapshot(fake_snapshot, result, terminal=True)
    assert projected.setup_validated
    for lane_id in ("lakebase", "competitor"):
        lane = projected.lanes[lane_id]
        assert lane.verified, lane_id
        assert lane.stop_gate_evidence is not None and lane.stop_gate_evidence.exact
        assert lane.setup_diagnostic is None
        assert lane.workflow_launch_delay_ms is not None
    assert RunManager._round_five_setup_comparison(result, "Aurora") is not None


async def legacy_two_phase_setup_uses_assumed_clients_shared_t0_and_defers_burst(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_requests: list[dict[str, object]] = []

    class Sts:
        def assume_role(self, **kwargs):
            assert kwargs["RoleArn"] == f"arn:aws:iam::{ACCOUNT}:role/baseline-control"
            assert kwargs["DurationSeconds"] == 3600
            return {
                "Credentials": {
                    "AccessKeyId": "temporary-access",
                    "SecretAccessKey": "temporary-secret",
                    "SessionToken": "temporary-token",
                    "Expiration": datetime.now(UTC) + timedelta(hours=1),
                },
                "AssumedRoleUser": {
                    "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/baseline-control/setup"
                },
            }

    class Ssm:
        def send_command(self, **kwargs):
            command = kwargs["Parameters"]["commands"][0]
            encoded = command.rsplit(" ", 1)[1]
            request = json.loads(gzip.decompress(base64.urlsafe_b64decode(encoded)))
            setup_requests.append(request)
            return {"Command": {"CommandId": f"command-{len(setup_requests)}"}}

        def get_command_invocation(self, **kwargs):
            request = setup_requests[int(kwargs["CommandId"].rsplit("-", 1)[1]) - 1]
            receipt = {
                "protocol": "connection-spike-setup-v1",
                "action": request["action"],
                "bout_id": request["bout_id"],
                "lane_id": request["lane_id"],
                "nonce": request["nonce"],
                "status": "verified",
            }
            return {
                "Status": "Success",
                "StandardOutputContent": (
                    f"SETUP_RESULT:{json.dumps(receipt, separators=(',', ':'))}\n"
                    f"SETUP_SETTLED:{request['nonce']}\n"
                    f"RUNNER_FLOCK_RELEASED:{request['bout_id']}\n"
                ),
            }

    ssm = Ssm()
    origins: list[tuple[str, str]] = []

    class SessionFactory:
        def __call__(self, **kwargs):
            origin = "assumed" if "aws_access_key_id" in kwargs else "ambient"

            class Session:
                def client(self, name, **client_kwargs):
                    assert client_kwargs == {"region_name": "us-west-2"}
                    origins.append((origin, name))
                    if origin == "ambient":
                        assert name == "sts"
                        return Sts()
                    return ssm if name == "ssm" else SimpleNamespace()

            return Session()

    config = ConnectionSpikeSetupConfig(
        region="us-west-2",
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
        aurora_proxy_secret_arn=(f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora-proxy"),
        rds_proxy_secret_arn=(f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds-proxy"),
        deterministic_name_prefix="anti-demo-r5",
        ownership_tags=(("Owner", "anti-demo"), ("owner", "anti-demo")),
        trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
        trust_bundle_sha256="b" * 64,
        runner_public_key_sha256="c" * 64,
        baseline_sha256="d" * 64,
        lakebase_credential_sha256="e" * 64,
        competitor_credential_sha256="f" * 64,
        runner_role_arn=f"arn:aws:iam::{ACCOUNT}:role/runner",
        proxy_role_permissions_boundary_arn=(f"arn:aws:iam::{ACCOUNT}:policy/proxy-boundary"),
        secret_name_prefix="anti-demo/r5",
        competitor_master_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds-master"
        ),
    )
    assert config.proxy_registration == {"DBInstanceIdentifiers": ["rds-source"]}

    class Fence:
        async def assert_current(self, scope):
            assert scope.fencing_token == 11

    class Journal:
        async def events(self, scope):
            return ()

        async def commit(self, event, *, authority_scope=None):
            del authority_scope
            raise AssertionError("fake coordinator owns this test boundary")

        async def scopes(self, bout_id):
            del bout_id
            return ()

    ticks = 1_000_000_000

    def monotonic_ns():
        nonlocal ticks
        ticks += 1_000_000
        return ticks

    orchestrator = LiveConnectionSpikeSetupOrchestrator(
        config,
        journal=Journal(),
        fence=Fence(),
        fresh_lakebase_host=lambda: _value("lakebase-pooled.test"),
        session_factory=SessionFactory(),
        monotonic_ns=monotonic_ns,
    )
    iam_calls: list[tuple[str, dict[str, object]]] = []

    class ServiceIam:
        def __init__(self, *, drift: bool = False):
            self.drift = drift

        def get_role(self, **kwargs):
            iam_calls.append(("get_role", kwargs))
            return {
                "Role": {
                    "RoleName": "proxy-service",
                    "Arn": config.proxy_service_role_arn,
                    "AssumeRolePolicyDocument": orchestrator._proxy_trust_policy(),
                }
            }

        def get_role_policy(self, **kwargs):
            iam_calls.append(("get_role_policy", kwargs))
            policy = orchestrator._proxy_service_policy()
            if self.drift:
                policy = {**policy, "Statement": [*policy["Statement"], {"Effect": "Allow"}]}
            return {"PolicyDocument": policy}

    await orchestrator._verify_proxy_service_role(SimpleNamespace(iam=ServiceIam()))
    assert (
        "get_role_policy",
        {
            "RoleName": "proxy-service",
            "PolicyName": "proxy-service-secrets",
        },
    ) in iam_calls
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="policy document changed"):
        await orchestrator._verify_proxy_service_role(SimpleNamespace(iam=ServiceIam(drift=True)))
    original_coordinator = orchestrator._coordinator
    creation_order: list[str] = []
    coordinators: list[Coordinator] = []

    class Coordinator:
        def __init__(self, resources, clients, scope):
            self.resources = resources
            self.clients = clients
            self.scope = scope

        async def create_resource(self, scope, spec):
            del scope
            creation_order.append(spec.resource_kind)
            if spec.ordinal == 7:
                self.resources.proxy_endpoint = "dynamic-proxy.test"
            return SimpleNamespace(provider_id=f"provider-{spec.ordinal}")

        async def seal(self, scope):
            del scope
            return SimpleNamespace()

        async def reconcile_incomplete(self, scope):
            del scope
            return SimpleNamespace(complete=True)

    resource_kinds = (
        "proxy_security_group",
        "proxy_default_egress",
        "proxy_ingress",
        "proxy_egress",
        "runner_egress",
        "rds_ingress",
        "rds_proxy",
        "proxy_target_group",
        "proxy_target",
    )
    specs = tuple(
        ResourceSpec(index, kind, f"resource-{index}", metadata={"resource_id": index})
        for index, kind in enumerate(resource_kinds, 1)
    )

    def coordinator(scope, clients, resources):
        value = Coordinator(resources, clients, scope)
        coordinators.append(value)
        return value, specs

    async def preflight(*args):
        nonlocal ticks
        del args
        # Model slow, unscored preflight work. The shared setup T0 must be
        # captured only after this time has passed.
        ticks += 5_000_000_000

    async def verify_topology(clients, resources):
        del clients
        assert resources.proxy_endpoint == "dynamic-proxy.test"
        creation_order.append("topology_reread")

    async def wait_proxy_available(clients, resources):
        del clients
        assert resources.proxy_endpoint == "dynamic-proxy.test"

    async def verify_journaled_resources(scope, coordinator, measured_specs):
        del scope, coordinator
        assert tuple(item.resource_kind for item in measured_specs) == resource_kinds

    monkeypatch.setattr(orchestrator, "_coordinator", coordinator)
    monkeypatch.setattr(orchestrator, "_preflight_baseline", preflight)
    monkeypatch.setattr(orchestrator, "_verify_proxy_topology", verify_topology)
    monkeypatch.setattr(orchestrator, "_wait_proxy_available", wait_proxy_available)
    monkeypatch.setattr(orchestrator, "_verify_journaled_resources", verify_journaled_resources)

    measured = SimpleNamespace(sufficient=True, failures=())

    async def preflight_capacity(run_id, **digests):
        # The arm records what this machine measured, so the stand-in has to answer.
        # Sufficient, because this test is about the setup phase's two stops; a runner
        # that cannot hold the clients is covered where that refusal is the subject.
        del run_id, digests
        return measured

    adapter = SimpleNamespace(
        config=SimpleNamespace(
            targets=(SimpleNamespace(lane_id="lakebase"), SimpleNamespace(lane_id="competitor"))
        ),
        check=lambda: _value(None),
        preflight_capacity=preflight_capacity,
    )
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    # Arming no longer waits for the setup clocks. The capacity preflight measures this runner,
    # which is the same answer before the bell as after it, and requiring both stops first put an
    # SSM round trip inside the dead period the round is judged on. The precondition moved to the
    # bout, which is what actually needs the endpoints setup produces.
    with pytest.raises(ConnectionSpikeLiveOperationError, match="before both timed setup stops"):
        await engine.run(await engine.check())

    progress = []

    async def capture_progress(value):
        progress.append(value)

    setup = await engine.setup("bout-two-phase", 11, capture_progress)
    arm = await engine.check()

    # A fan-in arm is the four digests plus the capacity that justified attempting
    # 10,000 clients per lane. Compared against the runner's own functions, because an
    # arm whose digests this side computed differently is an arm the runner refuses.
    assert arm.contract_sha256 == runner_fanin.contract_sha256()
    assert arm.config_sha256 == runner_fanin.config_sha256()
    assert arm.generator_sha256 == runner_fanin.generator_sha256()
    assert arm.capacity_model_sha256 == runner_fanin.capacity_model_sha256()
    assert arm.preflight is measured
    assert setup.deadline_ns - setup.t0_ns == 30 * 60 * 1_000_000_000
    assert setup.t0_ns >= 6_000_000_000
    assert setup.launch_skew_ms <= 10
    assert progress
    assert all(item.setup_elapsed_ms is not None for item in progress)
    assert progress[0].setup_elapsed_ms < 100
    finalized_setup = RunManager._round_five_finalize_setup(setup, {})
    assert finalized_setup is not None
    for lane_id, lane_stop in (
        ("lakebase", setup.lakebase),
        ("competitor", setup.competitor),
    ):
        stop_progress = next(
            item for item in progress if item.lane_id == lane_id and item.phase == "setup_stop"
        )
        exact_elapsed_ms = (lane_stop.stopped_ns - setup.t0_ns) / 1_000_000
        assert stop_progress.status == "verified"
        assert stop_progress.setup_elapsed_ms == pytest.approx(exact_elapsed_ms)
        assert finalized_setup.lanes[lane_id].setup_elapsed_ms == pytest.approx(exact_elapsed_ms)
    assert all(
        item.stop_gate_evidence and item.stop_gate_evidence.exact for item in setup.observations
    )
    public_gates = [
        RunManager._round_five_public_gate(item.stop_gate_evidence) for item in setup.observations
    ]
    assert all(gate is not None and gate.exact for gate in public_gates)
    assert creation_order == [*resource_kinds, "topology_reread"]
    assert [request["action"] for request in setup_requests if request["lane_id"] == "rds"] == [
        "verify"
    ]
    competitor_request = next(request for request in setup_requests if request["lane_id"] == "rds")
    assert competitor_request["endpoint_host"] == "dynamic-proxy.test"
    assert competitor_request["credential_host"] == "rds-direct.test"
    assert competitor_request["endpoint_host"] != competitor_request["credential_host"]
    assert [
        request["action"] for request in setup_requests if request["lane_id"] == "lakebase"
    ] == ["verify"]
    setup_common = {
        "protocol",
        "action",
        "nonce",
        "bout_id",
        "lane_id",
        "endpoint_host",
        "credential_host",
        "port",
        "dbname",
        "username",
        "trust_bundle_path",
        "trust_bundle_sha256",
        "credential_sha256",
    }
    assert all(set(request) == setup_common for request in setup_requests)
    assert origins == [
        ("ambient", "sts"),
        ("assumed", "ssm"),
        ("assumed", "rds"),
        ("assumed", "ec2"),
        ("assumed", "iam"),
        ("assumed", "secretsmanager"),
    ]
    assert all("password" not in json.dumps(request).lower() for request in setup_requests)
    owned = ResourceSpec(
        1,
        "proxy_secret",
        "owned",
        metadata={"tags": {"anti-demo-bout-id": "bout-two-phase", "owner": "anti-demo"}},
    )
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="ownership tags changed"):
        orchestrator._require_exact_tags(
            owned,
            [
                {"Key": "anti-demo-bout-id", "Value": "foreign-bout"},
                {"Key": "owner", "Value": "anti-demo"},
            ],
        )

    resources = coordinators[0].resources
    assert resources.secret_arn == config.rds_proxy_secret_arn
    assert resources.proxy_role_arn == config.proxy_service_role_arn
    resources.proxy_security_group_id = "sg-proxy"
    resources.rds_security_group_id = "sg-rds"
    rule_calls: list[tuple[str, dict[str, object]]] = []

    class RuleEc2:
        def authorize_security_group_ingress(self, **kwargs):
            rule_calls.append(("authorize", kwargs))
            return {"SecurityGroupRules": [{"SecurityGroupRuleId": "sgr-exact"}]}

        def describe_security_group_rules(self, **kwargs):
            rule_calls.append(("describe", kwargs))
            rule_spec = next(item for item in real_specs if item.resource_kind == "proxy_ingress")
            return {
                "SecurityGroupRules": [
                    {
                        "SecurityGroupRuleId": "sgr-exact",
                        "GroupId": "sg-proxy",
                        "IsEgress": False,
                        "ReferencedGroupInfo": {"GroupId": "sg-runner"},
                        "IpProtocol": "tcp",
                        "FromPort": 5432,
                        "ToPort": 5432,
                        "Description": rule_spec.deterministic_name,
                        "Tags": orchestrator._tags(rule_spec),
                    }
                ]
            }

        def revoke_security_group_ingress(self, **kwargs):
            rule_calls.append(("revoke", kwargs))
            return {}

        authorize_security_group_egress = authorize_security_group_ingress
        revoke_security_group_egress = revoke_security_group_ingress

    rule_clients = SimpleNamespace(ec2=RuleEc2())
    real_coordinator, real_specs = original_coordinator(
        CreationScope("bout-two-phase", 11, config.baseline_sha256),
        rule_clients,
        resources,
    )
    network_spec = next(item for item in real_specs if item.resource_kind == "proxy_security_group")
    assert network_spec.metadata["competitor_id"] == "rds_postgres"
    assert network_spec.metadata["competitor_target_id"] == "rds-source"
    assert tuple(item.ordinal for item in real_specs) == tuple(range(1, 10))
    rule_spec = next(item for item in real_specs if item.resource_kind == "proxy_ingress")
    assert "Owner" in {tag["Key"] for tag in orchestrator._tags(rule_spec)}
    rule_adapter = real_coordinator._adapters["proxy_ingress"]
    created_rule = await rule_adapter.create(rule_spec)
    recovered_rule = await rule_adapter.inspect(rule_spec, provider_id=None)
    await rule_adapter.delete(created_rule)
    authorize = rule_calls[0][1]
    assert authorize["TagSpecifications"][0]["ResourceType"] == "security-group-rule"
    assert authorize["IpPermissions"][0]["UserIdGroupPairs"][0]["Description"] == (
        rule_spec.deterministic_name
    )
    assert recovered_rule == created_rule
    assert rule_calls[-1][1]["SecurityGroupRuleIds"] == ["sgr-exact"]

    default_spec = next(item for item in real_specs if item.resource_kind == "proxy_default_egress")
    default_adapter = real_coordinator._adapters["proxy_default_egress"]
    await default_adapter.delete(orchestrator._observation(default_spec, "sg-proxy:default-egress"))
    restored_default_egress = rule_calls[-1][1]
    assert restored_default_egress["GroupId"] == "sg-proxy"
    assert restored_default_egress["IpPermissions"] == [
        {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
    ]
    assert restored_default_egress["TagSpecifications"] == [
        {
            "ResourceType": "security-group-rule",
            "Tags": orchestrator._tags(default_spec),
        }
    ]

    target_group_spec = next(
        item for item in real_specs if item.resource_kind == "proxy_target_group"
    )
    target_group_arn = f"arn:aws:rds:us-west-2:{ACCOUNT}:target-group:prx-tg-owned"
    target_group_calls: list[tuple[str, dict[str, object]]] = []

    class TargetGroupRds:
        tags: list[dict[str, str]] = []
        modified = False

        def describe_db_proxy_target_groups(self, **kwargs):
            target_group_calls.append(("describe", kwargs))
            return {
                "TargetGroups": [
                    {
                        "TargetGroupName": "default",
                        "TargetGroupArn": target_group_arn,
                        "ConnectionPoolConfig": {
                            "MaxConnectionsPercent": 90 if self.modified else 100,
                            "ConnectionBorrowTimeout": 120,
                        },
                    }
                ]
            }

        def add_tags_to_resource(self, **kwargs):
            target_group_calls.append(("tag", kwargs))
            self.tags = kwargs["Tags"]
            return {}

        def list_tags_for_resource(self, **kwargs):
            target_group_calls.append(("list_tags", kwargs))
            return {"TagList": self.tags}

        def modify_db_proxy_target_group(self, **kwargs):
            target_group_calls.append(("modify", kwargs))
            self.modified = True
            return {}

    target_group_rds = TargetGroupRds()
    target_group_clients = SimpleNamespace(rds=target_group_rds)
    created_target_group = await orchestrator._configure_target_group(
        target_group_clients,
        resources,
        target_group_spec,
    )
    assert [name for name, _ in target_group_calls] == [
        "describe",
        "tag",
        "list_tags",
        "modify",
    ]
    assert target_group_calls[1][1] == {
        "ResourceName": target_group_arn,
        "Tags": orchestrator._tags(target_group_spec),
    }
    inspected_target_group = await orchestrator._inspect_target_group(
        target_group_clients,
        resources,
        target_group_spec,
        created_target_group.provider_id,
    )
    assert inspected_target_group == created_target_group
    assert [name for name, _ in target_group_calls[-2:]] == ["describe", "list_tags"]

    discovery_arguments: dict[str, object] = {}

    class DiscoverySecrets:
        def list_secrets(self, **kwargs):
            discovery_arguments.update(kwargs)
            return {
                "SecretList": [
                    {
                        "Name": "anti-demo/r5/drifted",
                        "DeletedDate": datetime.now(UTC),
                        "Tags": [],
                    }
                ]
            }

    discovery_clients = SimpleNamespace(
        secretsmanager=DiscoverySecrets(),
        iam=SimpleNamespace(
            list_roles=lambda **kwargs: {"Roles": []},
            list_role_policies=lambda **kwargs: {"PolicyNames": []},
        ),
        ec2=SimpleNamespace(
            describe_security_groups=lambda **kwargs: {"SecurityGroups": []},
            describe_security_group_rules=lambda **kwargs: {"SecurityGroupRules": []},
        ),
        rds=SimpleNamespace(describe_db_proxies=lambda **kwargs: {"DBProxies": []}),
    )
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="prior-bout add-ons"):
        await orchestrator._discover_orphaned_addons(discovery_clients, "sg-rds")
    assert discovery_arguments["IncludePlannedDeletion"] is True

    aurora_config = replace(
        config,
        competitor_id="aurora_serverless_v2",
        competitor_target_id="aurora-source",
        competitor_resource_id="cluster-RESOURCE",
        competitor_direct_host="aurora-direct.test",
        competitor_credential_sha256="a" * 64,
        competitor_master_secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora-master"
        ),
    )
    aurora = LiveConnectionSpikeSetupOrchestrator(
        aurora_config,
        journal=Journal(),
        fence=Fence(),
        fresh_lakebase_host=lambda: _value("lakebase-pooled.test"),
        session_factory=SessionFactory(),
    )
    assert aurora_config.proxy_registration == {"DBClusterIdentifiers": ["aurora-source"]}
    assert aurora_config.competitor_credential_id == "aurora"
    assert aurora_config.proxy_secret_arn == aurora_config.aurora_proxy_secret_arn
    aurora_targets = (
        {
            "Type": "RDS_INSTANCE",
            "TrackedClusterId": "aurora-source",
            "RdsResourceId": "db-WRITER",
            "TargetHealth": {"State": "AVAILABLE"},
        },
        {
            "Type": "RDS_INSTANCE",
            "TrackedClusterId": "aurora-source",
            "RdsResourceId": "db-READER",
        },
        {
            "Type": "TRACKED_CLUSTER",
            "RdsResourceId": "aurora-source",
        },
    )
    assert aurora._proxy_targets_match(aurora_targets)
    assert aurora._proxy_targets_available(aurora_targets)
    assert not aurora._proxy_targets_match(
        (*aurora_targets, {"Type": "TRACKED_CLUSTER", "RdsResourceId": "foreign-cluster"})
    )

    class AuroraRds:
        def describe_db_clusters(self, **kwargs):
            assert kwargs == {"DBClusterIdentifier": "aurora-source"}
            return {
                "DBClusters": [
                    {
                        "DBClusterIdentifier": "aurora-source",
                        "DbClusterResourceId": "cluster-RESOURCE",
                        "Endpoint": "aurora-direct.test",
                        "Status": "available",
                        "DBSubnetGroup": "aurora-subnets",
                        "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-aurora"}],
                    }
                ]
            }

        def describe_db_subnet_groups(self, **kwargs):
            assert kwargs == {"DBSubnetGroupName": "aurora-subnets"}
            return {"DBSubnetGroups": [{"VpcId": "vpc-sealed"}]}

        def register_db_proxy_targets(self, **kwargs):
            assert kwargs == {
                "DBProxyName": "aurora-proxy",
                "DBClusterIdentifiers": ["aurora-source"],
            }
            return {}

    source = await aurora._read_competitor_source(SimpleNamespace(rds=AuroraRds()))
    assert source.direct_host == "aurora-direct.test"
    assert source.security_group_ids == ("sg-aurora",)
    registration = await aurora._register_proxy_target(
        SimpleNamespace(rds=AuroraRds()),
        SimpleNamespace(names=SimpleNamespace(proxy_name="aurora-proxy")),
        ResourceSpec(14, "proxy_target", "aurora-target"),
    )
    assert registration.provider_id == "cluster-RESOURCE"


def test_proxy_secret_policy_grants_only_exact_secret_without_stage_context() -> None:
    secret_arn = f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:bout-proxy"

    assert LiveConnectionSpikeSetupOrchestrator._proxy_secret_policy(secret_arn) == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "secretsmanager:GetSecretValue",
                "Resource": secret_arn,
            }
        ],
    }


def test_iam_policy_comparison_ignores_only_semantically_irrelevant_array_order() -> None:
    first = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue"],
                "Resource": ["arn:secret:aurora", "arn:secret:rds"],
            }
        ],
    }
    reordered = {
        "Statement": [
            {
                "Resource": ["arn:secret:rds", "arn:secret:aurora"],
                "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
                "Effect": "Allow",
            }
        ],
        "Version": "2012-10-17",
    }
    broadened = {
        **reordered,
        "Statement": [
            {
                **reordered["Statement"][0],
                "Action": [
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                    "secretsmanager:PutSecretValue",
                ],
            }
        ],
    }

    canonical = LiveConnectionSpikeSetupOrchestrator._canonical_policy
    assert canonical(first) == canonical(reordered)
    assert canonical(first) != canonical(broadened)


@pytest.mark.parametrize(
    ("policy_kind", "accepted"),
    (("legacy", True), ("extra_action", False)),
)
async def test_proxy_policy_cleanup_accepts_only_known_legacy_policy(
    policy_kind: str, accepted: bool
) -> None:
    secret_arn = f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:bout-proxy"
    action: object = "secretsmanager:GetSecretValue"
    condition: dict[str, object] = {"StringEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}}
    if policy_kind == "extra_action":
        action = ["secretsmanager:GetSecretValue", "secretsmanager:PutSecretValue"]
        condition = {}
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": action,
                "Resource": secret_arn,
                **({"Condition": condition} if condition else {}),
            }
        ],
    }
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)

    class Iam:
        def get_role_policy(self, **kwargs):
            assert kwargs == {"RoleName": "owned-role", "PolicyName": "owned-policy"}
            return {"PolicyDocument": policy}

    resources = SimpleNamespace(
        names=SimpleNamespace(proxy_role_name="owned-role", proxy_policy_name="owned-policy"),
        proxy_role_arn=f"arn:aws:iam::{ACCOUNT}:role/owned-role",
        secret_arn=secret_arn,
    )
    inspection = orchestrator._inspect_proxy_policy(
        SimpleNamespace(iam=Iam()),
        resources,
        ResourceSpec(5, "proxy_iam_policy", "owned-policy"),
        f"arn:aws:iam::{ACCOUNT}:role/owned-role:policy/owned-policy",
    )
    if accepted:
        assert await inspection is not None
    else:
        with pytest.raises(
            ConnectionSpikeLiveConfigurationError, match="Proxy secret policy changed"
        ):
            await inspection


async def test_aurora_pending_proxy_capacity_is_woken_before_strict_topology_check() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="aurora_serverless_v2",
        competitor_target_id="aurora-source",
        competitor_resource_id="cluster-RESOURCE",
        competitor_credential_id="aurora",
        competitor_direct_host="aurora-direct.test",
        competitor_credential_sha256="a" * 64,
        poll_interval_seconds=0.5,
    )
    orchestrator._monotonic_ns = lambda: 1_000_000_000
    order: list[str] = []
    target_health = [
        {"State": "UNAVAILABLE", "Reason": "PENDING_PROXY_CAPACITY"},
        {"State": "AVAILABLE"},
    ]

    async def unexpected_sleep(_seconds):
        raise AssertionError("expected Aurora target states must not poll again")

    orchestrator._sleep = unexpected_sleep

    class Rds:
        def describe_db_proxies(self, **kwargs):
            assert kwargs == {"DBProxyName": "aurora-proxy"}
            return {"DBProxies": [{"Status": "available"}]}

        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "aurora-proxy"}
            health = target_health.pop(0)
            order.append(
                "pending_capacity"
                if health.get("Reason") == "PENDING_PROXY_CAPACITY"
                else "available"
            )
            return {
                "Targets": [
                    {
                        "Type": "RDS_INSTANCE",
                        "TrackedClusterId": "aurora-source",
                        "RdsResourceId": "db-WRITER",
                        "TargetHealth": health,
                    },
                    {
                        "Type": "RDS_INSTANCE",
                        "TrackedClusterId": "aurora-source",
                        "RdsResourceId": "db-READER",
                    },
                    {
                        "Type": "TRACKED_CLUSTER",
                        "RdsResourceId": "aurora-source",
                    },
                ]
            }

    async def verify_journaled_resources(*args):
        del args

    async def runner_action(ssm, **kwargs):
        del ssm
        assert kwargs["lane_id"] == "aurora"
        assert kwargs["credential_host"] == "aurora-direct.test"
        endpoint = kwargs["endpoint_host"]
        assert endpoint in {"aurora-direct.test", "dynamic-proxy.test"}
        order.append(
            "direct_transaction" if endpoint == "aurora-direct.test" else "proxy_transaction"
        )

    async def verify_topology(clients, resources):
        del clients, resources
        order.append("strict_topology")

    orchestrator._verify_journaled_resources = verify_journaled_resources
    orchestrator._runner_action = runner_action
    orchestrator._verify_proxy_topology = verify_topology

    class Gate:
        async def wait(self):
            return None

    stop = await orchestrator._setup_competitor(
        "bout-aurora-wake",
        CreationScope("bout-aurora-wake", 7, "b" * 64),
        SimpleNamespace(rds=Rds(), ssm=SimpleNamespace()),
        SimpleNamespace(),
        (),
        SimpleNamespace(
            names=SimpleNamespace(proxy_name="aurora-proxy"),
            proxy_endpoint="dynamic-proxy.test",
            secret_arn="aurora-secret",
        ),
        Gate(),
        [1_000_000_000],
        None,
    )

    assert order == [
        "pending_capacity",
        "direct_transaction",
        "available",
        "strict_topology",
    ]
    assert not target_health
    assert stop.endpoint_host == "dynamic-proxy.test"


async def test_cleanup_inspection_accepts_owned_aurora_provider_target_shape() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="aurora_serverless_v2",
        competitor_target_id="aurora-source",
        competitor_resource_id="cluster-RESOURCE",
    )

    class Rds:
        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "aurora-proxy"}
            return {
                "Targets": [
                    {
                        "Type": "RDS_INSTANCE",
                        "TrackedClusterId": "aurora-source",
                        "RdsResourceId": "aurora-writer",
                    },
                    {"Type": "TRACKED_CLUSTER", "RdsResourceId": "aurora-source"},
                ]
            }

    observed = await orchestrator._inspect_proxy_target(
        SimpleNamespace(rds=Rds()),
        SimpleNamespace(names=SimpleNamespace(proxy_name="aurora-proxy")),
        ResourceSpec(14, "proxy_target", "aurora-target"),
        "cluster-RESOURCE",
    )

    assert observed is not None
    assert observed.provider_id == "cluster-RESOURCE"


@pytest.mark.parametrize(
    ("method_name", "resource_kind"),
    (
        ("_inspect_proxy_target", "proxy_target"),
        ("_inspect_target_group", "proxy_target_group"),
    ),
)
@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("DBProxyNotFoundFault", True), ("AccessDenied", False)),
)
async def test_proxy_child_inspection_only_accepts_parent_not_found(
    method_name: str,
    resource_kind: str,
    error_code: str,
    expected_absent: bool,
) -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Rds:
        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "deleted-proxy"}
            raise ProviderError

        def describe_db_proxy_target_groups(self, **kwargs):
            assert kwargs == {"DBProxyName": "deleted-proxy"}
            raise ProviderError

    inspection = getattr(orchestrator, method_name)(
        SimpleNamespace(rds=Rds()),
        SimpleNamespace(names=SimpleNamespace(proxy_name="deleted-proxy")),
        ResourceSpec(14, resource_kind, "deleted-proxy-child"),
        "owned-provider-id",
    )
    if expected_absent:
        assert await inspection is None
    else:
        with pytest.raises(ProviderError):
            await inspection


async def test_cleanup_retry_accepts_an_already_reset_target_group_without_tags() -> None:
    """A completed reset is absence of our mutation, even after AWS drops child tags."""

    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        region="us-west-2",
        expected_account_id=ACCOUNT,
    )

    class Rds:
        def describe_db_proxy_target_groups(self, **kwargs):
            assert kwargs == {"DBProxyName": "owned-proxy"}
            return {
                "TargetGroups": [
                    {
                        "TargetGroupName": "default",
                        "TargetGroupArn": (
                            f"arn:aws:rds:us-west-2:{ACCOUNT}:target-group:prx-tg-owned"
                        ),
                        "ConnectionPoolConfig": {
                            "MaxConnectionsPercent": 100,
                            "MaxIdleConnectionsPercent": 50,
                            "ConnectionBorrowTimeout": 120,
                        },
                    }
                ]
            }

        def list_tags_for_resource(self, **kwargs):
            raise AssertionError(
                f"an already-reset target group must not require stale child tags: {kwargs}"
            )

    observed = await orchestrator._inspect_target_group(
        SimpleNamespace(rds=Rds()),
        SimpleNamespace(names=SimpleNamespace(proxy_name="owned-proxy")),
        ResourceSpec(
            2,
            "proxy_target_group",
            "owned-proxy-target-group",
            metadata={"tags": {"anti-demo-bout-id": "owned-bout"}},
        ),
        "owned-proxy:default",
    )

    assert observed is None


@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("InvalidSecurityGroupRuleId.NotFound", True), ("AccessDenied", False)),
)
async def test_security_rule_inspection_only_accepts_exact_rule_not_found(
    error_code: str, expected_absent: bool
) -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(runner_security_group_id="sg-runner")
    resources = SimpleNamespace(
        proxy_security_group_id="sg-proxy",
        rds_security_group_id="sg-rds",
    )

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Ec2:
        def describe_security_group_rules(self, **kwargs):
            assert kwargs == {"SecurityGroupRuleIds": ["sgr-owned"]}
            raise ProviderError

        def unused(self, **kwargs):
            raise AssertionError(kwargs)

        authorize_security_group_ingress = unused
        authorize_security_group_egress = unused
        revoke_security_group_ingress = unused
        revoke_security_group_egress = unused

    adapter = orchestrator._security_rule_adapter(
        SimpleNamespace(ec2=Ec2()), resources, "rds_ingress"
    )
    inspection = adapter.inspect(
        ResourceSpec(11, "rds_ingress", "owned-rds-ingress"),
        provider_id="sgr-owned",
    )
    if expected_absent:
        assert await inspection is None
    else:
        with pytest.raises(ProviderError):
            await inspection


@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("InvalidGroup.NotFound", True), ("AccessDenied", False)),
)
async def test_default_egress_inspection_treats_only_missing_parent_as_absent(
    error_code: str, expected_absent: bool
) -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    resources = SimpleNamespace(proxy_security_group_id="sg-deleted")

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Ec2:
        def describe_security_groups(self, **kwargs):
            assert kwargs == {"GroupIds": ["sg-deleted"]}
            raise ProviderError

    adapter = orchestrator._default_egress_adapter(SimpleNamespace(ec2=Ec2()), resources)
    inspection = adapter.inspect(
        ResourceSpec(7, "proxy_default_egress", "owned-default-egress"),
        provider_id="sg-deleted:default-egress",
    )
    if expected_absent:
        assert await inspection is None
    else:
        with pytest.raises(ProviderError):
            await inspection


async def test_default_egress_inspection_accepts_only_scoped_replacement_rule() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    resources = SimpleNamespace(proxy_security_group_id="sg-proxy")

    class Ec2:
        def describe_security_groups(self, **kwargs):
            assert kwargs == {"GroupIds": ["sg-proxy"]}
            return {
                "SecurityGroups": [
                    {
                        "IpPermissionsEgress": [
                            {
                                "IpProtocol": "tcp",
                                "FromPort": 5432,
                                "ToPort": 5432,
                                "UserIdGroupPairs": [
                                    {
                                        "GroupId": "sg-database",
                                        "Description": "owned-proxy-egress",
                                    }
                                ],
                            }
                        ]
                    }
                ]
            }

    adapter = orchestrator._default_egress_adapter(SimpleNamespace(ec2=Ec2()), resources)
    observed = await adapter.inspect(
        ResourceSpec(2, "proxy_default_egress", "owned-default-egress"),
        provider_id="sg-proxy:default-egress",
    )

    assert observed is not None
    assert observed.provider_id == "sg-proxy:default-egress"


async def test_default_egress_inspection_rejects_restored_world_egress() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    resources = SimpleNamespace(proxy_security_group_id="sg-proxy")

    class Ec2:
        def describe_security_groups(self, **kwargs):
            assert kwargs == {"GroupIds": ["sg-proxy"]}
            return {
                "SecurityGroups": [
                    {
                        "IpPermissionsEgress": [
                            {
                                "IpProtocol": "-1",
                                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            }
                        ]
                    }
                ]
            }

    adapter = orchestrator._default_egress_adapter(SimpleNamespace(ec2=Ec2()), resources)
    observed = await adapter.inspect(
        ResourceSpec(2, "proxy_default_egress", "owned-default-egress"),
        provider_id="sg-proxy:default-egress",
    )

    assert observed is None


@pytest.mark.parametrize(
    ("error_code", "expected_absent"),
    (("DBProxyNotFoundFault", True), ("AccessDenied", False)),
)
async def test_proxy_inspection_only_accepts_not_found_tag_lookup_race(
    error_code: str, expected_absent: bool
) -> None:
    proxy_arn = f"arn:aws:rds:us-west-2:{ACCOUNT}:db-proxy:prx-owned"
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(region="us-west-2", expected_account_id=ACCOUNT)

    class ProviderError(Exception):
        response = {"Error": {"Code": error_code}}

    class Rds:
        def describe_db_proxies(self, **kwargs):
            assert kwargs == {"DBProxyName": "owned-proxy"}
            return {
                "DBProxies": [
                    {
                        "DBProxyName": "owned-proxy",
                        "DBProxyArn": proxy_arn,
                        "Status": "deleting",
                    }
                ]
            }

        def list_tags_for_resource(self, **kwargs):
            assert kwargs == {"ResourceName": proxy_arn}
            raise ProviderError

    inspection = orchestrator._inspect_proxy(
        SimpleNamespace(rds=Rds()),
        ResourceSpec(12, "rds_proxy", "owned-proxy"),
        proxy_arn,
    )
    if expected_absent:
        assert await inspection is None

        polls = 0

        class DelayedRds:
            def delete_db_proxy(self, **kwargs):
                assert kwargs == {"DBProxyName": "owned-proxy"}

            def describe_db_proxies(self, **kwargs):
                nonlocal polls
                assert kwargs == {"DBProxyName": "owned-proxy"}
                polls += 1
                # Present for 241 polls (status "deleting"), then absent forever.
                if polls > 241:
                    raise ProviderError
                return {
                    "DBProxies": [
                        {
                            "DBProxyName": "owned-proxy",
                            "DBProxyArn": proxy_arn,
                            "Status": "deleting",
                        }
                    ]
                }

            def list_tags_for_resource(self, **kwargs):
                assert kwargs == {"ResourceName": proxy_arn}
                return {"TagList": []}

        async def no_sleep(_seconds):
            return None

        delayed = object.__new__(LiveConnectionSpikeSetupOrchestrator)
        delayed.config = SimpleNamespace(
            region="us-west-2",
            expected_account_id=ACCOUNT,
            poll_interval_seconds=0.5,
        )
        delayed._sleep = no_sleep
        await delayed._delete_proxy(
            SimpleNamespace(rds=DelayedRds()),
            delayed._observation(
                ResourceSpec(
                    12,
                    "rds_proxy",
                    "owned-proxy",
                    metadata={"tags": {}},
                ),
                proxy_arn,
            ),
        )
        # Swarm defect #5: one NotFound is unknown, not clean. The loop returns
        # only after PROXY_DELETE_ABSENCE_CONFIRMATIONS consecutive absences, so it
        # keeps polling past the first NotFound (poll 242) until the count is met.
        assert polls == 241 + PROXY_DELETE_ABSENCE_CONFIRMATIONS
    else:
        with pytest.raises(ProviderError):
            await inspection


async def test_wait_proxy_available_fails_fast_with_sanitized_auth_error() -> None:
    orchestrator = object.__new__(LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="rds_postgres",
        competitor_target_id="rds-source",
        competitor_resource_id="db-RESOURCE",
        poll_interval_seconds=0.5,
    )

    async def unexpected_sleep(_seconds):
        raise AssertionError("AUTH_FAILURE must not poll again")

    orchestrator._sleep = unexpected_sleep

    class Rds:
        def describe_db_proxies(self, **kwargs):
            assert kwargs == {"DBProxyName": "rds-proxy"}
            return {"DBProxies": [{"Status": "available"}]}

        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "rds-proxy"}
            return {
                "Targets": [
                    {
                        "Type": "RDS_INSTANCE",
                        "RdsResourceId": "db-RESOURCE",
                        "TargetHealth": {
                            "State": "UNAVAILABLE",
                            "Reason": "AUTH_FAILURE",
                            "Description": "Proxy leaked-secret diagnostic must stay hidden",
                        },
                    }
                ]
            }

    with pytest.raises(
        ConnectionSpikeLiveOperationError,
        match="^Round 5 RDS Proxy target credential registration failed$",
    ) as raised:
        await orchestrator._wait_proxy_available(
            SimpleNamespace(rds=Rds()),
            SimpleNamespace(names=SimpleNamespace(proxy_name="rds-proxy")),
        )
    assert "leaked-secret" not in str(raised.value)


def _refusal(output: str) -> str:
    """The message `_validate_setup_output` raises for a runner that refused."""

    with pytest.raises(ConnectionSpikeLiveOperationError) as raised:
        LiveConnectionSpikeSetupOrchestrator._validate_setup_output(
            output,
            bout_id="b" * 32,
            lane_id="competitor",
            action="verify",
            nonce="n" * 64,
        )
    return str(raised.value)


def test_setup_output_failure_repeats_the_runner_refusal_token() -> None:
    """A refused runner must be quoted, not summarised into one useless class.

    This is the 2026-08-24 failure exactly: the runner printed
    `RUNNER_ERROR:baseline_auth_hash_invalid`, settled, and released its flock,
    and the app turned all of that into a bare
    `ConnectionSpikeLiveOperationError`. Two bouts died that way, and the word
    the runner had already said was recovered only from SSM afterwards.
    """

    message = _refusal(
        "RUNNER_ERROR:baseline_auth_hash_invalid\n"
        f"SETUP_SETTLED:{'n' * 64}\n"
        f"RUNNER_FLOCK_RELEASED:{'b' * 32}\n"
    )
    assert "baseline_auth_hash_invalid" in message


def test_setup_output_and_operator_log_preserve_only_the_typed_deadline_category() -> None:
    token = "setup_verify_deadline_state_none_attempts_7_elapsed_100s"
    output = (
        f"RUNNER_ERROR:{token}\n"
        "provider said host=private.example.test password=must-not-escape\n"
        f"SETUP_SETTLED:{'n' * 64}\n"
        f"RUNNER_FLOCK_RELEASED:{'b' * 32}\n"
    )

    with pytest.raises(ConnectionSpikeLiveOperationError) as raised:
        LiveConnectionSpikeSetupOrchestrator._validate_setup_output(
            output,
            bout_id="b" * 32,
            lane_id="aurora",
            action="verify",
            nonce="n" * 64,
        )

    diagnosis = operator_diagnosis(raised.value)
    assert token in diagnosis
    assert "private.example.test" not in diagnosis
    assert "must-not-escape" not in diagnosis


def test_setup_output_failure_bounds_what_it_repeats() -> None:
    """The runner chooses the word; it does not choose how much of it we print.

    The refusal line is remote text on its way to a log, so anything that is
    not one short lowercase identifier is dropped back to the plain sentence.
    A `RUNNER_ERROR:` line carrying a host, an ARN or a password must not
    become the thing this improvement prints.
    """

    leaky = _refusal(
        "RUNNER_ERROR:failed password=hunter2 host=db.internal.example.com\n"
        f"SETUP_SETTLED:{'n' * 64}\n"
    )
    assert "hunter2" not in leaky
    assert "db.internal.example.com" not in leaky
    assert leaky == "Round 5 setup runner did not return exact sanitized evidence"

    silent = _refusal(f"SETUP_SETTLED:{'n' * 64}\n")
    assert silent == "Round 5 setup runner did not return exact sanitized evidence"


def test_setup_output_still_accepts_the_exact_sealed_evidence() -> None:
    """The quoting change must not widen what counts as a verified setup."""

    nonce, bout = "n" * 64, "b" * 32
    receipt = {
        "protocol": "connection-spike-setup-v1",
        "action": "verify",
        "bout_id": bout,
        "lane_id": "competitor",
        "nonce": nonce,
        "status": "verified",
    }
    LiveConnectionSpikeSetupOrchestrator._validate_setup_output(
        f"SETUP_RESULT:{json.dumps(receipt, separators=(',', ':'))}\n"
        f"SETUP_SETTLED:{nonce}\n"
        f"RUNNER_FLOCK_RELEASED:{bout}\n",
        bout_id=bout,
        lane_id="competitor",
        action="verify",
        nonce=nonce,
    )


async def _value(value):
    return value
