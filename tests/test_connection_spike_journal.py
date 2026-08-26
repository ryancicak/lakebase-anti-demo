from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from server.connection_spike_journal import (
    CreationScope,
    JournalContractError,
    JournalEvent,
    LifecycleState,
    ResourceObservation,
    ResourceSpec,
    Round5CreationCoordinator,
)


class Journal:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.rows = []
        self.authorities = []

    async def commit(self, event, *, authority_scope=None) -> None:
        self.trace.append(f"commit:{event.ordinal}:{event.lifecycle_state}")
        self.rows.append(event)
        self.authorities.append(authority_scope)

    async def events(self, scope):
        return [
            row
            for row in self.rows
            if row.bout_id == scope.bout_id and row.fencing_token == scope.fencing_token
        ]

    async def scopes(self, bout_id):
        return tuple(
            {
                CreationScope(row.bout_id, row.fencing_token, row.runtime_seal_sha256)
                for row in self.rows
                if row.bout_id == bout_id
            }
        )


class Fence:
    async def assert_current(self, _scope) -> None:
        return None


class Adapter:
    def __init__(self, kind: str, trace: list[str], metadata: dict[str, object]) -> None:
        self.kind = kind
        self.trace = trace
        self.metadata = metadata
        self.resource: ResourceObservation | None = None

    async def create(self, spec):
        self.trace.append(f"create:{spec.ordinal}")
        self.resource = ResourceObservation(
            resource_kind=self.kind,
            provider_id=f"provider-{spec.ordinal}",
            deterministic_name=spec.deterministic_name,
            client_token=spec.client_token,
            metadata=self.metadata,
        )
        return self.resource

    async def inspect(self, _spec, *, provider_id):
        assert provider_id is None or provider_id.startswith("provider-")
        return self.resource

    async def delete(self, resource):
        self.trace.append(f"delete:{resource.provider_id}")
        self.resource = None


def clock():
    value = datetime(2026, 8, 18, tzinfo=UTC)

    def tick():
        nonlocal value
        value += timedelta(milliseconds=1)
        return value

    return tick


@pytest.mark.asyncio
async def test_intents_precede_mutations_and_receipt_binds_reverse_cleanup() -> None:
    trace: list[str] = []
    journal = Journal(trace)
    first = Adapter("network", trace, {"owner": "bout-1"})
    second = Adapter("database", trace, {"owner": "bout-1"})
    coordinator = Round5CreationCoordinator(
        journal=journal,
        fence=Fence(),
        adapters={"network": first, "database": second},
        clock=clock(),
    )
    scope = CreationScope("bout-1", 9, "a" * 64)
    specs = [
        ResourceSpec(1, "network", deterministic_name="r5-network", metadata=first.metadata),
        ResourceSpec(2, "database", client_token="r5-database", metadata=second.metadata),
    ]

    receipt = await coordinator.create_resources(scope, specs)
    assert receipt.runtime_seal_sha256 == scope.runtime_seal_sha256
    assert trace.index("commit:1:create_intent") < trace.index("create:1")
    assert trace.index("commit:2:create_intent") < trace.index("create:2")

    report = await coordinator.cleanup(scope, receipt)
    assert report.complete and report.deleted_ordinals == (2, 1)
    assert trace.index("commit:2:delete_intent") < trace.index("delete:provider-2")
    assert trace.index("delete:provider-2") < trace.index("delete:provider-1")


@pytest.mark.asyncio
async def test_cleanup_refuses_wrong_owner_before_dependencies_and_rejects_secrets() -> None:
    trace: list[str] = []
    journal = Journal(trace)
    first = Adapter("network", trace, {"owner": "bout-1"})
    second = Adapter("database", trace, {"owner": "bout-1"})
    coordinator = Round5CreationCoordinator(
        journal=journal,
        fence=Fence(),
        adapters={"network": first, "database": second},
        clock=clock(),
    )
    scope = CreationScope("bout-1", 9, "a" * 64)
    await coordinator.create_resource(
        scope,
        ResourceSpec(1, "network", deterministic_name="network", metadata=first.metadata),
    )
    second_spec = ResourceSpec(
        2, "database", deterministic_name="database", metadata=second.metadata
    )
    # Crash window: intent is durable and the provider accepted the mutation,
    # but no CREATED completion or receipt was committed.
    await journal.commit(JournalEvent.creation_intent(scope, second_spec, now=clock()()))
    second.resource = ResourceObservation(
        resource_kind="database",
        provider_id="provider-2",
        deterministic_name="database",
        metadata={"owner": "somebody-else"},
    )

    report = await coordinator.reconcile_incomplete(scope)
    assert not report.complete
    assert report.refusal is not None and report.refusal.error == "ownership_metadata_mismatch"
    assert not any(item.startswith("delete:") for item in trace)
    assert journal.rows[-1].lifecycle_state is LifecycleState.REFUSED

    second.resource = ResourceObservation(
        resource_kind="database",
        provider_id="provider-2",
        deterministic_name="database",
        metadata=second.metadata,
    )
    fresh_authority = CreationScope("bout-1", 10, "a" * 64)
    recovered = await coordinator.reconcile_incomplete(
        scope,
        authority_scope=fresh_authority,
    )
    assert recovered.complete and recovered.deleted_ordinals == (2, 1)
    assert trace.index("delete:provider-2") < trace.index("delete:provider-1")
    assert journal.authorities[-1] == fresh_authority
    with pytest.raises(JournalContractError, match="not secret-free"):
        ResourceSpec(3, "secret", deterministic_name="bad", metadata={"password": "raw"})
