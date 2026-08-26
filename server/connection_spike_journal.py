"""Fenced, secret-free creation journal orchestration for Round 5.

This module deliberately contains no cloud or database client.  Live integrations
provide a durable :class:`CreationJournalStore`, a fence guard, and one resource
adapter per resource kind.  In particular, ``commit()`` must not return until the
event is durable: the coordinator relies on that boundary before every provider
mutation.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

COORDINATION_SCHEMA = "anti_demo_coordination"
ROUND5_CREATION_JOURNAL_TABLE = f"{COORDINATION_SCHEMA}.round5_creation_journal"

_SHA256 = re.compile(r"[0-9a-f]{64}")
_PUBLIC_ERROR_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")
_SENSITIVE_KEY_PARTS = {
    "accesskey",
    "authorization",
    "cookie",
    "credential",
    "password",
    "passwd",
    "privatekey",
    "secret",
    "sessiontoken",
}
_SAFE_IDENTITY_SUFFIXES = ("arn", "checksum", "digest", "id", "name", "ref", "sha256")

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]


class JournalContractError(ValueError):
    """A journal value cannot be represented by the safe persistence contract."""


class JournalMutationError(RuntimeError):
    """A provider mutation failed after its intent was durably committed."""


class JournalRefusalError(RuntimeError):
    """A mutation was refused because exact identity or ownership was not proven."""


class LifecycleState(StrEnum):
    CREATE_INTENT = "create_intent"
    CREATED = "created"
    CREATE_FAILED = "create_failed"
    DELETE_INTENT = "delete_intent"
    DELETED = "deleted"
    DELETE_FAILED = "delete_failed"
    REFUSED = "refused"


def _utc(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise JournalContractError(f"{field_name} must be timezone-aware")


def _sha256(value: str, field_name: str) -> None:
    if _SHA256.fullmatch(value) is None:
        raise JournalContractError(f"{field_name} must be a lowercase SHA-256 digest")


def _normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.casefold())


def validate_secret_free_metadata(value: Mapping[str, object]) -> dict[str, JsonValue]:
    """Copy JSON metadata while rejecting fields that could persist credentials."""

    def safe(item: object, path: str) -> JsonValue:
        if isinstance(item, float) and not math.isfinite(item):
            raise JournalContractError(f"metadata value at {path} must be finite")
        if item is None or isinstance(item, str | bool | int | float):
            return item
        if isinstance(item, Mapping):
            result: dict[str, JsonValue] = {}
            for raw_key, child in item.items():
                if not isinstance(raw_key, str) or not raw_key:
                    raise JournalContractError(f"metadata key at {path} must be non-empty text")
                normalized = _normalized_key(raw_key)
                is_sensitive = any(part in normalized for part in _SENSITIVE_KEY_PARTS)
                if is_sensitive and not normalized.endswith(_SAFE_IDENTITY_SUFFIXES):
                    raise JournalContractError(
                        f"metadata field {path}.{raw_key} is not secret-free"
                    )
                result[raw_key] = safe(child, f"{path}.{raw_key}")
            return result
        if isinstance(item, list | tuple):
            return [safe(child, f"{path}[]") for child in item]
        raise JournalContractError(f"metadata value at {path} is not JSON-compatible")

    copied = safe(value, "metadata")
    assert isinstance(copied, dict)
    return copied


@dataclass(frozen=True)
class CreationScope:
    bout_id: str
    fencing_token: int
    runtime_seal_sha256: str

    def __post_init__(self) -> None:
        if not self.bout_id.strip():
            raise JournalContractError("bout_id is required")
        if self.fencing_token < 1:
            raise JournalContractError("fencing_token must be positive")
        _sha256(self.runtime_seal_sha256, "runtime_seal_sha256")


@dataclass(frozen=True)
class ResourceSpec:
    """Stable resource identity and the exact ownership metadata to journal."""

    ordinal: int
    resource_kind: str
    deterministic_name: str | None = None
    client_token: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.ordinal < 1:
            raise JournalContractError("resource ordinal must be positive")
        if not self.resource_kind.strip():
            raise JournalContractError("resource_kind is required")
        if not (
            (self.deterministic_name is not None and self.deterministic_name.strip())
            or (self.client_token is not None and self.client_token.strip())
        ):
            raise JournalContractError("a deterministic name or client token is required")
        object.__setattr__(self, "metadata", validate_secret_free_metadata(self.metadata))


@dataclass(frozen=True)
class ResourceObservation:
    """Provider evidence used for reconciliation and exact ownership checks."""

    resource_kind: str
    provider_id: str
    deterministic_name: str | None = None
    client_token: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.resource_kind.strip() or not self.provider_id.strip():
            raise JournalContractError("observed resource kind and provider id are required")
        object.__setattr__(self, "metadata", validate_secret_free_metadata(self.metadata))


@dataclass(frozen=True)
class JournalEvent:
    """One append-only row in ``anti_demo_coordination.round5_creation_journal``."""

    bout_id: str
    fencing_token: int
    ordinal: int
    resource_kind: str
    deterministic_name: str | None
    client_token: str | None
    provider_id: str | None
    lifecycle_state: LifecycleState
    metadata: Mapping[str, object]
    runtime_seal_sha256: str
    intent_at: datetime
    occurred_at: datetime
    completed_at: datetime | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        CreationScope(self.bout_id, self.fencing_token, self.runtime_seal_sha256)
        ResourceSpec(
            ordinal=self.ordinal,
            resource_kind=self.resource_kind,
            deterministic_name=self.deterministic_name,
            client_token=self.client_token,
            metadata=self.metadata,
        )
        _utc(self.intent_at, "intent_at")
        _utc(self.occurred_at, "occurred_at")
        if self.occurred_at < self.intent_at:
            raise JournalContractError("occurred_at cannot precede intent_at")
        if self.completed_at is not None:
            _utc(self.completed_at, "completed_at")
            if self.completed_at < self.intent_at:
                raise JournalContractError("completed_at cannot precede intent_at")
        intent_state = self.lifecycle_state in {
            LifecycleState.CREATE_INTENT,
            LifecycleState.DELETE_INTENT,
        }
        if intent_state != (self.completed_at is None):
            raise JournalContractError("only intent events omit completed_at")
        if (
            self.lifecycle_state
            in {
                LifecycleState.CREATED,
                LifecycleState.DELETE_INTENT,
                LifecycleState.DELETE_FAILED,
            }
            and not self.provider_id
        ):
            raise JournalContractError(f"{self.lifecycle_state} requires provider_id")
        if self.error is not None and _PUBLIC_ERROR_CODE.fullmatch(self.error) is None:
            raise JournalContractError("error must be a secret-free public error code")
        object.__setattr__(self, "metadata", validate_secret_free_metadata(self.metadata))

    @classmethod
    def creation_intent(
        cls, scope: CreationScope, spec: ResourceSpec, *, now: datetime
    ) -> JournalEvent:
        return cls(
            bout_id=scope.bout_id,
            fencing_token=scope.fencing_token,
            ordinal=spec.ordinal,
            resource_kind=spec.resource_kind,
            deterministic_name=spec.deterministic_name,
            client_token=spec.client_token,
            provider_id=None,
            lifecycle_state=LifecycleState.CREATE_INTENT,
            metadata=spec.metadata,
            runtime_seal_sha256=scope.runtime_seal_sha256,
            intent_at=now,
            occurred_at=now,
        )


class CreationJournalStore(Protocol):
    """Durable append-only storage for ``ROUND5_CREATION_JOURNAL_TABLE``."""

    async def commit(
        self,
        event: JournalEvent,
        *,
        authority_scope: CreationScope | None = None,
    ) -> None:
        """Durably commit one event under the current lease authority."""
        ...

    async def events(self, scope: CreationScope) -> Sequence[JournalEvent]:
        """Return the scope's events in durable commit order, oldest first."""
        ...

    async def scopes(self, bout_id: str) -> Sequence[CreationScope]:
        """Return every persisted ownership scope for one bout."""
        ...


class FenceGuard(Protocol):
    async def assert_current(self, scope: CreationScope) -> None: ...


class ResourceAdapter(Protocol):
    """Live provider boundary; create must honor the spec's stable name/token."""

    async def create(self, spec: ResourceSpec) -> ResourceObservation: ...

    async def inspect(
        self, spec: ResourceSpec, *, provider_id: str | None
    ) -> ResourceObservation | None: ...

    async def delete(self, resource: ResourceObservation) -> None: ...


@dataclass(frozen=True)
class JournalReceipt:
    bout_id: str
    fencing_token: int
    runtime_seal_sha256: str
    creation_sha256: str
    resource_count: int
    issued_at: datetime

    def __post_init__(self) -> None:
        CreationScope(self.bout_id, self.fencing_token, self.runtime_seal_sha256)
        _sha256(self.creation_sha256, "creation_sha256")
        if self.resource_count < 1:
            raise JournalContractError("receipt must bind at least one resource")
        _utc(self.issued_at, "issued_at")


@dataclass(frozen=True)
class CleanupRefusal:
    ordinal: int
    resource_kind: str
    error: str


@dataclass(frozen=True)
class CleanupReport:
    deleted_ordinals: tuple[int, ...]
    already_absent_ordinals: tuple[int, ...]
    refusal: CleanupRefusal | None = None

    @property
    def complete(self) -> bool:
        return self.refusal is None


def _spec(event: JournalEvent) -> ResourceSpec:
    return ResourceSpec(
        ordinal=event.ordinal,
        resource_kind=event.resource_kind,
        deterministic_name=event.deterministic_name,
        client_token=event.client_token,
        metadata=event.metadata,
    )


def exact_ownership_error(
    spec: ResourceSpec,
    observed: ResourceObservation,
    *,
    expected_provider_id: str | None = None,
) -> str | None:
    """Return a public refusal code unless all recorded identity is exact."""

    if observed.resource_kind != spec.resource_kind:
        return "resource_kind_mismatch"
    if expected_provider_id is not None and observed.provider_id != expected_provider_id:
        return "provider_id_mismatch"
    if spec.deterministic_name is not None and (
        observed.deterministic_name != spec.deterministic_name
    ):
        return "deterministic_name_mismatch"
    if spec.client_token is not None and observed.client_token != spec.client_token:
        return "client_token_mismatch"
    if dict(observed.metadata) != dict(spec.metadata):
        return "ownership_metadata_mismatch"
    return None


def _receipt_payload(scope: CreationScope, created: Sequence[JournalEvent]) -> bytes:
    resources = []
    for event in sorted(created, key=lambda item: item.ordinal):
        resources.append(
            {
                "ordinal": event.ordinal,
                "resource_kind": event.resource_kind,
                "deterministic_name": event.deterministic_name,
                "client_token": event.client_token,
                "provider_id": event.provider_id,
                "metadata": event.metadata,
                "intent_at": event.intent_at.astimezone(UTC).isoformat(),
                "completed_at": (
                    event.completed_at.astimezone(UTC).isoformat()
                    if event.completed_at is not None
                    else None
                ),
            }
        )
    payload = {
        "bout_id": scope.bout_id,
        "fencing_token": scope.fencing_token,
        "runtime_seal_sha256": scope.runtime_seal_sha256,
        "resources": resources,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _created_events(events: Sequence[JournalEvent]) -> tuple[JournalEvent, ...]:
    intents: set[int] = set()
    created: dict[int, JournalEvent] = {}
    for event in events:
        if event.lifecycle_state is LifecycleState.CREATE_INTENT:
            if event.ordinal in intents:
                raise JournalContractError("journal has duplicate creation intents")
            intents.add(event.ordinal)
        if event.lifecycle_state is LifecycleState.CREATED:
            if event.ordinal in created:
                raise JournalContractError("journal has duplicate created completions")
            created[event.ordinal] = event
    if intents != set(created):
        raise JournalContractError("creation journal is incomplete and cannot be sealed")
    return tuple(created[ordinal] for ordinal in sorted(created))


def build_receipt(
    scope: CreationScope, events: Sequence[JournalEvent], *, issued_at: datetime
) -> JournalReceipt:
    created = _created_events(events)
    if not created:
        raise JournalContractError("cannot seal an empty creation journal")
    if any(
        event.bout_id != scope.bout_id
        or event.fencing_token != scope.fencing_token
        or event.runtime_seal_sha256 != scope.runtime_seal_sha256
        for event in created
    ):
        raise JournalContractError("creation event differs from the receipt scope")
    return JournalReceipt(
        bout_id=scope.bout_id,
        fencing_token=scope.fencing_token,
        runtime_seal_sha256=scope.runtime_seal_sha256,
        creation_sha256=hashlib.sha256(_receipt_payload(scope, created)).hexdigest(),
        resource_count=len(created),
        issued_at=issued_at,
    )


def verify_receipt(
    scope: CreationScope, events: Sequence[JournalEvent], receipt: JournalReceipt
) -> tuple[JournalEvent, ...]:
    if (
        receipt.bout_id != scope.bout_id
        or receipt.fencing_token != scope.fencing_token
        or receipt.runtime_seal_sha256 != scope.runtime_seal_sha256
    ):
        raise JournalRefusalError("runtime_seal_receipt_scope_mismatch")
    if any(
        event.bout_id != scope.bout_id
        or event.fencing_token != scope.fencing_token
        or event.runtime_seal_sha256 != scope.runtime_seal_sha256
        for event in events
    ):
        raise JournalRefusalError("runtime_seal_journal_scope_mismatch")
    try:
        created = _created_events(events)
    except JournalContractError as exc:
        raise JournalRefusalError("runtime_seal_receipt_journal_mismatch") from exc
    measured = hashlib.sha256(_receipt_payload(scope, created)).hexdigest()
    if len(created) != receipt.resource_count or measured != receipt.creation_sha256:
        raise JournalRefusalError("runtime_seal_receipt_hash_mismatch")
    return created


class Round5CreationCoordinator:
    """Pure sequencing for fenced creation, sealing, and reverse cleanup."""

    def __init__(
        self,
        *,
        journal: CreationJournalStore,
        fence: FenceGuard,
        adapters: Mapping[str, ResourceAdapter],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._journal = journal
        self._fence = fence
        self._adapters = dict(adapters)
        self._clock = clock or (lambda: datetime.now(UTC))

    def _adapter(self, resource_kind: str) -> ResourceAdapter:
        try:
            return self._adapters[resource_kind]
        except KeyError as exc:
            raise JournalContractError(
                f"no Round 5 creation adapter for resource kind {resource_kind!r}"
            ) from exc

    async def _commit(
        self,
        event: JournalEvent,
        authority_scope: CreationScope,
    ) -> None:
        await self._fence.assert_current(authority_scope)
        await self._journal.commit(event, authority_scope=authority_scope)

    async def create_resource(self, scope: CreationScope, spec: ResourceSpec) -> JournalEvent:
        adapter = self._adapter(spec.resource_kind)
        await self._fence.assert_current(scope)
        if any(event.ordinal == spec.ordinal for event in await self._journal.events(scope)):
            raise JournalRefusalError("resource_ordinal_already_journaled")
        intent = JournalEvent.creation_intent(scope, spec, now=self._clock())
        await self._commit(intent, scope)
        try:
            # Fence again after the durable write and immediately before mutation.
            await self._fence.assert_current(scope)
            observed = await adapter.create(spec)
            refusal = exact_ownership_error(spec, observed)
            if refusal is not None:
                refused_at = self._clock()
                event = replace(
                    intent,
                    provider_id=observed.provider_id,
                    lifecycle_state=LifecycleState.REFUSED,
                    occurred_at=refused_at,
                    completed_at=refused_at,
                    error=refusal,
                )
                await self._commit(event, scope)
                raise JournalRefusalError(refusal)
        except JournalRefusalError:
            raise
        except Exception as exc:
            failed_at = self._clock()
            await self._commit(
                replace(
                    intent,
                    lifecycle_state=LifecycleState.CREATE_FAILED,
                    occurred_at=failed_at,
                    completed_at=failed_at,
                    error="provider_create_failed",
                ),
                scope,
            )
            raise JournalMutationError("provider_create_failed") from exc
        completed_at = self._clock()
        completed = replace(
            intent,
            provider_id=observed.provider_id,
            lifecycle_state=LifecycleState.CREATED,
            occurred_at=completed_at,
            completed_at=completed_at,
        )
        await self._commit(completed, scope)
        return completed

    async def create_resources(
        self, scope: CreationScope, specs: Sequence[ResourceSpec]
    ) -> JournalReceipt:
        ordered = sorted(specs, key=lambda item: item.ordinal)
        if not ordered or len({item.ordinal for item in ordered}) != len(ordered):
            raise JournalContractError("resource ordinals must be non-empty and unique")
        for spec in ordered:
            await self.create_resource(scope, spec)
        return await self.seal(scope)

    async def seal(self, scope: CreationScope) -> JournalReceipt:
        await self._fence.assert_current(scope)
        return build_receipt(scope, await self._journal.events(scope), issued_at=self._clock())

    async def cleanup(
        self,
        scope: CreationScope,
        receipt: JournalReceipt,
        *,
        authority_scope: CreationScope | None = None,
    ) -> CleanupReport:
        authority = authority_scope or scope
        await self._fence.assert_current(authority)
        events = list(await self._journal.events(scope))
        created = verify_receipt(scope, events, receipt)
        return await self._cleanup_events(scope, authority, events, created)

    async def reconcile_incomplete(
        self,
        scope: CreationScope,
        *,
        authority_scope: CreationScope | None = None,
    ) -> CleanupReport:
        """Recover resources whose creation journal never reached a sealed receipt.

        A crash may leave only ``CREATE_INTENT`` even though the provider accepted
        the mutation.  Stable names/client tokens permit inspection, while the
        current fence, runtime seal, and exact ownership metadata authorize cleanup.
        """

        authority = authority_scope or scope
        await self._fence.assert_current(authority)
        events = list(await self._journal.events(scope))
        intents: dict[int, JournalEvent] = {}
        for event in events:
            if (
                event.bout_id != scope.bout_id
                or event.fencing_token != scope.fencing_token
                or event.runtime_seal_sha256 != scope.runtime_seal_sha256
            ):
                raise JournalRefusalError("runtime_seal_journal_scope_mismatch")
            if event.lifecycle_state is LifecycleState.CREATE_INTENT:
                if event.ordinal in intents:
                    raise JournalRefusalError("duplicate_creation_intent")
                intents[event.ordinal] = event
        return await self._cleanup_events(
            scope,
            authority,
            events,
            tuple(intents[ordinal] for ordinal in sorted(intents)),
        )

    async def _cleanup_events(
        self,
        scope: CreationScope,
        authority_scope: CreationScope,
        events: Sequence[JournalEvent],
        creations: Sequence[JournalEvent],
    ) -> CleanupReport:
        latest = {event.ordinal: event for event in events}
        provider_ids = {
            event.ordinal: event.provider_id for event in events if event.provider_id is not None
        }
        deleted: list[int] = []
        absent: list[int] = []

        for creation in reversed(creations):
            current = latest.get(creation.ordinal, creation)
            previously_deleted = current.lifecycle_state is LifecycleState.DELETED
            spec = _spec(creation)
            adapter = self._adapter(spec.resource_kind)
            expected_provider_id = provider_ids.get(creation.ordinal)
            try:
                observed = await adapter.inspect(spec, provider_id=expected_provider_id)
            except Exception:
                await self._fence.assert_current(authority_scope)
                refusal = await self._refuse(
                    creation,
                    "provider_inspection_failed",
                    provider_id=expected_provider_id,
                    authority_scope=authority_scope,
                )
                return CleanupReport(tuple(deleted), tuple(absent), refusal)
            if observed is None:
                if previously_deleted:
                    absent.append(creation.ordinal)
                    continue
                when = self._clock()
                await self._commit(
                    replace(
                        creation,
                        provider_id=expected_provider_id,
                        lifecycle_state=LifecycleState.DELETED,
                        occurred_at=when,
                        completed_at=when,
                        error=None,
                    ),
                    authority_scope,
                )
                absent.append(creation.ordinal)
                continue
            ownership_error = exact_ownership_error(
                spec, observed, expected_provider_id=expected_provider_id
            )
            if ownership_error is not None:
                refusal = await self._refuse(
                    creation,
                    ownership_error,
                    provider_id=observed.provider_id,
                    authority_scope=authority_scope,
                )
                return CleanupReport(tuple(deleted), tuple(absent), refusal)

            try:
                await self._fence.assert_current(authority_scope)
                intent_at = self._clock()
                delete_intent = replace(
                    creation,
                    provider_id=observed.provider_id,
                    lifecycle_state=LifecycleState.DELETE_INTENT,
                    occurred_at=intent_at,
                    intent_at=intent_at,
                    completed_at=None,
                    error=None,
                )
                await self._commit(delete_intent, authority_scope)
                await self._fence.assert_current(authority_scope)
                await adapter.delete(observed)
                remaining = await adapter.inspect(spec, provider_id=observed.provider_id)
                if remaining is not None:
                    raise JournalMutationError("provider_delete_unconfirmed")
            except Exception:
                failed_at = self._clock()
                await self._commit(
                    replace(
                        creation,
                        provider_id=observed.provider_id,
                        lifecycle_state=LifecycleState.DELETE_FAILED,
                        occurred_at=failed_at,
                        completed_at=failed_at,
                        error="provider_delete_failed",
                    ),
                    authority_scope,
                )
                refusal = CleanupRefusal(
                    creation.ordinal, creation.resource_kind, "provider_delete_failed"
                )
                return CleanupReport(tuple(deleted), tuple(absent), refusal)

            completed_at = self._clock()
            await self._commit(
                replace(
                    creation,
                    provider_id=observed.provider_id,
                    lifecycle_state=LifecycleState.DELETED,
                    occurred_at=completed_at,
                    completed_at=completed_at,
                    error=None,
                ),
                authority_scope,
            )
            deleted.append(creation.ordinal)

        return CleanupReport(tuple(deleted), tuple(absent))

    async def _refuse(
        self,
        event: JournalEvent,
        error: str,
        *,
        provider_id: str | None = None,
        authority_scope: CreationScope,
    ) -> CleanupRefusal:
        when = self._clock()
        await self._commit(
            replace(
                event,
                provider_id=provider_id,
                lifecycle_state=LifecycleState.REFUSED,
                occurred_at=when,
                completed_at=when,
                error=error,
            ),
            authority_scope,
        )
        return CleanupRefusal(event.ordinal, event.resource_kind, error)
