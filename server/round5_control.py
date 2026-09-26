"""Durable, versioned resident-runner control plane for Round 5."""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import logging
import re
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol

import psycopg

from .coordination import (
    COORDINATION_SCHEMA,
    CoordinationObjectsMissingError,
    read_coordination_objects,
)

logger = logging.getLogger(__name__)

ROUND5_CONTROL_PROTOCOL = "round5-resident-control-v3"
ROUND5_CONTROL_SCHEMA_VERSION = 3
ROUND5_CONTROL_OUTBOX_TABLE = f"{COORDINATION_SCHEMA}.round5_control_outbox_v3"
ROUND5_RUNNER_EVENT_TABLE = f"{COORDINATION_SCHEMA}.round5_runner_event_v3"
# The bout-execution deadline: a post-bell RELEASE/dispatch may legitimately take
# this long (cold Proxy build on the competitor lane, etc.).
ROUND5_RESIDENT_DEADLINE_SECONDS = 720.0
# The bounded budget for the ARM path only. Automatic backstage warm has already
# staged both resident pools (LiveConnectionSpikeEngine.warm ->
# stage_resident_generation) and benchmarked capacity before a slot reaches
# READY, so ARM against a READY warm slot is an O(1)-shaped fast rebind, not a
# cold per-bout PREPARED build. Capping ARM at this budget turns "can wait up to
# 720s" into a fast, bounded failure that says the ring was not actually warm,
# instead of blocking the operator on the full bout-execution deadline.
ROUND5_ARM_STAGE_DEADLINE_SECONDS = 45.0
# An abandoned ARM must not spend the normal 12-minute running-bout settlement
# deadline ahead of provider cleanup. CANCEL is durable before settlement is
# awaited, so this budget may defer observation without losing cleanup intent.
ROUND5_ABANDONED_ARM_SETTLEMENT_SECONDS = ROUND5_ARM_STAGE_DEADLINE_SECONDS
# A RELEASE is process-local permission layered over a durable intent. If the
# process that held that permission dies, no replacement process may infer the
# bell from the row alone. Tombstone such rows after the bout deadline so the
# 10 Hz publisher does not scan crashed-bell debt forever.
ROUND5_ORPHANED_RELEASE_TTL_SECONDS = ROUND5_RESIDENT_DEADLINE_SECONDS
ROUND5_ORPHAN_REAP_INTERVAL_SECONDS = 30.0

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$")


class Round5ResidentBindingChangedError(RuntimeError):
    """Readiness events for a resident job carry a different binding than expected.

    Raised by ``wait_agent_ready`` when the runner's readiness proof does not match
    the binding this generation staged.  During an ownership transition (a prior
    claim's staged resident is still draining), this is a settle-first/RETRYABLE
    condition, not a permanent baseline defect: the warm provider classifies it as
    retryable while resident-settlement debt exists, and only fails closed
    (``warm_baseline_unexpected``) when there is no claim/debt to explain it.
    """


def canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise ValueError("resident control payload is not canonical JSON") from exc


def canonical_request_sha256(request: Mapping[str, object]) -> str:
    value = dict(request)
    claimed = value.pop("prepared_request_digest", None)
    digest = hashlib.sha256(canonical_json(value)).hexdigest()
    if claimed != digest:
        raise ValueError("resident request digest does not match canonical bytes")
    return digest


def _safe_id(value: str, name: str) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{name} is not a safe resident-control identity")
    return value


def _digest(value: str, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} is not a lowercase SHA-256 digest")
    return value


class Round5ControlKind(StrEnum):
    PRELOAD = "preload"
    STAGE = "stage"
    RELEASE = "release"
    CANCEL = "cancel"


class ResidentLiveness(StrEnum):
    """Tri-state (plus ABSENT) resident liveness for idle READY keep-alive.

    The idle "Temporarily Unavailable" flicker came from collapsing these into one
    boolean: a STALE or ABSENT heartbeat (transient, off-path) was indistinguishable
    from an IDENTITY_CHANGED runner and demoted READY on the first miss. The contract
    is: only IDENTITY_CHANGED (an attested runner change) demotes; STALE/ABSENT are
    transient misses handled by the coordinator's strike budget (RetryableWarmError).
    """

    CURRENT = "current"
    STALE = "stale"
    IDENTITY_CHANGED = "identity_changed"
    ABSENT = "absent"


class Round5RunnerEventKind(StrEnum):
    AGENT_READY = "agent_ready"
    HEARTBEAT = "heartbeat"
    PREPARED = "prepared"
    RELEASE_OBSERVED = "release_observed"
    PROGRESS = "progress"
    RESULT = "result"
    FAILED = "failed"
    SETTLED = "settled"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class Round5ControlBinding:
    installation_id: str
    lane_id: str
    generation: int
    warm_attempt_token: str
    claim_id: str | None
    bout_id: str | None
    bell_id: str | None
    fence: int
    job_id: str
    runner_boot_id: str
    runner_process_boot_id: str
    runner_harness_sha256: str
    request_sha256: str

    def __post_init__(self) -> None:
        _safe_id(self.installation_id, "installation_id")
        if self.lane_id not in {"lakebase", "competitor"}:
            raise ValueError("resident control lane is invalid")
        if isinstance(self.generation, bool) or self.generation <= 0:
            raise ValueError("resident generation must be positive")
        _safe_id(self.warm_attempt_token, "warm_attempt_token")
        for value, name in (
            (self.claim_id, "claim_id"),
            (self.bout_id, "bout_id"),
            (self.bell_id, "bell_id"),
        ):
            if value is not None:
                _safe_id(value, name)
        if isinstance(self.fence, bool) or self.fence < 0:
            raise ValueError("resident fence cannot be negative")
        _digest(self.job_id, "job_id")
        _safe_id(self.runner_boot_id, "runner_boot_id")
        _safe_id(self.runner_process_boot_id, "runner_process_boot_id")
        _digest(self.runner_harness_sha256, "runner_harness_sha256")
        _digest(self.request_sha256, "request_sha256")

    @property
    def claim_bound(self) -> bool:
        return (
            self.claim_id is not None
            and self.bout_id is not None
            and self.bell_id is not None
            and self.fence > 0
        )

    def wire_value(self) -> dict[str, object]:
        return {
            "installation_id": self.installation_id,
            "lane_id": self.lane_id,
            "generation": self.generation,
            "warm_attempt_token": self.warm_attempt_token,
            "claim_id": self.claim_id,
            "bout_id": self.bout_id,
            "bell_id": self.bell_id,
            "fence": self.fence,
            "job_id": self.job_id,
            "runner_boot_id": self.runner_boot_id,
            "runner_process_boot_id": self.runner_process_boot_id,
            "runner_harness_sha256": self.runner_harness_sha256,
            "request_sha256": self.request_sha256,
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> Round5ControlBinding:
        generation = value.get("generation")
        fence = value.get("fence")
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or isinstance(fence, bool)
            or not isinstance(fence, int)
        ):
            raise ValueError("resident control numeric binding is invalid")
        return cls(
            installation_id=str(value.get("installation_id") or ""),
            lane_id=str(value.get("lane_id") or ""),
            generation=generation,
            warm_attempt_token=str(value.get("warm_attempt_token") or ""),
            claim_id=str(value["claim_id"]) if value.get("claim_id") else None,
            bout_id=str(value["bout_id"]) if value.get("bout_id") else None,
            bell_id=str(value["bell_id"]) if value.get("bell_id") else None,
            fence=fence,
            job_id=str(value.get("job_id") or ""),
            runner_boot_id=str(value.get("runner_boot_id") or ""),
            runner_process_boot_id=str(value.get("runner_process_boot_id") or ""),
            runner_harness_sha256=str(value.get("runner_harness_sha256") or ""),
            request_sha256=str(value.get("request_sha256") or ""),
        )


@dataclass(frozen=True, slots=True)
class Round5ControlEvent:
    event_id: str
    binding: Round5ControlBinding
    sequence: int
    kind: Round5ControlKind
    created_at: datetime
    payload: Mapping[str, object]
    protocol: str = ROUND5_CONTROL_PROTOCOL
    schema_version: int = ROUND5_CONTROL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.protocol != ROUND5_CONTROL_PROTOCOL:
            raise ValueError("resident control protocol is invalid")
        if self.schema_version != ROUND5_CONTROL_SCHEMA_VERSION:
            raise ValueError("resident control schema is invalid")
        _digest(self.event_id, "event_id")
        if isinstance(self.sequence, bool) or self.sequence <= 0:
            raise ValueError("resident control sequence must be positive")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("resident control timestamp must be timezone-aware")
        if self.kind != Round5ControlKind.PRELOAD and not self.binding.claim_bound:
            raise ValueError("stage, release, and cancel require a complete claim binding")
        if self.kind == Round5ControlKind.PRELOAD and (
            self.binding.claim_bound
            or self.binding.claim_id is not None
            or self.binding.bout_id is not None
            or self.binding.bell_id is not None
            or self.binding.fence != 0
        ):
            raise ValueError("preload must precede a bout claim")
        if self.kind in {Round5ControlKind.PRELOAD, Round5ControlKind.STAGE}:
            request = self.payload.get("request")
            if not isinstance(request, Mapping):
                raise ValueError("preload/stage requires canonical request bytes")
            if canonical_request_sha256(request) != self.binding.request_sha256:
                raise ValueError("resident stage request binding changed")
        expected = hashlib.sha256(canonical_json(self.identity_value())).hexdigest()
        if self.event_id != expected:
            raise ValueError("resident control event identity changed")

    @property
    def installation_id(self) -> str:
        return self.binding.installation_id

    @property
    def generation(self) -> int:
        return self.binding.generation

    @property
    def lane_id(self) -> str:
        return self.binding.lane_id

    @property
    def job_id(self) -> str:
        return self.binding.job_id

    def identity_value(self) -> dict[str, object]:
        return {
            "protocol": self.protocol,
            "schema_version": self.schema_version,
            "binding": self.binding.wire_value(),
            "sequence": self.sequence,
            "kind": self.kind.value,
            "created_at": self.created_at.astimezone(UTC).isoformat(),
            "payload": dict(self.payload),
        }

    def wire_value(self) -> dict[str, object]:
        return {"event_id": self.event_id, **self.identity_value()}

    @classmethod
    def create(
        cls,
        *,
        binding: Round5ControlBinding,
        sequence: int,
        kind: Round5ControlKind,
        payload: Mapping[str, object] | None = None,
        created_at: datetime | None = None,
    ) -> Round5ControlEvent:
        timestamp = created_at or datetime.now(UTC)
        identity = {
            "protocol": ROUND5_CONTROL_PROTOCOL,
            "schema_version": ROUND5_CONTROL_SCHEMA_VERSION,
            "binding": binding.wire_value(),
            "sequence": sequence,
            "kind": kind.value,
            "created_at": timestamp.astimezone(UTC).isoformat(),
            "payload": dict(payload or {}),
        }
        return cls(
            event_id=hashlib.sha256(canonical_json(identity)).hexdigest(),
            binding=binding,
            sequence=sequence,
            kind=kind,
            created_at=timestamp,
            payload=dict(payload or {}),
        )

    @classmethod
    def from_wire(cls, raw: Mapping[str, object]) -> Round5ControlEvent:
        binding = raw.get("binding")
        payload = raw.get("payload")
        sequence = raw.get("sequence")
        schema_version = raw.get("schema_version")
        if (
            not isinstance(binding, Mapping)
            or not isinstance(payload, Mapping)
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
        ):
            raise ValueError("resident control wire shape is invalid")
        return cls(
            event_id=str(raw.get("event_id") or ""),
            binding=Round5ControlBinding.from_wire(binding),
            sequence=sequence,
            kind=Round5ControlKind(str(raw.get("kind") or "")),
            created_at=datetime.fromisoformat(str(raw.get("created_at") or "")),
            payload=dict(payload),
            protocol=str(raw.get("protocol") or ""),
            schema_version=schema_version,
        )

    def encoded_body(self) -> str:
        return base64.urlsafe_b64encode(
            gzip.compress(canonical_json(self.wire_value()), mtime=0)
        ).decode()


@dataclass(frozen=True, slots=True)
class Round5RunnerEvent:
    event_id: str
    binding: Round5ControlBinding
    sequence: int
    kind: Round5RunnerEventKind
    occurred_at: datetime
    payload: Mapping[str, object]

    @property
    def generation(self) -> int:
        return self.binding.generation

    @property
    def lane_id(self) -> str:
        return self.binding.lane_id

    @property
    def job_id(self) -> str:
        return self.binding.job_id


def _verified_runner_event(
    *,
    event_id: object,
    binding: object,
    sequence: object,
    kind: object,
    occurred_at: object,
    payload: object,
) -> Round5RunnerEvent:
    if (
        not isinstance(binding, Mapping)
        or isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or sequence <= 0
        or not isinstance(occurred_at, datetime)
        or occurred_at.tzinfo is None
        or occurred_at.utcoffset() is None
        or not isinstance(payload, Mapping)
    ):
        raise ValueError("resident runner event row is malformed")
    runner_binding = Round5ControlBinding.from_wire(binding)
    runner_kind = Round5RunnerEventKind(str(kind))
    normalized_payload = dict(payload)
    identity = {
        "binding": runner_binding.wire_value(),
        "sequence": sequence,
        "kind": runner_kind.value,
        "occurred_at": occurred_at.astimezone(UTC).isoformat(),
        "payload": normalized_payload,
    }
    expected = hashlib.sha256(canonical_json(identity)).hexdigest()
    if str(event_id) != expected:
        raise ValueError("resident runner event identity changed")
    return Round5RunnerEvent(
        event_id=expected,
        binding=runner_binding,
        sequence=sequence,
        kind=runner_kind,
        occurred_at=occurred_at,
        payload=normalized_payload,
    )


def round5_control_migration_statements() -> tuple[str, ...]:
    """Schema-owner DDL. Runtime code must never call this."""

    return (
        f"""
        CREATE TABLE IF NOT EXISTS {ROUND5_CONTROL_OUTBOX_TABLE} (
            event_id text PRIMARY KEY,
            installation_id text NOT NULL,
            lane_id text NOT NULL,
            generation bigint NOT NULL,
            warm_attempt_token text NOT NULL,
            job_id text NOT NULL,
            sequence bigint NOT NULL,
            kind text NOT NULL,
            payload jsonb NOT NULL,
            created_at timestamptz NOT NULL,
            published_at timestamptz,
            UNIQUE (
                installation_id, lane_id, generation, warm_attempt_token,
                job_id, sequence
            )
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {ROUND5_RUNNER_EVENT_TABLE} (
            event_id text PRIMARY KEY,
            installation_id text NOT NULL,
            lane_id text NOT NULL,
            generation bigint NOT NULL,
            warm_attempt_token text NOT NULL,
            job_id text NOT NULL,
            sequence bigint NOT NULL,
            kind text NOT NULL,
            binding jsonb NOT NULL,
            payload jsonb NOT NULL,
            occurred_at timestamptz NOT NULL,
            UNIQUE (
                installation_id, lane_id, generation, warm_attempt_token,
                job_id, sequence
            )
        )
        """,
    )


async def migrate_round5_control(cursor: Any) -> None:
    for statement in round5_control_migration_statements():
        await cursor.execute(statement)


class Round5ControlStore(Protocol):
    async def initialize(self) -> None: ...

    async def enqueue(self, event: Round5ControlEvent) -> None: ...

    async def pending(
        self,
        limit: int = 32,
        *,
        allowed_release_ids: Collection[str] = (),
    ) -> tuple[Round5ControlEvent, ...]: ...

    async def release_dispatchable(self, event_id: str) -> bool: ...

    async def expire_orphaned_releases(
        self,
        *,
        created_before: datetime,
        expired_at: datetime,
        protected_release_ids: Collection[str] = (),
    ) -> tuple[str, ...]: ...

    async def mark_published(
        self,
        event_id: str,
        *,
        published_at: datetime,
    ) -> None: ...

    async def runner_events(
        self,
        job_id: str,
        *,
        after_sequence: int,
    ) -> tuple[Round5RunnerEvent, ...]: ...

    async def binding_for_job(self, job_id: str) -> Round5ControlBinding | None: ...

    async def control_event(
        self,
        job_id: str,
        sequence: int,
    ) -> Round5ControlEvent | None: ...

    async def latest_resident_attestation(
        self,
        installation_id: str,
        lane_id: str,
        warm_attempt_token: str,
    ) -> Round5RunnerEvent | None: ...


class InMemoryRound5ControlStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.outbox: dict[str, tuple[Round5ControlEvent, datetime | None]] = {}
        self.events: dict[str, list[Round5RunnerEvent]] = {}

    async def initialize(self) -> None:
        return None

    async def enqueue(self, event: Round5ControlEvent) -> None:
        async with self._lock:
            logical = (
                event.installation_id,
                event.lane_id,
                event.generation,
                event.binding.warm_attempt_token,
                event.job_id,
                event.sequence,
            )
            for current, _published in self.outbox.values():
                current_logical = (
                    current.installation_id,
                    current.lane_id,
                    current.generation,
                    current.binding.warm_attempt_token,
                    current.job_id,
                    current.sequence,
                )
                if current_logical == logical and current != event:
                    raise ValueError("resident outbox logical identity conflict")
            existing = self.outbox.get(event.event_id)
            if existing is not None and existing[0] != event:
                raise ValueError("resident outbox event identity conflict")
            self.outbox.setdefault(event.event_id, (event, None))
            if event.kind == Round5ControlKind.CANCEL:
                for event_id, (current, published_at) in tuple(self.outbox.items()):
                    if (
                        current.job_id == event.job_id
                        and current.kind == Round5ControlKind.RELEASE
                        and published_at is None
                    ):
                        self.outbox[event_id] = (current, event.created_at)

    async def pending(
        self,
        limit: int = 32,
        *,
        allowed_release_ids: Collection[str] = (),
    ) -> tuple[Round5ControlEvent, ...]:
        async with self._lock:
            allowed = frozenset(allowed_release_ids)
            values = [
                event
                for event, published_at in self.outbox.values()
                if published_at is None
                and (event.kind != Round5ControlKind.RELEASE or event.event_id in allowed)
            ]
            values.sort(
                key=lambda event: (
                    event.installation_id,
                    event.lane_id,
                    event.generation,
                    event.binding.warm_attempt_token,
                    event.job_id,
                    event.sequence,
                    event.created_at,
                    event.event_id,
                )
            )
            return tuple(values[:limit])

    async def mark_published(
        self,
        event_id: str,
        *,
        published_at: datetime,
    ) -> None:
        async with self._lock:
            event, current = self.outbox[event_id]
            self.outbox[event_id] = (event, current or published_at)

    async def release_dispatchable(self, event_id: str) -> bool:
        async with self._lock:
            row = self.outbox.get(event_id)
            if row is None:
                return False
            event, published_at = row
            if event.kind != Round5ControlKind.RELEASE or published_at is not None:
                return False
            return not any(
                current.kind == Round5ControlKind.CANCEL
                and current.job_id == event.job_id
                for current, _current_published_at in self.outbox.values()
            )

    async def expire_orphaned_releases(
        self,
        *,
        created_before: datetime,
        expired_at: datetime,
        protected_release_ids: Collection[str] = (),
    ) -> tuple[str, ...]:
        async with self._lock:
            protected = frozenset(protected_release_ids)
            expired: list[str] = []
            for event_id, (event, published_at) in tuple(self.outbox.items()):
                if (
                    event.kind == Round5ControlKind.RELEASE
                    and published_at is None
                    and event.created_at <= created_before
                    and event_id not in protected
                ):
                    self.outbox[event_id] = (event, expired_at)
                    expired.append(event_id)
            return tuple(expired)

    async def append_runner_event(self, event: Round5RunnerEvent) -> None:
        async with self._lock:
            values = self.events.setdefault(event.job_id, [])
            same = next(
                (current for current in values if current.sequence == event.sequence),
                None,
            )
            if same is not None:
                if same != event:
                    raise ValueError("resident runner sequence conflict")
                return
            values.append(event)
            values.sort(key=lambda current: current.sequence)

    async def runner_events(
        self,
        job_id: str,
        *,
        after_sequence: int,
    ) -> tuple[Round5RunnerEvent, ...]:
        async with self._lock:
            return tuple(
                event for event in self.events.get(job_id, ()) if event.sequence > after_sequence
            )

    async def binding_for_job(self, job_id: str) -> Round5ControlBinding | None:
        async with self._lock:
            for event, _published in self.outbox.values():
                if event.job_id == job_id:
                    return event.binding
            return None

    async def control_event(
        self,
        job_id: str,
        sequence: int,
    ) -> Round5ControlEvent | None:
        async with self._lock:
            return next(
                (
                    event
                    for event, _published in self.outbox.values()
                    if event.job_id == job_id and event.sequence == sequence
                ),
                None,
            )

    async def latest_resident_attestation(
        self,
        installation_id: str,
        lane_id: str,
        warm_attempt_token: str,
    ) -> Round5RunnerEvent | None:
        async with self._lock:
            values = [
                event
                for events in self.events.values()
                for event in events
                if event.binding.installation_id == installation_id
                and event.lane_id == lane_id
                and event.binding.warm_attempt_token == warm_attempt_token
                and event.kind
                in {
                    Round5RunnerEventKind.AGENT_READY,
                    Round5RunnerEventKind.HEARTBEAT,
                }
            ]
            return max(values, key=lambda event: event.occurred_at, default=None)


class LakebaseRound5ControlStore:
    """Runtime store: verify and use pre-migrated objects; execute no DDL."""

    def __init__(
        self,
        run: Callable[[Callable[[Any], Awaitable[Any]]], Awaitable[Any]],
    ) -> None:
        self._run = run

    async def initialize(self) -> None:
        async def verify(cursor: Any) -> None:
            objects = await read_coordination_objects(
                cursor,
                (ROUND5_CONTROL_OUTBOX_TABLE, ROUND5_RUNNER_EVENT_TABLE),
            )
            if not objects.complete:
                raise CoordinationObjectsMissingError(
                    "Round 5 resident control objects are missing: " + objects.describe_missing()
                )
            expected = {
                "round5_control_outbox_v3": {
                    "event_id",
                    "installation_id",
                    "lane_id",
                    "generation",
                    "warm_attempt_token",
                    "job_id",
                    "sequence",
                    "kind",
                    "payload",
                    "created_at",
                    "published_at",
                },
                "round5_runner_event_v3": {
                    "event_id",
                    "installation_id",
                    "lane_id",
                    "generation",
                    "warm_attempt_token",
                    "job_id",
                    "sequence",
                    "kind",
                    "binding",
                    "payload",
                    "occurred_at",
                },
            }
            await cursor.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = ANY(%s)
                """,
                (COORDINATION_SCHEMA, list(expected)),
            )
            actual: dict[str, set[str]] = {name: set() for name in expected}
            for table_name, column_name in await cursor.fetchall():
                if str(table_name) in actual:
                    actual[str(table_name)].add(str(column_name))
            if actual != expected:
                raise CoordinationObjectsMissingError(
                    "Round 5 resident control schema version is incomplete"
                )
            # Repair debt written by versions that committed CANCEL without
            # suppressing the gated RELEASE. The matching cancel is durable
            # proof that the release must never cross its gate.
            await cursor.execute(
                f"""
                UPDATE {ROUND5_CONTROL_OUTBOX_TABLE} AS release
                SET published_at = COALESCE(release.published_at, cancel.created_at)
                FROM {ROUND5_CONTROL_OUTBOX_TABLE} AS cancel
                WHERE release.published_at IS NULL
                  AND release.kind = %s
                  AND cancel.kind = %s
                  AND cancel.installation_id = release.installation_id
                  AND cancel.lane_id = release.lane_id
                  AND cancel.generation = release.generation
                  AND cancel.warm_attempt_token = release.warm_attempt_token
                  AND cancel.job_id = release.job_id
                """,
                (
                    Round5ControlKind.RELEASE.value,
                    Round5ControlKind.CANCEL.value,
                ),
            )

        await self._run(verify)

    async def enqueue(self, event: Round5ControlEvent) -> None:
        value = json.dumps(event.wire_value(), sort_keys=True, separators=(",", ":"))

        async def insert(cursor: Any) -> None:
            try:
                await cursor.execute(
                    f"""
                    INSERT INTO {ROUND5_CONTROL_OUTBOX_TABLE} (
                        event_id, installation_id, lane_id, generation,
                        warm_attempt_token, job_id, sequence, kind, payload, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
                    )
                    ON CONFLICT (
                        installation_id, lane_id, generation, warm_attempt_token,
                        job_id, sequence
                    ) DO UPDATE SET event_id = EXCLUDED.event_id
                    WHERE {ROUND5_CONTROL_OUTBOX_TABLE}.event_id = EXCLUDED.event_id
                      AND {ROUND5_CONTROL_OUTBOX_TABLE}.payload = EXCLUDED.payload
                    RETURNING event_id
                    """,
                    (
                        event.event_id,
                        event.installation_id,
                        event.lane_id,
                        event.generation,
                        event.binding.warm_attempt_token,
                        event.job_id,
                        event.sequence,
                        event.kind.value,
                        value,
                        event.created_at,
                    ),
                )
            except psycopg.errors.UniqueViolation as exc:
                raise ValueError("resident outbox event identity conflict") from exc
            row = await cursor.fetchone()
            if row is None or str(row[0]) != event.event_id:
                raise ValueError("resident outbox logical identity conflict")
            if event.kind == Round5ControlKind.CANCEL:
                # CANCEL and suppression commit in the same transaction. A
                # process death after this callback therefore cannot strand the
                # gated RELEASE at the head of the ordered pending window.
                await cursor.execute(
                    f"""
                    UPDATE {ROUND5_CONTROL_OUTBOX_TABLE}
                    SET published_at = COALESCE(published_at, %s)
                    WHERE job_id = %s
                      AND kind = %s
                      AND published_at IS NULL
                    """,
                    (
                        event.created_at,
                        event.job_id,
                        Round5ControlKind.RELEASE.value,
                    ),
                )

        await self._run(insert)

    async def pending(
        self,
        limit: int = 32,
        *,
        allowed_release_ids: Collection[str] = (),
    ) -> tuple[Round5ControlEvent, ...]:
        async def select(cursor: Any) -> tuple[Round5ControlEvent, ...]:
            await cursor.execute(
                f"""
                SELECT payload
                FROM {ROUND5_CONTROL_OUTBOX_TABLE}
                WHERE published_at IS NULL
                  AND (
                    kind <> %s
                    OR event_id = ANY(%s)
                  )
                ORDER BY installation_id, lane_id, generation,
                         warm_attempt_token, job_id, sequence, created_at, event_id
                LIMIT %s
                """,
                (
                    Round5ControlKind.RELEASE.value,
                    list(allowed_release_ids),
                    limit,
                ),
            )
            return tuple(
                Round5ControlEvent.from_wire(
                    row[0] if isinstance(row[0], Mapping) else json.loads(str(row[0]))
                )
                for row in await cursor.fetchall()
            )

        return await self._run(select)

    async def mark_published(
        self,
        event_id: str,
        *,
        published_at: datetime,
    ) -> None:
        async def update(cursor: Any) -> None:
            await cursor.execute(
                f"""
                UPDATE {ROUND5_CONTROL_OUTBOX_TABLE}
                SET published_at = COALESCE(published_at, %s)
                WHERE event_id = %s
                """,
                (published_at, event_id),
            )

        await self._run(update)

    async def release_dispatchable(self, event_id: str) -> bool:
        """Revalidate one RELEASE against its durable cancellation fence."""

        async def select(cursor: Any) -> bool:
            await cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM {ROUND5_CONTROL_OUTBOX_TABLE} AS release
                    WHERE release.event_id = %s
                      AND release.kind = %s
                      AND release.published_at IS NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {ROUND5_CONTROL_OUTBOX_TABLE} AS cancel
                          WHERE cancel.kind = %s
                            AND cancel.installation_id = release.installation_id
                            AND cancel.lane_id = release.lane_id
                            AND cancel.generation = release.generation
                            AND cancel.warm_attempt_token = release.warm_attempt_token
                            AND cancel.job_id = release.job_id
                      )
                )
                """,
                (
                    event_id,
                    Round5ControlKind.RELEASE.value,
                    Round5ControlKind.CANCEL.value,
                ),
            )
            row = await cursor.fetchone()
            return bool(row and row[0])

        return await self._run(select)

    async def expire_orphaned_releases(
        self,
        *,
        created_before: datetime,
        expired_at: datetime,
        protected_release_ids: Collection[str] = (),
    ) -> tuple[str, ...]:
        """Tombstone crashed-bell RELEASE intents after their execution budget."""

        async def update(cursor: Any) -> tuple[str, ...]:
            await cursor.execute(
                f"""
                UPDATE {ROUND5_CONTROL_OUTBOX_TABLE}
                SET published_at = %s
                WHERE kind = %s
                  AND published_at IS NULL
                  AND created_at <= %s
                  AND NOT (event_id = ANY(%s))
                RETURNING event_id
                """,
                (
                    expired_at,
                    Round5ControlKind.RELEASE.value,
                    created_before,
                    list(protected_release_ids),
                ),
            )
            return tuple(str(row[0]) for row in await cursor.fetchall())

        return await self._run(update)

    async def runner_events(
        self,
        job_id: str,
        *,
        after_sequence: int,
    ) -> tuple[Round5RunnerEvent, ...]:
        async def select(cursor: Any) -> tuple[Round5RunnerEvent, ...]:
            await cursor.execute(
                f"""
                SELECT event_id, binding, sequence, kind, occurred_at, payload
                FROM {ROUND5_RUNNER_EVENT_TABLE}
                WHERE job_id = %s AND sequence > %s
                ORDER BY sequence
                """,
                (job_id, after_sequence),
            )
            return tuple(
                _verified_runner_event(
                    event_id=row[0],
                    binding=row[1],
                    sequence=row[2],
                    kind=row[3],
                    occurred_at=row[4],
                    payload=row[5],
                )
                for row in await cursor.fetchall()
            )

        return await self._run(select)

    async def binding_for_job(self, job_id: str) -> Round5ControlBinding | None:
        async def select(cursor: Any) -> Round5ControlBinding | None:
            await cursor.execute(
                f"""
                SELECT payload
                FROM {ROUND5_CONTROL_OUTBOX_TABLE}
                WHERE job_id = %s
                ORDER BY sequence
                LIMIT 1
                """,
                (job_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            raw = row[0] if isinstance(row[0], Mapping) else json.loads(str(row[0]))
            return Round5ControlEvent.from_wire(raw).binding

        return await self._run(select)

    async def control_event(
        self,
        job_id: str,
        sequence: int,
    ) -> Round5ControlEvent | None:
        async def select(cursor: Any) -> Round5ControlEvent | None:
            await cursor.execute(
                f"""
                SELECT payload
                FROM {ROUND5_CONTROL_OUTBOX_TABLE}
                WHERE job_id = %s AND sequence = %s
                """,
                (job_id, sequence),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            raw = row[0] if isinstance(row[0], Mapping) else json.loads(str(row[0]))
            return Round5ControlEvent.from_wire(raw)

        return await self._run(select)

    async def latest_resident_attestation(
        self,
        installation_id: str,
        lane_id: str,
        warm_attempt_token: str,
    ) -> Round5RunnerEvent | None:
        async def select(cursor: Any) -> Round5RunnerEvent | None:
            await cursor.execute(
                f"""
                SELECT event_id, binding, sequence, kind, occurred_at, payload
                FROM {ROUND5_RUNNER_EVENT_TABLE}
                WHERE installation_id = %s
                  AND lane_id = %s
                  AND warm_attempt_token = %s
                  AND kind IN ('agent_ready', 'heartbeat')
                ORDER BY occurred_at DESC, sequence DESC
                LIMIT 1
                """,
                (installation_id, lane_id, warm_attempt_token),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return _verified_runner_event(
                event_id=row[0],
                binding=row[1],
                sequence=row[2],
                kind=row[3],
                occurred_at=row[4],
                payload=row[5],
            )

        return await self._run(select)


class Round5ControlDispatcher:
    def __init__(
        self,
        store: Round5ControlStore,
        send: Callable[[Round5ControlEvent], Awaitable[None]],
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_persistent_failure: Callable[[str], Awaitable[None]] | None = None,
        persistent_failure_threshold: int = 3,
        persistent_failure_window_seconds: float = 8.0,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if persistent_failure_threshold <= 0:
            raise ValueError("persistent failure threshold must be positive")
        if persistent_failure_window_seconds < 0:
            raise ValueError("persistent failure window cannot be negative")
        self.store = store
        self._send = send
        self._sleep = sleep
        self._on_persistent_failure = on_persistent_failure
        self._persistent_failure_threshold = persistent_failure_threshold
        self._persistent_failure_window_seconds = persistent_failure_window_seconds
        self._first_failure_at: datetime | None = None
        self._now = now
        self._wake = asyncio.Event()
        self._closed = False
        self._task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._initialized = False
        self._allowed_releases: set[str] = set()
        self._held_releases: set[str] = set()
        self._next_orphan_reap_at: datetime | None = None
        self._consecutive_failures = 0
        self._persistent_failure_reported = False

    async def start(self) -> asyncio.Task[None]:
        # Every enqueue calls start(), and concurrent lanes may call it at the
        # same time. Schema verification and the legacy CANCEL/RELEASE repair
        # are process-start work, not per-event work; serialize them once for
        # this dispatcher before exposing its publishing task.
        async with self._start_lock:
            if not self._initialized:
                await self.store.initialize()
                self._initialized = True
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self.run(), name="round5-control-outbox")
            return self._task

    def wake(self) -> None:
        self._wake.set()

    def allow_release(self, event_id: str) -> None:
        """Open one exact RELEASE only after its eligibility edge."""

        _digest(event_id, "event_id")
        self._held_releases.add(event_id)
        self._allowed_releases.add(event_id)
        self._wake.set()

    def hold_release(self, event_id: str) -> None:
        """Protect a live gated RELEASE while its topology gate is still closed."""

        _digest(event_id, "event_id")
        self._held_releases.add(event_id)

    def discard_release(self, event_id: str) -> None:
        """Forget a process-local gate after its durable RELEASE is suppressed."""

        _digest(event_id, "event_id")
        self._held_releases.discard(event_id)
        self._allowed_releases.discard(event_id)

    async def close(self) -> None:
        self._closed = True
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def publish_once(self) -> int:
        published = 0
        try:
            now = self._now()
            if self._next_orphan_reap_at is None or now >= self._next_orphan_reap_at:
                self._next_orphan_reap_at = now + timedelta(
                    seconds=ROUND5_ORPHAN_REAP_INTERVAL_SECONDS
                )
                try:
                    expired = await self.store.expire_orphaned_releases(
                        created_before=now
                        - timedelta(seconds=ROUND5_ORPHANED_RELEASE_TTL_SECONDS),
                        expired_at=now,
                        protected_release_ids=self._held_releases,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Reaping is maintenance, never a delivery dependency. A
                    # coordination blip must not block STAGE/CANCEL or turn this
                    # 30-second task into a 10 Hz retry loop.
                    logger.warning("round5_control_orphan_reap_failed")
                else:
                    if expired:
                        logger.warning(
                            "round5_control_orphan_releases_expired count=%d",
                            len(expired),
                        )
                    self._held_releases.difference_update(expired)
                    self._allowed_releases.difference_update(expired)
            try:
                pending_events = await self.store.pending(
                    allowed_release_ids=self._allowed_releases,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # A failed SCAN of the outbox is NOT a delivery failure: there is no
                # real outbox row we failed to publish. Counting an (idle, empty)
                # scan blip toward the persistent-failure budget is what let a
                # transient store hiccup withdraw idle READY and flash the fight card
                # "Temporarily Unavailable". Log and treat as nothing-to-do; only a
                # failed _send()/mark_published() of a real row (below) counts.
                logger.warning("round5_control_outbox_scan_failed")
                return 0
            for event in pending_events:
                # RELEASE is a durable intent, not permission to cross the
                # measured edge.  Lakebase's row is committed with the bell and
                # opened only after the process captures T0; the competitor row
                # is staged while the Proxy is provisioning and opened only
                # after the exact control-plane gate.  Gated rows stay
                # unpublished so allow_release can still open the current bout,
                # but they are excluded from pending() so crashed bells cannot
                # fill the ordered LIMIT 32 window.
                if (
                    event.kind == Round5ControlKind.RELEASE
                    and (
                        event.event_id not in self._allowed_releases
                        or not await self.store.release_dispatchable(event.event_id)
                    )
                ):
                    self.discard_release(event.event_id)
                    continue
                await self._send(event)
                await self.store.mark_published(
                    event.event_id,
                    published_at=self._now(),
                )
                self.discard_release(event.event_id)
                published += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._record_delivery_failure()
            raise
        else:
            self._record_delivery_success()
            return published

    async def _record_delivery_failure(self) -> None:
        self._consecutive_failures += 1
        failures = self._consecutive_failures
        now = self._now()
        if self._first_failure_at is None:
            self._first_failure_at = now
        if failures & (failures - 1) == 0:
            logger.warning(
                "round5_control_outbox_publish_failed consecutive_failures=%d",
                failures,
            )
        # Withdraw readiness only for a GENUINELY persistent outbox failure -- both a
        # count threshold AND a minimum elapsed WINDOW. The dispatcher polls the
        # outbox at ~10Hz, so a bare count of 3 fired after ~0.3s: a transient DB
        # blip during an idle outbox scan withdrew Round 5 readiness and flashed the
        # fight card "Temporarily Unavailable". Requiring the failure to persist across
        # a real wall-clock window keeps a sub-second blip from ever demoting READY.
        elapsed = (now - self._first_failure_at).total_seconds()
        if (
            failures < self._persistent_failure_threshold
            or elapsed < self._persistent_failure_window_seconds
            or self._persistent_failure_reported
            or self._on_persistent_failure is None
        ):
            return
        self._persistent_failure_reported = True
        try:
            await self._on_persistent_failure("resident_control_delivery_failed")
        except asyncio.CancelledError:
            raise
        except Exception:
            # The reason is deliberately fixed.  This callback can fail because
            # the same coordination endpoint is unavailable; neither that
            # provider exception nor its identifiers belong in readiness.
            logger.error("round5_control_readiness_withdrawal_failed")

    def _record_delivery_success(self) -> None:
        if self._consecutive_failures:
            logger.warning(
                "round5_control_outbox_publish_recovered consecutive_failures=%d",
                self._consecutive_failures,
            )
        self._consecutive_failures = 0
        self._persistent_failure_reported = False
        self._first_failure_at = None

    async def run(self) -> None:
        while not self._closed:
            # Clear before scanning.  A gate opening while the scan is in
            # flight then remains set and forces an immediate rescan; clearing
            # after the scan loses that wake and adds the full poll interval to
            # the Proxy-gate dispatch edge.
            self._wake.clear()
            try:
                published = await self.publish_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                published = 0
            if published or self._wake.is_set():
                continue
            # Back off while the outbox is failing so a transient fault is not
            # re-hit at the full ~10Hz idle poll rate (which turned a sub-second
            # blip into the persistent-failure threshold almost instantly). A clean
            # idle scan keeps the responsive 0.1s poll; consecutive failures grow the
            # gap up to a 2s ceiling so the persistent-failure WINDOW reflects a real
            # sustained outage rather than poll frequency.
            idle_timeout = 0.1
            if self._consecutive_failures:
                idle_timeout = min(2.0, 0.1 * float(2 ** min(self._consecutive_failures, 5)))
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=idle_timeout)
            except TimeoutError:
                await self._sleep(0)


class Round5ResidentTransport:
    """Await resident progress/results/settlement without consulting SSM."""

    def __init__(
        self,
        store: Round5ControlStore,
        dispatcher: Round5ControlDispatcher,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        deadline_seconds: float = ROUND5_RESIDENT_DEADLINE_SECONDS,
    ) -> None:
        self.store = store
        self.dispatcher = dispatcher
        self._sleep = sleep
        self.deadline_seconds = deadline_seconds

    async def _enqueue(
        self,
        *,
        binding: Round5ControlBinding,
        sequence: int,
        kind: Round5ControlKind,
        payload: Mapping[str, object] | None = None,
    ) -> Round5ControlEvent:
        await self.dispatcher.start()
        existing = await self.store.control_event(binding.job_id, sequence)
        if existing is not None:
            if (
                existing.binding != binding
                or existing.kind != kind
                or dict(existing.payload) != dict(payload or {})
            ):
                raise ValueError("resident control sequence was reused")
            self.dispatcher.wake()
            return existing
        event = Round5ControlEvent.create(
            binding=binding,
            sequence=sequence,
            kind=kind,
            payload=payload,
        )
        await self.store.enqueue(event)
        self.dispatcher.wake()
        return event

    async def preload(
        self,
        *,
        binding: Round5ControlBinding,
        request: Mapping[str, object],
    ) -> None:
        await self._enqueue(
            binding=binding,
            sequence=1,
            kind=Round5ControlKind.PRELOAD,
            payload={"request": dict(request)},
        )

    async def stage(
        self,
        *,
        binding: Round5ControlBinding,
        request: Mapping[str, object],
    ) -> None:
        await self._enqueue(
            binding=binding,
            sequence=1,
            kind=Round5ControlKind.STAGE,
            payload={"request": dict(request)},
        )
        await self.wait_prepared(binding)

    async def stage_and_release(
        self,
        *,
        binding: Round5ControlBinding,
        request: Mapping[str, object],
    ) -> None:
        await self.stage(binding=binding, request=request)
        await self.release(binding=binding)

    async def stage_for_release(
        self,
        *,
        binding: Round5ControlBinding,
        request: Mapping[str, object],
    ) -> Round5ControlEvent:
        """Prepare the resident and durably hold RELEASE behind its exact gate."""

        await self.stage(binding=binding, request=request)
        event = await self._enqueue(
            binding=binding,
            sequence=2,
            kind=Round5ControlKind.RELEASE,
        )
        # The competitor can remain staged while RDS Proxy becomes available
        # for longer than the bout execution deadline. This process still owns
        # that exact gate, so the crashed-bell janitor must not infer orphanhood
        # merely from age.
        self.dispatcher.hold_release(event.event_id)
        return event

    async def release(self, *, binding: Round5ControlBinding) -> None:
        event = await self._enqueue(
            binding=binding,
            sequence=2,
            kind=Round5ControlKind.RELEASE,
        )
        self.dispatcher.allow_release(event.event_id)

    async def cancel(
        self,
        *,
        binding: Round5ControlBinding,
        await_settlement: bool = True,
    ) -> None:
        await self._enqueue(
            binding=binding,
            sequence=3,
            kind=Round5ControlKind.CANCEL,
        )
        release = await self.store.control_event(binding.job_id, 2)
        if release is not None and release.kind == Round5ControlKind.RELEASE:
            # The CANCEL transaction has now tombstoned the durable RELEASE.
            # Drop its process-local permission too: a prior send failure leaves
            # that permission live because publish_once never reached its normal
            # post-publish discard.
            self.dispatcher.discard_release(release.event_id)
        if await_settlement:
            await self.wait_settled(binding)

    async def result(
        self,
        binding: Round5ControlBinding,
        *,
        on_progress: Callable[[Mapping[str, object]], Awaitable[None]] | None,
    ) -> dict[str, object]:
        sequence = 0
        result: dict[str, object] | None = None
        failure: str | None = None
        async with asyncio.timeout(self.deadline_seconds):
            while True:
                events = await self.store.runner_events(
                    binding.job_id,
                    after_sequence=sequence,
                )
                for event in events:
                    if event.binding != binding:
                        raise RuntimeError("resident runner event binding changed")
                    sequence = event.sequence
                    if event.kind == Round5RunnerEventKind.PROGRESS:
                        if on_progress is not None:
                            await on_progress(event.payload)
                    elif event.kind == Round5RunnerEventKind.RESULT:
                        result = dict(event.payload)
                    elif event.kind == Round5RunnerEventKind.FAILED:
                        failure = str(event.payload.get("code") or "resident_runner_failed")
                    elif event.kind == Round5RunnerEventKind.QUARANTINED:
                        raise RuntimeError(
                            str(event.payload.get("code") or "resident_control_quarantined")
                        )
                    elif event.kind == Round5RunnerEventKind.SETTLED:
                        if failure is not None:
                            raise RuntimeError(failure)
                        if result is None:
                            raise RuntimeError("resident settled without publishing a result")
                        return result
                await self._sleep(0.05)

    async def wait_agent_ready(
        self,
        binding: Round5ControlBinding,
        *,
        not_before: datetime | None = None,
    ) -> Mapping[str, object]:
        sequence = 0
        async with asyncio.timeout(60):
            while True:
                events = await self.store.runner_events(
                    binding.job_id,
                    after_sequence=sequence,
                )
                for event in events:
                    if not_before is not None and event.occurred_at < not_before:
                        sequence = event.sequence
                        continue
                    expected_binding = binding.wire_value()
                    actual_binding = event.binding.wire_value()
                    expected_binding.pop("runner_process_boot_id")
                    process_boot_id = actual_binding.pop("runner_process_boot_id")
                    if actual_binding != expected_binding or process_boot_id in {"", "unattested"}:
                        raise Round5ResidentBindingChangedError(
                            "resident readiness binding changed"
                        )
                    sequence = event.sequence
                    if event.kind == Round5RunnerEventKind.AGENT_READY:
                        payload = event.payload
                        if (
                            payload.get("worker_count") != 4
                            or payload.get("worker_ready_indexes") != [0, 1, 2, 3]
                            or payload.get("warm_attempt_token") != binding.warm_attempt_token
                            or payload.get("runner_boot_id") != binding.runner_boot_id
                            or not isinstance(
                                payload.get("runner_process_boot_id"),
                                str,
                            )
                            or payload.get("runner_process_boot_id") != process_boot_id
                            or isinstance(payload.get("process_pid"), bool)
                            or not isinstance(payload.get("process_pid"), int)
                            or int(payload["process_pid"]) <= 0
                            or payload.get("runner_harness_sha256") != binding.runner_harness_sha256
                        ):
                            raise RuntimeError("resident agent readiness proof is incomplete")
                        return payload
                    if event.kind == Round5RunnerEventKind.FAILED:
                        raise RuntimeError("resident agent preload failed")
                await self._sleep(0.05)

    async def wait_prepared(self, binding: Round5ControlBinding) -> None:
        sequence = 0
        async with asyncio.timeout(self.deadline_seconds):
            while True:
                for event in await self.store.runner_events(
                    binding.job_id,
                    after_sequence=sequence,
                ):
                    if event.binding != binding:
                        raise RuntimeError("resident preparation binding changed")
                    sequence = event.sequence
                    if event.kind == Round5RunnerEventKind.PREPARED:
                        if (
                            event.payload.get("state") != "prepared"
                            or event.payload.get("worker_ready_count") != 4
                            or event.payload.get("request_sha256") != binding.request_sha256
                        ):
                            raise RuntimeError("resident preparation proof is incomplete")
                        return
                    if event.kind in {
                        Round5RunnerEventKind.FAILED,
                        Round5RunnerEventKind.QUARANTINED,
                    }:
                        raise RuntimeError(
                            str(event.payload.get("code") or "resident_preparation_failed")
                        )
                await self._sleep(0.05)

    async def wait_settled(self, binding: Round5ControlBinding) -> None:
        sequence = 0
        async with asyncio.timeout(self.deadline_seconds):
            while True:
                for event in await self.store.runner_events(
                    binding.job_id,
                    after_sequence=sequence,
                ):
                    if event.binding != binding:
                        raise RuntimeError("resident settlement binding changed")
                    sequence = event.sequence
                    if event.kind == Round5RunnerEventKind.SETTLED:
                        return
                    if event.kind == Round5RunnerEventKind.QUARANTINED:
                        raise RuntimeError(
                            str(event.payload.get("code") or "resident_control_quarantined")
                        )
                await self._sleep(0.05)

    async def settle_registry_job(self, job_id: str) -> None:
        binding = await self.store.binding_for_job(job_id)
        if binding is None:
            return
        if any(
            event.kind == Round5RunnerEventKind.SETTLED
            for event in await self.store.runner_events(job_id, after_sequence=0)
        ):
            return
        await self.cancel(binding=binding, await_settlement=True)

    async def resident_is_current(
        self,
        *,
        installation_id: str,
        lane_id: str,
        warm_attempt_token: str,
        runner_boot_id: str,
        process_boot_id: str,
        process_pid: int,
        harness_sha256: str,
        now: datetime,
        heartbeat_seconds: float = 15.0,
    ) -> bool:
        # Heartbeat freshness window aligned to PROVENANCE_FRESHNESS_SECONDS (15s):
        # a 5s window was TIGHTER than the runner's own beat cadence, so an idle
        # keep-alive probe routinely landed between beats, read "not current", and
        # (via validate_ready -> False) demoted READY -> the idle "Temporarily
        # Unavailable" flicker. A stale beat within this wider window is a transient
        # miss handled by the coordinator's strike budget (validate_ready False =
        # strike), NOT an attested identity change; the identity fields below are what
        # actually attest a genuine runner change and demote once persistent.
        return (
            await self.resident_liveness(
                installation_id=installation_id,
                lane_id=lane_id,
                warm_attempt_token=warm_attempt_token,
                runner_boot_id=runner_boot_id,
                process_boot_id=process_boot_id,
                process_pid=process_pid,
                harness_sha256=harness_sha256,
                now=now,
                heartbeat_seconds=heartbeat_seconds,
            )
        ) is ResidentLiveness.CURRENT

    async def resident_liveness(
        self,
        *,
        installation_id: str,
        lane_id: str,
        warm_attempt_token: str,
        runner_boot_id: str,
        process_boot_id: str,
        process_pid: int,
        harness_sha256: str,
        now: datetime,
        heartbeat_seconds: float = 15.0,
    ) -> ResidentLiveness:
        """Classify resident liveness for the idle keep-alive contract.

        Separates an ATTESTED identity change (demote) from a transient STALE/ABSENT
        heartbeat (strike). Identity is judged FIRST and independently of freshness, so
        a runner that rebooted is IDENTITY_CHANGED even if its stale beat is recent, and
        a runner whose identity still matches is only STALE (never a false identity
        change) when its beat has merely aged past the window.
        """

        event = await self.store.latest_resident_attestation(
            installation_id,
            lane_id,
            warm_attempt_token,
        )
        if event is None:
            return ResidentLiveness.ABSENT
        identity_ok = (
            event.payload.get("runner_boot_id") == runner_boot_id
            and event.payload.get("runner_process_boot_id") == process_boot_id
            and event.payload.get("process_pid") == process_pid
            and event.payload.get("runner_harness_sha256") == harness_sha256
            and event.payload.get("worker_ready_indexes") == [0, 1, 2, 3]
        )
        if not identity_ok:
            return ResidentLiveness.IDENTITY_CHANGED
        fresh = (now - event.occurred_at).total_seconds() <= heartbeat_seconds
        return ResidentLiveness.CURRENT if fresh else ResidentLiveness.STALE
