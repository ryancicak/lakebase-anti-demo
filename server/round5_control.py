"""Durable, versioned resident-runner control plane for Round 5."""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
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

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$")


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

    async def pending(self, limit: int = 32) -> tuple[Round5ControlEvent, ...]: ...

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

    async def pending(self, limit: int = 32) -> tuple[Round5ControlEvent, ...]:
        async with self._lock:
            values = [event for event, published_at in self.outbox.values() if published_at is None]
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

        await self._run(insert)

    async def pending(self, limit: int = 32) -> tuple[Round5ControlEvent, ...]:
        async def select(cursor: Any) -> tuple[Round5ControlEvent, ...]:
            await cursor.execute(
                f"""
                SELECT payload
                FROM {ROUND5_CONTROL_OUTBOX_TABLE}
                WHERE published_at IS NULL
                ORDER BY installation_id, lane_id, generation,
                         warm_attempt_token, job_id, sequence, created_at, event_id
                LIMIT %s
                """,
                (limit,),
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
    ) -> None:
        if persistent_failure_threshold <= 0:
            raise ValueError("persistent failure threshold must be positive")
        self.store = store
        self._send = send
        self._sleep = sleep
        self._on_persistent_failure = on_persistent_failure
        self._persistent_failure_threshold = persistent_failure_threshold
        self._wake = asyncio.Event()
        self._closed = False
        self._task: asyncio.Task[None] | None = None
        self._allowed_releases: set[str] = set()
        self._consecutive_failures = 0
        self._persistent_failure_reported = False

    async def start(self) -> asyncio.Task[None]:
        await self.store.initialize()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run(), name="round5-control-outbox")
        return self._task

    def wake(self) -> None:
        self._wake.set()

    def allow_release(self, event_id: str) -> None:
        """Open one exact RELEASE only after its eligibility edge."""

        _digest(event_id, "event_id")
        self._allowed_releases.add(event_id)
        self._wake.set()

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
            for event in await self.store.pending():
                # RELEASE is a durable intent, not permission to cross the
                # measured edge.  Lakebase's row is committed with the bell and
                # opened only after the process captures T0; the competitor row
                # is staged while the Proxy is provisioning and opened only
                # after the exact control-plane gate.
                if (
                    event.kind == Round5ControlKind.RELEASE
                    and event.event_id not in self._allowed_releases
                ):
                    continue
                await self._send(event)
                await self.store.mark_published(
                    event.event_id,
                    published_at=datetime.now(UTC),
                )
                self._allowed_releases.discard(event.event_id)
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
        if failures & (failures - 1) == 0:
            logger.warning(
                "round5_control_outbox_publish_failed consecutive_failures=%d",
                failures,
            )
        if (
            failures < self._persistent_failure_threshold
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
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=0.1)
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
        return await self._enqueue(
            binding=binding,
            sequence=2,
            kind=Round5ControlKind.RELEASE,
        )

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
                        raise RuntimeError("resident readiness binding changed")
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
        heartbeat_seconds: float = 5.0,
    ) -> bool:
        event = await self.store.latest_resident_attestation(
            installation_id,
            lane_id,
            warm_attempt_token,
        )
        return bool(
            event is not None
            and (now - event.occurred_at).total_seconds() <= heartbeat_seconds
            and event.payload.get("runner_boot_id") == runner_boot_id
            and event.payload.get("runner_process_boot_id") == process_boot_id
            and event.payload.get("process_pid") == process_pid
            and event.payload.get("runner_harness_sha256") == harness_sha256
            and event.payload.get("worker_ready_indexes") == [0, 1, 2, 3]
        )
