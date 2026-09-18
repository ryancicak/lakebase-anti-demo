"""Durable Round 5 warm-slot and bell contracts.

Round 5 is prepared by a supervised installation-scoped coordinator.  HTTP
requests may claim a READY generation and ring it, but they never perform warm
work.  The mutable head and its append-only event are changed atomically by the
store implementations in this module.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

from .coordination import (
    COORDINATION_SCHEMA,
    CoordinationObjectsMissingError,
    read_coordination_objects,
)
from .round5_control import (
    ROUND5_CONTROL_OUTBOX_TABLE,
    LakebaseRound5ControlStore,
    Round5ControlEvent,
    Round5ControlKind,
)

logger = logging.getLogger(__name__)

ROUND5_WARM_PROTOCOL = "round5-bell-to-10k-v4"
ROUND5_WARM_SLOT_TABLE = f"{COORDINATION_SCHEMA}.round5_warm_slot_v4"
ROUND5_WARM_EVENT_TABLE = f"{COORDINATION_SCHEMA}.round5_warm_event_v4"
ROUND5_SLOT_ORDINAL = 1

PROXY_SETUP_DEADLINE_SECONDS = 30 * 60
RUNNER_DEADLINE_SECONDS = 660
CONTROL_PLANE_MARGIN_SECONDS = 60
DISPATCH_MARGIN_SECONDS = 60
DEFAULT_CLAIM_TTL_SECONDS = 180
DEFAULT_COORDINATOR_TTL_SECONDS = 90
DEFAULT_RETRY_CEILING_SECONDS = 60
PROVENANCE_FRESHNESS_SECONDS = 15
CLEANUP_FINALIZE_CONFLICT_ATTEMPTS = 40

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_RUNNER_ID = re.compile(r"^i-[0-9a-f]{8,17}$")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _required(value: str, name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _digest(value: str, name: str) -> str:
    if _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase sha256 digest")
    return value


def _safe_identifier(value: str, name: str) -> str:
    if _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} is not a safe identifier")
    return value


def _safe_code(value: str, name: str = "code") -> str:
    if _SAFE_CODE.fullmatch(value) is None:
        raise ValueError(f"{name} is not a safe event code")
    return value


def stable_round5_id(*parts: str) -> str:
    """Return a deterministic, secret-free identity for one logical operation."""

    if not parts or any(not isinstance(part, str) or not part for part in parts):
        raise ValueError("stable_round5_id requires non-empty string parts")
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def round5_warm_attempt_token(slot: Round5WarmSlot) -> str:
    if slot.warm_attempt_token is None:
        raise WarmFenceLostError("warm attempt token is unavailable")
    return slot.warm_attempt_token


def _receipt_expiry_bound(slot: Round5WarmSlot) -> datetime:
    if slot.shared_receipt is None or not slot.variants:
        raise WarmFenceLostError("READY receipt bounds are unavailable")
    return min(
        slot.shared_receipt.lakebase_runner.expires_at,
        slot.shared_receipt.competitor_runner.expires_at,
        *(receipt.expires_at for receipt in slot.variants.values()),
    )


class Round5WarmState(StrEnum):
    WARMING = "warming"
    READY = "ready"
    CLAIMED = "claimed"
    RUNNING = "running"
    CLEANING = "cleaning"
    BLOCKED = "blocked"


class Round5Variant(StrEnum):
    AURORA = "aurora"
    RDS = "rds"


class WarmStoreConflictError(RuntimeError):
    """The slot changed after it was read and before it was written."""


class WarmCoordinatorHeldError(RuntimeError):
    """Another live process owns the warm coordinator lease."""


class WarmClaimUnavailableError(RuntimeError):
    """No current READY generation can be atomically claimed."""


class WarmFenceLostError(RuntimeError):
    """The coordinator, claim, or cleanup fence changed."""


class RetryableWarmError(RuntimeError):
    """A provider or transport fault that the coordinator may retry unattended."""

    def __init__(self, code: str) -> None:
        super().__init__(_safe_code(code))
        self.code = code


class BlockedWarmError(RuntimeError):
    """A configuration, ownership, security, or capacity defect."""

    def __init__(self, code: str) -> None:
        super().__init__(_safe_code(code))
        self.code = code


@dataclass(frozen=True, slots=True)
class Round5RunnerReceipt:
    lane_id: str
    instance_id: str
    boot_id: str
    process_boot_id: str
    process_pid: int
    instance_type: str
    image_sha256: str
    loaded_harness_sha256: str
    capacity_model_sha256: str
    expires_at: datetime

    def __post_init__(self) -> None:
        if self.lane_id not in {"lakebase", "competitor"}:
            raise ValueError("runner lane_id must be lakebase or competitor")
        if _RUNNER_ID.fullmatch(self.instance_id) is None:
            raise ValueError("runner instance_id is invalid")
        _safe_identifier(self.boot_id, "boot_id")
        _safe_identifier(self.process_boot_id, "process_boot_id")
        if isinstance(self.process_pid, bool) or self.process_pid <= 0:
            raise ValueError("runner process_pid must be positive")
        if self.instance_type != "c7i.2xlarge":
            raise ValueError("Round 5 V4 requires c7i.2xlarge runners")
        _digest(self.image_sha256, "image_sha256")
        _digest(self.loaded_harness_sha256, "loaded_harness_sha256")
        _digest(self.capacity_model_sha256, "capacity_model_sha256")
        _utc(self.expires_at, "expires_at")


@dataclass(frozen=True, slots=True)
class Round5VariantReceipt:
    variant: Round5Variant
    target_sha256: str
    source_sha256: str
    secret_ref_sha256: str
    role_sha256: str
    auth_sha256: str
    tls_sha256: str
    security_group_sha256: str
    subnet_sha256: str
    vpc_sha256: str
    proxy_absent: bool
    proxy_absence_observed_at: datetime
    request_template_sha256: str
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "target_sha256",
            "source_sha256",
            "secret_ref_sha256",
            "role_sha256",
            "auth_sha256",
            "tls_sha256",
            "security_group_sha256",
            "subnet_sha256",
            "vpc_sha256",
            "request_template_sha256",
        ):
            _digest(getattr(self, name), name)
        if not self.proxy_absent:
            raise ValueError("a warm receipt must prove that its per-bout Proxy is absent")
        _utc(self.proxy_absence_observed_at, "proxy_absence_observed_at")
        _utc(self.expires_at, "expires_at")


@dataclass(frozen=True, slots=True)
class Round5SharedReceipt:
    source_sha256: str
    config_sha256: str
    runner_image_sha256: str
    fanin_contract_sha256: str
    capacity_model_sha256: str
    lakebase_binding_sha256: str
    static_network_fixture_sha256: str
    lakebase_runner: Round5RunnerReceipt
    competitor_runner: Round5RunnerReceipt

    def __post_init__(self) -> None:
        for name in (
            "source_sha256",
            "config_sha256",
            "runner_image_sha256",
            "fanin_contract_sha256",
            "capacity_model_sha256",
            "lakebase_binding_sha256",
            "static_network_fixture_sha256",
        ):
            _digest(getattr(self, name), name)
        if self.lakebase_runner.lane_id != "lakebase":
            raise ValueError("lakebase_runner receipt is assigned to the wrong lane")
        if self.competitor_runner.lane_id != "competitor":
            raise ValueError("competitor_runner receipt is assigned to the wrong lane")
        if self.lakebase_runner.instance_id == self.competitor_runner.instance_id:
            raise ValueError("Round 5 V4 requires two distinct physical runners")


@dataclass(frozen=True, slots=True)
class Round5BoutClaim:
    claim_id: str
    bell_id: str
    session_id: str
    bout_id: str
    selected_variant: Round5Variant
    bout_fence: int
    claimed_at: datetime
    claim_expires_at: datetime
    capsule_generation: int
    lakebase_job_id: str
    competitor_job_id: str
    warm_attempt_token: str

    def __post_init__(self) -> None:
        for name in ("claim_id", "bell_id", "session_id", "bout_id"):
            _safe_identifier(getattr(self, name), name)
        if self.bout_fence <= 0 or self.capsule_generation <= 0:
            raise ValueError("claim fences and generations must be positive")
        _utc(self.claimed_at, "claimed_at")
        _utc(self.claim_expires_at, "claim_expires_at")
        _digest(self.lakebase_job_id, "lakebase_job_id")
        _digest(self.competitor_job_id, "competitor_job_id")
        _safe_identifier(self.warm_attempt_token, "warm_attempt_token")


@dataclass(frozen=True, slots=True)
class Round5WarmSlot:
    installation_id: str
    generation: int
    state: Round5WarmState
    revision: int
    coordinator_fence: int
    process_epoch: str
    broker_epoch: str
    warm_contract_sha256: str
    warming_started_at: datetime
    slot_ordinal: int = ROUND5_SLOT_ORDINAL
    ready_at: datetime | None = None
    ready_expires_at: datetime | None = None
    renew_by: datetime | None = None
    provenance_expires_at: datetime | None = None
    shared_receipt: Round5SharedReceipt | None = None
    variants: Mapping[Round5Variant, Round5VariantReceipt] = field(default_factory=dict)
    claim: Round5BoutClaim | None = None
    bell_id: str | None = None
    bell_at_utc: datetime | None = None
    last_attempt_at: datetime | None = None
    attempt_count: int = 0
    next_retry_at: datetime | None = None
    last_error_code: str | None = None
    last_error_at: datetime | None = None
    coordinator_owner: str | None = None
    coordinator_lease_expires_at: datetime | None = None
    warm_attempt_token: str | None = None

    def __post_init__(self) -> None:
        _safe_identifier(self.installation_id, "installation_id")
        if self.generation <= 0 or self.revision <= 0:
            raise ValueError("warm generation and revision must be positive")
        if self.coordinator_fence < 0:
            raise ValueError("coordinator_fence cannot be negative")
        if self.slot_ordinal != ROUND5_SLOT_ORDINAL:
            raise ValueError("only the installation Round 5 warm slot is supported")
        if self.process_epoch:
            _safe_identifier(self.process_epoch, "process_epoch")
        if self.broker_epoch:
            _safe_identifier(self.broker_epoch, "broker_epoch")
        _digest(self.warm_contract_sha256, "warm_contract_sha256")
        _utc(self.warming_started_at, "warming_started_at")
        for name in (
            "ready_at",
            "ready_expires_at",
            "renew_by",
            "provenance_expires_at",
            "last_attempt_at",
            "next_retry_at",
            "last_error_at",
            "bell_at_utc",
            "coordinator_lease_expires_at",
        ):
            value = getattr(self, name)
            if value is not None:
                _utc(value, name)
        if self.last_error_code is not None:
            _safe_code(self.last_error_code, "last_error_code")
        if self.bell_id is not None:
            _safe_identifier(self.bell_id, "bell_id")
        if self.warm_attempt_token is not None:
            _safe_identifier(self.warm_attempt_token, "warm_attempt_token")
        if self.state == Round5WarmState.READY:
            if (
                self.shared_receipt is None
                or set(self.variants) != {Round5Variant.AURORA, Round5Variant.RDS}
                or self.ready_at is None
                or self.ready_expires_at is None
                or self.renew_by is None
                or self.provenance_expires_at is None
                or self.warm_attempt_token is None
                or self.claim is not None
            ):
                raise ValueError("READY requires complete shared and two-variant receipts")
        if self.state in {
            Round5WarmState.CLAIMED,
            Round5WarmState.RUNNING,
            Round5WarmState.CLEANING,
        } and self.claim is None:
            raise ValueError(f"{self.state.value} requires a bout claim")
        if self.state == Round5WarmState.RUNNING and (
            self.bell_id is None or self.bell_at_utc is None
        ):
            raise ValueError("RUNNING requires a durable bell identity")


@dataclass(frozen=True, slots=True)
class Round5WarmEvent:
    event_id: str
    idempotency_key: str
    installation_id: str
    generation: int
    slot_ordinal: int
    revision: int
    process_epoch: str
    broker_epoch: str
    event_type: str
    from_state: Round5WarmState | None
    to_state: Round5WarmState
    occurred_at: datetime
    coordinator_fence: int
    claim_id: str | None = None
    bout_fence: int | None = None
    detail: Mapping[str, str | int | float | bool | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _safe_identifier(self.event_id, "event_id")
        _safe_identifier(self.idempotency_key, "idempotency_key")
        _safe_code(self.event_type, "event_type")
        _utc(self.occurred_at, "occurred_at")
        encoded = json.dumps(dict(self.detail), sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > 2_048:
            raise ValueError("warm event detail exceeds its safe bound")
        lowered = encoded.casefold()
        if any(
            token in lowered
            for token in (
                "password",
                "secret_access",
                "session_token",
                "authorization",
                "arn:",
                "-----begin",
            )
        ):
            raise ValueError("warm event detail contains forbidden credential material")


@dataclass(frozen=True, slots=True)
class Round5LaunchCapsule:
    """Ephemeral launch material; this value is never sent to a store."""

    generation: int
    coordinator_fence: int
    credential_generation: int
    broker_epoch: str
    runner_contexts: Mapping[str, object]
    aws_control_contexts: Mapping[Round5Variant, object]
    lakebase_context: object
    variant_contexts: Mapping[Round5Variant, object]
    control_expires_at: datetime
    dispatch_expires_at: Mapping[str, datetime]
    expires_at: datetime
    renew_by: datetime
    warm_attempt_token: str

    def __post_init__(self) -> None:
        if self.generation <= 0 or self.coordinator_fence <= 0:
            raise ValueError("capsule generation and coordinator fence must be positive")
        if self.credential_generation <= 0:
            raise ValueError("credential_generation must be positive")
        _safe_identifier(self.broker_epoch, "broker_epoch")
        _safe_identifier(self.warm_attempt_token, "warm_attempt_token")
        if set(self.runner_contexts) != {"lakebase", "competitor"}:
            raise ValueError("capsule requires independent runner contexts")
        if set(self.aws_control_contexts) != {Round5Variant.AURORA, Round5Variant.RDS}:
            raise ValueError("capsule requires both AWS variant contexts")
        if set(self.variant_contexts) != {Round5Variant.AURORA, Round5Variant.RDS}:
            raise ValueError("capsule requires both variant launch contexts")
        if set(self.dispatch_expires_at) != {"lakebase", "competitor"}:
            raise ValueError("capsule requires separate dispatch credential expirations")
        _utc(self.control_expires_at, "control_expires_at")
        _utc(self.expires_at, "expires_at")
        _utc(self.renew_by, "renew_by")
        for value in self.dispatch_expires_at.values():
            _utc(value, "dispatch_expires_at")
        if self.expires_at > min(
            self.control_expires_at,
            *self.dispatch_expires_at.values(),
        ):
            raise ValueError("capsule expiry exceeds an immutable credential bound")
        if self.renew_by >= self.expires_at:
            raise ValueError("capsule renew_by must precede expiration")

    def meets_launch_margin(self, now: datetime) -> bool:
        now = _utc(now, "now")
        control_margin = timedelta(
            seconds=PROXY_SETUP_DEADLINE_SECONDS + CONTROL_PLANE_MARGIN_SECONDS
        )
        dispatch_margin = timedelta(seconds=RUNNER_DEADLINE_SECONDS + DISPATCH_MARGIN_SECONDS)
        return (
            self.control_expires_at - now >= control_margin
            and all(
                expiration - now >= dispatch_margin
                for expiration in self.dispatch_expires_at.values()
            )
            and self.expires_at > now
        )


@dataclass(frozen=True, slots=True)
class Round5WarmPreparation:
    shared_receipt: Round5SharedReceipt
    variants: Mapping[Round5Variant, Round5VariantReceipt]
    capsule: Round5LaunchCapsule

    def __post_init__(self) -> None:
        if set(self.variants) != {Round5Variant.AURORA, Round5Variant.RDS}:
            raise ValueError("warming must prepare both competitor variants")
        if not all(receipt.proxy_absent for receipt in self.variants.values()):
            raise ValueError("warming may not pre-create an RDS Proxy")


@dataclass(frozen=True, slots=True)
class BellContext:
    bell_id: str
    claim_id: str
    warm_generation: int
    bout_id: str
    bout_fence: int
    bell_at_utc: datetime
    t0_monotonic_ns: int


class Round5WarmProvider(Protocol):
    async def reconcile(self, slot: Round5WarmSlot) -> bool: ...

    async def validate_ready(
        self,
        slot: Round5WarmSlot,
        capsule: Round5LaunchCapsule,
    ) -> bool: ...

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
    ) -> Round5WarmPreparation: ...

    async def refresh_capsule(
        self,
        slot: Round5WarmSlot,
        previous: Round5LaunchCapsule,
    ) -> Round5LaunchCapsule: ...


class Round5WarmStore(Protocol):
    mode: str

    async def initialize(self) -> None: ...

    async def close(self) -> None: ...

    async def read(self, installation_id: str) -> Round5WarmSlot | None: ...

    async def events(self, installation_id: str) -> tuple[Round5WarmEvent, ...]: ...

    async def ensure_warming(
        self,
        *,
        installation_id: str,
        warm_contract_sha256: str,
        process_epoch: str,
        broker_epoch: str,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def acquire_coordinator(
        self,
        *,
        installation_id: str,
        process_epoch: str,
        broker_epoch: str,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot: ...

    async def heartbeat_coordinator(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot: ...

    async def adopt_contract(
        self,
        slot: Round5WarmSlot,
        *,
        warm_contract_sha256: str,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def record_attempt(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        warm_attempt_token: str,
    ) -> Round5WarmSlot: ...

    async def publish_ready(
        self,
        slot: Round5WarmSlot,
        *,
        preparation: Round5WarmPreparation,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def record_retry(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
        next_retry_at: datetime,
    ) -> Round5WarmSlot: ...

    async def record_blocked(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def freshness_lost(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def update_capsule_receipt(
        self,
        slot: Round5WarmSlot,
        *,
        capsule: Round5LaunchCapsule,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def renew_provenance(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot: ...

    async def claim_ready(
        self,
        slot: Round5WarmSlot,
        *,
        claim: Round5BoutClaim,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def release_expired_claim(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        still_fresh: bool,
    ) -> Round5WarmSlot: ...

    async def renew_claim(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot: ...

    async def accept_bell(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        bell_id: str,
        bell_at_utc: datetime,
    ) -> Round5WarmSlot: ...

    async def begin_cleanup(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        now: datetime,
    ) -> Round5WarmSlot: ...

    async def finish_cleanup_and_rewarm(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        now: datetime,
    ) -> Round5WarmSlot: ...


class InMemoryRound5WarmStore:
    """Deterministic store with the same CAS semantics as the durable store."""

    mode = "memory"

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._slots: dict[str, Round5WarmSlot] = {}
        self._events: dict[str, list[Round5WarmEvent]] = {}
        self.control_outbox: list[Round5ControlEvent] = []

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def read(self, installation_id: str) -> Round5WarmSlot | None:
        async with self._lock:
            return self._slots.get(installation_id)

    async def events(self, installation_id: str) -> tuple[Round5WarmEvent, ...]:
        async with self._lock:
            return tuple(self._events.get(installation_id, ()))

    def _event(
        self,
        old: Round5WarmSlot | None,
        new: Round5WarmSlot,
        event_type: str,
        *,
        now: datetime,
        detail: Mapping[str, str | int | float | bool | None] | None = None,
    ) -> Round5WarmEvent:
        key = stable_round5_id(
            new.installation_id,
            str(new.slot_ordinal),
            str(new.generation),
            str(new.revision),
            event_type,
        )
        return Round5WarmEvent(
            event_id=key,
            idempotency_key=key,
            installation_id=new.installation_id,
            generation=new.generation,
            slot_ordinal=new.slot_ordinal,
            revision=new.revision,
            process_epoch=new.process_epoch,
            broker_epoch=new.broker_epoch,
            event_type=event_type,
            from_state=old.state if old is not None else None,
            to_state=new.state,
            occurred_at=now,
            coordinator_fence=new.coordinator_fence,
            claim_id=new.claim.claim_id if new.claim is not None else None,
            bout_fence=new.claim.bout_fence if new.claim is not None else None,
            detail=dict(detail or {}),
        )

    def _put(
        self,
        old: Round5WarmSlot | None,
        new: Round5WarmSlot,
        event_type: str,
        *,
        now: datetime,
        detail: Mapping[str, str | int | float | bool | None] | None = None,
    ) -> Round5WarmSlot:
        current = self._slots.get(new.installation_id)
        if old is None:
            if current is not None:
                raise WarmStoreConflictError("warm slot already exists")
        elif current is None or current.revision != old.revision:
            raise WarmStoreConflictError("warm slot revision changed")
        self._slots[new.installation_id] = new
        self._events.setdefault(new.installation_id, []).append(
            self._event(old, new, event_type, now=now, detail=detail)
        )
        return new

    async def ensure_warming(
        self,
        *,
        installation_id: str,
        warm_contract_sha256: str,
        process_epoch: str,
        broker_epoch: str,
        now: datetime,
    ) -> Round5WarmSlot:
        _safe_identifier(installation_id, "installation_id")
        _digest(warm_contract_sha256, "warm_contract_sha256")
        now = _utc(now, "now")
        async with self._lock:
            current = self._slots.get(installation_id)
            if current is not None:
                return current
            created = Round5WarmSlot(
                installation_id=installation_id,
                generation=1,
                state=Round5WarmState.WARMING,
                revision=1,
                coordinator_fence=0,
                process_epoch=process_epoch,
                broker_epoch=broker_epoch,
                warm_contract_sha256=warm_contract_sha256,
                warming_started_at=now,
            )
            return self._put(None, created, "warm_started", now=now)

    async def acquire_coordinator(
        self,
        *,
        installation_id: str,
        process_epoch: str,
        broker_epoch: str,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot:
        now = _utc(now, "now")
        async with self._lock:
            current = self._slots.get(installation_id)
            if current is None:
                raise WarmStoreConflictError("warm slot does not exist")
            active_owner = (
                current.coordinator_owner is not None
                and current.coordinator_owner != process_epoch
                and current.coordinator_lease_expires_at is not None
                and current.coordinator_lease_expires_at > now
            )
            if active_owner:
                raise WarmCoordinatorHeldError("another process owns Round 5 warming")
            replacement = current.coordinator_owner != process_epoch
            state = (
                Round5WarmState.WARMING
                if replacement
                and current.state
                in {Round5WarmState.READY, Round5WarmState.BLOCKED}
                else current.state
            )
            updated = replace(
                current,
                state=state,
                revision=current.revision + 1,
                coordinator_fence=current.coordinator_fence + (1 if replacement else 0),
                process_epoch=process_epoch,
                broker_epoch=broker_epoch,
                coordinator_owner=process_epoch,
                coordinator_lease_expires_at=now + ttl,
                ready_at=None if state == Round5WarmState.WARMING else current.ready_at,
                ready_expires_at=(
                    None if state == Round5WarmState.WARMING else current.ready_expires_at
                ),
                renew_by=None if state == Round5WarmState.WARMING else current.renew_by,
                provenance_expires_at=(
                    None
                    if state == Round5WarmState.WARMING
                    else current.provenance_expires_at
                ),
                last_error_code=(
                    None
                    if state == Round5WarmState.WARMING
                    else current.last_error_code
                ),
                last_error_at=(
                    None if state == Round5WarmState.WARMING else current.last_error_at
                ),
            )
            return self._put(
                current,
                updated,
                "warm_started" if replacement else "warm_step",
                now=now,
                detail={"leader_replaced": replacement},
            )

    async def heartbeat_coordinator(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot:
        now = _utc(now, "now")
        async with self._lock:
            current = self._slots.get(slot.installation_id)
            if (
                current is None
                or current.revision != slot.revision
                or current.coordinator_owner != slot.process_epoch
                or current.coordinator_fence != slot.coordinator_fence
            ):
                raise WarmFenceLostError("warm coordinator fence changed")
            updated = replace(
                current,
                revision=current.revision + 1,
                coordinator_lease_expires_at=now + ttl,
            )
            return self._put(current, updated, "warm_step", now=now)

    async def adopt_contract(
        self,
        slot: Round5WarmSlot,
        *,
        warm_contract_sha256: str,
        now: datetime,
    ) -> Round5WarmSlot:
        _digest(warm_contract_sha256, "warm_contract_sha256")
        if slot.state in {
            Round5WarmState.CLAIMED,
            Round5WarmState.RUNNING,
            Round5WarmState.CLEANING,
        }:
            raise WarmFenceLostError("an active claim must clean before contract adoption")
        return await self._replace(
            slot,
            "warm_started",
            now=now,
            generation=slot.generation + 1,
            state=Round5WarmState.WARMING,
            warm_contract_sha256=warm_contract_sha256,
            warming_started_at=now,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            warm_attempt_token=None,
            shared_receipt=None,
            variants={},
            claim=None,
            bell_id=None,
            bell_at_utc=None,
            attempt_count=0,
            next_retry_at=None,
            last_error_code=None,
            last_error_at=None,
        )

    async def record_attempt(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        warm_attempt_token: str,
    ) -> Round5WarmSlot:
        _safe_identifier(warm_attempt_token, "warm_attempt_token")
        return await self._replace(
            slot,
            "warm_step",
            now=now,
            last_attempt_at=now,
            attempt_count=slot.attempt_count + 1,
            next_retry_at=None,
            warm_attempt_token=warm_attempt_token,
        )

    async def publish_ready(
        self,
        slot: Round5WarmSlot,
        *,
        preparation: Round5WarmPreparation,
        now: datetime,
    ) -> Round5WarmSlot:
        now = _utc(now, "now")
        capsule = preparation.capsule
        if capsule.generation != slot.generation:
            raise WarmFenceLostError("capsule generation changed")
        if capsule.coordinator_fence != slot.coordinator_fence:
            raise WarmFenceLostError("capsule coordinator fence changed")
        if capsule.warm_attempt_token != slot.warm_attempt_token:
            raise WarmFenceLostError("capsule warm attempt changed")
        if not capsule.meets_launch_margin(now):
            raise BlockedWarmError("credential_margin_insufficient")
        receipt_expirations = [
            preparation.shared_receipt.lakebase_runner.expires_at,
            preparation.shared_receipt.competitor_runner.expires_at,
            *(receipt.expires_at for receipt in preparation.variants.values()),
            capsule.expires_at,
        ]
        ready_expires_at = min(receipt_expirations)
        renew_by = min(capsule.renew_by, ready_expires_at)
        return await self._replace(
            slot,
            "warm_ready",
            now=now,
            state=Round5WarmState.READY,
            ready_at=now,
            ready_expires_at=ready_expires_at,
            renew_by=renew_by,
            provenance_expires_at=now
            + timedelta(seconds=PROVENANCE_FRESHNESS_SECONDS),
            shared_receipt=preparation.shared_receipt,
            variants=dict(preparation.variants),
            claim=None,
            bell_id=None,
            bell_at_utc=None,
            next_retry_at=None,
            last_error_code=None,
            last_error_at=None,
        )

    async def record_retry(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
        next_retry_at: datetime,
    ) -> Round5WarmSlot:
        _safe_code(code)
        return await self._replace(
            slot,
            "warm_retry",
            now=now,
            state=Round5WarmState.WARMING,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            next_retry_at=next_retry_at,
            last_error_code=code,
            last_error_at=now,
        )

    async def record_blocked(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
    ) -> Round5WarmSlot:
        _safe_code(code)
        return await self._replace(
            slot,
            "warm_blocked",
            now=now,
            state=Round5WarmState.BLOCKED,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            next_retry_at=None,
            last_error_code=code,
            last_error_at=now,
        )

    async def freshness_lost(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
    ) -> Round5WarmSlot:
        _safe_code(code)
        return await self._replace(
            slot,
            "warm_freshness_lost",
            now=now,
            state=Round5WarmState.WARMING,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            last_error_code=code,
            last_error_at=now,
        )

    async def update_capsule_receipt(
        self,
        slot: Round5WarmSlot,
        *,
        capsule: Round5LaunchCapsule,
        now: datetime,
    ) -> Round5WarmSlot:
        if slot.state != Round5WarmState.READY:
            raise WarmFenceLostError("capsule refresh requires READY")
        if (
            capsule.generation != slot.generation
            or capsule.coordinator_fence != slot.coordinator_fence
            or capsule.warm_attempt_token != slot.warm_attempt_token
        ):
            raise WarmFenceLostError("refreshed capsule belongs to another generation")
        if not capsule.meets_launch_margin(now):
            raise BlockedWarmError("credential_margin_insufficient")
        if capsule.warm_attempt_token != slot.warm_attempt_token:
            raise WarmFenceLostError("refreshed capsule warm attempt changed")
        receipt_bound = _receipt_expiry_bound(slot)
        ready_expires_at = min(receipt_bound, capsule.expires_at)
        renew_by = min(ready_expires_at, capsule.renew_by)
        if renew_by <= now:
            raise BlockedWarmError("credential_refresh_expired")
        return await self._replace(
            slot,
            "warm_capsule_refreshed",
            now=now,
            broker_epoch=capsule.broker_epoch,
            ready_expires_at=ready_expires_at,
            renew_by=renew_by,
        )

    async def renew_provenance(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot:
        if slot.state != Round5WarmState.READY:
            raise WarmFenceLostError("provenance renewal requires READY")
        return await self._replace(
            slot,
            "runner_provenance_refreshed",
            now=now,
            provenance_expires_at=min(
                _receipt_expiry_bound(slot),
                now + ttl,
            ),
        )

    async def claim_ready(
        self,
        slot: Round5WarmSlot,
        *,
        claim: Round5BoutClaim,
        now: datetime,
    ) -> Round5WarmSlot:
        now = _utc(now, "now")
        if (
            slot.state != Round5WarmState.READY
            or slot.ready_expires_at is None
            or slot.provenance_expires_at is None
            or slot.renew_by is None
        ):
            raise WarmClaimUnavailableError("Round 5 has no READY generation")
        if (
            slot.ready_expires_at <= now
            or slot.provenance_expires_at <= now
            or slot.renew_by <= now
            or _receipt_expiry_bound(slot) <= now
            or claim.capsule_generation != slot.generation
        ):
            raise WarmClaimUnavailableError("Round 5 warm evidence is stale")
        return await self._replace(
            slot,
            "claim_created",
            now=now,
            state=Round5WarmState.CLAIMED,
            claim=claim,
        )

    async def release_expired_claim(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        still_fresh: bool,
    ) -> Round5WarmSlot:
        if (
            slot.state != Round5WarmState.CLAIMED
            or slot.claim is None
            or slot.claim.claim_expires_at > now
        ):
            return slot
        ready = (
            still_fresh
            and slot.ready_expires_at is not None
            and slot.ready_expires_at > now
            and slot.renew_by is not None
            and slot.renew_by > now
            and slot.provenance_expires_at is not None
            and slot.provenance_expires_at > now
        )
        return await self._replace(
            slot,
            "claim_released",
            now=now,
            state=Round5WarmState.READY if ready else Round5WarmState.WARMING,
            claim=None,
            ready_at=slot.ready_at if ready else None,
            ready_expires_at=slot.ready_expires_at if ready else None,
            renew_by=slot.renew_by if ready else None,
        )

    async def renew_claim(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        now: datetime,
        ttl: timedelta,
    ) -> Round5WarmSlot:
        now = _utc(now, "now")
        if (
            slot.state != Round5WarmState.CLAIMED
            or slot.claim is None
            or slot.claim.claim_id != claim_id
        ):
            raise WarmFenceLostError("Round 5 claim changed before renewal")
        return await self._replace(
            slot,
            "claim_renewed",
            now=now,
            claim=replace(slot.claim, claim_expires_at=now + ttl),
        )

    async def accept_bell(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        bell_id: str,
        bell_at_utc: datetime,
    ) -> Round5WarmSlot:
        if slot.state == Round5WarmState.RUNNING and slot.bell_id == bell_id:
            return slot
        if (
            slot.state != Round5WarmState.CLAIMED
            or slot.claim is None
            or slot.claim.claim_id != claim_id
        ):
            raise WarmFenceLostError("Round 5 claim changed before bell")
        updated = await self._replace(
            slot,
            "bell_accepted",
            now=bell_at_utc,
            state=Round5WarmState.RUNNING,
            bell_id=bell_id,
            bell_at_utc=bell_at_utc,
        )
        return updated

    async def begin_cleanup(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        now: datetime,
    ) -> Round5WarmSlot:
        if slot.state == Round5WarmState.CLEANING and slot.claim is not None:
            if slot.claim.claim_id == claim_id:
                return slot
        if (
            slot.state not in {Round5WarmState.CLAIMED, Round5WarmState.RUNNING}
            or slot.claim is None
            or slot.claim.claim_id != claim_id
        ):
            raise WarmFenceLostError("Round 5 claim changed before cleanup")
        return await self._replace(
            slot,
            "cleanup_started",
            now=now,
            state=Round5WarmState.CLEANING,
        )

    async def finish_cleanup_and_rewarm(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        now: datetime,
        warm_contract_sha256: str | None = None,
    ) -> Round5WarmSlot:
        if (
            slot.state != Round5WarmState.CLEANING
            or slot.claim is None
            or slot.claim.claim_id != claim_id
        ):
            raise WarmFenceLostError("Round 5 cleanup fence changed")
        return await self._replace(
            slot,
            "rewarm_enqueued",
            now=now,
            generation=slot.generation + 1,
            state=Round5WarmState.WARMING,
            warm_contract_sha256=(
                warm_contract_sha256 or slot.warm_contract_sha256
            ),
            warming_started_at=now,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            warm_attempt_token=None,
            shared_receipt=None,
            variants={},
            claim=None,
            bell_id=None,
            bell_at_utc=None,
            attempt_count=0,
            next_retry_at=None,
            last_error_code=None,
            last_error_at=None,
        )

    async def _replace(
        self,
        slot: Round5WarmSlot,
        event_type: str,
        *,
        now: datetime,
        **changes: object,
    ) -> Round5WarmSlot:
        now = _utc(now, "now")
        async with self._lock:
            current = self._slots.get(slot.installation_id)
            if current is None or current.revision != slot.revision:
                raise WarmStoreConflictError("warm slot revision changed")
            if current.coordinator_fence != slot.coordinator_fence:
                raise WarmFenceLostError("warm coordinator fence changed")
            updated = replace(current, revision=current.revision + 1, **changes)
            return self._put(current, updated, event_type, now=now)


def _datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    return _utc(parsed, "datetime")


def _to_json(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {
            str(key.value if isinstance(key, StrEnum) else key): _to_json(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_to_json(item) for item in value]
    if hasattr(value, "__dataclass_fields__"):
        return _to_json(asdict(value))
    return value


def _runner_receipt(raw: Mapping[str, object]) -> Round5RunnerReceipt:
    return Round5RunnerReceipt(
        lane_id=str(raw["lane_id"]),
        instance_id=str(raw["instance_id"]),
        boot_id=str(raw["boot_id"]),
        process_boot_id=str(raw["process_boot_id"]),
        process_pid=int(raw["process_pid"]),
        instance_type=str(raw["instance_type"]),
        image_sha256=str(raw["image_sha256"]),
        loaded_harness_sha256=str(raw["loaded_harness_sha256"]),
        capacity_model_sha256=str(raw["capacity_model_sha256"]),
        expires_at=_datetime(raw["expires_at"]) or datetime.min.replace(tzinfo=UTC),
    )


def _variant_receipt(raw: Mapping[str, object]) -> Round5VariantReceipt:
    return Round5VariantReceipt(
        variant=Round5Variant(str(raw["variant"])),
        target_sha256=str(raw["target_sha256"]),
        source_sha256=str(raw["source_sha256"]),
        secret_ref_sha256=str(raw["secret_ref_sha256"]),
        role_sha256=str(raw["role_sha256"]),
        auth_sha256=str(raw["auth_sha256"]),
        tls_sha256=str(raw["tls_sha256"]),
        security_group_sha256=str(raw["security_group_sha256"]),
        subnet_sha256=str(raw["subnet_sha256"]),
        vpc_sha256=str(raw["vpc_sha256"]),
        proxy_absent=bool(raw["proxy_absent"]),
        proxy_absence_observed_at=(
            _datetime(raw["proxy_absence_observed_at"]) or datetime.min.replace(tzinfo=UTC)
        ),
        request_template_sha256=str(raw["request_template_sha256"]),
        expires_at=_datetime(raw["expires_at"]) or datetime.min.replace(tzinfo=UTC),
    )


def _slot_from_json(raw: Mapping[str, object]) -> Round5WarmSlot:
    shared_raw = raw.get("shared_receipt")
    shared: Round5SharedReceipt | None = None
    if isinstance(shared_raw, Mapping):
        shared = Round5SharedReceipt(
            source_sha256=str(shared_raw["source_sha256"]),
            config_sha256=str(shared_raw["config_sha256"]),
            runner_image_sha256=str(shared_raw["runner_image_sha256"]),
            fanin_contract_sha256=str(shared_raw["fanin_contract_sha256"]),
            capacity_model_sha256=str(shared_raw["capacity_model_sha256"]),
            lakebase_binding_sha256=str(shared_raw["lakebase_binding_sha256"]),
            static_network_fixture_sha256=str(shared_raw["static_network_fixture_sha256"]),
            lakebase_runner=_runner_receipt(shared_raw["lakebase_runner"]),  # type: ignore[arg-type]
            competitor_runner=_runner_receipt(shared_raw["competitor_runner"]),  # type: ignore[arg-type]
        )
    variants_raw = raw.get("variants")
    variants = (
        {
            Round5Variant(str(key)): _variant_receipt(value)
            for key, value in variants_raw.items()
            if isinstance(value, Mapping)
        }
        if isinstance(variants_raw, Mapping)
        else {}
    )
    claim_raw = raw.get("claim")
    claim = (
        Round5BoutClaim(
            claim_id=str(claim_raw["claim_id"]),
            bell_id=str(claim_raw["bell_id"]),
            session_id=str(claim_raw["session_id"]),
            bout_id=str(claim_raw["bout_id"]),
            selected_variant=Round5Variant(str(claim_raw["selected_variant"])),
            bout_fence=int(claim_raw["bout_fence"]),
            claimed_at=_datetime(claim_raw["claimed_at"]) or datetime.min.replace(tzinfo=UTC),
            claim_expires_at=(
                _datetime(claim_raw["claim_expires_at"]) or datetime.min.replace(tzinfo=UTC)
            ),
            capsule_generation=int(claim_raw["capsule_generation"]),
            lakebase_job_id=str(claim_raw["lakebase_job_id"]),
            competitor_job_id=str(claim_raw["competitor_job_id"]),
            warm_attempt_token=str(claim_raw["warm_attempt_token"]),
        )
        if isinstance(claim_raw, Mapping)
        else None
    )
    return Round5WarmSlot(
        installation_id=str(raw["installation_id"]),
        slot_ordinal=int(raw["slot_ordinal"]),
        generation=int(raw["generation"]),
        state=Round5WarmState(str(raw["state"])),
        revision=int(raw["revision"]),
        coordinator_fence=int(raw["coordinator_fence"]),
        process_epoch=str(raw["process_epoch"]),
        broker_epoch=str(raw["broker_epoch"]),
        warm_contract_sha256=str(raw["warm_contract_sha256"]),
        warming_started_at=(
            _datetime(raw["warming_started_at"]) or datetime.min.replace(tzinfo=UTC)
        ),
        ready_at=_datetime(raw.get("ready_at")),  # type: ignore[arg-type]
        ready_expires_at=_datetime(raw.get("ready_expires_at")),  # type: ignore[arg-type]
        renew_by=_datetime(raw.get("renew_by")),  # type: ignore[arg-type]
        provenance_expires_at=_datetime(raw.get("provenance_expires_at")),  # type: ignore[arg-type]
        shared_receipt=shared,
        variants=variants,
        claim=claim,
        bell_id=str(raw["bell_id"]) if raw.get("bell_id") else None,
        bell_at_utc=_datetime(raw.get("bell_at_utc")),  # type: ignore[arg-type]
        last_attempt_at=_datetime(raw.get("last_attempt_at")),  # type: ignore[arg-type]
        attempt_count=int(raw.get("attempt_count") or 0),
        next_retry_at=_datetime(raw.get("next_retry_at")),  # type: ignore[arg-type]
        last_error_code=(
            str(raw["last_error_code"]) if raw.get("last_error_code") else None
        ),
        last_error_at=_datetime(raw.get("last_error_at")),  # type: ignore[arg-type]
        coordinator_owner=(
            str(raw["coordinator_owner"]) if raw.get("coordinator_owner") else None
        ),
        coordinator_lease_expires_at=_datetime(raw.get("coordinator_lease_expires_at")),  # type: ignore[arg-type]
        warm_attempt_token=(
            str(raw["warm_attempt_token"])
            if raw.get("warm_attempt_token")
            else None
        ),
    )


def round5_warm_migration_statements() -> tuple[str, ...]:
    """Schema-owner DDL; runtime initialization only verifies these objects."""

    return (
        f"""
        CREATE TABLE IF NOT EXISTS {ROUND5_WARM_SLOT_TABLE} (
            installation_id text NOT NULL,
            slot_ordinal integer NOT NULL,
            generation bigint NOT NULL,
            state text NOT NULL,
            revision bigint NOT NULL,
            coordinator_fence bigint NOT NULL,
            process_epoch text NOT NULL,
            broker_epoch text NOT NULL,
            payload jsonb NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (installation_id, slot_ordinal)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {ROUND5_WARM_EVENT_TABLE} (
            event_id text PRIMARY KEY,
            idempotency_key text UNIQUE NOT NULL,
            installation_id text NOT NULL,
            slot_ordinal integer NOT NULL,
            generation bigint NOT NULL,
            revision bigint NOT NULL,
            event_type text NOT NULL,
            from_state text,
            to_state text NOT NULL,
            occurred_at timestamptz NOT NULL,
            payload jsonb NOT NULL
        )
        """,
    )


async def migrate_round5_warm(cursor: Any) -> None:
    for statement in round5_warm_migration_statements():
        await cursor.execute(statement)


class LakebaseRound5WarmStore:
    """Warm-slot store using the lease store's credential-rotating DB runner."""

    mode = "lakebase"

    def __init__(
        self,
        run: Callable[[Callable[[Any], Awaitable[Any]]], Awaitable[Any]],
        *,
        database: str = "anti_demo",
    ) -> None:
        self._run = run
        self.database = database

    async def initialize(self) -> None:
        async def verify(cursor: Any) -> None:
            objects = await read_coordination_objects(
                cursor,
                (ROUND5_WARM_SLOT_TABLE, ROUND5_WARM_EVENT_TABLE),
            )
            if not objects.complete:
                raise CoordinationObjectsMissingError(
                    "Round 5 warming objects are missing: "
                    + objects.describe_missing()
                )

        await self._run(verify)
        await LakebaseRound5ControlStore(self._run).initialize()

    async def close(self) -> None:
        return None

    async def read(self, installation_id: str) -> Round5WarmSlot | None:
        async def select(cursor: Any) -> Round5WarmSlot | None:
            await cursor.execute(
                f"SELECT payload FROM {ROUND5_WARM_SLOT_TABLE} "
                "WHERE installation_id = %s AND slot_ordinal = %s",
                (installation_id, ROUND5_SLOT_ORDINAL),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            raw = row[0] if isinstance(row[0], Mapping) else json.loads(str(row[0]))
            return _slot_from_json(raw)

        return await self._run(select)

    async def events(self, installation_id: str) -> tuple[Round5WarmEvent, ...]:
        async def select(cursor: Any) -> tuple[Round5WarmEvent, ...]:
            await cursor.execute(
                f"SELECT payload FROM {ROUND5_WARM_EVENT_TABLE} "
                "WHERE installation_id = %s ORDER BY revision, occurred_at",
                (installation_id,),
            )
            parsed: list[Round5WarmEvent] = []
            for row in await cursor.fetchall():
                raw = row[0] if isinstance(row[0], Mapping) else json.loads(str(row[0]))
                parsed.append(
                    Round5WarmEvent(
                        event_id=str(raw["event_id"]),
                        idempotency_key=str(raw["idempotency_key"]),
                        installation_id=str(raw["installation_id"]),
                        generation=int(raw["generation"]),
                        slot_ordinal=int(raw["slot_ordinal"]),
                        revision=int(raw["revision"]),
                        process_epoch=str(raw["process_epoch"]),
                        broker_epoch=str(raw["broker_epoch"]),
                        event_type=str(raw["event_type"]),
                        from_state=(
                            Round5WarmState(str(raw["from_state"]))
                            if raw.get("from_state")
                            else None
                        ),
                        to_state=Round5WarmState(str(raw["to_state"])),
                        occurred_at=_datetime(raw["occurred_at"])
                        or datetime.min.replace(tzinfo=UTC),
                        coordinator_fence=int(raw["coordinator_fence"]),
                        claim_id=str(raw["claim_id"]) if raw.get("claim_id") else None,
                        bout_fence=(
                            int(raw["bout_fence"]) if raw.get("bout_fence") is not None else None
                        ),
                        detail=dict(raw.get("detail") or {}),
                    )
                )
            return tuple(parsed)

        return await self._run(select)

    async def _persist(
        self,
        old: Round5WarmSlot | None,
        new: Round5WarmSlot,
        event_type: str,
        *,
        now: datetime,
        detail: Mapping[str, str | int | float | bool | None] | None = None,
    ) -> Round5WarmSlot:
        memory = InMemoryRound5WarmStore()
        event = memory._event(old, new, event_type, now=now, detail=detail)
        payload = json.dumps(_to_json(new), sort_keys=True, separators=(",", ":"))
        event_payload = json.dumps(_to_json(event), sort_keys=True, separators=(",", ":"))

        async def write(cursor: Any) -> bool:
            if old is None:
                predicate = "NOT EXISTS (SELECT 1 FROM " + ROUND5_WARM_SLOT_TABLE + (
                    " WHERE installation_id = %s AND slot_ordinal = %s)"
                )
                predicate_args: tuple[object, ...] = (
                    new.installation_id,
                    new.slot_ordinal,
                )
                conflict_sql = "DO NOTHING"
                conflict_args: tuple[object, ...] = ()
            else:
                predicate = (
                    "EXISTS (SELECT 1 FROM "
                    + ROUND5_WARM_SLOT_TABLE
                    + " WHERE installation_id = %s AND slot_ordinal = %s "
                    "AND revision = %s AND coordinator_fence = %s)"
                )
                predicate_args = (
                    old.installation_id,
                    old.slot_ordinal,
                    old.revision,
                    old.coordinator_fence,
                )
                conflict_sql = f"""
                    DO UPDATE SET
                        generation = EXCLUDED.generation,
                        state = EXCLUDED.state,
                        revision = EXCLUDED.revision,
                        coordinator_fence = EXCLUDED.coordinator_fence,
                        process_epoch = EXCLUDED.process_epoch,
                        broker_epoch = EXCLUDED.broker_epoch,
                        payload = EXCLUDED.payload,
                        updated_at = EXCLUDED.updated_at
                    WHERE {ROUND5_WARM_SLOT_TABLE}.revision = %s
                      AND {ROUND5_WARM_SLOT_TABLE}.coordinator_fence = %s
                """
                conflict_args = (old.revision, old.coordinator_fence)
            await cursor.execute(
                f"""
                WITH permitted AS (
                    SELECT 1 WHERE {predicate}
                ), changed AS (
                    INSERT INTO {ROUND5_WARM_SLOT_TABLE} (
                        installation_id, slot_ordinal, generation, state, revision,
                        coordinator_fence, process_epoch, broker_epoch, payload, updated_at
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
                    FROM permitted
                    ON CONFLICT (installation_id, slot_ordinal) {conflict_sql}
                    RETURNING 1
                ), appended AS (
                    INSERT INTO {ROUND5_WARM_EVENT_TABLE} (
                        event_id, idempotency_key, installation_id, slot_ordinal,
                        generation, revision, event_type, from_state, to_state,
                        occurred_at, payload
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    FROM changed
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING 1
                )
                SELECT EXISTS(SELECT 1 FROM changed), EXISTS(SELECT 1 FROM appended)
                """,
                (
                    *predicate_args,
                    new.installation_id,
                    new.slot_ordinal,
                    new.generation,
                    new.state.value,
                    new.revision,
                    new.coordinator_fence,
                    new.process_epoch,
                    new.broker_epoch,
                    payload,
                    now,
                    *conflict_args,
                    event.event_id,
                    event.idempotency_key,
                    event.installation_id,
                    event.slot_ordinal,
                    event.generation,
                    event.revision,
                    event.event_type,
                    event.from_state.value if event.from_state is not None else None,
                    event.to_state.value,
                    event.occurred_at,
                    event_payload,
                ),
            )
            row = await cursor.fetchone()
            return bool(row and row[0] and row[1])

        if not await self._run(write):
            raise WarmStoreConflictError("warm slot compare-and-swap was refused")
        return new

    async def ensure_warming(self, **kwargs: Any) -> Round5WarmSlot:
        existing = await self.read(str(kwargs["installation_id"]))
        if existing is not None:
            return existing
        now = _utc(kwargs["now"], "now")
        created = Round5WarmSlot(
            installation_id=str(kwargs["installation_id"]),
            generation=1,
            state=Round5WarmState.WARMING,
            revision=1,
            coordinator_fence=0,
            process_epoch=str(kwargs["process_epoch"]),
            broker_epoch=str(kwargs["broker_epoch"]),
            warm_contract_sha256=str(kwargs["warm_contract_sha256"]),
            warming_started_at=now,
        )
        try:
            return await self._persist(None, created, "warm_started", now=now)
        except WarmStoreConflictError:
            winner = await self.read(created.installation_id)
            if winner is None:
                raise
            return winner

    async def _mutate(
        self,
        slot: Round5WarmSlot,
        event_type: str,
        *,
        now: datetime,
        **changes: object,
    ) -> Round5WarmSlot:
        current = await self.read(slot.installation_id)
        if current is None or current.revision != slot.revision:
            raise WarmStoreConflictError("warm slot revision changed")
        updated = replace(current, revision=current.revision + 1, **changes)
        return await self._persist(current, updated, event_type, now=now)

    async def acquire_coordinator(self, **kwargs: Any) -> Round5WarmSlot:
        current = await self.read(str(kwargs["installation_id"]))
        if current is None:
            raise WarmStoreConflictError("warm slot does not exist")
        now = _utc(kwargs["now"], "now")
        process_epoch = str(kwargs["process_epoch"])
        if (
            current.coordinator_owner not in {None, process_epoch}
            and current.coordinator_lease_expires_at is not None
            and current.coordinator_lease_expires_at > now
        ):
            raise WarmCoordinatorHeldError("another process owns Round 5 warming")
        replacement = current.coordinator_owner != process_epoch
        state = (
            Round5WarmState.WARMING
            if replacement
            and current.state
            in {Round5WarmState.READY, Round5WarmState.BLOCKED}
            else current.state
        )
        return await self._mutate(
            current,
            "warm_started" if replacement else "warm_step",
            now=now,
            state=state,
            coordinator_fence=current.coordinator_fence + (1 if replacement else 0),
            process_epoch=process_epoch,
            broker_epoch=str(kwargs["broker_epoch"]),
            coordinator_owner=process_epoch,
            coordinator_lease_expires_at=now + kwargs["ttl"],
            ready_at=None if state == Round5WarmState.WARMING else current.ready_at,
            ready_expires_at=None if state == Round5WarmState.WARMING else current.ready_expires_at,
            renew_by=None if state == Round5WarmState.WARMING else current.renew_by,
            provenance_expires_at=(
                None
                if state == Round5WarmState.WARMING
                else current.provenance_expires_at
            ),
            last_error_code=(
                None if state == Round5WarmState.WARMING else current.last_error_code
            ),
            last_error_at=(
                None if state == Round5WarmState.WARMING else current.last_error_at
            ),
        )

    async def heartbeat_coordinator(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        return await self._mutate(
            slot,
            "warm_step",
            now=kwargs["now"],
            coordinator_lease_expires_at=kwargs["now"] + kwargs["ttl"],
        )

    async def adopt_contract(
        self,
        slot: Round5WarmSlot,
        *,
        warm_contract_sha256: str,
        now: datetime,
    ) -> Round5WarmSlot:
        _digest(warm_contract_sha256, "warm_contract_sha256")
        if slot.state in {
            Round5WarmState.CLAIMED,
            Round5WarmState.RUNNING,
            Round5WarmState.CLEANING,
        }:
            raise WarmFenceLostError("an active claim must clean before contract adoption")
        return await self._mutate(
            slot,
            "warm_started",
            now=now,
            generation=slot.generation + 1,
            state=Round5WarmState.WARMING,
            warm_contract_sha256=warm_contract_sha256,
            warming_started_at=now,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            warm_attempt_token=None,
            shared_receipt=None,
            variants={},
            claim=None,
            bell_id=None,
            bell_at_utc=None,
            attempt_count=0,
            next_retry_at=None,
            last_error_code=None,
            last_error_at=None,
        )

    async def record_attempt(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        _safe_identifier(kwargs["warm_attempt_token"], "warm_attempt_token")
        return await self._mutate(
            slot,
            "warm_step",
            now=kwargs["now"],
            last_attempt_at=kwargs["now"],
            attempt_count=slot.attempt_count + 1,
            next_retry_at=None,
            warm_attempt_token=kwargs["warm_attempt_token"],
        )

    async def publish_ready(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        helper = InMemoryRound5WarmStore()
        helper._slots[slot.installation_id] = slot
        updated = await helper.publish_ready(slot, **kwargs)
        return await self._persist(slot, updated, "warm_ready", now=kwargs["now"])

    async def record_retry(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        return await self._mutate(
            slot,
            "warm_retry",
            now=kwargs["now"],
            state=Round5WarmState.WARMING,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            next_retry_at=kwargs["next_retry_at"],
            last_error_code=_safe_code(kwargs["code"]),
            last_error_at=kwargs["now"],
        )

    async def record_blocked(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        return await self._mutate(
            slot,
            "warm_blocked",
            now=kwargs["now"],
            state=Round5WarmState.BLOCKED,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            next_retry_at=None,
            last_error_code=_safe_code(kwargs["code"]),
            last_error_at=kwargs["now"],
        )

    async def freshness_lost(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        return await self._mutate(
            slot,
            "warm_freshness_lost",
            now=kwargs["now"],
            state=Round5WarmState.WARMING,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            last_error_code=_safe_code(kwargs["code"]),
            last_error_at=kwargs["now"],
        )

    async def update_capsule_receipt(
        self, slot: Round5WarmSlot, **kwargs: Any
    ) -> Round5WarmSlot:
        capsule: Round5LaunchCapsule = kwargs["capsule"]
        now: datetime = kwargs["now"]
        if not capsule.meets_launch_margin(now):
            raise BlockedWarmError("credential_margin_insufficient")
        ready_expires_at = min(_receipt_expiry_bound(slot), capsule.expires_at)
        renew_by = min(ready_expires_at, capsule.renew_by)
        if renew_by <= now:
            raise BlockedWarmError("credential_refresh_expired")
        return await self._mutate(
            slot,
            "warm_capsule_refreshed",
            now=now,
            broker_epoch=capsule.broker_epoch,
            ready_expires_at=ready_expires_at,
            renew_by=renew_by,
        )

    async def renew_provenance(
        self,
        slot: Round5WarmSlot,
        **kwargs: Any,
    ) -> Round5WarmSlot:
        now: datetime = kwargs["now"]
        return await self._mutate(
            slot,
            "runner_provenance_refreshed",
            now=now,
            provenance_expires_at=min(
                _receipt_expiry_bound(slot),
                now + kwargs["ttl"],
            ),
        )

    async def claim_ready(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        claim: Round5BoutClaim = kwargs["claim"]
        now: datetime = kwargs["now"]
        if (
            slot.state != Round5WarmState.READY
            or slot.ready_expires_at is None
            or slot.ready_expires_at <= now
            or slot.renew_by is None
            or slot.renew_by <= now
            or slot.provenance_expires_at is None
            or slot.provenance_expires_at <= now
            or _receipt_expiry_bound(slot) <= now
        ):
            raise WarmClaimUnavailableError("Round 5 has no current READY generation")
        return await self._mutate(
            slot,
            "claim_created",
            now=now,
            state=Round5WarmState.CLAIMED,
            claim=claim,
        )

    async def claim_ready_with_leases(
        self,
        slot: Round5WarmSlot,
        *,
        session_id: str,
        bout_id: str,
        selected_variant: Round5Variant,
        capsule_generation: int,
        main_ring_key: str,
        cleanup_ring_key: str,
        operator: object,
        round_id: str,
        round_title: str,
        competitor_id: str,
        competitor_name: str,
        now: datetime,
        ttl: timedelta,
        claim_ttl: timedelta,
    ) -> tuple[Round5WarmSlot, object, object]:
        """Atomically claim both ring rows and the READY warm generation."""

        from .coordination import COORDINATION_TABLE, BoutLease, owner_subject
        from .models import SessionState

        now = _utc(now, "now")
        claim_id = f"claim-{uuid4().hex}"
        bell_id = f"bell-{uuid4().hex}"

        async def commit(cursor: Any) -> tuple[Round5WarmSlot, BoutLease, BoutLease]:
            async with cursor.connection.transaction():
                await cursor.execute(
                    f"""
                    SELECT ring_key, fencing_token, lease_id, expires_at
                    FROM {COORDINATION_TABLE}
                    WHERE ring_key = ANY(%s)
                    ORDER BY ring_key
                    FOR UPDATE
                    """,
                    ([main_ring_key, cleanup_ring_key],),
                )
                rows = await cursor.fetchall()
                by_key = {str(row[0]): row for row in rows}
                if set(by_key) != {main_ring_key, cleanup_ring_key}:
                    raise WarmClaimUnavailableError(
                        "Round 5 ring rows are unavailable"
                    )
                await cursor.execute(
                    f"""
                    SELECT payload
                    FROM {ROUND5_WARM_SLOT_TABLE}
                    WHERE installation_id = %s AND slot_ordinal = %s
                    FOR UPDATE
                    """,
                    (slot.installation_id, slot.slot_ordinal),
                )
                warm_row = await cursor.fetchone()
                if warm_row is None:
                    raise WarmClaimUnavailableError("Round 5 warm slot is unavailable")
                raw = (
                    warm_row[0]
                    if isinstance(warm_row[0], Mapping)
                    else json.loads(str(warm_row[0]))
                )
                current = _slot_from_json(raw)
                await cursor.execute("SELECT clock_timestamp()")
                clock_row = await cursor.fetchone()
                if (
                    clock_row is None
                    or not isinstance(clock_row[0], datetime)
                    or clock_row[0].tzinfo is None
                ):
                    raise WarmClaimUnavailableError(
                        "Round 5 database clock is unavailable"
                    )
                locked_now = clock_row[0]
                if any(
                    row[2] is not None
                    and row[3] is not None
                    and row[3] > locked_now
                    for row in by_key.values()
                ):
                    raise WarmClaimUnavailableError("Round 5 ring is already held")
                if (
                    current.revision != slot.revision
                    or current.coordinator_fence != slot.coordinator_fence
                    or current.state != Round5WarmState.READY
                    or current.ready_expires_at is None
                    or current.ready_expires_at <= locked_now
                    or current.renew_by is None
                    or current.renew_by <= locked_now
                    or current.provenance_expires_at is None
                    or current.provenance_expires_at <= locked_now
                    or _receipt_expiry_bound(current) <= locked_now
                    or capsule_generation != current.generation
                ):
                    raise WarmClaimUnavailableError(
                        "Round 5 READY generation changed while claiming"
                    )
                fences = {
                    key: int(row[1]) + 1 for key, row in by_key.items()
                }
                claim = Round5BoutClaim(
                    claim_id=claim_id,
                    bell_id=bell_id,
                    session_id=session_id,
                    bout_id=bout_id,
                    selected_variant=selected_variant,
                    bout_fence=fences[cleanup_ring_key],
                    claimed_at=locked_now,
                    claim_expires_at=locked_now + claim_ttl,
                    capsule_generation=capsule_generation,
                    lakebase_job_id=stable_round5_id(
                        ROUND5_WARM_PROTOCOL,
                        str(current.generation),
                        bell_id,
                        "lakebase",
                    ),
                    competitor_job_id=stable_round5_id(
                        ROUND5_WARM_PROTOCOL,
                        str(current.generation),
                        bell_id,
                        "competitor",
                    ),
                    warm_attempt_token=round5_warm_attempt_token(current),
                )
                updated = replace(
                    current,
                    state=Round5WarmState.CLAIMED,
                    revision=current.revision + 1,
                    claim=claim,
                )
                subject = owner_subject(operator)
                leases: dict[str, BoutLease] = {}
                for ring_key in (main_ring_key, cleanup_ring_key):
                    lease_id = str(uuid4())
                    await cursor.execute(
                        f"""
                        UPDATE {COORDINATION_TABLE}
                        SET fencing_token = %s,
                            lease_id = %s::uuid,
                            session_id = %s,
                            owner_subject = %s,
                            owner_display_name = %s,
                            owner_email = %s,
                            phase = 'checking',
                            session_state = 'checking',
                            round_id = %s,
                            round_title = %s,
                            competitor_id = %s,
                            competitor_name = %s,
                            started_at = %s,
                            updated_at = %s,
                            expires_at = %s
                        WHERE ring_key = %s
                        RETURNING started_at, updated_at, expires_at
                        """,
                        (
                            fences[ring_key],
                            lease_id,
                            session_id,
                            subject,
                            operator.display_name,
                            operator.email,
                            round_id,
                            round_title,
                            competitor_id,
                            competitor_name,
                            locked_now,
                            locked_now,
                            locked_now + ttl,
                            ring_key,
                        ),
                    )
                    lease_row = await cursor.fetchone()
                    if lease_row is None:
                        raise WarmClaimUnavailableError(
                            "Round 5 ring changed while claiming"
                        )
                    leases[ring_key] = BoutLease(
                        lease_id=lease_id,
                        fencing_token=fences[ring_key],
                        session_id=session_id,
                        operator=operator,
                        owner_subject=subject,
                        phase="checking",
                        session_state=SessionState.CHECKING,
                        round_id=round_id,
                        round_title=round_title,
                        competitor_id=competitor_id,
                        competitor_name=competitor_name,
                        started_at=lease_row[0],
                        updated_at=lease_row[1],
                        expires_at=lease_row[2],
                    )
                event = InMemoryRound5WarmStore()._event(
                    current,
                    updated,
                    "claim_created",
                    now=locked_now,
                )
                await cursor.execute(
                    f"""
                    UPDATE {ROUND5_WARM_SLOT_TABLE}
                    SET generation = %s, state = %s, revision = %s,
                        coordinator_fence = %s, process_epoch = %s,
                        broker_epoch = %s, payload = %s::jsonb, updated_at = %s
                    WHERE installation_id = %s AND slot_ordinal = %s
                    """,
                    (
                        updated.generation,
                        updated.state.value,
                        updated.revision,
                        updated.coordinator_fence,
                        updated.process_epoch,
                        updated.broker_epoch,
                        json.dumps(_to_json(updated), sort_keys=True, separators=(",", ":")),
                        locked_now,
                        updated.installation_id,
                        updated.slot_ordinal,
                    ),
                )
                await cursor.execute(
                    f"""
                    INSERT INTO {ROUND5_WARM_EVENT_TABLE} (
                        event_id, idempotency_key, installation_id, slot_ordinal,
                        generation, revision, event_type, from_state, to_state,
                        occurred_at, payload
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    """,
                    (
                        event.event_id,
                        event.idempotency_key,
                        event.installation_id,
                        event.slot_ordinal,
                        event.generation,
                        event.revision,
                        event.event_type,
                        event.from_state.value if event.from_state else None,
                        event.to_state.value,
                        event.occurred_at,
                        json.dumps(_to_json(event), sort_keys=True, separators=(",", ":")),
                    ),
                )
                return (
                    updated,
                    leases[main_ring_key],
                    leases[cleanup_ring_key],
                )

        return await self._run(commit)

    async def release_expired_claim(
        self, slot: Round5WarmSlot, **kwargs: Any
    ) -> Round5WarmSlot:
        helper = InMemoryRound5WarmStore()
        helper._slots[slot.installation_id] = slot
        updated = await helper.release_expired_claim(slot, **kwargs)
        if updated is slot:
            return slot
        return await self._persist(slot, updated, "claim_released", now=kwargs["now"])

    async def renew_claim(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        now: datetime = _utc(kwargs["now"], "now")
        claim_id = str(kwargs["claim_id"])
        ttl: timedelta = kwargs["ttl"]
        if (
            slot.state != Round5WarmState.CLAIMED
            or slot.claim is None
            or slot.claim.claim_id != claim_id
        ):
            raise WarmFenceLostError("Round 5 claim changed before renewal")
        return await self._mutate(
            slot,
            "claim_renewed",
            now=now,
            claim=replace(slot.claim, claim_expires_at=now + ttl),
        )

    async def accept_bell(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        if slot.state == Round5WarmState.RUNNING and slot.bell_id == kwargs["bell_id"]:
            return slot
        claim = slot.claim
        if (
            slot.state != Round5WarmState.CLAIMED
            or claim is None
            or claim.claim_id != kwargs["claim_id"]
        ):
            raise WarmFenceLostError("Round 5 claim changed before bell")
        return await self._mutate(
            slot,
            "bell_accepted",
            now=kwargs["bell_at_utc"],
            state=Round5WarmState.RUNNING,
            bell_id=kwargs["bell_id"],
            bell_at_utc=kwargs["bell_at_utc"],
        )

    async def accept_bell_with_leases(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        bell_id: str,
        bell_at_utc: datetime,
        main_ring_key: str,
        main_lease: object,
        cleanup_ring_key: str,
        cleanup_lease: object,
        ttl: timedelta,
        release_event: Round5ControlEvent | None = None,
    ) -> tuple[Round5WarmSlot, datetime, datetime, datetime, datetime]:
        """Atomically commit both ring rows, the warm slot, and its event."""

        from .coordination import COORDINATION_TABLE

        if (
            slot.state != Round5WarmState.CLAIMED
            or slot.claim is None
            or slot.claim.claim_id != claim_id
        ):
            raise WarmFenceLostError("Round 5 claim changed before bell")
        if (
            release_event is None
            or release_event.kind != Round5ControlKind.RELEASE
            or release_event.sequence != 2
            or release_event.binding.installation_id != slot.installation_id
            or release_event.binding.generation != slot.generation
            or release_event.binding.warm_attempt_token
            != slot.claim.warm_attempt_token
            or release_event.binding.claim_id != claim_id
            or release_event.binding.bout_id != slot.claim.bout_id
            or release_event.binding.bell_id != bell_id
            or release_event.binding.fence != slot.claim.bout_fence
            or release_event.binding.job_id != slot.claim.lakebase_job_id
            or release_event.lane_id != "lakebase"
            or release_event.created_at != bell_at_utc
        ):
            raise WarmFenceLostError(
                "Round 5 bell release binding is incomplete"
            )
        updated = replace(
            slot,
            state=Round5WarmState.RUNNING,
            revision=slot.revision + 1,
            bell_id=bell_id,
            bell_at_utc=bell_at_utc,
        )
        event = InMemoryRound5WarmStore()._event(
            slot,
            updated,
            "bell_accepted",
            now=bell_at_utc,
        )
        payload = json.dumps(_to_json(updated), sort_keys=True, separators=(",", ":"))
        event_payload = json.dumps(_to_json(event), sort_keys=True, separators=(",", ":"))
        release = release_event
        release_payload = json.dumps(
            release.wire_value(),
            sort_keys=True,
            separators=(",", ":"),
        )

        async def commit(cursor: Any) -> tuple[datetime, datetime, datetime, datetime] | None:
            await cursor.execute(
                f"""
                WITH guard AS MATERIALIZED (
                    SELECT 1
                    FROM {COORDINATION_TABLE} AS m
                    JOIN {COORDINATION_TABLE} AS c ON c.ring_key = %s
                    JOIN {ROUND5_WARM_SLOT_TABLE} AS w
                      ON w.installation_id = %s AND w.slot_ordinal = %s
                    WHERE m.ring_key = %s
                      AND m.lease_id = %s::uuid
                      AND m.fencing_token = %s
                      AND m.session_id = %s
                      AND m.owner_subject = %s
                      AND m.phase = 'armed'
                      AND m.expires_at > clock_timestamp()
                      AND c.lease_id = %s::uuid
                      AND c.fencing_token = %s
                      AND c.session_id = %s
                      AND c.owner_subject = %s
                      AND c.phase = 'armed'
                      AND c.expires_at > clock_timestamp()
                      AND w.generation = %s
                      AND w.revision = %s
                      AND w.coordinator_fence = %s
                      AND w.state = 'claimed'
                      AND w.payload->'claim'->>'claim_id' = %s
                      AND EXISTS (
                          SELECT 1
                          FROM {ROUND5_CONTROL_OUTBOX_TABLE} AS s
                          WHERE s.installation_id = %s
                            AND s.lane_id = 'lakebase'
                            AND s.generation = %s
                            AND s.warm_attempt_token = %s
                            AND s.job_id = %s
                            AND s.sequence = 1
                            AND s.kind = 'stage'
                            AND s.payload->'binding' = %s::jsonb
                      )
                    FOR UPDATE OF m, c, w
                ), main AS (
                    UPDATE {COORDINATION_TABLE}
                    SET phase = 'run_committed',
                        session_state = 'running',
                        updated_at = clock_timestamp(),
                        expires_at = clock_timestamp() + %s
                    WHERE ring_key = %s
                      AND lease_id = %s::uuid
                      AND fencing_token = %s
                      AND session_id = %s
                      AND owner_subject = %s
                      AND phase = 'armed'
                      AND expires_at > clock_timestamp()
                      AND EXISTS (SELECT 1 FROM guard)
                    RETURNING updated_at, expires_at
                ), cleanup AS (
                    UPDATE {COORDINATION_TABLE}
                    SET phase = 'run_committed',
                        session_state = 'running',
                        updated_at = clock_timestamp(),
                        expires_at = clock_timestamp() + %s
                    WHERE ring_key = %s
                      AND lease_id = %s::uuid
                      AND fencing_token = %s
                      AND session_id = %s
                      AND owner_subject = %s
                      AND phase = 'armed'
                      AND expires_at > clock_timestamp()
                      AND EXISTS (SELECT 1 FROM main)
                    RETURNING updated_at, expires_at
                ), warm AS (
                    UPDATE {ROUND5_WARM_SLOT_TABLE}
                    SET generation = %s,
                        state = %s,
                        revision = %s,
                        coordinator_fence = %s,
                        process_epoch = %s,
                        broker_epoch = %s,
                        payload = %s::jsonb,
                        updated_at = %s
                    WHERE installation_id = %s
                      AND slot_ordinal = %s
                      AND generation = %s
                      AND revision = %s
                      AND coordinator_fence = %s
                      AND state = 'claimed'
                      AND payload->'claim'->>'claim_id' = %s
                      AND EXISTS (SELECT 1 FROM cleanup)
                    RETURNING 1
                ), appended AS (
                    INSERT INTO {ROUND5_WARM_EVENT_TABLE} (
                        event_id, idempotency_key, installation_id, slot_ordinal,
                        generation, revision, event_type, from_state, to_state,
                        occurred_at, payload
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    FROM warm
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING 1
                ), released AS (
                    INSERT INTO {ROUND5_CONTROL_OUTBOX_TABLE} (
                        event_id, installation_id, lane_id, generation,
                        warm_attempt_token, job_id, sequence, kind, payload,
                        created_at
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
                    FROM appended
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING 1
                )
                SELECT main.updated_at, main.expires_at,
                       cleanup.updated_at, cleanup.expires_at
                FROM main CROSS JOIN cleanup CROSS JOIN warm
                CROSS JOIN appended CROSS JOIN released
                """,
                (
                    cleanup_ring_key,
                    updated.installation_id,
                    updated.slot_ordinal,
                    main_ring_key,
                    main_lease.lease_id,
                    main_lease.fencing_token,
                    main_lease.session_id,
                    main_lease.owner_subject,
                    cleanup_lease.lease_id,
                    cleanup_lease.fencing_token,
                    cleanup_lease.session_id,
                    cleanup_lease.owner_subject,
                    slot.generation,
                    slot.revision,
                    slot.coordinator_fence,
                    claim_id,
                    release.installation_id,
                    release.generation,
                    release.binding.warm_attempt_token,
                    release.job_id,
                    json.dumps(
                        release.binding.wire_value(),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    ttl,
                    main_ring_key,
                    main_lease.lease_id,
                    main_lease.fencing_token,
                    main_lease.session_id,
                    main_lease.owner_subject,
                    ttl,
                    cleanup_ring_key,
                    cleanup_lease.lease_id,
                    cleanup_lease.fencing_token,
                    cleanup_lease.session_id,
                    cleanup_lease.owner_subject,
                    updated.generation,
                    updated.state.value,
                    updated.revision,
                    updated.coordinator_fence,
                    updated.process_epoch,
                    updated.broker_epoch,
                    payload,
                    bell_at_utc,
                    updated.installation_id,
                    updated.slot_ordinal,
                    slot.generation,
                    slot.revision,
                    slot.coordinator_fence,
                    claim_id,
                    event.event_id,
                    event.idempotency_key,
                    event.installation_id,
                    event.slot_ordinal,
                    event.generation,
                    event.revision,
                    event.event_type,
                    event.from_state.value if event.from_state else None,
                    event.to_state.value,
                    event.occurred_at,
                    event_payload,
                    release.event_id,
                    release.installation_id,
                    release.lane_id,
                    release.generation,
                    release.binding.warm_attempt_token,
                    release.job_id,
                    release.sequence,
                    release.kind.value,
                    release_payload,
                    release.created_at,
                ),
            )
            row = await cursor.fetchone()
            return tuple(row) if row is not None else None  # type: ignore[return-value]

        async def read_back(
            cursor: Any,
        ) -> tuple[datetime, datetime, datetime, datetime] | None:
            """Prove — after an ambiguous commit — that the bell exactly landed.

            A dropped socket during ``COMMIT`` (or an idempotent resend that the
            CTE's ``ON CONFLICT DO NOTHING`` collapsed to zero returned rows)
            leaves ``commit`` returning ``None`` even though the four-row bell
            transaction actually committed.  Rather than falsely reporting a
            rollback and re-arming a bout that is already RUNNING, read the three
            rows and the release outbox back and require that every field equals
            the exact identity this call would have written.  Only an exact match
            rejoins; anything else is a genuine loss.
            """

            await cursor.execute(
                f"""
                SELECT
                    m.phase, m.updated_at, m.expires_at,
                    m.lease_id::text, m.fencing_token, m.session_id, m.owner_subject,
                    c.phase, c.updated_at, c.expires_at,
                    c.lease_id::text, c.fencing_token, c.session_id, c.owner_subject,
                    w.state, w.generation, w.revision, w.coordinator_fence,
                    w.payload->>'bell_id',
                    w.payload->'claim'->>'claim_id',
                    EXISTS (
                        SELECT 1 FROM {ROUND5_CONTROL_OUTBOX_TABLE} AS s
                        WHERE s.event_id = %s
                    )
                FROM {COORDINATION_TABLE} AS m
                JOIN {COORDINATION_TABLE} AS c ON c.ring_key = %s
                JOIN {ROUND5_WARM_SLOT_TABLE} AS w
                  ON w.installation_id = %s AND w.slot_ordinal = %s
                WHERE m.ring_key = %s
                """,
                (
                    release.event_id,
                    cleanup_ring_key,
                    updated.installation_id,
                    updated.slot_ordinal,
                    main_ring_key,
                ),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            (
                main_phase,
                main_updated_at,
                main_expires_at,
                main_lease_id,
                main_fence,
                main_session,
                main_owner,
                cleanup_phase,
                cleanup_updated_at,
                cleanup_expires_at,
                cleanup_lease_id,
                cleanup_fence,
                cleanup_session,
                cleanup_owner,
                warm_state,
                warm_generation,
                warm_revision,
                warm_coordinator_fence,
                warm_bell_id,
                warm_claim_id,
                release_present,
            ) = row
            if (
                main_phase != "run_committed"
                or str(main_lease_id) != str(main_lease.lease_id)
                or int(main_fence) != int(main_lease.fencing_token)
                or str(main_session) != str(main_lease.session_id)
                or str(main_owner) != str(main_lease.owner_subject)
                or cleanup_phase != "run_committed"
                or str(cleanup_lease_id) != str(cleanup_lease.lease_id)
                or int(cleanup_fence) != int(cleanup_lease.fencing_token)
                or str(cleanup_session) != str(cleanup_lease.session_id)
                or str(cleanup_owner) != str(cleanup_lease.owner_subject)
                or str(warm_state) != Round5WarmState.RUNNING.value
                or int(warm_generation) != updated.generation
                or int(warm_revision) != updated.revision
                or int(warm_coordinator_fence) != updated.coordinator_fence
                or str(warm_bell_id) != bell_id
                or str(warm_claim_id) != claim_id
                or not bool(release_present)
            ):
                return None
            return (
                main_updated_at,
                main_expires_at,
                cleanup_updated_at,
                cleanup_expires_at,
            )

        times = await self._run(commit)
        if times is None:
            times = await self._run(read_back)
        if times is None:
            raise WarmFenceLostError("Round 5 bell transaction was rolled back")
        return updated, *times

    async def begin_cleanup(self, slot: Round5WarmSlot, **kwargs: Any) -> Round5WarmSlot:
        if slot.state == Round5WarmState.CLEANING and slot.claim is not None:
            if slot.claim.claim_id == kwargs["claim_id"]:
                return slot
        if (
            slot.claim is None
            or slot.claim.claim_id != kwargs["claim_id"]
            or slot.state not in {Round5WarmState.CLAIMED, Round5WarmState.RUNNING}
        ):
            raise WarmFenceLostError("Round 5 claim changed before cleanup")
        return await self._mutate(
            slot,
            "cleanup_started",
            now=kwargs["now"],
            state=Round5WarmState.CLEANING,
        )

    async def finish_cleanup_and_rewarm(
        self, slot: Round5WarmSlot, **kwargs: Any
    ) -> Round5WarmSlot:
        if (
            slot.state != Round5WarmState.CLEANING
            or slot.claim is None
            or slot.claim.claim_id != kwargs["claim_id"]
        ):
            raise WarmFenceLostError("Round 5 cleanup fence changed")
        now: datetime = kwargs["now"]
        return await self._mutate(
            slot,
            "rewarm_enqueued",
            now=now,
            generation=slot.generation + 1,
            state=Round5WarmState.WARMING,
            warm_contract_sha256=(
                kwargs.get("warm_contract_sha256")
                or slot.warm_contract_sha256
            ),
            warming_started_at=now,
            ready_at=None,
            ready_expires_at=None,
            renew_by=None,
            provenance_expires_at=None,
            warm_attempt_token=None,
            shared_receipt=None,
            variants={},
            claim=None,
            bell_id=None,
            bell_at_utc=None,
            attempt_count=0,
            next_retry_at=None,
            last_error_code=None,
            last_error_at=None,
        )


class Round5WarmCoordinator:
    """Supervised automatic warm loop and O(1) request-path claim boundary."""

    def __init__(
        self,
        *,
        installation_id: str,
        warm_contract_sha256: str,
        store: Round5WarmStore,
        provider: Round5WarmProvider,
        process_epoch: str | None = None,
        broker_epoch: str | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_source: random.Random | None = None,
        claim_ttl_seconds: float = DEFAULT_CLAIM_TTL_SECONDS,
        coordinator_ttl_seconds: float = DEFAULT_COORDINATOR_TTL_SECONDS,
        retry_ceiling_seconds: float = DEFAULT_RETRY_CEILING_SECONDS,
    ) -> None:
        self.installation_id = _safe_identifier(installation_id, "installation_id")
        self.warm_contract_sha256 = _digest(
            warm_contract_sha256, "warm_contract_sha256"
        )
        self.store = store
        self.provider = provider
        self.process_epoch = process_epoch or f"process-{uuid4().hex}"
        self.broker_epoch = broker_epoch or f"broker-{uuid4().hex}"
        _safe_identifier(self.process_epoch, "process_epoch")
        _safe_identifier(self.broker_epoch, "broker_epoch")
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic_ns = monotonic_ns
        self._sleep = sleep
        self._random = random_source or random.Random()
        self._claim_ttl = timedelta(seconds=claim_ttl_seconds)
        self._coordinator_ttl = timedelta(seconds=coordinator_ttl_seconds)
        self._retry_ceiling_seconds = retry_ceiling_seconds
        self._capsule: Round5LaunchCapsule | None = None
        self._wake = asyncio.Event()
        self._closed = False
        self._task: asyncio.Task[None] | None = None
        self._bell_contexts: dict[str, BellContext] = {}
        self._last_slot: Round5WarmSlot | None = None
        self._inherited_claim_ids: set[str] = set()
        self._verified_clean_claim_ids: set[str] = set()
        self._cleanup_origins: dict[str, tuple[int, int, str | None]] = {}
        self._completed_cleanups: dict[str, tuple[int, int, str | None]] = {}
        self._local_readiness_error_code: str | None = None
        self._active_claim_ids: set[str] = set()

    @property
    def capsule(self) -> Round5LaunchCapsule | None:
        return self._capsule

    @property
    def ring_ready(self) -> bool:
        slot = self._last_slot
        return bool(
            slot is not None
            and self._claimable(slot, self._clock())
        )

    async def start(self) -> asyncio.Task[None]:
        """Start warming without waiting for the potentially hour-long provider work."""

        if self._task is not None and not self._task.done():
            return self._task
        await self.store.initialize()
        self._last_slot = await self.store.ensure_warming(
            installation_id=self.installation_id,
            warm_contract_sha256=self.warm_contract_sha256,
            process_epoch=self.process_epoch,
            broker_epoch=self.broker_epoch,
            now=self._clock(),
        )
        self._task = asyncio.create_task(
            self.run(),
            name=f"round5-warm-{self.installation_id}",
        )
        self._task.add_done_callback(
            lambda task: task.exception() if not task.cancelled() else None
        )
        return self._task

    async def close(self) -> None:
        self._closed = True
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        self._capsule = None
        await self.store.close()

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        transient_failures = 0
        while not self._closed:
            try:
                delay = await self.run_one_cycle()
            except asyncio.CancelledError:
                raise
            except (WarmCoordinatorHeldError, WarmStoreConflictError, WarmFenceLostError):
                delay = 1.0
                transient_failures = 0
            except (RetryableWarmError, BlockedWarmError):
                delay = min(5.0, self._retry_ceiling_seconds)
                transient_failures = 0
            except Exception:
                # A transient store or transport fault (e.g. a dropped database
                # connection during the hour-long warm) must never kill the
                # supervised loop: warming would then stop permanently with no
                # coordinator to recover it.  Log with a doubling cadence and
                # retry under a capped backoff so the loop stays alive.
                transient_failures += 1
                if transient_failures & (transient_failures - 1) == 0:
                    logger.warning(
                        "round5_warm_cycle_failed installation=%s "
                        "consecutive_failures=%d",
                        self.installation_id,
                        transient_failures,
                        exc_info=True,
                    )
                exponent = min(10, transient_failures - 1)
                delay = min(
                    self._retry_ceiling_seconds,
                    max(0.1, float(2**exponent) / 4),
                )
            else:
                transient_failures = 0
            self._wake.clear()
            wake = asyncio.create_task(self._wake.wait())
            timer = asyncio.create_task(self._sleep(max(0.0, delay)))
            try:
                await asyncio.wait((wake, timer), return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in (wake, timer):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(wake, timer, return_exceptions=True)

    async def _run_holding_lease(
        self,
        holder: list[Round5WarmSlot],
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Run a possibly hour-long provider call while renewing the lease.

        The coordinator lease TTL (default 90s) is far shorter than the
        ``provider.reconcile``/``provider.prepare`` work it protects (up to an
        hour).  Without renewal the lease expires mid-preparation, another
        process seizes coordination, and the eventual ``publish_ready`` loses
        its fence.  A background beat renews the lease on a third of its TTL and
        publishes the freshest slot revision into ``holder[0]`` so the caller
        compare-and-swaps against the latest revision after the work returns.
        Leadership loss ends the beat quietly; the caller's own CAS then fails
        and the supervised loop recovers.
        """

        stop = asyncio.Event()
        interval = max(0.02, self._coordinator_ttl.total_seconds() / 3)

        async def beat() -> None:
            while True:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                    return
                except TimeoutError:
                    pass
                try:
                    holder[0] = await self.store.heartbeat_coordinator(
                        holder[0],
                        now=self._clock(),
                        ttl=self._coordinator_ttl,
                    )
                except (WarmFenceLostError, WarmStoreConflictError):
                    return

        beat_task = asyncio.create_task(beat())
        try:
            return await operation()
        finally:
            stop.set()
            await asyncio.gather(beat_task, return_exceptions=True)

    async def run_one_cycle(self) -> float:
        now = self._clock()
        slot = await self.store.ensure_warming(
            installation_id=self.installation_id,
            warm_contract_sha256=self.warm_contract_sha256,
            process_epoch=self.process_epoch,
            broker_epoch=self.broker_epoch,
            now=now,
        )
        if (
            slot.state
            in {
                Round5WarmState.CLAIMED,
                Round5WarmState.RUNNING,
                Round5WarmState.CLEANING,
            }
            and slot.claim is not None
            and slot.process_epoch != self.process_epoch
        ):
            self._inherited_claim_ids.add(slot.claim.claim_id)
        slot = await self.store.acquire_coordinator(
            installation_id=self.installation_id,
            process_epoch=self.process_epoch,
            broker_epoch=self.broker_epoch,
            now=now,
            ttl=self._coordinator_ttl,
        )
        self._last_slot = slot
        if (
            slot.warm_contract_sha256 != self.warm_contract_sha256
            and slot.state
            not in {
                Round5WarmState.CLAIMED,
                Round5WarmState.RUNNING,
                Round5WarmState.CLEANING,
            }
        ):
            slot = await self.store.adopt_contract(
                slot,
                warm_contract_sha256=self.warm_contract_sha256,
                now=self._clock(),
            )
            self._last_slot = slot
        if (
            slot.state == Round5WarmState.CLEANING
            and slot.claim is not None
            and slot.claim.claim_id in self._verified_clean_claim_ids
        ):
            self._last_slot = await self._finish_cleanup_transition(
                slot.claim.claim_id
            )
            self._capsule = None
            return 0.0
        if slot.state in {Round5WarmState.RUNNING, Round5WarmState.CLEANING}:
            if (
                slot.claim is None
                or slot.claim.claim_id not in self._inherited_claim_ids
            ):
                return 1.0
            reconciled = await self.provider.reconcile(slot)
            if reconciled and slot.claim is not None:
                claim_id = slot.claim.claim_id
                cleaning = (
                    await self.store.begin_cleanup(
                        slot,
                        claim_id=claim_id,
                        now=self._clock(),
                    )
                    if slot.state == Round5WarmState.RUNNING
                    else slot
                )
                self._cleanup_origins[claim_id] = (
                    cleaning.generation,
                    cleaning.coordinator_fence,
                    cleaning.coordinator_owner,
                )
                self._verified_clean_claim_ids.add(claim_id)
                self._last_slot = await self._finish_cleanup_transition(claim_id)
                self._capsule = None
                self._inherited_claim_ids.discard(claim_id)
                return 0.0
            return 1.0
        if slot.state == Round5WarmState.CLAIMED:
            # A claim held by an operator that is still actively arming is
            # renewed backstage; a claim whose owner has gone away is left to
            # expire and be released.  Membership in ``_active_claim_ids`` is the
            # in-memory "operator still present" signal, registered when this
            # process hands the claim to the arm path and cleared at the bell or
            # on release.  It is intentionally not durable: after a restart the
            # new process will not renew an unknown claim, so it expires safely.
            if (
                slot.claim is not None
                and slot.claim.claim_id in self._active_claim_ids
                and self._capsule_current(slot, now)
            ):
                renewed = await self.store.renew_claim(
                    slot,
                    claim_id=slot.claim.claim_id,
                    now=now,
                    ttl=self._claim_ttl,
                )
                self._last_slot = renewed
                return max(0.1, self._claim_ttl.total_seconds() / 3)
            still_fresh = self._capsule_current(slot, now)
            released = await self.store.release_expired_claim(
                slot,
                now=now,
                still_fresh=still_fresh,
            )
            self._last_slot = released
            return 0.0 if released.state == Round5WarmState.WARMING else 1.0
        if slot.state == Round5WarmState.READY:
            if not self._capsule_current(slot, now):
                self._capsule = None
                self._last_slot = await self.store.freshness_lost(
                    slot,
                    code="launch_capsule_missing",
                    now=now,
                )
                return 0.0
            assert self._capsule is not None
            try:
                provenance_current = await self.provider.validate_ready(
                    slot,
                    self._capsule,
                )
            except asyncio.CancelledError:
                raise
            except (RetryableWarmError, BlockedWarmError) as exc:
                provenance_current = False
                provenance_code = exc.code
            else:
                provenance_code = "runner_provenance_changed"
            if not provenance_current:
                self._capsule = None
                self._last_slot = await self.store.freshness_lost(
                    slot,
                    code=provenance_code,
                    now=self._clock(),
                )
                return 0.0
            slot = await self.store.renew_provenance(
                slot,
                now=self._clock(),
                ttl=timedelta(seconds=PROVENANCE_FRESHNESS_SECONDS),
            )
            self._last_slot = slot
            if slot.renew_by is not None and slot.renew_by <= now:
                try:
                    refreshed = await self.provider.refresh_capsule(slot, self._capsule)
                    if not refreshed.meets_launch_margin(now):
                        raise BlockedWarmError("credential_margin_insufficient")
                    slot = await self.store.update_capsule_receipt(
                        slot,
                        capsule=refreshed,
                        now=now,
                    )
                    self._last_slot = slot
                    self._capsule = refreshed
                except asyncio.CancelledError:
                    raise
                except (RetryableWarmError, BlockedWarmError) as exc:
                    self._capsule = None
                    self._last_slot = await self.store.freshness_lost(
                        slot,
                        code=exc.code,
                        now=now,
                    )
                    return 0.0
            remaining = (
                (slot.renew_by - now).total_seconds()
                if slot.renew_by is not None
                else self._coordinator_ttl.total_seconds() / 2
            )
            provenance_remaining = (
                (slot.provenance_expires_at - now).total_seconds()
                if slot.provenance_expires_at is not None
                else 0.0
            )
            return max(
                0.1,
                min(
                    remaining,
                    provenance_remaining / 2,
                    self._coordinator_ttl.total_seconds() / 2,
                ),
            )
        if slot.state == Round5WarmState.BLOCKED:
            return self._coordinator_ttl.total_seconds() / 2
        if slot.next_retry_at is not None and slot.next_retry_at > now:
            return max(0.1, (slot.next_retry_at - now).total_seconds())

        warm_attempt_token = f"attempt-{uuid4().hex}"
        slot = await self.store.record_attempt(
            slot,
            now=now,
            warm_attempt_token=warm_attempt_token,
        )
        # ``holder`` carries the freshest slot revision out of the lease-renewing
        # beat so that ``publish_ready``/``record_retry``/``record_blocked`` all
        # compare-and-swap against the revision the heartbeat last wrote, not the
        # stale pre-preparation revision.
        holder = [slot]
        try:
            async def prepare_generation() -> Round5WarmPreparation:
                await self.provider.reconcile(holder[0])
                return await self.provider.prepare(
                    generation=holder[0].generation,
                    coordinator_fence=holder[0].coordinator_fence,
                    process_epoch=self.process_epoch,
                    broker_epoch=self.broker_epoch,
                    warm_attempt_token=warm_attempt_token,
                )

            preparation = await self._run_holding_lease(holder, prepare_generation)
            slot = holder[0]
            if not preparation.capsule.meets_launch_margin(self._clock()):
                raise BlockedWarmError("credential_margin_insufficient")
            ready = await self.store.publish_ready(
                slot,
                preparation=preparation,
                now=self._clock(),
            )
            self._last_slot = ready
            self._capsule = preparation.capsule
            self._local_readiness_error_code = None
            assert ready.renew_by is not None
            assert ready.provenance_expires_at is not None
            return max(
                0.1,
                min(
                    (ready.renew_by - self._clock()).total_seconds(),
                    (
                        ready.provenance_expires_at - self._clock()
                    ).total_seconds()
                    / 2,
                ),
            )
        except asyncio.CancelledError:
            raise
        except RetryableWarmError as exc:
            slot = holder[0]
            exponent = min(10, max(0, slot.attempt_count - 1))
            ceiling = min(self._retry_ceiling_seconds, float(2**exponent))
            delay = self._random.uniform(0.0, ceiling)
            self._last_slot = await self.store.record_retry(
                slot,
                code=exc.code,
                now=self._clock(),
                next_retry_at=self._clock() + timedelta(seconds=delay),
            )
            return delay
        except BlockedWarmError as exc:
            self._last_slot = await self.store.record_blocked(
                holder[0],
                code=exc.code,
                now=self._clock(),
            )
            return self._coordinator_ttl.total_seconds() / 2

    def _capsule_current(self, slot: Round5WarmSlot, now: datetime) -> bool:
        capsule = self._capsule
        return bool(
            capsule is not None
            and capsule.generation == slot.generation
            and capsule.coordinator_fence == slot.coordinator_fence
            and capsule.meets_launch_margin(now)
        )

    def _claimable(self, slot: Round5WarmSlot, now: datetime) -> bool:
        return bool(
            slot.state == Round5WarmState.READY
            and slot.ready_expires_at is not None
            and slot.ready_expires_at > now
            and slot.renew_by is not None
            and slot.renew_by > now
            and slot.provenance_expires_at is not None
            and slot.provenance_expires_at > now
            and self._capsule_current(slot, now)
        )

    async def claim(
        self,
        *,
        session_id: str,
        bout_id: str,
        selected_variant: Round5Variant,
        bout_fence: int,
    ) -> tuple[Round5WarmSlot, Round5LaunchCapsule]:
        """Atomically bind READY in O(1); this method performs no provider work."""

        now = self._clock()
        slot = await self.store.read(self.installation_id)
        if slot is None or not self._claimable(slot, now):
            raise WarmClaimUnavailableError("Round 5 is preparing backstage")
        capsule = self._capsule
        assert capsule is not None
        claim_id = f"claim-{uuid4().hex}"
        bell_id = f"bell-{uuid4().hex}"
        claim = Round5BoutClaim(
            claim_id=claim_id,
            bell_id=bell_id,
            session_id=session_id,
            bout_id=bout_id,
            selected_variant=selected_variant,
            bout_fence=bout_fence,
            claimed_at=now,
            claim_expires_at=now + self._claim_ttl,
            capsule_generation=capsule.generation,
            lakebase_job_id=stable_round5_id(
                ROUND5_WARM_PROTOCOL,
                str(slot.generation),
                bell_id,
                "lakebase",
            ),
            competitor_job_id=stable_round5_id(
                ROUND5_WARM_PROTOCOL,
                str(slot.generation),
                bell_id,
                "competitor",
            ),
            warm_attempt_token=round5_warm_attempt_token(slot),
        )
        claimed = await self.store.claim_ready(slot, claim=claim, now=now)
        self._last_slot = claimed
        self.mark_claim_active(claim_id)
        return claimed, capsule

    async def claim_with_leases(
        self,
        *,
        session_id: str,
        bout_id: str,
        selected_variant: Round5Variant,
        main_store: object,
        cleanup_store: object,
        operator: object,
        round_id: str,
        round_title: str,
        competitor_id: str,
        competitor_name: str,
    ) -> tuple[Round5WarmSlot, Round5LaunchCapsule, object, object]:
        """Claim the main ring, cleanup ring, and warm generation atomically."""

        atomic = getattr(self.store, "claim_ready_with_leases", None)
        if not callable(atomic):
            raise WarmClaimUnavailableError("atomic Round 5 claim is unavailable")
        now = self._clock()
        slot = await self.store.read(self.installation_id)
        if slot is None or not self._claimable(slot, now):
            raise WarmClaimUnavailableError("Round 5 is preparing backstage")
        capsule = self._capsule
        assert capsule is not None
        claimed, main_lease, cleanup_lease = await atomic(
            slot,
            session_id=session_id,
            bout_id=bout_id,
            selected_variant=selected_variant,
            capsule_generation=capsule.generation,
            main_ring_key=main_store.ring_key,
            cleanup_ring_key=cleanup_store.ring_key,
            operator=operator,
            round_id=round_id,
            round_title=round_title,
            competitor_id=competitor_id,
            competitor_name=competitor_name,
            now=now,
            ttl=self._coordinator_ttl,
            claim_ttl=self._claim_ttl,
        )
        self._last_slot = claimed
        if claimed.claim is not None:
            self.mark_claim_active(claimed.claim.claim_id)
        return claimed, capsule, main_lease, cleanup_lease

    def mark_claim_active(self, claim_id: str) -> None:
        """Signal that an operator is actively holding this claim through ARM.

        While registered, the supervised loop renews the claim's expiry backstage
        instead of releasing it, so a slow ARM (SSM capacity preflight, long
        credential mint) cannot drop the claim before the bell.  The manager
        clears the registration at the bell and on every arm-abandon path.
        """

        _safe_identifier(claim_id, "claim_id")
        self._active_claim_ids.add(claim_id)

    def release_claim_active(self, claim_id: str) -> None:
        """Stop backstage renewal so an abandoned claim can expire and release."""

        self._active_claim_ids.discard(claim_id)

    async def renew_claim(self, claim_id: str) -> Round5WarmSlot:
        """Renew a still-held claim's expiry through the durable store (no AWS)."""

        slot = await self.store.read(self.installation_id)
        if slot is None or slot.claim is None or slot.claim.claim_id != claim_id:
            raise WarmFenceLostError("Round 5 claim is unavailable")
        renewed = await self.store.renew_claim(
            slot,
            claim_id=claim_id,
            now=self._clock(),
            ttl=self._claim_ttl,
        )
        self._last_slot = renewed
        return renewed

    async def accept_bell(self, claim_id: str) -> BellContext:
        existing = self._bell_contexts.get(claim_id)
        if existing is not None:
            return existing
        if self._local_readiness_error_code is not None:
            raise WarmFenceLostError("Round 5 resident control delivery is unavailable")
        slot = await self.store.read(self.installation_id)
        if slot is None or slot.claim is None or slot.claim.claim_id != claim_id:
            raise WarmFenceLostError("Round 5 claim is unavailable")
        if slot.state == Round5WarmState.RUNNING and slot.bell_id and slot.bell_at_utc:
            raise WarmFenceLostError("bell context belonged to a replaced server process")
        bell_id = slot.claim.bell_id
        bell_at = self._clock()
        running = await self.store.accept_bell(
            slot,
            claim_id=claim_id,
            bell_id=bell_id,
            bell_at_utc=bell_at,
        )
        self._last_slot = running
        # Deliberately adjacent to the successful durable transaction.  No
        # provider call, credential refresh, event publication, or log write
        # occurs between this capture and returning the context to the manager.
        t0 = self._monotonic_ns()
        assert running.claim is not None
        context = BellContext(
            bell_id=bell_id,
            claim_id=claim_id,
            warm_generation=running.generation,
            bout_id=running.claim.bout_id,
            bout_fence=running.claim.bout_fence,
            bell_at_utc=bell_at,
            t0_monotonic_ns=t0,
        )
        self.release_claim_active(claim_id)
        self._bell_contexts[claim_id] = context
        return context

    async def accept_bell_with_leases(
        self,
        claim_id: str,
        *,
        main_store: object,
        main_lease: object,
        cleanup_store: object,
        cleanup_lease: object,
        ttl: timedelta,
        release_event_factory: Callable[[datetime], Round5ControlEvent] | None = None,
    ) -> tuple[BellContext, object, object]:
        """Use the durable four-row bell transaction when the store supports it."""

        from .models import SessionState

        existing = self._bell_contexts.get(claim_id)
        if existing is not None:
            return existing, main_lease, cleanup_lease
        if self._local_readiness_error_code is not None:
            raise WarmFenceLostError("Round 5 resident control delivery is unavailable")
        atomic = getattr(self.store, "accept_bell_with_leases", None)
        if not callable(atomic):
            context = await self.accept_bell(claim_id)
            return context, main_lease, cleanup_lease
        slot = await self.store.read(self.installation_id)
        if slot is None or slot.claim is None or slot.claim.claim_id != claim_id:
            raise WarmFenceLostError("Round 5 claim is unavailable")
        bell_id = slot.claim.bell_id
        bell_at = self._clock()
        release_event = (
            release_event_factory(bell_at)
            if release_event_factory is not None
            else None
        )
        (
            running,
            main_updated_at,
            main_expires_at,
            cleanup_updated_at,
            cleanup_expires_at,
        ) = await atomic(
            slot,
            claim_id=claim_id,
            bell_id=bell_id,
            bell_at_utc=bell_at,
            main_ring_key=main_store.ring_key,
            main_lease=main_lease,
            cleanup_ring_key=cleanup_store.ring_key,
            cleanup_lease=cleanup_lease,
            ttl=ttl,
            release_event=release_event,
        )
        t0 = self._monotonic_ns()
        context = BellContext(
            bell_id=bell_id,
            claim_id=claim_id,
            warm_generation=running.generation,
            bout_id=running.claim.bout_id,
            bout_fence=running.claim.bout_fence,
            bell_at_utc=bell_at,
            t0_monotonic_ns=t0,
        )
        self._last_slot = running
        self.release_claim_active(claim_id)
        self._bell_contexts[claim_id] = context
        main_committed = replace(
            main_lease,
            phase="run_committed",
            session_state=SessionState.RUNNING,
            updated_at=main_updated_at,
            expires_at=main_expires_at,
        )
        cleanup_committed = replace(
            cleanup_lease,
            phase="run_committed",
            session_state=SessionState.RUNNING,
            updated_at=cleanup_updated_at,
            expires_at=cleanup_expires_at,
        )
        return context, main_committed, cleanup_committed

    async def invalidate_readiness(
        self,
        code: str = "resident_control_delivery_failed",
    ) -> None:
        """Fail closed when the durable resident-control outbox cannot drain."""

        code = _safe_code(code)
        # These process-local changes happen before coordination I/O.  If the
        # database is part of the same outage, this replica still stops
        # advertising and accepting a prepared generation immediately.
        self._local_readiness_error_code = code
        self._capsule = None
        try:
            for _attempt in range(3):
                slot = await self.store.read(self.installation_id)
                self._last_slot = slot
                if slot is None or slot.state != Round5WarmState.READY:
                    return
                try:
                    self._last_slot = await self.store.freshness_lost(
                        slot,
                        code=code,
                        now=self._clock(),
                    )
                    return
                except WarmStoreConflictError:
                    continue
            raise WarmStoreConflictError(
                "Round 5 resident-control readiness withdrawal could not stabilize"
            )
        finally:
            self._wake.set()

    def _require_current_cleanup_owner(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
    ) -> None:
        if (
            slot.coordinator_owner != self.process_epoch
            or slot.process_epoch != self.process_epoch
            or slot.coordinator_lease_expires_at is None
            or slot.coordinator_lease_expires_at <= now
        ):
            raise WarmFenceLostError(
                "Round 5 cleanup coordinator ownership is not current"
            )

    async def begin_cleanup(self, claim_id: str) -> Round5WarmSlot:
        self.release_claim_active(claim_id)
        slot = await self.store.read(self.installation_id)
        if slot is None:
            raise WarmFenceLostError("Round 5 warm slot is unavailable")
        now = self._clock()
        self._require_current_cleanup_owner(slot, now=now)
        cleaning = await self.store.begin_cleanup(
            slot,
            claim_id=claim_id,
            now=now,
        )
        self._last_slot = cleaning
        self._cleanup_origins[claim_id] = (
            cleaning.generation,
            cleaning.coordinator_fence,
            cleaning.coordinator_owner,
        )
        return cleaning

    async def finish_cleanup_and_rewarm(self, claim_id: str) -> Round5WarmSlot:
        slot = await self.store.read(self.installation_id)
        if slot is None:
            raise WarmFenceLostError("Round 5 warm slot is unavailable")
        self._require_current_cleanup_owner(slot, now=self._clock())
        if not (
            (
                slot.state == Round5WarmState.CLEANING
                and slot.claim is not None
                and slot.claim.claim_id == claim_id
            )
            or claim_id in self._completed_cleanups
        ):
            raise WarmFenceLostError("Round 5 cleanup claim changed")
        self._verified_clean_claim_ids.add(claim_id)
        self.release_claim_active(claim_id)
        warmed = await self._finish_cleanup_transition(claim_id)
        self._bell_contexts.pop(claim_id, None)
        self._capsule = None
        self._last_slot = warmed
        self._wake.set()
        return warmed

    async def _finish_cleanup_transition(
        self,
        claim_id: str,
    ) -> Round5WarmSlot:
        completed = self._completed_cleanups.get(claim_id)
        origin = self._cleanup_origins.get(claim_id)
        current = await self.store.read(self.installation_id)
        if current is None:
            raise WarmFenceLostError("Round 5 warm slot is unavailable")
        self._require_current_cleanup_owner(current, now=self._clock())
        if origin is None and current.state == Round5WarmState.CLEANING:
            if current.claim is None or current.claim.claim_id != claim_id:
                raise WarmFenceLostError("Round 5 cleanup claim changed")
            origin = (
                current.generation,
                current.coordinator_fence,
                current.coordinator_owner,
            )
            self._cleanup_origins[claim_id] = origin
        if origin is None:
            if (
                completed is not None
                and current.generation == completed[0]
                and current.coordinator_fence == completed[1]
                and current.coordinator_owner == completed[2]
                and current.claim is None
                and current.state
                in {
                    Round5WarmState.WARMING,
                    Round5WarmState.READY,
                    Round5WarmState.BLOCKED,
                }
            ):
                return current
            raise WarmFenceLostError("Round 5 cleanup origin is unavailable")
        generation, coordinator_fence, coordinator_owner = origin
        for _attempt in range(CLEANUP_FINALIZE_CONFLICT_ATTEMPTS):
            current = await self.store.read(self.installation_id)
            if current is None:
                raise WarmFenceLostError("Round 5 warm slot is unavailable")
            now = self._clock()
            self._require_current_cleanup_owner(current, now=now)
            if (
                current.generation == generation + 1
                and current.coordinator_fence == coordinator_fence
                and current.coordinator_owner == coordinator_owner
                and current.claim is None
                and current.state
                in {
                    Round5WarmState.WARMING,
                    Round5WarmState.READY,
                    Round5WarmState.BLOCKED,
                }
            ):
                self._completed_cleanups[claim_id] = (
                    current.generation,
                    current.coordinator_fence,
                    current.coordinator_owner,
                )
                self._verified_clean_claim_ids.discard(claim_id)
                return current
            if (
                current.generation != generation
                or current.coordinator_fence != coordinator_fence
                or current.coordinator_owner != coordinator_owner
                or current.state != Round5WarmState.CLEANING
                or current.claim is None
                or current.claim.claim_id != claim_id
            ):
                raise WarmFenceLostError(
                    "Round 5 cleanup claim, fence, or owner changed"
                )
            try:
                warmed = await self.store.finish_cleanup_and_rewarm(
                    current,
                    claim_id=claim_id,
                    now=now,
                    warm_contract_sha256=self.warm_contract_sha256,
                )
            except WarmStoreConflictError:
                # The CLEANING->WARMING compare-and-swap lost to a concurrent
                # writer -- in practice the continuous warm-capsule renewal that
                # bumps the slot revision every few seconds. Immediate retries
                # (the old sleep(0)) just lose again in lockstep. Back off with
                # full jitter so the renewal can settle between attempts and a
                # retry can win, instead of exhausting the attempts and raising.
                # The Proxy is already confirmed absent before this transition,
                # so this is a rewarm-liveness retry, never a billing risk; the
                # manager's automatic convergence loop retries the whole
                # reconcile if even this bounded window is somehow exhausted.
                ceiling = min(0.25, 0.01 * (2 ** min(_attempt, 5)))
                await self._sleep(self._random.uniform(0.0, ceiling))
                continue
            if (
                warmed.generation != generation + 1
                or warmed.state != Round5WarmState.WARMING
                or warmed.claim is not None
                or warmed.coordinator_fence != coordinator_fence
                or warmed.coordinator_owner != coordinator_owner
            ):
                raise WarmFenceLostError(
                    "Round 5 cleanup transition returned an invalid generation"
                )
            self._completed_cleanups[claim_id] = (
                warmed.generation,
                warmed.coordinator_fence,
                warmed.coordinator_owner,
            )
            self._verified_clean_claim_ids.discard(claim_id)
            return warmed
        raise WarmStoreConflictError(
            "Round 5 cleanup transition could not stabilize"
        )

    async def public_status(self) -> dict[str, object]:
        slot = await self.store.read(self.installation_id)
        self._last_slot = slot
        return self._public_status(slot)

    def public_status_cached(self) -> dict[str, object]:
        """Return the last coordinator observation without request-path I/O."""

        return self._public_status(self._last_slot)

    def _public_status(
        self,
        slot: Round5WarmSlot | None,
    ) -> dict[str, object]:
        if slot is None:
            return {
                "round5_warm_state": "warming",
                "round5_ring_ready": False,
                "round5_cleanup_owed": False,
                "round5_warm_last_error_code": (
                    self._local_readiness_error_code
                ),
            }
        now = self._clock()
        ring_ready = self._claimable(slot, now)
        variants = {
            variant.value: {
                "state": "ready" if receipt.proxy_absent and receipt.expires_at > now else "stale",
                "expires_at": receipt.expires_at.isoformat(),
            }
            for variant, receipt in slot.variants.items()
        }
        runners: dict[str, object] = {}
        if slot.shared_receipt is not None:
            for receipt in (
                slot.shared_receipt.lakebase_runner,
                slot.shared_receipt.competitor_runner,
            ):
                runners[receipt.lane_id] = {
                    "state": "ready" if receipt.expires_at > now else "stale",
                    "boot_id": receipt.boot_id,
                    "expires_at": receipt.expires_at.isoformat(),
                }
        return {
            "round5_warm_state": slot.state.value,
            "round5_warm_generation": slot.generation,
            "round5_warm_revision": slot.revision,
            "round5_warm_started_at": slot.warming_started_at.isoformat(),
            "round5_warm_ready_at": slot.ready_at.isoformat() if slot.ready_at else None,
            "round5_warm_renew_by": slot.renew_by.isoformat() if slot.renew_by else None,
            "round5_ring_ready": ring_ready,
            "round5_warm_variants": variants,
            "round5_warm_runners": runners,
            "round5_warm_attempt_count": slot.attempt_count,
            "round5_warm_last_attempt_at": (
                slot.last_attempt_at.isoformat() if slot.last_attempt_at else None
            ),
            "round5_warm_next_retry_at": (
                slot.next_retry_at.isoformat() if slot.next_retry_at else None
            ),
            "round5_warm_last_error_code": (
                self._local_readiness_error_code or slot.last_error_code
            ),
            "round5_claim_id": slot.claim.claim_id if slot.claim else None,
            "round5_claim_expires_at": (
                slot.claim.claim_expires_at.isoformat() if slot.claim else None
            ),
            "round5_cleanup_owed": slot.state == Round5WarmState.CLEANING,
        }
