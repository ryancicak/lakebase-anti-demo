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
import os
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
# A throttled/timed-out provenance probe (validate_ready RetryableWarmError) is a
# transient READ failure, not a runner identity change: retry in place and stay
# READY. Only escalate to freshness_lost (a full rewarm) after this many
# consecutive probe failures, or when provenance is about to lapse.
MAX_PROVENANCE_PROBE_FAILURES = 5
PROVENANCE_PROBE_RETRY_SECONDS = 2.0
PROVENANCE_PROBE_MIN_SLACK_SECONDS = 3.0
# The 45-minute runner receipts cannot be extended in place without a fresh live
# proxy-absence + runner attestation. Rather than slide the bound (which would
# rest READY on stale evidence), rewarm cleanly this far ahead of the wall so the
# full prepare() re-observes proxy absence and runner identity and publishes new
# receipts.
RUNNER_RECEIPT_REFRESH_MARGIN_SECONDS = 10 * 60
# Refresh the short-lived dispatch credential / provenance this many seconds BEFORE
# ``renew_by`` lands, so the freshly-minted capsule (with a new launch margin and
# renew_by) is published while the OLD capsule is still claimable. Refreshing only
# AT renew_by opened a window where ``_claimable()`` (launch-margin gated) briefly
# lapsed and the idle fight card flashed "Temporarily Unavailable" until the
# in-flight refresh republished. The lead must exceed a slow (executor-contended)
# refresh so the launch margin never lapses during idle keep-alive.
RUNNER_CREDENTIAL_REFRESH_LEAD_SECONDS = 60.0
# A retryable warm failure that persists this many consecutive attempts stops
# being treated as transient: escalate to a DISTINCT terminal code instead of
# retrying forever (still never first-failure BLOCKED).
MAX_TRANSIENT_WARM_ATTEMPTS = 8
# Post-bout rewarm storm fix (bounded token mint rate). The resident runner is
# single-bout and systemd-respawned (~2s) after every burst, so a rewarm must
# deliver exactly ONE fresh PRELOAD per lane for its warm_attempt_token and then
# give the resident a fair window to consume it and beat. Minting a distinct
# token on every WARMING beat produced a superseded PRELOAD backlog the resident
# could never drain: it beat token B_k just as the coordinator minted B_{k+1},
# so its beat was RLS-denied (superseded), it tore down and chased the next
# token, and validate_ready read ABSENT/STALE forever (the observed 149/149 idle
# flicker after a towel; a restart recovered only because it minted ONE token).
# Reuse the SAME in-flight token across retryable WARMING retries for this long
# before deliberately superseding it ONCE (a new token == a freshly delivered
# PRELOAD), so at most one live PRELOAD per lane exists and the resident can
# converge. The overall attempt flood stays bounded by MAX_TRANSIENT_WARM_ATTEMPTS
# -> a named terminal block. The window must exceed the resident's respawn +
# PRELOAD-consume + first-beat latency with margin, and stay well under the
# provenance freshness/receipt horizons.
WARM_ATTEMPT_TOKEN_REUSE_SECONDS = 30.0
# Attested resident IDENTITY-change re-establishment bound. An idle READY probe
# that reads a genuine IDENTITY_CHANGED (Patch 1 tri-state -- the resident this
# generation's receipts attest is provably gone/replaced) forces a CLEAN, FENCED
# rewarm (discard the stale engines/capsule/receipts + retire the old resident
# binding, then re-PRELOAD both lanes for ONE fresh token). If the identity will
# not stabilize within this many consecutive clean rewarms (a resident that keeps
# respawning under load -- e.g. a concurrent round's DB contention churning the
# shared runner), stop the tight identity-refresh <-> rewarming churn and escalate
# to the named, SELF-VERIFIABLE block ``runner_identity_unstable`` (re-checked on a
# bounded interval so it self-recovers once the resident settles) instead of an
# invisible, unbounded token/PRELOAD flood that only a full restart cleared. The
# budget is reset the instant a CURRENT probe proves a fresh, stable identity.
MAX_IDENTITY_REESTABLISH_ATTEMPTS = 5
# Post-bout rewarm capsule re-establishment bound. After a cleanup advances the
# generation to N+1, the READY keep-alive re-checks that this process still holds
# the launch capsule for exactly this slot (``_capsule_belongs``). If a rewarm
# publishes READY but the very next probe finds the capsule no longer belongs (a
# superseding owner bumped the fence/token, or the durable N+1 slot's token
# diverged), the loop tears the slot down (``launch_capsule_missing``) and rewarms
# -- and because that rewarm SUCCEEDS, the ``MAX_TRANSIENT_WARM_ATTEMPTS`` bound
# (checked only in the rewarm ERROR path) never fires and the transient
# ``publish_ready`` keeps clearing ``last_error`` to None. That was an invisible,
# unbounded ``identity-refresh <-> rewarming`` flood (frozen gen, err=None, 20+
# flips) that only a restart cleared. Bound it exactly like the identity path:
# after this many consecutive non-belonging READY probes without a belonging
# capsule in between, escalate to the named, SELF-VERIFIABLE block
# ``warm_capsule_unrecoverable`` (re-checked on a bounded interval, self-recovers
# once a rewarm's capsule belongs again). Reset the instant the capsule belongs.
MAX_CAPSULE_REESTABLISH_ATTEMPTS = 5
# Some BLOCKED codes are SELF-VERIFIABLE: they describe a condition the coordinator
# can re-check itself (a momentarily insufficient/expired credential margin, or a
# transient that merely persisted past the escalation count). These are re-attempted
# at a bounded interval so the same process recovers on its own -- never a latch a
# human must clear. TRUE permanent anti-cheat/config blocks (identity change, orphan
# Proxy, fixture drift, unexpected) stay latched and are surfaced honestly.
SELF_VERIFIABLE_BLOCK_RETRY_SECONDS = 60.0
SELF_VERIFIABLE_BLOCK_CODES = frozenset(
    {
        "credential_margin_insufficient",
        "credential_refresh_expired",
        "warm_provider_retryable_persistent",
        # A competitor source that stayed non-available (backing-up / modifying /
        # failing-over / rebooting) past the transient escalation count is still a
        # provider-state condition the coordinator can re-verify itself -- never a
        # terminal identity block a human must clear.
        "warm_source_unavailable_persistent",
        # A resident whose ATTESTED identity kept changing faster than a clean
        # rewarm could stabilize it (MAX_IDENTITY_REESTABLISH_ATTEMPTS exhausted).
        # This is a resident-state condition the coordinator re-verifies itself: it
        # rechecks on a bounded interval and self-recovers the instant the resident
        # settles, so it is surfaced (never a silent churn) but is NOT a permanent
        # latch a human must clear.
        "runner_identity_unstable",
        # A post-bout rewarm whose launch capsule kept failing ``_capsule_belongs``
        # faster than a rewarm could publish a belonging one
        # (MAX_CAPSULE_REESTABLISH_ATTEMPTS exhausted). A capsule-lineage condition
        # the coordinator re-verifies itself: it rechecks on a bounded interval and
        # self-recovers the instant a rewarm's capsule belongs, so it is surfaced
        # (never a silent churn) but is NOT a permanent latch a human must clear.
        "warm_capsule_unrecoverable",
    }
)


def _blocked_is_terminal(slot: Round5WarmSlot) -> bool:
    """A BLOCKED slot whose code is not self-verifiable will not self-recover.

    A BLOCKED state can never be terminal while claim, cleanup, or resident
    settlement debt is still outstanding (``claim`` present or
    ``requires_cleaned_bout`` set): that debt is a condition the coordinator can
    re-verify and settle itself, so the slot must keep CLEANING/retrying with
    typed diagnostics rather than latching for a human. Only a BLOCKED slot with
    no such debt and a non-self-verifiable code is genuinely terminal.
    """
    if slot.state != Round5WarmState.BLOCKED:
        return False
    if getattr(slot, "claim", None) is not None or getattr(
        slot, "requires_cleaned_bout", False
    ):
        return False
    return slot.last_error_code not in SELF_VERIFIABLE_BLOCK_CODES
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
    return _receipt_bound_of(slot.shared_receipt, slot.variants)


def _receipt_bound_of(
    shared_receipt: Round5SharedReceipt,
    variants: Mapping[Round5Variant, Round5VariantReceipt],
) -> datetime:
    return min(
        shared_receipt.lakebase_runner.expires_at,
        shared_receipt.competitor_runner.expires_at,
        *(receipt.expires_at for receipt in variants.values()),
    )


def _assert_immutable_receipt_identity(
    slot: Round5WarmSlot,
    shared_receipt: Round5SharedReceipt,
    variants: Mapping[Round5Variant, Round5VariantReceipt],
) -> None:
    """A receipt RENEWAL may advance freshness but never the attested identity.

    ``identity immutable, provenance renewable``: renewing the 45-minute receipt
    horizon may move ``expires_at`` and the freshly-observed proxy-absence
    timestamp forward, but the runner boot/process/image/harness digests and the
    per-variant target/network digests must be byte-identical to what was
    published at warm. A mismatch means the "renewal" is really a new identity
    smuggled in without a full attested warm -- fail closed.
    """

    if slot.shared_receipt is None or not slot.variants:
        raise WarmFenceLostError("READY receipt identity is unavailable")
    for lane_old, lane_new in (
        (slot.shared_receipt.lakebase_runner, shared_receipt.lakebase_runner),
        (slot.shared_receipt.competitor_runner, shared_receipt.competitor_runner),
    ):
        if replace(lane_old, expires_at=lane_new.expires_at) != lane_new:
            raise WarmFenceLostError("renewed receipt runner identity changed")
    if (
        replace(
            slot.shared_receipt,
            lakebase_runner=shared_receipt.lakebase_runner,
            competitor_runner=shared_receipt.competitor_runner,
        )
        != shared_receipt
    ):
        raise WarmFenceLostError("renewed receipt shared identity changed")
    if set(slot.variants) != set(variants):
        raise WarmFenceLostError("renewed receipt variant set changed")
    for key, old_variant in slot.variants.items():
        new_variant = variants[key]
        if (
            replace(
                old_variant,
                proxy_absence_observed_at=new_variant.proxy_absence_observed_at,
                expires_at=new_variant.expires_at,
            )
            != new_variant
        ):
            raise WarmFenceLostError("renewed receipt variant identity changed")


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
    cleaned_bout_id: str | None = None
    requires_cleaned_bout: bool = False

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
        if self.cleaned_bout_id is not None:
            # ``cleaned_bout_id`` is retained as an AUDIT record of the last
            # cleanup even after ``requires_cleaned_bout`` clears on the next
            # successful publish_ready (the cleanup signal is episodic, not
            # sticky), so it may legitimately outlive the active lineage flag.
            _safe_identifier(self.cleaned_bout_id, "cleaned_bout_id")
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

    def launch_margin_cliff(self) -> datetime:
        """The last instant at which ``meets_launch_margin`` is still satisfied.

        This is the true claimability cliff -- the credential-launch deadline, NOT the
        ``renew_by`` refresh trigger. Driving the keep-alive refresh off THIS cliff (with
        lead) is what keeps idle READY continuously claimable: refreshing off ``renew_by``
        alone flashed "Temporarily Unavailable" whenever ``renew_by`` sat AFTER the cliff,
        because the refresh then fired after the capsule was already un-launchable.
        """

        control_margin = timedelta(
            seconds=PROXY_SETUP_DEADLINE_SECONDS + CONTROL_PLANE_MARGIN_SECONDS
        )
        dispatch_margin = timedelta(seconds=RUNNER_DEADLINE_SECONDS + DISPATCH_MARGIN_SECONDS)
        return min(
            self.control_expires_at - control_margin,
            *(expiration - dispatch_margin for expiration in self.dispatch_expires_at.values()),
            self.expires_at,
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

    async def reestablish(self, slot: Round5WarmSlot) -> None:
        """Retire the current resident + discard stale engines after an attested
        identity change, so the next ``prepare`` is a genuinely fresh PRELOAD rather
        than a rewarm over the changed identity. Optional: the coordinator invokes it
        defensively, so a provider that cannot re-establish (e.g. a stateless test
        double) may omit it and rely on the fresh ``prepare`` alone."""
        ...

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
        requires_cleaned_bout: bool,
    ) -> Round5WarmPreparation: ...

    async def refresh_capsule(
        self,
        slot: Round5WarmSlot,
        previous: Round5LaunchCapsule,
    ) -> Round5LaunchCapsule: ...

    async def refresh_preparation(
        self,
        slot: Round5WarmSlot,
        previous: Round5LaunchCapsule,
    ) -> Round5WarmPreparation:
        """Renew credentials AND republish fresh receipts off a live probe.

        Re-observes per-bout Proxy absence and re-reads runner identity, returning
        a full preparation (fresh receipts + rotated capsule) on the SAME immutable
        runner identity so READY can renew in place across the runner-receipt
        horizon without a full ``prepare()`` rewarm.
        """
        ...


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

    async def record_cleanup_retry(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
        next_retry_at: datetime,
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
                and current.claim is None
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
                broker_epoch=broker_epoch if replacement else current.broker_epoch,
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
        if slot.state == Round5WarmState.CLEANING or slot.claim is not None:
            raise WarmFenceLostError("cannot publish READY while Round 5 cleanup is owed")
        if not all(receipt.proxy_absent for receipt in preparation.variants.values()):
            raise WarmFenceLostError("cannot publish READY while a per-bout Proxy is present")
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
            # The cleanup lineage signal is EPISODIC: once generation N+1 has been
            # re-attested and published READY, the immediate-cleanup lineage is
            # over. Clear the flag so a genuine binding drift in a LATER generation
            # fails closed instead of being masked as a retryable cleanup
            # transition. ``cleaned_bout_id`` is left intact as an audit record.
            requires_cleaned_bout=False,
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

    async def record_cleanup_retry(
        self,
        slot: Round5WarmSlot,
        *,
        code: str,
        now: datetime,
        next_retry_at: datetime,
    ) -> Round5WarmSlot:
        if slot.state != Round5WarmState.CLEANING or slot.claim is None:
            raise WarmFenceLostError("cleanup retry requires retained CLEANING debt")
        return await self._replace(
            slot,
            "cleanup_retry",
            now=now,
            next_retry_at=next_retry_at,
            last_error_code=_safe_code(code),
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
        shared_receipt: Round5SharedReceipt | None = None,
        variants: Mapping[Round5Variant, Round5VariantReceipt] | None = None,
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
        changes: dict[str, object] = {"broker_epoch": capsule.broker_epoch}
        if shared_receipt is not None or variants is not None:
            if shared_receipt is None or variants is None:
                raise WarmFenceLostError("receipt renewal requires both receipt halves")
            _assert_immutable_receipt_identity(slot, shared_receipt, variants)
            receipt_bound = _receipt_bound_of(shared_receipt, variants)
            changes["shared_receipt"] = shared_receipt
            changes["variants"] = variants
        else:
            receipt_bound = _receipt_expiry_bound(slot)
        ready_expires_at = min(receipt_bound, capsule.expires_at)
        renew_by = min(ready_expires_at, capsule.renew_by)
        if renew_by <= now:
            raise BlockedWarmError("credential_refresh_expired")
        return await self._replace(
            slot,
            "warm_capsule_refreshed",
            now=now,
            ready_expires_at=ready_expires_at,
            renew_by=renew_by,
            **changes,
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

    async def return_unstarted_claim(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        now: datetime,
    ) -> Round5WarmSlot:
        """Return CLAIMED -> READY when ARM failed before any resident mutation.

        Expired or post-stage claims still fence into CLEANING. This path is
        only for a start-state/fence refusal that created no AWS work.
        """

        now = _utc(now, "now")
        claim = slot.claim
        if (
            slot.state != Round5WarmState.CLAIMED
            or claim is None
            or claim.claim_id != claim_id
            or slot.bell_at_utc is not None
        ):
            raise WarmFenceLostError("Round 5 unstarted claim cannot be returned")
        return await self._replace(
            slot,
            "claim_returned_unstarted",
            now=now,
            state=Round5WarmState.READY,
            claim=None,
            bell_id=None,
            bell_at_utc=None,
        )

    async def release_expired_claim(
        self,
        slot: Round5WarmSlot,
        *,
        now: datetime,
        still_fresh: bool,
    ) -> Round5WarmSlot:
        """Fence an EXPIRED claim into CLEANING with the claim RETAINED.

        An expired pre-bell claim may have staged ARM residents whose exact
        logical job IDs live only in the durable claim.  Returning to READY or
        WARMING here would (a) abandon those residents to collide with the next
        warm generation's ``wait_agent_ready`` (the ``warm_baseline_unexpected``
        latch), and (b) race the manager's own ``begin_cleanup`` and drop the
        claim out from under it -- the no-bell ``WarmFenceLostError`` loop the
        live incident wedged on.  So an expired CLAIMED slot fails safe into
        CLEANING, keeping the claim so cleanup can settle the exact residents and
        prove provider absence before any rewarm.  ``begin_cleanup`` is idempotent
        on an already-CLEANING slot with the same claim, so this generic release
        never beats -- and is never beaten by -- active manager cleanup.
        """
        if (
            slot.state != Round5WarmState.CLAIMED
            or slot.claim is None
            or slot.claim.claim_expires_at > now
        ):
            return slot
        # ``still_fresh`` is retained for signature/back-compatibility only.
        # Readiness is never trusted here; it is re-derived by the warm loop
        # after cleanup settles the exact residents and proves provider absence.
        del still_fresh
        return await self._replace(
            slot,
            "cleanup_started",
            now=now,
            state=Round5WarmState.CLEANING,
        )

    # NOTE: the direct ``release_claim`` (CLAIMED -> READY/WARMING) API was removed.
    # It was the only release that skipped the cleanup fence and it rested on a
    # caller-supplied "residents settled" proof. Every pre-bell abandon now goes
    # through ``begin_cleanup``/``release_expired_claim`` (CLEANING, claim retained)
    # and converges via exact durable SETTLED + provider absence + rewarm.

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
        # Reject a bell whose claim has already expired at the authoritative now.
        # An expired claim is (or is about to be) fenced into CLEANING; ringing it
        # would launch a bout over a claim the cleanup path is reclaiming. Equality
        # counts as expired (<=), so a bell exactly at the deadline is refused.
        if slot.claim.claim_expires_at <= _utc(bell_at_utc, "bell_at_utc"):
            raise WarmClaimUnavailableError("Round 5 claim expired before bell")
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
        # BLOCKED is accepted here so a claim-bearing BLOCKED (unsettled residents)
        # can be routed into CLEANING rather than warmed/normalized over the debt.
        if (
            slot.state
            not in {
                Round5WarmState.CLAIMED,
                Round5WarmState.RUNNING,
                Round5WarmState.BLOCKED,
            }
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
            cleaned_bout_id=slot.claim.bout_id,
            requires_cleaned_bout=True,
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
        cleaned_bout_id=(
            str(raw["cleaned_bout_id"])
            if raw.get("cleaned_bout_id")
            else None
        ),
        # Honor an EXPLICIT ``requires_cleaned_bout`` value (including false) so a
        # slot that has cleared the episodic lineage flag but retains
        # ``cleaned_bout_id`` for audit round-trips as false. Only INFER from
        # ``cleaned_bout_id`` for backward-compatibility when the field is absent
        # entirely (rows written before the flag became episodic).
        requires_cleaned_bout=(
            bool(raw["requires_cleaned_bout"])
            if "requires_cleaned_bout" in raw
            else bool(raw.get("cleaned_bout_id"))
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
            and current.claim is None
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
            broker_epoch=(
                str(kwargs["broker_epoch"]) if replacement else current.broker_epoch
            ),
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

    async def record_cleanup_retry(
        self, slot: Round5WarmSlot, **kwargs: Any
    ) -> Round5WarmSlot:
        if slot.state != Round5WarmState.CLEANING or slot.claim is None:
            raise WarmFenceLostError("cleanup retry requires retained CLEANING debt")
        return await self._mutate(
            slot,
            "cleanup_retry",
            now=kwargs["now"],
            next_retry_at=kwargs["next_retry_at"],
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
        shared_receipt: Round5SharedReceipt | None = kwargs.get("shared_receipt")
        variants: Mapping[Round5Variant, Round5VariantReceipt] | None = kwargs.get(
            "variants"
        )
        if not capsule.meets_launch_margin(now):
            raise BlockedWarmError("credential_margin_insufficient")
        changes: dict[str, object] = {"broker_epoch": capsule.broker_epoch}
        if shared_receipt is not None or variants is not None:
            if shared_receipt is None or variants is None:
                raise WarmFenceLostError("receipt renewal requires both receipt halves")
            _assert_immutable_receipt_identity(slot, shared_receipt, variants)
            receipt_bound = _receipt_bound_of(shared_receipt, variants)
            changes["shared_receipt"] = shared_receipt
            changes["variants"] = variants
        else:
            receipt_bound = _receipt_expiry_bound(slot)
        ready_expires_at = min(receipt_bound, capsule.expires_at)
        renew_by = min(ready_expires_at, capsule.renew_by)
        if renew_by <= now:
            raise BlockedWarmError("credential_refresh_expired")
        return await self._mutate(
            slot,
            "warm_capsule_refreshed",
            now=now,
            ready_expires_at=ready_expires_at,
            renew_by=renew_by,
            **changes,
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
        capsule_broker_epoch: str,
        capsule_warm_attempt_token: str,
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
                # Ordinary claims create scoped ring rows lazily. Round 5 claims
                # both rows in one transaction, including on the first bout of a
                # pristine installation, so materialize both before locking them.
                await cursor.execute(
                    f"""
                    INSERT INTO {COORDINATION_TABLE} (ring_key, fencing_token)
                    VALUES (%s, 0), (%s, 0)
                    ON CONFLICT (ring_key) DO NOTHING
                    """,
                    (main_ring_key, cleanup_ring_key),
                )
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
                try:
                    if current.shared_receipt is None or not current.variants:
                        raise WarmFenceLostError(
                            "locked READY receipt identity is unavailable"
                        )
                    _assert_immutable_receipt_identity(
                        slot,
                        current.shared_receipt,
                        current.variants,
                    )
                    receipt_identity_changed = False
                except WarmFenceLostError:
                    receipt_identity_changed = True
                # Coordinator heartbeats and provenance renewal advance revision
                # while leaving this generation, fence, and capsule valid. Requiring
                # the caller's pre-transaction revision made a claim impossible when
                # database latency exceeded the renewal interval. The locked row's
                # semantic identity and freshness checks below are the actual fence.
                failed_checks = [
                    name
                    for name, failed in (
                        (
                            "coordinator_fence",
                            current.coordinator_fence != slot.coordinator_fence,
                        ),
                        ("state", current.state != Round5WarmState.READY),
                        (
                            "ready_expiry",
                            current.ready_expires_at is None
                            or current.ready_expires_at <= locked_now,
                        ),
                        (
                            "renew_by",
                            current.renew_by is None or current.renew_by <= locked_now,
                        ),
                        (
                            "provenance",
                            current.provenance_expires_at is None
                            or current.provenance_expires_at <= locked_now,
                        ),
                        ("receipt_expiry", _receipt_expiry_bound(current) <= locked_now),
                        ("receipt_identity", receipt_identity_changed),
                        ("generation", capsule_generation != current.generation),
                        ("broker_epoch", capsule_broker_epoch != current.broker_epoch),
                        (
                            "warm_attempt",
                            capsule_warm_attempt_token != current.warm_attempt_token,
                        ),
                    )
                    if failed
                ]
                if failed_checks:
                    logger.warning(
                        "Round 5 locked claim refused checks=%s "
                        "slot_revision=%d current_revision=%d "
                        "slot_generation=%d current_generation=%d",
                        ",".join(failed_checks),
                        slot.revision,
                        current.revision,
                        slot.generation,
                        current.generation,
                    )
                    raise WarmClaimUnavailableError(
                        "Round 5 READY generation changed while claiming"
                    )
                if current.revision != slot.revision:
                    logger.info(
                        "Round 5 locked claim accepted safe revision skew "
                        "slot_revision=%d current_revision=%d generation=%d",
                        slot.revision,
                        current.revision,
                        current.generation,
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

    async def return_unstarted_claim(
        self, slot: Round5WarmSlot, **kwargs: Any
    ) -> Round5WarmSlot:
        helper = InMemoryRound5WarmStore()
        helper._slots[slot.installation_id] = slot
        updated = await helper.return_unstarted_claim(slot, **kwargs)
        return await self._persist(
            slot, updated, "claim_returned_unstarted", now=kwargs["now"]
        )

    async def return_unstarted_with_leases(
        self,
        slot: Round5WarmSlot,
        *,
        claim_id: str,
        main_ring_key: str,
        cleanup_ring_key: str,
        main_lease: object,
        cleanup_lease: object,
        now: datetime,
    ) -> Round5WarmSlot:
        """Atomically restore READY and release both rings for an unstarted ARM."""

        from .coordination import COORDINATION_TABLE

        now = _utc(now, "now")
        _safe_identifier(claim_id, "claim_id")

        async def commit(cursor: Any) -> Round5WarmSlot:
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
                ring_rows = await cursor.fetchall()
                by_key = {str(row[0]): row for row in ring_rows}
                expected = {
                    main_ring_key: main_lease,
                    cleanup_ring_key: cleanup_lease,
                }
                if set(by_key) != set(expected) or any(
                    lease is None
                    or str(row[2]) != str(lease.lease_id)
                    or int(row[1]) != lease.fencing_token
                    for key, lease in expected.items()
                    for row in (by_key.get(key),)
                    if row is not None
                ):
                    raise WarmFenceLostError(
                        "Round 5 ring identity changed before unstarted rollback"
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
                    raise WarmFenceLostError("Round 5 warm slot is unavailable")
                raw = (
                    warm_row[0]
                    if isinstance(warm_row[0], Mapping)
                    else json.loads(str(warm_row[0]))
                )
                current = _slot_from_json(raw)
                claim = current.claim
                if (
                    current.state != Round5WarmState.CLAIMED
                    or claim is None
                    or claim.claim_id != claim_id
                    or current.bell_at_utc is not None
                ):
                    raise WarmFenceLostError(
                        "Round 5 unstarted claim cannot be returned"
                    )
                updated = replace(
                    current,
                    state=Round5WarmState.READY,
                    revision=current.revision + 1,
                    claim=None,
                    bell_id=None,
                    bell_at_utc=None,
                )
                event = InMemoryRound5WarmStore()._event(
                    current,
                    updated,
                    "claim_returned_unstarted",
                    now=now,
                )
                await cursor.execute(
                    f"""
                    UPDATE {ROUND5_WARM_SLOT_TABLE}
                    SET generation = %s, state = %s, revision = %s,
                        coordinator_fence = %s, process_epoch = %s,
                        broker_epoch = %s, payload = %s::jsonb, updated_at = %s
                    WHERE installation_id = %s AND slot_ordinal = %s
                      AND state = 'claimed'
                      AND generation = %s
                      AND revision = %s
                    RETURNING 1
                    """,
                    (
                        updated.generation,
                        updated.state.value,
                        updated.revision,
                        updated.coordinator_fence,
                        updated.process_epoch,
                        updated.broker_epoch,
                        json.dumps(
                            _to_json(updated), sort_keys=True, separators=(",", ":")
                        ),
                        now,
                        updated.installation_id,
                        updated.slot_ordinal,
                        current.generation,
                        current.revision,
                    ),
                )
                if await cursor.fetchone() is None:
                    raise WarmStoreConflictError(
                        "Round 5 warm slot changed while returning"
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
                        json.dumps(
                            _to_json(event), sort_keys=True, separators=(",", ":")
                        ),
                    ),
                )
                for lease, ring_key in (
                    (main_lease, main_ring_key),
                    (cleanup_lease, cleanup_ring_key),
                ):
                    if lease is None:
                        continue
                    await cursor.execute(
                        f"""
                        UPDATE {COORDINATION_TABLE}
                        SET lease_id = NULL,
                            session_id = NULL,
                            owner_subject = NULL,
                            owner_display_name = NULL,
                            owner_email = NULL,
                            phase = NULL,
                            session_state = NULL,
                            round_id = NULL,
                            round_title = NULL,
                            competitor_id = NULL,
                            competitor_name = NULL,
                            started_at = NULL,
                            updated_at = clock_timestamp(),
                            expires_at = NULL
                        WHERE ring_key = %s
                          AND lease_id = %s::uuid
                          AND fencing_token = %s
                          AND session_id = %s
                          AND owner_subject = %s
                        RETURNING fencing_token
                        """,
                        (
                            ring_key,
                            lease.lease_id,
                            lease.fencing_token,
                            lease.session_id,
                            lease.owner_subject,
                        ),
                    )
                    if await cursor.fetchone() is None:
                        raise WarmFenceLostError(
                            "Round 5 ring changed during unstarted rollback"
                        )
                return updated

        return await self._run(commit)

    async def release_expired_claim(
        self, slot: Round5WarmSlot, **kwargs: Any
    ) -> Round5WarmSlot:
        helper = InMemoryRound5WarmStore()
        helper._slots[slot.installation_id] = slot
        updated = await helper.release_expired_claim(slot, **kwargs)
        if updated is slot:
            return slot
        # An expired claim now fences into CLEANING (claim retained) rather than
        # returning to READY/WARMING; label the durable event to match the state
        # the shared in-memory transition produced.
        event_type = (
            "cleanup_started"
            if updated.state == Round5WarmState.CLEANING
            else "claim_released"
        )
        return await self._persist(slot, updated, event_type, now=kwargs["now"])

    # release_claim (direct CLAIMED -> READY/WARMING) removed; see the in-memory
    # store note. Pre-bell abandon goes through begin_cleanup/release_expired_claim.

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
        if claim.claim_expires_at <= _utc(kwargs["bell_at_utc"], "bell_at_utc"):
            raise WarmClaimUnavailableError("Round 5 claim expired before bell")
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
                      -- Reject a bell whose claim has expired at the DB clock, in
                      -- the same transaction/predicate as the fence check, so a
                      -- concurrent expire/CLEANING and this bell cannot both win.
                      AND (w.payload->'claim'->>'claim_expires_at')::timestamptz
                          > clock_timestamp()
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
        # BLOCKED accepted (claim-bearing BLOCKED -> CLEANING); see in-memory store.
        if (
            slot.claim is None
            or slot.claim.claim_id != kwargs["claim_id"]
            or slot.state
            not in {
                Round5WarmState.CLAIMED,
                Round5WarmState.RUNNING,
                Round5WarmState.BLOCKED,
            }
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
            cleaned_bout_id=slot.claim.bout_id,
            requires_cleaned_bout=True,
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
        max_active_arm_renewal_seconds: float | None = None,
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
        # A durable cross-check so a leaked/stuck ARM task (active-id set but never
        # cleared, same process still alive) cannot renew an active claim forever.
        # Renewal is bounded to this horizon measured from the claim's durable
        # claimed_at; past it the claim is left to expire and fence into CLEANING.
        self._max_active_arm_renewal = timedelta(
            seconds=(
                max(600.0, 4 * claim_ttl_seconds)
                if max_active_arm_renewal_seconds is None
                else max_active_arm_renewal_seconds
            )
        )
        # Optional tighter, per-claim bound the manager may register from the
        # durable armed lease deadline; the min of this and the horizon above wins.
        self._active_claim_deadlines: dict[str, datetime] = {}
        # Blocker 2: exactly one in-flight cleanup convergence per claim_id. Both
        # the supervised loop and any manager request await the SAME coalesced task,
        # so there is a single provider reconcile+finish sequence per claim (durable
        # CAS on finish coalesces across replicas).
        self._cleanup_tasks: dict[str, asyncio.Task[Round5WarmSlot]] = {}
        # Blocker 2: give the provider a DURABLE authority guard it must consult
        # immediately before every external mutation. Cancellation of the held
        # operation is only an optimization; even if the provider swallows
        # CancelledError, a stale coordinator (owner/fence changed or lease expired)
        # is refused at the mutation boundary. No-op for providers that predate the
        # guard (the in-memory fakes).
        if hasattr(self.provider, "authority_guard"):
            self.provider.authority_guard = self._authority_guard
        self._capsule: Round5LaunchCapsule | None = None
        self._provenance_probe_failures = 0
        # Bounded token mint rate (post-bout rewarm storm fix). The token this
        # process is currently trying to warm-and-attest, and when it was minted.
        # Reused across retryable WARMING retries within WARM_ATTEMPT_TOKEN_REUSE_
        # SECONDS so the single-bout resident sees ONE PRELOAD per lane instead of
        # a superseded backlog; cleared the instant the token reaches READY (so a
        # later freshness_lost mints a genuinely fresh token and never re-warms a
        # token that was already publicly claimable -- anti-replay) and rotated
        # deliberately once when the reuse window lapses without attestation.
        self._warm_attempt_token: str | None = None
        self._warm_attempt_minted_at: datetime | None = None
        self._wake = asyncio.Event()
        self._closed = False
        # Ordered-shutdown authority revocation: close() sets this the INSTANT it
        # begins (fail-closed), so any in-flight or slipped-in provider mutation is
        # refused at the durable authority guard (``_authority_guard``) before it
        # runs -- not only for tasks that happened to be in a point-in-time snapshot.
        self._authority_revoked = False
        # Serializes close() against cleanup-task REGISTRATION so there is no TOCTOU
        # between close()'s snapshot and a concurrent begin_cleanup/converge_cleanup:
        # both take this lock, so once close() has set the closing flag under it no
        # new cleanup task can register and every registered task is in the snapshot.
        self._lifecycle_lock = asyncio.Lock()
        # Bounded shutdown handoff: close() waits for in-flight cleanup (fence held)
        # only up to this deadline, which MUST stay below the platform graceful-
        # shutdown grace (Databricks Apps / Uvicorn == 60s in app.yaml) so a stalled
        # shielded AWS call cannot hang deploy/stop until an external kill. On the
        # deadline close() leaves durable CLEANING + leases + the still-beating
        # heartbeat in place (NEVER revoking authority or closing the store while this
        # process may still be mutating) and hands off to restart/takeover via the
        # coordinator lease TTL.
        self._shutdown_handoff_deadline = float(
            os.environ.get("ANTI_DEMO_ROUND5_SHUTDOWN_HANDOFF_SECONDS", "45")
        )
        self._task: asyncio.Task[None] | None = None
        self._bell_contexts: dict[str, BellContext] = {}
        self._last_slot: Round5WarmSlot | None = None
        self._inherited_claim_ids: set[str] = set()
        self._verified_clean_claim_ids: set[str] = set()
        self._cleanup_origins: dict[str, tuple[int, int, str | None]] = {}
        self._completed_cleanups: dict[str, tuple[int, int, str | None]] = {}
        self._completed_abandons: dict[str, tuple[int, int, str | None]] = {}
        self._local_readiness_error_code: str | None = None
        # Patch 2: a SUSTAINED resident-control outbox failure while Round 5 is IDLE
        # (no active claim) soft-degrades -- new claims are refused but the durable
        # READY capsule is NOT torn down (no freshness_lost, no _capsule clear) so
        # recovery is instant when delivery resumes. Only a failure WITH an active
        # claim hard-demotes. Cleared once the resident control plane is proven healthy
        # again (a CURRENT keep-alive probe).
        self._readiness_degraded = False
        self._active_claim_ids: set[str] = set()
        # Consecutive WARMING-rewarm coordination-contention losses (a fence/lease
        # loss or store CAS conflict raised by publish_ready or the held-lease beat
        # during a rewarm). Tracked so the loop LOGS the contention at a doubling
        # cadence (visible above /readyz noise) and records a durable, named
        # last_error_code instead of vanishing into run()'s silent 1s retry -- the
        # live post-bell-towel wedge churned WARMING for 11+ minutes at attempt 20+
        # with last_error_code=None and no log because this loss was unrecorded.
        self._warm_contention_failures = 0
        self._cleanup_convergence_failures: dict[str, int] = {}
        # Consecutive ATTESTED resident IDENTITY changes observed at an idle READY
        # probe without a stable CURRENT in between. A bare freshness_lost rewarmed
        # over the SAME stale provider engines and cleared last_error at the
        # transient publish_ready, so a resident whose identity kept changing looped
        # identity-refresh <-> rewarming for minutes with err=None and an
        # un-claimable ring until a full restart. Tracked so the recovery is bounded
        # (-> the named self-verifiable ``runner_identity_unstable`` block after
        # MAX_IDENTITY_REESTABLISH_ATTEMPTS) and reset only when a CURRENT probe
        # proves a fresh, stable identity.
        self._identity_reestablish_failures = 0
        # Consecutive READY probes whose launch capsule did NOT belong to the slot
        # (``launch_capsule_missing``) without a belonging capsule in between. A
        # post-bout rewarm can publish READY and then find on the very next probe
        # that the capsule no longer belongs; the rewarm SUCCEEDS so the transient-
        # warm bound never fires and the transient ``publish_ready`` clears
        # ``last_error`` -> an invisible, unbounded ``launch_capsule_missing`` flood.
        # Tracked so the recovery is bounded (-> the named, self-verifiable
        # ``warm_capsule_unrecoverable`` block after MAX_CAPSULE_REESTABLISH_ATTEMPTS)
        # and reset only when a rewarm's capsule actually belongs to the slot.
        self._capsule_missing_failures = 0

    @property
    def capsule(self) -> Round5LaunchCapsule | None:
        return self._capsule

    @property
    def last_slot(self) -> Round5WarmSlot | None:
        """The most recent slot this coordinator observed/wrote (e.g. after a bell).

        Set synchronously by the durable transitions (accept_bell, claim, cleanup),
        so the manager can refresh its cached record from authoritative RUNNING
        truth immediately after a bell without an extra request-path read.
        """

        return self._last_slot

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

    async def close(self, *, shutdown_deadline_seconds: float | None = None) -> None:
        # 1. Begin closing TOCTOU-free under the lifecycle lock: set the closing flag,
        #    wake the loop, and snapshot the in-flight cleanup tasks. ``converge_cleanup``
        #    and ``begin_cleanup`` take the SAME lock / check the SAME flag, so once this
        #    critical section runs NO new cleanup task can register (they refuse) and
        #    every registered task is in ``pending``. Authority is NOT revoked here and
        #    the per-op heartbeats are NOT stopped: an in-flight reconcile may hold a
        #    SHIELDED, un-cancellable AWS call, and its coordinator fence MUST remain
        #    held (via that task's own ``_run_holding_lease`` beat) until the call
        #    actually returns -- otherwise cancelling/revoking + closing the store here
        #    would drop the fence while the AWS call runs and a replica could
        #    acquire_coordinator and double-reconcile the same resource (Finding B).
        async with self._lifecycle_lock:
            already_closed = self._closed
            self._closed = True
            self._wake.set()
            pending = [
                task for task in self._cleanup_tasks.values() if not task.done()
            ]
        if already_closed:
            return
        # 2. Stop new work: cancel the supervised loop so no further cycle runs. (The
        #    per-cleanup-task heartbeats live inside each cleanup task, NOT this loop,
        #    so cancelling the loop does not drop any in-flight cleanup's fence.)
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        self._capsule = None
        # 3. Wait for every in-flight cleanup task to finish, fence held, but ONLY up to
        #    a BOUNDED shutdown handoff deadline below the platform graceful-shutdown
        #    grace. We NEVER cancel these tasks (a cancel cannot stop a shielded AWS
        #    call and would detach it while dropping the fence). A converge that
        #    finishes success -> N+1; one that hits fence-loss/exception leaves the slot
        #    CLEANING for takeover.
        if pending:
            deadline = self._shutdown_handoff_deadline
            if shutdown_deadline_seconds is not None:
                # Never wait longer than the remaining platform grace when it is known.
                deadline = min(deadline, max(0.0, shutdown_deadline_seconds))
            try:
                done, still_pending = await asyncio.wait(
                    pending, timeout=max(0.0, deadline)
                )
            except asyncio.CancelledError:
                # Finding B nuance: close() ITSELF was cancelled (e.g. the SIGTERM
                # handler cancels the close task). ``asyncio.wait`` does not cancel the
                # tasks it awaited, and we MUST NOT cancel them or stop their
                # heartbeats here -- an in-flight reconcile may hold a shielded AWS
                # call. Leave the in-flight cleanup running with the fence held until
                # it finishes or the process dies; do NOT revoke authority / release
                # engines / close the store. Re-raise the cancellation to the caller;
                # the durable CLEANING + leases + heartbeat remain intact for takeover.
                logger.error(
                    "round5_shutdown_blocked installation_id=%s reason=close_cancelled "
                    "remaining=%d; leaving in-flight cleanup running with the fence "
                    "held (authority NOT revoked, store NOT closed)",
                    self.installation_id,
                    len([task for task in pending if not task.done()]),
                )
                raise
            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None:
                    logger.error(
                        "round5_cleanup_convergence_failed_at_close diagnostic=%s",
                        exc,
                        exc_info=exc,
                    )
            if still_pending:
                # DEADLINE HIT while shielded AWS work may still be in flight. Do NOT
                # revoke authority, release adopted engines, or close the store: any of
                # those would either drop the fence while THIS process is still
                # mutating or let a replica double-reconcile. Leave the durable CLEANING
                # slot + leases + the still-beating heartbeat in place and hand off to
                # restart/takeover -- process death + coordinator lease TTL expiry is
                # the takeover mechanism (the taker still _require_current_cleanup_owner).
                blocked_claims = sorted(
                    claim_id
                    for claim_id, task in self._cleanup_tasks.items()
                    if not task.done()
                )
                logger.error(
                    "round5_shutdown_blocked installation_id=%s remaining=%d "
                    "claim_ids=%s deadline_seconds=%.1f; retaining durable CLEANING + "
                    "leases + heartbeat for restart/takeover (authority NOT revoked, "
                    "adopted engines NOT released, store NOT closed)",
                    self.installation_id,
                    len(still_pending),
                    blocked_claims,
                    deadline,
                )
                return
        # 4. All in-flight cleanup finished within the deadline (or none was pending):
        #    orderly close -- revoke authority (fail-closed for any late mutation),
        #    release adopted engines, then close the store.
        self._authority_revoked = True
        release_all = getattr(self.provider, "release_all_adopted_engines", None)
        if callable(release_all):
            release_all()
        await self.store.close()

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        transient_failures = 0
        coordination_failures = 0
        while not self._closed:
            try:
                delay = await self.run_one_cycle()
            except asyncio.CancelledError:
                raise
            except (
                WarmCoordinatorHeldError,
                WarmStoreConflictError,
                WarmFenceLostError,
            ) as exc:
                # Coordination contention escaping run_one_cycle -- most commonly
                # acquire_coordinator refusing because ANOTHER owner holds the lease
                # (WarmCoordinatorHeldError), or a CAS/fence loss on a non-WARMING
                # branch. Previously fully SILENT (delay=1, no counter, no log), which
                # is why a persistent two-owner standoff or a churning rewarm loss
                # could run for many minutes drowned under /readyz. LOG at a doubling
                # cadence with the durable head so the next standoff is visible; the
                # WARMING rewarm path additionally records a durable last_error_code.
                coordination_failures += 1
                if coordination_failures & (coordination_failures - 1) == 0:
                    slot = self._last_slot
                    logger.warning(
                        "round5_warm_coordination_contention installation=%s "
                        "consecutive=%d cause=%s state=%s generation=%s: %s",
                        self.installation_id,
                        coordination_failures,
                        type(exc).__name__,
                        getattr(getattr(slot, "state", None), "value", None),
                        getattr(slot, "generation", None),
                        exc,
                    )
                delay = 1.0
                transient_failures = 0
            except (RetryableWarmError, BlockedWarmError):
                delay = min(5.0, self._retry_ceiling_seconds)
                transient_failures = 0
                coordination_failures = 0
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
                coordination_failures = 0
            else:
                transient_failures = 0
                coordination_failures = 0
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
        Leadership loss CANCELS the in-flight provider work and surfaces
        ``WarmFenceLostError``: a stale ``prepare``/``refresh`` must not run to
        completion and record a spurious terminal BLOCKED after authority has
        already moved to another live process.  The supervised loop then recovers
        under the new owner's fence.
        """

        stop = asyncio.Event()
        interval = max(0.02, self._coordinator_ttl.total_seconds() / 3)
        op_task = asyncio.create_task(operation())
        fence_lost = False

        async def beat() -> None:
            nonlocal fence_lost
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
                    try:
                        rebased = await self._rebase_held_slot(holder[0])
                    except Exception:
                        rebased = None
                    if rebased is None:
                        # Authority moved: abort the held provider operation instead
                        # of letting it finish and CAS/block under a fence we no
                        # longer own.
                        fence_lost = True
                        op_task.cancel()
                        return
                    holder[0] = rebased

        beat_task = asyncio.create_task(beat())
        try:
            try:
                result = await op_task
            except asyncio.CancelledError:
                if fence_lost:
                    raise WarmFenceLostError(
                        "coordinator fence lost during held provider operation"
                    ) from None
                raise
            if fence_lost:
                raise WarmFenceLostError(
                    "coordinator fence lost during held provider operation"
                )
            return result
        finally:
            stop.set()
            if not op_task.done():
                op_task.cancel()
            await asyncio.gather(beat_task, op_task, return_exceptions=True)

    async def _rebase_held_slot(self, held: Round5WarmSlot) -> Round5WarmSlot | None:
        """Renew a held operation's lease after this process moved the revision.

        The revision is a compare-and-swap token, not authority. This process
        advances it itself while a held operation runs: the supervised loop's
        ``acquire_coordinator`` writes at the top of every cycle, and
        ``begin_cleanup`` wakes that loop right after the manager's cleanup task
        has read the slot. Reading that as a lost fence cancelled the cleanup at
        its first beat, and the retries raced the same way (live 2026-09-26: a
        75 s Round 5 towel lost three 30 s attempts in a row before converging).
        Authority has moved only when another owner or fence holds the slot, or
        the slot has left the held state, generation or claim. Then this returns
        None and the caller cancels the operation exactly as before.
        """

        held_claim = held.claim.claim_id if held.claim is not None else None
        for _ in range(3):
            current = await self.store.read(self.installation_id)
            if (
                current is None
                or current.coordinator_owner != self.process_epoch
                or current.coordinator_fence != held.coordinator_fence
                or current.generation != held.generation
                or current.state != held.state
                or (current.claim.claim_id if current.claim is not None else None)
                != held_claim
            ):
                return None
            try:
                return await self.store.heartbeat_coordinator(
                    current,
                    now=self._clock(),
                    ttl=self._coordinator_ttl,
                )
            except (WarmFenceLostError, WarmStoreConflictError):
                continue
        return None

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
        if (
            slot.state == Round5WarmState.CLAIMED
            and slot.claim is not None
            and slot.claim.claim_id in self._inherited_claim_ids
        ):
            # A replacement process has no process-local active-claim signal and
            # must not normalize an expired pre-bell claim directly back to
            # READY/WARMING. ARM may already have staged PREPARED residents whose
            # exact logical job IDs live only in the durable claim. Move under
            # the cleanup fence first, then force the provider's abandoned-claim
            # reconciliation to settle both jobs and prove provider absence.
            claim_id = slot.claim.claim_id
            cleaning = await self.store.begin_cleanup(
                slot,
                claim_id=claim_id,
                now=self._clock(),
            )
            self._last_slot = cleaning
            # Route through the SINGLE coalesced convergence primitive -- no direct
            # provider.reconcile bypass -- so an inherited/restart takeover and a
            # concurrent manager request for the same claim collapse to one reconcile.
            return await self._drive_cleanup_convergence(cleaning, claim_id)
        if slot.state == Round5WarmState.RUNNING:
            # A live same-process bout is NOT the loop's to clean. Only an inherited
            # RUNNING (from a departed process) is fenced under CLEANING and
            # reconciled here, so it cannot stay externally ringable while its old
            # jobs and provider resources are proven absent.
            if (
                slot.claim is None
                or slot.claim.claim_id not in self._inherited_claim_ids
            ):
                return 1.0
            claim_id = slot.claim.claim_id
            cleaning = await self.store.begin_cleanup(
                slot,
                claim_id=claim_id,
                now=self._clock(),
            )
            self._last_slot = cleaning
            return await self._drive_cleanup_convergence(cleaning, claim_id)
        if slot.state == Round5WarmState.CLEANING:
            # Every CLAIMED->CLEANING route -- inherited takeover OR a same-process
            # self-fence (arm setup failure, TTL release, manager expiry) -- has the
            # coordinator as a GUARANTEED convergence owner, so a self-fenced slot is
            # never wedged waiting for a manager cleanup worker that may not exist.
            # begin_cleanup/reconcile/finish are idempotent and CAS-guarded, so this
            # never double-drives with the manager's own cleanup.
            if slot.claim is None:
                return 1.0
            if slot.next_retry_at is not None and slot.next_retry_at > now:
                return max(0.1, (slot.next_retry_at - now).total_seconds())
            return await self._drive_cleanup_convergence(slot, slot.claim.claim_id)
        if slot.state == Round5WarmState.CLAIMED:
            # A claim held by an operator that is still actively arming is
            # renewed backstage; a claim whose owner has gone away is fenced into
            # CLEANING (below).  Membership in ``_active_claim_ids`` is the
            # in-memory "operator still present" signal, registered when this
            # process hands the claim to the arm path and cleared at the bell or
            # on release.  It is intentionally not durable: after a restart the
            # new process will not renew an unknown claim, so the inherited-claim
            # cleanup path (above) takes it over instead.
            #
            # Renewal is gated on capsule IDENTITY (``_capsule_belongs``), NOT on
            # the launch margin (``_capsule_current``).  A live no-bell incident
            # dropped an actively-arming claim ~14s before its arm deadline purely
            # because the capsule's dispatch-credential launch margin had lapsed
            # while CLAIMED -- the READY-branch capsule refresh does not run under a
            # claim, so the capsule ages -- even though the runner identity was
            # intact and the operator was still mid-arm.  Launch margin is the
            # CLAIMABILITY gate for a NEW ring (``_claimable``), never a reason to
            # revoke an arm already in flight.  The claim's durable TTL is renewed
            # here on a third of its horizon, so it cannot lapse before the arm
            # deadline regardless of the two clocks' skew.
            if (
                slot.claim is not None
                and                 slot.claim.claim_id in self._active_claim_ids
                and self._capsule_belongs(slot)
                and now < self._active_claim_renewal_deadline(slot.claim)
            ):
                renewed = await self.store.renew_claim(
                    slot,
                    claim_id=slot.claim.claim_id,
                    now=now,
                    ttl=self._claim_ttl,
                )
                self._last_slot = renewed
                return max(0.1, self._claim_ttl.total_seconds() / 3)
            # Past the durable renewal horizon (or armed-lease deadline) a lingering
            # active-id is treated as a leak: stop renewing so the claim expires and
            # fences into CLEANING rather than being kept alive forever by a stuck
            # ARM task that never cleared its registration.
            if (
                slot.claim is not None
                and slot.claim.claim_id in self._active_claim_ids
                and now >= self._active_claim_renewal_deadline(slot.claim)
            ):
                self.release_claim_active(slot.claim.claim_id)
            # The operator abandoned, or this process lost its capsule.  Fence an
            # EXPIRED claim into CLEANING with the claim RETAINED (never READY or
            # WARMING over possibly-staged residents); a not-yet-expired inactive
            # claim simply waits for the manager's cleanup or its own expiry.  The
            # manager drives the same-process cleanup (settle exact residents +
            # prove absence + rewarm); begin_cleanup is idempotent, so this fence
            # never collides with it.
            fenced = await self.store.release_expired_claim(
                slot,
                now=now,
                still_fresh=False,
            )
            self._last_slot = fenced
            return 1.0
        if slot.state == Round5WarmState.READY:
            sealer = getattr(self.provider, "seal_absent_cleanup_journals", None)
            if callable(sealer):
                try:
                    await sealer(slot.generation)
                except Exception:
                    logger.warning(
                        "round5_journal_seal_failed generation=%s",
                        slot.generation,
                        exc_info=True,
                    )
            # Freshness gate is capsule IDENTITY only, not launch margin: a capsule
            # that has merely reached its renew_by must be refreshed (below), not
            # torn down and full-rewarmed. Only a genuinely missing/wrong-generation
            # capsule is freshness_lost.
            if not self._capsule_belongs(slot):
                return await self._reestablish_capsule(slot)
            # The capsule belongs this probe: the launch_capsule_missing churn (if
            # any) made progress, so reset its bounded budget HERE -- never at the
            # transient publish_ready -- so a rewarm that publishes READY but then
            # fails _capsule_belongs again cannot silently zero the counter.
            self._capsule_missing_failures = 0
            assert self._capsule is not None
            try:
                provenance_current = await self.provider.validate_ready(
                    slot,
                    self._capsule,
                )
            except asyncio.CancelledError:
                raise
            except RetryableWarmError as exc:
                # A throttled/timed-out provenance probe is a transient READ
                # failure off the request path -- NOT a runner identity change.
                # Keep the capsule, stay READY, and retry in place. Escalate to a
                # full rewarm (freshness_lost) only after repeated failures or when
                # provenance is about to lapse. Never swallow the error and fall
                # through to a "still current" success, which would fake READY.
                probe_now = self._clock()
                self._provenance_probe_failures += 1
                provenance_slack = (
                    (slot.provenance_expires_at - probe_now).total_seconds()
                    if slot.provenance_expires_at is not None
                    else 0.0
                )
                if (
                    self._provenance_probe_failures >= MAX_PROVENANCE_PROBE_FAILURES
                    or provenance_slack <= PROVENANCE_PROBE_MIN_SLACK_SECONDS
                ):
                    self._provenance_probe_failures = 0
                    self._capsule = None
                    self._last_slot = await self.store.freshness_lost(
                        slot,
                        code=exc.code,
                        now=probe_now,
                    )
                    return 0.0
                return max(
                    0.1,
                    min(PROVENANCE_PROBE_RETRY_SECONDS, provenance_slack / 2),
                )
            except BlockedWarmError as exc:
                self._provenance_probe_failures = 0
                self._capsule = None
                self._last_slot = await self.store.freshness_lost(
                    slot,
                    code=exc.code,
                    now=self._clock(),
                )
                return 0.0
            self._provenance_probe_failures = 0
            if provenance_current:
                # A CURRENT keep-alive probe proves the resident identity is stable
                # again: reset the attested-identity-change re-establishment budget
                # (Patch 5) and lift a prior soft-degrade / identity-reestablish
                # marker so claims are re-admitted. This is the SOLE place the marker
                # + budget are lifted, so the ring stays un-claimable through a churn
                # (never handed to a resident whose identity is still changing) and
                # becomes claimable only once a fresh identity is proven stable. The
                # capsule was never cleared here, so re-admission is instant.
                self._identity_reestablish_failures = 0
                self._capsule_missing_failures = 0
                if (
                    self._readiness_degraded
                    or self._local_readiness_error_code is not None
                ):
                    self._readiness_degraded = False
                    self._local_readiness_error_code = None
            if not provenance_current:
                # validate_ready() returns False ONLY for an ATTESTED runner identity
                # change (Patch 1 tri-state): a merely STALE/ABSENT heartbeat raises
                # RetryableWarmError and is absorbed by the strike budget above, so a
                # False here is a genuine identity change. A bare freshness_lost here
                # rewarmed over the SAME stale provider engines and cleared last_error
                # at the transient publish_ready, so a resident whose identity kept
                # changing (a systemd respawn storm under a concurrent round's DB load)
                # looped identity-refresh <-> rewarming for minutes with err=None and
                # an un-claimable ring, recovering only by a full restart. Force a
                # CLEAN, FENCED, BOUNDED, OBSERVABLE re-establishment instead.
                return await self._reestablish_identity(slot)
            slot = await self.store.renew_provenance(
                slot,
                now=self._clock(),
                ttl=timedelta(seconds=PROVENANCE_FRESHNESS_SECONDS),
            )
            self._last_slot = slot
            # 45-minute runner receipts are RENEWABLE, not a full-rewarm wall:
            # identity is immutable, provenance is renewable. When the credential
            # renew_by lands, or the receipt horizon approaches, republish fresh
            # receipts off a live probe (refresh_preparation re-observes per-bout
            # Proxy absence and re-reads runner identity, then re-seals brand-new
            # receipts on the SAME immutable identity). READY stays READY across the
            # horizon; only a genuine identity change or lapsed credential escalates.
            margin_now = self._clock()
            receipt_slack = (
                _receipt_expiry_bound(slot) - margin_now
            ).total_seconds()
            # Refresh with LEAD time relative to the TRUE launch-margin cliff (not
            # ``renew_by``): fire the credential/provenance refresh
            # RUNNER_CREDENTIAL_REFRESH_LEAD_SECONDS before the capsule stops meeting
            # its launch margin, so the new capsule is published while the OLD one is
            # still claimable. ``renew_by`` alone was insufficient -- it can sit AFTER
            # the cliff, so refreshing off it fired too late and flashed the idle
            # "Temporarily Unavailable" window. ``renew_by`` still forces a refresh if
            # it (or the receipt wall) lands first.
            cliff = self._capsule.launch_margin_cliff()
            cliff_due = (
                cliff - margin_now
            ).total_seconds() <= RUNNER_CREDENTIAL_REFRESH_LEAD_SECONDS
            renew_due = cliff_due or (
                slot.renew_by is not None and slot.renew_by <= margin_now
            )
            receipt_due = receipt_slack <= RUNNER_RECEIPT_REFRESH_MARGIN_SECONDS
            if renew_due or receipt_due:
                # Credential rotation must hold the coordinator lease for its whole
                # duration: the AWS refresh can outlast the default lease TTL, and a
                # second process must not prepare the same runners while a refresh
                # is in flight.
                holder = [slot]
                try:

                    async def refresh_preparation() -> Round5WarmPreparation:
                        return await self.provider.refresh_preparation(
                            holder[0], self._capsule
                        )

                    preparation = await self._run_holding_lease(
                        holder, refresh_preparation
                    )
                    slot = holder[0]
                    refresh_now = self._clock()
                    if not preparation.capsule.meets_launch_margin(refresh_now):
                        raise BlockedWarmError("credential_margin_insufficient")
                    slot = await self.store.update_capsule_receipt(
                        slot,
                        capsule=preparation.capsule,
                        shared_receipt=preparation.shared_receipt,
                        variants=preparation.variants,
                        now=refresh_now,
                    )
                    self._last_slot = slot
                    self._capsule = preparation.capsule
                except asyncio.CancelledError:
                    raise
                except RetryableWarmError:
                    # Transient refresh failure (throttle/timeout). The existing
                    # capsule is still valid, so stay READY in place and retry
                    # shortly rather than tearing down to a full rewarm. Escalate to
                    # freshness_lost only once the still-held credential can no
                    # longer meet the launch margin.
                    if self._capsule is None or not self._capsule.meets_launch_margin(
                        self._clock()
                    ):
                        self._capsule = None
                        self._last_slot = await self.store.freshness_lost(
                            slot,
                            code="credential_refresh_retryable",
                            now=self._clock(),
                        )
                        return 0.0
                    return max(0.1, PROVENANCE_PROBE_RETRY_SECONDS)
                except BlockedWarmError as exc:
                    self._capsule = None
                    self._last_slot = await self.store.freshness_lost(
                        slot,
                        code=exc.code,
                        now=self._clock(),
                    )
                    return 0.0
            delay_now = self._clock()
            # Wake LEAD seconds before the launch-margin cliff so the lead-time refresh
            # above fires while the current capsule is still claimable (no lapse).
            cliff_remaining = (
                self._capsule.launch_margin_cliff() - delay_now
            ).total_seconds() - RUNNER_CREDENTIAL_REFRESH_LEAD_SECONDS
            renew_remaining = (
                (slot.renew_by - delay_now).total_seconds()
                if slot.renew_by is not None
                else self._coordinator_ttl.total_seconds() / 2
            )
            remaining = min(cliff_remaining, renew_remaining)
            provenance_remaining = (
                (slot.provenance_expires_at - delay_now).total_seconds()
                if slot.provenance_expires_at is not None
                else 0.0
            )
            # Wake before the receipt refresh margin so the clean pre-wall rewarm
            # above always fires with time to spare.
            receipt_remaining = (
                _receipt_expiry_bound(slot) - delay_now
            ).total_seconds() - RUNNER_RECEIPT_REFRESH_MARGIN_SECONDS
            return max(
                0.1,
                min(
                    remaining,
                    provenance_remaining / 2,
                    receipt_remaining,
                    self._coordinator_ttl.total_seconds() / 2,
                ),
            )
        if slot.state == Round5WarmState.BLOCKED:
            # Blocker 4: a BLOCKED slot that still carries a CLAIM (unsettled
            # residents/jobs) must NEVER be normalized to WARMING/READY or re-warmed
            # (prepare) over that debt. Route it into CLEANING and converge through
            # the single cleanup owner, preserving the claim/job identities.
            if slot.claim is not None:
                cleaning = await self.store.begin_cleanup(
                    slot, claim_id=slot.claim.claim_id, now=now
                )
                self._last_slot = cleaning
                return await self._drive_cleanup_convergence(
                    cleaning, slot.claim.claim_id
                )
            # No claim: a TRUE permanent block (identity change, orphan Proxy,
            # fixture drift, unexpected) stays latched for an operator and is
            # surfaced honestly -- the same process must not silently churn it. A
            # SELF-VERIFIABLE block (a momentarily insufficient/expired credential
            # margin, a persisted transient, or a post-clean rewarm lineage) is
            # re-attempted at a bounded interval so the process recovers on its own.
            if _blocked_is_terminal(slot) or slot.last_error_at is None:
                return self._coordinator_ttl.total_seconds() / 2
            elapsed = (now - slot.last_error_at).total_seconds()
            if elapsed < SELF_VERIFIABLE_BLOCK_RETRY_SECONDS:
                return max(0.1, SELF_VERIFIABLE_BLOCK_RETRY_SECONDS - elapsed)
            slot = await self.store.freshness_lost(
                slot,
                code=f"{slot.last_error_code}_recheck",
                now=now,
            )
            self._last_slot = slot
            # fall through to re-attempt the warm below
        if slot.next_retry_at is not None and slot.next_retry_at > now:
            return max(0.1, (slot.next_retry_at - now).total_seconds())

        warm_attempt_token = self._select_warm_attempt_token(slot, now)
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
                    requires_cleaned_bout=holder[0].requires_cleaned_bout,
                )

            preparation = await self._run_holding_lease(holder, prepare_generation)
            slot = holder[0]
            if not preparation.capsule.meets_launch_margin(self._clock()):
                raise BlockedWarmError("credential_margin_insufficient")
            # Fresh-PRELOAD invariant: never publish READY unless BOTH lanes prove
            # a PRELOAD -> agent_ready round trip for THIS attempt token happened
            # this attempt. Closes the hole where a skipped/superseded re-PRELOAD
            # would otherwise reach READY on stale evidence and then flicker.
            self._require_fresh_preload(preparation, warm_attempt_token)
            ready = await self.store.publish_ready(
                slot,
                preparation=preparation,
                now=self._clock(),
            )
            self._last_slot = ready
            self._capsule = preparation.capsule
            # Do NOT clear an in-flight attested-identity-change re-establishment
            # marker here: this rewarm publishing READY is not yet PROOF the new
            # resident identity is stable (it may change again a beat later). Keep the
            # ring un-claimable and the recovery observable until a CURRENT keep-alive
            # probe confirms it -- the sole place the marker + budget are lifted (the
            # READY branch above). Clearing it at this transient READY is exactly what
            # made the identity churn invisible (err=None) before.
            # NEVER clear the readiness error at this transient READY while an
            # identity OR a capsule re-establishment is in flight: this publish is
            # not yet proof the new slot is stably claimable (the next probe may find
            # the capsule does not belong, or the identity changed again). Clearing
            # here is exactly what made both churns invisible (err=None). The marker
            # is lifted only by a CURRENT probe on a belonging capsule (READY branch).
            if (
                self._identity_reestablish_failures == 0
                and self._capsule_missing_failures == 0
            ):
                self._local_readiness_error_code = None
            self._warm_contention_failures = 0
            # The in-flight attempt token reached READY. Clear the reuse tracker so
            # a subsequent freshness_lost mints a genuinely fresh token rather than
            # re-warming a token that was already publicly claimable (anti-replay).
            self._warm_attempt_token = None
            self._warm_attempt_minted_at = None
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
        except (WarmFenceLostError, WarmStoreConflictError) as exc:
            # A fence/lease loss or store CAS conflict during the REWARM (raised by
            # publish_ready's fence check or the held-lease beat's
            # heartbeat_coordinator) must NOT vanish into run()'s silent 1s retry.
            # That silent path is the live post-bell-towel wedge: prepare() succeeds
            # (runners/variants report ready), then this loss aborts before READY, so
            # run() re-loops -- re-running a full paid prepare() each cycle -- while
            # last_error_code stays None, no log fires, and READY never lands (11+
            # minutes, attempt 20+, revision climbing). Make it OBSERVABLE and BOUNDED:
            #   * record a durable, named last_error_code on the CURRENT WARMING slot
            #     (best-effort, only while THIS process still owns the coordinator) so
            #     the public status shows WHY instead of a blank rewarm, and
            #   * back off with capped exponential jitter (via next_retry_at) so the
            #     loop stops re-preparing every cycle, and
            #   * WARN at a doubling cadence with a distinct, non-/readyz signal.
            # If authority genuinely moved, the next cycle's acquire_coordinator
            # defers to the new owner (WarmCoordinatorHeldError); otherwise this
            # process re-attempts under its own retained fence once contention clears
            # and converges to READY.
            self._warm_contention_failures += 1
            failures = self._warm_contention_failures
            exponent = min(10, max(0, failures - 1))
            ceiling = min(self._retry_ceiling_seconds, float(2**exponent))
            delay = max(0.5, self._random.uniform(0.0, ceiling))
            if failures & (failures - 1) == 0:
                logger.warning(
                    "round5_warm_rewarm_contention installation=%s generation=%s "
                    "consecutive=%d cause=%s: %s",
                    self.installation_id,
                    getattr(holder[0], "generation", None),
                    failures,
                    type(exc).__name__,
                    exc,
                )
            code = (
                "warm_fence_contention"
                if isinstance(exc, WarmFenceLostError)
                else "warm_store_contention"
            )
            try:
                current = await self.store.read(self.installation_id)
                if (
                    current is not None
                    and current.state == Round5WarmState.WARMING
                    and current.claim is None
                    and current.coordinator_owner == self.process_epoch
                ):
                    self._last_slot = await self.store.record_retry(
                        current,
                        code=code,
                        now=self._clock(),
                        next_retry_at=self._clock() + timedelta(seconds=delay),
                    )
            except (WarmFenceLostError, WarmStoreConflictError):
                # Authority/revision moved again between the loss and this best-effort
                # record: do not write under a lost fence. The next cycle re-reads and
                # re-acquires; the contention counter still drives the backoff below.
                pass
            return delay
        except RetryableWarmError as exc:
            self._warm_contention_failures = 0
            slot = holder[0]
            if slot.attempt_count >= MAX_TRANSIENT_WARM_ATTEMPTS:
                # A transient failure that persists this many consecutive attempts
                # is no longer plausibly transient. Stop retrying forever and
                # escalate to a DISTINCT terminal code (operator-attention), rather
                # than churn silently or pretend it will self-heal.
                self._last_slot = await self.store.record_blocked(
                    slot,
                    code=f"{exc.code}_persistent",
                    now=self._clock(),
                )
                return self._coordinator_ttl.total_seconds() / 2
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
            self._warm_contention_failures = 0
            self._last_slot = await self.store.record_blocked(
                holder[0],
                code=exc.code,
                now=self._clock(),
            )
            return self._coordinator_ttl.total_seconds() / 2

    async def _reestablish_capsule(self, slot: Round5WarmSlot) -> float:
        """Recover from a READY slot whose launch capsule no longer belongs (bounded).

        The READY keep-alive found the durable slot READY but this process no longer
        holds a capsule matching its generation/fence/token (``_capsule_belongs`` is
        False): a cleanup advanced the generation to N+1, or a superseding owner
        bumped the fence/token after this process published READY. The old path just
        did ``freshness_lost("launch_capsule_missing")`` and rewarmed; because that
        rewarm SUCCEEDS, the ``MAX_TRANSIENT_WARM_ATTEMPTS`` bound (checked only in
        the rewarm ERROR path) never fired, and the transient ``publish_ready`` kept
        clearing ``last_error`` -> an invisible, unbounded ``launch_capsule_missing``
        flood (frozen gen, err=None, 20+ ``identity-refresh <-> rewarming`` flips)
        that only a restart cleared. Force the SAME bounded, observable recovery the
        attested-identity path uses:

        * DISCARD the stale in-memory capsule and mark the recovery observable with a
          DURABLE marker (``warm_capsule_reestablishing``) that the transient
          ``publish_ready`` does NOT clear, so the ring stays un-claimable and
          ``public_status`` names WHY across the whole recovery -- lifted only once a
          rewarm's capsule belongs again (the READY branch resets the budget) and a
          CURRENT probe proves it (the provenance branch clears the marker).
        * BOUND it: after ``MAX_CAPSULE_REESTABLISH_ATTEMPTS`` consecutive
          non-belonging probes with no belonging capsule in between, escalate to the
          named, self-verifiable block ``warm_capsule_unrecoverable`` (re-checked on
          a bounded interval, self-recovers the instant a rewarm's capsule belongs)
          instead of the unbounded flood.
        """

        now = self._clock()
        self._capsule = None
        self._capsule_missing_failures += 1
        failures = self._capsule_missing_failures
        # Observable across the transient publish_ready the next rewarm performs.
        self._local_readiness_error_code = "warm_capsule_reestablishing"
        if failures & (failures - 1) == 0:
            logger.warning(
                "round5_warm_capsule_reestablish installation=%s generation=%s "
                "consecutive=%d: READY slot's launch capsule does not belong; "
                "forcing a bounded fenced rewarm",
                self.installation_id,
                slot.generation,
                failures,
            )
        if failures >= MAX_CAPSULE_REESTABLISH_ATTEMPTS:
            # The capsule will not stabilize within a bounded number of rewarms.
            # Surface a NAMED, self-verifiable block (re-checked on a bounded
            # interval, self-recovers once a rewarm's capsule belongs) instead of an
            # invisible, unbounded launch_capsule_missing flood.
            self._local_readiness_error_code = "warm_capsule_unrecoverable"
            self._last_slot = await self.store.record_blocked(
                slot,
                code="warm_capsule_unrecoverable",
                now=now,
            )
            return self._coordinator_ttl.total_seconds() / 2
        # A bounded, named freshness_lost -> WARMING drives the clean rewarm on the
        # next cycle. The persistent recovery marker above keeps it observable across
        # the transient publish_ready.
        self._last_slot = await self.store.freshness_lost(
            slot,
            code="launch_capsule_missing",
            now=now,
        )
        return 0.0

    async def _reestablish_identity(self, slot: Round5WarmSlot) -> float:
        """Recover from an ATTESTED resident identity change (fail-closed, bounded).

        Distinct from a transient STALE/ABSENT strike: the resident this generation's
        receipts attest is provably gone/replaced, so the provider's in-memory engines,
        the launch capsule, and the durable receipts all pin a stale identity. The old
        ``freshness_lost``-only path re-warmed over those stale engines and cleared
        ``last_error`` at the transient ``publish_ready``, so a resident whose identity
        kept changing looped identity-refresh <-> rewarming for minutes with
        ``err=None`` and an un-claimable ring until a full process restart minted one
        clean PRELOAD. Force the clean, fenced, bounded, observable re-establishment:

        * DISCARD the stale capsule (here) and the provider's stale engines / receipts
          / preparation, and RETIRE the old resident binding/job so a wedged
          same-process resident is drained rather than left beating a superseded token
          (``provider.reestablish``). The next ``prepare`` therefore builds fresh
          engines and issues a genuinely fresh PRELOAD for ONE bounded in-flight token
          (``_select_warm_attempt_token``), requiring fresh agent_ready identities on
          BOTH lanes before READY (``_require_fresh_preload``). Anti-replay and the
          single resident owner are preserved: a token that reached READY is never
          re-warmed, and no duplicate resident process is created.
        * Record a DURABLE, named recovery marker (``runner_identity_reestablishing``)
          that the transient ``publish_ready`` does NOT clear, so the recovery is
          observable in ``public_status`` and the ring stays un-claimable until a
          CURRENT probe proves a fresh, stable identity.
        * BOUND it: after ``MAX_IDENTITY_REESTABLISH_ATTEMPTS`` attested changes with no
          stable CURRENT in between, escalate to the named, self-verifiable block
          ``runner_identity_unstable`` (re-checked on a bounded interval, self-recovers
          once the resident settles) instead of an unbounded token/PRELOAD flood.
        """

        now = self._clock()
        self._identity_reestablish_failures += 1
        failures = self._identity_reestablish_failures
        # Discard the stale in-memory capsule and mark the recovery observable: the
        # named marker makes the ring un-claimable (``_claimable`` gates on it) and
        # public_status names WHY across the whole recovery INCLUDING the transient
        # READY the rewarm publishes before the confirming CURRENT probe. Lifted only
        # by a CURRENT probe (READY branch). We use the same coordinator-local marker
        # the idle soft-degrade uses, so no new claimability gate is introduced.
        self._capsule = None
        self._local_readiness_error_code = "runner_identity_reestablishing"
        # Retire the old resident binding/job and discard the provider's stale
        # engines/receipts/preparation so the next prepare() is a genuinely fresh
        # PRELOAD, not a rewarm over the changed identity. Best-effort and typed: a
        # lost fence/CAS propagates (the next cycle defers to the new owner); any other
        # failure is logged but never blocks recovery -- the fresh PRELOAD supersedes
        # the old binding regardless.
        reestablish = getattr(self.provider, "reestablish", None)
        if callable(reestablish):
            try:
                await reestablish(slot)
            except asyncio.CancelledError:
                raise
            except (WarmFenceLostError, WarmStoreConflictError):
                raise
            except Exception:
                logger.warning(
                    "round5_warm_identity_reestablish_discard_failed installation=%s "
                    "generation=%s consecutive=%d",
                    self.installation_id,
                    slot.generation,
                    failures,
                    exc_info=True,
                )
        if failures & (failures - 1) == 0:
            logger.warning(
                "round5_warm_identity_reestablish installation=%s generation=%s "
                "consecutive=%d: attested resident identity change; forcing a clean "
                "fenced rewarm (discard stale engines + retire old resident, "
                "re-PRELOAD both lanes for a fresh token)",
                self.installation_id,
                slot.generation,
                failures,
            )
        if failures >= MAX_IDENTITY_REESTABLISH_ATTEMPTS:
            # The resident identity will not stabilize within a bounded number of
            # clean rewarms. Stop the tight identity-refresh <-> rewarming churn and
            # surface a NAMED, self-verifiable block (re-checked on a bounded interval,
            # self-recovers once the resident settles) instead of an invisible,
            # unbounded token/PRELOAD flood that only a full restart cleared.
            self._local_readiness_error_code = "runner_identity_unstable"
            self._last_slot = await self.store.record_blocked(
                slot,
                code="runner_identity_unstable",
                now=now,
            )
            return self._coordinator_ttl.total_seconds() / 2
        # A bounded, named freshness_lost -> WARMING drives the clean rewarm on the
        # next cycle. Keep the durable slot code (``runner_provenance_changed``) that
        # names the attested change; the persistent recovery marker above is what
        # keeps it observable across the transient publish_ready.
        self._last_slot = await self.store.freshness_lost(
            slot,
            code="runner_provenance_changed",
            now=now,
        )
        return 0.0

    def _select_warm_attempt_token(
        self, slot: Round5WarmSlot, now: datetime
    ) -> str:
        """Choose the warm_attempt_token for this WARMING beat (bounded mint rate).

        Reuse the SAME in-flight token across retryable WARMING retries so the
        single-bout resident sees exactly ONE PRELOAD per lane and can converge,
        instead of chasing a superseded PRELOAD backlog minted faster than it can
        drain (the post-bout rewarm storm). A fresh token is minted only when:

        * there is no in-flight token that still matches the durable slot (a fresh
          warm episode -- first boot, a post-clean rewarm to N+1 which resets the
          durable token to ``None``, or a token this process did not mint, e.g. a
          restart/takeover), or
        * the reuse window elapsed without the resident attesting the in-flight
          token: rotate ONCE, observably, so a genuinely wedged/rebooted resident
          gets a freshly delivered PRELOAD rather than an idempotent no-op.

        Reuse is confined to a token that never became READY/claimed (it is
        cleared at ``publish_ready``), so a superseded bout token is never reused
        and anti-replay is preserved. The overall retry flood stays bounded by
        ``MAX_TRANSIENT_WARM_ATTEMPTS`` -> a named terminal block.
        """

        in_flight = self._warm_attempt_token
        minted_at = self._warm_attempt_minted_at
        if (
            in_flight is not None
            and in_flight == slot.warm_attempt_token
            and minted_at is not None
        ):
            if (now - minted_at).total_seconds() < WARM_ATTEMPT_TOKEN_REUSE_SECONDS:
                return in_flight
            # The reuse window lapsed without the resident attesting this token.
            # Supersede exactly once, observably, so the outbox does not accrue a
            # backlog and an operator can see that a rewarm is not converging.
            logger.warning(
                "round5_warm_attempt_token_rotated installation=%s generation=%s "
                "attempt=%d prior_token_age_s=%.1f: resident did not attest the "
                "in-flight PRELOAD within the reuse window; delivering a fresh one",
                self.installation_id,
                slot.generation,
                slot.attempt_count,
                (now - minted_at).total_seconds(),
            )
        token = f"attempt-{uuid4().hex}"
        self._warm_attempt_token = token
        self._warm_attempt_minted_at = now
        return token

    def _require_fresh_preload(
        self, preparation: Round5WarmPreparation, warm_attempt_token: str
    ) -> None:
        """Prove the preparation is a FRESH PRELOAD for this exact attempt token.

        A preparation may only be published READY when BOTH lanes carry this
        attempt's token and an attested resident process identity that could only
        come from a PRELOAD -> agent_ready round trip observed THIS warm attempt
        (each warm builds fresh engines whose adapters start unattested, so an
        attested process_boot_id/process_pid cannot be inherited from a prior
        attempt). This fails closed with a distinct, non-secret code rather than
        resting READY on stale/duplicate-PRELOAD evidence -- the exact hazard a
        skipped or superseded re-PRELOAD would otherwise slip past ``publish_ready``.
        """

        shared = preparation.shared_receipt
        capsule = preparation.capsule
        if capsule.warm_attempt_token != warm_attempt_token:
            raise BlockedWarmError("warm_ready_without_fresh_preload")
        for runner in (shared.lakebase_runner, shared.competitor_runner):
            if (
                not runner.process_boot_id
                or runner.process_boot_id == "unattested"
                or runner.process_pid <= 0
            ):
                raise BlockedWarmError("warm_ready_without_fresh_preload")

    def _capsule_current(self, slot: Round5WarmSlot, now: datetime) -> bool:
        capsule = self._capsule
        return bool(
            capsule is not None
            and capsule.generation == slot.generation
            and capsule.coordinator_fence == slot.coordinator_fence
            and capsule.broker_epoch == slot.broker_epoch
            and capsule.warm_attempt_token == slot.warm_attempt_token
            and capsule.meets_launch_margin(now)
        )

    def _capsule_belongs(self, slot: Round5WarmSlot) -> bool:
        """Whether this process holds the capsule for exactly this slot generation.

        Identity only -- deliberately NOT gated on launch margin. The READY
        keep-alive uses this so that a capsule which has reached its ``renew_by``
        (and therefore no longer meets the launch margin, because dispatch creds
        are minted at margin+epsilon) is REFRESHED by the renew_by branch rather
        than declared ``freshness_lost`` and full-``prepare()``-rewarmed. Charging
        the launch-margin check ahead of the refresh was the ~3-minute rewarm
        storm (151 warm_ready/freshness_lost cycles overnight). Launch-margin
        remains the CLAIMABILITY gate (``_capsule_current`` / ``_claimable``); a
        capsule the refresh cannot revive still fails there and in the refresh
        branch's own margin check.
        """

        capsule = self._capsule
        return bool(
            capsule is not None
            and capsule.generation == slot.generation
            and capsule.coordinator_fence == slot.coordinator_fence
            and capsule.broker_epoch == slot.broker_epoch
            and capsule.warm_attempt_token == slot.warm_attempt_token
        )

    def _claimable(self, slot: Round5WarmSlot, now: datetime) -> bool:
        # ``renew_by`` is the credential REFRESH trigger, NOT the claimability cliff:
        # the real cliff is the launch margin checked by ``_capsule_current`` (Patch 3).
        # Gating claimability on ``renew_by > now`` too made the ring un-claimable in
        # the [renew_by, launch-margin-cliff) refresh window and flashed the idle fight
        # card "Temporarily Unavailable" every keep-alive cycle. The lead-time refresh
        # republishes a fresh capsule before the cliff, so dropping ``renew_by`` here
        # keeps idle READY continuously claimable across credential renewals while the
        # true launch-margin cliff still fails closed.
        return bool(
            slot.state == Round5WarmState.READY
            and self._local_readiness_error_code is None
            and not self._readiness_degraded
            and slot.ready_expires_at is not None
            and slot.ready_expires_at > now
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
        # Wake the supervised loop so it enters the claim-renewal cadence promptly
        # instead of finishing a long READY-branch sleep first; a slow ARM must
        # never let the fresh claim lapse before the loop starts renewing it.
        self.wake()
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
            capsule_broker_epoch=capsule.broker_epoch,
            capsule_warm_attempt_token=capsule.warm_attempt_token,
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
            # Enter the renewal cadence promptly (see ``claim``); a slow ARM must
            # not let the fresh claim lapse before the loop starts renewing it.
            self.wake()
        return claimed, capsule, main_lease, cleanup_lease

    async def return_unstarted_claim(
        self,
        claim_id: str,
        *,
        main_store: object | None = None,
        main_lease: object | None = None,
        cleanup_store: object | None = None,
        cleanup_lease: object | None = None,
    ) -> Round5WarmSlot:
        """Return an ARM claim that never staged residents back to READY.

        Rings are released in the same durable transaction when the store
        implements ``return_unstarted_with_leases``. Memory stores restore READY
        first; the manager then releases the matching leases.
        """

        _safe_identifier(claim_id, "claim_id")
        self.release_claim_active(claim_id)
        slot = await self.store.read(self.installation_id)
        if slot is None:
            raise WarmFenceLostError("Round 5 warm slot is unavailable")
        now = self._clock()
        atomic = getattr(self.store, "return_unstarted_with_leases", None)
        if (
            callable(atomic)
            and main_store is not None
            and cleanup_store is not None
        ):
            updated = await atomic(
                slot,
                claim_id=claim_id,
                main_ring_key=main_store.ring_key,
                cleanup_ring_key=cleanup_store.ring_key,
                main_lease=main_lease,
                cleanup_lease=cleanup_lease,
                now=now,
            )
        else:
            returned = getattr(self.store, "return_unstarted_claim", None)
            if not callable(returned):
                raise WarmClaimUnavailableError(
                    "Round 5 unstarted claim return is unavailable"
                )
            updated = await returned(slot, claim_id=claim_id, now=now)
        self._last_slot = updated
        self.wake()
        return updated

    def mark_claim_active(
        self, claim_id: str, *, armed_deadline: datetime | None = None
    ) -> None:
        """Signal that an operator is actively holding this claim through ARM.

        While registered, the supervised loop renews the claim's expiry backstage
        instead of releasing it, so a slow ARM (SSM capacity preflight, long
        credential mint) cannot drop the claim before the bell.  The manager
        clears the registration at the bell and on every arm-abandon path.

        ``armed_deadline`` optionally binds renewal to the durable armed lease
        deadline; the manager may register it so a leaked ARM task cannot renew
        past the lease it actually holds. Renewal is always additionally bounded
        by a durable horizon from the claim's ``claimed_at`` (see the CLAIMED
        branch), so an active-id can never outlive the session indefinitely.
        """

        _safe_identifier(claim_id, "claim_id")
        self._active_claim_ids.add(claim_id)
        if armed_deadline is not None:
            self._active_claim_deadlines[claim_id] = _utc(
                armed_deadline, "armed_deadline"
            )

    def set_active_claim_deadline(
        self, claim_id: str, armed_deadline: datetime
    ) -> None:
        """Register/refresh the durable armed-lease deadline for an active claim."""

        _safe_identifier(claim_id, "claim_id")
        if claim_id in self._active_claim_ids:
            self._active_claim_deadlines[claim_id] = _utc(
                armed_deadline, "armed_deadline"
            )

    def release_claim_active(self, claim_id: str) -> None:
        """Stop backstage renewal so an abandoned claim can expire and release."""

        self._active_claim_ids.discard(claim_id)
        self._active_claim_deadlines.pop(claim_id, None)

    def adopt_claimed_engine(self, claim_id: str, engine: object) -> None:
        """Hand the manager's claimed engine to the provider (the sole janitor).

        Lets the provider's cleanup reconcile cancel the ARM-staged residents on the
        exact engine that staged them, under the coordinator's lease/fence, so the
        manager performs no external cleanup mutation of its own.
        """

        _safe_identifier(claim_id, "claim_id")
        adopt = getattr(self.provider, "adopt_claimed_engine", None)
        if callable(adopt):
            adopt(claim_id, engine)

    def _active_claim_renewal_deadline(self, claim: Round5BoutClaim) -> datetime:
        """The latest instant an active claim may still be renewed.

        When the manager registered the authoritative armed-lease deadline, renewal
        is allowed through EXACTLY that deadline -- it is NOT min'd with the coarse
        leak-fallback horizon, so a legitimately long configured arm TTL (e.g.
        1800s) is honored in full. The claimed_at leak-fallback horizon applies
        ONLY when no armed deadline was registered (a leaked/stuck active-id that
        never learned its real deadline).
        """

        armed = self._active_claim_deadlines.get(claim.claim_id)
        if armed is not None:
            return armed
        return claim.claimed_at + self._max_active_arm_renewal

    async def _drive_cleanup_convergence(
        self, cleaning: Round5WarmSlot, claim_id: str
    ) -> float:
        """Supervised-loop entry to cleanup convergence: delegates to the single
        coalesced ``converge_cleanup`` so the loop never double-drives the manager's
        request for the same claim."""

        self._cleanup_origins.setdefault(
            claim_id,
            (
                cleaning.generation,
                cleaning.coordinator_fence,
                cleaning.coordinator_owner,
            ),
        )
        try:
            result = await self.converge_cleanup(claim_id)
        except WarmFenceLostError:
            # This owner no longer has CAS authority to persist a retry marker.
            # Keep the durable slot in CLEANING for the winner to adopt, but make
            # the authority transfer explicit rather than silently spinning.
            logger.warning(
                "round5_cleanup_fence_lost claim_id=%s "
                "state=CLEANING action=await_takeover",
                claim_id,
            )
            self._last_slot = await self.store.read(self.installation_id)
            return 1.0
        except (RetryableWarmError, BlockedWarmError) as exc:
            failures = self._cleanup_convergence_failures.get(claim_id, 0) + 1
            self._cleanup_convergence_failures[claim_id] = failures
            ceiling = min(
                self._retry_ceiling_seconds,
                max(1.0, float(2 ** min(failures - 1, 6))),
            )
            delay = max(1.0, self._random.uniform(0.0, ceiling))
            current = await self.store.read(self.installation_id)
            if (
                current is not None
                and current.state == Round5WarmState.CLEANING
                and current.claim is not None
                and current.claim.claim_id == claim_id
                and current.coordinator_owner == self.process_epoch
            ):
                current = await self.store.record_cleanup_retry(
                    current,
                    code=exc.code,
                    now=self._clock(),
                    next_retry_at=self._clock() + timedelta(seconds=delay),
                )
                self._last_slot = current
            return delay
        self._cleanup_convergence_failures.pop(claim_id, None)
        self._last_slot = result
        return 0.0 if result.state == Round5WarmState.WARMING else 1.0

    async def converge_cleanup(self, claim_id: str) -> Round5WarmSlot:
        """The SINGLE coalesced cleanup convergence for a claim (blocker 2).

        Both the supervised loop and any manager-driven request route through here,
        keyed by ``claim_id``. Concurrent callers await the SAME in-flight task, so
        there is exactly one provider reconcile+finish sequence per claim within a
        process; across replicas the durable finish CAS (fence/generation) coalesces
        to a single N+1 transition and the provider's reconcile is idempotent.
        """

        _safe_identifier(claim_id, "claim_id")
        # Registration is serialized against close() via the lifecycle lock so a
        # convergence started concurrently with (or after) close cannot slip a new
        # task past close()'s snapshot and mutate after the store is closed. Once
        # closing/closed, refuse outright -- no new cleanup task is created.
        async with self._lifecycle_lock:
            if self._closed:
                raise WarmFenceLostError(
                    "Round 5 coordinator is closing; cleanup convergence refused"
                )
            existing = self._cleanup_tasks.get(claim_id)
            if existing is not None and not existing.done():
                # A waiter awaits the SHARED task through a shield: if the waiter is
                # cancelled, the shared cleanup task keeps running for the others.
                task = existing
            else:
                task = asyncio.create_task(self._converge_cleanup_once(claim_id))
                self._cleanup_tasks[claim_id] = task
                # The registry entry's lifetime is owned by TASK COMPLETION, not the
                # creator's control flow. If the creator is cancelled while awaiting,
                # the entry stays registered (the done callback has not fired) so a
                # concurrent or immediately-following caller coalesces onto the SAME
                # still-running task rather than starting a second reconcile. On
                # completion (success OR exception) the callback clears the entry so
                # a retry can start fresh.
                task.add_done_callback(
                    lambda finished, cid=claim_id: self._cleanup_task_finished(
                        cid, finished
                    )
                )
        # The creator, too, only shields (OUTSIDE the lock -- never hold the
        # lifecycle lock across the reconcile): its cancellation must not cancel the
        # shared task out from under the joiners.
        return await asyncio.shield(task)

    def _cleanup_task_finished(
        self, claim_id: str, finished: asyncio.Task[Round5WarmSlot]
    ) -> None:
        if self._cleanup_tasks.get(claim_id) is finished:
            self._cleanup_tasks.pop(claim_id, None)
        if finished.cancelled():
            return
        exc = finished.exception()
        if exc is not None:
            logger.error(
                "round5_cleanup_convergence_failed claim_id=%s diagnostic=%s",
                claim_id,
                exc,
                exc_info=exc,
            )

    def _release_provider_adopted_engine(self, claim_id: str) -> None:
        release = getattr(self.provider, "release_adopted_engine", None)
        if callable(release):
            release(claim_id)

    async def _converge_cleanup_once(self, claim_id: str) -> Round5WarmSlot:
        slot = await self.store.read(self.installation_id)
        if slot is None:
            raise WarmFenceLostError("Round 5 warm slot is unavailable")
        # Already converged for this claim (finished to N+1 by a prior/coalesced run).
        if slot.claim is None or slot.claim.claim_id != claim_id:
            self._release_provider_adopted_engine(claim_id)
            self._last_slot = slot
            return slot
        if slot.state != Round5WarmState.CLEANING:
            raise WarmFenceLostError("Round 5 cleanup fence changed before convergence")
        self._require_current_cleanup_owner(slot, now=self._clock())
        self._cleanup_origins.setdefault(
            claim_id,
            (slot.generation, slot.coordinator_fence, slot.coordinator_owner),
        )
        # Blocker 3: run the (possibly long) provider reconciliation UNDER the lease
        # heartbeat so the coordinator lease cannot silently lapse mid-cleanup; a
        # lost fence aborts the reconcile and surfaces WarmFenceLostError.
        holder = [slot]

        async def do_reconcile() -> bool:
            return await self.provider.reconcile(holder[0])

        reconciled = await self._run_holding_lease(holder, do_reconcile)
        if reconciled:
            self._verified_clean_claim_ids.add(claim_id)
            warmed = await self._finish_cleanup_transition(claim_id)
            self._release_provider_adopted_engine(claim_id)
            self._capsule = None
            self._inherited_claim_ids.discard(claim_id)
            self._last_slot = warmed
            return warmed
        self._last_slot = holder[0]
        return holder[0]

    async def abandon_claim(self, claim_id: str) -> Round5WarmSlot:
        """Fence a pre-bell claim into CLEANING (retaining the claim).

        There is NO direct CLAIMED->READY/WARMING fast path: it rested on an
        in-process/forgeable "residents settled" signal, and skipping the cleanup
        fence could strand a staged resident or race the cleanup owner. Every
        no-bell abandon now enters CLEANING and converges the one and only way --
        exact durable SETTLED + provider/journal absence, then
        finish_cleanup_and_rewarm to generation N+1. This simply delegates to the
        idempotent CLEANING fence so callers that still invoke ``abandon_claim``
        get the safe route.
        """

        return await self.begin_cleanup(claim_id)

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
        # Patch 2 (claim-aware, fail-closed-but-no-flicker): a persistent outbox
        # failure is handled by the ACTIVITY state of the ring, and NEVER by clearing
        # the capsule before a durable commit.
        #   * IDLE (slot.claim is None): SOFT degrade -- refuse NEW claims/bells but
        #     keep the durable READY slot and the in-memory capsule intact (no
        #     freshness_lost, no _capsule clear). No bout is being served, so there is
        #     nothing to tear down; recovery is instant when delivery resumes. This is
        #     what stops a sustained-but-recoverable idle outbox fault from flashing
        #     the fight card into a full WARMING rewarm.
        #   * ACTIVE (slot.claim present): a live bout cannot get its resident control,
        #     so hard-demote via a durable freshness_lost and withdraw the capsule --
        #     only AFTER the CAS commits (a blip that cannot win the CAS leaves the
        #     capsule intact).
        try:
            slot = await self.store.read(self.installation_id)
            self._last_slot = slot
            if slot is None or slot.state != Round5WarmState.READY:
                # Only a READY (idle) ring is soft-degraded here. A CLAIMED/RUNNING ring
                # is a live bout that owns its own control-delivery failure path; the
                # warm keep-alive must NOT tear it down or steal it. (A READY slot never
                # carries a claim -- claim() transitions it to CLAIMED -- so idle READY
                # is the only case this handler acts on.)
                return
            # SOFT degrade: refuse new claims/bells but keep the durable READY slot and
            # the in-memory capsule intact (no freshness_lost, no _capsule clear), so a
            # sustained-but-recoverable idle outbox fault never rewarms/flashes the ring;
            # recovery is instant once a CURRENT keep-alive probe lifts the degrade.
            self._readiness_degraded = True
            self._local_readiness_error_code = code
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

    async def _authority_guard(self) -> None:
        """Durable authority check the provider invokes right before any mutation.

        Reads the authoritative durable slot and refuses (WarmFenceLostError) if
        this process no longer owns the coordinator lease -- another process took
        over (owner/fence changed) or the lease expired. This is the durable half
        of Blocker 2: ``_run_holding_lease`` cancellation is advisory (a provider
        may swallow CancelledError), but a stale owner is still refused HERE,
        immediately before the external mutation, so it cannot mutate under a fence
        it no longer holds.
        """

        if self._authority_revoked:
            raise WarmFenceLostError(
                "Round 5 coordinator authority was revoked during shutdown"
            )
        current = await self.store.read(self.installation_id)
        if current is None:
            raise WarmFenceLostError(
                "Round 5 warm slot vanished before a provider mutation"
            )
        self._require_current_cleanup_owner(current, now=self._clock())

    async def begin_cleanup(self, claim_id: str) -> Round5WarmSlot:
        # The closed-check AND the whole durable CLEANING transition run under the
        # SAME lifecycle lock as converge_cleanup registration and close(). This makes
        # refusing-once-closing ATOMIC with the flag and closes the TOCTOU where
        # begin_cleanup passed the _closed check, yielded at ``store.read``, and then
        # resumed after close() had revoked authority and closed the store: close()
        # must take this lock for its critical section, and begin_cleanup holds it
        # until its CAS completes, so the two are strictly serialized (begin_cleanup
        # fully completes before close, or is refused after close set the flag).
        async with self._lifecycle_lock:
            if self._closed:
                raise WarmFenceLostError(
                    "Round 5 coordinator is closing; begin_cleanup refused"
                )
            self.release_claim_active(claim_id)
            for attempt in range(CLEANUP_FINALIZE_CONFLICT_ATTEMPTS):
                slot = await self.store.read(self.installation_id)
                if slot is None:
                    raise WarmFenceLostError("Round 5 warm slot is unavailable")
                now = self._clock()
                self._require_current_cleanup_owner(slot, now=now)
                try:
                    cleaning = await self.store.begin_cleanup(
                        slot,
                        claim_id=claim_id,
                        now=now,
                    )
                    break
                except WarmStoreConflictError:
                    # The compare-and-swap lost to this process's own supervised
                    # loop, which rewrites the slot on every cycle while a bout is
                    # RUNNING. Live 2026-09-26 a verified bout's handoff logged three
                    # "durable cleanup start is not settled" errors in a row before
                    # the manager's retry won. Re-read and retry with the same
                    # jittered backoff finish_cleanup uses; a real owner, fence or
                    # claim change still refuses through the checks above.
                    if attempt + 1 >= CLEANUP_FINALIZE_CONFLICT_ATTEMPTS:
                        raise
                    ceiling = min(0.25, 0.01 * (2 ** min(attempt, 5)))
                    await self._sleep(self._random.uniform(0.0, ceiling))
            self._last_slot = cleaning
            self._cleanup_origins[claim_id] = (
                cleaning.generation,
                cleaning.coordinator_fence,
                cleaning.coordinator_owner,
            )
            # Establish EXCLUSIVE janitor ownership of the adopted engine BEFORE the
            # supervised loop is woken to converge this CLEANING slot. This runs
            # synchronously (no await) immediately after the durable CLAIMED->CLEANING
            # transition, so there is no interleaving point between making CLEANING
            # durable and marking the engine janitor-owned: the loop we are about to
            # wake sees an engine already transferred to the single cleanup owner, and
            # the engine's own legacy self-cleanup path is suppressed.
            transfer = getattr(
                self.provider, "transfer_adopted_engine_at_cleaning", None
            )
            if callable(transfer):
                transfer(claim_id)
            # Wake the supervised loop so its generic CLEANING branch converges this
            # cleanup promptly instead of waiting out the loop's last scheduled sleep.
            # This mirrors the latency guarantee ``finish_cleanup_and_rewarm`` already
            # provides. We only reach here after a SUCCESSFUL durable transition -- a
            # rejected stale fence raises in ``_require_current_cleanup_owner`` (or the
            # store CAS) above and never wakes. A successful transition (including an
            # idempotent already-CLEANING call) returns a CLEANING slot that may owe
            # convergence work, so it wakes; anything that is somehow not CLEANING owes
            # the loop nothing and must not wake.
            if cleaning.state == Round5WarmState.CLEANING:
                self._wake.set()
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
                "round5_start_stage": "rewarming",
                "round5_ring_ready": False,
                "round5_cleanup_owed": False,
                "round5_warm_last_error_code": (
                    self._local_readiness_error_code
                ),
            }
        now = self._clock()
        ring_ready = self._claimable(slot, now)
        start_stage = (
            "ready"
            if ring_ready
            else "cleaning"
            if slot.state == Round5WarmState.CLEANING
            else "claim-drain"
            if slot.state == Round5WarmState.CLAIMED
            else "terminal-blocked"
            if _blocked_is_terminal(slot)
            else "identity-refresh"
            if slot.state == Round5WarmState.READY
            else "rewarming"
        )
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
            "round5_start_stage": start_stage,
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
            # True only for a permanent block that will NOT self-recover; the
            # catalog must not promise auto-unlock in that case. Self-verifiable
            # blocks retry on a bounded interval and are not terminal.
            "round5_warm_blocked_terminal": _blocked_is_terminal(slot),
            "round5_claim_id": slot.claim.claim_id if slot.claim else None,
            "round5_claim_expires_at": (
                slot.claim.claim_expires_at.isoformat() if slot.claim else None
            ),
            "round5_cleanup_owed": slot.state == Round5WarmState.CLEANING,
        }
