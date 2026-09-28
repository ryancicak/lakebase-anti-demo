from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol
from uuid import uuid4

LOGGER = logging.getLogger(__name__)


class ModelScoreError(RuntimeError):
    """Base class for the Round 4 Managed Sync proof."""


class ModelScoreNotArmedError(ModelScoreError):
    """The synced table does not satisfy the sealed start contract."""


class ModelScorePipelineError(ModelScoreError):
    """Managed Sync reported a terminal or unhealthy pipeline state."""


class ModelScoreStaleStatusError(ModelScoreError):
    """Managed Sync status cannot prove it processed the source commit."""


class ModelScoreVerificationError(ModelScoreError):
    """The source or fresh application read did not match the exact update."""


class ModelScoreTimeoutError(ModelScoreError):
    """The bounded Managed Sync polling window expired."""


class ModelScoreDuplicateRedoError(ModelScoreError):
    """A redo was repeated or did not contain a distinct v2 update."""


class ModelScorePhase(StrEnum):
    PREFLIGHT = "preflight"
    ARMED = "armed"
    COMMITTING_SOURCE = "committing_source"
    WAITING_SYNC = "waiting_sync"
    READING_APPLICATION = "reading_application"
    VERIFIED = "verified"
    FAILED = "failed"


class ManagedSyncState(StrEnum):
    RUNNING = "running"
    STARTING = "starting"
    FAILED = "failed"
    STOPPED = "stopped"


class ModelScoreProofKind(StrEnum):
    INITIAL = "initial"
    REDO = "redo"


@dataclass(frozen=True)
class ModelScoreRow:
    entity_id: str
    score: float
    model_version: str
    proof_nonce: str


OWNED_PROOF_SHAPES: tuple[tuple[float, str, str], ...] = (
    (0.81, "risk-v1", "round4-v1-"),
    (0.33, "risk-v2", "round4-v2-"),
)


def is_owned_prior_proof(row: ModelScoreRow | None) -> bool:
    """True when a row is residue this demo wrote during an earlier Round 4 proof.

    A completed Round 4 leaves its run-owned update in the source table, so both
    the engine's re-arm and the readiness gate need the same answer to "may I
    overwrite this?".  Anything that is not an exact demo-owned shape belongs to
    somebody else and must never be replaced.
    """

    if row is None:
        return False
    for score, model_version, prefix in OWNED_PROOF_SHAPES:
        if row.score != score or row.model_version != model_version:
            continue
        if not row.proof_nonce.startswith(prefix):
            continue
        suffix = row.proof_nonce.removeprefix(prefix)
        if len(suffix) == 32 and all(character in "0123456789abcdef" for character in suffix):
            return True
    return False


@dataclass(frozen=True)
class ModelScoreUpdate:
    entity_id: str
    score: float
    model_version: str
    proof_nonce: str

    def __post_init__(self) -> None:
        if not self.entity_id.strip():
            raise ValueError("entity_id is required")
        if not self.model_version.strip():
            raise ValueError("model_version is required")
        if not self.proof_nonce.strip():
            raise ValueError("proof_nonce is required")

    @property
    def row(self) -> ModelScoreRow:
        return ModelScoreRow(
            entity_id=self.entity_id,
            score=self.score,
            model_version=self.model_version,
            proof_nonce=self.proof_nonce,
        )


@dataclass(frozen=True)
class DeltaCommit:
    version: int
    committed_at: datetime


@dataclass(frozen=True)
class ManagedSyncStatus:
    pipeline_id: str
    source_table: str
    synced_table: str
    state: ManagedSyncState
    cdf_enabled: bool
    continuous: bool
    source_version: int
    last_processed_version: int
    last_sync_delta_version: int
    last_sync_delta_commit_time: datetime
    observed_at: datetime
    sync_end_time: datetime | None = None
    failure: str | None = None


@dataclass(frozen=True)
class ModelScoreProgress:
    phase: ModelScorePhase
    status: str
    occurred_at: datetime
    attempt: int | None = None
    elapsed_ms: float | None = None


@dataclass(frozen=True)
class ModelScoreContract:
    pipeline_id: str
    source_table: str
    synced_table: str
    entity_id: str = "customer-0001"
    baseline_score: float = 0.25
    baseline_model_version: str = "risk-v0"
    baseline_proof_nonce: str = "round4-baseline"

    @property
    def baseline(self) -> ModelScoreRow:
        return ModelScoreRow(
            entity_id=self.entity_id,
            score=self.baseline_score,
            model_version=self.baseline_model_version,
            proof_nonce=self.baseline_proof_nonce,
        )

    @property
    def sha256(self) -> str:
        values = (
            self.pipeline_id,
            self.source_table,
            self.synced_table,
            self.entity_id,
            repr(self.baseline_score),
            self.baseline_model_version,
            self.baseline_proof_nonce,
        )
        return hashlib.sha256("\0".join(values).encode()).hexdigest()


@dataclass(frozen=True)
class ModelScoreArm:
    arm_id: str
    armed_at: datetime
    contract_sha256: str
    source_version: int
    baseline: ModelScoreRow


@dataclass(frozen=True)
class ModelScoreProofResult:
    kind: ModelScoreProofKind
    update: ModelScoreUpdate
    source_version: int
    delta_commit_time: datetime
    sync_end_time: datetime
    managed_availability_ms: float
    application_read_elapsed_ms: float
    poll_attempts: int
    verified_row: ModelScoreRow
    lane_id: str = "lakebase"
    lane_name: str = "Lakebase"


@dataclass(frozen=True)
class ModelScoreRunResult:
    arm_id: str
    contract_sha256: str
    initial: ModelScoreProofResult
    redo: ModelScoreProofResult | None = None


class ModelScoreAdapter(Protocol):
    async def inspect_sync(self) -> ManagedSyncStatus: ...

    async def read_source(self, entity_id: str) -> ModelScoreRow | None: ...

    async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit: ...

    async def read_application_fresh(self, entity_id: str) -> ModelScoreRow | None: ...


ProgressCallback = Callable[[ModelScoreProgress], Awaitable[None]]


#: How recently a change must have gone end to end on the running pipeline update
#: for Prepare to reuse that as its warm proof.
#:
#: **Measured, not assumed.** The worry behind proving warm before every bell was
#: that a continuous pipeline left idle might park the compute its next change
#: needs. Measured live on 2026-09-28, the first change after an idle spell on
#: the same running update synced (commit to sync end, the pipeline's own
#: timestamps) in 5.96 s right after a start, 5.46 s after 5 minutes idle,
#: 4.20 s after 10 and 3.14 s after 15: no slower than a warm one. Fifteen
#: minutes is the longest idle measured, so it is the longest reuse allowed; a
#: restarted update, with a new ID, is always proven again.
WARM_PROOF_REUSE_SECONDS = 900.0

#: How recently the Delta source storage must have been checked for Prepare to
#: skip the check. The storage preflight finds access denials and a missing Delta
#: path, neither of which comes and goes in minutes, and any failed preparation
#: forgets the check so the next one makes it again.
STORAGE_CHECK_REUSE_SECONDS = 600.0


class PipelineActivation(Protocol):
    """Whatever can turn this round's pipeline on before a bout and off after it.

    A protocol rather than a concrete collaborator because the engine must know
    as little about powering a pipeline as possible: it can ask for a running
    pipeline and it can say a bout is finished with, and it cannot express any
    other request. The live implementation is
    ``model_score_live.Round4PipelineActivation``; every other construction of
    this engine passes nothing and behaves exactly as it did before.
    """

    async def ensure_running(self, notify: Callable[[str], Awaitable[None]]) -> None: ...

    def release_when_idle(self) -> None: ...

    async def release_now(self) -> None: ...


class ModelScoreEngine:
    """Neutral, one-lane proof of Delta-to-application Managed Sync availability."""

    def __init__(
        self,
        adapter: ModelScoreAdapter,
        *,
        contract: ModelScoreContract,
        max_poll_attempts: int = 20,
        poll_interval_seconds: float = 0.25,
        inspect_timeout_seconds: float = 5.0,
        commit_timeout_seconds: float = 60.0,
        read_timeout_seconds: float = 30.0,
        progress_timeout_seconds: float = 2.0,
        max_status_age_seconds: float = 30.0,
        arm_catch_up_seconds: float = 0.0,
        arm_catch_up_poll_seconds: float = 2.0,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        activation: PipelineActivation | None = None,
    ) -> None:
        if max_poll_attempts < 1:
            raise ValueError("max_poll_attempts must be at least one")
        if arm_catch_up_seconds < 0 or arm_catch_up_poll_seconds < 0:
            raise ValueError("the arm catch-up bound and its poll cannot be negative")
        if poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds cannot be negative")
        if inspect_timeout_seconds <= 0:
            raise ValueError("inspect_timeout_seconds must be positive")
        if commit_timeout_seconds <= 0:
            raise ValueError("commit_timeout_seconds must be positive")
        if read_timeout_seconds <= 0:
            raise ValueError("read_timeout_seconds must be positive")
        if progress_timeout_seconds <= 0:
            raise ValueError("progress_timeout_seconds must be positive")
        if max_status_age_seconds < 0:
            raise ValueError("max_status_age_seconds cannot be negative")
        self.adapter = adapter
        self.contract = contract
        self.max_poll_attempts = max_poll_attempts
        self.poll_interval_seconds = poll_interval_seconds
        self.inspect_timeout_seconds = inspect_timeout_seconds
        self.commit_timeout_seconds = commit_timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds
        self.progress_timeout_seconds = progress_timeout_seconds
        self.max_status_age_seconds = max_status_age_seconds
        self.arm_catch_up_seconds = arm_catch_up_seconds
        self.arm_catch_up_poll_seconds = arm_catch_up_poll_seconds
        self._clock_ns = clock_ns
        self._now = now
        self._sleep = sleep
        self._activation = activation
        self._used_initial_arms: set[str] = set()
        self._used_redo_arms: set[str] = set()
        self._used_nonces: set[str] = set()
        self._issued_results: dict[str, ModelScoreRunResult] = {}
        self._owned_update_rows: dict[str, ModelScoreRow] = {}
        self._pending_adapter_tasks: set[asyncio.Task[object]] = set()
        self._settlement_task: asyncio.Task[None] | None = None

    @property
    def activation(self) -> PipelineActivation | None:
        """The pipeline activation this engine powers its sync through, if any."""

        return self._activation

    async def settle_and_restore_baseline(self) -> None:
        """Settle issued I/O, then restore and verify the exact owned baseline.

        Adapter calls are deliberately shielded from cancellation because live
        Statement Execution uses worker threads.  Cancelling the asyncio waiter
        cannot prove that an already-issued MERGE did not commit.
        """

        settlement = self._settlement_task
        if settlement is None or settlement.done():
            settlement = asyncio.create_task(self._settle_and_restore_baseline())
            self._settlement_task = settlement
        try:
            await asyncio.shield(settlement)
        finally:
            if self._settlement_task is settlement and settlement.done():
                self._settlement_task = None

    async def _settle_and_restore_baseline(self) -> None:
        await self._settle_adapter_tasks()

        status = await self._inspect_sync()
        self._validate_contract_status(status, arming=False)
        source = await self._read_source(
            self.contract.entity_id,
            "settlement source read",
        )
        baseline = self.contract.baseline
        restored = False

        if source != baseline:
            if source is None or self._owned_update_rows.get(source.proof_nonce) != source:
                raise ModelScoreVerificationError(
                    "Round 4 settlement refused to replace a source row not issued by this engine"
                )
            commit = await self._commit_source_update(
                ModelScoreUpdate(
                    entity_id=baseline.entity_id,
                    score=baseline.score,
                    model_version=baseline.model_version,
                    proof_nonce=baseline.proof_nonce,
                ),
                after_version=status.source_version,
            )
            if commit.version <= status.source_version:
                raise ModelScoreVerificationError(
                    "Baseline restoration did not advance the authoritative Delta source version"
                )
            restored_source = await self._read_source(
                baseline.entity_id,
                "restored baseline source read",
            )
            if restored_source != baseline:
                raise ModelScoreVerificationError(
                    "Baseline restoration did not produce the exact source row"
                )
            restored_status, _ = await self._wait_for_version(commit, None)
            if self._aware(
                restored_status.last_sync_delta_commit_time,
                "baseline status Delta commit timestamp",
            ) != self._aware(commit.committed_at, "baseline CDF commit timestamp"):
                raise ModelScoreVerificationError(
                    "Baseline sync timestamp does not match the exact restoration commit"
                )
            restored = True
        else:
            await self._wait_for_settlement_version(status.source_version, status)

        final_source = await self._read_source(
            baseline.entity_id,
            "final settlement source read",
        )
        final_application = await self._read_application(
            baseline.entity_id,
            "final settlement fresh application Postgres read",
        )
        if final_source != baseline or final_application != baseline:
            raise ModelScoreVerificationError(
                "Round 4 settlement did not verify the exact baseline in source and application"
            )
        if restored:
            # The restore was itself a change carried end to end, so a Ring Again
            # soon after can reuse it as the warm proof.
            self._note_proven_sync()
        # Last, and only on the path that proved the baseline is back. A release
        # scheduled before this point, or on the failure path, would stop a
        # pipeline that settlement still needs -- and a retry of a settlement
        # that failed needs it just as much.
        await self._release_pipeline()

    async def _wait_for_settlement_version(
        self,
        source_version: int,
        status: ManagedSyncStatus,
    ) -> ManagedSyncStatus:
        for attempt in range(1, self.max_poll_attempts + 1):
            self._validate_contract_status(status, arming=False)
            if (
                status.source_version > source_version
                or status.last_processed_version > source_version
                or status.last_sync_delta_version > source_version
            ):
                raise ModelScoreStaleStatusError(
                    "Managed Sync overshot the source version being settled"
                )
            if (
                status.source_version == source_version
                and status.last_processed_version == source_version
                and status.last_sync_delta_version == source_version
            ):
                return status
            if attempt < self.max_poll_attempts:
                try:
                    await asyncio.wait_for(
                        self._sleep(self.poll_interval_seconds),
                        timeout=self.inspect_timeout_seconds,
                    )
                except TimeoutError as exc:
                    raise ModelScoreTimeoutError(
                        "Managed Sync settlement polling exceeded its wall-clock bound"
                    ) from exc
                status = await self._inspect_sync()
        raise ModelScoreTimeoutError(
            f"Managed Sync did not settle Delta version {source_version} "
            f"within {self.max_poll_attempts} polls"
        )

    async def _settle_adapter_tasks(self) -> None:
        while pending := tuple(self._pending_adapter_tasks):
            await asyncio.gather(
                *(asyncio.shield(task) for task in pending),
                return_exceptions=True,
            )

    async def arm(self, on_progress: ProgressCallback | None = None) -> ModelScoreArm:
        status = await self._prepare(on_progress, allow_reuse=True)
        arm = ModelScoreArm(
            arm_id=uuid4().hex,
            armed_at=self._aware(self._now(), "armed_at"),
            contract_sha256=self.contract.sha256,
            source_version=status.source_version,
            baseline=self.contract.baseline,
        )
        await self._emit(on_progress, ModelScorePhase.ARMED, "Managed Sync baseline verified")
        return arm

    async def prewarm(self) -> None:
        """Do an arm's whole preparation between bouts, so the next Prepare can reuse it.

        The same checks an arm makes, including the warm-up round trip, which is
        forced here rather than reused: the point is to leave a fresh proof and a
        fresh storage check behind for the presenter's Prepare. It writes only the
        sealed baseline, exactly as an arm's warm-up does, and returns nothing a
        bout could use. The manager runs it only while Round 4 is idle.
        """

        await self._prepare(None, allow_reuse=False)

    async def _prepare(
        self,
        on_progress: ProgressCallback | None,
        *,
        allow_reuse: bool,
    ) -> ManagedSyncStatus:
        try:
            return await self._prepare_once(on_progress, allow_reuse=allow_reuse)
        except asyncio.CancelledError:
            raise
        except BaseException:
            # A failed preparation proves nothing about the storage it read, so
            # the next one checks it again rather than trusting an old answer.
            self._forget_storage_check()
            raise

    async def _prepare_once(
        self,
        on_progress: ProgressCallback | None,
        *,
        allow_reuse: bool,
    ) -> ManagedSyncStatus:
        if not self._storage_check_is_fresh():
            await self._check_source_storage(on_progress)
        await self._ensure_pipeline_running(on_progress)
        await self._emit(on_progress, ModelScorePhase.PREFLIGHT, "Inspecting Managed Sync")
        status = await self._await_arm_status(on_progress)

        # Independent reads of two different systems, so neither waits on the other.
        source, application = await asyncio.gather(
            self._read_source(
                self.contract.entity_id,
                "baseline source read",
            ),
            self._read_application(
                self.contract.entity_id,
                "baseline fresh application Postgres read",
            ),
        )
        if source != self.contract.baseline or application != self.contract.baseline:
            if source != application or not self._is_owned_prior_proof(source):
                raise ModelScoreNotArmedError(
                    "The exact baseline is absent and the current row is not one matching "
                    "demo-owned Round 4 proof"
                )
        reuse_age = self._reusable_warm_proof_age(source, application) if allow_reuse else None
        if reuse_age is None:
            status = await self._prove_pipeline_warm(status, on_progress)
        else:
            await self._emit(
                on_progress,
                ModelScorePhase.PREFLIGHT,
                f"Managed Sync carried a change end to end {reuse_age:.0f}s ago on this "
                "same run; reusing that proof",
            )
        confirmed = await self._inspect_sync()
        self._validate_contract_status(confirmed, arming=True)
        if (
            confirmed.source_version != status.source_version
            or confirmed.last_processed_version != status.last_processed_version
            or confirmed.last_sync_delta_version != status.last_sync_delta_version
            or confirmed.last_sync_delta_commit_time
            != status.last_sync_delta_commit_time
            or confirmed.sync_end_time != status.sync_end_time
        ):
            raise ModelScoreNotArmedError(
                "Managed Sync advanced while the exact baseline was being verified"
            )
        self._validate_caught_up_baseline(confirmed)
        return status

    def _storage_check_is_fresh(self) -> bool:
        age_of = getattr(self._activation, "storage_check_age", None)
        if not callable(age_of):
            return False
        age = age_of()
        return age is not None and 0 <= age <= STORAGE_CHECK_REUSE_SECONDS

    def _forget_storage_check(self) -> None:
        forget = getattr(self._activation, "forget_storage_check", None)
        if callable(forget):
            forget()

    async def _check_source_storage(self, on_progress: ProgressCallback | None) -> None:
        await self._emit(
            on_progress,
            ModelScorePhase.PREFLIGHT,
            "Checking Delta source storage before pipeline start",
        )
        preflight = getattr(self.adapter, "preflight_source", None)
        if callable(preflight):
            async def report_preflight(detail: str) -> None:
                await self._emit(on_progress, ModelScorePhase.PREFLIGHT, detail)

            try:
                source = await self._await_adapter_operation(
                    preflight(self.contract.entity_id, on_progress=report_preflight),
                    wall_clock_seconds=max(self.read_timeout_seconds, 150.0),
                )
            except TimeoutError as exc:
                raise ModelScoreTimeoutError(
                    "Delta source storage preflight exceeded its wall-clock bound"
                ) from exc
            if source != self.contract.baseline and not self._is_owned_prior_proof(source):
                raise ModelScoreNotArmedError(
                    "The Delta source is readable, but its row is neither the exact "
                    "baseline nor a matching demo-owned Round 4 proof"
                )
            if bool(getattr(self.adapter, "source_repaired", False)):
                require_full_refresh = getattr(self.activation, "require_full_refresh", None)
                if not callable(require_full_refresh):
                    raise ModelScoreNotArmedError(
                        "The repaired Delta source has no sealed Managed Sync rebase path"
                    )
                require_full_refresh()
                await self._emit(
                    on_progress,
                    ModelScorePhase.PREFLIGHT,
                    "Rebasing the preserved Managed Sync pipeline onto the repaired source",
                )
            note = getattr(self._activation, "note_storage_check", None)
            if callable(note):
                note()

    async def _ensure_pipeline_running(self, on_progress: ProgressCallback | None) -> None:
        """Give the pipeline back before anything tries to inspect it.

        **Before** ``_inspect_sync``, not inside the healthy/not-healthy check
        that follows it, and that ordering is a measurement rather than a
        preference. A stopped pipeline's synced table omits
        ``continuous_update_status`` altogether, which
        ``_validate_database_synced_table`` requires as a non-empty mapping, so
        ``_inspect_sync`` raises on the way to discovering the pipeline is down.
        There is no state object to test ``!= RUNNING`` against at that point.

        Nothing here is timed. ``armed_expires_at`` is derived from an
        ``armed_at`` the manager captures *after* this method returns, and the
        bout's clock does not start until the bell, so a wait of a minute or two
        comes out of the operator's patience and out of nothing else. It is made
        visible for exactly that reason: this is the one place in an arm where a
        wait can be long, and a silent long block in front of an audience is
        indistinguishable from a hang.
        """

        if self._activation is None:
            return

        async def notify(status: str) -> None:
            await self._emit(on_progress, ModelScorePhase.PREFLIGHT, status)

        await self._activation.ensure_running(notify)

    async def _release_pipeline(self) -> None:
        """Tell the activation this bout is finished with the pipeline.

        Only ever reached after ``_settle_and_restore_baseline`` has returned,
        which is what makes this a *settled* release rather than a bout-end hook.
        D20's third blocker is that settlement needs the pipeline live and races
        anything that stops it; waiting for settlement to finish is how that race
        is not run at all.

        **Which of the two releases is owed turns on whether a redo is still
        available, and the engine can answer that from its own state rather
        than being told.** The activation's idle window exists for the redo and
        for nothing else: a verified bout leaves a ``RedoState.READY`` the
        presenter may take at any time, and a redo does not re-arm, so it would
        land straight on a stopped pipeline. An engine that issued a result is
        therefore in exactly the case that window was bought for.

        An engine that issued none is not. That is what a towel looks like from
        in here -- ``run`` was cancelled mid-flight, ``_finish_model_score``
        returns early on a non-null towel, and no redo snapshot is ever
        published -- and it is what a failed bout looks like too. Neither can be
        redone, so neither is owed twenty minutes of billing.

        A redo that has already been *taken* still counts as an issued result
        and still gets the window, which is conservative in the direction that
        costs $0.20 rather than the one that kills a round.
        """

        activation = self._activation
        if activation is None:
            return
        if self._issued_results:
            activation.release_when_idle()
            return
        await activation.release_now()

    def _reusable_warm_proof_age(
        self,
        source: ModelScoreRow | None,
        application: ModelScoreRow | None,
    ) -> float | None:
        """Seconds since a change last went end to end on the running update, if reusable.

        The warm-up commit below exists so a cold pipeline's start-up never lands
        inside the measurement. A change that went through this same pipeline
        update end to end within :data:`WARM_PROOF_REUSE_SECONDS` proves the same
        thing, so a quick Ring Again does not have to prove it a second time.
        Only the exact baseline in both source and application qualifies: a row
        left by an earlier proof still needs the commit that restores it.
        """

        if source != self.contract.baseline or application != self.contract.baseline:
            return None
        age_of = getattr(self._activation, "warm_proof_age", None)
        if not callable(age_of):
            return None
        age = age_of()
        if age is None or age < 0 or age > WARM_PROOF_REUSE_SECONDS:
            return None
        return float(age)

    def _note_proven_sync(self) -> None:
        """Tell the activation a change just went end to end, for a later Prepare."""

        note = getattr(self._activation, "note_proven_sync", None)
        if callable(note):
            note()

    async def _prove_pipeline_warm(
        self,
        status: ManagedSyncStatus,
        on_progress: ProgressCallback | None,
    ) -> ManagedSyncStatus:
        """Round-trip a throwaway Delta version so warmth is proven, not assumed.

        Round 4's headline number is ``sync_end - committed_at`` on the bout's
        own commit, so whatever compute the pipeline has to provision in order
        to process that commit lands between those two timestamps and is inside
        the measurement by construction. Reading a healthy status does not rule
        that out: a stopped continuous pipeline reports ``IDLE``, and even a
        genuinely idle one may have parked the compute that a cold commit would
        have to wait for.

        The only honest proof is to make the pipeline do the work once, before
        the bell, on a version nobody is timing. This runs unconditionally --
        the caller has already refused anything that is not ours to overwrite --
        because the common case after the success-path settlement was added is
        that the row is *already* the exact baseline, which is precisely the
        case where the old conditional repair skipped and left the bout's commit
        to pay for the cold start.

        The commit carries the exact sealed baseline, so it advances the Delta
        version -- which is the thing the pipeline must react to -- while
        leaving behind the same row it started from. It is therefore invisible
        to the readiness gate, indistinguishable from an untouched baseline to
        the next bout, and not one of ``OWNED_PROOF_SHAPES``, so it can never be
        mistaken for a bout's proof row.
        """

        await self._emit(
            on_progress,
            ModelScorePhase.PREFLIGHT,
            "Proving Managed Sync is warm before the bell",
        )
        baseline = self.contract.baseline
        commit = await self._commit_source_update(
            ModelScoreUpdate(
                entity_id=baseline.entity_id,
                score=baseline.score,
                model_version=baseline.model_version,
                proof_nonce=baseline.proof_nonce,
            ),
            after_version=status.source_version,
        )
        if commit.version <= status.source_version:
            raise ModelScoreNotArmedError(
                "Baseline repair did not advance the authoritative Delta source version"
            )
        repaired_source = await self._read_source(
            baseline.entity_id,
            "repaired baseline source read",
        )
        if repaired_source != baseline:
            raise ModelScoreNotArmedError(
                "Baseline repair did not produce the exact source row"
            )
        try:
            repaired_status, _ = await self._wait_for_version(
                commit,
                self._warm_up_progress(on_progress),
            )
        except ModelScoreTimeoutError as exc:
            # Refusing here is the whole point. A cold pipeline that resumes
            # slowly enough to blow the window would otherwise resume *inside*
            # it on the next try and bill its start-up to the headline number.
            # The operator finds out now, with nothing on screen.
            raise ModelScoreNotArmedError(
                "Managed Sync did not process the warm-up commit inside the arming "
                "window, so the pipeline cannot be proven warm and Round 4 would "
                "measure its start-up. Start the Managed Sync pipeline and arm again."
            ) from exc
        cdf_commit_time = self._aware(commit.committed_at, "baseline CDF commit timestamp")
        status_commit_time = self._aware(
            repaired_status.last_sync_delta_commit_time,
            "baseline status Delta commit timestamp",
        )
        if status_commit_time != cdf_commit_time:
            raise ModelScoreNotArmedError(
                "Baseline sync timestamp does not match the exact CDF repair commit"
            )
        repaired_application = await self._read_application(
            baseline.entity_id,
            "repaired baseline fresh application Postgres read",
        )
        if repaired_application != baseline:
            raise ModelScoreNotArmedError(
                "Baseline repair did not reach the exact fresh application row"
            )
        self._note_proven_sync()
        return repaired_status

    @staticmethod
    def _warm_up_progress(
        on_progress: ProgressCallback | None,
    ) -> ProgressCallback | None:
        """Relabel warm-up polling so it cannot read as the bout's own wait.

        ``_wait_for_version`` emits ``WAITING_SYNC`` with the Delta version in
        the text, which is exactly what the audience sees while the measured
        commit is in flight. The warm-up happens before the bell and must never
        borrow that language or that phase.
        """

        if on_progress is None:
            return None

        async def relabelled(progress: ModelScoreProgress) -> None:
            await on_progress(
                replace(
                    progress,
                    phase=ModelScorePhase.PREFLIGHT,
                    status="Proving Managed Sync is warm before the bell",
                    elapsed_ms=None,
                )
            )

        return relabelled

    async def _warm_application_endpoint(
        self,
        on_progress: ProgressCallback | None,
    ) -> None:
        """Pay any Lakebase endpoint wake before the measured clock starts.

        ``application_read_elapsed_ms`` runs from the Delta commit to the
        successful fresh application read, so whatever the endpoint has to do
        to accept a connection in between lands inside that number. Arming does
        leave the endpoint awake, but it cannot keep it awake until the bell:
        arming's last Postgres touch is the repaired baseline read, and after
        it come the arm-to-bell gap, the commit and the *whole* reverse-ETL
        wait with nothing touching Postgres at all. The suspend timer runs
        through all of it, so even a prompt operator can meet a suspended
        endpoint and have its wake billed to the end-to-end proof.

        That is also why this runs immediately before the commit rather than
        after the sync wait. The commit timestamp is where the clock starts, so
        a warm-up placed after it would pay exactly the same wake inside
        exactly the same window and change the published number by nothing.

        Reading the contracted primary key through a fresh application
        connection is the cheapest thing that reopens it. It writes nothing,
        advances no Delta version, and returns a row that is already either the
        sealed baseline or this bout's own prior proof, so it leaves no residue
        for settlement to find and cannot be mistaken for anything. The value
        is discarded on purpose: warming an endpoint only needs the connection,
        and verifying the exact row is the measured read's job.

        A failure here is deliberately not fatal. The warm-up proves nothing
        arming has not already proven, so refusing on it would end a live bout
        over an optimisation; and if the endpoint genuinely is unreachable, the
        measured read fails moments later with an error that names the real
        problem instead of blaming a preflight.
        """

        await self._emit(
            on_progress,
            ModelScorePhase.PREFLIGHT,
            "Proving the application endpoint is warm before the bell",
        )
        try:
            await self._read_application(
                self.contract.entity_id,
                "application endpoint warm-up read",
            )
        except Exception:
            LOGGER.warning(
                "Round 4 application endpoint warm-up failed; the bout continues and "
                "may pay an endpoint wake inside application_read_elapsed_ms",
                exc_info=True,
            )

    @staticmethod
    def _is_owned_prior_proof(row: ModelScoreRow | None) -> bool:
        return is_owned_prior_proof(row)

    async def run(
        self,
        arm: ModelScoreArm,
        update: ModelScoreUpdate,
        on_progress: ProgressCallback | None = None,
    ) -> ModelScoreRunResult:
        self._validate_arm(arm)
        if arm.arm_id in self._used_initial_arms:
            raise ModelScoreVerificationError("This armed initial update has already been used")
        self._validate_initial_update(update)
        self._reserve_nonce(update.proof_nonce)
        self._owned_update_rows[update.proof_nonce] = update.row
        self._used_initial_arms.add(arm.arm_id)
        try:
            await self._warm_application_endpoint(on_progress)
            await self._emit(
                on_progress,
                ModelScorePhase.COMMITTING_SOURCE,
                "Committing the run-owned model score update",
            )
        except asyncio.CancelledError:
            self._used_initial_arms.discard(arm.arm_id)
            self._used_nonces.discard(update.proof_nonce)
            self._owned_update_rows.pop(update.proof_nonce, None)
            raise
        proof = await self._prove_update(
            ModelScoreProofKind.INITIAL,
            update,
            after_version=arm.source_version,
            on_progress=on_progress,
        )
        self._note_proven_sync()
        result = ModelScoreRunResult(
            arm_id=arm.arm_id,
            contract_sha256=self.contract.sha256,
            initial=proof,
        )
        self._issued_results[arm.arm_id] = result
        return result

    async def redo(
        self,
        arm: ModelScoreArm,
        result: ModelScoreRunResult,
        update: ModelScoreUpdate,
        on_progress: ProgressCallback | None = None,
    ) -> ModelScoreRunResult:
        self._validate_arm(arm)
        if result.arm_id != arm.arm_id or result.contract_sha256 != self.contract.sha256:
            raise ModelScoreDuplicateRedoError("The redo does not belong to this armed proof")
        if self._issued_results.get(arm.arm_id) is not result:
            raise ModelScoreDuplicateRedoError("The redo requires the engine-issued initial result")
        if result.redo is not None or arm.arm_id in self._used_redo_arms:
            raise ModelScoreDuplicateRedoError("This proof has already been redone")
        if update.entity_id != result.initial.update.entity_id:
            raise ModelScoreDuplicateRedoError("Redo must update the same primary key")
        if update.score == result.initial.update.score:
            raise ModelScoreDuplicateRedoError("Redo score must be different")
        if update.model_version == result.initial.update.model_version:
            raise ModelScoreDuplicateRedoError("Redo model version must be different")
        if update.proof_nonce == result.initial.update.proof_nonce:
            raise ModelScoreDuplicateRedoError("Redo proof nonce must be different")
        self._reserve_nonce(update.proof_nonce)
        self._owned_update_rows[update.proof_nonce] = update.row
        self._used_redo_arms.add(arm.arm_id)
        try:
            await self._warm_application_endpoint(on_progress)
            await self._emit(
                on_progress,
                ModelScorePhase.COMMITTING_SOURCE,
                "Committing the run-owned model score update",
            )
        except asyncio.CancelledError:
            self._used_redo_arms.discard(arm.arm_id)
            self._used_nonces.discard(update.proof_nonce)
            self._owned_update_rows.pop(update.proof_nonce, None)
            raise
        proof = await self._prove_update(
            ModelScoreProofKind.REDO,
            update,
            after_version=result.initial.source_version,
            on_progress=on_progress,
        )
        self._note_proven_sync()
        redone = replace(result, redo=proof)
        self._issued_results[arm.arm_id] = redone
        return redone

    async def _prove_update(
        self,
        kind: ModelScoreProofKind,
        update: ModelScoreUpdate,
        *,
        after_version: int,
        on_progress: ProgressCallback | None,
    ) -> ModelScoreProofResult:
        commit = await self._commit_source_update(update, after_version=after_version)
        committed_at = self._aware(
            commit.committed_at,
            "CDF delta commit timestamp",
        )
        now_utc = self._aware(self._now(), "current time")
        if committed_at > now_utc + timedelta(seconds=2):
            raise ModelScoreStaleStatusError(
                "CDF Delta commit timestamp exceeds the host clock tolerance"
            )
        if commit.version <= after_version:
            raise ModelScoreStaleStatusError(
                "Delta commit version did not advance beyond the prior source version"
            )
        # Checked alongside the sync wait rather than before it: the check still
        # has to pass for the proof to stand, but the room no longer watches it.
        # The clock is the commit and sync timestamps, so this moves no number.
        verification = asyncio.ensure_future(
            self._read_source(update.entity_id, "committed source verification read")
        )
        try:
            status, attempts = await self._wait_for_version(commit, on_progress)
        except BaseException:
            verification.cancel()
            raise
        source = await verification
        if source != update.row:
            raise ModelScoreVerificationError("The source row does not match the committed update")
        assert status.sync_end_time is not None
        status_committed_at = self._aware(
            status.last_sync_delta_commit_time,
            "status delta commit timestamp",
        )
        if status_committed_at != committed_at:
            raise ModelScoreStaleStatusError(
                "Managed Sync status Delta commit timestamp does not match the exact CDF commit"
            )
        sync_end = self._aware(status.sync_end_time, "sync end timestamp")
        if sync_end < committed_at:
            raise ModelScoreStaleStatusError("Sync end timestamp predates the Delta commit")
        availability_ms = (sync_end - committed_at).total_seconds() * 1000

        await self._emit(
            on_progress,
            ModelScorePhase.READING_APPLICATION,
            "Reading the score through a fresh application Postgres connection",
            elapsed_ms=self._elapsed_ms_since(committed_at),
        )
        application = await self._read_application(
            update.entity_id,
            "final fresh application Postgres read",
        )
        if application != update.row:
            raise ModelScoreVerificationError(
                "Fresh application read did not match the exact PK, score, model version, and nonce"
            )
        read_completed_at = self._aware(
            self._now(),
            "successful fresh application read timestamp",
        )
        if read_completed_at < committed_at:
            raise ModelScoreStaleStatusError(
                "Successful fresh application read predates the authoritative Delta commit"
            )
        result = ModelScoreProofResult(
            kind=kind,
            update=update,
            source_version=commit.version,
            delta_commit_time=committed_at,
            sync_end_time=sync_end,
            managed_availability_ms=availability_ms,
            application_read_elapsed_ms=(
                read_completed_at - committed_at
            ).total_seconds()
            * 1000,
            poll_attempts=attempts,
            verified_row=application,
        )
        await self._emit(
            on_progress,
            ModelScorePhase.VERIFIED,
            "Exact application score verified",
            elapsed_ms=result.application_read_elapsed_ms,
        )
        return result

    async def _wait_for_version(
        self,
        commit: DeltaCommit,
        on_progress: ProgressCallback | None,
    ) -> tuple[ManagedSyncStatus, int]:
        for attempt in range(1, self.max_poll_attempts + 1):
            await self._emit(
                on_progress,
                ModelScorePhase.WAITING_SYNC,
                f"Waiting for Managed Sync to process Delta version {commit.version}",
                attempt,
                elapsed_ms=self._elapsed_ms_since(
                    self._aware(commit.committed_at, "CDF delta commit timestamp")
                ),
            )
            status = await self._inspect_sync()
            self._validate_contract_status(status, arming=False)
            if status.source_version < commit.version:
                raise ModelScoreStaleStatusError(
                    "Managed Sync status source version predates the committed update"
                )
            if (
                status.source_version > commit.version
                or status.last_sync_delta_version > commit.version
                or status.last_processed_version > commit.version
            ):
                raise ModelScoreStaleStatusError(
                    "Managed Sync overshot the requested Delta commit version"
                )
            if (
                status.last_sync_delta_version == commit.version
                and status.last_processed_version == commit.version
                and status.last_processed_version >= status.source_version
            ):
                if status.sync_end_time is None:
                    raise ModelScoreStaleStatusError(
                        "Caught-up Managed Sync status omitted its sync end timestamp"
                    )
                return status, attempt
            if attempt < self.max_poll_attempts:
                try:
                    await asyncio.wait_for(
                        self._sleep(self.poll_interval_seconds),
                        timeout=self.inspect_timeout_seconds,
                    )
                except TimeoutError as exc:
                    raise ModelScoreTimeoutError(
                        "Managed Sync polling interval exceeded its wall-clock bound"
                    ) from exc
        raise ModelScoreTimeoutError(
            f"Managed Sync did not process Delta version {commit.version} "
            f"within {self.max_poll_attempts} polls"
        )

    async def _inspect_sync(self) -> ManagedSyncStatus:
        try:
            return await self._await_adapter_operation(
                self.adapter.inspect_sync(),
                wall_clock_seconds=self.inspect_timeout_seconds,
            )
        except TimeoutError as exc:
            raise ModelScoreTimeoutError(
                "Managed Sync inspection exceeded its wall-clock bound"
            ) from exc

    async def _await_arm_status(
        self, on_progress: ProgressCallback | None
    ) -> ManagedSyncStatus:
        """The first status a just-started pipeline can be armed on, within a bound.

        An arm right after a settle meets a pipeline that was just stopped and is
        starting again. On 2026-09-27 two such arms were refused within 36
        seconds: one while the restarted pipeline had not yet published its
        continuous status, one while it was still processing the last Delta
        version. Each would have armed seconds later. So those two shapes are
        waited out, visibly, for up to `arm_catch_up_seconds`. That wait comes out
        of the operator's patience before the bell and never out of the
        measurement (see `_ensure_pipeline_running`).

        The validators are unchanged: only a status that passes them is ever
        returned. Anything other than those two shapes -- identity, CDF,
        continuity, staleness, health -- still refuses at once. With a zero
        bound this is exactly the old single inspection.
        """

        if self.arm_catch_up_seconds <= 0:
            # Not even a clock read: the proof clock is the bout's own, and a test
            # of it asserts nothing touches it before the bell.
            status = await self._inspect_sync_for_arm()
            self._validate_contract_status(status, arming=True)
            self._validate_caught_up_baseline(status)
            return status
        deadline_ns = self._clock_ns() + int(self.arm_catch_up_seconds * 1_000_000_000)
        announced = False
        while True:
            status: ManagedSyncStatus | None
            try:
                status = await self._inspect_sync_for_arm()
            except ModelScoreError:
                if self._clock_ns() >= deadline_ns:
                    raise
                status = None
            if status is not None:
                self._validate_contract_status(status, arming=True)
                if self._is_caught_up(status) or self._clock_ns() >= deadline_ns:
                    self._validate_caught_up_baseline(status)
                    return status
            if not announced:
                announced = True
                await self._emit(
                    on_progress,
                    ModelScorePhase.PREFLIGHT,
                    "Waiting for Managed Sync to catch up after its restart",
                )
            await self._sleep(self.arm_catch_up_poll_seconds)

    @staticmethod
    def _is_caught_up(status: ManagedSyncStatus) -> bool:
        return status.sync_end_time is not None and (
            status.source_version
            == status.last_processed_version
            == status.last_sync_delta_version
        )

    async def _inspect_sync_for_arm(self) -> ManagedSyncStatus:
        """Fence transient provider projections before validating the arm.

        Managed Sync's pipeline and synced-table endpoints are eventually
        consistent, including while an otherwise healthy continuous pipeline
        is already running. Retrying does not make any state acceptable: the
        returned projection still passes the unchanged strict validators, and
        persistent configuration or identity drift still fails closed after
        the existing finite poll budget.
        """

        last_error: ModelScoreError | None = None
        for attempt in range(1, self.max_poll_attempts + 1):
            try:
                return await self._inspect_sync()
            except ModelScoreError as exc:
                last_error = exc
                if attempt == self.max_poll_attempts:
                    raise
                await self._sleep(self.poll_interval_seconds)
        assert last_error is not None
        raise last_error

    async def _commit_source_update(
        self,
        update: ModelScoreUpdate,
        *,
        after_version: int | None = None,
    ) -> DeltaCommit:
        # An adapter that can take the head it would otherwise read skips one
        # statement on the path the room is watching. The caller's version is the
        # head it just inspected, or a lower bound for a commit whose nonce is
        # unique, and a conflict makes the adapter read the head again anyway.
        if after_version is not None and getattr(self.adapter, "accepts_known_head", False):
            operation = self.adapter.commit_source_update(update, after_version=after_version)
        else:
            operation = self.adapter.commit_source_update(update)
        try:
            return await self._await_adapter_operation(
                operation,
                wall_clock_seconds=self.commit_timeout_seconds,
            )
        except TimeoutError as exc:
            raise ModelScoreTimeoutError(
                "Source Delta commit exceeded its wall-clock bound"
            ) from exc

    async def _read_source(
        self,
        entity_id: str,
        operation: str,
    ) -> ModelScoreRow | None:
        try:
            return await self._await_adapter_operation(
                self.adapter.read_source(entity_id),
                wall_clock_seconds=self.read_timeout_seconds,
            )
        except TimeoutError as exc:
            raise ModelScoreTimeoutError(
                f"{operation} exceeded its wall-clock bound"
            ) from exc

    async def _read_application(
        self,
        entity_id: str,
        operation: str,
    ) -> ModelScoreRow | None:
        try:
            return await self._await_adapter_operation(
                self.adapter.read_application_fresh(entity_id),
                wall_clock_seconds=self.read_timeout_seconds,
            )
        except TimeoutError as exc:
            raise ModelScoreTimeoutError(
                f"{operation} exceeded its wall-clock bound"
            ) from exc

    async def _await_adapter_operation(
        self,
        operation: Awaitable[object],
        *,
        wall_clock_seconds: float,
    ):
        task = asyncio.ensure_future(operation)
        self._pending_adapter_tasks.add(task)

        def settled(completed: asyncio.Future[object]) -> None:
            self._pending_adapter_tasks.discard(task)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(settled)
        return await asyncio.wait_for(
            asyncio.shield(task),
            timeout=wall_clock_seconds,
        )

    def _validate_contract_status(
        self,
        status: ManagedSyncStatus,
        *,
        arming: bool,
    ) -> None:
        error_type = ModelScoreNotArmedError if arming else ModelScoreStaleStatusError
        if (
            status.pipeline_id != self.contract.pipeline_id
            or status.source_table != self.contract.source_table
            or status.synced_table != self.contract.synced_table
        ):
            raise error_type("Managed Sync identity does not match the contract")
        if not status.cdf_enabled:
            raise error_type("The source Delta table is not CDF-enabled")
        if not status.continuous:
            raise error_type("The synced table is not in continuous mode")
        observed_at = self._aware(status.observed_at, "Managed Sync observed_at")
        commit_time = self._aware(
            status.last_sync_delta_commit_time,
            "status delta commit timestamp",
        )
        if status.sync_end_time is None:
            raise error_type("Managed Sync status omitted its sync end timestamp")
        sync_end = self._aware(status.sync_end_time, "status sync end timestamp")
        now_utc = self._aware(self._now(), "current time")
        if not commit_time <= sync_end <= observed_at:
            raise ModelScoreStaleStatusError(
                "Managed Sync timestamps must satisfy commit <= sync end <= observed"
            )
        if observed_at > now_utc + timedelta(seconds=2):
            raise ModelScoreStaleStatusError(
                "Managed Sync observed_at exceeds the host clock tolerance"
            )
        age = (now_utc - observed_at).total_seconds()
        if age > self.max_status_age_seconds:
            raise ModelScoreStaleStatusError("Managed Sync status is stale")
        if (
            status.source_version < 0
            or status.last_processed_version < 0
            or status.last_sync_delta_version < 0
        ):
            raise ModelScoreStaleStatusError("Managed Sync status versions are invalid")
        if status.state != ManagedSyncState.RUNNING:
            detail = status.failure or status.state.value
            raise ModelScorePipelineError(f"Managed Sync pipeline is not healthy: {detail}")

    @staticmethod
    def _validate_caught_up_baseline(status: ManagedSyncStatus) -> None:
        if status.sync_end_time is None:
            raise ModelScoreNotArmedError(
                "Managed Sync baseline status omitted its sync end timestamp"
            )
        if not (
            status.source_version
            == status.last_processed_version
            == status.last_sync_delta_version
        ):
            raise ModelScoreNotArmedError(
                "Managed Sync baseline is not fully caught up to one exact Delta version"
            )

    def _validate_arm(self, arm: ModelScoreArm) -> None:
        if arm.contract_sha256 != self.contract.sha256 or arm.baseline != self.contract.baseline:
            raise ModelScoreNotArmedError("The Model Score contract changed after arming")

    def _validate_initial_update(self, update: ModelScoreUpdate) -> None:
        if update.entity_id != self.contract.entity_id:
            raise ModelScoreVerificationError("Initial update must use the contracted primary key")
        baseline = self.contract.baseline
        if update.row == baseline:
            raise ModelScoreVerificationError("Initial update must differ from the baseline")
        if update.proof_nonce == baseline.proof_nonce:
            raise ModelScoreVerificationError("Initial proof nonce must be run-owned and distinct")

    def _reserve_nonce(self, proof_nonce: str) -> None:
        if proof_nonce in self._used_nonces:
            raise ModelScoreDuplicateRedoError("Proof nonce has already been used")
        self._used_nonces.add(proof_nonce)

    @staticmethod
    def _aware(value: datetime | None, label: str) -> datetime:
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ModelScoreStaleStatusError(f"{label} must be timezone-aware")
        return value.astimezone(UTC)

    def _elapsed_ms_since(self, started_at: datetime) -> float:
        current = self._aware(self._now(), "progress elapsed timestamp")
        return max(0.0, (current - started_at).total_seconds() * 1000)

    async def _emit(
        self,
        callback: ProgressCallback | None,
        phase: ModelScorePhase,
        status: str,
        attempt: int | None = None,
        *,
        elapsed_ms: float | None = None,
    ) -> None:
        if callback is None:
            return
        try:
            await asyncio.wait_for(
                callback(
                    ModelScoreProgress(
                        phase=phase,
                        status=status,
                        occurred_at=self._aware(self._now(), "progress timestamp"),
                        attempt=attempt,
                        elapsed_ms=elapsed_ms,
                    )
                ),
                timeout=self.progress_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return
