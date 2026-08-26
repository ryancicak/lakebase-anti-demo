from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from server.model_score import (
    OWNED_PROOF_SHAPES,
    DeltaCommit,
    ManagedSyncState,
    ManagedSyncStatus,
    ModelScoreAdapter,
    ModelScoreArm,
    ModelScoreContract,
    ModelScoreDuplicateRedoError,
    ModelScoreEngine,
    ModelScoreNotArmedError,
    ModelScorePhase,
    ModelScorePipelineError,
    ModelScoreProgress,
    ModelScoreProofKind,
    ModelScoreRow,
    ModelScoreStaleStatusError,
    ModelScoreTimeoutError,
    ModelScoreUpdate,
    ModelScoreVerificationError,
    is_owned_prior_proof,
)
from server.models import (
    ComparisonKind,
    ComparisonSnapshot,
    LaneSnapshot,
    MetricDirection,
    MetricRole,
    MetricSpec,
    MetricUnit,
    MetricValue,
    RedoSnapshot,
    RedoState,
)

NOW = datetime(2026, 8, 18, 12, 0, 10, tzinfo=UTC)


def model_score_contract() -> ModelScoreContract:
    return ModelScoreContract(
        pipeline_id="round4-model-score-sync",
        source_table="main.anti_demo.model_scores",
        synced_table="public.model_scores",
    )


def healthy_status(
    source_version: int,
    processed_version: int,
    *,
    sync_end_time: datetime | None = NOW - timedelta(milliseconds=1500),
    last_sync_delta_version: int | None = None,
    last_sync_delta_commit_time: datetime | None = None,
    observed_at: datetime = NOW - timedelta(seconds=1),
) -> ManagedSyncStatus:
    return ManagedSyncStatus(
        pipeline_id="round4-model-score-sync",
        source_table="main.anti_demo.model_scores",
        synced_table="public.model_scores",
        state=ManagedSyncState.RUNNING,
        cdf_enabled=True,
        continuous=True,
        source_version=source_version,
        last_processed_version=processed_version,
        last_sync_delta_version=(
            processed_version
            if last_sync_delta_version is None
            else last_sync_delta_version
        ),
        last_sync_delta_commit_time=(
            NOW - timedelta(seconds=2)
            if last_sync_delta_commit_time is None
            else last_sync_delta_commit_time
        ),
        observed_at=observed_at,
        sync_end_time=sync_end_time,
    )


WARM_UP_COMMIT_TIME = NOW - timedelta(seconds=20)


def warm_up_commit(version: int) -> DeltaCommit:
    """The throwaway baseline commit arm() makes to prove the pipeline is warm."""

    return DeltaCommit(version=version, committed_at=WARM_UP_COMMIT_TIME)


def warm_up_status(version: int) -> ManagedSyncStatus:
    """What the sync reports once it has applied the arm-time warm-up commit."""

    return healthy_status(
        version,
        version,
        last_sync_delta_commit_time=WARM_UP_COMMIT_TIME,
        sync_end_time=WARM_UP_COMMIT_TIME + timedelta(milliseconds=500),
    )


def baseline_update(contract: ModelScoreContract) -> ModelScoreUpdate:
    return ModelScoreUpdate(
        contract.entity_id,
        contract.baseline_score,
        contract.baseline_model_version,
        contract.baseline_proof_nonce,
    )


class FakeModelScoreAdapter(ModelScoreAdapter):
    def __init__(
        self,
        contract: ModelScoreContract,
        *,
        statuses: list[ManagedSyncStatus],
        commits: list[DeltaCommit],
    ) -> None:
        self.statuses = deque(statuses)
        self.commits = deque(commits)
        self.baseline = contract.baseline
        self.source = contract.baseline
        self.application = contract.baseline
        self.application_override: ModelScoreRow | None = None
        self.committed_updates: list[ModelScoreUpdate] = []
        self.fresh_reads = 0
        self.inspection_calls = 0
        self.commit_calls = 0
        self.source_read_calls = 0
        self.application_read_calls = 0
        self.hang_inspection_calls: set[int] = set()
        self.hang_commit_calls: set[int] = set()
        self.hang_source_read_calls: set[int] = set()
        self.hang_application_read_calls: set[int] = set()
        self.commit_observer = None

    async def inspect_sync(self) -> ManagedSyncStatus:
        self.inspection_calls += 1
        if self.inspection_calls in self.hang_inspection_calls:
            await asyncio.Event().wait()
        status = self.statuses.popleft()
        if (
            self.committed_updates
            and status.last_processed_version >= status.source_version
        ):
            self.application = self.source
        return status

    async def read_source(self, entity_id: str) -> ModelScoreRow | None:
        self.source_read_calls += 1
        if self.source_read_calls in self.hang_source_read_calls:
            await asyncio.Event().wait()
        return self.source if self.source.entity_id == entity_id else None

    async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit:
        self.commit_calls += 1
        if self.commit_calls in self.hang_commit_calls:
            await asyncio.Event().wait()
        if self.commit_observer is not None:
            self.commit_observer()
        self.committed_updates.append(update)
        self.source = update.row
        return self.commits.popleft()

    async def read_application_fresh(self, entity_id: str) -> ModelScoreRow | None:
        self.application_read_calls += 1
        if self.application_read_calls in self.hang_application_read_calls:
            await asyncio.Event().wait()
        self.fresh_reads += 1
        row = self.application_override or self.application
        return row if row.entity_id == entity_id else None

    @property
    def proof_commits(self) -> list[ModelScoreUpdate]:
        """Commits that changed the row away from the sealed baseline.

        Arming now round-trips a throwaway baseline-content commit to prove the
        pipeline is warm, and settlement writes the baseline back. Neither is a
        proof row, so tests asking "what did the bout write?" filter them out
        rather than counting raw commits.
        """

        return [update for update in self.committed_updates if update.row != self.baseline]


def ticking_clock(step_ns: int = 3_000_000):
    value = 0

    def clock() -> int:
        nonlocal value
        value += step_ns
        return value

    return clock


def initial_update(contract: ModelScoreContract) -> ModelScoreUpdate:
    return ModelScoreUpdate(
        entity_id=contract.entity_id,
        score=0.81,
        model_version="risk-v1",
        proof_nonce="round4-v1-001",
    )


async def armed_engine(
    *,
    poll_statuses: list[ManagedSyncStatus],
    commits: list[DeltaCommit],
    max_poll_attempts: int = 20,
    inspect_timeout_seconds: float = 5.0,
    commit_timeout_seconds: float = 5.0,
    read_timeout_seconds: float = 5.0,
    progress_timeout_seconds: float = 2.0,
    clock_ns=None,
) -> tuple[ModelScoreEngine, FakeModelScoreAdapter, object, ModelScoreContract]:
    contract = model_score_contract()
    # Arming spends a Delta version on its warm-up round trip, so the sync
    # starts one version back and the warm-up lands it on 10 -- the version
    # every caller below still expects to arm against.
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[
            healthy_status(9, 9),
            warm_up_status(10),
            warm_up_status(10),
            *poll_statuses,
        ],
        commits=[warm_up_commit(10), *commits],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        max_poll_attempts=max_poll_attempts,
        poll_interval_seconds=0,
        inspect_timeout_seconds=inspect_timeout_seconds,
        commit_timeout_seconds=commit_timeout_seconds,
        read_timeout_seconds=read_timeout_seconds,
        progress_timeout_seconds=progress_timeout_seconds,
        now=lambda: NOW,
        clock_ns=clock_ns or ticking_clock(),
    )
    arm = await engine.arm()
    return engine, adapter, arm, contract


async def two_armed_engine(
    *,
    poll_statuses: list[ManagedSyncStatus],
    commits: list[DeltaCommit],
) -> tuple[
    ModelScoreEngine,
    FakeModelScoreAdapter,
    object,
    object,
    ModelScoreContract,
]:
    contract = model_score_contract()
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[
            healthy_status(8, 8),
            warm_up_status(9),
            warm_up_status(9),
            warm_up_status(9),
            warm_up_status(10),
            warm_up_status(10),
            *poll_statuses,
        ],
        commits=[warm_up_commit(9), warm_up_commit(10), *commits],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
        clock_ns=ticking_clock(),
    )
    first_arm = await engine.arm()
    second_arm = await engine.arm()
    return engine, adapter, first_arm, second_arm, contract


async def test_initial_run_verifies_exact_app_row_and_timestamp_arithmetic() -> None:
    commit_time = NOW - timedelta(seconds=8)
    sync_end = commit_time + timedelta(seconds=2, milliseconds=125)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(11, 10),
            healthy_status(
                11,
                11,
                sync_end_time=sync_end,
                last_sync_delta_commit_time=commit_time,
            ),
        ],
        commits=[
            DeltaCommit(version=11, committed_at=commit_time)
        ],
    )

    result = await engine.run(arm, initial_update(contract))

    assert result.redo is None
    assert result.initial.kind == ModelScoreProofKind.INITIAL
    assert result.initial.source_version == 11
    assert result.initial.delta_commit_time == commit_time
    assert result.initial.sync_end_time == sync_end
    assert result.initial.managed_availability_ms == 2125.0
    assert result.initial.application_read_elapsed_ms == 8000.0
    assert result.initial.poll_attempts == 2
    assert result.initial.update.model_version == "risk-v1"
    assert result.initial.update.proof_nonce == "round4-v1-001"
    assert result.initial.verified_row == initial_update(contract).row
    assert adapter.proof_commits == [initial_update(contract)]
    assert adapter.committed_updates == [baseline_update(contract), initial_update(contract)]
    # arm baseline, arm warm-up round trip, the bell's endpoint warm-up, then
    # one fresh proof read
    assert adapter.fresh_reads == 4


async def test_duplicate_initial_nonce_does_not_consume_arm_before_commit() -> None:
    first_commit = NOW - timedelta(seconds=8)
    second_commit = NOW - timedelta(seconds=4)
    engine, adapter, first_arm, retry_arm, contract = await two_armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=first_commit + timedelta(seconds=1),
                last_sync_delta_commit_time=first_commit,
            ),
            healthy_status(
                12,
                12,
                sync_end_time=second_commit + timedelta(seconds=1),
                last_sync_delta_commit_time=second_commit,
            ),
        ],
        commits=[
            DeltaCommit(version=11, committed_at=first_commit),
            DeltaCommit(version=12, committed_at=second_commit),
        ],
    )
    used_nonce = "round4-shared-initial-nonce"
    await engine.run(
        first_arm,
        ModelScoreUpdate(contract.entity_id, 0.81, "risk-v1", used_nonce),
    )

    with pytest.raises(ModelScoreDuplicateRedoError, match="nonce"):
        await engine.run(
            retry_arm,
            ModelScoreUpdate(contract.entity_id, 0.82, "risk-v1", used_nonce),
        )
    assert len(adapter.proof_commits) == 1

    result = await engine.run(
        retry_arm,
        ModelScoreUpdate(
            contract.entity_id,
            0.82,
            "risk-v1",
            "round4-distinct-initial-nonce",
        ),
    )
    assert result.initial.source_version == 12
    assert len(adapter.proof_commits) == 2

    with pytest.raises(ModelScoreVerificationError, match="already been used"):
        await engine.run(
            retry_arm,
            ModelScoreUpdate(
                contract.entity_id,
                0.83,
                "risk-v1",
                "round4-third-initial-nonce",
            ),
        )
    assert len(adapter.proof_commits) == 2


async def test_rejects_status_commit_timestamp_that_differs_from_exact_cdf_commit() -> None:
    status_commit_time = NOW - timedelta(seconds=5)
    writer_commit_time = NOW + timedelta(seconds=2)
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=status_commit_time + timedelta(seconds=1),
                last_sync_delta_commit_time=status_commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=writer_commit_time)],
    )

    with pytest.raises(ModelScoreStaleStatusError, match="does not match the exact CDF"):
        await engine.run(arm, initial_update(contract))


async def test_writer_commit_rejects_one_millisecond_beyond_clock_tolerance() -> None:
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[healthy_status(11, 11)],
        commits=[
            DeltaCommit(
                version=11,
                committed_at=NOW + timedelta(seconds=2, milliseconds=1),
            )
        ],
    )

    with pytest.raises(ModelScoreStaleStatusError, match="CDF.*clock tolerance"):
        await engine.run(arm, initial_update(contract))


async def test_hung_progress_callback_times_out_without_blocking_proof() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=commit_time + timedelta(seconds=1),
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
        progress_timeout_seconds=0.01,
    )

    async def hung_callback(_progress) -> None:
        await asyncio.Event().wait()

    result = await asyncio.wait_for(
        engine.run(arm, initial_update(contract), hung_callback),
        timeout=0.5,
    )

    assert result.initial.verified_row == initial_update(contract).row


async def test_progress_callback_failure_does_not_change_arm_or_result() -> None:
    contract = model_score_contract()
    commit_time = NOW - timedelta(seconds=5)
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[
            healthy_status(9, 9),
            warm_up_status(10),
            warm_up_status(10),
            healthy_status(
                11,
                11,
                sync_end_time=commit_time + timedelta(seconds=1),
                last_sync_delta_commit_time=commit_time,
            ),
        ],
        commits=[
            warm_up_commit(10),
            DeltaCommit(version=11, committed_at=commit_time),
        ],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
    )

    async def failing_callback(_progress) -> None:
        raise RuntimeError("presenter disconnected")

    arm = await engine.arm(failing_callback)
    result = await engine.run(arm, initial_update(contract), failing_callback)

    assert result.arm_id == arm.arm_id
    assert result.initial.verified_row == initial_update(contract).row


async def test_progress_callback_preserves_engine_task_cancellation() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=commit_time + timedelta(seconds=1),
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
        progress_timeout_seconds=5,
    )
    callback_entered = asyncio.Event()

    async def blocked_callback(_progress) -> None:
        callback_entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        engine.run(arm, initial_update(contract), blocked_callback)
    )
    await callback_entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert adapter.proof_commits == []
    result = await engine.run(arm, initial_update(contract))
    assert result.initial.verified_row == initial_update(contract).row


async def test_arm_requires_cdf_continuous_caught_up_exact_baseline() -> None:
    contract = model_score_contract()
    status = healthy_status(10, 9)
    adapter = FakeModelScoreAdapter(contract, statuses=[status], commits=[])
    engine = ModelScoreEngine(adapter, contract=contract, now=lambda: NOW)

    with pytest.raises(ModelScoreNotArmedError, match="not fully caught up"):
        await engine.arm()

    status = healthy_status(10, 10)
    status = ManagedSyncStatus(**{**status.__dict__, "cdf_enabled": False})
    adapter = FakeModelScoreAdapter(contract, statuses=[status], commits=[])
    engine = ModelScoreEngine(adapter, contract=contract, now=lambda: NOW)
    with pytest.raises(ModelScoreNotArmedError, match="not CDF-enabled"):
        await engine.arm()


async def test_arm_restores_only_matching_demo_owned_prior_proof_off_clock() -> None:
    contract = model_score_contract()
    prior = ModelScoreRow(
        contract.entity_id,
        0.33,
        "risk-v2",
        "round4-v2-0123456789abcdef0123456789abcdef",
    )
    commit_time = NOW - timedelta(seconds=1)
    repaired_status = healthy_status(
        11,
        11,
        last_sync_delta_commit_time=commit_time,
        sync_end_time=commit_time + timedelta(milliseconds=250),
        observed_at=commit_time + timedelta(milliseconds=500),
    )
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[healthy_status(10, 10), repaired_status, repaired_status],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )
    adapter.source = prior
    adapter.application = prior
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
    )

    arm = await engine.arm()

    assert arm.source_version == 11
    assert adapter.committed_updates == [
        ModelScoreUpdate(
            contract.entity_id,
            contract.baseline_score,
            contract.baseline_model_version,
            contract.baseline_proof_nonce,
        )
    ]
    assert adapter.source == adapter.application == contract.baseline

    foreign = FakeModelScoreAdapter(
        contract,
        statuses=[healthy_status(10, 10)],
        commits=[],
    )
    foreign.source = foreign.application = ModelScoreRow(
        contract.entity_id,
        0.33,
        "risk-v2",
        "foreign-nonce",
    )
    rejecting = ModelScoreEngine(foreign, contract=contract, now=lambda: NOW)
    with pytest.raises(ModelScoreNotArmedError, match="not one matching demo-owned"):
        await rejecting.arm()
    assert foreign.committed_updates == []


def _naive(field: str) -> ManagedSyncStatus:
    """A healthy status with one of its three timestamps stripped of its zone."""

    status = healthy_status(10, 10)
    timestamp = getattr(status, field)
    assert isinstance(timestamp, datetime)
    return replace(status, **{field: timestamp.replace(tzinfo=None)})


async def test_every_status_arming_refuses_and_the_reason_it_gives() -> None:
    """One row per refusal: the status the control plane reported, and the answer.

    These were nine tests of one shape -- build a single bad status, stand an
    adapter on it, arm, and name the error. The exception type and the message
    are what tell them apart, so both are the row's assertion, and the row names
    itself so a failure says which status produced it.

    Every row also asserts that nothing was committed. Arming writes to the
    source table on the happy path, so "it refused" and "it refused before
    touching anything" are different claims and the second is the one that
    matters: a refusal that had already written would leave residue behind for
    the next bout to arm against.
    """

    cases: tuple[tuple[str, ManagedSyncStatus, type[Exception], str], ...] = (
        (
            "the sync is behind the source",
            healthy_status(10, 10, last_sync_delta_version=9),
            ModelScoreNotArmedError,
            "fully caught up",
        ),
        (
            "the baseline status omits its sync end",
            healthy_status(10, 10, sync_end_time=None),
            ModelScoreNotArmedError,
            "omitted its sync end",
        ),
        (
            "the host clock is one millisecond beyond tolerance",
            healthy_status(10, 10, observed_at=NOW + timedelta(seconds=2, milliseconds=1)),
            ModelScoreStaleStatusError,
            "host clock tolerance",
        ),
        (
            # Inside the skew tolerance, so only the ordering can catch it.
            "the timestamps are out of order within the clock skew",
            healthy_status(
                10,
                10,
                last_sync_delta_commit_time=NOW,
                sync_end_time=NOW - timedelta(microseconds=1),
                observed_at=NOW + timedelta(seconds=1),
            ),
            ModelScoreStaleStatusError,
            "commit <= sync end <= observed",
        ),
        (
            # `IDLE` used to be accepted as healthy; a stopped pipeline reports it.
            "the continuous pipeline is stopped",
            replace(healthy_status(10, 10), state=ManagedSyncState.STOPPED),
            ModelScorePipelineError,
            "not healthy",
        ),
        (
            # Provisioning is exactly the cold start the measurement must exclude.
            "the pipeline is still provisioning",
            replace(healthy_status(10, 10), state=ManagedSyncState.STARTING),
            ModelScorePipelineError,
            "not healthy",
        ),
        # A naive timestamp in any of the three fields is unusable, because the
        # arithmetic downstream of it silently compares wall clocks in different
        # zones rather than failing.
        (
            "a naive delta commit time",
            _naive("last_sync_delta_commit_time"),
            ModelScoreStaleStatusError,
            "timezone-aware",
        ),
        (
            "a naive sync end time",
            _naive("sync_end_time"),
            ModelScoreStaleStatusError,
            "timezone-aware",
        ),
        (
            "a naive observation time",
            _naive("observed_at"),
            ModelScoreStaleStatusError,
            "timezone-aware",
        ),
    )

    for name, status, error, match in cases:
        contract = model_score_contract()
        adapter = FakeModelScoreAdapter(contract, statuses=[status], commits=[])
        engine = ModelScoreEngine(adapter, contract=contract, now=lambda: NOW)

        with pytest.raises(error, match=match):
            await engine.arm()
        assert adapter.committed_updates == [], name


async def test_rejects_stale_commit_status_and_invalid_sync_timestamp() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[healthy_status(10, 10)],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )
    with pytest.raises(ModelScoreStaleStatusError, match="source version predates"):
        await engine.run(arm, initial_update(contract))

    engine, _, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=commit_time - timedelta(milliseconds=1),
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )
    with pytest.raises(ModelScoreStaleStatusError, match="commit <= sync end <= observed"):
        await engine.run(arm, initial_update(contract))

    engine, _, arm, contract = await armed_engine(
        poll_statuses=[],
        commits=[DeltaCommit(version=10, committed_at=commit_time)],
    )
    with pytest.raises(ModelScoreStaleStatusError, match="did not advance"):
        await engine.run(arm, initial_update(contract))


async def test_rejects_missing_or_naive_timestamps() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=None,
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )
    with pytest.raises(ModelScoreStaleStatusError, match="omitted"):
        await engine.run(arm, initial_update(contract))

    engine, _, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=NOW,
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time.replace(tzinfo=None))],
    )
    with pytest.raises(ModelScoreStaleStatusError, match="timezone-aware"):
        await engine.run(arm, initial_update(contract))


async def test_status_clock_skew_boundary_is_inclusive_at_two_seconds() -> None:
    contract = model_score_contract()
    observed_at = NOW + timedelta(seconds=2)
    warmed = healthy_status(
        10,
        10,
        last_sync_delta_commit_time=WARM_UP_COMMIT_TIME,
        sync_end_time=WARM_UP_COMMIT_TIME + timedelta(milliseconds=500),
        observed_at=observed_at,
    )
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[
            healthy_status(9, 9, observed_at=observed_at),
            warmed,
            warmed,
        ],
        commits=[warm_up_commit(10)],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
    )

    arm = await engine.arm()

    assert arm.source_version == 10


async def test_equal_commit_sync_end_and_observed_timestamps_are_accepted() -> None:
    contract = model_score_contract()
    start = healthy_status(
        9,
        9,
        last_sync_delta_commit_time=NOW,
        sync_end_time=NOW,
        observed_at=NOW,
    )
    warmed = healthy_status(
        10,
        10,
        last_sync_delta_commit_time=NOW,
        sync_end_time=NOW,
        observed_at=NOW,
    )
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[start, warmed, warmed],
        commits=[DeltaCommit(version=10, committed_at=NOW)],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
    )

    arm = await engine.arm()

    assert arm.source_version == 10


@pytest.mark.parametrize(
    "wrong_row",
    [
        ModelScoreRow("customer-0001", 0.80, "risk-v1", "round4-v1-001"),
        ModelScoreRow("customer-0001", 0.81, "risk-v1", "wrong-nonce"),
    ],
)
async def test_rejects_wrong_application_value_or_nonce(wrong_row: ModelScoreRow) -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=NOW - timedelta(seconds=4),
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )
    adapter.application_override = wrong_row

    with pytest.raises(ModelScoreVerificationError, match="exact PK, score"):
        await engine.run(arm, initial_update(contract))


async def test_pipeline_failure_is_terminal_and_polling_is_bounded() -> None:
    commit_time = NOW - timedelta(seconds=5)
    failed = ManagedSyncStatus(
        **{
            **healthy_status(11, 10).__dict__,
            "state": ManagedSyncState.FAILED,
            "failure": "permission revoked",
        }
    )
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[failed],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )
    with pytest.raises(ModelScorePipelineError, match="permission revoked"):
        await engine.run(arm, initial_update(contract))

    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[healthy_status(11, 10), healthy_status(11, 10)],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
        max_poll_attempts=2,
    )
    with pytest.raises(ModelScoreTimeoutError, match="within 2 polls"):
        await engine.run(arm, initial_update(contract))
    assert not adapter.statuses


async def test_rejects_overshot_sync_version_instead_of_attributing_later_commit() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                12,
                12,
                sync_end_time=NOW - timedelta(seconds=3),
                last_sync_delta_version=12,
                last_sync_delta_commit_time=NOW - timedelta(seconds=4),
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )

    with pytest.raises(ModelScoreStaleStatusError, match="overshot"):
        await engine.run(arm, initial_update(contract))


async def test_rejects_source_version_overshoot_before_status_can_regress() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(12, 10),
            healthy_status(
                11,
                11,
                sync_end_time=NOW - timedelta(seconds=3),
                last_sync_delta_commit_time=commit_time,
            ),
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )

    with pytest.raises(ModelScoreStaleStatusError, match="overshot"):
        await engine.run(arm, initial_update(contract))

    assert len(adapter.statuses) == 1


async def test_hung_sync_inspection_is_bounded_by_wall_clock() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
        inspect_timeout_seconds=0.01,
    )
    # Arming spends three inspections: preflight, the warm-up round trip, and
    # the confirmation re-read. The bout's first poll is the fourth.
    adapter.hang_inspection_calls.add(4)

    with pytest.raises(ModelScoreTimeoutError, match="Managed Sync inspection"):
        await engine.run(arm, initial_update(contract))


@pytest.mark.parametrize(
    ("hang_attribute", "message"),
    [
        ("hang_source_read_calls", "baseline source read"),
        (
            "hang_application_read_calls",
            "baseline fresh application Postgres read",
        ),
    ],
)
async def test_hung_baseline_reads_are_bounded_by_wall_clock(
    hang_attribute: str,
    message: str,
) -> None:
    contract = model_score_contract()
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[healthy_status(10, 10)],
        commits=[],
    )
    getattr(adapter, hang_attribute).add(1)
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        read_timeout_seconds=0.01,
        now=lambda: NOW,
    )

    with pytest.raises(ModelScoreTimeoutError, match=message):
        await engine.arm()


@pytest.mark.parametrize(
    # Call numbers count past arming, which now commits once and reads the
    # source and the application twice each to prove the pipeline is warm. The
    # bell then spends a third application read warming the endpoint, so the
    # measured read is the fourth.
    ("hang_attribute", "call_number", "message"),
    [
        ("hang_commit_calls", 2, "Source Delta commit"),
        ("hang_source_read_calls", 3, "committed source verification read"),
        (
            "hang_application_read_calls",
            4,
            "final fresh application Postgres read",
        ),
    ],
)
async def test_hung_proof_operations_are_bounded_by_wall_clock(
    hang_attribute: str,
    call_number: int,
    message: str,
) -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=commit_time + timedelta(seconds=1),
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
        commit_timeout_seconds=0.01,
        read_timeout_seconds=0.01,
    )
    getattr(adapter, hang_attribute).add(call_number)

    with pytest.raises(ModelScoreTimeoutError, match=message):
        await engine.run(arm, initial_update(contract))


async def test_settlement_waits_for_cancelled_commit_then_restores_both_rows() -> None:
    contract = model_score_contract()
    commit_started = asyncio.Event()
    release_commit = asyncio.Event()

    class DelayedCommitAdapter(FakeModelScoreAdapter):
        async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit:
            # Hold the bout's own commit, not arming's warm-up round trip.
            if update.row != self.baseline and not self.proof_commits:
                commit_started.set()
                await release_commit.wait()
            return await super().commit_source_update(update)

    adapter = DelayedCommitAdapter(
        contract,
        statuses=[
            healthy_status(9, 9),
            warm_up_status(10),
            warm_up_status(10),
            healthy_status(11, 10),
            healthy_status(
                12,
                12,
                last_sync_delta_commit_time=NOW - timedelta(seconds=2),
            ),
        ],
        commits=[
            warm_up_commit(10),
            DeltaCommit(version=11, committed_at=NOW - timedelta(seconds=4)),
            DeltaCommit(version=12, committed_at=NOW - timedelta(seconds=2)),
        ],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
    )
    arm = await engine.arm()
    update = initial_update(contract)
    run = asyncio.create_task(engine.run(arm, update))
    await commit_started.wait()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    settlement = asyncio.create_task(engine.settle_and_restore_baseline())
    await asyncio.sleep(0)
    assert not settlement.done()
    release_commit.set()
    await settlement

    assert adapter.committed_updates == [
        baseline_update(contract),
        update,
        baseline_update(contract),
    ]
    assert adapter.source == contract.baseline
    assert adapter.application == contract.baseline


async def test_settlement_refuses_to_replace_an_unowned_source_row() -> None:
    contract = model_score_contract()
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[healthy_status(10, 10)],
        commits=[],
    )
    foreign = ModelScoreRow(contract.entity_id, 0.91, "foreign", "foreign-nonce")
    adapter.source = foreign
    adapter.application = foreign
    engine = ModelScoreEngine(adapter, contract=contract, now=lambda: NOW)

    with pytest.raises(ModelScoreVerificationError, match="not issued by this engine"):
        await engine.settle_and_restore_baseline()

    assert adapter.source == foreign
    assert not adapter.committed_updates


async def test_application_elapsed_uses_authoritative_delta_commit_time() -> None:
    commit_time = NOW - timedelta(seconds=5)
    calls = 0

    def proof_clock() -> int:
        nonlocal calls
        calls += 1
        return 1_000_000 if calls == 1 else 126_000_000

    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=commit_time + timedelta(seconds=1),
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
        clock_ns=proof_clock,
    )

    def observe_dispatch() -> None:
        assert calls == 0

    adapter.commit_observer = observe_dispatch
    result = await engine.run(arm, initial_update(contract))

    assert result.initial.application_read_elapsed_ms == 5000.0


async def test_application_elapsed_rejects_read_before_authoritative_delta_commit() -> None:
    commit_time = NOW + timedelta(seconds=1)
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=commit_time,
                last_sync_delta_commit_time=commit_time,
                observed_at=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )

    with pytest.raises(ModelScoreStaleStatusError, match="read predates"):
        await engine.run(arm, initial_update(contract))


@pytest.mark.parametrize(
    "changed_status",
    [
        replace(
            healthy_status(11, 11, sync_end_time=NOW - timedelta(seconds=4)),
            pipeline_id="some-other-pipeline",
        ),
        replace(
            healthy_status(11, 11, sync_end_time=NOW - timedelta(seconds=4)),
            source_table="main.other.model_scores",
        ),
        replace(
            healthy_status(11, 11, sync_end_time=NOW - timedelta(seconds=4)),
            synced_table="public.some_other_table",
        ),
        replace(
            healthy_status(11, 11, sync_end_time=NOW - timedelta(seconds=4)),
            cdf_enabled=False,
        ),
        replace(
            healthy_status(11, 11, sync_end_time=NOW - timedelta(seconds=4)),
            continuous=False,
        ),
    ],
)
async def test_mid_poll_identity_or_policy_change_fails_closed(
    changed_status: ManagedSyncStatus,
) -> None:
    commit_time = NOW - timedelta(seconds=5)
    changed_status = replace(changed_status, last_sync_delta_commit_time=commit_time)
    engine, _, arm, contract = await armed_engine(
        poll_statuses=[changed_status],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )

    with pytest.raises(ModelScoreStaleStatusError):
        await engine.run(arm, initial_update(contract))


async def test_arm_rechecks_status_after_baseline_reads_and_rejects_race() -> None:
    contract = model_score_contract()
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[
            healthy_status(9, 9),
            warm_up_status(10),
            healthy_status(11, 11),
        ],
        commits=[warm_up_commit(10)],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
    )

    with pytest.raises(ModelScoreNotArmedError, match="advanced"):
        await engine.arm()


@pytest.mark.parametrize(
    "confirmed_status",
    [
        replace(
            warm_up_status(10),
            last_sync_delta_commit_time=WARM_UP_COMMIT_TIME - timedelta(milliseconds=250),
        ),
        replace(
            warm_up_status(10),
            sync_end_time=WARM_UP_COMMIT_TIME + timedelta(milliseconds=750),
        ),
    ],
)
async def test_arm_rejects_full_confirmation_position_movement(
    confirmed_status: ManagedSyncStatus,
) -> None:
    contract = model_score_contract()
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[healthy_status(9, 9), warm_up_status(10), confirmed_status],
        commits=[warm_up_commit(10)],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
    )

    with pytest.raises(ModelScoreNotArmedError, match="advanced"):
        await engine.arm()


async def test_duplicate_redo_nonce_preserves_redo_eligibility_before_commit() -> None:
    first_commit = NOW - timedelta(seconds=8)
    second_commit = NOW - timedelta(seconds=5)
    redo_commit = NOW - timedelta(seconds=2)
    engine, adapter, first_arm, second_arm, contract = await two_armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=first_commit + timedelta(seconds=1),
                last_sync_delta_commit_time=first_commit,
            ),
            healthy_status(
                12,
                12,
                sync_end_time=second_commit + timedelta(seconds=1),
                last_sync_delta_commit_time=second_commit,
            ),
            healthy_status(
                13,
                13,
                sync_end_time=redo_commit + timedelta(milliseconds=500),
                last_sync_delta_commit_time=redo_commit,
            ),
        ],
        commits=[
            DeltaCommit(version=11, committed_at=first_commit),
            DeltaCommit(version=12, committed_at=second_commit),
            DeltaCommit(version=13, committed_at=redo_commit),
        ],
    )
    first = await engine.run(
        first_arm,
        ModelScoreUpdate(
            contract.entity_id,
            0.81,
            "risk-v1",
            "round4-first-initial-nonce",
        ),
    )
    await engine.run(
        second_arm,
        ModelScoreUpdate(
            contract.entity_id,
            0.82,
            "risk-v1",
            "round4-used-by-other-initial",
        ),
    )

    with pytest.raises(ModelScoreDuplicateRedoError, match="nonce"):
        await engine.redo(
            first_arm,
            first,
            ModelScoreUpdate(
                contract.entity_id,
                0.33,
                "risk-v2",
                "round4-used-by-other-initial",
            ),
        )
    assert len(adapter.proof_commits) == 2

    redone = await engine.redo(
        first_arm,
        first,
        ModelScoreUpdate(
            contract.entity_id,
            0.33,
            "risk-v2",
            "round4-distinct-redo-nonce",
        ),
    )
    assert redone.redo is not None
    assert redone.redo.source_version == 13
    assert len(adapter.proof_commits) == 3

    with pytest.raises(ModelScoreDuplicateRedoError, match="already been redone"):
        await engine.redo(
            first_arm,
            redone,
            ModelScoreUpdate(
                contract.entity_id,
                0.44,
                "risk-v3",
                "round4-second-redo-nonce",
            ),
        )
    assert len(adapter.proof_commits) == 3


async def test_redo_updates_same_pk_with_distinct_v2_values_exactly_once() -> None:
    first_commit = NOW - timedelta(seconds=8)
    second_commit = NOW - timedelta(seconds=4)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=first_commit + timedelta(seconds=1),
                last_sync_delta_commit_time=first_commit,
            ),
            healthy_status(
                12,
                12,
                sync_end_time=second_commit + timedelta(seconds=1),
                last_sync_delta_commit_time=second_commit,
            ),
        ],
        commits=[
            DeltaCommit(version=11, committed_at=first_commit),
            DeltaCommit(version=12, committed_at=second_commit),
        ],
    )
    first = initial_update(contract)
    result = await engine.run(arm, first)

    invalid_updates = [
        ModelScoreUpdate(contract.entity_id, first.score, "risk-v2", "round4-v2-002"),
        ModelScoreUpdate(contract.entity_id, 0.33, first.model_version, "round4-v2-002"),
        ModelScoreUpdate(contract.entity_id, 0.33, "risk-v2", first.proof_nonce),
    ]
    for update in invalid_updates:
        with pytest.raises(ModelScoreDuplicateRedoError):
            await engine.redo(arm, result, update)

    redo = ModelScoreUpdate(
        entity_id=contract.entity_id,
        score=0.33,
        model_version="risk-v2",
        proof_nonce="round4-v2-002",
    )
    redone = await engine.redo(arm, result, redo)

    assert redone.initial == result.initial
    assert redone.redo is not None
    assert redone.redo.kind == ModelScoreProofKind.REDO
    assert redone.redo.update.model_version == "risk-v2"
    assert redone.redo.update.proof_nonce == "round4-v2-002"
    assert redone.redo.update.entity_id == first.entity_id == contract.entity_id
    assert redone.redo.verified_row == redo.row
    assert [item.proof_nonce for item in adapter.proof_commits] == [
        first.proof_nonce,
        redo.proof_nonce,
    ]
    with pytest.raises(ModelScoreDuplicateRedoError, match="already been redone"):
        await engine.redo(arm, redone, ModelScoreUpdate(
            contract.entity_id,
            0.44,
            "risk-v3",
            "round4-v3-003",
        ))
    assert len(adapter.proof_commits) == 2


async def test_redo_rejects_forged_structurally_matching_initial_result() -> None:
    commit_time = NOW - timedelta(seconds=5)
    engine, adapter, arm, contract = await armed_engine(
        poll_statuses=[
            healthy_status(
                11,
                11,
                sync_end_time=commit_time + timedelta(seconds=1),
                last_sync_delta_commit_time=commit_time,
            )
        ],
        commits=[DeltaCommit(version=11, committed_at=commit_time)],
    )
    result = await engine.run(arm, initial_update(contract))
    forged = replace(result)

    with pytest.raises(ModelScoreDuplicateRedoError, match="engine-issued"):
        await engine.redo(
            arm,
            forged,
            ModelScoreUpdate(contract.entity_id, 0.33, "risk-v2", "round4-v2-002"),
        )
    assert len(adapter.proof_commits) == 1


def test_generic_metric_and_redo_models_hold_future_round_shapes() -> None:
    p99 = MetricSpec(
        id="application_p99",
        label="Application p99",
        role=MetricRole.SECONDARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    )
    redo = RedoSnapshot(
        state=RedoState.VERIFIED,
        lanes={"lakebase": LaneSnapshot(id="lakebase", name="Lakebase")},
        metric_specs=[p99],
        metrics=[
            MetricValue(
                spec_id=p99.id,
                lane_id="lakebase",
                value=12.5,
                display_value="12.50ms",
            )
        ],
        comparison=ComparisonSnapshot(
            kind=ComparisonKind.CAPABILITY_GAP,
            winner_lane_id="lakebase",
            detail="No equivalent managed capability was timed",
        ),
    )

    payload = redo.model_dump(mode="json")
    assert payload["metrics"][0]["value"] == 12.5
    assert payload["metrics"][0]["lane_id"] == "lakebase"
    assert payload["metric_specs"][0]["id"] == "application_p99"
    assert payload["comparison"]["kind"] == "capability_gap"
    assert payload["lanes"]["lakebase"]["state"] == "sealed"
    legacy_value = MetricValue(spec_id="legacy", value=1).model_dump(mode="json")
    assert "lane_id" not in legacy_value


@pytest.mark.parametrize(
    ("kind", "winner", "margin", "message"),
    [
        (
            ComparisonKind.CAPABILITY_GAP,
            None,
            None,
            "requires a winner and forbids a margin",
        ),
        (
            ComparisonKind.CAPABILITY_GAP,
            "lakebase",
            MetricValue(spec_id="availability", lane_id="lakebase", value=10.0),
            "requires a winner and forbids a margin",
        ),
        (
            ComparisonKind.NOT_COMPARABLE,
            "lakebase",
            None,
            "not_comparable cannot declare a winner or margin",
        ),
        (
            ComparisonKind.NOT_COMPARABLE,
            None,
            MetricValue(spec_id="correctness", lane_id="lakebase", value=True),
            "not_comparable cannot declare a winner or margin",
        ),
    ],
)
def test_non_comparable_kinds_forbid_winner_and_margin(
    kind: ComparisonKind,
    winner: str | None,
    margin: MetricValue | None,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        ComparisonSnapshot(kind=kind, winner_lane_id=winner, margin=margin)

    valid_gap = ComparisonSnapshot(
        kind=ComparisonKind.CAPABILITY_GAP,
        winner_lane_id="lakebase",
    )
    assert valid_gap.winner_lane_id == "lakebase"
    assert valid_gap.margin is None


class ReplayableManagedSync(ModelScoreAdapter):
    """A stateful Delta source plus continuous sync, replayable across bouts.

    The canned-status fakes elsewhere in this file cannot answer the question
    that matters for a repeated demo: what does a *second* bout see after a
    first one finished successfully?
    """

    def __init__(self, contract: ModelScoreContract, *, version: int = 10) -> None:
        self.contract = contract
        self.source = contract.baseline
        self.application = contract.baseline
        self.version = version
        self.committed_updates: list[ModelScoreUpdate] = []

    async def inspect_sync(self) -> ManagedSyncStatus:
        return healthy_status(self.version, self.version)

    async def read_source(self, entity_id: str) -> ModelScoreRow | None:
        return self.source if self.source.entity_id == entity_id else None

    async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit:
        self.version += 1
        self.source = update.row
        self.application = update.row
        self.committed_updates.append(update)
        return DeltaCommit(version=self.version, committed_at=NOW - timedelta(seconds=2))

    async def read_application_fresh(self, entity_id: str) -> ModelScoreRow | None:
        return self.application if self.application.entity_id == entity_id else None


def replayable_engine(adapter: ReplayableManagedSync) -> ModelScoreEngine:
    # The manager builds a brand-new engine for every bout, so a second bout
    # cannot rely on anything the first one remembered in process.
    return ModelScoreEngine(
        adapter,
        contract=adapter.contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
        clock_ns=ticking_clock(),
    )


def run_owned_update(contract: ModelScoreContract) -> ModelScoreUpdate:
    return ModelScoreUpdate(
        entity_id=contract.entity_id,
        score=0.81,
        model_version="risk-v1",
        proof_nonce=f"round4-v1-{uuid4().hex}",
    )


async def test_a_completed_bout_leaves_its_own_proof_row_in_the_source_table() -> None:
    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    engine = replayable_engine(adapter)
    update = run_owned_update(contract)

    await engine.run(await engine.arm(), update)

    # Nothing on the success path puts the baseline back; only a towel does.
    assert adapter.source == update.row
    assert adapter.application == update.row


async def test_a_second_bout_arms_on_the_state_the_first_bout_left_behind() -> None:
    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    first = replayable_engine(adapter)
    await first.run(await first.arm(), run_owned_update(contract))

    second = replayable_engine(adapter)
    arm = await second.arm()

    assert adapter.source == contract.baseline
    assert adapter.application == contract.baseline
    assert arm.baseline == contract.baseline

    redone = run_owned_update(contract)
    await second.run(arm, redone)
    assert adapter.source == redone.row


async def test_arming_reclaims_an_abandoned_proof_row_that_settlement_would_refuse() -> None:
    """Characterization. The two mechanisms disagree on purpose, and it matters.

    Settlement asks "did *I* issue this row?" out of process memory, so the
    engine that abandoned a publication -- or any engine after a restart --
    refuses the very row the demo wrote. Arming asks the different and purely
    structural question "is this one of the demo-owned shapes?", so it reclaims
    that same row and re-seeds the sealed baseline with no process state at all.
    That is why an abandoned proof row is not a hand-repair job.
    """

    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    abandoned = run_owned_update(contract)
    adapter.source = abandoned.row
    adapter.application = abandoned.row

    # No engine in this process issued it, which is what a restart looks like.
    with pytest.raises(ModelScoreVerificationError, match="not issued by this engine"):
        await replayable_engine(adapter).settle_and_restore_baseline()
    assert adapter.source == abandoned.row

    arm = await replayable_engine(adapter).arm()

    assert adapter.source == contract.baseline
    assert adapter.application == contract.baseline
    assert arm.baseline == contract.baseline
    assert not is_owned_prior_proof(adapter.source)


class StoppedUntilStarted(ReplayableManagedSync):
    """A Managed Sync that behaves the way a stopped one measurably behaves.

    Not "reports ``ManagedSyncState.STOPPED``", which is the thing everyone
    assumes and is not what happens. Measured against the live sealed pipeline on
    2026-08-24: a stopped pipeline's synced table omits
    ``continuous_update_status`` **entirely**, and
    ``_validate_database_synced_table`` requires it as a non-empty mapping --- so
    ``inspect_sync`` *raises* on its way to discovering the pipeline is down and
    never returns a status object at all. Any fix that waited for a non-RUNNING
    status would be waiting for a value that is never produced.
    """

    def __init__(self, contract: ModelScoreContract) -> None:
        super().__init__(contract)
        self.running = False
        self.inspections_while_stopped = 0

    async def inspect_sync(self) -> ManagedSyncStatus:
        if not self.running:
            self.inspections_while_stopped += 1
            # A plain RuntimeError rather than the live module's error class:
            # this module is below `model_score_live` and the substance is that
            # the call raises at all, not which type comes out of it.
            raise RuntimeError(
                "Round 4 Database Synced Table continuous update status is missing"
            )
        return await super().inspect_sync()


class RecordingActivation:
    def __init__(self, adapter: StoppedUntilStarted) -> None:
        self._adapter = adapter
        self.notices: list[str] = []
        self.releases = 0
        self.immediate_releases = 0

    async def ensure_running(self, notify) -> None:
        if not self._adapter.running:
            await notify("Starting the Managed Sync pipeline")
            self._adapter.running = True

    def release_when_idle(self) -> None:
        self.releases += 1

    async def release_now(self) -> None:
        self.immediate_releases += 1
        self._adapter.running = False


async def test_a_stopped_pipeline_is_started_before_the_arm_inspects_it() -> None:
    """The round survives a pipeline that was switched off to save money.

    This is the regression that reaches an audience. Round 4's pipeline bills
    around $14.57/day resident, so it is now stopped once a bout has settled ---
    and if arming cannot recover from that, the saving buys a dead round at the
    bell instead. The ordering is the substance: ``ensure_running`` runs *before*
    ``_inspect_sync``, because a stopped pipeline makes that call raise rather
    than report.
    """

    contract = model_score_contract()
    adapter = StoppedUntilStarted(contract)
    activation = RecordingActivation(adapter)
    notices: list[str] = []
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
        clock_ns=ticking_clock(),
        activation=activation,
    )

    arm = await engine.arm(lambda event: notices.append(event.status))

    assert arm.baseline == contract.baseline
    # Never inspected while down: the start happened first, so the call that
    # cannot survive a stopped pipeline was never made against one.
    assert adapter.inspections_while_stopped == 0
    # And the wait is visible. An arm that blocks silently for a minute in front
    # of a room is indistinguishable from one that has hung.
    assert any("Starting the Managed Sync pipeline" in notice for notice in notices)


async def test_an_engine_with_no_activation_arms_exactly_as_it_always_did() -> None:
    """The amendment is opt-in, and every other construction is untouched."""

    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    engine = replayable_engine(adapter)

    assert (await engine.arm()).baseline == contract.baseline
    await engine.settle_and_restore_baseline()


async def test_the_pipeline_is_released_only_after_settlement_proves_the_baseline() -> None:
    """A stop scheduled any earlier is a stop that races the thing it follows.

    D20's third blocker is that settlement needs the pipeline live, and
    ``settle_and_restore_baseline`` is fire-and-forget after the terminal
    receipt. So the release is the last statement on the settled path and is not
    reached at all when settlement refuses --- a settlement that failed will be
    retried, and a retry needs the pipeline just as much as the first attempt.

    *Which* release is owed is decided here too, out of the engine's own state
    rather than by being told, and the two must not be swapped. A bout that
    issued a result leaves a redo the presenter may still take, which is the
    only thing the activation's idle window is bought for. A bout that issued
    none --- a towel, or a run that failed --- can never be redone, so holding
    a pipeline that bills by the day for it protects nothing.
    """

    contract = model_score_contract()
    adapter = StoppedUntilStarted(contract)
    activation = RecordingActivation(adapter)

    def engine_for() -> ModelScoreEngine:
        return ModelScoreEngine(
            adapter,
            contract=contract,
            poll_interval_seconds=0,
            now=lambda: NOW,
            clock_ns=ticking_clock(),
            activation=activation,
        )

    engine = engine_for()
    await engine.run(await engine.arm(), run_owned_update(contract))
    # Verified, and the pipeline is still nobody's to stop: settlement has not
    # run yet.
    assert activation.releases == 0

    await engine.settle_and_restore_baseline()
    assert (activation.releases, activation.immediate_releases) == (1, 0)

    # A settlement that refuses releases nothing. This engine never issued the
    # row, which is what a restart mid-bout looks like.
    abandoned = run_owned_update(contract)
    adapter.source = abandoned.row
    adapter.application = abandoned.row
    with pytest.raises(ModelScoreVerificationError, match="not issued by this engine"):
        await engine_for().settle_and_restore_baseline()
    assert (activation.releases, activation.immediate_releases) == (1, 0)

    # A towel: armed, so the pipeline is up and is this bout's doing, but no
    # result was ever issued, so there is no redo standing between the operator
    # and a pipeline they have just asked to stop.
    towelled = engine_for()
    await towelled.arm()
    await towelled.settle_and_restore_baseline()
    assert (activation.releases, activation.immediate_releases) == (1, 1)


async def test_a_second_bout_refuses_a_source_row_this_demo_never_wrote() -> None:
    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    foreign = ModelScoreRow(contract.entity_id, 0.91, "someone-elses-model", "not-a-demo-nonce")
    adapter.source = foreign
    adapter.application = foreign

    with pytest.raises(ModelScoreNotArmedError, match="demo-owned Round 4 proof"):
        await replayable_engine(adapter).arm()

    assert adapter.source == foreign
    assert adapter.committed_updates == []


def test_the_proof_nonces_the_manager_mints_are_recognised_as_demo_owned() -> None:
    contract = model_score_contract()
    for score, model_version, prefix in OWNED_PROOF_SHAPES:
        row = ModelScoreRow(contract.entity_id, score, model_version, f"{prefix}{uuid4().hex}")
        assert is_owned_prior_proof(row)
    assert not is_owned_prior_proof(
        ModelScoreRow(contract.entity_id, 0.81, "risk-v1", "round4-v1-001")
    )


# ---------------------------------------------------------------------------
# Round 4 measures ``sync_end - committed_at`` on the bout's own commit, so any
# compute the pipeline must provision to process that commit is inside the
# headline number by construction. Arming used to read a status and refuse; it
# never started anything and never made the pipeline do work. These pin the
# replacement: warmth is proven before the bell or the bout does not start.
# ---------------------------------------------------------------------------


class ColdManagedSync(ReplayableManagedSync):
    """Caught up on paper, but nothing is running to process the next commit.

    This is the shape that makes the defect dangerous rather than merely wrong:
    every reading the old gate took was healthy, because a stopped continuous
    pipeline has genuinely applied everything committed *so far*.
    """

    def __init__(self, contract: ModelScoreContract, *, version: int = 10) -> None:
        super().__init__(contract, version=version)
        self.synced_version = version

    async def inspect_sync(self) -> ManagedSyncStatus:
        return healthy_status(self.version, self.synced_version)

    async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit:
        self.version += 1
        self.source = update.row
        self.committed_updates.append(update)
        return DeltaCommit(version=self.version, committed_at=NOW - timedelta(seconds=2))


def cold_engine(adapter: ColdManagedSync, *, max_poll_attempts: int = 3) -> ModelScoreEngine:
    return ModelScoreEngine(
        adapter,
        contract=adapter.contract,
        max_poll_attempts=max_poll_attempts,
        poll_interval_seconds=0,
        now=lambda: NOW,
        clock_ns=ticking_clock(),
    )


async def test_arm_round_trips_a_throwaway_version_even_when_the_baseline_is_exact() -> None:
    """The regression: an exact baseline used to mean no repair, so no warm-up.

    Success-path settlement restores the exact baseline after every bout, so
    the common case at arming time is that the row already matches and the old
    conditional repair skipped entirely. The first data through the pipeline
    after a long idle was then the measured commit.
    """

    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    assert adapter.source == contract.baseline  # nothing to repair

    arm = await replayable_engine(adapter).arm()

    assert adapter.committed_updates == [baseline_update(contract)]
    assert arm.source_version == adapter.version


async def test_arm_warm_up_leaves_no_residue_and_does_not_disturb_the_baseline() -> None:
    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)

    arm = await replayable_engine(adapter).arm()

    # The warm-up carries the sealed baseline, so it spends a Delta version
    # without changing the row anybody compares against.
    assert adapter.source == contract.baseline
    assert adapter.application == contract.baseline
    assert arm.baseline == contract.baseline
    # And it can never be mistaken for a bout's proof row.
    assert not is_owned_prior_proof(adapter.source)
    assert adapter.committed_updates[0].proof_nonce == contract.baseline_proof_nonce


async def test_arm_warm_up_leaves_the_row_the_readiness_gate_expects() -> None:
    """The gate short-circuits on an exact baseline, so it never sees the warm-up."""

    from server.lifecycle import ROUND4_BASELINE_ROW

    contract = ModelScoreContract(
        pipeline_id="round4-model-score-sync",
        source_table="main.anti_demo.model_scores",
        synced_table="public.model_scores",
        entity_id=ROUND4_BASELINE_ROW.entity_id,
        baseline_score=ROUND4_BASELINE_ROW.score,
        baseline_model_version=ROUND4_BASELINE_ROW.model_version,
        baseline_proof_nonce=ROUND4_BASELINE_ROW.proof_nonce,
    )
    adapter = ReplayableManagedSync(contract)

    await replayable_engine(adapter).arm()

    assert adapter.source == ROUND4_BASELINE_ROW
    assert not is_owned_prior_proof(adapter.source)


async def test_arm_refuses_when_the_warm_up_round_trip_exceeds_the_window() -> None:
    """A cold pipeline is caught at arming time, with nothing on screen."""

    contract = model_score_contract()
    adapter = ColdManagedSync(contract)

    with pytest.raises(ModelScoreNotArmedError, match="cannot be proven warm"):
        await cold_engine(adapter).arm()

    # It refused rather than timing out into a plausible-but-wrong measurement.
    assert adapter.synced_version == 10
    assert adapter.committed_updates == [baseline_update(contract)]


async def test_a_cold_pipeline_that_wakes_inside_the_window_still_arms() -> None:
    """Warmth is proven empirically, so a pipeline that does the work passes."""

    contract = model_score_contract()

    class WakingManagedSync(ColdManagedSync):
        async def inspect_sync(self) -> ManagedSyncStatus:
            status = healthy_status(self.version, self.synced_version)
            self.synced_version = self.version
            self.application = self.source
            return status

    adapter = WakingManagedSync(contract)
    arm = await cold_engine(adapter).arm()

    assert arm.source_version == adapter.version
    assert adapter.source == contract.baseline


async def test_the_warm_up_never_speaks_in_the_bouts_voice() -> None:
    """Arming stays in PREFLIGHT so the warm-up cannot read as the measured wait."""

    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    seen: list[ModelScoreProgress] = []

    async def record(progress: ModelScoreProgress) -> None:
        seen.append(progress)

    await replayable_engine(adapter).arm(record)

    assert seen  # the warm-up does report progress, as arming
    assert {progress.phase for progress in seen} == {
        ModelScorePhase.PREFLIGHT,
        ModelScorePhase.ARMED,
    }
    assert ModelScorePhase.WAITING_SYNC not in {progress.phase for progress in seen}
    # No Delta version leaks into arming's on-screen text.
    assert not any("version" in progress.status.casefold() for progress in seen)


async def test_settlement_after_an_arm_only_exit_finds_nothing_to_undo() -> None:
    """Towelled, cancelled or abandoned before the bell: the warm-up self-settles."""

    contract = model_score_contract()
    adapter = ReplayableManagedSync(contract)
    engine = replayable_engine(adapter)
    await engine.arm()

    await engine.settle_and_restore_baseline()

    assert adapter.source == contract.baseline
    assert adapter.application == contract.baseline
    # Settlement had no residue to remove, so it wrote nothing of its own.
    assert adapter.committed_updates == [baseline_update(contract)]


class InterruptibleManagedSync(ReplayableManagedSync):
    """A replayable sync whose final application read can fail or hang on demand.

    Round 4's terminal paths -- verified, failed, cancelled and the client
    abort that cancels the run task -- all converge on settlement, so the
    interesting axis is how the bout ended, not why.
    """

    def __init__(self, contract: ModelScoreContract) -> None:
        super().__init__(contract)
        self.corrupt_application = False
        self.slow_application = False
        self.application_started = asyncio.Event()

    async def read_application_fresh(self, entity_id: str) -> ModelScoreRow | None:
        if self.slow_application:
            self.slow_application = False
            self.application_started.set()
            # Live Statement Execution runs on a worker thread and keeps going
            # after its asyncio waiter is cancelled, so settlement waits for it
            # rather than assuming a cancelled waiter means nothing ran.
            await asyncio.sleep(0.05)
        if self.corrupt_application:
            return ModelScoreRow(entity_id, 0.0, "drifted", "drifted-nonce")
        return await super().read_application_fresh(entity_id)


@pytest.mark.parametrize("exit_path", ["verified", "failed", "cancelled", "client_abort"])
async def test_settlement_restores_the_baseline_on_every_exit_path(exit_path: str) -> None:
    contract = model_score_contract()
    adapter = InterruptibleManagedSync(contract)
    engine = replayable_engine(adapter)
    arm = await engine.arm()
    update = run_owned_update(contract)

    if exit_path == "verified":
        await engine.run(arm, update)
    elif exit_path == "failed":
        adapter.corrupt_application = True
        with pytest.raises(ModelScoreVerificationError):
            await engine.run(arm, update)
        adapter.corrupt_application = False
    else:
        # "cancelled" is the towel cancelling the run task; "client_abort" is
        # the request going away underneath it. Both arrive here as a cancel.
        adapter.slow_application = True
        task = asyncio.create_task(engine.run(arm, update))
        await adapter.application_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    await engine.settle_and_restore_baseline()

    assert adapter.source == contract.baseline
    assert adapter.application == contract.baseline
    # Whatever happened, nothing demo-owned is left for the next bout to inherit.
    assert not is_owned_prior_proof(adapter.source)


async def test_the_measured_availability_is_still_sync_end_minus_delta_commit() -> None:
    """Pin the headline arithmetic against a real Round 4 receipt.

    Commit 00:59:27Z, sync end 00:59:32.089596Z, two poll attempts. Proving the
    pipeline warm must not redefine the number, subtract an estimated start-up,
    or widen what counts as the measurement.
    """

    now = datetime(2026, 8, 19, 1, 0, 0, tzinfo=UTC)
    committed_at = datetime(2026, 8, 19, 0, 59, 27, tzinfo=UTC)
    sync_end = datetime(2026, 8, 19, 0, 59, 32, 89596, tzinfo=UTC)
    contract = model_score_contract()

    def status(source_version: int, processed: int) -> ManagedSyncStatus:
        return ManagedSyncStatus(
            pipeline_id=contract.pipeline_id,
            source_table=contract.source_table,
            synced_table=contract.synced_table,
            state=ManagedSyncState.RUNNING,
            cdf_enabled=True,
            continuous=True,
            source_version=source_version,
            last_processed_version=processed,
            last_sync_delta_version=processed,
            last_sync_delta_commit_time=committed_at,
            observed_at=now - timedelta(seconds=1),
            sync_end_time=sync_end,
        )

    warm = status(10, 10)
    adapter = FakeModelScoreAdapter(
        contract,
        statuses=[status(9, 9), warm, warm, status(11, 10), status(11, 11)],
        commits=[
            DeltaCommit(version=10, committed_at=committed_at),
            DeltaCommit(version=11, committed_at=committed_at),
        ],
    )
    engine = ModelScoreEngine(
        adapter,
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: now,
        clock_ns=ticking_clock(),
    )

    result = await engine.run(await engine.arm(), initial_update(contract))

    assert result.initial.delta_commit_time == committed_at
    assert result.initial.sync_end_time == sync_end
    assert result.initial.managed_availability_ms == pytest.approx(5089.596)
    assert result.initial.poll_attempts == 2
    assert result.initial.managed_availability_ms == pytest.approx(
        (sync_end - committed_at).total_seconds() * 1000
    )


# ---------------------------------------------------------------------------
# The secondary number, ``application_read_elapsed_ms``, runs from the Delta
# commit to the successful fresh application read. Arming leaves the Lakebase
# endpoint awake, but its last Postgres touch is the repaired baseline read;
# the arm-to-bell gap, the commit and the entire reverse-ETL wait then pass
# with nothing touching Postgres, so the endpoint's suspend timer can come due
# and bill its wake to that number. These pin the warm-up that stops it, and
# pin that the headline ``managed_availability_ms`` is unmoved either way.
# ---------------------------------------------------------------------------

ENDPOINT_WAKE_MS = 2410.0
APPLICATION_READ_MS = 1.0
SYNC_LAG_MS = 500.0


class WallClock:
    """A clock the fakes advance, so a simulated wake is actually measurable."""

    def __init__(self, start: datetime = NOW) -> None:
        self.value = start

    def now(self) -> datetime:
        return self.value

    def advance_ms(self, milliseconds: float) -> None:
        self.value += timedelta(milliseconds=milliseconds)


class SuspendingEndpointSync(ModelScoreAdapter):
    """A healthy Managed Sync whose Lakebase endpoint scales to zero when idle.

    Every fresh application connection costs ``APPLICATION_READ_MS``; the first
    one taken after the endpoint has suspended pays ``ENDPOINT_WAKE_MS`` on top.
    The Delta commit is stamped from the same clock, which is the whole point:
    it is what decides whether a wake falls inside the measured window or
    before it.
    """

    def __init__(
        self,
        contract: ModelScoreContract,
        clock: WallClock,
        *,
        version: int = 10,
        deny_warm_up: bool = False,
    ) -> None:
        self.contract = contract
        self.clock = clock
        self.deny_warm_up = deny_warm_up
        self.source = contract.baseline
        self.application = contract.baseline
        self.version = version
        self.suspended = False
        self.proof_committed = False
        self.committed_at = clock.now() - timedelta(seconds=5)
        self.committed_updates: list[ModelScoreUpdate] = []
        self.application_reads = 0
        self.denied_warm_ups = 0

    def suspend(self) -> None:
        """The endpoint scaled to zero while nothing was touching Postgres."""

        self.suspended = True

    async def inspect_sync(self) -> ManagedSyncStatus:
        settled = self.committed_at + timedelta(milliseconds=SYNC_LAG_MS)
        return ManagedSyncStatus(
            pipeline_id=self.contract.pipeline_id,
            source_table=self.contract.source_table,
            synced_table=self.contract.synced_table,
            state=ManagedSyncState.RUNNING,
            cdf_enabled=True,
            continuous=True,
            source_version=self.version,
            last_processed_version=self.version,
            last_sync_delta_version=self.version,
            last_sync_delta_commit_time=self.committed_at,
            observed_at=settled,
            sync_end_time=settled,
        )

    async def read_source(self, entity_id: str) -> ModelScoreRow | None:
        return self.source if self.source.entity_id == entity_id else None

    async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit:
        self.version += 1
        self.source = update.row
        self.application = update.row
        self.committed_at = self.clock.now()
        self.committed_updates.append(update)
        if update.row != self.contract.baseline:
            self.proof_committed = True
        return DeltaCommit(version=self.version, committed_at=self.committed_at)

    async def read_application_fresh(self, entity_id: str) -> ModelScoreRow | None:
        self.application_reads += 1
        if self.deny_warm_up and self.suspended and not self.proof_committed:
            # The warm-up, and only the warm-up, cannot reach the endpoint.
            self.denied_warm_ups += 1
            raise ConnectionError("no route to the suspended Lakebase endpoint")
        if self.suspended:
            self.suspended = False
            self.clock.advance_ms(ENDPOINT_WAKE_MS)
        self.clock.advance_ms(APPLICATION_READ_MS)
        return self.application if self.application.entity_id == entity_id else None


def suspending_engine(adapter: SuspendingEndpointSync) -> ModelScoreEngine:
    return ModelScoreEngine(
        adapter,
        contract=adapter.contract,
        poll_interval_seconds=0,
        now=adapter.clock.now,
        clock_ns=ticking_clock(),
    )


async def armed_against_a_suspending_endpoint(
    *,
    deny_warm_up: bool = False,
) -> tuple[ModelScoreEngine, SuspendingEndpointSync, ModelScoreArm, ModelScoreContract]:
    """Arm while the endpoint is awake, then let it suspend before the bell."""

    contract = model_score_contract()
    adapter = SuspendingEndpointSync(contract, WallClock(), deny_warm_up=deny_warm_up)
    engine = suspending_engine(adapter)
    arm = await engine.arm()
    adapter.suspend()
    return engine, adapter, arm, contract


async def test_the_endpoint_wake_lands_in_the_end_to_end_proof_without_the_warm_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The defect, held still: 2410 ms of wake billed to the published number."""

    async def no_warm_up(self: ModelScoreEngine, on_progress: object) -> None:
        return None

    monkeypatch.setattr(ModelScoreEngine, "_warm_application_endpoint", no_warm_up)
    engine, adapter, arm, contract = await armed_against_a_suspending_endpoint()

    result = await engine.run(arm, initial_update(contract))

    assert result.initial.application_read_elapsed_ms == pytest.approx(
        ENDPOINT_WAKE_MS + APPLICATION_READ_MS
    )
    assert result.initial.application_read_elapsed_ms == pytest.approx(2411.0)
    # The headline was never exposed to it, which is why the defect survived.
    assert result.initial.managed_availability_ms == pytest.approx(SYNC_LAG_MS)


async def test_the_warm_up_keeps_the_endpoint_wake_out_of_the_end_to_end_proof() -> None:
    """The same simulated wake, against the same bout: 2411.0 ms becomes 1.0 ms."""

    engine, adapter, arm, contract = await armed_against_a_suspending_endpoint()

    result = await engine.run(arm, initial_update(contract))

    assert result.initial.application_read_elapsed_ms == pytest.approx(
        APPLICATION_READ_MS
    )
    assert result.initial.application_read_elapsed_ms == pytest.approx(1.0)
    assert result.initial.managed_availability_ms == pytest.approx(SYNC_LAG_MS)
    # The wake was genuinely paid, just before the clock rather than inside it.
    assert not adapter.suspended


async def test_a_denied_warm_up_still_cannot_corrupt_the_headline_number() -> None:
    """Deny the warm-up its connection and the wake goes back into the secondary.

    This is the corruption the warm-up exists to prevent, so running the bout
    straight through it is the strongest available evidence that the primary
    metric -- two Delta timestamps with no Postgres in between -- is immune by
    construction rather than by luck.
    """

    engine, adapter, arm, contract = await armed_against_a_suspending_endpoint(
        deny_warm_up=True
    )

    result = await engine.run(arm, initial_update(contract))

    assert adapter.denied_warm_ups == 1
    # The wake landed in the measured read, exactly as it did before the fix.
    assert result.initial.application_read_elapsed_ms == pytest.approx(2411.0)
    # And the headline did not move by a microsecond.
    assert result.initial.managed_availability_ms == pytest.approx(SYNC_LAG_MS)
    assert result.initial.managed_availability_ms == pytest.approx(
        (result.initial.sync_end_time - result.initial.delta_commit_time).total_seconds()
        * 1000
    )
    # A failed warm-up is not a failed bout.
    assert result.initial.verified_row == initial_update(contract).row


async def test_the_endpoint_warm_up_writes_nothing_and_spends_no_delta_version() -> None:
    """It is a throwaway read, so it leaves nothing for settlement to undo."""

    engine, adapter, arm, contract = await armed_against_a_suspending_endpoint()
    version_at_the_bell = adapter.version

    result = await engine.run(arm, initial_update(contract))

    # Two reads to arm, one to warm the endpoint, one measured.
    assert adapter.application_reads == 4
    # Arming's throwaway baseline round trip, then the bout's own proof. The
    # endpoint warm-up added neither a commit nor a Delta version.
    assert adapter.committed_updates == [
        baseline_update(contract),
        initial_update(contract),
    ]
    assert adapter.version == version_at_the_bell + 1
    assert result.initial.source_version == adapter.version


async def test_the_endpoint_warm_up_never_moves_the_on_screen_clock() -> None:
    """PREFLIGHT with a null elapsed, before the bout ever speaks in its own voice."""

    engine, adapter, arm, contract = await armed_against_a_suspending_endpoint()
    seen: list[ModelScoreProgress] = []

    async def record(progress: ModelScoreProgress) -> None:
        seen.append(progress)

    await engine.run(arm, initial_update(contract), record)

    assert seen[0].phase == ModelScorePhase.PREFLIGHT
    assert seen[0].elapsed_ms is None
    assert seen[0].attempt is None
    # The manager maps PREFLIGHT to a sealed lane, so this has to come before
    # the bout starts reporting, not in the middle of it.
    phases = [progress.phase for progress in seen]
    assert phases.index(ModelScorePhase.PREFLIGHT) < phases.index(
        ModelScorePhase.COMMITTING_SOURCE
    )
    assert phases.count(ModelScorePhase.PREFLIGHT) == 1
    # No Delta version leaks into the warm-up's on-screen text.
    assert "version" not in seen[0].status.casefold()


async def test_the_redo_is_warmed_too_because_it_is_measured_the_same_way() -> None:
    """The redo publishes the same secondary metric, so it needs the same guard."""

    engine, adapter, arm, contract = await armed_against_a_suspending_endpoint()
    result = await engine.run(arm, initial_update(contract))
    assert result.initial.application_read_elapsed_ms == pytest.approx(1.0)

    # Nothing touches Postgres between the two bouts either.
    adapter.suspend()
    redone = await engine.redo(
        arm,
        result,
        ModelScoreUpdate(
            entity_id=contract.entity_id,
            score=0.33,
            model_version="risk-v2",
            proof_nonce="round4-v2-001",
        ),
    )

    assert redone.redo is not None
    assert redone.redo.application_read_elapsed_ms == pytest.approx(1.0)
    assert redone.redo.managed_availability_ms == pytest.approx(SYNC_LAG_MS)
