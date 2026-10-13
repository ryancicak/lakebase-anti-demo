"""CLEANING with a NULL bell must reconstruct the journal debt a precommit left.

A bell refused after its CreateDBProxy precommit leaves a durable CREATE_INTENT row
behind. The no-bell cleanup used to refuse that debt on every retry and stay CLEANING.
These tests drive the real provider / engine / orchestrator / coordinator path.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server.connection_spike_journal import CreationScope, JournalEvent, ResourceSpec
from server.connection_spike_live import (
    ConnectionSpikeCleanupError,
    ConnectionSpikeWarmSetupContext,
    LiveConnectionSpikeEngine,
    LiveRound5WarmProvider,
)
from server.round5_warm import (
    BlockedWarmError,
    InMemoryRound5WarmStore,
    RetryableWarmError,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmState,
    WarmBellMarginError,
    WarmFenceLostError,
)
from tests.test_round5_bell_margin import lakebase_dispatch_for
from tests.test_round5_chaos_stabilization import _absence_orchestrator
from tests.test_round5_warm import DIGEST, Clock, Provider, coordinator, warm_ready

INSTALLATION = "install-one"


class _SettledLane:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    async def cancel_job(self, job_id: str) -> None:
        self.cancelled.append(job_id)

    def settlement_pending(self, _run_id: str) -> bool:
        return False


class _JanitorThenWarmProvider:
    """Live no-bell janitor with a fake warm so coordinator tests never prepare on AWS."""

    def __init__(self, clock: Clock, live: LiveRound5WarmProvider) -> None:
        self._live = live
        self._warm = Provider(clock)
        self._warm.release_prepare.set()

    @property
    def authority_guard(self):
        return getattr(self._live, "authority_guard", None)

    @authority_guard.setter
    def authority_guard(self, value) -> None:
        self._live.authority_guard = value

    async def reconcile(self, slot):
        return await self._live.reconcile(slot)

    async def prepare(self, **kwargs):
        return await self._warm.prepare(**kwargs)

    async def validate_ready(self, slot, launch_capsule):
        return await self._warm.validate_ready(slot, launch_capsule)

    async def refresh_capsule(self, slot, previous):
        return await self._warm.refresh_capsule(slot, previous)

    async def refresh_preparation(self, slot, previous):
        return await self._warm.refresh_preparation(slot, previous)

    def adopt_claimed_engine(self, claim_id, engine) -> None:
        self._live.adopt_claimed_engine(claim_id, engine)

    def release_adopted_engine(self, claim_id) -> None:
        self._live.release_adopted_engine(claim_id)


def _instrument_orchestrator(orchestrator):
    reconstruct_calls: list[str] = []
    prove_calls: list[str] = []
    original_reconstruct = orchestrator.reconcile_failed_cleanup
    original_prove = orchestrator.prove_bout_absent

    async def reconstruct(bout_id, token, **kwargs):
        reconstruct_calls.append(str(bout_id))
        return await original_reconstruct(bout_id, token, **kwargs)

    async def prove(bout_id):
        prove_calls.append(str(bout_id))
        return await original_prove(bout_id)

    orchestrator.reconcile_failed_cleanup = reconstruct
    orchestrator.prove_bout_absent = prove
    return reconstruct_calls, prove_calls


async def _orphan_intent_fixture(bout_id: str, fence: int):
    orchestrator, journal, rds, _ec2 = _absence_orchestrator()
    reconstruct_calls, prove_calls = _instrument_orchestrator(orchestrator)
    config = orchestrator.config
    names = orchestrator.names_for_bout(
        config.deterministic_name_prefix, bout_id, config.secret_name_prefix or "anti-demo-round5"
    )
    tags = dict(config.ownership_tags)
    tags.update(
        {
            "anti-demo-bout-id": bout_id,
            "anti-demo:bout-token": names.token,
            "anti-demo:bout-fence": str(fence),
        }
    )
    scope = CreationScope(bout_id, fence, config.baseline_sha256)
    spec = ResourceSpec(
        ordinal=1,
        resource_kind="rds_proxy",
        deterministic_name=names.proxy_name,
        metadata={
            "tags": tags,
            "baseline_sha256": config.baseline_sha256,
            "competitor_id": config.competitor_id,
            "competitor_target_id": config.competitor_target_id,
            "competitor_resource_id": config.competitor_resource_id,
        },
    )
    intent = JournalEvent.creation_intent(scope, spec, now=datetime(2026, 10, 7, tzinfo=UTC))
    await journal.commit(intent, authority_scope=scope)
    lakebase, competitor = _SettledLane(), _SettledLane()
    engine = LiveConnectionSpikeEngine(
        lakebase,
        lane_adapters={"lakebase": lakebase, "competitor": competitor},
        setup_orchestrator=orchestrator,
    )
    return (
        orchestrator,
        journal,
        rds,
        engine,
        reconstruct_calls,
        prove_calls,
        lakebase,
        competitor,
    )


def _engine_factory(orchestrator):
    def factory(_competitor):
        lakebase, competitor = _SettledLane(), _SettledLane()
        return LiveConnectionSpikeEngine(
            lakebase,
            lane_adapters={"lakebase": lakebase, "competitor": competitor},
            setup_orchestrator=orchestrator,
        )

    return factory


def _cleaning_slot(claim):
    return SimpleNamespace(
        state=Round5WarmState.CLEANING,
        claim=claim,
        bell_id=None,
        bell_at_utc=None,
        cleaned_bout_id=None,
        requires_cleaned_bout=False,
    )


def _claim(bout_id: str, fence: int, *, claim_id: str = "claim-orphan"):
    return SimpleNamespace(
        claim_id=claim_id,
        bout_id=bout_id,
        lakebase_job_id="1" * 64,
        competitor_job_id="2" * 64,
        capsule_generation=3,
        warm_attempt_token="attempt-x",
        selected_variant=Round5Variant.AURORA,
        bout_fence=fence,
    )


def _live_provider(engine, *, claim_id: str, factory=None):
    provider = LiveRound5WarmProvider(
        SimpleNamespace(),
        factory or (lambda _competitor: (_ for _ in ()).throw(AssertionError("adopted engine"))),
    )
    provider.adopt_claimed_engine(claim_id, engine)
    return provider


async def test_cleaning_null_bell_with_create_intent_reconstructs_instead_of_blocking() -> None:
    """No-bell CLEANING with a precommitted CREATE_INTENT must reconstruct."""

    bout_id, fence = "bout-orphan-intent", 11
    (
        _orchestrator,
        journal,
        rds,
        engine,
        reconstruct_calls,
        prove_calls,
        lakebase,
        competitor,
    ) = await _orphan_intent_fixture(bout_id, fence)
    assert bout_id in await journal.unresolved_bout_ids()
    claim = _claim(bout_id, fence)
    provider = _live_provider(engine, claim_id=claim.claim_id)
    cleaning = _cleaning_slot(claim)

    assert await provider.reconcile(cleaning) is True

    assert await journal.unresolved_bout_ids() == ()
    assert rds.proxies == {}
    assert reconstruct_calls == [bout_id]
    assert prove_calls == [bout_id]
    assert lakebase.cancelled == [claim.lakebase_job_id]
    assert competitor.cancelled == [claim.competitor_job_id]


async def test_cleanup_convergence_progresses_instead_of_repeating_journal_debt() -> None:
    """Coordinator cycles must change state rather than loop the same block."""

    bout_id, fence = "bout-orphan-loop", 7
    clock = Clock()
    fake = Provider(clock)
    manager = coordinator(clock, fake)
    await warm_ready(manager, fake)
    claimed, _capsule = await manager.claim(
        session_id="session-orphan",
        bout_id=bout_id,
        selected_variant=Round5Variant.AURORA,
        bout_fence=fence,
    )
    assert claimed.claim is not None
    cleaning = await manager.begin_cleanup(claimed.claim.claim_id)
    assert cleaning.state == Round5WarmState.CLEANING and cleaning.bell_id is None
    generation = cleaning.generation
    (
        orchestrator,
        journal,
        _rds,
        engine,
        reconstruct_calls,
        prove_calls,
        _lakebase,
        _competitor,
    ) = await _orphan_intent_fixture(bout_id, fence)
    live = LiveRound5WarmProvider(SimpleNamespace(), _engine_factory(orchestrator))
    live.adopt_claimed_engine(claimed.claim.claim_id, engine)
    hybrid = _JanitorThenWarmProvider(clock, live)
    hybrid.authority_guard = manager._authority_guard
    manager.provider = hybrid
    error_codes: list[str | None] = []
    for _ in range(10):
        clock.advance(61)
        await manager.run_one_cycle()
        slot = await manager.store.read(INSTALLATION)
        assert slot is not None
        error_codes.append(slot.last_error_code)

    slot = await manager.store.read(INSTALLATION)
    assert slot is not None
    assert slot.state != Round5WarmState.CLEANING
    assert "cleanup_reconcile_blocked" not in error_codes
    assert await journal.unresolved_bout_ids() == ()
    assert reconstruct_calls
    assert prove_calls
    assert slot.generation != generation or slot.claim is None


async def test_restarted_provider_recovers_durable_cleaning_null_bell_journal_debt() -> None:
    """Rebuilding the coordinator/provider over the same durable state recovers."""

    bout_id, fence = "bout-orphan-restart", 13
    clock = Clock()
    store = InMemoryRound5WarmStore()
    fake = Provider(clock)
    original = coordinator(clock, fake, store, process_epoch="process-one")
    await warm_ready(original, fake)
    claimed, _capsule = await original.claim(
        session_id="session-restart",
        bout_id=bout_id,
        selected_variant=Round5Variant.AURORA,
        bout_fence=fence,
    )
    assert claimed.claim is not None
    cleaning = await original.begin_cleanup(claimed.claim.claim_id)
    assert cleaning.state == Round5WarmState.CLEANING and cleaning.bell_id is None
    (
        orchestrator,
        journal,
        _rds,
        _engine,
        reconstruct_calls,
        prove_calls,
        _lakebase,
        _competitor,
    ) = await _orphan_intent_fixture(bout_id, fence)
    assert bout_id in await journal.unresolved_bout_ids()
    await original.close()
    clock.advance(91)

    live = LiveRound5WarmProvider(SimpleNamespace(), _engine_factory(orchestrator))
    hybrid = _JanitorThenWarmProvider(clock, live)
    restarted = Round5WarmCoordinator(
        installation_id=INSTALLATION,
        warm_contract_sha256=DIGEST,
        store=store,
        provider=hybrid,
        process_epoch="process-two",
        broker_epoch="broker-two",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await restarted.run_one_cycle()
    slot = await store.read(INSTALLATION)
    assert slot is not None
    assert slot.state != Round5WarmState.CLEANING
    assert slot.last_error_code != "cleanup_reconcile_blocked"
    assert await journal.unresolved_bout_ids() == ()
    assert reconstruct_calls == [bout_id]
    assert prove_calls == [bout_id]


async def test_empty_journal_no_bell_claim_proves_absence_without_reconstruct() -> None:
    """Control: the empty-journal no-bell path still settles via prove_bout_absent."""

    bout_id, fence = "bout-abandoned-empty", 5
    orchestrator, journal, rds, _ec2 = _absence_orchestrator()
    reconstruct_calls, prove_calls = _instrument_orchestrator(orchestrator)
    lakebase, competitor = _SettledLane(), _SettledLane()
    engine = LiveConnectionSpikeEngine(
        lakebase,
        lane_adapters={"lakebase": lakebase, "competitor": competitor},
        setup_orchestrator=orchestrator,
    )
    claim = _claim(bout_id, fence, claim_id="claim-empty")
    provider = _live_provider(engine, claim_id=claim.claim_id)
    assert await provider.reconcile(_cleaning_slot(claim)) is True
    assert reconstruct_calls == []
    assert prove_calls == [bout_id]
    assert await journal.unresolved_bout_ids() == ()
    assert rds.proxies == {}
    assert lakebase.cancelled == [claim.lakebase_job_id]


async def test_real_cleanup_failure_stays_cleaning_and_retries() -> None:
    """Control: reconstructive cleanup that cannot delete keeps CLEANING and retries."""

    bout_id, fence = "bout-cleanup-fails", 17
    (
        orchestrator,
        journal,
        rds,
        engine,
        reconstruct_calls,
        _prove_calls,
        _lakebase,
        _competitor,
    ) = await _orphan_intent_fixture(bout_id, fence)
    names = orchestrator.names_for_bout(
        orchestrator.config.deterministic_name_prefix,
        bout_id,
        orchestrator.config.secret_name_prefix or "anti-demo-round5",
    )
    tags = dict(orchestrator.config.ownership_tags)
    tags.update(
        {
            "anti-demo-bout-id": bout_id,
            "anti-demo:bout-token": names.token,
            "anti-demo:bout-fence": str(fence),
        }
    )
    rds.proxies[names.proxy_name] = {
        "DBProxyName": names.proxy_name,
        "DBProxyArn": "arn:aws:rds:us-west-2:123456789012:db-proxy:prx-fail",
        "Tags": [{"Key": key, "Value": value} for key, value in sorted(tags.items())],
    }

    def refuse_delete(**_kwargs):
        raise RuntimeError("DeleteDBProxy refused")

    rds.delete_db_proxy = refuse_delete
    claim = _claim(bout_id, fence, claim_id="claim-fail")
    provider = _live_provider(engine, claim_id=claim.claim_id)
    cleaning = _cleaning_slot(claim)

    with pytest.raises((BlockedWarmError, RetryableWarmError)) as blocked:
        await provider.reconcile(cleaning)
    assert blocked.value.code in {"cleanup_reconcile_blocked", "cleanup_reconcile_retryable"}
    assert bout_id in await journal.unresolved_bout_ids()
    assert names.proxy_name in rds.proxies
    assert reconstruct_calls == [bout_id]

    with pytest.raises((BlockedWarmError, RetryableWarmError)):
        await provider.reconcile(cleaning)
    assert bout_id in await journal.unresolved_bout_ids()
    assert reconstruct_calls == [bout_id, bout_id]


async def test_unresolved_journal_debt_still_blocks_absence_proof() -> None:
    """Control: unresolved journal debt that reconstruct cannot seal still blocks."""

    bout_id, fence = "bout-unsafe-debt", 19
    orchestrator, journal, rds, _ec2 = _absence_orchestrator()
    _reconstruct_calls, prove_calls = _instrument_orchestrator(orchestrator)
    journal.unresolved.add(bout_id)
    lakebase, competitor = _SettledLane(), _SettledLane()
    engine = LiveConnectionSpikeEngine(
        lakebase,
        lane_adapters={"lakebase": lakebase, "competitor": competitor},
        setup_orchestrator=orchestrator,
    )
    claim = _claim(bout_id, fence, claim_id="claim-unsafe")
    provider = _live_provider(engine, claim_id=claim.claim_id)

    with pytest.raises(BlockedWarmError) as blocked:
        await provider.reconcile(_cleaning_slot(claim))
    assert blocked.value.code == "cleanup_reconcile_blocked"
    cause = blocked.value.__cause__
    assert isinstance(cause, ConnectionSpikeCleanupError)
    assert cause.reason_code == "current_bout_journal_debt"
    assert bout_id in await journal.unresolved_bout_ids()
    assert rds.delete_proxy_calls == []
    assert prove_calls[-1] == bout_id
    assert cause.stage == "journal_ownership"


async def _no_bell_live_engine(orchestrator):
    lakebase, competitor = _SettledLane(), _SettledLane()
    engine = LiveConnectionSpikeEngine(
        lakebase,
        lane_adapters={"lakebase": lakebase, "competitor": competitor},
        setup_orchestrator=orchestrator,
    )
    return engine


@pytest.mark.parametrize(
    "transient",
    [TimeoutError("journal read timed out"), ConnectionError("journal closed")],
)
async def test_wrapped_journal_ownership_timeout_classifies_retryable(
    transient: BaseException,
) -> None:
    """A TimeoutError/ConnectionError under journal_ownership_unavailable retries."""

    bout_id, fence = "bout-journal-timeout", 23
    orchestrator, _journal, _rds, _ec2 = _absence_orchestrator()

    async def boom() -> tuple[str, ...]:
        raise transient

    orchestrator.unresolved_bout_ids = boom
    claim = _claim(bout_id, fence, claim_id="claim-timeout")
    provider = _live_provider(await _no_bell_live_engine(orchestrator), claim_id=claim.claim_id)

    with pytest.raises(RetryableWarmError) as retryable:
        await provider.reconcile(_cleaning_slot(claim))
    assert retryable.value.code == "cleanup_reconcile_retryable"
    wrapped = retryable.value.__cause__
    assert isinstance(wrapped, ConnectionSpikeCleanupError)
    assert wrapped.reason_code == "journal_ownership_unavailable"
    assert wrapped.__cause__ is transient


async def test_wrapped_journal_ownership_corruption_stays_blocked() -> None:
    bout_id, fence = "bout-journal-corrupt", 29
    orchestrator, _journal, _rds, _ec2 = _absence_orchestrator()

    async def boom() -> tuple[str, ...]:
        raise ValueError("journal row is corrupt")

    orchestrator.unresolved_bout_ids = boom
    claim = _claim(bout_id, fence, claim_id="claim-corrupt")
    provider = _live_provider(await _no_bell_live_engine(orchestrator), claim_id=claim.claim_id)

    with pytest.raises(BlockedWarmError) as blocked:
        await provider.reconcile(_cleaning_slot(claim))
    assert blocked.value.code == "cleanup_reconcile_blocked"
    wrapped = blocked.value.__cause__
    assert isinstance(wrapped, ConnectionSpikeCleanupError)
    assert wrapped.reason_code == "journal_ownership_unavailable"
    assert isinstance(wrapped.__cause__, ValueError)


async def test_prove_bout_absent_wrapped_timeout_is_retryable() -> None:
    """Existing prove_bout_absent journal wrap must classify the same way."""

    bout_id, fence = "bout-prove-timeout", 31
    orchestrator, journal, _rds, _ec2 = _absence_orchestrator()

    async def empty() -> tuple[str, ...]:
        return ()

    async def boom() -> tuple[str, ...]:
        raise TimeoutError("prove journal read timed out")

    orchestrator.unresolved_bout_ids = empty
    journal.unresolved_bout_ids = boom
    claim = _claim(bout_id, fence, claim_id="claim-prove-timeout")
    provider = _live_provider(await _no_bell_live_engine(orchestrator), claim_id=claim.claim_id)

    with pytest.raises(RetryableWarmError) as retryable:
        await provider.reconcile(_cleaning_slot(claim))
    assert retryable.value.code == "cleanup_reconcile_retryable"
    wrapped = retryable.value.__cause__
    assert isinstance(wrapped, ConnectionSpikeCleanupError)
    assert wrapped.reason_code == "journal_ownership_unavailable"
    assert isinstance(wrapped.__cause__, TimeoutError)


async def test_cleanup_fence_loss_is_not_reclassified() -> None:
    bout_id, fence = "bout-fence-lost", 37
    orchestrator, _journal, _rds, _ec2 = _absence_orchestrator()
    claim = _claim(bout_id, fence, claim_id="claim-fence")
    provider = _live_provider(await _no_bell_live_engine(orchestrator), claim_id=claim.claim_id)

    async def lost() -> None:
        raise WarmFenceLostError("coordinator authority moved before journal read")

    provider.authority_guard = lost
    with pytest.raises(WarmFenceLostError, match="authority moved"):
        await provider.reconcile(_cleaning_slot(claim))


async def test_coordinator_retries_wrapped_journal_timeout_while_cleaning() -> None:
    bout_id, fence = "bout-coord-timeout", 41
    clock = Clock()
    fake = Provider(clock)
    manager = coordinator(clock, fake)
    await warm_ready(manager, fake)
    claimed, _capsule = await manager.claim(
        session_id="session-timeout",
        bout_id=bout_id,
        selected_variant=Round5Variant.AURORA,
        bout_fence=fence,
    )
    assert claimed.claim is not None
    cleaning = await manager.begin_cleanup(claimed.claim.claim_id)
    assert cleaning.state == Round5WarmState.CLEANING
    generation = cleaning.generation
    orchestrator, _journal, _rds, _ec2 = _absence_orchestrator()

    async def boom() -> tuple[str, ...]:
        raise TimeoutError("journal read timed out")

    orchestrator.unresolved_bout_ids = boom
    live = LiveRound5WarmProvider(SimpleNamespace(), _engine_factory(orchestrator))
    live.adopt_claimed_engine(claimed.claim.claim_id, await _no_bell_live_engine(orchestrator))
    hybrid = _JanitorThenWarmProvider(clock, live)
    hybrid.authority_guard = manager._authority_guard
    manager.provider = hybrid
    clock.advance(61)
    await manager.run_one_cycle()
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None
    assert slot.state == Round5WarmState.CLEANING
    assert slot.generation == generation
    assert slot.last_error_code == "cleanup_reconcile_retryable"
    assert slot.next_retry_at is not None
    clock.advance(61)
    await manager.run_one_cycle()
    again = await manager.store.read(INSTALLATION)
    assert again is not None
    assert again.state == Round5WarmState.CLEANING
    assert again.last_error_code == "cleanup_reconcile_retryable"


async def test_an_orchestrator_that_cannot_report_journal_ownership_fails_closed() -> None:
    bout_id, fence = "bout-no-ownership-read", 43
    proved: list[str] = []

    class OwnershipBlindOrchestrator:
        async def prove_bout_absent(self, bout_id: str) -> None:
            proved.append(bout_id)

    claim = _claim(bout_id, fence, claim_id="claim-no-ownership-read")
    engine = await _no_bell_live_engine(OwnershipBlindOrchestrator())
    with pytest.raises(ConnectionSpikeCleanupError) as refused:
        await engine.reconcile_abandoned_claim(claim)
    assert refused.value.reason_code == "journal_ownership_unavailable"
    assert proved == []


async def test_a_bell_refused_after_its_real_precommit_converges_to_a_fresh_generation() -> None:
    # The pre-bell check passes with 31 s of launch life, the CreateDBProxy precommit then
    # takes 35 s, and the bell is refused: CLEANING, a NULL bell and a CREATE_INTENT row.
    bout_id, fence = "bout-margin-after-precommit", 9
    clock = Clock()
    fake = Provider(clock)
    fake.preparation_factory = lakebase_dispatch_for(871)
    manager = coordinator(clock, fake)
    await warm_ready(manager, fake)
    claim_at = clock.now
    claimed, _capsule = await manager.claim(
        session_id="session-margin-after-precommit",
        bout_id=bout_id,
        selected_variant=Round5Variant.AURORA,
        bout_fence=fence,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    manager.mark_claim_active(claim_id, armed_deadline=claim_at + timedelta(seconds=240))
    orchestrator, journal, rds, ec2 = _absence_orchestrator()
    reconstruct_calls, prove_calls = _instrument_orchestrator(orchestrator)
    orchestrator._warm_context = ConnectionSpikeWarmSetupContext(
        clients=SimpleNamespace(
            rds=rds,
            ec2=ec2,
            expires_at=datetime.now(UTC) + timedelta(hours=2),
        ),
        rds_security_group_id="sg-rds",
        proxy_security_group_id="sg-proxy",
        observed_at=datetime.now(UTC),
    )
    await orchestrator.prepare(bout_id, fence)
    engine = await _no_bell_live_engine(orchestrator)
    for _ in range(12):
        clock.advance(10)
        await manager.run_one_cycle()

    manager.require_dispatchable(claimed.claim)
    await engine.precommit_launch_intent(bout_id, fence)
    assert bout_id in await journal.unresolved_bout_ids()
    clock.advance(35)
    with pytest.raises(WarmBellMarginError):
        await manager.accept_bell(claim_id)

    cleaning = await manager.begin_cleanup(claim_id)
    assert cleaning.state == Round5WarmState.CLEANING
    assert cleaning.bell_id is None and cleaning.bell_at_utc is None
    live = LiveRound5WarmProvider(SimpleNamespace(), _engine_factory(orchestrator))
    live.adopt_claimed_engine(claim_id, engine)
    hybrid = _JanitorThenWarmProvider(clock, live)
    hybrid.authority_guard = manager._authority_guard
    manager.provider = hybrid
    trajectory: list[tuple[Round5WarmState, str | None]] = []
    slot = cleaning
    for _ in range(12):
        clock.advance(61)
        await manager.run_one_cycle()
        slot = await manager.store.read(INSTALLATION)
        assert slot is not None
        trajectory.append((slot.state, slot.last_error_code))
        if slot.state == Round5WarmState.READY:
            break

    assert all(code != "cleanup_reconcile_blocked" for _state, code in trajectory)
    assert slot.state == Round5WarmState.READY, trajectory
    assert slot.generation > claimed.generation
    assert await journal.unresolved_bout_ids() == ()
    assert rds.proxies == {}
    assert reconstruct_calls == [bout_id]
    assert prove_calls == [bout_id]
