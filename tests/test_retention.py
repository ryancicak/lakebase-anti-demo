"""Bounded retention for the in-memory session store and the per-session event log.

The defect these pin, in one sentence: ``RunManager._records`` was written on every
session create and never read back out, so an installation meant to run unattended
grew by roughly a megabyte per bout attempt -- abandoned attempts included -- until
the process was restarted.

Two properties are load-bearing here and are worth more than the memory they save.

1. A record may only be released once it can be *proved* to own nothing: no
   background task, no ring lease, no open cost window. Releasing a record whose
   settlement is still running would orphan the task -- still holding the record,
   no longer reachable from ``close()`` -- and trade a bounded leak for an
   unbounded one.

2. Capping the event log must not renumber it. Sequence numbers used to be derived
   from ``len(events)``, and a consumer resumes by sequence, so dropping early
   events without a retained base offset would hand a resuming client a window
   that looks contiguous and is not. That is a corrupted play-by-play, which on a
   live stage is a far worse failure than the leak.
"""

from __future__ import annotations

import asyncio
import time

from server.manager import EventLog, RunManager
from server.models import (
    BoutOperator,
    CompetitorId,
    Corner,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.receipts import load_receipts


def _card(round_id: RoundId = RoundId.WAKE_IDLE_APP) -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="data_analyst",
        corners=[Corner.PERFORMANCE],
        round_id=round_id,
    )


def _immediate_release(manager: RunManager) -> None:
    """Retention with the clock taken out of it.

    The window is what a live installation tunes; every test below is about *which*
    records may go, not when, so it is collapsed to a nanosecond and the floor is
    set to the single record a sweep must never take on age alone -- the newest.
    """
    manager._session_retention_seconds = 1e-9
    manager._session_retention_floor = 1


async def _next_attempt(manager: RunManager) -> str:
    """A fresh attempt, so the record under test stops being the newest one.

    Every release below is staged the way it happens live: the operator starts the
    next bout, and the settled record behind it goes. The newest record is floor-
    protected on purpose, because it is the one a browser is most likely still
    holding, so nothing can be released while it is the only thing in the store.
    """
    return (await manager.create(_card())).id


async def _sweep(manager: RunManager) -> None:
    """Run one retention pass without creating another session to trigger it."""
    async with manager._records_lock:
        manager._release_settled_records(time.monotonic_ns())


# --------------------------------------------------------------------------- #
# What may be released, and what may not
# --------------------------------------------------------------------------- #


async def test_a_record_owning_nothing_is_released_whatever_state_it_stopped_in() -> None:
    """Both ends of the state range, because the policy is not keyed on state.

    A fight card is created per bout attempt, and an abandoned one never reaches
    a terminal state at all: it sits in ``draft`` with no task and no lease
    forever. A policy keyed on terminal states would have left exactly the
    records the leak was made of, so the draft row is the one that matters and
    the verified row is what stops the rule being written backwards.
    """

    cases: tuple[tuple[str, SessionState], ...] = (
        ("a settled terminal record", SessionState.VERIFIED),
        ("an abandoned draft", SessionState.DRAFT),
    )

    for name, state in cases:
        manager = RunManager()
        _immediate_release(manager)
        created = await manager.create(_card())
        manager._records[created.id].snapshot.state = state
        assert manager._records[created.id].snapshot.state == state, name

        kept = await _next_attempt(manager)

        assert created.id not in manager._records, name
        # The record that displaced it is floor-protected, so a sweep that took
        # everything would pass the assertion above for the wrong reason.
        assert kept in manager._records, name
        await manager.close()


async def test_a_record_with_pending_settlement_is_retained() -> None:
    """Settlement runs after the bout is terminal and holds the record it settles."""
    manager = RunManager()
    _immediate_release(manager)
    created = await manager.create(_card())
    record = manager._records[created.id]
    record.snapshot.state = SessionState.VERIFIED

    entered = asyncio.Event()
    release = asyncio.Event()

    async def settle() -> None:
        entered.set()
        await release.wait()

    manager._schedule_round_settlement(record, settle, label="Round 6")
    await asyncio.wait_for(entered.wait(), timeout=1)

    await _next_attempt(manager)
    assert created.id in manager._records, "a settling record must not be released"

    # And it becomes releasable the moment its settlement finishes, so a stuck
    # settlement postpones the release rather than cancelling it.
    release.set()
    task = record.settlement_task
    assert task is not None
    await asyncio.wait_for(asyncio.shield(task), timeout=1)
    await _sweep(manager)
    assert created.id not in manager._records
    await manager.close()


async def test_what_pins_a_record_against_release() -> None:
    """Three kinds of ownership, each of which outranks the retention window.

    The rows share the shape that matters: pin the record, let the next attempt
    arrive so age and floor no longer protect it, and require it to still be
    there. Where the pin can be let go the row goes on to require the record to
    become releasable immediately -- so a pin postpones a release rather than
    cancelling it, which is the difference between a bounded leak and a record
    nothing will ever collect.

    A running bout has no such second half: it is pinned by *being* running, and
    the only way to stop that is to stop the bout.
    """

    async def pin_running(manager: RunManager, record) -> None:
        record.snapshot.state = SessionState.RUNNING
        record.task = asyncio.create_task(asyncio.Event().wait())

    async def unpin_running(manager: RunManager, record) -> None:
        record.task.cancel()

    async def pin_ring_lease(manager: RunManager, record) -> None:
        # A held lease is authority over real cloud resources, terminal state or
        # not, so a FAILED record still holding one may not be forgotten.
        record.snapshot.state = SessionState.FAILED
        await manager._claim_bout(record, BoutOperator(display_name="Operator"))

    async def unpin_ring_lease(manager: RunManager, record) -> None:
        assert await manager._release_bout(record)

    async def pin_cost_window(manager: RunManager, record) -> None:
        record.snapshot.state = SessionState.VERIFIED
        record.cost_bout_id = "open-window"

    async def unpin_cost_window(manager: RunManager, record) -> None:
        record.cost_bout_id = None

    cases = (
        ("a running bout", pin_running, unpin_running, False),
        ("a held ring lease", pin_ring_lease, unpin_ring_lease, True),
        ("an open cost window", pin_cost_window, unpin_cost_window, True),
    )

    for name, pin, unpin, releasable_once_unpinned in cases:
        manager = RunManager()
        _immediate_release(manager)
        created = await manager.create(_card())
        record = manager._records[created.id]
        await pin(manager, record)

        await _next_attempt(manager)
        assert created.id in manager._records, name

        await unpin(manager, record)
        if releasable_once_unpinned:
            await _sweep(manager)
            assert created.id not in manager._records, name
        await manager.close()


async def test_the_newest_records_survive_the_idle_window() -> None:
    """Age alone may not evict.

    Releasing a record the browser is still holding costs the operator the screen
    they are presenting from: the next reconcile 404s and the UI drops back to
    setup. The floor is what keeps that from being an idle timer's decision.
    """
    manager = RunManager()
    manager._session_retention_seconds = 1e-9
    manager._session_retention_floor = 3
    ids = [(await manager.create(_card())).id for _ in range(6)]

    await _sweep(manager)

    assert [session_id in manager._records for session_id in ids] == [
        False,
        False,
        False,
        True,
        True,
        True,
    ]
    await manager.close()


# --------------------------------------------------------------------------- #
# The bound itself
# --------------------------------------------------------------------------- #


async def test_many_abandoned_attempts_do_not_grow_the_store() -> None:
    """The leak, driven hard enough to show: 200 attempts, bounded retention."""
    manager = RunManager()
    manager._session_retention_max = 8
    manager._session_retention_floor = 4
    # The window is left at its default so this proves the *cap*, not the sweep:
    # nothing here is old enough to age out.
    high_water = 0
    for _ in range(200):
        await manager.create(_card())
        high_water = max(high_water, len(manager._records))

    assert high_water <= 8, "the cap is the hard bound, whatever the arrival rate"
    assert len(manager._records) == 8
    await manager.close()


async def test_total_retained_events_stay_bounded_across_many_bouts() -> None:
    """The memory bound, deterministically: records x events, not RSS.

    Every snapshot-bearing event holds a serialised session, so the retained event
    count is the thing footprint is proportional to. Driving the same publish
    pattern a bout uses shows it flat: 40 bouts of 20 events each retain no more
    than the cap allows, where before the store kept all 800.
    """
    manager = RunManager()
    manager._session_retention_max = 6
    manager._session_retention_floor = 2
    published = 0
    for _ in range(40):
        created = await manager.create(_card())
        record = manager._records[created.id]
        record.event_log.limit = 10
        for index in range(20):
            await record.event_log.publish("lane_update", {"index": index})
            published += 1
        record.snapshot.state = SessionState.VERIFIED

    retained = sum(len(record.event_log.events) for record in manager._records.values())
    assert published == 800
    assert len(manager._records) <= 6
    assert retained <= 6 * 10, "records x events is the whole bound"
    # And the sequences of what is retained still describe where they came from.
    for record in manager._records.values():
        assert record.event_log.next_sequence == 22
        assert record.event_log.first_retained_sequence == 12
    await manager.close()


async def test_the_cap_never_releases_a_record_that_owns_work() -> None:
    """Capacity pressure is not a licence to forget an unfinished bout."""
    manager = RunManager()
    manager._session_retention_max = 4
    manager._session_retention_floor = 1
    busy = await manager.create(_card())
    record = manager._records[busy.id]
    record.task = asyncio.create_task(asyncio.Event().wait())

    for _ in range(40):
        await manager.create(_card())

    assert busy.id in manager._records
    # The cap counts the record it cannot release, so the store lands *on* the cap
    # with three releasable records beside the busy one rather than one over it.
    assert len(manager._records) == 4
    record.task.cancel()
    await manager.close()


async def test_the_cap_does_not_drop_the_newest_records_to_get_under_it() -> None:
    """The floor binds capacity pressure too, not only the idle window.

    This is the shape the floor was built for, and the one the cap used to break:
    the store is over its cap, every older record is pinned by unfinished work, so
    the only records that *can* be released are the newest few -- the ones a
    browser is reconciling against. Releasing those 404s the screen the operator is
    presenting from, which is the exact failure the floor exists to prevent.

    Staying over the cap is the better of the two failures, and it is reported: the
    sweep warns that none of the excess could be proved settled.
    """
    manager = RunManager()
    manager._session_retention_max = 4
    manager._session_retention_floor = 2
    # The window is left alone: this is about the cap, not about age.

    pinned: list[str] = []
    tasks: list[asyncio.Task[None]] = []
    for _ in range(5):
        created = await manager.create(_card())
        record = manager._records[created.id]
        record.task = asyncio.create_task(asyncio.Event().wait())
        tasks.append(record.task)
        pinned.append(created.id)

    # Two releasable records, arriving last, so they are the two the floor covers
    # and simultaneously the only two the sweep could take.
    newest = [(await manager.create(_card())).id for _ in range(2)]

    assert all(session_id in manager._records for session_id in pinned)
    assert [session_id in manager._records for session_id in newest] == [True, True]
    assert len(manager._records) == 7, "over the cap, and honestly so"

    for task in tasks:
        task.cancel()
    await manager.close()


async def test_a_released_session_keeps_its_receipt_on_disk() -> None:
    """The recap is built from receipts, so a released record blanks nothing.

    ``/api/receipts`` and ``frontend/src/recap.ts`` never read the in-memory store;
    the receipt is written by ``EventLog.publish`` before any sweep can reach the
    record it came from.
    """
    manager = RunManager()
    _immediate_release(manager)
    created = await manager.create(_card())
    record = manager._records[created.id]
    record.snapshot.state = SessionState.VERIFIED
    record.snapshot.remembered_result = "LAKEBASE WIN"
    await record.event_log.publish(
        "run_finished",
        {"session": record.snapshot.model_dump(mode="json")},
    )

    await _next_attempt(manager)
    assert created.id not in manager._records

    receipts = load_receipts()
    assert [receipt.session_id for receipt in receipts] == [created.id]
    assert receipts[0].remembered_result == "LAKEBASE WIN"
    await manager.close()


# --------------------------------------------------------------------------- #
# Sequence stability across event-log eviction
# --------------------------------------------------------------------------- #


async def test_sequences_stay_monotonic_and_stable_across_eviction() -> None:
    log = EventLog(limit=3)
    for index in range(6):
        await log.publish("lane_update", {"index": index})

    assert [event.sequence for event in log.events] == [4, 5, 6]
    assert [event.payload["index"] for event in log.events] == [3, 4, 5]
    assert log.evicted == 3
    assert log.first_retained_sequence == 4
    assert log.next_sequence == 7

    # The identity that matters: an event's sequence is fixed when it is published
    # and never changes because something older was dropped.
    survivor = log.events[0]
    await log.publish("lane_update", {"index": 6})
    assert survivor.sequence == 4
    assert log.events[-1].sequence == 7


async def test_a_consumer_resuming_after_the_floor_is_told_what_it_missed() -> None:
    log = EventLog(limit=2)
    for index in range(5):
        await log.publish("lane_update", {"index": index})

    # Sequence 1 is long gone. The events served start at the floor -- never at the
    # requested cursor reinterpreted as an index -- and the first one carries the
    # exact number of events this consumer will never see.
    delivered = []
    async for event in log.stream(after_sequence=1):
        delivered.append(event)
        if len(delivered) == 2:
            break

    assert [event.sequence for event in delivered] == [4, 5]
    assert delivered[0].gap_before == 2
    assert delivered[1].gap_before is None
    assert [event.payload["index"] for event in delivered] == [3, 4]


async def test_an_intact_resume_reports_no_gap() -> None:
    log = EventLog(limit=10)
    for index in range(4):
        await log.publish("lane_update", {"index": index})

    delivered = []
    async for event in log.stream(after_sequence=2):
        delivered.append(event)
        if len(delivered) == 2:
            break

    assert [event.sequence for event in delivered] == [3, 4]
    assert all(event.gap_before is None for event in delivered)
    # An unbroken delivery keeps the wire shape it has always had.
    assert "gap_before" not in delivered[0].model_dump(mode="json")


async def test_a_live_subscriber_is_outrun_and_told_rather_than_shifted() -> None:
    """The failure mode the base offset exists to prevent, exercised live."""
    log = EventLog(limit=2)
    await log.publish("lane_update", {"index": 0})

    delivered = []
    stream = log.stream(after_sequence=0)
    delivered.append(await anext(stream))
    assert delivered[0].sequence == 1
    assert delivered[0].gap_before is None

    # Four more events while the subscriber is not reading: its cursor of 1 is now
    # below the floor of 4.
    for index in range(1, 5):
        await log.publish("lane_update", {"index": index})

    resumed = await anext(stream)
    assert resumed.sequence == 4, "no silently shifted window"
    assert resumed.gap_before == 2, "sequences 2 and 3 are gone and the gap says so"
    assert (await anext(stream)).sequence == 5
    await stream.aclose()


async def test_the_event_floor_is_reportable_for_a_live_session() -> None:
    """An unused seam, and it has to keep reporting the truth to be worth keeping.

    No transport calls this and none should be given it lightly -- see
    ``RunManager.event_floor`` for why the browser's 409 was rejected. What this
    pins is that the floor tracks eviction rather than staying at 1.
    """
    manager = RunManager()
    created = await manager.create(_card())
    record = manager._records[created.id]
    record.event_log.limit = 2
    assert await manager.event_floor(created.id) == 1

    for index in range(4):
        await record.event_log.publish("lane_update", {"index": index})

    assert await manager.event_floor(created.id) == 4
    await manager.close()


async def test_one_session_cannot_grow_its_log_without_bound() -> None:
    manager = RunManager()
    created = await manager.create(_card())
    record = manager._records[created.id]
    record.event_log.limit = 16

    for index in range(500):
        await record.event_log.publish("lane_update", {"index": index})

    assert len(record.event_log.events) == 16
    assert record.event_log.next_sequence == 502
    assert [event.sequence for event in record.event_log.events] == list(range(486, 502))
    await manager.close()


# --------------------------------------------------------------------------- #
# The other process-lifetime dictionaries
# --------------------------------------------------------------------------- #


async def test_scoped_lease_stores_are_bounded_by_the_round_enum() -> None:
    """Believed bounded, now pinned: six rounds plus the one Round 5 cleanup ring."""
    manager = RunManager(round_isolation=True, installation_id="test-installation")

    for _ in range(3):
        for round_id in RoundId:
            manager._lease_store_for_round(round_id)
        manager._round5_cleanup_store()

    assert len(manager._scoped_lease_stores) == len(RoundId) + 1
    await manager.close()
