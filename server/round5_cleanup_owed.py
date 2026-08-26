"""A Round 5 cleanup that never proved the RDS Proxy gone, and who gets told.

Round 5 is the only round that *creates* a billable AWS resource per bout. The
RDS Proxy it stands up bills from the moment it is ``available`` until somebody
deletes it, and the only thing that deletes it is this app's backstage cleanup.
So the question "did cleanup reach the deletion?" is a money question, and it
needs an answer on a surface an operator reads without being asked to look.

**Why this module exists at all, rather than a field on the snapshot.** The
snapshot already carries ``round5_setup.cleanup_failure`` and
``towel.cleanup_failure``, and both are real -- they reach the browser and the
sealed receipt. Neither reaches an operator who is not looking at that bout. A
towel thrown during Round 5 setup left a Proxy ``available`` for twenty minutes
while ``/readyz`` reported ``status: ready, degraded: false``, and the app was
not lying so much as answering a narrower question than the one being asked. A
surface that reports health must name what it actually checked.

**Shape borrowed wholesale from ``pipeline_power.owed_stop_notice``**, which
solved the same problem for Round 4's forgotten pipeline stop: a process-local
record, no I/O on the read path, a due time so an ordinary settling bout does
not fire it, and one shared sentence so no two surfaces can disagree about what
happened. ``/readyz`` is polled every few seconds by a platform health check;
anything that took a round trip to answer would turn a health endpoint into a
load generator against the database the ring is fenced on.

**It never claims ``degraded``, and that is deliberate.** ``_apply_startup_reap``
and ``_apply_owed_pipeline_stop`` both record the rule this follows: spend is
not availability. A leaked Proxy stops no round from arming -- the next bout
builds its own -- and lowering the one field an operator checks before a demo,
for a money problem, makes that field less trustworthy rather than more. It gets
fields of its own that say exactly what is wrong, and the caller decides.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

#: How long a cleanup may be failing before the notice becomes visible.
#:
#: Not zero, for the reason ``owed_stop_notice`` is silent inside a redo window:
#: a signal that fires on every ordinary bout is learned away, and then it is
#: not a signal. The first moments after a towel are legitimately a Proxy that
#: exists with no delete issued yet, because the delete is what is being started.
#:
#: Not long either. ``wait_for_proxy_delete_accepted`` returns as soon as AWS
#: accepts ``DeleteDBProxy``, which is seconds on the deployed app -- the slow
#: part, up to the 31.5 minutes once measured, is AWS making the Proxy
#: *disappear* after accepting, and that case does not reach here at all because
#: the caller gates on acceptance rather than absence. Two minutes is therefore
#: an order of magnitude of headroom over the thing being waited for, and two
#: minutes rather than the fifty-odd the retry budget runs to.
GRACE_SECONDS = 120.0


def leaked_proxy_sentence(
    resource: str,
    since: str,
    *,
    attempts: int | None = None,
    still_retrying: bool,
) -> str:
    """The one sentence every surface says about a Proxy cleanup could not remove.

    **Shared rather than repeated, on the precedent
    ``pipeline_power.owed_stop_sentence`` set.** That helper exists because the
    same warning had been written twice within hours and the two copies already
    disagreed. This fact has three readers -- the towel's ``cleanup_failure``,
    the Round 5 setup snapshot's ``cleanup_failure``, and ``/readyz`` -- reached
    by three different routes, which is exactly the shape that produced the
    disagreement last time.

    It carries **no dollar figure**, for the reason ``owed_stop_sentence``
    carries none: this process cannot see the Proxy. It knows only that cleanup
    did not confirm the deletion, which is a statement about what this app did,
    not a reading of the account. Naming a rate for a resource nobody has looked
    at would be the same defect as a health field that read ``degraded`` without
    checking.
    """

    named = f" '{resource}'" if resource else " this bout created"
    when = f" since {since}" if since else ""
    counted = f" after {attempts} automatic attempts" if attempts is not None else ""
    tail = (
        "automatic cleanup is still retrying"
        if still_retrying
        else "automatic cleanup has stopped retrying"
    )
    return (
        f"ROUND 5 BACKSTAGE CLEANUP did not converge{counted}{when}, so the RDS "
        f"Proxy{named} MAY STILL BE RUNNING AND BILLING — its deletion was never "
        f"confirmed · {tail}; the ring stays held until cleanup is confirmed. "
        f"Retry cleanup, and if that does not clear it, check "
        f"'aws rds describe-db-proxies' and delete it by hand."
    )


@dataclass(frozen=True, slots=True)
class Round5CleanupOwed:
    """One bout whose Proxy deletion this process could not confirm."""

    session_id: str
    #: The deterministic Proxy name, or empty when the engine could not name it.
    #: Empty is reported as such rather than guessed at: an operator who is told
    #: the wrong name looks in the wrong place and concludes there is nothing
    #: there, which is worse than being told to list them.
    resource: str
    since: str
    due_at: datetime
    attempts: int | None = None
    still_retrying: bool = True

    @property
    def detail(self) -> str:
        return leaked_proxy_sentence(
            self.resource,
            self.since,
            attempts=self.attempts,
            still_retrying=self.still_retrying,
        )


#: Bouts this process has failed to clean up, keyed by session id. Process-local
#: on purpose and not durable: it describes what *this* replica is failing to
#: do right now, and a replica that restarts has a reaper and a startup orphan
#: sweep for what it left behind. Making it durable would report the same leak
#: from a process that is not the one leaking.
_owed: dict[str, Round5CleanupOwed] = {}


def record_round5_cleanup_owed(
    session_id: str,
    *,
    resource: str = "",
    attempts: int | None = None,
    still_retrying: bool = True,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Round5CleanupOwed:
    """Note that this bout's Proxy deletion is not confirmed, or update the note.

    Re-recording an already-noted session keeps its original ``since`` and due
    time. The automatic retry calls this once per failed attempt, and a clock
    that restarted on each call would hold the notice below its due time forever
    -- a counter that resets faster than it counts is how a bound becomes no
    bound at all.
    """

    stamped = now().astimezone(UTC)
    existing = _owed.get(session_id)
    since = existing.since if existing is not None else stamped.isoformat()
    due_at = existing.due_at if existing is not None else stamped + timedelta(seconds=GRACE_SECONDS)
    owed = Round5CleanupOwed(
        session_id=session_id,
        # A later call that can name the Proxy beats an earlier one that could
        # not; a later call that cannot must never erase a name already held.
        resource=resource or (existing.resource if existing is not None else ""),
        since=since,
        # Abandonment is due immediately. The grace window exists to let an
        # ordinary settling bout finish quietly, and a cleanup that has stopped
        # retrying is not going to finish.
        due_at=stamped if not still_retrying else due_at,
        attempts=attempts if attempts is not None else getattr(existing, "attempts", None),
        still_retrying=still_retrying,
    )
    _owed[session_id] = owed
    return owed


def clear_round5_cleanup_owed(session_id: str) -> None:
    """Cleanup confirmed the Proxy is gone. Forget it, including from ``/readyz``."""

    _owed.pop(session_id, None)


def reset_round5_cleanup_owed() -> None:
    """Drop every record. For process teardown and for tests."""

    _owed.clear()


def round5_cleanup_owed_notice(
    *, now: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> Round5CleanupOwed | None:
    """The oldest Proxy this process has failed to delete, or ``None``. No I/O.

    Oldest rather than newest: the ring admits one bout at a time so there is
    normally at most one, and when there is more than one the one that has been
    billing longest is the one an operator should be told about first.
    """

    stamped = now().astimezone(UTC)
    due = [owed for owed in _owed.values() if stamped >= owed.due_at]
    if not due:
        return None
    return min(due, key=lambda owed: owed.since)
