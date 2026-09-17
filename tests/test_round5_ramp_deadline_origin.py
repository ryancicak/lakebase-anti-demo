"""The scored fan-in run budget must originate at the authoritative release (T0).

Regression coverage for the live ``fanin_worker_ramp_ready_timeout`` failure.

Root cause (confirmed by independent audit): ``_execute_sharded_fanin`` anchored
``run_deadline = loop.time() + FANIN_WORKER_RUN_TIMEOUT_SECONDS`` at resident
*stage*.  The competitor lane then dwells ~10-11 minutes behind the RDS Proxy
exact gate between the "ready" barrier and the authoritative release.  By the
time the parent reached ``await_stage("ramp_ready")`` the 600 s deadline -- spent
entirely on the unscored Proxy wait -- had already expired, so every bout timed
out mechanically the instant it was released, regardless of Aurora capacity.

The fix splits one clock into two:
  * a pre-release *readiness* budget (``FANIN_WORKER_READY_BUDGET_SECONDS``) that
    bounds only the "ready" barrier, and
  * the scored ``FANIN_WORKER_RUN_TIMEOUT_SECONDS`` budget, re-based off the
    *release* instant and used only for ramp/hold/samples/result.
The release dwell itself waits for release-or-cancel without subtracting any
scored budget, and pre-release parent telemetry is reset at T0 so it cannot
contaminate the scored ramp window.

Both structural tests (locking the ordering the way the rest of this suite locks
parent-barrier invariants) and behavioral tests (driving the real
``_execute_sharded_fanin`` against a fake resident pool) are included; the
behavioral tests fail against the stage-anchored code.
"""

from __future__ import annotations

import asyncio
import inspect
import queue
import threading

import pytest

from runner import connection_spike_runner as runner


# --------------------------------------------------------------------------- #
# Structural invariants (fail against the stage-anchored source).
# --------------------------------------------------------------------------- #
def _sharded_source() -> str:
    return inspect.getsource(runner._execute_sharded_fanin)


def test_run_budget_is_not_anchored_at_stage() -> None:
    """No scored-budget origin exists before the authoritative release."""

    source = _sharded_source()
    release = source.index("release_ns.value = time.monotonic_ns()")
    pre_release = source[:release]
    # The stage/preparation region must NOT anchor the scored run budget.  Old
    # code had exactly this line at stage; its presence pre-release is the bug.
    assert "FANIN_WORKER_RUN_TIMEOUT_SECONDS" not in pre_release
    # Pre-release, run_deadline holds only the readiness budget.
    assert "stage_entered_at + FANIN_WORKER_READY_BUDGET_SECONDS" in pre_release


def test_run_budget_is_rebased_at_authoritative_release() -> None:
    """release_ns -> run_deadline -> telemetry reset -> release_event -> ramp."""

    source = _sharded_source()
    release_ns = source.index("release_ns.value = time.monotonic_ns()")
    rebase = source.index("FANIN_WORKER_RUN_TIMEOUT_SECONDS", release_ns)
    telemetry_reset = source.index("parent_safety = fanin.TelemetrySummary()", rebase)
    release_event = source.index("release_event.set()", telemetry_reset)
    ramp_ready = source.index('await_stage("ramp_ready")', release_event)
    # Exactly the audit-mandated order.
    assert release_ns < rebase < telemetry_reset < release_event < ramp_ready
    # The re-base is release-anchored, not stage-anchored.
    assert "released_at + FANIN_WORKER_RUN_TIMEOUT_SECONDS" in source


def test_release_wait_does_not_subtract_scored_budget() -> None:
    """The Proxy dwell waits release-or-cancel with no scored-budget timeout."""

    source = _sharded_source()
    gate = source.index("if resident_release_gate is not None:")
    release_ns = source.index("release_ns.value = time.monotonic_ns()", gate)
    dwell = source[gate:release_ns]
    # No asyncio.timeout / run_deadline bound on the unscored dwell (old code
    # wrapped it in ``asyncio.timeout(max(0.0, run_deadline - loop.time()))``).
    assert "asyncio.timeout" not in dwell
    assert "run_deadline" not in dwell
    # It waits both events and settles promptly on cancel.
    assert "return_when=asyncio.FIRST_COMPLETED" in dwell
    assert 'RunnerCancelled("fanin_cancelled")' in dwell


def test_ready_barrier_precedes_release() -> None:
    source = _sharded_source()
    ready = source.index('await_stage("ready")')
    release_ns = source.index("release_ns.value = time.monotonic_ns()")
    # ``ramp_ready`` also appears inside the await_stage body; anchor on the
    # actual post-release call site.
    ramp_ready = source.index('await_stage("ramp_ready")', release_ns)
    assert ready < release_ns < ramp_ready


def test_release_emits_structured_deadline_trace() -> None:
    source = _sharded_source()
    assert "FANIN_RELEASE_TRACE_JSON:" in source
    assert '"deadline_origin": "release"' in source
    assert '"worker_count": fanin.WORKER_COUNT' in source


def test_bounded_main_does_not_double_bound_the_fanin_path() -> None:
    source = inspect.getsource(runner.main)
    assert "timeout = None if fanin_request is not None else RUN_TIMEOUT_SECONDS" in source


def test_scored_constants_are_preserved_exactly() -> None:
    assert runner.fanin.TARGET_CLIENTS_PER_LANE == 10_000
    assert runner.fanin.WORKER_COUNT == 4
    assert runner.fanin.PARTITION_CLIENTS_PER_LANE == 2_500
    assert runner.fanin.HOLD_SECONDS == 30
    assert runner.fanin.SAMPLED_QUERIES_PER_LANE == 64
    # 600 s scored worker budget and 720 s resident visibility unchanged.
    assert runner.FANIN_WORKER_RUN_TIMEOUT_SECONDS == 600.0
    assert runner.fanin.RUN_TIMEOUT_SECONDS == 600.0
    assert "VisibilityTimeout=720" in inspect.getsource(runner._resident_agent)


# --------------------------------------------------------------------------- #
# Behavioral coverage against a fake resident pool.
# --------------------------------------------------------------------------- #
class _FakeValue:
    """Stand-in for a multiprocessing shared ``Value("q", 0)``."""

    def __init__(self) -> None:
        self.value = 0
        self._lock = threading.Lock()

    def get_lock(self) -> threading.Lock:
        return self._lock


class _FakeProc:
    """A worker that reports alive with pid 0 so parent telemetry no-ops."""

    def __init__(self, index: int) -> None:
        self.name = f"round5-fanin-{index}"
        self.pid = 0  # excluded from telemetry -> sample_parent_safety returns early
        self.exitcode = None
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout: float | None = None) -> None:
        del timeout

    def terminate(self) -> None:
        self._alive = False


class _FakePool:
    worker_ready_indexes = tuple(range(runner.fanin.WORKER_COUNT))

    def __init__(self) -> None:
        self.context = None
        self.control_queue: queue.Queue = queue.Queue()
        self.result_queue: queue.Queue = queue.Queue()
        self.release_event = threading.Event()
        self.hold_prepare_event = threading.Event()
        self.hold_epoch_event = threading.Event()
        self.sample_release_event = threading.Event()
        self.teardown_event = threading.Event()
        self.cancel_event = threading.Event()
        self.release_ns = _FakeValue()
        self.hold_ns = _FakeValue()
        self.processes = [_FakeProc(i) for i in range(runner.fanin.WORKER_COUNT)]
        self.submitted: list = []

    def submit(self, request) -> None:
        self.submitted.append(request)


_RUN_ID = "job-deadline-origin"
_DIGEST = "prepared-digest-deadline-origin"


def _request() -> dict:
    return {
        "run_id": _RUN_ID,
        "prepared_request_digest": _DIGEST,
        "targets": [{"lane_id": "competitor"}],
    }


def _seed_ready(pool: _FakePool) -> None:
    for index in range(runner.fanin.WORKER_COUNT):
        pool.control_queue.put(
            (
                "ready",
                index,
                {
                    "run_id": _RUN_ID,
                    "worker_index": index,
                    "worker_pid": 4096 + index,
                    "runner_harness_sha256": runner.LOADED_RUNNER_HARNESS_SHA256,
                    "request_sha256": _DIGEST,
                },
            )
        )


@pytest.mark.asyncio
async def test_cancel_during_unreleased_dwell_settles_promptly_with_zero_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A towel during the Proxy dwell settles as a cancel, not a run-timeout.

    Fails against the stage-anchored code: with the scored budget mocked below
    the tiny dwell, old code raised ``asyncio.TimeoutError`` out of the dwell's
    ``asyncio.timeout(run_deadline - now)`` wrapper long before the towel; the
    fix waits release-or-cancel with no scored bound and settles on the towel.
    """

    # Tiny scored budget; generous readiness budget.  In the fixed code the
    # scored budget is only consulted post-release, so a tiny value is harmless
    # here -- the dwell must be governed purely by release-or-cancel.
    monkeypatch.setattr(runner, "FANIN_WORKER_RUN_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(runner, "FANIN_WORKER_READY_BUDGET_SECONDS", 30.0)

    pool = _FakePool()
    _seed_ready(pool)
    cancelled = asyncio.Event()
    release_gate = asyncio.Event()  # never set: the Proxy "never finishes"

    async def towel_after_dwell() -> None:
        # Dwell far longer than the mocked scored budget before the towel.
        await asyncio.sleep(0.3)
        cancelled.set()

    towel = asyncio.create_task(towel_after_dwell())
    try:
        with pytest.raises(runner.RunnerCancelled) as excinfo:
            await runner._execute_sharded_fanin(
                _request(),
                cancelled,
                resident_pool=pool,
                resident_release_gate=release_gate,
            )
        assert "fanin_cancelled" in str(excinfo.value)
    finally:
        towel.cancel()
        await asyncio.gather(towel, return_exceptions=True)

    # The authoritative release epoch was never taken -> no scored ramp began
    # -> zero client work.  (The finally-block sets the raw worker events during
    # teardown, so ``release_ns`` -- not ``release_event`` -- is the T0 witness.)
    assert pool.release_ns.value == 0


@pytest.mark.asyncio
async def test_post_release_ramp_stall_times_out_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After release, a stalled ramp fails deterministically with cleanup.

    This proves the scored deadline is live *after* release (re-based at T0):
    with no worker reaching ``ramp_ready`` the parent raises the ramp-ready
    timeout within the re-based budget and terminates every worker in finally.
    """

    monkeypatch.setattr(runner, "FANIN_WORKER_RUN_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(runner, "FANIN_WORKER_READY_BUDGET_SECONDS", 30.0)

    pool = _FakePool()
    _seed_ready(pool)
    cancelled = asyncio.Event()
    release_gate = asyncio.Event()
    release_gate.set()  # Proxy is ready: release fires right after the ready barrier

    with pytest.raises(runner.RunnerContractError) as excinfo:
        await runner._execute_sharded_fanin(
            _request(),
            cancelled,
            resident_pool=pool,
            resident_release_gate=release_gate,
        )
    assert str(excinfo.value) == "fanin_worker_ramp_ready_timeout"

    # Release happened (deadline re-based at T0) and every worker was cleaned up.
    assert pool.release_ns.value != 0
    assert pool.release_event.is_set()
    assert all(not proc.is_alive() for proc in pool.processes)
