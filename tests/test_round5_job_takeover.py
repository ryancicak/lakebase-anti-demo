"""Process-safe ownership, rejoin, and takeover for the resident runner job registry.

These tests close the verified crash gap: if the first invocation dies after
atomically claiming a job but before writing a terminal result and ``settled``,
a duplicate used to poll the registry for the full
``FANIN_SSM_COMMAND_TIMEOUT_SECONDS`` (660 s) and then fail. The per-job flock
lets a duplicate detect owner death immediately -- the kernel releases the lock
when the owner exits for any reason -- and either replay a recovered terminal
outcome or take over and run the job to completion.

The owner-death and race tests use ``os.fork`` so the "owner" is a genuinely
separate process that really exits (or is ``SIGKILL``-ed) while holding the
lock; nothing here mocks the death.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import signal
import time
from pathlib import Path

import pytest

from runner import connection_spike_runner as runner

JOB_ID = "a" * 64
PREPARED_DIGEST = "b" * 64

requires_fork = pytest.mark.skipif(
    not hasattr(os, "fork"), reason="owner-death tests require POSIX fork"
)


def _request(job_id: str = JOB_ID, digest: str = PREPARED_DIGEST) -> dict[str, object]:
    return {
        "action": "run_lane_v3",
        "job_id": job_id,
        "prepared_request_digest": digest,
    }


def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not reached within timeout")


# --------------------------------------------------------------------------- #
# Takeover decisions (no live process required: releasing the lock == death)
# --------------------------------------------------------------------------- #


def test_takeover_when_owner_dies_mid_flight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A claimed-and-running job whose owner is gone is taken over, not waited on."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = _request()
    job, owner, lock = runner._claim_job(request)
    assert owner
    runner._atomic_job_write(job, "state", "running")
    # Releasing the lock is exactly what the kernel does when the owner exits.
    runner._release_job_lock(lock)

    started = time.monotonic()
    rejoin = runner._rejoin_or_takeover_job(job, "run-takeover", request)
    elapsed = time.monotonic() - started

    assert rejoin.disposition == "takeover"
    assert rejoin.lock is not None
    # The whole point: detection is immediate, not a 660 s poll.
    assert elapsed < 2.0
    # The immutable identity is preserved across the takeover.
    assert runner._read_job_value(job, "prepared_request_digest") == PREPARED_DIGEST
    runner._release_job_lock(rejoin.lock)


def test_takeover_recovers_a_complete_result_left_unsettled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An owner that produced a result but died before settling is replayed, not re-run."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = _request()
    job, _owner, lock = runner._claim_job(request)
    runner._atomic_job_write(job, "result_gzip_base64", "enc-complete")
    runner._atomic_job_write(job, "state", "completed")
    runner._release_job_lock(lock)  # died after writing the result, before settling

    rejoin = runner._rejoin_or_takeover_job(job, "run-x", request)

    assert rejoin.disposition == "replay"
    assert rejoin.encoded_result == "enc-complete"
    assert rejoin.was_cancelled is False
    # The recovering invocation durably settles the recovered terminal state.
    assert runner._read_job_value(job, "settled") == "true"


def test_takeover_replays_a_deterministic_failure_left_unsettled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = _request()
    job, _owner, lock = runner._claim_job(request)
    runner._atomic_job_write(job, "state", "failed")
    runner._release_job_lock(lock)

    rejoin = runner._rejoin_or_takeover_job(job, "run-x", request)

    assert rejoin.disposition == "replay"
    assert rejoin.encoded_result is None
    assert rejoin.was_cancelled is False
    assert runner._read_job_value(job, "settled") == "true"


def test_takeover_replays_a_cancellation_left_unsettled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = _request()
    job, _owner, lock = runner._claim_job(request)
    runner._atomic_job_write(job, "state", "cancelled")
    runner._release_job_lock(lock)

    rejoin = runner._rejoin_or_takeover_job(job, "run-x", request)

    assert rejoin.disposition == "replay"
    assert rejoin.was_cancelled is True
    assert runner._read_job_value(job, "settled") == "true"


def test_settled_result_is_replayed_without_touching_the_lock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The fast path: a fully settled job replays immediately, even under a held lock."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = _request()
    job, _owner, lock = runner._claim_job(request)
    runner._atomic_job_write(job, "result_gzip_base64", "enc-settled")
    runner._atomic_job_write(job, "state", "completed")
    runner._atomic_job_write(job, "settled", "true")
    # Owner is still "alive" (lock held); the settled fast path must not block on it.
    try:
        rejoin = runner._rejoin_or_takeover_job(job, "run-x", request)
    finally:
        runner._release_job_lock(lock)

    assert rejoin.disposition == "replay"
    assert rejoin.encoded_result == "enc-settled"


def test_takeover_refuses_a_different_prepared_request_digest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Immutability is enforced even on the takeover path: a mismatch is a conflict."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    job, _owner, lock = runner._claim_job(_request())
    runner._atomic_job_write(job, "state", "running")
    runner._release_job_lock(lock)

    with pytest.raises(runner.RunnerContractError, match="fanin_job_identity_conflict"):
        runner._rejoin_or_takeover_job(job, "run-x", _request(digest="c" * 64))


# --------------------------------------------------------------------------- #
# Real owner death and live-owner arbitration (os.fork)
# --------------------------------------------------------------------------- #


@requires_fork
def test_takeover_after_a_real_owner_process_is_killed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A SIGKILL-ed owner releases the lock via the kernel; the duplicate takes over."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = _request()
    ready = tmp_path / "ready"

    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child process
        try:
            job, owner, _lock = runner._claim_job(request)
            if owner:
                runner._atomic_job_write(job, "state", "running")
                ready.write_text("1")
            # Hold the lock (owner "alive") until the parent kills us.
            while True:
                time.sleep(3600)
        finally:
            os._exit(0)

    _wait_for(ready.exists)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)  # reap so the kernel has released the lock

    job_directory = runner.JOB_ROOT / JOB_ID
    started = time.monotonic()
    rejoin = runner._rejoin_or_takeover_job(job_directory, "run-x", request)
    assert time.monotonic() - started < 5.0
    assert rejoin.disposition == "takeover"
    runner._release_job_lock(rejoin.lock)


@requires_fork
def test_a_live_owner_is_waited_on_and_never_taken_over(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """While the owner is alive and holding the lock, a duplicate waits -- never takes over."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    # Shrink the rejoin ceiling so the "wait for a live owner" path is observable fast.
    monkeypatch.setattr(runner, "FANIN_SSM_COMMAND_TIMEOUT_SECONDS", 1.0)
    request = _request()
    ready = tmp_path / "ready"
    stop = tmp_path / "stop"

    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child process
        try:
            job, owner, _lock = runner._claim_job(request)
            if owner:
                runner._atomic_job_write(job, "state", "running")
                ready.write_text("1")
            while not stop.exists():
                time.sleep(0.02)
        finally:
            os._exit(0)

    try:
        _wait_for(ready.exists)
        job_directory = runner.JOB_ROOT / JOB_ID
        with pytest.raises(
            runner.RunnerContractError, match="fanin_job_rejoin_timeout"
        ):
            runner._rejoin_or_takeover_job(job_directory, "run-x", request)
    finally:
        stop.write_text("1")
        os.waitpid(pid, 0)


@requires_fork
def test_a_live_owner_that_finishes_is_rejoined_and_its_result_replayed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The duplicate that arrives while the owner runs replays the owner's real result."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = _request()
    ready = tmp_path / "ready"

    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child process
        try:
            job, owner, lock = runner._claim_job(request)
            if owner:
                runner._atomic_job_write(job, "state", "running")
                ready.write_text("1")
                time.sleep(0.3)
                runner._atomic_job_write(job, "result_gzip_base64", "enc-owner")
                runner._atomic_job_write(job, "state", "completed")
                runner._atomic_job_write(job, "settled", "true")
                runner._release_job_lock(lock)
        finally:
            os._exit(0)

    _wait_for(ready.exists)
    job_directory = runner.JOB_ROOT / JOB_ID
    rejoin = runner._rejoin_or_takeover_job(job_directory, "run-x", request)
    os.waitpid(pid, 0)

    assert rejoin.disposition == "replay"
    assert rejoin.encoded_result == "enc-owner"


@requires_fork
def test_exactly_one_owner_under_concurrent_claims(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Concurrent claims of one job id yield exactly one owner -- never two socket fan-ins."""
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    runner.JOB_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    request = _request()
    start = tmp_path / "start"
    contenders = 6
    out_paths = [tmp_path / f"out-{index}" for index in range(contenders)]

    children: list[int] = []
    for out_path in out_paths:
        pid = os.fork()
        if pid == 0:  # pragma: no cover - runs in the child process
            try:
                _wait_for(start.exists, timeout=10.0)
                job, owner, lock = runner._claim_job(request)
                out_path.write_text("owner" if owner else "contender")
                if owner:
                    # Hold the lock briefly so contenders observe a live owner,
                    # then settle so any rejoiner resolves rather than takes over.
                    time.sleep(0.3)
                    runner._atomic_job_write(job, "result_gzip_base64", "enc-race")
                    runner._atomic_job_write(job, "state", "completed")
                    runner._atomic_job_write(job, "settled", "true")
                    runner._release_job_lock(lock)
            except runner.RunnerContractError as exc:
                out_path.write_text(f"error:{exc.args[0]}")
            finally:
                os._exit(0)
        children.append(pid)

    start.write_text("go")
    for pid in children:
        os.waitpid(pid, 0)

    outcomes = [path.read_text() for path in out_paths]
    assert outcomes.count("owner") == 1, outcomes
    # Every non-owner is a clean contender -- no crashes, no second owner.
    assert all(value in {"owner", "contender"} for value in outcomes), outcomes
    assert runner._read_job_value(runner.JOB_ROOT / JOB_ID, "state") == "completed"


# --------------------------------------------------------------------------- #
# End-to-end main(): a dead owner is taken over and its stale run dir reclaimed
# --------------------------------------------------------------------------- #


def test_main_takes_over_a_dead_owner_and_reclaims_its_run_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    monkeypatch.setattr(runner, "RUN_ROOT", tmp_path / "runs")
    monkeypatch.setattr(runner, "LOCK_PATH", tmp_path / "runner.lock")
    run_id = "run-takeover-e2e"

    # A prior owner claimed and started the job, then died without settling: the
    # registry directory exists with a published identity and no lock is held.
    runner.JOB_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    job_directory = runner.JOB_ROOT / JOB_ID
    job_directory.mkdir(mode=0o700)
    runner._atomic_job_write(job_directory, "prepared_request_digest", PREPARED_DIGEST)
    runner._atomic_job_write(job_directory, "state", "running")
    # ...and it left a stale run directory behind.
    runner.RUN_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    stale = runner.RUN_ROOT / run_id
    stale.mkdir(mode=0o700)
    (stale / "owner").write_text(run_id, encoding="utf-8")
    (stale / "leftover").write_text("from the dead owner", encoding="utf-8")

    fanin_request = _request()
    expected = {"protocol": runner.PROTOCOL, "lanes": [], "contracts_verified": True}

    monkeypatch.setattr(runner.sys, "argv", ["connection_spike_runner.py", "ignored"])
    monkeypatch.setattr(runner, "_decode_payload", lambda _arg: {"protocol": runner.PROTOCOL})
    monkeypatch.setattr(
        runner,
        "_decode_fanin_request",
        lambda _arg: (run_id, (), "a" * 64, fanin_request),
    )
    monkeypatch.setattr(runner, "_validate_runtime", lambda: None)
    monkeypatch.setattr(runner, "_validate_trust_bundle", lambda _digest: None)

    async def fake_execute(request, targets, cancelled):
        assert request is fanin_request
        return dict(expected)

    monkeypatch.setattr(runner, "_execute_fanin_request", fake_execute)

    exit_code = runner.main()

    assert exit_code == 0
    payloads = [
        line.removeprefix("RESULT_GZIP_BASE64:")
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("RESULT_GZIP_BASE64:")
    ]
    assert len(payloads) == 1
    decoded = json.loads(gzip.decompress(base64.urlsafe_b64decode(payloads[0])))
    assert decoded == expected
    # The takeover settled the job and preserved the immutable identity.
    assert runner._read_job_value(job_directory, "settled") == "true"
    assert runner._read_job_value(job_directory, "state") == "completed"
    assert runner._read_job_value(job_directory, "prepared_request_digest") == PREPARED_DIGEST
    # The stale run directory was reclaimed, and the fresh one cleaned up on exit.
    assert not stale.exists()
