"""What `antidemo serve` must keep true about launching and about the log it leaves.

Two properties are load-bearing and neither is obvious from reading the calls:

* The serve command never resolves dependencies. A resolve at serve time stalls
  the demo it is about to start, and has already replaced a provisioned
  interpreter mid-flight.
* Rolling the log keeps the inode. The serving process inherited the descriptor
  at launch and cannot be told to reopen, so a rename would leave it appending
  to a file with no name for the rest of the month.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

from server import server_launch
from server.cli import _parser, _serve_log_path
from server.server_launch import (
    DEFAULT_LOG_KEEP,
    DEFAULT_LOG_MAX_BYTES,
    LOG_KEEP_ENV,
    LOG_MAX_BYTES_ENV,
    _exit_code,
    default_log_path,
    environment_dir,
    log_limits,
    open_log,
    require_serving_environment,
    roll_path,
    rotate_if_needed,
    rotate_log,
    serve_command,
    supervise,
)


def provisioned(root: Path) -> Path:
    (root / ".venv" / "bin").mkdir(parents=True)
    (root / ".venv" / "bin" / "uvicorn").write_text("#!/bin/sh\n")
    return root


# --------------------------------------------------------------------------
# The serve command
# --------------------------------------------------------------------------


def test_the_serve_command_never_resolves_dependencies() -> None:
    """The whole point. `uv run` without --no-sync re-resolves on any drift.

    A stray `.python-version` pinning 3.12 was enough to make uv delete a
    provisioned 3.14 environment and start rebuilding it, seconds before a
    demo, which presents as a hang rather than as an error.
    """

    command = serve_command("127.0.0.1", 8000)

    assert command[:3] == ["uv", "run", "--no-sync"]
    assert command[3:] == ["uvicorn", "app:app", "--host", "127.0.0.1", "--port", "8000"]


def test_the_foreground_path_execs_that_same_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two spellings of "run uvicorn" is how one of them loses --no-sync.

    So this asserts on what the foreground path actually hands to the kernel,
    not on what the module looks like.
    """

    from server import cli

    handed: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(cli, "require_serving_environment", lambda: provisioned(tmp_path))
    monkeypatch.setattr(cli, "manifest_path", lambda: tmp_path / "manifest.json")
    monkeypatch.setattr(
        cli.os, "execvp", lambda file, arguments: handed.append((file, arguments))
    )

    cli._serve("127.0.0.1", 8123)

    assert handed == [("uv", serve_command("127.0.0.1", 8123))]
    assert "--no-sync" in handed[0][1]


# --------------------------------------------------------------------------
# Refusing an environment that was never provisioned
# --------------------------------------------------------------------------


def test_an_unprovisioned_environment_is_refused_by_name(tmp_path: Path) -> None:
    """`uv run --no-sync` would build an empty venv and die on a broken import.

    That failure reads as a bug in this repository rather than as a missing
    install step, which is the exact silent-failure shape being removed. So the
    refusal has to happen here and has to name the command that fixes it.
    """

    with pytest.raises(RuntimeError) as failure:
        require_serving_environment(tmp_path, {})

    assert "uv sync" in str(failure.value)
    assert str(tmp_path / ".venv") in str(failure.value)


def test_a_provisioned_environment_is_accepted(tmp_path: Path) -> None:
    assert require_serving_environment(provisioned(tmp_path), {}) == tmp_path / ".venv"


def test_uvs_own_environment_override_is_honoured(tmp_path: Path) -> None:
    """Otherwise the check would refuse a tree that serves perfectly well."""

    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "bin").mkdir(parents=True)
    (elsewhere / "bin" / "uvicorn").write_text("#!/bin/sh\n")

    absolute = {"UV_PROJECT_ENVIRONMENT": str(elsewhere)}
    assert require_serving_environment(tmp_path, absolute) == elsewhere

    # uv reads a relative value relative to the project, not to the caller's cwd.
    relative = {"UV_PROJECT_ENVIRONMENT": "elsewhere"}
    assert environment_dir(tmp_path, relative) == tmp_path / "elsewhere"


# --------------------------------------------------------------------------
# Limits
# --------------------------------------------------------------------------


def test_log_limits_default_to_a_bounded_ceiling() -> None:
    assert log_limits({}) == (DEFAULT_LOG_MAX_BYTES, DEFAULT_LOG_KEEP)
    # 40 MiB for one port, which is weeks at the rate a real server writes.
    assert DEFAULT_LOG_MAX_BYTES * (DEFAULT_LOG_KEEP + 1) <= 64 * 1024 * 1024


def test_log_limits_are_configurable_and_keeping_nothing_is_legitimate() -> None:
    assert log_limits({LOG_MAX_BYTES_ENV: "65536", LOG_KEEP_ENV: "2"}) == (65536, 2)
    assert log_limits({LOG_KEEP_ENV: "0"})[1] == 0


@pytest.mark.parametrize(
    "environment",
    [
        {LOG_MAX_BYTES_ENV: "eight megabytes"},
        {LOG_MAX_BYTES_ENV: "100"},
        {LOG_KEEP_ENV: "-1"},
    ],
)
def test_a_mistyped_limit_is_refused_rather_than_ignored(environment: dict) -> None:
    """A cap that silently reverted to the default is discovered when the disk fills."""

    with pytest.raises(RuntimeError):
        log_limits(environment)


def test_the_default_log_sits_beside_the_selected_manifest(tmp_path: Path) -> None:
    generation = tmp_path / "gen"
    generation.mkdir()
    environ = {"ANTI_DEMO_MANIFEST": str(generation / "manifest.json")}

    assert default_log_path(8123, environ) == generation / "server-8123.log"
    # Per port, for the same reason the launch record is: two servers on two
    # ports are two different things.
    assert default_log_path(8000, environ) != default_log_path(8001, environ)
    assert default_log_path(8000, {}) is None


# --------------------------------------------------------------------------
# Rolling
# --------------------------------------------------------------------------


def test_rolling_keeps_the_inode_the_serving_process_holds(tmp_path: Path) -> None:
    """The property a rename would break, and the reason for copy-then-truncate."""

    log = tmp_path / "server.log"
    log.write_text("first generation\n")
    before = log.stat().st_ino

    rotate_log(log, keep=3)

    assert log.stat().st_ino == before, "a renamed log orphans every open writer"
    assert log.read_text() == ""
    assert roll_path(log, 1).read_text() == "first generation\n"


def test_an_appending_writer_resumes_at_zero_after_a_roll(tmp_path: Path) -> None:
    """Why the descriptor is opened O_APPEND.

    Without O_APPEND the writer keeps its own offset, and a truncate leaves a
    hole the size of the file that was rolled away -- megabytes of NUL bytes
    that no reader can tell from real output.
    """

    log = tmp_path / "server.log"
    descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, b"before the roll\n")
        rotate_log(log, keep=2, fd=descriptor)
        os.write(descriptor, b"after the roll\n")
    finally:
        os.close(descriptor)

    assert log.read_bytes() == b"after the roll\n"
    assert b"\x00" not in log.read_bytes()
    assert roll_path(log, 1).read_bytes() == b"before the roll\n"


def test_rolls_age_and_the_oldest_is_dropped(tmp_path: Path) -> None:
    log = tmp_path / "server.log"
    for generation in ("one", "two", "three", "four"):
        log.write_text(f"{generation}\n")
        rotate_log(log, keep=2)

    assert roll_path(log, 1).read_text() == "four\n"
    assert roll_path(log, 2).read_text() == "three\n"
    assert not roll_path(log, 3).exists(), "keep=2 must cap the disk at two rolls"


def test_keeping_nothing_still_truncates(tmp_path: Path) -> None:
    log = tmp_path / "server.log"
    log.write_text("discard me\n")

    rotate_log(log, keep=0)

    assert log.read_text() == ""
    assert not roll_path(log, 1).exists()


def test_rolling_waits_for_the_cap(tmp_path: Path) -> None:
    log = tmp_path / "server.log"
    log.write_text("x" * 100)

    assert rotate_if_needed(log, max_bytes=1024, keep=1) is False
    assert log.read_text() == "x" * 100
    assert rotate_if_needed(log, max_bytes=100, keep=1) is True
    assert log.read_text() == ""


def test_a_log_that_is_already_too_big_is_rolled_before_it_is_appended_to(
    tmp_path: Path,
) -> None:
    """A restart is the one moment a roll is free, so it is not skipped."""

    log = tmp_path / "gen" / "server.log"
    log.parent.mkdir()
    log.write_text("y" * 5000)

    descriptor = open_log(log, max_bytes=1024, keep=1)
    try:
        os.write(descriptor, b"this run\n")
    finally:
        os.close(descriptor)

    assert log.read_bytes() == b"this run\n"
    assert roll_path(log, 1).read_text() == "y" * 5000
    assert log.stat().st_mode & 0o777 == 0o600


def test_a_missing_log_is_not_an_error(tmp_path: Path) -> None:
    """Rolling must never be the thing that takes a server down."""

    assert rotate_if_needed(tmp_path / "absent.log", max_bytes=1, keep=1) is False


# --------------------------------------------------------------------------
# The supervisor
# --------------------------------------------------------------------------


def test_the_supervisor_rolls_the_log_while_the_server_runs(tmp_path: Path) -> None:
    """The reason there is a supervisor at all.

    Rolling only at startup would never roll a process that is meant to live
    for weeks, which is exactly the process this launches.
    """

    log = tmp_path / "server.log"
    log.write_text("z" * 5000)
    ticks: list[float] = []
    calls = iter([(0, 0), (0, 0), (4242, 0)])
    clock = iter([0.0, 0.0, 100.0, 100.0])

    status = supervise(
        4242,
        log,
        max_bytes=1024,
        keep=1,
        log_fd=None,
        rotation_interval=30.0,
        tick_seconds=0.5,
        sleep=ticks.append,
        waitpid=lambda _pid, _flags: next(calls),
        monotonic=lambda: next(clock),
    )

    assert status == 0
    assert ticks == [0.5, 0.5], "it must not busy-loop"
    assert log.read_text() == "", "the log grew past the cap and was not rolled"


def test_the_supervisor_notices_a_stopped_server_without_waiting_for_a_roll(
    tmp_path: Path,
) -> None:
    """Measured, not assumed: with one timer for both, `kill` plus `antidemo status`
    showed the supervisor still sitting there for up to thirty seconds, which
    reads as a shutdown that did not work.
    """

    ticks: list[float] = []
    calls = iter([(0, 0), (0, 0), (0, 0), (11, 0)])

    supervise(
        11,
        tmp_path / "server.log",
        max_bytes=1 << 30,
        keep=1,
        log_fd=None,
        sleep=ticks.append,
        waitpid=lambda _pid, _flags: next(calls),
    )

    assert ticks and max(ticks) <= 1.0, "the reap tick must not be the rotation interval"


def test_without_a_respawn_the_supervisor_only_waits(tmp_path: Path) -> None:
    """Restarting is something it is given, never something it assumes.

    `respawn=None` has to be the earlier wait-and-rotate behaviour exactly, so
    that every caller which does not want a restarter cannot accidentally get
    one.
    """

    log = tmp_path / "server.log"
    log.write_text("")
    status = supervise(
        99,
        log,
        max_bytes=1024,
        keep=1,
        log_fd=None,
        tick_seconds=0.0,
        sleep=lambda _seconds: None,
        waitpid=lambda _pid, _flags: (99, 256),
    )

    assert status == 256


# --------------------------------------------------------------------------
# Restarting a crashed server, and making every restart loud


def waits(*statuses: int, alive_ticks: int = 0):
    """A `waitpid` that reports each status in turn, one child at a time."""

    queue = list(statuses)
    pending = [alive_ticks]

    def waitpid(pid: int, _flags: int):
        if pending[0] > 0:
            pending[0] -= 1
            return (0, 0)
        pending[0] = alive_ticks
        if not queue:
            raise ChildProcessError
        return (pid, queue.pop(0))

    return waitpid


def test_a_crashed_server_is_brought_back_and_the_restart_is_written_down(
    tmp_path: Path,
) -> None:
    """The whole trade: a resurrection is allowed, a silent one is not.

    A restart nobody can see is the objection that kept this out of the first
    round. So the count and the reason land in a durable file before the
    replacement starts, because the process that died cannot report anything and
    the one that comes back has no memory of having been restarted.
    """

    record = tmp_path / "server-8411.restarts.json"
    journal = server_launch.RestartJournal(record)
    spawned: list[int] = []

    def respawn() -> int:
        spawned.append(len(spawned) + 1)
        return 500 + len(spawned)

    status = supervise(
        499,
        tmp_path / "server.log",
        max_bytes=1 << 30,
        keep=1,
        log_fd=None,
        tick_seconds=0.0,
        backoff_seconds=0.0,
        respawn=respawn,
        journal=journal,
        sleep=lambda _seconds: None,
        # A crash, then a crash, then a clean exit that ends the series.
        waitpid=waits(256, int(signal.SIGKILL), 0),
    )

    assert status == 0
    assert spawned == [1, 2], "each crash must be replaced exactly once"
    history = server_launch.read_restart_history(record)
    # The clean exit cleared it: the series ended on purpose, so the next start
    # is a first start rather than the third of three.
    assert history.restarts == 0
    assert history.flapping is False


def test_the_record_names_the_signal_or_the_exit_code(tmp_path: Path) -> None:
    record = tmp_path / "restarts.json"
    journal = server_launch.RestartJournal(record)
    supervise(
        1,
        tmp_path / "server.log",
        max_bytes=1 << 30,
        keep=1,
        log_fd=None,
        tick_seconds=0.0,
        backoff_seconds=0.0,
        respawn=lambda: 2,
        journal=journal,
        sleep=lambda _seconds: None,
        waitpid=waits(int(signal.SIGKILL), int(signal.SIGTERM)),
    )

    history = server_launch.read_restart_history(record)
    # SIGTERM ended it deliberately, so the journal was cleared -- but the
    # SIGKILL that came first was recorded while it mattered.
    assert history.restarts == 0


def test_a_deliberate_stop_is_never_restarted(tmp_path: Path) -> None:
    """`antidemo reset` stops the server. A restarter that fought it would be the
    `KeepAlive` problem reinvented one layer down.
    """

    record = tmp_path / "restarts.json"
    for status in (0, int(signal.SIGTERM), int(signal.SIGINT), int(signal.SIGHUP)):
        respawned = False

        def respawn() -> int:
            nonlocal respawned
            respawned = True
            return 2

        assert (
            supervise(
                1,
                tmp_path / "server.log",
                max_bytes=1 << 30,
                keep=1,
                log_fd=None,
                tick_seconds=0.0,
                backoff_seconds=0.0,
                respawn=respawn,
                journal=server_launch.RestartJournal(record),
                sleep=lambda _seconds: None,
                waitpid=lambda pid, _flags, stopped=status: (pid, stopped),
            )
            == status
        )
        assert respawned is False, f"status {status} is a deliberate stop"


def test_a_flap_gives_up_and_says_so_instead_of_looping_forever(tmp_path: Path) -> None:
    """A crash loop restarting cannot fix must look broken, not busy.

    And the giving-up has to be in the record, because a supervisor that quietly
    stopped trying would be the same invisibility one step further along.
    """

    record = tmp_path / "restarts.json"
    journal = server_launch.RestartJournal(record)
    attempts = 0

    def respawn() -> int:
        nonlocal attempts
        attempts += 1
        return 100 + attempts

    status = supervise(
        99,
        tmp_path / "server.log",
        max_bytes=1 << 30,
        keep=1,
        log_fd=None,
        tick_seconds=0.0,
        backoff_seconds=0.0,
        max_restarts=3,
        respawn=respawn,
        journal=journal,
        sleep=lambda _seconds: None,
        waitpid=lambda pid, _flags: (pid, 256),
    )

    assert status == 256
    assert attempts == 3, "it must stop at the budget rather than loop"
    history = server_launch.read_restart_history(record)
    assert history.restarts == 3
    assert history.gave_up is True
    assert history.flapping is True
    assert "did not fix it" in history.last_reason


def test_an_old_restart_is_history_and_a_recent_one_is_a_flap(tmp_path: Path) -> None:
    record = tmp_path / "restarts.json"
    journal = server_launch.RestartJournal(record)
    journal.record("exit code 1", now=1_000.0)

    fresh = server_launch.read_restart_history(record, now=1_060.0, window=600.0)
    assert (fresh.restarts, fresh.recent, fresh.flapping) == (1, 1, True)

    stale = server_launch.read_restart_history(record, now=1_000_000.0, window=600.0)
    assert (stale.restarts, stale.recent, stale.flapping) == (1, 0, False)


def test_a_replacement_supervisor_inherits_the_flap_budget_it_is_continuing(
    tmp_path: Path,
) -> None:
    """`/readyz` and the give-up check must be counting the same deaths.

    A supervisor that carried only the total forward started every run with a
    fresh budget, so an operator re-running `antidemo serve --background` after a
    give-up got a whole new run of restarts before the next one -- while
    `/readyz` was still, correctly, reporting `supervisor_gave_up`. The two
    surfaces disagreed about the same crash loop.
    """

    record = tmp_path / "restarts.json"
    # Real epoch seconds, because the journal prunes what it reads against the
    # wall clock -- the same clock the supervisor that wrote them was on.
    base = time.time()
    first = server_launch.RestartJournal(record, window=600.0)
    for offset in (-30.0, -20.0, -10.0):
        first.record("exit code 1", now=base + offset)

    replacement = server_launch.RestartJournal(record, window=600.0)
    assert replacement.recent(base) == 3, "the deaths already recorded still count"
    replacement.record("exit code 1", now=base)
    assert replacement.recent(base) == 4

    # The window still prunes: a series that ended long ago is history, and a
    # fresh budget is the right answer then.
    aged = tmp_path / "aged.json"
    old = server_launch.RestartJournal(aged, window=600.0)
    old.record("exit code 1", now=base - 10_000.0)
    assert server_launch.RestartJournal(aged, window=600.0).recent(base) == 0


def test_ready_is_qualified_when_the_server_is_up_and_refusing(capsys) -> None:
    """`READY` is a liveness answer, and it used to be the only answer.

    `/api/health` is a static literal on purpose -- a liveness probe that depended
    on anything external would report the dependency rather than the process -- so
    a server serving 503 on `/readyz` and refusing every control action with 409
    printed `READY http://…` and nothing else. That is the one thing an operator
    reading this output cannot afford to be wrong about.
    """

    server_launch._announce_degraded(
        lambda _host, _port: {
            "degraded": True,
            "degraded_detail": "AWS REJECTED THE CREDENTIALS IN THIS PROCESS",
            "degraded_capabilities": ["Every AWS lane"],
        },
        "127.0.0.1",
        8001,
    )

    reported = capsys.readouterr().err
    assert "DEGRADED" in reported
    assert "AWS REJECTED THE CREDENTIALS IN THIS PROCESS" in reported
    assert "Every AWS lane" in reported


@pytest.mark.parametrize(
    "readiness",
    [
        None,
        lambda _host, _port: {"degraded": False, "degraded_detail": None},
        lambda _host, _port: None,
        lambda _host, _port: (_ for _ in ()).throw(OSError("no route to host")),
    ],
    ids=["not-wired", "ready", "unreadable", "raises"],
)
def test_a_healthy_or_unreadable_readiness_surface_adds_nothing(readiness, capsys) -> None:
    """Advisory only. A readiness read that fails must not fail a started server."""

    server_launch._announce_degraded(readiness, "127.0.0.1", 8001)
    assert capsys.readouterr().err == ""


def test_a_corrupt_or_missing_record_reads_as_no_history(tmp_path: Path) -> None:
    """A counter file must never be able to stop a server from starting."""

    assert server_launch.read_restart_history(tmp_path / "absent.json").restarts == 0
    assert server_launch.read_restart_history(None).restarts == 0
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert server_launch.read_restart_history(broken).restarts == 0
    wrong_shape = tmp_path / "list.json"
    wrong_shape.write_text('["nope"]', encoding="utf-8")
    assert server_launch.read_restart_history(wrong_shape).restarts == 0


def wait_status_of(script: str, stop: signal.Signals | None = None) -> int:
    """Run a real process, optionally signal it, and return its wait status.

    Every case below goes through this rather than through a hand-built integer,
    and that is the point of the test rather than a detail of it. The version
    that built its own statuses passed `int(signal.SIGTERM)` -- a raw
    signal-*death* status -- for the SIGTERM case, and so asserted that the
    supervisor handles a shape the running system never produces. What it does
    produce is an exit code: `serve_command` runs `uv run --no-sync uvicorn`, uv
    spawns the server rather than exec'ing it, and a signal that stops the server
    reaches the supervisor as uv's `128 + N`. The synthetic status hid that, and
    a SIGTERM to the pid the launch record advertises restarted the server it had
    just stopped. So the statuses here are produced by processes that really do
    trap a signal and exit, or really are killed, or really are a runner
    reporting a killed child.

    `stop` is sent once the script has said it is ready, which is what keeps the
    signal from arriving before the trap is installed.
    """
    child = subprocess.Popen(["/bin/sh", "-c", script], stdout=subprocess.PIPE)
    try:
        if stop is not None:
            assert child.stdout is not None
            assert child.stdout.readline().strip() == b"ready"
            os.kill(child.pid, stop)
        return os.waitpid(child.pid, 0)[1]
    finally:
        if child.stdout is not None:
            child.stdout.close()
        child.returncode = 0  # already reaped above; keep Popen's __del__ quiet


#: A process that installs a handler for `stop`, cleans up, and exits `128 + N`
#: of its own accord -- uvicorn's shape, which re-raises the signal after a
#: graceful shutdown and is then reported this way by the uv that ran it. The
#: busy loop is deliberate: `sh` defers a trap until the foreground command
#: finishes, so a single long `sleep` would swallow the signal for its duration.
def traps_and_exits(stop: signal.Signals) -> str:
    return (
        f'trap "exit {128 + int(stop)}" {stop.name.removeprefix("SIG")}; '
        "echo ready; while :; do sleep 0.02; done"
    )


#: A runner whose child was killed, reporting it the way every runner does. This
#: is what an OOM kill looks like by the time the supervisor sees it, and it must
#: still read as a crash even though it arrives in the same `128 + N` band.
RUNNER_REPORTING_A_KILLED_CHILD = "sleep 30 & kill -KILL $!; wait $!; exit $?"


@pytest.mark.parametrize(
    ("script", "stop", "reason", "deliberate"),
    [
        ("exit 0", None, "a clean exit", True),
        ("exit 1", None, "exit code 1", False),
        # The four an operator reaches for, in the shape the running system
        # actually produces: trapped, cleaned up, reported as an exit code.
        (
            traps_and_exits(signal.SIGTERM),
            signal.SIGTERM,
            "SIGTERM (15) reported as exit code 143",
            True,
        ),
        (
            traps_and_exits(signal.SIGINT),
            signal.SIGINT,
            "SIGINT (2) reported as exit code 130",
            True,
        ),
        (
            traps_and_exits(signal.SIGHUP),
            signal.SIGHUP,
            "SIGHUP (1) reported as exit code 129",
            True,
        ),
        (
            traps_and_exits(signal.SIGQUIT),
            signal.SIGQUIT,
            "SIGQUIT (3) reported as exit code 131",
            True,
        ),
        # A crash must stay a crash, however it is dressed. The first is an OOM
        # kill as a runner reports it, and it sits in the same band as the four
        # above; the last two are what reaches us when the runner itself, rather
        # than the server, is what died.
        (RUNNER_REPORTING_A_KILLED_CHILD, None, "exit code 137", False),
        ("echo ready; while :; do sleep 0.02; done", signal.SIGKILL, "SIGKILL (9)", False),
        ("kill -SEGV $$", None, "SIGSEGV (11)", False),
        # Still reachable, and still deliberate: a signal aimed at the runner
        # itself, which has no handler for it and dies of it.
        ("echo ready; while :; do sleep 0.02; done", signal.SIGTERM, "SIGTERM (15)", True),
    ],
    ids=[
        "clean-exit",
        "exit-1",
        "trapped-sigterm",
        "trapped-sigint",
        "trapped-sighup",
        "trapped-sigquit",
        "oom-kill-through-a-runner",
        "sigkill",
        "sigsegv",
        "signal-death",
    ],
)
def test_why_the_server_stopped_and_whether_that_was_on_purpose(
    script: str,
    stop: signal.Signals | None,
    reason: str,
    deliberate: bool,
) -> None:
    """One function answers both, so a record cannot disagree with a decision."""

    assert server_launch.describe_exit(wait_status_of(script, stop)) == (reason, deliberate)


def test_restarting_is_on_unless_it_is_turned_off() -> None:
    assert server_launch.restart_enabled({}) is True
    assert server_launch.restart_enabled({server_launch.RESTART_ENV: "0"}) is False
    assert server_launch.restart_enabled({server_launch.RESTART_ENV: "off"}) is False
    assert server_launch.restart_enabled({server_launch.RESTART_ENV: "1"}) is True


def test_the_record_sits_beside_the_launch_record_or_nowhere_at_all(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    path = server_launch.restart_record_path(
        {"ANTI_DEMO_MANIFEST": str(manifest), "ANTI_DEMO_SERVER_PORT": "8411"}
    )
    assert path == tmp_path / "server-8411.restarts.json"
    # No selected manifest is the same condition that means no launch record,
    # and it gets the same answer rather than a guessed directory.
    assert server_launch.restart_record_path({}) is None


def test_a_reaped_server_is_not_an_error(tmp_path: Path) -> None:
    def already_gone(_pid: int, _flags: int):
        raise ChildProcessError

    assert (
        supervise(
            7,
            tmp_path / "server.log",
            max_bytes=1024,
            keep=1,
            log_fd=None,
            tick_seconds=0.0,
            sleep=lambda _seconds: None,
            waitpid=already_gone,
        )
        == 0
    )


def test_a_signalled_server_is_reported_the_way_a_shell_reports_one() -> None:
    assert _exit_code(0) == 0
    assert _exit_code(1 << 8) == 1
    assert _exit_code(int(signal.SIGTERM)) == 128 + int(signal.SIGTERM)


# --------------------------------------------------------------------------
# Where the log goes, from the CLI's point of view
# --------------------------------------------------------------------------


def test_serve_accepts_a_background_mode_and_a_log_location() -> None:
    arguments = _parser().parse_args(
        ["serve", "--background", "--port", "8123", "--log", "/tmp/x.log"]
    )

    assert arguments.background is True
    assert arguments.log == "/tmp/x.log"
    # The foreground default has to stay the default: `antidemo setup` ends in it.
    assert _parser().parse_args(["serve"]).background is False


def test_an_explicit_log_location_outranks_the_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = tmp_path / "gen"
    generation.mkdir()
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation / "manifest.json"))

    assert _serve_log_path(8123, str(tmp_path / "chosen.log")) == tmp_path / "chosen.log"
    assert _serve_log_path(8123, "") == generation / "server-8123.log"


def test_a_background_server_with_nowhere_to_log_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A log nobody can find is how the last workaround hid half a broken demo."""

    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)

    with pytest.raises(RuntimeError) as failure:
        _serve_log_path(8123, "")

    assert "--log" in str(failure.value)
    assert "ANTI_DEMO_MANIFEST" in str(failure.value)


# --------------------------------------------------------------------------
# The property that must not regress
# --------------------------------------------------------------------------


def test_the_daemon_closes_every_inherited_descriptor_before_it_spawns() -> None:
    """The generation lock is why.

    `flock` lives on the open file description, so it survives `fork` even
    though `generation_lock.py` marks the descriptor close-on-exec. A daemon
    that inherited a held one would keep it for the life of the demo and
    deadlock every later mutation. Closing an inherited copy cannot release the
    claim of whatever opened it, so this is safe as well as necessary.
    """

    import inspect

    source = inspect.getsource(server_launch._run_daemon)
    close_all = source.index("os.closerange(3,")
    spawn = source.index("subprocess.Popen(")
    assert close_all < spawn, "spawning before closing would hand the lock to the server"
