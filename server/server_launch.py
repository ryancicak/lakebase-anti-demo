"""Start the orchestrator so it outlives the shell, and keep its log bounded.

Three hazards, all of which have already happened to this installation.

**A resolve at serve time.** ``uv run`` re-resolves whenever the environment
drifts, and it does it at the moment the operator is reaching for the tool. A
stray ``.python-version`` pinning 3.12 was enough: uv deleted the provisioned
3.14 environment and started rebuilding it, which from the outside is
indistinguishable from a hang, seconds before a demo. So nothing here resolves.
``--no-sync`` downgrades that to a warning and uses the environment as it
stands. The cost of never resolving is that a genuinely *unprovisioned*
environment has to be caught by hand, because ``uv run --no-sync`` will still
create an empty virtualenv and then fail with ``No module named uvicorn`` --
which is the same silent-failure shape this file exists to remove. Hence
:func:`require_serving_environment`, which refuses first and names ``uv sync``.

**A server tied to the shell that started it.** ``os.execvp`` keeps the caller's
process group, and ``nohup`` only ignores SIGHUP, so closing a terminal or
ending a shell session still tears the server down with the group. Surviving
that needs a session of its own, which needs the double fork in
:func:`daemonize`. An out-of-tree script that did this correctly but hardcoded
one machine's paths already existed; because it exec'd uvicorn directly it
bypassed ``cli.py:_serve`` and therefore ``apply_manifest_environment``, and the
coordination store silently fell back to memory. The supported path has to be
good enough that nobody writes that script again.

**A log that grows for weeks.** The process is meant to live for weeks, so
rolling the file only at startup would never roll it at all. The daemon
therefore keeps a supervisor: the parent of the server, whose only jobs are to
wait for it and to roll the log while it runs. Rolling is copy-then-truncate,
not rename: the server inherited *this* descriptor, and renaming the file would
leave it appending to an inode with no name for the rest of the month. The
descriptor is opened ``O_APPEND``, which is what makes truncation safe -- an
appending writer recomputes its offset on every write, so it resumes at zero
instead of leaving a hole the size of the file that was rolled away.

Nothing here may end up holding the generation lock. ``flock`` lives on the open
file description, so it survives ``fork`` even though ``generation_lock.py``
marks the descriptor close-on-exec; a daemon that inherited one would hold it
for the life of the demo and deadlock every future mutation. Two things prevent
that: the caller does its one manifest write and releases the lock *before*
calling in here, and the daemon closes every descriptor above stderr before it
spawns anything. That second step also releases nothing it should not -- a lock
belongs to the open file description, so closing an inherited copy while the
shell that opened it still holds its own leaves that shell's claim intact.

**Restart-after-crash lives here, and not in a launchd plist.** The two things a
supervisor artifact buys are different problems and they separate cleanly.
Restart-after-crash needs a process watching the server, which this daemon
already is; doing it here needs no artifact, no absolute path, and no assumption
about which init system the box runs. Start-at-boot genuinely does need an
artifact, and that is the only part left outstanding.

Three earlier objections to restarting at all, and what became of them:

* *An SSO session expires in hours, so a restarted server would come up
  credentialed to nothing.* Retired by the owner's decision: long-running
  installs authenticate with long-lived IAM keys, and a restarted child inherits
  this daemon's environment, which is the environment that started it. What is
  left of the concern -- keys revoked or rotated out from under a live process --
  is now watched by ``server/aws_credential_probe.py`` and reported through
  ``/readyz``.
* *A restart during ``antidemo reset`` would come up against a manifest mid-rewrite.*
  Retired: ``app.py`` now serves a truthful waiting state for a transitional
  status, refuses control actions with a 409 naming it, and starts normally when
  the status becomes servable -- without ever writing the manifest. And a
  deliberate stop is not restarted at all: see below.
* *This demo's failures are the product, so resurrecting a crashed server hides
  why it went down.* This one was over-stated, and the correction matters. The
  failures that are the product are measurements -- a cold start, a lane that
  cannot scale to zero, a competitor that folds under a connection spike -- and
  every one of them is rendered on screen by a *running* server. A uvicorn
  process dying of an unhandled exception or an OOM kill is not a finding about
  Aurora; it is an outage that erases the findings. What survives the correction
  is the visibility half, and that is answered rather than dismissed: every
  restart is counted and reasoned in a durable record beside the log, which the
  process that comes back reads and republishes through the same ``degraded``
  vocabulary. A flapping server is loud. A silently resurrected one is what this
  is designed not to be.

Two bounds keep the restarter honest. A **deliberate stop is final**: a child
that exits cleanly, or on the signal an operator sends it, is not brought back,
so ``antidemo reset`` and an operator's ``kill`` both still work and nothing fights
them. Recognising that stop is subtler than it sounds, because the child waited
on here is ``uv`` and the pid an operator is given is uvicorn one level further
down; :func:`describe_exit` carries the reasoning. And a **flap gives up**: more
than :data:`RESTART_MAX_IN_WINDOW` deaths
inside :data:`RESTART_WINDOW_SECONDS` stops the restarting and says so in the
record, because a crash loop that restarting cannot fix must not be hidden
behind an infinite one.

**Why there is still no launchd plist here.** Only start-at-boot is left, and
it needs a generated, path-agnostic artifact -- the objection to
``.launch8080.py`` was hardcoded paths, and a hand-written plist would carry the
same defect one directory deeper. Generating one belongs behind a ``antidemo``
subcommand so the paths come from the running installation rather than from
whoever typed the file, and that lands in ``server/cli.py``. The template and
the patch are in the report; a plausible-looking plist committed without them is
worse than none.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from logging import WARNING, Formatter, Handler, StreamHandler, getLogger
from pathlib import Path

from .manifest import PROJECT_ROOT
from .process_registry import (
    SERVER_HOST_ENV,
    SERVER_LOG_PATH_ENV,
    SERVER_PORT_ENV,
    SUPERVISOR_PID_ENV,
    state_dir_from_environ,
)

#: Where uv puts the project environment unless told otherwise.
DEFAULT_ENVIRONMENT_DIR = ".venv"
ENVIRONMENT_DIR_ENV = "UV_PROJECT_ENVIRONMENT"

LOG_MAX_BYTES_ENV = "ANTI_DEMO_LOG_MAX_BYTES"
LOG_KEEP_ENV = "ANTI_DEMO_LOG_KEEP"

#: 8 MiB times five rolls caps one port's logs at 40 MiB. Measured against the
#: real thing: a server writes on the order of 2 MB a day, so this is weeks of
#: history rather than an arbitrary round number.
DEFAULT_LOG_MAX_BYTES = 8 * 1024 * 1024
DEFAULT_LOG_KEEP = 5

#: How often the supervisor looks at the log. The cap is only enforced when
#: somebody looks, so this interval *is* the overshoot: at 30s a server logging
#: hard rolled 18 MB past a 64 KB cap in a measured run. Looking often is close
#: to free -- one `fstat` -- and does not multiply the copying, because a roll
#: only happens once the file has grown past the cap again, so the number of
#: copies is set by how much is written and not by how often we check.
ROTATION_INTERVAL_SECONDS = 2.0

#: How often the supervisor asks whether the server has exited. Shorter than the
#: rotation interval: with one timer for both, an operator who stopped the server
#: and immediately ran `antidemo status` saw the supervisor still sitting there for
#: up to half a minute, which reads as a failed shutdown.
REAP_TICK_SECONDS = 0.5

#: How long the foreground process waits for the daemon to answer /api/health
#: before it reports what the log says instead.
STARTUP_GRACE_SECONDS = 45.0

RESTART_ENV = "ANTI_DEMO_SUPERVISOR_RESTART"

#: A crash loop that restarting cannot fix has to stop being restarted, or the
#: record fills with identical entries and the operator learns nothing except
#: that something is wrong. Six deaths inside ten minutes is well past any
#: transient cause -- a port that is still held, a manifest that cannot be
#: parsed, an import error -- and every one of those needs a human.
RESTART_MAX_IN_WINDOW = 6
RESTART_WINDOW_SECONDS = 600.0

#: A moment between restarts, so a server that dies instantly on startup does
#: not spin the CPU while it uses up its budget.
RESTART_BACKOFF_SECONDS = 2.0

#: Signals an operator sends to stop a server. A child stopped by one of these
#: is a deliberate stop and is never brought back: `antidemo reset` stops the server
#: this way, and a restarter that fought it would be the `KeepAlive` problem
#: reinvented one layer down.
DELIBERATE_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT)

#: What a runner reports when the process *it* was running died of a signal:
#: 128 plus the signal number, the convention every shell prints. Load-bearing
#: here rather than cosmetic, because ``uv run`` sits between this supervisor and
#: uvicorn -- see :func:`describe_exit`.
SIGNAL_EXIT_CODE_BASE = 128


#: When, how bad, and which lane wrote it -- the three things needed to triage a
#: line, and the three `logging.lastResort` omits. The level is padded so a wall
#: of these stays scannable next to uvicorn's own `INFO:     ` prefix.
OPERATOR_LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"

#: UTC, and the same ISO-8601 spelling as every other instant this project
#: writes down -- receipts, the restart journal, `startup-reap.jsonl` -- so a log
#: line and a receipt can be lined up without converting anything.
OPERATOR_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

#: Marks the handler as ours, so configuring twice is a no-op. `app.py`
#: configures at import and `cli.py` at entry, and `antidemo serve` in the
#: foreground runs both in one process.
_OPERATOR_HANDLER_FLAG = "_anti_demo_operator_log"


class _OperatorStderrHandler(StreamHandler):
    """A stderr handler that resolves ``sys.stderr`` when it writes, not when built.

    Copied in shape from ``logging._StderrHandler``, which is what
    ``logging.lastResort`` is, and for the same reason: whoever configures
    logging is not necessarily running when the line is written. Binding the
    stream at construction would have pinned whatever ``sys.stderr`` happened to
    be at import, which under the daemon is the right file only by luck of
    ordering -- ``_run_daemon`` redirects before it spawns, but a foreground
    ``antidemo serve`` and the test suite both reassign it afterwards.
    """

    def __init__(self, level: int = WARNING) -> None:
        # Deliberately not `StreamHandler.__init__`: that assigns `self.stream`,
        # which is a read-only property here.
        Handler.__init__(self, level)

    @property
    def stream(self):  # type: ignore[override]
        return sys.stderr


def configure_operator_logging(level: int = WARNING) -> None:
    """Make module-level warnings and errors legible wherever this process logs.

    **What was actually wrong.** Nothing in this repository configures logging,
    and the conclusion drawn from that -- that warnings vanish -- was wrong.
    ``logging.lastResort`` emits ``WARNING`` and above to stderr with no
    configuration at all, and ``_run_daemon`` points stderr at
    ``server-<port>.log``, so ``ORPHAN RISK`` has been reaching the server log
    the whole time. What it has not been doing is *saying* so: ``lastResort``
    formats with a bare ``%(message)s``, so the line carries no level, no
    timestamp and no logger name. An operator grepping a log for ``ERROR``
    matched nothing, concluded the signal did not exist, and was reading a file
    that contained it. A seven-bout campaign reached exactly that conclusion.

    So this replaces an implicit, unlabelled fallback with an explicit, labelled
    handler at *the same threshold*. That symmetry is the design:

    * ``WARNING`` and above, because that is precisely what ``lastResort``
      passed. Nothing that is quiet today becomes loud, which matters in a
      codebase whose retry loops log every five seconds on purpose -- surfacing
      library ``DEBUG`` would bury the one line this exists to reveal, which is
      the same failure pointing the other way.
    * stderr, because that is where ``lastResort`` wrote and where both
      surfaces already look: the daemon's stderr *is* the server log, and
      Databricks Apps captures stderr into the stream ``/logz`` serves.
    * The root logger only, and propagation is never disabled. ``caplog``
      attaches to root as well, so every existing assertion keeps working, and
      a future handler can still be added beside this one.

    **It cannot break a bout.** Every failure here is swallowed: a diagnostic
    that raises is worse than no diagnostic, and this one runs on the startup
    path of a process whose job is to measure something else. A write that
    fails -- a full disk under the log -- is absorbed by ``logging`` itself,
    which is why the level that says a resource is still billing cannot become
    the reason a towel fails.

    **It adds no new exception text.** The formatter renders ``exc_info``
    exactly as ``lastResort``'s default formatter already did, so the quoting
    boundary is untouched: this widens no path by which a psycopg DSN or a
    secret ARN could reach the log that was not already open.
    """

    try:
        root = getLogger()
        for existing in root.handlers:
            if getattr(existing, _OPERATOR_HANDLER_FLAG, False):
                return
        formatter = Formatter(OPERATOR_LOG_FORMAT, OPERATOR_LOG_DATE_FORMAT)
        # UTC. A server that logs in the operator's local time is unreadable the
        # moment two people in two timezones compare notes about one bout.
        formatter.converter = time.gmtime
        handler = _OperatorStderrHandler(level)
        handler.setFormatter(formatter)
        setattr(handler, _OPERATOR_HANDLER_FLAG, True)
        root.addHandler(handler)
        # Only ever lowered, and only to the threshold `lastResort` already
        # applied. NOTSET is left alone -- it lets everything through, and
        # raising it here would silence records that reach the log today.
        if root.level > level:
            root.setLevel(level)
    except Exception:  # noqa: BLE001 - a diagnostic may never break a bout
        return


def restart_record_path(environ: Mapping[str, str] | None = None) -> Path | None:
    """Where the restart history for this port lives, or ``None`` if nowhere.

    Beside the launch record and named the same way, so the supervisor that
    writes it and the server that reads it derive the same path from the same
    two environment variables rather than passing one to the other. ``None``
    when there is no selected manifest to sit beside -- the same condition that
    makes there be no launch record, and answered the same way: the feature is
    absent rather than improvised into some other directory.
    """
    environ = os.environ if environ is None else environ
    state_dir = state_dir_from_environ(environ)
    if state_dir is None:
        return None
    port = (environ.get(SERVER_PORT_ENV) or "").strip() or "unknown"
    return state_dir / f"server-{port}.restarts.json"


def restart_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Restarting is on unless an operator turns it off.

    On by default because the mode this runs in exists to survive weeks
    unattended, and a crash at 3am with nobody watching is the case it is for.
    """
    environ = os.environ if environ is None else environ
    raw = (environ.get(RESTART_ENV) or "").strip().casefold()
    return raw not in {"0", "off", "no", "false", "never"}


def _stop_signal_reported_as(code: int) -> signal.Signals | None:
    """Which stop signal a positive exit code stands for, if it stands for one.

    Only the four an operator sends to stop a server. ``137`` (SIGKILL, which is
    what an OOM kill looks like from here) and ``139`` (SIGSEGV) arrive by the
    very same convention and must keep reading as crashes.
    """
    for one in DELIBERATE_STOP_SIGNALS:
        if code == SIGNAL_EXIT_CODE_BASE + int(one):
            return one
    return None


def describe_exit(status: int) -> tuple[str, bool]:
    """Say why the server stopped, and whether that was somebody's decision.

    The boolean is what decides a restart, so the two answers are computed in
    one place: a reason string that disagreed with the restart decision would be
    a record that lies about why the process came back.

    **A stop signal usually arrives as an exit code, not as a signal death, and
    reading only the second is what made this supervisor fight an operator.**
    The child waited on here is ``uv``, not uvicorn: ``serve_command`` runs
    ``uv run --no-sync uvicorn``, and uv spawns the server rather than exec'ing
    it. So a SIGTERM sent to the pid the launch record advertises kills a
    *grandchild*, and uv -- like every runner and every shell -- reports that
    death to us as exit code ``128 + 15``. Measured on this installation: a
    SIGTERM to the advertised pid produced ``exit code 143``, which this function
    called a crash, and the supervisor respawned the server the operator had just
    stopped, re-running startup reconciliation nobody asked for. SIGHUP and
    SIGQUIT arrived the same way, as 129 and 131.

    So classification cannot rest on the supervisor's own knowledge alone. It
    knows about the signals it relayed itself, but the discoverable path -- the
    pid on screen -- never touches the supervisor, and no portable mechanism lets
    a process learn how its grandchild died. That leaves the ``128 + N``
    convention, and the trade-off it carries: 143 means "stopped with SIGTERM" by
    convention, not by guarantee, so a server that chose to exit 143 on its own
    account is now read as a deliberate stop and is not restarted. That is the
    right way round to be wrong. Being wrong the other way fights the operator on
    the one command that must always work, and does it on *every* stop rather
    than on a contrived one; and the band is narrow -- only the four stop
    signals, so a crash still reads as a crash whether it exits 1, is OOM-killed
    (137) or segfaults (139). Both outcomes are named in the log either way, so
    neither is silent.
    """
    try:
        code = os.waitstatus_to_exitcode(status)
    except ValueError:
        return "an unreadable wait status", False
    if code >= 0:
        if code == 0:
            return "a clean exit", True
        stopped_by = _stop_signal_reported_as(code)
        if stopped_by is not None:
            return f"{stopped_by.name} ({int(stopped_by)}) reported as exit code {code}", True
        return f"exit code {code}", False
    number = -code
    try:
        name = signal.Signals(number).name
    except ValueError:
        name = f"signal {number}"
    deliberate = number in {int(one) for one in DELIBERATE_STOP_SIGNALS}
    return f"{name} ({number})", deliberate


@dataclass(frozen=True)
class RestartHistory:
    """What the record says, from the point of view of the process reading it."""

    restarts: int = 0
    recent: int = 0
    last_at: str | None = None
    last_reason: str | None = None
    gave_up: bool = False
    # The stamps behind `recent`, not just their count. A replacement supervisor
    # has to inherit the flap budget it is continuing, and it can only do that
    # from the moments themselves -- see `RestartJournal.__init__`.
    recent_epoch_seconds: tuple[float, ...] = ()

    @property
    def flapping(self) -> bool:
        """Is this process one of a series, rather than one that has been up?

        A restart three weeks ago is history. One inside the window means the
        process answering right now is a replacement, which is the thing a
        monitor and an operator both need told.
        """
        return self.recent > 0 or self.gave_up


def read_restart_history(
    path: Path | None,
    *,
    now: float | None = None,
    window: float = RESTART_WINDOW_SECONDS,
) -> RestartHistory:
    """Read the record, tolerating every way it can be absent or malformed.

    A server must not fail to start because a counter file is corrupt, so every
    failure here reads as "no history", which is also the truth on a first run.
    """
    if path is None:
        return RestartHistory()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return RestartHistory()
    if not isinstance(raw, dict):
        return RestartHistory()
    stamps = raw.get("recent_epoch_seconds")
    stamps = [float(one) for one in stamps if isinstance(one, int | float)] if (
        isinstance(stamps, list)
    ) else []
    moment = time.time() if now is None else now
    within = tuple(one for one in stamps if moment - one <= window)
    return RestartHistory(
        restarts=int(raw.get("restarts") or 0),
        recent=len(within),
        last_at=raw.get("last_at") or None,
        last_reason=raw.get("last_reason") or None,
        gave_up=bool(raw.get("gave_up")),
        recent_epoch_seconds=within,
    )


class RestartJournal:
    """The durable half of "every restart is loud".

    Durable because the process that needs to report the restart is not the
    process that observed it: the one that died cannot report anything, and the
    one that comes back has no memory of having been restarted. So the
    supervisor writes it down and the replacement reads it.

    Written by the supervisor, which is not the serving process and never the
    holder of the mutation lock. This is not the manifest and is never treated
    as installation state -- losing it costs a restart count, nothing more.
    """

    def __init__(self, path: Path, *, window: float = RESTART_WINDOW_SECONDS) -> None:
        self.path = path
        self._window = window
        self._restarts = 0
        self._stamps: list[float] = []
        self._existing = read_restart_history(path, window=window)
        self._restarts = self._existing.restarts
        # Inherited, not restarted. Carrying only the total forward gave every
        # new supervisor a fresh budget: an operator who re-ran `antidemo serve
        # --background` after a give-up got another full run of deaths before the
        # next one, while `/readyz` was still reporting `supervisor_gave_up`. The
        # window prunes these on its own, so stale stamps cost nothing.
        self._stamps = list(self._existing.recent_epoch_seconds)

    def recent(self, now: float) -> int:
        return sum(1 for stamp in self._stamps if now - stamp <= self._window)

    def record(self, reason: str, *, now: float, gave_up: bool = False) -> None:
        self._restarts += 1
        self._stamps.append(now)
        self._write(reason=reason, gave_up=gave_up)

    def give_up(self, reason: str) -> None:
        self._write(reason=reason, gave_up=True)

    def clear(self) -> None:
        """Forget the history. Called when a server is stopped on purpose.

        A deliberate stop ends the series, so the next start is a first start
        and must not inherit a flap that has been dealt with.
        """
        with contextlib.suppress(OSError):
            self.path.unlink()

    def _write(self, *, reason: str, gave_up: bool) -> None:
        payload = {
            "restarts": self._restarts,
            "recent_epoch_seconds": self._stamps[-RESTART_MAX_IN_WINDOW * 2 :],
            "last_at": datetime.now(UTC).isoformat(),
            "last_reason": reason,
            "gave_up": gave_up,
        }
        # Best effort throughout: a supervisor that died trying to write its own
        # bookkeeping would be a monitoring feature taking down the thing it
        # monitors, which is the trade this whole module refuses to make.
        with contextlib.suppress(OSError, TypeError, ValueError):
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".json.new")
            temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, self.path)


def serve_command(host: str, port: int) -> list[str]:
    """The one argv both the foreground and the daemonized path serve through.

    Shared deliberately. Two spellings of "run uvicorn" is how one of them ends
    up without ``--no-sync``.
    """
    return [
        "uv",
        "run",
        # Not an optimisation. See the module docstring: a resolve here stalls
        # the demo it is about to start, and can replace the interpreter the
        # environment was provisioned with.
        "--no-sync",
        "uvicorn",
        "app:app",
        "--host",
        host,
        "--port",
        str(port),
    ]


def environment_dir(
    root: Path | None = None,
    environ: dict[str, str] | None = None,
) -> Path:
    """Where the project environment lives, honouring uv's own override."""
    root = PROJECT_ROOT if root is None else root
    environ = os.environ if environ is None else environ
    configured = (environ.get(ENVIRONMENT_DIR_ENV) or "").strip()
    if not configured:
        return root / DEFAULT_ENVIRONMENT_DIR
    candidate = Path(configured).expanduser()
    # uv reads a relative UV_PROJECT_ENVIRONMENT relative to the project root,
    # not to the current directory.
    return candidate if candidate.is_absolute() else root / candidate


def require_serving_environment(
    root: Path | None = None,
    environ: dict[str, str] | None = None,
) -> Path:
    """Refuse to start when the environment was never provisioned. Returns it.

    The check has to be explicit precisely *because* nothing here resolves.
    ``uv run --no-sync`` in a tree with no environment happily creates an empty
    one and then dies on ``No module named uvicorn``, which reads as a bug in
    this repository rather than as a missing install step.
    """
    directory = environment_dir(root, environ)
    entrypoint = directory / "bin" / "uvicorn"
    if entrypoint.exists():
        return directory
    raise RuntimeError(
        f"The project environment at {directory} has no uvicorn, so there is nothing "
        "to serve. 'antidemo serve' never resolves dependencies -- a resolve at serve "
        "time is what stalls the demo it is about to start -- so it will not build "
        "one for you. Run 'uv sync' and try again."
    )


def _positive_int(environ: dict[str, str], name: str, default: int, *, floor: int) -> int:
    raw = (environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer; got {raw!r}") from error
    if value < floor:
        raise RuntimeError(f"{name} must be at least {floor}; got {value}")
    return value


def log_limits(environ: dict[str, str] | None = None) -> tuple[int, int]:
    """(max bytes, rolls to keep). Refuses nonsense rather than falling back.

    A cap silently reset to the default because it was mistyped is the kind of
    setting an operator only discovers when the disk is full.
    """
    environ = os.environ if environ is None else environ
    # A cap below a few KB would roll on every request and bury the log in
    # rolls; a keep of zero is legitimate and means "truncate, keep nothing".
    return (
        _positive_int(environ, LOG_MAX_BYTES_ENV, DEFAULT_LOG_MAX_BYTES, floor=4096),
        _positive_int(environ, LOG_KEEP_ENV, DEFAULT_LOG_KEEP, floor=0),
    )


def default_log_path(port: int, environ: dict[str, str] | None = None) -> Path | None:
    """The log beside the manifest, or None when no generation is selected.

    Named after the port for the same reason the launch record is: two servers
    on two ports are two different things and must not share a file.
    """
    state_dir = state_dir_from_environ(environ)
    return None if state_dir is None else state_dir / f"server-{port}.log"


def roll_path(path: Path, index: int) -> Path:
    return path.with_name(f"{path.name}.{index}")


def _shift_rolls(path: Path, keep: int) -> None:
    """Make room at `.1` by moving every existing roll one place older."""
    with contextlib.suppress(OSError):
        roll_path(path, keep).unlink(missing_ok=True)
    for index in range(keep - 1, 0, -1):
        source = roll_path(path, index)
        with contextlib.suppress(OSError):
            if source.exists():
                source.replace(roll_path(path, index + 1))


def rotate_log(path: Path, *, keep: int, fd: int | None = None) -> None:
    """Roll `path` aside without moving the inode any live writer has open.

    Copy-then-truncate, which is what ``logrotate`` calls ``copytruncate`` and
    is the only correct choice here: the serving process inherited this exact
    descriptor at launch and cannot be told to reopen. The window between the
    copy and the truncate can lose a line written inside it. That is the known
    cost of the technique, and it buys never orphaning a writer.
    """
    if keep > 0:
        _shift_rolls(path, keep)
        try:
            shutil.copyfile(path, roll_path(path, 1))
            roll_path(path, 1).chmod(0o600)
        except OSError:
            # A roll that cannot be written must not stop the truncate; the
            # point of the cap is that the disk stays bounded either way.
            pass
    if fd is None:
        with contextlib.suppress(OSError), open(path, "r+b") as handle:
            handle.truncate(0)
        return
    with contextlib.suppress(OSError):
        os.ftruncate(fd, 0)


def rotate_if_needed(
    path: Path,
    *,
    max_bytes: int,
    keep: int,
    fd: int | None = None,
) -> bool:
    """Roll the log when it has outgrown the cap. True when it rolled."""
    try:
        size = os.fstat(fd).st_size if fd is not None else path.stat().st_size
    except OSError:
        return False
    if size < max_bytes:
        return False
    rotate_log(path, keep=keep, fd=fd)
    return True


def open_log(path: Path, *, max_bytes: int, keep: int) -> int:
    """Open the log for appending, rolling it first if it is already too big.

    ``O_APPEND`` is load-bearing twice over: it keeps the server and the
    supervisor from overwriting each other, and it is what lets a later
    truncate-in-place resume at zero instead of writing into a hole.
    """
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    # O_CREAT's mode applies only to a file this call created, and a log carries
    # endpoint names, identities and query text. Narrow an inherited one too.
    with contextlib.suppress(OSError):
        os.fchmod(descriptor, 0o600)
    rotate_if_needed(path, max_bytes=max_bytes, keep=keep, fd=descriptor)
    return descriptor


def log_tail(path: Path, lines: int = 20) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return "\n".join(text.splitlines()[-lines:])


def _highest_descriptor() -> int:
    try:
        limit = os.sysconf("SC_OPEN_MAX")
    except (OSError, ValueError):
        limit = 0
    if not isinstance(limit, int) or limit <= 3:
        limit = 4096
    # Closing tens of thousands of descriptors that were never opened is pure
    # waste; anything this process actually inherited is far below the cap.
    return min(limit, 4096)


def supervise(
    child_pid: int,
    log_path: Path,
    *,
    max_bytes: int,
    keep: int,
    # Descriptor 1, because the daemon has already pointed its own stdout at the
    # log. Passing None makes it look the file up by path instead, which is what
    # a test wants and what nothing in production does.
    log_fd: int | None = 1,
    rotation_interval: float = ROTATION_INTERVAL_SECONDS,
    tick_seconds: float = REAP_TICK_SECONDS,
    respawn: Callable[[], int] | None = None,
    journal: RestartJournal | None = None,
    max_restarts: int = RESTART_MAX_IN_WINDOW,
    backoff_seconds: float = RESTART_BACKOFF_SECONDS,
    sleep=time.sleep,
    waitpid=os.waitpid,
    monotonic=time.monotonic,
    wall_clock=time.time,
) -> int:
    """Wait for the server, roll its log, and bring it back if it crashes.

    Returns the wait status of the last child it will not replace. ``respawn``
    is the only way it can start another one, so passing ``None`` gives the
    earlier wait-and-rotate behaviour exactly.

    Three rules, in the order they are applied:

    1. A **deliberate stop** is final. A clean exit, an operator's signal to this
       process, or a stop signal reaching the server itself -- which arrives here
       as an exit code, not as a signal death -- ends the loop, so nothing here
       fights ``antidemo reset`` or a ``kill``, whichever pid it was aimed at.
    2. A **crash** is recorded and replaced. Recorded first: if the replacement
       dies before it can be recorded, the record still names the death that
       started the series.
    3. A **flap gives up**. Past the budget, the loop stops and the record says
       it stopped, because a server nobody is watching that cannot stay up needs
       to look broken rather than busy.
    """

    stopping = False

    def relay(signal_number: int, _frame: object) -> None:
        # The operator signals the pid in the launch record, which is the server
        # itself, so this path is for whoever signals the supervisor instead.
        # Either way, being asked to stop ends any thought of restarting: a
        # supervisor that replaced the child it was just told to kill would be
        # unkillable without a second signal.
        nonlocal stopping
        stopping = True
        with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
            os.kill(child_pid, signal_number)

    # All four, not just the two a terminal sends. An operator who signals the
    # supervisor with SIGHUP or SIGQUIT was reaching for the same thing, and the
    # default disposition of both is to kill this process -- which would leave
    # the server and its runner alive with nothing watching them.
    for number in DELIBERATE_STOP_SIGNALS:
        with contextlib.suppress(ValueError, OSError):
            signal.signal(number, relay)

    next_rotation = monotonic() + rotation_interval
    while True:
        try:
            finished, status = waitpid(child_pid, os.WNOHANG)
        except ChildProcessError:
            return 0
        if finished:
            reason, deliberate = describe_exit(status)
            if deliberate or stopping or respawn is None:
                if deliberate or stopping:
                    if journal is not None:
                        # The series is over and was ended on purpose, so the
                        # next start is a first start rather than the sixth of
                        # six.
                        journal.clear()
                    # Said out loud, because this is the branch that decides not
                    # to act. A stop the operator asked for and a server that
                    # exited 143 for its own reasons are indistinguishable from
                    # here (see `describe_exit`), and the log line is what keeps
                    # the second one from being a silent disappearance.
                    print(
                        f"SUPERVISOR the server stopped with {reason}; "
                        "a deliberate stop is final, so it stays stopped",
                        flush=True,
                    )
                return status
            now = wall_clock()
            if journal is not None:
                if journal.recent(now) >= max_restarts:
                    journal.give_up(
                        f"{reason}, and {max_restarts} restarts inside "
                        f"{RESTART_WINDOW_SECONDS:.0f}s did not fix it"
                    )
                    return status
                journal.record(reason, now=now)
            print(
                f"SUPERVISOR the server stopped with {reason}; restarting it",
                flush=True,
            )
            sleep(backoff_seconds)
            child_pid = respawn()
            next_rotation = monotonic() + rotation_interval
            continue
        sleep(tick_seconds)
        if monotonic() >= next_rotation:
            next_rotation = monotonic() + rotation_interval
            rotate_if_needed(log_path, max_bytes=max_bytes, keep=keep, fd=log_fd)


def _detach_session() -> None:
    """Leave the caller's session for good. Returns only in the final child.

    Two forks in total, counting the one the caller already did. The first put
    this process somewhere that is not a process group leader, which is what
    lets ``setsid`` succeed; the second gives up session leadership, and only a
    session leader can ever be handed a controlling terminal. Without that
    second fork the daemon could reacquire one and be signalled with it again,
    which is the whole failure being fixed.
    """
    os.setsid()
    if os.fork() > 0:
        os._exit(0)


def _run_daemon(
    command: list[str],
    *,
    log_path: Path,
    root: Path,
    max_bytes: int,
    keep: int,
    announce: int,
) -> None:
    """The daemon: redirect, spawn the server, supervise it, never return."""
    os.chdir(root)
    os.umask(0o077)
    log_fd = open_log(log_path, max_bytes=max_bytes, keep=keep)
    null_fd = os.open(os.devnull, os.O_RDONLY)
    os.dup2(null_fd, 0)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)
    for descriptor in (null_fd, log_fd):
        if descriptor > 2:
            os.close(descriptor)

    # Tell the caller which process to look for before the descriptor that
    # carries the answer is closed with everything else.
    with contextlib.suppress(OSError):
        os.write(announce, f"{os.getpid()}\n".encode())
    with contextlib.suppress(OSError):
        os.close(announce)

    # Everything above stderr, gone. The generation lock is the reason: it lives
    # on an open file description and so survives fork, and a copy inherited
    # from -- for instance -- a shell that opened it on descriptor 9 would be
    # held by this daemon for weeks and deadlock every mutation. Closing our
    # copy cannot release that shell's own claim.
    os.closerange(3, _highest_descriptor())

    # Set here, in the supervisor, and after the fork -- so it is inherited by
    # every server this process spawns and by nothing else. The operator's shell
    # never sees it, which is what makes an unset value a truthful "this server
    # has no supervisor" rather than an accident of where `antidemo serve` was run.
    #
    # This is the only way the server can learn it. Its own parent is the `uv`
    # shim, one below this process, so `os.getppid` in the server names the wrong
    # thing -- the labelling defect that made a launch record call the shim the
    # launcher. Announced through the pipe above under the same pid, so the
    # operator's screen and the launch record cannot disagree.
    os.environ[SUPERVISOR_PID_ENV] = str(os.getpid())

    def spawn() -> int:
        return subprocess.Popen(command, close_fds=True).pid

    restart = restart_enabled()
    record = restart_record_path()
    status = supervise(
        spawn(),
        log_path,
        max_bytes=max_bytes,
        keep=keep,
        respawn=spawn if restart else None,
        journal=RestartJournal(record) if restart and record is not None else None,
    )
    os._exit(_exit_code(status))


def _exit_code(status: int) -> int:
    """Turn a wait status into the exit code this daemon should carry."""
    try:
        code = os.waitstatus_to_exitcode(status)
    except ValueError:
        return 70
    # Negative means the server was signalled; report it the way a shell does.
    return code if code >= 0 else 128 - code


def serve_in_background(
    host: str,
    port: int,
    *,
    log_path: Path,
    root: Path | None = None,
    environ: dict[str, str] | None = None,
    online=None,
    readiness=None,
    grace_seconds: float = STARTUP_GRACE_SECONDS,
) -> int:
    """Start the server detached from this shell and report where it went.

    Returns the exit code for ``antidemo serve``. Called only after the caller has
    finished, and released, any manifest mutation: the fork below would carry a
    held ``flock`` into a process that lives for weeks.
    """
    root = PROJECT_ROOT if root is None else root
    environ = os.environ if environ is None else environ
    max_bytes, keep = log_limits(environ)
    command = serve_command(host, port)

    # Set in this process so the fork carries it. The server verifies the claim
    # against its own stdout before recording it, so a log path it is not
    # actually writing to is recorded as no log path rather than a wrong one.
    environ[SERVER_LOG_PATH_ENV] = str(log_path)
    environ[SERVER_HOST_ENV] = host
    environ[SERVER_PORT_ENV] = str(port)

    # The pipe answers two questions at once: which pid to name, and -- by
    # reaching EOF with nothing in it -- whether the daemon died before it could
    # say anything.
    read_fd, write_fd = os.pipe()
    intermediate = os.fork()
    if intermediate == 0:
        os.close(read_fd)
        try:
            _detach_session()
            _run_daemon(
                command,
                log_path=log_path,
                root=root,
                max_bytes=max_bytes,
                keep=keep,
                announce=write_fd,
            )
        except BaseException:  # noqa: B036 - a forked child must never unwind
            # stderr is still the operator's terminal until _run_daemon
            # redirects it, so a failure this early is visible.
            traceback.print_exc()
        os._exit(70)

    os.close(write_fd)
    # The intermediate process exits immediately; reaping it here is what keeps
    # a zombie out of the caller's shell.
    with contextlib.suppress(ChildProcessError, OSError):
        os.waitpid(intermediate, 0)
    with os.fdopen(read_fd, "r", encoding="utf-8") as handle:
        announced = handle.read().strip()
    supervisor_pid = int(announced) if announced.isdigit() else 0

    print(f"SERVING in the background · supervisor pid {supervisor_pid or 'UNREPORTED'}")
    print(f"LOG    {log_path}")
    if online is None:
        return 0
    if _wait_until_online(online, host, port, grace_seconds):
        print(f"READY  http://{host if host not in {'0.0.0.0', '::'} else '127.0.0.1'}:{port}/")
        _announce_degraded(readiness, host, port)
        print("STOP   './antidemo status' names the pid to signal")
        return 0
    print(
        f"NOT SERVING yet: port {port} did not answer /api/health within "
        f"{grace_seconds:.0f}s. The log says:",
        file=sys.stderr,
    )
    tail = log_tail(log_path)
    print(tail or "(the log is empty, so the daemon died before writing)", file=sys.stderr)
    return 1


def _announce_degraded(readiness, host: str, port: int) -> None:
    """Say what READY does not: up, and refusing to do the thing it is for.

    ``READY`` above means only that the process answered ``/api/health``, which is
    a liveness check and is deliberately a static literal -- it must stay trivially
    true, because a liveness probe that depends on anything external reports the
    dependency rather than the process. That leaves a server serving 503 on
    ``/readyz`` and refusing every control action with 409 announcing itself as
    ready, which is the one thing an operator reading this output cannot afford to
    be wrong about.

    Advisory and fail-soft: an unreadable readiness surface downgrades this to
    silence rather than turning a started server into a failed command.
    """
    if readiness is None:
        return
    try:
        payload = readiness(host, port)
    except Exception:  # noqa: BLE001 - an advisory line may not fail the start
        return
    if not isinstance(payload, dict) or not payload.get("degraded"):
        return
    detail = str(payload.get("degraded_detail") or "").strip()
    print(
        "DEGRADED  the server is up and refusing control actions"
        + (f" · {detail}" if detail else ""),
        file=sys.stderr,
    )
    lost = payload.get("degraded_capabilities")
    if isinstance(lost, list):
        for one in lost:
            print(f"          · {one}", file=sys.stderr)
    print("          './antidemo status' has the full readiness surface", file=sys.stderr)


def _wait_until_online(online, host: str, port: int, grace_seconds: float) -> bool:
    deadline = time.monotonic() + grace_seconds
    while True:
        if online(host, port):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)
