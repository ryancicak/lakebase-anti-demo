"""A pidfile that describes the process actually serving, or admits it cannot.

The hazard this replaces: the pidfile and launch record under the state directory
were written by a wrapper *outside* the process they described. Any launch that
did not go through that one wrapper left both files naming a pid that had already
exited, and left the recorded log path pointing at a file nobody was writing.
Tooling that trusted either would signal the wrong process or call a live server
dead.

Two halves, because neither alone is sufficient:

* Write time, from inside the server. The process that serves registers itself,
  so the recorded pid is correct by construction and no wrapper is required. An
  ad-hoc ``uvicorn app:app`` therefore records itself truthfully instead of
  leaving a previous generation's record in place. The recorded log path is only
  claimed when this process's stdout is verified to be that exact file, so a
  launch that logs somewhere else records no log path rather than a wrong one.

* Read time, from outside. A crash writes nothing, so write-time hygiene can
  never make a record trustworthy on its own. Every reader must re-verify that
  the recorded pid is alive *and* is still this application before acting on it.
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

#: 3 added `supervisor_pid` and retired `launcher_pid`. The bump is load-bearing
#: rather than decorative: it is the only thing that separates "this server has no
#: supervisor" from "this record was written before anything recorded one", and a
#: reader that conflated those two would be back to guessing. See
#: :meth:`ServerProcessRecord.supervision`.
RECORD_SCHEMA = 3

#: The first schema that records a supervisor at all.
SUPERVISOR_PID_SCHEMA = 3

SERVER_LOG_PATH_ENV = "ANTI_DEMO_SERVER_LOG_PATH"
SERVER_PORT_ENV = "ANTI_DEMO_SERVER_PORT"
SERVER_HOST_ENV = "ANTI_DEMO_SERVER_HOST"
MANIFEST_ENV = "ANTI_DEMO_MANIFEST"

#: Exported by the supervisor in `server_launch._run_daemon`, immediately before it
#: spawns the server, so the serving process can record who is watching it.
#:
#: The server cannot work this out for itself. Its own parent is the ``uv`` shim --
#: `serve_command` runs ``uv run --no-sync uvicorn`` and uv *spawns* uvicorn rather
#: than exec'ing it -- so the tree is three deep and `os.getppid` names the middle
#: process. Walking up from there is not portable and would be a guess anyway. The
#: supervisor is the one process that knows its own pid for certain, so it says so.
#:
#: Absence is meaningful and is not a failure to look: only a supervisor sets this,
#: so an unset value means the server was started without one -- a foreground
#: `antidemo serve`, or a bare uvicorn.
SUPERVISOR_PID_ENV = "ANTI_DEMO_SUPERVISOR_PID"

#: What `ServerProcessRecord.supervision` can answer. `UNKNOWN` exists because a
#: record written by an older launcher is silent on the question, and silence must
#: not be read as "no supervisor" -- there is a live one on this machine right now
#: whose record predates the field.
SUPERVISION_SUPERVISED = "supervised"
SUPERVISION_UNSUPERVISED = "unsupervised"
SUPERVISION_UNKNOWN = "unknown"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000

#: Fields a wrapper may have recorded that the server itself cannot re-derive.
#: They are carried forward so self-registration never destroys provenance.
#: Deliberately no pids: a pid is the one thing that must never be inherited from
#: a record written by a process that has since exited.
CARRIED_FIELDS = (
    "manifest_version",
    "installation_id",
    "run_id",
    "canonical_manifest_seal",
)


def _positive_pid(value: object) -> int | None:
    """A pid, or ``None`` for every way one can fail to be there.

    Tolerant on purpose: this reads a JSON file and an environment variable, and
    neither is worth failing a start over. ``bool`` is excluded because it is an
    ``int`` and ``True`` would otherwise arrive as pid 1.
    """
    if isinstance(value, bool) or not isinstance(value, int | str):
        return None
    try:
        pid = int(value)
    except ValueError:
        return None
    return pid if pid > 0 else None


@dataclass(frozen=True)
class ServerProcessRecord:
    """What one serving process asserts about itself.

    **Three pids, and the two that are easy to confuse are named apart.** A
    supervised launch is three processes deep -- supervisor, ``uv`` shim, uvicorn
    -- because `serve_command` runs ``uv run --no-sync uvicorn`` and uv spawns the
    server rather than exec'ing it. So:

    * ``pid`` is the server. This is the one to signal, and the only one any
      reader here verifies is alive and is this application.
    * ``parent_pid`` is the server's *immediate* parent, which under a supervisor
      is the ``uv`` shim and not the supervisor. Recorded because it is what
      ``os.getppid`` returns and is occasionally useful; never to be read as "the
      thing supervising this".
    * ``supervisor_pid`` is the supervisor, which is the process that restarts the
      server and rolls its log. It is one above ``parent_pid`` in a supervised
      launch and is ``None`` when there is no supervisor at all.

    The field this replaces was called ``launcher_pid`` and held ``parent_pid``,
    so it named the ``uv`` shim while asserting it was the launcher. Signalling it
    happened to work -- uv relays -- which is why nothing caught it. It is gone
    rather than redefined: a key whose meaning has been wrong is worse when it
    quietly starts meaning something else.
    """

    pid: int
    parent_pid: int
    host: str
    port: int
    started_at_utc: str
    launch_mode: str
    executable: str
    argv: tuple[str, ...]
    identity_tokens: tuple[str, ...]
    stdout_kind: str
    log_path: str | None
    manifest_path: str | None
    supervisor_pid: int | None = None
    record_schema: int = RECORD_SCHEMA
    carried: dict[str, object] = field(default_factory=dict)

    @property
    def supervision(self) -> str:
        """Whether anything is supervising this server, or that it cannot say.

        Three answers, not two. A record written before schema
        :data:`SUPERVISOR_PID_SCHEMA` did not record a supervisor and so cannot
        deny one either; reading its silence as "unsupervised" would be this
        project's recurring bug -- a surface reporting something it never checked
        -- committed by the reader instead of the writer.
        """
        if self.record_schema < SUPERVISOR_PID_SCHEMA:
            return SUPERVISION_UNKNOWN
        return (
            SUPERVISION_SUPERVISED
            if self.supervisor_pid
            else SUPERVISION_UNSUPERVISED
        )

    def to_dict(self) -> dict[str, object]:
        """Serialize as a superset of the legacy keys so old readers still work."""
        payload: dict[str, object] = dict(self.carried)
        payload.update(asdict(self))
        payload.pop("carried", None)
        payload["argv"] = list(self.argv)
        payload["identity_tokens"] = list(self.identity_tokens)
        # An exact synonym of `pid`, kept because the wrapper that used to write
        # this record wrote that spelling and it has never been anything else.
        # `launcher_pid` was the other half of that pair and is deliberately not
        # written any more -- see the class docstring.
        payload["server_pid"] = self.pid
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ServerProcessRecord:
        # `launcher_pid` is still accepted, because records written by the
        # launcher that wrote it are on disk right now -- but only as what it
        # actually contained, which is the immediate parent.
        pid = payload.get("pid", payload.get("server_pid"))
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            raise ValueError("launch record has no usable pid")
        log_path = payload.get("log_path")
        manifest_path = payload.get("manifest_path")
        return cls(
            pid=pid,
            parent_pid=int(payload.get("parent_pid") or payload.get("launcher_pid") or 0),
            host=str(payload.get("host") or ""),
            port=int(payload.get("port") or 0),
            started_at_utc=str(payload.get("started_at_utc") or ""),
            launch_mode=str(payload.get("launch_mode") or "unknown"),
            executable=str(payload.get("executable") or ""),
            argv=tuple(str(item) for item in payload.get("argv") or ()),
            identity_tokens=tuple(
                str(item) for item in payload.get("identity_tokens") or ()
            ),
            stdout_kind=str(payload.get("stdout_kind") or "unknown"),
            log_path=str(log_path) if log_path else None,
            manifest_path=str(manifest_path) if manifest_path else None,
            # Never from `launcher_pid`. That key held the immediate parent, so
            # promoting it here would re-import the exact confusion this field
            # exists to end -- it is read below, as what it really was.
            supervisor_pid=_positive_pid(payload.get("supervisor_pid")),
            record_schema=int(payload.get("record_schema") or 1),
            carried={
                name: payload[name] for name in CARRIED_FIELDS if name in payload
            },
        )


@dataclass(frozen=True)
class RecordStatus:
    """The only conclusion a reader is entitled to draw from the record."""

    state: str
    detail: str
    record: ServerProcessRecord | None = None

    @property
    def safe_to_signal(self) -> bool:
        """True only when the recorded pid was proven to be this application."""
        return self.state == "live"


def record_paths(state_dir: Path, port: int) -> tuple[Path, Path]:
    return (
        state_dir / f"server-{port}.pid",
        state_dir / f"server-{port}.launch.json",
    )


def state_dir_from_environ(environ: dict[str, str] | None = None) -> Path | None:
    """The state directory is wherever the selected manifest lives, or nowhere."""
    environ = os.environ if environ is None else environ
    configured = (environ.get(MANIFEST_ENV) or "").strip()
    if not configured:
        return None
    candidate = Path(configured).expanduser().resolve().parent
    return candidate if candidate.is_dir() else None


def serving_endpoint(
    argv: list[str] | tuple[str, ...] | None = None,
    environ: dict[str, str] | None = None,
) -> tuple[str, int]:
    """Recover the host and port this process serves from its own invocation.

    Uvicorn does not hand the port to the application, so the serving process has
    to read its own argv. That is also what makes self-registration work for an
    ad-hoc launch, which sets no environment variables at all.
    """
    argv = list(sys.argv if argv is None else argv)
    environ = os.environ if environ is None else environ
    found: dict[str, str] = {}
    for index, token in enumerate(argv):
        for flag, name in (("--host", "host"), ("--port", "port")):
            if token == flag and index + 1 < len(argv):
                found[name] = argv[index + 1]
            elif token.startswith(f"{flag}="):
                found[name] = token.split("=", 1)[1]
    host = found.get("host") or environ.get(SERVER_HOST_ENV) or DEFAULT_HOST
    raw_port = found.get("port") or environ.get(SERVER_PORT_ENV) or ""
    try:
        port = int(raw_port)
    except ValueError:
        port = DEFAULT_PORT
    return host, (port if port > 0 else DEFAULT_PORT)


#: An ASGI target such as `app:app`. Its presence in argv is what separates a real
#: server invocation from any other process that happens to import this module,
#: including the test suite, which must never claim the live state directory.
_ASGI_TARGET = re.compile(r"[A-Za-z_][\w.]*:[A-Za-z_]\w*")


def is_server_invocation(argv: list[str] | tuple[str, ...] | None = None) -> bool:
    argv = list(sys.argv if argv is None else argv)
    return any(_ASGI_TARGET.fullmatch(token) for token in argv)


def entrypoint_token(entrypoint: str) -> str:
    """How ``ps`` will spell ``argv[0]``, which is not always what Python got.

    A ``python -m uvicorn`` launch is the case that matters. Python rewrites
    ``sys.argv[0]`` to the resolved file, so the process sees
    ``…/site-packages/uvicorn/__main__.py`` -- but the kernel kept the original
    argument vector, so ``ps`` reports ``-m uvicorn`` and the string
    ``__main__.py`` appears nowhere in it. Recording the basename therefore
    recorded a token that could never match, and every reader concluded a
    perfectly healthy server was some other process: `antidemo status` reported
    ``IS ALIVE BUT IS NOT THIS APP · DO NOT SIGNAL`` against a server it had
    itself just been talking to.

    Fixed here, at write time, rather than by relaxing the comparison at read
    time. The read-time check is the guard that stops the tooling signalling a
    pid that has been recycled by something else, and a false positive there is
    far worse than the false negative this replaces -- so it is left exactly as
    it was, and given a token that can actually be true.

    ``-m uvicorn`` is a *stronger* token than ``uvicorn`` alone would be: it
    matches only a module launch of that module, not any path that happens to
    contain the name.

    Known and accepted: a launch that names the file directly
    (``python …/uvicorn/__main__.py app:app``) records ``-m uvicorn`` and will not
    verify. That shape is not a supported way to start this server, and failing
    closed on it is the correct direction for this function to be wrong in.
    """

    name = Path(entrypoint).name
    if name != "__main__.py":
        return name
    module = Path(entrypoint).parent.name
    return f"-m {module}" if module else name


def identity_tokens(argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Tokens that must reappear in a live command line for it to be this process.

    Derived from the invocation rather than hardcoded, so the check keeps working
    if the entrypoint or the server changes.
    """
    tokens: list[str] = []
    for index, token in enumerate(argv):
        if not token:
            continue
        if index == 0:
            tokens.append(entrypoint_token(token))
            continue
        tokens.append(Path(token).name if os.sep in token else token)
    return tuple(dict.fromkeys(tokens))


def stdout_identity() -> tuple[str, tuple[int, int] | None]:
    """Classify this process's stdout, and identify it when it is a regular file."""
    try:
        info = os.fstat(1)
    except OSError:
        return "unknown", None
    if stat.S_ISREG(info.st_mode):
        return "file", (info.st_dev, info.st_ino)
    if stat.S_ISFIFO(info.st_mode):
        return "pipe", None
    if stat.S_ISCHR(info.st_mode):
        return "tty", None
    return "unknown", None


def verified_log_path(claimed: str | None, target: tuple[int, int] | None) -> str | None:
    """Return the claimed log path only if stdout is provably that same file."""
    if not claimed or target is None:
        return None
    try:
        info = os.stat(claimed)
    except OSError:
        return None
    return claimed if (info.st_dev, info.st_ino) == target else None


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Alive, owned by somebody else. Liveness is what this answers.
        return True
    except OSError:
        return False
    return True


def _command_line(pid: int) -> str | None:
    try:
        completed = subprocess.run(
            ["ps", "-ww", "-o", "args=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip() or None


def _pidfile_pid(pid_path: Path) -> int | None:
    try:
        return int(pid_path.read_text().strip())
    except (OSError, ValueError):
        return None


def read_record(state_dir: Path, port: int) -> ServerProcessRecord | None:
    _, launch_path = record_paths(state_dir, port)
    try:
        payload = json.loads(launch_path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return ServerProcessRecord.from_dict(payload)
    except (TypeError, ValueError):
        return None


def inspect_record(
    state_dir: Path,
    port: int,
    *,
    pid_is_alive=_pid_is_alive,
    command_line=_command_line,
) -> RecordStatus:
    """Classify the on-disk record without ever trusting it."""
    pid_path, launch_path = record_paths(state_dir, port)
    if not pid_path.exists() and not launch_path.exists():
        return RecordStatus("absent", "NO LAUNCH RECORD FOR THIS PORT")
    record = read_record(state_dir, port)
    if record is None:
        return RecordStatus(
            "unreadable",
            "LAUNCH RECORD IS UNREADABLE · DO NOT SIGNAL ANY PID",
        )
    # `kill $(cat server-<port>.pid)` reads the pidfile, not the launch record, so
    # the two disagreeing is its own hazard regardless of which pid is alive.
    stated = _pidfile_pid(pid_path)
    if stated is not None and stated != record.pid:
        return RecordStatus(
            "inconsistent",
            f"PIDFILE SAYS {stated} · LAUNCH RECORD SAYS {record.pid} · DO NOT SIGNAL",
            record,
        )
    if not pid_is_alive(record.pid):
        return RecordStatus(
            "exited",
            f"STALE RECORD · PID {record.pid} HAS EXITED · DO NOT SIGNAL",
            record,
        )
    live = command_line(record.pid)
    if live is None:
        return RecordStatus(
            "unverified",
            f"PID {record.pid} IS ALIVE BUT UNIDENTIFIED · DO NOT SIGNAL",
            record,
        )
    if not record.identity_tokens or any(
        token not in live for token in record.identity_tokens
    ):
        return RecordStatus(
            "foreign",
            f"PID {record.pid} IS ALIVE BUT IS NOT THIS APP · DO NOT SIGNAL",
            record,
        )
    where = record.log_path or f"UNMANAGED {record.stdout_kind.upper()}"
    return RecordStatus(
        "live",
        f"PID {record.pid} IS SERVING · {record.launch_mode.upper()} LAUNCH · LOG {where}",
        record,
    )


def _atomic_write(path: Path, text: str) -> None:
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    temporary.write_text(text)
    os.replace(temporary, path)


def build_record(
    *,
    argv: list[str] | tuple[str, ...] | None = None,
    environ: dict[str, str] | None = None,
    pid: int | None = None,
    parent_pid: int | None = None,
    supervisor_pid: int | None = None,
    carried: dict[str, object] | None = None,
    stdout: tuple[str, tuple[int, int] | None] | None = None,
    launch_mode: str | None = None,
    started_at_utc: str | None = None,
) -> ServerProcessRecord:
    argv = tuple(sys.argv if argv is None else argv)
    environ = os.environ if environ is None else environ
    host, port = serving_endpoint(argv, environ)
    stdout_kind, stdout_target = stdout_identity() if stdout is None else stdout
    claimed_log = (environ.get(SERVER_LOG_PATH_ENV) or "").strip() or None
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return ServerProcessRecord(
        pid=os.getpid() if pid is None else pid,
        parent_pid=os.getppid() if parent_pid is None else parent_pid,
        host=host,
        port=port,
        started_at_utc=started_at_utc or now,
        # `antidemo serve` is the only path that exports the endpoint, so its presence
        # is what distinguishes the supported launcher from a bare uvicorn.
        launch_mode=(
            launch_mode
            if launch_mode
            else "launcher"
            if environ.get(SERVER_PORT_ENV)
            else "adhoc"
        ),
        executable=sys.executable,
        argv=argv,
        identity_tokens=identity_tokens(argv),
        stdout_kind=stdout_kind,
        log_path=verified_log_path(claimed_log, stdout_target),
        manifest_path=(environ.get(MANIFEST_ENV) or "").strip() or None,
        # Not derived from `parent_pid`: the parent is the `uv` shim, and only the
        # supervisor can say which process it is. Absent means unsupervised, which
        # is a real answer rather than a shrug -- nothing else ever sets this.
        supervisor_pid=(
            _positive_pid(environ.get(SUPERVISOR_PID_ENV))
            if supervisor_pid is None
            else supervisor_pid
        ),
        carried=dict(carried or {}),
    )


def write_record(state_dir: Path, record: ServerProcessRecord) -> ServerProcessRecord:
    """Replace any previous record for this port with one describing this process."""
    pid_path, launch_path = record_paths(state_dir, record.port)
    _atomic_write(launch_path, json.dumps(record.to_dict(), indent=2) + "\n")
    _atomic_write(pid_path, f"{record.pid}\n")
    return record


def register_serving_process(
    *,
    state_dir: Path | None = None,
    argv: list[str] | tuple[str, ...] | None = None,
    environ: dict[str, str] | None = None,
) -> ServerProcessRecord | None:
    """Claim the pidfile for this process. Returns None when there is nothing to own."""
    if not is_server_invocation(argv):
        return None
    resolved = state_dir_from_environ(environ) if state_dir is None else state_dir
    if resolved is None or not resolved.is_dir():
        return None
    _, port = serving_endpoint(argv, environ)
    previous = read_record(resolved, port)
    carried: dict[str, object] = {}
    if previous is not None and previous.manifest_path == (
        (os.environ if environ is None else environ).get(MANIFEST_ENV) or ""
    ).strip():
        carried = dict(previous.carried)
    return write_record(
        resolved,
        build_record(argv=argv, environ=environ, carried=carried),
    )


def unregister_serving_process(
    record: ServerProcessRecord | None,
    *,
    state_dir: Path | None = None,
    environ: dict[str, str] | None = None,
) -> bool:
    """Remove the record only while it still describes this process."""
    if record is None:
        return False
    resolved = state_dir_from_environ(environ) if state_dir is None else state_dir
    if resolved is None:
        return False
    on_disk = read_record(resolved, record.port)
    if on_disk is None or on_disk.pid != record.pid:
        return False
    removed = False
    for path in record_paths(resolved, record.port):
        try:
            path.unlink()
            removed = True
        except OSError:
            continue
    return removed
