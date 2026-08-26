"""One exclusive claim on a generation directory, enforced by the kernel.

The hazard this closes: nothing stopped two processes from mutating one
installation at once. ``manifest.py`` writes atomically, so no single write is
torn -- but every mutation is a *sequence* of writes. ``antidemo reset`` moves the
status to ``seeding``, reseeds, re-seals four rounds and only then writes
``ready``. A second mutator that starts anywhere inside that window reads a
half-finished manifest, acts on it, and writes its own answer over the top. Two
operators doing exactly that cycled one manifest ``ready -> seeding -> ready ->
seeding`` for forty minutes and left it at ``seeding`` with nothing running;
``app.py`` refuses to start on a status that is not ``ready``, so the demo was
down. The corruption is therefore at the *operation* level, not the write level,
and the fix has to be an operation-level lock.

Why ``flock`` and not a pidfile:

* A pidfile has to be deleted by whoever wrote it. A mutator killed
  mid-operation cannot delete anything, so the lock is held forever by a file
  nobody owns and recovery requires knowing to ``rm`` it. That trades a
  five-minute wedge for an undiagnosable one, which is worse than no lock.
* ``flock(2)`` attaches to the open file description, so the kernel releases it
  when the last descriptor closes -- including on ``SIGKILL``, a panic, or a
  closed laptop. **There is no stale lock to clean up, ever.** The interesting
  failure becomes the opposite one: the lock is held while the recorded pid is
  not the live holder. This module reports that as unattributable and points at
  ``lsof`` instead of guessing, the same discipline ``process_registry.py``
  applies to a launch record.

The JSON payload inside the lock file is diagnostics only. Authority always
comes from trying the lock, never from reading the file. That is also why the
payload is written *in place* rather than through the ``.tmp`` + ``os.replace``
dance used elsewhere in this tree: replacing the file would swap the inode, and
two processes flocking two different inodes exclude nobody.

Readers are never blocked. Nothing here touches ``manifest.json``, so
``antidemo status``, ``load_manifest`` and the running server keep working at full
speed while a mutation holds the lock. The server is deliberately *not* a lock
holder: it only reads, and a long-lived holder would make ``antidemo setup``
impossible without stopping the demo.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import getpass
import json
import os
import secrets
import socket
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

#: `datetime.UTC` is 3.11+, and this module has a caller the rest of the tree
#: does not: `bootstrap.sh` runs it through whatever `python3` is on PATH, which
#: on a stock Mac is /usr/bin/python3 at 3.9. An ImportError here would make
#: bootstrap die *at the lock step* on a fresh contributor's laptop -- a lock
#: that stops the tool from running at all is worse than no lock. Everything in
#: this module stays inside 3.9's stdlib for that reason.
#: UP017 wants `datetime.UTC` here; that alias is the 3.11 spelling this module
#: cannot use, so the lint is suppressed rather than obeyed.
_UTC = timezone.utc  # noqa: UP017

#: The lock lives beside the manifest, so it is scoped to one generation exactly
#: the way every other piece of generation state is.
GENERATION_LOCK_NAME = "mutation.lock"

#: Set by a holder for its children. A nested mutator that presents this token
#: is recognized as part of the same logical operation instead of deadlocking
#: against its own parent -- which is what `bootstrap.sh` running `antidemo setup`
#: would otherwise do.
LOCK_TOKEN_ENV = "ANTI_DEMO_GENERATION_LOCK_TOKEN"

#: Manifest statuses that mean "a multi-step mutation was part-way through".
#: None of them is servable, and none of them clears itself.
TRANSITIONAL_STATUSES = ("provisioning", "seeding", "waiting_for_zero")

_DO_NOT_DELETE = (
    "Do not delete the lock file: the kernel releases the lock when that "
    "process exits, and removing the file only breaks exclusion for the next "
    "caller."
)

_READS_ARE_FINE = (
    "Read-only commands are unaffected -- './antidemo status' and the running "
    "server keep working while a mutation runs."
)


class GenerationBusyError(RuntimeError):
    """Another process is mutating this generation. The message says which one."""

    def __init__(self, message: str, *, holder: LockHolder | None = None) -> None:
        super().__init__(message)
        self.holder = holder


@dataclass(frozen=True)
class LockHolder:
    """What one holder claims about itself. Never trusted, only reported."""

    pid: int
    parent_pid: int
    operation: str
    argv: tuple[str, ...]
    host: str
    user: str
    claimed_at: str
    token: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "pid": self.pid,
                "parent_pid": self.parent_pid,
                "operation": self.operation,
                "argv": list(self.argv),
                "host": self.host,
                "user": self.user,
                "claimed_at": self.claimed_at,
                "token": self.token,
            },
            indent=2,
        )

    @classmethod
    def from_payload(cls, text: str) -> LockHolder | None:
        """Parse a payload, or admit it says nothing usable."""
        try:
            payload = json.loads(text)
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        pid = payload.get("pid")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return None
        return cls(
            pid=pid,
            parent_pid=int(payload.get("parent_pid") or 0),
            operation=str(payload.get("operation") or "an unnamed operation"),
            argv=tuple(str(item) for item in payload.get("argv") or ()),
            host=str(payload.get("host") or ""),
            user=str(payload.get("user") or ""),
            claimed_at=str(payload.get("claimed_at") or ""),
            token=str(payload.get("token") or ""),
        )

    def age_phrase(self, *, now: datetime | None = None) -> str:
        """How long ago this holder claimed the lock, in words an operator reads."""
        moment = now or datetime.now(_UTC)
        try:
            claimed = datetime.fromisoformat(self.claimed_at.replace("Z", "+00:00"))
        except ValueError:
            return "AN UNKNOWN TIME AGO"
        if claimed.tzinfo is None:
            claimed = claimed.replace(tzinfo=_UTC)
        seconds = int((moment - claimed).total_seconds())
        if seconds < 0:
            return "IN THE FUTURE (CLOCK SKEW)"
        if seconds < 90:
            return f"{seconds}S AGO"
        return f"{seconds // 60}M AGO"


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Alive and owned by somebody else. Liveness is the whole question here.
        return True
    except OSError:
        return False
    return True


def generation_lock_path(manifest_path: Path | str) -> Path:
    """The lock for the generation that owns `manifest_path`."""
    return Path(manifest_path).expanduser().resolve().parent / GENERATION_LOCK_NAME


def _open_lock(lock_path: Path) -> int:
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    # The lock must never survive into an exec'd child. `antidemo setup` ends by
    # exec'ing uvicorn, and a server that inherited a held lock would be the
    # long-lived holder this design exists to avoid.
    flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
    fcntl.fcntl(descriptor, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
    return descriptor


def _try_flock(descriptor: int) -> bool:
    """Take the exclusive lock without waiting. False means somebody else has it."""
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        if error.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
            return False
        raise
    return True


def _write_payload(lock_path: Path, text: str) -> None:
    """Replace the payload inside the *same* inode the lock is taken on.

    In place, not through `.tmp` + `os.replace` like the manifest: replacing the
    file would give the next caller a different inode to lock, and two processes
    flocking two different inodes exclude nobody. The caller holds the exclusive
    lock while this runs, so the only reader who can see a partial write is one
    that is not taking the lock -- and those treat an unparseable payload as
    "holder unidentified" rather than as an empty lock.
    """
    encoded = (text + "\n").encode("utf-8")
    descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.write(descriptor, encoded)
        os.ftruncate(descriptor, len(encoded))
    finally:
        os.close(descriptor)


def read_holder(lock_path: Path) -> LockHolder | None:
    """Read the recorded holder without taking the lock. Diagnostics only."""
    try:
        return LockHolder.from_payload(lock_path.read_text(encoding="utf-8"))
    except OSError:
        return None


def describe_holder(
    lock_path: Path,
    holder: LockHolder | None,
    *,
    now: datetime | None = None,
    pid_is_alive: Callable[[int], bool] = _pid_is_alive,
) -> str:
    """Render the refusal an operator reads, saying only what was proven."""
    if holder is None:
        return (
            "ANOTHER PROCESS IS MUTATING THIS GENERATION · HOLDER UNIDENTIFIED\n"
            f"The lock at {lock_path} is held, but it carries no readable "
            "record of who holds it. Find the process with "
            f"'lsof {lock_path}' before assuming anything.\n"
            f"{_READS_ARE_FINE}\n{_DO_NOT_DELETE}"
        )
    where = f"{holder.user}@{holder.host}".strip("@") or "an unnamed operator"
    if not pid_is_alive(holder.pid):
        return (
            "ANOTHER PROCESS IS MUTATING THIS GENERATION · HOLDER UNATTRIBUTABLE\n"
            f"The lock at {lock_path} is held, but the pid it records "
            f"({holder.pid}, {holder.operation}, {where}) has exited. The lock is "
            "released by the kernel, so something that shares that process's file "
            "handle is still holding it -- usually the shell or wrapper that "
            f"launched it. Find it with 'lsof {lock_path}'.\n"
            f"{_READS_ARE_FINE}\n{_DO_NOT_DELETE}"
        )
    return (
        "ANOTHER PROCESS IS MUTATING THIS GENERATION\n"
        f"  what:  {holder.operation}\n"
        f"  who:   pid {holder.pid} ({where})\n"
        f"  since: {holder.claimed_at or 'an unrecorded time'} "
        f"({holder.age_phrase(now=now)})\n"
        f"  lock:  {lock_path}\n"
        f"Wait for it to finish, or inspect it with 'ps -p {holder.pid}'. "
        f"{_READS_ARE_FINE}\n{_DO_NOT_DELETE}"
    )


#: Locks held by *this* process, so a nested acquire is reentrant instead of
#: deadlocking against itself. `flock` conflicts between two descriptors of one
#: file even inside a single process, so this cannot be left to the kernel.
_HELD_BY_THIS_PROCESS: dict[Path, str] = {}


def held_by_this_process(lock_path: Path) -> bool:
    return lock_path in _HELD_BY_THIS_PROCESS


def lock_is_held(
    lock_path: Path,
    *,
    check_this_process: bool = True,
) -> bool:
    """Answer "is somebody mutating this generation" by trying the lock.

    Probing takes the lock and releases it immediately, which disturbs no
    holder and -- unlike reading the payload -- cannot be fooled by residue
    left behind by a process that has since exited.
    """
    if check_this_process and held_by_this_process(lock_path):
        return True
    if not lock_path.exists():
        return False
    try:
        descriptor = _open_lock(lock_path)
    except OSError:
        # An unreadable lock file is not evidence that anyone holds it, and
        # claiming otherwise here would refuse work for the wrong reason.
        return False
    try:
        if not _try_flock(descriptor):
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class GenerationLock:
    """A held claim. `inherited` means an ancestor holds the real lock."""

    path: Path
    holder: LockHolder
    inherited: bool


@contextmanager
def hold_generation(
    manifest_path: Path | str,
    operation: str,
    *,
    environ: dict[str, str] | None = None,
    argv: list[str] | tuple[str, ...] | None = None,
    now: datetime | None = None,
    pid_is_alive: Callable[[int], bool] = _pid_is_alive,
) -> Iterator[GenerationLock]:
    """Hold this generation for the duration of one mutating operation.

    Refuses immediately rather than waiting: an operator who ran the wrong
    command wants to be told so, not to have their terminal hang behind a
    fifteen-minute Terraform apply.
    """
    environment = os.environ if environ is None else environ
    lock_path = generation_lock_path(manifest_path)
    arguments = tuple(str(item) for item in (sys.argv if argv is None else argv))

    if held_by_this_process(lock_path):
        yield GenerationLock(
            path=lock_path,
            holder=_this_holder(
                operation,
                arguments,
                token=_HELD_BY_THIS_PROCESS[lock_path],
                now=now,
            ),
            inherited=True,
        )
        return

    descriptor = _open_lock(lock_path)
    try:
        if not _try_flock(descriptor):
            existing = read_holder(lock_path)
            presented = (environment.get(LOCK_TOKEN_ENV) or "").strip()
            if existing is not None and presented and presented == existing.token:
                # Same logical operation, one level down: `bootstrap.sh` holding
                # the lock and then running `./antidemo setup` inside it.
                yield GenerationLock(path=lock_path, holder=existing, inherited=True)
                return
            raise GenerationBusyError(
                describe_holder(lock_path, existing, now=now, pid_is_alive=pid_is_alive),
                holder=existing,
            )

        holder = _this_holder(operation, arguments, token=secrets.token_hex(16), now=now)
        _write_payload(lock_path, holder.to_json())
        _HELD_BY_THIS_PROCESS[lock_path] = holder.token
        environment[LOCK_TOKEN_ENV] = holder.token
        try:
            yield GenerationLock(path=lock_path, holder=holder, inherited=False)
        finally:
            _HELD_BY_THIS_PROCESS.pop(lock_path, None)
            if environment.get(LOCK_TOKEN_ENV) == holder.token:
                environment.pop(LOCK_TOKEN_ENV, None)
            # Clear the record before releasing, so a reader that arrives after
            # the lock is free cannot mistake residue for a live holder.
            try:
                _write_payload(lock_path, json.dumps({"released_at": _stamp(None)}, indent=2))
            except OSError:
                pass
    finally:
        os.close(descriptor)


def _stamp(now: datetime | None) -> str:
    return (now or datetime.now(_UTC)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _this_holder(
    operation: str,
    argv: tuple[str, ...],
    *,
    token: str,
    now: datetime | None,
    pid: int | None = None,
) -> LockHolder:
    try:
        user = getpass.getuser()
    except (OSError, KeyError):
        user = ""
    return LockHolder(
        pid=os.getpid() if pid is None else pid,
        parent_pid=os.getppid(),
        operation=operation,
        argv=argv,
        host=socket.gethostname(),
        user=user,
        claimed_at=_stamp(now),
        token=token,
    )


def transitional_status_recovery(
    status: str,
    *,
    manifest_path: Path | str,
    now: datetime | None = None,
    pid_is_alive: Callable[[int], bool] = _pid_is_alive,
) -> str | None:
    """Say what to do about a status that is not `ready`, and mean it.

    `status: seeding` with nothing running used to be a dead end: the only cure
    was knowing that `antidemo setup` resumes it. The lock is what makes the two
    cases distinguishable without guessing -- if nobody holds it, no mutation is
    in flight, so the status was abandoned rather than in progress.
    """
    if status not in TRANSITIONAL_STATUSES:
        return None
    lock_path = generation_lock_path(manifest_path)
    if lock_is_held(lock_path):
        holder = read_holder(lock_path)
        if holder is not None and pid_is_alive(holder.pid):
            return (
                f"A mutation is in progress: {holder.operation} (pid {holder.pid}, "
                f"started {holder.claimed_at}, {holder.age_phrase(now=now)}). This "
                "status is mid-operation, not broken. Wait for that command to "
                "finish."
            )
        return (
            "This generation is locked by a process that cannot be identified "
            f"from {lock_path}, so a mutation may still be in flight. Find it "
            f"with 'lsof {lock_path}' before running anything that writes."
        )
    return (
        f"No process is mutating this generation (nothing holds {lock_path}), so "
        f"this '{status}' status was left behind by an interrupted run and will "
        "not clear itself. Run './antidemo setup' to finish it: on a non-ready "
        "manifest that resumes the interrupted provision from where it stopped."
    )


def _cli() -> int:
    """The lock, for `bootstrap.sh`.

    macOS ships no `flock(1)`, and a shell cannot hold a lock through a helper
    that exits. Both problems have one answer: the shell opens the lock file on
    a numbered descriptor with `exec 9>>`, this command locks *that* inherited
    descriptor, and the lock then lives on the shell's open file description
    until the shell exits -- kernel-released, exactly like every other holder.
    """
    parser = argparse.ArgumentParser(prog="python3 -m server.generation_lock")
    subparsers = parser.add_subparsers(dest="command", required=True)

    acquire = subparsers.add_parser("acquire", help="Lock an inherited descriptor")
    acquire.add_argument("--fd", type=int, required=True)
    acquire.add_argument("--manifest", default=os.environ.get("ANTI_DEMO_MANIFEST", ""))
    acquire.add_argument("--operation", required=True)
    acquire.add_argument("--pid", type=int, default=0)

    release = subparsers.add_parser("release", help="Clear the record of a finished holder")
    release.add_argument("--manifest", default=os.environ.get("ANTI_DEMO_MANIFEST", ""))

    report = subparsers.add_parser("status", help="Report the holder, keeping no lock")
    report.add_argument("--manifest", default=os.environ.get("ANTI_DEMO_MANIFEST", ""))

    args = parser.parse_args()
    lock_path = generation_lock_path(args.manifest)

    if args.command == "status":
        if not lock_is_held(lock_path):
            print(f"free {lock_path}")
            return 0
        print(describe_holder(lock_path, read_holder(lock_path)), file=sys.stderr)
        return 1

    if args.command == "release":
        # The lock itself is already gone: it went when the shell closed its
        # descriptor. This only clears the record so the next reader does not
        # see a holder that has finished.
        try:
            _write_payload(lock_path, json.dumps({"released_at": _stamp(None)}, indent=2))
        except OSError:
            pass
        return 0

    if not _try_flock(args.fd):
        print(describe_holder(lock_path, read_holder(lock_path)), file=sys.stderr)
        return 1
    holder = _this_holder(
        args.operation,
        tuple(sys.argv),
        token=secrets.token_hex(16),
        now=None,
        # The shell that opened the descriptor is the real holder; this helper
        # exits immediately and the lock outlives it.
        pid=args.pid or os.getppid(),
    )
    _write_payload(lock_path, holder.to_json())
    print(f"export {LOCK_TOKEN_ENV}='{holder.token}'")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
