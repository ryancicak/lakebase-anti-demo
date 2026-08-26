"""Browser-initiated recovery of a swept installation, under D9a.

Installations disappear underneath their operator. The account this was developed
in is swept fortnightly by its own automation, which deletes Aurora, RDS and the
IAM users together; an account with no such automation reaches the same state via
an expired TTL, a manual teardown or a neighbour's cleanup. Either way, recovery
today means an operator noticing and re-running the installer by hand.
D9a permits a browser to *request* that re-run, bound by five conditions, and
this module is where four of the five are enforced (the fifth, the confirmation
dialog, is half here and half in the browser).

Three things about the shape are load-bearing and are easy to undo by accident:

**The serving process never provisions.** It writes an attempt record and forks
a detached process, which runs the installer, which takes the generation lock
itself. Nothing here calls ``hold_generation``; nothing here writes a manifest.
The server stays what the wait gate needs it to be -- an outside observer of
somebody else's mutation that happens to have started it.

**Only ``verified_missing`` may spend money.** That is the one presence state
meaning AWS was successfully read and positively reported the resources absent.
``unverified`` -- "I could not look" -- must never authorise a spend, and the
inversion that makes this worth stating twice is that a *real* reap produces
``unverified``, not ``verified_missing``: the sweep that deletes the databases
deletes the IAM users too, so the account cannot be read at all. So the commonest
real trigger for recovery is a state that must refuse to recover, and the honest
answer there is to name the credential as the thing to fix.

**The rate limit is durable.** It is seeded from its journal on every read, not
carried in memory, because a restart loop over an in-memory budget is a
provisioning loop and the failure mode is a bill. This is defect 2.4 in
``OPEN-FINDINGS.md`` -- ``RestartJournal`` carried the total forward but not the
stamps, so every new supervisor got a fresh budget. Do not re-make it here.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import select
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .cost_model import CarryingWindow, EstimateScope, estimate_carrying_cost
from .generation_lock import (
    LOCK_TOKEN_ENV,
    TRANSITIONAL_STATUSES,
    describe_holder,
    generation_lock_path,
    lock_is_held,
    read_holder,
    transitional_status_recovery,
)
from .lifecycle import (
    INSTALLATION_PRESENCE_TTL_SECONDS,
    installation_presence_async,
    reset_installation_presence_cache,
)
from .manifest import load_manifest, manifest_path
from .process_registry import state_dir_from_environ
from .reconcile import (
    INSTALLATION_REPAIR_COMMAND,
    PRESENCE_MISSING,
    PRESENCE_NEVER_CHECKED,
    PRESENCE_UNVERIFIED,
    InstallationPresence,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: Where the attempt journal and the per-attempt progress files live. Anchored on
#: the *manifest* directory rather than the artifact root, because the rate limit
#: and the mutation lock are both per-generation and must agree about which
#: generation they are bounding. It has a second effect that is deliberate: in
#: deployed mode ``manifest_path()`` raises, so there is physically nowhere to
#: write an attempt, and the deployed refusal cannot be undone by a policy edit.
RECOVERY_DIR_NAME = "recovery"
ATTEMPT_JOURNAL_NAME = "recovery-attempts.jsonl"

#: Two limits, both read from the journal on every check. Thirty minutes is longer
#: than a successful install takes, so the second press cannot be a double-click;
#: three a day is more than a human recovering from one sweep ever needs and far
#: fewer than a restart loop would issue in a minute.
MIN_SECONDS_BETWEEN_ATTEMPTS = 1800.0
MAX_ATTEMPTS_PER_WINDOW = 3
RATE_WINDOW_SECONDS = 86400.0

#: Overridable argv for the mutator, as a JSON list. The one seam that lets the
#: spawn path be proven without spending money: a test points it at a fake
#: mutator and asserts the fork, the environment scrub and the status file. Read
#: from the server's own environment, never from a request, and unreachable in
#: deployed mode because recovery refuses there before this is consulted.
RECOVERY_COMMAND_ENV = "ANTI_DEMO_RECOVERY_COMMAND"

#: A shared secret required only when the request did not come from loopback.
#: See `authorisation_refusal`.
RECOVERY_TOKEN_ENV = "ANTI_DEMO_RECOVERY_TOKEN"
RECOVERY_TOKEN_HEADER = "x-anti-demo-recovery-token"

#: Set on the child so a `ps` reader, and the log itself, can tie a running
#: installer back to the click that started it.
ATTEMPT_ENV = "ANTI_DEMO_RECOVERY_ATTEMPT"

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost", "::ffff:127.0.0.1"})


def deployed(environ: dict[str, str] | None = None) -> bool:
    """Whether this is the Databricks App rather than a laptop.

    The same two names `app.py` and the runtime binder already use, so a
    deployment cannot be deployed for one of them and local for the other.
    """
    env = os.environ if environ is None else environ
    return env.get("ANTI_DEMO_ENV") == "databricks-app" or bool(env.get("DATABRICKS_APP_NAME"))


# --------------------------------------------------------------------------
# Who is reading this screen
#
# A separate question from who may spend money, and it must stay separate.
# `authorisation_refusal` below decides the second from a property of the
# socket, deliberately refusing to trust a claimed identity with a dollar. What
# follows decides only *which prose to render*, and the worst thing a wrong
# answer here can do is show an operator paragraph to somebody who did not need
# it, or withhold one from somebody who has `/readyz` and a terminal.
# --------------------------------------------------------------------------

#: Everything the recovery surface says, in the words of the person who can act.
AUDIENCE_OPERATOR = "operator"
#: An authenticated Databricks user who is watching the demo. Names no shell
#: command, no state directory and no credential, because none of them are
#: theirs to run, look in or fix.
AUDIENCE_VIEWER = "viewer"


def sealed_owner_email() -> str:
    """The email the manifest sealed as this installation's owner, or empty.

    Read from the manifest rather than from ``ANTI_DEMO_LOCAL_OPERATOR_EMAIL``,
    and that distinction is the whole reason this function exists.
    ``apply_manifest_environment`` writes that variable, and it is called from
    ``cli.py::_serve`` and the lifecycle -- never from ``app.py``. So on the one
    runtime where this question has an answer worth asking, the deployed app,
    the variable is unset and reading it would classify every caller as a
    stranger. ``load_manifest`` consults ``ANTI_DEMO_MANIFEST_JSON`` first, which
    is exactly how the deployed app receives its manifest, so the owner is
    knowable there.

    Empty for any manifest that cannot be read or whose owner is not an email.
    Nothing downstream may read empty as a match; see :func:`audience_for`.
    """
    try:
        owner = load_manifest().owner
    except Exception:  # noqa: BLE001 - a screen may never fail on its own audience
        return ""
    owner = owner.strip().casefold()
    return owner if "@" in owner else ""


def audience_for(caller_email: str | None) -> str:
    """Whether this request should be answered in operator prose or not at all.

    **Local is always the operator, unconditionally.** Not a shortcut: locally
    ``operator_from_request`` synthesises one identity for every caller, so there
    is nothing here to discriminate on and pretending otherwise would invent a
    half-trustworthy answer. It is also the correct answer -- a local checkout is
    the machine that holds the Terraform state, and the only path on which any
    advice this surface gives can actually be followed. The local single-process
    path is how every bout to date has been run, and it is unchanged.

    **Deployed compares the forwarded end-user identity against the sealed
    owner.** Databricks Apps forwards the authenticated user on every request and
    this application already depends on it: every control route calls
    ``operator_from_request``, which returns 401 when the headers are absent, and
    bouts have been driven from the deployed app. So the identity arrives. What
    it is worth is a narrower claim: it authenticates and does not authorise --
    the same reading ``authorisation_refusal`` takes -- and it is trusted here
    only because the consequence is a paragraph rather than a dollar.

    **An unknown on either side is a viewer.** The two mistakes are not
    symmetric. Calling the owner a viewer costs him a panel he can also get from
    ``/readyz``, from ``./antidemo status``, and in full from a local checkout.
    Calling a viewer the operator puts a Terraform state path and a CLI command
    on a projector in front of a room, which is the defect this exists to close.
    """
    if not deployed():
        return AUDIENCE_OPERATOR
    owner = sealed_owner_email()
    caller = (caller_email or "").strip().casefold()
    if owner and caller and owner == caller:
        return AUDIENCE_OPERATOR
    return AUDIENCE_VIEWER


# --------------------------------------------------------------------------
# Wording
#
# Every refusal an operator can meet lives here, in one place, so the API and
# the browser cannot end up disagreeing about what "gone" means. That
# disagreement is this project's most-repeated defect and it has three incidents
# to its name.
# --------------------------------------------------------------------------

RECOVERY_OFFERED = "offered"

def deployed_state_directory() -> str:
    """Name this installation's state directory, or describe it when unknown.

    Derived from ``state_dir_from_environ()`` rather than spelled out, which is
    the correction ``receipts.artifact_root`` already made against this exact
    literal: a hardcoded ``.anti-demo-v7`` outlived the generation that named it,
    and by v8 the sentence below was sending an operator to a superseded
    directory. There is no version-stamped fallback here for the same reason
    ``manifest_path`` has no default -- guessing is what made a second generation
    invisible.

    Unknown is the ordinary case wherever this sentence renders, and that is the
    point rather than a gap. The deployed app has no ``ANTI_DEMO_MANIFEST`` at
    all -- its manifest arrives as a secret environment variable -- so the
    directory being described sits on a *different machine* and this process
    cannot see it. Described rather than named there, which is weaker and true,
    instead of named and wrong.
    """
    directory = state_dir_from_environ()
    if directory is None:
        return "the installation's state directory on the operator's machine"
    return f"{directory}/"


def deployed_refusal() -> str:
    """Why nothing deployed can re-create anything, with no path it cannot see.

    A function rather than a constant because the directory it names is resolved
    from the environment on every read. One name, so the API's two refusals and
    ``build_offer`` cannot drift apart -- that disagreement is this project's
    most-repeated defect.
    """
    return (
        "RECOVERY CANNOT RUN HERE. This is the deployed Databricks App, and its "
        "inability to re-create anything is physical rather than a policy: it "
        "has no Terraform state (the state file is a local file under "
        f"{deployed_state_directory()}), no terraform, aws or databricks "
        "binaries, and no manifest path at all -- the manifest arrives as a "
        "secret environment variable -- so it cannot even take the mutation "
        f"lock that guards a mutation. Run '{INSTALLATION_REPAIR_COMMAND}' from "
        "a checkout on the operator's machine. This screen will notice when it "
        "finishes."
    )

REFUSAL_UNVERIFIED_HEAD = (
    "RECOVERY IS REFUSED: THE ACCOUNT COULD NOT BE READ, WHICH IS NOT THE SAME "
    "AS THE RESOURCES BEING GONE."
)

REFUSAL_NEVER_CHECKED = (
    "RECOVERY IS REFUSED: THE ACCOUNT HAS NOT BEEN READ YET in this process, so "
    "nothing here knows whether the sealed resources exist. This is not a report "
    "that anything is missing. Press 'Check the account now' -- the answer takes "
    "a few seconds -- and this screen will say which of the three answers it is."
)

REFUSAL_PRESENT = (
    "NOTHING TO RECOVER: the account was read and every sealed resource is in "
    "it. Re-creating them would spend money to no purpose. If a round is still "
    "failing, the cause is not missing infrastructure."
)

REFUSAL_MUTATION_IN_PROGRESS = (
    "RECOVERY IS REFUSED: A MUTATION IS ALREADY IN FLIGHT on this generation. "
    "Two installers over one Terraform state is the one thing the generation "
    "lock exists to prevent, and the second would be refused by the lock anyway."
)

REFUSAL_CLEANUP_FAILED = (
    "RECOVERY IS REFUSED: THE MANIFEST STATUS IS 'cleanup_failed', which "
    "'antidemo setup' refuses by name rather than resuming. A half-finished cleanup "
    "needs a person to decide what survived before anything re-creates anything. "
    "Run 'antidemo cleanup --dry-run' from a shell and read what it finds."
)

REFUSAL_ATTEMPT_RUNNING = (
    "RECOVERY IS REFUSED: A RECOVERY IS ALREADY RUNNING. Watch it below rather "
    "than starting a second one; a second installer would be refused by the "
    "generation lock and would leave a corpse in the log for the next reader to "
    "misdiagnose."
)


def unverified_refusal(presence: InstallationPresence, usd_per_day: str) -> str:
    """What to say when the sweep failed, which is what a real reap looks like.

    Deliberately long, and deliberately about credentials rather than about
    databases. Conflating "I could not look" with "it is gone" is the defect
    this whole surface exists to avoid, and the expensive direction of that
    mistake is exactly here: the button is one click from about ${usd}/day of
    infrastructure that may already be running.
    """
    return (
        f"{REFUSAL_UNVERIFIED_HEAD} {presence.reason.capitalize()}, so the "
        f"{presence.sealed} sealed resources are neither confirmed present nor "
        "confirmed gone.\n"
        "\n"
        "Fix the read, not the infrastructure. The usual cause is a lapsed SSO "
        "session:\n"
        "\n"
        "    aws sso login    # or whatever refreshes this install's AWS credentials\n"
        "    ./antidemo status\n"
        "\n"
        "This screen answers itself within about 30 seconds of the credentials "
        "coming back, with no restart.\n"
        "\n"
        "Read this next sentence before reaching for a workaround: if this "
        "account has automation that deletes idle resources, it takes the IAM "
        "users as well as the databases, so a real reap also lands here rather "
        "than on a confirmed absence. Where such a sweep exists that makes this "
        "state the likeliest way a genuine loss shows up -- and it still is not "
        f"proof of one. Re-creating on a guess costs about ${usd_per_day}/day. "
        "Restore the credential first; if the resources really are gone, the "
        "very next sweep will say so and the button will appear."
    )


# --------------------------------------------------------------------------
# Cost
# --------------------------------------------------------------------------


#: What the quoted figure does and does not count, said out loud beside it. A
#: number with no basis is how "about $35-40/day" became folklore in this tree.
COST_BASIS = (
    "AWS standing cost for 24 hours at public on-demand rates: the "
    "required_monthly_carrying_cost and installation_overhead scopes of "
    "server/cost_model.estimate_carrying_cost, which is the same arithmetic the "
    "ringside standing-cost disclosure quotes. It excludes Databricks and "
    "Lakebase consumption and anything a bout burns while running."
)


def daily_cost_usd() -> Decimal:
    """What keeping this installation alive costs for a day, from the cost model.

    Not a new constant. `estimate_carrying_cost` is the same arithmetic the
    standing-cost disclosure quotes on the ringside screen, so the figure the
    confirmation states cannot drift away from the figure the demo shows.
    """
    estimate = estimate_carrying_cost(CarryingWindow(seconds=Decimal(86400)))
    return estimate.total_usd(EstimateScope.CARRYING) + estimate.total_usd(
        EstimateScope.OVERHEAD
    )


def _money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def confirmation_phrase(run_id: str, usd_per_day: str) -> str:
    """The exact words the operator has to type before anything is spent.

    Two properties matter and both are deliberate. It **names what will be
    created** -- the generation, by its own run id -- and it **states the
    money**, so the act of confirming cannot be performed without having read
    both. And it is issued by the server: it cannot be guessed from the UI
    alone, cannot be defaulted into the request body, and a client that sends a
    stale one is refused. That is the same shape as `cleanup --force-round6`,
    which makes an operator type the environment's own token.
    """
    return f"recreate {run_id} for ${usd_per_day} a day"


# --------------------------------------------------------------------------
# Durable attempt journal
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RecoveryPaths:
    """Where one generation's recovery bookkeeping lives."""

    directory: Path
    journal: Path

    def attempt(self, attempt_id: str) -> Path:
        return self.directory / f"{attempt_id}.json"

    def log(self, attempt_id: str) -> Path:
        return self.directory / f"{attempt_id}.log"


def recovery_paths() -> RecoveryPaths:
    """Resolve the recovery directory, or raise if this install has no manifest.

    Raising is the point in deployed mode: there is no manifest path there, so
    there is nowhere to record an attempt and nothing to rate-limit against.
    """
    root = manifest_path().parent / RECOVERY_DIR_NAME
    return RecoveryPaths(directory=root, journal=root / ATTEMPT_JOURNAL_NAME)


@dataclass(frozen=True, slots=True)
class RateVerdict:
    """Whether the budget allows a spend, and how much of it is left."""

    allowed: bool
    refusal: str
    attempts_in_window: int
    seconds_until_next: float


class RecoveryJournal:
    """Every spawn this generation has ever authorised, on disk, append-only.

    Read from the file on every question rather than cached in the process. The
    whole reason this exists is to survive a restart -- a limiter that lives in
    memory turns a crash loop into a provisioning loop -- so an in-memory copy
    would be the bug wearing the fix's clothes.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def entries(self) -> list[dict[str, Any]]:
        """Every readable record, oldest first. A corrupt line is skipped, not fatal."""
        try:
            raw = self.path.read_text(encoding="utf-8")
        except OSError:
            return []
        records: list[dict[str, Any]] = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
            except ValueError:
                continue
            if isinstance(parsed, dict):
                records.append(parsed)
        return records

    def stamps(self) -> list[float]:
        stamps: list[float] = []
        for record in self.entries():
            try:
                stamps.append(float(record["at_epoch"]))
            except (KeyError, TypeError, ValueError):
                continue
        return sorted(stamps)

    def verdict(self, now: float) -> RateVerdict:
        stamps = self.stamps()
        within = [stamp for stamp in stamps if now - stamp <= RATE_WINDOW_SECONDS]
        if stamps:
            since = now - max(stamps)
            if since < MIN_SECONDS_BETWEEN_ATTEMPTS:
                wait = MIN_SECONDS_BETWEEN_ATTEMPTS - since
                return RateVerdict(
                    allowed=False,
                    refusal=(
                        "RECOVERY IS RATE LIMITED: the last attempt was "
                        f"{int(since // 60)} minute(s) ago and this generation "
                        f"allows one every {int(MIN_SECONDS_BETWEEN_ATTEMPTS // 60)} "
                        f"minutes. Wait about {int(wait // 60) + 1} more minute(s). "
                        "The limit is read from disk, so restarting the server "
                        "does not clear it -- that is deliberate: a restart loop "
                        "over a fresh budget is a provisioning loop, and the "
                        "failure mode is a bill."
                    ),
                    attempts_in_window=len(within),
                    seconds_until_next=wait,
                )
        if len(within) >= MAX_ATTEMPTS_PER_WINDOW:
            oldest = min(within)
            wait = RATE_WINDOW_SECONDS - (now - oldest)
            return RateVerdict(
                allowed=False,
                refusal=(
                    f"RECOVERY IS RATE LIMITED: {len(within)} attempts have "
                    f"already been authorised in the last 24 hours, which is the "
                    f"limit of {MAX_ATTEMPTS_PER_WINDOW}. Three failures in a day "
                    "is a problem a person needs to look at, not one more "
                    "install. Read the attempt log below, then run "
                    f"'{INSTALLATION_REPAIR_COMMAND}' from a shell if you still "
                    f"want it. The budget reopens in about {int(wait // 3600) + 1} "
                    "hour(s), and it is read from disk so a restart does not "
                    "reset it."
                ),
                attempts_in_window=len(within),
                seconds_until_next=wait,
            )
        return RateVerdict(
            allowed=True,
            refusal="",
            attempts_in_window=len(within),
            seconds_until_next=0.0,
        )

    def record(self, payload: dict[str, Any]) -> None:
        """Append one authorisation. Written *before* the fork, never after.

        Order matters and it is the conservative one: a spawn that fails after
        the record still costs a slot. Recording afterwards would mean a crash
        between fork and write leaves an installer running that the limiter has
        never heard of.
        """
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
        with contextlib.suppress(OSError):
            os.chmod(self.path, 0o600)


# --------------------------------------------------------------------------
# Attempt records: the progress channel
#
# A file per attempt, written by the detached mutator and polled over HTTP.
# The SSE stream cannot carry this: it is session-scoped
# (`/api/sessions/{id}/events`) and gated behind `manager.get(session_id)`, and
# during a recovery there is no session -- in the wait state the manager is a
# stub whose `create` refuses. A second stream would also be the wrong shape
# even if it existed, because the thing being watched outlives the watcher: the
# installer is a detached process, so the server may be restarted, or replaced
# by its supervisor, while it runs. A file survives that; an in-process
# subscriber does not.
# --------------------------------------------------------------------------

PHASE_SPAWNED = "spawned"
PHASE_RUNNING = "running"
PHASE_SUCCEEDED = "succeeded"
PHASE_FAILED = "failed"
PHASE_LOST = "lost"


@dataclass(frozen=True, slots=True)
class AttemptView:
    """One recovery attempt as the browser sees it."""

    attempt_id: str
    phase: str
    detail: str
    started_at: str = ""
    finished_at: str = ""
    exit_code: int | None = None
    pid: int | None = None
    log_tail: tuple[str, ...] = field(default_factory=tuple)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _write_attempt(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.new")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with contextlib.suppress(OSError):
        os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _read_attempt_file(path: Path) -> dict[str, Any] | None:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


#: How long a spawned attempt may go without reporting its own pid before it is
#: called lost. Generous, because the alternative failure is worse: a healthy
#: install reported as dead sends an operator to re-press a button that would be
#: refused by the generation lock the running installer holds.
SPAWN_REPORT_GRACE_SECONDS = 60.0


def _age_seconds(stamp: str) -> float:
    """Seconds since an ISO timestamp, or infinity if it cannot be read.

    Unreadable means "old": a record with no legible start cannot be waited on,
    and treating it as brand new would leave it pending forever.
    """
    try:
        return max((datetime.now(UTC) - datetime.fromisoformat(stamp)).total_seconds(), 0.0)
    except (TypeError, ValueError):
        return float("inf")


def _log_tail(path: Path, lines: int = 40) -> tuple[str, ...]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ()
    return tuple(text.splitlines()[-lines:])


def attempt_view(attempt_id: str, *, paths: RecoveryPaths | None = None) -> AttemptView | None:
    """Read one attempt's progress. Never blocks, never provokes a mutation."""
    resolved = paths or recovery_paths()
    record = _read_attempt_file(resolved.attempt(attempt_id))
    if record is None:
        return None
    phase = str(record.get("phase") or PHASE_SPAWNED)
    pid = record.get("pid")
    pid = int(pid) if isinstance(pid, int) else None
    detail = str(record.get("detail") or "")
    # Two ways an attempt can be gone, and they need different tests. Once it is
    # `running` it has told us its pid, so a dead pid is proof. Before that it
    # has no pid to check, and calling it lost immediately would report every
    # healthy spawn as a failure during the moment between the fork and the
    # child's first write -- so the only honest answer there is a deadline.
    vanished = (phase == PHASE_RUNNING and (pid is None or not _pid_is_alive(pid))) or (
        phase == PHASE_SPAWNED
        and _age_seconds(str(record.get("started_at") or "")) > SPAWN_REPORT_GRACE_SECONDS
    )
    if vanished:
        # The mutator vanished without recording an ending. Say so rather than
        # showing a progress bar that will never move: an interrupted provision
        # leaves the manifest transitional, and `antidemo setup` resumes from there.
        phase = PHASE_LOST
        detail = (
            "The recovery process is gone and never recorded an ending, so it "
            "was killed or the machine slept. Provisioning is resumable: the "
            "manifest is left part-way through and "
            f"'{INSTALLATION_REPAIR_COMMAND}' picks up where it stopped rather "
            "than starting over. Read the log below before re-pressing."
        )
    return AttemptView(
        attempt_id=attempt_id,
        phase=phase,
        detail=detail,
        started_at=str(record.get("started_at") or ""),
        finished_at=str(record.get("finished_at") or ""),
        exit_code=record.get("exit_code") if isinstance(record.get("exit_code"), int) else None,
        pid=pid,
        log_tail=_log_tail(resolved.log(attempt_id)),
    )


def latest_attempt(*, paths: RecoveryPaths | None = None) -> AttemptView | None:
    """The most recently authorised attempt, from the durable journal."""
    resolved = paths or recovery_paths()
    entries = RecoveryJournal(resolved.journal).entries()
    if not entries:
        return None
    newest = max(entries, key=lambda record: float(record.get("at_epoch") or 0.0))
    attempt_id = str(newest.get("attempt_id") or "")
    if not attempt_id:
        return None
    return attempt_view(attempt_id, paths=resolved)


# --------------------------------------------------------------------------
# Presence, with a timestamp this module can actually vouch for
# --------------------------------------------------------------------------

_OBSERVED: tuple[float, InstallationPresence | None] = (0.0, None)


def reset_observation() -> None:
    """Forget this module's timestamp. For tests."""
    global _OBSERVED
    _OBSERVED = (0.0, None)


async def observe_presence(*, force: bool = False) -> tuple[InstallationPresence, float]:
    """The presence verdict and the monotonic moment it was taken.

    `installation_presence` caches for `INSTALLATION_PRESENCE_TTL_SECONDS` and
    exposes no timestamp, so a surface that reported "checked just now" off it
    would be claiming freshness it had not established -- the exact defect this
    feature exists to avoid. The stamp kept here is honest in the direction that
    matters: if `/readyz` warmed the shared cache more recently than this module
    last asked, the age reported here is *older* than the truth. Overstating
    staleness costs a needless sweep; understating it would authorise a spend on
    a stale verdict.

    ``force=True`` drops the shared cache first, so the answer is a live read
    taken now. That is what D9a's second condition means by "just confirmed",
    and it is the only mode the spend path uses.
    """
    global _OBSERVED
    if force:
        reset_installation_presence_cache()
        presence = await installation_presence_async()
        _OBSERVED = (time.monotonic(), presence)
        return presence, 0.0
    stamped_at, cached = _OBSERVED
    moment = time.monotonic()
    if cached is not None and moment - stamped_at < INSTALLATION_PRESENCE_TTL_SECONDS:
        return cached, moment - stamped_at
    presence = await installation_presence_async()
    _OBSERVED = (moment, presence)
    return presence, 0.0


# --------------------------------------------------------------------------
# What a press would actually do
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ManifestState:
    """The manifest as the recovery surface needs it: status, run id, recovery text."""

    exists: bool
    status: str = ""
    run_id: str = ""
    transitional_recovery: str = ""
    unreadable: str = ""


def manifest_state() -> ManifestState:
    """Re-read the manifest from disk. Every call, deliberately.

    The readiness gate already re-reads it on every control action, which is why
    a running server starts refusing with a naming 409 the instant another
    process flips the status. This surface has to agree with that gate, and the
    only way to agree with something that re-reads is to re-read.
    """
    try:
        path = manifest_path()
    except RuntimeError as exc:
        return ManifestState(exists=False, unreadable=str(exc))
    if not path.exists():
        return ManifestState(exists=False)
    try:
        manifest = load_manifest(path)
    except Exception as exc:
        return ManifestState(exists=True, unreadable=f"{type(exc).__name__}")
    recovery = ""
    with contextlib.suppress(RuntimeError, OSError, ValueError):
        recovery = transitional_status_recovery(manifest.status, manifest_path=path) or ""
    return ManifestState(
        exists=True,
        status=manifest.status,
        run_id=manifest.run_id,
        transitional_recovery=recovery,
    )


def recovery_plan(state: ManifestState) -> str:
    """Name which of the three things `antidemo setup` would do, before it does it.

    A single button that means three different things is the dishonesty this
    project keeps rediscovering. `setup` branches on the manifest: no manifest
    provisions from scratch, a transitional status resumes from where it
    stopped, and a `ready` status re-applies Terraform and reseeds.
    """
    if not state.exists:
        return (
            "There is no manifest, so this would be a first provision: it "
            "creates the AWS resources and the Databricks and Lakebase ones, "
            "and seals a new generation."
        )
    if state.status in TRANSITIONAL_STATUSES:
        return (
            f"The manifest is '{state.status}', so this would RESUME an "
            "interrupted provision from where it stopped rather than starting "
            "over. Nothing already built is rebuilt."
        )
    if state.status == "ready":
        return (
            "The manifest still says 'ready', which is what a sweep leaves "
            "behind, so this would re-apply Terraform -- re-creating the deleted "
            "AWS resources -- and then reseed Rounds 2 and 3 and re-verify 4 and "
            "6. That is not destructive to infrastructure. It IS destructive to "
            "demo state: any bout in progress loses its baseline. It does not "
            "re-create Databricks or Lakebase resources; only a first provision "
            "does that."
        )
    return (
        f"The manifest status is '{state.status}', which is not a state "
        "recovery knows how to act on."
    )


# --------------------------------------------------------------------------
# Authorisation
# --------------------------------------------------------------------------


def authorisation_refusal(client_host: str, token: str | None) -> str:
    """Whether this caller may spend money, and what that answer assumes.

    D9 protected four things and spawning preserves three; the fourth is that
    the authority to spend money lived behind a shell prompt, and an HTTP route
    moves it behind a port. `operator_from_request` is no help -- it fabricates
    "Local operator" from environment variables locally, and in deployed mode it
    authenticates without authorising.

    So the answer here is a property of the socket, not of a claimed identity:
    **a request from loopback is treated as already having shell access to this
    machine**, which is exactly the authority `antidemo setup` requires. That
    assumption is stated rather than hidden, and it is false in one situation --
    a server bound to a routable address, where a loopback source address can be
    a proxy on the same host rather than a person at the keyboard. For that case
    the shared secret in `ANTI_DEMO_RECOVERY_TOKEN` is required as well: set it,
    and loopback stops being sufficient.
    """
    expected = os.environ.get(RECOVERY_TOKEN_ENV, "").strip()
    if expected:
        if (token or "").strip() != expected:
            return (
                "RECOVERY IS REFUSED: this server has "
                f"{RECOVERY_TOKEN_ENV} set, so a recovery must present it in the "
                f"'{RECOVERY_TOKEN_HEADER}' header. The token is the operator's "
                "own; it is not shown in this UI and never travels in a URL."
            )
        return ""
    if client_host not in _LOOPBACK_HOSTS:
        where = client_host or "an unknown address"
        return (
            f"RECOVERY IS REFUSED: this request came from {where}, not from this "
            "machine. Recovery treats a loopback connection as proof of shell "
            "access, which is the authority provisioning has always needed. A "
            "request from anywhere else must present the operator's own "
            f"{RECOVERY_TOKEN_ENV} in the '{RECOVERY_TOKEN_HEADER}' header."
        )
    return ""


# --------------------------------------------------------------------------
# The spawn
# --------------------------------------------------------------------------


def mutator_command() -> list[str]:
    """The argv of the thing that actually provisions.

    `./antidemo setup --no-serve` and nothing else. `--no-serve` matters: without it
    the installer would try to start a second server on the port this one is
    already answering on.
    """
    override = os.environ.get(RECOVERY_COMMAND_ENV, "").strip()
    if override:
        parsed = json.loads(override)
        if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
            raise ValueError(f"{RECOVERY_COMMAND_ENV} must be a JSON list of strings")
        return parsed
    return [str(PROJECT_ROOT / "antidemo"), "setup", "--no-serve"]


def child_environment(environ: dict[str, str], attempt_id: str) -> dict[str, str]:
    """The environment the mutator gets: this one, minus the lock's escape hatch.

    `ANTI_DEMO_GENERATION_LOCK_TOKEN` is the mutation lock's reentrancy escape
    hatch -- a child presenting a live token is waved past a held lock. A server
    started from a shell that was itself holding the lock would inherit that
    token and hand its child a free pass past the exclusion the whole design
    rests on. Scrubbed here rather than trusted not to be set.
    """
    child = dict(environ)
    child.pop(LOCK_TOKEN_ENV, None)
    child[ATTEMPT_ENV] = attempt_id
    return child


#: How long the serving process will wait for the two halves of the spawn
#: handshake -- the intermediate exiting, and the daemon announcing its pid down
#: the pipe. Both are microseconds of work in the healthy case: the announcement
#: is written *before* `execve`, so it does not include the child's interpreter
#: startup, and the intermediate `os._exit`s immediately after its own fork.
#: Measured at 5-7ms end to end for the whole of `_spawn_detached`, so this is
#: some three orders of magnitude of headroom and cannot be reached by a child
#: that is merely slow.
#:
#: It exists because the alternative to a bound here is not a slower answer, it
#: is no answer: see `_reap_intermediate` for what the unbounded version did.
SPAWN_HANDSHAKE_TIMEOUT_SECONDS = 10.0


def _reap_intermediate(pid: int, *, timeout: float) -> None:
    """Reap the first fork's child, giving up rather than waiting forever.

    This was `os.waitpid(intermediate, 0)`, and the missing bound was the
    serious half of a fork-safety defect. `os.fork` below runs from a process
    that is multi-threaded in production *by construction*: the endpoint that
    reaches here, `api.recover_installation`, awaits `observe_presence(force=
    True)` on the line before, and that is `asyncio.to_thread` -- so the
    executor's worker is alive and forking is exactly the hazard CPython's
    `DeprecationWarning: ... use of fork() may lead to deadlocks in the child`
    is complaining about. Only the calling thread survives into the child, so a
    lock another thread held at fork time is held forever there, and the child
    allocates -- `os.execve` alone has to build argv and envp -- before it ever
    reaches `execve`.

    If the child does wedge that way it never writes its pid and never exits, so
    an unbounded wait here never returns. `recover_installation` is `async def`
    and calls `spawn_recovery` inline, so that wait is on the event loop: the
    whole server would stop answering, with no log line and no timeout, and the
    code path that had stopped is the one that recovers a broken installation.
    A bound turns that into a verdict the existing machinery can already
    express -- see `_spawn_detached` for how -- rather than a silent hang.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            reaped, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            # Already reaped -- an asyncio child watcher installed by some other
            # part of the process is entitled to have got there first.
            return
        except OSError:
            return
        if reaped == pid or time.monotonic() >= deadline:
            return
        time.sleep(0.001)


def _announced_pid(read_fd: int, *, timeout: float) -> bytes:
    """Read the daemon's announcement of its own pid, or give up saying nothing.

    Bounded for the same reason as `_reap_intermediate`, and it is the more
    exposed of the two: the write end is held open by the child, so a child that
    wedges before its `os.write` leaves this pipe neither readable nor closed and
    an unbounded `os.read` blocks on the event loop forever.

    Returning empty on a timeout is deliberately not a new outcome. It is the
    same bytes an already-dead child produces, and `_spawn_detached` has always
    read that as "pid unknown".
    """
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return b""
        try:
            readable, _, _ = select.select([read_fd], [], [], remaining)
        except OSError:
            return b""
        if not readable:
            return b""
        try:
            return os.read(read_fd, 64)
        except OSError:
            return b""


def _spawn_detached(command: list[str], *, cwd: Path, log: Path, env: dict[str, str]) -> int:
    """Double-fork the mutator into its own session and report its pid.

    Two forks, not one. The first gets out of this process group so `setsid`
    can succeed and lets the intermediate be reaped immediately -- a `Popen` we
    never wait on would leave a zombie in a server that runs for weeks. The
    second gives up session leadership so the daemon can never be handed a
    controlling terminal.

    Every descriptor above stderr is closed in the child. The generation lock
    lives on an open file description and survives `fork`, so a copy inherited
    from -- for instance -- a shell that opened it would otherwise be held by
    this long-lived child and deadlock every future mutation.
    """
    log.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    read_fd, write_fd = os.pipe()
    intermediate = os.fork()
    if intermediate == 0:  # pragma: no cover - exercised only in the forked child
        try:
            os.close(read_fd)
            os.setsid()
            if os.fork() > 0:
                os._exit(0)
            os.chdir(cwd)
            os.umask(0o077)
            log_fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            null_fd = os.open(os.devnull, os.O_RDONLY)
            os.dup2(null_fd, 0)
            os.dup2(log_fd, 1)
            os.dup2(log_fd, 2)
            for descriptor in (null_fd, log_fd):
                if descriptor > 2:
                    os.close(descriptor)
            os.write(write_fd, f"{os.getpid()}\n".encode())
            os.close(write_fd)
            os.closerange(3, 4096)
            os.execve(command[0], command, env)
        except BaseException:
            os._exit(127)
    os.close(write_fd)
    # Both halves of the handshake are bounded. A pid of 0 -- which is what an
    # unreadable announcement has always produced -- leaves the attempt record
    # saying `spawned` with no pid, and `attempt_view` calls that `lost` once
    # `SPAWN_REPORT_GRACE_SECONDS` has passed, with wording that tells the
    # operator provisioning is resumable. That is a slow, readable, *loud*
    # failure. What it replaces is the server hanging on the event loop with
    # nothing written anywhere.
    _reap_intermediate(intermediate, timeout=SPAWN_HANDSHAKE_TIMEOUT_SECONDS)
    announced = _announced_pid(read_fd, timeout=SPAWN_HANDSHAKE_TIMEOUT_SECONDS)
    with contextlib.suppress(OSError):
        os.close(read_fd)
    try:
        return int(announced.decode().strip())
    except ValueError:
        return 0


@dataclass(frozen=True, slots=True)
class SpawnResult:
    attempt_id: str
    pid: int
    log_path: str


def spawn_recovery(
    *,
    run_id: str,
    plan: str,
    operator: str,
    usd_per_day: str,
    paths: RecoveryPaths | None = None,
    now: float | None = None,
) -> SpawnResult:
    """Record the authorisation, then fork the mutator. In that order.

    Nothing in here takes the generation lock, writes a manifest, or declares
    readiness. The child does the first of those; the other two are nobody's
    business but the installer's.
    """
    resolved = paths or recovery_paths()
    moment = time.time() if now is None else now
    attempt_id = datetime.fromtimestamp(moment, UTC).strftime("%Y%m%dT%H%M%SZ")
    # A same-second second press would otherwise overwrite the first attempt's
    # file; the rate limiter makes this all but unreachable, but "all but" is
    # not a guarantee and the collision would hide a running installer.
    suffix = 0
    while resolved.attempt(attempt_id).exists():
        suffix += 1
        attempt_id = f"{attempt_id}-{suffix}"

    command = mutator_command()
    log_path = resolved.log(attempt_id)
    attempt_path = resolved.attempt(attempt_id)

    RecoveryJournal(resolved.journal).record(
        {
            "attempt_id": attempt_id,
            "at_epoch": moment,
            "at": datetime.fromtimestamp(moment, UTC).isoformat(),
            "run_id": run_id,
            "operator": operator,
            "usd_per_day": usd_per_day,
            "plan": plan,
            "command": command,
        }
    )
    _write_attempt(
        attempt_path,
        {
            "attempt_id": attempt_id,
            "phase": PHASE_SPAWNED,
            "detail": "The installer has been launched and has not reported in yet.",
            "started_at": _now_iso(),
            "run_id": run_id,
            "plan": plan,
            "operator": operator,
            "usd_per_day": usd_per_day,
            "command": command,
            "log": str(log_path),
            "pid": None,
        },
    )

    pid = _spawn_detached(
        [sys.executable, "-m", "server.selfheal", "--attempt", str(attempt_path)],
        cwd=PROJECT_ROOT,
        log=log_path,
        env=child_environment(dict(os.environ), attempt_id),
    )
    logger.warning(
        "Recovery attempt %s spawned as pid %s; this process provisions nothing itself",
        attempt_id,
        pid,
    )
    return SpawnResult(attempt_id=attempt_id, pid=pid, log_path=str(log_path))


# --------------------------------------------------------------------------
# The detached child
# --------------------------------------------------------------------------


def run_attempt(attempt_path: Path) -> int:
    """Run the recorded mutator and keep the progress file honest throughout.

    This wrapper does not take the generation lock and must not: `antidemo setup`
    takes it itself, inside its own process, which is what keeps the exclusion
    with any concurrent installer real rather than advisory.
    """
    record = _read_attempt_file(attempt_path)
    if record is None:
        return 78
    command = record.get("command")
    if not isinstance(command, list) or not command:
        return 78
    record.update(
        {
            "phase": PHASE_RUNNING,
            "pid": os.getpid(),
            "detail": (
                "The installer is running. It re-applies Terraform and reseeds, "
                "which takes minutes; this page keeps polling."
            ),
            "started_at": record.get("started_at") or _now_iso(),
        }
    )
    _write_attempt(attempt_path, record)
    try:
        completed = subprocess.run(command, check=False)  # noqa: S603
        code = completed.returncode
    except Exception as exc:
        record.update(
            {
                "phase": PHASE_FAILED,
                "exit_code": None,
                "finished_at": _now_iso(),
                "detail": f"The installer could not be started ({type(exc).__name__}).",
            }
        )
        _write_attempt(attempt_path, record)
        return 70
    record.update(
        {
            "phase": PHASE_SUCCEEDED if code == 0 else PHASE_FAILED,
            "exit_code": code,
            "finished_at": _now_iso(),
            "detail": (
                "The installer finished. Nothing here declares the installation "
                "healthy -- press 'Check the account now' and read the sweep, "
                "because a green exit over an empty account is exactly the "
                "false-readiness this surface exists to refuse."
                if code == 0
                else (
                    f"The installer exited {code}. Read the log below. "
                    "Provisioning is resumable: a re-press continues from where "
                    "it stopped rather than starting over."
                )
            ),
        }
    )
    _write_attempt(attempt_path, record)
    return code


def _cli(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2 or args[0] != "--attempt":
        print("usage: python -m server.selfheal --attempt <path>", file=sys.stderr)
        return 64
    return run_attempt(Path(args[1]))


# --------------------------------------------------------------------------
# The whole verdict, assembled once
# --------------------------------------------------------------------------


#: Machine-readable reasons, so the browser can branch on the answer instead of
#: matching on prose -- and so an HTTP status can be chosen without a second
#: decision that could disagree with the first.
CODE_OFFERED = "offered"
CODE_DEPLOYED = "deployed"
CODE_UNVERIFIED = "unverified"
CODE_NEVER_CHECKED = "never_checked"
CODE_PRESENT = "present"
CODE_CLEANUP_FAILED = "cleanup_failed"
CODE_MUTATION_IN_PROGRESS = "mutation_in_progress"
CODE_ATTEMPT_RUNNING = "attempt_running"
CODE_RATE_LIMITED = "rate_limited"


@dataclass(frozen=True, slots=True)
class RecoveryOffer:
    """Whether the button exists, and if not, exactly why not."""

    offered: bool
    code: str
    refusal: str
    confirmation_phrase: str
    usd_per_day: str
    plan: str
    attempts_in_window: int
    attempts_allowed: int


def mutation_in_progress() -> tuple[bool, str]:
    """Whether somebody else is mid-mutation, and who. Never holds the lock."""
    try:
        lock_path = generation_lock_path(manifest_path())
    except (RuntimeError, OSError, ValueError):
        return False, ""
    if not lock_is_held(lock_path):
        return False, ""
    return True, describe_holder(lock_path, read_holder(lock_path))


def build_offer(
    presence: InstallationPresence,
    state: ManifestState,
    *,
    now: float | None = None,
) -> RecoveryOffer:
    """The single place that decides whether money may be spent.

    Both the GET that renders the screen and the POST that spends run through
    this, so the button can never appear for a reason the endpoint would refuse
    -- and, more to the point, can never fail to appear for a reason the
    endpoint would allow.
    """
    usd = _money(daily_cost_usd())
    plan = recovery_plan(state)

    def refuse(code: str, refusal: str, *, attempts: int = 0) -> RecoveryOffer:
        return RecoveryOffer(
            offered=False,
            code=code,
            refusal=refusal,
            confirmation_phrase="",
            usd_per_day=usd,
            plan=plan,
            attempts_in_window=attempts,
            attempts_allowed=MAX_ATTEMPTS_PER_WINDOW,
        )

    if deployed():
        return refuse(CODE_DEPLOYED, deployed_refusal())
    # The three states that are not a verified absence, refused before anything
    # else is even consulted. `unverified` first because it is the one a real
    # sweep produces, and the one whose refusal has to be read.
    if presence.state == PRESENCE_UNVERIFIED:
        return refuse(CODE_UNVERIFIED, unverified_refusal(presence, usd))
    if presence.state == PRESENCE_NEVER_CHECKED:
        return refuse(CODE_NEVER_CHECKED, REFUSAL_NEVER_CHECKED)
    if presence.state != PRESENCE_MISSING:
        return refuse(CODE_PRESENT, REFUSAL_PRESENT)
    if state.status == "cleanup_failed":
        return refuse(CODE_CLEANUP_FAILED, REFUSAL_CLEANUP_FAILED)
    held, holder = mutation_in_progress()
    if held:
        return refuse(
            CODE_MUTATION_IN_PROGRESS, f"{REFUSAL_MUTATION_IN_PROGRESS}\n\n{holder}"
        )
    try:
        paths = recovery_paths()
    except (RuntimeError, OSError, ValueError):
        return refuse(CODE_DEPLOYED, deployed_refusal())
    running = latest_attempt(paths=paths)
    if running is not None and running.phase in {PHASE_SPAWNED, PHASE_RUNNING}:
        return refuse(CODE_ATTEMPT_RUNNING, REFUSAL_ATTEMPT_RUNNING)
    verdict = RecoveryJournal(paths.journal).verdict(time.time() if now is None else now)
    if not verdict.allowed:
        return refuse(
            CODE_RATE_LIMITED, verdict.refusal, attempts=verdict.attempts_in_window
        )
    return RecoveryOffer(
        offered=True,
        code=CODE_OFFERED,
        refusal="",
        confirmation_phrase=confirmation_phrase(state.run_id, usd),
        usd_per_day=usd,
        plan=plan,
        attempts_in_window=verdict.attempts_in_window,
        attempts_allowed=MAX_ATTEMPTS_PER_WINDOW,
    )


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(_cli())
