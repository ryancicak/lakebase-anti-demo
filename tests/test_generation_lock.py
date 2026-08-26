"""Exclusion between real processes, tested with real processes.

A concurrency fix that is only exercised in one process proves nothing: the
whole question is what the *kernel* does with two file descriptions, so most of
what follows spawns an actual interpreter and races it. The one thing never
touched is `.anti-demo-v7`; every test owns a `tmp_path` generation.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import psycopg
import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from server import cli as cli_module
from server.generation_lock import (
    GENERATION_LOCK_NAME,
    LOCK_TOKEN_ENV,
    GenerationBusyError,
    LockHolder,
    describe_holder,
    generation_lock_path,
    hold_generation,
    lock_is_held,
    read_holder,
    transitional_status_recovery,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Acquires the generation lock in a separate process, announces that it has it,
#: and then waits to be told to stop -- or to be killed, which is the case that
#: matters most.
_HOLDER_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from pathlib import Path
    sys.path.insert(0, sys.argv[1])
    from server.generation_lock import hold_generation

    manifest, ready, stop, operation = (Path(sys.argv[2]), Path(sys.argv[3]),
                                        Path(sys.argv[4]), sys.argv[5])
    with hold_generation(manifest, operation):
        ready.write_text("held")
        while not stop.exists():
            time.sleep(0.02)
    """
)


@pytest.fixture
def generation(tmp_path: Path) -> Path:
    directory = tmp_path / ".anti-demo-v7"
    directory.mkdir()
    return directory / "manifest.json"


class Holder:
    """A live process holding the lock, for as long as the test wants it."""

    def __init__(self, manifest: Path, operation: str) -> None:
        self._ready = manifest.parent / "holder-ready"
        self._stop = manifest.parent / "holder-stop"
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _HOLDER_SCRIPT,
                str(REPO_ROOT),
                str(manifest),
                str(self._ready),
                str(self._stop),
                operation,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def wait_until_holding(self, timeout: float = 15.0) -> Holder:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._ready.exists():
                return self
            if self.process.poll() is not None:
                _, stderr = self.process.communicate()
                raise AssertionError(f"holder exited early: {stderr.decode()}")
            time.sleep(0.02)
        raise AssertionError("holder never took the lock")

    def release(self) -> None:
        self._stop.write_text("stop")
        self.process.wait(timeout=15)

    def kill(self) -> None:
        """Die without unwinding, the way a killed mutator does."""
        self.process.send_signal(signal.SIGKILL)
        self.process.wait(timeout=15)


@pytest.fixture
def holder_factory(generation: Path):
    live: list[Holder] = []

    def make(operation: str = "antidemo reset") -> Holder:
        holder = Holder(generation, operation)
        live.append(holder)
        return holder.wait_until_holding()

    yield make
    for holder in live:
        if holder.process.poll() is None:
            holder.kill()


# --------------------------------------------------------------------------
# Two mutators racing
# --------------------------------------------------------------------------


def test_a_second_mutator_is_refused_and_told_who_holds_it(generation, holder_factory) -> None:
    """The collision that took the demo down, in the smallest form that shows it."""
    holder = holder_factory("antidemo reset")

    with pytest.raises(GenerationBusyError) as refusal:
        with hold_generation(generation, "antidemo setup", environ={}):
            pytest.fail("the second mutator must never get inside the lock")

    message = str(refusal.value)
    assert "ANOTHER PROCESS IS MUTATING THIS GENERATION" in message
    # What holds it, since when, and what to do -- all three, or the refusal is
    # just a different way of being stuck.
    assert "antidemo reset" in message
    assert f"pid {holder.process.pid}" in message
    assert "since:" in message
    assert "ps -p" in message
    assert "Do not delete the lock file" in message


def test_the_lock_is_available_again_the_moment_the_holder_finishes(
    generation, holder_factory
) -> None:
    holder = holder_factory()
    assert lock_is_held(generation_lock_path(generation)) is True

    holder.release()

    with hold_generation(generation, "antidemo setup", environ={}) as claim:
        assert claim.inherited is False
    assert lock_is_held(generation_lock_path(generation)) is False


def test_two_racing_mutators_produce_exactly_one_winner(generation) -> None:
    """Start both at once and count the ones that got in.

    Anything other than exactly one means the lock is decorative.
    """
    script = textwrap.dedent(
        """
        import sys, time
        from pathlib import Path
        sys.path.insert(0, sys.argv[1])
        from server.generation_lock import GenerationBusyError, hold_generation
        try:
            with hold_generation(Path(sys.argv[2]), sys.argv[3], environ={}):
                # Wide enough that both racers overlap on any machine.
                time.sleep(1.0)
            print("WON")
        except GenerationBusyError:
            print("REFUSED")
        """
    )
    racers = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(REPO_ROOT), str(generation), f"demo racer {index}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for index in range(2)
    ]
    results = [racer.communicate(timeout=30)[0].strip() for racer in racers]

    assert sorted(results) == ["REFUSED", "WON"], results


# --------------------------------------------------------------------------
# The stale-lock problem, which is where naive locking gets worse than none
# --------------------------------------------------------------------------


def test_a_killed_mutator_leaves_no_lock_behind(generation, holder_factory) -> None:
    """SIGKILL is the whole reason this is `flock` and not a pidfile.

    A pidfile would still be sitting there naming a dead process, and recovery
    would require an operator who happens to know to delete it. The kernel drops
    an advisory lock when the last descriptor closes, so the next mutator simply
    proceeds.
    """
    holder = holder_factory("antidemo reset")
    lock_path = generation_lock_path(generation)
    assert lock_is_held(lock_path) is True

    holder.kill()

    assert lock_is_held(lock_path) is False
    # The record is still on disk and still names the dead pid. It must not be
    # able to block anyone, because it is diagnostics, not authority.
    residue = read_holder(lock_path)
    assert residue is not None and residue.pid == holder.process.pid
    with hold_generation(generation, "antidemo setup", environ={}) as claim:
        assert claim.inherited is False
        assert claim.holder.pid == os.getpid()


def test_a_record_naming_a_dead_pid_never_blocks_a_mutator(generation) -> None:
    """Hand-written residue from an earlier generation is not a lock."""
    lock_path = generation_lock_path(generation)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps(
            {
                "pid": 999_999,
                "operation": "antidemo setup",
                "claimed_at": "2026-08-21T21:04:11Z",
                "token": "whatever",
            }
        )
    )

    with hold_generation(generation, "antidemo reset", environ={}) as claim:
        assert claim.inherited is False


def test_a_lock_held_by_an_unattributable_process_says_how_to_find_it(
    generation, holder_factory
) -> None:
    """The one case `flock` cannot explain, explained.

    If the lock is held but the recorded pid is gone, something sharing that
    process's descriptor still has it -- typically the shell that launched it.
    Guessing would be worse than useless here, so the refusal points at `lsof`.
    """
    holder_factory("antidemo reset")

    with pytest.raises(GenerationBusyError) as refusal:
        with hold_generation(
            generation,
            "antidemo setup",
            environ={},
            pid_is_alive=lambda _pid: False,
        ):
            pytest.fail("must not enter a held lock")

    message = str(refusal.value)
    assert "HOLDER UNATTRIBUTABLE" in message
    assert "lsof" in message
    assert "Do not delete the lock file" in message


def test_an_unreadable_record_on_a_held_lock_still_refuses(generation, holder_factory) -> None:
    holder_factory()
    lock_path = generation_lock_path(generation)
    # A reader can catch a payload mid-rewrite. Unparseable must mean "somebody
    # holds this and I cannot say who", never "nobody holds this".
    assert "UNIDENTIFIED" in describe_holder(lock_path, None)
    assert lock_is_held(lock_path) is True


# --------------------------------------------------------------------------
# Reads, which must never be blocked
# --------------------------------------------------------------------------


def test_reads_are_untouched_while_a_mutation_holds_the_lock(
    generation, holder_factory, monkeypatch
) -> None:
    """`antidemo status` and manifest reads are the operator's only view during a
    fifteen-minute apply. Queueing them behind it would make the tool feel
    broken exactly when it is working."""
    generation.write_text(json.dumps({"status": "seeding"}))
    holder = holder_factory("antidemo reset")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))

    # A plain read of the manifest file, which is not the locked file at all.
    assert json.loads(generation.read_text())["status"] == "seeding"

    checks = {check.name: check for check in cli_module._generation_checks()}

    assert checks["generation_lock"].ok is True
    assert checks["generation_lock"].advisory is True
    assert "DEMO RESET IS MUTATING THIS GENERATION" in checks["generation_lock"].detail
    assert f"PID {holder.process.pid}" in checks["generation_lock"].detail


def test_status_reports_a_free_generation_without_creating_a_lock(
    generation, monkeypatch
) -> None:
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))

    checks = {check.name: check for check in cli_module._generation_checks()}

    assert checks["generation_lock"].detail == "NO PROCESS IS MUTATING THIS GENERATION"
    # Reporting must not be a side effect: a read that creates the lock file
    # would make `antidemo status` the thing that writes into the generation.
    assert not generation_lock_path(generation).exists()


def test_status_on_a_wedged_generation_names_the_cure(generation, monkeypatch) -> None:
    """The dead end that made today's outage a 40-minute one.

    `status: seeding` with nothing running was recoverable only by knowing that
    resealing fixes it. `antidemo status` now says so where the operator is already
    looking.

    The sweep at the end is the half that caught a live defect. The verdict used
    to be "did `transitional_status_recovery` return advice", which is not the
    same question as "is this installation healthy": `cleanup_failed` is the one
    status with no *resume* path at all, so it returned None and printed
    `PASS manifest_status CLEANUP_FAILED` with exit 0 -- while `setup` and
    `renew` refused it by name, `_load_ready_manifest` raised `InvalidStateError`
    on it, and self-heal refused it with REFUSAL_CLEANUP_FAILED. The recoverable
    statuses failed and the unrecoverable one passed. Nothing but `ready` may
    read as healthy here, and none of these may be advisory -- an advisory line
    would print WARN and still exit 0, which is the same all-clear wearing a
    different word.
    """
    generation.write_text("{}")  # parsing is stubbed below; only the status matters
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))

    def manifest_status_for(status: str):
        monkeypatch.setattr(
            cli_module, "load_manifest", lambda path=None: type("M", (), {"status": status})()
        )
        return {
            check.name: check for check in cli_module._generation_checks()
        }["manifest_status"]

    seeding = manifest_status_for("seeding")
    assert seeding.ok is False
    assert "SEEDING" in seeding.detail
    assert "./antidemo setup" in seeding.detail

    # Every value of the manifest's status Literal except `ready`.
    for status in ("provisioning", "seeding", "waiting_for_zero", "cleanup_failed"):
        wedged = manifest_status_for(status)
        assert wedged.ok is False, status
        assert wedged.advisory is False, status
        assert status.upper() in wedged.detail, status

    assert manifest_status_for("ready").ok is True


def test_status_does_not_call_an_unsealed_generation_broken(generation, monkeypatch) -> None:
    """A generation with no manifest yet is the normal starting state."""
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    assert not generation.exists()

    checks = {check.name: check for check in cli_module._generation_checks()}

    assert checks["manifest_status"].ok is True
    assert checks["manifest_status"].detail == "NO MANIFEST SEALED YET"


def test_status_says_so_when_the_manifest_cannot_be_read(generation, monkeypatch) -> None:
    """A diagnostic that goes quiet about a corrupt manifest sends the operator
    looking in the wrong place, which is the wedge this whole change is about."""
    generation.write_text("{ this is not json")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))

    checks = {check.name: check for check in cli_module._generation_checks()}

    assert checks["manifest_status"].ok is False
    assert "COULD NOT BE READ" in checks["manifest_status"].detail
    assert str(generation) in checks["manifest_status"].detail


def test_status_needs_no_selected_generation_to_run(monkeypatch) -> None:
    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)

    assert cli_module._generation_checks() == []


# --------------------------------------------------------------------------
# Recovering a transitional status
# --------------------------------------------------------------------------


def test_a_transitional_status_with_nobody_working_names_its_own_cure(generation) -> None:
    """`status: seeding` and no holder was previously a dead end."""
    recovery = transitional_status_recovery("seeding", manifest_path=generation)

    assert recovery is not None
    assert "No process is mutating this generation" in recovery
    assert "./antidemo setup" in recovery
    assert "will not clear itself" in recovery


def test_a_transitional_status_with_a_live_holder_says_to_wait(
    generation, holder_factory
) -> None:
    holder = holder_factory("antidemo reset")

    recovery = transitional_status_recovery("seeding", manifest_path=generation)

    assert recovery is not None
    assert "A mutation is in progress" in recovery
    assert f"pid {holder.process.pid}" in recovery
    assert "Wait for that command to finish" in recovery


def test_a_ready_status_needs_no_advice(generation) -> None:
    assert transitional_status_recovery("ready", manifest_path=generation) is None
    assert transitional_status_recovery("cleanup_failed", manifest_path=generation) is None


def test_every_transitional_status_is_recoverable(generation) -> None:
    for status in ("provisioning", "seeding", "waiting_for_zero"):
        recovery = transitional_status_recovery(status, manifest_path=generation)
        assert recovery is not None and "./antidemo setup" in recovery, status


# --------------------------------------------------------------------------
# Nesting, so a holder cannot deadlock against its own child
# --------------------------------------------------------------------------


def test_a_child_presenting_the_token_joins_the_same_operation(generation) -> None:
    """`bootstrap.sh` holds the lock and then runs `./antidemo setup` inside it.

    Without this the safety feature would make the documented happy path
    impossible, which is how a lock earns its reputation.
    """
    holder = Holder(generation, "bootstrap.sh --apply").wait_until_holding()
    try:
        token = read_holder(generation_lock_path(generation)).token
        with hold_generation(
            generation, "antidemo setup", environ={LOCK_TOKEN_ENV: token}
        ) as claim:
            assert claim.inherited is True
            assert claim.holder.operation == "bootstrap.sh --apply"
    finally:
        holder.release()


def test_a_wrong_token_is_refused(generation, holder_factory) -> None:
    holder_factory("bootstrap.sh --apply")

    with pytest.raises(GenerationBusyError):
        with hold_generation(
            generation, "antidemo setup", environ={LOCK_TOKEN_ENV: "not-the-token"}
        ):
            pytest.fail("a stale token must not open the lock")


def test_a_nested_acquire_in_one_process_does_not_deadlock(generation) -> None:
    """`flock` conflicts between two descriptors of one file even inside a single
    process, so reentrancy cannot be left to the kernel."""
    with hold_generation(generation, "antidemo setup", environ={}) as outer:
        with hold_generation(generation, "antidemo reset", environ={}) as inner:
            assert outer.inherited is False
            assert inner.inherited is True


def test_the_token_does_not_survive_the_operation(generation) -> None:
    environ: dict[str, str] = {}
    with hold_generation(generation, "antidemo setup", environ=environ):
        assert environ[LOCK_TOKEN_ENV]
    assert LOCK_TOKEN_ENV not in environ
    # And the record no longer names a holder, so a token left in some other
    # shell cannot be matched against it later.
    assert read_holder(generation_lock_path(generation)) is None


# --------------------------------------------------------------------------
# Mechanics that the exclusion depends on
# --------------------------------------------------------------------------


def test_the_lock_keeps_one_inode_across_acquisitions(generation) -> None:
    """The record is rewritten in place, never replaced.

    `manifest.py` writes through `.tmp` + `os.replace` and is right to. Doing
    that here would hand the next caller a different inode to lock, and two
    processes flocking two different inodes exclude nobody.
    """
    lock_path = generation_lock_path(generation)
    with hold_generation(generation, "antidemo setup", environ={}):
        first = lock_path.stat().st_ino
    with hold_generation(generation, "antidemo reset", environ={}):
        assert lock_path.stat().st_ino == first


def test_the_lock_is_created_private_to_the_operator(generation) -> None:
    with hold_generation(generation, "antidemo setup", environ={}):
        mode = generation_lock_path(generation).stat().st_mode & 0o777
    assert mode == 0o600


def test_the_lock_lives_beside_the_manifest_it_protects(generation) -> None:
    assert generation_lock_path(generation) == generation.parent / GENERATION_LOCK_NAME
    # Two generations are two locks: resealing v7 must not block v6.
    other = generation.parent.parent / ".anti-demo-v6" / "manifest.json"
    assert generation_lock_path(other) != generation_lock_path(generation)


def test_the_record_survives_a_round_trip() -> None:
    holder = LockHolder(
        pid=4711,
        parent_pid=4710,
        operation="antidemo reset",
        argv=("antidemo", "reset"),
        host="stage",
        user="operator",
        claimed_at="2026-08-21T21:04:11Z",
        token="abc",
    )

    assert LockHolder.from_payload(holder.to_json()) == holder


@pytest.mark.parametrize("payload", ["", "not json", "[]", '{"pid": 0}', '{"pid": "x"}'])
def test_an_unusable_record_is_reported_as_unusable(payload: str) -> None:
    assert LockHolder.from_payload(payload) is None


def test_the_age_of_a_claim_is_stated_in_words(generation) -> None:
    with hold_generation(generation, "antidemo setup", environ={}) as claim:
        assert claim.holder.age_phrase().endswith("AGO")
        assert LockHolder.from_payload(
            claim.holder.to_json()
        ).age_phrase() != "AN UNKNOWN TIME AGO"


# --------------------------------------------------------------------------
# The command line an operator actually meets
# --------------------------------------------------------------------------


def test_a_mutating_subcommand_refuses_and_says_so_on_stderr(
    generation, holder_factory, monkeypatch, capsys
) -> None:
    holder = holder_factory("antidemo setup")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr("sys.argv", ["antidemo", "reset"])
    monkeypatch.setattr(
        cli_module,
        "reset",
        lambda _timeout: pytest.fail("reset must not run against a locked generation"),
    )

    assert cli_module.main() == 1

    stderr = capsys.readouterr().err
    assert stderr.startswith("REFUSED")
    assert "antidemo setup" in stderr
    assert f"pid {holder.process.pid}" in stderr


def test_a_mutating_subcommand_proceeds_on_a_free_generation(
    generation, monkeypatch, capsys
) -> None:
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr("sys.argv", ["antidemo", "reset"])
    observed: list[bool] = []

    def fake_reset(_timeout):
        # The lock must be held for the duration of the operation, not merely
        # taken and dropped before it starts.
        observed.append(lock_is_held(generation_lock_path(generation)))
        return type("M", (), {"run_id": "run-1"})()

    monkeypatch.setattr(cli_module, "reset", fake_reset)

    assert cli_module.main() == 0
    assert observed == [True]
    assert "READY run-1" in capsys.readouterr().out
    # And it is released again, so a second command is not wedged by the first.
    assert lock_is_held(generation_lock_path(generation)) is False


#: The denial a live `./antidemo cleanup --dry-run` met on an installation
#: authenticating as the app-runtime IAM user, which is not granted
#: `secretsmanager:ListSecrets` the way operator policy 3 is. The account
#: number is the AWS documentation placeholder rather than the one the real
#: message carried, because `test_no_live_identifiers_committed` is right about
#: that and because the whole point of the assertions below is that this string
#: never reaches a screen.
_DENIED_LIST_SECRETS = ClientError(
    {
        "Error": {
            "Code": "AccessDeniedException",
            "Message": (
                "User: arn:aws:iam::123456789012:user/anti-demo-app-runtime is not "
                "authorized to perform: secretsmanager:ListSecrets"
            ),
        }
    },
    "ListSecrets",
)

#: The same defect one vendor over, in the shape `_round5_active_journal_addons`
#: can actually produce it. That call site names `InvalidSchemaName` and
#: `UndefinedTable` -- the pair meaning "no journal here" -- and lets every other
#: `psycopg.Error` through, and `InsufficientPrivilege` is the one an installation
#: authenticating as the app-runtime identity meets when `docs/DEPLOY.md`'s
#: coordination grant set was never issued for the journal table.
#:
#: The message is the shape a real one carries and every part of it is a
#: placeholder: the host cannot resolve, the address is RFC5737 documentation
#: space, and the role is the same synthetic name the AWS case above uses. All
#: three are here to be asserted *absent* from stderr.
_DENIED_JOURNAL_READ = psycopg.errors.InsufficientPrivilege(
    "permission denied for table round5_creation_journal (connection to "
    "instance-under-test.database.example.invalid (192.0.2.10) port 5432 as "
    "anti-demo-app-runtime)"
)

#: A login failure, not a grant failure, and the distinction is the whole reason
#: this case exists separately. SQLSTATE 28P01 means the credential is wrong or
#: expired; `coordination.privilege_refusal` deliberately excludes class 28 for
#: that reason, so this must land in the fault branch and must not be answered
#: with a grant set the operator already holds.
_EXPIRED_JOURNAL_CREDENTIAL = psycopg.errors.InvalidPassword(
    "password authentication failed for user anti-demo-app-runtime "
    "(instance-under-test.database.example.invalid, 192.0.2.10)"
)


@pytest.mark.parametrize(
    ("argv", "operation", "failure", "expected", "forbidden"),
    [
        pytest.param(
            ["antidemo", "reset"],
            "reset",
            RuntimeError("terraform disagreed"),
            ("ERROR", "terraform disagreed"),
            (),
            id="a-runtime-error",
        ),
        pytest.param(
            ["antidemo", "cleanup", "--dry-run"],
            "cleanup",
            _DENIED_LIST_SECRETS,
            (
                # The failure, said the way `manager.operator_diagnosis` says it.
                "ClientError[AccessDeniedException]@ListSecrets",
                # And the part that stops a false all-clear on money: the lines
                # already on the screen are named as an incomplete inventory.
                "INCOMPLETE",
                "antidemo cleanup --dry-run",
            ),
            # `_message_is_ours_to_quote` withholds a third party's words, and an
            # AccessDenied message quotes the principal ARN verbatim.
            ("anti-demo-app-runtime", "arn:aws:iam::"),
            id="an-aws-access-denial",
        ),
        pytest.param(
            ["antidemo", "cleanup", "--dry-run"],
            "cleanup",
            NoCredentialsError(),
            ("NoCredentialsError", "INCOMPLETE"),
            # A fault is not a denial. Sending an operator whose credentials
            # expired to grant themselves a permission they hold is a dead end.
            ("docs/iam/", "Grant it"),
            id="an-aws-fault-that-is-not-a-denial",
        ),
        pytest.param(
            ["antidemo", "cleanup", "--dry-run"],
            "cleanup",
            _DENIED_JOURNAL_READ,
            (
                # `operator_diagnosis` keeps the SQLSTATE, which is the half an
                # operator can act on, and drops the sentence around it.
                "InsufficientPrivilege[SQLSTATE 42501]",
                "INCOMPLETE",
                "antidemo cleanup --dry-run",
                # The remedy names the right system. A `GRANT` in Lakebase and an
                # IAM policy are not interchangeable, and an operator who reaches
                # for the wrong one finds nothing to fix.
                "LAKEBASE GRANT, NOT AWS IAM",
                "docs/DEPLOY.md",
            ),
            # A psycopg message carries the endpoint host, the address it resolved
            # to, the login role and the relation. None of it is ours to quote,
            # and the host and address between them locate the instance.
            (
                "instance-under-test.database.example.invalid",
                "192.0.2.10",
                "anti-demo-app-runtime",
                "permission denied",
            ),
            id="a-postgres-grant-refusal",
        ),
        pytest.param(
            ["antidemo", "cleanup", "--dry-run"],
            "cleanup",
            _EXPIRED_JOURNAL_CREDENTIAL,
            ("InvalidPassword[SQLSTATE 28P01]", "INCOMPLETE"),
            # The credential is the thing to fix, so the grant set must not be
            # offered -- and the connection identifiers stay out of this branch
            # too, which is the branch a broken installation reaches most often.
            (
                "docs/DEPLOY.md",
                "Grant it",
                "LAKEBASE GRANT",
                "instance-under-test.database.example.invalid",
                "192.0.2.10",
                "password authentication failed",
            ),
            id="a-postgres-fault-that-is-not-a-refusal",
        ),
    ],
)
def test_the_lock_is_released_even_when_the_operation_fails(
    generation, monkeypatch, capsys, argv, operation, failure, expected, forbidden
) -> None:
    """Every way an operation can fail becomes an exit code, never a traceback.

    The AWS case is the one that got out. `botocore`'s `ClientError` is a bare
    `Exception`, so it walked past the `(RuntimeError, OSError, ValueError)`
    handler below and `antidemo cleanup --dry-run` -- the command README points a
    stranger at to find out what they are being charged for -- ended in an
    unhandled traceback. It did so *after* printing its inventory, so the
    operator read plausible cost lines and could reasonably take them for a
    complete report. That is why the refusal has to void what came before it
    rather than merely exist.

    The Postgres cases are the same defect one vendor over, reached on the same
    line of the same command. `psycopg.Error` hangs off `Exception` directly too,
    so `_round5_active_journal_addons` -- which names two psycopg classes and
    lets the rest through -- ended `cleanup --dry-run` in a traceback under the
    same plausible inventory. Both vendors are parametrized here rather than
    split into sibling tests because there is one escape route and one handler
    chain, and a second test would let the two drift apart.
    """

    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr("sys.argv", argv)

    def exploding(*_args, **_kwargs):
        # Printed before the failure on purpose: the ordering is the defect.
        print("CHECK Resource reconciliation: 3 expected, 0 orphaned")
        raise failure

    monkeypatch.setattr(cli_module, operation, exploding)

    assert cli_module.main() == 1
    assert lock_is_held(generation_lock_path(generation)) is False

    captured = capsys.readouterr()
    assert "CHECK Resource reconciliation" in captured.out, (
        "the fake must reach the screen before it fails, or this proves nothing "
        "about a report the operator has already read"
    )
    for fragment in expected:
        assert fragment in captured.err, captured.err
    for fragment in forbidden:
        assert fragment not in captured.err, captured.err


def test_the_shell_facing_cli_locks_an_inherited_descriptor(generation) -> None:
    """The mechanism `bootstrap.sh` depends on, exercised through a real shell.

    macOS has no `flock(1)`, so the shell opens the file and this command locks
    the descriptor it inherits. The lock has to outlive the helper -- that is the
    entire point -- so the test checks it from a *third* process afterwards.
    """
    script = textwrap.dedent(
        f"""
        set -euo pipefail
        cd {REPO_ROOT}
        exec 9<>"{generation.parent / GENERATION_LOCK_NAME}"
        LOCK_ENV="$({sys.executable} -m server.generation_lock acquire --fd 9 \\
          --pid $$ --manifest "{generation}" --operation 'bootstrap.sh --apply')"
        eval "$LOCK_ENV"
        # The helper has exited by now. If the lock died with it, this reports free.
        {sys.executable} -m server.generation_lock status --manifest "{generation}" \\
          >/dev/null 2>&1 && echo LOCK_LOST || echo LOCK_HELD
        printf 'TOKEN=%s\\n' "${{{LOCK_TOKEN_ENV}:-unset}}"
        """
    )
    completed = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=60
    )

    assert completed.returncode == 0, completed.stderr
    assert "LOCK_HELD" in completed.stdout, completed.stdout
    assert "TOKEN=unset" not in completed.stdout
    # The shell has exited, so the kernel has already dropped the lock.
    assert lock_is_held(generation_lock_path(generation)) is False


# --------------------------------------------------------------------------
# The server, which reads and must never hold
# --------------------------------------------------------------------------


def _v7_manifest_text(status: str) -> str:
    """The two fields `_load_ready_manifest` reaches before it refuses."""
    return json.dumps({"status": status})


def test_the_startup_refusal_now_names_the_cure(generation, monkeypatch) -> None:
    """`app.py:108` used to say only the status, which was a dead end."""
    import app as app_module

    generation.write_text(_v7_manifest_text("seeding"))
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr(
        app_module,
        "load_manifest",
        lambda: type("M", (), {"status": "seeding", "expiry_warning": lambda self: None})(),
    )

    with pytest.raises(Exception) as failure:
        app_module._load_ready_manifest()

    message = str(failure.value)
    # The opening sentence is load-bearing: bootstrap.sh's deploy gate and the
    # control API both match on it.
    assert message.startswith("Demo setup is currently SEEDING, not READY")
    assert "No process is mutating this generation" in message
    assert "./antidemo setup" in message


def test_the_startup_refusal_says_to_wait_when_a_mutation_is_running(
    generation, monkeypatch, holder_factory
) -> None:
    import app as app_module

    holder = holder_factory("antidemo reset")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    monkeypatch.setattr(
        app_module,
        "load_manifest",
        lambda: type("M", (), {"status": "seeding", "expiry_warning": lambda self: None})(),
    )

    with pytest.raises(Exception) as failure:
        app_module._load_ready_manifest()

    message = str(failure.value)
    assert "A mutation is in progress" in message
    assert f"pid {holder.process.pid}" in message


def test_the_server_reads_a_ready_manifest_while_a_mutation_holds_the_lock(
    generation, monkeypatch, holder_factory
) -> None:
    """The requirement that decides whether this fix is usable.

    If the serving process were a lock holder, or were blocked by one, then
    resealing an installation would mean taking the demo down first -- and an
    operator who could not reseal without downtime would go around the lock.
    """
    import app as app_module

    holder_factory("antidemo reset")
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(generation))
    ready = type("M", (), {"status": "ready", "expiry_warning": lambda self: None})()
    monkeypatch.setattr(app_module, "load_manifest", lambda: ready)

    assert app_module._load_ready_manifest() is ready
    # And it took nothing: the mutation still owns the generation afterwards.
    assert lock_is_held(generation_lock_path(generation)) is True


def test_a_recovery_lookup_that_fails_is_never_the_failure(monkeypatch) -> None:
    """A diagnostic must not be able to replace the diagnosis."""
    import app as app_module

    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)

    assert app_module._transitional_recovery("seeding") is None


def test_the_shell_facing_cli_refuses_a_generation_somebody_else_holds(
    generation, holder_factory
) -> None:
    holder = holder_factory("antidemo reset")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "server.generation_lock",
            "status",
            "--manifest",
            str(generation),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=60,
    )

    assert completed.returncode == 1
    assert "ANOTHER PROCESS IS MUTATING THIS GENERATION" in completed.stderr
    assert f"pid {holder.process.pid}" in completed.stderr


@pytest.mark.skipif(
    not Path("/usr/bin/python3").exists(), reason="no system python3 to check against"
)
def test_the_lock_helper_runs_on_the_system_python_bootstrap_will_find() -> None:
    """bootstrap.sh gets whatever `python3` is on PATH, not this venv.

    On a stock Mac that is /usr/bin/python3 at 3.9, and the first version of this
    module imported `datetime.UTC` (3.11+). The result was bootstrap dying at the
    lock step on a fresh laptop -- a lock that stops the tool from running at all,
    which is strictly worse than no lock. So the helper stays inside what the
    oldest interpreter bootstrap can reach is able to run.
    """
    completed = subprocess.run(
        [
            "/usr/bin/python3",
            "-m",
            "server.generation_lock",
            "status",
            "--manifest",
            "/nonexistent-generation/manifest.json",
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=60,
    )

    assert completed.returncode == 0, f"{completed.stdout}\n{completed.stderr}"
    assert completed.stdout.startswith("free ")


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_bootstrap_refuses_a_generation_another_process_is_mutating() -> None:
    """The specific collision that took the demo down.

    bootstrap.sh cannot be driven from Python -- it demands a service principal
    secret and real AWS credentials long before it reaches the generation
    directory -- so its lock block is extracted verbatim by a shell harness and
    run against a generation a real live process is holding. The harness also
    pins the ordering, because a lock taken after the first write would not have
    prevented today's outage.
    """
    harness = REPO_ROOT / "tests" / "bootstrap_generation_lock_harness.sh"
    completed = subprocess.run(
        ["bash", str(harness)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=300,
    )

    assert completed.returncode == 0, f"{completed.stdout}\n{completed.stderr}"
    assert "FAIL" not in completed.stdout, completed.stdout
