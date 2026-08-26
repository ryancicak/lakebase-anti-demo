"""The money-losing log lines have to reach the place an operator reads.

Every existing assertion about `ORPHAN RISK` is a `caplog` assertion, and
`caplog` installs a handler of its own. So the whole suite would keep passing
if the serving process emitted those records precisely nowhere -- which is
close to what a seven-bout campaign concluded had happened, after grepping the
server log for `WARNING` and `ERROR` and finding neither.

What these tests pin instead is the destination. `ORPHAN RISK` is the only
signal that a towel failed to delete a billable AWS resource, so the question
worth asking is not whether a `LogRecord` was created but whether the sentence
arrives on the file descriptor the launcher points at the server log --
labelled well enough that grepping for the level finds it.

The end-to-end case runs in a subprocess on purpose, for two reasons. It is the
only way to exercise the real thing: `uvicorn` calls `logging.config.dictConfig`
before it imports the ASGI app, and `dictConfig` closes every handler it finds,
so applying it inside a pytest worker would tear down pytest's own capture for
the rest of the session. And a subprocess has a real file descriptor 2, which
is exactly what `server_launch._run_daemon` redirects into the log an operator
reads.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

import pytest

from server.server_launch import configure_operator_logging

REPO_ROOT = Path(__file__).resolve().parent.parent

#: Drive the genuine cooldown-delete path with the fake AWS engine that
#: `tests/test_safe_change_live.py` already uses, under the logging
#: configuration a served process actually has. A clone that never leaves
#: `creating` exhausts its budget and logs the `ORPHAN RISK` line before it
#: raises, so nothing here has to fabricate the message.
_DRIVER = """
import asyncio
import logging.config

import uvicorn.config

# Exactly what uvicorn does, and it does it before importing `app:app`.
logging.config.dictConfig(uvicorn.config.LOGGING_CONFIG)

from server.server_launch import configure_operator_logging

configure_operator_logging()

from server.safe_change import SafeChangeProvider
from server.safe_change_live import SafeChangeControlPlaneError
from tests.test_safe_change_live import (
    FakeAwsSession,
    RDS_SOURCE,
    VirtualClock,
    mid_restore_rds_adapter,
    plan,
    quiet_report,
    refuses_delete_while_creating,
)


async def main() -> None:
    session = FakeAwsSession()
    clock = VirtualClock()
    adapter = mid_restore_rds_adapter(session, clock)
    change_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    artifact = await adapter.create_isolated(change_plan, quiet_report)
    refuses_delete_while_creating(
        session,
        clock,
        instances=(change_plan.artifact_id,),
        creating_seconds=float("inf"),
    )
    print("ARTIFACT " + change_plan.artifact_id, flush=True)
    try:
        await adapter.delete_isolated(change_plan, artifact, quiet_report)
    except SafeChangeControlPlaneError:
        print("RAISED as expected", flush=True)
        return
    raise AssertionError("the clone was deleted, so no orphan was ever reported")


asyncio.run(main())
"""


#: Drive two real Round 2 bouts through the real `RunManager`, under the logging
#: configuration a served process actually has, and let both mid-bout failure
#: shapes reach fd 2 on their own. Nothing here fabricates a log line: the first
#: bout fails a lane the way the engine really reports one, and the second raises
#: the refusal a live campaign actually saw.
_BOUT_DRIVER = """
import asyncio
import logging.config
import os
import tempfile

# A receipt with nowhere to go logs its own traceback, which would let the
# traceback assertion below pass on noise that has nothing to do with a refusal.
os.environ["ANTI_DEMO_ARTIFACT_ROOT"] = tempfile.mkdtemp(prefix="anti-demo-log-test-")

import uvicorn.config

# Exactly what uvicorn does, and it does it before importing `app:app`.
logging.config.dictConfig(uvicorn.config.LOGGING_CONFIG)

from server.server_launch import configure_operator_logging

configure_operator_logging()

from server.manager import RunManager
from server.models import CompetitorId, Corner, RoundId, SessionCreate, SessionState
from server.safe_change_live import ControlPlaneCommandError
from tests.test_manager import FakeSafeChangeEngine, wait_for_state


class RefusedByTheControlPlane(FakeSafeChangeEngine):
    \"\"\"The refusal a live campaign saw, raised where the bout would see it.\"\"\"

    async def run(self, arm, on_progress):
        raise ControlPlaneCommandError("Databricks control-plane request was refused")


def request() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
    )


async def bout(engine) -> str:
    manager = RunManager(safe_change_factory=lambda: engine)
    manager._cleanup_retry_initial = 0.001
    manager._cleanup_retry_max = 0.002
    created = await manager.create(request())
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    failed = await wait_for_state(manager, created.id, SessionState.FAILED)
    assert failed.state == SessionState.FAILED, failed.state
    return created.id


async def main() -> None:
    lane_engine = FakeSafeChangeEngine()
    lane_engine.fail_run = True
    print("LANE_SESSION " + await bout(lane_engine), flush=True)
    print("RUN_SESSION " + await bout(RefusedByTheControlPlane()), flush=True)


asyncio.run(main())
"""


#: Emit one record either side of the threshold from a real `server.*` logger,
#: under the logging configuration a served process actually has. Unlike the two
#: drivers above this synthesises the records rather than driving a bout: what is
#: under test is the delivery channel and the level it cuts at, and routing that
#: through a business path would only add a way for the test to fail for an
#: unrelated reason.
_LEVEL_DRIVER = """
import logging
import logging.config

import uvicorn.config

# Exactly what uvicorn does, and it does it before importing `app:app`.
logging.config.dictConfig(uvicorn.config.LOGGING_CONFIG)

from server.server_launch import configure_operator_logging

configure_operator_logging()

logger = logging.getLogger("server.recovery_live")
# Neither sentinel may contain a level name. `assert "WARNING" in line` passes
# on the message text alone if it does, which would let the unlabelled
# `logging.lastResort` line -- the original defect -- satisfy the label check.
logger.info("SENTINEL_QUIET a step that went fine")
logger.warning("SENTINEL_LOUD a transient nobody saw")
"""


def _session(stdout: str, prefix: str) -> str:
    return next(
        line.removeprefix(prefix + " ")
        for line in stdout.splitlines()
        if line.startswith(prefix + " ")
    )


def test_a_mid_bout_refusal_reaches_the_log_an_operator_reads() -> None:
    """The lane reason and the dropped exception both land on a real fd 2.

    `caplog` cannot make this claim. It installs a handler of its own, so a
    `caplog` assertion passes when the serving process emits the record
    precisely nowhere -- which is what a campaign concluded had happened after
    grepping the server log for `ERROR` and finding nothing over a file that
    contained the lines. So this runs the genuine manager path in a subprocess
    and asserts against the descriptor `server_launch._run_daemon` redirects
    into `server-<port>.log`.

    Both mid-bout shapes are checked because they are different defects. A
    refused *lane* never raises out of `engine.run` at all -- Rounds 1, 2 and 3
    report it as a lane result -- so it reached only the SSE stream and the bout
    record. A refusal that does escape was caught by a bare `except Exception`
    that dropped it and substituted a fixed sentence.
    """

    completed = subprocess.run(
        [sys.executable, "-c", _BOUT_DRIVER],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, (
        f"the driver never reached a failed bout\n"
        f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}"
    )
    stderr = completed.stderr

    lane_lines = [line for line in stderr.splitlines() if "lane refused" in line]
    assert lane_lines, (
        "a mid-bout lane refusal reached no log at all. It was a lane result on "
        f"the SSE stream and a field in the bout record and nothing else.\n{stderr}"
    )
    lane = lane_lines[0]
    # Findable by level. This is the specific thing that made the campaign
    # conclude the signal did not exist.
    assert "ERROR" in lane, f"an operator grepping for ERROR would miss this: {lane!r}"
    # Findable by origin, and by which lane and which round refused.
    assert "server.manager" in lane
    assert "lane=lakebase" in lane
    assert "round=make_schema_change_safely" in lane
    # And by the reason, which is the entire point of the exercise.
    assert "reason=isolated endpoint contract rejected" in lane
    assert _session(completed.stdout, "LANE_SESSION") in lane
    # An ISO-8601 UTC instant, so a log line and a receipt can be lined up.
    assert lane.startswith("20") and lane[4] == "-" and "Z " in lane

    bout_lines = [line for line in stderr.splitlines() if "bout failed" in line]
    assert bout_lines, f"the dropped exception reached no log either\n{stderr}"
    bout = bout_lines[0]
    assert "ERROR" in bout
    assert "server.manager" in bout
    # The refusal's own words survive, because `ControlPlaneCommandError` is this
    # codebase's own exception and `ControlPlaneCommandError` alone does not say
    # whether the request was refused, timed out, or returned nonsense.
    assert "diagnosis=ControlPlaneCommandError: Databricks control-plane request was refused" in (
        bout
    )
    assert _session(completed.stdout, "RUN_SESSION") in bout
    # A traceback is acceptable in the log and is not acceptable on a screen --
    # and it has to be attached to *this* record rather than merely present
    # somewhere in the stream, or an unrelated library traceback satisfies the
    # assertion and the guard proves nothing.
    lines = stderr.splitlines()
    after = lines[lines.index(bout) + 1 :]
    assert after and after[0] == "Traceback (most recent call last):", (
        f"the diagnosis carries no traceback of its own\n{after[:5]}"
    )
    frames = after[: after.index("") if "" in after else 12]
    assert any("ControlPlaneCommandError" in frame for frame in frames), frames
    assert any("_run_safe_change" in frame for frame in frames), frames

    # The screen sentence is unchanged, and it is the one without the diagnosis
    # welded on. A refusal that reads differently on the panel than it did
    # yesterday would be a behaviour change hiding inside a logging fix.
    assert "The live isolated-change proof failed unexpectedly" not in bout


@pytest.fixture
def restored_logging():
    """Give the process its logging back, whatever the test did to it."""

    root = logging.getLogger()
    handlers = list(root.handlers)
    level = root.level
    try:
        yield root
    finally:
        for handler in list(root.handlers):
            if handler not in handlers:
                root.removeHandler(handler)
        for handler in handlers:
            if handler not in root.handlers:
                root.addHandler(handler)
        root.setLevel(level)


def test_the_orphan_risk_line_reaches_the_log_an_operator_reads() -> None:
    """The one that matters: a real bout-path warning on a real descriptor.

    `server_launch._run_daemon` points both stdout and stderr at
    `server-<port>.log`, so "arrives on fd 2" and "is in the server log" are
    the same claim for a `antidemo serve --background` launch, which is how the
    campaign ran.
    """

    completed = subprocess.run(
        [sys.executable, "-c", _DRIVER],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, (
        f"the driver did not reach the orphan path\n"
        f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}"
    )
    artifact_id = next(
        line.removeprefix("ARTIFACT ")
        for line in completed.stdout.splitlines()
        if line.startswith("ARTIFACT ")
    )

    orphan_lines = [line for line in completed.stderr.splitlines() if "ORPHAN RISK" in line]
    assert orphan_lines, (
        "the only signal that a billable AWS resource was left behind never "
        f"reached the server log.\n--- stderr ---\n{completed.stderr}"
    )
    line = orphan_lines[0]
    # Findable by the resource, because sweeping it by hand needs the name.
    assert artifact_id in line
    # Findable by level. This is the specific thing that made the campaign
    # conclude the signal did not exist: `logging.lastResort` formats with a
    # bare `%(message)s`, so `grep ERROR` over a log full of these matched
    # nothing at all.
    assert "ERROR" in line, f"an operator grepping for ERROR would miss this: {line!r}"
    # Findable by origin, so the reader knows which lane left the resource.
    assert "server.safe_change_live" in line


def test_where_the_threshold_cuts_holds_on_a_real_descriptor() -> None:
    """`WARNING` arrives on fd 2 and `INFO` does not. Both halves are load-bearing.

    This is the pair of facts every "no failures in the logs" claim rests on,
    so neither half may be asserted through a capture fixture. `capsys` proves a
    record was formatted; it cannot distinguish that from a handler bound to a
    stream nothing reads, which is the exact confusion a seven-bout campaign
    fell into. So this runs under the real `uvicorn` `dictConfig` -- which clears
    root's handlers, and would close and discard ours if it ever ran second --
    and reads the descriptor `_run_daemon` points at `server-<port>.log` and
    Databricks Apps captures into `databricks apps logs`.

    The `WARNING` half is what makes error-absence mean anything: if it can be
    silenced without a test going red, "nothing in the logs" stops being
    evidence and `ORPHAN RISK` becomes invisible on the deployed app.

    The `INFO` half pins the threshold from the other side. Dropping it would
    look like a generosity and behave like a denial of service: the credential
    probe and the cleanup retries log on a five-second loop, and burying one
    `ORPHAN RISK` line under thousands is the same defect facing the other way.
    An `INFO` line that never reaches the deployed log is this design working,
    not a fault to be reported as one.
    """

    completed = subprocess.run(
        [sys.executable, "-c", _LEVEL_DRIVER],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, (
        f"the driver never got as far as logging anything\n"
        f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}"
    )

    warnings = [line for line in completed.stderr.splitlines() if "SENTINEL_LOUD" in line]
    assert warnings, (
        "a module WARNING reached no log at all, so absence of error lines on "
        f"the deployed app proves nothing.\n--- stderr ---\n{completed.stderr}"
    )
    line = warnings[0]
    # Findable by level. This is the specific thing that made the campaign
    # conclude the signal did not exist.
    assert "WARNING" in line, f"an operator grepping for WARNING would miss this: {line!r}"
    # Findable by origin, so the reader knows which lane wrote it.
    assert "server.recovery_live" in line
    # An ISO-8601 UTC instant, matching every other timestamp this project
    # writes down, so a log line and a receipt can be lined up. Whether a
    # resource "is still billing" four seconds or four hours ago is the whole
    # question, and `logging.lastResort` emitted neither the level nor the time.
    assert line.startswith("20") and line[4] == "-" and "Z " in line

    # Only meaningful because the WARNING above arrived: that is what rules out
    # a driver that died before it logged, which would satisfy this vacuously.
    assert "SENTINEL_QUIET" not in completed.stderr, (
        "INFO now reaches the operator log. The five-second retry loops in the "
        "credential probe and the cleanup path will bury ORPHAN RISK.\n"
        f"--- stderr ---\n{completed.stderr}"
    )
    assert "SENTINEL_QUIET" not in completed.stdout


def test_configuring_twice_does_not_double_every_line(restored_logging, capsys) -> None:
    """`app.py` configures at import; the CLI configures at entry. Both can run."""

    root = restored_logging
    for handler in list(root.handlers):
        root.removeHandler(handler)

    configure_operator_logging()
    configure_operator_logging()
    logging.getLogger("server.safe_change_live").error("ORPHAN RISK: only once")

    captured = capsys.readouterr()
    assert captured.err.count("ORPHAN RISK: only once") == 1


def test_debug_from_a_chatty_library_stays_out_of_the_log(
    restored_logging,
    capsys,
) -> None:
    """Surfacing everything would bury the line this exists to reveal.

    The threshold is deliberately the same one `logging.lastResort` applied, so
    nothing that is quiet today becomes loud. A botocore wire-level `DEBUG`
    stream would push an `ORPHAN RISK` line off the end of a rolled log.
    """

    root = restored_logging
    for handler in list(root.handlers):
        root.removeHandler(handler)

    configure_operator_logging()
    chatty = logging.getLogger("botocore.endpoint")
    chatty.setLevel(logging.DEBUG)
    chatty.debug("wire trace nobody asked for")
    chatty.info("still not worth a line")
    chatty.warning("this one is worth a line")

    captured = capsys.readouterr()
    assert "wire trace nobody asked for" not in captured.err
    assert "still not worth a line" not in captured.err
    assert "this one is worth a line" in captured.err


def test_an_unwritable_stream_never_takes_the_bout_down(restored_logging) -> None:
    """A diagnostic must not break the thing it is diagnosing.

    The daemon's stderr is a file on a disk that can fill. If writing the line
    that says a resource is still billing could raise, a full disk would turn a
    cost problem into a failed bout.
    """

    root = restored_logging
    for handler in list(root.handlers):
        root.removeHandler(handler)

    configure_operator_logging()

    class Unwritable:
        def write(self, _text: str) -> int:
            raise OSError("no space left on device")

        def flush(self) -> None:
            raise OSError("no space left on device")

    original = sys.stderr
    sys.stderr = Unwritable()  # type: ignore[assignment]
    try:
        # Must return normally. `logging.raiseExceptions` is left alone, so the
        # handler's own error reporting is unchanged; what matters is that the
        # caller is not the one that pays.
        logging.getLogger("server.safe_change_live").error("ORPHAN RISK: disk full")
    finally:
        sys.stderr = original
