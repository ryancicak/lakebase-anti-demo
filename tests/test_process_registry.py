from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from server.cli import _status_checks
from server.process_registry import (
    MANIFEST_ENV,
    SERVER_LOG_PATH_ENV,
    SERVER_PORT_ENV,
    SUPERVISOR_PID_ENV,
    ServerProcessRecord,
    build_record,
    identity_tokens,
    inspect_record,
    is_server_invocation,
    read_record,
    record_paths,
    register_serving_process,
    serving_endpoint,
    state_dir_from_environ,
    unregister_serving_process,
    verified_log_path,
)

ADHOC_ARGV = (
    "/Users/demo/lakebase-anti-demo/.venv/bin/uvicorn",
    "app:app",
    "--host",
    "127.0.0.1",
    "--port",
    "8001",
)
LIVE_COMMAND_LINE = (
    "/opt/homebrew/Cellar/python@3.14/3.14.5/bin/Python "
    "/Users/demo/lakebase-anti-demo/.venv/bin/uvicorn app:app "
    "--host 127.0.0.1 --port 8001"
)


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / ".anti-demo-v7"
    directory.mkdir()
    monkeypatch.setenv(MANIFEST_ENV, str(directory / "manifest.json"))
    monkeypatch.delenv(SERVER_LOG_PATH_ENV, raising=False)
    return directory


def _write_legacy_record(state_dir: Path, *, pid: int, log_path: str) -> None:
    """Reproduce the wrapper-written record that started the confusion."""
    pid_path, launch_path = record_paths(state_dir, 8001)
    pid_path.write_text(f"{pid}\n")
    launch_path.write_text(
        json.dumps(
            {
                "launcher_pid": pid,
                "server_pid": pid + 381,
                "manifest_path": str(state_dir / "manifest.json"),
                "manifest_version": 7,
                "installation_id": "11111111-2222-3333-4444-555555555555",
                "run_id": "ad-20260820-1446-abcd",
                "host": "127.0.0.1",
                "port": 8001,
                "log_path": log_path,
            }
        )
    )


def test_the_port_is_recovered_from_an_ad_hoc_uvicorn_invocation() -> None:
    """Uvicorn never tells the app its port, so self-registration reads its own argv."""
    assert serving_endpoint(ADHOC_ARGV, {}) == ("127.0.0.1", 8001)
    assert serving_endpoint(("uvicorn", "app:app", "--port=9100"), {}) == (
        "127.0.0.1",
        9100,
    )
    assert serving_endpoint(("uvicorn", "app:app"), {"ANTI_DEMO_SERVER_PORT": "8123"}) == (
        "127.0.0.1",
        8123,
    )
    assert serving_endpoint(("uvicorn", "app:app"), {}) == ("127.0.0.1", 8000)
    assert serving_endpoint(("uvicorn", "--port", "nonsense"), {}) == ("127.0.0.1", 8000)


def test_identity_tokens_come_from_the_invocation_not_from_a_hardcoded_name() -> None:
    assert identity_tokens(ADHOC_ARGV) == (
        "uvicorn",
        "app:app",
        "--host",
        "127.0.0.1",
        "--port",
        "8001",
    )
    assert all(token in LIVE_COMMAND_LINE for token in identity_tokens(ADHOC_ARGV))


def test_the_two_legacy_files_naming_different_pids_is_reported_not_resolved(
    state_dir: Path,
) -> None:
    """The audited layout: server-8001.pid said 5243, launch.json said 5624."""
    _write_legacy_record(state_dir, pid=5243, log_path=str(state_dir / "server-8001.log"))

    status = inspect_record(
        state_dir,
        8001,
        pid_is_alive=lambda _pid: False,
        command_line=lambda _pid: None,
    )

    assert status.state == "inconsistent"
    assert status.safe_to_signal is False
    assert status.detail == (
        "PIDFILE SAYS 5243 · LAUNCH RECORD SAYS 5624 · DO NOT SIGNAL"
    )


def test_an_exited_pid_is_reported_stale_instead_of_being_trusted(state_dir: Path) -> None:
    """The audited failure: both recorded pids had exited hours earlier."""
    write_record_for(state_dir, pid=5624)

    status = inspect_record(
        state_dir,
        8001,
        pid_is_alive=lambda _pid: False,
        command_line=lambda _pid: None,
    )

    assert status.state == "exited"
    assert status.safe_to_signal is False
    assert status.detail == "STALE RECORD · PID 5624 HAS EXITED · DO NOT SIGNAL"


MODULE_ARGV = (
    "/Users/demo/lakebase-anti-demo/.venv/lib/python3.12/site-packages/uvicorn/__main__.py",
    "app:app",
    "--host",
    "127.0.0.1",
    "--port",
    "8080",
)
#: What `ps -o args=` reports for that launch. Python rewrote `sys.argv[0]` to the
#: resolved file; the kernel kept the vector the process was execed with, and
#: `__main__.py` is nowhere in it.
MODULE_COMMAND_LINE = (
    "/Users/demo/lakebase-anti-demo/.venv/bin/python -m uvicorn app:app "
    "--host 127.0.0.1 --port 8080"
)


def test_a_module_launched_server_can_verify_as_itself(state_dir: Path) -> None:
    """`python -m uvicorn` recorded a token that could never appear in `ps`.

    `sys.argv[0]` is `…/uvicorn/__main__.py`, so the recorded identity token was
    `__main__.py`, while the live command line says `-m uvicorn`. The token was
    absent by construction, so every reader classified a healthy server as
    `foreign` and printed DO NOT SIGNAL -- which is what made `antidemo status` fail
    all day against a server it had just been talking to.
    """
    from server.process_registry import write_record

    record = write_record(
        state_dir,
        build_record(
            argv=MODULE_ARGV,
            environ={MANIFEST_ENV: str(state_dir / "manifest.json")},
            pid=10586,
            stdout=("pipe", None),
        ),
    )

    status = inspect_record(
        state_dir,
        8080,
        pid_is_alive=lambda _pid: True,
        command_line=lambda _pid: MODULE_COMMAND_LINE,
    )

    assert status.state == "live"
    assert status.safe_to_signal is True
    assert "-m uvicorn" in record.identity_tokens
    assert "__main__.py" not in record.identity_tokens

    # The guard is the point of the function, so the same record must still
    # refuse a pid that has been recycled by something else. A false positive
    # here would signal a stranger's process; the bug above only lied about a
    # healthy one.
    for stranger in (
        "/usr/bin/vim TASKS.md",
        # A different app served by the same module launch on the same host.
        "/usr/bin/python -m uvicorn other:app --host 127.0.0.1 --port 8080",
        # This app, but a module launch of something that is not uvicorn.
        "/usr/bin/python -m hypercorn app:app --host 127.0.0.1 --port 8080",
    ):
        refused = inspect_record(
            state_dir,
            8080,
            pid_is_alive=lambda _pid: True,
            command_line=lambda _pid, line=stranger: line,
        )
        assert refused.state == "foreign", stranger
        assert refused.safe_to_signal is False


def test_a_recycled_pid_running_something_else_is_never_signalled(state_dir: Path) -> None:
    write_record_for(state_dir, pid=5243)

    status = inspect_record(
        state_dir,
        8001,
        pid_is_alive=lambda _pid: True,
        command_line=lambda _pid: "/usr/bin/vim TASKS.md",
    )

    assert status.state == "foreign"
    assert status.safe_to_signal is False
    assert "IS NOT THIS APP" in status.detail


def test_an_unidentifiable_live_pid_fails_closed(state_dir: Path) -> None:
    write_record_for(state_dir, pid=5243)

    status = inspect_record(
        state_dir,
        8001,
        pid_is_alive=lambda _pid: True,
        command_line=lambda _pid: None,
    )

    assert status.state == "unverified"
    assert status.safe_to_signal is False


def test_a_matching_live_pid_is_the_only_state_safe_to_signal(state_dir: Path) -> None:
    write_record_for(state_dir, pid=43345)

    status = inspect_record(
        state_dir,
        8001,
        pid_is_alive=lambda _pid: True,
        command_line=lambda _pid: LIVE_COMMAND_LINE,
    )

    assert status.state == "live"
    assert status.safe_to_signal is True
    assert status.record is not None
    assert status.record.pid == 43345


def test_an_absent_or_unreadable_record_is_distinguished(state_dir: Path) -> None:
    assert inspect_record(state_dir, 8001).state == "absent"

    pid_path, launch_path = record_paths(state_dir, 8001)
    pid_path.write_text("5243\n")
    launch_path.write_text("{not json")
    assert inspect_record(state_dir, 8001).state == "unreadable"

    launch_path.write_text(json.dumps({"host": "127.0.0.1", "port": 8001}))
    assert inspect_record(state_dir, 8001).state == "unreadable"


def test_self_registration_replaces_a_stale_record_and_keeps_its_provenance(
    state_dir: Path,
) -> None:
    """An ad-hoc launch corrects the pid rather than leaving the old one in place."""
    _write_legacy_record(state_dir, pid=5243, log_path=str(state_dir / "server-8001.log"))

    record = register_serving_process(state_dir=state_dir, argv=ADHOC_ARGV)

    assert record is not None
    assert record.pid == os.getpid()
    assert record.port == 8001
    assert record.launch_mode == "adhoc"
    # Fields only the wrapper knew survive; the pid it got wrong does not.
    assert record.carried["run_id"] == "ad-20260820-1446-abcd"
    assert record.carried["installation_id"] == "11111111-2222-3333-4444-555555555555"

    pid_path, launch_path = record_paths(state_dir, 8001)
    assert pid_path.read_text() == f"{os.getpid()}\n"
    payload = json.loads(launch_path.read_text())
    # The legacy keys must not survive as second, contradictory answers.
    assert payload["pid"] == payload["server_pid"] == os.getpid()
    assert payload["parent_pid"] == os.getppid()
    # `launcher_pid` is not one of them any more. It held the immediate parent
    # while its name claimed the launcher, and under `antidemo serve` those are two
    # different processes -- the parent is the `uv` shim and the launcher is the
    # supervisor one above it.
    assert "launcher_pid" not in payload
    assert 5243 not in payload.values()
    assert read_record(state_dir, 8001) == record


def test_an_adhoc_launch_declines_to_claim_a_log_it_is_not_writing(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The audited failure: the record claimed a log file that had gone silent."""
    managed_log = state_dir / "server-8001.log"
    managed_log.write_text("earlier generation\n")
    elsewhere = state_dir / "r3-server2.log"
    elsewhere.write_text("where the output really goes\n")
    monkeypatch.setenv(SERVER_LOG_PATH_ENV, str(managed_log))

    stdout_is_the_other_file = (
        "file",
        (os.stat(elsewhere).st_dev, os.stat(elsewhere).st_ino),
    )
    misdirected = build_record(
        argv=ADHOC_ARGV,
        environ={SERVER_LOG_PATH_ENV: str(managed_log)},
        stdout=stdout_is_the_other_file,
    )
    assert misdirected.log_path is None

    stdout_is_the_managed_file = (
        "file",
        (os.stat(managed_log).st_dev, os.stat(managed_log).st_ino),
    )
    honest = build_record(
        argv=ADHOC_ARGV,
        environ={SERVER_LOG_PATH_ENV: str(managed_log)},
        stdout=stdout_is_the_managed_file,
    )
    assert honest.log_path == str(managed_log)


def test_the_supervisor_is_recorded_as_itself_and_never_as_the_uv_shim() -> None:
    """The measured defect: `launcher_pid` named the `uv` shim, one too low.

    `serve_command` runs ``uv run --no-sync uvicorn`` and uv spawns the server
    rather than exec'ing it, so a supervised tree is three deep and the server's
    own parent is the shim. The live record on 8080 said ``parent_pid: 69465``,
    which was ``uv run``; the supervisor was 69464. Signalling worked anyway,
    because uv relays, which is why nothing caught it.

    So the supervisor tells the server its pid rather than the server guessing,
    and the three answers stay separable.
    """
    supervised = build_record(
        argv=ADHOC_ARGV,
        environ={SERVER_PORT_ENV: "8001", SUPERVISOR_PID_ENV: "69464"},
        parent_pid=69465,
        stdout=("pipe", None),
    )

    assert supervised.supervisor_pid == 69464
    assert supervised.parent_pid == 69465
    assert supervised.supervision == "supervised"

    unsupervised = build_record(argv=ADHOC_ARGV, environ={}, stdout=("pipe", None))
    assert unsupervised.supervisor_pid is None
    assert unsupervised.supervision == "unsupervised"


def test_a_record_written_before_supervisor_tracking_says_it_cannot_tell() -> None:
    """Silence must not be read as "no supervisor". There is such a record live.

    The server on 8080 was launched by a launcher that recorded no supervisor at
    all, so its record cannot answer the question -- and a reader that took the
    missing field for a denial would be committing this project's recurring bug
    on the way to reading about it.
    """
    older = ServerProcessRecord.from_dict(
        {
            "pid": 69466,
            "launcher_pid": 69465,
            "port": 8080,
            "record_schema": 2,
        }
    )

    assert older.supervision == "unknown"
    assert older.supervisor_pid is None
    # `launcher_pid` is still read, but only as what it really held.
    assert older.parent_pid == 69465


def test_launch_mode_names_the_supported_path_only_when_it_was_used() -> None:
    """`antidemo serve` exports the endpoint; a bare uvicorn does not."""
    launcher = build_record(
        argv=ADHOC_ARGV,
        environ={SERVER_PORT_ENV: "8001"},
        stdout=("pipe", None),
    )
    adhoc = build_record(argv=ADHOC_ARGV, environ={}, stdout=("pipe", None))

    assert launcher.launch_mode == "launcher"
    assert adhoc.launch_mode == "adhoc"


def test_a_claimed_log_path_is_only_kept_when_stdout_is_that_exact_file(
    tmp_path: Path,
) -> None:
    log = tmp_path / "server.log"
    log.write_text("")
    identity = (os.stat(log).st_dev, os.stat(log).st_ino)

    assert verified_log_path(str(log), identity) == str(log)
    assert verified_log_path(str(log), (identity[0], identity[1] + 1)) is None
    assert verified_log_path(str(tmp_path / "missing.log"), identity) is None
    assert verified_log_path(None, identity) is None
    # A pipe or a terminal identifies no file, so nothing may be claimed.
    assert verified_log_path(str(log), None) is None


def test_only_the_process_named_by_the_record_may_release_it(state_dir: Path) -> None:
    record = register_serving_process(state_dir=state_dir, argv=ADHOC_ARGV)
    assert record is not None

    impostor = ServerProcessRecord(**{**record.__dict__, "pid": record.pid + 1})
    assert unregister_serving_process(impostor, state_dir=state_dir) is False
    assert read_record(state_dir, 8001) is not None

    assert unregister_serving_process(record, state_dir=state_dir) is True
    assert inspect_record(state_dir, 8001).state == "absent"
    assert unregister_serving_process(record, state_dir=state_dir) is False


def test_only_a_real_asgi_server_invocation_may_claim_the_record(
    state_dir: Path,
) -> None:
    """The test suite imports this module, so a non-server process must be inert."""
    assert is_server_invocation(ADHOC_ARGV) is True
    assert is_server_invocation(("uvicorn", "server.app:build", "--port", "8001")) is True
    assert is_server_invocation((".venv/bin/pytest", "-q", "tests")) is False
    assert is_server_invocation(("python", "-m", "pytest", "--port", "8001")) is False

    assert register_serving_process(state_dir=state_dir, argv=("pytest", "-q")) is None
    assert inspect_record(state_dir, 8000).state == "absent"


def test_registration_is_skipped_when_no_state_directory_is_selected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv(MANIFEST_ENV, raising=False)
    assert state_dir_from_environ() is None
    assert register_serving_process(argv=ADHOC_ARGV) is None

    monkeypatch.setenv(MANIFEST_ENV, str(tmp_path / "gone" / "manifest.json"))
    assert state_dir_from_environ() is None
    assert register_serving_process(argv=ADHOC_ARGV) is None


def test_status_reports_the_exact_disagreement_the_audit_found(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A served port with a dead pid in the record must be a loud, named failure."""
    write_record_for(state_dir, pid=5624)
    monkeypatch.setattr("server.cli._app_is_online", lambda _host, _port: True)

    checks = {
        check.name: check
        for check in _status_checks(
            "127.0.0.1",
            8001,
            pid_is_alive=lambda _pid: False,
            command_line=lambda _pid: None,
        )
    }

    assert checks["server_launch_record"].ok is False
    assert "HAS EXITED" in checks["server_launch_record"].detail
    assert checks["server_port"].ok is True
    assert checks["server_record_agrees"].ok is False
    assert "PORT ANSWERS BUT RECORD IS EXITED" in checks["server_record_agrees"].detail


def test_status_is_quiet_when_the_record_and_the_port_agree(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_serving_process(state_dir=state_dir, argv=ADHOC_ARGV)
    monkeypatch.setattr("server.cli._app_is_online", lambda _host, _port: True)

    checks = _status_checks(
        "127.0.0.1",
        8001,
        pid_is_alive=lambda _pid: True,
        command_line=lambda _pid: LIVE_COMMAND_LINE,
    )

    assert [check.name for check in checks if not check.ok] == []
    assert "IS SERVING" in checks[0].detail


def test_status_reads_back_the_restart_history_the_supervisor_wrote(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement server looks exactly like an original one on this screen.

    Same port, same record, same answers -- and the difference is the whole story
    when a round has started misbehaving. The supervisor writes the deaths down
    for that reason and nothing was reading them back.
    """
    from server.server_launch import RestartJournal

    register_serving_process(state_dir=state_dir, argv=ADHOC_ARGV)
    monkeypatch.setattr("server.cli._app_is_online", lambda _host, _port: True)
    monkeypatch.setenv(SERVER_PORT_ENV, "8001")
    journal = RestartJournal(state_dir / "server-8001.restarts.json")
    journal.record("SIGKILL (9)", now=time.time())

    checks = {
        check.name: check
        for check in _status_checks(
            "127.0.0.1",
            8001,
            pid_is_alive=lambda _pid: True,
            command_line=lambda _pid: LIVE_COMMAND_LINE,
        )
    }
    history = checks["server_restart_history"]

    assert "1 RESTART(S) RECORDED, 1 IN THE LAST" in history.detail
    assert "SIGKILL (9)" in history.detail
    # A flap reports as a WARN and does not decide the exit code: the restart may
    # already have been dealt with, and `antidemo status` failing for it would make
    # the command useless in exactly the situation it is run in.
    assert history.ok is False
    assert history.advisory is True

    journal.give_up("restarting six times in ten minutes did not fix it")
    gave_up = next(
        check
        for check in _status_checks(
            "127.0.0.1",
            8001,
            pid_is_alive=lambda _pid: True,
            command_line=lambda _pid: LIVE_COMMAND_LINE,
        )
        if check.name == "server_restart_history"
    )
    # Giving up is the one line that is a fault: nothing is watching the port now
    # and it will not fix itself.
    assert gave_up.ok is False
    assert gave_up.advisory is False
    assert "SUPERVISOR GAVE UP" in gave_up.detail


def test_status_says_plainly_when_there_is_no_restart_history(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_serving_process(state_dir=state_dir, argv=ADHOC_ARGV)
    monkeypatch.setattr("server.cli._app_is_online", lambda _host, _port: True)
    monkeypatch.setenv(SERVER_PORT_ENV, "8001")

    history = next(
        check
        for check in _status_checks(
            "127.0.0.1",
            8001,
            pid_is_alive=lambda _pid: True,
            command_line=lambda _pid: LIVE_COMMAND_LINE,
        )
        if check.name == "server_restart_history"
    )

    assert history.ok is True
    assert history.detail == "NO RESTARTS RECORDED"


def write_record_for(state_dir: Path, *, pid: int) -> ServerProcessRecord:
    from server.process_registry import write_record

    record = build_record(
        argv=ADHOC_ARGV,
        environ={MANIFEST_ENV: str(state_dir / "manifest.json")},
        pid=pid,
        stdout=("pipe", None),
    )
    return write_record(state_dir, record)


# --------------------------------------------------------------------------- #
# What the background launcher reads before it says READY
# --------------------------------------------------------------------------- #


def test_the_readiness_read_keeps_the_answer_a_503_carries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A degraded server answers 503 here, so the error body *is* the answer.

    Discarding it would leave exactly the blind spot this read exists to close:
    `/api/health` is a static literal by design, so a server refusing every
    control action with 409 was announcing itself as READY and nothing more.
    """
    import io
    import urllib.error

    from server.cli import _app_readiness

    payload = {
        "status": "degraded",
        "degraded": True,
        "degraded_detail": "AWS REJECTED THE CREDENTIALS IN THIS PROCESS",
    }

    def refuse(url, timeout=None):
        assert url == "http://127.0.0.1:8001/readyz"
        raise urllib.error.HTTPError(
            url, 503, "Service Unavailable", {}, io.BytesIO(json.dumps(payload).encode())
        )

    monkeypatch.setattr("server.cli.urllib.request.urlopen", refuse)
    assert _app_readiness("0.0.0.0", 8001) == payload


@pytest.mark.parametrize(
    "failure",
    [OSError("no route to host"), ValueError("nonsense")],
    ids=["unreachable", "unparseable"],
)
def test_an_unreadable_readiness_surface_is_reported_as_neither(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    """`None` is not "ready" and is not "degraded". It is "could not tell"."""
    from server.cli import _app_readiness

    def refuse(_url, timeout=None):
        raise failure

    monkeypatch.setattr("server.cli.urllib.request.urlopen", refuse)
    assert _app_readiness("127.0.0.1", 8001) is None
