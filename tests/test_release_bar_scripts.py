"""`scripts/release_bar/` must stay runnable, because nothing else exercises it offline.

The release bar runs against a real installation for hours, so a script that
no longer parses, or a helper that picks the wrong generation, is found at the
start of a seven-hour run instead of in CI. These tests hold what can be held
without an app or AWS: every script parses and answers `--help` with no
environment, the one command refuses to start without its inputs, and the
helpers the verdict depends on do what the docs say.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
BAR = REPO / "scripts" / "release_bar"
SCRIPTS = sorted(path for path in BAR.glob("*.py") if not path.name.startswith("_"))
BARE_ENV = {
    key: value
    for key, value in os.environ.items()
    if key not in {"ANTI_DEMO_APP_URL", "ANTI_DEMO_PROFILE", "ANTI_DEMO_MANIFEST"}
}


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"release_bar_{name}", BAR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_step_has_a_script():
    assert {path.name for path in SCRIPTS} == {
        "chaos.py",
        "gone.py",
        "lease_check.py",
        "leftovers.py",
        "ready.py",
        "restart.py",
        "summarize.py",
    }


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda path: path.name)
def test_each_script_answers_help_without_an_installation(script: Path):
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        env=BARE_ENV,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_run_sh_parses():
    result = subprocess.run(
        ["bash", "-n", str(BAR / "run.sh")], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_run_sh_refuses_without_its_inputs(tmp_path: Path):
    usage = subprocess.run(
        ["bash", str(BAR / "run.sh")], capture_output=True, text=True, env=BARE_ENV, timeout=60
    )
    assert usage.returncode == 2
    assert "usage:" in usage.stderr

    unset = subprocess.run(
        ["bash", str(BAR / "run.sh"), str(tmp_path)],
        capture_output=True,
        text=True,
        env=BARE_ENV,
        timeout=60,
        cwd=tmp_path,
    )
    assert unset.returncode != 0
    assert "ANTI_DEMO_APP_URL" in unset.stderr
    assert not (tmp_path / "release-bar-evidence").exists()


def test_latest_manifest_compares_generations_as_numbers(tmp_path: Path):
    common = _load("_common")
    for generation in ("7", "10", "9"):
        directory = tmp_path / f".anti-demo-v{generation}"
        directory.mkdir()
        (directory / "manifest.json").write_text("{}", encoding="utf-8")
    assert common.latest_manifest(tmp_path) == tmp_path / ".anti-demo-v10" / "manifest.json"


def test_the_run_id_survives_the_uninstall(tmp_path: Path, monkeypatch):
    common = _load("_common")
    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)
    older = tmp_path / ".anti-demo-v7"
    newer = tmp_path / ".anti-demo-v8"
    older.mkdir()
    newer.mkdir()
    (older / "manifest.json").write_text(json.dumps({"run_id": "old-run"}), encoding="utf-8")
    (newer / "manifest.json").write_text(json.dumps({"run_id": "new-run"}), encoding="utf-8")
    assert common.installation_run_id(tmp_path) == "new-run"

    # A selected manifest wins over the newest generation.
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(older / "manifest.json"))
    assert common.installation_run_id(tmp_path) == "old-run"
    monkeypatch.delenv("ANTI_DEMO_MANIFEST")

    # `./antidemo cleanup` removes the manifest and leaves a receipt beside it.
    (newer / "manifest.json").unlink()
    (newer / "cleanup-receipt.json").write_text(json.dumps({"run_id": "new-run"}), encoding="utf-8")
    assert common.installation_run_id(tmp_path) == "new-run"


def test_only_the_aws_key_is_taken_from_env_bootstrap(tmp_path: Path, monkeypatch):
    common = _load("_common")
    (tmp_path / ".env.bootstrap").write_text(
        "# a comment\n"
        "export AWS_ACCESS_KEY_ID=example-id\n"
        "AWS_SECRET_ACCESS_KEY='example-secret'\n"
        'AWS_DEFAULT_REGION="us-west-2"\n'
        "DATABRICKS_APP_NAME=lakebase-anti-demo-rc\n",
        encoding="utf-8",
    )
    for key in (*common.AWS_KEYS, "DATABRICKS_APP_NAME"):
        monkeypatch.delenv(key, raising=False)
    common.load_checkout_aws(tmp_path)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "example-id"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "example-secret"
    assert os.environ["AWS_DEFAULT_REGION"] == "us-west-2"
    # In the environment it would make the manifest loader believe it runs in the app.
    assert "DATABRICKS_APP_NAME" not in os.environ


def test_all_ready_needs_every_one_of_the_six_rounds():
    common = _load("_common")
    ready = {"state": "ready", "can_start": True}
    board = dict.fromkeys(common.ALL_ROUNDS, ready)
    assert common.all_ready(board)
    assert not common.all_ready({**board, "survive_connection_spike": {"state": "ready"}})
    missing = dict(board)
    missing.pop("recover_deleted_order")
    assert not common.all_ready(missing)


def test_only_round_1_is_canceled_before_the_bell():
    chaos = _load("chaos")
    every = chaos.ALL_ROUNDS
    assert chaos.wave_rounds("pre-bell-cancel", every, set()) == ("wake_idle_app",)
    assert chaos.wave_rounds("pre-bell-cancel", ("recover_deleted_order",), set()) == ()
    assert chaos.wave_rounds("late-towel", every, set()) == every
    assert chaos.wave_rounds("late-towel", every, {"late-towel:wake_idle_app"}) == every[1:]


def _evidence(tmp_path: Path, *, failed_scenario: bool) -> Path:
    evidence = tmp_path / "run"
    phase = evidence / "chaos" / "all-aurora"
    phase.mkdir(parents=True)
    results = [
        {"scenario": "early-towel", "round": "wake_idle_app", "passed": True},
        {
            "scenario": "late-towel",
            "round": "survive_connection_spike",
            "passed": not failed_scenario,
            "error": None if not failed_scenario else "TimeoutError",
            "detail": None if not failed_scenario else "did not return ready",
        },
    ]
    (phase / "results.json").write_text(json.dumps(results), encoding="utf-8")
    (phase / "isolation-failures.json").write_text("[]", encoding="utf-8")
    (evidence / "steps.tsv").write_text(
        "preflight\t0\t2026-01-01T00:00:00Z\t2026-01-01T00:01:00Z\n"
        f"chaos-all-aurora\t{1 if failed_scenario else 0}\t2026-01-01T00:01:00Z\t"
        "2026-01-01T01:00:00Z\n",
        encoding="utf-8",
    )
    return evidence


@pytest.mark.parametrize("failed_scenario", [False, True])
def test_summary_fails_exactly_when_a_step_did(tmp_path: Path, failed_scenario: bool):
    evidence = _evidence(tmp_path, failed_scenario=failed_scenario)
    result = subprocess.run(
        [sys.executable, str(BAR / "summarize.py"), str(evidence)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    summary = (evidence / "summary.md").read_text(encoding="utf-8")
    if failed_scenario:
        assert result.returncode == 1
        assert "**FAILED**" in summary
        assert "late-towel survive_connection_spike: TimeoutError" in summary
        assert "Chaos scenarios: 1/2 passed." in summary
    else:
        assert result.returncode == 0, result.stdout
        assert "**PASSED**" in summary
        assert "Chaos scenarios: 2/2 passed." in summary


class FakeApp:
    """Just enough of the app's session API for the drivers to run a bout against.

    Every round is always READY on the board. A session goes armed on ARM,
    running on RUN, verified a second after the bell, and toweled on TOWEL.
    Once `restarted` is set, every session from before is gone, as after a real
    restart.
    """

    def __init__(self, common) -> None:
        self.rounds = common.ALL_ROUNDS
        self.sessions: dict[str, dict] = {}
        self.calls: list[tuple[str, str]] = []
        self.restarted = False

    def headers(self) -> dict[str, str]:
        return {}

    def call(self, method: str, path: str, body=None, timeout: float = 120):
        import time

        self.calls.append((method, path))
        if path == "/api/bout/all":
            return 200, {"rounds": {r: {"state": "ready", "can_start": True} for r in self.rounds}}
        if method == "POST" and path == "/api/sessions":
            session_id = f"s{len(self.sessions) + 1}"
            self.sessions[session_id] = {"id": session_id, "state": "created", "lanes": {}}
            return 201, {"id": session_id}
        session_id, _, control = path.removeprefix("/api/sessions/").partition("/")
        session = self.sessions.get(session_id)
        if session is None or self.restarted:
            return 404, {"detail": "Session not found"}
        if method == "GET":
            if session["state"] == "running" and time.monotonic() - session["bell"] > 1:
                session["state"] = "verified"
            return 200, dict(session)
        if control == "arm":
            session["state"] = "armed"
        elif control == "cancel-arm":
            session.update(state="failed", failure="Fight-card check cancelled by the owner.")
        elif control == "run":
            session.update(state="running", bell=time.monotonic())
        elif control == "towel":
            if session["state"] != "running":
                return 409, {"detail": "The bout must be running"}
            session["state"] = "towelled"
        return 200, dict(session)


def test_chaos_runs_a_bout_through_every_kind_of_wave(tmp_path: Path, monkeypatch):
    common = _load("_common")
    chaos = _load("chaos")
    app = FakeApp(common)
    monkeypatch.setattr(chaos, "AppClient", lambda: app)
    monkeypatch.setattr(chaos, "ROUNDS", ("wake_idle_app",))
    monkeypatch.setattr(
        chaos, "SCENARIOS", ("pre-bell-cancel", "early-towel", "full-proof", "rapid-rearm-towel")
    )
    monkeypatch.setenv("CHAOS_QUIET", "1")
    harness = chaos.Harness(tmp_path / "chaos")

    assert harness.run() == 0
    results = json.loads((tmp_path / "chaos" / "results.json").read_text(encoding="utf-8"))
    assert [(item["scenario"], item["passed"]) for item in results] == [
        ("pre-bell-cancel", True),
        ("early-towel", True),
        ("full-proof", True),
        ("rapid-rearm-towel", True),
    ]
    # A verified bout asks for its reset, as the UI does.
    assert ("POST", "/api/sessions/s3/cooldown") in app.calls


def test_restart_waits_for_the_redeploy_and_then_for_every_round(tmp_path: Path, monkeypatch):
    import threading
    import time

    common = _load("_common")
    restart = _load("restart")
    app = FakeApp(common)
    monkeypatch.setattr(restart, "AppClient", lambda: app)
    evidence = tmp_path / "restart"
    monkeypatch.setattr(
        sys, "argv", ["restart.py", str(evidence), "--after-bell", "0", "recover_deleted_order"]
    )

    def redeploy() -> None:
        # run.sh's part: redeploy once the driver says the bouts are in the air.
        while not (evidence / "READY_FOR_RESTART").exists():
            time.sleep(0.05)
        app.restarted = True

    deployer = threading.Thread(target=redeploy, daemon=True)
    deployer.start()
    assert restart.main() == 0
    summary = json.loads((evidence / "summary.json").read_text(encoding="utf-8"))
    assert summary["healed"] is True
    assert summary["rounds_in_play"] == ["recover_deleted_order"]
    events = [
        json.loads(line)["kind"]
        for line in (evidence / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert events.index("restart_begin") < events.index("restart_done") < events.index("all_ready")
