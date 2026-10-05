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
import re
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
        "backup.py",
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


def _run_sh_array(name: str) -> set[str]:
    source = (BAR / "run.sh").read_text(encoding="utf-8")
    match = re.search(rf"^{name}=\((.*?)\)", source, flags=re.MULTILINE | re.DOTALL)
    assert match is not None, f"run.sh defines no {name} array"
    return set(match.group(1).split())


def test_run_sh_races_every_aws_lane_round_alone_against_rds_too():
    """The solo RDS waves are the only place the bar meets a round and RDS by themselves.

    v1.1 gave Round 4 an AWS lane, and run.sh still said Rounds 4 and 6 raced Lakebase
    alone, so the bar never ran Round 4 by itself against RDS. Rounds 4 and 6 race AWS on
    every installation that seals their lanes (`round_availability.aws_backed`), which a
    v1.1 install does.
    """

    from server.models import RoundId
    from server.round_availability import AWS_BACKED_ROUNDS

    raced = AWS_BACKED_ROUNDS | {RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS}
    assert _run_sh_array("AWS_LANE_ROUNDS") == {round_id.value for round_id in raced}
    assert _run_sh_array("AWS_LANE_ROUNDS") <= _run_sh_array("ROUNDS")


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
    for key in (*common.AWS_KEYS, "DATABRICKS_APP_NAME", "ANTI_DEMO_MANIFEST"):
        monkeypatch.delenv(key, raising=False)
    common.load_checkout_aws(tmp_path)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "example-id"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "example-secret"
    assert os.environ["AWS_DEFAULT_REGION"] == "us-west-2"
    # In the environment it would make the manifest loader believe it runs in the app.
    assert "DATABRICKS_APP_NAME" not in os.environ


def test_the_region_is_the_installation_s_own(tmp_path: Path, monkeypatch):
    """`.env.bootstrap` needs no region, and a region from the shell could be another one.

    Both leak checks read `AWS_DEFAULT_REGION` and crashed on a v1.1 test installation
    whose file had none. Scanning another region, `gone.py` would find nothing and say
    ALL GONE.
    """

    common = _load("_common")
    for key in (*common.AWS_KEYS, "ANTI_DEMO_MANIFEST"):
        monkeypatch.delenv(key, raising=False)
    (tmp_path / ".env.bootstrap").write_text("AWS_ACCESS_KEY_ID=example-id\n", encoding="utf-8")
    generation = tmp_path / ".anti-demo-v7"
    generation.mkdir()
    (generation / "manifest.json").write_text(
        json.dumps({"run_id": "ad-test", "aws": {"region": "eu-west-1"}}), encoding="utf-8"
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    common.load_checkout_aws(tmp_path)
    assert os.environ["AWS_DEFAULT_REGION"] == "eu-west-1"

    # `./antidemo cleanup` removes the manifest, and its receipt keeps the region.
    (generation / "manifest.json").unlink()
    (generation / "cleanup-receipt.json").write_text(
        json.dumps({"run_id": "ad-test", "aws_region": "eu-west-1"}), encoding="utf-8"
    )
    monkeypatch.delenv("AWS_DEFAULT_REGION")
    common.load_checkout_aws(tmp_path)
    assert os.environ["AWS_DEFAULT_REGION"] == "eu-west-1"
    assert common.installation_run_id(tmp_path) == "ad-test"


def test_the_bar_waits_out_the_longest_lane_the_app_allows():
    # rc13 (2026-10-03): AWS took 14.6 minutes to clone Aurora inside a backup. Every racing
    # round now decides such a bout at the one maximum, and a harness that gave up before it
    # would fail a bout the app was still timing honestly.
    from server.bout_limit import BOUT_TIME_LIMIT_SECONDS

    chaos = _load("chaos")
    assert chaos.VERDICT_TIMEOUT_SECONDS > BOUT_TIME_LIMIT_SECONDS + 300
    source = (BAR / "chaos.py").read_text(encoding="utf-8")
    assert "TERMINAL, 1500" not in source


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


@pytest.mark.parametrize(
    "round_id", ["put_model_score_in_app", "analyze_live_orders_without_slowing_checkout"]
)
def test_summary_reports_each_crash_step_as_it_does_the_restart(tmp_path: Path, round_id: str):
    step = f"crash-{round_id}"
    evidence = _evidence(tmp_path, failed_scenario=False)
    (evidence / step).mkdir()
    (evidence / step / "summary.json").write_text(
        json.dumps({"healed": True, "ready_after_restart_s": {round_id: 0.5}}),
        encoding="utf-8",
    )
    with (evidence / "steps.tsv").open("a", encoding="utf-8") as steps:
        steps.write(f"{step}\t1\t2026-01-01T01:00:00Z\t2026-01-01T01:10:00Z\n")

    result = subprocess.run(
        [sys.executable, str(BAR / "summarize.py"), str(evidence)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    summary = (evidence / "summary.md").read_text(encoding="utf-8")
    assert result.returncode == 1
    assert f"| {step} | FAILED (exit 1): READY again after {round_id} 0.5s |" in summary
    assert f"- {step}: see logs/{step}.log" in summary


def test_the_crash_step_lands_inside_a_round_four_and_a_round_six_bout():
    # Both rounds race two lanes from parked, and a bout of either is over before a
    # redeploy even stops the app, so only the crash step can strand their pipelines.
    assert _run_sh_array("CRASH_ROUNDS") == {
        "put_model_score_in_app",
        "analyze_live_orders_without_slowing_checkout",
    }
    source = (BAR / "run.sh").read_text(encoding="utf-8")
    body = source.split("crash() {", 1)[1].split("\n}\n", 1)[0]
    assert '--after-bell 20 --stop-start "$EVIDENCE/crash-$round"' in body
    assert 'CHAOS_ROUNDS="$round" CHAOS_SCENARIOS=full-proof' in body


def test_the_crash_and_backup_steps_run_by_default():
    source = (BAR / "run.sh").read_text(encoding="utf-8")
    match = re.search(r'^STEPS="\$\{STEPS:-([a-z,]+)\}"', source, flags=re.MULTILINE)
    assert match is not None
    assert match.group(1).split(",") == [
        "chaos",
        "backup",
        "restart",
        "crash",
        "leaks",
        "lease",
    ]


def test_the_backup_step_covers_every_source_check_that_was_strict():
    # rc10 (2026-10-02) found Rounds 2, 3 and 5 refusing or pausing on a source in its
    # daily backup; Round 1's arm and Round 5's seal had the same check.
    assert _run_sh_array("BACKUP_PAIRS") == {
        "wake_idle_app:aurora_serverless_v2",
        "make_schema_change_safely:aurora_serverless_v2",
        "make_schema_change_safely:rds_postgres",
        "recover_deleted_order:aurora_serverless_v2",
        "recover_deleted_order:rds_postgres",
        "survive_connection_spike:aurora_serverless_v2",
        "survive_connection_spike:rds_postgres",
    }
    source = (BAR / "run.sh").read_text(encoding="utf-8")
    assert 'if want backup && [[ "$stopped" == 0 ]]; then backup || stopped=1; fi' in source


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
        self.stop_start_error: Exception | None = None

    def headers(self) -> dict[str, str]:
        return {}

    def stop_start(self) -> str:
        if self.stop_start_error is not None:
            raise self.stop_start_error
        self.restarted = True
        return "lakebase-anti-demo-rc"

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


class LosesOneAnswer(FakeApp):
    """The FakeApp, but one POST's answer is cut off on its way back, as on rc8.

    `acted` says whether the app had acted on that request before its answer was lost.
    """

    def __init__(self, common, lost: type[Exception], control: str, *, acted: bool) -> None:
        super().__init__(common)
        self.lost_type = lost
        self.control = control
        self.acted = acted
        self.answered = False

    def call(self, method: str, path: str, body=None, timeout: float = 120):
        if method == "POST" and path.endswith(f"/{self.control}") and not self.answered:
            self.answered = True
            if self.acted:
                super().call(method, path, body, timeout)
            else:
                self.calls.append((method, path))
            raise self.lost_type(method, path, ConnectionResetError("connection reset by peer"))
        return super().call(method, path, body, timeout)


class RefusesOneBell(FakeApp):
    """The FakeApp, but the first bell is refused, which leaves its card armed."""

    def __init__(self, common) -> None:
        super().__init__(common)
        self.refused = False

    def call(self, method: str, path: str, body=None, timeout: float = 120):
        if method == "POST" and path.endswith("/run") and not self.refused:
            self.refused = True
            self.calls.append((method, path))
            return 500, {"detail": "the bell could not be rung"}
        return super().call(method, path, body, timeout)


def test_a_post_whose_answer_is_lost_is_named_and_a_get_is_retried(monkeypatch):
    import http.client

    common = _load("_common")
    client = common.AppClient("https://app.example.invalid", "profile")
    monkeypatch.setattr(client, "headers", lambda: {})
    monkeypatch.setattr(common.time, "sleep", lambda _seconds: None)
    answers: list[object] = []

    class Answer:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self) -> bytes:
            return b'{"state": "armed"}'

    def urlopen(_request, timeout):
        answer = answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(common.urllib.request, "urlopen", urlopen)

    answers[:] = [http.client.IncompleteRead(b"x" * 3756, 44937)]
    with pytest.raises(common.ResponseLost) as lost:
        client.call("POST", "/api/sessions/s1/arm")
    assert lost.value.path == "/api/sessions/s1/arm"
    # Nothing was sent twice: the caller decides, from the session, whether to.
    assert answers == []

    answers[:] = [http.client.IncompleteRead(b"x" * 3756, 44937), Answer()]
    assert client.call("GET", "/api/sessions/s1") == (200, {"state": "armed"})


@pytest.mark.parametrize(
    ("control", "acted"),
    [("arm", True), ("arm", False), ("run", True), ("run", False), ("towel", True)],
)
def test_chaos_settles_a_post_whose_answer_was_lost(
    tmp_path: Path, monkeypatch, control: str, acted: bool
):
    # rc8, 2026-10-01 18:12:50Z: an arm's answer was cut off on the way back, and the
    # scenario failed although the app had armed the card.
    common = _load("_common")
    chaos = _load("chaos")
    app = LosesOneAnswer(common, chaos.ResponseLost, control, acted=acted)
    monkeypatch.setattr(chaos, "AppClient", lambda: app)
    monkeypatch.setattr(chaos, "ROUNDS", ("put_model_score_in_app",))
    monkeypatch.setattr(chaos, "SCENARIOS", ("early-towel",))
    monkeypatch.setenv("CHAOS_QUIET", "1")
    harness = chaos.Harness(tmp_path / "chaos")

    assert harness.run() == 0
    sent = [path for method, path in app.calls if method == "POST" and path.endswith(f"/{control}")]
    # Sent again only when the session showed the app had never acted on it.
    assert len(sent) == (1 if acted else 2)
    events = (tmp_path / "chaos" / "events.jsonl").read_text(encoding="utf-8")
    assert '"kind": "response_lost"' in events


def test_a_failed_scenario_releases_its_card_so_the_next_one_still_runs(
    tmp_path: Path, monkeypatch
):
    # rc8, 2026-10-01: the recovery rang the bell into a fight card still checking, the
    # app rightly refused, and the four scenarios behind it found the round in use.
    common = _load("_common")
    chaos = _load("chaos")
    app = RefusesOneBell(common)
    monkeypatch.setattr(chaos, "AppClient", lambda: app)
    monkeypatch.setattr(chaos, "ROUNDS", ("put_model_score_in_app",))
    monkeypatch.setattr(chaos, "SCENARIOS", ("early-towel", "early-towel"))
    monkeypatch.setenv("CHAOS_QUIET", "1")
    harness = chaos.Harness(tmp_path / "chaos")

    assert harness.run() == 1
    results = json.loads((tmp_path / "chaos" / "results.json").read_text(encoding="utf-8"))
    assert [item["passed"] for item in results] == [False, True]
    assert "recovery_error" not in results[0]
    # The armed card was released, as "Change the matchup" does, not rung into.
    assert ("POST", "/api/sessions/s1/cancel-arm") in app.calls
    assert [path for method, path in app.calls if path == "/api/sessions/s1/run"] == [
        "/api/sessions/s1/run"
    ]


#: What the Databricks Apps front end answers in the app's place. rc23's bar got it once on
#: /api/bout/all (2026-10-05, 17:10:45Z), and the app's own log never saw that request.
PLATFORM_503 = {
    "error_code": "TEMPORARILY_UNAVAILABLE",
    "message": (
        "The service at /api/bout/all is temporarily unavailable. Please try again later. "
        "[TraceId: -]"
    ),
}


class PlatformAnswersOnce(FakeApp):
    """The FakeApp, but its front end answers one request in the app's place, as on rc23.

    `reached` says whether that request, a POST, had reached the app anyway.
    """

    def __init__(
        self, common, method: str, suffix: str, *, reached: bool = False, answer=None
    ) -> None:
        super().__init__(common)
        self.method = method
        self.suffix = suffix
        self.reached = reached
        self.answer = answer or (503, PLATFORM_503)
        self.answered = False

    def call(self, method: str, path: str, body=None, timeout: float = 120):
        if method == self.method and path.endswith(self.suffix) and not self.answered:
            self.answered = True
            if self.reached:
                super().call(method, path, body, timeout)
            else:
                self.calls.append((method, path))
            return self.answer
        return super().call(method, path, body, timeout)


@pytest.mark.parametrize(
    ("status", "payload", "platform"),
    [
        pytest.param(503, PLATFORM_503, True, id="front-end-503"),
        pytest.param(502, {"text": "<html>502 Bad Gateway</html>"}, True, id="a-proxy-page"),
        pytest.param(504, {}, True, id="an-empty-504"),
        pytest.param(503, {"detail": "The control plane is unavailable"}, False, id="the-apps"),
        pytest.param(500, PLATFORM_503, False, id="not-a-gateway-status"),
        pytest.param(409, {"detail": "The bout must be running"}, False, id="a-refusal"),
    ],
)
def test_the_front_end_answering_is_told_apart_from_the_app(status, payload, platform):
    assert _load("_common").answered_by_the_platform(status, payload) is platform


def test_chaos_asks_the_board_again_when_the_front_end_answered(tmp_path: Path, monkeypatch):
    common = _load("_common")
    chaos = _load("chaos")
    app = PlatformAnswersOnce(common, "GET", "/api/bout/all")
    monkeypatch.setattr(chaos, "AppClient", lambda: app)
    monkeypatch.setattr(chaos, "PLATFORM_RETRY_SECONDS", (0.0,) * 4)
    monkeypatch.setenv("CHAOS_QUIET", "1")
    harness = chaos.Harness(tmp_path / "chaos")

    board = harness.all_status()

    assert set(board["rounds"]) == set(common.ALL_ROUNDS)
    assert app.calls == [("GET", "/api/bout/all"), ("GET", "/api/bout/all")]


def test_a_scenario_rides_out_the_front_end_answering_a_poll(tmp_path: Path, monkeypatch):
    common = _load("_common")
    chaos = _load("chaos")
    app = PlatformAnswersOnce(common, "GET", "/api/sessions/s1")
    monkeypatch.setattr(chaos, "AppClient", lambda: app)
    monkeypatch.setattr(chaos, "PLATFORM_RETRY_SECONDS", (0.0,) * 4)
    monkeypatch.setattr(chaos, "ROUNDS", ("put_model_score_in_app",))
    monkeypatch.setattr(chaos, "SCENARIOS", ("early-towel",))
    monkeypatch.setenv("CHAOS_QUIET", "1")
    harness = chaos.Harness(tmp_path / "chaos")

    assert harness.run() == 0
    assert app.answered


@pytest.mark.parametrize(("control", "reached"), [("arm", True), ("arm", False), ("run", False)])
def test_chaos_settles_a_post_the_front_end_answered(
    tmp_path: Path, monkeypatch, control: str, reached: bool
):
    common = _load("_common")
    chaos = _load("chaos")
    app = PlatformAnswersOnce(common, "POST", f"/{control}", reached=reached)
    monkeypatch.setattr(chaos, "AppClient", lambda: app)
    monkeypatch.setattr(chaos, "ROUNDS", ("put_model_score_in_app",))
    monkeypatch.setattr(chaos, "SCENARIOS", ("early-towel",))
    monkeypatch.setenv("CHAOS_QUIET", "1")
    harness = chaos.Harness(tmp_path / "chaos")

    assert harness.run() == 0
    sent = [path for method, path in app.calls if method == "POST" and path.endswith(f"/{control}")]
    # Sent again only when the session showed the app had never acted on it.
    assert len(sent) == (1 if reached else 2)
    events = (tmp_path / "chaos" / "events.jsonl").read_text(encoding="utf-8")
    assert '"kind": "response_lost"' in events


def test_the_apps_own_503_still_fails_the_scenario(tmp_path: Path, monkeypatch):
    common = _load("_common")
    chaos = _load("chaos")
    app = PlatformAnswersOnce(
        common, "GET", "/api/sessions/s1", answer=(503, {"detail": "The control plane is down"})
    )
    monkeypatch.setattr(chaos, "AppClient", lambda: app)
    monkeypatch.setattr(chaos, "ROUNDS", ("put_model_score_in_app",))
    monkeypatch.setattr(chaos, "SCENARIOS", ("early-towel",))
    monkeypatch.setenv("CHAOS_QUIET", "1")
    harness = chaos.Harness(tmp_path / "chaos")

    assert harness.run() == 1
    results = json.loads((tmp_path / "chaos" / "results.json").read_text(encoding="utf-8"))
    assert [item["passed"] for item in results] == [False]


def test_restart_settles_an_arm_whose_answer_was_lost(tmp_path: Path, monkeypatch):
    common = _load("_common")
    restart = _load("restart")
    app = LosesOneAnswer(common, restart.ResponseLost, "arm", acted=True)
    monkeypatch.setattr(restart, "AppClient", lambda: app)
    monkeypatch.setattr(restart.time, "sleep", lambda _seconds: None)
    app.stop_start_error = RuntimeError("stop here: the bouts are already rung")
    argv = ["restart.py", str(tmp_path / "restart"), "put_model_score_in_app", "--stop-start"]
    monkeypatch.setattr(sys, "argv", argv)

    assert restart.main() == 2
    events = [
        json.loads(line)
        for line in (tmp_path / "restart" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    kinds = [event["kind"] for event in events]
    assert "response_lost" in kinds
    assert {"kind": "armed", "state": "armed"}.items() <= next(
        event for event in events if event["kind"] == "armed"
    ).items()
    assert kinds.index("bell") > kinds.index("armed")


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


def test_restart_can_stop_and_start_the_app_itself(tmp_path: Path, monkeypatch):
    """The crash step: seconds, not a redeploy's minutes, so it lands inside a Round 4 bout."""

    common = _load("_common")
    restart = _load("restart")
    app = FakeApp(common)
    monkeypatch.setattr(restart, "AppClient", lambda: app)
    evidence = tmp_path / "crash"
    arguments = [str(evidence), "--after-bell", "0", "--stop-start", "put_model_score_in_app"]
    monkeypatch.setattr(sys, "argv", ["restart.py", *arguments])

    assert restart.main() == 0
    events = [
        json.loads(line)
        for line in (evidence / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    kinds = [event["kind"] for event in events]
    assert kinds.index("restart_begin") < kinds.index("stop_start_done")
    assert kinds.index("stop_start_done") < kinds.index("restart_done") < kinds.index("all_ready")
    assert next(e for e in events if e["kind"] == "stop_start_done")["app"] == (
        "lakebase-anti-demo-rc"
    )

    failed = FakeApp(common)
    failed.stop_start_error = RuntimeError("no app in this workspace serves the URL")
    monkeypatch.setattr(restart, "AppClient", lambda: failed)
    monkeypatch.setattr(
        sys, "argv", ["restart.py", str(tmp_path / "crash-2"), "--after-bell", "0", "--stop-start"]
    )
    assert restart.main() == 2


def test_stop_start_stops_only_the_app_at_the_url(monkeypatch):
    import types

    common = _load("_common")
    touched: list[tuple[str, str]] = []

    class Wait:
        def result(self):
            return None

    class Apps:
        def list(self):
            return [
                types.SimpleNamespace(name="someone-elses", url="https://other.example"),
                types.SimpleNamespace(name="ours", url="https://ours.example/"),
            ]

        def stop(self, name):
            touched.append(("stop", name))
            return Wait()

        def start(self, name):
            touched.append(("start", name))
            return Wait()

    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = lambda profile: types.SimpleNamespace(apps=Apps())
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)

    client = common.AppClient(base_url="https://ours.example", profile="test")
    assert client.stop_start() == "ours"
    assert touched == [("stop", "ours"), ("start", "ours")]

    stranger = common.AppClient(base_url="https://nobody.example", profile="test")
    with pytest.raises(RuntimeError, match="no app in this workspace serves"):
        stranger.stop_start()
    assert touched == [("stop", "ours"), ("start", "ours")]


#: One empty page for every listing `gone.remaining` pages through.
_EMPTY_LISTINGS = {
    ("ec2", "describe_instances"): [{"Reservations": []}],
    ("ec2", "describe_volumes"): [{"Volumes": []}],
    ("ec2", "describe_security_groups"): [{"SecurityGroups": []}],
    ("ec2", "describe_security_group_rules"): [{"SecurityGroupRules": []}],
    ("ec2", "describe_subnets"): [{"Subnets": []}],
    ("ec2", "describe_route_tables"): [{"RouteTables": []}],
    ("ec2", "describe_vpc_endpoints"): [{"VpcEndpoints": []}],
    ("rds", "describe_db_clusters"): [{"DBClusters": []}],
    ("rds", "describe_db_instances"): [{"DBInstances": []}],
    ("rds", "describe_db_proxies"): [{"DBProxies": []}],
    ("rds", "describe_db_parameter_groups"): [{"DBParameterGroups": []}],
    ("rds", "describe_db_cluster_parameter_groups"): [{"DBClusterParameterGroups": []}],
    ("sqs", "list_queues"): [{}],
    ("secretsmanager", "list_secrets"): [{"SecretList": []}],
    ("iam", "list_roles"): [{"Roles": []}],
    ("iam", "list_instance_profiles"): [{"InstanceProfiles": []}],
    ("iam", "list_policies"): [{"Policies": []}],
    ("glue", "get_jobs"): [{"Jobs": []}],
    ("glue", "get_connections"): [{"ConnectionList": []}],
    ("dms", "describe_replication_instances"): [{"ReplicationInstances": []}],
    ("dms", "describe_endpoints"): [{"Endpoints": []}],
    ("dms", "describe_replication_tasks"): [{"ReplicationTasks": []}],
    ("dms", "describe_replication_subnet_groups"): [{"ReplicationSubnetGroups": []}],
}


class _ListingSession:
    """Answers every read `gone.remaining` and `leftovers.leftovers` make, and reaches no AWS.

    A listing is its pages, or a function of the paginator's arguments returning them.
    """

    region_name = "us-west-2"

    def __init__(self, pages, responses):
        self.pages = {**_EMPTY_LISTINGS, **pages}
        self.responses = responses
        self.asked: list[tuple[str, str, dict]] = []

    def client(self, service, **_options):
        session = self

        class Paginator:
            def __init__(self, operation):
                self.operation = operation

            def paginate(self, **arguments):
                pages = session.pages[(service, self.operation)]
                return iter(pages(**arguments) if callable(pages) else pages)

        class Client:
            def get_paginator(self, operation):
                return Paginator(operation)

            def __getattr__(self, operation):
                def call(**arguments):
                    session.asked.append((service, operation, arguments))
                    answer = session.responses[(service, operation)]
                    return answer(**arguments) if callable(answer) else answer

                return call

        return Client()


def test_gone_counts_what_round_four_s_glue_lane_leaves_behind():
    gone = _load("gone")
    run_id = "ad-test-001"
    ours = [{"Key": "anti-demo-run-id", "Value": run_id}]
    session = _ListingSession(
        pages={
            ("ec2", "describe_subnets"): [{"Subnets": [{"SubnetId": "subnet-1"}]}],
            ("ec2", "describe_route_tables"): [{"RouteTables": [{"RouteTableId": "rtb-1"}]}],
            ("ec2", "describe_vpc_endpoints"): [
                {
                    "VpcEndpoints": [
                        {"VpcEndpointId": "vpce-1", "State": "deleting"},
                        {"VpcEndpointId": "vpce-2", "State": "deleted"},
                    ]
                }
            ],
            ("glue", "get_jobs"): [
                {"Jobs": [{"Name": "lakebase-ant-x-r4-writer-rds"}, {"Name": "someone-elses-job"}]}
            ],
            ("glue", "get_connections"): [{"ConnectionList": [{"Name": "lakebase-ant-x-r4-rds"}]}],
        },
        responses={
            ("sts", "get_caller_identity"): {"Account": "123456789012"},
            ("s3", "list_buckets"): {
                "Buckets": [{"Name": "lakebase-ant-x-r4-glue"}, {"Name": "unrelated-bucket"}]
            },
            ("s3", "get_bucket_tagging"): {"TagSet": ours},
            ("glue", "get_tags"): lambda ResourceArn: {"Tags": {"anti-demo-run-id": run_id}},
        },
    )

    counts = gone.remaining(session, run_id)

    assert counts["subnets"] == 1 and counts["route_tables"] == 1
    # An endpoint still deleting is not gone yet; one AWS reports deleted is.
    assert counts["vpc_endpoints"] == 1
    assert counts["s3_buckets"] == 1
    assert counts["glue_jobs"] == 1 and counts["glue_connections"] == 1
    asked = [str(arguments) for _service, _operation, arguments in session.asked]
    # Only the lane's own names are asked about.
    assert not any("unrelated-bucket" in item or "someone-elses-job" in item for item in asked)
    assert (
        "glue",
        "get_tags",
        {"ResourceArn": "arn:aws:glue:us-west-2:123456789012:job/lakebase-ant-x-r4-writer-rds"},
    ) in session.asked


def test_gone_counts_round_six_parameter_groups_by_their_own_tags():
    # Their describes carry no tags, so only groups named like the installation's
    # are asked about, and each counts only if its tags say it is this run's.
    gone = _load("gone")
    run_id = "ad-test-001"
    arn = "arn:aws:rds:us-west-2:123456789012"
    tags = {
        f"{arn}:pg:ours-r6-rds-lakeflow": [{"Key": "anti-demo-run-id", "Value": run_id}],
        f"{arn}:pg:theirs-r6-rds-lakeflow": [{"Key": "anti-demo-run-id", "Value": "other"}],
        f"{arn}:cluster-pg:ours-r6-aurora-lakeflow": [{"Key": "anti-demo-run-id", "Value": run_id}],
    }
    session = _ListingSession(
        pages={
            ("rds", "describe_db_parameter_groups"): [
                {
                    "DBParameterGroups": [
                        {
                            "DBParameterGroupName": "ours-r6-rds-lakeflow",
                            "DBParameterGroupArn": f"{arn}:pg:ours-r6-rds-lakeflow",
                        },
                        {
                            "DBParameterGroupName": "theirs-r6-rds-lakeflow",
                            "DBParameterGroupArn": f"{arn}:pg:theirs-r6-rds-lakeflow",
                        },
                        {
                            "DBParameterGroupName": "default.postgres17",
                            "DBParameterGroupArn": f"{arn}:pg:default.postgres17",
                        },
                    ]
                }
            ],
            ("rds", "describe_db_cluster_parameter_groups"): [
                {
                    "DBClusterParameterGroups": [
                        {
                            "DBClusterParameterGroupName": "ours-r6-aurora-lakeflow",
                            "DBClusterParameterGroupArn": (
                                f"{arn}:cluster-pg:ours-r6-aurora-lakeflow"
                            ),
                        }
                    ]
                }
            ],
        },
        responses={
            ("sts", "get_caller_identity"): {"Account": "123456789012"},
            ("s3", "list_buckets"): {"Buckets": []},
            ("rds", "list_tags_for_resource"): lambda ResourceName: {"TagList": tags[ResourceName]},
        },
    )

    counts = gone.remaining(session, run_id)

    assert counts["rds_parameter_groups"] == 1
    assert counts["rds_cluster_parameter_groups"] == 1
    asked = [arguments.get("ResourceName") for _s, _o, arguments in session.asked]
    assert f"{arn}:pg:default.postgres17" not in asked


def _dms_tags(run_id: str, arns_by_owner: dict[str, str]):
    """list_tags_for_resource for DMS: this run's tag on its own ARNs, another run's elsewhere."""

    def answer(ResourceArn):
        owner = arns_by_owner.get(ResourceArn, "someone-else")
        return {"TagList": [{"Key": "anti-demo-run-id", "Value": owner}]}

    return answer


def test_gone_counts_round_six_dms_resources_by_their_own_tags():
    gone = _load("gone")
    run_id = "ad-test-001"
    ours = "lakebase-ant-x-r6"
    instance = "arn:aws:dms:us-west-2:123456789012:rep:OURS"
    endpoint = "arn:aws:dms:us-west-2:123456789012:endpoint:OURS"
    task = "arn:aws:dms:us-west-2:123456789012:task:OURS"
    subnet_group = f"arn:aws:dms:us-west-2:123456789012:subgrp:{ours}-dms"
    session = _ListingSession(
        pages={
            ("dms", "describe_replication_instances"): [
                {
                    "ReplicationInstances": [
                        {
                            "ReplicationInstanceIdentifier": f"{ours}-dms",
                            "ReplicationInstanceArn": instance,
                        },
                        {
                            "ReplicationInstanceIdentifier": "not-ours",
                            "ReplicationInstanceArn": "x",
                        },
                    ]
                }
            ],
            ("dms", "describe_endpoints"): [
                {
                    "Endpoints": [
                        {"EndpointIdentifier": f"{ours}-src-rds", "EndpointArn": endpoint},
                        {"EndpointIdentifier": "lakebase-ant-other", "EndpointArn": "other"},
                    ]
                }
            ],
            ("dms", "describe_replication_tasks"): [
                {
                    "ReplicationTasks": [
                        {"ReplicationTaskIdentifier": f"{ours}-cdc-rds", "ReplicationTaskArn": task}
                    ]
                }
            ],
            ("dms", "describe_replication_subnet_groups"): [
                {"ReplicationSubnetGroups": [{"ReplicationSubnetGroupIdentifier": f"{ours}-dms"}]}
            ],
        },
        responses={
            ("sts", "get_caller_identity"): {"Account": "123456789012"},
            ("s3", "list_buckets"): {"Buckets": [{"Name": f"{ours}-cdc"}]},
            ("s3", "get_bucket_tagging"): {
                "TagSet": [{"Key": "anti-demo-run-id", "Value": run_id}]
            },
            ("dms", "list_tags_for_resource"): _dms_tags(
                run_id, {instance: run_id, endpoint: run_id, task: run_id, subnet_group: run_id}
            ),
        },
    )

    counts = gone.remaining(session, run_id)

    assert counts["dms_replication_instances"] == 1
    assert counts["dms_endpoints"] == 1
    assert counts["dms_tasks"] == 1
    assert counts["dms_subnet_groups"] == 1
    # The lane's bucket is found by its own suffix.
    assert counts["s3_buckets"] == 1
    asked = [arguments for service, _, arguments in session.asked if service == "dms"]
    # Only the lane's own names are asked about.
    assert {"ResourceArn": "x"} not in asked


def test_gone_reads_an_empty_dms_account_as_nothing_left():
    gone = _load("gone")

    class EmptyDms(_ListingSession):
        def client(self, service, **options):
            client = super().client(service, **options)
            if service != "dms":
                return client

            class Refusing:
                def get_paginator(self, operation):
                    class Pages:
                        def paginate(self, **_arguments):
                            from botocore.exceptions import ClientError

                            raise ClientError(
                                {"Error": {"Code": "ResourceNotFoundFault"}}, operation
                            )

                    return Pages()

            return Refusing()

    session = EmptyDms(
        pages={},
        responses={
            ("sts", "get_caller_identity"): {"Account": "123456789012"},
            ("s3", "list_buckets"): {"Buckets": []},
        },
    )
    counts = gone.remaining(session, "ad-test-001")
    assert counts["dms_tasks"] == counts["dms_endpoints"] == 0


def test_gone_names_round_six_unity_catalog_objects_as_the_installer_does():
    from server.round6_aws_lifecycle import uc_names

    gone = _load("gone")
    run_id = "ad-20" + "990101-0000-" + "test"  # assembled, so no real-shaped ID is committed
    names = gone.unity_catalog_names(run_id)
    assert names["storage-credentials"] == uc_names(run_id)["storage_credential"]
    assert names["external-locations"] == uc_names(run_id)["external_location"]


def test_gone_counts_round_six_unity_catalog_objects_left_in_the_metastore():
    gone = _load("gone")
    asked: list[list[str]] = []

    def run(command, **_kwargs):
        asked.append(command)
        found = "storage-credentials" in command[3]
        return subprocess.CompletedProcess(
            command,
            0 if found else 1,
            stdout="{}" if found else "",
            stderr="" if found else "Error: RESOURCE_DOES_NOT_EXIST: not found",
        )

    counts = gone.unity_catalog_remaining("profile-x", "ad-test-001", run=run)
    assert counts == {"uc_storage_credentials": 1, "uc_external_locations": 0}
    assert all(command[-2:] == ["-p", "profile-x"] for command in asked)


def test_gone_stops_rather_than_calling_an_unreadable_metastore_gone():
    gone = _load("gone")

    def run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="Error: 403 Forbidden")

    with pytest.raises(SystemExit, match="could not read"):
        gone.unity_catalog_remaining("profile-x", "ad-test-001", run=run)


def test_the_databricks_profile_is_found_before_and_after_the_uninstall(tmp_path: Path):
    common = _load("_common")
    generation = tmp_path / ".anti-demo-v7"
    generation.mkdir()
    (generation / "manifest.json").write_text(
        json.dumps({"run_id": "ad-1", "databricks": {"profile": "installed-profile"}}),
        encoding="utf-8",
    )
    assert common.installation_databricks_profile(tmp_path) == "installed-profile"
    (generation / "manifest.json").unlink()
    (generation / "cleanup-receipt.json").write_text(
        json.dumps({"run_id": "ad-1", "lakebase_profiles": ["receipt-profile"]}),
        encoding="utf-8",
    )
    assert common.installation_databricks_profile(tmp_path) == "receipt-profile"


def test_leftovers_counts_a_round_six_dms_task_still_running_for_this_run_only():
    leftovers = _load("leftovers")
    run_id = "ad-test-001"
    ours = "arn:aws:dms:us-west-2:123456789012:task:OURS"
    parked = "arn:aws:dms:us-west-2:123456789012:task:PARKED"
    theirs = "arn:aws:dms:us-west-2:123456789012:task:THEIRS"
    session = _ListingSession(
        pages={
            ("dms", "describe_replication_tasks"): [
                {
                    "ReplicationTasks": [
                        {
                            "ReplicationTaskIdentifier": "lakebase-ant-x-r6-cdc-rds",
                            "ReplicationTaskArn": ours,
                            "Status": "running",
                        },
                        {
                            "ReplicationTaskIdentifier": "lakebase-ant-x-r6-cdc-aurora",
                            "ReplicationTaskArn": parked,
                            "Status": "stopped",
                        },
                        {
                            "ReplicationTaskIdentifier": "lakebase-ant-y-r6-cdc-rds",
                            "ReplicationTaskArn": theirs,
                            "Status": "running",
                        },
                    ]
                }
            ],
        },
        responses={
            ("sts", "get_caller_identity"): {"Account": "123456789012"},
            ("dms", "list_tags_for_resource"): _dms_tags(run_id, {ours: run_id, parked: run_id}),
        },
    )

    found = leftovers.leftovers(session, run_id)

    assert dict(found) == {("dms-task", "app", "running"): 1}


def test_leftovers_counts_a_round_four_glue_run_still_active():
    """A Round 4 bout's Glue run is the one per-bout thing that is not a resource."""

    leftovers = _load("leftovers")
    run_id = "ad-test-001"
    runs = {
        "lakebase-ant-x-r4-writer-rds": [
            {"JobRuns": [{"JobRunState": "RUNNING"}, {"JobRunState": "STOPPED"}]}
        ],
        "lakebase-ant-x-r4-writer-aurora": [{"JobRuns": [{"JobRunState": "TIMEOUT"}]}],
        # Another installation's writer, in the same account.
        "lakebase-ant-y-r4-writer-rds": [{"JobRuns": [{"JobRunState": "RUNNING"}]}],
    }
    session = _ListingSession(
        pages={
            ("glue", "get_jobs"): [
                {"Jobs": [{"Name": name} for name in runs] + [{"Name": "someone-elses-job"}]}
            ],
            ("glue", "get_job_runs"): lambda JobName: runs[JobName],
        },
        responses={
            ("sts", "get_caller_identity"): {"Account": "123456789012"},
            ("glue", "get_tags"): lambda ResourceArn: {
                "Tags": {"anti-demo-run-id": "ad-other" if "-y-" in ResourceArn else run_id}
            },
        },
    )

    assert leftovers.leftovers(session, run_id) == {("glue-job-run", "app", "RUNNING"): 1}
    asked = [str(arguments) for _service, _operation, arguments in session.asked]
    assert not any("someone-elses-job" in item for item in asked)


def test_restart_holds_round_four_under_the_redeploy_too():
    """v1.1's Round 4 leaves a pipeline and a Glue run behind a dead process."""

    restart = _load("restart")
    assert set(restart.DEFAULT_ROUNDS) == _run_sh_array("RESTART_ROUNDS")
    assert "put_model_score_in_app" in restart.DEFAULT_ROUNDS


def _client_error(code: str):
    from botocore.exceptions import ClientError

    return ClientError({"Error": {"Code": code, "Message": code}}, "ListTagsForResource")


def test_a_tag_read_reads_gone_as_nothing_and_deleting_as_not_yet():
    common = _load("_common")

    def gone(**_arguments):
        raise _client_error("DBProxyNotFoundFault")

    def deleting(**_arguments):
        raise _client_error("InvalidResourceStateFault")

    assert common.tags_of(gone, "TagList", ResourceName="arn:proxy") is None
    with pytest.raises(common.ResourceInTransition, match="arn:dms:rep:X is still being deleted"):
        common.tags_of(deleting, "TagList", ResourceArn="arn:dms:rep:X")


def test_a_leak_check_counts_again_once_a_deletion_finishes():
    # rc8's ALL GONE check, 2026-10-01 19:57:56Z: another installation's DMS replication
    # instance was mid-deletion, its tags could not be read, and the check crashed.
    common = _load("_common")
    answers: list[object] = [
        common.ResourceInTransition("arn:dms:rep:X is still being deleted"),
        {"dms_replication_instances": 0},
    ]
    slept: list[float] = []

    def count():
        answer = answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    assert common.counted_when_settled(count, sleep=slept.append) == {
        "dms_replication_instances": 0
    }
    assert slept == [common.TRANSITION_POLL_SECONDS]


def test_a_leak_check_stops_on_a_deletion_that_never_finishes():
    common = _load("_common")

    def count():
        raise common.ResourceInTransition("arn:dms:rep:X is still being deleted")

    with pytest.raises(SystemExit, match="arn:dms:rep:X is still being deleted"):
        common.counted_when_settled(count, wait=0, sleep=lambda _seconds: None)


def test_leftovers_skips_a_proxy_deleted_between_its_listing_and_its_tag_read():
    # Every Proxy in the account is asked in turn, other teams' too.
    leftovers = _load("leftovers")

    def tags(**_arguments):
        raise _client_error("DBProxyNotFoundFault")

    proxy = {"DBProxyArn": "arn:aws:rds:us-west-2:123456789012:db-proxy:prx-theirs"}
    session = _ListingSession(
        pages={("rds", "describe_db_proxies"): [{"DBProxies": [{**proxy, "Status": "deleting"}]}]},
        responses={
            ("rds", "list_tags_for_resource"): tags,
            ("sts", "get_caller_identity"): {"Account": "123456789012"},
        },
    )

    assert not leftovers.leftovers(session, "ad-test-001")
