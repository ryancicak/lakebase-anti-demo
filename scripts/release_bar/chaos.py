"""Chaos waves against a deployed app: every round at once, or the rounds you name.

Each wave starts one bout per round in parallel and then disturbs it the way a
presenter would:

    pre-bell-cancel    cancel while the fight card is being checked (Round 1 only)
    early-towel        throw the towel just after the bell
    mid-stage-towel    ... in the middle of the race
    late-towel         ... near the end
    rapid-rearm-towel  towel, and arm a new bout the moment the round is READY again
    full-proof         let the bout finish
    finish-linger      let it finish and leave the result on screen; nothing may change

A scenario passes when the bout ends verified or toweled (a canceled check for
`pre-bell-cancel`) and its round comes back READY. The run also fails if a round
not in play is ever anything but READY: one round must never disturb another.
Every `/api/bout/all` answer is kept in `events.jsonl`, so a failure can be placed
to the second after the app's own log has rolled over.

Configuration is environment variables, as `run.sh` sets them:

    ANTI_DEMO_APP_URL     the deployed app (required)
    ANTI_DEMO_PROFILE     a Databricks CLI profile that can reach it (required)
    EVIDENCE_DIR          where to write (default: release-bar-evidence/chaos-<UTC>)
    CHAOS_ROUNDS          comma-separated round ids (default: all six)
    CHAOS_SCENARIOS       comma-separated waves (default: the release bar's six)
    CHAOS_COMPETITOR      aurora_serverless_v2 (default) or rds_postgres
    CHAOS_LINGER_SECONDS  how long `finish-linger` watches a result (default 300)
    CHAOS_DELAYS          "scenario:round_id=seconds" towel-timing overrides
    CHAOS_SKIP            "scenario:round_id" pairs to leave out
    CHAOS_YIELD_ROUNDS    rounds another actor may use during the run
    CHAOS_PAUSE_FILE      while this file exists, no new wave starts
    CHAOS_QUIET=1         events to the file only, not stdout

Exit status: 0 when every scenario passed with no isolation failure, 1 when any
failed, 2 when the run could not start or stopped early.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import ALL_ROUNDS, AppClient  # noqa: E402

ROUNDS = tuple(
    item for item in os.environ.get("CHAOS_ROUNDS", ",".join(ALL_ROUNDS)).split(",") if item
)
YIELD_ROUNDS = set(item for item in os.environ.get("CHAOS_YIELD_ROUNDS", "").split(",") if item)
SCENARIOS = tuple(
    item
    for item in os.environ.get(
        "CHAOS_SCENARIOS",
        ("pre-bell-cancel,early-towel,mid-stage-towel,late-towel,rapid-rearm-towel,finish-linger"),
    ).split(",")
    if item
)
DELAYS = {
    "early-towel": dict.fromkeys(ROUNDS, 0.30),
    "mid-stage-towel": {
        "wake_idle_app": 1.50,
        "make_schema_change_safely": 5.0,
        "recover_deleted_order": 2.0,
        "put_model_score_in_app": 2.5,
        "survive_connection_spike": 16.0,
        "analyze_live_orders_without_slowing_checkout": 2.5,
    },
    "late-towel": {
        "wake_idle_app": 3.0,
        "make_schema_change_safely": 75.0,
        "recover_deleted_order": 75.0,
        "put_model_score_in_app": 7.0,
        "survive_connection_spike": 75.0,
        "analyze_live_orders_without_slowing_checkout": 6.0,
    },
    "rapid-rearm-towel": dict.fromkeys(ROUNDS, 0.35),
}
#: "scenario:round_id=seconds" overrides, e.g. to sweep one round's towel timing.
for _item in os.environ.get("CHAOS_DELAYS", "").split(","):
    if _item:
        _key, _seconds = _item.split("=")
        _scenario, _round = _key.split(":")
        DELAYS.setdefault(_scenario, {})[_round] = float(_seconds)
TERMINAL = {"verified", "towelled", "failed"}
#: "scenario:round_id" pairs to leave out, e.g. a known failure on the build under test.
SKIP = set(item for item in os.environ.get("CHAOS_SKIP", "").split(",") if item)


def now() -> str:
    return datetime.now(UTC).isoformat()


def wave_rounds(scenario: str, rounds: tuple[str, ...], skip: set[str]) -> tuple[str, ...]:
    """The rounds one wave drives: all named, less skips, and Round 1 alone before the bell.

    Only Round 1 can cancel a fight card that is still checking, so a solo run of
    any other round has no `pre-bell-cancel` scenario rather than a Round 1 one.
    """
    return tuple(
        round_id
        for round_id in rounds
        if f"{scenario}:{round_id}" not in skip
        and (scenario != "pre-bell-cancel" or round_id == "wake_idle_app")
    )


class Harness:
    def __init__(self, evidence: Path) -> None:
        self.evidence = evidence
        self.evidence.mkdir(parents=True, exist_ok=False)
        self.raw = self.evidence / "events.jsonl"
        self.lock = threading.Lock()
        self.client = AppClient()
        # Mint the first token now, so a bad profile fails before any bout starts.
        self.client.headers()
        self.results: list[dict[str, Any]] = []
        self.isolation_failures: list[dict[str, Any]] = []
        self.owned_active: set[str] = set()
        self.released_at: dict[str, float] = {}
        self.stop_poll = threading.Event()
        self.poller: threading.Thread | None = None

    def log(self, kind: str, **payload: Any) -> None:
        event = {"at": now(), "kind": kind, **payload}
        line = json.dumps(event, sort_keys=True, default=str)
        with self.lock:
            with self.raw.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        if os.environ.get("CHAOS_QUIET") != "1":
            print(line, flush=True)

    def request(
        self, method: str, path: str, *, body: dict[str, Any] | None = None, timeout: float = 300
    ) -> tuple[int, Any]:
        # The client re-mints its OAuth token every few minutes: the first all-rounds
        # run died after an hour on the token it minted at start.
        with self.lock:
            self.client.headers()
        status_code, payload = self.client.call(method, path, body, timeout=timeout)
        logged_payload = payload
        if "/api/sessions/" in path and isinstance(payload, dict):
            logged_payload = {
                "id": payload.get("id"),
                "state": payload.get("state"),
                "failure": payload.get("failure"),
                "lanes": {
                    key: {
                        "state": value.get("state"),
                        "phase": (value.get("activity") or {}).get("phase"),
                        "elapsed_ms": value.get("elapsed_ms"),
                        "error": value.get("error"),
                    }
                    for key, value in (payload.get("lanes") or {}).items()
                },
                "round5_start": payload.get("round5_start"),
            }
        self.log(
            "http",
            method=method,
            path=path,
            status=status_code,
            payload=logged_payload,
        )
        return status_code, payload

    def all_status(self) -> dict[str, Any]:
        issued_at = time.monotonic()
        status, payload = self.request("GET", "/api/bout/all", timeout=60)
        if status != 200:
            raise RuntimeError(f"/api/bout/all returned {status}")
        payload["_issued_at"] = issued_at
        return payload

    def check_isolation(self, payload: dict[str, Any], context: str) -> None:
        rounds = payload.get("rounds", {})
        issued_at = payload.get("_issued_at", float("inf"))
        with self.lock:
            owned = set(self.owned_active)
            released = dict(self.released_at)
        for round_id in ALL_ROUNDS:
            # A poll sent before this round was released can land after it, and
            # still carries the cleanup state that preceded READY. That is request
            # reordering, not a flicker (05:02:25 Round 4, pass 3).
            if released.get(round_id, float("-inf")) > issued_at:
                continue
            item = rounds.get(round_id, {})
            state = item.get("state")
            can_start = item.get("can_start")
            acceptable_active = round_id in owned and state in {
                "bout_in_progress",
                "cleanup_in_progress",
                "temporarily_unavailable",
            }
            yielded_active = round_id in YIELD_ROUNDS and state in {
                "bout_in_progress",
                "cleanup_in_progress",
                "temporarily_unavailable",
            }
            if not (acceptable_active or yielded_active) and (
                state != "ready" or can_start is not True
            ):
                failure = {
                    "at": now(),
                    "context": context,
                    "round": round_id,
                    "state": state,
                    "can_start": can_start,
                    "owned_active": sorted(owned),
                    "detail": item.get("detail"),
                }
                with self.lock:
                    self.isolation_failures.append(failure)
                self.log("isolation_failure", **failure)

    def poll_loop(self) -> None:
        while not self.stop_poll.wait(2):
            try:
                payload = self.all_status()
                self.check_isolation(payload, "continuous")
            except Exception as exc:
                self.log("poll_error", error=type(exc).__name__, detail=str(exc))

    def start_poller(self) -> None:
        self.poller = threading.Thread(target=self.poll_loop, daemon=True)
        self.poller.start()

    def stop_poller(self) -> None:
        self.stop_poll.set()
        if self.poller:
            self.poller.join(timeout=10)

    def create(self, round_id: str) -> str:
        status, payload = self.request(
            "POST",
            "/api/sessions",
            body={
                "competitor": os.environ.get("CHAOS_COMPETITOR", "aurora_serverless_v2"),
                "primary_persona": "sre",
                "corners": ["performance", "simplicity"],
                "round_id": round_id,
            },
        )
        if status != 201:
            raise RuntimeError(f"{round_id} create returned {status}: {payload}")
        return str(payload["id"])

    def post(self, session_id: str, control: str, timeout: float = 300) -> dict[str, Any]:
        status, payload = self.request(
            "POST", f"/api/sessions/{session_id}/{control}", timeout=timeout
        )
        if status != 200:
            raise RuntimeError(f"{control} returned {status}: {payload}")
        return payload

    def towel_or_finished(self, session_id: str) -> tuple[dict[str, Any], bool]:
        """Throw the towel, or accept that the bout already finished -- as the UI does.

        A towel that loses the race with the finish is refused with a 409 ("The
        bout must be running"), which is the server being right: there is nothing
        left to stop. Round 1 against RDS times Lakebase alone and ends in seconds,
        so a mid-bout towel there routinely arrives after the finish.
        """

        status, payload = self.request("POST", f"/api/sessions/{session_id}/towel", timeout=600)
        if status == 200:
            return payload, False
        if status == 409:
            latest = self.get_session(session_id)
            if latest.get("state") in TERMINAL:
                return latest, True
        raise RuntimeError(f"towel returned {status}: {payload}")

    def get_session(self, session_id: str) -> dict[str, Any]:
        status, payload = self.request("GET", f"/api/sessions/{session_id}", timeout=60)
        if status != 200:
            raise RuntimeError(f"session poll returned {status}")
        return payload

    def wait_session(
        self, session_id: str, wanted: set[str], timeout: float, round_id: str
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        latest: dict[str, Any] = {}
        while time.monotonic() < deadline:
            latest = self.get_session(session_id)
            if latest.get("state") in wanted:
                return latest
            time.sleep(1)
        raise TimeoutError(
            f"{round_id} session did not reach {sorted(wanted)}: {latest.get('state')}"
        )

    def wait_round_ready(self, round_id: str, timeout: float = 1200) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        latest: dict[str, Any] = {}
        while time.monotonic() < deadline:
            payload = self.all_status()
            latest = payload.get("rounds", {}).get(round_id, {})
            self.check_isolation(payload, f"wait-ready:{round_id}")
            if latest.get("state") == "ready" and latest.get("can_start") is True:
                with self.lock:
                    self.owned_active.discard(round_id)
                    self.released_at[round_id] = time.monotonic()
                return latest
            time.sleep(2)
        raise TimeoutError(f"{round_id} did not return ready: {latest}")

    @staticmethod
    def _result_key(snapshot: dict[str, Any]) -> str:
        """What the result screen is drawn from, minus the cleanup that runs under it."""

        return json.dumps(
            {
                "state": snapshot.get("state"),
                "failure": snapshot.get("failure"),
                "remembered_result": snapshot.get("remembered_result"),
                "towel": snapshot.get("towel"),
                "lanes": {
                    lane_id: {
                        "state": lane.get("state"),
                        "elapsed_ms": lane.get("elapsed_ms"),
                        "error": lane.get("error"),
                    }
                    for lane_id, lane in (snapshot.get("lanes") or {}).items()
                },
            },
            sort_keys=True,
            default=str,
        )

    def linger(self, session_id: str, round_id: str, verified: dict[str, Any]) -> dict[str, Any]:
        seconds = float(os.environ.get("CHAOS_LINGER_SECONDS", "300"))
        (self.evidence / f"linger-{round_id}-start.json").write_text(
            json.dumps(verified, indent=2, sort_keys=True), encoding="utf-8"
        )
        started = time.monotonic()
        base = self._result_key(verified)
        changes: list[dict[str, Any]] = []
        cards: list[dict[str, Any]] = []
        cooldowns: list[dict[str, Any]] = []
        cooldown_failures: list[dict[str, Any]] = []
        errors = 0
        last_card: tuple[Any, ...] | None = None
        last_cooldown: tuple[Any, ...] | None = None
        latest = verified
        while time.monotonic() - started < seconds:
            time.sleep(10)
            after = round(time.monotonic() - started, 1)
            try:
                latest = self.get_session(session_id)
                card = self.all_status()["rounds"][round_id]
            except Exception as exc:  # recorded, never fatal: the point is to keep watching
                errors += 1
                self.log(
                    "linger_poll_error",
                    round=round_id,
                    error=type(exc).__name__,
                    detail=str(exc),
                )
                continue
            key = self._result_key(latest)
            if key != base:
                changes.append({"after_s": after, "now": json.loads(key)})
                self.log(
                    "linger_result_changed", round=round_id, after_s=after, now=json.loads(key)
                )
                base = key
            card_key = (card.get("state"), card.get("active_phase"), card.get("can_start"))
            if card_key != last_card:
                cards.append(
                    {
                        "after_s": after,
                        "state": card_key[0],
                        "phase": card_key[1],
                        "can_start": card_key[2],
                        "detail": card.get("detail"),
                    }
                )
                last_card = card_key
            cooldown = latest.get("cooldown") or {}
            cooldown_key = (cooldown.get("state"), cooldown.get("failure"))
            if cooldown_key != last_cooldown:
                cooldowns.append(
                    {"after_s": after, "state": cooldown_key[0], "failure": cooldown_key[1]}
                )
                last_cooldown = cooldown_key
            if cooldown.get("state") == "failed" or cooldown.get("failure"):
                cooldown_failures.append({"after_s": after, "failure": cooldown.get("failure")})
        (self.evidence / f"linger-{round_id}-end.json").write_text(
            json.dumps(latest, indent=2, sort_keys=True), encoding="utf-8"
        )
        return {
            "seconds": seconds,
            "result_changes": changes,
            "card_states": cards,
            "cooldown_states": cooldowns,
            "cooldown_failures": cooldown_failures,
            "poll_errors": errors,
        }

    @staticmethod
    def summary(snapshot: dict[str, Any]) -> dict[str, Any]:
        lanes = snapshot.get("lanes", {})
        return {
            "state": snapshot.get("state"),
            "failure": snapshot.get("failure"),
            "lanes": {
                lane_id: {
                    "state": lane.get("state"),
                    "activity": lane.get("activity"),
                    "metrics": lane.get("metrics"),
                }
                for lane_id, lane in lanes.items()
            },
            "round5_setup": snapshot.get("round5_setup"),
            "round5_runtime": snapshot.get("round5_runtime"),
            "integration_contest": snapshot.get("integration_contest"),
        }

    def run_one(self, round_id: str, scenario: str) -> dict[str, Any]:
        started = time.monotonic()
        result: dict[str, Any] = {
            "round": round_id,
            "scenario": scenario,
            "started_at": now(),
            "passed": False,
        }
        try:
            pre = self.all_status()["rounds"][round_id]
            if pre.get("state") != "ready" or pre.get("can_start") is not True:
                raise RuntimeError(f"yield: round was not ready before ownership: {pre}")
            with self.lock:
                self.owned_active.add(round_id)
            session_id = self.create(round_id)
            result["session_id"] = session_id
            if scenario == "pre-bell-cancel":
                cancel_won = threading.Event()
                final_lock = threading.Lock()
                final: dict[str, Any] | None = None

                def cancel_until_won() -> None:
                    nonlocal final
                    deadline = time.monotonic() + 10
                    while not cancel_won.is_set() and time.monotonic() < deadline:
                        status, payload = self.request(
                            "POST",
                            f"/api/sessions/{session_id}/cancel-arm",
                            timeout=600,
                        )
                        if status == 200:
                            with final_lock:
                                if final is None:
                                    final = payload
                            cancel_won.set()
                            return
                        time.sleep(0.01)

                # Round 1's local check can finish faster than a GET-then-cancel
                # round trip. Keep several direct cancel requests in flight while
                # ARM establishes CHECKING so one can acquire the record lock in
                # the supported cancellation window.
                with concurrent.futures.ThreadPoolExecutor(max_workers=9) as arm_pool:
                    cancel_futures = [arm_pool.submit(cancel_until_won) for _ in range(8)]
                    arm_future = arm_pool.submit(self.post, session_id, "arm", 600)
                    arm_future.result()
                    for future in cancel_futures:
                        future.result()
                if final is None:
                    snapshot = self.get_session(session_id)
                    if snapshot.get("state") == "armed":
                        raise RuntimeError("pre-bell checking window was missed")
                    raise TimeoutError("pre-bell cancel did not reach checking window")
                ready = self.wait_round_ready(round_id)
                result.update(
                    passed=(
                        final.get("state") == "failed"
                        and str(final.get("failure") or "").startswith("Fight-card check cancelled")
                    ),
                    terminal=self.summary(final),
                    ready=ready,
                )
                result["elapsed_seconds"] = round(time.monotonic() - started, 2)
                result["finished_at"] = now()
                with self.lock:
                    self.results.append(result)
                    (self.evidence / "results.partial.json").write_text(
                        json.dumps(self.results, indent=2, sort_keys=True),
                        encoding="utf-8",
                    )
                self.log("scenario_result", **result)
                return result
            self.post(session_id, "arm", timeout=600)
            armed = self.wait_session(session_id, {"armed", "failed"}, 600, round_id)
            if armed.get("state") != "armed":
                raise RuntimeError(f"arm failed: {armed.get('failure')}")
            self.post(session_id, "run", timeout=600)
            running = self.wait_session(
                session_id,
                {"running", "verified", "failed", "towelled"},
                600,
                round_id,
            )
            delay = DELAYS.get(scenario, {}).get(round_id)
            if delay is not None and running.get("state") == "running":
                time.sleep(delay)
                final, raced = self.towel_or_finished(session_id)
                if raced:
                    result["towel_raced_finish"] = True
                    final = self.wait_session(session_id, TERMINAL, 1500, round_id)
            elif delay is not None:
                final = running
            else:
                final = self.wait_session(session_id, TERMINAL, 1500, round_id)
                if scenario == "finish-linger" and final.get("state") == "verified":
                    # The presenter lets the bout finish and leaves the result up
                    # without touching anything. Nothing on screen may change.
                    result["linger"] = self.linger(session_id, round_id, final)
                    final = self.get_session(session_id)
                # Mirror the UI (App.tsx redoAfterProof -> startReset): only ask for
                # the reset when the verified session has not started (or has failed)
                # its own. Round 5 cleans up by itself and has no reset control.
                cooldown = final.get("cooldown") or {}
                if (
                    final.get("state") == "verified"
                    and round_id != "survive_connection_spike"
                    and (not cooldown or cooldown.get("state") == "failed")
                ):
                    status, payload = self.request(
                        "POST", f"/api/sessions/{session_id}/cooldown", timeout=600
                    )
                    result["cooldown_post"] = status
                    if status not in (200, 409):
                        raise RuntimeError(f"cooldown returned {status}: {payload}")
            ready = self.wait_round_ready(round_id)
            linger = result.get("linger")
            result.update(
                passed=final.get("state") in {"verified", "towelled"}
                and (
                    scenario != "finish-linger"
                    or (
                        linger is not None
                        and not linger["result_changes"]
                        and not linger["cooldown_failures"]
                        and linger["poll_errors"] == 0
                    )
                ),
                terminal=self.summary(final),
                ready=ready,
            )
            if scenario == "rapid-rearm-towel":
                # Claim again immediately after this round's own cleanup reached
                # READY. This is the re-arm stress; other rounds remain live.
                with self.lock:
                    self.owned_active.add(round_id)
                rearm_id = self.create(round_id)
                result["rearm_session_id"] = rearm_id
                self.post(rearm_id, "arm", timeout=600)
                rearmed = self.wait_session(rearm_id, {"armed", "failed"}, 600, round_id)
                if rearmed.get("state") != "armed":
                    raise RuntimeError(f"rapid re-arm failed: {rearmed.get('failure')}")
                self.post(rearm_id, "run", timeout=600)
                rearm_running = self.wait_session(
                    rearm_id,
                    {"running", "verified", "failed", "towelled"},
                    600,
                    round_id,
                )
                if rearm_running.get("state") == "running":
                    time.sleep(0.35)
                    rearm_final, raced = self.towel_or_finished(rearm_id)
                    if raced:
                        result["rearm_towel_raced_finish"] = True
                        rearm_final = self.wait_session(rearm_id, TERMINAL, 1500, round_id)
                else:
                    rearm_final = rearm_running
                rearm_ready = self.wait_round_ready(round_id)
                result["rapid_rearm_terminal"] = self.summary(rearm_final)
                result["rapid_rearm_ready"] = rearm_ready
                result["passed"] = result["passed"] and rearm_final.get("state") in {
                    "towelled",
                    "verified",
                }
        except Exception as exc:
            # passed may already be True from the first bout of rapid-rearm-towel;
            # an error anywhere in the scenario is a failure (15:32Z Round 4 re-arm).
            result.update(passed=False, error=type(exc).__name__, detail=str(exc))
            self.log(
                "scenario_error",
                round=round_id,
                scenario=scenario,
                error=result["error"],
                detail=result["detail"],
            )
            try:
                session_id = result.get("rearm_session_id") or result.get("session_id")
                if session_id:
                    snapshot = self.get_session(session_id)
                    if snapshot.get("state") == "checking" and round_id == "wake_idle_app":
                        self.post(session_id, "cancel-arm", timeout=600)
                    elif snapshot.get("state") in {"armed", "checking"}:
                        self.post(session_id, "run", timeout=600)
                        self.wait_session(
                            session_id,
                            {"running", "verified", "failed", "towelled"},
                            600,
                            round_id,
                        )
                        current = self.get_session(session_id)
                        if current.get("state") == "running":
                            self.post(session_id, "towel", timeout=600)
                    elif snapshot.get("state") == "running":
                        self.post(session_id, "towel", timeout=600)
                    self.wait_round_ready(round_id, timeout=1200)
            except Exception as recovery:
                result["recovery_error"] = f"{type(recovery).__name__}: {recovery}"
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        result["finished_at"] = now()
        with self.lock:
            self.results.append(result)
            (self.evidence / "results.partial.json").write_text(
                json.dumps(self.results, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        self.log("scenario_result", **result)
        return result

    def run(self) -> int:
        initial = self.all_status()
        (self.evidence / "initial-bout-all.json").write_text(
            json.dumps(initial, indent=2, sort_keys=True), encoding="utf-8"
        )
        if any(
            initial["rounds"][round_id].get("state") != "ready"
            or initial["rounds"][round_id].get("can_start") is not True
            for round_id in ROUNDS
        ):
            raise RuntimeError("initial live ring was not fully ready; yielding")
        self.start_poller()
        try:
            for scenario in SCENARIOS:
                # A PAUSE file holds the next wave (not the one in flight) until removed.
                pauses = [self.evidence / "PAUSE"]
                if os.environ.get("CHAOS_PAUSE_FILE"):
                    pauses.append(Path(os.environ["CHAOS_PAUSE_FILE"]))
                while any(pause.exists() for pause in pauses):
                    time.sleep(5)
                rounds = wave_rounds(scenario, ROUNDS, SKIP)
                if not rounds:
                    continue
                self.log("wave_start", scenario=scenario)
                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                    futures = {
                        pool.submit(self.run_one, round_id, scenario): round_id
                        for round_id in rounds
                    }
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                self.log("wave_complete", scenario=scenario)
        finally:
            self.stop_poller()
        final = self.all_status()
        (self.evidence / "final-bout-all.json").write_text(
            json.dumps(final, indent=2, sort_keys=True), encoding="utf-8"
        )
        (self.evidence / "results.json").write_text(
            json.dumps(self.results, indent=2, sort_keys=True), encoding="utf-8"
        )
        (self.evidence / "isolation-failures.json").write_text(
            json.dumps(self.isolation_failures, indent=2, sort_keys=True), encoding="utf-8"
        )
        passed = all(item.get("passed") for item in self.results)
        return 0 if passed and not self.isolation_failures else 1


def main() -> int:
    if {"-h", "--help"} & set(sys.argv[1:]):
        print(__doc__)
        return 0
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    evidence = Path(os.environ.get("EVIDENCE_DIR") or f"release-bar-evidence/chaos-{stamp}")
    print(f"EVIDENCE_DIR={evidence}", flush=True)
    harness = Harness(evidence)
    try:
        return harness.run()
    except Exception as exc:
        harness.log("fatal", error=type(exc).__name__, detail=str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
