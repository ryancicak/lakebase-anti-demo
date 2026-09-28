"""Restart the app under live bouts, and require every round to heal by itself.

    restart.py EVIDENCE_DIR [--after-bell SECS] [--competitor ID] [ROUND_ID ...]

Starts one bout per named round (default: Rounds 2, 3 and 5, the three holding
per-bout AWS resources mid-race), waits SECS after the last bell (default 90),
then writes READY_FOR_RESTART into EVIDENCE_DIR and waits for the app to restart.
The restart is `run.sh`'s job: it redeploys the same build with `bootstrap.sh
--deploy-only`, which fails its Databricks identity check when launched from a
Python subprocess. If that deploy fails, `run.sh` writes DEPLOY_FAILED instead.

The restart has happened once the old sessions are gone (they live in the old
process). From then on the board is polled until all six rounds are READY again,
for up to 45 minutes. Round 5 is the slow one: it inherits the old bout's claim,
waits out the old coordinator lease, then waits for AWS to finish deleting.

Writes `events.jsonl` (every card change, with seconds since the restart) and
`summary.json` (each round's time back to READY). Needs `ANTI_DEMO_APP_URL` and
`ANTI_DEMO_PROFILE`. Exit 0 when every round healed, 1 when one did not in time,
2 when the test could not start or the redeploy failed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import AppClient, all_ready  # noqa: E402

DEFAULT_ROUNDS = ("make_schema_change_safely", "recover_deleted_order", "survive_connection_spike")
RESTART_TIMEOUT_SECONDS = 30 * 60
HEAL_TIMEOUT_SECONDS = 45 * 60


def now() -> str:
    return datetime.now(UTC).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("evidence", type=Path)
    parser.add_argument("rounds", nargs="*", default=list(DEFAULT_ROUNDS))
    parser.add_argument("--after-bell", type=float, default=90.0, metavar="SECS")
    parser.add_argument("--competitor", default="aurora_serverless_v2")
    args = parser.parse_args()
    evidence: Path = args.evidence
    evidence.mkdir(parents=True, exist_ok=False)
    client = AppClient()

    def log(kind: str, **payload: Any) -> None:
        line = json.dumps({"at": now(), "kind": kind, **payload}, default=str)
        with (evidence / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        print(line[:300], flush=True)

    def get(path: str) -> tuple[int | None, Any]:
        """A GET that reports an app mid-restart as no answer instead of raising."""
        try:
            return client.call("GET", path, timeout=60)
        except Exception as error:  # noqa: BLE001
            return None, {"error": type(error).__name__}

    status, board = get("/api/bout/all")
    if status != 200 or not all_ready((board or {}).get("rounds") or {}):
        log("refused", reason="not every round was READY", status=status)
        return 2

    sessions: dict[str, str] = {}
    for round_id in args.rounds:
        status, created = client.call(
            "POST",
            "/api/sessions",
            {
                "competitor": args.competitor,
                "primary_persona": "sre",
                "corners": ["performance", "simplicity"],
                "round_id": round_id,
            },
        )
        if status != 201:
            log("create_failed", round=round_id, status=status, body=created)
            return 2
        sessions[round_id] = created["id"]
        client.call("POST", f"/api/sessions/{created['id']}/arm", timeout=600)
    for round_id, session_id in sessions.items():
        snapshot: Any = {}
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            _, snapshot = get(f"/api/sessions/{session_id}")
            if (snapshot or {}).get("state") in {"armed", "failed"}:
                break
            time.sleep(1)
        state = (snapshot or {}).get("state")
        log("armed", round=round_id, session=session_id, state=state)
        if state != "armed":
            # An armed card nobody rings expires by itself, so nothing is left behind.
            log("arm_failed", round=round_id, failure=(snapshot or {}).get("failure"))
            return 2
    for round_id, session_id in sessions.items():
        status, snapshot = client.call("POST", f"/api/sessions/{session_id}/run", timeout=600)
        log("bell", round=round_id, status=status, state=(snapshot or {}).get("state"))
    time.sleep(args.after_bell)
    for round_id, session_id in sessions.items():
        _, snapshot = get(f"/api/sessions/{session_id}")
        lanes = {
            lane_id: (lane or {}).get("state")
            for lane_id, lane in ((snapshot or {}).get("lanes") or {}).items()
        }
        log("before_restart", round=round_id, state=(snapshot or {}).get("state"), lanes=lanes)

    log("restart_begin")
    (evidence / "READY_FOR_RESTART").write_text(now(), encoding="utf-8")
    probe = next(iter(sessions.values()))
    deadline = time.monotonic() + RESTART_TIMEOUT_SECONDS
    while True:
        if (evidence / "DEPLOY_FAILED").exists():
            log("deploy_failed")
            return 2
        status, _ = get(f"/api/sessions/{probe}")
        if status == 404:
            break
        if time.monotonic() >= deadline:
            log("restart_not_observed", last_status=status)
            return 1
        time.sleep(5)
    log("restart_done", old_session_status=status)
    for round_id, session_id in sessions.items():
        status, snapshot = get(f"/api/sessions/{session_id}")
        log("old_session_after_restart", round=round_id, status=status)

    started = time.monotonic()
    last: dict[str, tuple[Any, ...]] = {}
    ready_after: dict[str, float] = {}
    while time.monotonic() - started < HEAL_TIMEOUT_SECONDS:
        status, board = get("/api/bout/all")
        cards = (board or {}).get("rounds") or {}
        after = round(time.monotonic() - started, 1)
        for round_id, card in cards.items():
            card = card or {}
            key = (status, card.get("state"), card.get("can_start"), card.get("active_phase"))
            if key != last.get(round_id):
                log(
                    "card",
                    round=round_id,
                    after_restart_s=after,
                    http=status,
                    state=key[1],
                    can_start=key[2],
                    phase=key[3],
                    detail=str(card.get("detail") or "")[:200],
                )
                last[round_id] = key
                if key[1] == "ready" and key[2] is True:
                    ready_after[round_id] = after
        if status == 200 and all_ready(cards):
            log("all_ready", after_restart_s=after)
            summary = {
                "healed": True,
                "rounds_in_play": list(sessions),
                "ready_after_restart_s": ready_after,
                "all_ready_after_restart_s": after,
            }
            (evidence / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            return 0
        time.sleep(10)
    log("timeout")
    summary = {"healed": False, "rounds_in_play": list(sessions), "last_cards": last}
    (evidence / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
