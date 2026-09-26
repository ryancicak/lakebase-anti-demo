"""The browser must judge a Round 5 lane by the same warm-baseline ceiling as the server.

The frontend re-derives every lane's contract from the serialized evidence
(`frontend/src/round5.ts`, `roundFiveLaneResult`). Its copy of the preexisting-session
ceiling drifted to zero while the runner and server accepted a settled warm pool, so a
verified Lakebase win rendered as NO DECLARED WINNER, with sharing blocked, whenever the
pooler was still warm from the previous bout. The same one-line guard as
`test_fanin_digest_mirrors`: the cheapest check on a constant changed on one side only.
"""

from __future__ import annotations

import re
from pathlib import Path

from runner import round5_fanin as runner_fanin
from server import connection_fanin

ROUND5_TS = Path(__file__).resolve().parents[1] / "frontend" / "src" / "round5.ts"


def _frontend_int(name: str) -> int:
    match = re.search(rf"export const {name} = (\d[\d_]*)\b", ROUND5_TS.read_text())
    assert match is not None, f"{name} is not declared in {ROUND5_TS}"
    return int(match.group(1).replace("_", ""))


def test_the_frontend_warm_baseline_ceiling_matches_the_server_and_runner() -> None:
    frontend = _frontend_int("ROUND_FIVE_MAX_PREEXISTING_CLIENT_SESSIONS")
    assert frontend == connection_fanin.MAX_PREEXISTING_CLIENT_SESSIONS
    assert frontend == runner_fanin.MAX_PREEXISTING_CLIENT_SESSIONS


def test_the_frontend_retry_budget_matches_the_server_and_runner() -> None:
    """A lane the server verified with one retried login must not read as a loss."""

    frontend = _frontend_int("ROUND_FIVE_MAX_RETRIES")
    assert frontend == connection_fanin.MAX_RETRIES
    assert frontend == runner_fanin.MAX_RETRIES
    shard_budgets = runner_fanin.PARTITION_RETRY_BUDGET * runner_fanin.WORKER_COUNT
    assert shard_budgets <= runner_fanin.MAX_RETRIES
