"""Every round's state on a deployed app, and a wait for all six to be READY.

    ready.py              print each round's card; exit 0 when all six are READY
    ready.py --wait SECS  poll every 10 s, for up to SECS, until all six are READY
    ready.py --expect-commit SHA
                          first require the app to be serving commit SHA, with no
                          uncommitted changes, as `/api/version` reports it

Needs `ANTI_DEMO_APP_URL` and `ANTI_DEMO_PROFILE`. Read-only: it asks for the
board and starts nothing. Exit status 1 means some round was not READY in time,
2 that the board could not be read at all, 3 that the app serves another build.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import ALL_ROUNDS, AppClient, all_ready  # noqa: E402


def describe(rounds: dict[str, dict[str, object]]) -> str:
    return "\n".join(
        f"  {round_id}: state={(rounds.get(round_id) or {}).get('state')} "
        f"phase={(rounds.get(round_id) or {}).get('active_phase')} "
        f"can_start={(rounds.get(round_id) or {}).get('can_start')}"
        for round_id in ALL_ROUNDS
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--wait", type=float, default=0.0, metavar="SECS")
    parser.add_argument("--expect-commit", metavar="SHA")
    args = parser.parse_args()
    client = AppClient()
    try:
        # A profile that cannot sign in will not start working by waiting.
        client.headers()
    except Exception as error:  # noqa: BLE001
        print(f"cannot sign in as {client.profile}: {error}")
        return 2
    if args.expect_commit:
        try:
            _, build = client.call("GET", "/api/version", timeout=60)
        except Exception as error:  # noqa: BLE001
            print(f"the app could not be reached: {type(error).__name__}: {error}")
            return 2
        served = str((build or {}).get("commit") or "")
        if not served or not args.expect_commit.startswith(served) or build.get("dirty"):
            print(
                f"the app serves {served or 'an unstamped build'}"
                f"{' with uncommitted changes' if build.get('dirty') else ''}, "
                f"not {args.expect_commit[:12]}"
            )
            return 3
        print(f"the app serves {served}, clean")
    deadline = time.monotonic() + args.wait
    rounds: dict[str, dict[str, object]] = {}
    while True:
        try:
            rounds = client.rounds()
        except Exception as error:  # noqa: BLE001 - an app mid-restart answers nothing useful
            if time.monotonic() >= deadline:
                print(f"the board could not be read: {type(error).__name__}: {error}")
                return 2
        else:
            if all_ready(rounds):
                print(describe(rounds))
                return 0
            if time.monotonic() >= deadline:
                print(describe(rounds))
                return 1
        time.sleep(10)


if __name__ == "__main__":
    sys.exit(main())
