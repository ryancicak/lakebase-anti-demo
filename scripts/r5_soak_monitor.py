"""Round 5 soak watcher: poll the deployed /readyz and scan for provider residue.

Read-only. Mints an M2M token (via r5_probe) and records the Round 5 warm/ring
truth every INTERVAL seconds to an NDJSON log, and every AWS_EVERY polls also
scans RDS for orphan per-bout Proxies (a bout leaves none once cleanup
converges). Flags: warm state != ready, terminal block, cleanup owed, orphan
Proxy, or a generation/revision churn faster than a healthy renew cadence
(rewarm storm). This does NOT start a bout and never mutates anything.

Usage:
  ANTI_DEMO_PROFILE=<profile> python scripts/r5_soak_monitor.py \
      --interval 60 --aws-every 10 --log /path/to/r5_soak.ndjson [--once]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import r5_probe  # noqa: E402

# The per-bout Proxy/SG names all share this sealed prefix; nothing with this
# prefix should exist while no bout is running.
PROXY_NAME_PREFIX = os.environ.get("ANTI_DEMO_R5_PROXY_PREFIX", "anti-demo-r5")
REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-west-2"))

WARM_FIELDS = (
    "status",
    "round5_warm_state",
    "round5_warm_generation",
    "round5_warm_revision",
    "round5_ring_ready",
    "round5_warm_last_error_code",
    "round5_warm_blocked_terminal",
    "round5_warm_attempt_count",
    "round5_warm_next_retry_at",
    "round5_cleanup_owed",
    "round5_cleanup_owed_detail",
    "round5_recovery_state",
    "degraded",
)


def _readyz() -> dict[str, object]:
    try:
        status, body = r5_probe.probe("/readyz")
    except Exception as exc:  # noqa: BLE001
        return {"probe_error": f"{type(exc).__name__}: {exc}"}
    try:
        payload = json.loads(body)
    except ValueError:
        return {"probe_http": status, "probe_error": "non_json_body"}
    row = {k: payload.get(k) for k in WARM_FIELDS}
    row["probe_http"] = status
    return row


def _orphan_proxies() -> dict[str, object]:
    try:
        import boto3

        rds = boto3.client("rds", region_name=REGION)
        names: list[str] = []
        paginator = rds.get_paginator("describe_db_proxies")
        for page in paginator.paginate():
            for proxy in page.get("DBProxies", []):
                name = str(proxy.get("DBProxyName", ""))
                if name.startswith(PROXY_NAME_PREFIX):
                    names.append(name)
        return {"orphan_proxy_count": len(names), "orphan_proxies": names}
    except Exception as exc:  # noqa: BLE001
        return {"orphan_proxy_error": f"{type(exc).__name__}: {exc}"}


def _flags(row: dict[str, object]) -> list[str]:
    flags: list[str] = []
    if row.get("probe_error"):
        flags.append(f"PROBE_ERROR:{row['probe_error']}")
    state = row.get("round5_warm_state")
    if state not in (None, "ready"):
        flags.append(f"WARM_STATE={state}")
    if row.get("round5_warm_blocked_terminal"):
        flags.append("BLOCKED_TERMINAL")
    if row.get("round5_warm_last_error_code"):
        flags.append(f"ERR={row['round5_warm_last_error_code']}")
    if row.get("round5_cleanup_owed"):
        flags.append("CLEANUP_OWED")
    if row.get("round5_ring_ready") is False:
        flags.append("RING_NOT_READY")
    if row.get("degraded"):
        flags.append("DEGRADED")
    if int(row.get("orphan_proxy_count") or 0) > 0:
        flags.append(f"ORPHAN_PROXY={row.get('orphan_proxies')}")
    return flags


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=60.0)
    ap.add_argument("--aws-every", type=int, default=10, help="AWS proxy scan every N polls")
    ap.add_argument("--log", default=os.environ.get("ANTI_DEMO_R5_SOAK_LOG", "r5_soak.ndjson"))
    ap.add_argument("--once", action="store_true", help="one poll then exit")
    args = ap.parse_args()

    print(f"r5_soak_monitor: interval={args.interval}s aws_every={args.aws_every} log={args.log}")
    poll = 0
    last_gen = None
    churn_window: list[float] = []
    while True:
        poll += 1
        now = datetime.now(UTC).isoformat()
        row: dict[str, object] = {"ts": now, "poll": poll}
        row.update(_readyz())
        if args.aws_every > 0 and (poll == 1 or poll % args.aws_every == 0):
            row.update(_orphan_proxies())
        # rewarm-storm heuristic: track generation changes per 10 min
        gen = row.get("round5_warm_generation")
        if gen is not None and gen != last_gen:
            churn_window.append(time.monotonic())
            last_gen = gen
        churn_window = [t for t in churn_window if time.monotonic() - t < 600]
        if len(churn_window) >= 5:
            row["rewarm_storm"] = len(churn_window)
        flags = _flags(row)
        row["flags"] = flags
        line = json.dumps(row, separators=(",", ":"))
        with open(args.log, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        marker = "  !! " + " ".join(flags) if flags else "  ok"
        print(
            f"{now} poll#{poll} state={row.get('round5_warm_state')} "
            f"gen={row.get('round5_warm_generation')} rev={row.get('round5_warm_revision')} "
            f"err={row.get('round5_warm_last_error_code')} "
            f"cleanup_owed={row.get('round5_cleanup_owed')} "
            f"proxies={row.get('orphan_proxy_count', '-')}{marker}",
            flush=True,
        )
        if args.once:
            return 1 if flags else 0
        time.sleep(max(1.0, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
