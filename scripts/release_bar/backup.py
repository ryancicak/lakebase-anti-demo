"""One round's full bout while its AWS source is mid-backup.

    backup.py CHECKOUT ROUND COMPETITOR OUT_DIR

For the minute or minutes of each daily automated backup, an RDS instance or an Aurora
cluster reads `backing-up`. On 2026-10-02 rc10 found Rounds 2 and 3 refusing a bout over it
and Round 5's ring turning unclaimable; Round 1's arm and Round 5's seal checks were as
strict. AWS picks those windows from a morning block in this region, so a bar run overnight
never meets one.

A manual snapshot puts a source in the same state on demand. This takes one of ROUND's
COMPETITOR source, waits for the source to read `backing-up`, and runs chaos.py's
`full-proof` bout of ROUND against COMPETITOR while it does. For Round 5 it also runs the
installer's seal check (`_round5_topology_check`, what `antidemo doctor` runs) inside the
same window. Then it deletes the snapshot, whatever happened.

It passes only when the bout passed, the seal check (Round 5) passed, and both started while
the source still read `backing-up`. A bout that missed the window proves nothing, so it fails.

Prints one summary line last and writes OUT_DIR/backup.json. Exit 0 on a pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import installation_run_id, use_checkout  # noqa: E402

ROUND_NUMBERS = {
    "wake_idle_app": 1,
    "make_schema_change_safely": 2,
    "recover_deleted_order": 3,
    "survive_connection_spike": 5,
}
COMPETITORS = {"aurora_serverless_v2": "aurora", "rds_postgres": "rds"}
BACKING_UP = "backing-up"
POLL_SECONDS = 2.0
#: How long a fresh snapshot may take to put its source into `backing-up`.
BACKING_UP_WAIT_SECONDS = 240.0
#: How long a source in some other state may take to read `available` or `backing-up`.
SOURCE_SETTLE_WAIT_SECONDS = 600.0
#: How long to wait for a snapshot to finish before deleting it, and for the delete.
SNAPSHOT_SETTLE_SECONDS = 1800.0
#: A bout or check counts as inside the window if the source read `backing-up` at a
#: sample no later than this many seconds after it started.
START_TOLERANCE_SECONDS = 5.0


def log(message: str) -> None:
    print(f"{datetime.now(UTC):%H:%M:%SZ} {message}", flush=True)


class Source:
    """ROUND's COMPETITOR source: an Aurora cluster or an RDS instance, by its own name."""

    def __init__(self, rds: Any, run_id: str, round_id: str, competitor: str) -> None:
        self.rds = rds
        self.kind = COMPETITORS[competitor]
        suffix = f"-r{ROUND_NUMBERS[round_id]}-{self.kind}"
        tagged = {"Key": "anti-demo-run-id", "Value": run_id}
        if self.kind == "aurora":
            operation, key, name = "describe_db_clusters", "DBClusters", "DBClusterIdentifier"
        else:
            operation, key, name = "describe_db_instances", "DBInstances", "DBInstanceIdentifier"
        found = [
            item[name]
            for page in rds.get_paginator(operation).paginate()
            for item in page[key]
            if item[name].endswith(suffix) and tagged in item.get("TagList", [])
        ]
        if len(found) != 1:
            raise SystemExit(f"expected one {suffix} source for {run_id}, found {found}")
        self.identifier = found[0]
        self.snapshot = (
            f"anti-demo-backup-r{ROUND_NUMBERS[round_id]}-{self.kind}-"
            f"{datetime.now(UTC):%Y%m%d%H%M%S}"
        )
        self.tags = [tagged, {"Key": "managed-by", "Value": "release-bar"}]
        self.taken = False

    def status(self) -> str:
        if self.kind == "aurora":
            item = self.rds.describe_db_clusters(DBClusterIdentifier=self.identifier)["DBClusters"]
            return str(item[0]["Status"]).lower()
        item = self.rds.describe_db_instances(DBInstanceIdentifier=self.identifier)["DBInstances"]
        return str(item[0]["DBInstanceStatus"]).lower()

    def take_snapshot(self) -> None:
        if self.kind == "aurora":
            self.rds.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=self.snapshot,
                DBClusterIdentifier=self.identifier,
                Tags=self.tags,
            )
        else:
            self.rds.create_db_snapshot(
                DBSnapshotIdentifier=self.snapshot,
                DBInstanceIdentifier=self.identifier,
                Tags=self.tags,
            )
        self.taken = True

    def snapshot_status(self) -> str | None:
        try:
            if self.kind == "aurora":
                items = self.rds.describe_db_cluster_snapshots(
                    DBClusterSnapshotIdentifier=self.snapshot
                )["DBClusterSnapshots"]
            else:
                items = self.rds.describe_db_snapshots(DBSnapshotIdentifier=self.snapshot)[
                    "DBSnapshots"
                ]
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {
                "DBClusterSnapshotNotFoundFault",
                "DBSnapshotNotFound",
                "DBSnapshotNotFoundFault",
            }:
                return None
            raise
        return str(items[0]["Status"]).lower() if items else None

    def delete_snapshot(self) -> None:
        """Wait for the snapshot to finish, delete it, and wait until it is gone."""

        if not self.taken:
            # The bout ran inside AWS's own backup; that snapshot is AWS's to keep.
            return
        deadline = time.monotonic() + SNAPSHOT_SETTLE_SECONDS
        while self.snapshot_status() not in {None, "available", "failed"}:
            if time.monotonic() >= deadline:
                raise SystemExit(f"snapshot {self.snapshot} never finished; delete it by hand")
            time.sleep(10)
        if self.snapshot_status() is not None:
            if self.kind == "aurora":
                self.rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=self.snapshot)
            else:
                self.rds.delete_db_snapshot(DBSnapshotIdentifier=self.snapshot)
        while self.snapshot_status() is not None:
            if time.monotonic() >= deadline:
                raise SystemExit(f"snapshot {self.snapshot} is still present; delete it by hand")
            time.sleep(10)
        log(f"snapshot {self.snapshot} deleted")


class StatusRecorder(threading.Thread):
    """The source's status every POLL_SECONDS, with the time it was read."""

    def __init__(self, source: Source) -> None:
        super().__init__(daemon=True)
        self.source = source
        self.samples: list[tuple[datetime, str]] = []
        self.stopped = threading.Event()

    def run(self) -> None:
        while not self.stopped.is_set():
            try:
                self.samples.append((datetime.now(UTC), self.source.status()))
            except Exception as error:  # noqa: BLE001 - one unreadable sample is not a result
                self.samples.append((datetime.now(UTC), f"unreadable: {error}"[:80]))
            self.stopped.wait(POLL_SECONDS)

    def status_at(self, moment: datetime) -> str | None:
        """The last status read no later than START_TOLERANCE_SECONDS after ``moment``."""

        latest = None
        for at, status in self.samples:
            if (at - moment).total_seconds() <= START_TOLERANCE_SECONDS:
                latest = status
        return latest


def seal_check(checkout: Path) -> tuple[bool, str]:
    """The installer's Round 5 seal check, against this installation, right now."""

    from server import lifecycle
    from server.manifest import load_manifest

    manifest = load_manifest()
    lifecycle.apply_manifest_environment(manifest)
    check = lifecycle._round5_topology_check(manifest)
    return bool(check.ok), str(check.detail)[:200]


def full_proof(round_id: str, competitor: str, evidence: Path) -> dict[str, Any] | None:
    environment = {
        **os.environ,
        "EVIDENCE_DIR": str(evidence),
        "CHAOS_QUIET": "1",
        "CHAOS_ROUNDS": round_id,
        "CHAOS_SCENARIOS": "full-proof",
        "CHAOS_COMPETITOR": competitor,
    }
    with (evidence.parent / "chaos.log").open("w", encoding="utf-8") as output:
        subprocess.run(
            [sys.executable, "-u", str(Path(__file__).with_name("chaos.py"))],
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            check=False,
        )
    for name in ("results.json", "results.partial.json"):
        path = evidence / name
        if path.exists():
            rows = json.loads(path.read_text(encoding="utf-8"))
            rows = rows if isinstance(rows, list) else rows.get("results", [])
            for row in rows:
                if row.get("round") == round_id:
                    return row
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("round_id", choices=sorted(ROUND_NUMBERS))
    parser.add_argument("competitor", choices=sorted(COMPETITORS))
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    checkout, round_id, competitor, out = args.checkout, args.round_id, args.competitor, args.out
    out.mkdir(parents=True, exist_ok=True)
    use_checkout(checkout)
    run_id = installation_run_id(checkout)
    if not run_id:
        raise SystemExit(f"no installation found in {checkout}")
    source = Source(boto3.client("rds"), run_id, round_id, competitor)
    log(f"{round_id} vs {competitor}: source {source.identifier} reads {source.status()}")
    report: dict[str, Any] = {
        "round": round_id,
        "competitor": competitor,
        "source": source.identifier,
        "snapshot": source.snapshot,
    }
    recorder = StatusRecorder(source)
    try:
        failures = exercise(checkout, round_id, competitor, out, source, recorder, report)
    finally:
        recorder.stopped.set()
        try:
            source.delete_snapshot()
        except SystemExit as error:
            report["cleanup"] = str(error)
    if "cleanup" in report:
        failures.append(f"cleanup: {report['cleanup']}")
    report["result"] = "passed" if not failures else "; ".join(failures)
    report["samples"] = [(at.isoformat(), status) for at, status in recorder.samples]
    (out / "backup.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    verdict = "PASSED" if not failures else "FAILED"
    print(f"{verdict} {round_id} vs {competitor} inside a backup: {report['result']}", flush=True)
    return 0 if not failures else 1


def begin_backup(source: Source, report: dict[str, Any]) -> str | None:
    """Put the source inside a backup, or say why it could not be.

    AWS's own daily backup counts when it is already running. That is the case this step
    exists for, and a manual snapshot would be refused then, because the source is not
    `available`: rc13's Round 3 Aurora pair began while the cluster still read `backing-up`
    from its automated snapshot (2026-10-03), and CreateDBClusterSnapshot failed the pair
    before any bout ran. Any other state is waited out until the source can be snapshotted.
    """

    deadline = time.monotonic() + SOURCE_SETTLE_WAIT_SECONDS
    while True:
        status = source.status()
        if status == BACKING_UP:
            report["backup"] = "automated, already running"
            report["snapshot"] = None
            log(f"source is already inside AWS's own backup ({BACKING_UP})")
            return None
        if status == "available":
            break
        if time.monotonic() >= deadline:
            return f"the source never read available or {BACKING_UP} (it read {status})"
        time.sleep(POLL_SECONDS)
    source.take_snapshot()
    report["backup"] = "manual snapshot"
    log(f"snapshot {source.snapshot} requested")
    deadline = time.monotonic() + BACKING_UP_WAIT_SECONDS
    while source.status() != BACKING_UP:
        if time.monotonic() >= deadline:
            return f"the snapshot never put the source into {BACKING_UP}"
        time.sleep(POLL_SECONDS)
    return None


def exercise(
    checkout: Path,
    round_id: str,
    competitor: str,
    out: Path,
    source: Source,
    recorder: StatusRecorder,
    report: dict[str, Any],
) -> list[str]:
    """Put the source inside a backup, then run the bout (and Round 5's seal check) inside it."""

    failure = begin_backup(source, report)
    if failure:
        return [failure]
    log(f"source reads {BACKING_UP}; starting the bout")
    recorder.start()
    failures: list[str] = []
    if round_id == "survive_connection_spike":
        checked_at = datetime.now(UTC)
        ok, detail = seal_check(checkout)
        seal = {"ok": ok, "detail": detail, "at": checked_at.isoformat()}
        seal["source"] = recorder.status_at(checked_at)
        report["seal_check"] = seal
        log(f"Round 5 seal check: ok={ok} ({detail})")
        if not ok:
            failures.append(f"the Round 5 seal check refused: {detail}")
        if seal["source"] != BACKING_UP:
            failures.append("the Round 5 seal check ran after the backup")
    row = full_proof(round_id, competitor, out / "chaos")
    recorder.stopped.set()
    recorder.join(timeout=10)
    if row is None:
        return [*failures, "the bout left no result"]
    started = datetime.fromisoformat(str(row["started_at"]))
    bout = {
        "passed": bool(row.get("passed")),
        "started_at": row["started_at"],
        "source_at_start": recorder.status_at(started),
        "detail": str(row.get("detail") or "")[:300],
    }
    report["bout"] = bout
    if not bout["passed"]:
        failures.append(f"the bout failed: {bout['detail'] or 'see chaos.log'}")
    if bout["source_at_start"] != BACKING_UP:
        failures.append(
            f"the bout started after the backup (source read {bout['source_at_start']}), "
            "so it proves nothing"
        )
    return failures


if __name__ == "__main__":
    sys.exit(main())
