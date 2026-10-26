"""One round's full bout while its Azure source takes a backup: the Azure edition's `backup` step.

    azure_backup.py CHECKOUT ROUND OUT_DIR

The AWS edition's `backup.py` puts a source into `backing-up` with a manual snapshot, because rc10
found Rounds 2 and 3 refusing a bout over that state (2026-10-02). Azure takes its daily backup when
it chooses, so a run overnight may never meet one either. An on-demand backup is the same thing on
purpose.

This asks Azure for an on-demand backup of ROUND's source server (``PUT .../backups/{name}``, sent
once: a second could start a second backup), reads the source's state and the backup's operation
every 2 s, and runs chaos.py's `full-proof` bout of ROUND against Azure while the backup runs. Then
it deletes whatever backup of that name exists, whatever happened before.

The bell is the moment that counts: it's when the source is checked again and Azure's restore is
requested. So it passes only when the bout passed and its bell rang while the backup was still in
progress. A bout that passed after the backup had ended proves nothing about one, so that is
inconclusive, not a pass. The source's states during the backup are in OUT_DIR/backup.json: a state
other than Ready is what rc10 met on AWS.

Azure takes no on-demand backup of a Burstable server (2026-10-20: it accepts the request, then
fails it ``CustomerOnDemandBackupCannotBePerformedOnBurstableServer``), and Rounds 2 and 3's
sources are B2s. On those this step is inconclusive, saying so, and only a bigger source can run it.
Round 5's is one: a General Purpose D2ds_v5, whose PgBouncer the bell switches on, as AWS's backs
up Round 5's RDS and Aurora too.

Prints one summary line last. Exit 0 on a pass, 1 when the bout failed or the backup could not be
deleted, 2 when Azure would not take the backup or the bout missed it.
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

ROUND_SERVERS = {"make_schema_change_safely": "r2", "recover_deleted_order": "r3",
                 "survive_connection_spike": "r5"}
POLL_SECONDS = 2.0
#: How long the source may take to read Ready before the backup is asked for.
SOURCE_SETTLE_WAIT_SECONDS = 600.0
#: How long a backup, and then its delete, may take.
BACKUP_SETTLE_SECONDS = 1800.0
#: The bell counts as inside the backup if a sample no later than this after it read the backup
#: in progress.
BELL_TOLERANCE_SECONDS = 5.0
IN_PROGRESS = "InProgress"
ENDED = frozenset({"Succeeded", "Failed", "Canceled"})


def log(message: str) -> None:
    print(f"{datetime.now(UTC):%H:%M:%SZ} {message}", flush=True)


class Recorder(threading.Thread):
    """The source's state and the backup operation's status every POLL_SECONDS."""

    def __init__(self, arm: Any, server: str, operation_url: str) -> None:
        super().__init__(daemon=True)
        self.arm = arm
        self.server = server
        self.operation_url = operation_url
        self.samples: list[tuple[datetime, str, str]] = []
        self.ended = threading.Event()
        self.stopped = threading.Event()
        #: Azure's own error for a backup operation that ended Failed, "code: message".
        self.error: str | None = None

    def run(self) -> None:
        while not self.stopped.is_set():
            at = datetime.now(UTC)
            try:
                found = self.arm.get_server(self.server) or {}
                state = str((found.get("properties") or {}).get("state") or "absent")
            except Exception as error:  # noqa: BLE001 - one unreadable sample is not a result
                state = f"unreadable: {type(error).__name__}"
            operation = "ended"
            if not self.ended.is_set():
                try:
                    polled = self.arm.request("GET", self.operation_url)
                    body = polled.body if isinstance(polled.body, dict) else {}
                    operation = str(body.get("status") or f"HTTP {polled.status}")
                    failure = body.get("error") if isinstance(body.get("error"), dict) else None
                    if operation == "Failed" and failure:
                        self.error = f"{failure.get('code')}: {str(failure.get('message'))[:200]}"
                except Exception as error:  # noqa: BLE001
                    operation = f"unreadable: {type(error).__name__}"
                if operation in ENDED:
                    self.ended.set()
            self.samples.append((at, state, operation))
            self.stopped.wait(POLL_SECONDS)

    def operation_at(self, moment: datetime) -> str | None:
        """The backup's last status read no later than BELL_TOLERANCE_SECONDS after `moment`."""

        latest = None
        for at, _, operation in self.samples:
            if (at - moment).total_seconds() <= BELL_TOLERANCE_SECONDS:
                latest = operation
        return latest


def full_proof(round_id: str, evidence: Path) -> dict[str, Any] | None:
    environment = {
        **os.environ,
        "EVIDENCE_DIR": str(evidence),
        "CHAOS_QUIET": "1",
        "CHAOS_ROUNDS": round_id,
        "CHAOS_SCENARIOS": "full-proof",
        "CHAOS_COMPETITOR": "azure_postgres",
    }
    with (evidence.parent / "chaos.log").open("w", encoding="utf-8") as output:
        subprocess.run([sys.executable, "-u", str(Path(__file__).with_name("chaos.py"))],
                       env=environment, stdout=output, stderr=subprocess.STDOUT, check=False)
    for name in ("results.json", "results.partial.json"):
        path = evidence / name
        if path.exists():
            rows = json.loads(path.read_text(encoding="utf-8"))
            rows = rows if isinstance(rows, list) else rows.get("results", [])
            for row in rows:
                if row.get("round") == round_id:
                    return row
    return None


def bell_at(evidence: Path, session_id: str) -> datetime | None:
    """When chaos.py rang the bout's bell: its POST to the session's `run`."""

    path = evidence / "events.jsonl"
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        event = json.loads(line) if line.strip() else {}
        if (event.get("kind") == "http" and event.get("method") == "POST"
                and str(event.get("path") or "") == f"/api/sessions/{session_id}/run"):
            return datetime.fromisoformat(str(event["at"]))
    return None


def delete_backup(arm: Any, url: str, recorder: Recorder | None) -> str:
    """Delete the backup of this name if one exists, and wait until it reads 404.

    Waits first for a backup still being taken (Azure may refuse to delete it), then sends the
    delete again while Azure refuses it, within BACKUP_SETTLE_SECONDS. Returns what it did.
    """

    deadline = time.monotonic() + BACKUP_SETTLE_SECONDS
    while recorder is not None and recorder.is_alive() and not recorder.ended.is_set():
        if time.monotonic() >= deadline:
            raise SystemExit("the backup never finished; delete it by hand")
        time.sleep(POLL_SECONDS)
    # A resource Azure accepted moments ago can read 404 for a while (a new server did for over
    # a minute), so one 404 doesn't prove there is nothing to delete.
    for attempt in range(4):
        if arm.request("GET", url).status != 404:
            break
        if attempt < 3:
            time.sleep(10)
    else:
        return "no backup to delete"
    while True:
        response = arm.request("DELETE", url)
        if response.status in (204, 404):
            break
        if response.status in (200, 202):
            status, _ = arm.wait_operation(response, bound_seconds=BACKUP_SETTLE_SECONDS)
            if status == "Succeeded":
                break
        if time.monotonic() >= deadline:
            raise SystemExit(f"the backup's delete was refused ({response.status}); "
                             "delete it by hand")
        time.sleep(10)
    while arm.request("GET", url).status != 404:
        if time.monotonic() >= deadline:
            raise SystemExit("the backup still reads present; delete it by hand")
        time.sleep(10)
    return "backup deleted"


def take_backup(arm: Any, server: str, url: str, report: dict[str, Any]) -> str:
    """Ask for the backup once the source reads Ready. Returns its operation's URL, or raises
    LookupError with why Azure would not take one."""

    deadline = time.monotonic() + SOURCE_SETTLE_WAIT_SECONDS
    while str(((arm.get_server(server) or {}).get("properties") or {}).get("state")) != "Ready":
        if time.monotonic() >= deadline:
            raise LookupError("the source never read Ready")
        time.sleep(POLL_SECONDS)
    accepted = arm.request("PUT", url, {}, idempotent=False)
    if accepted.status not in (200, 201, 202):
        body = accepted.body if isinstance(accepted.body, dict) else {}
        error = body.get("error") if isinstance(body.get("error"), dict) else {}
        raise LookupError(f"Azure refused the backup: {error.get('code') or accepted.status}: "
                          f"{str(error.get('message') or '')[:200]}")
    operation = accepted.headers.get("Azure-AsyncOperation") or accepted.headers.get(
        "azure-asyncoperation") or ""
    if not operation:
        raise LookupError(f"Azure accepted the backup ({accepted.status}) with no operation "
                          "to follow")
    log(f"backup {report['backup']} of {server} accepted ({accepted.status})")
    return operation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("round_id", choices=sorted(ROUND_SERVERS))
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    checkout, round_id, out = args.checkout.resolve(), args.round_id, args.out
    out.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(checkout))
    from server.azure_arm import POSTGRES_API, ArmClient, AzureCredentials
    from server.azure_identity import az_cli_token_source
    from server.manifest import load_manifest

    manifest = load_manifest(checkout / ".anti-demo-azure" / "manifest.json")
    azure = manifest.azure
    if azure is None:
        print("not an Azure installation's manifest")
        return 2
    arm = ArmClient(AzureCredentials(tenant_id=azure.tenant_id,
                                     subscription_id=azure.subscription_id),
                    azure.resource_group, token_source=az_cli_token_source(azure.tenant_id))
    server = azure.servers[ROUND_SERVERS[round_id]].server
    name = f"release-bar-{datetime.now(UTC):%Y%m%dt%H%M%S}"
    url = f"{arm.server_path(server)}/backups/{name}?api-version={POSTGRES_API}"
    report: dict[str, Any] = {"round": round_id, "source": server, "backup": name}
    failures: list[str] = []
    inconclusive: list[str] = []
    recorder: Recorder | None = None
    try:
        try:
            operation = take_backup(arm, server, url, report)
        except LookupError as refusal:
            inconclusive.append(str(refusal))
        else:
            recorder = Recorder(arm, server, operation)
            recorder.start()
            row = full_proof(round_id, out / "chaos")
            if row is None:
                failures.append("the bout left no result")
            else:
                bell = bell_at(out / "chaos", str(row.get("session_id") or ""))
                report["bout"] = {
                    "passed": bool(row.get("passed")),
                    "started_at": row["started_at"],
                    "bell_at": bell.isoformat() if bell else None,
                    "backup_at_bell": recorder.operation_at(bell) if bell else None,
                }
                if not row.get("passed"):
                    failures.append("the bout failed")
                elif recorder.error:
                    # 2026-10-20: CustomerOnDemandBackupCannotBePerformedOnBurstableServer, for
                    # Rounds 2 and 3's B2s sources. Azure accepts the request, then fails it.
                    inconclusive.append(f"Azure accepted the backup, then failed it: "
                                        f"{recorder.error}")
                elif report["bout"]["backup_at_bell"] != IN_PROGRESS:
                    inconclusive.append("the backup had ended before the bell, so the bout "
                                        "did not race inside it")
    except Exception as error:  # noqa: BLE001 - the backup is still deleted below
        failures.append(f"{type(error).__name__}: {str(error)[:200]}")
    finally:
        try:
            report["cleanup"] = delete_backup(arm, url, recorder)
            log(report["cleanup"])
        except (SystemExit, Exception) as error:  # noqa: BLE001 - reported, never lost
            failures.append(f"cleanup: {error}")
        if recorder is not None:
            recorder.stopped.set()
            recorder.join(timeout=10)
            report["samples"] = [(at.isoformat(), state, op) for at, state, op in recorder.samples]
            report["source_states"] = sorted({state for _, state, _ in recorder.samples})
            report["backup_states"] = sorted({op for _, _, op in recorder.samples})
    report["result"] = "; ".join(failures or inconclusive) or "passed"
    (out / "backup.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    verdict = "FAILED" if failures else "INCONCLUSIVE" if inconclusive else "PASSED"
    print(f"{verdict} {round_id} inside an on-demand backup (source states "
          f"{report.get('source_states')}): {report['result']}", flush=True)
    return 1 if failures else 2 if inconclusive else 0


if __name__ == "__main__":
    raise SystemExit(main())
