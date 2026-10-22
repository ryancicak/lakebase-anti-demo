"""Round 4's Azure lane: the installation's standing ADF data flow, cold at the bell.

The Azure counterpart of `server/round4_race_live.GlueWriterLane`, behind the same `Lane` verbs of
`server/round4_race.py`, so the engine, the bell, the verifier and the evidence rule are the AWS
edition's. What differs is what Phase 0 measured (2026-10-18/19):

* **The bell is one ``createRun`` naming a version.** A data flow is a batch, not a stream. It
  reads one Delta version (``versionAsOf``, the flow's ``version`` parameter), the one just after
  the version Prepare verified, as the Glue job's change feed starts there. Nothing is looked up
  first, so the lane's clock holds ADF and none of the lane's own bookkeeping.
* **Settling reads the head.** The engine tells a settling lane no version ("start from the
  snapshot"), and a batch can't follow a feed, so the lane asks the source for its head version
  (the restore commit), off the clock, and reads that.
* **Parking is "cancel what is in flight and see it end"** (`AdfDataFlowJob.park`). ADF has no run
  slot to release afterwards, unlike Glue.
* **Any ended run is the end of the lane's attempt, Succeeded included.** A batch that finished
  without the row will never write it, so waiting out the bound would score Azure as censored
  (a lower-bound win) for a run that already said it was done. The engine still reads once more
  after a failure, so a row that landed before the run ended is verified as usual.
* **Settling can't reuse the bout's run.** A run reads one pinned version, so one still running
  for the bout can never carry the restore committed after it. It's parked, and a run at the head
  takes its place.
  - The engine that builds this lane must give settle a bound sized for an ADF cold run (Phase 0:
    up to 408 s, against the Glue-era 420 s), so 900 s.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from .azure_adf_job import AdfDataFlowJob
from .model_score import DeltaCommit
from .round4_race import COMPETITOR_LANE, Bell, Notify, Reader

LOGGER = logging.getLogger(__name__)

#: ADF's run states that end a run. Succeeded is one: a batch that ended has written all it will.
ENDED_STATES = frozenset({"Succeeded", "Failed", "Cancelled"})
#: How long a settling lane would wait for an ADF cold run: Phase 0's slowest was 407.5 s.
ADF_SETTLE_TIMEOUT_SECONDS = 900.0
_TABLE_ID = re.compile(r"[A-Za-z0-9-]{1,64}")


async def _quiet(_message: str) -> None:
    return None


class VersionedSource(Protocol):
    async def head_version(self) -> int: ...


#: The ledger row the guarded upsert keeps for an entity: delta_table_id, delta_commit_version,
#: deleted, applied_at. None when the target holds no row for it.
LedgerReader = Callable[[str], Awaitable[Mapping[str, Any] | None]]


class AdfWriterLane:
    """Azure's lane: the sealed ADF pipeline that moves Round 4's Delta row into Azure Postgres."""

    lane_id = COMPETITOR_LANE
    label = "Azure Data Factory → Azure Database for PostgreSQL"

    def __init__(
        self,
        job: AdfDataFlowJob,
        source: VersionedSource,
        open_reader: Callable[[], Awaitable[Reader]],
        ledger: LedgerReader,
        *,
        entity_id: str,
    ) -> None:
        self._job = job
        self._source = source
        self._open_reader = open_reader
        self._ledger = ledger
        self._entity_id = entity_id
        self._run_id = ""
        self._version: int | None = None

    def _threaded_notify(self, notify: Notify) -> Callable[[str], None]:
        loop = asyncio.get_running_loop()

        def say(message: str) -> None:
            asyncio.run_coroutine_threadsafe(notify(message), loop)

        return say

    async def confirm_parked(self, notify: Notify) -> None:
        say = self._threaded_notify(notify)
        await asyncio.to_thread(lambda: self._job.park(say))

    async def park(self, notify: Notify) -> None:
        say = self._threaded_notify(notify)
        await asyncio.to_thread(lambda: self._job.park(say))

    async def open_reader(self) -> Reader:
        return await self._open_reader()

    async def before_bell(self) -> None:
        """Nothing: the version a run reads rides on its own ``createRun``."""

    async def start(self, bell: Bell) -> None:
        """One ``createRun`` naming the version to read and the Delta table it belongs to.

        The table ID rides on every run, as Glue's ``--source_table_id`` does, so a source
        re-created since the last run wins the guard instead of looking like an older version. It
        reaches the flow as a quoted string, so only a UUID's characters are accepted.
        """

        if not _TABLE_ID.fullmatch(bell.source_table_id or ""):
            raise ValueError("Round 4's source table ID is not a Delta table ID")
        if bell.source_version is None:
            version = await self._source.head_version()  # settling: no clock is running
        else:
            version = bell.source_version + 1
        parameters = {"version": version, "table_id": bell.source_table_id}
        run_id = await asyncio.to_thread(lambda: self._job.start(parameters))
        self._run_id, self._version = run_id, version

    async def ensure_running(self, bell: Bell) -> bool:
        """Have a run that can carry the source's head (settling only). Always returns True.

        Our own run already reading the head is kept. Anything else in flight read an earlier
        version, so it's parked and a run at the head started. Either way it's a cold run, which
        is the expected wait, so the engine gives it the whole bound and no restart kick.
        """

        active = {run.run_id for run in await asyncio.to_thread(self._job.active_runs)}
        head = await self._source.head_version()
        if self._run_id in active and self._version is not None and self._version >= head:
            return True
        if active:
            await self.park(_quiet)
        await self.start(bell)
        return True

    async def failure(self) -> str | None:
        run_id = self._run_id
        if not run_id:
            return None
        run = await asyncio.to_thread(lambda: self._job.run(run_id))
        if run.state not in ENDED_STATES:
            return None
        if run.state == "Succeeded":
            return "The Data Factory run ended without delivering the row"
        # The card shows this sentence. ADF's own message can name a linked service, a server
        # or a storage account, so it goes to the log alone.
        LOGGER.warning("Round 4's ADF run %s ended %s: %s", run_id, run.state,
                       run.message or "no error message")
        return f"The Data Factory run ended {run.state}"

    async def evidence(self, commit: DeltaCommit, bell: Bell) -> Mapping[str, Any]:
        """The ledger's record of the bout's row, and when ADF says its run started and ended."""

        evidence: dict[str, Any] = {"adf_run": self._run_id, "competitor": "azure_postgres",
                                    "adf_version": self._version}
        row = await self._ledger(self._entity_id)
        if row is not None:
            version = int(row["delta_commit_version"])
            applied = row.get("applied_at")
            evidence.update({
                "ledger_delta_table_id": str(row["delta_table_id"]),
                "ledger_delta_commit_version": version,
                "ledger_deleted": bool(row.get("deleted")),
                "ledger_applied_at": applied.isoformat() if applied is not None else None,
                "ledger_version_covers_commit": version >= commit.version,
            })
        run_id = self._run_id
        if run_id:
            run = await asyncio.to_thread(lambda: self._job.run(run_id))
            if run.started_on is not None:
                evidence["adf_run_started_on"] = run.started_on.isoformat()
            if run.completed_on is not None:
                evidence["adf_run_completed_on"] = run.completed_on.isoformat()
        return evidence
