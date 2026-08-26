"""Stop and start the Round 4 Managed Sync pipeline between sessions.

The pipeline is created with ``continuous: true`` in its spec, and that field
governs *how it syncs while it is up* -- streaming rather than on a schedule --
not whether it stays up. The two are worth keeping apart, because this module
exists precisely to take it down between sessions: it ran uninterrupted from
2026-08-20T15:05Z until there was a way to stop it, and has been stopped and
resumed repeatedly since. Left up it bills the better part of $15 a day for a
round that runs for about twenty minutes, which makes it the largest single
standing line in the installation and the only one that costs real money while
nobody is watching -- but that rate is what it costs *while running*, and a
stopped pipeline bills nothing. A day is the longest horizon this rate is quoted
over anywhere here, deliberately: it is up for the minutes of a bout, so a
figure projected across a month would price a scenario this installation does
not produce, in the largest number in the repository. The
rate itself is not written down anywhere in this docstring on purpose --
:data:`PIPELINE_USD_PER_DAY` derives it, and the note there records why a prose
copy of it is the one thing this module must not carry.

The dangerous way to make that cost go away is to change the synced table's
``scheduling_policy``. That field is a *create-time* input: moving it recreates
the synced table, which mints a new ``synced_table_uid`` and a new
``pipeline_id``, which in turn demotes the manifest to v2 and destroys Rounds 5
and 6. Paying for a day of idle pipeline is enormously cheaper than that, so this
module cannot reach the synced table at all: every request it issues is checked
against the sealed pipeline's own ``/api/2.0/pipelines`` prefix before it is
sent, and there is no code path here that writes the manifest.

What is deliberate is recorded locally rather than inferred from the cloud. A
pipeline that was stopped on purpose, a pipeline that is still coming back up,
and a pipeline that fell over all three stop being ``RUNNING``, and an operator
who cannot tell those apart will either panic at a healthy installation or ignore
a broken one. The ledger file this module writes is what makes ``doctor`` able to
say which happened.

The seat alone does not save the money, and that is the harder half of the
problem. Stopping between sessions takes that daily figure to roughly what the
bouts themselves consume, but only if somebody remembers a command at the moment
a demo ends --- which is exactly when they are least likely to.
:func:`session_notice` was the answer to that, and it puts the bill in front of
the operator every time a session starts, priced and accrued, alongside the one
command that ends it.

**That answer holds for a laptop and does not hold for the deployed app, which
is where the money is actually spent.** ``session_notice`` reasons from "every
session passes through ``antidemo serve``", which is true of a checkout and false
of a Databricks App: its one caller is ``_pipeline_session_notice`` in
``server/cli.py``, reached only from ``_serve``, and the deployed app never
executes ``cli.py``. So the mitigation has never once printed where the pipeline
is billing. Under the owner's standing posture --- one deploy, then nobody inputs
anything, ever --- that leaves the cost with no bound on it at all, and D20a's
asymmetry inverts: "the operator must not be able to lose money by forgetting"
becomes "money is lost by default, permanently".

That reachability was re-verified from source rather than inherited, and it
holds: ``app.yaml`` runs ``python -m uvicorn app:app``, so no surface in this
module reaches a deployed operator through the CLI. The half of the gap that
*is* now closed is the loudest one --- an owed stop left behind by a process
that did not come back. :func:`owed_stop_notice` puts it on ``/readyz``, which
is the only surface a deployed operator reaches without inputting anything;
:func:`session_notice`'s other three directions remain laptop-only, because
they describe a session that is about to start and a deployed app has no such
moment.

So two things were added under a narrow amendment to D9a and D20a, and the
narrowness is the point. The app may start and stop **this one sealed pipeline**,
at arm and after settlement, and nothing else:

* :func:`workspace_api`, so the app can issue the two calls at all. Everything
  here already went through an injected ``api`` callable; the CLI wires that to a
  subprocess against the ``databricks`` binary, which does not exist in the Apps
  runtime.
* :class:`DurablePipelinePowerStore`, so a stop the app made is recorded
  somewhere that survives a restart. The file marker below cannot serve that
  purpose in the app, because :func:`stop_marker_path` resolves through
  ``manifest_path()``, which raises there.

The automatic-stop reasoning D20a rejected is **not** what is built here, and the
distinction matters. D20a rejected a *timer or shutdown hook that guesses* the
session is over. The stop added here fires only after Round 4's own settlement
has finished and the engine has been idle since, and the state it produces is one
arm now recovers from by itself in about twenty seconds --- so the failure mode
D20a was protecting against, a round that cannot arm because a mechanism guessed
wrong, no longer follows from guessing wrong.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from .coordination import (
    COORDINATION_SCHEMA,
    CoordinationObjectsMissingError,
    read_coordination_objects,
)
from .manifest import DemoManifest, manifest_path

logger = logging.getLogger(__name__)

#: The pipeline's metered rate: DBU per hour, from posted platform usage.
#:
#: **The meter is held here rather than a price, because the price is a product
#: and the product is what drifted.** Three figures for this one pipeline were
#: recorded independently -- ``$14.18/day`` in this module, ``$14.80/day`` in
#: `OPEN-FINDINGS.md` under D15a and D20, and ``$14.8012/day`` captured into
#: `frontend/src/standing-cost.fixture.json` -- and they disagree by about 4%,
#: which was read as two of them being wrong.
#:
#: **None of them is wrong, and none is a transcription error.** Divided back
#: through the same DBU price they are 1.3130, 1.3492 and 1.3705 DBU/h, and all
#: three sit inside the 1.31--1.37 DBU/h range D20 records this meter ranging over
#: across sampling windows. One meter, three windows, and the spread between them
#: is the meter's own variance rather than a disagreement to be resolved by
#: picking a winner or by averaging three figures into false precision.
#:
#: So this is the widest-sampled of the three -- D20's 301 usage intervals from
#: 2026-08-20T15:00:00Z, over the 53.4 hours before that bout -- and the other two
#: are where :data:`PIPELINE_DBU_PER_HOUR_RANGE` ends rather than rivals to it.
PIPELINE_DBU_PER_HOUR = Decimal("1.349239")

#: What one of those DBUs costs, from ``system.billing.list_prices``. The same
#: price `server.standing_cost` multiplies its `ROUND4_PIPELINE_LABEL` component
#: by, so the panel's figure and this one are one derivation rather than two.
PIPELINE_USD_PER_DBU = Decimal("0.45")

#: The lowest and highest this meter has been observed at across **complete**
#: sampling windows. Kept beside the point estimate because a point estimate on
#: its own is what invites the false precision that produced three figures: every
#: line this module prints carries :data:`PIPELINE_RATE_TOLERANCE`, which is
#: derived from this, so a figure cannot be repeated without its uncertainty.
#:
#: **One higher reading is deliberately excluded, and saying so is the point.**
#: The 2026-08-20 cost analysis projected this pipeline at 1.411313 DBU/h -- some
#: 3% above the top of this range -- from **nine hours** of posted usage, and that
#: document sets the figure aside itself: "the day is incomplete and is
#: deliberately not mixed into any figure above". Stated as a meter rather than as
#: dollars, because a dollar figure for it here would be the sixth copy of exactly
#: the thing this constant exists to stop. Every complete-window reading since has
#: landed inside the
#: range above, including the priced runs this installation recorded on 2026-08-21
#: and 2026-08-22. Excluding a partial-window projection is a judgement rather
#: than arithmetic, so it is recorded here rather than made silently; widen this
#: range and every figure and tolerance derived from it widens with it.
PIPELINE_DBU_PER_HOUR_RANGE = (Decimal("1.31"), Decimal("1.37"))

_HOURS_PER_DAY = Decimal(24)

#: What the pipeline costs while it is running, in USD per day.
#:
#: **Derived, and never written down.** A literal here is precisely the defect
#: this module was cleaned up for: `tests/test_pipeline_power.py` refuses any
#: dollars-per-day literal anywhere under `server/` that falls inside the band
#: below, so a future copy of this figure fails a test rather than drifting for a
#: week. The arithmetic is deliberately the same one
#: `standing_cost._priced_platform` performs -- DBU/hour times USD/DBU -- which is
#: why the two cannot diverge.
#:
#: Still a constant rather than priced live, for the original reason: `doctor` has
#: to put a number in front of an operator without a posted-usage payload in hand,
#: and a check that silently omits the cost when the payload is missing is exactly
#: the silence this is meant to end.
PIPELINE_USD_PER_DAY = PIPELINE_DBU_PER_HOUR * PIPELINE_USD_PER_DBU * _HOURS_PER_DAY

#: The same band, in dollars per day, low then high.
PIPELINE_USD_PER_DAY_RANGE = tuple(
    dbu * PIPELINE_USD_PER_DBU * _HOURS_PER_DAY for dbu in PIPELINE_DBU_PER_HOUR_RANGE
)

#: How much of the figure above is measurement rather than price, as an operator
#: reads it. Derived from the band, so widening the band cannot leave a narrower
#: claim behind -- which is exactly how "63%" and "56%" outlived their
#: denominators. Every operator-facing line carries this, because the figure a
#: stranger reads is the figure a stranger repeats.
PIPELINE_RATE_TOLERANCE = "±{:.0f}%".format(
    (PIPELINE_USD_PER_DAY_RANGE[1] - PIPELINE_USD_PER_DAY_RANGE[0])
    / 2
    / PIPELINE_USD_PER_DAY
    * 100
)

_SECONDS_PER_DAY = Decimal(86400)

#: Roughly how long ``start()`` takes to bring the pipeline back to ``RUNNING``.
#:
#: **One measurement now stands behind this, where previously none did.** The
#: constant used to document itself as measured and was not; D20's "a cold
#: serverless pipeline walks ``DEPLOYING → … → RUNNING`` over minutes" was the
#: only figure on the record, and it was the pessimistic one.
#:
#: Measured 2026-08-24 against the live sealed pipeline, twice, through exactly
#: the call :func:`start` issues. Stopped for 4m38s it reached
#: ``SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE`` --- fully healthy, which is what
#: Round 4 needs --- in **19.2 s**; stopped for 90 s, in **25.1 s**. D20's "over
#: minutes" describes provisioning a pipeline that does not exist yet; resuming
#: one that does is a different and much cheaper thing.
#:
#: **Still presented as an estimate, and kept at 30 rather than lowered to 25.**
#: Two samples on one installation are a scale, not a distribution, and the error
#: that costs something is the optimistic one: an operator told 19 s who waits 40
#: concludes the start failed. Every caller must still admit a cold start can
#: take longer and point at `antidemo pipeline status` for what is actually
#: observed.
#:
#: For the figure an operator actually feels, the whole cold arm was **55.0 s**
#: against **24.3 s** warm --- the resume plus the unchanged baseline proof that
#: follows it.
RESTART_SECONDS_ESTIMATE = 30

#: How long :func:`wait_until_running` will wait for a resumed pipeline before it
#: gives up and says so.
#:
#: **Sized as an envelope around the measurement above, not as a prediction of
#: it.** Nineteen seconds observed, five minutes allowed: the gap is deliberate,
#: because this bound is what an audience-facing arm blocks on and the two ways
#: to get it wrong are not symmetric. Too short turns a slow-but-healthy resume
#: into a round that refuses at the bell --- the exact failure D20a rejected
#: automation to avoid. Too long costs an operator some staring at a progress
#: line they can read, on a path that has not started timing anything yet.
#:
#: It has to expire at all because a start can genuinely fail, and an arm that
#: waits forever on a pipeline that is never coming back is worse than one that
#: says so.
START_WAIT_TIMEOUT_SECONDS = 300.0

#: How long a resumed pipeline is given to reach ``RUNNING`` before a check
#: treats it as a failure rather than as a resume still in progress.
#:
#: This exists because ``start()`` used to delete the stop marker outright, which
#: opened a window where the pipeline was not yet ``RUNNING`` and nothing
#: recorded that anyone had asked it to be: `doctor` went red at an installation
#: that was doing exactly what it had been told. A check that cries wolf during a
#: normal resume is one an operator learns to skip, and this module's only
#: defence against a forgotten stop is being believed.
#:
#: **Deliberately not derived from RESTART_SECONDS_ESTIMATE above.** A grace
#: period sized to an estimate inherits that estimate's error in the one
#: direction that matters --- printing a failure at an installation that is
#: merely slow. Ten minutes is an envelope around the only evidence this project
#: has, D20's "over minutes", rather than a prediction. It has to expire
#: eventually: a start that silently failed must stop reading as a resume in
#: progress, and after this it does.
RESUME_GRACE_SECONDS = 600

#: The local record of what was last deliberately asked of the pipeline. Named
#: for the stop because that is the state worth recording loudest, but it carries
#: a resume too --- see ``_INTENT_RESUMING``.
STOP_MARKER_FILENAME = "round4-pipeline-stopped.json"

_INTENT_STOPPED = "stopped"
_INTENT_RESUMING = "resuming"

#: A stop this installation owes the pipeline but has not yet performed.
#:
#: Written when a settled Round 4 bout *schedules* its delayed release, and
#: superseded by the ``stopped`` record the release itself writes. It exists for
#: the one failure the release cannot survive: the process holding the timer
#: going away. A graceful shutdown performs the owed stop, but a SIGKILL, an OOM
#: or a container eviction runs no handler at all, and before this record the
#: only trace of the owed stop was an in-memory task that died with the process
#: -- so a pipeline billing $14.57/day read afterwards as one nobody had ever
#: intended to stop.
#:
#: **It is not in effect until it is overdue**, which is what stops it crying
#: wolf. See :func:`read_stop_marker`.
_INTENT_STOP_OWED = "stop_owed"

#: Which marker fields each intent's timestamp and actor live in, in one place.
#:
#: **Shared because writing it twice already lost a due time.**
#: ``DurablePipelinePowerStore.append`` chose the timestamp with
#: ``stopped_at or resumed_at or owed_at`` while ``latest`` mapped the column
#: back by intent, and an owed stop carries a ``resumed_at`` forward on purpose
#: so the accrued figure keeps its origin -- so the resume's timestamp won the
#: first-non-empty race, and the record came back out of the coordination table
#: claiming to be due twenty minutes before it actually was.
#:
#: That is the noisy direction, and the one the whole due gate exists to close:
#: a deployed app reading its own history back would have reported an overdue
#: stop through the entire redo window, after every settled bout. It was
#: invisible for as long as nothing read a ``stop_owed`` row back out again.
#:
#: **Agreeing on the mapping fixed the due time and did not fix the loss below
#: it.** There is one timestamp column, an owed stop legitimately carries two
#: timestamps, and selecting either one by intent throws the other away
#: whichever one it picks. Reading it back by intent means the *right* one now
#: survives -- but ``resumed_at`` still reached the table and stopped there, so
#: :func:`_accrual` fell through to ``manifest.created_at`` on every durable
#: read. See :func:`_resume_origin`, which recovers it from the pipeline's own
#: history rather than widening the row to carry it twice.
_INTENT_FIELDS: dict[str, tuple[str, str]] = {
    _INTENT_STOPPED: ("stopped_at", "stopped_by"),
    _INTENT_RESUMING: ("resumed_at", "resumed_by"),
    _INTENT_STOP_OWED: ("owed_at", "owed_by"),
}


class PipelinePowerError(RuntimeError):
    """A stop or start could not be performed safely."""


def owed_stop_sentence(owed_since: str) -> str:
    """The one sentence every surface says about a stop that was owed and lost.

    **Shared rather than repeated, on the precedent
    `aws_credential_probe.effective_credential_verdict` set.** This sentence had
    two independent copies within hours of being written -- one in
    :meth:`PipelinePower.summary`, one in :func:`session_notice` -- which already
    disagreed about who went away ("the server" against "this process") and about
    how to spell the command. A third copy was about to be written for the
    deployed app, where the same fact reaches an operator by a different route
    entirely, and three copies of a sentence about money is how the rate itself
    came to have three values.

    It deliberately carries **no dollar figure**. The three callers know
    different things about what the pipeline is doing right now: `summary` has
    just read the cloud state, :func:`session_notice` has promised not to look,
    and :func:`owed_stop_detail` cannot look from inside a health check. Each
    frames the money in the terms it can actually stand behind, and this states
    only the part all three know first-hand -- that a stop was scheduled, that
    nothing performed it, and what ends it.
    """

    when = f" FROM {owed_since}" if owed_since else ""
    return (
        f"A STOP WAS OWED{when} AND NEVER HAPPENED — a settled Round 4 bout "
        f"scheduled it and the process that owed it went away first · stop it "
        f"with 'antidemo pipeline stop'"
    )


@dataclass(frozen=True, slots=True)
class PipelinePower:
    """What the Round 4 pipeline is doing, what that costs, and what it has cost."""

    pipeline_id: str
    #: The state the pipeline itself reports, e.g. ``RUNNING`` or ``IDLE``.
    #: Empty when the account could not be asked.
    cloud_state: str
    #: True only when this installation wrote a stop marker for *this* pipeline
    #: and has not since asked for it back. A pipeline that is not running
    #: without one of these is a failure, not a choice, and the two must never
    #: render the same way.
    stopped_deliberately: bool
    stopped_at: str = ""
    stopped_by: str = ""
    #: When a resume was requested and is still settling. Distinct from both of
    #: the states above: a pipeline on its way back up is neither a deliberate
    #: stop nor a failure, and reporting it as either is how a normal resume
    #: turned into a red gate.
    resuming_since: str = ""
    #: When a stop that this installation scheduled became due without being
    #: performed. Distinct from every state above: the pipeline is up, nobody
    #: stopped it, and something here *meant* to. Empty while a scheduled stop is
    #: merely pending, because a bout deliberately holds the pipeline for its
    #: redo window and that is not a fault.
    stop_owed_since: str = ""
    #: What has accrued at the standing rate, and ``accrual_basis`` is the
    #: account of which origin it accrued from. Absent when nothing local records
    #: an origin -- a rate is easy to skim past, an accrued figure is not, but an
    #: accrued figure with an invented start date is worse than neither.
    accrued_usd: Decimal | None = None
    accrual_basis: str = ""

    @property
    def running(self) -> bool:
        return self.cloud_state.strip().upper() == "RUNNING"

    @property
    def resuming(self) -> bool:
        return bool(self.resuming_since) and not self.running

    @property
    def stop_owed(self) -> bool:
        """An overdue stop against a pipeline that is still billing for it.

        Gated on ``running`` because an owed stop that somebody else has since
        satisfied -- by hand, or by a later bout -- is discharged rather than
        outstanding, and a record that outlived its subject must not report a
        stopped pipeline as one still leaking money.
        """

        return bool(self.stop_owed_since) and self.running

    @property
    def usd_per_day(self) -> Decimal:
        """What this pipeline bills per day in the state it is actually in.

        A stopped pipeline has no continuous update to bill for, so the honest
        figure is zero rather than a smaller non-zero guess.
        """

        return PIPELINE_USD_PER_DAY if self.running else Decimal(0)

    def _accrued_clause(self) -> str:
        """The bill so far, or silence. Never a figure without its origin."""

        if self.accrued_usd is None:
            return ""
        return f" · ~${self.accrued_usd:.2f} accrued {self.accrual_basis}".rstrip()

    def summary(self) -> str:
        """One line naming the state and its price, for `doctor` and `cleanup`.

        The number is always present, in both directions: what a running
        pipeline is costing, and what a stopped one is saving. A state without a
        price beside it is what lets accidental burn go unnoticed.

        What each direction says about *arming* changed when the engine learned
        to start this pipeline itself. It used to be that the money-saving state
        and the round-refusing state were the same state, so a stop had to warn
        an operator of a precondition they would otherwise meet at the bell.
        Arm now starts a stopped pipeline and waits for it, so that warning
        would send an operator to run a command the next arm runs for them --
        and a surface that asks for input the system no longer needs is how an
        unattended installation acquires an attendant. The cost is named
        instead, because a wait before the bout clock is a real thing to know
        and is the only thing a stop still costs.
        """

        if self.running:
            rate = (
                f"RUNNING · ${PIPELINE_USD_PER_DAY:.2f}/day {PIPELINE_RATE_TOLERANCE}"
                f"{self._accrued_clause()}"
            )
            if self.stop_owed:
                # Named as a lost stop rather than as a running pipeline,
                # because those two read identically on every other surface and
                # the difference is the entire finding: a bout scheduled this
                # stop, something took the process away before it fired, and
                # nothing else anywhere would ever mention it.
                return f"{rate} · {owed_stop_sentence(self.stop_owed_since)}"
            return f"{rate} · stop it after the session with 'antidemo pipeline stop'"
        if self.resuming:
            state = self.cloud_state or "not readable from here"
            return (
                f"RESUMING since {self.resuming_since} · currently {state}, not yet "
                f"RUNNING · an arm will wait for it rather than fail · watch "
                f"'antidemo pipeline status'"
            )
        # "up to": the saving is the standing rate less whatever the pipeline
        # runs during sessions, and nothing here measures session length. A bare
        # figure would read as a measurement without being one.
        #
        # Stated per day, which is the longest horizon this rate may be quoted
        # over. It used to be a month, and a month is a window this pipeline is
        # never up for: it runs for the minutes of a bout, so the projection
        # priced a scenario the installation does not produce and did it in the
        # largest figure in the repository.
        if self.stopped_deliberately:
            stamp = f" at {self.stopped_at}" if self.stopped_at else ""
            actor = f" by {self.stopped_by}" if self.stopped_by else ""
            return (
                f"STOPPED ON PURPOSE{stamp}{actor} · $0.00/day "
                f"(saving up to ${PIPELINE_USD_PER_DAY:.2f}/day while it would "
                f"otherwise be up) · Round 4 starts it back up at "
                f"arm and waits for it, adding roughly {RESTART_SECONDS_ESTIMATE}s "
                f"before the bout clock begins · no operator action needed"
            )
        state = self.cloud_state or "UNKNOWN"
        return (
            f"NOT RUNNING ({state}) and no deliberate stop was recorded · "
            f"$0.00/day, but this is a failure rather than a choice"
        )


def workspace_api(workspace: Any) -> Callable[..., dict[str, Any]]:
    """An ``api`` callable for :func:`stop` and :func:`start`, over the SDK.

    The signature is the one those two already take --- ``(profile, method,
    path, *, body=None, timeout=...)`` --- deliberately unchanged, so this is a
    second implementation of an existing seam rather than a second seam. The CLI
    wires that callable to ``lifecycle._databricks_api``, which shells out to the
    ``databricks`` binary with ``-p profile``. In the Apps runtime that binary
    does not exist and there is no CLI profile to name, which is the whole reason
    the deployed app has never been able to touch this pipeline.

    ``profile`` is accepted and ignored, and that is not an oversight worth
    tidying away. A ``WorkspaceClient`` is already bound to one workspace by the
    ambient credentials the app was given; there is nothing for a profile to
    select. Taking the argument is what lets the two callers stay identical.

    Nothing here decides *what* may be addressed. Both entry points route every
    path through :func:`_require_pipeline_path` before it arrives, so a request
    that reached the synced table would already have been refused by the time
    this runs.
    """

    def call(
        profile: str,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        timeout: float = 600,
    ) -> dict[str, Any]:
        del profile, timeout
        payload = workspace.api_client.do(method.upper(), path, body=body)
        return dict(payload) if isinstance(payload, Mapping) else {}

    return call


def stop_marker_path(manifest_file: Path | None = None) -> Path:
    """Where the deliberate-stop record lives: beside the manifest it belongs to."""

    base = manifest_file if manifest_file is not None else manifest_path()
    return base.parent / STOP_MARKER_FILENAME


def _actor(manifest: DemoManifest) -> str:
    """Who asked. Recorded so a stop nobody remembers making has a name on it."""

    return (
        str(getattr(manifest, "owner", "") or "")
        or os.environ.get("USER", "")
        or "unknown operator"
    )


def _write_ledger(marker_path: Path | None, record: dict[str, Any]) -> bool:
    """Replace the power record, atomically enough for a file one process writes.

    ``0o600`` because the record carries the run ID and the operator's name, and
    it sits beside a manifest held at the same mode.

    Returns whether a file was written, and does not raise when there is nowhere
    to write one. A deployed Databricks App has no manifest *path* -- the
    manifest arrives as a secret environment variable -- so ``stop_marker_path()``
    raises there, and it would raise **after** the stop or start had already been
    issued. Failing then would leave the cloud mutated and the caller holding an
    exception about a file. The durable store is where the app's record actually
    goes; see :func:`record_power_request`.
    """

    try:
        marker = marker_path if marker_path is not None else stop_marker_path()
    except Exception:
        return False
    marker.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    marker.chmod(0o600)
    return True


def coordination_endpoint_for(manifest: DemoManifest) -> str:
    """Which Lakebase endpoint this installation coordinates on.

    The same expression :func:`server.manifest.apply_manifest_environment` uses
    to populate ``ANTI_DEMO_COORDINATION_ENDPOINT_NAME``, and pinned against it
    by test rather than by hope, because the two silently disagreeing would put
    `doctor` back to reading a deliberate stop as a broken round.
    """

    databricks = manifest.databricks
    return databricks.coordination_endpoint_name or (
        f"projects/{databricks.project_id}/branches/coordination/endpoints/primary"
    )


def _coordination_store() -> Any | None:
    """A coordination connection for a CLI that was never given one, or None.

    ``antidemo serve`` calls `apply_manifest_environment` and every coordination
    consumer downstream of it reads the endpoint out of the environment. `doctor`
    and `cleanup` do not, so the environment they inherit says nothing about the
    installation they are inspecting, and a store built from it would refuse.
    The manifest is asked directly instead -- it is the same manifest `doctor`
    is checking, and it is already open in the caller's hand.
    """

    from .coordination import LakebaseBoutLeaseStore, build_lease_store
    from .manifest import load_manifest

    if os.environ.get("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", "").strip():
        store = build_lease_store()
        return store if getattr(store, "mode", None) == "lakebase" else None
    manifest = load_manifest(manifest_path())
    endpoint = coordination_endpoint_for(manifest)
    if not endpoint:
        return None
    return LakebaseBoutLeaseStore(
        endpoint_name=endpoint,
        database=manifest.databricks.database,
        profile=manifest.databricks.profile,
        user=manifest.databricks.user,
    )


def _durable_ledger(pipeline_id: str) -> dict[str, Any] | None:
    """The newest coordination-table power record for this pipeline, or None.

    Reached only when the local marker had no answer, which on a laptop means
    either that nothing has ever stopped this pipeline or that the stop was made
    by something other than this laptop -- in practice the deployed app,
    releasing the pipeline after a bout settled.

    It cannot tell those two apart without asking, so `doctor` and `cleanup` pay
    a coordination round trip, of the order of a second, on runs where no local
    marker exists. `_round4_check`'s gate was written when the marker was the
    only record and reads as though the ordinary path still costs nothing; it no
    longer quite does. That is the right way to spend a second: the alternative
    is `doctor` calling a deliberate $14.57/day saving a broken Round 4, and an
    operator's correct response to a broken Round 4 is to start the pipeline.

    Never raises, and never blocks a running event loop. A serving process is
    refused rather than made to wait: nothing inside the app reads this, the
    app is the process that *wrote* the record, and a coordination round trip
    on the request path to answer a question nobody asked there would be a poor
    trade at best and a stalled reply at worst.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        return None

    async def read() -> dict[str, Any] | None:
        installed = _durable_store
        if installed is not None:
            return await installed.latest(pipeline_id)
        store = _coordination_store()
        if store is None:
            # Nothing to ask, so the marker file was the whole record. Correct
            # for a developer checkout, where nothing else could have stopped
            # this pipeline in the first place.
            return None
        try:
            return await DurablePipelinePowerStore(store._run).latest(pipeline_id)
        finally:
            with suppress(Exception):
                await store.close()

    try:
        return asyncio.run(read())
    except Exception:
        # Warning, not debug. The commonest cause is a coordination schema
        # provisioned before this table existed, and the visible consequence is
        # a red Round 4 on an installation that is deliberately saving money --
        # so the one line that explains it has to be readable beside the red.
        logger.warning(
            "Could not read durable Round 4 power records, so a stop made by the "
            "deployed app cannot be told apart from a failure here. If this "
            "installation predates the power table, 'antidemo renew' provisions it.",
            exc_info=True,
        )
        return None


def _read_ledger(
    pipeline_id: str, *, path: Path | None = None, durable: bool = True
) -> dict[str, Any] | None:
    """The last deliberate power request for this exact pipeline, whatever it was.

    A record naming a different pipeline is ignored rather than trusted. Manifest
    directories outlive the resources they describe, and a stale record read as
    current would report a freshly provisioned, fully billing pipeline as
    deliberately stopped -- the precise false all-clear this module exists to
    prevent.

    Returns the raw record regardless of age, which is what separates it from
    :func:`read_stop_marker`. Age decides whether a request is still *in effect*;
    it does not decide whether the request happened, and the timestamp on an
    expired resume is still the best evidence of when billing resumed.

    The file is asked first and the coordination table only if the file has no
    answer, which keeps the ordinary local path exactly as cheap as it was: a
    laptop that stopped its own pipeline finds its own marker and never opens a
    connection. The fallback exists because the app now stops this pipeline too,
    and the app has no manifest directory to leave a marker in -- so without it
    the operator's next `doctor` reads a deliberate, self-healing saving as a
    Round 4 failure.

    ``durable=False`` declines the fallback outright, for the one caller that
    has promised never to touch the network: :func:`session_notice` runs seconds
    before a demo, where a slow workspace or an expired SSO session would sit
    between an operator and their own server.
    """

    marker: Path | None = path
    if marker is None:
        try:
            marker = stop_marker_path()
        except Exception:
            # No manifest directory -- a deployed replica. The durable record is
            # the only one there has ever been here, so fall through to it.
            marker = None
    payload: Any = None
    if marker is not None:
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            payload = None
    if not isinstance(payload, dict) or str(payload.get("pipeline_id") or "") != pipeline_id:
        payload = None
    if payload is None and path is None and durable:
        payload = _durable_ledger(pipeline_id)
        # Held to the same identity check the file is, rather than trusted
        # because it came from a keyed query. The consequence of a mismatch
        # slipping through is identical whichever store it came from: a fully
        # billing pipeline reported as deliberately switched off.
        if isinstance(payload, dict) and str(payload.get("pipeline_id") or "") != pipeline_id:
            payload = None
    return payload


def _intent(payload: Mapping[str, Any]) -> str:
    """Which direction the record was written in.

    A record with no ``intent`` at all is a stop. That is not a guess: the field
    was added when the resume direction was, and every record written before it
    could only ever have been a stop. Reading a live pre-existing marker as a
    resume would clear a deliberate stop nobody had asked back.
    """

    return str(payload.get("intent") or _INTENT_STOPPED)


def _parse_stamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def read_stop_marker(
    pipeline_id: str,
    *,
    path: Path | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    durable: bool = True,
) -> dict[str, Any] | None:
    """The power request that is still *in effect* for this pipeline, or None.

    This is the gate `doctor` gets its answer from, so what counts as "in effect"
    decides whether a red check is printed. Two records qualify:

    * a **stop**, for as long as it stands. It is cleared by a resume and by
      nothing else, least of all by elapsed time -- a stop that aged out of this
      answer would hand back the false all-clear the marker exists to prevent.
    * a **resume**, for :data:`RESUME_GRACE_SECONDS` only. Inside that window the
      pipeline is on its way up and a check must not call that a failure; outside
      it, a resume that has still not produced a ``RUNNING`` pipeline *is* a
      failure and must be free to say so.
    * an **owed stop**, from its due time onward and never before it. This is the
      resume rule run backwards, and for the identical reason. A settled bout
      deliberately holds the pipeline for :data:`IDLE_STOP_SECONDS` so a redo can
      land on it, so during that window a stop being owed is the correct, healthy
      state and a check that shouted about it would shout after every single
      bout. Past the due time the stop should have happened and has not, which is
      the whole defect this record exists to make visible.

    A resume with an unreadable timestamp expires immediately, and an owed stop
    with one is treated as already due. Both fail toward the noisier answer on
    purpose: a spurious red is recoverable in one command, a suppressed red is a
    wedged round discovered at the bell -- or, here, a pipeline left running at
    $14.57/day with nobody told about it.
    """

    payload = _read_ledger(pipeline_id, path=path, durable=durable)
    if payload is None:
        return None
    intent = _intent(payload)
    if intent == _INTENT_STOP_OWED:
        due_at = _parse_stamp(payload.get("owed_at"))
        if due_at is None:
            # An owed stop with no readable due time is overdue by default,
            # the same direction an unreadable resume expires in and for the
            # same reason: the recoverable error is the noisy one.
            return payload
        return payload if now().astimezone(UTC) >= due_at else None
    if intent != _INTENT_RESUMING:
        return payload
    resumed_at = _parse_stamp(payload.get("resumed_at"))
    if resumed_at is None:
        return None
    elapsed = (now().astimezone(UTC) - resumed_at).total_seconds()
    return payload if 0 <= elapsed <= RESUME_GRACE_SECONDS else None


def _sealed_pipeline_id(manifest: DemoManifest) -> str:
    sealed = getattr(manifest, "round4", None)
    pipeline_id = str(getattr(sealed, "pipeline_id", "") or "") if sealed is not None else ""
    if not pipeline_id:
        raise PipelinePowerError(
            "This manifest seals no Round 4 pipeline, so there is nothing to stop or start"
        )
    return pipeline_id


def _require_pipeline_path(pipeline_id: str, path: str) -> str:
    """Refuse any request that is not addressed to the sealed pipeline itself.

    This is the guarantee the module is built around. The synced table lives
    under `/api/2.0/postgres` and `/api/2.0/database/synced_tables`; a request
    that reached either could recreate it and take Rounds 5 and 6 with it. Rather
    than trusting every future caller to remember that, every path is checked
    here, so a stop that grew a synced-table write would fail closed instead of
    silently demoting the manifest.
    """

    prefix = f"/api/2.0/pipelines/{pipeline_id}"
    if path != prefix and not path.startswith(f"{prefix}/"):
        raise PipelinePowerError(
            f"Refusing a Round 4 power request addressed outside the sealed pipeline: {path}"
        )
    return path


def _accrual(
    manifest: DemoManifest,
    ledger: Mapping[str, Any] | None,
    *,
    running: bool,
    now: datetime,
) -> tuple[Decimal | None, str]:
    """What this pipeline has billed since it was last known to start.

    The point of an accrued figure rather than only a rate: a daily rate is easy
    to read past, and the same rate multiplied by the three days nobody stopped it
    is not. This is the whole of the reporting mechanism's teeth, so the two ways
    it could lie are closed explicitly.

    It never accrues from a guessed origin. Two origins are admissible --- a
    recorded resume, and ``manifest.created_at``, which is when `antidemo setup`
    created the pipeline and is the same origin
    :mod:`server.standing_cost` measures its window from. When neither is
    readable there is no figure, because a plausible start date would produce a
    plausible bill, and this number's only job is to be trusted.

    **The two are not the same kind of number, and saying which is which is the
    difference between a report and a lie.** A recorded resume is when billing
    actually restarted, so the figure from it is a measurement.
    ``manifest.created_at`` is only when the pipeline came into existence, and
    the arithmetic from it --- rate times elapsed --- silently assumes the
    pipeline never stopped in between. This module exists because it stops
    constantly, so that figure is an **upper bound**: every stop since
    installation is time it did not bill for. It described itself as a floor
    for as long as it existed, which is the wrong direction and the expensive
    one --- an operator told "at least $65" when the truth is forty cents reads
    a two-order-of-magnitude over-estimate as a conservative one, and the
    correct response to a floor is alarm.

    It also never accrues against a pipeline that is not running, and never from
    a *stop* record. A stop record beside a ``RUNNING`` pipeline is a
    contradiction --- an out-of-band start, or a stop that did not take --- and
    the honest response to a contradiction is to withhold the derived figure
    rather than date it from the wrong end.
    """

    if not running:
        return None, ""
    intent = _intent(ledger) if ledger is not None else ""
    if intent == _INTENT_STOPPED:
        return None, ""
    # An owed stop carries forward the ``resumed_at`` of the resume it
    # superseded, precisely so that scheduling a stop does not cost the accrued
    # figure its only honest origin.
    since = (
        _parse_stamp(ledger.get("resumed_at"))
        if intent in (_INTENT_RESUMING, _INTENT_STOP_OWED)
        else None
    )
    if since is not None:
        basis = f"since it was restarted at {since:%Y-%m-%dT%H:%MZ}"
    else:
        created = getattr(manifest, "created_at", None)
        if not isinstance(created, datetime):
            return None, ""
        since = created if created.tzinfo is not None else created.replace(tzinfo=UTC)
        basis = (
            f"assuming it has run since this installation was created at "
            f"{since:%Y-%m-%dT%H:%MZ} — no start is recorded here, so this is a "
            f"ceiling rather than a measurement, and every stop since then is "
            f"time it did not bill for"
        )
    seconds = Decimal(str((now - since.astimezone(UTC)).total_seconds()))
    if seconds <= 0:
        return None, ""
    return PIPELINE_USD_PER_DAY * seconds / _SECONDS_PER_DAY, basis


def power_state(
    manifest: DemoManifest,
    get_pipeline: Callable[[str], dict[str, Any]],
    *,
    marker_path: Path | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PipelinePower:
    """Ask the account what the pipeline is doing and read the local intent.

    Never raises on an unreachable account: an operator asking what a pipeline
    costs is often doing so precisely because something is wrong, and a check
    that refuses to answer takes away the number they came for.
    """

    pipeline_id = _sealed_pipeline_id(manifest)
    try:
        payload = get_pipeline(pipeline_id)
        cloud_state = str((payload or {}).get("state") or "")
    except Exception:
        cloud_state = ""
    ledger = _read_ledger(pipeline_id, path=marker_path)
    in_effect = read_stop_marker(pipeline_id, path=marker_path, now=now)
    stopped = in_effect is not None and _intent(in_effect) == _INTENT_STOPPED
    resuming = in_effect is not None and _intent(in_effect) == _INTENT_RESUMING
    owed = in_effect is not None and _intent(in_effect) == _INTENT_STOP_OWED
    accrued, basis = _accrual(
        manifest, ledger, running=cloud_state.strip().upper() == "RUNNING", now=now()
    )
    return PipelinePower(
        pipeline_id=pipeline_id,
        cloud_state=cloud_state,
        stopped_deliberately=stopped,
        stopped_at=str((in_effect or {}).get("stopped_at") or "") if stopped else "",
        stopped_by=str((in_effect or {}).get("stopped_by") or "") if stopped else "",
        resuming_since=str((in_effect or {}).get("resumed_at") or "") if resuming else "",
        stop_owed_since=str((in_effect or {}).get("owed_at") or "") if owed else "",
        accrued_usd=accrued,
        accrual_basis=basis,
    )


def stop(
    manifest: DemoManifest,
    api: Callable[..., dict[str, Any]],
    *,
    marker_path: Path | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    on_record: Callable[[dict[str, Any]], None] | None = None,
) -> PipelinePower:
    """Stop the sealed pipeline and record that the stop was deliberate.

    Only ``/api/2.0/pipelines/<id>/stop`` is issued. The synced table's spec --
    including the ``scheduling_policy`` whose mutation would recreate it -- is
    never read, never written, and cannot be addressed from here.

    ``on_record`` hands the caller the same record the file marker receives, so a
    caller with a durable store can persist it without this function growing an
    ``await``. The app is that caller.
    """

    pipeline_id = _sealed_pipeline_id(manifest)
    profile = manifest.databricks.profile
    api(
        profile,
        "post",
        _require_pipeline_path(pipeline_id, f"/api/2.0/pipelines/{pipeline_id}/stop"),
    )
    stopped_at = now().astimezone(UTC).isoformat()
    stopped_by = _actor(manifest)
    record = {
        "intent": _INTENT_STOPPED,
        "pipeline_id": pipeline_id,
        "run_id": manifest.run_id,
        "stopped_at": stopped_at,
        "stopped_by": stopped_by,
        "usd_per_day_saved": f"{PIPELINE_USD_PER_DAY:.2f}",
    }
    _write_ledger(marker_path, record)
    if on_record is not None:
        on_record(record)
    return PipelinePower(
        pipeline_id=pipeline_id,
        cloud_state="",
        stopped_deliberately=True,
        stopped_at=stopped_at,
        stopped_by=stopped_by,
    )


def owed_stop_record(
    manifest: DemoManifest,
    *,
    due_at: datetime,
    resumed_at: str = "",
    marker_path: Path | None = None,
) -> dict[str, Any]:
    """Record that a stop has been scheduled but not yet performed.

    Deliberately built and written here rather than in the caller, so the three
    power intents keep one shape and one writer. It issues **no verb**: nothing
    about the pipeline changes, which is the point -- this is a promise, and the
    reason it is durable is that the process holding the promise may not live to
    keep it.

    ``resumed_at`` is carried forward from the resume this owed stop supersedes
    so that :func:`_accrual` keeps dating the bill from when billing actually
    restarted. Without it the single-slot marker would lose the only origin the
    accrued figure can honestly use, and a real number would quietly degrade
    into a ceiling measured from installation. The durable store carries the
    same fact by a different route --- it cannot store this field at all, and
    recovers the origin from the resume row instead; see
    :func:`_resume_origin`. The caller supplies it when *its*
    arm issued the resume; when the arm found the pipeline already up there is
    nobody to supply it, so the record standing here is read and its origin
    carried rather than overwritten.

    The read is local-only. This runs on the tail of a settled bout inside the
    serving process, and a coordination round trip there would put the network
    between a bout finishing and its release being scheduled.
    """

    pipeline_id = _sealed_pipeline_id(manifest)
    if not resumed_at:
        superseded = _read_ledger(pipeline_id, path=marker_path, durable=False) or {}
        resumed_at = str(superseded.get("resumed_at") or "")
    record = {
        "intent": _INTENT_STOP_OWED,
        "pipeline_id": pipeline_id,
        "run_id": manifest.run_id,
        "owed_at": due_at.astimezone(UTC).isoformat(),
        "owed_by": _actor(manifest),
        "usd_per_day_owed": f"{PIPELINE_USD_PER_DAY:.2f}",
    }
    if resumed_at:
        record["resumed_at"] = resumed_at
    _write_ledger(marker_path, record)
    return record


def start(
    manifest: DemoManifest,
    api: Callable[..., dict[str, Any]],
    *,
    marker_path: Path | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    on_record: Callable[[dict[str, Any]], None] | None = None,
) -> PipelinePower:
    """Restart the sealed pipeline and record that a resume is in progress.

    ``full_refresh`` is explicitly false. A full refresh would re-seed the
    destination from scratch, which is both slow and a change to the thing Round
    4 measures; the resumed pipeline is supposed to pick up from the Delta
    version it had already processed.

    The record is **rewritten, not deleted.** Deleting it was a real defect
    rather than a tidiness question: the request returns immediately and the
    pipeline needs seconds or minutes to reach ``RUNNING``, so between the two
    there was a pipeline that was not running and no local record that anyone had
    asked it to run. `doctor` in that window fell through to the full Round 4
    check and printed red at an installation that was behaving correctly. The
    resume record is what makes that interval say "resuming" instead, and
    :data:`RESUME_GRACE_SECONDS` is what stops it from saying so forever.
    """

    pipeline_id = _sealed_pipeline_id(manifest)
    profile = manifest.databricks.profile
    api(
        profile,
        "post",
        _require_pipeline_path(pipeline_id, f"/api/2.0/pipelines/{pipeline_id}/updates"),
        body={"full_refresh": False},
    )
    resumed_at = now().astimezone(UTC).isoformat()
    record = {
        "intent": _INTENT_RESUMING,
        "pipeline_id": pipeline_id,
        "run_id": manifest.run_id,
        "resumed_at": resumed_at,
        "resumed_by": _actor(manifest),
        "usd_per_day_resumed": f"{PIPELINE_USD_PER_DAY:.2f}",
    }
    _write_ledger(marker_path, record)
    if on_record is not None:
        on_record(record)
    return PipelinePower(
        pipeline_id=pipeline_id,
        cloud_state="",
        stopped_deliberately=False,
        resuming_since=resumed_at,
    )


# ---------------------------------------------------------------------------
# The same record, kept where a deployed replica can keep it.
#
# Everything above locates the power record through `manifest_path()`, which
# raises in a Databricks App: the manifest arrives as a secret environment
# variable and there is no state directory to sit beside. So the app could not
# write this record even if it could issue the calls, and a stop it made would be
# indistinguishable from a pipeline that fell over -- which is precisely the
# confusion the record exists to prevent, arriving by a new route.
#
# This is the same two-places split `server/receipts.py` already makes, for the
# same reason and in the same shape: a file for the laptop, a table on the
# coordination database for the app. Nothing here is a new idea. It borrows the
# receipt store's `read_coordination_objects` probe before any DDL, its
# consumer/owner split, its append-only contract, and a connection runner handed
# in rather than built.
# ---------------------------------------------------------------------------

#: The append-only power history on the coordination database. A module-level
#: constant because ``lifecycle._coordination_runtime_grants`` imports it, so a
#: rename breaks that import rather than silently orphaning the grant.
PIPELINE_POWER_TABLE = f"{COORDINATION_SCHEMA}.round4_pipeline_power"

#: The sequence behind ``event_id``. Named for the same reason the table is: the
#: grant plan needs it, and an ``INSERT`` against a ``bigserial`` the app cannot
#: draw from fails at the first stop rather than at review.
PIPELINE_POWER_SEQUENCE = "round4_pipeline_power_event_id_seq"

#: How many rows back :meth:`DurablePipelinePowerStore.latest` reads.
#:
#: One row was enough while a record's only job was to describe itself. An owed
#: stop also has to say when billing started, and that fact lives in the
#: ``resuming`` row it supersedes rather than in the owed row itself -- the
#: table has one timestamp column and the owed row spends it on the due time.
#:
#: **Bounded rather than unbounded, and small rather than generous.** A settled
#: bout writes at most three rows -- resume, owed, stopped -- so the resume this
#: is looking for is normally two rows back and never more than a few bouts
#: back. Reading twenty covers several bouts of history for one indexed keyed
#: read, and failing to find a resume inside it produces an honest "origin
#: unknown" rather than a scan of a table that grows for the life of the
#: installation.
_POWER_HISTORY_LOOKBACK = 20


def _marker_from_row(row: Any, pipeline_id: str) -> dict[str, Any]:
    """One coordination row in the file marker's own shape.

    The single timestamp column is mapped back through :data:`_INTENT_FIELDS`,
    which is the same map ``append`` chose it with. Reading it back under a key
    the writer did not use is what once made an owed stop arrive overdue.
    """

    intent = str(row[0])
    stamp_field, actor_field = _INTENT_FIELDS.get(intent, _INTENT_FIELDS[_INTENT_RESUMING])
    return {
        "intent": intent,
        "pipeline_id": pipeline_id,
        "run_id": row[1] or "",
        stamp_field: row[2].astimezone(UTC).isoformat() if row[2] is not None else "",
        actor_field: str(row[3] or ""),
        "usd_per_day": str(row[4] or ""),
    }


def _resume_origin(older: Any) -> str:
    """When the billing an owed stop is about actually began, or ``""``.

    ``older`` is this pipeline's rows *strictly older than* the owed stop,
    newest first. The answer is the most recent resume that has not since been
    cancelled by a stop, which is exactly the origin
    :func:`owed_stop_record` carries forward on disk --- recovered here rather
    than stored twice, because the alternative is a second timestamp column and
    a coordination schema change to carry a fact the table already holds.

    **It is read from history rather than trusted from the record, and that is
    the stronger answer.** The carry-forward on disk only works when a local
    marker exists; the deployed app has no manifest directory, so an arm that
    found the pipeline already up had nothing to carry and wrote an owed stop
    with no origin at all. The row for the resume that actually started the
    billing is in the table either way.

    A ``stopped`` row ends the walk and yields nothing. Billing stopped there,
    so any resume older than it belongs to a period that has already ended, and
    dating a bill from it would be the same wrong-end error :func:`_accrual`
    refuses a stop record for. Another ``stop_owed`` is walked past: it is a
    promise about the pipeline, not a change to it, so it interrupts nothing.
    """

    for row in older:
        intent = str(row[0])
        if intent == _INTENT_RESUMING:
            return row[2].astimezone(UTC).isoformat() if row[2] is not None else ""
        if intent == _INTENT_STOPPED:
            return ""
    return ""


#: The store this process records power requests to, or None when it has none.
#:
#: Process-global for the same reason the receipt store is: the writer is the
#: Round 4 engine, which is constructed per bout from a sealed manifest and has
#: no application state in hand. Threading a coordination runner through the
#: engine factory to reach two calls would be a far larger change than the thing
#: it enables.
_durable_store: DurablePipelinePowerStore | None = None


def install_pipeline_power_store(store: DurablePipelinePowerStore | None) -> None:
    """Point every subsequent stop and start at ``store``, or at nothing."""
    global _durable_store
    _durable_store = store
    # The startup snapshot below was read through whichever store was installed
    # before this one, so it describes a history this process is no longer
    # reading. Dropping it here is what stops a re-installed store carrying a
    # stale owed stop into a second lifespan.
    install_owed_stop_snapshot(None)


def installed_pipeline_power_store() -> DurablePipelinePowerStore | None:
    """The durable store this process is using, for tests and health reporting."""
    return _durable_store


class DurablePipelinePowerStore:
    """Deliberate stops and starts, one row each, on the coordination database.

    Append-only, like the receipt history and for the same reason: a correction
    is a new row, :meth:`latest` reads the newest, and the app needs no ``UPDATE``
    and no ``DELETE`` anywhere near a record whose only job is to be believed.

    The runner is handed in exactly as ``readiness.StartupReadinessStore`` and
    ``receipts.DurableReceiptStore`` take one. This module has no business
    resolving a Lakebase host, a user and an OAuth token when the process already
    has a store that does.
    """

    def __init__(
        self,
        run: Callable[[Callable[[Any], Awaitable[Any]]], Awaitable[Any]],
    ) -> None:
        self._run = run

    async def initialize(self) -> None:
        """Confirm the power table exists, and create it only if it does not.

        Same consumer/owner split as the receipt history, the cost ledger and the
        readiness row, and the same reason: on the deployed path this runs as a
        principal with no DDL on ``anti_demo_coordination``, and ``CREATE TABLE
        IF NOT EXISTS`` checks the ACL *before* the ``IF NOT EXISTS``, so an
        unconditional create is refused for a statement that would have done
        nothing.
        """

        async def ensure(cursor: Any) -> None:
            objects = await read_coordination_objects(cursor, (PIPELINE_POWER_TABLE,))
            if objects.complete:
                return
            try:
                if not objects.schema_present:
                    await cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {COORDINATION_SCHEMA}")
                await cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_POWER_TABLE} (
                        event_id bigserial PRIMARY KEY,
                        pipeline_id text NOT NULL,
                        intent text NOT NULL,
                        run_id text,
                        requested_at timestamptz NOT NULL,
                        requested_by text NOT NULL,
                        usd_per_day text NOT NULL,
                        written_at timestamptz NOT NULL DEFAULT clock_timestamp()
                    )
                    """
                )
            except psycopg.errors.InsufficientPrivilege as exc:
                raise CoordinationObjectsMissingError(
                    f"The Round 4 pipeline power history is missing "
                    f"{objects.describe_missing()}, and this identity may not create "
                    "it. Provision the coordination schema with an identity that owns "
                    "it (`antidemo setup`), then grant this one the runtime privileges "
                    "in docs/DEPLOY.md."
                ) from exc

        await self._run(ensure)

    async def append(self, record: Mapping[str, Any]) -> None:
        """Record one deliberate power request, in the file marker's own shape.

        Takes the dict :func:`stop` and :func:`start` already build rather than a
        parallel set of arguments, so the two stores cannot come to disagree
        about what a power record contains.
        """

        intent = _intent(record)
        # Selected by intent through the same map :meth:`latest` reads back
        # with, never by "whichever field is populated". An owed stop carries a
        # ``resumed_at`` forward so the accrued figure keeps its origin, so a
        # first-non-empty rule silently wrote the resume's timestamp into the
        # due-time column and the record came back overdue on arrival.
        stamp_field, actor_field = _INTENT_FIELDS.get(intent, _INTENT_FIELDS[_INTENT_RESUMING])
        stamp = _parse_stamp(record.get(stamp_field))

        async def insert(cursor: Any) -> None:
            await cursor.execute(
                f"""
                INSERT INTO {PIPELINE_POWER_TABLE} (
                    pipeline_id, intent, run_id, requested_at, requested_by, usd_per_day
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    str(record.get("pipeline_id") or ""),
                    intent,
                    str(record.get("run_id") or "") or None,
                    stamp or datetime.now(UTC),
                    str(record.get(actor_field) or ""),
                    str(
                        record.get("usd_per_day_saved")
                        or record.get("usd_per_day_resumed")
                        or record.get("usd_per_day_owed")
                        or f"{PIPELINE_USD_PER_DAY:.2f}"
                    ),
                ),
            )

        await self._run(insert)

    async def latest(self, pipeline_id: str) -> dict[str, Any] | None:
        """The newest power request for this exact pipeline, in marker shape.

        Keyed on ``pipeline_id`` for the reason :func:`_read_ledger` is: a
        coordination database outlives the resources it coordinates, and a record
        naming a pipeline that no longer exists, read as current, would report a
        freshly provisioned and fully billing pipeline as deliberately stopped.

        **An owed stop comes back with its accrual origin, which is why this
        reads more than one row.** The two records this store round-trips are
        not symmetric: a stop and a resume each carry one timestamp, and an owed
        stop carries two --- when the stop falls due, and when the billing it is
        about began. The table has one timestamp column, so the owed row spends
        it on the due time and the origin is simply absent from the row.
        Returning the row alone therefore handed :func:`_accrual` a record with
        no ``resumed_at``, which fell through to ``manifest.created_at`` and
        reported ~$65 "since this installation was created" against the same
        record's ~$0.40 on disk --- over-estimated by two orders of magnitude
        and, until it was corrected, called a floor.

        The origin is recovered from this pipeline's own history instead of
        stored a second time; see :func:`_resume_origin`. That keeps the
        coordination schema exactly as it is, which matters more than it looks:
        a column here has to appear in ``lifecycle._coordination_runtime_grants``
        and in :meth:`initialize` together, and a from-scratch install refuses if
        they disagree.
        """

        async def select(cursor: Any) -> Any:
            await cursor.execute(
                f"""
                SELECT intent, run_id, requested_at, requested_by, usd_per_day
                FROM {PIPELINE_POWER_TABLE}
                WHERE pipeline_id = %s
                ORDER BY event_id DESC
                LIMIT {_POWER_HISTORY_LOOKBACK}
                """,
                (pipeline_id,),
            )
            return await cursor.fetchall()

        rows = list(await self._run(select) or ())
        if not rows:
            return None
        record = _marker_from_row(rows[0], pipeline_id)
        if _intent(record) == _INTENT_STOP_OWED:
            origin = _resume_origin(rows[1:])
            if origin:
                # Added beside ``owed_at``, never in place of it. Both belong to
                # this record and the last time these two met, one overwrote the
                # other and the stop came back due twenty minutes early.
                record["resumed_at"] = origin
        return record


async def record_power_request(record: Mapping[str, Any]) -> bool:
    """Persist a power record durably if this process can, and never raise.

    Never raising is the same rule receipts keep: a souvenir must not fail the
    thing it describes. Here the thing it describes is a pipeline that has
    already been stopped or started, so an exception would leave the cloud
    mutated and the caller unwinding a mutation it cannot take back.

    Returning ``False`` is not silence. A process with nowhere durable to write
    is one where a later `doctor` cannot tell a deliberate stop from a failure,
    so it is logged at warning level rather than swallowed.
    """

    # Before the write is even attempted, and unconditionally. The snapshot
    # below is "what a previous process left owed"; this process has now formed
    # a newer intent about the same pipeline, so the snapshot is superseded
    # whether or not it reaches the table. Clearing it only on a successful
    # append would leave the far noisier failure -- a stop this process really
    # did perform, still reported as owed by every later `/readyz`.
    install_owed_stop_snapshot(None)
    store = _durable_store
    if store is None:
        return False
    try:
        await store.append(record)
    except Exception:
        logger.warning(
            "A Round 4 pipeline power request was issued but could not be recorded "
            "durably, so a later check cannot tell this deliberate change from a "
            "failure",
            exc_info=True,
        )
        return False
    return True


# ---------------------------------------------------------------------------
# The same owed stop, where a deployed operator can actually reach it.
#
# `session_notice` below is the mitigation for a forgotten stop, and it prints
# from `cli._serve`. The deployed app is `python -m uvicorn app:app` -- it never
# executes `cli.py` at all -- so on the one installation that bills money
# unattended, that notice has never run. `doctor` cannot cover the gap either:
# it is a CLI command, and an operator under the standing posture ("one deploy,
# then nobody inputs anything, ever") runs neither.
#
# What a deployed operator can reach without inputting anything is `/readyz`.
# So the record is read **once, at startup** -- the same shape `app._restart_history`
# and `app.state.startup_reap` already use -- and evaluated per request against
# its own due time, which is what lets one cheap read honour a redo window that
# has not expired yet when the process comes up.
#
# It is a *snapshot of what a previous process left behind*, which is the whole
# scope of the problem: an owed stop this process wrote is held by this
# process's own live timer, and `aclose` discharges it. Only a process that
# never came back leaves one for somebody else to find.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OwedStopNotice:
    """An overdue stop this process inherited, and the sentence for it."""

    since: str
    detail: str


#: The newest power record for the sealed pipeline when this process started,
#: kept only when it was an owed stop. ``None`` on every other path, including
#: the ordinary one where the last thing that happened was a completed stop.
_owed_stop_snapshot: dict[str, Any] | None = None


def install_owed_stop_snapshot(record: Mapping[str, Any] | None) -> None:
    """Set what this process inherited, or clear it. For startup and for tests."""
    global _owed_stop_snapshot
    _owed_stop_snapshot = dict(record) if record is not None else None


async def load_owed_stop_snapshot(manifest: DemoManifest) -> dict[str, Any] | None:
    """Read what the last process left owed against this pipeline, once.

    Called at startup, after the durable store is installed, and never again.
    That is deliberate rather than lazy: `/readyz` is polled every few seconds
    by a platform health check, and a coordination round trip per poll would
    turn a health endpoint into a load generator against the same database the
    ring is fenced on -- the identical reason `_apply_credential_verdict` reads
    a cached verdict rather than issuing an STS call.

    Never raises and never blocks a boot. This runs beside
    ``_open_pipeline_power_store``, which already treats every failure as "serve
    anyway"; a bookkeeping read that could stop the container from starting
    would be a far worse trade than the signal it delivers.

    Returns the record it kept, or ``None``. Only an owed stop is kept: a
    ``stopped`` record is the ordinary, healthy end state and a ``resuming``
    record belongs to a start nobody is waiting on any more.
    """

    install_owed_stop_snapshot(None)
    store = _durable_store
    if store is None:
        # No durable history to read. A developer checkout keeps the marker
        # file, and `session_notice` already covers the process that reads it.
        return None
    try:
        pipeline_id = _sealed_pipeline_id(manifest)
        record = await store.latest(pipeline_id)
    except Exception:
        logger.warning(
            "Could not read whether a Round 4 pipeline stop was left owed by a "
            "previous process, so /readyz cannot report one. The pipeline may be "
            "billing with nothing saying so; check 'antidemo pipeline status'.",
            exc_info=True,
        )
        return None
    if not isinstance(record, dict) or _intent(record) != _INTENT_STOP_OWED:
        return None
    # Held to the same identity check `_read_ledger` applies, and for the same
    # reason: a coordination database outlives the resources it coordinates, and
    # a row naming a pipeline that no longer exists would have this process warn
    # about money nothing is spending.
    if str(record.get("pipeline_id") or "") != pipeline_id:
        return None
    install_owed_stop_snapshot(record)
    return dict(record)


def owed_stop_notice(
    *, now: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> OwedStopNotice | None:
    """The overdue stop this process inherited, or ``None``. No I/O at all.

    Silent until ``owed_at`` has passed, which is the same gate
    :func:`read_stop_marker` applies and is not negotiable: a settled bout holds
    the pipeline for its redo window on purpose, so a notice inside that window
    would fire after every single bout and be learned away. A record whose due
    time cannot be read is treated as already due, failing toward the noisy
    answer exactly as the marker does.

    **What it claims is bounded by what it looked at, and the sentence says so.**
    This is a snapshot of the durable history, not a reading of the pipeline, and
    the two can differ: `antidemo pipeline stop` run from a laptop writes the
    file marker and -- with no durable store installed in a CLI process --
    nothing to the coordination table. So the dollar figure is stated
    conditionally on the pipeline still running, and the command that settles the
    question is named. Asserting a live bill from a startup snapshot would be the
    same defect as a health field that read ``degraded`` without checking.
    """

    record = _owed_stop_snapshot
    if record is None:
        return None
    since = str(record.get("owed_at") or "")
    due_at = _parse_stamp(since)
    if due_at is not None and now().astimezone(UTC) < due_at:
        return None
    return OwedStopNotice(
        since=since,
        detail=(
            f"{owed_stop_sentence(since)} · Read from the durable power history "
            f"when this process started, and nothing here takes a live reading, so "
            f"that is the record rather than the pipeline's state: while it runs it "
            f"bills ${PIPELINE_USD_PER_DAY:.2f}/day {PIPELINE_RATE_TOLERANCE}. A stop "
            f"made from a laptop leaves no durable record, so confirm with "
            f"'antidemo pipeline status'."
        ),
    )


def session_notice(
    manifest: DemoManifest,
    *,
    marker_path: Path | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[str]:
    """What to put in front of the operator as a session starts. Local reads only.

    This is where the forgotten stop is actually addressed, and the reasoning for
    putting it here rather than automating the stop is recorded in
    `OPEN-FINDINGS.md` under D20a. In short: there is no moment at which a demo
    session is definitively over --- D20's third blocker establishes that --- so
    there is nowhere safe to hang an automatic stop, and a stop that fires on a
    guess trades a recoverable overspend for an unrecoverable dead round. What
    *is* reliable is the moment a session **begins**, because every session
    passes through `antidemo serve`. So the prompt lands there.

    D20b since amended that: a served Round 4 bout now does release the pipeline
    once it has settled and stayed idle, so this prompt is no longer the only
    thing standing between the installation and a standing bill. It is still
    worth printing, because the release only happens if Round 4 is actually
    armed, and an operator who serves for another round leaves the pipeline up.

    It says the thing that is useful in each direction, and they are different
    things. A pipeline nobody has stopped gets the bill so far and the case for
    stopping it by hand. A pipeline that *is* stopped is told what the next arm
    will spend restarting it --- which is a cost to know, not a chore to do.

    **No network call, by construction**, which is now asserted rather than
    merely true: every read here passes ``durable=False``. This runs seconds
    before a demo, and a control-plane or coordination round trip here would put
    an expired SSO session or a slow workspace between an operator and their own
    server. The cost is that a stop made by the *deployed* app is invisible to
    this notice, which is the right way round --- that notice is for the laptop
    about to serve, and `doctor` is where the whole installation is asked about.
    The rest of that cost is stated rather than hidden: with no live reading, a
    running pipeline is described as one with no stop recorded against it, which
    is what is actually known.
    """

    try:
        pipeline_id = _sealed_pipeline_id(manifest)
    except PipelinePowerError:
        return []
    in_effect = read_stop_marker(pipeline_id, path=marker_path, now=now, durable=False)
    intent = _intent(in_effect) if in_effect is not None else ""
    if intent == _INTENT_STOPPED:
        stamp = str(in_effect.get("stopped_at") or "") if in_effect else ""
        when = f" at {stamp}" if stamp else ""
        return [
            f"PIPELINE {pipeline_id} IS STOPPED ON PURPOSE{when} — $0.00/day.",
            f"         Round 4 will start it at arm and wait, adding roughly "
            f"{RESTART_SECONDS_ESTIMATE}s before the bout clock. To spend that now "
            f"instead: ./antidemo pipeline start",
        ]
    if intent == _INTENT_RESUMING:
        stamp = str(in_effect.get("resumed_at") or "") if in_effect else ""
        return [
            f"PIPELINE {pipeline_id} IS RESUMING (requested {stamp}) — not yet RUNNING, "
            f"and an arm will wait for it rather than fail.",
            "         Confirm before the bell: ./antidemo pipeline status",
        ]
    if intent == _INTENT_STOP_OWED:
        stamp = str(in_effect.get("owed_at") or "") if in_effect else ""
        return [
            f"PIPELINE {pipeline_id} · {owed_stop_sentence(stamp)}",
            f"         Nothing here took a live reading, so that is the record rather "
            f"than the pipeline's state: while it runs it bills "
            f"${PIPELINE_USD_PER_DAY:.2f}/day {PIPELINE_RATE_TOLERANCE}.",
        ]
    ledger = _read_ledger(pipeline_id, path=marker_path, durable=False)
    accrued, basis = _accrual(manifest, ledger, running=True, now=now().astimezone(UTC))
    accrued_clause = f" ~${accrued:.2f} has accrued {basis}." if accrued is not None else ""
    return [
        f"PIPELINE {pipeline_id} has no deliberate stop recorded against it, so it is "
        f"billing ${PIPELINE_USD_PER_DAY:.2f}/day "
        f"{PIPELINE_RATE_TOLERANCE} while it is up.{accrued_clause}",
        "         A served Round 4 bout releases it once it has settled and stayed "
        "idle. Nothing else does, so if you serve and never arm Round 4: "
        "./antidemo pipeline stop",
    ]
