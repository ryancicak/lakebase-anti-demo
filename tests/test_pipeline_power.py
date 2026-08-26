"""The Round 4 pipeline's stop/start seat, and the trap it must never spring.

Stopping the pipeline is worth the better part of $15 a day. Recreating the
synced table while doing it costs Rounds 5 and 6, because a new
``synced_table_uid`` and ``pipeline_id`` demote the manifest to v2. These tests
exist because that trade is so lopsided that a stop which quietly reached the
synced table would be worse than never having built one.

No test here writes the pipeline's rate down. Every expected string is built from
:data:`server.pipeline_power.PIPELINE_USD_PER_DAY`, and
:func:`test_no_figure_for_this_meter_escapes_its_own_band` is what stops a sixth
copy of it appearing somewhere a derivation cannot reach.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from test_lifecycle import attach_round4, make_manifest

from server import pipeline_power
from server.pipeline_power import (
    PIPELINE_USD_PER_DAY,
    PipelinePower,
    PipelinePowerError,
    power_state,
    read_stop_marker,
    start,
    stop,
)
from server.standing_cost import ROUND4_PIPELINE_LABEL, PlatformComponent


def _manifest_with_round4():
    manifest = make_manifest()
    attach_round4(manifest)
    return manifest


def _recording_api(calls: list[tuple[str, str, dict | None]]):
    def api(profile, method, path, *, body=None, timeout=600):
        del profile, timeout
        calls.append((method, path, body))
        return {}

    return api


def test_stop_leaves_the_synced_table_identity_and_manifest_version_untouched(
    tmp_path, monkeypatch
) -> None:
    """The whole reason the stop is allowed to exist.

    Recreating the synced table is the one outcome that is worse than paying for
    an idle pipeline: it mints a new ``synced_table_uid`` and ``pipeline_id``,
    which demotes the manifest to v2 and destroys Rounds 5 and 6. This pins the
    two identities and the version across a stop, and pins that the stop wrote no
    manifest at all -- an assertion that would fail the moment a stop grew a
    re-seal, which is the shape the trap would actually arrive in.
    """

    manifest = _manifest_with_round4()
    before_uid = manifest.round4.synced_table_uid
    before_pipeline = manifest.round4.pipeline_id
    before_version = manifest.manifest_version
    saved: list[object] = []
    monkeypatch.setattr(
        "server.manifest.save_manifest",
        lambda candidate, path=None: saved.append(candidate),
    )
    calls: list[tuple[str, str, dict | None]] = []

    stop(manifest, _recording_api(calls), marker_path=tmp_path / "stopped.json")

    assert manifest.round4.synced_table_uid == before_uid
    assert manifest.round4.pipeline_id == before_pipeline
    assert manifest.manifest_version == before_version
    assert saved == []


def test_stop_and_start_address_only_the_sealed_pipeline(tmp_path) -> None:
    """No request may reach the synced table, by construction rather than habit.

    The synced table is mutable through ``/api/2.0/postgres`` and
    ``/api/2.0/database/synced_tables``, and a write to either could recreate it.
    Pinning the exact call list means a future stop that "just also refreshes the
    spec" fails here instead of in a demo.
    """

    manifest = _manifest_with_round4()
    pipeline_id = manifest.round4.pipeline_id
    marker = tmp_path / "stopped.json"
    calls: list[tuple[str, str, dict | None]] = []

    stop(manifest, _recording_api(calls), marker_path=marker)
    start(manifest, _recording_api(calls), marker_path=marker)

    assert calls == [
        ("post", f"/api/2.0/pipelines/{pipeline_id}/stop", None),
        ("post", f"/api/2.0/pipelines/{pipeline_id}/updates", {"full_refresh": False}),
    ]
    assert all(path.startswith(f"/api/2.0/pipelines/{pipeline_id}") for _, path, _ in calls)
    # A resumed pipeline picks up from the Delta version it had already
    # processed. A full refresh would re-seed the destination and change what
    # Round 4 measures.
    assert calls[1][2] == {"full_refresh": False}


def test_a_power_request_outside_the_sealed_pipeline_is_refused() -> None:
    manifest = _manifest_with_round4()
    pipeline_id = manifest.round4.pipeline_id

    with pytest.raises(PipelinePowerError, match="outside the sealed pipeline"):
        pipeline_power._require_pipeline_path(
            pipeline_id, f"/api/2.0/database/synced_tables/{pipeline_id}"
        )
    # A prefix that merely starts the same way is a different pipeline.
    with pytest.raises(PipelinePowerError, match="outside the sealed pipeline"):
        pipeline_power._require_pipeline_path(pipeline_id, f"/api/2.0/pipelines/{pipeline_id}x")


def test_a_resume_reads_as_neither_a_stop_nor_a_failure_until_it_should(
    tmp_path,
) -> None:
    """The gap `start()` used to open, from both ends.

    `start()` returns immediately and the pipeline needs seconds or minutes to
    reach RUNNING. Three different wrong answers are possible in that interval
    and all three cost something real, so all three are pinned here:

    * reading it as a **deliberate stop** is a false all-clear -- the operator is
      told they are saving the pipeline's daily rate while the pipeline bills;
    * reading it as a **failure** inside the window is the false red that
      `doctor` actually printed, and a check that cries wolf during a normal
      resume is one an operator learns to skip;
    * *still* reading it as a resume long afterwards suppresses a real red, which
      is a wedged Round 4 discovered at the bell.
    """

    manifest = _manifest_with_round4()
    pipeline_id = manifest.round4.pipeline_id
    marker = tmp_path / "stopped.json"
    calls: list[tuple[str, str, dict | None]] = []
    requested = datetime(2026, 8, 23, 14, 30, tzinfo=UTC)

    stop(manifest, _recording_api(calls), marker_path=marker)
    start(manifest, _recording_api(calls), marker_path=marker, now=lambda: requested)

    # The record survives the start. Deleting it is what opened the gap.
    assert marker.exists()

    def at(seconds: float):
        moment = requested + timedelta(seconds=seconds)
        return power_state(
            manifest,
            lambda identifier: {"state": "STARTING"},
            marker_path=marker,
            now=lambda: moment,
        )

    settling = at(5)
    assert settling.stopped_deliberately is False
    assert settling.resuming is True
    assert "RESUMING" in settling.summary()
    assert "an arm will wait for it rather than fail" in settling.summary()
    assert "STOPPED ON PURPOSE" not in settling.summary()
    assert "failure rather than a choice" not in settling.summary()
    # Still inside the window at the boundary, and outside it after.
    assert at(pipeline_power.RESUME_GRACE_SECONDS).resuming is True
    stale = at(pipeline_power.RESUME_GRACE_SECONDS + 1)
    assert stale.resuming is False
    assert "failure rather than a choice" in stale.summary()
    # And a resume that reached RUNNING is simply running, not resuming.
    reached = power_state(
        manifest,
        lambda identifier: {"state": "RUNNING"},
        marker_path=marker,
        now=lambda: requested + timedelta(seconds=30),
    )
    assert reached.resuming is False
    assert f"RUNNING · ${PIPELINE_USD_PER_DAY:.2f}/day" in reached.summary()
    # A record written before the resume direction existed carries no `intent`.
    # It can only ever have been a stop, and reading it as anything else would
    # clear a deliberate stop nobody asked back.
    marker.write_text(json.dumps({"pipeline_id": pipeline_id}), encoding="utf-8")
    legacy = power_state(manifest, lambda identifier: {"state": "IDLE"}, marker_path=marker)
    assert legacy.stopped_deliberately is True


def test_a_marker_from_another_pipeline_is_never_read_as_this_one(tmp_path) -> None:
    """A stale marker would report a fully billing pipeline as switched off.

    Manifest directories outlive the resources they describe. This is the false
    all-clear in miniature, and it is cheap to make impossible.
    """

    marker = tmp_path / "stopped.json"
    marker.write_text(json.dumps({"pipeline_id": "an-older-pipeline"}), encoding="utf-8")

    assert read_stop_marker("the-current-pipeline", path=marker) is None
    assert read_stop_marker("an-older-pipeline", path=marker) is not None


def test_doctor_can_tell_a_deliberate_stop_from_a_failure(tmp_path) -> None:
    """The two must never render the same way.

    A pipeline that was switched off to save money and one that fell over both
    stop being RUNNING. An operator who cannot distinguish them either panics at
    a healthy installation or ignores a broken one.
    """

    manifest = _manifest_with_round4()
    marker = tmp_path / "stopped.json"
    calls: list[tuple[str, str, dict | None]] = []
    stop(manifest, _recording_api(calls), marker_path=marker)

    chosen = power_state(manifest, lambda identifier: {"state": "IDLE"}, marker_path=marker)
    broken = power_state(
        manifest, lambda identifier: {"state": "FAILED"}, marker_path=tmp_path / "absent.json"
    )

    assert chosen.stopped_deliberately is True
    assert "STOPPED ON PURPOSE" in chosen.summary()
    assert broken.stopped_deliberately is False
    assert "failure rather than a choice" in broken.summary()
    assert "FAILED" in broken.summary()


def test_every_state_reports_what_it_costs_per_day() -> None:
    """A state without a price beside it is what allows quiet burn.

    The stopped case also has to name the *precondition*, not only the saving.
    The money-saving state and the state in which Round 4 refuses to arm are the
    same state, so a line that mentions one and not the other has set the
    operator up to find out at the bell.

    The rate is asserted *derived* rather than asserted equal to a figure written
    here. This project has now fixed the same defect five times -- a quantity
    written down twice, which drifts -- and this pipeline's rate was the fifth:
    ``$14.18/day``, ``$14.80/day`` and ``$14.8012/day`` were all recorded
    independently for one meter. Pinning a literal here would make this test the
    sixth copy, so what is pinned instead is the identity, and
    :func:`test_no_figure_for_this_meter_escapes_its_own_band` is what makes a
    sixth copy elsewhere fail rather than drift.
    """

    running = PipelinePower("p", "RUNNING", stopped_deliberately=False)
    stopped = PipelinePower("p", "IDLE", stopped_deliberately=True)

    # The figure is the meter times the price, and nothing else. A literal
    # substituted for either constant fails here.
    assert PIPELINE_USD_PER_DAY == (
        pipeline_power.PIPELINE_DBU_PER_HOUR * pipeline_power.PIPELINE_USD_PER_DBU * 24
    )
    # And it is the same arithmetic the panel performs on the same component, so
    # the operator's figure and the audience's cannot be two derivations.
    priced = PlatformComponent(
        label=ROUND4_PIPELINE_LABEL,
        dbu_per_hour=pipeline_power.PIPELINE_DBU_PER_HOUR,
        usd_per_dbu=pipeline_power.PIPELINE_USD_PER_DBU,
        attribution="D20's 301 usage intervals from 2026-08-20T15:00:00Z",
        grade="measured",
    )
    assert priced.usd_per_hour * 24 == PIPELINE_USD_PER_DAY
    # The point estimate has to sit inside its own band, which is what fails if
    # the meter is moved and the band recording its variance is left behind.
    low, high = pipeline_power.PIPELINE_USD_PER_DAY_RANGE
    assert low < PIPELINE_USD_PER_DAY < high

    rate = f"${PIPELINE_USD_PER_DAY:.2f}/day"
    assert running.usd_per_day == PIPELINE_USD_PER_DAY
    assert rate in running.summary()
    # And never a month of it. The pipeline is up for the minutes of a bout, so
    # projecting its while-running rate across a month describes a scenario this
    # installation does not produce -- and it was the largest number anywhere in
    # the repository, which makes it the one a reader quotes back. The daily rate
    # on a resource that should have been stopped is the honest form of the same
    # warning and is what these lines carry instead.
    assert "/month" not in running.summary()
    # Never the bare figure. The uncertainty travels with it, because this is the
    # string a stranger reads and repeats, and a point estimate repeated without
    # its band is how three figures for one meter came to look like a conflict.
    assert f"{rate} {pipeline_power.PIPELINE_RATE_TOLERANCE}" in running.summary()
    assert pipeline_power.PIPELINE_RATE_TOLERANCE == "±2%"
    assert stopped.usd_per_day == Decimal(0)
    assert "$0.00/day" in stopped.summary()
    assert f"saving up to {rate}" in stopped.summary()
    assert "/month" not in stopped.summary()
    # What a stop costs is the restart wait, and since D20b that wait is the
    # engine's to spend, not the operator's. A line telling an operator to run
    # 'pipeline start' would send them to do the next arm's job for it, which is
    # how an installation that is supposed to need no input acquires an
    # attendant. The number stays, because a wait before the bell is worth
    # knowing; the instruction goes.
    assert f"roughly {pipeline_power.RESTART_SECONDS_ESTIMATE}s" in stopped.summary()
    assert "no operator action needed" in stopped.summary()
    assert "refuse to arm" not in stopped.summary()
    assert "antidemo pipeline start" not in stopped.summary()


def test_no_figure_for_this_meter_escapes_its_own_band() -> None:
    """The check that is meant to make a sixth hardcoded copy fail, not drift.

    Four times before this one, the fix for a twice-written quantity was to
    derive it -- and four times a fresh hardcoded copy turned up somewhere the
    derivation could not reach: a docstring, a findings entry, a fixture. Fixing
    the fifth the same way and stopping would only buy another week.

    So this reads the source instead. Every dollars-per-day figure written in one
    of the sources below -- `server/`, the frontend's captured cost fixture, this
    file, and `OPEN-FINDINGS.md` where it exists -- that is close enough to this
    meter to be about it has to lie inside the band the meter is recorded as
    ranging over. That is a weaker claim than "equals the derived figure", and
    deliberately: `$14.18`, `$14.80` and `$14.8012` are all real readings of this
    meter and the whole finding is that they are the band's ends rather than
    rivals, so a rule that outlawed them would be asserting the error it is meant
    to prevent. What it does catch is the thing that actually keeps happening --
    a *new* figure for this meter, arrived at independently and quietly outside
    what the measurements support -- and it catches it wherever the figure is
    written, which is the part no derivation can do.

    The band is compared at its own recorded precision. Its endpoints are
    recorded to two decimal places in DBU/h, so their true edges are half a unit
    in the last place beyond them; a figure derived from the unrounded top would
    otherwise fail against the rounded one, which is exactly the kind of spurious
    conflict this whole exercise was about.

    **`OPEN-FINDINGS.md` is scanned only where it exists.** It is one of the
    files the publish step withholds, so in the public repository it is absent by
    design and reading it unconditionally is a `FileNotFoundError` on the first
    CI run. It is therefore included conditionally, and the anti-vacuity floor
    below moves with it. Read that lower floor as what the *published* tree can
    prove, not as the strength of this check: privately it sees 26 figures and
    the fifteen in the findings file are the ones most likely to drift, because
    prose is where every previous escape happened. If you are tightening this,
    tighten the private floor.

    **Both floors are asserted on every private run**, which is the part that was
    missing and is not the same fix as making the read conditional. A single
    counter meant the published floor was only ever evaluated in the published
    repository -- so a change that narrowed the scan to `server/*.py`, where it
    reaches 3 against a floor of 8, would have passed here and failed there, on
    the first CI run of a tree nobody could test. See
    `tests/test_publication_coupling.py` for the class this belongs to.
    """

    half_ulp = Decimal("0.005")
    low, high = pipeline_power.PIPELINE_DBU_PER_HOUR_RANGE
    per_dbu = pipeline_power.PIPELINE_USD_PER_DBU
    floor = (low - half_ulp) * per_dbu * 24
    ceiling = (high + half_ulp) * per_dbu * 24

    # Wide enough to catch any restatement of this meter, narrow enough to
    # exclude the quantities either side of it -- the platform lane's $17.28/day,
    # which is this pipeline plus the app, and the neutral runner below it.
    neighbourhood = PIPELINE_USD_PER_DAY * Decimal("0.12")
    root = Path(__file__).resolve().parent.parent
    written = re.compile(r"\$(\d+\.\d+) ?/day")
    findings = root / "OPEN-FINDINGS.md"
    # Every published place a figure for this meter is currently written, so the
    # scan is not reduced to `server/` alone once the findings file is withheld.
    # The frontend fixture matters most of the three: it is a *captured payload*
    # carrying `$14.8012/day` in its data and `$14.80`/`$14.57` in its provenance
    # note, and "a fixture" is one of the four places a fresh hardcoded copy has
    # already turned up. Nothing in `frontend/` was in scope before.
    files = [
        *sorted((root / "server").glob("*.py")),
        root / "frontend" / "src" / "standing-cost.fixture.json",
        Path(__file__).resolve(),
        *([findings] if findings.exists() else []),
    ]

    # Counted in two buckets rather than one, and both floors are asserted on
    # every private run. Making the read of `OPEN-FINDINGS.md` conditional was
    # necessary and not sufficient: with a single counter, the only thing that
    # ever evaluated the *published* floor was a run in the published repository,
    # where nobody looks until CI is already red -- so a refactor that dropped the
    # frontend fixture from `files` would leave `server/*.py` alone reaching 3
    # against a floor of 8, and would pass here forever. Splitting the counter is
    # what makes that failure land in this tree, at review time.
    published = 0
    withheld = 0
    for path in files:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for match in written.finditer(line):
                figure = Decimal(match.group(1))
                if abs(figure - PIPELINE_USD_PER_DAY) > neighbourhood:
                    continue
                if path == findings:
                    withheld += 1
                else:
                    published += 1
                assert floor <= figure <= ceiling, (
                    f"{path.relative_to(root)}:{number} states ${figure}/day for the "
                    "Round 4 pipeline, which is outside the "
                    f"${floor:.2f}-${ceiling:.2f}/day band its meter is recorded as "
                    "ranging over (D20c). Derive it from "
                    "pipeline_power.PIPELINE_DBU_PER_HOUR or cite that band -- do not "
                    "write down a sixth figure for it."
                )
    # Not vacuous: the sources really do carry figures for this meter, so a
    # refactor that moved them out from under this scan would fail here rather
    # than pass by finding nothing to check. Eleven publishable ones today (three
    # in `server/pipeline_power.py`, four in the frontend fixture, four in this
    # file's own prose) and fifteen more in the findings file, so both floors have
    # slack for an edit that legitimately drops one.
    #
    # THE PUBLISHED FLOOR IS ASSERTED UNCONDITIONALLY. That is the whole point of
    # the split above: it is the floor that governs the repository a stranger
    # clones, and it is the one no private run used to evaluate.
    assert published >= 8, (
        f"only {published} figures for this meter are in scope outside "
        f"OPEN-FINDINGS.md, which publication withholds. In the public repository "
        f"that is the entire scan, so this check has been quietly weakened exactly "
        f"where nobody runs it -- `server/*.py` alone reaches 3. Restore a "
        f"published source to `files` above, or lower this floor deliberately and "
        f"say here what the check is still worth."
    )
    if findings.exists():
        assert published + withheld >= 20


def test_the_running_line_carries_the_bill_so_far_or_no_figure_at_all() -> None:
    """A rate is skimmed past; an accrued total is not. That is the whole teeth.

    And the one way this number could do harm is by being dated from a guess, so
    the withheld case is pinned beside the stated one.
    """

    accrued = PipelinePower(
        "p",
        "RUNNING",
        stopped_deliberately=False,
        accrued_usd=Decimal("42.54"),
        accrual_basis="since it was restarted at 2026-08-21T16:12Z",
    )
    assert "~$42.54 accrued since it was restarted at 2026-08-21T16:12Z" in accrued.summary()

    silent = PipelinePower("p", "RUNNING", stopped_deliberately=False)
    assert "accrued" not in silent.summary()
    assert f"${PIPELINE_USD_PER_DAY:.2f}/day" in silent.summary()


def test_the_accrued_figure_is_dated_from_a_record_or_withheld(tmp_path) -> None:
    """Three origins, and only two of them are admissible.

    A recorded resume is the best answer, and it is the only one that is a
    *measurement*. `manifest.created_at` is the fallback: it is when `antidemo
    setup` made the pipeline and is the same origin `standing_cost` measures its
    window from, but rate-times-elapsed from it assumes the pipeline never
    stopped, and this module exists because it stops constantly. So that figure
    is a **ceiling**, and it must say so. It called itself a floor for as long as
    it existed, which is the wrong direction and the expensive one: a floor
    invites alarm, and this one over-states by however long the installation has
    been up. A *stop* record beside a RUNNING pipeline is a contradiction -- an
    out-of-band start, or a stop that did not take -- and dating a bill from the
    wrong end of one is worse than declining to.

    The ``created_at`` fallback is pinned rather than left on the wall clock, for
    the reason recorded on
    :func:`test_the_session_notice_names_the_bill_and_the_arm_precondition`: that
    origin is one end of the window ``later`` is the other end of, and an origin
    that floats overtakes a pinned ``later`` at a particular time of day. Left
    floating, the ``from_install`` case below stopped producing a figure once the
    wall clock passed 2026-08-24T16:12Z, which would have read as a regression in
    the fallback rather than as the clock.
    """

    manifest = _manifest_with_round4()
    marker = tmp_path / "stopped.json"
    calls: list[tuple[str, str, dict | None]] = []
    resumed = datetime(2026, 8, 21, 16, 12, tzinfo=UTC)
    later = resumed + timedelta(days=3)
    manifest.created_at = resumed - timedelta(days=1)
    manifest.expires_at = manifest.created_at + timedelta(hours=24)

    def observe(path):
        return power_state(
            manifest,
            lambda identifier: {"state": "RUNNING"},
            marker_path=path,
            now=lambda: later,
        )

    start(manifest, _recording_api(calls), marker_path=marker, now=lambda: resumed)
    # Three days at the standing rate, and the grace period being long expired does not
    # cost us the timestamp: expiry decides what is in effect, not what happened.
    from_resume = observe(marker)
    assert from_resume.accrued_usd == pytest.approx(
        PIPELINE_USD_PER_DAY * 3, abs=Decimal("0.01")
    )
    assert "restarted at 2026-08-21T16:12Z" in from_resume.accrual_basis

    from_install = observe(tmp_path / "absent.json")
    assert from_install.accrued_usd is not None
    # Four days of installation against three days of billing, so the fallback
    # over-states by a full day at the standing rate. Pinned as a number rather
    # than only as wording, because the wording is what was wrong: this figure
    # is strictly larger than the measured one above and used to be introduced
    # as a lower bound on it.
    assert from_install.accrued_usd > from_resume.accrued_usd
    assert from_install.accrued_usd == pytest.approx(
        PIPELINE_USD_PER_DAY * 4, abs=Decimal("0.01")
    )
    assert "ceiling rather than a measurement" in from_install.accrual_basis
    assert "floor" not in from_install.accrual_basis
    # The whole line an operator reads, not just the basis fragment: the figure
    # and the word that frames it have to arrive together or the framing is
    # decoration.
    assert "ceiling" in from_install.summary()

    stop(manifest, _recording_api(calls), marker_path=marker, now=lambda: resumed)
    contradicted = observe(marker)
    assert contradicted.running is True
    assert contradicted.accrued_usd is None
    assert "accrued" not in contradicted.summary()


def test_an_owed_stop_is_silent_until_it_is_due_and_then_names_the_money(
    tmp_path, monkeypatch
) -> None:
    """The record that makes a lost stop visible, and the window that keeps it quiet.

    A settled Round 4 bout schedules its stop twenty minutes out and writes down
    that it owes one, because the process holding that timer may not live to fire
    it -- and a ``SIGKILL``, an OOM kill or a container eviction runs no shutdown
    handler at all. Before this, nothing anywhere recorded that a stop had been
    owed, so no surface could report it and the next check read a fully billing
    pipeline as one nobody had ever intended to stop.

    **The due time is the whole of its manners.** Those twenty minutes are
    deliberate -- they are bought by the redo affordance -- so during them a stop
    being owed is the healthy state, and a record that shouted then would shout
    after every single bout and be learned away. This is the ``_INTENT_RESUMING``
    rule run backwards, and it is held to the same discipline for the same
    reason.

    It also must not cost the accrued figure its origin. The single-slot marker
    is being overwritten by the owed record, so the resume it supersedes is
    carried forward inside it; without that, a real bill dated from a real
    restart would quietly degrade into a floor dated from installation.
    """

    manifest = _manifest_with_round4()
    marker = tmp_path / "stopped.json"
    calls: list[tuple[str, str, dict | None]] = []
    resumed = datetime(2026, 8, 21, 16, 12, tzinfo=UTC)
    due = resumed + timedelta(minutes=20)
    manifest.created_at = resumed - timedelta(days=1)
    manifest.expires_at = manifest.created_at + timedelta(hours=24)

    # Collected exactly as `model_score_live.ensure_running` collects it, because
    # the durable half of this test has to replay the same two writes the app
    # makes, in the same order.
    resumes: list[dict] = []
    start(
        manifest,
        _recording_api(calls),
        marker_path=marker,
        now=lambda: resumed,
        on_record=resumes.append,
    )
    # No `resumed_at` supplied, which is the case where this bout's arm found the
    # pipeline already up: the origin has to be carried off the record being
    # superseded rather than lost with it.
    record = pipeline_power.owed_stop_record(manifest, due_at=due, marker_path=marker)
    assert record["intent"] == "stop_owed"
    assert record["resumed_at"] == resumed.isoformat()
    # No verb: this is a promise about the pipeline, not a change to it.
    assert [method for method, _, _ in calls] == ["post"]

    pipeline_id = manifest.round4.pipeline_id

    def observe(now):
        return power_state(
            manifest,
            lambda identifier: {"state": "RUNNING"},
            marker_path=marker,
            now=lambda: now,
        )

    inside = due - timedelta(seconds=1)
    assert read_stop_marker(pipeline_id, path=marker, now=lambda: inside) is None
    quiet = observe(inside)
    assert quiet.stop_owed is False
    assert "OWED" not in quiet.summary()

    overdue = observe(due + timedelta(minutes=5))
    assert overdue.stop_owed is True
    assert "A STOP WAS OWED" in overdue.summary()
    assert "antidemo pipeline stop" in overdue.summary()
    # The origin survived the overwrite, so the bill is still dated from the
    # restart rather than from the installation.
    assert "restarted at 2026-08-21T16:12Z" in overdue.accrual_basis

    # And an owed stop somebody else has since satisfied is discharged, not
    # reported: a record that outlived its subject must not describe a stopped
    # pipeline as one still leaking money.
    settled = power_state(
        manifest,
        lambda identifier: {"state": "IDLE"},
        marker_path=marker,
        now=lambda: due + timedelta(minutes=5),
    )
    assert settled.stop_owed is False

    # ------------------------------------------------------------------
    # One verdict, three surfaces, one sentence.
    #
    # This sentence had two copies within hours of being written and they
    # already disagreed about who went away and how to spell the command. A
    # third was needed for the deployed app, so it is shared instead --- the
    # `effective_credential_verdict` precedent --- and byte-identical text is
    # what holds that. Substring assertions per surface would pass happily
    # while the three drifted apart again.
    # ------------------------------------------------------------------
    late = due + timedelta(minutes=5)
    shared = pipeline_power.owed_stop_sentence(due.isoformat())

    notice = pipeline_power.session_notice(manifest, marker_path=marker, now=lambda: late)
    assert any(shared in line for line in notice)
    assert shared in overdue.summary()
    # The rate is framed per surface rather than shared, because the three know
    # different things: `summary` has just read the cloud state and says RUNNING,
    # the session notice has promised not to look and says so.
    assert f"${PIPELINE_USD_PER_DAY:.2f}/day" in " ".join(notice)
    assert "took a live reading" in " ".join(notice)

    # ------------------------------------------------------------------
    # And the surface a deployed operator can actually reach.
    #
    # `session_notice` prints from `cli._serve`; `app.yaml` runs
    # `python -m uvicorn app:app`, so the deployed app never executes `cli.py`
    # and has never once printed it. The record is carried instead through the
    # real durable store --- the same `append`/`latest` round trip the app
    # performs, including the intent-to-field mapping that turns a row back
    # into `owed_at` --- and read off the real `/readyz` payload rather than
    # out of a log capture, because what an operator reads is the endpoint.
    # ------------------------------------------------------------------
    # Nothing here injects a clock into `/readyz`. The endpoint uses its own,
    # which is the only way to prove it honours the redo window on the deployed
    # path -- a notice handed a frozen `now` would prove the helper works and
    # say nothing about the surface an operator reads. So the two records differ
    # in the one thing that matters: `record` fell due in the past, and the
    # second falls due twenty minutes from the real present.
    import app as app_module

    table = _FakePowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    # The two writes the deployed app makes, in its order: the arm's resume, then
    # the settled bout's owed stop. Both through the real `append`.
    asyncio.run(store.append(resumes[0]))
    asyncio.run(store.append(record))

    # ------------------------------------------------------------------
    # Both halves of one record have to come back out together.
    #
    # This is the third change to meet this row and the previous pair produced a
    # bug neither could see alone: one made the owed record carry `resumed_at`
    # forward to keep the accrued figure's origin, the other picked the durable
    # timestamp first-non-empty, the carried resume won, and the row came back
    # due twenty minutes early. That is now selected by intent -- which fixed the
    # due time and left the origin on the floor of the `append`, because the
    # table has ONE timestamp column and an owed stop legitimately has two.
    #
    # So this asserts on what the store returns, not on what was handed to it,
    # and it asserts both facts at once. A fix for either one alone reads as a
    # pass on the other's assertion here.
    # ------------------------------------------------------------------
    round_tripped = asyncio.run(store.latest(pipeline_id))
    assert round_tripped is not None
    # The due time: the thing the previous interaction broke.
    assert round_tripped["owed_at"] == due.isoformat()
    # The accrual origin: the thing this change is about. Recovered from the
    # `resuming` row, because `append` had nowhere to put it.
    assert round_tripped.get("resumed_at") == resumed.isoformat(), (
        "the owed stop came back out of the store without the resume it was "
        "scheduled against, so the accrued figure has no origin and falls "
        f"through to manifest.created_at: {round_tripped}"
    )
    assert pipeline_power._intent(round_tripped) == "stop_owed"

    # And the number that reaches an operator, off the durable record alone --
    # no marker file, which is the deployed replica's actual condition and the
    # `doctor`/`cleanup` path where this was wrong. Asserted as a figure, not
    # only as wording: the defect was a *number*, and it was ~40x the truth on a
    # day-old installation and unbounded as the installation ages.
    pipeline_power.install_pipeline_power_store(store)
    try:
        durable = power_state(
            manifest,
            lambda identifier: {"state": "RUNNING"},
            now=lambda: due + timedelta(minutes=5),
        )
        assert durable.accrued_usd == pytest.approx(
            PIPELINE_USD_PER_DAY * Decimal(25) / Decimal(24 * 60), abs=Decimal("0.01")
        )
        assert "restarted at 2026-08-21T16:12Z" in durable.accrual_basis
        # The mislabel, in the direction that made it worse: an over-estimate
        # introduced as a lower bound.
        assert "ceiling" not in durable.summary()
        assert "floor" not in durable.summary()
        assert "this installation was created" not in durable.summary()
        # It must not have bought the origin by losing the due gate again.
        assert durable.stop_owed is True
        assert (
            power_state(
                manifest,
                lambda identifier: {"state": "RUNNING"},
                now=lambda: due - timedelta(seconds=1),
            ).stop_owed
            is False
        )
    finally:
        pipeline_power.install_pipeline_power_store(None)

    # A stop between the resume and the owed stop ends that billing period, so
    # the resume behind it is no longer this bill's origin and must not be
    # carried. Dating a live bill from a period that already closed is the same
    # wrong-end error a stop record beside a RUNNING pipeline is refused for.
    stopped_between = pipeline_power.DurablePipelinePowerStore(_FakePowerTable().run)
    asyncio.run(stopped_between.append(resumes[0]))
    asyncio.run(
        stopped_between.append(
            {
                "intent": "stopped",
                "pipeline_id": pipeline_id,
                "run_id": manifest.run_id,
                "stopped_at": (resumed + timedelta(minutes=5)).isoformat(),
                "stopped_by": "somebody else",
            }
        )
    )
    asyncio.run(stopped_between.append(record))
    interrupted = asyncio.run(stopped_between.latest(pipeline_id))
    assert interrupted["owed_at"] == due.isoformat()
    assert "resumed_at" not in interrupted

    pipeline_power.install_pipeline_power_store(store)
    try:
        assert asyncio.run(pipeline_power.load_owed_stop_snapshot(manifest)) is not None

        payload = _readyz_on_a_healthy_box(monkeypatch, app_module)
        assert payload["round4_stop_owed"] is True
        # Unchanged by the origin now riding along on the same record, and
        # pinned here deliberately: `/readyz` is rendered in `app.py`, which
        # this change may not edit, and these three fields read out of this
        # module. `since` is the *due* time and must stay the due time -- a
        # record that now carries two timestamps is exactly the condition under
        # which the wrong one gets picked, which is how this pair of fields
        # broke the last time these two facts met.
        assert payload["round4_stop_owed_since"] == due.isoformat()
        assert payload["round4_stop_owed_since"] != resumed.isoformat()
        assert resumed.isoformat() not in payload["round4_stop_owed_detail"]
        assert shared in payload["round4_stop_owed_detail"]
        # Claims the record, never a live reading it did not take. The deployed
        # app cannot ask the control plane from inside a health check, and a
        # laptop's `antidemo pipeline stop` writes no durable row, so asserting
        # a live bill here would be the same defect as a health field that read
        # `degraded` without checking.
        assert "rather than the pipeline's state" in payload["round4_stop_owed_detail"]
        assert f"${PIPELINE_USD_PER_DAY:.2f}/day" in payload["round4_stop_owed_detail"]
        # Reported, never conflated with a functional fault. An owed stop breaks
        # no round -- the next arm finds the pipeline already up -- and lowering
        # the field an operator checks before a demo for a spend problem would
        # make that field less trustworthy, not more.
        assert payload["status"] == "ready"
        assert payload["degraded"] is False
        assert payload["degraded_detail"] is None
        assert payload["degraded_capabilities"] == []

        # Inside the redo window the pipeline is *supposed* to be up. A surface
        # that shouted here would shout after every settled bout and be learned
        # away -- the specific false alarm the owed record was built to avoid.
        pending = pipeline_power.owed_stop_record(
            manifest,
            due_at=datetime.now(UTC) + timedelta(minutes=20),
            marker_path=marker,
        )
        asyncio.run(store.append(pending))
        assert asyncio.run(pipeline_power.load_owed_stop_snapshot(manifest)) is not None
        quiet_payload = _readyz_on_a_healthy_box(monkeypatch, app_module)
        assert quiet_payload["round4_stop_owed"] is False
        assert quiet_payload["round4_stop_owed_since"] is None
        assert quiet_payload["round4_stop_owed_detail"] is None

        # And a stop this process goes on to make discharges the inherited
        # record, so a restarted app that then releases the pipeline itself
        # stops warning about it rather than warning forever. Re-established as
        # overdue first, or the assertion below would pass on the pending record
        # already being silent and prove nothing.
        asyncio.run(store.append(record))
        asyncio.run(pipeline_power.load_owed_stop_snapshot(manifest))
        assert pipeline_power.owed_stop_notice() is not None
        # Collected through `on_record` and persisted separately, which is
        # exactly what `model_score_live._stop_off_loop` does.
        made: list[dict] = []
        stop(manifest, _recording_api(calls), marker_path=marker, on_record=made.append)
        asyncio.run(pipeline_power.record_power_request(made[0]))
        assert pipeline_power.owed_stop_notice() is None
    finally:
        pipeline_power.install_pipeline_power_store(None)


class _FakePowerTable:
    """The coordination power table, in the two statements the store issues.

    Rows are held in insertion order and read back positionally, so the real
    ``append`` parameter order and the real ``latest`` column order both have to
    be right. That is the half of this path that could silently break: the
    intent-to-field mapping in ``latest`` is what turns a row back into
    ``owed_at``, and an owed stop whose due time came back under the wrong key
    would read as due immediately and warn during every redo window.

    **``ORDER BY event_id DESC`` and the ``LIMIT`` are both modelled, and the
    limit is read off the statement rather than assumed.** ``latest`` now reads
    a bounded window rather than one row, because an owed stop's accrual origin
    lives in the ``resuming`` row it supersedes -- the table has one timestamp
    column and the owed row spends it on the due time. A fake that returned
    every matching row regardless of the ``LIMIT`` would let a ``latest`` that
    dropped the bound pass here and scan a growing table in production.
    """

    def __init__(self) -> None:
        self.rows: list[tuple] = []
        self._result: list[tuple] = []

    async def execute(self, sql: str, params: tuple = ()) -> None:
        if "INSERT" in sql:
            self.rows.append(tuple(params))
            return
        if "SELECT" in sql:
            matched = [row for row in self.rows if row[0] == params[0]]
            limit = re.search(r"LIMIT\s+(\d+)", sql)
            assert limit is not None, "the power history read must stay bounded"
            # (pipeline_id, intent, run_id, requested_at, requested_by, usd_per_day)
            # projected to the SELECT's own column list, newest first.
            self._result = [
                (row[1], row[2], row[3], row[4], row[5]) for row in reversed(matched)
            ][: int(limit.group(1))]

    async def fetchone(self):
        return self._result[0] if self._result else None

    async def fetchall(self):
        return list(self._result)

    async def run(self, operation):
        return await operation(self)


def _readyz_on_a_healthy_box(monkeypatch, app_module) -> dict:
    """Render `/readyz` for a durable, verified, settled process.

    A default test app is already degraded on its coordination mode, which would
    hide whether an owed stop changed anything. This is the state a demo runs in,
    and therefore the only one in which "does this degrade?" has a visible
    answer. Modelled on `test_reap._readyz_on_a_healthy_box`, for the same reason.
    """

    from types import SimpleNamespace

    state = app_module.app.state
    monkeypatch.setattr(state, "coordination_mode", "lakebase", raising=False)
    monkeypatch.setattr(state, "readiness_verified", True, raising=False)
    monkeypatch.setattr(state, "credential_sentry", None, raising=False)
    monkeypatch.setattr(state, "restart_history", None, raising=False)
    monkeypatch.setattr(state, "startup_reap", None, raising=False)
    monkeypatch.setattr(
        state,
        "readiness_gate",
        SimpleNamespace(
            status=SimpleNamespace(
                ring_ready=True, maintenance_state="ready", maintenance_detail=None
            ),
            recovery=None,
            round5_recovery=None,
        ),
        raising=False,
    )
    return json.loads(app_module._readiness_response().body)


def test_the_session_notice_names_the_bill_and_the_arm_precondition(tmp_path) -> None:
    """The mechanism that closes the forgotten stop, in the only place it lands.

    Money-spent, under D18: this line is the entire reason a forgotten stop costs
    a session rather than a run of days, and it is printed by `antidemo serve`,
    which no test would otherwise exercise. Both directions are load-bearing --
    see D20a.

    **Both ends of the accrual window are fixed here, and `now` is derived from
    the origin rather than pinned beside it.** `make_manifest` dates the
    installation from the wall clock, and the accrued figure this notice states
    is `now` minus that origin, so pinning only `now` made the window negative
    for every run after 09:00Z -- at which point `_accrual` correctly declines to
    state a figure it cannot date, and the assertion below failed on the clock
    rather than on the behaviour. Deriving `now` from `created_at` keeps the
    window three hours whenever this runs, which is what the assertion is
    actually about.
    """

    manifest = _manifest_with_round4()
    marker = tmp_path / "stopped.json"
    calls: list[tuple[str, str, dict | None]] = []
    manifest.created_at = datetime(2026, 8, 24, 9, 0, tzinfo=UTC)
    manifest.expires_at = manifest.created_at + timedelta(hours=24)
    now = manifest.created_at + timedelta(hours=3)

    # Nothing stopped: the bill, the accrual, and the command that ends it.
    billing = pipeline_power.session_notice(manifest, marker_path=marker, now=lambda: now)
    joined = " ".join(billing)
    assert f"${PIPELINE_USD_PER_DAY:.2f}/day" in joined
    assert "/month" not in joined
    assert "accrued" in joined
    assert "./antidemo pipeline stop" in joined
    # Never claims a live reading it did not take.
    assert "RUNNING" not in joined

    # Stopped: what the next arm will spend bringing it back, offered as a cost
    # to know rather than a chore to do. Under D20b the arm starts it, so an
    # imperative here would be an instruction to duplicate the engine's work.
    stop(manifest, _recording_api(calls), marker_path=marker, now=lambda: now)
    warned = " ".join(
        pipeline_power.session_notice(manifest, marker_path=marker, now=lambda: now)
    )
    assert "STOPPED ON PURPOSE" in warned
    assert "start it at arm and wait" in warned
    assert "CANNOT ARM" not in warned
    assert f"${PIPELINE_USD_PER_DAY:.2f}/day" not in warned

    # An installation with no sealed Round 4 has nothing to say and must not
    # raise: this runs seconds before a demo.
    bare = make_manifest()
    assert pipeline_power.session_notice(bare, marker_path=marker, now=lambda: now) == []


def test_a_stop_the_app_made_is_still_readable_where_no_marker_file_can_exist(
    monkeypatch,
) -> None:
    """The deployed app's stop must not read as a Round 4 failure on the laptop.

    Money-spent and a-round-dying both apply. The app stops this pipeline after a
    bout settles and has no manifest directory to leave a marker in, so the only
    record is the coordination row. If that row stops being consulted, the next
    `antidemo doctor` reports a deliberate, self-healing $14.57/day saving as a
    broken Round 4 -- and the operator's correct response to a broken Round 4 is
    to start the pipeline and leave it running.

    The reverse direction is asserted in the same test on purpose: the local
    marker must still win, and must still be enough on its own, because a laptop
    that stopped its own pipeline has to keep working with no coordination
    endpoint configured at all.
    """

    manifest = _manifest_with_round4()
    pipeline_id = manifest.round4.pipeline_id
    row = {
        "intent": "stopped",
        "pipeline_id": pipeline_id,
        "run_id": manifest.run_id,
        "stopped_at": "2026-08-24T02:40:00+00:00",
        "stopped_by": "app-service-principal",
    }

    class Coordination:
        def __init__(self) -> None:
            self.reads = 0

        async def latest(self, requested: str) -> dict | None:
            self.reads += 1
            return row if requested == pipeline_id else None

    # No manifest directory, exactly as in a Databricks App: the marker path
    # itself raises, which used to take the whole read down with it.
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda: (_ for _ in ()).throw(RuntimeError("no dir"))
    )

    table = Coordination()
    monkeypatch.setattr(pipeline_power, "_durable_store", table)
    power = power_state(manifest, lambda identifier: {"state": "IDLE"})
    assert power.stopped_deliberately is True
    assert power.stopped_by == "app-service-principal"
    assert table.reads > 0

    # A row naming some other pipeline is not this pipeline's stop. A
    # coordination database outlives the resources it coordinates, and a stale
    # row read as current reports a fully billing pipeline as switched off.
    row["pipeline_id"] = "some-other-pipeline"
    assert power_state(manifest, lambda identifier: {"state": "IDLE"}).stopped_deliberately is False
    row["pipeline_id"] = pipeline_id

    # And the notice that runs seconds before a demo asks nothing of the network,
    # which is a promise in its docstring rather than an accident of ordering.
    before = table.reads
    pipeline_power.session_notice(manifest)
    assert table.reads == before


def test_the_coordination_endpoint_doctor_asks_is_the_one_serve_uses(monkeypatch) -> None:
    """Two derivations of one endpoint, pinned together rather than left to drift.

    `antidemo serve` puts the coordination endpoint in the environment via
    `apply_manifest_environment`; `doctor` and `cleanup` never call it, so they
    have to derive the same endpoint from the manifest themselves. If those two
    expressions ever disagree, nothing fails loudly -- the durable read simply
    finds nothing, and `doctor` goes back to reporting a deliberate, money-saving
    stop as a broken Round 4, which is the exact defect this whole path exists to
    remove.
    """

    from server.manifest import apply_manifest_environment

    manifest = _manifest_with_round4()
    monkeypatch.delenv("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", raising=False)
    apply_manifest_environment(manifest)

    assert pipeline_power.coordination_endpoint_for(manifest) == os.environ.get(
        "ANTI_DEMO_COORDINATION_ENDPOINT_NAME"
    )
    assert pipeline_power.coordination_endpoint_for(manifest)


async def test_a_process_that_cannot_record_pipeline_power_still_serves(caplog) -> None:
    """Installing the power store may never be what stops the app from booting.

    The deployed app now boots *degraded* rather than refusing when its AWS
    credential is bad, so this line is reached on paths it previously was not,
    and it is reached before the app can serve anything. A store install that
    raised on a degraded boot would trade a serving app for a bookkeeping table.

    Three shapes, one rule, matching `_open_receipt_store` beside it: a
    process-local ring keeps its marker file and wants no store; a table that is
    absent -- which is every install until setup provisions it -- gets none,
    loudly, because that is precisely when `doctor` starts misreading stops.
    """

    import app as app_module

    class ProcessLocal:
        mode = "memory"

    assert await app_module._open_pipeline_power_store(ProcessLocal()) is None

    class Refusing:
        mode = "lakebase"

        async def _run(self, operation):
            raise RuntimeError("relation round4_pipeline_power does not exist")

    with caplog.at_level(logging.ERROR):
        assert await app_module._open_pipeline_power_store(Refusing()) is None
    assert "cannot record deliberate Round 4 pipeline stops durably" in caplog.text


def test_power_state_still_answers_when_the_account_cannot_be_reached(tmp_path) -> None:
    """An operator asking what a pipeline costs is often asking because it is broken."""

    manifest = _manifest_with_round4()

    def unreachable(identifier):
        raise RuntimeError("workspace unreachable")

    power = power_state(manifest, unreachable, marker_path=tmp_path / "absent.json")

    assert power.cloud_state == ""
    assert power.running is False
    assert power.pipeline_id == manifest.round4.pipeline_id


def test_the_marker_records_when_and_who_and_what_it_saves(tmp_path) -> None:
    manifest = _manifest_with_round4()
    marker = tmp_path / "stopped.json"
    frozen = datetime(2026, 8, 23, 14, 30, tzinfo=UTC)

    stop(manifest, _recording_api([]), marker_path=marker, now=lambda: frozen)

    record = json.loads(marker.read_text(encoding="utf-8"))
    assert record["intent"] == "stopped"
    assert record["pipeline_id"] == manifest.round4.pipeline_id
    assert record["run_id"] == manifest.run_id
    assert record["stopped_at"] == frozen.isoformat()
    assert record["stopped_by"]
    assert record["usd_per_day_saved"] == f"{PIPELINE_USD_PER_DAY:.2f}"
    assert Path(marker).stat().st_mode & 0o777 == 0o600

    start(manifest, _recording_api([]), marker_path=marker, now=lambda: frozen)
    resumed = json.loads(marker.read_text(encoding="utf-8"))
    assert resumed["intent"] == "resuming"
    assert resumed["resumed_at"] == frozen.isoformat()
    assert resumed["resumed_by"]
    assert Path(marker).stat().st_mode & 0o777 == 0o600
