"""The release bar's backup step: a bout inside a source's backup, judged on that alone.

A pass needs the bout to pass AND to have started while the source read `backing-up`; a
bout that missed the window proves nothing. The snapshot is deleted whatever happened.
"""

from __future__ import annotations

import importlib.util
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

BAR = Path(__file__).resolve().parents[1] / "scripts" / "release_bar"


def _load():
    spec = importlib.util.spec_from_file_location("release_bar_backup", BAR / "backup.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backup = _load()
NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


class FakeSource:
    def __init__(self, statuses=("backing-up",)) -> None:
        self.statuses = list(statuses)
        self.snapshots = 0
        self.deleted = 0
        self.snapshot = "anti-demo-backup-r2-rds-test"
        self.identifier = "lakebase-ant-test-r2-rds"

    def status(self) -> str:
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]

    def take_snapshot(self) -> None:
        self.snapshots += 1

    def delete_snapshot(self) -> None:
        self.deleted += 1


class FakeRecorder:
    """status_at answers from a fixed timeline instead of a polling thread."""

    def __init__(self, timeline: dict[str, str]) -> None:
        self.timeline = timeline
        self.stopped = threading.Event()
        self.samples: list[tuple[datetime, str]] = []

    def start(self) -> None:
        pass

    def join(self, timeout: float | None = None) -> None:
        pass

    def status_at(self, moment: datetime) -> str | None:
        return self.timeline.get(moment.isoformat())


def _exercise(monkeypatch, *, row, timeline, round_id="make_schema_change_safely", seal=None):
    monkeypatch.setattr(backup, "full_proof", lambda *args: row)
    if seal is not None:
        monkeypatch.setattr(backup, "seal_check", lambda checkout: seal)
    monkeypatch.setattr(backup.time, "sleep", lambda seconds: None)
    report: dict = {}
    failures = backup.exercise(
        Path("checkout"),
        round_id,
        "rds_postgres",
        Path("out"),
        FakeSource(["available", "backing-up"]),
        FakeRecorder(timeline),
        report,
    )
    return failures, report


def test_a_bout_that_passed_inside_the_backup_passes(monkeypatch) -> None:
    row = {"round": "make_schema_change_safely", "passed": True, "started_at": NOW.isoformat()}

    failures, report = _exercise(monkeypatch, row=row, timeline={NOW.isoformat(): "backing-up"})

    assert failures == []
    assert report["bout"]["source_at_start"] == "backing-up"


def test_a_bout_that_missed_the_backup_proves_nothing_and_fails(monkeypatch) -> None:
    row = {"round": "make_schema_change_safely", "passed": True, "started_at": NOW.isoformat()}

    failures, _ = _exercise(monkeypatch, row=row, timeline={NOW.isoformat(): "available"})

    assert failures == [
        "the bout started after the backup (source read available), so it proves nothing"
    ]


def test_a_bout_that_failed_inside_the_backup_fails(monkeypatch) -> None:
    row = {
        "round": "make_schema_change_safely",
        "passed": False,
        "started_at": NOW.isoformat(),
        "detail": "arm failed: RDS source instance is not available",
    }

    failures, _ = _exercise(monkeypatch, row=row, timeline={NOW.isoformat(): "backing-up"})

    assert failures == ["the bout failed: arm failed: RDS source instance is not available"]


def test_round_five_also_runs_the_seal_check_inside_the_backup(monkeypatch) -> None:
    started = NOW + timedelta(seconds=30)
    row = {"round": "survive_connection_spike", "passed": True, "started_at": started.isoformat()}
    frozen = SimpleNamespace(now=lambda tz: NOW, fromisoformat=datetime.fromisoformat)
    monkeypatch.setattr(backup, "datetime", frozen)

    failures, report = _exercise(
        monkeypatch,
        row=row,
        timeline={NOW.isoformat(): "backing-up", started.isoformat(): "backing-up"},
        round_id="survive_connection_spike",
        seal=(False, "RDS source is not on default.postgres17, available, and in-sync"),
    )

    assert failures == [
        "the Round 5 seal check refused: "
        "RDS source is not on default.postgres17, available, and in-sync"
    ]
    assert report["seal_check"]["source"] == "backing-up"


def test_a_snapshot_that_never_backs_the_source_up_fails_without_a_bout(monkeypatch) -> None:
    monkeypatch.setattr(backup, "full_proof", lambda *args: pytest.fail("ran a bout"))
    monkeypatch.setattr(backup.time, "sleep", lambda seconds: None)
    clock = iter(range(0, 10_000, 10))
    monkeypatch.setattr(backup.time, "monotonic", lambda: next(clock))

    failures = backup.exercise(
        Path("checkout"),
        "make_schema_change_safely",
        "rds_postgres",
        Path("out"),
        FakeSource(["available"]),
        FakeRecorder({}),
        {},
    )

    assert failures == ["the snapshot never put the source into backing-up"]


def test_a_source_already_inside_its_own_backup_runs_the_bout_without_a_snapshot(
    monkeypatch,
) -> None:
    # rc13 (2026-10-03): Round 3's Aurora cluster still read backing-up from its automated
    # snapshot when the pair began, and the manual snapshot was refused before any bout ran.
    row = {"round": "recover_deleted_order", "passed": True, "started_at": NOW.isoformat()}
    monkeypatch.setattr(backup, "full_proof", lambda *args: row)
    monkeypatch.setattr(backup.time, "sleep", lambda seconds: None)
    source = FakeSource(["backing-up"])
    report: dict = {}

    failures = backup.exercise(
        Path("checkout"),
        "recover_deleted_order",
        "aurora_serverless_v2",
        Path("out"),
        source,
        FakeRecorder({NOW.isoformat(): "backing-up"}),
        report,
    )

    assert failures == []
    assert source.snapshots == 0
    assert report["backup"] == "automated, already running"
    assert report["snapshot"] is None


def test_a_source_in_another_state_is_waited_out_before_its_snapshot(monkeypatch) -> None:
    row = {"round": "make_schema_change_safely", "passed": True, "started_at": NOW.isoformat()}
    monkeypatch.setattr(backup, "full_proof", lambda *args: row)
    monkeypatch.setattr(backup.time, "sleep", lambda seconds: None)
    source = FakeSource(["modifying", "modifying", "available", "backing-up"])
    report: dict = {}

    failures = backup.exercise(
        Path("checkout"),
        "make_schema_change_safely",
        "rds_postgres",
        Path("out"),
        source,
        FakeRecorder({NOW.isoformat(): "backing-up"}),
        report,
    )

    assert failures == []
    assert source.snapshots == 1
    assert report["backup"] == "manual snapshot"


def test_a_snapshot_never_taken_is_never_deleted() -> None:
    class Rds:
        def describe_db_cluster_snapshots(self, **kwargs):
            raise AssertionError("looked for a snapshot it never took")

    source = backup.Source.__new__(backup.Source)
    source.rds, source.kind, source.snapshot, source.taken = Rds(), "aurora", "never", False
    source.delete_snapshot()


def test_the_status_at_a_moment_is_the_last_sample_no_later_than_its_tolerance() -> None:
    recorder = backup.StatusRecorder(FakeSource())
    recorder.samples = [
        (NOW, "available"),
        (NOW + timedelta(seconds=4), "backing-up"),
        (NOW + timedelta(seconds=9), "available"),
    ]

    assert recorder.status_at(NOW) == "backing-up"
    assert recorder.status_at(NOW - timedelta(seconds=10)) is None
    assert recorder.status_at(NOW + timedelta(seconds=30)) == "available"


def test_main_deletes_the_snapshot_and_fails_when_the_bout_raises(monkeypatch, tmp_path) -> None:
    source = FakeSource()
    monkeypatch.setattr(backup, "use_checkout", lambda checkout: None)
    monkeypatch.setattr(backup, "installation_run_id", lambda checkout: "ad-test")
    monkeypatch.setattr(backup, "Source", lambda *args: source)
    monkeypatch.setattr(backup.boto3, "client", lambda name: object())

    def explode(*args, **kwargs):
        raise RuntimeError("the app went away")

    monkeypatch.setattr(backup, "exercise", explode)
    monkeypatch.setattr(
        backup.sys,
        "argv",
        ["backup.py", "checkout", "make_schema_change_safely", "rds_postgres", str(tmp_path)],
    )

    with pytest.raises(RuntimeError, match="the app went away"):
        backup.main()
    assert source.deleted == 1
