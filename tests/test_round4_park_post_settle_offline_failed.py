"""Setup does not end a fresh install on a synced table its own park knocked offline.

2026-10-13 (v1.2.0, a fresh install). Setup parked Round 4's new pipeline at 15:59:25Z, 30 s
into its first update's RUNNING and after two healthy reads. The baseline had been synced by
15:59:07Z. Both views of the synced table then read SYNCED_TABLE_OFFLINE_FAILED, and the closing
check refused the install at about 16:42Z. By about 17:00Z, with nothing else done, both views
read SYNCED_TABLE_ONLINE_PIPELINE_FAILED, the shape every settled park leaves.

So setup now:
1. lets a sync run healthy for a minimum before it parks;
2. waits for the table to leave OFFLINE_FAILED after the park;
3. failing that, starts the pipeline again once, settles and re-parks it.

Only a table still offline after all of that reaches the closing check, which refuses it as
before.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from server import lifecycle

PIPELINE = "pipe-1"
NAMES = {
    "synced_table_id": "main.anti_demo_online_ad_test_001.model_scores",
    "resource_name": "test_synced",
}
MANIFEST = SimpleNamespace(databricks=SimpleNamespace(profile="profile"), round4=None)
OFFLINE = "SYNCED_TABLE_OFFLINE_FAILED"
PARKED = "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"
SYNCING = "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE"


class TimedWorkspace:
    """A pipeline and both views of its synced table, driven by a fake clock.

    Running, the sync reads healthy from ``healthy_after`` seconds into the update. Each stop
    parks the pipeline and leaves ``offline_views`` reading OFFLINE_FAILED for the next entry
    of ``offline_for`` seconds, then ONLINE_PIPELINE_FAILED.
    """

    def __init__(
        self,
        monkeypatch,
        *,
        offline_for: list[float],
        offline_views: frozenset[str] = frozenset({"postgres", "database"}),
        healthy_after: float = 6.0,
        start_fails: bool = False,
    ) -> None:
        self.now = 0.0
        self.running = True
        self.running_since = 0.0
        self.healthy_after = healthy_after
        self.offline_for = list(offline_for)
        self.offline_views = offline_views
        self.offline_until: float | None = None
        self.start_fails = start_fails
        self.stops: list[float] = []
        self.starts: list[float] = []
        monkeypatch.setattr(lifecycle, "_round4_get_pipeline", self.get_pipeline)
        monkeypatch.setattr(lifecycle, "_round4_get_synced_table", self.get_postgres_table)
        monkeypatch.setattr(lifecycle, "_round4_get_database_synced_table", self.get_database_table)
        monkeypatch.setattr(lifecycle, "_round4_pipeline_verb", self.verb)
        monkeypatch.setattr(lifecycle, "_start_round4_pipeline", self.start)
        monkeypatch.setattr(lifecycle.time, "monotonic", lambda: self.now)
        monkeypatch.setattr(lifecycle.time, "sleep", self.sleep)

    def _state(self, view: str) -> tuple[str, bool]:
        if self.running:
            healthy = self.now - self.running_since >= self.healthy_after
            return (SYNCING, True) if healthy else ("SYNCED_TABLE_ONLINE", False)
        if (
            self.offline_until is not None
            and self.now < self.offline_until
            and view in self.offline_views
        ):
            return OFFLINE, False
        return PARKED, False

    def get_pipeline(self, _manifest, pipeline_id):
        assert pipeline_id == PIPELINE
        if self.running:
            return {"state": "RUNNING", "latest_updates": [{"update_id": "u", "state": "RUNNING"}]}
        return {"state": "IDLE", "latest_updates": [{"update_id": "u", "state": "CANCELED"}]}

    def get_database_table(self, _manifest, names):
        assert names is NAMES
        state, continuous = self._state("database")
        status = {"detailed_state": state}
        if continuous:
            status["continuous_update_status"] = {"last_processed_commit_version": 2}
        return {"data_synchronization_status": status}

    def get_postgres_table(self, _manifest, names):
        assert names is NAMES
        state, _ = self._state("postgres")
        return {"status": {"detailed_state": state}}

    def verb(self, _manifest, pipeline_id, verb):
        assert (pipeline_id, verb) == (PIPELINE, "stop")
        self.stops.append(self.now)
        self.running = False
        self.offline_until = self.now + (self.offline_for.pop(0) if self.offline_for else 0.0)

    def start(self, _manifest, names, pipeline_id):
        assert (names, pipeline_id) == (NAMES, PIPELINE)
        self.starts.append(self.now)
        if self.start_fails:
            raise RuntimeError("the repair's update failed")
        self.running, self.running_since = True, self.now
        # The real start returns once the sync reads healthy.
        self.now += self.healthy_after

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def offline(self) -> bool:
        return lifecycle._round4_synced_table_reads_offline_failed(MANIFEST, NAMES)


def test_a_new_pipeline_is_parked_only_after_a_minimum_of_healthy_sync(monkeypatch) -> None:
    workspace = TimedWorkspace(monkeypatch, offline_for=[0.0])

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert len(workspace.stops) == 1 and workspace.starts == []
    # Two healthy reads came at about 6 s and 9 s. The park waited for the minimum after them.
    assert (
        workspace.stops[0]
        >= workspace.healthy_after + lifecycle._ROUND4_PARK_MIN_HEALTHY_SECONDS
    )


def test_a_table_its_park_left_offline_is_waited_out(monkeypatch, capsys) -> None:
    workspace = TimedWorkspace(monkeypatch, offline_for=[30.0])

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert len(workspace.stops) == 1 and workspace.starts == []
    assert workspace.now >= workspace.stops[0] + 30.0
    assert not workspace.offline()
    assert "REPAIR" not in capsys.readouterr().out


@pytest.mark.parametrize("view", ["postgres", "database"])
def test_either_view_alone_reading_offline_is_waited_out(monkeypatch, view) -> None:
    workspace = TimedWorkspace(
        monkeypatch, offline_for=[30.0], offline_views=frozenset({view})
    )

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert len(workspace.stops) == 1 and workspace.starts == []
    assert workspace.now >= workspace.stops[0] + 30.0
    assert not workspace.offline()


def test_a_table_still_offline_after_the_wait_is_started_settled_and_parked_again(
    monkeypatch, capsys
) -> None:
    # The first park leaves it offline for as long as 2026-10-13's (more than 40 minutes);
    # the re-park, of a sync that ran, leaves it online.
    workspace = TimedWorkspace(monkeypatch, offline_for=[3000.0, 0.0])

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert len(workspace.starts) == 1 and len(workspace.stops) == 2
    assert workspace.starts[0] >= workspace.stops[0] + lifecycle._ROUND4_POST_PARK_SETTLE_SECONDS
    # The re-park also waited out its minimum of healthy sync.
    assert workspace.stops[1] >= workspace.starts[0] + lifecycle._ROUND4_PARK_MIN_HEALTHY_SECONDS
    assert not workspace.offline()
    assert capsys.readouterr().out.count("REPAIR") == 1


def test_the_repair_is_bounded_and_then_left_to_the_closing_check(monkeypatch, capsys) -> None:
    workspace = TimedWorkspace(monkeypatch, offline_for=[3000.0, 3000.0])

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert len(workspace.starts) == lifecycle._ROUND4_PARK_REPAIRS == 1
    assert len(workspace.stops) == 2
    assert workspace.offline()
    assert "leaving it to the closing check" in capsys.readouterr().out


def test_a_repair_whose_start_fails_parks_the_pipeline_as_it_is(monkeypatch, capsys) -> None:
    workspace = TimedWorkspace(monkeypatch, offline_for=[3000.0], start_fails=True)

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert len(workspace.starts) == 1 and len(workspace.stops) == 1
    assert "repair start failed (RuntimeError); parking it as it is" in capsys.readouterr().out


def test_a_park_without_a_carry_neither_waits_nor_repairs(monkeypatch) -> None:
    # A carry that failed parks at once (setup's own rule), so its park repairs nothing.
    workspace = TimedWorkspace(monkeypatch, offline_for=[3000.0])

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=None)

    assert len(workspace.stops) == 1 and workspace.starts == []
    assert workspace.now < lifecycle._ROUND4_POST_PARK_SETTLE_SECONDS
