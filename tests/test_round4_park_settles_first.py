"""Setup parks Round 4's pipeline only once its update has settled into a continuous sync.

2026-10-05, rc20. The reinstall created Round 4's pipeline at 10:34:44Z, carried the baseline,
and parked it at 10:35:19Z, three seconds after its first update reached RUNNING. The update
ended CANCELED, as every park's does, but the synced table read SYNCED_TABLE_OFFLINE_FAILED in
one of its two views, and setup's closing check refused it as a broken table, 45 minutes later.
A park of a settled sync leaves the table online with only its update cancelled, the shape
every earlier install parked into.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from server import lifecycle

PIPELINE = "pipe-1"
NAMES = {"synced_table_id": "main.anti_demo_online_ad_test_001.model_scores"}
MANIFEST = SimpleNamespace(databricks=SimpleNamespace(profile="profile"), round4=None)


def pipeline(state: str, update: str) -> dict:
    return {"state": state, "latest_updates": [{"update_id": "u1", "state": update}]}


def synced(state: str, *, continuous: bool) -> dict:
    status = {"detailed_state": state}
    if continuous:
        status["continuous_update_status"] = {"last_processed_commit_version": 2}
    return {"data_synchronization_status": status}


class Workspace:
    """Scripted pipeline and synced-table reads, a fake clock, and every verb recorded."""

    def __init__(self, monkeypatch, pipelines: list[dict], tables: list[dict]) -> None:
        self.pipelines = list(pipelines)
        self.tables = list(tables)
        self.last_pipeline = self.pipelines[-1]
        self.last_table = self.tables[-1] if self.tables else {}
        self.verbs: list[str] = []
        self.now = 0.0
        self.stopped_at_read: int | None = None
        self.reads = 0
        monkeypatch.setattr(lifecycle, "_round4_get_pipeline", self.get_pipeline)
        monkeypatch.setattr(lifecycle, "_round4_get_database_synced_table", self.get_table)
        monkeypatch.setattr(lifecycle, "_round4_pipeline_verb", self.verb)
        monkeypatch.setattr(lifecycle.time, "monotonic", lambda: self.now)
        monkeypatch.setattr(lifecycle.time, "sleep", self.sleep)

    def get_pipeline(self, _manifest, pipeline_id):
        assert pipeline_id == PIPELINE
        self.reads += 1
        if self.pipelines:
            self.last_pipeline = self.pipelines.pop(0)
        return self.last_pipeline

    def get_table(self, _manifest, names):
        assert names is NAMES
        if self.tables:
            self.last_table = self.tables.pop(0)
        return self.last_table

    def verb(self, _manifest, pipeline_id, verb):
        assert pipeline_id == PIPELINE
        self.verbs.append(verb)
        self.stopped_at_read = self.reads
        # Once stopped, the pipeline parks: idle, its update cancelled.
        self.pipelines = [pipeline("IDLE", "CANCELED")]

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_rc20s_new_pipeline_is_parked_only_once_its_sync_has_settled(monkeypatch) -> None:
    workspace = Workspace(
        monkeypatch,
        pipelines=[
            pipeline("RUNNING", "RUNNING"),  # the settle's first look: still unsettled
            pipeline("RUNNING", "SETTING_UP_TABLES"),
            pipeline("RUNNING", "RUNNING"),
            pipeline("RUNNING", "RUNNING"),
            pipeline("RUNNING", "RUNNING"),
            pipeline("RUNNING", "RUNNING"),  # the park loop's read before its stop
        ],
        tables=[
            synced("SYNCED_TABLE_PROVISIONING_INITIAL_SNAPSHOT", continuous=False),
            synced("SYNCED_TABLE_ONLINE", continuous=False),
            synced("SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE", continuous=True),
            synced("SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE", continuous=True),
        ],
    )

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert workspace.verbs == ["stop"]
    # Not until the sync read healthy twice: the stop came on the read after the fifth.
    assert workspace.stopped_at_read == 6


def test_a_park_without_a_settle_stops_at_once(monkeypatch) -> None:
    # The carry's failure path: a broken pipeline is not left billing for a settle.
    workspace = Workspace(monkeypatch, pipelines=[pipeline("RUNNING", "RUNNING")], tables=[])

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE)

    assert workspace.verbs == ["stop"]
    assert workspace.stopped_at_read == 1


def test_a_sync_that_never_settles_is_parked_after_the_bound(monkeypatch, capsys) -> None:
    workspace = Workspace(
        monkeypatch,
        pipelines=[pipeline("RUNNING", "RUNNING")],
        tables=[synced("SYNCED_TABLE_ONLINE", continuous=False)],
    )

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert workspace.verbs == ["stop"]
    assert workspace.now >= lifecycle._ROUND4_PARK_SETTLE_SECONDS
    assert "did not settle into a continuous sync" in capsys.readouterr().out


@pytest.mark.parametrize("update", ["CANCELED", "COMPLETED"])
def test_a_parked_pipeline_is_not_waited_on(monkeypatch, update):
    workspace = Workspace(monkeypatch, pipelines=[pipeline("IDLE", update)], tables=[])

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    assert workspace.now == 0.0
    assert workspace.verbs == []


def test_a_stop_in_progress_is_waited_out_without_a_settle(monkeypatch):
    workspace = Workspace(
        monkeypatch,
        pipelines=[
            pipeline("RUNNING", "STOPPING"),
            pipeline("RUNNING", "STOPPING"),
            pipeline("IDLE", "CANCELED"),
        ],
        tables=[],
    )

    lifecycle._park_round4_pipeline(MANIFEST, PIPELINE, settle_names=NAMES)

    # Two park-loop sleeps, and no settle: a stopping update is not bringing a table up.
    assert workspace.now == 3.0
    assert workspace.verbs == []


def test_the_carry_settles_only_after_it_worked(monkeypatch) -> None:
    parks: list[object] = []
    monkeypatch.setattr(lifecycle, "_round4_pipeline_parked", lambda *_: False)
    monkeypatch.setattr(lifecycle, "_repair_round4_baseline", lambda *_: 2)
    monkeypatch.setattr(
        lifecycle,
        "_park_round4_pipeline",
        lambda _manifest, _pipeline_id, *, settle_names=None: parks.append(settle_names),
    )
    monkeypatch.setattr(lifecycle, "_wait_round4_baseline", lambda *_a, **_k: ({}, {}))

    lifecycle._carry_round4_baseline(
        MANIFEST,
        NAMES,
        "warehouse",
        project_uid="p",
        branch_uid="b",
        pipeline_id=PIPELINE,
        timeout=60.0,
    )

    def failing(*_a, **_k):
        raise RuntimeError("the baseline never reached the synced table")

    monkeypatch.setattr(lifecycle, "_wait_round4_baseline", failing)
    with pytest.raises(RuntimeError, match="never reached"):
        lifecycle._carry_round4_baseline(
            MANIFEST,
            NAMES,
            "warehouse",
            project_uid="p",
            branch_uid="b",
            pipeline_id=PIPELINE,
            timeout=60.0,
        )

    assert parks == [NAMES, None]
