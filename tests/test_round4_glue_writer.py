"""Round 4's AWS Glue writer: its reduction rules, its ledger guard and its one-transaction apply.

The script runs only inside AWS Glue, so everything it decides is kept in plain Python and tested
here without Spark. Every rule in docs/design/v1.1-rounds-4-6-aws.md ("Round 4 CDF change
reduction") has its own test, because the branch prototype broke one of them: it kept the first of
two differing same-type images.
"""

from __future__ import annotations

import importlib.util
import itertools
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "glue" / "round4_writer.py"


def _load():
    spec = importlib.util.spec_from_file_location("round4_glue_writer", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


writer = _load()


def change(change_type, version, *, key="customer-0001", score=0.81, model="risk-v1", nonce="n1"):
    return {
        "entity_id": key,
        "score": score,
        "model_version": model,
        "proof_nonce": nonce,
        "_change_type": change_type,
        "_commit_version": version,
    }


def test_the_script_imports_without_spark_or_glue():
    assert "pyspark" not in sys.modules
    assert "awsglue" not in sys.modules


def test_a_preimage_is_never_applied():
    winners, unresolved = writer.reduce_changes([change("update_preimage", 7, nonce="old")])

    assert winners == []
    assert unresolved == []


def test_an_update_applies_its_postimage_not_its_preimage():
    winners, _ = writer.reduce_changes(
        [
            change("update_preimage", 7, score=0.25, model="risk-v0", nonce="baseline"),
            change("update_postimage", 7, score=0.81, model="risk-v1", nonce="bout"),
        ]
    )

    assert winners == [change("update_postimage", 7, score=0.81, model="risk-v1", nonce="bout")]


def test_only_the_highest_version_of_a_key_counts():
    winners, _ = writer.reduce_changes(
        [
            change("update_postimage", 9, nonce="later"),
            change("update_postimage", 8, nonce="earlier"),
        ]
    )

    assert [(w["_commit_version"], w["proof_nonce"]) for w in winners] == [(9, "later")]


def test_a_delete_at_the_highest_version_deletes_the_key_whatever_came_before():
    winners, _ = writer.reduce_changes(
        [change("insert", 3, nonce="kept"), change("update_postimage", 4), change("delete", 5)]
    )

    assert [(w["_change_type"], w["_commit_version"]) for w in winners] == [("delete", 5)]


@pytest.mark.parametrize(
    ("types", "expected"),
    [
        (("insert", "update_postimage"), "update_postimage"),
        (("update_postimage", "delete"), "delete"),
        (("insert", "delete"), "delete"),
        (("insert", "update_postimage", "delete"), "delete"),
    ],
)
def test_different_types_at_one_version_resolve_by_fixed_precedence(types, expected):
    for order in itertools.permutations(types):
        winners, unresolved = writer.reduce_changes(
            [change(change_type, 6, nonce=change_type) for change_type in order]
        )

        assert unresolved == []
        assert [w["_change_type"] for w in winners] == [expected]


def test_two_differing_images_of_one_type_are_unresolved_never_first_come():
    rows = [
        change("update_postimage", 6, score=0.81, nonce="a"),
        change("update_postimage", 6, score=0.33, nonce="b"),
    ]

    for order in (rows, list(reversed(rows))):
        winners, unresolved = writer.reduce_changes(order)

        assert winners == []
        assert unresolved == [("customer-0001", 6)]


def test_a_same_type_conflict_is_unresolved_even_beside_a_higher_ranked_type():
    winners, unresolved = writer.reduce_changes(
        [
            change("insert", 6, nonce="a"),
            change("insert", 6, nonce="b"),
            change("delete", 6),
        ]
    )

    assert winners == []
    assert unresolved == [("customer-0001", 6)]


def test_identical_images_of_one_type_are_one_change():
    winners, unresolved = writer.reduce_changes(
        [change("update_postimage", 6), change("update_postimage", 6)]
    )

    assert unresolved == []
    assert winners == [change("update_postimage", 6)]


def test_a_conflict_at_a_superseded_version_does_not_matter():
    winners, unresolved = writer.reduce_changes(
        [
            change("update_postimage", 6, nonce="a"),
            change("update_postimage", 6, nonce="b"),
            change("update_postimage", 7, nonce="final"),
        ]
    )

    assert unresolved == []
    assert [w["proof_nonce"] for w in winners] == ["final"]


def test_a_run_start_snapshot_is_one_insert_per_key():
    winners, unresolved = writer.reduce_changes(
        [
            change("insert", 42, key="customer-0001", nonce="bout"),
            change("insert", 42, key="spike-1", nonce="other"),
        ]
    )

    assert unresolved == []
    assert [(w["entity_id"], w["_change_type"], w["_commit_version"]) for w in winners] == [
        ("customer-0001", "insert", 42),
        ("spike-1", "insert", 42),
    ]


def test_keys_are_reduced_independently():
    winners, unresolved = writer.reduce_changes(
        [
            change("update_postimage", 5, key="a", nonce="a5"),
            change("update_postimage", 6, key="b", nonce="b6a"),
            change("update_postimage", 6, key="b", nonce="b6b"),
        ]
    )

    assert [w["proof_nonce"] for w in winners] == ["a5"]
    assert unresolved == [("b", 6)]


@pytest.mark.parametrize(
    "row",
    [
        change("merge", 3),
        {**change("insert", 3), "entity_id": ""},
        {**change("insert", 3), "entity_id": None},
        {**change("insert", 3), "_commit_version": None},
        {**change("insert", 3), "_commit_version": -1},
        {**change("insert", 3), "_commit_version": True},
    ],
)
def test_a_malformed_change_fails_the_batch(row):
    with pytest.raises(writer.UnresolvableChangeError):
        writer.reduce_changes([row])


def test_an_unresolved_key_absent_from_its_snapshot_is_deleted():
    resolved = writer.resolve_from_snapshot("customer-0001", 6, [])

    assert resolved["_change_type"] == "delete"
    assert resolved["_commit_version"] == 6


def test_an_unresolved_key_takes_its_one_snapshot_row():
    resolved = writer.resolve_from_snapshot(
        "customer-0001",
        6,
        [{"entity_id": "customer-0001", "score": 0.33, "model_version": "v", "proof_nonce": "n"}],
    )

    assert resolved == {
        "entity_id": "customer-0001",
        "_change_type": "update_postimage",
        "_commit_version": 6,
        "score": 0.33,
        "model_version": "v",
        "proof_nonce": "n",
    }


def test_duplicate_source_keys_fail_the_batch():
    with pytest.raises(writer.UnresolvableChangeError):
        writer.resolve_from_snapshot("customer-0001", 6, [{"score": 1.0}, {"score": 2.0}])


def test_the_ledger_guard_takes_a_new_table_or_a_higher_version_and_nothing_else():
    upsert, tombstone = writer.ledger_statements("round4", "model_score_ledger")

    for statement in (upsert, tombstone):
        assert statement.startswith('INSERT INTO "round4"."model_score_ledger"')
        assert "ON CONFLICT (entity_id) DO UPDATE" in statement
        assert statement.endswith(
            'WHERE "round4"."model_score_ledger".delta_table_id IS DISTINCT FROM '
            "EXCLUDED.delta_table_id "
            'OR "round4"."model_score_ledger".delta_commit_version < '
            "EXCLUDED.delta_commit_version"
        )
    assert "deleted = false" in upsert
    assert "deleted = true" in tombstone
    assert "score = NULL" in tombstone


def test_ledger_identifiers_are_quoted():
    upsert, _ = writer.ledger_statements('ro"und4', "t")

    assert upsert.startswith('INSERT INTO "ro""und4"."t"')
    with pytest.raises(ValueError):
        writer.ledger_statements("", "t")


class FakeStatement:
    def __init__(self, connection, sql):
        self.connection = connection
        self.sql = sql
        self.parameters: dict[int, object] = {}
        self.closed = False

    def setString(self, index, value):  # noqa: N802 - the JDBC method name
        self.parameters[index] = value

    def setDouble(self, index, value):  # noqa: N802
        self.parameters[index] = ("double", value)

    def setLong(self, index, value):  # noqa: N802
        self.parameters[index] = ("long", value)

    def setNull(self, index, sql_type):  # noqa: N802
        self.parameters[index] = ("null", sql_type)

    def executeUpdate(self):  # noqa: N802
        if (
            self.connection.fail_on is not None
            and len(self.connection.executed) == self.connection.fail_on
        ):
            raise RuntimeError("the database went away")
        self.connection.executed.append((self.sql, dict(self.parameters)))
        return 1

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, *, fail_on=None):
        self.fail_on = fail_on
        self.executed: list[tuple[str, dict[int, object]]] = []
        self.statements: list[FakeStatement] = []
        self.commits = 0
        self.rollbacks = 0

    def prepareStatement(self, sql):  # noqa: N802
        statement = FakeStatement(self, sql)
        self.statements.append(statement)
        return statement

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


DOUBLE = 8


def _apply(connection, changes):
    writer.apply_changes(
        connection,
        changes,
        table_id="table-1",
        upsert="UPSERT",
        tombstone="TOMBSTONE",
        double_type=DOUBLE,
    )


def test_a_batch_is_one_transaction_with_every_parameter_bound():
    connection = FakeConnection()

    _apply(
        connection,
        [
            change("update_postimage", 9, score=0.81, model="risk-v1", nonce="bout"),
            change("delete", 9, key="gone"),
        ],
    )

    assert connection.executed == [
        (
            "UPSERT",
            {
                1: "customer-0001",
                2: ("double", 0.81),
                3: "risk-v1",
                4: "bout",
                5: "table-1",
                6: ("long", 9),
            },
        ),
        ("TOMBSTONE", {1: "gone", 2: "table-1", 3: ("long", 9)}),
    ]
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert all(statement.closed for statement in connection.statements)


def test_a_null_score_is_bound_as_a_typed_null():
    connection = FakeConnection()

    _apply(connection, [change("insert", 3, score=None)])

    assert connection.executed[0][1][2] == ("null", DOUBLE)


def test_a_failed_write_rolls_the_whole_batch_back():
    connection = FakeConnection(fail_on=1)

    with pytest.raises(RuntimeError):
        _apply(connection, [change("insert", 3, key="a"), change("insert", 3, key="b")])

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert all(statement.closed for statement in connection.statements)


def test_a_failed_batch_is_tried_again_on_a_fresh_connection():
    connections = [FakeConnection(fail_on=0), FakeConnection()]
    handed_out: list[FakeConnection] = []
    discarded: list[int] = []
    slept: list[float] = []

    def connect():
        handed_out.append(connections[len(handed_out)])
        return handed_out[-1]

    writer.apply_with_retry(
        connect,
        lambda: discarded.append(1),
        [change("insert", 3)],
        table_id="table-1",
        upsert="UPSERT",
        tombstone="TOMBSTONE",
        double_type=DOUBLE,
        sleep=slept.append,
    )

    assert handed_out == connections
    assert discarded == [1]
    assert slept == [writer.APPLY_RETRY_SECONDS]
    assert connections[1].commits == 1


def test_a_batch_that_keeps_failing_fails_the_run():
    discarded: list[int] = []
    slept: list[float] = []

    with pytest.raises(RuntimeError):
        writer.apply_with_retry(
            lambda: FakeConnection(fail_on=0),
            lambda: discarded.append(1),
            [change("insert", 3)],
            table_id="table-1",
            upsert="UPSERT",
            tombstone="TOMBSTONE",
            double_type=DOUBLE,
            attempts=3,
            retry_seconds=1.0,
            sleep=slept.append,
        )

    assert len(discarded) == 3
    assert slept == [1.0, 2.0]


def test_every_run_has_its_own_checkpoint():
    from datetime import UTC, datetime

    first = writer.checkpoint_location(
        "bucket", "aurora", "bout-1", datetime(2026, 9, 28, 1, 2, 3, 4, UTC)
    )
    second = writer.checkpoint_location(
        "bucket", "aurora", "bout-1", datetime(2026, 9, 28, 1, 2, 3, 5, UTC)
    )

    assert first == "s3://bucket/checkpoints/aurora/20260928T010203000004Z-bout-1/"
    assert first != second
    assert writer.marker_key("rds", "bout-1") == "markers/rds/bout-1.json"


def test_a_bout_s_run_reads_the_change_feed_from_its_version_and_any_other_from_the_snapshot():
    # Measured on the test installation (2026-09-29): opening with the snapshot put a 13-21 s
    # file-listing job on the AWS clock that Lakebase's resuming pipeline never runs.
    assert writer.stream_options("43") == {"readChangeFeed": "true", "startingVersion": "43"}
    assert writer.stream_options(writer.FROM_SNAPSHOT) == {"readChangeFeed": "true"}
    with pytest.raises(ValueError):
        writer.stream_options("-1")
    with pytest.raises(ValueError):
        writer.stream_options("latest")


def test_the_snapshot_default_is_one_spelling_in_the_writer_the_app_and_terraform():
    from server.round4_glue import FROM_SNAPSHOT, starting_version

    terraform = (REPO / "infra" / "aws" / "round4_glue.tf").read_text(encoding="utf-8")

    assert FROM_SNAPSHOT == writer.FROM_SNAPSHOT
    assert f'"--starting_version"    = "{writer.FROM_SNAPSHOT}"' in terraform
    assert "starting_version" in writer.ARGUMENTS
    assert starting_version(None) == writer.FROM_SNAPSHOT
    assert starting_version(42) == "43"


def test_a_commit_is_found_by_its_own_delta_log_entry():
    assert writer.commit_log_key("s3://root/metastore/m/tables/t", 43) == (
        "root",
        "metastore/m/tables/t/_delta_log/00000000000000000043.json",
    )
    assert writer.commit_log_key("s3://root/tables/t/", 0)[1] == (
        "tables/t/_delta_log/00000000000000000000.json"
    )
    for location in ("gs://root/tables/t", "s3://root", "s3://root/", "tables/t"):
        with pytest.raises(ValueError):
            writer.commit_log_key(location, 1)


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_a_run_waits_for_its_bout_s_commit_before_it_starts_streaming():
    clock = Clock()
    asked: list[tuple[str, str]] = []
    answers = iter([False, False, True])

    def committed(bucket, key):
        asked.append((bucket, key))
        return next(answers)

    writer.wait_for_commit(
        committed,
        "s3://root/tables/t",
        43,
        timeout_seconds=10.0,
        poll_seconds=0.5,
        clock=clock,
        sleep=clock.sleep,
    )

    assert asked == [("root", "tables/t/_delta_log/00000000000000000043.json")] * 3
    assert clock.now == 1.0


def test_a_commit_that_never_lands_fails_the_run_at_its_bound():
    clock = Clock()

    with pytest.raises(TimeoutError, match="Version 43 .* was not committed within 5s"):
        writer.wait_for_commit(
            lambda _bucket, _key: False,
            "s3://root/tables/t",
            43,
            timeout_seconds=5.0,
            poll_seconds=0.5,
            clock=clock,
            sleep=clock.sleep,
        )
    assert clock.now == 5.0
