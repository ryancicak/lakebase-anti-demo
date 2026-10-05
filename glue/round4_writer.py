"""Round 4's AWS lane, run by AWS Glue: the lakehouse table into one competitor's database.

AWS moves the data. This script runs as an AWS Glue 5.0 streaming job (Spark with the open-source
Delta reader), reads Round 4's Delta table straight from its S3 location, and writes each change
into one competitor's target table over JDBC. Two jobs run it, one per competitor. The app starts a
run at the bell and stops it once the bout's row has been put back.

**Every run gets a fresh checkpoint.** No run depends on an earlier run's checkpoint, or on change
files older than itself, which Delta's vacuum is free to remove. A run started at the bell is told
the version just after the one every destination was verified to hold, and reads the change feed
from there. That is what a checkpointed stream resuming would read, and what Lakebase's own
pipeline reads, so its first batch is the bout's own change. Any other run (the installer's proof,
or one carrying a restored row back) opens with the table's current snapshot and then follows every
later commit.

**Changes are reduced by the rules in docs/design/v1.1-rounds-4-6-aws.md** ("Round 4 CDF change
reduction"), and applied through a ledger on the target row: the source's Delta table ID and the
highest commit version applied, with a delete kept as a tombstone. Each key is one guarded upsert,
and a whole batch commits in one transaction, so replaying a batch changes nothing.

The reduction and the SQL are plain Python and import without Spark, so the tests exercise them
directly. Everything that needs Spark or Glue is inside `main()`.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

#: Every argument a run reads. The job's default arguments carry all of them. A run started by the
#: app overrides `run_tag`, `source_table_id` and, at the bell, `starting_version`;
#: `source_table_id` is read from the table itself when a run is started by hand with the default.
ARGUMENTS = (
    "source_path",
    "source_table_id",
    "starting_version",
    "bucket",
    "competitor",
    "connection_name",
    "database",
    "target_schema",
    "target_table",
    "trigger_seconds",
    "run_tag",
)

#: The default `source_table_id`: read the ID from the Delta log at start.
READ_TABLE_ID_AT_START = "read-at-start"

#: The default `starting_version`: open the change feed with the table's current snapshot.
FROM_SNAPSHOT = "snapshot"

#: How long a run told a starting version waits for that version to be committed. A cold run gets
#: here long after the bell's commit has landed; the bound is for a commit that never does, when
#: the bout has already failed and settling stops the run.
COMMIT_WAIT_SECONDS = 600.0
COMMIT_POLL_SECONDS = 0.5

#: The source's primary key and the image the application reads.
KEY_COLUMN = "entity_id"
IMAGE_COLUMNS = ("score", "model_version", "proof_nonce")

#: Precedence between *different* change types at one version. It never picks between two rows of
#: the same type, and a pre-image is never applied at all.
RANK = {"insert": 1, "update_postimage": 2, "delete": 3}
PREIMAGE = "update_preimage"

#: How many times a batch is applied before the run fails. The apply is idempotent (every write is
#: guarded by the ledger), so a transient network or failover error is simply tried again.
APPLY_ATTEMPTS = 3
APPLY_RETRY_SECONDS = 1.0


class UnresolvableChangeError(RuntimeError):
    """A change the reduction rules cannot resolve, so the batch must fail rather than guess."""


def _image(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in IMAGE_COLUMNS)


def reduce_changes(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[str, int]]]:
    """Reduce one batch of change rows to at most one change per key.

    Returns the winning change for every key it can resolve, and the ``(key, version)`` pairs it
    cannot. For those the caller reads the authoritative snapshot of the key as of that version.

    - Pre-images are dropped before anything else.
    - Only the key's highest ``_commit_version`` in the batch matters. A lower one is superseded,
      and a delete at the highest version deletes the key, whatever lower versions held.
    - At that version, every row of one change type must carry the same image. Differing images of
      one type are unorderable: the change feed exposes no order inside a commit. The key is then
      unresolved, never decided by arrival order.
    - Otherwise ``delete`` outranks ``update_postimage``, which outranks ``insert``.
    """

    by_key: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        change_type = row.get("_change_type")
        if change_type == PREIMAGE:
            continue
        if change_type not in RANK:
            raise UnresolvableChangeError(f"unknown change type {change_type!r}")
        key = row.get(KEY_COLUMN)
        if not isinstance(key, str) or not key:
            raise UnresolvableChangeError("a change row has no key")
        version = row.get("_commit_version")
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise UnresolvableChangeError(f"change for {key!r} has no valid commit version")
        by_key.setdefault(key, []).append(row)

    winners: list[dict[str, Any]] = []
    unresolved: list[tuple[str, int]] = []
    for key in sorted(by_key):
        changes = by_key[key]
        top = max(int(change["_commit_version"]) for change in changes)
        images_by_type: dict[str, tuple[Any, ...]] = {}
        kept_by_type: dict[str, Mapping[str, Any]] = {}
        unorderable = False
        for change in changes:
            if int(change["_commit_version"]) != top:
                continue
            change_type = str(change["_change_type"])
            image = _image(change)
            if change_type not in images_by_type:
                images_by_type[change_type] = image
                kept_by_type[change_type] = change
            elif images_by_type[change_type] != image:
                unorderable = True
        if unorderable:
            unresolved.append((key, top))
            continue
        winner = max(kept_by_type.values(), key=lambda change: RANK[str(change["_change_type"])])
        winners.append(
            {
                KEY_COLUMN: key,
                "_change_type": str(winner["_change_type"]),
                "_commit_version": top,
                **{column: winner.get(column) for column in IMAGE_COLUMNS},
            }
        )
    return winners, unresolved


def resolve_from_snapshot(
    key: str,
    version: int,
    snapshot_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """The change for an unresolved key, from the table's own rows for it as of ``version``.

    No row means the key did not exist at that version, so it is deleted. One row is its image.
    More than one means the source holds duplicate keys, which no primary-key target can mirror,
    so the batch fails.
    """

    if len(snapshot_rows) > 1:
        raise UnresolvableChangeError(
            f"the source holds {len(snapshot_rows)} rows for {key!r} at version {version}"
        )
    if not snapshot_rows:
        return {
            KEY_COLUMN: key,
            "_change_type": "delete",
            "_commit_version": version,
            **{column: None for column in IMAGE_COLUMNS},
        }
    row = snapshot_rows[0]
    return {
        KEY_COLUMN: key,
        "_change_type": "update_postimage",
        "_commit_version": version,
        **{column: row.get(column) for column in IMAGE_COLUMNS},
    }


def _quoted(identifier: str) -> str:
    if not identifier or "\x00" in identifier:
        raise ValueError("an identifier is empty or holds a NUL")
    return '"' + identifier.replace('"', '""') + '"'


def ledger_statements(schema: str, table: str) -> tuple[str, str]:
    """The guarded upsert and the guarded tombstone for the ledger table.

    A change applies when it comes from a different Delta table (the source was re-created, and its
    versions restarted) or carries a higher commit version than the row holds. Anything else is a
    duplicate or a late replay, and changes nothing.
    """

    target = f"{_quoted(schema)}.{_quoted(table)}"
    guard = (
        f"WHERE {target}.delta_table_id IS DISTINCT FROM EXCLUDED.delta_table_id "
        f"OR {target}.delta_commit_version < EXCLUDED.delta_commit_version"
    )
    columns = (
        "entity_id, score, model_version, proof_nonce, delta_table_id, delta_commit_version, "
        "deleted, applied_at"
    )
    upsert = (
        f"INSERT INTO {target} ({columns}) VALUES (?, ?, ?, ?, ?, ?, false, clock_timestamp()) "
        "ON CONFLICT (entity_id) DO UPDATE SET score = EXCLUDED.score, "
        "model_version = EXCLUDED.model_version, proof_nonce = EXCLUDED.proof_nonce, "
        "delta_table_id = EXCLUDED.delta_table_id, "
        "delta_commit_version = EXCLUDED.delta_commit_version, deleted = false, "
        f"applied_at = EXCLUDED.applied_at {guard}"
    )
    tombstone = (
        f"INSERT INTO {target} ({columns}) VALUES (?, NULL, NULL, NULL, ?, ?, true, "
        "clock_timestamp()) ON CONFLICT (entity_id) DO UPDATE SET score = NULL, "
        "model_version = NULL, proof_nonce = NULL, delta_table_id = EXCLUDED.delta_table_id, "
        "delta_commit_version = EXCLUDED.delta_commit_version, deleted = true, "
        f"applied_at = EXCLUDED.applied_at {guard}"
    )
    return upsert, tombstone


def apply_changes(
    connection: Any,
    changes: Sequence[Mapping[str, Any]],
    *,
    table_id: str,
    upsert: str,
    tombstone: str,
    double_type: int,
) -> None:
    """Apply one reduced batch in one transaction on a JDBC connection with autocommit off.

    ``double_type`` is ``java.sql.Types.DOUBLE``, passed in so this runs without a JVM in tests.
    """

    try:
        for change in changes:
            if change["_change_type"] == "delete":
                statement = connection.prepareStatement(tombstone)
                statement.setString(1, change[KEY_COLUMN])
                statement.setString(2, table_id)
                statement.setLong(3, int(change["_commit_version"]))
            else:
                statement = connection.prepareStatement(upsert)
                statement.setString(1, change[KEY_COLUMN])
                if change["score"] is None:
                    statement.setNull(2, double_type)
                else:
                    statement.setDouble(2, float(change["score"]))
                statement.setString(3, change["model_version"])
                statement.setString(4, change["proof_nonce"])
                statement.setString(5, table_id)
                statement.setLong(6, int(change["_commit_version"]))
            try:
                statement.executeUpdate()
            finally:
                statement.close()
        connection.commit()
    except BaseException:
        try:
            connection.rollback()
        except Exception:  # noqa: BLE001 - the connection is discarded either way
            pass
        raise


def apply_with_retry(
    connect: Callable[[], Any],
    discard: Callable[[], None],
    changes: Sequence[Mapping[str, Any]],
    *,
    table_id: str,
    upsert: str,
    tombstone: str,
    double_type: int,
    attempts: int = APPLY_ATTEMPTS,
    retry_seconds: float = APPLY_RETRY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Apply a batch, reconnecting and trying again on failure, within a bound.

    Safe to repeat because every write is guarded by the ledger. After the last attempt the error
    is raised, which fails the run: a lane that cannot write is not left pretending to run.
    """

    for attempt in range(1, attempts + 1):
        try:
            apply_changes(
                connect(),
                changes,
                table_id=table_id,
                upsert=upsert,
                tombstone=tombstone,
                double_type=double_type,
            )
            return
        except Exception:
            discard()
            if attempt == attempts:
                raise
            sleep(retry_seconds * attempt)


def checkpoint_location(bucket: str, competitor: str, run_tag: str, started_at: datetime) -> str:
    """A checkpoint no other run has used: this run's own start time and tag."""

    stamp = started_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"s3://{bucket}/checkpoints/{competitor}/{stamp}-{run_tag}/"


def marker_key(competitor: str, run_tag: str) -> str:
    """Where a run records when its stream started and when its first batch landed."""

    return f"markers/{competitor}/{run_tag}.json"


def stream_options(starting_version: str) -> dict[str, str]:
    """The change-feed options for a run: from a version, or opening with the snapshot."""

    options = {"readChangeFeed": "true"}
    if starting_version != FROM_SNAPSHOT:
        version = int(starting_version)
        if version < 0:
            raise ValueError(f"A starting version cannot be negative: {starting_version}")
        options["startingVersion"] = str(version)
    return options


def commit_log_key(source_path: str, version: int) -> tuple[str, str]:
    """The bucket and key of the Delta log entry that commits ``version`` of the table."""

    scheme, _, rest = source_path.partition("://")
    bucket, _, prefix = rest.partition("/")
    if scheme not in {"s3", "s3a"} or not bucket or not prefix.strip("/"):
        raise ValueError(f"Not an S3 table location: {source_path}")
    return bucket, f"{prefix.strip('/')}/_delta_log/{version:020d}.json"


def wait_for_commit(
    committed: Callable[[str, str], bool],
    source_path: str,
    version: int,
    *,
    timeout_seconds: float = COMMIT_WAIT_SECONDS,
    poll_seconds: float = COMMIT_POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Return once ``version`` is committed, so a stream never starts ahead of its bout."""

    bucket, key = commit_log_key(source_path, version)
    deadline = clock() + timeout_seconds
    while not committed(bucket, key):
        if clock() >= deadline:
            raise TimeoutError(
                f"Version {version} of {source_path} was not committed within "
                f"{timeout_seconds:.0f}s"
            )
        sleep(poll_seconds)


def main(argv: Sequence[str]) -> None:  # pragma: no cover - runs only inside AWS Glue
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    args = getResolvedOptions(list(argv), list(ARGUMENTS))
    started_at = datetime.now(UTC)
    spark = SparkSession.builder.getOrCreate()
    glue = GlueContext(spark.sparkContext)
    jvm = spark.sparkContext._jvm
    source_path = args["source_path"]
    competitor = args["competitor"]
    run_tag = args["run_tag"]
    bucket = args["bucket"]
    options = stream_options(args["starting_version"])

    table_id = args["source_table_id"]
    if table_id == READ_TABLE_ID_AT_START:
        from delta.tables import DeltaTable

        table_id = str(DeltaTable.forPath(spark, source_path).detail().first()["id"])

    conf = glue.extract_jdbc_conf(args["connection_name"])
    host_and_port = conf["url"].split("//", 1)[1].split("/", 1)[0]
    url = (
        f"jdbc:postgresql://{host_and_port}/{args['database']}"
        "?sslmode=require&ApplicationName=lakebase-anti-demo-round4-glue"
    )
    upsert, tombstone = ledger_statements(args["target_schema"], args["target_table"])
    held: dict[str, Any] = {"connection": None}

    def connect() -> Any:
        current = held["connection"]
        if current is None or current.isClosed():
            current = jvm.java.sql.DriverManager.getConnection(url, conf["user"], conf["password"])
            current.setAutoCommit(False)
            held["connection"] = current
        return current

    def discard() -> None:
        current, held["connection"] = held["connection"], None
        if current is not None:
            try:
                current.close()
            except Exception:  # noqa: BLE001 - it is being replaced either way
                pass

    s3 = boto3.client("s3")
    marker: dict[str, Any] = {
        "competitor": competitor,
        "run_tag": run_tag,
        "run_started_at": started_at.isoformat(),
        "starting_version": args["starting_version"],
    }
    # The stream's thread records the first batch while the driver's records the stream start,
    # and either can land first.
    marker_lock = threading.Lock()

    def write_marker(**fields: Any) -> None:
        with marker_lock:
            marker.update(fields)
            body = json.dumps(marker).encode()
            try:
                s3.put_object(
                    Bucket=bucket,
                    Key=marker_key(competitor, run_tag),
                    Body=body,
                    ContentType="application/json",
                )
            except Exception:  # noqa: BLE001 - evidence only; the data path never waits on it
                pass

    def write(batch: Any, batch_id: int) -> None:
        rows = [
            row.asDict()
            for row in batch.select(
                KEY_COLUMN, *IMAGE_COLUMNS, "_change_type", "_commit_version"
            ).collect()
        ]
        winners, unresolved = reduce_changes(rows)
        for key, version in unresolved:
            snapshot_rows = [
                row.asDict()
                for row in spark.read.format("delta")
                .option("versionAsOf", version)
                .load(source_path)
                .where(col(KEY_COLUMN) == key)
                .select(KEY_COLUMN, *IMAGE_COLUMNS)
                .collect()
            ]
            winners.append(resolve_from_snapshot(key, version, snapshot_rows))
        if not winners:
            return
        apply_with_retry(
            connect,
            discard,
            winners,
            table_id=table_id,
            upsert=upsert,
            tombstone=tombstone,
            double_type=jvm.java.sql.Types.DOUBLE,
        )
        if "first_batch_applied_at" not in marker:
            write_marker(
                first_batch_applied_at=datetime.now(UTC).isoformat(),
                first_batch_id=batch_id,
                first_batch_changes=len(winners),
                first_batch_max_version=max(int(w["_commit_version"]) for w in winners),
            )

    def committed(bucket_name: str, key: str) -> bool:
        # A listing, not a HEAD: the role may list only the table's prefix, and a HEAD of a key
        # that does not exist yet can come back as a refusal rather than a miss.
        listed = s3.list_objects_v2(Bucket=bucket_name, Prefix=key, MaxKeys=1)
        return any(item.get("Key") == key for item in listed.get("Contents", []))

    if "startingVersion" in options:
        wait_for_commit(committed, source_path, int(options["startingVersion"]))

    query = (
        spark.readStream.format("delta")
        .options(**options)
        .load(source_path)
        .writeStream.foreachBatch(write)
        .option("checkpointLocation", checkpoint_location(bucket, competitor, run_tag, started_at))
        .trigger(processingTime=f"{int(args['trigger_seconds'])} seconds")
        .queryName(f"round4-{competitor}")
        .start()
    )
    write_marker(stream_started_at=datetime.now(UTC).isoformat())
    query.awaitTermination()


if __name__ == "__main__":  # pragma: no cover - runs only inside AWS Glue
    main(sys.argv)
