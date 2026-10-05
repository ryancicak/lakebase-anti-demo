"""Round 6's AWS lane, run by AWS Glue: DMS's change files into a Delta table in the lakehouse.

AWS moves the data. AWS DMS captures each change to Round 6's source table from one competitor's
r6 database, over logical replication, and writes it as a Parquet file under this job's source
prefix, carrying DMS's operation (`I`, `U` or `D`) and commit timestamp beside the row. This script
runs as an AWS Glue 5.0 job (Spark with the open-source Delta writer) and streams those files into a
Delta table, one row per change, which Databricks reads as a Unity Catalog external table on the
same SQL warehouse as Lakebase's lane. Two jobs run it, one per competitor. The app starts a run at
the bell and stops it once the bout's row has been read.

**The checkpoint is standing.** Unlike Round 4's writer, every run resumes the same checkpoint, so a
run started at the bell reads exactly the files DMS wrote since the last run, the bout's own change
among them. DMS's files are append-only and are never rewritten, so nothing a run has read can
change underneath it.

**The schema is declared, not inferred.** Inferring it needs files already present and would carry
whatever type the first file happened to hold; declaring it makes a type DMS did not write a failed
run instead of a silently different table. The columns are DMS's two, then the source table's, in
the order DMS writes them (docs/design/v1.1-rounds-4-6-aws.md, section 4).

Everything that needs Spark or Glue is inside `main()`; the rest imports without either, so the
tests exercise it directly.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence

#: Every argument a run reads. The job's default arguments carry all of them.
ARGUMENTS = ("source", "target", "checkpoint", "competitor", "trigger_seconds")

#: The Parquet columns DMS writes for Round 6's source table, in order, with their Spark types:
#: its operation and commit timestamp (`TimestampColumnName`), then the table's own columns.
SOURCE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("Op", "string"),
    ("dms_commit_ts", "string"),
    ("order_id", "string"),
    ("sku", "string"),
    ("store", "string"),
    ("quantity", "int"),
    ("total_cents", "int"),
    ("status", "string"),
    ("proof_nonce", "string"),
)

#: When the change reached the lakehouse, stamped by this job as it writes the batch.
APPLIED_COLUMN = "applied_at"


def schema_ddl(columns: Sequence[tuple[str, str]] = SOURCE_COLUMNS) -> str:
    """The source schema as Spark DDL, so the reader needs no files to know it."""

    return ", ".join(f"`{name}` {kind}" for name, kind in columns)


def trigger_interval(seconds: str) -> str:
    """The micro-batch interval, as Spark's `processingTime` takes it.

    Refuses anything but a positive whole number of seconds: a zero or a typo would either spin the
    job or make it wait far longer than the bout does.
    """

    value = int(seconds)
    if value <= 0:
        raise ValueError("trigger_seconds must be a positive whole number")
    return f"{value} seconds"


def s3_location(value: str) -> str:
    """An `s3://bucket/prefix` location, refusing anything a run could write outside its bucket."""

    if not value.startswith("s3://") or len(value.removeprefix("s3://").split("/", 1)) < 2:
        raise ValueError(f"not an s3://bucket/prefix location: {value!r}")
    return value.rstrip("/")


def main(argv: Sequence[str]) -> None:
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    args = getResolvedOptions(list(argv), list(ARGUMENTS))
    source = s3_location(args["source"]) + "/"
    target = s3_location(args["target"])
    checkpoint = s3_location(args["checkpoint"])
    interval = trigger_interval(args["trigger_seconds"])

    spark = SparkSession.builder.getOrCreate()
    changes = (
        spark.readStream.schema(schema_ddl())
        .format("parquet")
        .load(source)
        .withColumn(APPLIED_COLUMN, F.current_timestamp())
    )
    query = (
        changes.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=interval)
        .start(target)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main(sys.argv)
