"""Round 4's AWS lane at install time: its writer, its target, its credential and its proof.

Terraform builds the standing pieces (``infra/aws/round4_glue.tf``). The installer does the rest,
on every setup, and each step is idempotent:

1. finds Round 4's Delta source in Unity Catalog, so Terraform can scope the Glue role to its one
   S3 prefix;
2. in each r4 database, creates the writer role, the schema it owns, the ledger table the job
   writes and the view the application reads, and sets the writer's password;
3. puts the same password on each competitor's Glue connection;
4. runs each job once, until its target reads the source's baseline exactly, and parks it.

The lane is sealed only after step 4 has passed for both competitors, so an installation whose
role, route or credential is broken fails its install rather than its first bout.
"""

from __future__ import annotations

import hashlib
import secrets
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import quote

from psycopg import sql

from .manifest import DemoManifest, Round4AwsLaneSeal, Round4AwsResources
from .model_score import ModelScoreRow
from .round4_glue import FROM_SNAPSHOT, TERMINAL_STATES, GlueLaneError, GlueWriterJob

ROUND4_WRITER_ROLE = "round4_writer"
ROUND4_TARGET_SCHEMA = "round4"
ROUND4_TARGET_TABLE = "model_score_ledger"
ROUND4_TARGET_VIEW = "model_scores"
#: The same names, qualified, for the DDL below. Plain lowercase identifiers, so they need no
#: quoting, and constants, so every table this project creates can be named by a scan.
ROUND4_LEDGER_TABLE = "round4.model_score_ledger"
ROUND4_LEDGER_VIEW = "round4.model_scores"
ROUND4_COMPETITORS = ("aurora", "rds")

#: The Glue connection's password until the installer has set the real one. Mirrors the literal
#: in ``infra/aws/round4_glue.tf``.
ROUND4_PLACEHOLDER_PASSWORD = "set-by-the-installer"

#: The script Terraform uploads, whose digest is sealed and checked by ``doctor``.
ROUND4_SCRIPT = Path(__file__).resolve().parents[1] / "glue" / "round4_writer.py"

#: How long one proof run may take from its start to its target reading the baseline. A cold
#: start took 60-120 s in the spike; the rest is room for a first run's network setup.
PROOF_TIMEOUT_SECONDS = 600.0
PROOF_POLL_SECONDS = 5.0


def script_sha256() -> str:
    return hashlib.sha256(ROUND4_SCRIPT.read_bytes()).hexdigest()


def target_statements() -> tuple[sql.Composable, ...]:
    """The writer's schema, its ledger and the view the app reads. Idempotent.

    The ledger is the target row itself: the source's Delta table ID and the highest commit
    version applied, with a delete kept as a tombstone rather than removed. The view is what
    both lanes' application reads look at: the same four columns as Lakebase's synced table,
    and on this lane only the rows that are not tombstones.
    """

    schema = sql.Identifier(ROUND4_TARGET_SCHEMA)
    writer = sql.Identifier(ROUND4_WRITER_ROLE)
    return (
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(schema, writer),
        sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(schema, writer),
        sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(schema),
        sql.SQL(
            f"""CREATE TABLE IF NOT EXISTS {ROUND4_LEDGER_TABLE} (
    entity_id text PRIMARY KEY,
    score double precision,
    model_version text,
    proof_nonce text,
    delta_table_id text NOT NULL,
    delta_commit_version bigint NOT NULL CHECK (delta_commit_version >= 0),
    deleted boolean NOT NULL DEFAULT false,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT model_score_ledger_live_row_is_whole CHECK (
        deleted OR (score IS NOT NULL AND model_version IS NOT NULL AND proof_nonce IS NOT NULL)
    )
)"""
        ),
        sql.SQL("ALTER TABLE {} OWNER TO {}").format(
            sql.Identifier(ROUND4_TARGET_SCHEMA, ROUND4_TARGET_TABLE), writer
        ),
        sql.SQL(
            f"CREATE OR REPLACE VIEW {ROUND4_LEDGER_VIEW} AS "
            "SELECT entity_id, score, model_version, proof_nonce "
            f"FROM {ROUND4_LEDGER_TABLE} WHERE NOT deleted"
        ),
        sql.SQL("ALTER VIEW {} OWNER TO {}").format(
            sql.Identifier(ROUND4_TARGET_SCHEMA, ROUND4_TARGET_VIEW), writer
        ),
    )


async def ensure_writer_target(cursor: Any, *, database: str, password: str) -> None:
    """The writer role, with ``password``, and everything it owns, on one r4 database.

    The installer's own login creates the role, so on PostgreSQL 16 and later it holds the
    role's ADMIN option and can grant itself membership, which is what lets it hand the
    writer ownership of its schema. The writer is otherwise ``NOSUPERUSER NOCREATEDB
    NOCREATEROLE NOREPLICATION NOBYPASSRLS``, and that is verified, not assumed.
    """

    writer = sql.Identifier(ROUND4_WRITER_ROLE)
    await cursor.execute(
        "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s",
        (ROUND4_WRITER_ROLE,),
    )
    exists = await cursor.fetchone() is not None
    statement = (
        sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}")
        if exists
        else sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}")
    )
    await cursor.execute(statement.format(writer, sql.Literal(password)))
    await cursor.execute(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls "
        "FROM pg_catalog.pg_roles WHERE rolname = %s",
        (ROUND4_WRITER_ROLE,),
    )
    attributes = await cursor.fetchone()
    if attributes is None or any(attributes):
        raise RuntimeError(
            f"The Round 4 writer role {ROUND4_WRITER_ROLE!r} must be NOSUPERUSER, NOCREATEDB, "
            "NOCREATEROLE, NOREPLICATION and NOBYPASSRLS; it owns its schema and nothing else"
        )
    await cursor.execute(sql.SQL("GRANT {} TO CURRENT_USER").format(writer))
    await cursor.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), writer)
    )
    for statement in target_statements():
        await cursor.execute(statement)


def set_connection_password(glue: Any, name: str, password: str) -> None:
    """Put ``password`` on one Glue connection, keeping everything else Terraform set on it."""

    current = glue.get_connection(Name=name, HidePassword=True).get("Connection") or {}
    if current.get("Name") != name:
        raise RuntimeError(f"Glue returned a different connection for {name}")
    properties = {
        key: value
        for key, value in (current.get("ConnectionProperties") or {}).items()
        if key not in {"PASSWORD", "ENCRYPTED_PASSWORD"}
    }
    if properties.get("USERNAME") != ROUND4_WRITER_ROLE:
        raise RuntimeError(f"Glue connection {name} does not log in as {ROUND4_WRITER_ROLE}")
    properties["PASSWORD"] = password
    connection_input: dict[str, Any] = {
        "Name": name,
        "ConnectionType": current.get("ConnectionType") or "JDBC",
        "ConnectionProperties": properties,
        "PhysicalConnectionRequirements": current.get("PhysicalConnectionRequirements") or {},
    }
    if current.get("Description"):
        connection_input["Description"] = current["Description"]
    glue.update_connection(Name=name, ConnectionInput=connection_input)


def new_writer_password() -> str:
    """A fresh writer password. URL-safe, so it needs no quoting in a JDBC property."""

    return secrets.token_urlsafe(32)


def prove_lane(
    writer: GlueWriterJob,
    *,
    competitor: str,
    source_table_id: str,
    expected: ModelScoreRow,
    read_target: Callable[[], ModelScoreRow | None],
    notify: Callable[[str], None] = print,
    timeout_seconds: float = PROOF_TIMEOUT_SECONDS,
    poll_seconds: float = PROOF_POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> float:
    """Run one job until it has applied a batch and its target reads ``expected``, then park it.

    The target alone proves nothing on a re-run: it already holds the baseline from the last
    bout, and the ledger guard makes the run's own write of the same version a no-op. So the
    proof also waits for the run's marker to say its first batch was applied, which it records
    only after reading the table from S3 and committing over JDBC: the role, the route and the
    credential this proof exists to catch.

    Parks first, so a run an earlier attempt left behind cannot hold the slot, and parks again
    on every way out, so a failed proof never leaves a writer billing. Returns how long it took.
    """

    writer.park(notify)
    run_tag = f"install-{int(time.time())}"
    began = clock()
    # From the snapshot: the proof knows nothing about the target but what it must end up reading.
    run_id, _ = writer.start(
        {
            "--run_tag": run_tag,
            "--source_table_id": source_table_id,
            "--starting_version": FROM_SNAPSHOT,
        }
    )
    applied = False
    try:
        while True:
            run = writer.run(run_id)
            if run.state in TERMINAL_STATES:
                raise GlueLaneError(
                    f"Round 4's {competitor} Glue writer ended {run.state} before its target "
                    f"read the baseline: {run.error_message or 'no error message'}"
                )
            if run.state == "RUNNING":
                if not applied:
                    marker = writer.marker(competitor, run_tag) or {}
                    applied = bool(marker.get("first_batch_applied_at"))
                if applied and read_target() == expected:
                    return clock() - began
            if clock() - began >= timeout_seconds:
                missing = (
                    "its target does not read the baseline"
                    if applied
                    else "it has not applied a batch yet"
                )
                raise GlueLaneError(
                    f"Round 4's {competitor} Glue writer did not carry the baseline into its "
                    f"target within {timeout_seconds:.0f}s: {missing} (run {run_id} is "
                    f"{run.state})"
                )
            sleep(poll_seconds)
    finally:
        writer.park(notify)


def seal(lane: Mapping[str, Any]) -> Round4AwsResources:
    """The lane's seal from Terraform's ``round4_glue`` output, checked against this checkout."""

    if lane.get("script_sha256") != script_sha256():
        raise RuntimeError(
            "Terraform uploaded a Round 4 Glue script other than glue/round4_writer.py in this "
            "checkout; apply again from the checkout you mean to seal"
        )
    jobs = lane.get("jobs") or {}
    connections = lane.get("connections") or {}
    return Round4AwsResources(
        source_location=str(lane["source_location"]),
        bucket=str(lane["bucket"]),
        script_key=str(lane["script_key"]),
        script_sha256=str(lane["script_sha256"]),
        role_arn=str(lane["role_arn"]),
        subnet_id=str(lane["subnet_id"]),
        subnet_cidr=str(lane["subnet_cidr"]),
        route_table_id=str(lane["route_table_id"]),
        s3_endpoint_id=str(lane["s3_endpoint_id"]),
        security_group_id=str(lane["security_group_id"]),
        aurora=Round4AwsLaneSeal(
            job_name=str(jobs["aurora"]), connection_name=str(connections["aurora"])
        ),
        rds=Round4AwsLaneSeal(job_name=str(jobs["rds"]), connection_name=str(connections["rds"])),
    )


def source_location(profile: str, source_table_full_name: str, api: Callable[..., Any]) -> str:
    """Where Unity Catalog stores Round 4's managed Delta source, as an ``s3://`` location."""

    table = api(
        profile,
        "get",
        "/api/2.1/unity-catalog/tables/" + quote(source_table_full_name, safe=""),
    )
    location = str((table or {}).get("storage_location") or "").rstrip("/")
    if not location.startswith("s3://") or location.count("/") < 3:
        raise RuntimeError(
            "Round 4's AWS lane reads its Delta source straight from S3, but Unity Catalog "
            f"reports no s3:// location for {source_table_full_name}"
        )
    return location


def source_table_id(rows: list[Mapping[str, Any]]) -> str:
    """The Delta table ID from ``DESCRIBE DETAIL``'s one row."""

    if len(rows) != 1 or not str(rows[0].get("id") or ""):
        raise RuntimeError("DESCRIBE DETAIL did not return Round 4's Delta table ID")
    return str(rows[0]["id"])


async def read_target_row(connection: Any, entity_id: str) -> ModelScoreRow | None:
    """The application's read of the AWS lane, through the view."""

    async with connection.cursor() as cursor:
        await cursor.execute(
            sql.SQL(
                "SELECT entity_id, score, model_version, proof_nonce FROM {} WHERE entity_id = %s"
            ).format(sql.Identifier(ROUND4_TARGET_SCHEMA, ROUND4_TARGET_VIEW)),
            (entity_id,),
        )
        rows = await cursor.fetchall()
    if len(rows) != 1:
        return None
    entity, score, model_version, nonce = rows[0]
    return ModelScoreRow(
        entity_id=str(entity),
        score=float(score),
        model_version=str(model_version),
        proof_nonce=str(nonce),
    )


def check_lane(
    manifest: DemoManifest,
    session: Any,
    *,
    hosts: Mapping[str, str],
) -> tuple[bool, str]:
    """``doctor``'s read-only check of a sealed lane: jobs, connections, script and parking."""

    sealed = manifest.round4_aws
    if sealed is None:
        if manifest.round4_aws_source_location:
            return False, "the Glue lane was built but never proven; run 'antidemo setup' again"
        return True, "no AWS lane on this installation"
    glue = session.client("glue")
    s3 = session.client("s3")
    head = s3.head_object(Bucket=sealed.bucket, Key=sealed.script_key)
    shipped = (head.get("Metadata") or {}).get("sha256")
    if shipped != sealed.script_sha256:
        return False, "the Glue script in S3 is not the sealed one"
    for competitor in ROUND4_COMPETITORS:
        lane = sealed.lane(competitor)
        job = glue.get_job(JobName=lane.job_name).get("Job") or {}
        command = job.get("Command") or {}
        arguments = job.get("DefaultArguments") or {}
        exact_job = (
            job.get("Role") == sealed.role_arn
            and command.get("ScriptLocation") == f"s3://{sealed.bucket}/{sealed.script_key}"
            and (job.get("Connections") or {}).get("Connections") == [lane.connection_name]
            and (job.get("ExecutionProperty") or {}).get("MaxConcurrentRuns") == 1
            and job.get("GlueVersion") == "5.0"
            and arguments.get("--competitor") == competitor
            and arguments.get("--source_path") == sealed.source_location
            and arguments.get("--target_schema") == sealed.target_schema
            and arguments.get("--target_table") == sealed.target_table
        )
        if not exact_job:
            return False, f"the {competitor} Glue job differs from its seal"
        connection = (
            glue.get_connection(Name=lane.connection_name, HidePassword=True).get("Connection")
            or {}
        )
        properties = connection.get("ConnectionProperties") or {}
        physical = connection.get("PhysicalConnectionRequirements") or {}
        url = str(properties.get("JDBC_CONNECTION_URL") or "")
        exact_connection = (
            properties.get("USERNAME") == sealed.writer_role
            and url.startswith(f"jdbc:postgresql://{hosts[competitor]}:5432/")
            and physical.get("SubnetId") == sealed.subnet_id
            and physical.get("SecurityGroupIdList") == [sealed.security_group_id]
        )
        if not exact_connection:
            return False, f"the {competitor} Glue connection differs from its seal"
        active = [
            run
            for run in glue.get_job_runs(JobName=lane.job_name, MaxResults=5).get("JobRuns") or []
            if run.get("JobRunState") in {"STARTING", "RUNNING", "STOPPING", "WAITING"}
        ]
        if active:
            return False, f"the {competitor} Glue writer is running while no bout is in flight"
    return True, "both Glue writers sealed, parked, and running the sealed script"
