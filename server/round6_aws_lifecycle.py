"""Round 6's AWS lane at install time: its source, its credentials, its catalog and its proof.

Terraform builds the standing AWS pieces (``infra/aws/round6_aws.tf``). The installer does the
rest, on every setup, and each step is idempotent:

1. makes sure DMS's account-wide ``dms-vpc-role`` exists, adopting it if another installation or
   anyone else made it, and creating it only if nobody has; nothing ever deletes it;
2. creates the lane's Unity Catalog storage credential, which names the lane's read-only IAM role
   before Terraform has made it, to learn the external ID that role's trust requires. An
   identity that may not create it seals why, and Round 6 then races Lakebase alone;
3. in each r6 database, creates the source table exactly as Lakebase's, at its baseline, and the
   capture role DMS logs in as, with the replication privilege and nothing but read on that one
   table, and sets its password;
4. puts the same password on each competitor's DMS source endpoint, and tests both endpoints;
5. starts each task once, which creates its standing replication slot, commits a proof order,
   runs the Glue job until the lakehouse reads that exact order through the external table, and
   parks both.

The lane is sealed only after step 5 has passed for both competitors, so an installation whose
role, route, endpoint or grant is broken fails its install rather than its first bout.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import time
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import quote

from botocore.exceptions import ClientError
from psycopg import sql

from .manifest import DemoManifest, Round6AwsLaneSeal, Round6AwsResources
from .round4_glue import TERMINAL_STATES, GlueWriterJob
from .round6_dms import DmsCaptureTask
from .round6_lifecycle import (
    ROUND6_BASELINE_NONCE,
    ROUND6_BASELINE_ORDER_ID,
    ROUND6_BASELINE_QUANTITY,
    ROUND6_BASELINE_SKU,
    ROUND6_BASELINE_STATUS,
    ROUND6_BASELINE_STORE,
    ROUND6_BASELINE_TOTAL_CENTS,
)

ROUND6_CAPTURE_ROLE = "round6_capture"
ROUND6_SOURCE_SCHEMA = "round6"
ROUND6_SOURCE_TABLE = "live_orders"
#: The same name, qualified, for the DDL below. A plain lowercase identifier, so it needs no
#: quoting, and a constant, so every table this project creates can be named by a scan.
ROUND6_SOURCE_QUALIFIED = "round6.live_orders"
ROUND6_COMPETITORS = ("aurora", "rds")

#: The Unity Catalog tables over the lane's Delta tables, one per competitor, beside Lakebase's
#: history table in Round 6's sealed schema.
ROUND6_HISTORY_TABLE_PREFIX = "aws_live_orders_history"

#: The source endpoint's password until the installer has set the real one. Mirrors the literal
#: in ``infra/aws/round6_aws.tf``.
ROUND6_PLACEHOLDER_PASSWORD = "set-by-the-installer"

#: The script Terraform uploads, whose digest is sealed and checked by ``doctor``.
ROUND6_SCRIPT = Path(__file__).resolve().parents[1] / "glue" / "round6_writer.py"

#: DMS's account-wide role for a VPC, and the AWS-managed policy it carries. Both names are fixed
#: by AWS for the whole account.
DMS_VPC_ROLE = "dms-vpc-role"
DMS_VPC_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"

#: How long one proof may take from the task's start to the lakehouse reading the proof order.
#: Cold, the spike's bouts took 62-93 s; the rest is room for a first run's network setup and a
#: first slot.
PROOF_TIMEOUT_SECONDS = 900.0
PROOF_POLL_SECONDS = 5.0
#: How long an endpoint connection test may take. DMS tested both in under a minute in the spike.
ENDPOINT_TEST_SECONDS = 600.0
ENDPOINT_TEST_POLL_SECONDS = 5.0

#: The source table's columns, exactly as Lakebase's (`round6_lifecycle._ensure_source`), so the
#: two lanes carry the same row.
SOURCE_COLUMNS = (
    ("order_id", "text", "NO"),
    ("sku", "text", "NO"),
    ("store", "text", "NO"),
    ("quantity", "integer", "NO"),
    ("total_cents", "integer", "NO"),
    ("status", "text", "NO"),
    ("proof_nonce", "text", "NO"),
)
BASELINE_ROW = (
    ROUND6_BASELINE_ORDER_ID,
    ROUND6_BASELINE_SKU,
    ROUND6_BASELINE_STORE,
    ROUND6_BASELINE_QUANTITY,
    ROUND6_BASELINE_TOTAL_CENTS,
    ROUND6_BASELINE_STATUS,
    ROUND6_BASELINE_NONCE,
)


class Round6AwsUnsupported(RuntimeError):
    """This installation's identity may not create the lane's Unity Catalog objects.

    Not a failure of the install: Round 6 then races Lakebase alone and says why.
    """


def script_sha256() -> str:
    return hashlib.sha256(ROUND6_SCRIPT.read_bytes()).hexdigest()


def uc_names(run_id: str) -> dict[str, str]:
    """The lane's Unity Catalog object names, from the run ID as Round 6's other names are."""

    suffix = re.sub(r"[^a-z0-9]+", "_", run_id.casefold()).strip("_")
    if not suffix or len(suffix) > 43:
        raise RuntimeError("Run ID cannot be converted to safe Round 6 identifiers")
    return {
        "storage_credential": f"anti_demo_r6_aws_{suffix}",
        "external_location": f"anti_demo_r6_aws_{suffix}",
    }


def history_table_full_name(catalog: str, schema: str, competitor: str) -> str:
    return f"{catalog}.{schema}.{ROUND6_HISTORY_TABLE_PREFIX}_{competitor}"


def new_capture_password() -> str:
    """A fresh capture-role password. URL-safe, so DMS needs no quoting for it."""

    return secrets.token_urlsafe(32)


def source_statements() -> tuple[sql.Composable, ...]:
    """The source schema and table, exactly as Lakebase's table is. Idempotent.

    ``REPLICA IDENTITY FULL`` as on Lakebase, so a delete carries its whole row on both lanes.
    The installer's own login owns both; the capture role only reads.
    """

    schema = sql.Identifier(ROUND6_SOURCE_SCHEMA)
    return (
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION CURRENT_USER").format(schema),
        sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(schema),
        sql.SQL(
            f"""CREATE TABLE IF NOT EXISTS {ROUND6_SOURCE_QUALIFIED} (
    order_id text PRIMARY KEY,
    sku text NOT NULL,
    store text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    total_cents integer NOT NULL CHECK (total_cents >= 0),
    status text NOT NULL,
    proof_nonce text NOT NULL UNIQUE
)"""
        ),
        sql.SQL(f"ALTER TABLE {ROUND6_SOURCE_QUALIFIED} REPLICA IDENTITY FULL"),
    )


async def ensure_capture_source(cursor: Any, *, database: str, password: str) -> None:
    """The source table at its baseline and the capture role, with ``password``, on one database.

    The capture role is ``NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS``, verified rather than
    assumed, and is given exactly two things: RDS's replication role, which is what lets DMS open
    a logical replication slot, and read on the one table.
    """

    for statement in source_statements():
        await cursor.execute(statement)
    await cursor.execute(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (ROUND6_SOURCE_SCHEMA, ROUND6_SOURCE_TABLE),
    )
    columns = tuple(tuple(row) for row in await cursor.fetchall())
    if columns != SOURCE_COLUMNS:
        raise RuntimeError(f"{ROUND6_SOURCE_QUALIFIED} columns are not exactly Lakebase's")
    await cursor.execute(
        "SELECT c.relreplident FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s",
        (ROUND6_SOURCE_SCHEMA, ROUND6_SOURCE_TABLE),
    )
    identity = await cursor.fetchone()
    if identity is None or str(identity[0]) != "f":
        raise RuntimeError(f"{ROUND6_SOURCE_QUALIFIED} is not REPLICA IDENTITY FULL")

    # Back to its one-row baseline, as Lakebase's source is on every setup.
    await cursor.execute(
        sql.SQL(
            f"INSERT INTO {ROUND6_SOURCE_QUALIFIED} (order_id, sku, store, quantity, total_cents, "
            "status, proof_nonce) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (order_id) DO NOTHING"
        ),
        BASELINE_ROW,
    )
    await cursor.execute(
        sql.SQL(f"DELETE FROM {ROUND6_SOURCE_QUALIFIED} WHERE order_id <> %s"),
        (ROUND6_BASELINE_ORDER_ID,),
    )
    await cursor.execute(
        sql.SQL(
            "SELECT order_id, sku, store, quantity, total_cents, status, proof_nonce "
            f"FROM {ROUND6_SOURCE_QUALIFIED}"
        )
    )
    rows = [tuple(row) for row in await cursor.fetchall()]
    if rows != [BASELINE_ROW]:
        raise RuntimeError(f"{ROUND6_SOURCE_QUALIFIED} is not at its exact baseline")

    capture = sql.Identifier(ROUND6_CAPTURE_ROLE)
    await cursor.execute(
        "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s", (ROUND6_CAPTURE_ROLE,)
    )
    exists = await cursor.fetchone() is not None
    statement = (
        sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}")
        if exists
        else sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}")
    )
    await cursor.execute(statement.format(capture, sql.Literal(password)))
    await cursor.execute(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolbypassrls "
        "FROM pg_catalog.pg_roles WHERE rolname = %s",
        (ROUND6_CAPTURE_ROLE,),
    )
    attributes = await cursor.fetchone()
    if attributes is None or any(attributes):
        raise RuntimeError(
            f"The Round 6 capture role {ROUND6_CAPTURE_ROLE!r} must be NOSUPERUSER, NOCREATEDB, "
            "NOCREATEROLE and NOBYPASSRLS; it replicates and reads one table and nothing else"
        )
    await cursor.execute(sql.SQL("GRANT rds_replication TO {}").format(capture))
    await cursor.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), capture)
    )
    await cursor.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(ROUND6_SOURCE_SCHEMA), capture
        )
    )
    await cursor.execute(
        sql.SQL(f"GRANT SELECT ON {ROUND6_SOURCE_QUALIFIED} TO {{}}").format(capture)
    )


async def replication_slots(cursor: Any) -> list[str]:
    """The logical replication slots on this database, which DMS's first start creates."""

    await cursor.execute(
        "SELECT slot_name FROM pg_catalog.pg_replication_slots "
        "WHERE slot_type = 'logical' AND database = current_database() ORDER BY slot_name"
    )
    return [str(row[0]) for row in await cursor.fetchall()]


async def commit_proof_order(cursor: Any, order_id: str, nonce: str) -> None:
    """One proof order, committed on its own, the shape a bout's checkout has."""

    await cursor.execute(
        sql.SQL(
            f"INSERT INTO {ROUND6_SOURCE_QUALIFIED} (order_id, sku, store, quantity, total_cents, "
            "status, proof_nonce) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ),
        (
            order_id,
            ROUND6_BASELINE_SKU,
            ROUND6_BASELINE_STORE,
            ROUND6_BASELINE_QUANTITY,
            ROUND6_BASELINE_TOTAL_CENTS,
            "install-proof",
            nonce,
        ),
    )


async def withdraw_order(cursor: Any, order_id: str) -> None:
    await cursor.execute(
        sql.SQL(f"DELETE FROM {ROUND6_SOURCE_QUALIFIED} WHERE order_id = %s AND order_id <> %s"),
        (order_id, ROUND6_BASELINE_ORDER_ID),
    )


def ensure_dms_vpc_role(iam: Any) -> str:
    """Adopt DMS's account-wide VPC role, or create it if nobody has. Returns which.

    Never tagged with this installation's run, and never deleted by it: another installation in
    the account, or anything else using DMS there, may rely on it (the collision class of the
    fixes before PR #27).
    """

    try:
        iam.get_role(RoleName=DMS_VPC_ROLE)
        return "adopted"
    except ClientError as error:
        if str((error.response.get("Error") or {}).get("Code")) != "NoSuchEntity":
            raise
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "dms.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    try:
        iam.create_role(
            RoleName=DMS_VPC_ROLE,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="AWS DMS's account-wide VPC role (created by Lakebase: The Anti-Demo)",
        )
    except ClientError as error:
        # Another installation made it between the read and the create.
        if str((error.response.get("Error") or {}).get("Code")) != "EntityAlreadyExists":
            raise
        return "adopted"
    iam.attach_role_policy(RoleName=DMS_VPC_ROLE, PolicyArn=DMS_VPC_POLICY_ARN)
    return "created"


def ensure_storage_credential(
    api: Callable[..., Any],
    api_optional: Callable[..., Any],
    profile: str,
    *,
    name: str,
    role_arn: str,
) -> str:
    """The lane's read-only storage credential, returning its external ID.

    Created with validation skipped, because the role it names does not exist until Terraform's
    lane apply, which needs this external ID for the role's trust. Databricks accepts that
    (measured 2026-09-29). An existing credential is reused only if it names exactly this role.
    """

    path = "/api/2.1/unity-catalog/storage-credentials"
    current = api_optional(profile, f"{path}/{quote(name, safe='')}")
    if current is None:
        try:
            current = api(
                profile,
                "post",
                path,
                body={
                    "name": name,
                    "aws_iam_role": {"role_arn": role_arn},
                    "read_only": True,
                    "skip_validation": True,
                    "comment": "Lakebase: The Anti-Demo, Round 6's AWS lane (read-only)",
                },
            )
        except RuntimeError as error:
            if _permission_refusal(str(error)):
                raise Round6AwsUnsupported(
                    "This installation's identity may not create a Unity Catalog storage "
                    f"credential: {error}"
                ) from error
            raise
    role = (current or {}).get("aws_iam_role") or {}
    if role.get("role_arn") != role_arn:
        raise RuntimeError(
            f"The storage credential {name} names a different IAM role than Round 6's lane"
        )
    external_id = str(role.get("external_id") or "")
    if not re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", external_id
    ):
        raise RuntimeError(f"The storage credential {name} returned no usable external ID")
    return external_id


def validate_storage_credential(
    api: Callable[..., Any],
    profile: str,
    *,
    credential: str,
    url: str,
) -> None:
    """Ask Unity Catalog whether the lane's credential works on ``url``, and fail if it does not.

    Run against the lane's bucket itself, never its empty ``delta/`` path. Unity Catalog checks a
    path by listing it, and the lane's Delta tables do not exist until its first proof writes
    them. On the v1.1 test installation of 2026-09-29, creating the external location over that
    empty path failed "AWS IAM role does not have LIST permissions ... No such file or
    directory", while the role, its self-trust and its external ID all passed this check. A
    check that fails here is this installation's own configuration, so it fails the install; it
    is never read as a privilege the identity lacks.
    """

    result = api(
        profile,
        "post",
        "/api/2.1/unity-catalog/validate-storage-credentials",
        body={"storage_credential_name": credential, "url": url, "read_only": True},
    )
    failed = [
        f"{item.get('operation') or item.get('configuration_operation')}: "
        f"{item.get('message') or 'failed'}"
        for item in (result or {}).get("results") or []
        if str(item.get("result") or "").upper() == "FAIL"
    ]
    if failed:
        raise RuntimeError(
            f"Round 6's storage credential {credential} does not work on {url}: "
            + "; ".join(failed)
        )


def ensure_external_location(
    api: Callable[..., Any],
    api_optional: Callable[..., Any],
    profile: str,
    *,
    name: str,
    url: str,
    credential: str,
    validate_url: str,
) -> None:
    """The read-only external location over the lane's Delta tables.

    Created with validation skipped, because its path is empty until the lane's first proof
    writes a table, and Unity Catalog reads an empty path as a missing LIST permission. So,
    before creating it, the credential is checked on ``validate_url``, the lane's bucket
    (``validate_storage_credential``). That check runs only while the location does not exist:
    once it does, Unity Catalog refuses to validate any path overlapping it, which on the v1.1
    test installation's first re-run failed setup ("overlaps with an existing external
    location"). An existing location was proven by use: the proof created each history table
    on it and read it back.
    """

    path = "/api/2.1/unity-catalog/external-locations"
    current = api_optional(profile, f"{path}/{quote(name, safe='')}")
    if current is None:
        validate_storage_credential(api, profile, credential=credential, url=validate_url)
        try:
            current = api(
                profile,
                "post",
                path,
                body={
                    "name": name,
                    "url": url,
                    "credential_name": credential,
                    "read_only": True,
                    "skip_validation": True,
                    "comment": "Lakebase: The Anti-Demo, Round 6's AWS lane (read-only)",
                },
            )
        except RuntimeError as error:
            if _permission_refusal(str(error)):
                raise Round6AwsUnsupported(
                    "This installation's identity may not create a Unity Catalog external "
                    f"location: {error}"
                ) from error
            raise
    if (current or {}).get("url", "").rstrip("/") != url.rstrip("/") or (current or {}).get(
        "credential_name"
    ) != credential:
        raise RuntimeError(f"The external location {name} is not the lane's")


def _permission_refusal(text: str) -> bool:
    """Whether Databricks refused this identity a privilege, which makes the lane unsupported.

    A message about the lane's own IAM role is never that: it is this installation's AWS
    configuration, and reading it as a missing privilege is how a defect of ours once became a
    quiet "Round 6 races Lakebase alone" (the v1.1 test installation, 2026-09-29).
    """

    lowered = text.casefold()
    if "iam role" in lowered:
        return False
    return any(
        marker in lowered
        for marker in ("permission_denied", "permission denied", "does not have", "403")
    )


def history_table_statement(full_name: str, location: str) -> str:
    """The external table over one competitor's Delta table. Idempotent."""

    quoted = ".".join(f"`{part}`" for part in full_name.split("."))
    return f"CREATE TABLE IF NOT EXISTS {quoted} USING DELTA LOCATION '{location}'"


def history_read_statement(full_name: str, order_id: str, nonce: str) -> str:
    """The verifier's read: that one order, as one insert, in one lane's history.

    The same question on both lanes. Lakebase's feed writes its change type and DMS writes its
    operation; each is asked for the insert. The IDs are this project's own UUIDs and nonces,
    checked before they are interpolated.
    """

    for value in (order_id, nonce):
        if not re.fullmatch(r"[A-Za-z0-9-]{1,80}", value):
            raise ValueError(f"not a proof identifier: {value!r}")
    quoted = ".".join(f"`{part}`" for part in full_name.split("."))
    return (
        f"SELECT count(*) AS n FROM {quoted} "
        f"WHERE order_id = '{order_id}' AND proof_nonce = '{nonce}' AND Op = 'I'"
    )


def set_endpoint_password(dms: Any, endpoint_arn: str, password: str) -> None:
    """Put ``password`` on one source endpoint, after checking it is the capture role's."""

    endpoints = (
        dms.describe_endpoints(Filters=[{"Name": "endpoint-arn", "Values": [endpoint_arn]}]).get(
            "Endpoints"
        )
        or []
    )
    if len(endpoints) != 1 or endpoints[0].get("EndpointType", "").upper() != "SOURCE":
        raise RuntimeError(f"DMS returned no source endpoint {endpoint_arn}")
    if endpoints[0].get("Username") != ROUND6_CAPTURE_ROLE:
        raise RuntimeError(
            f"The DMS endpoint {endpoint_arn} does not log in as {ROUND6_CAPTURE_ROLE}"
        )
    dms.modify_endpoint(EndpointArn=endpoint_arn, Password=password)


def test_endpoint(
    dms: Any,
    *,
    instance_arn: str,
    endpoint_arn: str,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Ask DMS to connect to one endpoint from the replication instance, and wait for the answer.

    DMS refuses a test with ``InvalidResourceStateFault`` while another test of the same
    endpoint is running, and it starts tests of its own: on the v1.1 test installation of
    2026-09-29 every lane endpoint already had a result the installer never asked for, one of
    them a failure on the placeholder password, and the installer's first test was refused. So
    a refused test is asked for again until DMS accepts it. A test DMS has accepted reads as
    ``testing`` until it finishes, so the answer read after that is this test's own.
    """

    deadline = clock() + ENDPOINT_TEST_SECONDS
    while True:
        try:
            dms.test_connection(ReplicationInstanceArn=instance_arn, EndpointArn=endpoint_arn)
            break
        except ClientError as error:
            code = str((error.response.get("Error") or {}).get("Code"))
            if code != "InvalidResourceStateFault" or clock() >= deadline:
                raise
        sleep(ENDPOINT_TEST_POLL_SECONDS)
    while True:
        try:
            connections = (
                dms.describe_connections(
                    Filters=[
                        {"Name": "endpoint-arn", "Values": [endpoint_arn]},
                        {"Name": "replication-instance-arn", "Values": [instance_arn]},
                    ]
                ).get("Connections")
                or []
            )
        except ClientError as error:
            if str((error.response.get("Error") or {}).get("Code")) != "ResourceNotFoundFault":
                raise
            connections = []
        status = str(connections[0].get("Status") or "") if connections else ""
        if status == "successful":
            return
        if status == "failed":
            raise RuntimeError(
                f"DMS could not connect to {endpoint_arn}: "
                f"{connections[0].get('LastFailureMessage') or 'no reason given'}"
            )
        if clock() >= deadline:
            raise RuntimeError(
                f"DMS's connection test of {endpoint_arn} did not finish within "
                f"{ENDPOINT_TEST_SECONDS:.0f}s"
            )
        sleep(ENDPOINT_TEST_POLL_SECONDS)


def new_proof_identity(competitor: str) -> tuple[str, str]:
    """A proof order's ID and nonce: unique per proof, so no earlier change can ever match."""

    return str(uuid.uuid4()), f"install-{competitor}-{secrets.token_hex(6)}"


def prove_lane(
    task: DmsCaptureTask,
    writer: GlueWriterJob,
    *,
    competitor: str,
    wait_for_slot: Callable[[], None],
    commit_proof: Callable[[str, str], None],
    withdraw_proof: Callable[[str], None],
    ensure_history_table: Callable[[], bool],
    read_history: Callable[[str, str], bool],
    notify: Callable[[str], None] = print,
    timeout_seconds: float = PROOF_TIMEOUT_SECONDS,
    poll_seconds: float = PROOF_POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> float:
    """Carry one proof order from the source into the lakehouse, then park both halves.

    The task starts first, because its first start creates the slot, and a change committed
    before the slot exists is never captured. So the proof order is committed only once the
    slot is there. Then the Glue job runs until the lakehouse reads that exact order, through the
    same external table and warehouse the verifier uses. ``ensure_history_table`` makes the
    external table once the job's first Delta commit exists, and answers whether it does.

    Parks first, so anything an earlier attempt left running cannot hold a slot, and parks again
    on every way out, so a failed proof never leaves a lane billing. The proof order is withdrawn
    at the end; its delete is captured at the next start, and its nonce can never match a bout.
    Returns how long the proof took.
    """

    writer.park(notify)
    task.park(notify)
    order_id, nonce = new_proof_identity(competitor)
    began = clock()
    committed = False
    try:
        task.start()
        task.wait_running()
        wait_for_slot()
        commit_proof(order_id, nonce)
        committed = True
        run_id, _ = writer.start({})
        table_ready = False
        while True:
            run = writer.run(run_id)
            if run.state in TERMINAL_STATES:
                raise RuntimeError(
                    f"Round 6's {competitor} Glue writer ended {run.state} before the lakehouse "
                    f"read the proof order: {run.error_message or 'no error message'}"
                )
            state = task.state()
            if not state.running:
                raise RuntimeError(
                    f"Round 6's {competitor} DMS task left running before the lakehouse read the "
                    f"proof order: {state.failure or state.stop_reason or state.status}"
                )
            table_ready = table_ready or ensure_history_table()
            if table_ready and read_history(order_id, nonce):
                return clock() - began
            if clock() - began >= timeout_seconds:
                missing = (
                    "the lakehouse does not read the proof order"
                    if table_ready
                    else "the Glue writer has not made its Delta table"
                )
                raise RuntimeError(
                    f"Round 6's {competitor} AWS lane did not carry a proof order into the "
                    f"lakehouse within {timeout_seconds:.0f}s: {missing} (run {run_id} is "
                    f"{run.state})"
                )
            sleep(poll_seconds)
    finally:
        try:
            writer.park(notify)
        finally:
            try:
                task.park(notify)
            finally:
                if committed:
                    withdraw_proof(order_id)


def seal(
    lane: Mapping[str, Any],
    *,
    external_id: str,
    names: Mapping[str, str],
    history_tables: Mapping[str, str],
) -> Round6AwsResources:
    """The lane's seal from Terraform's ``round6_aws`` output, checked against this checkout."""

    if lane.get("script_sha256") != script_sha256():
        raise RuntimeError(
            "Terraform uploaded a Round 6 Glue script other than glue/round6_writer.py in this "
            "checkout; apply again from the checkout you mean to seal"
        )
    sources = lane.get("source_endpoints") or {}
    targets = lane.get("target_endpoints") or {}
    tasks = lane.get("tasks") or {}
    jobs = lane.get("jobs") or {}
    locations = lane.get("history_locations") or {}

    def half(competitor: str) -> Round6AwsLaneSeal:
        return Round6AwsLaneSeal(
            source_endpoint_arn=str(sources[competitor]),
            target_endpoint_arn=str(targets[competitor]),
            task_arn=str(tasks[competitor]),
            job_name=str(jobs[competitor]),
            history_location=str(locations[competitor]),
            history_table_full_name=str(history_tables[competitor]),
        )

    subnets = [str(subnet) for subnet in lane.get("subnet_ids") or []]
    if len(subnets) != 2:
        raise RuntimeError("Terraform's Round 6 lane has no two DMS subnets")
    return Round6AwsResources(
        bucket=str(lane["bucket"]),
        script_key=str(lane["script_key"]),
        script_sha256=str(lane["script_sha256"]),
        glue_role_arn=str(lane["glue_role_arn"]),
        dms_s3_role_arn=str(lane["dms_s3_role_arn"]),
        uc_role_arn=str(lane["uc_role_arn"]),
        subnet_ids=(subnets[0], subnets[1]),
        subnet_cidr=str(lane["subnet_cidr"]),
        route_table_id=str(lane["route_table_id"]),
        s3_endpoint_id=str(lane["s3_endpoint_id"]),
        security_group_id=str(lane["security_group_id"]),
        replication_instance_arn=str(lane["replication_instance_arn"]),
        uc_storage_credential=names["storage_credential"],
        uc_external_location=names["external_location"],
        uc_external_id=external_id,
        aurora=half("aurora"),
        rds=half("rds"),
    )


def check_lane(
    manifest: DemoManifest, session: Any, *, hosts: Mapping[str, str]
) -> tuple[bool, str]:
    """``doctor``'s read-only check of a sealed lane: tasks, endpoints, jobs, script and parking."""

    sealed = manifest.round6_aws
    if sealed is None:
        if manifest.round6_aws_unsupported:
            return True, f"no AWS lane on this installation: {manifest.round6_aws_unsupported}"
        if manifest.round6_aws_uc_external_id:
            return False, "the DMS lane was built but never proven; run 'antidemo setup' again"
        return True, "no AWS lane on this installation"
    dms = session.client("dms")
    glue = session.client("glue")
    s3 = session.client("s3")
    head = s3.head_object(Bucket=sealed.bucket, Key=sealed.script_key)
    if (head.get("Metadata") or {}).get("sha256") != sealed.script_sha256:
        return False, "the Glue script in S3 is not the sealed one"
    instances = (
        dms.describe_replication_instances(
            Filters=[
                {"Name": "replication-instance-arn", "Values": [sealed.replication_instance_arn]}
            ]
        ).get("ReplicationInstances")
        or []
    )
    if len(instances) != 1 or instances[0].get("ReplicationInstanceStatus") != "available":
        return False, "the DMS replication instance is not available"
    for competitor in ROUND6_COMPETITORS:
        lane = sealed.lane(competitor)
        endpoints = (
            dms.describe_endpoints(
                Filters=[{"Name": "endpoint-arn", "Values": [lane.source_endpoint_arn]}]
            ).get("Endpoints")
            or []
        )
        exact_endpoint = (
            len(endpoints) == 1
            and endpoints[0].get("Username") == sealed.capture_role
            and endpoints[0].get("ServerName") == hosts[competitor]
            and str(endpoints[0].get("SslMode") or "") == "require"
        )
        if not exact_endpoint:
            return False, f"the {competitor} DMS source endpoint differs from its seal"
        state = DmsCaptureTask(session, lane.task_arn).state()
        # The status alone, as before: one read, between bouts, with nothing racing it.
        if not state.at_rest_by_status:
            return False, f"the {competitor} DMS task is {state.status} while no bout is in flight"
        if state.status == "ready":
            return False, f"the {competitor} DMS task has never run, so it has no slot"
        job = glue.get_job(JobName=lane.job_name).get("Job") or {}
        arguments = job.get("DefaultArguments") or {}
        exact_job = (
            job.get("Role") == sealed.glue_role_arn
            and (job.get("Command") or {}).get("ScriptLocation")
            == f"s3://{sealed.bucket}/{sealed.script_key}"
            and (job.get("ExecutionProperty") or {}).get("MaxConcurrentRuns") == 1
            and job.get("GlueVersion") == "5.0"
            and arguments.get("--competitor") == competitor
            and arguments.get("--target") == lane.history_location
        )
        if not exact_job:
            return False, f"the {competitor} Glue job differs from its seal"
        active = [
            run
            for run in glue.get_job_runs(JobName=lane.job_name, MaxResults=5).get("JobRuns") or []
            if run.get("JobRunState") in {"STARTING", "RUNNING", "STOPPING", "WAITING"}
        ]
        if active:
            return False, f"the {competitor} Glue writer is running while no bout is in flight"
    return True, "both DMS tasks and Glue writers sealed, parked, and running the sealed script"
