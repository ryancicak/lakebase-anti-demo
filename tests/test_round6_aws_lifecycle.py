"""Round 6's AWS lane at install time: its source, its credentials, its catalog, its proof.

The lane is sealed only after each competitor has carried a proof order from its source into the
lakehouse, so these pin the proof's every way out (success, a writer that ends, a task that stops,
a lakehouse that never reads it) and that both halves are parked whichever way it goes. They also
pin the two account-level hazards the design names: DMS's account-wide role is adopted and never
owned, and a storage credential this identity may not create seals the lane as unsupported.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from server import round6_aws_lifecycle as lane
from server.round4_glue import GlueRun
from server.round6_dms import DmsTaskState

# Built by concatenation, as this tree's other fixtures are, so no literal here has the shape
# tests/test_no_live_identifiers_committed.py refuses.
EXTERNAL_ID = "00000000" + "-0000-4000-8000-" + "000000000001"
ROLE = "arn:aws:iam::123456789012:role/i0123456789abcdef0123-r6-uc"
FAKE_RUN_ID = "ad-20" + "990101-0000-" + "test"


def fake_aws_id(kind: str, digit: str) -> str:
    return f"{kind}-0" + digit * 16


def rendered(statement) -> str:
    return statement if isinstance(statement, str) else statement.as_string(None)


class Cursor:
    """An async cursor that records each statement and answers from a script."""

    def __init__(self, answers: dict[str, list] | None = None) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.answers = answers or {}
        self._last = ""

    async def execute(self, statement, parameters=()):
        text = rendered(statement)
        self.statements.append((text, tuple(parameters)))
        self._last = text

    def _answer(self):
        for fragment, rows in self.answers.items():
            if fragment in self._last:
                return rows
        return []

    async def fetchone(self):
        rows = self._answer()
        return rows[0] if rows else None

    async def fetchall(self):
        return list(self._answer())


def healthy_source(**overrides) -> Cursor:
    answers = {
        "information_schema.columns": list(lane.SOURCE_COLUMNS),
        "c.relreplident": [("f",)],
        "SELECT order_id, sku": [lane.BASELINE_ROW],
        "SELECT 1 FROM pg_catalog.pg_roles": [],
        "SELECT rolsuper": [(False, False, False, False)],
    }
    answers.update(overrides)
    return Cursor(answers)


def ensure(cursor: Cursor) -> list[str]:
    asyncio.run(lane.ensure_capture_source(cursor, database="anti_demo", password="pw-123"))
    return [text for text, _ in cursor.statements]


class TestTheSource:
    def test_the_table_is_exactly_lakebases_with_a_full_replica_identity(self):
        ddl = " ".join(rendered(statement) for statement in lane.source_statements())
        for fragment in (
            "order_id text PRIMARY KEY",
            "quantity integer NOT NULL CHECK (quantity > 0)",
            "total_cents integer NOT NULL CHECK (total_cents >= 0)",
            "proof_nonce text NOT NULL UNIQUE",
            "ALTER TABLE round6.live_orders REPLICA IDENTITY FULL",
            'REVOKE ALL ON SCHEMA "round6" FROM PUBLIC',
        ):
            assert fragment in ddl

    def test_the_capture_role_replicates_and_reads_one_table_and_nothing_else(self):
        statements = ensure(healthy_source())
        assert "CREATE ROLE \"round6_capture\" LOGIN PASSWORD 'pw-123'" in statements
        assert 'GRANT rds_replication TO "round6_capture"' in statements
        assert 'GRANT CONNECT ON DATABASE "anti_demo" TO "round6_capture"' in statements
        assert 'GRANT USAGE ON SCHEMA "round6" TO "round6_capture"' in statements
        assert 'GRANT SELECT ON round6.live_orders TO "round6_capture"' in statements
        granted = [text for text in statements if text.startswith("GRANT")]
        assert not any("INSERT" in text or "UPDATE" in text or "DELETE" in text for text in granted)

    def test_a_rerun_sets_the_password_again_on_the_existing_role(self):
        statements = ensure(healthy_source(**{"SELECT 1 FROM pg_catalog.pg_roles": [(1,)]}))
        assert "ALTER ROLE \"round6_capture\" LOGIN PASSWORD 'pw-123'" in statements
        assert not any(text.startswith("CREATE ROLE") for text in statements)

    def test_the_source_is_put_back_to_its_one_row_baseline(self):
        cursor = healthy_source()
        ensure(cursor)
        deletes = [(text, args) for text, args in cursor.statements if text.startswith("DELETE")]
        assert deletes == [
            (
                "DELETE FROM round6.live_orders WHERE order_id <> %s",
                (lane.ROUND6_BASELINE_ORDER_ID,),
            )
        ]

    @pytest.mark.parametrize(
        "override, message",
        [
            ({"information_schema.columns": [("order_id", "text", "NO")]}, "columns"),
            ({"c.relreplident": [("d",)]}, "REPLICA IDENTITY FULL"),
            ({"SELECT order_id, sku": []}, "baseline"),
            ({"SELECT rolsuper": [(True, False, False, False)]}, "NOSUPERUSER"),
        ],
    )
    def test_a_source_or_role_that_is_not_exact_is_refused(self, override, message):
        with pytest.raises(RuntimeError, match=message):
            ensure(healthy_source(**override))

    def test_a_proof_order_can_never_withdraw_the_baseline(self):
        cursor = Cursor()
        asyncio.run(lane.withdraw_order(cursor, lane.ROUND6_BASELINE_ORDER_ID))
        text, arguments = cursor.statements[0]
        assert "AND order_id <> %s" in text
        assert arguments == (lane.ROUND6_BASELINE_ORDER_ID, lane.ROUND6_BASELINE_ORDER_ID)


class Iam:
    def __init__(self, *, exists: bool = False, create_error: str | None = None) -> None:
        self.exists = exists
        self.create_error = create_error
        self.calls: list[tuple[str, dict]] = []

    def get_role(self, **arguments):
        self.calls.append(("get_role", arguments))
        if not self.exists:
            raise ClientError({"Error": {"Code": "NoSuchEntity"}}, "GetRole")
        return {"Role": {"RoleName": arguments["RoleName"]}}

    def create_role(self, **arguments):
        self.calls.append(("create_role", arguments))
        if self.create_error:
            raise ClientError({"Error": {"Code": self.create_error}}, "CreateRole")
        return {}

    def attach_role_policy(self, **arguments):
        self.calls.append(("attach_role_policy", arguments))
        return {}


class TestTheAccountWideDmsRole:
    def test_an_existing_role_is_adopted_and_left_untouched(self):
        iam = Iam(exists=True)
        assert lane.ensure_dms_vpc_role(iam) == "adopted"
        assert [name for name, _ in iam.calls] == ["get_role"]

    def test_a_missing_role_is_created_untagged_with_only_its_aws_policy(self):
        iam = Iam()
        assert lane.ensure_dms_vpc_role(iam) == "created"
        create = next(arguments for name, arguments in iam.calls if name == "create_role")
        assert create["RoleName"] == "dms-vpc-role"
        # Never this installation's: no run tag, so no uninstall ever counts or deletes it.
        assert "Tags" not in create
        trust = json.loads(create["AssumeRolePolicyDocument"])
        assert trust["Statement"][0]["Principal"] == {"Service": "dms.amazonaws.com"}
        attach = next(arguments for name, arguments in iam.calls if name == "attach_role_policy")
        assert attach == {"RoleName": "dms-vpc-role", "PolicyArn": lane.DMS_VPC_POLICY_ARN}

    def test_a_role_another_installation_made_first_is_adopted(self):
        iam = Iam(create_error="EntityAlreadyExists")
        assert lane.ensure_dms_vpc_role(iam) == "adopted"
        assert "attach_role_policy" not in [name for name, _ in iam.calls]


class Api:
    """The Databricks REST calls the installer makes, with scripted answers."""

    def __init__(self, existing=None, *, refuse: str | None = None) -> None:
        self.existing = existing
        self.refuse = refuse
        self.posts: list[tuple[str, dict]] = []

    def optional(self, profile, path):
        return self.existing

    def __call__(self, profile, method, path, *, body=None):
        self.posts.append((path, body))
        if self.refuse:
            raise RuntimeError(self.refuse)
        return {
            **body,
            "aws_iam_role": {**body.get("aws_iam_role", {}), "external_id": EXTERNAL_ID},
        }


class TestTheStorageCredential:
    def test_a_new_credential_names_the_role_before_it_exists_and_reads_only(self):
        api = Api()
        external = lane.ensure_storage_credential(
            api, api.optional, "p", name="anti_demo_r6_aws_x", role_arn=ROLE
        )
        assert external == EXTERNAL_ID
        path, body = api.posts[0]
        assert path == "/api/2.1/unity-catalog/storage-credentials"
        assert body["aws_iam_role"] == {"role_arn": ROLE}
        assert body["read_only"] is True
        assert body["skip_validation"] is True

    def test_an_existing_credential_for_this_role_is_reused(self):
        api = Api({"aws_iam_role": {"role_arn": ROLE, "external_id": EXTERNAL_ID}})
        assert lane.ensure_storage_credential(api, api.optional, "p", name="c", role_arn=ROLE) == (
            EXTERNAL_ID
        )
        assert api.posts == []

    def test_a_credential_by_this_name_for_another_role_is_refused(self):
        other = "arn:aws:iam::123456789012:role/someone-else"
        api = Api({"aws_iam_role": {"role_arn": other, "external_id": EXTERNAL_ID}})
        with pytest.raises(RuntimeError, match="different IAM role"):
            lane.ensure_storage_credential(api, api.optional, "p", name="c", role_arn=ROLE)

    def test_an_identity_that_may_not_create_one_makes_the_lane_unsupported(self):
        api = Api(refuse="PERMISSION_DENIED: User does not have CREATE STORAGE CREDENTIAL")
        with pytest.raises(lane.Round6AwsUnsupported, match="may not create"):
            lane.ensure_storage_credential(api, api.optional, "p", name="c", role_arn=ROLE)

    def test_any_other_failure_is_a_failure_not_a_skip(self):
        api = Api(refuse="INTERNAL_ERROR: try again")
        with pytest.raises(RuntimeError) as raised:
            lane.ensure_storage_credential(api, api.optional, "p", name="c", role_arn=ROLE)
        assert not isinstance(raised.value, lane.Round6AwsUnsupported)

    def test_a_credential_with_no_usable_external_id_is_refused(self):
        api = Api({"aws_iam_role": {"role_arn": ROLE, "external_id": "account"}})
        with pytest.raises(RuntimeError, match="external ID"):
            lane.ensure_storage_credential(api, api.optional, "p", name="c", role_arn=ROLE)


#: What Databricks answered when the external location was created over the lane's empty path,
#: on the v1.1 test installation of 2026-09-29.
EMPTY_PATH_REFUSAL = (
    "Error: AWS IAM role does not have LIST permissions on url s3://b/delta. Please contact "
    "your account admin to update the storage credential. No such file or directory: s3://b/delta"
)


class LocationApi:
    """Unity Catalog as `ensure_external_location` meets it: a credential check, then a create."""

    def __init__(self, existing=None, *, refuse_create: str | None = None) -> None:
        self.existing = existing
        self.refuse_create = refuse_create
        self.calls: list[tuple[str, dict]] = []

    def optional(self, profile, path):
        return self.existing

    def __call__(self, profile, method, path, *, body=None):
        self.calls.append((path, body))
        if path.endswith("/validate-storage-credentials"):
            return {"isDir": True, "results": [{"operation": "LIST", "result": "PASS"}]}
        if self.refuse_create:
            raise RuntimeError(self.refuse_create)
        return body


def _location(api: LocationApi) -> None:
    lane.ensure_external_location(
        api,
        api.optional,
        "p",
        name="loc",
        url="s3://b/delta",
        credential="cred",
        validate_url="s3://b/",
    )


class TestTheExternalLocation:
    def test_a_new_one_checks_the_credential_on_the_bucket_then_skips_validation(self):
        api = LocationApi()
        _location(api)
        (check, _), (create, body) = api.calls
        assert check == "/api/2.1/unity-catalog/validate-storage-credentials"
        assert create == "/api/2.1/unity-catalog/external-locations"
        assert body["read_only"] is True and body["skip_validation"] is True

    def test_an_existing_one_is_never_validated_again(self):
        # The re-run incident: Unity Catalog refuses to validate a path overlapping an existing
        # location, so a re-run that checked again failed setup.
        api = LocationApi({"url": "s3://b/delta", "credential_name": "cred"})
        _location(api)
        assert api.calls == []

    def test_an_existing_one_that_is_not_the_lanes_is_refused(self):
        api = LocationApi({"url": "s3://other/delta", "credential_name": "cred"})
        with pytest.raises(RuntimeError, match="is not the lane's"):
            _location(api)

    def test_an_iam_role_message_is_our_defect_not_a_missing_privilege(self):
        api = LocationApi(refuse_create=EMPTY_PATH_REFUSAL)
        with pytest.raises(RuntimeError) as raised:
            _location(api)
        assert not isinstance(raised.value, lane.Round6AwsUnsupported)

    def test_a_missing_privilege_makes_the_lane_unsupported(self):
        api = LocationApi(
            refuse_create="PERMISSION_DENIED: User does not have CREATE EXTERNAL LOCATION"
        )
        with pytest.raises(lane.Round6AwsUnsupported, match="may not create"):
            _location(api)


def validation(*results):
    def api(profile, method, path, *, body=None):
        assert (method, path) == ("post", "/api/2.1/unity-catalog/validate-storage-credentials")
        assert body == {"storage_credential_name": "cred", "url": "s3://b/", "read_only": True}
        return {"isDir": True, "results": list(results)}

    return api


class TestTheCredentialCheck:
    def test_a_credential_that_passes_every_check_is_accepted(self):
        lane.validate_storage_credential(
            validation(
                {"operation": "READ", "result": "PASS"},
                {"operation": "LIST", "result": "PASS"},
                {"configuration_operation": "SELF_ASSUME_ROLE", "result": "PASS"},
                {"configuration_operation": "EXTERNAL_ID_CONDITION", "result": "SKIP"},
            ),
            "p",
            credential="cred",
            url="s3://b/",
        )

    def test_a_failed_check_fails_the_install_and_names_itself(self):
        with pytest.raises(RuntimeError, match="SELF_ASSUME_ROLE: not self-assuming"):
            lane.validate_storage_credential(
                validation(
                    {"operation": "LIST", "result": "PASS"},
                    {
                        "configuration_operation": "SELF_ASSUME_ROLE",
                        "result": "FAIL",
                        "message": "not self-assuming",
                    },
                ),
                "p",
                credential="cred",
                url="s3://b/",
            )

    def test_the_app_is_granted_select_on_each_aws_history_before_the_seal(self):
        # The proofs read as the installer, so only this grant lets the deployed app, which
        # reads as its own service principal, see what the AWS lane delivered.
        from server.lifecycle import UnityCatalogAppGrant

        grant = UnityCatalogAppGrant(
            "TABLE", "main.anti_demo_r6_x.aws_live_orders_history_rds", ("SELECT",)
        )
        assert grant.statement("app-sp") == (
            "GRANT SELECT ON TABLE `main`.`anti_demo_r6_x`.`aws_live_orders_history_rds` "
            "TO `app-sp`"
        )
        source = (Path(__file__).resolve().parent.parent / "server" / "lifecycle.py").read_text(
            encoding="utf-8"
        )
        body = source.split("def _prepare_and_reseal_round6_aws(", 1)[1].split("\ndef ", 1)[0]
        granted = body.index('UnityCatalogAppGrant("TABLE", full_name, ("SELECT",))')
        assert "for full_name in history_tables.values():" in body[granted - 200 : granted]
        assert "sealed6.app_service_principal_client_id" in body[granted : granted + 200]
        assert granted < body.index("manifest.round6_aws = seal(")

    def test_the_stage_has_the_credential_checked_on_the_bucket(self):
        # Through `ensure_external_location`, which checks only before it first creates.
        source = (Path(__file__).resolve().parent.parent / "server" / "lifecycle.py").read_text(
            encoding="utf-8"
        )
        body = source.split("def _prepare_and_reseal_round6_aws(", 1)[1].split("\ndef ", 1)[0]
        assert "validate_storage_credential(" not in body
        location = body.index("ensure_external_location(")
        assert "validate_url=f\"s3://{lane['bucket']}/\"" in body[location : location + 500]


class TestTheLakehouseRead:
    def test_the_verifier_asks_for_one_order_as_one_insert(self):
        statement = lane.history_read_statement(
            "main.anti_demo_r6_x.aws_live_orders_history_rds",
            "00000000-0000-4000-8000-000000000006",
            "install-rds-abc123",
        )
        assert statement == (
            "SELECT count(*) AS n FROM `main`.`anti_demo_r6_x`.`aws_live_orders_history_rds` "
            "WHERE order_id = '00000000-0000-4000-8000-000000000006' "
            "AND proof_nonce = 'install-rds-abc123' AND Op = 'I'"
        )

    @pytest.mark.parametrize("value", ["x' OR '1'='1", "a;b", "", "x" * 81])
    def test_nothing_but_a_proof_identifier_reaches_the_statement(self, value):
        with pytest.raises(ValueError):
            lane.history_read_statement("c.s.t", value, "nonce")

    def test_the_external_table_is_over_the_lanes_own_location(self):
        assert lane.history_table_statement(
            "main.s.aws_live_orders_history_aurora", "s3://b/delta/aurora/live_orders_history"
        ) == (
            "CREATE TABLE IF NOT EXISTS `main`.`s`.`aws_live_orders_history_aurora` "
            "USING DELTA LOCATION 's3://b/delta/aurora/live_orders_history'"
        )

    def test_the_uc_names_follow_the_run_like_round_sixes_other_names(self):
        assert lane.uc_names(FAKE_RUN_ID) == {
            "storage_credential": "anti_demo_r6_aws_ad_20990101_0000_test",
            "external_location": "anti_demo_r6_aws_ad_20990101_0000_test",
        }


class Dms:
    def __init__(
        self, *, username="round6_capture", statuses=("successful",), busy_tests=0
    ) -> None:
        self.username = username
        self.statuses = list(statuses)
        #: How many test requests DMS refuses because a test of its own is still running.
        self.busy_tests = busy_tests
        self.calls: list[tuple[str, dict]] = []

    def describe_endpoints(self, **arguments):
        self.calls.append(("describe_endpoints", arguments))
        return {"Endpoints": [{"EndpointType": "SOURCE", "Username": self.username}]}

    def modify_endpoint(self, **arguments):
        self.calls.append(("modify_endpoint", arguments))

    def test_connection(self, **arguments):
        self.calls.append(("test_connection", arguments))
        if self.busy_tests:
            self.busy_tests -= 1
            raise ClientError(
                {"Error": {"Code": "InvalidResourceStateFault", "Message": "test in progress"}},
                "TestConnection",
            )

    def describe_connections(self, **arguments):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return {"Connections": [{"Status": status, "LastFailureMessage": "password rejected"}]}


class TestTheEndpoints:
    def test_the_password_goes_only_to_the_capture_roles_endpoint(self):
        dms = Dms()
        lane.set_endpoint_password(dms, "arn:e", "pw")
        assert ("modify_endpoint", {"EndpointArn": "arn:e", "Password": "pw"}) in dms.calls

    def test_an_endpoint_logging_in_as_anyone_else_gets_no_password(self):
        dms = Dms(username="antidemo_admin")
        with pytest.raises(RuntimeError, match="does not log in as round6_capture"):
            lane.set_endpoint_password(dms, "arn:e", "pw")
        assert "modify_endpoint" not in [name for name, _ in dms.calls]

    def test_a_connection_test_waits_for_dms_answer(self):
        clock = Clock()
        dms = Dms(statuses=["testing", "testing", "successful"])
        lane.test_endpoint(
            dms, instance_arn="arn:i", endpoint_arn="arn:e", clock=clock, sleep=clock.sleep
        )

    def test_a_failed_connection_test_says_why(self):
        dms = Dms(statuses=["failed"])
        with pytest.raises(RuntimeError, match="password rejected"):
            lane.test_endpoint(dms, instance_arn="arn:i", endpoint_arn="arn:e")

    def test_a_test_refused_while_dms_runs_its_own_is_asked_for_again(self):
        # The incident: DMS was already testing the endpoint, and refused the installer's test.
        clock = Clock()
        dms = Dms(statuses=["testing", "successful"], busy_tests=2)
        lane.test_endpoint(
            dms, instance_arn="arn:i", endpoint_arn="arn:e", clock=clock, sleep=clock.sleep
        )
        assert [name for name, _ in dms.calls].count("test_connection") == 3

    def test_a_test_dms_never_accepts_fails_at_the_bound(self):
        clock = Clock()
        dms = Dms(busy_tests=10_000)
        with pytest.raises(ClientError, match="InvalidResourceStateFault"):
            lane.test_endpoint(
                dms, instance_arn="arn:i", endpoint_arn="arn:e", clock=clock, sleep=clock.sleep
            )
        assert clock.value >= lane.ENDPOINT_TEST_SECONDS

    def test_any_other_refusal_is_not_retried(self):
        class Denied(Dms):
            def test_connection(self, **arguments):
                self.calls.append(("test_connection", arguments))
                raise ClientError(
                    {"Error": {"Code": "AccessDeniedFault", "Message": "no"}}, "TestConnection"
                )

        dms = Denied()
        with pytest.raises(ClientError, match="AccessDeniedFault"):
            lane.test_endpoint(dms, instance_arn="arn:i", endpoint_arn="arn:e")
        assert [name for name, _ in dms.calls] == ["test_connection"]


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


class FakeTask:
    def __init__(self, *, running=True, failure="") -> None:
        self.events: list[str] = []
        self.running = running
        self.failure = failure

    def park(self, notify=None):
        self.events.append("task.park")

    def start(self):
        self.events.append("task.start")
        return "start-replication", 0.0

    def wait_running(self):
        self.events.append("task.wait_running")

    def state(self):
        return DmsTaskState(status="running" if self.running else "failed", failure=self.failure)


class FakeWriter:
    def __init__(self, events: list[str], states=("RUNNING",), error="") -> None:
        self.events = events
        self.states = list(states)
        self.error = error

    def park(self, notify=None):
        self.events.append("writer.park")

    def start(self, arguments):
        self.events.append("writer.start")
        return "jr_1", 0.0

    def run(self, run_id):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return GlueRun(run_id, state, None, None, self.error)


def prove(task, writer, *, reads=(True,), table=(True,), timeout=900.0):
    clock = Clock()
    answers, tables = list(reads), list(table)
    events = task.events
    committed: list[str] = []

    def commit(order_id, nonce):
        events.append("commit")
        committed.append(order_id)

    return lane.prove_lane(
        task,
        writer,
        competitor="rds",
        wait_for_slot=lambda: events.append("slot"),
        commit_proof=commit,
        withdraw_proof=lambda order_id: events.append(
            "withdraw" if order_id in committed else "withdraw-unknown"
        ),
        ensure_history_table=lambda: tables.pop(0) if len(tables) > 1 else tables[0],
        read_history=lambda order_id, nonce: answers.pop(0) if len(answers) > 1 else answers[0],
        notify=lambda _message: None,
        timeout_seconds=timeout,
        clock=clock,
        sleep=clock.sleep,
    )


class TestTheProof:
    def test_the_slot_exists_before_the_proof_order_and_both_halves_park_either_side(self):
        task = FakeTask()
        writer = FakeWriter(task.events)
        prove(task, writer, reads=(False, True))
        assert task.events == [
            "writer.park",
            "task.park",
            "task.start",
            "task.wait_running",
            "slot",
            "commit",
            "writer.start",
            "writer.park",
            "task.park",
            "withdraw",
        ]

    def test_the_lakehouse_is_read_only_once_the_writer_has_made_its_table(self):
        task = FakeTask()
        reads: list[str] = []
        writer = FakeWriter(task.events)
        clock = Clock()
        tables = [False, False, True]

        lane.prove_lane(
            task,
            writer,
            competitor="aurora",
            wait_for_slot=lambda: None,
            commit_proof=lambda order_id, nonce: None,
            withdraw_proof=lambda order_id: None,
            ensure_history_table=lambda: tables.pop(0),
            read_history=lambda order_id, nonce: reads.append(order_id) or True,
            notify=lambda _message: None,
            clock=clock,
            sleep=clock.sleep,
        )
        assert len(reads) == 1

    def test_a_writer_that_ends_fails_the_proof_and_still_parks_and_withdraws(self):
        task = FakeTask()
        writer = FakeWriter(task.events, states=["RUNNING", "FAILED"], error="AccessDenied")
        with pytest.raises(RuntimeError, match="ended FAILED.*AccessDenied"):
            prove(task, writer, reads=(False,))
        assert task.events[-3:] == ["writer.park", "task.park", "withdraw"]

    def test_a_task_that_stops_fails_the_proof_with_dms_reason(self):
        task = FakeTask(running=False, failure="slot has been invalidated")
        writer = FakeWriter(task.events)
        with pytest.raises(RuntimeError, match="slot has been invalidated"):
            prove(task, writer, reads=(False,))
        assert task.events[-1] == "withdraw"

    def test_a_proof_that_never_arrives_is_a_named_timeout(self):
        task = FakeTask()
        writer = FakeWriter(task.events)
        with pytest.raises(RuntimeError, match="does not read the proof order"):
            prove(task, writer, reads=(False,), timeout=60.0)
        with pytest.raises(RuntimeError, match="has not made its Delta table"):
            prove(FakeTask(), FakeWriter([]), table=(False,), timeout=60.0)

    def test_nothing_is_withdrawn_when_nothing_was_committed(self):
        task = FakeTask()

        def no_slot():
            raise RuntimeError("no slot")

        with pytest.raises(RuntimeError, match="no slot"):
            lane.prove_lane(
                task,
                FakeWriter(task.events),
                competitor="rds",
                wait_for_slot=no_slot,
                commit_proof=lambda order_id, nonce: task.events.append("commit"),
                withdraw_proof=lambda order_id: task.events.append("withdraw"),
                ensure_history_table=lambda: True,
                read_history=lambda order_id, nonce: True,
                notify=lambda _message: None,
            )
        assert "commit" not in task.events and "withdraw" not in task.events
        assert task.events[-2:] == ["writer.park", "task.park"]

    def test_every_proof_order_is_new(self):
        first, second = lane.new_proof_identity("rds"), lane.new_proof_identity("rds")
        assert first != second
        assert first[1].startswith("install-rds-")


def lane_output(**overrides) -> dict:
    output = {
        "bucket": "lakebase-ant-ad-20260-i0123456789abcdef0123-r6-cdc",
        "script_key": "scripts/round6_writer.py",
        "script_sha256": lane.script_sha256(),
        "glue_role_arn": "arn:aws:iam::123456789012:role/i0123-r6-glue-1",
        "dms_s3_role_arn": "arn:aws:iam::123456789012:role/i0123-r6-dms-s3-1",
        "uc_role_arn": ROLE,
        "subnet_ids": [fake_aws_id("subnet", "a"), fake_aws_id("subnet", "b")],
        "subnet_cidr": "172.31.99.0/24",
        "route_table_id": fake_aws_id("rtb", "c"),
        "s3_endpoint_id": fake_aws_id("vpce", "d"),
        "security_group_id": fake_aws_id("sg", "e"),
        "replication_instance_arn": "arn:aws:dms:us-west-2:123456789012:rep:REPLICATIONAAA",
        "source_endpoints": {
            competitor: f"arn:aws:dms:us-west-2:123456789012:endpoint:SRC{competitor.upper()}"
            for competitor in ("aurora", "rds")
        },
        "target_endpoints": {
            competitor: f"arn:aws:dms:us-west-2:123456789012:endpoint:DST{competitor.upper()}"
            for competitor in ("aurora", "rds")
        },
        "tasks": {
            competitor: f"arn:aws:dms:us-west-2:123456789012:task:TASK{competitor.upper()}"
            for competitor in ("aurora", "rds")
        },
        "jobs": {competitor: f"lane-writer-{competitor}" for competitor in ("aurora", "rds")},
        "history_locations": {
            competitor: (
                f"s3://lakebase-ant-ad-20260-i0123456789abcdef0123-r6-cdc/delta/{competitor}/"
                "live_orders_history"
            )
            for competitor in ("aurora", "rds")
        },
    }
    output.update(overrides)
    return output


HISTORY = {
    competitor: f"main.anti_demo_r6_x.aws_live_orders_history_{competitor}"
    for competitor in ("aurora", "rds")
}
NAMES = {"storage_credential": "anti_demo_r6_aws_x", "external_location": "anti_demo_r6_aws_x"}


class TestTheSeal:
    def test_the_seal_carries_each_competitors_own_half(self):
        sealed = lane.seal(
            lane_output(), external_id=EXTERNAL_ID, names=NAMES, history_tables=HISTORY
        )
        assert sealed.lane("rds_postgres").task_arn.endswith(":task:TASKRDS")
        assert sealed.lane("aurora").history_table_full_name.endswith("_aurora")
        assert sealed.uc_external_id == EXTERNAL_ID
        assert sealed.capture_role == "round6_capture"

    def test_a_script_other_than_this_checkouts_is_refused(self):
        with pytest.raises(RuntimeError, match="other than glue/round6_writer.py"):
            lane.seal(
                lane_output(script_sha256="0" * 64),
                external_id=EXTERNAL_ID,
                names=NAMES,
                history_tables=HISTORY,
            )

    def test_two_lanes_may_not_share_a_task(self):
        shared = lane_output()
        shared["tasks"] = {competitor: shared["tasks"]["rds"] for competitor in ("aurora", "rds")}
        with pytest.raises(ValueError, match="must not share a task"):
            lane.seal(shared, external_id=EXTERNAL_ID, names=NAMES, history_tables=HISTORY)

    def test_a_history_table_outside_the_lanes_bucket_is_refused(self):
        moved = lane_output()
        moved["history_locations"] = {
            **moved["history_locations"],
            "rds": "s3://someone-elses-bucket/delta/rds/live_orders_history",
        }
        with pytest.raises(ValueError, match="lane's own bucket"):
            lane.seal(moved, external_id=EXTERNAL_ID, names=NAMES, history_tables=HISTORY)
