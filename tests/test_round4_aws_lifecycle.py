"""Round 4's AWS lane at install time: the proof run, the credential and the seal.

The lane is sealed only after each Glue job has carried the source's baseline into its target,
so these pin the proof's every way out: success, a run that ends, a run that never gets there,
and that the job is parked whichever way it goes.
"""

from __future__ import annotations

import pytest

from server import round4_aws_lifecycle as lane
from server.manifest import Round4AwsResources
from server.model_score import ModelScoreRow
from server.round4_glue import GlueLaneError, GlueRun

BASELINE = ModelScoreRow("customer-0001", 0.25, "risk-v0", "round4-baseline")


WROTE = {"stream_started_at": "t0", "first_batch_applied_at": "t1"}


class FakeWriter:
    def __init__(self, states, *, error="", markers=(WROTE,)):
        self.states = list(states)
        self.error = error
        self.markers = list(markers)
        self.marker_reads: list[tuple[str, str]] = []
        self.parks = 0
        self.started: list[dict[str, str]] = []

    def park(self, notify=None):
        self.parks += 1

    def start(self, arguments):
        self.started.append(dict(arguments))
        return "jr_1", 0.0

    def run(self, run_id):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return GlueRun(run_id, state, None, None, self.error)

    def marker(self, competitor, run_tag):
        self.marker_reads.append((competitor, run_tag))
        return self.markers.pop(0) if len(self.markers) > 1 else self.markers[0]


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


def prove(writer, reads, *, timeout=600.0):
    clock = Clock()
    answers = list(reads)
    return lane.prove_lane(
        writer,
        competitor="rds",
        source_table_id="table-1",
        expected=BASELINE,
        read_target=lambda: answers.pop(0) if len(answers) > 1 else answers[0],
        notify=lambda _message: None,
        timeout_seconds=timeout,
        poll_seconds=5.0,
        clock=clock,
        sleep=clock.sleep,
    )


def test_a_proof_passes_once_the_target_reads_the_baseline_and_parks_either_side():
    writer = FakeWriter(["STARTING", "RUNNING", "RUNNING"])

    # No read while the run is starting; one miss, then the baseline.
    elapsed = prove(writer, [None, BASELINE])

    assert elapsed == 10.0
    assert writer.parks == 2
    # The proof reads the snapshot: it knows nothing about the target but what it must read.
    assert writer.started == [
        {
            "--run_tag": writer.started[0]["--run_tag"],
            "--source_table_id": "table-1",
            "--starting_version": "snapshot",
        }
    ]
    assert writer.started[0]["--run_tag"].startswith("install-")


def test_a_target_read_is_trusted_only_while_the_run_is_running():
    # A row left by an earlier run must not pass a proof whose own run is still starting.
    writer = FakeWriter(["STARTING", "STARTING", "RUNNING"])

    elapsed = prove(writer, [BASELINE])

    assert elapsed == 10.0


def test_a_target_already_at_the_baseline_passes_only_once_the_run_has_applied_a_batch():
    # On the test installation's first re-run, both proofs "passed" in 2-3 s: the targets held
    # the baseline from the last bout before the new script had done anything.
    writer = FakeWriter(
        ["RUNNING"],
        markers=[None, {"stream_started_at": "t0"}, WROTE],
    )

    elapsed = prove(writer, [BASELINE])

    assert elapsed == 10.0
    assert writer.marker_reads == [("rds", writer.started[0]["--run_tag"])] * 3


def test_a_run_that_never_applies_a_batch_fails_the_proof_saying_so():
    writer = FakeWriter(["RUNNING"], markers=[{"stream_started_at": "t0"}])

    with pytest.raises(GlueLaneError, match="it has not applied a batch yet"):
        prove(writer, [BASELINE], timeout=30.0)

    assert writer.parks == 2


def test_a_run_that_ends_fails_the_proof_with_its_own_error_and_is_parked():
    writer = FakeWriter(["STARTING", "FAILED"], error="AccessDenied reading s3://bucket/table")

    with pytest.raises(GlueLaneError, match="AccessDenied reading"):
        prove(writer, [None])

    assert writer.parks == 2


def test_a_run_that_never_carries_the_baseline_fails_by_its_bound_and_is_parked():
    writer = FakeWriter(["RUNNING"])

    with pytest.raises(GlueLaneError, match="within 30s"):
        prove(writer, [ModelScoreRow("customer-0001", 0.81, "risk-v1", "stale")], timeout=30.0)

    assert writer.parks == 2


class FakeGlue:
    def __init__(self, connection):
        self.connection = connection
        self.updates: list[dict] = []

    def get_connection(self, Name, HidePassword):  # noqa: N803 - boto3's keywords
        assert HidePassword is True
        return {"Connection": self.connection}

    def update_connection(self, Name, ConnectionInput):  # noqa: N803
        self.updates.append(ConnectionInput)


def connection(**overrides):
    base = {
        "Name": "lakebase-ant-x-r4-rds",
        "ConnectionType": "JDBC",
        "ConnectionProperties": {
            "JDBC_CONNECTION_URL": "jdbc:postgresql://db.example.internal:5432/anti_demo",
            "USERNAME": "round4_writer",
            "ENCRYPTED_PASSWORD": "old",
        },
        "PhysicalConnectionRequirements": {
            "SubnetId": "subnet-0123456789abcdef0",
            "SecurityGroupIdList": ["sg-0123456789abcdef0"],
            "AvailabilityZone": "us-west-2a",
        },
    }
    base.update(overrides)
    return base


def test_the_password_replaces_only_the_password():
    glue = FakeGlue(connection())

    lane.set_connection_password(glue, "lakebase-ant-x-r4-rds", "new-password")

    (update,) = glue.updates
    assert update["ConnectionProperties"] == {
        "JDBC_CONNECTION_URL": "jdbc:postgresql://db.example.internal:5432/anti_demo",
        "USERNAME": "round4_writer",
        "PASSWORD": "new-password",
    }
    assert update["PhysicalConnectionRequirements"]["SubnetId"] == "subnet-0123456789abcdef0"


def test_a_connection_that_logs_in_as_anyone_else_is_refused():
    glue = FakeGlue(connection(ConnectionProperties={"USERNAME": "antidemo_admin"}))

    with pytest.raises(RuntimeError, match="does not log in as round4_writer"):
        lane.set_connection_password(glue, "lakebase-ant-x-r4-rds", "new-password")
    assert glue.updates == []


def test_a_writer_password_needs_no_quoting():
    password = lane.new_writer_password()

    assert len(password) >= 40
    assert all(character.isalnum() or character in "-_" for character in password)


def terraform_lane(**overrides):
    values = {
        "bucket": "lakebase-ant-x-r4-glue",
        "script_key": "scripts/round4_writer.py",
        "script_sha256": lane.script_sha256(),
        "role_arn": "arn:aws:iam::123456789012:role/ix-r4-glue-1",
        "subnet_id": "subnet-0123456789abcdef0",
        "subnet_cidr": "172.31.99.0/24",
        "route_table_id": "rtb-0123456789abcdef0",
        "s3_endpoint_id": "vpce-0123456789abcdef0",
        "security_group_id": "sg-0123456789abcdef0",
        "source_location": "s3://root-bucket/metastore/tables/abc",
        "jobs": {
            "aurora": "lakebase-ant-x-r4-writer-aurora",
            "rds": "lakebase-ant-x-r4-writer-rds",
        },
        "connections": {"aurora": "lakebase-ant-x-r4-aurora", "rds": "lakebase-ant-x-r4-rds"},
    }
    values.update(overrides)
    return values


def test_the_seal_is_terraform_s_lane():
    sealed = lane.seal(terraform_lane())

    assert isinstance(sealed, Round4AwsResources)
    assert sealed.lane("rds_postgres").job_name == "lakebase-ant-x-r4-writer-rds"
    assert sealed.lane("aurora").connection_name == "lakebase-ant-x-r4-aurora"
    assert sealed.target_view == "model_scores"


def test_a_script_other_than_this_checkout_s_is_never_sealed():
    with pytest.raises(RuntimeError, match="other than glue/round4_writer.py"):
        lane.seal(terraform_lane(script_sha256="0" * 64))


def test_the_source_location_is_unity_catalog_s():
    calls = []

    def api(profile, method, path):
        calls.append((profile, method, path))
        return {"storage_location": "s3://root-bucket/metastore/tables/abc/"}

    assert lane.source_location("p", "c.s.model_scores_source", api) == (
        "s3://root-bucket/metastore/tables/abc"
    )
    assert calls == [("p", "get", "/api/2.1/unity-catalog/tables/c.s.model_scores_source")]


@pytest.mark.parametrize("location", ["", "abfss://container/x", "s3://bucket"])
def test_a_source_the_lane_cannot_read_from_s3_is_refused(location):
    with pytest.raises(RuntimeError, match="no s3:// location"):
        lane.source_location("p", "c.s.t", lambda *_: {"storage_location": location})


def test_the_ledger_is_owned_by_the_writer_and_read_through_the_view():
    rendered = [statement.as_string(None) for statement in lane.target_statements()]

    assert rendered[0] == 'CREATE SCHEMA IF NOT EXISTS "round4" AUTHORIZATION "round4_writer"'
    assert any(
        line.startswith("CREATE TABLE IF NOT EXISTS round4.model_score_ledger (")
        for line in rendered
    )
    assert 'ALTER TABLE "round4"."model_score_ledger" OWNER TO "round4_writer"' in rendered
    assert (
        "CREATE OR REPLACE VIEW round4.model_scores AS SELECT entity_id, score, model_version, "
        "proof_nonce FROM round4.model_score_ledger WHERE NOT deleted"
    ) in rendered
