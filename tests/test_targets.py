import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import boto3
import psycopg
import pytest
from botocore.exceptions import BotoCoreError, ClientError
from botocore.stub import Stubber

from server.capacity import (
    AURORA_AUTO_PAUSE_SECONDS,
    RDS_SCORED_ROUNDS,
    rds_lane_is_scored,
)
from server.cost_model import ACU_SAMPLE_LEAD_SECONDS, ACU_SAMPLE_TAIL_SECONDS
from server.models import RoundId
from server.safe_change_live import (
    CURRENT_USER_PATH,
    LAKEBASE_API_ROOT,
    lakebase_resource_path,
)
from server.targets import (
    AuroraCredentialProvider,
    ConnectionMaterial,
    LakebaseCredentialProvider,
    PsycopgPreparedTarget,
    RdsCredentialProvider,
    TargetConfigurationError,
    TargetNotArmedError,
    _aurora_pause_wait_reason,
)
from server.verifier import FatalProbeError

ACCOUNT_ID = "123456789012"
REGION = "us-west-2"

# A round whose RDS lane is actually raced, so the live control-plane path runs.
# Round 1 no longer reaches AWS at all, so tests that exercise `describe-*` must
# name a round that does.
SCORED_ROUND = RoundId.SURVIVE_CONNECTION_SPIKE
UNSCORED_ROUNDS = tuple(
    round_id for round_id in RoundId if not rds_lane_is_scored(round_id)
)
AURORA_SECRET_ARN = (
    f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:anti-demo-aurora-AbCdEf"
)
RDS_SECRET_ARN = (
    f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:anti-demo-rds-AbCdEf"
)


class FakeStsClient:
    def __init__(self, account_id: str = ACCOUNT_ID) -> None:
        self.account_id = account_id

    def get_caller_identity(self):
        return {"Account": self.account_id, "Arn": "arn:aws:iam::example:user/test"}


class FakeRdsClient:
    def __init__(self, engine: str = "postgres") -> None:
        self.engine = engine

    def describe_db_instances(self, DBInstanceIdentifier: str):
        assert DBInstanceIdentifier == "anti-demo-rds"
        return {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": DBInstanceIdentifier,
                    "DBInstanceArn": f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:db:{DBInstanceIdentifier}",
                    "DBInstanceStatus": "available",
                    "DBInstanceClass": "db.t4g.medium",
                    "Engine": self.engine,
                    "EngineVersion": "17.10",
                    "PubliclyAccessible": True,
                    "Endpoint": {
                        "Address": "anti-demo.example.us-west-2.rds.amazonaws.com",
                        "Port": 5432,
                    },
                    "MasterUserSecret": {"SecretArn": RDS_SECRET_ARN},
                }
            ]
        }


class FakeRdsSecretsClient:
    def get_secret_value(self, SecretId: str):
        assert SecretId == RDS_SECRET_ARN
        return {
            "ARN": RDS_SECRET_ARN,
            "SecretString": json.dumps(
                {
                    "engine": "postgres",
                    "host": "anti-demo.example.us-west-2.rds.amazonaws.com",
                    "port": 5432,
                    "username": "postgres",
                    "password": "not-logged",
                    "dbInstanceIdentifier": "anti-demo-rds",
                }
            ),
        }


class FakeRdsSession:
    def __init__(self, engine: str = "postgres", account_id: str = ACCOUNT_ID) -> None:
        self.rds = FakeRdsClient(engine)
        self.sts = FakeStsClient(account_id)
        self.secrets = FakeRdsSecretsClient()

    def client(self, service: str):
        return {
            "rds": self.rds,
            "sts": self.sts,
            "secretsmanager": self.secrets,
        }[service]


class FakeAuroraRdsClient:
    def __init__(
        self,
        *,
        secret_arn: str = AURORA_SECRET_ARN,
        readers: int = 0,
        events: list[dict[str, object]] | None = None,
    ) -> None:
        self.secret_arn = secret_arn
        self.readers = readers
        self.events = events or []

    def describe_db_clusters(self, DBClusterIdentifier: str):
        members = [
            {
                "DBInstanceIdentifier": "anti-demo-aurora-writer",
                "IsClusterWriter": True,
            }
        ]
        members.extend(
            {
                "DBInstanceIdentifier": f"anti-demo-reader-{index}",
                "IsClusterWriter": False,
            }
            for index in range(self.readers)
        )
        return {
            "DBClusters": [
                {
                    "DBClusterIdentifier": DBClusterIdentifier,
                    "DBClusterArn": (
                        f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:cluster:{DBClusterIdentifier}"
                    ),
                    "Engine": "aurora-postgresql",
                    "EngineVersion": "17.10",
                    "Status": "available",
                    "Endpoint": "anti-demo.cluster-example.us-west-2.rds.amazonaws.com",
                    "Port": 5432,
                    "DBClusterMembers": members,
                    "MasterUserSecret": {"SecretArn": self.secret_arn},
                    "ServerlessV2ScalingConfiguration": {
                        "MinCapacity": 0,
                        "MaxCapacity": 2,
                        "SecondsUntilAutoPause": 300,
                    },
                }
            ]
        }

    def describe_db_instances(self, DBInstanceIdentifier: str):
        return {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": DBInstanceIdentifier,
                    "DBInstanceArn": (
                        f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:db:{DBInstanceIdentifier}"
                    ),
                    "DBClusterIdentifier": "anti-demo-aurora",
                    "DBInstanceStatus": "available",
                    "DBInstanceClass": "db.serverless",
                    "Engine": "aurora-postgresql",
                }
            ]
        }

    def describe_events(self, **kwargs):
        assert kwargs["SourceIdentifier"] == "anti-demo-aurora"
        assert kwargs["SourceType"] == "db-cluster"
        assert kwargs["EventCategories"] == ["serverless"]
        return {"Events": self.events}


class FakeCloudWatchClient:
    # `capacity_acu` is what the writer is reporting. It defaults to the parked
    # zero every other test in this file relies on; a non-zero value is what an
    # awake or descending writer looks like, which is the only state in which
    # the sampler's own failure text ever reached the screen.
    def __init__(self, capacity_acu: float = 0.0) -> None:
        self.dimensions = None
        self.capacity_acu = capacity_acu

    def get_metric_statistics(self, **kwargs):
        self.dimensions = kwargs["Dimensions"]
        now = datetime.now(UTC)
        return {
            "Datapoints": [
                {"Timestamp": now - timedelta(seconds=90), "Maximum": self.capacity_acu},
                {"Timestamp": now - timedelta(seconds=30), "Maximum": self.capacity_acu},
            ]
        }


class FakeSecretsClient:
    def __init__(self, host: str | None) -> None:
        self.host = host

    def get_secret_value(self, SecretId: str):
        assert SecretId == AURORA_SECRET_ARN
        payload = {
            "port": 5432,
            "username": "postgres",
            "password": "not-logged",
            "dbClusterIdentifier": "anti-demo-aurora",
        }
        if self.host is not None:
            payload["host"] = self.host
        return {
            "ARN": AURORA_SECRET_ARN,
            "SecretString": json.dumps(payload),
        }


class FakeAuroraSession:
    def __init__(
        self,
        *,
        secret_host: str | None = "anti-demo.cluster-example.us-west-2.rds.amazonaws.com",
        readers: int = 0,
        events: list[dict[str, object]] | None = None,
        capacity_acu: float = 0.0,
    ) -> None:
        self.rds = FakeAuroraRdsClient(readers=readers, events=events)
        self.sts = FakeStsClient()
        self.cloudwatch = FakeCloudWatchClient(capacity_acu)
        self.secrets = FakeSecretsClient(secret_host)

    def client(self, service: str):
        return {
            "rds": self.rds,
            "sts": self.sts,
            "cloudwatch": self.cloudwatch,
            "secretsmanager": self.secrets,
        }[service]


class FakeLakebaseRunner:
    """The control plane as REST, which is how the deployed app can reach it.

    Every arm and every bout goes through this seam, and it used to be a
    `databricks` subprocess -- a binary the app never installs. Asserting on the
    method and path is what keeps this suite honest about that: a fake that
    matched substrings of an argv line would go on passing after the app lost
    the ability to make the call at all.
    """

    def __init__(self, endpoint: dict[str, object]) -> None:
        self.endpoint = endpoint
        self.requests: list[tuple[str, str]] = []

    async def json(self, method: str, path: str, *, body=None, timeout_seconds: float = 120.0):
        del timeout_seconds
        self.requests.append((method, path))
        if method == "GET" and path == lakebase_resource_path(str(self.endpoint["name"])):
            return self.endpoint
        if method == "POST" and path == f"{LAKEBASE_API_ROOT}/credentials":
            assert body == {"endpoint": self.endpoint["name"]}
            return {"token": "oauth-token"}
        if method == "GET" and path == CURRENT_USER_PATH:
            return {"userName": "operator@databricks.com"}
        raise AssertionError((method, path))


def configure_aws(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_PROFILE", "anti-demo-admin")
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("AWS_EXPECTED_ACCOUNT_ID", ACCOUNT_ID)


def configure_lakebase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_PROFILE", "fe-vm-test")
    monkeypatch.setenv(
        "LAKEBASE_ENDPOINT_NAME",
        "projects/anti-demo/branches/production/endpoints/primary",
    )
    monkeypatch.setenv("LAKEBASE_EXPECTED_REGION", REGION)


def configure_rds(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_aws(monkeypatch)
    monkeypatch.setenv("RDS_INSTANCE_ID", "anti-demo-rds")
    monkeypatch.setenv("RDS_SECRET_ARN", RDS_SECRET_ARN)
    monkeypatch.setenv("RDS_DATABASE", "anti_demo")


def configure_aurora(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_aws(monkeypatch)
    monkeypatch.setenv("AURORA_CLUSTER_ID", "anti-demo-aurora")
    monkeypatch.setenv("AURORA_SECRET_ARN", AURORA_SECRET_ARN)


async def test_rds_provider_qualifies_a_real_postgres_instance_as_ineligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_rds(monkeypatch)
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: FakeRdsSession())

    evidence = await RdsCredentialProvider(round_id=SCORED_ROUND).assert_armed()

    assert evidence["eligible"] is False
    assert evidence["state"] == "NO_SCALE_TO_ZERO"
    assert evidence["db_instance_identifier"] == "anti-demo-rds"
    assert evidence["engine"] == "postgres"
    assert evidence["publicly_accessible"] is True
    assert "manual stop/start" in str(evidence["reason"])


async def test_rds_provider_uses_profileless_session_for_environment_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_AUTH_MODE", "environment")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("AWS_EXPECTED_ACCOUNT_ID", ACCOUNT_ID)
    monkeypatch.setenv("RDS_INSTANCE_ID", "anti-demo-rds")
    calls: list[dict[str, str]] = []

    def session_factory(**kwargs: str) -> FakeRdsSession:
        calls.append(kwargs)
        return FakeRdsSession()

    monkeypatch.setattr("server.targets.boto3.Session", session_factory)

    await RdsCredentialProvider(round_id=SCORED_ROUND).assert_armed()

    assert calls == [{"region_name": REGION}]


async def test_rds_managed_secret_supplies_credentials_but_not_target_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_rds(monkeypatch)
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: FakeRdsSession())

    material = await RdsCredentialProvider().connection_material()

    assert material.host == "anti-demo.example.us-west-2.rds.amazonaws.com"
    assert material.database == "anti_demo"


async def test_rds_provider_rejects_a_non_postgres_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_rds(monkeypatch)
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: FakeRdsSession(engine="mysql"),
    )

    with pytest.raises(TargetConfigurationError, match="not an Amazon RDS for PostgreSQL"):
        await RdsCredentialProvider(round_id=SCORED_ROUND).assert_armed()


async def test_aws_provider_requires_explicit_expected_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_rds(monkeypatch)
    monkeypatch.delenv("AWS_EXPECTED_ACCOUNT_ID")

    with pytest.raises(TargetConfigurationError, match="AWS_EXPECTED_ACCOUNT_ID"):
        await RdsCredentialProvider(round_id=SCORED_ROUND).assert_armed()


class ExplodingSession:
    """A session factory that fails the test simply by being called.

    Stronger than counting calls after the fact: if anything in the arming path
    tries to build a session, the test dies at the point of the attempt and names
    the arguments it was about to go to AWS with.
    """

    def __init__(self) -> None:
        self.attempts: list[dict[str, str]] = []

    def __call__(self, **kwargs: str) -> object:
        self.attempts.append(kwargs)
        raise AssertionError(
            f"the arming path built an AWS session for an unscored round: {kwargs}"
        )


@pytest.mark.parametrize("round_id", UNSCORED_ROUNDS, ids=str)
async def test_an_unscored_rds_lane_refuses_without_reaching_aws(
    monkeypatch: pytest.MonkeyPatch,
    round_id: RoundId,
) -> None:
    """The refusal is a property of the engine, so it needs no instance to observe.

    This is the test that lets Round 1's RDS instance be deleted. The environment
    here carries no ``RDS_INSTANCE_ID``, no ``AWS_EXPECTED_ACCOUNT_ID`` and no
    region -- the state an operator is in once the instance, its managed secret
    and its manifest fields are gone -- and the lane must still return its
    ``NO_SCALE_TO_ZERO`` verdict rather than failing to arm on a
    ``DBInstanceNotFound`` or on missing configuration.

    Before the round-aware short circuit, every one of these raised
    ``TargetConfigurationError`` from ``_require`` before AWS was even reached.
    """

    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_REGION",
        "AWS_EXPECTED_ACCOUNT_ID",
        "RDS_INSTANCE_ID",
        "RDS_SECRET_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    session_factory = ExplodingSession()
    monkeypatch.setattr("server.targets.boto3.Session", session_factory)

    evidence = await RdsCredentialProvider(round_id=round_id).assert_armed()

    assert evidence["eligible"] is False
    assert evidence["state"] == "NO_SCALE_TO_ZERO"
    assert evidence["engine"] == "postgres"
    assert evidence["basis"] == "engine_capability"
    assert evidence["aws_calls"] == 0
    assert evidence["round_id"] == str(round_id)
    assert session_factory.attempts == []


async def test_the_refusal_reads_identically_whether_or_not_a_box_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deleting the instance must not change one word a reader sees.

    The user-facing sentence is the whole output of this lane. If the structural
    path and the live path worded it differently, removing Round 1's instance
    would silently rewrite the screen.
    """

    configure_rds(monkeypatch)
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: FakeRdsSession())
    observed = await RdsCredentialProvider(round_id=SCORED_ROUND).assert_armed()

    monkeypatch.delenv("RDS_INSTANCE_ID")
    structural = await RdsCredentialProvider(round_id=RoundId.WAKE_IDLE_APP).assert_armed()

    assert structural["reason"] == observed["reason"]
    assert structural["state"] == observed["state"]
    assert "manual stop/start" in str(structural["reason"])


async def test_the_refusal_claims_no_observation_it_did_not_make(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent box has no class, version or status, and must not report one.

    The capacity disclosure stamps observed capacity from these keys. Filling
    them from the configured constants would put `db.t4g.medium` on screen as an
    observation of an instance that does not exist -- the exact substitution of a
    modelled value for a measured one this project keeps correcting.
    """

    monkeypatch.setattr("server.targets.boto3.Session", ExplodingSession())

    evidence = await RdsCredentialProvider(round_id=RoundId.WAKE_IDLE_APP).assert_armed()

    for absent in (
        "instance_class",
        "engine_version",
        "db_instance_identifier",
        "db_instance_status",
        "publicly_accessible",
    ):
        assert absent not in evidence


async def test_a_scored_round_still_describes_the_instance_through_botocore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The short circuit must not quietly disable the lane that is raced.

    Stubbed at the botocore layer rather than with a hand-written fake, so the
    request parameters and the response shape are both validated against the RDS
    service model. A fake that skipped that validation is how the ``RestoreToTime``
    defect survived.
    """

    # Built before the profile is set, so the real client resolves no profile.
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    rds = boto3.Session(
        aws_access_key_id="test-access",
        aws_secret_access_key="test-secret",
        region_name=REGION,
    ).client("rds")
    configure_rds(monkeypatch)
    stub = Stubber(rds)
    stub.add_response(
        "describe_db_instances",
        {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "anti-demo-rds",
                    "DBInstanceArn": f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:db:anti-demo-rds",
                    "DBInstanceStatus": "available",
                    "DBInstanceClass": "db.t4g.medium",
                    "Engine": "postgres",
                    "EngineVersion": "17.10",
                    "PubliclyAccessible": True,
                }
            ]
        },
        {"DBInstanceIdentifier": "anti-demo-rds"},
    )
    stub.activate()

    class StubbedSession:
        def client(self, name: str):
            return rds if name == "rds" else FakeStsClient()

    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: StubbedSession())

    evidence = await RdsCredentialProvider(round_id=SCORED_ROUND).assert_armed()

    stub.assert_no_pending_responses()
    assert evidence["instance_class"] == "db.t4g.medium"
    assert evidence["engine_version"] == "17.10"
    assert evidence["state"] == "NO_SCALE_TO_ZERO"


def test_round_one_is_not_a_scored_rds_lane() -> None:
    """The premise the deletion rests on, asserted rather than assumed.

    Round 1 races Aurora and keeps its cluster. Its RDS lane is never prepared,
    connected to or timed, which is why Terraform stands no instance up for it.
    """

    assert not rds_lane_is_scored(RoundId.WAKE_IDLE_APP)
    assert RoundId.WAKE_IDLE_APP in UNSCORED_ROUNDS
    assert RDS_SCORED_ROUNDS == {
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        RoundId.RECOVER_DELETED_ORDER,
        RoundId.SURVIVE_CONNECTION_SPIKE,
    }


def idle_lakebase_endpoint() -> dict[str, object]:
    return {
        "name": "projects/anti-demo/branches/production/endpoints/primary",
        "status": {
            "current_state": "IDLE",
            "hosts": {"host": "ep-example.database.us-west-2.cloud.databricks.com"},
        },
    }


def local_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    """A laptop, stated rather than inherited from whatever exported the suite.

    `conftest` does not scrub these two, so a shell that happened to export
    either one would silently move every case below onto the deployed branch.
    """

    for name in ("ANTI_DEMO_ENV", "DATABRICKS_APP_NAME"):
        monkeypatch.delenv(name, raising=False)


async def test_lakebase_still_demands_a_profile_on_a_laptop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The local path, which is the one the operator's own server runs on.

    Nothing about the deployed waiver below is allowed to reach here: a laptop
    has several credential sets and a profile is how one of them gets chosen, so
    an unset `DATABRICKS_PROFILE` is still a refusal rather than an invitation to
    guess.
    """

    configure_lakebase(monkeypatch)
    local_runtime(monkeypatch)
    monkeypatch.delenv("DATABRICKS_PROFILE")
    runner = FakeLakebaseRunner(idle_lakebase_endpoint())

    with pytest.raises(TargetConfigurationError, match="DATABRICKS_PROFILE"):
        await LakebaseCredentialProvider(runner=runner).assert_armed()

    assert runner.requests == []


async def test_lakebase_waives_only_the_profile_when_deployed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 1 refused in the deployed app for a binding it may not hold.

    `app.py:_bind_deployed_runtime` pops `DATABRICKS_PROFILE` so the app can only
    authenticate as its ambient OAuth identity, and `DatabricksRestRunner`
    already reads an empty profile as "use the ambient client". Demanding it here
    too was the whole of the Round 1 failure, and the waiver is scoped to that
    one name -- the endpoint, database and region are still required, because
    those the deployment does hold and a wrong one addresses the wrong database.
    """

    configure_lakebase(monkeypatch)
    monkeypatch.delenv("DATABRICKS_PROFILE")
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    runner = FakeLakebaseRunner(idle_lakebase_endpoint())

    evidence = await LakebaseCredentialProvider(runner=runner).assert_armed()

    assert evidence["state"] == "IDLE"
    monkeypatch.delenv("LAKEBASE_EXPECTED_REGION")
    with pytest.raises(TargetConfigurationError, match="LAKEBASE_EXPECTED_REGION"):
        await LakebaseCredentialProvider(runner=runner).assert_armed()


async def test_lakebase_profile_waiver_asks_the_runtime_binders_own_predicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One predicate, so the waiver cannot disagree with the pop causing it.

    `selfheal.deployed` is what `app.py` and the runtime binder already answer.
    Asserting through it rather than re-reading the variables is what stops a
    third spelling of "is this the app" appearing in this module: a rename there
    fails this test instead of quietly restoring the Round 1 refusal.
    """

    configure_lakebase(monkeypatch)
    monkeypatch.delenv("DATABRICKS_PROFILE")
    local_runtime(monkeypatch)
    monkeypatch.setattr("server.selfheal.deployed", lambda environ=None: True)
    runner = FakeLakebaseRunner(idle_lakebase_endpoint())

    assert (await LakebaseCredentialProvider(runner=runner).assert_armed())["state"] == "IDLE"

    monkeypatch.setattr("server.selfheal.deployed", lambda environ=None: False)
    with pytest.raises(TargetConfigurationError, match="DATABRICKS_PROFILE"):
        await LakebaseCredentialProvider(runner=runner).assert_armed()


@pytest.mark.parametrize("marker", ["ANTI_DEMO_ENV", "DATABRICKS_APP_NAME"])
async def test_lakebase_profile_waiver_follows_both_deployment_markers(
    monkeypatch: pytest.MonkeyPatch,
    marker: str,
) -> None:
    """Either name means deployed, and a near-miss value does not."""

    configure_lakebase(monkeypatch)
    local_runtime(monkeypatch)
    monkeypatch.delenv("DATABRICKS_PROFILE")
    monkeypatch.setenv(marker, "databricks-app" if marker == "ANTI_DEMO_ENV" else "some-app")
    runner = FakeLakebaseRunner(idle_lakebase_endpoint())

    assert (await LakebaseCredentialProvider(runner=runner).assert_armed())["state"] == "IDLE"

    monkeypatch.setenv(marker, "")
    with pytest.raises(TargetConfigurationError, match="DATABRICKS_PROFILE"):
        await LakebaseCredentialProvider(runner=runner).assert_armed()


async def test_lakebase_never_falls_back_to_generic_pg_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_lakebase(monkeypatch)
    monkeypatch.setenv("PGHOST", "wrong.example.com")
    runner = FakeLakebaseRunner(
        {
            "name": "projects/anti-demo/branches/production/endpoints/primary",
            "status": {"hosts": {}},
        }
    )

    with pytest.raises(TargetConfigurationError, match="unrecognized region"):
        await LakebaseCredentialProvider(runner=runner).connection_material()


async def test_lakebase_rejects_a_cross_region_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_lakebase(monkeypatch)
    runner = FakeLakebaseRunner(
        {
            "name": "projects/anti-demo/branches/production/endpoints/primary",
            "status": {
                "current_state": "IDLE",
                "hosts": {
                    "host": "ep-example.database.us-east-1.cloud.databricks.com"
                },
            },
        }
    )

    with pytest.raises(TargetConfigurationError, match="us-east-1, expected us-west-2"):
        await LakebaseCredentialProvider(runner=runner).assert_armed()


async def test_lakebase_idle_evidence_carries_the_fresh_observation_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_lakebase(monkeypatch)
    runner = FakeLakebaseRunner(
        {
            "name": "projects/anti-demo/branches/production/endpoints/primary",
            "status": {
                "current_state": "IDLE",
                "hosts": {
                    "host": "ep-example.database.us-west-2.cloud.databricks.com"
                },
            },
        }
    )
    before = datetime.now(UTC)

    evidence = await LakebaseCredentialProvider(runner=runner).assert_armed()

    observed_at = datetime.fromisoformat(str(evidence["observed_at"]))
    assert before <= observed_at <= datetime.now(UTC)


async def test_lakebase_idle_evidence_uses_the_provider_transition_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_lakebase(monkeypatch)
    started_at = datetime.now(UTC) - timedelta(seconds=60)
    transitioned_at = started_at + timedelta(seconds=58)
    runner = FakeLakebaseRunner(
        {
            "name": "projects/anti-demo/branches/production/endpoints/primary",
            "update_time": transitioned_at.isoformat(),
            "status": {
                "current_state": "IDLE",
                "hosts": {
                    "host": "ep-example.database.us-west-2.cloud.databricks.com"
                },
            },
        }
    )

    evidence = await LakebaseCredentialProvider(runner=runner).assert_armed(
        not_before=started_at
    )

    assert datetime.fromisoformat(str(evidence["observed_at"])) == transitioned_at


async def test_aurora_proves_two_writer_level_zero_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    session = FakeAuroraSession()
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    evidence = await AuroraCredentialProvider().assert_armed()

    assert evidence["state"] == "SCALE_ZERO"
    assert evidence["samples"] == 2
    assert session.cloudwatch.dimensions == [
        {"Name": "DBInstanceIdentifier", "Value": "anti-demo-aurora-writer"}
    ]


async def test_aurora_uses_exact_successful_pause_event_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    started_at = datetime.now(UTC) - timedelta(minutes=7)
    paused_at = started_at + timedelta(minutes=6, seconds=22)
    events = [
        {
            "Date": started_at + timedelta(seconds=15),
            "Message": "Successfully resumed the DB instance: anti-demo-aurora-writer",
        },
        {
            "Date": paused_at,
            "Message": "Successfully paused the DB instance: anti-demo-aurora-writer",
        },
    ]
    session = FakeAuroraSession(events=events)
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    evidence = await AuroraCredentialProvider().assert_armed(not_before=started_at)

    assert evidence["state"] == "SCALE_ZERO"
    assert evidence["evidence"] == "RDS_EVENT_SUCCESSFULLY_PAUSED"
    assert evidence["observed_at"] == paused_at.isoformat()
    assert session.cloudwatch.dimensions is None


async def test_aurora_does_not_reuse_pause_event_followed_by_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    started_at = datetime.now(UTC) - timedelta(minutes=2)
    events = [
        {
            "Date": started_at + timedelta(seconds=15),
            "Message": "Successfully paused the DB instance: anti-demo-aurora-writer",
        },
        {
            "Date": started_at + timedelta(seconds=30),
            "Message": (
                "Initiated resume for the DB instance: "
                "anti-demo-aurora-writer due to user activity"
            ),
        },
    ]
    session = FakeAuroraSession(events=events)
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    evidence = await AuroraCredentialProvider().assert_armed(not_before=started_at)

    assert evidence.get("evidence") != "RDS_EVENT_SUCCESSFULLY_PAUSED"
    assert session.cloudwatch.dimensions == [
        {"Name": "DBInstanceIdentifier", "Value": "anti-demo-aurora-writer"}
    ]


# The Round 1 return-to-idle wait, and which gate is allowed to explain it.
#
# A measured 479-second wait decomposed as 172s of Aurora holding plateau
# capacity after the app disconnected, then the 300s auto-pause floor, then 7s
# for AWS to publish the pause event. For all 479 seconds the screen read
# "Aurora writer has not produced two consecutive zero-capacity samples" -- the
# CloudWatch fallback's text, from a rule that contributed zero seconds and
# never fired. These tests pin the explanation to the gate that is blocking and,
# just as importantly, pin that nothing about the verdict moved with it.


def _resume_event(when: datetime, *, initiated: bool = False) -> dict[str, object]:
    # Verbatim shapes from `aws rds describe-events --event-categories serverless`
    # on the sealed r1 cluster, 2026-08-21. Both forms appear during a wait: the
    # `Initiated` one for the 13-22s before the resume completes.
    return {
        "Date": when,
        "Message": (
            "Initiated resume for the DB instance: "
            "anti-demo-aurora-writer due to user activity"
        )
        if initiated
        else "Successfully resumed the DB instance: anti-demo-aurora-writer",
    }


async def test_aurora_wait_names_the_pause_floor_rather_than_the_sampler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    resumed_at = datetime.now(UTC) - timedelta(seconds=120)
    session = FakeAuroraSession(events=[_resume_event(resumed_at)], capacity_acu=1.5)
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    with pytest.raises(TargetNotArmedError) as raised:
        await AuroraCredentialProvider().assert_armed()

    message = str(raised.value)
    # The gate is an RDS pause event that has not happened. The sampling rule is
    # not the gate, so it does not get to describe the wait.
    assert "zero-capacity samples" not in message
    assert f"{resumed_at:%H:%M:%SZ}" in message
    earliest = resumed_at + timedelta(seconds=AURORA_AUTO_PAUSE_SECONDS)
    assert f"{earliest:%H:%M:%SZ}" in message
    assert f"{AURORA_AUTO_PAUSE_SECONDS}s idle floor" in message


async def test_aurora_wait_calls_the_floor_a_floor_and_promises_no_pause_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Measured tails pushed the real pause 35-182 seconds past this floor across
    # six cycles, so the earliest possible pause must never read as a forecast.
    configure_aurora(monkeypatch)
    resumed_at = datetime.now(UTC) - timedelta(seconds=45)
    session = FakeAuroraSession(
        events=[_resume_event(resumed_at, initiated=True)],
        capacity_acu=2.0,
    )
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    with pytest.raises(TargetNotArmedError) as raised:
        await AuroraCredentialProvider().assert_armed()

    message = str(raised.value)
    assert "cannot pause it before" in message
    assert "a floor and not a forecast" in message
    # The clock that matters starts on Aurora's own quiesce, not on the bell.
    assert "not when the bout disconnects" in message
    for promise in ("will pause at", "expect", "should pause", "estimated"):
        assert promise not in message


async def test_aurora_wait_reports_a_pause_that_is_in_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The last 6-8 seconds of every wait: AWS has begun the pause and has not
    # yet published the success. Falling through here is what let the sampler
    # claim the floor for the tail of the wait too.
    configure_aurora(monkeypatch)
    began_at = datetime.now(UTC) - timedelta(seconds=5)
    session = FakeAuroraSession(
        events=[
            {
                "Date": began_at,
                "Message": (
                    "Initiated pause for the DB instance: anti-demo-aurora-writer"
                ),
            }
        ],
        capacity_acu=0.5,
    )
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    with pytest.raises(TargetNotArmedError) as raised:
        await AuroraCredentialProvider().assert_armed()

    message = str(raised.value)
    assert f"began pausing at {began_at:%H:%M:%SZ}" in message
    assert "successful-pause event" in message
    assert "zero-capacity samples" not in message


async def test_aurora_pause_event_still_wins_the_instant_it_lands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The load-bearing assertion of the whole change: the explanation moved and
    # the arming decision did not. A fresh pause event arms the lane even while
    # the sampler is still reporting capacity, exactly as it did before.
    configure_aurora(monkeypatch)
    started_at = datetime.now(UTC) - timedelta(minutes=8)
    paused_at = datetime.now(UTC) - timedelta(seconds=2)
    session = FakeAuroraSession(
        events=[
            _resume_event(started_at + timedelta(seconds=10)),
            {
                "Date": paused_at,
                "Message": (
                    "Successfully paused the DB instance: anti-demo-aurora-writer"
                ),
            },
        ],
        capacity_acu=1.5,
    )
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    evidence = await AuroraCredentialProvider().assert_armed(not_before=started_at)

    assert evidence["evidence"] == "RDS_EVENT_SUCCESSFULLY_PAUSED"
    assert evidence["observed_at"] == paused_at.isoformat()
    # And it never reached the sampler, so nothing about the timing changed.
    assert session.cloudwatch.dimensions is None


async def test_aurora_sampler_keeps_its_own_words_when_no_event_explains_the_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No transition inside the 60-minute event window means the events have
    # nothing to say, and inventing an explanation would be the same defect
    # pointed the other way.
    configure_aurora(monkeypatch)
    session = FakeAuroraSession(events=[], capacity_acu=1.5)
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    with pytest.raises(
        TargetNotArmedError,
        match="has not produced two consecutive zero-capacity samples",
    ):
        await AuroraCredentialProvider().assert_armed()


def test_aurora_wait_reason_reports_an_unrecognised_transition_verbatim() -> None:
    # AWS may add a serverless message this code has never seen. Naming it beats
    # describing a sampler that is not the gate, and it cannot be mistaken for a
    # known state because the raw message travels with it.
    occurred_at = datetime(2026, 8, 21, 17, 41, 29, tzinfo=UTC)
    reason = _aurora_pause_wait_reason(
        "anti-demo-aurora-writer",
        occurred_at,
        "Aurora paused the DB instance: anti-demo-aurora-writer for reasons",
        AURORA_AUTO_PAUSE_SECONDS,
    )

    assert "17:41:29Z" in reason
    assert "is not a successful pause" in reason
    assert "for reasons" in reason


async def test_aurora_rejects_zero_samples_from_before_the_redo_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: FakeAuroraSession(),
    )

    with pytest.raises(TargetNotArmedError, match="after the re-do clock began"):
        await AuroraCredentialProvider().assert_armed(not_before=datetime.now(UTC))


async def test_aurora_secret_must_point_to_validated_cluster_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: FakeAuroraSession(secret_host="other.cluster.example.com"),
    )

    with pytest.raises(TargetConfigurationError, match="secret host does not match"):
        await AuroraCredentialProvider().connection_material()


async def test_aurora_managed_secret_can_supply_credentials_without_target_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: FakeAuroraSession(secret_host=None),
    )

    material = await AuroraCredentialProvider().connection_material()

    assert material.host == "anti-demo.cluster-example.us-west-2.rds.amazonaws.com"


async def test_aurora_rejects_reader_instances_for_single_writer_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: FakeAuroraSession(readers=1),
    )

    with pytest.raises(TargetConfigurationError, match="exactly one writer"):
        await AuroraCredentialProvider().assert_armed()


async def test_permanent_postgres_error_is_fatal_and_not_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_connect(**kwargs):
        raise psycopg.errors.UndefinedTable("anti_demo_probe is missing")

    monkeypatch.setattr("server.targets.psycopg.AsyncConnection.connect", fail_connect)
    target = PsycopgPreparedTarget(
        id="lakebase",
        name="Lakebase",
        material=ConnectionMaterial(
            host="example.com",
            port=5432,
            database="anti_demo",
            user="operator",
            password="not-logged",
        ),
    )

    with pytest.raises(FatalProbeError, match="SQLSTATE 42P01"):
        await target.attempt("nonce", "expected", 1)


# --- Aurora ACU sampling ---------------------------------------------------
#
# The arming gate has always called `get_metric_statistics` on
# `ServerlessDatabaseCapacity` at Period=60 and then read a single boolean out
# of the response. These cover retaining that data and integrating it across a
# bout window instead, so Aurora can be priced from consumed capacity rather
# than from elapsed-time-times-ceiling. AWS credentials for this installation
# expired at 2026-08-21T07:57:59Z, so none of this has been exercised against
# live CloudWatch: everything below runs against fakes.


class RecordingCloudWatchClient:
    """Records the request and replays a scripted capacity series."""

    def __init__(self, datapoints: list[dict[str, object]] | None = None) -> None:
        self.datapoints = datapoints or []
        self.requests: list[dict[str, object]] = []

    def get_metric_statistics(self, **kwargs):
        self.requests.append(kwargs)
        return {"Datapoints": list(self.datapoints)}


class ExplodingCloudWatchClient:
    def get_metric_statistics(self, **kwargs):
        raise ClientError(
            {"Error": {"Code": "ExpiredToken", "Message": "The security token expired"}},
            "GetMetricStatistics",
        )


class AuroraSamplingSession:
    def __init__(self, cloudwatch) -> None:
        self.rds = FakeAuroraRdsClient()
        self.sts = FakeStsClient()
        self.cloudwatch = cloudwatch

    def client(self, service: str):
        return {"rds": self.rds, "sts": self.sts, "cloudwatch": self.cloudwatch}[service]


def _bout_window() -> tuple[datetime, datetime]:
    started = datetime(2026, 8, 21, 1, 31, 32, tzinfo=UTC)
    return started, started + timedelta(seconds=813)


async def test_the_arming_gates_samples_are_never_turned_into_a_cost_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate's window is `now - 10 min -> now`, and it is worthless as a cost
    measurement precisely *because* the gate works: arming refuses unless every
    sample in it reads zero. Integrating it would set
    `BoutTelemetry.observed_acu_seconds_above_floor` to 0 and stamp it MEASURED --
    a fabricated zero wearing the highest evidence grade, which is worse than the
    unset field. Bout-window sampling is `sample_acu_seconds`, which reaches past
    the bell; the gate must not grow a second job.
    """

    configure_aurora(monkeypatch)
    session = FakeAuroraSession()
    monkeypatch.setattr("server.targets.boto3.Session", lambda **kwargs: session)

    evidence = await AuroraCredentialProvider().assert_armed()

    assert "acu_seconds_above_floor" not in evidence
    assert not any("acu_second" in str(key) for key in evidence)
    # The gate still reports what it is for: a writer proved parked at 0 ACU.
    assert evidence["capacity_acu"] == 0.0
    assert evidence["state"] == "SCALE_ZERO"


async def test_sampling_integrates_a_bout_window_into_acu_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    cloudwatch = RecordingCloudWatchClient(
        [
            {"Timestamp": datetime.now(UTC), "Average": 2.0, "Maximum": 2.0},
            {"Timestamp": datetime.now(UTC), "Average": 1.0, "Maximum": 2.0},
            {"Timestamp": datetime.now(UTC), "Average": 0.5, "Maximum": 1.0},
        ]
    )
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: AuroraSamplingSession(cloudwatch),
    )
    started, ended = _bout_window()

    acu_seconds = await AuroraCredentialProvider().sample_acu_seconds(
        started,
        ended,
        writer_instance_id="anti-demo-aurora-writer",
    )

    assert acu_seconds == Decimal("210")
    request = cloudwatch.requests[0]
    assert request["MetricName"] == "ServerlessDatabaseCapacity"
    assert request["Period"] == 60
    assert request["Dimensions"] == [
        {"Name": "DBInstanceIdentifier", "Value": "anti-demo-aurora-writer"}
    ]


async def test_the_window_reaches_past_the_bout_to_catch_the_billed_descent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Aurora's auto-pause descent is billed capacity that no lane clock covers --
    # 97.2% of Round 1's measured cost fell after the bell. The 300-second floor
    # is a minimum, not the descent: one measured Round 5 bout held capacity for
    # 15 minutes past its proxy deletion, so the window is sized well beyond it.
    configure_aurora(monkeypatch)
    cloudwatch = RecordingCloudWatchClient([{"Average": 1.0}])
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: AuroraSamplingSession(cloudwatch),
    )
    started, ended = _bout_window()

    await AuroraCredentialProvider().sample_acu_seconds(
        started,
        ended,
        writer_instance_id="anti-demo-aurora-writer",
    )

    request = cloudwatch.requests[0]
    assert request["EndTime"] == ended + timedelta(seconds=float(ACU_SAMPLE_TAIL_SECONDS))
    assert request["EndTime"] > ended + timedelta(seconds=AURORA_AUTO_PAUSE_SECONDS)
    assert request["StartTime"] == started - timedelta(seconds=float(ACU_SAMPLE_LEAD_SECONDS))
    # SampleCount is what makes the partial buckets at either end integrable.
    assert "SampleCount" in request["Statistics"]


async def test_sampling_may_read_the_cluster_dimension_as_well_as_the_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)
    cloudwatch = RecordingCloudWatchClient([{"Average": 2.0}])
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: AuroraSamplingSession(cloudwatch),
    )
    started, ended = _bout_window()

    acu_seconds = await AuroraCredentialProvider().sample_acu_seconds(
        started, ended, cluster_level=True
    )

    assert acu_seconds == Decimal("120")
    assert cloudwatch.requests[0]["Dimensions"] == [
        {"Name": "DBClusterIdentifier", "Value": "anti-demo-aurora"}
    ]


@pytest.mark.parametrize(
    "cloudwatch",
    [
        ExplodingCloudWatchClient(),
        RecordingCloudWatchClient([]),
        RecordingCloudWatchClient([{"Average": "not a number"}]),
    ],
    ids=["cloudwatch_unreachable", "no_datapoints", "malformed_payload"],
)
async def test_a_sampling_failure_can_never_fail_a_bout(
    monkeypatch: pytest.MonkeyPatch,
    cloudwatch,
) -> None:
    # Sampling is an opportunistic improvement to an estimate, never a gate.
    # Every failure mode degrades to "unmeasured" so the telemetry field stays
    # unset and the estimator falls back to its ceiling convention.
    configure_aurora(monkeypatch)
    monkeypatch.setattr(
        "server.targets.boto3.Session",
        lambda **kwargs: AuroraSamplingSession(cloudwatch),
    )
    started, ended = _bout_window()

    assert (
        await AuroraCredentialProvider().sample_acu_seconds(
            started, ended, writer_instance_id="anti-demo-aurora-writer"
        )
        is None
    )


async def test_a_missing_aws_session_entirely_still_returns_unmeasured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_aurora(monkeypatch)

    def explode(**kwargs):
        raise BotoCoreError()

    monkeypatch.setattr("server.targets.boto3.Session", explode)
    started, ended = _bout_window()

    assert await AuroraCredentialProvider().sample_acu_seconds(started, ended) is None
